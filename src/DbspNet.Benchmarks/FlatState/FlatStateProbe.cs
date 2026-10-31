// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks.FlatState;

/// <summary>
/// The flat-state probe (docs/design-flat-state-probe.md): Nexmark q20 with the algorithm held
/// fixed and only the memory model varied. One configuration per process:
/// <code>flatstate &lt;engine|managed|poh|native&gt; &lt;W&gt; &lt;events&gt; &lt;retract 0|1&gt; [batch] [segments] [checkpoint-dir]</code>
/// Prints one <c>RESULT</c> line. Only route + step + sink is timed; generating the batch is not.
/// With retractions on, each tick also retracts every tenth bid of the tick ten ticks back.
/// </summary>
internal static class FlatStateProbe
{
    private const int RetractLag = 10;
    private const int RetractEvery = 10;
    private const int WarmupEvents = 300_000;

    /// <summary><c>FLATSTATE_INPUT=utf8</c>: string cells arrive as UTF-8, as a connector delivers
    /// them — each tick's strings packed into one buffer (Arrow-style) before the timer starts, every
    /// cell a <see cref="Utf8String"/> slice of it. The managed arm then decodes each cell to a
    /// <c>string</c>, the flat arms copy the bytes into a UTF-8 arena, and the engine keeps the
    /// slices. Default (<c>string</c>): cells are the generator's managed strings.</summary>
    private static readonly bool Utf8Input =
        string.Equals(Environment.GetEnvironmentVariable("FLATSTATE_INPUT"), "utf8", StringComparison.OrdinalIgnoreCase);

    public static int Run(string[] args)
    {
        string Arg(int i, string fallback) => args.Length > i ? args[i] : fallback;
        int IntArg(int i, int fallback) => int.Parse(Arg(i, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);

        var arm = Arg(1, "managed");
        var workers = IntArg(2, 1);
        var events = IntArg(3, 1_000_000);
        var retract = IntArg(4, 0) != 0;
        var batch = IntArg(5, 10_000);
        var segments = IntArg(6, 5);
        var checkpointDir = args.Length > 7 ? args[7] : null;

        // Warm the JIT on a throwaway instance, then start from a collected heap.
        using (var warm = Create(arm, workers, retract))
        {
            Feed(warm, WarmupEvents, retract, batch, 1);
        }

        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();

        using var target = Create(arm, workers, retract);
        var m = Feed(target, events, retract, batch, segments);

        var state = target.StateChecksum();
        var managedLive = LiveManagedBytes();
        var native = NativeAccount.Bytes;
        var r = new List<string>
        {
            $"arm={arm}", $"input={(Utf8Input ? "utf8" : "string")}", $"W={workers}", $"events={events}", $"retract={(retract ? 1 : 0)}", $"batch={batch}",
            $"ns_ev={m.TimedNs / events:F1}", $"timed_ms={m.TimedNs / 1e6:F0}", $"cpu_ms={m.CpuMs:F0}",
            $"process_cpu_ms={Environment.CpuUsage.TotalTime.TotalMilliseconds:F0}",
            $"alloc_B_ev={m.AllocBytes / (double)events:F1}", $"pause_ms={m.PauseMs:F0}",
            $"gc0={m.Gen0}", $"gc1={m.Gen1}", $"gc2={m.Gen2}",
            $"live_managed_MB={managedLive / 1048576.0:F0}", $"native_MB={native / 1048576.0:F0}",
            $"out_ck={m.OutSum:x16}", $"out_rows={m.OutRows}",
            $"state_ck={(state.Rows < 0 ? "n/a" : state.Sum.ToString("x16", CultureInfo.InvariantCulture))}",
            $"state_rows={state.Rows}",
            $"seg_ms={string.Join(',', m.SegmentNs.Select(n => (n / 1e6).ToString("F0", CultureInfo.InvariantCulture)))}",
            $"seg_pause_ms={string.Join(',', m.SegmentPauseMs.Select(p => p.ToString("F0", CultureInfo.InvariantCulture)))}",
        };

        if (checkpointDir is not null && target is ArmWorkers w)
        {
            r.AddRange(CheckpointRound(arm, w, state, checkpointDir));
        }

        Console.WriteLine("RESULT " + string.Join(' ', r));
        return 0;
    }

    private sealed class Measured
    {
        public double TimedNs;
        public long AllocBytes;
        public double PauseMs;
        public double CpuMs;
        public int Gen0;
        public int Gen1;
        public int Gen2;
        public ulong OutSum;
        public long OutRows;
        public List<double> SegmentNs = [];
        public List<double> SegmentPauseMs = [];
    }

    private static Measured Feed(IArm arm, int events, bool retract, int batch, int segments)
    {
        var m = new Measured();
        var bids = new List<(object?[] Row, long W)>(batch);
        var auctions = new List<(object?[] Row, long W)>(batch);
        var ring = new Queue<List<object?[]>>();
        var perSegment = (events + segments - 1) / segments;
        double segNs = 0, segPause = 0;
        var seen = 0;
        var sw = new Stopwatch();
        using var stream = Stream(events, 42, GeneratorProfile.Feldera).GetEnumerator();
        var more = true;
        while (more)
        {
            bids.Clear();
            auctions.Clear();
            var tickBids = new List<object?[]>();
            var inTick = 0;
            while (inTick < batch && (more = stream.MoveNext()))
            {
                var e = stream.Current;
                inTick++;
                if (e.Table == NexmarkTable.Bid)
                {
                    bids.Add((e.Row, 1));
                    tickBids.Add(e.Row);
                }
                else if (e.Table == NexmarkTable.Auction)
                {
                    auctions.Add((e.Row, 1));
                }
            }

            if (inTick == 0)
            {
                break;
            }

            if (retract)
            {
                if (ring.Count == RetractLag)
                {
                    var old = ring.Dequeue();
                    for (var i = 0; i < old.Count; i += RetractEvery)
                    {
                        bids.Add((old[i], -1));
                    }
                }

                ring.Enqueue(tickBids);
            }

            if (Utf8Input)
            {
                ToUtf8(bids, auctions);
            }

            var pause0 = GC.GetTotalPauseDuration();
            var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
            var g0 = GC.CollectionCount(0);
            var g1 = GC.CollectionCount(1);
            var g2 = GC.CollectionCount(2);
            var cpu0 = Environment.CpuUsage.TotalTime;
            sw.Restart();
            arm.Step(bids, auctions);
            var (sum, rows) = arm.Sink();
            sw.Stop();
            m.CpuMs += (Environment.CpuUsage.TotalTime - cpu0).TotalMilliseconds;
            var pause = (GC.GetTotalPauseDuration() - pause0).TotalMilliseconds;
            m.AllocBytes += GC.GetTotalAllocatedBytes(precise: false) - alloc0;
            m.Gen0 += GC.CollectionCount(0) - g0;
            m.Gen1 += GC.CollectionCount(1) - g1;
            m.Gen2 += GC.CollectionCount(2) - g2;
            var ns = sw.Elapsed.TotalNanoseconds;
            m.TimedNs += ns;
            m.PauseMs += pause;
            m.OutSum += sum;
            m.OutRows += rows;
            segNs += ns;
            segPause += pause;
            seen += inTick;
            if (seen >= perSegment * (m.SegmentNs.Count + 1) || !more)
            {
                m.SegmentNs.Add(segNs);
                m.SegmentPauseMs.Add(segPause);
                segNs = 0;
                segPause = 0;
            }
        }

        return m;
    }

    /// <summary>Rewrite the tick's rows with every string cell a slice of one UTF-8 buffer.</summary>
    private static void ToUtf8(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions)
    {
        var bytes = 0;
        foreach (var list in (ReadOnlySpan<List<(object?[] Row, long W)>>)[bids, auctions])
        {
            foreach (var (r, _) in list)
            {
                foreach (var c in r)
                {
                    if (c is string str)
                    {
                        bytes += System.Text.Encoding.UTF8.GetByteCount(str);
                    }
                }
            }
        }

        var buffer = new byte[bytes];
        var at = 0;
        foreach (var list in (ReadOnlySpan<List<(object?[] Row, long W)>>)[bids, auctions])
        {
            for (var i = 0; i < list.Count; i++)
            {
                var (r, w) = list[i];
                var copy = (object?[])r.Clone();
                for (var c = 0; c < copy.Length; c++)
                {
                    if (copy[c] is string str)
                    {
                        var n = System.Text.Encoding.UTF8.GetBytes(str, buffer.AsSpan(at));
                        copy[c] = Utf8String.FromBytes(buffer.AsMemory(at, n));
                        at += n;
                    }
                }

                list[i] = (copy, w);
            }
        }
    }

    private static long LiveManagedBytes()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetGCMemoryInfo(GCKind.FullBlocking).PromotedBytes;
    }

    private static List<string> CheckpointRound(string arm, ArmWorkers w, (ulong Sum, long Rows) state, string root)
    {
        var dir = Path.Combine(root, $"flatstate-{Environment.ProcessId}");
        Directory.CreateDirectory(dir);
        try
        {
            var paths = Enumerable.Range(0, w.Workers.Length).Select(i => Path.Combine(dir, $"worker-{i}.ckpt")).ToArray();
            var probeKey = w.LastBidAuction;
            var sw = Stopwatch.StartNew();
            Parallel.For(0, w.Workers.Length, i => w.Workers[i].Checkpoint(paths[i]));
            var writeMs = sw.Elapsed.TotalMilliseconds;
            var bytes = paths.Sum(p => new FileInfo(p).Length);
            w.Release();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);

            var results = new List<string> { $"ckpt_write_ms={writeMs:F0}", $"ckpt_MB={bytes / 1048576.0:F0}" };
            void Round(string label, Func<string, IQ20Worker> restore)
            {
                sw.Restart();
                var restored = new IQ20Worker[paths.Length];
                Parallel.For(0, paths.Length, i => restored[i] = restore(paths[i]));
                var restoreMs = sw.Elapsed.TotalMilliseconds;
                sw.Restart();
                long probed = 0;
                foreach (var x in restored)
                {
                    probed += x.ProbeOne(probeKey);
                }

                var probeUs = sw.Elapsed.TotalMicroseconds;
                sw.Restart();
                var sums = new (ulong Sum, long Rows)[restored.Length];
                Parallel.For(0, restored.Length, i => sums[i] = restored[i].StateChecksum());
                var scanMs = sw.Elapsed.TotalMilliseconds;
                var ok = sums.Aggregate(0UL, (a, s) => a + s.Sum) == state.Sum && sums.Sum(s => s.Rows) == state.Rows;
                foreach (var x in restored)
                {
                    x.Dispose();
                }

                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                results.Add($"{label}_ms={restoreMs:F0} {label}_probe_us={probeUs:F0} {label}_scan_ms={scanMs:F0} {label}_ok={ok}");
                _ = probed;
            }

            switch (arm)
            {
                case "managed":
                    Round("restore", ManagedQ20Worker.Restore);
                    break;
                case "poh" when Utf8Input:
                    Round("restore", FlatQ20Worker<PohPages, byte>.Restore);
                    break;
                case "poh":
                    Round("restore", FlatQ20Worker<PohPages, char>.Restore);
                    break;
                case "native" when Utf8Input:
                    Round("restore", FlatQ20Worker<NativePages, byte>.Restore);
                    Round("mmap", FlatQ20Worker<NativePages, byte>.RestoreMapped);
                    break;
                default:
                    Round("restore", FlatQ20Worker<NativePages, char>.Restore);
                    Round("mmap", FlatQ20Worker<NativePages, char>.RestoreMapped);
                    break;
            }

            return results;
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static IArm Create(string arm, int workers, bool retract) => arm switch
    {
        "engine" => new EngineArm(workers, retract),
        "managed" => new ArmWorkers(workers, () => new ManagedQ20Worker()),
        "poh" when Utf8Input => new ArmWorkers(workers, () => new FlatQ20Worker<PohPages, byte>()),
        "poh" => new ArmWorkers(workers, () => new FlatQ20Worker<PohPages, char>()),
        "native" when Utf8Input => new ArmWorkers(workers, () => new FlatQ20Worker<NativePages, byte>()),
        "native" => new ArmWorkers(workers, () => new FlatQ20Worker<NativePages, char>()),
        _ => throw new ArgumentException($"unknown arm '{arm}' (engine|managed|poh|native)"),
    };

    private interface IArm : IDisposable
    {
        void Step(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions);

        (ulong Sum, long Rows) Sink();

        (ulong Sum, long Rows) StateChecksum();
    }

    /// <summary>The hand-written arms: W shards on the join key, stepped in parallel; the sink
    /// reads every worker's output serially on the driver, as the engine's sink does.</summary>
    private sealed class ArmWorkers : IArm
    {
        private readonly List<(object?[] Row, long W)>[] _bidShards;
        private readonly List<(object?[] Row, long W)>[] _auctionShards;

        public ArmWorkers(int workers, Func<IQ20Worker> make)
        {
            Workers = Enumerable.Range(0, workers).Select(_ => make()).ToArray();
            _bidShards = Enumerable.Range(0, workers).Select(_ => new List<(object?[], long)>()).ToArray();
            _auctionShards = Enumerable.Range(0, workers).Select(_ => new List<(object?[], long)>()).ToArray();
        }

        public IQ20Worker[] Workers { get; private set; }

        public long LastBidAuction { get; private set; }

        public void Step(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions)
        {
            if (bids.Count > 0)
            {
                LastBidAuction = (long)bids[^1].Row[0]!;
            }

            if (Workers.Length == 1)
            {
                Workers[0].Step(bids, auctions);
                return;
            }

            foreach (var s in _bidShards)
            {
                s.Clear();
            }

            foreach (var s in _auctionShards)
            {
                s.Clear();
            }

            var n = (ulong)Workers.Length;
            foreach (var b in bids)
            {
                _bidShards[(int)(FlatHash.Long((long)b.Row[0]!) % n)].Add(b);
            }

            foreach (var a in auctions)
            {
                _auctionShards[(int)(FlatHash.Long((long)a.Row[0]!) % n)].Add(a);
            }

            Parallel.For(0, Workers.Length, i => Workers[i].Step(_bidShards[i], _auctionShards[i]));
        }

        public (ulong Sum, long Rows) Sink()
        {
            ulong sum = 0;
            long rows = 0;
            foreach (var w in Workers)
            {
                var (s, r) = w.SinkTick();
                sum += s;
                rows += r;
            }

            return (sum, rows);
        }

        public (ulong Sum, long Rows) StateChecksum()
        {
            ulong sum = 0;
            long rows = 0;
            foreach (var w in Workers)
            {
                var (s, r) = w.StateChecksum();
                sum += s;
                rows += r;
            }

            return (sum, rows);
        }

        /// <summary>Free the state (before a restore round), keeping the object for its metadata.</summary>
        public void Release()
        {
            foreach (var w in Workers)
            {
                w.Dispose();
            }

            Workers = [];
        }

        public void Dispose() => Release();
    }

    /// <summary>The engine: q20 through SQL on the typed path, W=1 or parallel.</summary>
    private sealed class EngineArm : IArm
    {
        private readonly ParallelTypedCompiledQuery _q;
        private readonly TypedTableInput _bid;
        private readonly TypedTableInput _auction;

        public EngineArm(int workers, bool retract)
        {
            var ddl = NexmarkQueries.Ddl
                .Select(s => retract ? s.Replace("WITH ('append_only' = 'true')", "", StringComparison.Ordinal) : s)
                .ToArray();
            var sql = NexmarkQueries.All.Single(q => q.Id == "q20").Sql;
            var catalog = new Catalog();
            var resolver = new Resolver(catalog);
            foreach (var s in ddl)
            {
                resolver.Resolve(Parser.ParseStatement(s));
            }

            var plan = PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
            if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q))
            {
                throw new InvalidOperationException("q20 did not compile to the typed parallel path");
            }

            _q = q!;
            _bid = _q.Table("bid");
            _auction = _q.Table("auction");
        }

        public void Step(List<(object?[] Row, long W)> bids, List<(object?[] Row, long W)> auctions)
        {
            if (bids.Count > 0)
            {
                _bid.Push(bids);
            }

            if (auctions.Count > 0)
            {
                _auction.Push(auctions);
            }

            _q.Step();
        }

        public (ulong Sum, long Rows) Sink()
        {
            ulong sum = 0;
            long rows = 0;
            foreach (var (v, w) in _q.Current)
            {
                var h = FlatHash.Seed;
                foreach (var cell in v)
                {
                    h = FlatHash.Combine(h, cell switch
                    {
                        long x => FlatHash.Long(x),
                        Timestamp t => FlatHash.Long(t.Microseconds),
                        Utf8String u => FlatHash.Str(u.Span),
                        string x => FlatHash.Str(x),
                        _ => throw new InvalidOperationException($"unexpected cell {cell?.GetType().Name ?? "null"}"),
                    });
                }

                sum += (ulong)w * h;
                rows += w;
            }

            return (sum, rows);
        }

        public (ulong Sum, long Rows) StateChecksum() => (0, -1);

        public void Dispose() => _q.Dispose();
    }
}
