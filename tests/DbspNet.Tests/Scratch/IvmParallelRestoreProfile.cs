// Restore-only profile of an existing ivm-bench SF=3 checkpoint, serial or parallel, to apportion
// why the W=12 restore is slower than the serial one (docs/design-durable-state.md §1b).
// Each variant restores into a freshly compiled program and reports wall, allocation and GC.
//
// Gated on env vars, no-op otherwise:
//   IVM_SPEC          deploy spec JSON (dbt_to_program.py output)
//   IVM_SNAPSHOT_DIR  a checkpoint written by IvmMultiBatch (IVM_SNAPSHOT_DIR); its recorded W is
//                     the circuit's W
//   IVM_VARIANTS      space-separated, run in order (default "baseline"):
//                       baseline   the shipped restore (ParallelSnapshot / Snapshot.ReadAsync)
//                       deg=D      parallel only: every (worker, operator) load in ONE pool of D
//                       taskrun    the shipped restore's per-worker split, each worker on Task.Run
//                       seq        every load one at a time with the per-operator profile; prints
//                                  the summed CPU time per operator across workers
//   IVM_RESTORE_REPEAT  run the whole variant list this many times (default 1)
//   IVM_WINDOW_SHAPES=1 after the last variant, print every window-aggregate operator's partition
//                     shape per worker (partitions, rows, largest partition, frame kind)
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using DbspNet.Core.Circuit;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmParallelRestoreProfile
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmParallelRestoreProfile(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ProfileRestore()
    {
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        var snapshotDir = Environment.GetEnvironmentVariable("IVM_SNAPSHOT_DIR");
        if (string.IsNullOrEmpty(specPath) || string.IsNullOrEmpty(snapshotDir))
        {
            _out.WriteLine("IVM_SPEC / IVM_SNAPSHOT_DIR not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = IvmSpecViews.ToCompile(spec.Outputs, spec.Output_Bindings.Select(o => o.View));
        var fs = new LocalTableFileSystem(snapshotDir);
        var workers = await ParallelSnapshot.RecordedWorkersAsync(fs) ?? throw new InvalidOperationException("no checkpoint");
        var current = await Snapshot.TryReadCurrentPointerAsync(fs, default) ?? throw new InvalidOperationException("no pointer");
        var codecs = ArrowSqlSnapshotCodecs.Instance;
        var variants = (Environment.GetEnvironmentVariable("IVM_VARIANTS") ?? "baseline")
            .Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var shapes = Environment.GetEnvironmentVariable("IVM_WINDOW_SHAPES") == "1";
        var repeat = int.TryParse(Environment.GetEnvironmentVariable("IVM_RESTORE_REPEAT"), out var r) ? r : 1;
        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"PROFILE workers={workers} snapshot={current} processors={Environment.ProcessorCount} default_degree={Snapshot.DefaultRestoreParallelism}"));

        for (var rep = 0; rep < repeat; rep++)
        {
            foreach (var variant in variants)
            {
                GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
                if (workers > 1)
                {
                    Assert.True(
                        SqlProgram.TryCompileParallel(
                            spec.Program, outputViews, workers, out var compiled, codecs,
                            numericStringCoercion: true, nullCollation: NullCollation.Low),
                        "parallel compile refused the program");
                    using (compiled)
                    {
                        await Measure(variant, () => RestoreParallel(compiled!.Circuit, fs, current, variant));
                        if (shapes && variant == variants[^1])
                        {
                            PrintWindowShapes(compiled!.Circuit.Replicas);
                        }
                    }
                }
                else
                {
                    var compiled = SqlProgram.Compile(
                        spec.Program, outputViews, codecs, new CompileOptions(),
                        numericStringCoercion: true, nullCollation: NullCollation.Low);
                    await Measure(variant, () => RestoreSerial(compiled.Circuit, fs, current, variant));
                    if (shapes && variant == variants[^1])
                    {
                        PrintWindowShapes([compiled.Circuit]);
                    }
                }
            }
        }
    }

    private async Task Measure(string variant, Func<Task> restore)
    {
        var g0 = GC.CollectionCount(0);
        var g1 = GC.CollectionCount(1);
        var g2 = GC.CollectionCount(2);
        var pause0 = GC.GetTotalPauseDuration();
        var alloc0 = GC.GetTotalAllocatedBytes(precise: true);
        var cpu0 = Process.GetCurrentProcess().TotalProcessorTime;
        var t0 = Stopwatch.GetTimestamp();
        await restore();
        var wall = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
        var cpu = (Process.GetCurrentProcess().TotalProcessorTime - cpu0).TotalMilliseconds;
        var alloc = GC.GetTotalAllocatedBytes(precise: true) - alloc0;
        var pause = (GC.GetTotalPauseDuration() - pause0).TotalMilliseconds;
        _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"VARIANT {variant} wall_ms={wall:F0} cpu_ms={cpu:F0} alloc_gib={alloc / (double)(1L << 30):F2} " +
            $"gc_pause_ms={pause:F0} gc0={GC.CollectionCount(0) - g0} gc1={GC.CollectionCount(1) - g1} gc2={GC.CollectionCount(2) - g2} " +
            $"heap_gib={GC.GetTotalMemory(false) / (double)(1L << 30):F2}"));
    }

    private async Task RestoreSerial(RootCircuit circuit, LocalTableFileSystem fs, string current, string variant)
    {
        var manifest = await Snapshot.ReadManifestAsync(circuit, fs, current, default);
        if (variant == "seq")
        {
            Snapshot.ProfileLoad = true;
            try
            {
                await Snapshot.LoadDirAsync(circuit, fs, current, manifest, 1, default);
            }
            finally
            {
                Snapshot.ProfileLoad = false;
            }

            PrintOps(Snapshot.LastLoadProfile.Select(p => (p.Index, p.Operator, p.Ms, p.Ms)));
            return;
        }

        await Snapshot.LoadDirAsync(circuit, fs, current, manifest, Snapshot.DefaultRestoreParallelism, default);
    }

    private async Task RestoreParallel(ParallelCircuit circuit, LocalTableFileSystem fs, string current, string variant)
    {
        if (variant == "baseline")
        {
            await ParallelSnapshot.ReadAsync(circuit, fs);
            return;
        }

        var replicas = circuit.Replicas;
        var dirs = Enumerable.Range(0, replicas.Count).Select(w => $"{current}/worker-{w}").ToArray();
        var manifests = new SnapshotManifest[replicas.Count];
        for (var w = 0; w < replicas.Count; w++)
        {
            manifests[w] = await Snapshot.ReadManifestAsync(replicas[w], fs, dirs[w], default);
        }

        if (variant == "taskrun")
        {
            var perReplica = Math.Max(1, Snapshot.DefaultRestoreParallelism / replicas.Count);
            await Task.WhenAll(Enumerable.Range(0, replicas.Count).Select(w => Task.Run(async () =>
                await Snapshot.LoadDirAsync(replicas[w], fs, dirs[w], manifests[w], perReplica, default))));
            return;
        }

        if (variant == "seq")
        {
            // Per (worker, operator) time, one load at a time: summed it is the restore's total
            // single-threaded work, comparable with the serial program's own seq profile.
            var byOp = new Dictionary<int, (string Op, double Sum, double Max)>();
            var perWorker = new double[replicas.Count];
            Snapshot.ProfileLoad = true;
            try
            {
                for (var w = 0; w < replicas.Count; w++)
                {
                    await Snapshot.LoadDirAsync(replicas[w], fs, dirs[w], manifests[w], 1, default);
                    foreach (var (index, op, ms) in Snapshot.LastLoadProfile)
                    {
                        perWorker[w] += ms;
                        var e = byOp.TryGetValue(index, out var prev) ? prev : (op, 0.0, 0.0);
                        byOp[index] = (op, e.Item2 + ms, Math.Max(e.Item3, ms));
                    }
                }
            }
            finally
            {
                Snapshot.ProfileLoad = false;
            }

            _out.WriteLine("WORKERS_MS " + string.Join(' ', perWorker.Select(ms => ms.ToString("F0", CultureInfo.InvariantCulture))));
            PrintOps(byOp.Select(kv => (kv.Key, kv.Value.Op, kv.Value.Sum, kv.Value.Max)));
            return;
        }

        if (variant.StartsWith("deg=", StringComparison.Ordinal))
        {
            var degree = int.Parse(variant["deg=".Length..], CultureInfo.InvariantCulture);
            for (var w = 0; w < replicas.Count; w++)
            {
                replicas[w].RestoreTickCount(manifests[w].Tick);
                replicas[w].RestoreLogicalTime(manifests[w].LogicalTime);
            }

            var items = Enumerable.Range(0, replicas.Count)
                .SelectMany(w => manifests[w].SnapshottedIndices.Select(i => (W: w, I: i)))
                .OrderBy(x => x.I).ThenBy(x => x.W)
                .ToList();
            await Parallel.ForEachAsync(
                items,
                new ParallelOptions { MaxDegreeOfParallelism = degree },
                async (x, ct) =>
                {
                    var s = (ISnapshotable)replicas[x.W].Operators[x.I];
                    await s.LoadAsync(new TableFileSystemSnapshotContext(fs, dirs[x.W] + "/op-" + x.I), ct);
                });
            return;
        }

        throw new ArgumentException($"unknown variant '{variant}'");
    }

    private void PrintWindowShapes(IReadOnlyList<RootCircuit> replicas)
    {
        const System.Reflection.BindingFlags Flags = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic;
        for (var i = 0; i < replicas[0].Operators.Count; i++)
        {
            var type = replicas[0].Operators[i].GetType();
            if (!type.Name.StartsWith("PartitionedWindowAggregateOp", StringComparison.Ordinal))
            {
                continue;
            }

            var preceding = type.GetField("_preceding", Flags)!.GetValue(replicas[0].Operators[i]);
            var ordered = type.GetField("_orderValueOf", Flags)!.GetValue(replicas[0].Operators[i]) is not null;
            var parts = new List<string>();
            long rows = 0, largest = 0;
            foreach (var replica in replicas)
            {
                var accum = (System.Collections.IDictionary)type.GetField("_accum", Flags)!.GetValue(replica.Operators[i])!;
                long r = 0, big = 0;
                foreach (System.Collections.DictionaryEntry e in accum)
                {
                    var n = ((System.Collections.ICollection)e.Value!).Count;
                    r += n;
                    big = Math.Max(big, n);
                }

                rows += r;
                largest = Math.Max(largest, big);
                parts.Add($"{accum.Count}/{r}/{big}");
            }

            var frame = !ordered ? "whole" : preceding is null ? "running" : $"bounded({preceding})";
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"WINDOW op={i} frame={frame} rows={rows} largest_partition={largest} per_worker(parts/rows/largest)={string.Join(' ', parts)}"));
        }
    }

    private void PrintOps(IEnumerable<(int Index, string Op, double Sum, double Max)> ops)
    {
        var list = ops.OrderByDescending(o => o.Sum).ToList();
        _out.WriteLine(string.Create(CultureInfo.InvariantCulture, $"OPS total_ms={list.Sum(o => o.Sum):F0} count={list.Count}"));
        foreach (var byType in list.GroupBy(o => o.Op).OrderByDescending(g => g.Sum(o => o.Sum)))
        {
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"OPTYPE {byType.Key} n={byType.Count()} sum_ms={byType.Sum(o => o.Sum):F0}"));
        }

        foreach (var o in list.Take(15))
        {
            _out.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"OP {o.Index} {o.Op} sum_ms={o.Sum:F0} max_worker_ms={o.Max:F0}"));
        }
    }

    private sealed record Spec(
        List<string> Program,
        List<string>? Outputs,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
