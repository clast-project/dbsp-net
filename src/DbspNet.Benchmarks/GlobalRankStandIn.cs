// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Benchmarks;

/// <summary>
/// Stage 1 of the range-partitioned global rank (docs/design-bounded-memory.md §5.50.7): a synthetic
/// stand-in for <c>fact_engagement_status_monthly_snapshot</c>'s tail, <c>ROW_NUMBER() OVER (ORDER BY
/// ten keys)</c> with no <c>PARTITION BY</c> over N rows (15.16M on issue #2's SF25 tick 18), shaped like
/// the view's aggregate output: ten integer keys distinct as a tuple (leading key one of 13 months, the
/// second one of ~72K households), a count and a units value, arriving hash-sharded by the key as the
/// aggregate's output does. Arms, at W workers, each a fresh circuit over the same rows:
/// <list type="bullet">
/// <item><c>gather</c>: <c>Exchange(_ =&gt; 0)</c> → the rank → the output projection → integrate (today's plan, hand built);</item>
/// <item><c>range</c>: <c>Exchange(range)</c> on splitters sampled from the rows → the same rank over the
/// worker's range → a prototype offset operator (an all-gather of the ranges' count changes; on a changed
/// offset it re-emits its whole range from its own integrated copy) → projection → integrate;</item>
/// <item><c>sql</c>: the real view compiled at W (fidelity check for <c>gather</c>).</item>
/// </list>
/// Ticks: 1 bulk load; 2 a thousand value updates at unchanged keys (no offset moves); 3 a thousand inserts
/// at the front of the order (every later range shifts). Each tick's output checksum must agree across arms.
/// <c>dotnet run -c Release -- globalrank [rows] [runs] [arms] [workers]</c>
/// </summary>
internal static class GlobalRankStandIn
{
    private const int Keys = 10;
    private const int Households = 72_002;
    private const int Changes = 1_000;

    private static readonly int[] Months =
        [20250101, 20250201, 20250301, 20250401, 20250501, 20250601, 20250701, 20250801, 20250901, 20251001, 20251101, 20251201, 20260101];

    private static readonly string[] Ddl =
    [
        "CREATE TABLE agg (dim_date_key INT, household_key INT, account_offer_key INT, account_usage_key INT, platform_key INT, " +
            "preference_key INT, product_tier_key INT, product_version_key INT, engagement_type_key INT, mode_key INT, " +
            "engagement_total_count BIGINT, effective_units_count DOUBLE)",
        "CREATE VIEW v AS SELECT TRY_CAST(ROW_NUMBER() OVER (ORDER BY dim_date_key NULLS FIRST, household_key NULLS FIRST, " +
            "account_offer_key NULLS FIRST, account_usage_key NULLS FIRST, platform_key NULLS FIRST, preference_key NULLS FIRST, " +
            "product_tier_key NULLS FIRST, product_version_key NULLS FIRST, engagement_type_key NULLS FIRST, mode_key NULLS FIRST) AS INT) AS primary_key, " +
            "dim_date_key, household_key, account_offer_key, account_usage_key, platform_key, preference_key, product_tier_key, " +
            "product_version_key, engagement_type_key, mode_key, TRY_CAST(engagement_total_count AS INT) AS engagement_total_count, " +
            "TRY_CAST(ROUND(effective_units_count) AS INT) AS effective_units_count FROM agg WHERE engagement_total_count > 0",
    ];

    private static readonly SortKeyComparer<StructuralRow> Order = Comparer(StructuralRowComparer.Instance);
    private static readonly SortKeyComparer<StructuralRow> SortKeyOnly = Comparer(ConstantZeroComparer<StructuralRow>.Instance);

    private static SortKeyComparer<StructuralRow> Comparer(IComparer<StructuralRow> tieBreak)
    {
        var keys = new Func<StructuralRow, object?>[Keys];
        for (var i = 0; i < Keys; i++)
        {
            var c = i;
            keys[i] = row => row[c];
        }

        return new SortKeyComparer<StructuralRow>(keys, new bool[Keys], Enumerable.Repeat(true, Keys).ToArray(), tieBreak);
    }

    public static int Run(string[] args)
    {
        var n = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 15_159_841;
        var runs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1;
        var arms = args.Length > 3 ? args[3].Split(',') : ["gather", "range"];
        var workers = args.Length > 4 ? int.Parse(args[4], CultureInfo.InvariantCulture) : 12;
        _ = ArrowSqlSnapshotCodecs.Instance; // loads Persistence: block formats (and spill, if on)
        Console.WriteLine(
            $"globalrank: rows={n:N0} runs={runs} W={workers} STEP_SPILL_ROWS={SpillSettings.StepSpillRows?.ToString(CultureInfo.InvariantCulture) ?? "off"} " +
            $"server GC={System.Runtime.GCSettings.IsServerGC}");

        var rng = new Random(42);
        var t0 = Stopwatch.GetTimestamp();
        var rows = Rows(n, rng);
        var ticks = new List<(object?[] Values, long Weight)>[]
        {
            [.. rows.Select(r => (r, 1L))],
            Updates(rows, rng),
            Front(rng),
        };
        Console.WriteLine($"  generated in {Stopwatch.GetElapsedTime(t0).TotalSeconds:F1} s");

        var results = new Dictionary<string, List<double[]>>();
        var checks = new Dictionary<string, (long, long, ulong)[]>();
        for (var r = 0; r < runs; r++)
        {
            foreach (var arm in arms)
            {
                Settle();
                var (ms, bytes, check) = arm switch
                {
                    "gather" or "range" => RunHandBuilt(arm, ticks, workers, rng: new Random(7)),
                    "sql" => RunSql(ticks, workers),
                    _ => throw new ArgumentException(arm),
                };
                (results.TryGetValue(arm, out var l) ? l : results[arm] = []).Add(ms);
                checks.TryAdd(arm, check);
                Console.WriteLine(
                    $"  run {r} {arm,-6} " + string.Join("  ", ms.Select((m, i) => $"tick {i + 1} {m,8:F0} ms {bytes[i] / (1 << 20),7:N0} MiB")));
            }
        }

        Console.WriteLine();
        foreach (var (arm, l) in results)
        {
            var med = Enumerable.Range(0, ticks.Length).Select(i => l.Select(x => x[i]).Order().ElementAt(l.Count / 2)).ToArray();
            Console.WriteLine($"median {arm,-6} " + string.Join("  ", med.Select((m, i) => $"tick {i + 1} {m,8:F0} ms")));
        }

        var first = checks.First();
        foreach (var (arm, c) in checks)
        {
            Console.WriteLine($"check  {arm,-6} " + string.Join("  ", c.Select((x, i) => $"tick {i + 1} rows={x.Item1:N0} net={x.Item2:N0} sum={x.Item3:X16}")));
            if (!c.SequenceEqual(first.Value))
            {
                Console.WriteLine($"  MISMATCH: {arm} differs from {first.Key}");
            }
        }

        return 0;
    }

    // ---- Data ------------------------------------------------------------------------------------

    private static List<object?[]> Rows(int n, Random rng)
    {
        var seen = new HashSet<long>(n);
        var rows = new List<object?[]>(n);
        while (rows.Count < n)
        {
            int m = rng.Next(Months.Length), h = rng.Next(Households), offer = rng.Next(3), usage = rng.Next(2), pref = rng.Next(4),
                tier = rng.Next(5), version = rng.Next(5), type = rng.Next(3), mode = rng.Next(4);
            var packed = (long)m | ((long)h << 4) | ((long)offer << 21) | ((long)usage << 23) | ((long)pref << 24) |
                ((long)tier << 26) | ((long)version << 29) | ((long)type << 32) | ((long)mode << 34);
            if (seen.Add(packed))
            {
                rows.Add(Row(Months[m], h + 2, offer + 1, usage + 1, 1, pref + 1, tier + 1, version + 1, type + 1, mode + 1,
                    1 + rng.Next(40), rng.Next(100_000) + 0.25));
            }
        }

        return rows;
    }

    private static object?[] Row(int date, int household, int offer, int usage, int platform, int pref, int tier, int version, int type, int mode, long count, double units) =>
        [date, household, offer, usage, platform, pref, tier, version, type, mode, count, units];

    // A thousand rows' values change, keys unchanged: no rank moves, no range's count changes.
    private static List<(object?[] Values, long Weight)> Updates(List<object?[]> rows, Random rng)
    {
        var picked = new HashSet<int>();
        while (picked.Count < Changes)
        {
            picked.Add(rng.Next(rows.Count));
        }

        var delta = new List<(object?[] Values, long Weight)>();
        foreach (var i in picked)
        {
            var old = rows[i];
            var now = (object?[])old.Clone();
            now[10] = (long)old[10]! + 1;
            delta.Add((old, -1));
            delta.Add((now, 1));
        }

        return delta;
    }

    // A thousand inserts before every existing row: every row's rank moves by a thousand.
    private static List<(object?[] Values, long Weight)> Front(Random rng) =>
        [.. Enumerable.Range(0, Changes).Select(i => (Row(20240101, i + 2, 1, 1, 1, 1, 1, 1, 1, 1, 1 + rng.Next(40), rng.Next(100_000) + 0.25), 1L))];

    // The worker the aggregate upstream would hold the row on: by its key.
    private static int ShardOf(object?[] values, int workers)
    {
        var h = new HashCode();
        for (var i = 0; i < Keys; i++)
        {
            h.Add((int)values[i]!);
        }

        return (int)((uint)h.ToHashCode() % (uint)workers);
    }

    private static ZSet<StructuralRow, Z64>[] Shards(List<(object?[] Values, long Weight)> delta, int workers)
    {
        var builders = Enumerable.Range(0, workers).Select(_ => new ZSetBuilder<StructuralRow, Z64>(delta.Count / workers + 16)).ToArray();
        foreach (var (values, weight) in delta)
        {
            // Fresh cells per row, as a decoded delta has.
            var cells = new object?[values.Length];
            for (var i = 0; i < values.Length; i++)
            {
                cells[i] = values[i] switch { int x => x, long x => x, double x => x, var x => x };
            }

            builders[ShardOf(values, workers)].Add(new StructuralRow(cells), new Z64(weight));
        }

        return [.. builders.Select(b => b.Build())];
    }

    // ---- Arms ------------------------------------------------------------------------------------

    private static StructuralRow Project(StructuralRow r)
    {
        var v = new object?[13];
        v[0] = checked((int)(long)r[12]!);
        for (var i = 0; i < Keys; i++)
        {
            v[i + 1] = r[i];
        }

        v[11] = checked((int)(long)r[10]!);
        v[12] = (int)Math.Round((double)r[11]!);
        return new StructuralRow(v);
    }

    // Splitters: W−1 quantiles of a sample of the rows, by the ORDER BY keys (the protocol's sample barrier,
    // not timed here). A row's range is the number of splitters at or below it.
    private static StructuralRow[] Splitters(List<(object?[] Values, long Weight)> rows, int workers, Random rng)
    {
        const int perWorker = 1_024;
        var sample = Enumerable.Range(0, perWorker * workers).Select(_ => new StructuralRow(rows[rng.Next(rows.Count)].Values)).ToList();
        sample.Sort(Order);
        return [.. Enumerable.Range(1, workers - 1).Select(i => sample[i * sample.Count / workers])];
    }

    private static int RangeOf(StructuralRow[] splitters, StructuralRow row)
    {
        int lo = 0, hi = splitters.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (SortKeyOnly.Compare(splitters[mid], row) <= 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    private static (double[] Ms, long[] Bytes, (long, long, ulong)[] Check) RunHandBuilt(
        string arm, List<(object?[] Values, long Weight)>[] ticks, int workers, Random rng)
    {
        var splitters = arm == "range" ? Splitters(ticks[0], workers, rng) : [];
        using var pc = ParallelCircuit.Build(workers, cb =>
        {
            var (_, input) = cb.Input<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty, (x, y) => x.Plus(y), name: "in");
            Stream<ZSet<StructuralRow, Z64>> ranked;
            if (arm == "gather")
            {
                var routed = cb.Exchange(input, static _ => 0);
                ranked = cb.PartitionedRank<int>(routed, static _ => 0, Order, SortKeyOnly, RankFunction.RowNumber);
            }
            else
            {
                var routed = cb.Exchange(input, row => RangeOf(splitters, row));
                var local = cb.PartitionedRank<int>(routed, static _ => 0, Order, SortKeyOnly, RankFunction.RowNumber);
                var (coordinator, worker, abort) = cb.Rendezvous<long>()!.Value;
                ranked = new Stream<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty);
                cb.AddRawOperator(new RangeOffsetOp(routed, local, ranked, coordinator, worker, abort));
            }

            var (view, _) = cb.Integrate(cb.MapRows(ranked, Project));
            cb.Output(view, name: "out");
        });
        pc.ProfileOperators = true;
        return Drive(pc, ticks, workers, (p, w) => p.WorkerOutput<ZSet<StructuralRow, Z64>>("out", w).Current);
    }

    private static (double[] Ms, long[] Bytes, (long, long, ulong)[] Check) Drive(
        ParallelCircuit pc, List<(object?[] Values, long Weight)>[] ticks, int workers, Func<ParallelCircuit, int, ZSet<StructuralRow, Z64>> output)
    {
        var ms = new double[ticks.Length];
        var bytes = new long[ticks.Length];
        var check = new (long, long, ulong)[ticks.Length];
        for (var t = 0; t < ticks.Length; t++)
        {
            var shards = Shards(ticks[t], workers);
            for (var w = 0; w < workers; w++)
            {
                pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", w).Push(shards[w]);
            }

            Settle();
            pc.ResetOperatorProfile();
            (ms[t], bytes[t]) = Measure(pc.Step);
            for (var w = 0; w < workers; w++)
            {
                check[t] = Add(check[t], Checksum(output(pc, w)));
            }

            PrintProfile(pc, t + 1);
        }

        return (ms, bytes, check);
    }

    private static void PrintProfile(ParallelCircuit pc, int tick)
    {
        var per = pc.CollectOperatorProfilePerWorker();
        for (var i = 0; i < per[0].Count; i++)
        {
            var ms = per.Select(w => w[i].CumulativeMs).ToArray();
            var alloc = per.Sum(w => w[i].AllocBytes);
            if (ms.Max() < 50 && alloc < 64L << 20)
            {
                continue;
            }

            Console.WriteLine(
                $"    tick {tick} [{i,2}] {per[0][i].Name,-28} max {ms.Max(),8:F0} ms  median {ms.Order().ElementAt(ms.Length / 2),8:F0} ms  " +
                $"sum {ms.Sum(),9:F0} ms  {alloc / (1 << 20),7:N0} MiB  out={per.Sum(w => Math.Max(0, w[i].LastOutputRows)):N0}");
        }
    }

    private static (double[] Ms, long[] Bytes, (long, long, ulong)[] Check) RunSql(List<(object?[] Values, long Weight)>[] ticks, int workers)
    {
        if (!SqlProgram.TryCompileParallel(Ddl, new HashSet<string>(["v"], StringComparer.Ordinal), workers, out var p, ArrowSqlSnapshotCodecs.Instance))
        {
            throw new InvalidOperationException("the view did not compile in parallel");
        }

        using (p)
        {
            p!.Circuit.ProfileOperators = true;
            var ms = new double[ticks.Length];
            var bytes = new long[ticks.Length];
            var check = new (long, long, ulong)[ticks.Length];
            for (var t = 0; t < ticks.Length; t++)
            {
                p.Table("agg").Push(ticks[t].Select(x => ((object?[])x.Values.Clone(), x.Weight)));
                Settle();
                p.Circuit.ResetOperatorProfile();
                (ms[t], bytes[t]) = Measure(p.Step);
                check[t] = Checksum(p.Outputs["v"].CurrentDelta);
                PrintProfile(p.Circuit, t + 1);
            }

            return (ms, bytes, check);
        }
    }

    private static (long, long, ulong) Checksum(ZSet<StructuralRow, Z64> z)
    {
        long rows = 0, net = 0;
        ulong sum = 0;
        foreach (var (row, w) in z)
        {
            rows++;
            net += w.Value;
            var h = (ulong)(uint)row.GetHashCode() * 0x9E3779B97F4A7C15UL;
            sum += unchecked((ulong)w.Value * (h ^ (h >> 29)));
        }

        return (rows, net, sum);
    }

    private static (long, long, ulong) Add((long, long, ulong) a, (long, long, ulong) b) =>
        (a.Item1 + b.Item1, a.Item2 + b.Item2, unchecked(a.Item3 + b.Item3));

    private static (double Ms, long Bytes) Measure(Action a)
    {
        var b0 = GC.GetTotalAllocatedBytes(precise: true);
        var t0 = Stopwatch.GetTimestamp();
        a();
        var ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
        return (ms, GC.GetTotalAllocatedBytes(precise: true) - b0);
    }

    private static void Settle()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
    }

    /// <summary>
    /// The prototype's offset step: every worker's routed delta is its range's count change, all-gathered;
    /// the local ranks shift by the rows of the ranges before this one. On a changed offset the whole range
    /// re-emits from an integrated copy of the local ranking (the real operator would use the rank's own
    /// stored output, so this overstates the range arm's cost).
    /// </summary>
    private sealed class RangeOffsetOp(
        Stream<ZSet<StructuralRow, Z64>> routed,
        Stream<ZSet<StructuralRow, Z64>> local,
        Stream<ZSet<StructuralRow, Z64>> output,
        ExchangeCoordinator<long> coordinator,
        int worker,
        CancellationToken abort) : IOperator
    {
        private readonly long[] _counts = new long[coordinator.Workers];
        private readonly Dictionary<StructuralRow, long> _integrated = [];

        public void Step()
        {
            long change = 0;
            foreach (var (_, w) in routed.Current)
            {
                change += w.Value;
            }

            for (var j = 0; j < coordinator.Workers; j++)
            {
                coordinator.Publish(worker, j, change);
            }

            coordinator.Wait(abort);
            long before = 0, after = 0;
            for (var src = 0; src < coordinator.Workers; src++)
            {
                if (src < worker)
                {
                    before += _counts[src];
                }

                _counts[src] += coordinator.Read(src, worker);
                if (src < worker)
                {
                    after += _counts[src];
                }
            }

            var delta = local.Current;
            ZSetBuilder<StructuralRow, Z64> b;
            if (before == after)
            {
                b = new ZSetBuilder<StructuralRow, Z64>(delta.Count);
                foreach (var (row, w) in delta)
                {
                    b.Add(Shift(row, after), w);
                }

                Integrate(delta);
            }
            else
            {
                b = new ZSetBuilder<StructuralRow, Z64>(2 * _integrated.Count + delta.Count);
                foreach (var (row, w) in _integrated)
                {
                    b.Add(Shift(row, before), new Z64(-w));
                }

                Integrate(delta);
                foreach (var (row, w) in _integrated)
                {
                    b.Add(Shift(row, after), new Z64(w));
                }
            }

            output.SetCurrent(b.Build());
        }

        private void Integrate(ZSet<StructuralRow, Z64> delta)
        {
            foreach (var (row, w) in delta)
            {
                var next = _integrated.GetValueOrDefault(row) + w.Value;
                if (next == 0)
                {
                    _integrated.Remove(row);
                }
                else
                {
                    _integrated[row] = next;
                }
            }
        }

        private static StructuralRow Shift(StructuralRow row, long offset)
        {
            if (offset == 0)
            {
                return row;
            }

            var n = row.Count;
            var v = new object?[n];
            for (var i = 0; i < n - 1; i++)
            {
                v[i] = row[i];
            }

            v[n - 1] = (long)row[n - 1]! + offset;
            return new StructuralRow(v);
        }
    }
}
