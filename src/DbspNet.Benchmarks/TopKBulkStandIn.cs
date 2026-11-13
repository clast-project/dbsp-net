// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks;

/// <summary>
/// The bulk-load TOP-K stand-in (docs/design-bounded-memory.md §5.52): the real SQL of
/// <c>fact_engagement_status_monthly_snapshot</c>'s dedup, <c>ROW_NUMBER() OVER (PARTITION BY engagement_path,
/// dim_date_key ORDER BY event_time DESC) = 1</c>, compiled at W workers, over rows shaped like issue #2's SF25
/// tick 18 (36.1M rows over 32.2M partitions, ~1.12 rows per partition; one worker holds ~3M rows in 12 slices).
/// Ticks: 1 the bulk load (every partition new); 2 a few thousand retractions of partitions' first rows, inserts
/// into existing partitions and new partitions; 3 a second bulk of new partitions onto the non-empty store.
/// Prints the TOP-K's phase split (TOPKPHASE) and an output checksum per tick, so arms can be compared.
/// <c>dotnet run -c Release -- topkbulk [rows] [runs] [workers] [arms]</c>; arms <c>base</c> (the fast path
/// off) and <c>bulk</c> (on).
/// </summary>
internal static class TopKBulkStandIn
{
    private const int Changes = 2_000;
    private const double RowsPerPartition = 36_091_821 / 32_222_480.0;

    private static readonly int[] Months =
        [20250101, 20250201, 20250301, 20250401, 20250501, 20250601, 20250701, 20250801, 20250901, 20251001, 20251101, 20251201, 20260101];

    private static readonly string[] Ddl =
    [
        "CREATE TABLE t (event_time TIMESTAMP, household_key INT, account_offer_key INT, account_usage_key INT, platform_key INT, " +
            "preference_key INT, product_tier_key INT, product_version_key INT, engagement_type_key INT, mode_key INT, " +
            "engagement_path VARCHAR, effective_units DOUBLE, dim_date_key INT)",
        "CREATE VIEW v AS SELECT event_time, household_key, account_offer_key, account_usage_key, platform_key, preference_key, " +
            "product_tier_key, product_version_key, engagement_type_key, mode_key, engagement_path, effective_units, dim_date_key " +
            "FROM (SELECT event_time, household_key, account_offer_key, account_usage_key, platform_key, preference_key, " +
            "product_tier_key, product_version_key, engagement_type_key, mode_key, engagement_path, effective_units, dim_date_key, " +
            "ROW_NUMBER() OVER (PARTITION BY engagement_path, dim_date_key ORDER BY event_time DESC) AS rn FROM t) AS latest WHERE rn = 1",
    ];

    public static int Run(string[] args)
    {
        var n = args.Length > 1 ? int.Parse(args[1], CultureInfo.InvariantCulture) : 12_000_000;
        var runs = args.Length > 2 ? int.Parse(args[2], CultureInfo.InvariantCulture) : 1;
        var workers = args.Length > 3 ? int.Parse(args[3], CultureInfo.InvariantCulture) : 4;
        var arms = args.Length > 4 ? args[4].Split(',') : ["base", "bulk"];
        _ = ArrowSqlSnapshotCodecs.Instance; // loads Persistence: block formats (and spill, if on)
        Console.WriteLine(
            $"topkbulk: rows={n:N0} runs={runs} W={workers} STEP_SPILL_ROWS={SpillSettings.StepSpillRows?.ToString(CultureInfo.InvariantCulture) ?? "off"} " +
            $"writeback={SpillSettings.WindowWriteBackBytes >> 20} MiB server GC={System.Runtime.GCSettings.IsServerGC}");

        var t0 = Stopwatch.GetTimestamp();
        var ticks = Ticks(n, new Random(42));
        Console.WriteLine($"  generated in {Stopwatch.GetElapsedTime(t0).TotalSeconds:F1} s: " +
            string.Join(", ", ticks.Select((t, i) => $"tick {i + 1} {t.Count:N0} rows")));

        var checks = new Dictionary<string, (long, long, ulong)[]>();
        var results = new Dictionary<string, List<double[]>>();
        for (var r = 0; r < runs; r++)
        {
            foreach (var arm in arms)
            {
                var name = arm;
                Settle();
                PartitionedBulkMode.Enabled = arm != "base";
                var (ms, bytes, check) = RunSql(ticks, workers);
                PartitionedBulkMode.Reset();
                (results.TryGetValue(name, out var l) ? l : results[name] = []).Add(ms);
                checks.TryAdd(name, check);
                Console.WriteLine(
                    $"  run {r} {name,-8} " + string.Join("  ", ms.Select((m, i) => $"tick {i + 1} {m,8:F0} ms {bytes[i] / (1 << 20),7:N0} MiB")));
            }
        }

        Console.WriteLine();
        foreach (var (arm, l) in results)
        {
            var med = Enumerable.Range(0, ticks.Length).Select(i => l.Select(x => x[i]).Order().ElementAt(l.Count / 2)).ToArray();
            Console.WriteLine($"median {arm,-8} " + string.Join("  ", med.Select((m, i) => $"tick {i + 1} {m,8:F0} ms")));
        }

        var first = checks.First();
        foreach (var (arm, c) in checks)
        {
            Console.WriteLine($"check  {arm,-8} " + string.Join("  ", c.Select((x, i) => $"tick {i + 1} rows={x.Item1:N0} net={x.Item2:N0} sum={x.Item3:X16}")));
            if (!c.SequenceEqual(first.Value))
            {
                Console.WriteLine($"  MISMATCH: {arm} differs from {first.Key}");
            }
        }

        return 0;
    }

    // ---- Data ------------------------------------------------------------------------------------

    private static object?[] Row(Random rng, int partition, long time) =>
    [
        new Timestamp(time),
        2 + rng.Next(72_000), 1 + rng.Next(3), 1 + rng.Next(2), 1, 1 + rng.Next(4), 1 + rng.Next(5), 1 + rng.Next(5), 1 + rng.Next(3), 1 + rng.Next(4),
        Path(partition / Months.Length), rng.Next(100_000) + 0.25, Months[partition % Months.Length],
    ];

    private static string Path(int p) => string.Create(CultureInfo.InvariantCulture, $"acct-{p:D8}/offer/{p % 97:D2}");

    private static List<(object?[] Row, long Weight)>[] Ticks(int n, Random rng)
    {
        var partitions = (int)(n / RowsPerPartition);
        var time = 1_700_000_000_000_000L;
        var bulk = new List<(object?[] Row, long Weight)>(n);
        for (var i = 0; i < n; i++)
        {
            bulk.Add((Row(rng, i < partitions ? i : rng.Next(partitions), time + rng.NextInt64(1L << 40)), 1));
        }

        // Tick 2: retract each of some partitions' first row (its latest event_time), so the next one is promoted
        // (or the partition empties); insert a newer row into others; and open new partitions.
        var first = new Dictionary<int, (object?[] Row, long Time)>();
        foreach (var (row, _) in bulk)
        {
            var p = PartitionOf(row);
            var t = ((Timestamp)row[0]!).Microseconds;
            if (!first.TryGetValue(p, out var f) || t > f.Time)
            {
                first[p] = (row, t);
            }
        }

        var changes = new List<(object?[] Row, long Weight)>();
        foreach (var p in Enumerable.Range(0, Changes).Select(_ => rng.Next(partitions)).Distinct())
        {
            changes.Add((first[p].Row, -1));
        }

        for (var i = 0; i < Changes; i++)
        {
            changes.Add((Row(rng, rng.Next(partitions), time + (1L << 41) + i), 1));
            changes.Add((Row(rng, partitions + i, time + rng.NextInt64(1L << 40)), 1));
        }

        // Tick 3: a second bulk, new partitions only.
        var more = new List<(object?[] Row, long Weight)>(n / 4);
        var next = partitions + Changes;
        var morePartitions = (int)(n / 4 / RowsPerPartition);
        for (var i = 0; i < n / 4; i++)
        {
            more.Add((Row(rng, next + (i < morePartitions ? i : rng.Next(morePartitions)), time + rng.NextInt64(1L << 40)), 1));
        }

        return [bulk, changes, more];
    }

    private static int PartitionOf(object?[] row)
    {
        var path = (string)row[10]!;
        var p = int.Parse(path.AsSpan(5, 8), CultureInfo.InvariantCulture);
        return (p * Months.Length) + Array.IndexOf(Months, (int)row[12]!);
    }

    // ---- The arm -----------------------------------------------------------------------------------

    private static (double[] Ms, long[] Bytes, (long, long, ulong)[] Check) RunSql(List<(object?[] Row, long Weight)>[] ticks, int workers)
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
                p.Table("t").Push(ticks[t].Select(x => ((object?[])x.Row.Clone(), x.Weight)));
                Settle();
                p.Circuit.ResetOperatorProfile();
                ResetPhases(p.Circuit);
                (ms[t], bytes[t]) = Measure(p.Step);
                check[t] = Checksum(p.Outputs["v"].CurrentDelta);
                PrintProfile(p.Circuit, t + 1);
            }

            return (ms, bytes, check);
        }
    }

    private static IEnumerable<long[]> Phases(Core.Circuit.ParallelCircuit pc) =>
        pc.Replicas.SelectMany(r => r.Operators).OfType<PartitionedTopKCompactOp<StructuralRow, StructuralRow>>().Select(op => op.Phase);

    private static void ResetPhases(Core.Circuit.ParallelCircuit pc)
    {
        foreach (var ph in Phases(pc))
        {
            Array.Clear(ph);
        }
    }

    private static void PrintProfile(Core.Circuit.ParallelCircuit pc, int tick)
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

        var sum = new long[22];
        foreach (var ph in Phases(pc))
        {
            for (var q = 0; q < ph.Length && q < sum.Length; q++)
            {
                sum[q] += ph[q];
            }
        }

        static long Ms(long ticks) => ticks * 1000 / Stopwatch.Frequency;
        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"    TOPKPHASE tick={tick} slicing_mib={sum[0] >> 20} old_mib={sum[1] >> 20} integrate_mib={sum[2] >> 20} new_mib={sum[3] >> 20} emit_mib={sum[4] >> 20} endtick_mib={sum[5] >> 20} " +
            $"slicing_ms={Ms(sum[10])} old_ms={Ms(sum[11])} integrate_ms={Ms(sum[12])} new_ms={Ms(sum[13])} emit_ms={Ms(sum[14])} endtick_ms={Ms(sum[15])} " +
            $"delta_rows={sum[6]} slices={sum[7]} touched={sum[8]} rows_read={sum[9]} probes={sum[16]} probes_empty={sum[17]} flushes={sum[18]} flushed={sum[19]} bulk_parts={sum[20]} bulk_rows={sum[21]}"));
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
}
