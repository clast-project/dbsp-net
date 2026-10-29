// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks;

/// <summary>
/// Adversarial check for the compact MIN/MAX state (state-representation arc,
/// variant B): a source-maintained sliding window — each tick inserts N new rows
/// per group and deletes that group's N oldest — so the current MIN is retracted
/// every tick. The rescan-only ("compact") state rescans the group on that event;
/// the legacy state pays O(log n) per row; the hybrid rescans once per group and
/// then pays O(log n) in its sorted run (§4B.2). The <c>append</c> shape inserts
/// only, so the extreme is never retracted. A/Bs all three arms in one process
/// and reports the live heap per arm (the trace is shared, so differences are
/// the MIN/MAX state).
/// </summary>
internal static class MinMaxSlideBenchmark
{
    private const string Ddl = "CREATE TABLE t (g BIGINT NOT NULL, v BIGINT NOT NULL)";
    private const string Sql = "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g";

    /// <param name="appendOnly">Inserts only (no deletes): the extreme is never retracted.</param>
    /// <param name="structural">A/B the structural path's arms (§4B.3) instead of the typed path's.</param>
    public static void Run(int groups, int window, int perTick, int ticks, int reps, bool appendOnly = false,
        bool structural = false)
    {
        Console.WriteLine(
            $"=== MIN/MAX {(appendOnly ? "append-only" : "sliding window")}{(structural ? " (structural)" : "")}: " +
            $"groups={groups} window={window} perTick={perTick} ticks={ticks} ===");
        var kinds = structural
            ? new[] { MinMaxStateKind.Legacy, MinMaxStateKind.Hybrid }
            : new[] { MinMaxStateKind.Legacy, MinMaxStateKind.RescanOnly, MinMaxStateKind.Hybrid };
        for (var rep = 0; rep < reps; rep++)
        {
            foreach (var kind in kinds)
            {
                var (ms, bytes, live) = Measure(kind, groups, window, perTick, ticks, appendOnly, structural);
                var label = kind switch
                {
                    MinMaxStateKind.Legacy => "legacy",
                    MinMaxStateKind.RescanOnly => "compact",
                    _ => "hybrid",
                };
                Console.WriteLine(
                    $"  rep {rep} {label,-8} {ms,9:F1} ms  " +
                    $"{ms * 1000 / ticks,9:F1} us/tick  {bytes / (double)ticks / groups / perTick,8:F0} B/row-change  live {live / 1e6,8:F1} MB");
            }
        }
    }

    private static (double Ms, long Bytes, long Live) Measure(
        MinMaxStateKind kind, int groups, int window, int perTick, int ticks, bool appendOnly, bool structural)
    {
        var prev = CompactAggregatorStateMode.MinMax;
        var prevStructural = StructuralCompactAggregatorStateMode.MinMax;
        CompactAggregatorStateMode.MinMax = kind;
        StructuralCompactAggregatorStateMode.MinMax =
            kind == MinMaxStateKind.Legacy ? StructuralMinMaxStateKind.Legacy : StructuralMinMaxStateKind.Hybrid;
        CompiledQuery q;
        try
        {
            var resolver = new Resolver(new Catalog());
            resolver.Resolve(Parser.ParseStatement(Ddl));
            var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Sql))).Query;
            // ShareArrangements is implemented on the structural path only, so it forces
            // that compile; this query has no join, so it changes nothing else.
            q = PlanToCircuit.Compile(PlanOptimizer.Optimize(plan),
                options: structural ? new CompileOptions { ShareArrangements = true } : null);
        }
        finally
        {
            CompactAggregatorStateMode.MinMax = prev;
            StructuralCompactAggregatorStateMode.MinMax = prevStructural;
        }

        var batch = new List<(object?[], long)>(groups * window);
        for (long g = 0; g < groups; g++)
        {
            for (long v = 0; v < window; v++)
            {
                batch.Add((new object?[] { g, v }, 1L));
            }
        }

        q.Table("t").Push(batch);
        q.Step();

        long next = window;
        long oldest = 0;
        var input = q.Table("t");
        GC.Collect();
        var a0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (var tick = 0; tick < ticks; tick++)
        {
            batch.Clear();
            for (long g = 0; g < groups; g++)
            {
                for (var i = 0; i < perTick; i++)
                {
                    batch.Add((new object?[] { g, next + i }, 1L));
                    if (!appendOnly) batch.Add((new object?[] { g, oldest + i }, -1L));
                }
            }

            next += perTick;
            oldest += perTick;
            input.Push(batch);
            q.Step();
        }

        sw.Stop();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - a0;

        // Live heap with the query (trace + per-group MIN/MAX state) still reachable;
        // the trace is the same in every arm, so arm differences are the state.
        batch.Clear();
        batch.TrimExcess();
        // Compact first: GC.GetTotalMemory counts fragmentation, which the hybrid's
        // resized chunk arrays leave behind and which roughly doubles its reading.
        System.Runtime.GCSettings.LargeObjectHeapCompactionMode =
            System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        var info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
        var live = info.HeapSizeBytes - info.FragmentedBytes;
        GC.KeepAlive(q);
        return (sw.Elapsed.TotalMilliseconds, allocated, live);
    }
}
