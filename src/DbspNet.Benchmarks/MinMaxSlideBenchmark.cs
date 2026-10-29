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
/// every tick. The compact state rescans the group on that event; the legacy
/// state pays O(log n) per row. A/Bs both in one process.
/// </summary>
internal static class MinMaxSlideBenchmark
{
    private const string Ddl = "CREATE TABLE t (g BIGINT NOT NULL, v BIGINT NOT NULL)";
    private const string Sql = "SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g";

    public static void Run(int groups, int window, int perTick, int ticks, int reps)
    {
        Console.WriteLine(
            $"=== MIN/MAX sliding window: groups={groups} window={window} perTick={perTick} ticks={ticks} ===");
        for (var rep = 0; rep < reps; rep++)
        {
            foreach (var compact in new[] { false, true })
            {
                var (ms, bytes) = Measure(compact, groups, window, perTick, ticks);
                Console.WriteLine(
                    $"  rep {rep} {(compact ? "compact" : "legacy "),-8} {ms,9:F1} ms  " +
                    $"{ms * 1000 / ticks,9:F1} us/tick  {bytes / (double)ticks / groups / perTick,8:F0} B/row-change");
            }
        }
    }

    private static (double Ms, long Bytes) Measure(bool compact, int groups, int window, int perTick, int ticks)
    {
        var prev = CompactAggregatorStateMode.CompactMinMax;
        CompactAggregatorStateMode.CompactMinMax = compact;
        CompiledQuery q;
        try
        {
            var resolver = new Resolver(new Catalog());
            resolver.Resolve(Parser.ParseStatement(Ddl));
            var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Sql))).Query;
            q = PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
        }
        finally
        {
            CompactAggregatorStateMode.CompactMinMax = prev;
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
                    batch.Add((new object?[] { g, oldest + i }, -1L));
                }
            }

            next += perTick;
            oldest += perTick;
            input.Push(batch);
            q.Step();
        }

        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds, GC.GetAllocatedBytesForCurrentThread() - a0);
    }
}
