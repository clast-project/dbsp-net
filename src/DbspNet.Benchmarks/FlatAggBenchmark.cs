// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks;

/// <summary>
/// End-to-end A/B gate for the flat aggregate lazy merge-view
/// (docs/design-row-representation.md §14.9 decision): does replacing the eager
/// per-tick <c>afterGroup = beforeGroup + groupDelta</c> rebuild with the lazy
/// <see cref="LazyMergeMultiset{T}"/> close the q4 aggregate cost?
/// </summary>
/// <remarks>
/// <para>Drives the q4 shape: <c>MAX(price)</c> over growing per-auction groups.
/// <c>MAX</c> matters two ways — it is incremental (probes only the delta rows,
/// so the lazy view serves it in O(|delta|) instead of an O(K) rebuild) and it
/// blocks <c>NarrowAggregateInput</c>, so the inner value rows stay <b>wide</b>
/// (the full bid row), making the avoided rebuild a wide-row re-hash.</para>
/// <para>Workload: <c>G</c> auctions, each growing by one new distinct bid per
/// tick over <c>K</c> ticks. The eager path re-hashes every group in full each
/// tick → O(K²) per group; the lazy path probes only the new row → O(K). The
/// seam <see cref="FlatAggregateMode.ForceEagerRebuild"/> drives both in one
/// process; the accumulated output is cross-checked identical before timing is
/// reported.</para>
/// </remarks>
internal static class FlatAggBenchmark
{
    private const int Groups = 100;
    private static readonly int[] FinalSizes = { 128, 512, 2_048 };
    private const int Warmups = 1;
    private const int Runs = 3;

    private const string Sql =
        "SELECT auction, MAX(price) AS hi, COUNT(*) AS n FROM bids GROUP BY auction";

    private static readonly string[] Ddl =
    {
        "CREATE TABLE bids (auction INT NOT NULL, price BIGINT NOT NULL, " +
        "bidder BIGINT NOT NULL, url VARCHAR NOT NULL)",
    };

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Flat aggregate lazy merge-view A/B (q4 growing-group shape) ===");

        output.AppendLine("## Flat aggregate — eager rebuild vs lazy merge-view");
        output.AppendLine();
        output.AppendLine(
            "End-to-end A/B for docs/design-row-representation.md §14.9. The flat " +
            "`IncrementalAggregateOp` is driven through compiled SQL " +
            $"(`{Sql}`) over {Groups} auctions, each growing by one new wide bid row " +
            "per tick across K ticks. `MAX(price)` keeps the inner value rows wide " +
            "(blocks `NarrowAggregateInput`) and is incremental (probes only the " +
            "delta). **eager** forces today's `beforeGroup + groupDelta` rebuild " +
            "(re-hashes the whole group every tick, O(K²) per group); **lazy** is " +
            "the new `LazyMergeMultiset` view (probes the delta, O(K) per group). " +
            "Times are median ms for the whole K-tick step loop (compile excluded); " +
            "**Speedup** = eager/lazy (>1 = lazy wins). Outputs are verified " +
            "identical.");
        output.AppendLine();
        output.AppendLine("| K (final group size) | rows/tick | eager | lazy | Speedup |");
        output.AppendLine("|---------------------:|----------:|------:|-----:|--------:|");

        foreach (var k in FinalSizes)
        {
            var ticks = BuildTicks(Groups, k);

            var (eagerMs, eagerOut) = Measure(ticks, eager: true);
            var (lazyMs, lazyOut) = Measure(ticks, eager: false);

            if (!eagerOut.Equals(lazyOut))
            {
                throw new InvalidOperationException(
                    $"K={k}: lazy output disagrees with eager — the lazy view is not equivalent.");
            }

            var speedup = lazyMs > 0 ? eagerMs / lazyMs : 0.0;
            Console.WriteLine(
                $"    K={k,-5} rows/tick={Groups,-4} eager={FmtMs(eagerMs)} lazy={FmtMs(lazyMs)} " +
                $"{BenchmarkHarness.FormatRatio(speedup).Trim()}");
            output.AppendLine(
                $"| {k,20} | {Groups,9} | {FmtMs(eagerMs)} | {FmtMs(lazyMs)} | {BenchmarkHarness.FormatRatio(speedup).Trim()} |");
        }

        output.AppendLine();
    }

    // K ticks; tick t adds one new distinct bid to each of `groups` auctions.
    private static List<List<(object?[] Values, long Weight)>> BuildTicks(int groups, int k)
    {
        var ticks = new List<List<(object?[], long)>>(k);
        for (var t = 0; t < k; t++)
        {
            var tick = new List<(object?[], long)>(groups);
            for (var g = 0; g < groups; g++)
            {
                // Distinct wide rows: price/bidder/url all vary with (g, t).
                long price = (long)t * 7919 + g;
                long bidder = (long)g * 104729 + t;
                var url = "https://nexmark.example/auction/" + g + "/bid/" + t;
                tick.Add((new object?[] { g, price, bidder, url }, 1L));
            }

            ticks.Add(tick);
        }

        return ticks;
    }

    private static (double Ms, ZSet<StructuralRow, Z64> Output) Measure(
        List<List<(object?[] Values, long Weight)>> ticks,
        bool eager)
    {
        FlatAggregateMode.ForceEagerRebuild = eager;
        try
        {
            // Timed: the push/step loop only. Output is NOT accumulated here —
            // `acc += delta` is itself O(K²) (the running output Z-set is rebuilt
            // each tick) and would contaminate the operator measurement (worse for
            // the cheaper lazy path). The cross-check accumulation runs untimed
            // below.
            var times = new List<double>(Runs);
            var sw = new Stopwatch();

            for (var run = 0; run < Warmups + Runs; run++)
            {
                var q = Compile();

                sw.Restart();
                foreach (var tick in ticks)
                {
                    q.Table("bids").Push(tick);
                    q.Step();
                }

                sw.Stop();

                if (run >= Warmups)
                {
                    times.Add(sw.Elapsed.TotalMilliseconds);
                }
            }

            // Untimed pass: accumulate the per-tick output deltas for the
            // eager-vs-lazy cross-check.
            var qv = Compile();
            var acc = ZSet<StructuralRow, Z64>.Empty;
            foreach (var tick in ticks)
            {
                qv.Table("bids").Push(tick);
                qv.Step();
                acc += qv.Current;
            }

            times.Sort();
            return (times[times.Count / 2], acc);
        }
        finally
        {
            FlatAggregateMode.ForceEagerRebuild = false;
        }
    }

    private static CompiledQuery Compile()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Sql))).Query;
        return PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
    }

    private static string FmtMs(double ms) =>
        ms switch
        {
            < 1.0 => $"{ms * 1000.0,8:F0} µs",
            < 1_000.0 => $"{ms,8:F2} ms",
            _ => $"{ms / 1_000.0,8:F2} s",
        };
}
