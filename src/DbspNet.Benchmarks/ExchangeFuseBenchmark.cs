// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Core.Circuit;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Gate for the §15 lever — fusing a join's two input exchanges into one shared
/// barrier (<c>CompileOptions.CoalesceJoinExchange</c> →
/// <c>ExchangeIndexJoinOp</c>). For each join-heavy Nexmark query it A/Bs the
/// whole parallel pipeline unfused vs fused: a median-of-N <b>step</b> time (the
/// robust throughput number) plus one profiled pass per side to show the
/// mechanism — the exchange <b>Wait%</b> the fusion is meant to halve — and
/// cross-checks that the fused output is byte-identical to the unfused one.
/// </summary>
internal static class ExchangeFuseBenchmark
{
    public static void Run(StringBuilder output, string[] queryIds, int totalEvents, int workers, int runs)
    {
        Console.WriteLine();
        Console.WriteLine($"=== exchange-fuse gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        output.AppendLine("# DbspNet — join exchange-barrier fusion (§15 gate)");
        output.AppendLine();
        output.AppendLine(
            "A join shuffles both inputs by the join key through two independent " +
            "`ExchangeIndex` rendezvous; §15 found that barrier wait is the dominant " +
            "scaling cost. `CoalesceJoinExchange` fuses them into one `ExchangeIndexJoin` " +
            "(publish both sides, rendezvous once). Per query: **median-of-" +
            $"{runs} step** unfused vs fused (cross-checked identical output), and the " +
            "single-pass exchange **Wait%** each way (the term the fusion targets).");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, batch 10k, W={workers}. Host: .NET " +
            $"{Environment.Version}, {Environment.ProcessorCount} logical cores.");
        output.AppendLine();
        output.AppendLine("| Query | Unfused step (ms) | Fused step (ms) | Step↑ | Unfused Wait% | Fused Wait% | Output |");
        output.AppendLine("|---|---:|---:|---:|---:|---:|---|");

        foreach (var id in queryIds)
        {
            var query = Array.Find(NexmarkQueries.All, q => q.Id == id);
            if (query is null)
            {
                Console.WriteLine($"  (skipping unknown query {id})");
                continue;
            }

            Console.WriteLine($"  {id}…");
            var consumed = query.Tables.ToHashSet();
            var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);

            // Robust median step A/B + output cross-check.
            var unfused = SpineParallelHarness.MeasureMedian(
                plan, workers, SpineParallelHarness.Flat, events, consumed, batchSize: 10_000, runs);
            var fused = SpineParallelHarness.MeasureMedian(
                plan, workers, SpineParallelHarness.FlatCoalesced, events, consumed, batchSize: 10_000, runs);

            var same = SpineParallelHarness.SameMultiset(
                SpineParallelHarness.Materialize(unfused.Result),
                SpineParallelHarness.Materialize(fused.Result));

            // One profiled pass each for the Wait% mechanism.
            var unfusedWait = WaitPct(SpineParallelHarness.MeasureProfiled(
                plan, workers, SpineParallelHarness.Flat, events, consumed, batchSize: 10_000), workers);
            var fusedWait = WaitPct(SpineParallelHarness.MeasureProfiled(
                plan, workers, SpineParallelHarness.FlatCoalesced, events, consumed, batchSize: 10_000), workers);

            var stepUp = fused.StepMs > 0 ? unfused.StepMs / fused.StepMs : 1.0;
            var ok = same ? "ok" : "**MISMATCH**";
            output.AppendLine(
                $"| {id} | {unfused.StepMs:F1} | {fused.StepMs:F1} | {stepUp:F2}× | " +
                $"{unfusedWait:F0}% | {fusedWait:F0}% | {ok} |");
            Console.WriteLine(
                $"    unfused={unfused.StepMs,7:F1}ms fused={fused.StepMs,7:F1}ms  {stepUp:F2}×  " +
                $"wait {unfusedWait:F0}%→{fusedWait:F0}%  [{ok}]");
        }

        output.AppendLine();
        output.AppendLine(
            "**Reading it.** Step↑ > 1 means fusing the two barriers into one sped the " +
            "step up; the Wait% columns show the mechanism — the fused form should idle " +
            "less at the exchange. Output must be `ok` (the fusion is a pure coordination " +
            "change, identical math). **Note: each run is a single W.** The headline " +
            "result is W-dependent and the lever is **HELD, off by default** — see " +
            "design-row-representation.md §15.7: fusing helps only in the oversubscribed " +
            "W=host regime (high wait) and *regresses* at W≈P-core count (the sensible " +
            "operating point), because the ceiling is the straggler bound, not barrier count.");
        output.AppendLine();
    }

    /// <summary>Mean exchange Wait% of the step across workers from the last profiled pass.</summary>
    private static double WaitPct(double ctrlMs, int workers)
    {
        _ = ctrlMs;
        var steps = Math.Max(1, StepProfiler.Steps);
        double sumStep = 0, sumWait = 0;
        for (var w = 0; w < workers; w++)
        {
            sumStep += StepProfiler.StepTicksOf(w);
            sumWait += StepProfiler.WaitTicksOf(w);
        }

        return sumStep > 0 ? 100.0 * sumWait / sumStep : 0;
    }
}
