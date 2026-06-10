// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// End-to-end query-level A/B for the flat aggregate lazy merge-view
/// (docs/design-row-representation.md §14.10): on Nexmark q4's whole
/// <c>W</c>-replica parallel pipeline, how much does the lazy view beat the eager
/// per-tick rebuild once the win is diluted by the join / exchange / outer-AVG
/// around it?
/// </summary>
/// <remarks>
/// The <c>flatagg</c> gate measured the aggregate step in isolation (4.6–19×);
/// this measures the realistic, Amdahl-diluted query-level effect. Both configs
/// are <see cref="TraceFamily.Flat"/> (today's default) — the only difference is
/// <see cref="DbspNet.Core.Operators.Stateful.FlatAggregateMode.ForceEagerRebuild"/>,
/// driven through the shared <see cref="SpineParallelHarness"/>. q4 runs the
/// <b>typed</b> aggregator (<c>TypedSqlMinMaxAggregator</c>), which is incremental
/// (probes only the delta rows), so the lazy view removes the rebuild that was the
/// aggregate's only O(K)-per-tick term — the asymptotic O(K²)→O(K) regime, not just
/// the constant-factor one the structural <c>flatagg</c> gate showed. The
/// <b>step</b> phase is timed apart from split/gather (the change only moves step
/// work); the lazy output is cross-checked identical to eager.
/// </remarks>
internal static class Q4FlatBenchmark
{
    public static void Run(StringBuilder output, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? Environment.ProcessorCount, 1, Environment.ProcessorCount);
        var query = Array.Find(NexmarkQueries.All, q => q.Id == "q4")
            ?? throw new InvalidOperationException("q4 not found in NexmarkQueries.All");
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine();
        Console.WriteLine($"=== q4 flat lazy-view gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);
        var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);

        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            throw new InvalidOperationException("q4 has no parallel plan — cannot run the flat lazy-view gate");
        }

        probe!.Dispose();

        output.AppendLine("# DbspNet — q4 flat aggregate lazy merge-view gate");
        output.AppendLine();
        output.AppendLine(
            "Query-level A/B of the flat aggregate lazy merge-view " +
            "(`docs/design-row-representation.md` §14.10) on Nexmark q4 — *average " +
            "closing price by category* (join → per-group `MAX` → outer `AVG`). Both " +
            "configs are the `TraceFamily.Flat` default; the only difference is the " +
            "aggregate's post-delta group representation:");
        output.AppendLine();
        output.AppendLine(
            "- **flat·eager** — the old `afterGroup = beforeGroup + groupDelta` rebuild " +
            "(re-hashes the whole per-auction group every tick).");
        output.AppendLine(
            "- **flat·lazy** — the `LazyMergeMultiset` view; q4's typed incremental " +
            "`MAX` probes only the delta, so the view removes the aggregate's only " +
            "O(K)-per-tick term (asymptotic O(K²)→O(K)).");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, W={workers}, median **step** throughput of " +
            $"{runs} run(s) after one warmup, timed apart from split/gather. Host: " +
            $".NET {Environment.Version}, {Environment.ProcessorCount} cores. The lazy " +
            "output is cross-checked identical to eager.");
        output.AppendLine();

        foreach (var batchSize in new[] { 10_000, 100_000 })
        {
            RunBatch(output, plan, events, consumed, batchSize, workers, runs, totalEvents);
        }

        output.AppendLine(
            "**Reading it.** *Step↑* is flat·eager / flat·lazy on the step phase. The " +
            "operator-level `flatagg` gate isolated 4.6–19×; here that win is diluted by " +
            "the join, exchange, and outer AVG that the lazy view does not touch, so the " +
            "query-level number is the realistic gain. The win grows with batch size " +
            "(wider per-replica ticks → larger per-auction groups rebuilt per tick under " +
            "eager).");
        output.AppendLine();
    }

    private static void RunBatch(
        StringBuilder output, DbspNet.Sql.Plan.LogicalPlan plan, List<Event> events,
        HashSet<NexmarkTable> consumed, int batchSize, int workers, int runs, int totalEvents)
    {
        Console.WriteLine();
        Console.WriteLine($"  batch={batchSize:N0}");

        output.AppendLine($"## Batch = {batchSize:N0} events");
        output.AppendLine();
        output.AppendLine("| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |");
        output.AppendLine("|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|");

        // Eager first; its output is the reference the lazy config must reproduce.
        var (eSplit, eStep, eGather, eRows, eResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.FlatEager, events, consumed, batchSize, runs);
        var reference = SpineParallelHarness.Materialize(eResult);
        EmitRow(output, "flat·eager", eSplit, eStep, eGather, eRows, totalEvents, baseline: eStep);

        var (lSplit, lStep, lGather, lRows, lResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        if (!SpineParallelHarness.SameMultiset(reference, SpineParallelHarness.Materialize(lResult)))
        {
            throw new InvalidOperationException(
                $"flat·lazy output diverged from flat·eager at batch={batchSize} — aborting gate");
        }

        EmitRow(output, "flat·lazy", lSplit, lStep, lGather, lRows, totalEvents, baseline: eStep);

        output.AppendLine();
    }

    private static void EmitRow(
        StringBuilder output, string label, double splitMs, double stepMs, double gatherMs,
        long outputRows, int totalEvents, double baseline)
    {
        var stepEventsPerSec = stepMs > 0 ? totalEvents / (stepMs / 1000.0) : 0.0;
        var stepUp = stepMs > 0 ? baseline / stepMs : 1.0;
        Console.WriteLine(
            $"    {label,-11} split={splitMs,8:F1}  step={stepMs,8:F1}ms  " +
            $"({stepEventsPerSec,12:N0} ev/s)  {BenchmarkHarness.FormatRatio(stepUp).Trim()}");
        output.AppendLine(
            $"| {label} | {splitMs:F1} | {stepMs:F1} | {stepEventsPerSec:N0} | " +
            $"{BenchmarkHarness.FormatRatio(stepUp).Trim()} | {gatherMs:F1} | {outputRows:N0} |");
        _ = CultureInfo.InvariantCulture;
    }
}
