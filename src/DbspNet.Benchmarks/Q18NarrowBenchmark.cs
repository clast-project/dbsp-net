// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// W&gt;1 in-<c>Step</c> gate for the §22 narrow-key partitioned TOP-K
/// (<see cref="DbspNet.Core.Operators.Stateful.PartitionedTopKNarrowingMode"/>): on a
/// Nexmark TOP-K query's whole <c>W</c>-replica parallel pipeline, does keying the
/// per-partition <c>_accum</c> / <c>_window</c> by a narrow <c>{order, wide row}</c>
/// key (hash on the order value alone, whole-row compare only within an equal-order
/// tie group) instead of the full bid row move the step throughput? The
/// <c>reprbench topk</c> microbench priced the in-state prize at 50 % (q18) / 82 %
/// (q19); this checks the §16.11 claim that an in-<c>Step</c> per-row win translates
/// to W&gt;1 (the TOP-K op runs inside <c>Step</c>), against the §16.10 caveat that
/// it is Amdahl-diluted (q18 is 35 % op + 43 % exchange at W=10).
/// </summary>
/// <remarks>
/// Both configs are the <see cref="SpineParallelHarness.Flat"/> default; the only
/// difference is <see cref="SpineParallelHarness.FlatNarrowTopK"/>. The narrow path is
/// an incremental operator rewrite under retraction and ties (default-off, gated by
/// the equivalence PBT), so the narrow output is cross-checked identical to the
/// whole-row output every batch — re-proving correctness on the parallel typed path.
/// </remarks>
internal static class Q18NarrowBenchmark
{
    public static void Run(StringBuilder output, string queryId, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? 8, 1, Environment.ProcessorCount);
        var query = Array.Find(NexmarkQueries.All, q => q.Id == queryId)
            ?? throw new InvalidOperationException($"{queryId} not found in NexmarkQueries.All");
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine();
        Console.WriteLine($"=== {queryId} narrow TOP-K gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);
        var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);

        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            throw new InvalidOperationException($"{queryId} has no parallel plan — cannot run the narrow gate");
        }

        probe!.Dispose();

        output.AppendLine($"# DbspNet — {queryId} narrow-key partitioned TOP-K gate (§22)");
        output.AppendLine();
        output.AppendLine(
            "W>1 in-`Step` A/B of keying the partitioned TOP-K trace by a narrow " +
            "`{order, wide row}` key (`PartitionedTopKNarrowingMode`) vs whole-row keying. " +
            "Both configs are the `TraceFamily.Flat` default. Narrow output is cross-checked " +
            "identical to whole-row output.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, W={workers}, median **step** throughput of " +
            $"{runs} run(s) after one warmup, timed apart from split/gather. Host: " +
            $".NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();

        foreach (var batchSize in new[] { 10_000, 100_000 })
        {
            RunBatch(output, queryId, plan, events, consumed, batchSize, workers, runs, totalEvents);
        }

        output.AppendLine(
            "**Reading it.** *Step↑* is whole-row / narrow on the step phase. The narrow key " +
            "is in-`Step` (it shrinks what the TOP-K op hashes/stores), so per §16.11 a W=1 " +
            "win is expected to translate to W>1 — but per §16.10 it is Amdahl-diluted by the " +
            "exchange/coordination share (larger for q18 than q19).");
        output.AppendLine();
    }

    private static void RunBatch(
        StringBuilder output, string queryId, LogicalPlan plan, List<Event> events,
        HashSet<NexmarkTable> consumed, int batchSize, int workers, int runs, int totalEvents)
    {
        Console.WriteLine();
        Console.WriteLine($"  batch={batchSize:N0}");

        output.AppendLine($"## Batch = {batchSize:N0} events");
        output.AppendLine();
        output.AppendLine("| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |");
        output.AppendLine("|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|");

        // Whole-row first; its output is the reference the narrow config must reproduce.
        var (wSplit, wStep, wGather, wRows, wResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        var reference = SpineParallelHarness.Materialize(wResult);
        EmitRow(output, "flat·wholerow", wSplit, wStep, wGather, wRows, totalEvents, baseline: wStep);

        var (nSplit, nStep, nGather, nRows, nResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.FlatNarrowTopK, events, consumed, batchSize, runs);
        if (!SpineParallelHarness.SameMultiset(reference, SpineParallelHarness.Materialize(nResult)))
        {
            throw new InvalidOperationException(
                $"{queryId} flat·narrow output diverged from flat·wholerow at batch={batchSize} — aborting gate");
        }

        EmitRow(output, "flat·narrow", nSplit, nStep, nGather, nRows, totalEvents, baseline: wStep);

        output.AppendLine();
    }

    private static void EmitRow(
        StringBuilder output, string label, double splitMs, double stepMs, double gatherMs,
        long outputRows, int totalEvents, double baseline)
    {
        var stepEventsPerSec = stepMs > 0 ? totalEvents / (stepMs / 1000.0) : 0.0;
        var stepUp = stepMs > 0 ? baseline / stepMs : 1.0;
        Console.WriteLine(
            $"    {label,-14} split={splitMs,8:F1}  step={stepMs,8:F1}ms  " +
            $"({stepEventsPerSec,12:N0} ev/s)  {BenchmarkHarness.FormatRatio(stepUp).Trim()}");
        output.AppendLine(
            $"| {label} | {splitMs:F1} | {stepMs:F1} | {stepEventsPerSec:N0} | " +
            $"{BenchmarkHarness.FormatRatio(stepUp).Trim()} | {gatherMs:F1} | {outputRows:N0} |");
    }
}
