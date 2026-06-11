// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// W&gt;1 in-<c>Step</c> gate for cross-tick delta-builder pooling
/// (docs/design-row-representation.md §20, <see cref="DbspNet.Core.Operators.Stateful.DeltaPoolMode"/>):
/// on Nexmark q4's whole <c>W</c>-replica parallel pipeline, does reusing the
/// join/aggregate output builders across ticks (instead of a fresh dictionary each
/// <c>Step</c>) move the step throughput? The <c>w1profile</c> gate showed a thin
/// W=1 allocation win (q4 −7.5 %) once §16.8 pre-sizing already removed the resize
/// churn; this checks whether it translates in-<c>Step</c> at W&gt;1.
/// </summary>
/// <remarks>
/// Both configs are the <see cref="TraceFamily.Flat"/> default; the only difference
/// is <see cref="SpineParallelHarness.FlatPool"/>. Pooling is sound on q4 because its
/// flat pipeline puts no <c>z⁻¹</c> on an operator-output delta (§16.7); the pooled
/// output is cross-checked identical to the unpooled output, which also re-proves the
/// retention constraint end-to-end on the parallel path.
/// </remarks>
internal static class Q4PoolBenchmark
{
    public static void Run(StringBuilder output, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? 8, 1, Environment.ProcessorCount);
        var query = Array.Find(NexmarkQueries.All, q => q.Id == "q4")
            ?? throw new InvalidOperationException("q4 not found in NexmarkQueries.All");
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine();
        Console.WriteLine($"=== q4 delta-pooling gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);
        var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);

        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            throw new InvalidOperationException("q4 has no parallel plan — cannot run the pooling gate");
        }

        probe!.Dispose();

        output.AppendLine("# DbspNet — q4 cross-tick delta-pooling gate (§20)");
        output.AppendLine();
        output.AppendLine(
            "W>1 in-`Step` A/B of reusing the join/aggregate output builders across ticks " +
            "(`DeltaPoolMode`) vs a fresh builder each `Step`. Both configs are the " +
            "`TraceFamily.Flat` default. Pooled output is cross-checked identical.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, W={workers}, median **step** throughput of " +
            $"{runs} run(s) after one warmup, timed apart from split/gather. Host: " +
            $".NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();

        foreach (var batchSize in new[] { 10_000, 100_000 })
        {
            RunBatch(output, plan, events, consumed, batchSize, workers, runs, totalEvents);
        }

        output.AppendLine(
            "**Reading it.** *Step↑* is unpooled / pooled on the step phase. Pooling is " +
            "in-`Step` but only removes the *steady* builder backing that §16.8 pre-sizing " +
            "still re-allocates — a thin term, so a small step move is expected.");
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

        var (uSplit, uStep, uGather, uRows, uResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        var reference = SpineParallelHarness.Materialize(uResult);
        EmitRow(output, "flat·unpooled", uSplit, uStep, uGather, uRows, totalEvents, baseline: uStep);

        var (pSplit, pStep, pGather, pRows, pResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.FlatPool, events, consumed, batchSize, runs);
        if (!SpineParallelHarness.SameMultiset(reference, SpineParallelHarness.Materialize(pResult)))
        {
            throw new InvalidOperationException(
                $"flat·pooled output diverged from flat·unpooled at batch={batchSize} — aborting gate");
        }

        EmitRow(output, "flat·pooled", pSplit, pStep, pGather, pRows, totalEvents, baseline: uStep);

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
