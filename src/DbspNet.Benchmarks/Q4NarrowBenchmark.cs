// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// W&gt;1 in-<c>Step</c> gate for the non-linear-aggregate input narrowing lever
/// (docs/design-row-representation.md §18 — the term-2 / whole-row-hash attack).
/// Nexmark q4's inner <c>MAX(b.price) GROUP BY a.id, a.category</c> stores the full
/// ~17-column join row in its per-auction inner multiset because
/// <c>NarrowAggregateInput</c> conservatively bails on MIN/MAX. Narrowing the input
/// to <c>{a.id, a.category, b.price}</c> shrinks the hashed/stored value rows ~5×.
/// The <c>w1profile</c> gate showed −35% time / −23% alloc at W=1; this checks the
/// §16.11 claim that an <b>in-<c>Step</c></b> per-row win translates to W&gt;1
/// (unlike the out-of-<c>Step</c> output-boundary lever 2).
/// </summary>
/// <remarks>
/// Both configs are the <see cref="TraceFamily.Flat"/> default; the only difference
/// is whether the q4 plan was optimized with <see cref="NonLinearNarrowingMode"/>
/// enabled (a pure logical-plan rewrite — once baked into the plan it needs no
/// runtime seam). Narrowing is sound here because Nexmark is insert-only (a
/// non-negative per-group integral); the narrowed output is cross-checked identical
/// to the full-row output, which also re-confirms in-envelope correctness on the
/// parallel typed path.
/// </remarks>
internal static class Q4NarrowBenchmark
{
    public static void Run(StringBuilder output, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? 8, 1, Environment.ProcessorCount);
        var query = Array.Find(NexmarkQueries.All, q => q.Id == "q4")
            ?? throw new InvalidOperationException("q4 not found in NexmarkQueries.All");
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine();
        Console.WriteLine($"=== q4 non-linear narrowing gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        // Build the q4 plan both ways. Narrowing is a logical-plan rewrite, so the
        // seam only matters during Optimize (BuildPlan); the compiled parallel
        // circuit consumes the already-narrowed plan.
        var planFull = BuildPlan(query.Sql, narrow: false);
        var planNarrow = BuildPlan(query.Sql, narrow: true);

        if (!TypedPlanCompiler.TryCompileParallel(planNarrow, workers, out var probe))
        {
            throw new InvalidOperationException("q4 narrowed plan has no parallel plan — cannot run the gate");
        }

        probe!.Dispose();

        output.AppendLine("# DbspNet — q4 non-linear aggregate input narrowing gate (§18)");
        output.AppendLine();
        output.AppendLine(
            "W>1 in-`Step` A/B of narrowing the inner `MAX(price) GROUP BY auction` input " +
            "to `{auction_id, category, price}` (vs the full ~17-column join row). Both " +
            "configs are the `TraceFamily.Flat` default; the only difference is whether the " +
            "q4 plan was optimized with non-linear narrowing enabled.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, W={workers}, median **step** throughput of " +
            $"{runs} run(s) after one warmup, timed apart from split/gather. Host: " +
            $".NET {Environment.Version}, {Environment.ProcessorCount} cores. The narrowed " +
            "output is cross-checked identical to the full-row output (insert-only ⇒ sound).");
        output.AppendLine();

        foreach (var batchSize in new[] { 10_000, 100_000 })
        {
            RunBatch(output, planFull, planNarrow, events, consumed, batchSize, workers, runs, totalEvents);
        }

        output.AppendLine(
            "**Reading it.** *Step↑* is full / narrowed on the step phase. Narrowing is " +
            "in-`Step` (it shrinks the rows the inner aggregate hashes/stores), so per " +
            "§16.11 the W=1 win is expected to translate to W>1 rather than be Amdahl-eaten " +
            "the way the out-of-`Step` output-boundary lever was.");
        output.AppendLine();
    }

    private static LogicalPlan BuildPlan(string sql, bool narrow)
    {
        // Force the policy in both directions: the Nexmark DDL now declares its
        // tables append_only, so the default (Auto) would narrow BOTH arms and
        // collapse the A/B. Never = the pre-analysis baseline.
        var prev = NonLinearNarrowingMode.Mode;
        NonLinearNarrowingMode.Mode = narrow ? NonLinearNarrowing.Always : NonLinearNarrowing.Never;
        try
        {
            return SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, sql);
        }
        finally
        {
            NonLinearNarrowingMode.Mode = prev;
        }
    }

    private static void RunBatch(
        StringBuilder output, LogicalPlan planFull, LogicalPlan planNarrow, List<Event> events,
        HashSet<NexmarkTable> consumed, int batchSize, int workers, int runs, int totalEvents)
    {
        Console.WriteLine();
        Console.WriteLine($"  batch={batchSize:N0}");

        output.AppendLine($"## Batch = {batchSize:N0} events");
        output.AppendLine();
        output.AppendLine("| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |");
        output.AppendLine("|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|");

        // Full-row first; its output is the reference the narrowed config must reproduce.
        var (fSplit, fStep, fGather, fRows, fResult) =
            SpineParallelHarness.MeasureMedian(planFull, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        var reference = SpineParallelHarness.Materialize(fResult);
        EmitRow(output, "flat·full", fSplit, fStep, fGather, fRows, totalEvents, baseline: fStep);

        var (nSplit, nStep, nGather, nRows, nResult) =
            SpineParallelHarness.MeasureMedian(planNarrow, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        if (!SpineParallelHarness.SameMultiset(reference, SpineParallelHarness.Materialize(nResult)))
        {
            throw new InvalidOperationException(
                $"flat·narrow output diverged from flat·full at batch={batchSize} — aborting gate");
        }

        EmitRow(output, "flat·narrow", nSplit, nStep, nGather, nRows, totalEvents, baseline: fStep);

        output.AppendLine();
    }

    private static void EmitRow(
        StringBuilder output, string label, double splitMs, double stepMs, double gatherMs,
        long outputRows, int totalEvents, double baseline)
    {
        var stepEventsPerSec = stepMs > 0 ? totalEvents / (stepMs / 1000.0) : 0.0;
        var stepUp = stepMs > 0 ? baseline / stepMs : 1.0;
        Console.WriteLine(
            $"    {label,-12} split={splitMs,8:F1}  step={stepMs,8:F1}ms  " +
            $"({stepEventsPerSec,12:N0} ev/s)  {BenchmarkHarness.FormatRatio(stepUp).Trim()}");
        output.AppendLine(
            $"| {label} | {splitMs:F1} | {stepMs:F1} | {stepEventsPerSec:N0} | " +
            $"{BenchmarkHarness.FormatRatio(stepUp).Trim()} | {gatherMs:F1} | {outputRows:N0} |");
    }
}
