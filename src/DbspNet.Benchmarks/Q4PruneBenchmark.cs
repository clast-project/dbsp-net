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
/// W&gt;1 in-<c>Step</c> gate for projection pushdown through an INNER join
/// (docs/design-row-representation.md §21 — the term-2 / whole-row-hash attack the
/// §18 aggregate-input narrowing cannot reach because it sits above the join).
/// Nexmark q4's <c>auction ⋈ bid</c> stores the full ~10-column auction and ~7-column
/// bid rows in its join traces even though only <c>{id,category,date_time,expires}</c>
/// and <c>{auction,price,date_time}</c> are live (read by the aggregate, residual, and
/// equi-key). Pruning the stored rows to those shrinks the rows the join trace
/// whole-row-hashes on every integrate. The <c>w1profile</c> gate showed −50% time /
/// −33% alloc at W=1; this checks the §16.11 claim that an <b>in-<c>Step</c></b>
/// per-row win translates to W&gt;1 (the join integrate runs inside <c>Step</c>).
/// </summary>
/// <remarks>
/// Both configs are the <see cref="TraceFamily.Flat"/> default; the only difference is
/// whether the q4 plan was optimized with <see cref="JoinColumnPruningMode"/> enabled
/// (a pure logical-plan rewrite — once baked into the plan it needs no runtime seam).
/// Pruning is <b>unconditionally sound</b> (ordinary relational projection pushdown),
/// not envelope-restricted like §18; the pruned output is cross-checked identical to
/// the full-row output, which also re-confirms correctness on the parallel typed path
/// (the typed-compiler reflection gotcha is dodged — the rewrite is at Optimize time).
/// </remarks>
internal static class Q4PruneBenchmark
{
    public static void Run(StringBuilder output, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? 8, 1, Environment.ProcessorCount);
        var query = Array.Find(NexmarkQueries.All, q => q.Id == "q4")
            ?? throw new InvalidOperationException("q4 not found in NexmarkQueries.All");
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine();
        Console.WriteLine($"=== q4 join column-pruning gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        // Build the q4 plan both ways. Pruning is a logical-plan rewrite, so the seam
        // only matters during Optimize (BuildPlan); the compiled parallel circuit
        // consumes the already-pruned plan.
        var planFull = BuildPlan(query.Sql, prune: false);
        var planPrune = BuildPlan(query.Sql, prune: true);

        if (!TypedPlanCompiler.TryCompileParallel(planPrune, workers, out var probe))
        {
            throw new InvalidOperationException("q4 pruned plan has no parallel plan — cannot run the gate");
        }

        probe!.Dispose();

        output.AppendLine("# DbspNet — q4 join column-pruning gate (§21)");
        output.AppendLine();
        output.AppendLine(
            "W>1 in-`Step` A/B of pruning the `auction ⋈ bid` stored join rows to the columns " +
            "the aggregate/residual/equi-key read (`{id,category,date_time,expires}` and " +
            "`{auction,price,date_time}`) vs the full ~10/~7-column source rows. Both configs " +
            "are the `TraceFamily.Flat` default; the only difference is whether the q4 plan was " +
            "optimized with join column pruning enabled.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, W={workers}, median **step** throughput of " +
            $"{runs} run(s) after one warmup, timed apart from split/gather. Host: " +
            $".NET {Environment.Version}, {Environment.ProcessorCount} cores. The pruned " +
            "output is cross-checked identical to the full-row output (unconditionally sound).");
        output.AppendLine();

        foreach (var batchSize in new[] { 10_000, 100_000 })
        {
            RunBatch(output, planFull, planPrune, events, consumed, batchSize, workers, runs, totalEvents);
        }

        output.AppendLine(
            "**Reading it.** *Step↑* is full / pruned on the step phase. Pruning is in-`Step` " +
            "(it shrinks the rows the join trace hashes/stores on integrate), so per §16.11 the " +
            "W=1 win is expected to translate to W>1 rather than be Amdahl-eaten the way the " +
            "out-of-`Step` output-boundary lever was.");
        output.AppendLine();
    }

    private static LogicalPlan BuildPlan(string sql, bool prune)
    {
        var prev = JoinColumnPruningMode.Enabled;
        JoinColumnPruningMode.Enabled = prune;
        try
        {
            return SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, sql);
        }
        finally
        {
            JoinColumnPruningMode.Enabled = prev;
        }
    }

    private static void RunBatch(
        StringBuilder output, LogicalPlan planFull, LogicalPlan planPrune, List<Event> events,
        HashSet<NexmarkTable> consumed, int batchSize, int workers, int runs, int totalEvents)
    {
        Console.WriteLine();
        Console.WriteLine($"  batch={batchSize:N0}");

        output.AppendLine($"## Batch = {batchSize:N0} events");
        output.AppendLine();
        output.AppendLine("| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |");
        output.AppendLine("|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|");

        // Full-row first; its output is the reference the pruned config must reproduce.
        var (fSplit, fStep, fGather, fRows, fResult) =
            SpineParallelHarness.MeasureMedian(planFull, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        var reference = SpineParallelHarness.Materialize(fResult);
        EmitRow(output, "flat·full", fSplit, fStep, fGather, fRows, totalEvents, baseline: fStep);

        var (pSplit, pStep, pGather, pRows, pResult) =
            SpineParallelHarness.MeasureMedian(planPrune, workers, SpineParallelHarness.Flat, events, consumed, batchSize, runs);
        if (!SpineParallelHarness.SameMultiset(reference, SpineParallelHarness.Materialize(pResult)))
        {
            throw new InvalidOperationException(
                $"flat·prune output diverged from flat·full at batch={batchSize} — aborting gate");
        }

        EmitRow(output, "flat·prune", pSplit, pStep, pGather, pRows, totalEvents, baseline: fStep);

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
