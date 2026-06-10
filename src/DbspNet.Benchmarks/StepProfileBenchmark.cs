// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Core.Circuit;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Decomposes the parallel-circuit <b>step</b> of the exchange/scaling-bound
/// Nexmark queries (q18, q4, q19, q22) into its sub-phases so the W-scaling
/// ceiling can be attributed (docs/design-row-representation.md §15). The q18
/// profile established that these queries are <em>step-bound</em> and saturate
/// by ~W=12; this answers the next question — <em>why</em>: is the cost the
/// all-to-all <b>movement</b> of wide rows (split + gather-rebuild), the
/// <b>coordination</b> (idle at the exchange barrier), or residual <b>operator</b>
/// compute — and how much of the idle is genuine load <b>imbalance</b>.
/// </summary>
/// <remarks>
/// Per worker, per step, the <see cref="StepProfiler"/> records the time in
/// <c>split</c> (bucket rows by <c>hash(key)%W</c>), <c>wait</c> (block at the
/// exchange rendezvous — idle for the slowest splitter + barrier latency) and
/// <c>gather</c> (rebuild the post-shuffle indexed Z-set, re-hashing full rows),
/// plus the whole replica's step wall time; <c>op = step − split − wait − gather</c>
/// is the operator work. The phases are averaged across workers (mean per step);
/// the critical path is the slowest worker's step, and imbalance is the spread of
/// per-worker <em>busy</em> time (<c>step − wait</c>).
/// </remarks>
internal static class StepProfileBenchmark
{
    public static void Run(StringBuilder output, string[] queryIds, int totalEvents, int[]? sweep = null)
    {
        var host = Environment.ProcessorCount;
        var widths = (sweep is { Length: > 0 }
                ? sweep
                : new[] { 1, Math.Max(2, host / 2), host })
            .Where(w => w >= 1).Distinct().OrderBy(w => w).ToArray();

        Console.WriteLine();
        Console.WriteLine($"=== step profile (events={totalEvents:N0}, host={host} cores) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        output.AppendLine("# DbspNet — parallel step decomposition (movement vs coordination vs op)");
        output.AppendLine();
        output.AppendLine(
            "Each parallel-circuit **step** is split, per worker, into **split** (bucket " +
            "this shard's rows by `hash(key)%W`), **wait** (idle at the exchange barrier — " +
            "waiting for the slowest splitter + barrier latency), **gather** (rebuild the " +
            "post-shuffle indexed Z-set, re-hashing full rows) and **op** (the residual " +
            "operator compute: join / aggregate / TOP-K). The per-phase figures are the " +
            "**mean per-step ms across the W workers**; **Ctrl** is the controller's real " +
            "per-step wall clock (= Σ_tick max-worker step — what actually bounds " +
            "throughput). *Move%* = (split+gather)/step, *Wait%* = wait/step, *Op%* = " +
            "op/step. **Strag** = Ctrl / mean-Step — the barrier's straggler tax (1.00 = no " +
            "tax; >1 = the per-tick slowest worker drags the rest). *Imbal* = max-busy / " +
            "mean-busy, busy = step−wait, the *persistent* per-worker work skew.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, batch 10k. Host: .NET {Environment.Version}, {host} logical cores. " +
            "One warmup pass then one profiled pass per (query, W). **Single-pass — read " +
            "trends, not third-digit cell deltas.**");
        output.AppendLine();

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

            output.AppendLine($"## {id}");
            output.AppendLine();
            output.AppendLine("| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |");
            output.AppendLine("|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|");

            double w1Op = 0, w1Ctrl = 0;
            foreach (var w in widths)
            {
                Console.WriteLine($"    W={w}…");
                var critTotalMs = SpineParallelHarness.MeasureProfiled(
                    plan, w, SpineParallelHarness.Flat, events, consumed, batchSize: 10_000);
                var ctrlPerStep = critTotalMs / Math.Max(1, StepProfiler.Steps);

                var m = Summarize(w);
                if (w == 1)
                {
                    w1Op = m.Op;
                    w1Ctrl = ctrlPerStep;
                }

                // Straggler amplification: the controller's real per-step wall clock
                // (= Σ_tick max_worker step) over the mean per-worker step. >1 and
                // rising = the per-step/per-exchange barriers pay for the slowest
                // worker each tick (load imbalance + core heterogeneity + jitter).
                var strag = m.Step > 0 ? ctrlPerStep / m.Step : 1.0;
                var opDown = m.Op > 0 ? w1Op / m.Op : 1.0;
                var ctrlDown = ctrlPerStep > 0 ? w1Ctrl / ctrlPerStep : 1.0;
                output.AppendLine(
                    $"| {w} | {ctrlPerStep:F2} | {m.Step:F2} | {m.Split:F2} | {m.Wait:F2} | {m.Gather:F2} | {m.Op:F2} | " +
                    $"{m.MovePct:F0}% | {m.WaitPct:F0}% | {m.OpPct:F0}% | {strag:F2} | {m.Imbalance:F2} | {ctrlDown:F2}× | {opDown:F2}× |");
                Console.WriteLine(
                    $"      ctrl={ctrlPerStep,7:F2} step={m.Step,7:F2} split={m.Split,6:F2} wait={m.Wait,6:F2} " +
                    $"gather={m.Gather,6:F2} op={m.Op,6:F2}  wait={m.WaitPct,3:F0}% strag={strag:F2} imbal={m.Imbalance:F2}");
            }

            output.AppendLine();
        }

        output.AppendLine("**Reading it.** Per-step phase ms should fall ~`1/W` if that phase " +
            "parallelises. A high and *rising* **Wait%** / **Strag** with W means the ceiling " +
            "is coordination: the per-step/per-exchange barrier pays for the slowest worker " +
            "each tick, and as W grows each worker's 10k/W-row slice shrinks so the relative " +
            "variance — and the idle — grows. A flat **Gather**/**Split** that refuses to " +
            "shrink `1/W` would mean the wide-row movement is bandwidth-bound; an **Op** that " +
            "scales cleanly while *Ctrl* does not confirms the gap is coordination, not the " +
            "operator. **Imbal ≫ 1** is *persistent* skew (one worker always heavier — " +
            "rebalance the hash); **Strag ≫ 1 with Imbal ≈ 1** is *per-tick* straggling " +
            "(rotating unlucky worker + barrier) — only coarser ticks or fewer barriers help.");
        output.AppendLine();
        output.AppendLine("> **Host caveat.** This box is an i9-12900K — a *hybrid* 8 P-core " +
            "(16 threads) + 8 E-core (8 threads) part, 24 logical. Past W≈16 some workers land " +
            "on the slower E-cores and become *permanent* stragglers the barrier waits on every " +
            "tick, so the W>16 rows conflate this heterogeneity with the structural barrier " +
            "tax. On a homogeneous server CPU the W>16 degradation would be milder; the " +
            "structural trend (barrier tax rising with W at fine ticks) is the portable finding.");
        output.AppendLine();
    }

    private readonly record struct PhaseSummary(
        double Step, double Split, double Wait, double Gather, double Op,
        double MovePct, double WaitPct, double OpPct, double CritStep, double Imbalance);

    /// <summary>Average the profiler's per-worker tick totals into per-step milliseconds.</summary>
    private static PhaseSummary Summarize(int workers)
    {
        var steps = Math.Max(1, StepProfiler.Steps);
        double ToMs(long ticks) => ticks * 1000.0 / StepProfiler.Frequency / steps;

        double sumStep = 0, sumSplit = 0, sumWait = 0, sumGather = 0, sumOp = 0;
        double maxStep = 0, maxBusy = 0, sumBusy = 0;
        for (var w = 0; w < workers; w++)
        {
            var step = ToMs(StepProfiler.StepTicksOf(w));
            var split = ToMs(StepProfiler.SplitTicksOf(w));
            var wait = ToMs(StepProfiler.WaitTicksOf(w));
            var gather = ToMs(StepProfiler.GatherTicksOf(w));
            var op = Math.Max(0, step - split - wait - gather);
            var busy = step - wait;

            sumStep += step;
            sumSplit += split;
            sumWait += wait;
            sumGather += gather;
            sumOp += op;
            sumBusy += busy;
            maxStep = Math.Max(maxStep, step);
            maxBusy = Math.Max(maxBusy, busy);
        }

        var meanStep = sumStep / workers;
        var meanSplit = sumSplit / workers;
        var meanWait = sumWait / workers;
        var meanGather = sumGather / workers;
        var meanOp = sumOp / workers;
        var meanBusy = sumBusy / workers;

        var movePct = meanStep > 0 ? 100.0 * (meanSplit + meanGather) / meanStep : 0;
        var waitPct = meanStep > 0 ? 100.0 * meanWait / meanStep : 0;
        var opPct = meanStep > 0 ? 100.0 * meanOp / meanStep : 0;
        var imbalance = meanBusy > 0 ? maxBusy / meanBusy : 1.0;

        return new PhaseSummary(
            meanStep, meanSplit, meanWait, meanGather, meanOp,
            movePct, waitPct, opPct, maxStep, imbalance);
    }
}
