// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Fuller evaluation behind the "should we flip the spine + memtable default?"
/// decision (docs/design-row-representation.md §10): runs the candidate config
/// — <c>TraceFamily.Spine</c> + merge probe + memtable (staged) — against the
/// flat default across the stateful Nexmark queries, and sweeps the memtable
/// capacity on q4 to find the knee. The stateless queries (q0–q2) are included
/// as controls: they hold no spine trace, so flat ≈ spine confirms the spine
/// compile path adds no boundary regression.
/// </summary>
internal static class SpineEvalBenchmark
{
    private const int Batch = 10_000;            // the realistic q4 micro-batch (§8.3/§10)
    private const int DefaultCapacity = 8192;    // the §10 staging capacity
    private static readonly int[] SweepCapacities = { 1_024, 4_096, 8_192, 16_384, 32_768, 65_536 };

    public static void Run(StringBuilder output, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? Environment.ProcessorCount, 1, Environment.ProcessorCount);
        Console.WriteLine();
        Console.WriteLine($"=== spine + memtable evaluation (events={totalEvents:N0}, W={workers}, batch={Batch:N0}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        output.AppendLine("# DbspNet — spine + memtable evaluation");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} Nexmark events, W={workers}, batch={Batch:N0}, median **step** of " +
            $"{runs} run(s) after one warmup. Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores. " +
            "Every spine config's output is cross-checked against flat. **Step↑** is flat/config (> 1.0× = the " +
            "config out-steps the flat default).");
        output.AppendLine();
        output.AppendLine(
            "The candidate default is **spine·merge·staged** — `TraceFamily.Spine` with the merge probe and the " +
            $"per-tick memtable (capacity {DefaultCapacity:N0}, §10). **spine·merge** (memtable off) is shown to " +
            "isolate the memtable's contribution. q0–q2 are stateless controls (no spine trace).");
        output.AppendLine();

        PerQuery(output, events, workers, runs);
        CapacitySweep(output, events, workers, runs);

        output.AppendLine("## Verdict");
        output.AppendLine();
        output.AppendLine(
            "Read the **stateful** queries (q3/q4/q9) at *spine·merge·staged vs flat*: that is the flip decision. " +
            "The memtable's contribution is *spine·merge·staged vs spine·merge*. The controls (q0–q2) should sit at " +
            "~1.0× — any large deviation there is a spine-compile-path boundary cost unrelated to the trace. The " +
            "capacity sweep locates the flush threshold that maximises q4 step throughput; too small re-introduces " +
            "per-tick builds, too large grows the memtable's per-read merge cost.");
        output.AppendLine();
    }

    private static void PerQuery(StringBuilder output, List<Event> events, int workers, int runs)
    {
        output.AppendLine("## Per-query — flat vs spine·merge vs spine·merge·staged (batch " + Batch.ToString("N0", System.Globalization.CultureInfo.InvariantCulture) + ")");
        output.AppendLine();
        output.AppendLine("| Query | Description | flat step (ms) | spine·merge ↑ | spine·merge·staged ↑ |");
        output.AppendLine("|:------|:------------|---------------:|--------------:|---------------------:|");

        foreach (var query in NexmarkQueries.All)
        {
            Console.WriteLine($"  {query.Id} — {query.Description}");
            var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);
            if (!CanParallelize(plan, workers))
            {
                Console.WriteLine("    (no parallel form — skipped)");
                output.AppendLine($"| {query.Id} | {query.Description} | — (no parallel form) | — | — |");
                continue;
            }

            var consumed = query.Tables.ToHashSet();

            var (_, flatStep, _, _, flatResult) =
                SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.Flat, events, consumed, Batch, runs);
            var reference = SpineParallelHarness.Materialize(flatResult);

            var mergeUp = StepUp(plan, workers, consumed, events, runs, flatStep, reference,
                SpineParallelHarness.Spine(forcePointProbe: false, stagingCapacity: 0), query.Id, Batch);
            var stagedUp = StepUp(plan, workers, consumed, events, runs, flatStep, reference,
                SpineParallelHarness.Spine(forcePointProbe: false, stagingCapacity: DefaultCapacity), query.Id, Batch);

            Console.WriteLine($"    flat={flatStep,7:F1}ms  merge={Ratio(mergeUp)}  staged={Ratio(stagedUp)}");
            output.AppendLine($"| {query.Id} | {query.Description} | {flatStep:F1} | {Ratio(mergeUp)} | {Ratio(stagedUp)} |");
        }

        output.AppendLine();
    }

    private static void CapacitySweep(StringBuilder output, List<Event> events, int workers, int runs)
    {
        var query = Array.Find(NexmarkQueries.All, q => q.Id == "q4")!;
        var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine("  capacity sweep (q4)");
        output.AppendLine("## Memtable capacity sweep — q4 (batch " + Batch.ToString("N0", System.Globalization.CultureInfo.InvariantCulture) + ")");
        output.AppendLine();
        output.AppendLine("| Capacity (keys) | spine·merge·staged step (ms) | Step↑ vs flat |");
        output.AppendLine("|----------------:|-----------------------------:|--------------:|");

        var (_, flatStep, _, _, flatResult) =
            SpineParallelHarness.MeasureMedian(plan, workers, SpineParallelHarness.Flat, events, consumed, Batch, runs);
        var reference = SpineParallelHarness.Materialize(flatResult);
        output.AppendLine($"| flat (baseline) | {flatStep:F1} | 1.00× |");
        output.AppendLine($"| 0 (memtable off) | {Measure(plan, workers, consumed, events, runs, reference, SpineParallelHarness.Spine(false, 0), "q4")} |");

        foreach (var cap in SweepCapacities)
        {
            var line = Measure(plan, workers, consumed, events, runs, reference,
                SpineParallelHarness.Spine(forcePointProbe: false, stagingCapacity: cap), "q4");
            Console.WriteLine($"    cap={cap,-7} {line}");
            output.AppendLine($"| {cap:N0} | {line} |");
        }

        output.AppendLine();

        // local: measure one config, return "step | ratio" cells using flatStep.
        string Measure(LogicalPlan p, int w, HashSet<NexmarkTable> cons, List<Event> ev, int r,
            Dictionary<string, long> refOut, SpineParallelHarness.RunConfig cfg, string id)
        {
            var (_, step, _, _, result) = SpineParallelHarness.MeasureMedian(p, w, cfg, ev, cons, Batch, r);
            if (!SpineParallelHarness.SameMultiset(refOut, SpineParallelHarness.Materialize(result)))
            {
                throw new InvalidOperationException($"{id} cap-sweep output diverged from flat — aborting");
            }

            var up = flatStep > 0 ? flatStep / step : 1.0;
            return $"{step:F1} | {Ratio(up)}";
        }
    }

    private static double StepUp(
        LogicalPlan plan, int workers, HashSet<NexmarkTable> consumed, List<Event> events, int runs,
        double flatStep, Dictionary<string, long> reference, SpineParallelHarness.RunConfig config, string id, int batch)
    {
        var (_, step, _, _, result) = SpineParallelHarness.MeasureMedian(plan, workers, config, events, consumed, batch, runs);
        if (!SpineParallelHarness.SameMultiset(reference, SpineParallelHarness.Materialize(result)))
        {
            throw new InvalidOperationException($"{id} spine output diverged from flat — aborting eval");
        }

        return flatStep > 0 ? flatStep / step : 1.0;
    }

    private static bool CanParallelize(LogicalPlan plan, int workers)
    {
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q))
        {
            return false;
        }

        q!.Dispose();
        return true;
    }

    private static string Ratio(double r) => BenchmarkHarness.FormatRatio(r).Trim();
}
