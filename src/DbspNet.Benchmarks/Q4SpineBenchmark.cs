// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// End-to-end gate for the spine merge-probe on Nexmark q4 (the design-doc
/// "q4 gate", <c>docs/design-row-representation.md</c> §8). The operator-level
/// gates (<c>joinprobe</c>, <c>aggprobe</c>) already showed merge beats
/// point-probe on a single <c>Step</c>; this asks the *system* question the
/// design doc deferred: does routing q4's whole parallel pipeline onto the
/// spine trace family beat the flat default at the host core count, and does
/// the merge probe carry its operator-level win all the way through?
/// </summary>
/// <remarks>
/// q4 is the right target: it is step-bound (a join feeding a per-group MAX
/// then an outer AVG), so its cost lives in exactly the two operators the
/// merge probe touches. The harness streams the standard Nexmark event stream
/// through q4's <see cref="ParallelTypedCompiledQuery"/> at <c>W = host
/// cores</c> in three configurations and times the <b>step</b> phase apart
/// from ingest/egest (the merge probe only moves step work):
/// <list type="bullet">
///   <item><b>flat</b> — <see cref="TraceFamily.Flat"/>, today's default;</item>
///   <item><b>spine·point</b> — <see cref="TraceFamily.Spine"/> with
///     <see cref="SpineJoinProbeMode.ForcePointProbe"/> /
///     <see cref="SpineAggregateProbeMode.ForcePointProbe"/> set, i.e. the
///     spine substrate <em>without</em> the merge probe;</item>
///   <item><b>spine·merge</b> — <see cref="TraceFamily.Spine"/> with the merge
///     probe live.</item>
/// </list>
/// Reporting all three separates the two effects the single "spine vs flat"
/// number would conflate: <b>spine·point vs flat</b> is the cost/benefit of the
/// LSM substrate itself, and <b>spine·merge vs spine·point</b> is the
/// merge probe's end-to-end contribution. The merge probe ships as the typed
/// default only if spine·merge beats flat; if it doesn't, the honest finding
/// is that the operator-level win does not yet survive the surrounding
/// per-tick exchange/build/integrate cost (and the substrate needs sharing
/// before the flip is worth it). The batch-size sweep exercises the design
/// doc's open §5 caveat — small ticks (D→1, point-probe guard) vs large ticks
/// (wide D, where the merge dominates). Every config's final output is
/// cross-checked against flat; a mismatch aborts (the numbers would describe a
/// broken circuit).
/// </remarks>
internal static class Q4SpineBenchmark
{
    private enum Config
    {
        Flat,
        SpinePoint,
        SpineMerge,
        SpinePointStaged,
        SpineMergeStaged,
    }

    // Memtable flush threshold (distinct keys) for the staged configs — a few
    // q4 replica-ticks' worth, so the spine integrate amortises its batch build
    // across many ticks instead of building one per tick (docs §9.7).
    private const int StagingCapacity = 8192;

    public static void Run(StringBuilder output, int totalEvents, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? Environment.ProcessorCount, 1, Environment.ProcessorCount);
        var query = Array.Find(NexmarkQueries.All, q => q.Id == "q4")
            ?? throw new InvalidOperationException("q4 not found in NexmarkQueries.All");
        var consumed = query.Tables.ToHashSet();

        Console.WriteLine();
        Console.WriteLine($"=== q4 spine gate (events={totalEvents:N0}, W={workers}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);
        var plan = BuildPlan(NexmarkQueries.Ddl, query.Sql);

        // Verify the plan even has a parallel form before measuring.
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            throw new InvalidOperationException("q4 has no parallel plan — cannot run the spine gate");
        }

        probe!.Dispose();

        output.AppendLine("# DbspNet — q4 spine merge-probe gate");
        output.AppendLine();
        output.AppendLine(
            "End-to-end test of the spine merge-probe (`docs/design-row-representation.md` " +
            "§8) on Nexmark q4 — *average closing price by category* (join → per-group " +
            "MAX → outer AVG), the step-bound query whose cost lives in the two operators " +
            "the merge probe touches. The whole q4 pipeline runs as a `W`-replica " +
            "`ParallelCircuit` in three configurations; the **step** phase is timed apart " +
            "from ingest (split) and egest (gather), since the merge probe only moves step " +
            "work:");
        output.AppendLine();
        output.AppendLine(
            "- **flat** — the `TraceFamily.Flat` default (dictionary-backed traces).");
        output.AppendLine(
            "- **spine·point** — `TraceFamily.Spine` with the merge probe *forced off* " +
            "(`ForcePointProbe`): the LSM substrate, still per-key point-probing. Isolates " +
            "the cost/benefit of the substrate itself.");
        output.AppendLine(
            "- **spine·merge** — `TraceFamily.Spine` with the merge probe live. Isolates " +
            "the merge probe's contribution on top of spine·point.");
        output.AppendLine(
            $"- **spine·point·staged / spine·merge·staged** — the same two, with the " +
            $"per-tick **memtable** enabled (flush threshold {StagingCapacity:N0} keys, docs " +
            "§9.7): each tick's delta is an in-place dictionary merge that flushes to a " +
            "sorted batch only every few ticks, instead of a fresh batch build per tick. " +
            "This targets the §8.3 finding that the substrate's loss to flat was the " +
            "per-tick build, not the probe — so *staged vs un-staged* is staging's " +
            "contribution, and *spine·merge·staged vs flat* is the real question.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, W={workers}, median step throughput of {runs} " +
            $"run(s) after one warmup. Host: .NET {Environment.Version}, " +
            $"{Environment.ProcessorCount} cores.");
        output.AppendLine();
        output.AppendLine(
            "The merge probe ships as the typed default only if **spine·merge beats flat**. " +
            "If it does not, the operator-level win (see `join-probe-bench.md` / " +
            "`aggregate-probe-bench.md`) does not yet survive the surrounding per-tick " +
            "exchange/build/integrate cost — the substrate needs cross-operator sharing " +
            "before the flip pays.");
        output.AppendLine();

        // Sweep batch sizes: the standard Nexmark micro-batch (10k) is the realistic
        // operating point; the larger 100k batch widens per-tick D, where the merge
        // dominates; both exercise the §5 small-vs-large-tick caveat.
        foreach (var batchSize in new[] { 10_000, 100_000 })
        {
            RunBatch(output, plan, events, consumed, batchSize, workers, runs, totalEvents);
        }

        output.AppendLine(
            "**Reading it.** *Step↑ vs flat* is the headline gate: > 1.0× means the spine " +
            "config out-steps the flat default. *spine·merge vs spine·point* attributes the " +
            "merge probe's share. Expect the gap to widen with batch size (wider D → the " +
            "merge skips more whole-row hashing); at the small batch, ticks approach the " +
            "`D == 1` point-probe guard, so spine·merge and spine·point converge.");
        output.AppendLine();
    }

    private static void RunBatch(
        StringBuilder output, LogicalPlan plan, List<Event> events, HashSet<NexmarkTable> consumed,
        int batchSize, int workers, int runs, int totalEvents)
    {
        Console.WriteLine();
        Console.WriteLine($"  batch={batchSize:N0}");

        // Reference output (flat) — every config must reproduce it.
        var reference = Materialize(RunStream(plan, workers, Config.Flat, events, consumed, batchSize, out _, out _));

        output.AppendLine($"## Batch = {batchSize:N0} events");
        output.AppendLine();
        output.AppendLine("| Config | Split (ms) | Step (ms) | Step events/s | Step↑ vs flat | Gather (ms) | Output rows |");
        output.AppendLine("|:-------|-----------:|----------:|--------------:|--------------:|------------:|------------:|");

        double flatStepMs = 0;
        foreach (var config in new[]
        {
            Config.Flat, Config.SpinePoint, Config.SpineMerge,
            Config.SpinePointStaged, Config.SpineMergeStaged,
        })
        {
            var (splitMs, stepMs, gatherMs, outputRows, result) =
                MeasureMedian(plan, workers, config, events, consumed, batchSize, runs);

            if (!SameMultiset(reference, Materialize(result)))
            {
                throw new InvalidOperationException(
                    $"{config} output diverged from flat at batch={batchSize} — aborting gate");
            }

            if (config == Config.Flat)
            {
                flatStepMs = stepMs;
            }

            var stepEventsPerSec = stepMs > 0 ? totalEvents / (stepMs / 1000.0) : 0.0;
            var stepUp = flatStepMs > 0 ? flatStepMs / stepMs : 1.0;

            Console.WriteLine(
                $"    {Label(config),-12} split={splitMs,8:F1}  step={stepMs,8:F1}ms  " +
                $"({stepEventsPerSec,12:N0} ev/s)  {BenchmarkHarness.FormatRatio(stepUp).Trim()}");
            output.AppendLine(
                $"| {Label(config)} | {splitMs:F1} | {stepMs:F1} | {stepEventsPerSec:N0} | " +
                $"{BenchmarkHarness.FormatRatio(stepUp).Trim()} | {gatherMs:F1} | {outputRows:N0} |");
        }

        output.AppendLine();
    }

    /// <summary>
    /// Median of <paramref name="runs"/> full-stream passes (after one warmup) for one
    /// config, returning the median split / step / gather wall-clock and the final
    /// materialized output of the last pass (for the cross-check).
    /// </summary>
    private static (double SplitMs, double StepMs, double GatherMs, long OutputRows, IReadOnlyList<(object?[] Values, long Weight)> Result)
        MeasureMedian(
            LogicalPlan plan, int workers, Config config, List<Event> events,
            HashSet<NexmarkTable> consumed, int batchSize, int runs)
    {
        var splits = new List<double>(runs);
        var steps = new List<double>(runs);
        var gathers = new List<double>(runs);
        IReadOnlyList<(object?[], long)> last = Array.Empty<(object?[], long)>();
        long outputRows = 0;

        for (var run = 0; run < runs + 1; run++)
        {
            last = RunStream(plan, workers, config, events, consumed, batchSize, out var phases, out outputRows);
            if (run > 0)
            {
                splits.Add(phases.SplitMs);
                steps.Add(phases.StepMs);
                gathers.Add(phases.GatherMs);
            }
        }

        splits.Sort();
        steps.Sort();
        gathers.Sort();
        return (splits[splits.Count / 2], steps[steps.Count / 2], gathers[gathers.Count / 2], outputRows, last);
    }

    /// <summary>
    /// One full-stream pass through a fresh q4 parallel circuit in the given config.
    /// Accumulates the split (push) and step time over every micro-batch and times the
    /// final gather (materialize) once. The probe-mode flags are global statics read on
    /// the worker threads each <c>Step</c>; set before the run and cleared after.
    /// </summary>
    private static List<(object?[] Values, long Weight)> RunStream(
        LogicalPlan plan, int workers, Config config, List<Event> events,
        HashSet<NexmarkTable> consumed, int batchSize, out (double SplitMs, double StepMs, double GatherMs) phases,
        out long outputRows)
    {
        var options = config == Config.Flat
            ? CompileOptions.Default
            : new CompileOptions { TraceFamily = TraceFamily.Spine };

        var isPoint = config is Config.SpinePoint or Config.SpinePointStaged;
        var isStaged = config is Config.SpinePointStaged or Config.SpineMergeStaged;
        SpineJoinProbeMode.ForcePointProbe = isPoint;
        SpineAggregateProbeMode.ForcePointProbe = isPoint;
        // Read at trace construction during the compile below; cleared in finally.
        SpineStagingConfig.Capacity = isStaged ? StagingCapacity : 0;
        try
        {
            if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q, snapshotCodecs: null, options))
            {
                throw new InvalidOperationException($"q4 parallel compile failed for {config}");
            }

            using (q)
            {
                var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>());
                double splitMs = 0, stepMs = 0;
                var sw = new Stopwatch();
                var sinceStep = 0;
                foreach (var e in events)
                {
                    if (consumed.Contains(e.Table))
                    {
                        buffers[e.Table].Add((e.Row, 1L));
                    }

                    if (++sinceStep >= batchSize)
                    {
                        splitMs += Flush(q!, buffers, sw);
                        stepMs += Time(sw, q!.Step);
                        sinceStep = 0;
                    }
                }

                if (sinceStep > 0)
                {
                    splitMs += Flush(q!, buffers, sw);
                    stepMs += Time(sw, q!.Step);
                }

                // Gather: force the boundary decode by enumerating the output.
                sw.Restart();
                var rows = new List<(object?[] Values, long Weight)>();
                long nonZero = 0;
                foreach (var (values, weight) in q!.Current)
                {
                    rows.Add((values, weight));
                    if (weight != 0)
                    {
                        nonZero++;
                    }
                }

                sw.Stop();
                phases = (splitMs, stepMs, sw.Elapsed.TotalMilliseconds);
                outputRows = nonZero;
                return rows;
            }
        }
        finally
        {
            SpineJoinProbeMode.ForcePointProbe = false;
            SpineAggregateProbeMode.ForcePointProbe = false;
            SpineStagingConfig.Capacity = 0;
        }
    }

    private static double Flush(
        ParallelTypedCompiledQuery q, Dictionary<NexmarkTable, List<(object?[], long)>> buffers, Stopwatch sw)
    {
        sw.Restart();
        foreach (var (table, rows) in buffers)
        {
            if (rows.Count == 0)
            {
                continue;
            }

            q.Table(TableName(table)).Push(rows);
            rows.Clear();
        }

        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double Time(Stopwatch sw, Action action)
    {
        sw.Restart();
        action();
        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static Dictionary<string, long> Materialize(IReadOnlyList<(object?[] Values, long Weight)> rows)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in rows)
        {
            var key = string.Join("|", values.Select(v => v?.ToString() ?? "<null>"));
            map[key] = map.GetValueOrDefault(key) + weight;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static bool SameMultiset(Dictionary<string, long> a, Dictionary<string, long> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach (var (k, v) in a)
        {
            if (!b.TryGetValue(k, out var bv) || bv != v)
            {
                return false;
            }
        }

        return true;
    }

    private static string Label(Config config) => config switch
    {
        Config.Flat => "flat",
        Config.SpinePoint => "spine·point",
        Config.SpineMerge => "spine·merge",
        Config.SpinePointStaged => "spine·point·staged",
        _ => "spine·merge·staged",
    };

    private static string TableName(NexmarkTable t) => t switch
    {
        NexmarkTable.Person => "person",
        NexmarkTable.Auction => "auction",
        _ => "bid",
    };

    private static LogicalPlan BuildPlan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanOptimizer.Optimize(plan);
    }
}
