// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Shared machinery for the spine parallel-pipeline benchmarks
/// (<see cref="Q4SpineBenchmark"/> and <see cref="SpineEvalBenchmark"/>): compile
/// a query's <see cref="LogicalPlan"/> as a <c>W</c>-replica parallel circuit in
/// a knob-driven configuration, stream the Nexmark event set through it in
/// micro-batches, and time the <b>step</b> phase apart from ingest (split) and
/// egest (gather). A <see cref="RunConfig"/> captures the three orthogonal knobs
/// — trace family / compile options, the merge-vs-point probe, and the memtable
/// staging capacity — so callers can express flat, spine·point, spine·merge, and
/// their staged variants without the harness knowing the labels.
/// </summary>
internal static class SpineParallelHarness
{
    /// <summary>The knobs that define a measured configuration.</summary>
    /// <param name="Options">
    /// Compile options — trace family AND the memtable capacity
    /// (<see cref="CompileOptions.SpineStagingCapacity"/>), which the compiler
    /// realises at trace construction (docs §11). 0 disables the memtable.
    /// </param>
    /// <param name="ForcePointProbe">Force the per-key point probe (vs the batched merge).</param>
    internal readonly record struct RunConfig(CompileOptions Options, bool ForcePointProbe);

    internal static RunConfig Flat => new(CompileOptions.Default, false);

    internal static RunConfig Spine(bool forcePointProbe, int stagingCapacity) =>
        new(new CompileOptions { TraceFamily = TraceFamily.Spine, SpineStagingCapacity = stagingCapacity }, forcePointProbe);

    internal static LogicalPlan BuildPlan(string[] ddl, string sql)
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

    internal static string TableName(NexmarkTable t) => t switch
    {
        NexmarkTable.Person => "person",
        NexmarkTable.Auction => "auction",
        _ => "bid",
    };

    /// <summary>
    /// Median of <paramref name="runs"/> full-stream passes (after one warmup)
    /// for one config: the median split / step / gather wall-clock and the final
    /// materialised output of the last pass (for cross-checking against flat).
    /// </summary>
    internal static (double SplitMs, double StepMs, double GatherMs, long OutputRows, IReadOnlyList<(object?[] Values, long Weight)> Result)
        MeasureMedian(
            LogicalPlan plan, int workers, RunConfig config, List<Event> events,
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
    /// One full-stream pass through a fresh parallel circuit in the given config.
    /// Accumulates split (push) and step time over every micro-batch and times the
    /// final gather (materialise) once. The probe-mode and staging knobs are
    /// global statics read on the worker threads / at trace construction; set
    /// before the run and cleared after.
    /// </summary>
    private static List<(object?[] Values, long Weight)> RunStream(
        LogicalPlan plan, int workers, RunConfig config, List<Event> events,
        HashSet<NexmarkTable> consumed, int batchSize, out (double SplitMs, double StepMs, double GatherMs) phases,
        out long outputRows)
    {
        // ForcePointProbe is read on the worker threads every Step, so it must be
        // set for the whole run. The memtable capacity is carried in config.Options
        // and applied by the compiler at trace construction (docs §11), so the
        // harness no longer sets the staging seam directly.
        SpineJoinProbeMode.ForcePointProbe = config.ForcePointProbe;
        SpineAggregateProbeMode.ForcePointProbe = config.ForcePointProbe;
        try
        {
            if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q, snapshotCodecs: null, config.Options))
            {
                throw new InvalidOperationException("parallel compile failed");
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

    /// <summary>Folds a result delta set into a key→weight multiset for cross-checking.</summary>
    internal static Dictionary<string, long> Materialize(IReadOnlyList<(object?[] Values, long Weight)> rows)
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

    internal static bool SameMultiset(Dictionary<string, long> a, Dictionary<string, long> b)
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
}
