// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Single-core path A/B (docs/design-row-representation.md §21 scoping): the comparison's
/// "DbspNet 1c" column runs the <b>structural</b> single circuit
/// (<see cref="PlanToCircuit.Compile"/> → <c>CompiledQuery</c>: <c>object?[]</c> →
/// <c>StructuralRow</c> boundary + typed-inner, converting at scan/sink), whereas the
/// <b>typed W=1</b> path (<c>TypedPlanCompiler.TryCompileParallel(plan, 1)</c> →
/// <c>ParallelTypedCompiledQuery</c> with one worker) encodes <c>object?[]</c> →
/// <c>ZSet&lt;TRow&gt;</c> typed directly via the ingestor — i.e. it already does "typed
/// ingest". This measures how much of the single-core gap is that path choice (the prize a
/// single-circuit typed-ingest change would capture) vs a real per-row gap, by running the
/// SAME stream through both at W=1 and reporting typed/structural throughput.
/// </summary>
internal static class IngestPathBenchmark
{
    private static readonly string[] Queries =
        { "q0", "q1", "q2", "q22", "q3", "q20", "q4", "q9", "q18", "q19" };

    public static void Run(StringBuilder output, int totalEvents, int batchSize, int runs)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"=== single-core path A/B: structural vs typed-W=1 (events={totalEvents:N0}, " +
            $"batch={batchSize:N0}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        output.AppendLine("# DbspNet — single-core path A/B (structural vs typed W=1)");
        output.AppendLine();
        output.AppendLine(
            "The comparison's single-core column uses the **structural** `CompiledQuery` " +
            "(`object?[]`→`StructuralRow`→typed at the scan/sink). The **typed W=1** path " +
            "(`ParallelTypedCompiledQuery` at one worker) encodes `object?[]`→`ZSet<TRow>` " +
            "directly — typed ingest, already shipped on the parallel path. `typed/struct` is " +
            "the prize a single-circuit typed-ingest+egest change would capture for single-core.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, batch {batchSize:N0}, median of {runs} runs " +
            $"after one warmup. Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();
        output.AppendLine("| Query | structural ev/s | typed W=1 ev/s | typed/struct | struct out | typed out |");
        output.AppendLine("|:------|----------------:|---------------:|-------------:|-----------:|----------:|");

        foreach (var id in Queries)
        {
            var query = Array.Find(NexmarkQueries.All, q => q.Id == id);
            if (query is null)
            {
                continue;
            }

            var consumed = query.Tables.ToHashSet();
            try
            {
                var (sEvs, sOut) = MeasureStructural(query.Sql, events, consumed, batchSize, runs);
                var typed = MeasureTypedW1(query.Sql, events, consumed, batchSize, runs);
                if (typed is null)
                {
                    output.AppendLine($"| {id} | {sEvs:N0} | — (no W=1 parallel form) | — | {sOut:N0} | — |");
                    Console.WriteLine($"  {id,-4} struct {sEvs,12:N0} ev/s   typed: no W=1 parallel form");
                    continue;
                }

                var (tEvs, tOut) = typed.Value;
                var ratio = sEvs > 0 ? tEvs / sEvs : 0;
                output.AppendLine(
                    $"| {id} | {sEvs:N0} | {tEvs:N0} | {ratio:F2}× | {sOut:N0} | {tOut:N0} |");
                Console.WriteLine(
                    $"  {id,-4} struct {sEvs,12:N0}   typed {tEvs,12:N0} ev/s   typed/struct {ratio,5:F2}×" +
                    $"   out {sOut}/{tOut}");
            }
            catch (Exception ex)
            {
                var msg = ex.Message.Replace('\n', ' ').Replace('\r', ' ');
                output.AppendLine($"| {id} | FAILED | {msg} | | | |");
                Console.WriteLine($"  {id}: FAILED — {msg}");
            }
        }

        output.AppendLine();
        output.AppendLine(
            "**Reading it.** `typed/struct` > 1 means the structural single circuit is leaving " +
            "single-core throughput on the table that a typed-ingest+egest single circuit would " +
            "recover — i.e. that much of the single-core gap is a path choice, not a per-row floor.");
        output.AppendLine();
    }

    private static LogicalPlan BuildPlan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in NexmarkQueries.Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanOptimizer.Optimize(plan);
    }

    private static (double EventsPerSec, long OutputRows) MeasureStructural(
        string sql, List<Event> events, HashSet<NexmarkTable> consumed, int batchSize, int runs)
    {
        var times = new List<double>();
        long outRows = 0;
        for (var run = 0; run < runs + 1; run++)
        {
            var q = PlanToCircuit.Compile(BuildPlan(sql));
            var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>());
            var sw = Stopwatch.StartNew();
            var since = 0;
            foreach (var e in events)
            {
                if (consumed.Contains(e.Table))
                {
                    buffers[e.Table].Add((e.Row, 1L));
                }

                if (++since >= batchSize)
                {
                    foreach (var (t, list) in buffers)
                    {
                        if (list.Count > 0)
                        {
                            q.Table(TableName(t)).Push(list);
                            list.Clear();
                        }
                    }

                    q.Step();
                    since = 0;
                }
            }

            foreach (var (t, list) in buffers)
            {
                if (list.Count > 0)
                {
                    q.Table(TableName(t)).Push(list);
                }
            }

            q.Step();
            sw.Stop();
            if (run > 0)
            {
                times.Add(events.Count / sw.Elapsed.TotalSeconds);
            }

            outRows = q.Current.Count(kv => kv.Value.Value != 0);
        }

        times.Sort();
        return (times[times.Count / 2], outRows);
    }

    private static (double EventsPerSec, long OutputRows)? MeasureTypedW1(
        string sql, List<Event> events, HashSet<NexmarkTable> consumed, int batchSize, int runs)
    {
        if (!TypedPlanCompiler.TryCompileParallel(BuildPlan(sql), 1, out var probe))
        {
            return null;
        }

        probe!.Dispose();

        var times = new List<double>();
        long outRows = 0;
        for (var run = 0; run < runs + 1; run++)
        {
            if (!TypedPlanCompiler.TryCompileParallel(BuildPlan(sql), 1, out var q))
            {
                return null;
            }

            using (q)
            {
                var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>());
                var sw = Stopwatch.StartNew();
                var since = 0;
                foreach (var e in events)
                {
                    if (consumed.Contains(e.Table))
                    {
                        buffers[e.Table].Add((e.Row, 1L));
                    }

                    if (++since >= batchSize)
                    {
                        foreach (var (t, list) in buffers)
                        {
                            if (list.Count > 0)
                            {
                                q!.Table(TableName(t)).Push(list);
                                list.Clear();
                            }
                        }

                        q!.Step();
                        since = 0;
                    }
                }

                foreach (var (t, list) in buffers)
                {
                    if (list.Count > 0)
                    {
                        q!.Table(TableName(t)).Push(list);
                    }
                }

                q!.Step();
                sw.Stop();
                if (run > 0)
                {
                    times.Add(events.Count / sw.Elapsed.TotalSeconds);
                }

                outRows = MaterializeCount(q!);
            }
        }

        times.Sort();
        return (times[times.Count / 2], outRows);
    }

    private static long MaterializeCount(ParallelTypedCompiledQuery q)
    {
        long n = 0;
        foreach (var (_, w) in q.Current)
        {
            if (w != 0)
            {
                n++;
            }
        }

        return n;
    }

    private static string TableName(NexmarkTable t) => t switch
    {
        NexmarkTable.Person => "person",
        NexmarkTable.Auction => "auction",
        _ => "bid",
    };
}
