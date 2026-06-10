// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks.Nexmark;

/// <summary>
/// Throughput benchmark over the Nexmark workload, mirroring Feldera's
/// primary published benchmark. Each query is compiled independently, then
/// the full generated event stream is fed in fixed-size micro-batches
/// (push + <c>Step()</c> per batch) and we report sustained events/sec over
/// the whole stream — the standard Nexmark throughput metric.
/// </summary>
internal static class NexmarkBenchmark
{
    public static void Run(StringBuilder output, int totalEvents, int batchSize, int runs, int workers = 1)
    {
        workers = Math.Clamp(workers, 1, Environment.ProcessorCount);
        Console.WriteLine();
        Console.WriteLine(
            $"=== Nexmark throughput (events={totalEvents:N0}, batch={batchSize:N0}, runs={runs}, W={workers}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        var counts = CountByTable(events);
        Console.WriteLine(
            $"  person={counts.Person:N0}  auction={counts.Auction:N0}  bid={counts.Bid:N0}");

        output.AppendLine("## Nexmark throughput");
        output.AppendLine();
        output.AppendLine(
            "Feldera's primary published benchmark. An online-auction event stream " +
            "(Person / Auction / Bid in the standard 1 : 3 : 46 ratio) is fed in " +
            $"{batchSize:N0}-event micro-batches; each batch is pushed and `Step()`-ed. " +
            "Throughput is **total stream events** ÷ wall-clock, the median of " +
            $"{runs} run(s) after one warmup. This is the *cold-stream* number " +
            "(every event is genuinely new); DbspNet's incremental edge shows up " +
            "instead in the per-event latency benchmarks. Note the denominator is " +
            "always the whole 1 : 3 : 46 stream, so a query that only reads a subset " +
            "of tables (e.g. q3 reads auction + person, skipping the 92% bid " +
            "majority) reports a higher events/s — it is keeping up with that much " +
            "stream rate, not doing that much per-row work.");
        output.AppendLine();
        if (workers > 1)
        {
            output.AppendLine(
                $"**Parallel** runs each query across W={workers} data-parallel replicas " +
                "(`ParallelCircuit`, hash-sharded input + exchanges at join / group-by / " +
                "partitioned-TOP-K boundaries). The W>1 output is cross-checked against the " +
                "W=1 replica run; a query whose plan has no correct parallel form (e.g. a " +
                "global TOP-K) is marked *single-only*. Feldera-style comparison: pin W to " +
                "Feldera's worker count.");
            output.AppendLine();
        }

        output.AppendLine(
            $"Stream: {totalEvents:N0} events " +
            $"({counts.Person:N0} person, {counts.Auction:N0} auction, {counts.Bid:N0} bid). " +
            $"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();
        if (workers > 1)
        {
            output.AppendLine(
                $"| Query | Description | W=1 (events/s) | W={workers} (events/s) | Speedup | Last Δ rows | Status |");
            output.AppendLine(
                "|:------|:------------|---------------:|---------------:|--------:|------------:|:-------|");
        }
        else
        {
            output.AppendLine("| Query | Description | Throughput (events/s) | Last Δ rows | Status |");
            output.AppendLine("|:------|:------------|----------------------:|------------:|:-------|");
        }

        foreach (var query in NexmarkQueries.All)
        {
            RunOne(output, query, events, batchSize, runs, workers);
        }

        // Emit the declared capability gaps as explicit rows, so a side-by-side
        // comparison shows *why* DbspNet has no number here rather than a bare
        // "n/a" that reads as "the runner skipped it".
        foreach (var u in NexmarkQueries.NotSupported)
        {
            ReportUnsupported(output, u, workers);
        }

        output.AppendLine();
        output.AppendLine(
            "> *Last Δ rows* is the size of the output change-set emitted by the " +
            "final micro-batch (a smoke-test that the query produces output), not " +
            "the full materialized view size.");
        output.AppendLine(">");
        output.AppendLine(
            "> The `unsupported` rows (q5 / q7 / q8 / q11 / q12 — tumbling / sliding " +
            "/ session event-time and processing-time windows) require TUMBLE / HOP " +
            "/ SESSION windowing table functions that DbspNet does not yet expose; " +
            "they are listed explicitly so a Feldera comparison shows a declared gap, " +
            "not a silent omission. Among the queries that do run: q9 / q18 / q19 use " +
            "`ROW_NUMBER() OVER (PARTITION … ORDER …)` → a partitioned incremental " +
            "TOP-K (and, in parallel, an exchange on the partition key); q20 is a " +
            "filtered bid ⋈ auction join; q22 splits the bid URL with `SPLIT_INDEX`; " +
            "q15 / q16 / q17 (per-day / per-channel / per-auction statistics with " +
            "`COUNT(DISTINCT …)` and conditional `SUM(CASE …)` counts over a " +
            "`CAST(date_time AS DATE)` group key) compile but have no parallel form " +
            "today, so they run single-only.");
        output.AppendLine();
    }

    private static void ReportUnsupported(StringBuilder output, NexmarkQueries.Unsupported u, int workers)
    {
        Console.WriteLine($"  {u.Id}: unsupported — {u.Reason}");
        var blanks = workers > 1 ? "— | — | — | —" : "— | —";
        output.AppendLine($"| {u.Id} | {u.Description} | {blanks} | unsupported ({u.Reason}) |");
    }

    private static void RunOne(
        StringBuilder output, NexmarkQueries.Query query, List<Event> events, int batchSize, int runs, int workers)
    {
        try
        {
            _ = Compile(NexmarkQueries.Ddl, query.Sql); // probe; measured runs compile fresh.
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  {query.Id}: DID NOT COMPILE — {Short(ex)}");
            var cols = workers > 1 ? "— | — | — | —" : "— | —";
            output.AppendLine($"| {query.Id} | {query.Description} | {cols} | not compiled: {Short(ex)} |");
            return;
        }

        var consumed = query.Tables.ToHashSet();
        var (single, outputRows) = MeasureSingle(query, events, batchSize, runs, consumed);

        if (workers <= 1)
        {
            Console.WriteLine($"  {query.Id}: {single,12:N0} events/s   lastΔ={outputRows:N0}");
            output.AppendLine($"| {query.Id} | {query.Description} | {single:N0} | {outputRows:N0} | ok |");
            return;
        }

        // Parallel: attempt to compile at W; if the plan has no correct parallel
        // form, TryCompileParallel returns false and we report single-only.
        var parallel = MeasureParallel(query, events, batchSize, runs, workers, consumed);
        if (parallel is null)
        {
            Console.WriteLine($"  {query.Id}: {single,12:N0} events/s  (single-only)");
            output.AppendLine(
                $"| {query.Id} | {query.Description} | {single:N0} | — | — | {outputRows:N0} | single-only (no parallel plan) |");
            return;
        }

        var (par, parOutput, correct) = parallel.Value;
        var speedup = par > 0 ? par / single : 0.0;
        var status = correct ? "ok" : "**PARALLEL MISMATCH**";
        Console.WriteLine(
            $"  {query.Id}: W1={single,12:N0}  W{workers}={par,12:N0} events/s  " +
            $"{BenchmarkHarness.FormatRatio(speedup)}  {(correct ? "ok" : "MISMATCH")}");
        output.AppendLine(
            $"| {query.Id} | {query.Description} | {single:N0} | {par:N0} | " +
            $"{BenchmarkHarness.FormatRatio(speedup).Trim()} | {parOutput:N0} | {status} |");
    }

    /// <summary>Single-circuit throughput (events/s) + final output row count.</summary>
    private static (double EventsPerSec, long OutputRows) MeasureSingle(
        NexmarkQueries.Query query, List<Event> events, int batchSize, int runs, HashSet<NexmarkTable> consumed)
    {
        var times = new List<double>();
        long outputRows = 0;
        for (var run = 0; run < runs + 1; run++) // first run is warmup.
        {
            var q = Compile(NexmarkQueries.Ddl, query.Sql);
            var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>());

            var sw = Stopwatch.StartNew();
            var sinceStep = 0;
            foreach (var e in events)
            {
                if (consumed.Contains(e.Table))
                {
                    buffers[e.Table].Add((e.Row, 1L));
                }

                if (++sinceStep >= batchSize)
                {
                    FlushSingle(q, buffers);
                    q.Step();
                    sinceStep = 0;
                }
            }

            if (sinceStep > 0)
            {
                FlushSingle(q, buffers);
                q.Step();
            }

            sw.Stop();
            if (run > 0)
            {
                times.Add(events.Count / sw.Elapsed.TotalSeconds);
            }

            outputRows = CountSingleOutput(q);
        }

        times.Sort();
        return (times[times.Count / 2], outputRows);
    }

    /// <summary>
    /// Parallel throughput (events/s) at W workers, the final output row count, and
    /// whether that output matched the W=1 replica run. Returns <c>null</c> when the
    /// query has no parallel plan (TryCompileParallel fails).
    /// </summary>
    private static (double EventsPerSec, long OutputRows, bool Correct)? MeasureParallel(
        NexmarkQueries.Query query, List<Event> events, int batchSize, int runs, int workers,
        HashSet<NexmarkTable> consumed)
    {
        var plan = BuildPlan(NexmarkQueries.Ddl, query.Sql);
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            return null;
        }

        probe!.Dispose();

        // Reference: W=1 replica output (apples-to-apples with the W>1 row shape).
        var reference = RunParallelStream(plan, 1, events, batchSize, consumed, out _);

        var times = new List<double>();
        long outputRows = 0;
        var correct = true;
        for (var run = 0; run < runs + 1; run++)
        {
            var output = RunParallelStream(plan, workers, events, batchSize, consumed, out var elapsedSec);
            if (run > 0)
            {
                times.Add(events.Count / elapsedSec);
            }

            outputRows = output.Values.Count(w => w != 0);
            correct = SameMultiset(reference, output);
        }

        times.Sort();
        return (times[times.Count / 2], outputRows, correct);
    }

    /// <summary>
    /// Runs the whole event stream through a fresh W-worker parallel circuit and
    /// returns its final materialized output (row-string → net weight). Reports the
    /// wall-clock seconds via <paramref name="elapsedSec"/>.
    /// </summary>
    private static Dictionary<string, long> RunParallelStream(
        LogicalPlan plan, int workers, List<Event> events, int batchSize,
        HashSet<NexmarkTable> consumed, out double elapsedSec)
    {
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q))
        {
            throw new InvalidOperationException("parallel compile unexpectedly failed");
        }

        using (q)
        {
            var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>());
            var sw = Stopwatch.StartNew();
            var sinceStep = 0;
            foreach (var e in events)
            {
                if (consumed.Contains(e.Table))
                {
                    buffers[e.Table].Add((e.Row, 1L));
                }

                if (++sinceStep >= batchSize)
                {
                    FlushParallel(q!, buffers);
                    q!.Step();
                    sinceStep = 0;
                }
            }

            if (sinceStep > 0)
            {
                FlushParallel(q!, buffers);
                q!.Step();
            }

            sw.Stop();
            elapsedSec = sw.Elapsed.TotalSeconds;
            return MaterializeParallel(q!);
        }
    }

    private static void FlushSingle(CompiledQuery q, Dictionary<NexmarkTable, List<(object?[], long)>> buffers)
    {
        foreach (var (table, rows) in buffers)
        {
            if (rows.Count == 0)
            {
                continue;
            }

            q.Table(TableName(table)).Push(rows);
            rows.Clear();
        }
    }

    private static void FlushParallel(
        ParallelTypedCompiledQuery q, Dictionary<NexmarkTable, List<(object?[], long)>> buffers)
    {
        foreach (var (table, rows) in buffers)
        {
            if (rows.Count == 0)
            {
                continue;
            }

            q.Table(TableName(table)).Push(rows);
            rows.Clear();
        }
    }

    private static long CountSingleOutput(CompiledQuery q)
    {
        long n = 0;
        foreach (var (_, w) in q.Current)
        {
            if (w.Value != 0)
            {
                n++;
            }
        }

        return n;
    }

    private static Dictionary<string, long> MaterializeParallel(ParallelTypedCompiledQuery q)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in q.Current)
        {
            var key = string.Join("", values.Select(v => v?.ToString() ?? " "));
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

    private static (long Person, long Auction, long Bid) CountByTable(List<Event> events)
    {
        long p = 0, a = 0, b = 0;
        foreach (var e in events)
        {
            switch (e.Table)
            {
                case NexmarkTable.Person: p++; break;
                case NexmarkTable.Auction: a++; break;
                default: b++; break;
            }
        }

        return (p, a, b);
    }

    private static string TableName(NexmarkTable t) => t switch
    {
        NexmarkTable.Person => "person",
        NexmarkTable.Auction => "auction",
        _ => "bid",
    };

    private static string Short(Exception ex)
    {
        var msg = ex.Message.Replace('\n', ' ').Replace('\r', ' ');
        return msg.Length > 120 ? msg[..120] + "…" : msg;
    }

    private static CompiledQuery Compile(string[] ddl, string sql) =>
        PlanToCircuit.Compile(BuildPlan(ddl, sql));

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
