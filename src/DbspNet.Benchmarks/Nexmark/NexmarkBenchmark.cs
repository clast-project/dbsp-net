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
    public static void Run(StringBuilder output, int totalEvents, int batchSize, int runs)
    {
        Console.WriteLine();
        Console.WriteLine($"=== Nexmark throughput (events={totalEvents:N0}, batch={batchSize:N0}, runs={runs}) ===");
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
        output.AppendLine(
            $"Stream: {totalEvents:N0} events " +
            $"({counts.Person:N0} person, {counts.Auction:N0} auction, {counts.Bid:N0} bid). " +
            $"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();
        output.AppendLine("| Query | Description | Throughput (events/s) | Last Δ rows | Status |");
        output.AppendLine("|:------|:------------|----------------------:|------------:|:-------|");

        foreach (var query in NexmarkQueries.All)
        {
            RunOne(output, query, events, batchSize, runs);
        }

        output.AppendLine();
        output.AppendLine(
            "> *Last Δ rows* is the size of the output change-set emitted by the " +
            "final micro-batch (a smoke-test that the query produces output), not " +
            "the full materialized view size.");
        output.AppendLine(">");
        output.AppendLine(
            "> Queries q5 / q7 / q8 (tumbling / sliding event-time windows) are " +
            "omitted: they require TUMBLE / HOP windowing table functions that " +
            "DbspNet does not yet expose. q9 uses `ROW_NUMBER() OVER (PARTITION … " +
            "ORDER …)` which compiles to a partitioned incremental TOP-K.");
        output.AppendLine();
    }

    private static void RunOne(
        StringBuilder output, NexmarkQueries.Query query, List<Event> events, int batchSize, int runs)
    {
        CompiledQuery? probe;
        try
        {
            probe = Compile(NexmarkQueries.Ddl, query.Sql);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  {query.Id}: DID NOT COMPILE — {Short(ex)}");
            output.AppendLine(
                $"| {query.Id} | {query.Description} | — | — | not compiled: {Short(ex)} |");
            return;
        }

        _ = probe; // compile probe discarded; each measured run compiles fresh.

        var consumed = query.Tables.ToHashSet();
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
                    Flush(q, buffers);
                    q.Step();
                    sinceStep = 0;
                }
            }

            if (sinceStep > 0)
            {
                Flush(q, buffers);
                q.Step();
            }

            sw.Stop();

            if (run > 0)
            {
                times.Add(events.Count / sw.Elapsed.TotalSeconds);
            }

            outputRows = CountOutput(q);
        }

        times.Sort();
        var median = times[times.Count / 2];
        Console.WriteLine(
            $"  {query.Id}: {median,12:N0} events/s   lastΔ={outputRows:N0}");
        output.AppendLine(
            $"| {query.Id} | {query.Description} | {median:N0} | {outputRows:N0} | ok |");
    }

    private static void Flush(CompiledQuery q, Dictionary<NexmarkTable, List<(object?[], long)>> buffers)
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

    private static long CountOutput(CompiledQuery q)
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

    private static CompiledQuery Compile(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
    }
}
