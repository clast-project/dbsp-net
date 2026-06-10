// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Benchmarks.Nexmark;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// W=1 per-row execution-cost profiler (docs/design-row-representation.md §16).
/// Isolates *per-tuple* efficiency from parallelism by running each Nexmark
/// query through a single (non-parallel) circuit and reporting, per stream
/// event: wall-clock nanoseconds, managed bytes allocated, and GC collections.
///
/// <para>The exchange/scaling arc (§15) concluded that the residual Feldera
/// gaps on q4/q18/q19 are NOT scaling gaps — they are per-tuple execution cost.
/// This harness measures that cost directly. Allocation is captured with
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/>, which is accurate at W=1
/// because a single circuit's <c>Step()</c> runs synchronously on the calling
/// thread (no worker threads allocate). The differential between queries
/// (q0 = ingest/egest boundary only; q1 = +1 map; q2 = +filter; q4/q9/q20 =
/// +join/+aggregate; q18/q19 = +partitioned TOP-K) attributes per-row ns and
/// bytes to operator classes without a sampling profiler.</para>
/// </summary>
internal static class W1ProfileBenchmark
{
    private sealed record Row(
        string Id, string Shape, double NsPerEvent, double BytesPerEvent,
        long Gen0, long Gen1, long Gen2, long OutputRows);

    // Queries ordered as a differential ladder: each adds one operator class
    // over a lighter neighbour so the per-event delta is attributable.
    private static readonly (string Id, string Shape)[] Targets =
    {
        ("q0", "passthrough (ingest+egest boundary)"),
        ("q1", "+ 1 projection delegate (price map)"),
        ("q2", "+ filter (auction % 123 = 0)"),
        ("q22", "+ 3 string SPLIT_INDEX projections"),
        ("q3", "join (auction ⋈ person, filtered)"),
        ("q20", "join (bid ⋈ auction, wide output)"),
        ("q4", "join + nested MAX + outer AVG"),
        ("q9", "join + partitioned TOP-1"),
        ("q18", "partitioned TOP-1 dedup"),
        ("q19", "partitioned TOP-10"),
    };

    public static void Run(StringBuilder output, int totalEvents, int batchSize, int runs)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"=== W=1 per-row cost profile (events={totalEvents:N0}, batch={batchSize:N0}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);
        var counts = (
            Person: events.Count(e => e.Table == NexmarkTable.Person),
            Auction: events.Count(e => e.Table == NexmarkTable.Auction),
            Bid: events.Count(e => e.Table == NexmarkTable.Bid));
        Console.WriteLine($"  person={counts.Person:N0}  auction={counts.Auction:N0}  bid={counts.Bid:N0}");

        var rows = new List<Row>();
        foreach (var (id, shape) in Targets)
        {
            var query = NexmarkQueries.All.FirstOrDefault(q => q.Id == id);
            if (query is null)
            {
                continue;
            }

            try
            {
                var r = MeasureOne(query, shape, events, batchSize, runs);
                rows.Add(r);
                Console.WriteLine(
                    $"  {id,-4} {r.NsPerEvent,9:F1} ns/ev  {r.BytesPerEvent,9:F0} B/ev  " +
                    $"gc0/1/2={r.Gen0}/{r.Gen1}/{r.Gen2}  out={r.OutputRows:N0}  ({shape})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  {id}: FAILED — {ex.Message}");
            }
        }

        WriteReport(output, totalEvents, batchSize, runs, counts, rows);
    }

    private static Row MeasureOne(
        NexmarkQueries.Query query, string shape, List<Event> events, int batchSize, int runs)
    {
        var consumed = query.Tables.ToHashSet();

        // Throughput: median ns/event over `runs` timed passes after one warmup.
        var nsPerEvent = new List<double>();
        long outputRows = 0;
        for (var run = 0; run < runs + 1; run++)
        {
            var q = Compile(query.Sql);
            var elapsed = FeedStream(q, events, consumed, batchSize);
            if (run > 0)
            {
                nsPerEvent.Add(elapsed.TotalNanoseconds() / events.Count);
            }

            outputRows = CountOutput(q);
        }

        nsPerEvent.Sort();

        // Allocation + GC: a single dedicated pass on a fresh circuit. The
        // calling thread's allocation delta is the engine's per-event managed
        // allocation (buffer reuse keeps the harness's own allocation ~0 after
        // the first batch). Allocation is deterministic enough that one pass is
        // representative; GC counts are reported per the whole stream.
        var qAlloc = Compile(query.Sql);
        // Prime one batch so buffer/list backing arrays are already grown and
        // not counted as per-event allocation.
        FeedStream(qAlloc, events.Take(Math.Min(batchSize * 2, events.Count)).ToList(), consumed, batchSize);

        var qMeasure = Compile(query.Sql);
        var g0 = GC.CollectionCount(0);
        var g1 = GC.CollectionCount(1);
        var g2 = GC.CollectionCount(2);
        var bytes0 = GC.GetAllocatedBytesForCurrentThread();
        FeedStream(qMeasure, events, consumed, batchSize);
        var bytes1 = GC.GetAllocatedBytesForCurrentThread();

        var bytesPerEvent = (bytes1 - bytes0) / (double)events.Count;
        return new Row(
            query.Id, shape, nsPerEvent[nsPerEvent.Count / 2], bytesPerEvent,
            GC.CollectionCount(0) - g0, GC.CollectionCount(1) - g1, GC.CollectionCount(2) - g2,
            outputRows);
    }

    /// <summary>Feed the whole stream in micro-batches; return elapsed time.</summary>
    private static TimeSpan FeedStream(
        CompiledQuery q, List<Event> events, HashSet<NexmarkTable> consumed, int batchSize)
    {
        var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>(batchSize));
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
        return sw.Elapsed;
    }

    private static void Flush(CompiledQuery q, Dictionary<NexmarkTable, List<(object?[], long)>> buffers)
    {
        foreach (var (table, list) in buffers)
        {
            if (list.Count == 0)
            {
                continue;
            }

            q.Table(TableName(table)).Push(list);
            list.Clear();
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

    private static string TableName(NexmarkTable t) => t switch
    {
        NexmarkTable.Person => "person",
        NexmarkTable.Auction => "auction",
        _ => "bid",
    };

    private static CompiledQuery Compile(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in NexmarkQueries.Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
    }

    private static void WriteReport(
        StringBuilder o, int totalEvents, int batchSize, int runs,
        (long Person, long Auction, long Bid) counts, List<Row> rows)
    {
        o.AppendLine("## W=1 per-row execution cost");
        o.AppendLine();
        o.AppendLine(
            "Per-tuple efficiency, isolated from parallelism by running each query " +
            "through a single (non-parallel) circuit. The exchange/scaling arc (§15) " +
            "ruled out scaling as the cause of the residual q4/q18/q19 Feldera gaps; " +
            "this measures the per-tuple cost that remains. `ns/ev` and `B/ev` are " +
            "*per stream event* (the whole 1:3:46 Person:Auction:Bid stream is the " +
            "denominator, so a query reading only `bid` does ~92% of the stream as " +
            "real work, while a join also reading `auction` reads ~6%).");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {totalEvents:N0} events ({counts.Person:N0} person, " +
            $"{counts.Auction:N0} auction, {counts.Bid:N0} bid), batch {batchSize:N0}, " +
            $"median of {runs} runs. Allocation via `GC.GetAllocatedBytesForCurrentThread` " +
            $"(accurate at W=1). Host: .NET {Environment.Version}, " +
            $"{Environment.ProcessorCount} cores, Server GC.");
        o.AppendLine();
        o.AppendLine("| Query | Shape | ns/event | B/event | GC 0/1/2 | out rows |");
        o.AppendLine("|:------|:------|---------:|--------:|:---------|---------:|");
        foreach (var r in rows)
        {
            o.AppendLine(
                $"| {r.Id} | {r.Shape} | {r.NsPerEvent:F1} | {r.BytesPerEvent:F0} | " +
                $"{r.Gen0}/{r.Gen1}/{r.Gen2} | {r.OutputRows:N0} |");
        }

        o.AppendLine();
    }

    // Stopwatch.Elapsed.TotalNanoseconds isn't available on all targets; compute it.
    private static double TotalNanoseconds(this TimeSpan ts) => ts.Ticks * 100.0;
}
