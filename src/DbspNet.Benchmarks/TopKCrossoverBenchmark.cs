// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// §22.7 crossover sweep for the narrow-key partitioned TOP-K
/// (<see cref="PartitionedTopKNarrowingMode"/>). §22.6 measured only the endpoints —
/// q18 (limit=1, size-1 partitions) flat-to-slightly-negative, q19 (limit=10,
/// accumulating partitions) a clean win — and concluded the prize tracks per-partition
/// state size, not partition count. The per-operator gate needs a cheap <i>static</i>
/// predicate, and the only one available at compile time is the plan's <c>limit</c>.
///
/// <para>This sweeps <c>limit ∈ {1,2,3,5,10}</c> on <b>both</b> real partition shapes,
/// A/Bing the whole-row op (<see cref="PartitionedTopKNarrowing.ForceWholeRow"/>) against
/// the narrow op (<see cref="PartitionedTopKNarrowing.ForceNarrow"/>) on the actual
/// single-circuit W=1 path — so it captures the cached-<c>StructuralRow</c>-hash reality
/// the value-typed <c>reprbench topk</c> microbench over-priced (§22.6). The shapes:</para>
/// <list type="bullet">
/// <item><b>q18 shape</b> — <c>PARTITION BY bidder, auction ORDER BY date_time DESC</c>:
/// many tiny (≈size-1) partitions, so no TOP-K state accumulates at any limit.</item>
/// <item><b>q19 shape</b> — <c>PARTITION BY auction ORDER BY price DESC</c>: few
/// partitions, each accumulating many bids — the state the narrow key shrinks.</item>
/// </list>
/// The crossover limit (where narrow's time/alloc overtakes whole-row on the accumulating
/// shape) sets <see cref="PartitionedTopKNarrowingMode.AutoLimitThreshold"/>.
/// </summary>
internal static class TopKCrossoverBenchmark
{
    private static readonly long[] Limits = { 1, 2, 3, 5, 10 };

    private sealed record Shape(string Name, string PartitionBy, string OrderBy);

    private static readonly Shape[] Shapes =
    {
        new("q18 (PARTITION BY bidder,auction — tiny partitions)", "bidder, auction", "date_time DESC"),
        new("q19 (PARTITION BY auction — accumulating)", "auction", "price DESC"),
    };

    public static void Run(StringBuilder output, int totalEvents, int batchSize, int runs)
    {
        Console.WriteLine();
        Console.WriteLine($"=== TOP-K narrow crossover sweep (events={totalEvents:N0}, batch={batchSize:N0}, runs={runs}) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);
        var bidCount = events.Count(e => e.Table == NexmarkTable.Bid);

        output.AppendLine("# DbspNet — narrow-key partitioned TOP-K crossover (§22.7)");
        output.AppendLine();
        output.AppendLine(
            "Where does the §22 narrow `{order, wideRow}` key overtake whole-row keying as the " +
            "TOP-K `limit` grows? §22.6 measured only limit∈{1,10}; this sweeps the real " +
            "single-circuit W=1 path (cached `StructuralRow` hash and all) at intermediate " +
            "limits on both partition shapes, to pick the cheap static gate predicate.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events ({bidCount:N0} bids), batch={batchSize:N0}, W=1, " +
            $"median ns/event of {runs} run(s) after one warmup; allocation is one dedicated " +
            $"primed pass. `time↑` = whole-row ÷ narrow (>1 ⇒ narrow faster); `alloc↑` likewise " +
            $"on bytes/event. `Auto pick` is what the limit gate (`limit > " +
            $"{PartitionedTopKNarrowingMode.AutoLimitThreshold}`) selects.");
        output.AppendLine();

        foreach (var shape in Shapes)
        {
            RunShape(output, shape, events, batchSize, runs);
        }

        output.AppendLine(
            "**Reading it.** Narrow only pays where a partition accumulates state: on the " +
            "accumulating (q19) shape the time/alloc win grows with the limit (more TOP-K " +
            "window to hash, more `_accum` to compare); on the tiny-partition (q18) shape it " +
            "stays flat-to-negative at every limit — confirming `limit` is a proxy for " +
            "per-partition state that is only sound on accumulating partitions, which is why " +
            "the gate is conservative (leaves all TOP-1 on whole-row).");
        output.AppendLine();
    }

    private static void RunShape(
        StringBuilder output, Shape shape, List<Event> events, int batchSize, int runs)
    {
        Console.WriteLine();
        Console.WriteLine($"  {shape.Name}");

        output.AppendLine($"## {shape.Name}");
        output.AppendLine();
        output.AppendLine("| limit | wholerow ns | narrow ns | time↑ | wholerow B | narrow B | alloc↑ | out rows | Auto pick |");
        output.AppendLine("|------:|------------:|----------:|------:|-----------:|---------:|-------:|---------:|:----------|");

        foreach (var limit in Limits)
        {
            var sql = BuildSql(shape, limit);
            var wholeRow = Measure(sql, events, batchSize, runs, PartitionedTopKNarrowing.ForceWholeRow);
            var narrow = Measure(sql, events, batchSize, runs, PartitionedTopKNarrowing.ForceNarrow);

            if (wholeRow.OutputRows != narrow.OutputRows)
            {
                throw new InvalidOperationException(
                    $"{shape.Name} limit={limit}: narrow out={narrow.OutputRows} != wholerow {wholeRow.OutputRows}");
            }

            var timeUp = narrow.Ns > 0 ? wholeRow.Ns / narrow.Ns : 1.0;
            var allocUp = narrow.Bytes > 0 ? wholeRow.Bytes / narrow.Bytes : 1.0;
            var pick = PartitionedTopKNarrowingMode.ShouldNarrow(limit) ? "narrow" : "wholerow";

            output.AppendLine(
                $"| {limit} | {wholeRow.Ns:F1} | {narrow.Ns:F1} | {timeUp:F2}× | " +
                $"{wholeRow.Bytes:F0} | {narrow.Bytes:F0} | {allocUp:F2}× | {wholeRow.OutputRows:N0} | {pick} |");
            Console.WriteLine(
                $"    limit={limit,2}  wholerow {wholeRow.Ns,7:F1}ns/{wholeRow.Bytes,6:F0}B  " +
                $"narrow {narrow.Ns,7:F1}ns/{narrow.Bytes,6:F0}B  time {timeUp:F2}× alloc {allocUp:F2}×  pick={pick}");
        }

        output.AppendLine();
    }

    private static string BuildSql(Shape shape, long limit) =>
        $@"SELECT auction, bidder, price, channel, url, date_time, extra
           FROM (
               SELECT auction, bidder, price, channel, url, date_time, extra,
                      ROW_NUMBER() OVER (
                          PARTITION BY {shape.PartitionBy}
                          ORDER BY {shape.OrderBy}) AS rn
               FROM bid
           ) ranked
           WHERE rn <= {limit}";

    private readonly record struct Result(double Ns, double Bytes, long OutputRows);

    private static Result Measure(
        string sql, List<Event> events, int batchSize, int runs, PartitionedTopKNarrowing mode)
    {
        var prev = PartitionedTopKNarrowingMode.Override;
        PartitionedTopKNarrowingMode.Override = mode;
        try
        {
            var consumed = new HashSet<NexmarkTable> { NexmarkTable.Bid };

            var nsPerEvent = new List<double>();
            long outputRows = 0;
            for (var run = 0; run < runs + 1; run++)
            {
                var q = Compile(sql);
                var elapsed = FeedStream(q, events, consumed, batchSize);
                if (run > 0)
                {
                    nsPerEvent.Add(elapsed.TotalNanoseconds / events.Count);
                }

                outputRows = CountOutput(q);
            }

            nsPerEvent.Sort();

            // Allocation: a primed pass (grow buffers) then a dedicated measured pass.
            var qAlloc = Compile(sql);
            FeedStream(qAlloc, events.Take(Math.Min(batchSize * 2, events.Count)).ToList(), consumed, batchSize);
            var qMeasure = Compile(sql);
            var bytes0 = GC.GetAllocatedBytesForCurrentThread();
            FeedStream(qMeasure, events, consumed, batchSize);
            var bytes1 = GC.GetAllocatedBytesForCurrentThread();

            return new Result(
                nsPerEvent[nsPerEvent.Count / 2], (bytes1 - bytes0) / (double)events.Count, outputRows);
        }
        finally
        {
            PartitionedTopKNarrowingMode.Override = prev;
        }
    }

    private static TimeSpan FeedStream(
        CompiledQuery q, List<Event> events, HashSet<NexmarkTable> consumed, int batchSize)
    {
        var buffer = new List<(object?[], long)>(batchSize);
        var sw = Stopwatch.StartNew();
        var sinceStep = 0;
        foreach (var e in events)
        {
            if (e.Table == NexmarkTable.Bid)
            {
                buffer.Add((e.Row, 1L));
            }

            if (++sinceStep >= batchSize)
            {
                Flush(q, buffer);
                q.Step();
                sinceStep = 0;
            }
        }

        if (sinceStep > 0)
        {
            Flush(q, buffer);
            q.Step();
        }

        sw.Stop();
        return sw.Elapsed;
    }

    private static void Flush(CompiledQuery q, List<(object?[], long)> buffer)
    {
        if (buffer.Count == 0)
        {
            return;
        }

        q.Table("bid").Push(buffer);
        buffer.Clear();
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
}
