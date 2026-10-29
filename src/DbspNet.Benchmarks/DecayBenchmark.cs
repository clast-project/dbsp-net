// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Throughput-vs-state-size profiler (state-representation arc, Phase 0).
/// The M4 Nexmark run showed DbspNet's throughput decaying 1M→10M events while
/// Feldera's holds — including on stateless q22, which no state structure can
/// explain. This harness splits one long W=1 run into segments and reports, per
/// segment: engine throughput, the GC pause time that landed inside the timed
/// engine work, GC counts, heap size, and the operators' retained state rows.
///
/// <para>Two feed modes isolate the harness confound: <c>pregen</c> materializes
/// the whole stream up front (as the Nexmark runner does — tens of millions of
/// live objects every full GC must trace), <c>stream</c> generates each batch on
/// demand and drops it after the step. Only push+step is timed in both.</para>
/// </summary>
internal static class DecayBenchmark
{
    private static long LastLiveBytes;
    private static long LastOutputRows;

    private sealed record Segment(
        long EventsEnd, double EngineMs, double PauseMs, int Gen0, int Gen1, int Gen2,
        long AllocBytes, long HeapBytes, long StateRows);

    public static void Run(StringBuilder output, string[] queryIds, int totalEvents, int batchSize, int segments, bool pregen)
    {
        var mode = pregen ? "pregen" : "stream";
        Console.WriteLine();
        Console.WriteLine(
            $"=== Decay profile (events={totalEvents:N0}, batch={batchSize:N0}, segments={segments}, mode={mode}) ===");
        var events = pregen ? Generate(totalEvents) : null;

        output.AppendLine(CultureInfo.InvariantCulture, $"## Decay — mode={mode}, events={totalEvents:N0}, batch={batchSize:N0}");
        output.AppendLine();
        output.AppendLine(
            CultureInfo.InvariantCulture,
            $"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores, " +
            $"ServerGC={System.Runtime.GCSettings.IsServerGC}, " +
            $"DATAS={Environment.GetEnvironmentVariable("DOTNET_GCDynamicAdaptationMode") ?? "default"}.");
        output.AppendLine();

        foreach (var id in queryIds)
        {
            var query = NexmarkQueries.All.FirstOrDefault(q => q.Id == id);
            if (query is null)
            {
                Console.WriteLine($"  {id}: unknown query");
                continue;
            }

            // JIT warmup on a throwaway circuit (tiered compilation otherwise
            // bills the first segments), then settle the previous query's
            // garbage so it doesn't bill this one.
            MeasureOne(query, Stream(Math.Min(500_000, totalEvents)), 500_000, batchSize, 1, quiet: true);
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            var segs = MeasureOne(query, events ?? Stream(totalEvents), totalEvents, batchSize, segments);
            Report(output, id, segs);
            output.AppendLine(
                CultureInfo.InvariantCulture,
                $"Live state after full GC: **{LastLiveBytes / (double)(1 << 20):N0} MB** ({LastLiveBytes:N0} B); " +
                $"output delta rows (sum over steps): {LastOutputRows:N0}; generator: {DefaultProfile}.");
            output.AppendLine();
        }
    }

    private static List<Segment> MeasureOne(
        NexmarkQueries.Query query, IEnumerable<Event> events, int totalEvents, int batchSize, int segments, bool quiet = false)
    {
        var consumed = query.Tables.ToHashSet();
        var baseline = quiet ? 0 : GC.GetTotalMemory(forceFullCollection: true);
        var q = Compile(query.Sql);
        var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>(batchSize));
        var segSize = Math.Max(batchSize, totalEvents / segments);
        var result = new List<Segment>();

        double engineMs = 0;
        double pauseMs = 0;
        long alloc = 0;
        long outputRows = 0;
        int g0 = GC.CollectionCount(0), g1 = GC.CollectionCount(1), g2 = GC.CollectionCount(2);
        long seen = 0;
        var sinceStep = 0;
        var sinceSeg = 0L;

        void StepTimed()
        {
            var p0 = GC.GetTotalPauseDuration();
            var a0 = GC.GetAllocatedBytesForCurrentThread();
            var t0 = Stopwatch.GetTimestamp();
            foreach (var (table, list) in buffers)
            {
                if (list.Count > 0)
                {
                    q.Table(TableName(table)).Push(list);
                    list.Clear();
                }
            }

            q.Step();
            engineMs += Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
            alloc += GC.GetAllocatedBytesForCurrentThread() - a0;
            pauseMs += (GC.GetTotalPauseDuration() - p0).TotalMilliseconds;
            outputRows += q.Current.Count; // outside the timer

        }

        void CloseSegment()
        {
            long state = 0;
            foreach (var s in q.CollectStats())
            {
                state += Math.Max(0, s.RetainedRows);
            }

            int n0 = GC.CollectionCount(0), n1 = GC.CollectionCount(1), n2 = GC.CollectionCount(2);
            var seg = new Segment(seen, engineMs, pauseMs, n0 - g0, n1 - g1, n2 - g2, alloc,
                GC.GetGCMemoryInfo().HeapSizeBytes, state);
            result.Add(seg);
            if (!quiet)
            {
                Console.WriteLine(
                    $"  {query.Id,-4} @{seen,11:N0}  {sinceSeg / (seg.EngineMs / 1000.0),12:N0} ev/s  " +
                    $"pause {seg.PauseMs,8:F1}ms ({100 * seg.PauseMs / seg.EngineMs,4:F1}%)  " +
                    $"gc {seg.Gen0}/{seg.Gen1}/{seg.Gen2}  heap {seg.HeapBytes / (1 << 20),6:N0}MB  state {state:N0}");
            }

            (engineMs, pauseMs, alloc, sinceSeg) = (0, 0, 0, 0);
            (g0, g1, g2) = (n0, n1, n2);
        }

        foreach (var e in events)
        {
            seen++;
            sinceSeg++;
            if (consumed.Contains(e.Table))
            {
                buffers[e.Table].Add((e.Row, 1L));
            }

            if (++sinceStep >= batchSize)
            {
                StepTimed();
                sinceStep = 0;
                if (sinceSeg >= segSize)
                {
                    CloseSegment();
                }
            }
        }

        if (sinceStep > 0)
        {
            StepTimed();
        }

        if (sinceSeg > 0)
        {
            CloseSegment();
        }

        if (!quiet)
        {
            // Live state after a full blocking GC, outside the timer: what the
            // circuit actually retains, as opposed to heap size (which counts
            // uncollected garbage under Server GC's large gen0 budgets).
            var live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
            GC.KeepAlive(q);
            LastLiveBytes = live;
            LastOutputRows = outputRows;
            Console.WriteLine(
                $"  {query.Id,-4} live state after full GC: {live / (double)(1 << 20):N0} MB ({live:N0} B), " +
                $"output delta rows {outputRows:N0} [gen={DefaultProfile}]");
        }

        return result;
    }

    private static void Report(StringBuilder o, string id, List<Segment> segs)
    {
        o.AppendLine(CultureInfo.InvariantCulture, $"### {id}");
        o.AppendLine();
        o.AppendLine("| events (end) | ev/s | ev/s ex-pause | pause % | GC 0/1/2 | B/ev | heap MB | state rows |");
        o.AppendLine("|---:|---:|---:|---:|:--|---:|---:|---:|");
        long prev = 0;
        foreach (var s in segs)
        {
            var n = s.EventsEnd - prev;
            prev = s.EventsEnd;
            o.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {s.EventsEnd:N0} | {n / (s.EngineMs / 1000.0):N0} | {n / ((s.EngineMs - s.PauseMs) / 1000.0):N0} | " +
                $"{100 * s.PauseMs / s.EngineMs:F1} | {s.Gen0}/{s.Gen1}/{s.Gen2} | {s.AllocBytes / (double)n:F0} | " +
                $"{s.HeapBytes / (1 << 20):N0} | {s.StateRows:N0} |");
        }

        o.AppendLine();
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
}
