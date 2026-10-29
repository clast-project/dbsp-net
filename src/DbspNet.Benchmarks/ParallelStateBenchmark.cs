// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Reflection;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Core.Circuit;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// State-representation round 3 (docs/design-state-representation.md §8): the
/// deterministic instruments at W&gt;1. For each query × W it compiles the query
/// through <see cref="TypedPlanCompiler.TryCompileParallel"/> (W=1 included, so
/// every W goes through the same seam), prints the state-relevant operator and
/// aggregator types of replica 0 (the selection evidence), streams the events,
/// and reports <b>live bytes after a forced full GC</b> and <b>B/ev</b>
/// (process-wide allocation inside push+step only, so worker-thread allocation
/// counts and the generator's does not). No wall-clock is reported.
/// <c>dotnet run -- r3par [events] [batch] [W,W,..] [q..]</c>
/// </summary>
internal static class ParallelStateBenchmark
{
    public static int Run(string[] args)
    {
        int Arg(int i, int fallback) =>
            args.Length > i && int.TryParse(args[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)
                ? v : fallback;

        var events = Arg(1, 1_000_000);
        var batch = Arg(2, 10_000);
        var ws = args.Length > 3 && !args[3].StartsWith('q')
            ? args[3].Split(',').Select(s => int.Parse(s, CultureInfo.InvariantCulture)).ToArray()
            : [1, 4, 8, 14];
        var ids = args.Skip(1).Where(a => a.StartsWith('q')).ToArray();
        if (ids.Length == 0) ids = ["q4", "q9", "q15", "q16", "q18", "q19"];

        Console.WriteLine(
            $"# r3par events={events:N0} batch={batch:N0} gen={DefaultProfile} " +
            $"TOPK={Environment.GetEnvironmentVariable("DBSPNET_TOPK_STATE") ?? "-"} " +
            $"LINEAR={Environment.GetEnvironmentVariable("DBSPNET_LINEAR_AGG") ?? "-"} " +
            $"MINMAX={CompactAggregatorStateMode.MinMax} DISTINCT={(CompactAggregatorStateMode.CompactDistinct ? "compact" : "boxed")}");
        foreach (var id in ids)
        {
            var query = NexmarkQueries.All.First(q => q.Id == id);
            ulong? reference = null;
            foreach (var w in ws)
            {
                var (live, bev, ops, output) = MeasureOne(query, events, batch, w);
                reference ??= output;
                var same = reference == output ? "same" : "MISMATCH";
                Console.WriteLine(
                    $"RESULT {id} W={w} live={live} B/ev={bev:F1} digest={output:x16} vsFirstW={same} ops=[{ops}]");
            }
        }

        return 0;
    }

    private static (long Live, double BytesPerEvent, string Ops, ulong Digest) MeasureOne(
        NexmarkQueries.Query query, int totalEvents, int batchSize, int workers)
    {
        var consumed = query.Tables.ToHashSet();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        var baseline = GC.GetTotalMemory(forceFullCollection: true);

        if (!TypedPlanCompiler.TryCompileParallel(Plan(query.Sql), workers, out var q))
        {
            throw new InvalidOperationException($"{query.Id}: no parallel plan");
        }

        using (q)
        {
            var ops = DescribeState(q!.Circuit);
            var buffers = consumed.ToDictionary(t => t, _ => new List<(object?[], long)>(batchSize));
            long alloc = 0;
            ulong digest = 0;
            var sinceStep = 0;

            void Step()
            {
                var a0 = GC.GetTotalAllocatedBytes(precise: true);
                foreach (var (table, list) in buffers)
                {
                    if (list.Count > 0)
                    {
                        q.Table(TableName(table)).Push(list);
                        list.Clear();
                    }
                }

                q.Step();
                alloc += GC.GetTotalAllocatedBytes(precise: true) - a0;
                digest += Digest(q); // outside the allocation window
            }

            foreach (var e in Stream(totalEvents))
            {
                if (consumed.Contains(e.Table)) buffers[e.Table].Add((e.Row, 1L));
                if (++sinceStep >= batchSize)
                {
                    Step();
                    sinceStep = 0;
                }
            }

            if (sinceStep > 0) Step();

            buffers = null;
            var live = GC.GetTotalMemory(forceFullCollection: true) - baseline;
            GC.KeepAlive(q);
            return (live, alloc / (double)totalEvents, ops, digest);
        }
    }

    /// <summary>The state-bearing operators (TOP-K / aggregate) of every replica — for a
    /// TOP-K, whether its partition key is unboxed (not <c>StructuralRow</c>); for a
    /// trace-backed aggregate, the unit types of its composite. Replicas must agree.</summary>
    internal static string DescribeState(ParallelCircuit circuit)
    {
        var perReplica = circuit.Replicas.Select(DescribeReplica).ToList();
        return perReplica.All(r => r == perReplica[0])
            ? perReplica[0] + $" x{perReplica.Count} replicas"
            : "REPLICAS DIFFER: " + string.Join(" | ", perReplica);
    }

    private static string DescribeReplica(RootCircuit replica)
    {
        var parts = new List<string>();
        foreach (var op in replica.Operators)
        {
            var type = op.GetType();
            var name = type.Name.Split('`')[0];
            if (name.Contains("TopK", StringComparison.Ordinal))
            {
                var key = type.GetGenericArguments() is { Length: 2 } g ? g[1] : null;
                parts.Add($"{name}<key={(key == typeof(DbspNet.Core.Collections.StructuralRow) ? "boxed" : "unboxed")}>");
            }
            else if (name.Contains("Aggregate", StringComparison.Ordinal))
            {
                var units = AggregatorUnits(op);
                parts.Add(units is null ? name : $"{name}{{{units}}}");
            }
        }

        return string.Join(", ", parts);
    }

    private static string? AggregatorUnits(object op)
    {
        var aggField = op.GetType().GetField("_aggregator", BindingFlags.Instance | BindingFlags.NonPublic);
        var composite = aggField?.GetValue(op);
        var aggsField = composite?.GetType().GetField("_aggs", BindingFlags.Instance | BindingFlags.NonPublic);
        if (aggsField?.GetValue(composite) is not Array aggs) return null;
        return string.Join(" ", aggs.Cast<object>().Select(a => a.GetType().Name.Split('`')[0]));
    }

    /// <summary>Order-independent digest of one tick's output delta: sum of weight x hash(row).
    /// Summed over ticks it is a digest of the integrated view, without retaining it
    /// (which would bill the live-bytes measurement).</summary>
    private static ulong Digest(ParallelTypedCompiledQuery q)
    {
        ulong sum = 0;
        foreach (var (values, weight) in q.Current)
        {
            ulong h = 14695981039346656037UL;
            foreach (var v in values)
            {
                foreach (var c in v?.ToString() ?? "\u0000")
                {
                    h = (h ^ c) * 1099511628211UL;
                }

                h = (h ^ 0x1F) * 1099511628211UL;
            }

            sum += h * (ulong)weight;
        }

        return sum;
    }

    private static string TableName(NexmarkTable t) => t switch
    {
        NexmarkTable.Person => "person",
        NexmarkTable.Auction => "auction",
        _ => "bid",
    };

    private static LogicalPlan Plan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in NexmarkQueries.Ddl) resolver.Resolve(Parser.ParseStatement(s));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanOptimizer.Optimize(plan);
    }
}
