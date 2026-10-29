// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// State-representation round 3 (docs/design-state-representation.md §8): the four
/// shipped state variants — A compact partitioned TOP-K (unboxed key), B compact
/// COUNT(DISTINCT), G trace-free linear aggregate, B2 hybrid MIN/MAX — are selected
/// in <b>every replica</b> of a W&gt;1 typed parallel compile, and their thread-static
/// opt-outs reach those replicas too (the replicas are built on the calling thread).
/// Each Nexmark-shaped query (q4, q15/q16, q18, q19) is then driven through a random
/// insert/delete stream: per tick, parallel W=4 with the variants on must equal a
/// single circuit with every variant off, and the integrated output must equal a
/// one-tick batch recompute.
/// </summary>
public class ParallelStateSelectionTests
{
    private static readonly string[] Ddl =
    {
        "CREATE TABLE auction (id BIGINT NOT NULL, category BIGINT NOT NULL)",
        "CREATE TABLE bid (auction BIGINT NOT NULL, bidder BIGINT NOT NULL, price BIGINT NOT NULL, " +
        "channel VARCHAR NOT NULL, day BIGINT NOT NULL, ts BIGINT NOT NULL)",
    };

    // q4: join + MAX per auction (B2) + AVG per category (G).
    private const string Q4 =
        "SELECT q.category, AVG(q.final) AS avg_final FROM (" +
        "SELECT MAX(b.price) AS final, a.category FROM auction a JOIN bid b ON a.id = b.auction " +
        "GROUP BY a.id, a.category) q GROUP BY q.category";

    // q15/q16: COUNT(*) / FILTERed counts / FILTERed COUNT(DISTINCT) (G; B when G is off).
    private const string Q16 =
        "SELECT channel, day, COUNT(*) AS total, COUNT(*) FILTER (WHERE price < 10) AS r1, " +
        "COUNT(DISTINCT bidder) AS bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10) AS r1_bidders, " +
        "COUNT(DISTINCT auction) AS auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10) AS r2_auctions " +
        "FROM bid GROUP BY channel, day";

    // q18: TOP-1 dedup per (bidder, auction) (A).
    private const string Q18 =
        "SELECT auction, bidder, price, channel, ts FROM (SELECT auction, bidder, price, channel, ts, " +
        "ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY ts DESC) AS rn FROM bid) r WHERE rn <= 1";

    // q19: TOP-10 per auction (A).
    private const string Q19 =
        "SELECT auction, bidder, price, channel, ts FROM (SELECT auction, bidder, price, channel, ts, " +
        "ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) r WHERE rn <= 3";

    /// <summary>The variant switches (thread-static on this thread): A, G, B, B2.</summary>
    private sealed record Arms(bool TopK, bool Linear, bool Distinct, bool MinMax)
    {
        public static Arms All(bool on) => new(on, on, on, on);

        public T Run<T>(Func<T> body)
        {
            var (topK, linear, distinct, minMax) = (PartitionedTopKStateMode.Override, LinearAggregateMode.Enabled,
                CompactAggregatorStateMode.CompactDistinct, CompactAggregatorStateMode.MinMax);
            PartitionedTopKStateMode.Override = TopK ? PartitionedTopKState.Compact : PartitionedTopKState.Legacy;
            LinearAggregateMode.Enabled = Linear;
            CompactAggregatorStateMode.CompactDistinct = Distinct;
            CompactAggregatorStateMode.MinMax = MinMax ? MinMaxStateKind.Hybrid : MinMaxStateKind.Legacy;
            try
            {
                return body();
            }
            finally
            {
                PartitionedTopKStateMode.Override = topK;
                LinearAggregateMode.Enabled = linear;
                CompactAggregatorStateMode.CompactDistinct = distinct;
                CompactAggregatorStateMode.MinMax = minMax;
            }
        }
    }

    public static TheoryData<int> Workers => new() { 1, 2, 4 };

    [Theory]
    [MemberData(nameof(Workers))]
    public void Q4_HybridMinMaxAndLinearAvg_SelectedInEveryReplica(int workers)
    {
        Assert.Equal(
            ["IncrementalAggregateOp{TypedSqlMinMaxCompactAggregator}", "LinearAggregateOp"],
            StateOps(Q4, workers, Arms.All(true)));
        Assert.Equal(
            ["IncrementalAggregateOp{TypedSqlMinMaxAggregator}", "IncrementalAggregateOp{TypedAvgDoubleNullableAggregator}"],
            StateOps(Q4, workers, Arms.All(false)));

        // Each switch alone flips only its own operator.
        Assert.Equal(
            ["IncrementalAggregateOp{TypedSqlMinMaxCompactAggregator}", "IncrementalAggregateOp{TypedAvgDoubleNullableAggregator}"],
            StateOps(Q4, workers, Arms.All(true) with { Linear = false }));
        Assert.Equal(
            ["IncrementalAggregateOp{TypedSqlMinMaxAggregator}", "LinearAggregateOp"],
            StateOps(Q4, workers, Arms.All(true) with { MinMax = false }));
    }

    [Theory]
    [MemberData(nameof(Workers))]
    public void Q16_LinearAggregate_AndCompactDistinctFallback_SelectedInEveryReplica(int workers)
    {
        Assert.Equal(["LinearAggregateOp"], StateOps(Q16, workers, Arms.All(true)));

        // G off, B on: one shared unboxed COUNT(DISTINCT) unit per base column.
        var compact = Assert.Single(StateOps(Q16, workers, Arms.All(true) with { Linear = false }));
        Assert.Equal(
            "IncrementalAggregateOp{TypedSharedCountDistinctAggregator TypedSharedCountDistinctAggregator " +
            "TypedCountStarAggregator TypedCountNullableAggregator}",
            compact);

        var legacy = Assert.Single(StateOps(Q16, workers, Arms.All(false)));
        Assert.Contains("TypedCountDistinctAggregator", legacy, StringComparison.Ordinal);
        Assert.DoesNotContain("Shared", legacy, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Workers))]
    public void Q18_Q19_CompactTopKWithUnboxedKey_SelectedInEveryReplica(int workers)
    {
        foreach (var sql in new[] { Q18, Q19 })
        {
            Assert.Equal(["PartitionedTopKCompactOp<unboxed>"], StateOps(sql, workers, Arms.All(true)));
            var legacy = Assert.Single(StateOps(sql, workers, Arms.All(false)));
            Assert.StartsWith("PartitionedTopK", legacy, StringComparison.Ordinal);
            Assert.DoesNotContain("Compact", legacy, StringComparison.Ordinal);
            Assert.EndsWith("<boxed>", legacy, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(nameof(Q4), 20261029)]
    [InlineData(nameof(Q16), 20261030)]
    [InlineData(nameof(Q18), 20261031)]
    [InlineData(nameof(Q19), 20261101)]
    [InlineData(nameof(Q4), 7)]
    [InlineData(nameof(Q16), 8)]
    [InlineData(nameof(Q19), 9)]
    public void ParallelOn_EqualsSingleOff_EveryTick_AndEqualsBatch(string queryName, int seed)
    {
        var sql = queryName switch { nameof(Q4) => Q4, nameof(Q16) => Q16, nameof(Q18) => Q18, _ => Q19 };
        var plan = Plan(sql);
        var reference = Arms.All(false).Run(() => PlanToCircuit.Compile(plan));
        var parallel = Arms.All(true).Run(() =>
        {
            Assert.True(TypedPlanCompiler.TryCompileParallel(plan, 4, out var p));
            return p!;
        });

        var rng = new Random(seed);
        var liveBids = new List<object?[]>();
        var liveAuctions = new List<object?[]>();
        var integrated = new Dictionary<string, long>(StringComparer.Ordinal);
        long ts = 0;
        using (parallel)
        {
            for (var tick = 0; tick < 60; tick++)
            {
                var n = rng.Next(1, 25);
                for (var i = 0; i < n; i++)
                {
                    var (table, row, insert) = NextEvent(rng, liveBids, liveAuctions, ref ts, withAuctions: sql == Q4);
                    if (insert)
                    {
                        reference.Table(table).Insert(row);
                        parallel.Table(table).Insert(row);
                    }
                    else
                    {
                        reference.Table(table).Delete(row);
                        parallel.Table(table).Delete(row);
                    }
                }

                reference.Step();
                parallel.Step();
                var expected = Canon(reference.Current.Select(kv => (kv.Key.ToArray(), kv.Value.Value)));
                var got = Canon(parallel.Current);
                Assert.True(
                    DictEquals(expected, got),
                    $"tick {tick}: expected {Show(expected)}\n got {Show(got)}");
                foreach (var (k, w) in got) Add(integrated, k, w);
            }
        }

        // Batch: a fresh parallel circuit fed the final tables in one tick.
        var batch = Arms.All(true).Run(() =>
        {
            Assert.True(TypedPlanCompiler.TryCompileParallel(plan, 4, out var p));
            return p!;
        });
        using (batch)
        {
            foreach (var row in liveBids) batch.Table("bid").Insert(row);
            if (sql == Q4)
            {
                foreach (var row in liveAuctions) batch.Table("auction").Insert(row);
            }
            batch.Step();
            var once = Canon(batch.Current);
            Assert.True(DictEquals(integrated, once), $"batch {Show(once)}\n incremental {Show(integrated)}");
        }
    }

    private static (string Table, object?[] Row, bool Insert) NextEvent(
        Random rng, List<object?[]> bids, List<object?[]> auctions, ref long ts, bool withAuctions)
    {
        // Deletes only of live rows (well-formed tables); biased toward deleting the
        // highest-priced bid so MIN/MAX groups lose their extreme and promote.
        var roll = rng.Next(10);
        if (roll < 1 && withAuctions)
        {
            if (auctions.Count > 0 && rng.Next(2) == 0)
            {
                var victim = auctions[rng.Next(auctions.Count)];
                auctions.Remove(victim);
                return ("auction", victim, false);
            }

            var a = new object?[] { (long)rng.Next(8), (long)rng.Next(3) };
            auctions.Add(a);
            return ("auction", a, true);
        }

        if (roll < 4 && bids.Count > 0)
        {
            var victim = rng.Next(2) == 0
                ? bids.MaxBy(b => (long)b[2]!)!
                : bids[rng.Next(bids.Count)];
            bids.Remove(victim);
            return ("bid", victim, false);
        }

        var bid = new object?[]
        {
            (long)rng.Next(8), (long)rng.Next(6), (long)rng.Next(20), "c" + rng.Next(3), (long)rng.Next(3), ts++,
        };
        bids.Add(bid);
        return ("bid", bid, true);
    }

    /// <summary>
    /// Compiles <paramref name="sql"/> at <paramref name="workers"/> under
    /// <paramref name="arms"/> and returns the state-bearing operators of replica 0, after asserting
    /// every replica built the same ones.
    /// </summary>
    private static List<string> StateOps(string sql, int workers, Arms arms)
    {
        var plan = Plan(sql);
        var q = arms.Run(() =>
        {
            Assert.True(TypedPlanCompiler.TryCompileParallel(plan, workers, out var p), "parallel compile failed");
            return p!;
        });
        using (q)
        {
            var perReplica = q.Circuit.Replicas.Select(Describe).ToList();
            Assert.Equal(workers, perReplica.Count);
            foreach (var r in perReplica) Assert.Equal(perReplica[0], r);
            return perReplica[0];
        }
    }

    private static List<string> Describe(RootCircuit replica)
    {
        var parts = new List<string>();
        foreach (var op in replica.Operators)
        {
            var type = op.GetType();
            var name = type.Name.Split('`')[0];
            if (name.Contains("TopK", StringComparison.Ordinal))
            {
                var key = type.GetGenericArguments()[1];
                parts.Add($"{name}<{(key == typeof(StructuralRow) ? "boxed" : "unboxed")}>");
            }
            else if (name.Contains("Aggregate", StringComparison.Ordinal))
            {
                var composite = type.GetField("_aggregator", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
                    ?.GetValue(op);
                var units = composite?.GetType()
                    .GetField("_aggs", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
                    ?.GetValue(composite) as Array;
                parts.Add(units is null
                    ? name
                    : name + "{" + string.Join(" ", units.Cast<object>().Select(u => u.GetType().Name.Split('`')[0])) + "}");
            }
        }

        return parts;
    }

    private static void Add(Dictionary<string, long> map, string key, long w)
    {
        var v = map.GetValueOrDefault(key) + w;
        if (v == 0) map.Remove(key);
        else map[key] = v;
    }

    private static Dictionary<string, long> Canon(IEnumerable<(object?[] Values, long Weight)> rows)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in rows)
        {
            Add(map, string.Join("|", values.Select(v => v?.ToString() ?? "<null>")), weight);
        }

        return map;
    }

    private static bool DictEquals(Dictionary<string, long> a, Dictionary<string, long> b)
        => a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    private static string Show(Dictionary<string, long> m)
        => string.Join(", ", m.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => $"{e.Key}:{e.Value}"));

    private static LogicalPlan Plan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl) resolver.Resolve(Parser.ParseStatement(s));
        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
    }
}
