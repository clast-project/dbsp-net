// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Broadcast LEFT joins, and dimension views hinted small (docs/design-bounded-memory.md §5.44): a LEFT
// join whose right side is a small dimension replicates it and leaves the preserved side where it is,
// instead of sharding the fact on a key with a handful of values. A deployment-supplied count for a
// view stands in for the compiler's estimate (which can only bound a DISTINCT by its input). Required:
// every view equals the serial program on every tick, and the broadcast is really chosen.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class BroadcastLeftJoinTests
{
    // dim_v is a view: DISTINCT over a source the deployment calls large. fact keeps NULL codes
    // (bypass padding), codes with no dimension row (operator padding), and duplicates. Two left
    // joins in a chain (the second keyed on the first's output), then a GROUP BY on a column the
    // joins do not partition by, so a wrong partition claim would mis-aggregate.
    private static readonly string[] Sql =
    [
        "CREATE TABLE fact (id INT NOT NULL, code INT, tier VARCHAR, amt BIGINT NOT NULL)",
        "CREATE TABLE dim_src (code INT NOT NULL, label VARCHAR NOT NULL, noise INT NOT NULL)",
        "CREATE TABLE tiers (tier VARCHAR NOT NULL, rank_ INT NOT NULL)",
        "CREATE VIEW dim_v AS SELECT DISTINCT code, label FROM dim_src",
        "CREATE VIEW joined AS SELECT f.id, f.amt, COALESCE(d.label, 'none') AS label, t.rank_ " +
            "FROM fact f LEFT JOIN dim_v d ON f.code = d.code LEFT JOIN tiers t ON UPPER(COALESCE(f.tier, 'X')) = t.tier",
        "CREATE VIEW rollup AS SELECT label, rank_, SUM(amt) AS s, COUNT(*) AS n FROM joined GROUP BY label, rank_",
        // Grouped on the broadcast join's own key: only right if the join does not claim its
        // output is partitioned by that key (the fact never moved).
        "CREATE VIEW keyed AS SELECT f.code, COUNT(*) AS n, SUM(t.rank_) AS r FROM fact f LEFT JOIN tiers t ON f.code = t.rank_ GROUP BY f.code",
        // A preserved side partitioned by (id, code), then projected so those indices hold other
        // columns, then broadcast-joined (fused, §5.46) and grouped on the columns now at those
        // indices: only right if the join's output does not claim the stale partition.
        "CREATE VIEW per AS SELECT id, code, COUNT(*) AS c FROM fact GROUP BY id, code",
        "CREATE VIEW reord AS SELECT * FROM (SELECT c, COALESCE(code, 0) % 2 AS code2, id FROM per) p LEFT JOIN tiers t ON p.id % 3 = t.rank_",
        "CREATE VIEW reagg AS SELECT c, code2, COUNT(*) AS n, SUM(rank_) AS r FROM reord GROUP BY c, code2",
    ];

    private static readonly string[] Outputs = ["joined", "rollup", "keyed", "reagg"];

    private static readonly Dictionary<string, long> Counts = new(StringComparer.Ordinal)
    {
        ["fact"] = 10_000_000, ["dim_src"] = 5_000_000, ["tiers"] = 3,
    };

    private static int Broadcasts(ParallelCompiledProgram p) =>
        p.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.StartsWith("BroadcastExchangeOp", StringComparison.Ordinal));

    private static void Tick(Func<string, object> tbl, int tick)
    {
        if (tick == 0)
        {
            for (var c = 0; c < 4; c++)
            {
                Insert(tbl("dim_src"), c, "L" + c, 1);
                Insert(tbl("dim_src"), c, "L" + c, 2); // DISTINCT collapses these
            }

            Insert(tbl("tiers"), "A", 1);
            Insert(tbl("tiers"), "B", 2);
            Insert(tbl("tiers"), "X", 0);
            for (var i = 0; i < 60; i++)
            {
                int? code = i % 11 == 0 ? null : i % 6; // codes 4, 5 have no dimension row
                var tier = i % 7 == 0 ? null : i % 3 == 0 ? "a" : i % 3 == 1 ? "b" : "c";
                Insert(tbl("fact"), i, code, tier, (long)i);
                if (i % 13 == 0)
                {
                    Insert(tbl("fact"), i, code, tier, (long)i); // weight 2
                }
            }
        }
        else if (tick == 1)
        {
            // A dimension row for 4 matches padded rows; deleting both copies of code 1 pads its rows.
            Insert(tbl("dim_src"), 4, "L4", 1);
            Delete(tbl("dim_src"), 1, "L1", 1);
            Delete(tbl("dim_src"), 1, "L1", 2);
            Insert(tbl("tiers"), "C", 3);
            Delete(tbl("fact"), 2, 2, "c", 2L);
            Insert(tbl("fact"), 100, 5, "a", 100L);
            Insert(tbl("fact"), 101, null, null, 101L);
        }
        else
        {
            // Relabel code 0 (delete, insert in one tick), and drop tier A.
            Delete(tbl("dim_src"), 0, "L0", 1);
            Delete(tbl("dim_src"), 0, "L0", 2);
            Insert(tbl("dim_src"), 0, "L0b", 1);
            Delete(tbl("tiers"), "A", 1);
            Insert(tbl("fact"), 102, 4, "b", 102L);
        }
    }

    private static void AssertMatchesSerial(int workers, IReadOnlyDictionary<string, long> counts, int expectedBroadcasts)
    {
        var outputs = new HashSet<string>(Outputs, StringComparer.Ordinal);
        // The oracle compiles unfused: its preserved sides' NULL keys bypass the join (§5.46).
        CompiledProgram serial;
        var prev = IndexFusionMode.Enabled;
        IndexFusionMode.Enabled = false;
        try
        {
            serial = SqlProgram.Compile(Sql, outputs);
        }
        finally
        {
            IndexFusionMode.Enabled = prev;
        }

        Assert.True(SqlProgram.TryCompileParallel(
            Sql, outputs, workers, out var parallel,
            options: new CompileOptions { BroadcastMaxRows = 100, RelationRowCounts = counts }));
        using (parallel)
        {
            Assert.Equal(expectedBroadcasts, Broadcasts(parallel!));
            for (var t = 0; t < 3; t++)
            {
                Tick(name => serial.Table(name), t);
                Tick(name => parallel!.Table(name), t);
                serial.Step();
                parallel!.Step();
                foreach (var view in Outputs)
                {
                    var width = serial.Outputs[view].Schema.Count;
                    Assert.Equal(Materialize(serial.Outputs[view].CurrentView, width), Materialize(parallel.Outputs[view].CurrentView, width));
                }
            }
        }
    }

    // tiers is a 3-row input: both its LEFT joins broadcast. dim_v is estimated from dim_src (5M): hashed.
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void A_left_join_broadcasts_a_small_input_and_matches_serial(int workers) =>
        AssertMatchesSerial(workers, Counts, expectedBroadcasts: 3);

    // A supplied count for dim_v stands, so its LEFT join broadcasts too.
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void A_view_hinted_small_broadcasts_and_matches_serial(int workers) =>
        AssertMatchesSerial(workers, new Dictionary<string, long>(Counts, StringComparer.Ordinal) { ["dim_v"] = 1 }, expectedBroadcasts: 4);

    // Without counts, nothing broadcasts: still the serial answer.
    [Fact]
    public void Without_counts_both_joins_hash() =>
        AssertMatchesSerial(4, new Dictionary<string, long>(StringComparer.Ordinal), expectedBroadcasts: 0);

    // The broadcast LEFT joins' preserved sides are indexed in one pass with their chains (§5.46):
    // fewer pointwise operators than the unfused layout.
    [Fact]
    public void Broadcast_left_joins_fuse_the_preserved_sides_chain()
    {
        int ApplyOps(bool fused)
        {
            var prev = IndexFusionMode.Enabled;
            IndexFusionMode.Enabled = fused;
            try
            {
                Assert.True(SqlProgram.TryCompileParallel(
                    Sql, new HashSet<string>(Outputs, StringComparer.Ordinal), 4, out var p,
                    options: new CompileOptions { BroadcastMaxRows = 100, RelationRowCounts = Counts }));
                using (p)
                {
                    return p!.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.StartsWith("ApplyOp", StringComparison.Ordinal));
                }
            }
            finally
            {
                IndexFusionMode.Enabled = prev;
            }
        }

        var (fusedOps, unfusedOps) = (ApplyOps(true), ApplyOps(false));
        Assert.True(fusedOps < unfusedOps - 3, $"fused {fusedOps} ApplyOps, unfused {unfusedOps}");
    }

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var d = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var key = string.Join("|", Enumerable.Range(0, width).Select(i => kv.Key[i] is { } v ? FormattableString.Invariant($"{v}") : "<null>"));
            d[key] = d.GetValueOrDefault(key) + kv.Value.Value;
            if (d[key] == 0)
            {
                d.Remove(key);
            }
        }

        return d;
    }

    private static void Insert(object table, params object?[] values)
    {
        switch (table)
        {
            case TableInput t: t.Insert(values); break;
            case ShardedTableInput s: s.Insert(values); break;
            default: throw new InvalidOperationException();
        }
    }

    private static void Delete(object table, params object?[] values)
    {
        switch (table)
        {
            case TableInput t: t.Delete(values); break;
            case ShardedTableInput s: s.Delete(values); break;
            default: throw new InvalidOperationException();
        }
    }
}
