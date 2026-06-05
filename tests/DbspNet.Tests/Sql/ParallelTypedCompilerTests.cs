// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase 4 — the SQL planner inserts an exchange at each key-sensitive boundary
/// (join / group-by / distinct), so a data-parallel compile produces exactly the
/// single-circuit result for every worker count. Each test drives the same
/// incremental op-script through both the single and the parallel query and
/// asserts the gathered output matches after every tick.
/// </summary>
public class ParallelTypedCompilerTests
{
    private static LogicalPlan CompilePlan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static Dictionary<string, long> Materialize(IEnumerable<(object?[] Values, long Weight)> current)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in current)
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

    /// <summary>
    /// Compile <paramref name="query"/> both ways, run <paramref name="ticks"/>
    /// against each (a tick is a set of table mutations), and assert the output
    /// matches after every step.
    /// </summary>
    private static void AssertParallelMatchesSingle(
        string[] ddl, string query, int workers, params Action<Func<string, TypedTableInput>>[] ticks)
    {
        var plan = CompilePlan(ddl, query);

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var single), "single compile failed");
        Assert.True(TypedPlanCompiler.TryCompileParallel(plan, workers, out var parallel), "parallel compile failed");

        using (parallel)
        {
            Assert.Equal(workers, parallel!.Workers);
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](single!.Table);
                ticks[t](parallel.Table);
                single!.Step();
                parallel.Step();

                Assert.Equal(Materialize(single.Current), Materialize(parallel.Current));
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void GroupByAggregate_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 10L);
                tbl("t").Insert(1, 20L);
                tbl("t").Insert(2, 5L);
                tbl("t").Insert(3, 7L);
            },
            tbl =>
            {
                tbl("t").Insert(2, 5L);    // grows group 2
                tbl("t").Delete(1, 10L);   // shrinks group 1
            },
            tbl =>
            {
                tbl("t").Insert(4, 100L);  // new group
                tbl("t").Delete(3, 7L);    // removes group 3 entirely
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void EquiJoin_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            [
                "CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, y INT NOT NULL)",
            ],
            "SELECT a.k, a.x, b.y FROM a JOIN b ON a.k = b.k",
            workers,
            tbl =>
            {
                tbl("a").Insert(1, 100);
                tbl("a").Insert(2, 200);
                tbl("b").Insert(1, 11);
                tbl("b").Insert(1, 12);   // key 1 matches twice
                tbl("b").Insert(3, 33);   // no left match
            },
            tbl =>
            {
                tbl("a").Insert(3, 300);  // now key 3 matches
                tbl("b").Insert(2, 22);
                tbl("a").Delete(1, 100);  // retract a row of key 1
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Distinct_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (k INT NOT NULL)"],
            "SELECT DISTINCT k FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1);
                tbl("t").Insert(1);   // duplicate collapses
                tbl("t").Insert(2);
            },
            tbl =>
            {
                tbl("t").Insert(3);
                tbl("t").Delete(1);   // one of two copies of 1 — 1 still present
            },
            tbl =>
            {
                tbl("t").Delete(1);   // last copy of 1 — now absent
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void JoinThenGroupBy_MatchesSingle(int workers)
    {
        // Two key-sensitive boundaries (join key, then group key) — each gets its
        // own exchange; the join output is re-sharded before the aggregate.
        AssertParallelMatchesSingle(
            [
                "CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, y INT NOT NULL)",
            ],
            "SELECT a.k, COUNT(*) FROM a JOIN b ON a.k = b.k GROUP BY a.k",
            workers,
            tbl =>
            {
                tbl("a").Insert(1, 1);
                tbl("a").Insert(2, 2);
                tbl("b").Insert(1, 1);
                tbl("b").Insert(1, 2);
                tbl("b").Insert(2, 3);
            },
            tbl =>
            {
                tbl("a").Insert(2, 9);
                tbl("b").Insert(3, 3);
                tbl("a").Insert(3, 3);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void FilterProject_NoExchange_MatchesSingle(int workers)
    {
        // No key-sensitive operator: each worker filters its own shard and the
        // gather reconstructs — exercises sharded I/O without any exchange.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT v FROM t WHERE v > 5",
            workers,
            tbl =>
            {
                for (var i = 0; i < 12; i++)
                {
                    tbl("t").Insert(i);
                }
            },
            tbl => tbl("t").Delete(10));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void GroupByStringKey_MatchesSingle(int workers)
    {
        // String group key: the exchange must co-locate equal strings on one worker
        // via the stable per-column hash. (object.GetHashCode for string is
        // per-process-randomized — if the partition used it, a group could still
        // land coherently within one run, but recovery would re-shard wrongly; the
        // stable hash is what this test pins for the within-run path.)
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (region VARCHAR NOT NULL, amount INT NOT NULL)"],
            "SELECT region, SUM(amount) AS s, COUNT(*) AS c FROM t GROUP BY region",
            workers,
            tbl =>
            {
                tbl("t").Insert("north", 10);
                tbl("t").Insert("south", 5);
                tbl("t").Insert("north", 7);
                tbl("t").Insert("east", 3);
            },
            tbl =>
            {
                tbl("t").Insert("south", 2);
                tbl("t").Delete("north", 10);
                tbl("t").Insert("west", 1);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void GroupByCompositeKey_MatchesSingle(int workers)
    {
        // Two-column group key exercises StableHash.Combine — both columns must
        // feed the partition so equal (a, b) pairs co-locate.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (a INT NOT NULL, b VARCHAR NOT NULL, v INT NOT NULL)"],
            "SELECT a, b, SUM(v) AS s FROM t GROUP BY a, b",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, "x", 10);
                tbl("t").Insert(1, "y", 20);
                tbl("t").Insert(2, "x", 5);
                tbl("t").Insert(1, "x", 1);   // same (1,x) group
            },
            tbl =>
            {
                tbl("t").Insert(2, "x", 4);
                tbl("t").Delete(1, "y", 20);  // empties (1,y)
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void PartitionedTopK_MatchesSingle(int workers)
    {
        // ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) <= 1 — the Nexmark-q9
        // shape. The partitioned TOP-K must be preceded by an exchange on the
        // PARTITION BY key, or a group's rows split across workers and each worker
        // emits its own local winner (too many rows). This pins the exchange.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, tag INT NOT NULL)"],
            "SELECT g, v, tag FROM (" +
            "  SELECT g, v, tag, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC, tag ASC) AS rn" +
            "  FROM t) ranked " +
            "WHERE rn <= 1",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 10, 100);
                tbl("t").Insert(1, 30, 101);   // current winner of group 1
                tbl("t").Insert(1, 20, 102);
                tbl("t").Insert(2, 5, 200);
                tbl("t").Insert(2, 5, 199);     // tie on v=5 broken by tag ASC -> 199
            },
            tbl =>
            {
                tbl("t").Insert(1, 40, 103);    // new winner of group 1
                tbl("t").Insert(3, 7, 300);     // new group
                tbl("t").Delete(2, 5, 199);     // retract the group-2 winner -> 200 wins
            });
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void SkewedGroupKey_MatchesSingle(int workers)
    {
        // Extreme skew: one hot key carries the bulk of the rows, so it serializes
        // onto a single worker while the others stay near-idle. Correctness is
        // W-independent regardless of partition balance — only throughput suffers.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, SUM(v) AS s, COUNT(*) AS c FROM t GROUP BY k",
            workers,
            tbl =>
            {
                for (var i = 0; i < 200; i++)
                {
                    tbl("t").Insert(0, i);     // hot key 0
                }

                tbl("t").Insert(1, 1);
                tbl("t").Insert(2, 2);
            },
            tbl =>
            {
                for (var i = 0; i < 50; i++)
                {
                    tbl("t").Insert(0, 1000 + i);
                }

                tbl("t").Delete(1, 1);         // empties the cold group
            });
    }
}
