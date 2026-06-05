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
}
