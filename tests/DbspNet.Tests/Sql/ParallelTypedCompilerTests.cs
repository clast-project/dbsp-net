// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
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
        string[] ddl, string query, int workers, params Action<Func<string, TypedTableInput>>[] ticks) =>
        AssertPlanParallelMatchesSingle(CompilePlan(ddl, query), workers, ticks);

    private static void AssertPlanParallelMatchesSingle(
        LogicalPlan plan, int workers, params Action<Func<string, TypedTableInput>>[] ticks)
    {
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
    public void SplitProjection_MatchesSingle(int workers)
    {
        // A stateless projection using SPLIT_INDEX / SPLIT_PART. The point is that
        // it compiles on the TYPED path (so a parallel plan exists at all — the
        // structural fallback has none); the byte-wise split result is then
        // sharded trivially and must match the single-circuit output. Regression
        // guard for the q22 "single-only" fix.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (url VARCHAR NOT NULL)"],
            "SELECT SPLIT_INDEX(url, '/', 3) AS d1, SPLIT_INDEX(url, '/', 9) AS oob, " +
            "SPLIT_PART(url, '/', 1) AS p1 FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert("https://h/aaa/bbb");
                tbl("t").Insert("https://h/ccc/ddd");
            },
            tbl =>
            {
                tbl("t").Insert("no-delimiters-here");
                tbl("t").Delete("https://h/aaa/bbb");
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
    [InlineData(2)]
    public void GroupByComputedKey_CountStar_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL, p BIGINT NOT NULL)"],
            "SELECT CAST(ts AS DATE) AS day, COUNT(*) AS n FROM t GROUP BY CAST(ts AS DATE)",
            workers,
            tbl =>
            {
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(1_000_000L), 5L);
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(2_000_000L), 7L);
            });
    }

    [Theory]
    [InlineData(2)]
    public void GroupByComputedKey_SumCase_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL, p BIGINT NOT NULL)"],
            "SELECT CAST(ts AS DATE) AS day, " +
            "SUM(CASE WHEN p < 10 THEN 1 ELSE 0 END) AS lo FROM t GROUP BY CAST(ts AS DATE)",
            workers,
            tbl =>
            {
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(1_000_000L), 5L);
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(2_000_000L), 50L);
            });
    }

    [Theory]
    [InlineData(2)]
    public void GroupBy_FilterClause_MatchesSingle(int workers)
    {
        // FILTER desugars to CASE, so it must ride the same parallel aggregate
        // path (the Nexmark q15/q16 authoring shape).
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, COUNT(*) FILTER (WHERE v < 10) AS lo, " +
            "COUNT(DISTINCT v) FILTER (WHERE v >= 10) AS hi FROM t GROUP BY k",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 5L);
                tbl("t").Insert(1, 50L);
                tbl("t").Insert(1, 50L);
                tbl("t").Insert(2, 7L);
            },
            tbl =>
            {
                tbl("t").Insert(2, 100L);
                tbl("t").Delete(1, 5L);
            });
    }

    [Theory]
    [InlineData(2)]
    public void GroupByColumn_SumCase_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (k INT NOT NULL, p BIGINT NOT NULL)"],
            "SELECT k, SUM(CASE WHEN p < 10 THEN 1 ELSE 0 END) AS lo FROM t GROUP BY k",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 5L);
                tbl("t").Insert(1, 50L);
            });
    }

    [Theory]
    [InlineData(2)]
    public void GroupByComputedKey_CountDistinct_MatchesSingle(int workers)
    {
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL, b BIGINT NOT NULL)"],
            "SELECT CAST(ts AS DATE) AS day, COUNT(DISTINCT b) AS nb FROM t GROUP BY CAST(ts AS DATE)",
            workers,
            tbl =>
            {
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(1_000_000L), 5L);
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(1_500_000L), 5L);
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(2_000_000L), 7L);
            },
            tbl =>
            {
                // Drop one of the two b=5 rows (5 still present) and add a new value.
                tbl("t").Delete(new DbspNet.Sql.TypeSystem.Timestamp(1_500_000L), 5L);
                tbl("t").Insert(new DbspNet.Sql.TypeSystem.Timestamp(2_500_000L), 9L);
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
    public void InnerJoinCrossFilter_FoldedToResidual_MatchesSingle(int workers)
    {
        // Run the optimizer so the cross-cutting WHERE folds into the join's
        // Residual, which the in-memory join op applies during enumeration. The
        // parallel (exchanged) result must still equal the single-circuit one,
        // including under a delete that flips a row across the residual boundary.
        var plan = PlanOptimizer.Optimize(CompilePlan(
            [
                "CREATE TABLE a (k INT NOT NULL, lo INT NOT NULL, hi INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.k, b.v FROM a JOIN b ON a.k = b.k WHERE b.v BETWEEN a.lo AND a.hi"));

        AssertPlanParallelMatchesSingle(
            plan,
            workers,
            tbl =>
            {
                tbl("a").Insert(1, 0, 50);
                tbl("a").Insert(2, 10, 20);
                tbl("b").Insert(1, 25);   // passes (0..50)
                tbl("b").Insert(1, 99);   // fails  (> 50)
                tbl("b").Insert(2, 15);   // passes (10..20)
                tbl("b").Insert(2, 5);    // fails  (< 10)
            },
            tbl =>
            {
                tbl("b").Delete(1, 25);   // retract a passing row
                tbl("a").Insert(3, 0, 100);
                tbl("b").Insert(3, 100);  // passes (0..100)
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void JoinThenGroupBySupersetKey_ElidesExchange_MatchesSingle(int workers)
    {
        // The Nexmark-q4 shape: a join partitions by a.k, then a GROUP BY on
        // (a.k, a.g) — a superset of the join key — so the second exchange is
        // elided (each group already lives wholly on one worker). A residual
        // (BETWEEN-style) filter and a non-key aggregate exercise the same path
        // q4 takes. Output must still match the single-circuit result, including
        // after a delete that moves a group's MAX.
        AssertParallelMatchesSingle(
            [
                "CREATE TABLE a (k INT NOT NULL, g INT NOT NULL, lo INT NOT NULL, hi INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.k, a.g, MAX(b.v) AS m FROM a JOIN b ON a.k = b.k " +
            "WHERE b.v BETWEEN a.lo AND a.hi GROUP BY a.k, a.g",
            workers,
            tbl =>
            {
                tbl("a").Insert(1, 100, 0, 50);
                tbl("a").Insert(2, 100, 0, 50);
                tbl("a").Insert(3, 200, 10, 20);
                tbl("b").Insert(1, 10);
                tbl("b").Insert(1, 40);
                tbl("b").Insert(1, 99); // filtered out (> hi)
                tbl("b").Insert(2, 25);
                tbl("b").Insert(3, 15);
            },
            tbl =>
            {
                tbl("b").Delete(1, 40); // group (1,100) MAX drops 40 -> 10
                tbl("b").Insert(2, 45);
                tbl("a").Insert(4, 200, 0, 50);
                tbl("b").Insert(4, 30);
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
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void LargeBatchFilter_ParallelIngestEgest_MatchesSingle(int workers)
    {
        // A single Push above the 4096-row threshold engages the two-phase parallel
        // ingest (encode on the worker threads, then scatter), and the wide filter
        // output engages the parallel decode. The gathered result must still equal
        // the single-circuit query after every tick.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
            "SELECT id, v FROM t WHERE v > 100",
            workers,
            tbl =>
            {
                var batch = new List<(object?[] Values, long Weight)>(6000);
                for (var i = 0; i < 6000; i++)
                {
                    batch.Add((new object?[] { i, i % 1000 }, 1L));
                }

                tbl("t").Push(batch);
            },
            tbl =>
            {
                // Retract a large slice in one push (mixed effect; same batch both ways).
                var batch = new List<(object?[] Values, long Weight)>(6000);
                for (var i = 0; i < 6000; i++)
                {
                    batch.Add((new object?[] { i, i % 1000 }, i % 3 == 0 ? -1L : 0L));
                }

                tbl("t").Push(batch);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void LargeBatchStringDecimal_ParallelEncodeDecode_MatchesSingle(int workers)
    {
        // VARCHAR + DECIMAL columns make the boundary encode (string→Utf8String,
        // decimal→Decimal128) and decode the expensive part — both now run on the
        // worker threads. > 4096 distinct output rows engages the parallel decode.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (region VARCHAR NOT NULL, amount DECIMAL(10, 2) NOT NULL)"],
            "SELECT region, amount FROM t",
            workers,
            tbl =>
            {
                var regions = new[] { "north", "south", "east", "west" };
                var batch = new List<(object?[] Values, long Weight)>(9000);
                for (var i = 0; i < 9000; i++)
                {
                    // DECIMAL values cross the boundary as strings (BoundaryEncoder parses them).
                    batch.Add((new object?[] { regions[i % 4], $"{i % 2200}.25" }, 1L));
                }

                tbl("t").Push(batch);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void LargeBatchInjectiveProjection_DisjointGather_MatchesSingle(int workers)
    {
        // Reorders both source columns and adds a computed one — injective (keeps
        // every input column by identity), so RetainsAllInputColumns marks the
        // output shard-disjoint and the gather concatenates per-worker. A large
        // batch engages both the parallel ingest and the parallel concat gather.
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT v, k, k + v AS s FROM t",
            workers,
            tbl =>
            {
                var batch = new List<(object?[] Values, long Weight)>(7000);
                for (var i = 0; i < 7000; i++)
                {
                    batch.Add((new object?[] { i, 7000 - i }, 1L));   // distinct (k, v) pairs
                }

                tbl("t").Push(batch);
            });
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void ColumnDroppingProjection_GatherMergesCrossWorkerDuplicates(int workers)
    {
        // SELECT v drops the unique id, so rows distinct by the whole-row shard key
        // collapse to equal output rows that were sharded onto *different* workers.
        // The gather must SUM their weights (not concatenate), so each output key
        // appears exactly once. The AssertParallelMatchesSingle harness sums weights
        // in its comparison and would mask a concat bug, so check the raw output.
        var plan = CompilePlan(["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"], "SELECT v FROM t");
        Assert.True(TypedPlanCompiler.TryCompileParallel(plan, workers, out var parallel), "parallel compile failed");

        using (parallel)
        {
            const int rows = 6000;
            const int distinct = 50;
            var batch = new List<(object?[] Values, long Weight)>(rows);
            for (var i = 0; i < rows; i++)
            {
                batch.Add((new object?[] { i, i % distinct }, 1L));   // unique id, v in [0, 50)
            }

            parallel!.Table("t").Push(batch);
            parallel.Step();

            var seen = new HashSet<string>(StringComparer.Ordinal);
            var emitted = 0;
            foreach (var (values, weight) in parallel.Current)
            {
                emitted++;
                Assert.True(seen.Add(values[0]!.ToString()!), $"duplicate key {values[0]} in gathered output");
                Assert.Equal(rows / distinct, weight);   // every v appears rows/distinct times
            }

            Assert.Equal(distinct, emitted);
        }
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
