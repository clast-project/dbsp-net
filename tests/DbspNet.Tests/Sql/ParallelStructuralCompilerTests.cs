// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Increment 0 (docs/design-structural-parallel.md): the STRUCTURAL SQL compiler
/// inserts an exchange at each key-sensitive boundary (join / group-by / distinct
/// / partitioned window / top-K), so a data-parallel compile
/// (<see cref="PlanToCircuit.TryCompileParallel"/>) produces exactly the
/// single-circuit <see cref="PlanToCircuit.Compile(LogicalPlan, ISqlSnapshotCodecs, CompileOptions)"/>
/// result for every worker count. Each test drives the same incremental op-script
/// through both queries and asserts the gathered output matches after every tick,
/// swept over W = 1/2/4/8. W = 1 is the free structural-identity guard: every
/// exchange degrades to the same GroupProject the serial path emits.
/// </summary>
public class ParallelStructuralCompilerTests
{
    private static LogicalPlan CompilePlan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query);
    }

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var cells = new string[width];
            for (var i = 0; i < width; i++)
            {
                cells[i] = kv.Key[i]?.ToString() ?? "<null>";
            }

            var key = string.Join("|", cells);
            map[key] = map.GetValueOrDefault(key) + kv.Value.Value;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static void AssertParallelMatchesSerial(
        string[] ddl, string query, int workers, params Action<Func<string, object>>[] ticks) =>
        AssertPlanParallelMatchesSerial(CompilePlan(ddl, query), workers, ticks);

    private static void AssertPlanParallelMatchesSerial(
        LogicalPlan plan, int workers, params Action<Func<string, object>>[] ticks)
    {
        var single = PlanToCircuit.Compile(plan);
        Assert.True(
            PlanToCircuit.TryCompileParallel(plan, workers, out var parallel),
            "parallel compile refused a plan the oracle expected to shard");

        var width = plan.Schema.Count;
        using (parallel)
        {
            Assert.Equal(workers, parallel!.Workers);
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](name => single.Table(name));
                ticks[t](name => parallel.Table(name));
                single.Step();
                parallel.Step();

                Assert.Equal(Materialize(single.Current, width), Materialize(parallel.Current, width));
            }
        }
    }

    // As above but with fused single-barrier join exchange (CoalesceJoinExchange):
    // the ExchangeIndexJoin path must produce the same result as serial for every W.
    private static void AssertFusedMatchesSerial(
        string[] ddl, string query, int workers, params Action<Func<string, object>>[] ticks)
    {
        var plan = CompilePlan(ddl, query);
        var single = PlanToCircuit.Compile(plan);
        Assert.True(
            PlanToCircuit.TryCompileParallel(
                plan, workers, out var parallel,
                options: new CompileOptions { CoalesceJoinExchange = true }),
            "fused parallel compile refused a plan the oracle expected to shard");

        var width = plan.Schema.Count;
        using (parallel)
        {
            Assert.Equal(workers, parallel!.Workers);
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](name => single.Table(name));
                ticks[t](name => parallel.Table(name));
                single.Step();
                parallel.Step();
                Assert.Equal(Materialize(single.Current, width), Materialize(parallel.Current, width));
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void FusedThreeWayJoin_MatchesSerial(int workers)
    {
        // A 3-way inner join exercises two fused ExchangeIndexJoins (single barrier
        // each) — must stay byte-identical to serial for every W.
        AssertFusedMatchesSerial(
            [
                "CREATE TABLE a (k INT NOT NULL, x BIGINT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, y BIGINT NOT NULL)",
                "CREATE TABLE c (k INT NOT NULL, z BIGINT NOT NULL)",
            ],
            "SELECT a.x, b.y, c.z FROM a JOIN b ON a.k = b.k JOIN c ON a.k = c.k",
            workers,
            tbl =>
            {
                Insert(tbl("a"), 1, 10L);
                Insert(tbl("a"), 2, 20L);
                Insert(tbl("b"), 1, 100L);
                Insert(tbl("b"), 2, 200L);
                Insert(tbl("c"), 1, 1000L);
                Insert(tbl("c"), 2, 2000L);
            },
            tbl =>
            {
                Insert(tbl("b"), 1, 101L);
                Delete(tbl("a"), 2, 20L);
                Insert(tbl("c"), 3, 3000L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void FusedJoinThenGroupBy_MatchesSerial(int workers)
    {
        AssertFusedMatchesSerial(
            [
                "CREATE TABLE l (k INT NOT NULL, g INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, v BIGINT NOT NULL)",
            ],
            "SELECT l.g, SUM(r.v) FROM l JOIN r ON l.k = r.k GROUP BY l.g",
            workers,
            tbl =>
            {
                Insert(tbl("l"), 1, 10);
                Insert(tbl("l"), 2, 20);
                Insert(tbl("r"), 1, 100L);
                Insert(tbl("r"), 2, 200L);
            },
            tbl =>
            {
                Insert(tbl("r"), 1, 5L);
                Delete(tbl("l"), 2, 20);
            });
    }

    // As above but with broadcast join for small dimensions
    // (BroadcastSmallDimensionJoins): the right (dimension) side is replicated to
    // every worker instead of hash-sharded — must equal serial for every W.
    private static void AssertBroadcastMatchesSerial(
        string[] ddl, string query, int workers, params Action<Func<string, object>>[] ticks)
    {
        var plan = CompilePlan(ddl, query);
        var single = PlanToCircuit.Compile(plan);
        Assert.True(
            PlanToCircuit.TryCompileParallel(
                plan, workers, out var parallel,
                options: new CompileOptions { BroadcastSmallDimensionJoins = true }),
            "broadcast parallel compile refused a plan the oracle expected to shard");

        var width = plan.Schema.Count;
        using (parallel)
        {
            Assert.Equal(workers, parallel!.Workers);
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](name => single.Table(name));
                ticks[t](name => parallel.Table(name));
                single.Step();
                parallel.Step();
                Assert.Equal(Materialize(single.Current, width), Materialize(parallel.Current, width));
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void BroadcastDimensionJoin_MatchesSerial(int workers)
    {
        // fact ⋈ dim on a low-cardinality dim key (3 distinct) — the exact skew
        // shape broadcast targets. Right (dim) is a leaf scan → broadcast fires.
        AssertBroadcastMatchesSerial(
            [
                "CREATE TABLE fact (id INT NOT NULL, dk INT NOT NULL, amt BIGINT NOT NULL)",
                "CREATE TABLE dim (dk INT NOT NULL, label VARCHAR NOT NULL)",
            ],
            "SELECT fact.amt, dim.label FROM fact JOIN dim ON fact.dk = dim.dk",
            workers,
            tbl =>
            {
                Insert(tbl("dim"), 1, "a");
                Insert(tbl("dim"), 2, "b");
                Insert(tbl("dim"), 3, "c");
                for (var i = 0; i < 30; i++)
                {
                    Insert(tbl("fact"), i, (i % 3) + 1, (long)i);
                }
            },
            tbl =>
            {
                Insert(tbl("dim"), 2, "b2");
                Delete(tbl("fact"), 0, 1, 0L);
                Insert(tbl("fact"), 100, 1, 100L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void BroadcastDimensionJoinThenGroupBy_MatchesSerial(int workers)
    {
        AssertBroadcastMatchesSerial(
            [
                "CREATE TABLE fact (id INT NOT NULL, dk INT NOT NULL, amt BIGINT NOT NULL)",
                "CREATE TABLE dim (dk INT NOT NULL, grp INT NOT NULL)",
            ],
            "SELECT dim.grp, SUM(fact.amt) FROM fact JOIN dim ON fact.dk = dim.dk GROUP BY dim.grp",
            workers,
            tbl =>
            {
                Insert(tbl("dim"), 1, 10);
                Insert(tbl("dim"), 2, 10);
                Insert(tbl("dim"), 3, 20);
                for (var i = 0; i < 20; i++)
                {
                    Insert(tbl("fact"), i, (i % 3) + 1, (long)(i * 2));
                }
            },
            tbl =>
            {
                Delete(tbl("dim"), 3, 20);
                Insert(tbl("fact"), 50, 2, 500L);
            });
    }

    // Size-gated (production) broadcast: BroadcastMaxRows + RelationRowCounts. The
    // result must equal serial whether the size gate picks broadcast or hash.
    private static void AssertSizeGatedMatchesSerial(
        string[] ddl, string query, int workers, long maxRows,
        IReadOnlyDictionary<string, long> counts,
        params Action<Func<string, object>>[] ticks)
    {
        var plan = CompilePlan(ddl, query);
        var single = PlanToCircuit.Compile(plan);
        Assert.True(
            PlanToCircuit.TryCompileParallel(
                plan, workers, out var parallel,
                options: new CompileOptions { BroadcastMaxRows = maxRows, RelationRowCounts = counts }),
            "size-gated broadcast compile refused a plan the oracle expected to shard");

        var width = plan.Schema.Count;
        using (parallel)
        {
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](name => single.Table(name));
                ticks[t](name => parallel!.Table(name));
                single.Step();
                parallel!.Step();
                Assert.Equal(Materialize(single.Current, width), Materialize(parallel.Current, width));
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void SizeGatedBroadcast_SmallDimUnderThreshold_MatchesSerial(int workers)
    {
        // dim (3 rows) is under the 100-row threshold ⇒ broadcast path chosen.
        AssertSizeGatedMatchesSerial(
            [
                "CREATE TABLE fact (id INT NOT NULL, dk INT NOT NULL, amt BIGINT NOT NULL)",
                "CREATE TABLE dim (dk INT NOT NULL, label VARCHAR NOT NULL)",
            ],
            "SELECT fact.amt, dim.label FROM fact JOIN dim ON fact.dk = dim.dk",
            workers,
            maxRows: 100,
            new Dictionary<string, long>(StringComparer.Ordinal) { ["fact"] = 100_000, ["dim"] = 3 },
            tbl =>
            {
                Insert(tbl("dim"), 1, "a");
                Insert(tbl("dim"), 2, "b");
                for (var i = 0; i < 20; i++)
                {
                    Insert(tbl("fact"), i, (i % 2) + 1, (long)i);
                }
            });
    }

    [Theory]
    [InlineData(4)]
    public void SizeGatedBroadcast_LargeRightOverThreshold_MatchesSerial(int workers)
    {
        // "dim" is over threshold (and "fact" unknown) ⇒ hash join chosen; result
        // still identical. Proves the gate never makes a wrong-but-correct choice.
        AssertSizeGatedMatchesSerial(
            [
                "CREATE TABLE fact (id INT NOT NULL, dk INT NOT NULL, amt BIGINT NOT NULL)",
                "CREATE TABLE dim (dk INT NOT NULL, label VARCHAR NOT NULL)",
            ],
            "SELECT fact.amt, dim.label FROM fact JOIN dim ON fact.dk = dim.dk",
            workers,
            maxRows: 100,
            new Dictionary<string, long>(StringComparer.Ordinal) { ["dim"] = 1_000_000 },
            tbl =>
            {
                Insert(tbl("dim"), 1, "a");
                Insert(tbl("dim"), 2, "b");
                for (var i = 0; i < 20; i++)
                {
                    Insert(tbl("fact"), i, (i % 2) + 1, (long)i);
                }
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void SizeGatedBroadcast_Program_DerivesViewSizeFromBaseCounts(int workers)
    {
        // Program path: only BASE-table counts are supplied; the compiler estimates
        // the `dim` view (a small reference) is under threshold and broadcasts the
        // fact⋈dim view. Both output views must equal serial for every W.
        AssertProgramSizeGatedMatchesSerial(
            [
                "CREATE TABLE fact_src (id INT NOT NULL, dk INT NOT NULL, amt BIGINT NOT NULL)",
                "CREATE TABLE dim_src (dk INT NOT NULL, grp INT NOT NULL)",
                "CREATE VIEW dim AS SELECT dk, grp FROM dim_src",
                "CREATE VIEW joined AS SELECT f.amt AS amt, d.grp AS grp FROM fact_src f JOIN dim d ON f.dk = d.dk",
                "CREATE VIEW rollup AS SELECT grp, SUM(amt) AS s FROM joined GROUP BY grp",
            ],
            ["rollup"],
            workers,
            maxRows: 100,
            new Dictionary<string, long>(StringComparer.Ordinal) { ["fact_src"] = 100_000, ["dim_src"] = 3 },
            tbl =>
            {
                Insert(tbl("dim_src"), 1, 10);
                Insert(tbl("dim_src"), 2, 10);
                Insert(tbl("dim_src"), 3, 20);
                for (var i = 0; i < 30; i++)
                {
                    Insert(tbl("fact_src"), i, (i % 3) + 1, (long)i);
                }
            });
    }

    private static void AssertProgramSizeGatedMatchesSerial(
        string[] statements, string[] outputViews, int workers, long maxRows,
        IReadOnlyDictionary<string, long> baseCounts,
        params Action<Func<string, object>>[] ticks)
    {
        var outputs = new HashSet<string>(outputViews, StringComparer.Ordinal);
        var serial = SqlProgram.Compile(statements, outputs);
        Assert.True(
            SqlProgram.TryCompileParallel(
                statements, outputs, workers, out var parallel,
                options: new CompileOptions { BroadcastMaxRows = maxRows, RelationRowCounts = baseCounts }),
            "program size-gated broadcast compile refused");

        using (parallel)
        {
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](name => serial.Table(name));
                ticks[t](name => parallel!.Table(name));
                serial.Step();
                parallel!.Step();
                foreach (var view in outputViews)
                {
                    var width = serial.Outputs[view].Schema.Count;
                    Assert.Equal(
                        Materialize(serial.Outputs[view].CurrentView, width),
                        Materialize(parallel.Outputs[view].CurrentView, width));
                }
            }
        }
    }

    // A thin adapter so a single tick lambda can drive either input handle type
    // (serial TableInput or sharded ShardedTableInput) through the same calls.
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

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void GroupByAggregate_MatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k",
            workers,
            tbl =>
            {
                Insert(tbl("t"), 1, 10L);
                Insert(tbl("t"), 1, 20L);
                Insert(tbl("t"), 2, 5L);
                Insert(tbl("t"), 3, 7L);
            },
            tbl =>
            {
                Insert(tbl("t"), 2, 5L);
                Delete(tbl("t"), 1, 10L);
            },
            tbl =>
            {
                Insert(tbl("t"), 4, 100L);
                Delete(tbl("t"), 3, 7L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void EquiJoin_MatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            [
                "CREATE TABLE l (k INT NOT NULL, a BIGINT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, b BIGINT NOT NULL)",
            ],
            "SELECT l.a, r.b FROM l JOIN r ON l.k = r.k",
            workers,
            tbl =>
            {
                Insert(tbl("l"), 1, 100L);
                Insert(tbl("l"), 1, 101L);
                Insert(tbl("l"), 2, 200L);
                Insert(tbl("r"), 1, 10L);
                Insert(tbl("r"), 2, 20L);
                Insert(tbl("r"), 3, 30L);
            },
            tbl =>
            {
                Insert(tbl("r"), 1, 11L);
                Delete(tbl("l"), 2, 200L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void JoinThenGroupBy_MatchesSerial(int workers)
    {
        // Two exchanges: shuffle both join sides by k, then re-shuffle the join
        // output by the group key g.
        AssertParallelMatchesSerial(
            [
                "CREATE TABLE l (k INT NOT NULL, g INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, v BIGINT NOT NULL)",
            ],
            "SELECT l.g, SUM(r.v) FROM l JOIN r ON l.k = r.k GROUP BY l.g",
            workers,
            tbl =>
            {
                Insert(tbl("l"), 1, 10);
                Insert(tbl("l"), 2, 10);
                Insert(tbl("l"), 3, 20);
                Insert(tbl("r"), 1, 100L);
                Insert(tbl("r"), 2, 200L);
                Insert(tbl("r"), 3, 300L);
            },
            tbl =>
            {
                Insert(tbl("r"), 1, 5L);
                Delete(tbl("l"), 3, 20);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void JoinThenGroupByJoinKey_ElidesExchange(int workers)
    {
        // GROUP BY the join key: the join output is already partitioned by k, so
        // the aggregate's exchange is elided (IsKeySubset). Result must be
        // identical to serial regardless.
        AssertParallelMatchesSerial(
            [
                "CREATE TABLE l (k INT NOT NULL, a BIGINT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, b BIGINT NOT NULL)",
            ],
            "SELECT l.k, SUM(r.b) FROM l JOIN r ON l.k = r.k GROUP BY l.k",
            workers,
            tbl =>
            {
                Insert(tbl("l"), 1, 1L);
                Insert(tbl("l"), 2, 2L);
                Insert(tbl("r"), 1, 100L);
                Insert(tbl("r"), 1, 101L);
                Insert(tbl("r"), 2, 200L);
            },
            tbl =>
            {
                Delete(tbl("r"), 1, 100L);
                Insert(tbl("l"), 3, 3L);
                Insert(tbl("r"), 3, 300L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Distinct_MatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT DISTINCT k, v FROM t",
            workers,
            tbl =>
            {
                Insert(tbl("t"), 1, 1);
                Insert(tbl("t"), 1, 1);
                Insert(tbl("t"), 2, 2);
                Insert(tbl("t"), 3, 3);
            },
            tbl =>
            {
                Delete(tbl("t"), 1, 1);
                Insert(tbl("t"), 4, 4);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void GroupByStringKey_MatchesSerial(int workers)
    {
        // Stable per-column string hash: equal strings must co-locate regardless
        // of process-randomized GetHashCode.
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (s VARCHAR NOT NULL, v BIGINT NOT NULL)"],
            "SELECT s, SUM(v) FROM t GROUP BY s",
            workers,
            tbl =>
            {
                Insert(tbl("t"), "apple", 1L);
                Insert(tbl("t"), "banana", 2L);
                Insert(tbl("t"), "apple", 3L);
                Insert(tbl("t"), "cherry", 4L);
            },
            tbl =>
            {
                Insert(tbl("t"), "banana", 5L);
                Delete(tbl("t"), "apple", 1L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void GroupByCompositeKey_MatchesSerial(int workers)
    {
        // Composite key folds per-column hashes through StableHash.Combine.
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (a INT NOT NULL, b VARCHAR NOT NULL, v BIGINT NOT NULL)"],
            "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
            workers,
            tbl =>
            {
                Insert(tbl("t"), 1, "x", 1L);
                Insert(tbl("t"), 1, "y", 1L);
                Insert(tbl("t"), 2, "x", 1L);
                Insert(tbl("t"), 1, "x", 1L);
            },
            tbl =>
            {
                Delete(tbl("t"), 1, "x", 1L);
                Insert(tbl("t"), 3, "z", 1L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void PartitionedTopK_MatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (p INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT p, v FROM (SELECT p, v, ROW_NUMBER() OVER (PARTITION BY p ORDER BY v DESC) rn FROM t) sub WHERE rn <= 1",
            workers,
            tbl =>
            {
                Insert(tbl("t"), 1, 10L);
                Insert(tbl("t"), 1, 30L);
                Insert(tbl("t"), 1, 20L);
                Insert(tbl("t"), 2, 5L);
            },
            tbl =>
            {
                Insert(tbl("t"), 2, 50L);
                Delete(tbl("t"), 1, 30L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void WindowAggregate_MatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (p INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT p, SUM(v) OVER (PARTITION BY p) FROM t",
            workers,
            tbl =>
            {
                Insert(tbl("t"), 1, 10L);
                Insert(tbl("t"), 1, 20L);
                Insert(tbl("t"), 2, 5L);
            },
            tbl =>
            {
                Insert(tbl("t"), 1, 30L);
                Delete(tbl("t"), 2, 5L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void UnionAll_MatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            [
                "CREATE TABLE a (k INT NOT NULL, v BIGINT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v BIGINT NOT NULL)",
            ],
            "SELECT k, v FROM a UNION ALL SELECT k, v FROM b",
            workers,
            tbl =>
            {
                Insert(tbl("a"), 1, 1L);
                Insert(tbl("a"), 2, 2L);
                Insert(tbl("b"), 1, 1L);
                Insert(tbl("b"), 3, 3L);
            },
            tbl =>
            {
                Delete(tbl("a"), 1, 1L);
                Insert(tbl("b"), 4, 4L);
            });
    }

    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    public void SkewedGroupKey_MatchesSerial(int workers)
    {
        // One hot key dominates; correctness is W-independent even under skew.
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k",
            workers,
            tbl =>
            {
                for (var i = 0; i < 50; i++)
                {
                    Insert(tbl("t"), 7, (long)i);
                }

                Insert(tbl("t"), 1, 1L);
                Insert(tbl("t"), 2, 2L);
            },
            tbl =>
            {
                for (var i = 0; i < 10; i++)
                {
                    Delete(tbl("t"), 7, (long)i);
                }
            });
    }

    [Fact]
    public void GlobalWindow_RefusesParallel()
    {
        // No PARTITION BY ⇒ a single global window ⇒ nothing to shard on; the
        // compile must refuse so the caller falls back to the serial circuit.
        var plan = CompilePlan(
            ["CREATE TABLE t (v BIGINT NOT NULL)"],
            "SELECT SUM(v) OVER () FROM t");
        Assert.False(PlanToCircuit.TryCompileParallel(plan, 4, out var q));
        Assert.Null(q);
    }

    [Fact]
    public void LeftJoin_RefusesParallel()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, a BIGINT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, b BIGINT NOT NULL)",
            ],
            "SELECT l.a, r.b FROM l LEFT JOIN r ON l.k = r.k");
        Assert.False(PlanToCircuit.TryCompileParallel(plan, 4, out _));
    }

    [Fact]
    public void RealGroupKey_RefusesParallel()
    {
        // REAL has no stable partition hash ⇒ refuse rather than shard on an
        // unstable key.
        var plan = CompilePlan(
            ["CREATE TABLE t (k REAL NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");
        Assert.False(PlanToCircuit.TryCompileParallel(plan, 4, out _));
    }

    [Fact]
    public void SingleWorker_CompilesAndMatches()
    {
        // W=1 must always compile (byte-identical to serial) for any supported plan.
        AssertParallelMatchesSerial(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k",
            1,
            tbl => Insert(tbl("t"), 1, 10L));
    }

    // ---- Increment 2: whole multi-view program parallel ----

    private static void AssertProgramParallelMatchesSerial(
        string[] statements, string[] outputViews, int workers,
        params Action<Func<string, object>>[] ticks)
    {
        var outputs = new HashSet<string>(outputViews, StringComparer.Ordinal);
        var serial = SqlProgram.Compile(statements, outputs);
        Assert.True(
            SqlProgram.TryCompileParallel(statements, outputs, workers, out var parallel),
            "program parallel compile refused a program the oracle expected to shard");

        using (parallel)
        {
            Assert.Equal(workers, parallel!.Workers);
            for (var t = 0; t < ticks.Length; t++)
            {
                ticks[t](name => serial.Table(name));
                ticks[t](name => parallel.Table(name));
                serial.Step();
                parallel.Step();

                foreach (var view in outputViews)
                {
                    var width = serial.Outputs[view].Schema.Count;
                    Assert.Equal(
                        Materialize(serial.Outputs[view].CurrentView, width),
                        Materialize(parallel.Outputs[view].CurrentView, width));
                }
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Program_JoinThenAggregate_MatchesSerial(int workers)
    {
        // sources → joined (silver) → agg (gold output). The shared inter-view
        // stream `joined` is exchanged by k for the join and re-exchanged for the
        // group-by; both output views must match serial for every W.
        AssertProgramParallelMatchesSerial(
            [
                "CREATE TABLE l (k INT NOT NULL, a BIGINT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, b BIGINT NOT NULL)",
                "CREATE VIEW joined AS SELECT l.k AS k, l.a AS a, r.b AS b FROM l JOIN r ON l.k = r.k",
                "CREATE VIEW agg AS SELECT k, SUM(a) AS sa, SUM(b) AS sb FROM joined GROUP BY k",
            ],
            ["agg"],
            workers,
            tbl =>
            {
                Insert(tbl("l"), 1, 10L);
                Insert(tbl("l"), 2, 20L);
                Insert(tbl("r"), 1, 100L);
                Insert(tbl("r"), 1, 101L);
                Insert(tbl("r"), 2, 200L);
            },
            tbl =>
            {
                Delete(tbl("r"), 1, 100L);
                Insert(tbl("l"), 3, 30L);
                Insert(tbl("r"), 3, 300L);
            });
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Program_MultiOutput_MatchesSerial(int workers)
    {
        // Two designated outputs off one source: a filtered passthrough and a
        // grouped aggregate. Exercises the driver-side integrate-per-view gather.
        AssertProgramParallelMatchesSerial(
            [
                "CREATE TABLE t (k INT NOT NULL, g INT NOT NULL, v BIGINT NOT NULL)",
                "CREATE VIEW big AS SELECT k, v FROM t WHERE v > 5",
                "CREATE VIEW per_g AS SELECT g, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY g",
            ],
            ["big", "per_g"],
            workers,
            tbl =>
            {
                Insert(tbl("t"), 1, 100, 10L);
                Insert(tbl("t"), 2, 100, 3L);
                Insert(tbl("t"), 3, 200, 20L);
            },
            tbl =>
            {
                Insert(tbl("t"), 4, 100, 7L);
                Delete(tbl("t"), 3, 200, 20L);
            });
    }
}
