// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// PARTITION BY window aggregates take the typed → data-parallel (W&gt;1) path:
/// the operator partitions by the PARTITION BY key, so the planner inserts an
/// exchange on that key (co-locating each partition's whole window state on one
/// worker) and a W-worker compile produces exactly the single-circuit result.
/// Each test drives the same incremental op-script (inserts AND retractions, the
/// recompute-and-diff stress) through both the single and the parallel query and
/// asserts the gathered output matches after every tick — across the three frame
/// shapes (whole-partition / running / bounded RANGE, incl. the fraud-shaped
/// INTERVAL frame over a TIMESTAMP key) and SUM/COUNT/AVG/MIN/MAX. The negative
/// gate pins the sound structural fallback for a window with no PARTITION BY
/// (nothing to shard on).
/// </summary>
public class WindowAggregateParallelTests
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

    private static void AssertParallelMatchesSingle(
        string[] ddl, string query, int workers, params Action<Func<string, TypedTableInput>>[] ticks)
    {
        var plan = CompilePlan(ddl, query);
        Assert.True(TypedPlanCompiler.TryCompile(plan, out var single), "single typed compile failed");
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(plan, workers, out var parallel),
            "parallel compile failed — the window query did not take the typed/parallel path");

        using (parallel)
        {
            Assert.Equal(workers, parallel!.Workers);
            foreach (var tick in ticks)
            {
                tick(single!.Table);
                tick(parallel.Table);
                single!.Step();
                parallel.Step();

                Assert.Equal(Materialize(single.Current), Materialize(parallel.Current));
            }
        }
    }

    // ---- Whole-partition frame ----------------------------------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void WholePartition_SumAndCount_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT g, v, SUM(v) OVER (PARTITION BY g) AS s, COUNT(*) OVER (PARTITION BY g) AS c FROM t",
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
                tbl("t").Insert(2, 5L);     // grows partition 2's whole-partition sum
                tbl("t").Delete(1, 10L);    // shrinks partition 1
            },
            tbl =>
            {
                tbl("t").Insert(4, 100L);   // new partition
                tbl("t").Delete(3, 7L);     // empties partition 3
            });

    // ---- Running frame (UNBOUNDED PRECEDING AND CURRENT ROW) -----------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void RunningFrame_Sum_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 1, 10L);
                tbl("t").Insert(1, 3, 30L);
                tbl("t").Insert(1, 2, 20L);   // lands between — shifts the suffix
                tbl("t").Insert(2, 1, 5L);
            },
            tbl =>
            {
                tbl("t").Insert(1, 4, 40L);
                tbl("t").Delete(1, 2, 20L);   // retract a mid-partition row
            });

    // ---- Bounded RANGE frame (the fraud-relevant shape) ---------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void BoundedRange_Sum_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT g, ts, v, SUM(v) OVER " +
            "(PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 0, 1L);
                tbl("t").Insert(1, 1, 2L);
                tbl("t").Insert(1, 2, 4L);
                tbl("t").Insert(1, 5, 8L);    // outside the [3,5] frame of ts<=2 rows
                tbl("t").Insert(2, 1, 100L);
            },
            tbl =>
            {
                tbl("t").Insert(1, 3, 16L);   // enters the frames of ts in [3,5]
                tbl("t").Delete(1, 1, 2L);    // retract a row inside several frames
            });

    // ---- Multiple distinct OVER specs in one query --------------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void MultipleSpecs_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT g, ts, v, " +
            "SUM(v) OVER (PARTITION BY g) AS tot, " +
            "SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run, " +
            "COUNT(*) OVER (PARTITION BY ts) AS per_ts FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 1, 10L);
                tbl("t").Insert(1, 2, 20L);
                tbl("t").Insert(2, 1, 5L);
                tbl("t").Insert(2, 2, 7L);
            },
            tbl =>
            {
                tbl("t").Insert(1, 3, 1L);
                tbl("t").Delete(2, 1, 5L);
            });

    // ---- Fraud shape: INTERVAL RANGE over a TIMESTAMP key, BIGINT partition --

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void IntervalRangeOverTimestamp_FraudShape_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE txn (cust BIGINT NOT NULL, ts TIMESTAMP NOT NULL, amount BIGINT NOT NULL)"],
            "SELECT cust, ts, " +
            "SUM(amount) OVER (PARTITION BY cust ORDER BY ts " +
            "  RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) AS roll, " +
            "COUNT(*) OVER (PARTITION BY cust ORDER BY ts " +
            "  RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) AS cnt FROM txn",
            workers,
            tbl =>
            {
                // Per-customer rolling 1-second window (1s = 1_000_000 micros).
                tbl("txn").Insert(100L, new Timestamp(0L), 10L);
                tbl("txn").Insert(100L, new Timestamp(500_000L), 20L);    // within 1s of ts=0
                tbl("txn").Insert(100L, new Timestamp(1_800_000L), 40L);  // ts=0 has fallen out
                tbl("txn").Insert(200L, new Timestamp(0L), 5L);
                tbl("txn").Insert(200L, new Timestamp(2_000_000L), 7L);
            },
            tbl =>
            {
                tbl("txn").Insert(100L, new Timestamp(1_200_000L), 16L);    // enters several frames
                tbl("txn").Delete(100L, new Timestamp(500_000L), 20L);     // retract a mid-window txn
            });

    // ---- MIN/MAX over a bounded RANGE (a delete can move the extreme) --------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void BoundedRange_MinMax_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT g, ts, v, " +
            "MAX(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS mx, " +
            "MIN(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS mn FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 0, 5L);
                tbl("t").Insert(1, 1, 9L);    // current max of the [0,2] frames
                tbl("t").Insert(1, 2, 3L);    // current min
                tbl("t").Insert(2, 0, 100L);
            },
            tbl =>
            {
                tbl("t").Insert(1, 3, 7L);
                tbl("t").Delete(1, 1, 9L);    // retract the max — frames must recompute their extreme
            });

    // ---- Offset functions: LAG / LEAD / FIRST_VALUE / LAST_VALUE -------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void LagLead_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, ts, v, " +
            "LAG(v) OVER (PARTITION BY g ORDER BY ts) AS prev, " +
            "LEAD(v) OVER (PARTITION BY g ORDER BY ts) AS nxt FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 1, 10);
                tbl("t").Insert(1, 3, 30);
                tbl("t").Insert(1, 2, 20);   // lands between — shifts neighbours' LAG/LEAD
                tbl("t").Insert(2, 1, 5);
            },
            tbl =>
            {
                tbl("t").Insert(1, 4, 40);
                tbl("t").Delete(1, 2, 20);   // retract a middle row — neighbours' offsets move
            });

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void LagWithOffsetAndDefault_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, ts, v, LAG(v, 2, -1) OVER (PARTITION BY g ORDER BY ts) AS p2 FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 1, 10);
                tbl("t").Insert(1, 2, 20);   // p2 = -1 (no row two back)
                tbl("t").Insert(1, 3, 30);   // p2 = 10
                tbl("t").Insert(2, 1, 99);
            },
            tbl =>
            {
                tbl("t").Insert(1, 4, 40);
                tbl("t").Delete(1, 1, 10);   // shifts every later row's "two back"
            });

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void FirstLastValue_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, ts, v, " +
            "FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY ts) AS fv, " +
            "LAST_VALUE(v) OVER (PARTITION BY g ORDER BY ts) AS lv FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 2, 20);
                tbl("t").Insert(1, 3, 30);
                tbl("t").Insert(2, 5, 50);
            },
            tbl =>
            {
                tbl("t").Insert(1, 1, 10);   // new partition-1 first value
                tbl("t").Insert(1, 4, 40);   // new partition-1 last value
                tbl("t").Delete(2, 5, 50);   // empties partition 2
            });

    // ---- Mixed aggregate + offset in one query (chained typed ops) -----------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void MixedAggregateAndOffset_MatchesSingle(int workers) =>
        AssertParallelMatchesSingle(
            ["CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, ts, v, " +
            "SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run, " +
            "LAG(v) OVER (PARTITION BY g ORDER BY ts) AS prev FROM t",
            workers,
            tbl =>
            {
                tbl("t").Insert(1, 1, 10);
                tbl("t").Insert(1, 2, 20);
                tbl("t").Insert(2, 1, 5);
            },
            tbl =>
            {
                tbl("t").Insert(1, 3, 30);
                tbl("t").Delete(1, 1, 10);
            });

    // ---- Negative gate: sound structural fallback ----------------------------

    private static bool CompilesParallel(string[] ddl, string query)
    {
        var ok = TypedPlanCompiler.TryCompileParallel(CompilePlan(ddl, query), 4, out var q);
        q?.Dispose();
        return ok;
    }

    [Fact]
    public void NoPartitionByWindow_FallsBackToStructural_NoParallelPath()
    {
        // A single global window (no PARTITION BY) has nothing to shard on, so the
        // typed path declines it and the structural single-circuit compile runs.
        Assert.False(CompilesParallel(
            ["CREATE TABLE t (ts INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT ts, SUM(v) OVER (ORDER BY ts) AS run FROM t"));
    }

    [Fact]
    public void NoPartitionByOffset_FallsBackToStructural_NoParallelPath()
    {
        // Likewise for LAG/LEAD with no PARTITION BY — a single global ordering.
        Assert.False(CompilesParallel(
            ["CREATE TABLE t (ts INT NOT NULL, v INT NOT NULL)"],
            "SELECT ts, v, LAG(v) OVER (ORDER BY ts) AS p FROM t"));
    }
}
