// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using DbspNet.Tests.EndToEnd;

namespace DbspNet.Tests.Sql;

/// <summary>
/// End-to-end validation of the monomorphized window-aggregate order key
/// (design §23.7, <see cref="CompileOptions.MonomorphizeWindowOrderKey"/>). The
/// gate re-keys the typed <c>PARTITION BY … ORDER BY</c> window's per-partition
/// ordered state on the <b>unboxed</b> monotone long
/// (<see cref="LongKeyComparer{TRow}"/>) instead of the boxed
/// <c>SortKeyComparer</c>; it must be output-equivalent. Each test drives the
/// <b>same</b> incremental op-script (inserts AND retractions) through four paths
/// — the structural circuit (the ground-truth oracle, itself proven ≡ batch in
/// <see cref="WindowAggregateTests"/>), the typed boxed-comparer circuit, and the
/// typed monomorphized circuit on both the flat and spine trace families, at
/// W ∈ {1,2,4,8} — and asserts every path produces the same output delta after
/// every tick. The randomized section additionally accumulates the mono path's
/// deltas and asserts they equal the batch recomputation directly.
/// </summary>
public class WindowAggregateMonomorphizeTests
{
    /// <summary>One table mutation: a +weight insert or -weight delete of a row.</summary>
    private readonly record struct Mut(string Table, object?[] Row, long Weight);

    private static Mut Ins(string table, params object?[] row) => new(table, row, 1);

    private static Mut Del(string table, params object?[] row) => new(table, row, -1);

    private static LogicalPlan CompilePlan(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query);
    }

    private static void Apply(CompiledQuery q, Mut[] tick)
    {
        foreach (var m in tick)
        {
            if (m.Weight > 0)
            {
                q.Table(m.Table).Insert(m.Row);
            }
            else
            {
                q.Table(m.Table).Delete(m.Row);
            }
        }
    }

    private static void Apply(Func<string, TypedTableInput> tbl, Mut[] tick)
    {
        foreach (var m in tick)
        {
            if (m.Weight > 0)
            {
                tbl(m.Table).Insert(m.Row);
            }
            else
            {
                tbl(m.Table).Delete(m.Row);
            }
        }
    }

    private static Dictionary<string, long> Canon(IEnumerable<(object?[] Values, long Weight)> current)
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

    private static Dictionary<string, long> Canon(ZSet<StructuralRow, Z64> z) =>
        Canon(z.Select(kv => (kv.Key.ToArray(), kv.Value.Value)));

    /// <summary>
    /// Compile <paramref name="query"/> four ways and assert the structural,
    /// typed-boxed, typed-mono (flat), and typed-mono (spine) outputs all agree
    /// after every tick. Returns the number of order keys the mono compiles wired
    /// through the monomorphized comparer (0 for a window with no ORDER BY).
    /// </summary>
    private static void AssertAllPathsAgree(string ddl, string query, int workers, params Mut[][] ticks)
    {
        var plan = CompilePlan(ddl, query);
        var structural = PlanToCircuit.Compile(plan);

        // Explicit false/true on both arms so the A/B survives the default (which is
        // now on): boxed = the incumbent one-key SortKeyComparer, mono = LongKeyComparer.
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(
                plan, workers, out var boxed, null, new CompileOptions { MonomorphizeWindowOrderKey = false }),
            "typed boxed parallel compile failed");
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(
                plan, workers, out var mono, null, new CompileOptions { MonomorphizeWindowOrderKey = true }),
            "typed mono (flat) parallel compile failed");
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(
                plan, workers, out var monoSpine, null,
                new CompileOptions { MonomorphizeWindowOrderKey = true, TraceFamily = TraceFamily.Spine }),
            "typed mono (spine) parallel compile failed");

        using (boxed)
        using (mono)
        using (monoSpine)
        {
            Assert.Equal(workers, mono!.Workers);
            foreach (var tick in ticks)
            {
                Apply(structural, tick);
                Apply(boxed!.Table, tick);
                Apply(mono.Table, tick);
                Apply(monoSpine!.Table, tick);
                structural.Step();
                boxed.Step();
                mono.Step();
                monoSpine.Step();

                var expected = Canon(structural.Current);
                Assert.Equal(expected, Canon(boxed.Current));
                Assert.Equal(expected, Canon(mono.Current));
                Assert.Equal(expected, Canon(monoSpine.Current));
            }
        }
    }

    // ---- Engagement: the flag actually re-keys the ordered window ------------

    [Fact]
    public void MonomorphizeGate_Engages_ForOrderedWindow()
    {
        // A carrier-key ORDER BY window: the mono compile must wire the unboxed
        // comparer (one per replica). Measured as a strict increase in the shared
        // diagnostic counter — concurrent compiles can only add, so a non-increase
        // means the flag-on path did not engage.
        var plan = CompilePlan(
            "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM t");

        var before = TypedPlanCompiler.MonomorphizedWindowOrderKeyCount;
        Assert.True(TypedPlanCompiler.TryCompileParallel(
            plan, 1, out var q, null, new CompileOptions { MonomorphizeWindowOrderKey = true }));
        q!.Dispose();
        Assert.True(TypedPlanCompiler.MonomorphizedWindowOrderKeyCount > before);
    }

    [Fact]
    public void MonomorphizeGate_NoOp_ForWholePartitionWindow()
    {
        // No ORDER BY ⇒ no ordered state ⇒ the mono comparer is never wired, on the
        // typed path, regardless of the flag. Output is unaffected either way.
        var plan = CompilePlan(
            "CREATE TABLE t (g INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, v, SUM(v) OVER (PARTITION BY g) AS s FROM t");

        var before = TypedPlanCompiler.MonomorphizedWindowOrderKeyCount;
        Assert.True(TypedPlanCompiler.TryCompileParallel(
            plan, 4, out var q, null, new CompileOptions { MonomorphizeWindowOrderKey = true }));
        q!.Dispose();
        Assert.Equal(before, TypedPlanCompiler.MonomorphizedWindowOrderKeyCount);
    }

    // ---- Running frame (UNBOUNDED PRECEDING .. CURRENT ROW), INT order key ---

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void RunningSum_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM t",
            workers,
            [Ins("t", 1, 1, 10L), Ins("t", 1, 3, 30L), Ins("t", 1, 2, 20L), Ins("t", 2, 1, 5L)],
            [Ins("t", 1, 4, 40L), Del("t", 1, 2, 20L)],
            [Ins("t", 2, 2, 8L), Del("t", 1, 1, 10L)]);

    // ---- Running MIN/MAX (the daily_market 52-week shape), INT order key ------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void RunningMinMax_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, MIN(v) OVER (PARTITION BY g ORDER BY ts) AS lo, " +
            "MAX(v) OVER (PARTITION BY g ORDER BY ts) AS hi FROM t",
            workers,
            [Ins("t", 1, 0, 5L), Ins("t", 1, 1, 9L), Ins("t", 1, 2, 3L), Ins("t", 2, 0, 100L)],
            [Ins("t", 1, 3, 7L), Del("t", 1, 1, 9L)]);

    // ---- Running MIN over DESC order key (reverse-scan branch) ---------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void RunningMinDesc_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, v, MIN(v) OVER (PARTITION BY g ORDER BY ts DESC) AS lo FROM t",
            workers,
            [Ins("t", 1, 1, 10L), Ins("t", 1, 3, 30L), Ins("t", 1, 2, 20L), Ins("t", 2, 1, 5L)],
            [Ins("t", 1, 4, 40L), Del("t", 1, 3, 30L)]);

    // ---- Bounded RANGE frame, INT order key ----------------------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void BoundedRangeSum_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, v, SUM(v) OVER " +
            "(PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM t",
            workers,
            [Ins("t", 1, 0, 1L), Ins("t", 1, 1, 2L), Ins("t", 1, 2, 4L), Ins("t", 1, 5, 8L), Ins("t", 2, 1, 100L)],
            [Ins("t", 1, 3, 16L), Del("t", 1, 1, 2L)]);

    // ---- BIGINT order key (the SCD/logical-time carrier) ---------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void RunningSum_BigintKey_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts BIGINT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM t",
            workers,
            [Ins("t", 1, 1L, 10L), Ins("t", 1, 3L, 30L), Ins("t", 1, 2L, 20L), Ins("t", 2, 1L, 5L)],
            [Ins("t", 1, 4L, 40L), Del("t", 1, 2L, 20L)]);

    // ---- TIMESTAMP order key + INTERVAL RANGE (the fraud rolling-window shape) -

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void IntervalRangeOverTimestamp_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE txn (cust BIGINT NOT NULL, ts TIMESTAMP NOT NULL, amount BIGINT NOT NULL)",
            "SELECT cust, ts, " +
            "SUM(amount) OVER (PARTITION BY cust ORDER BY ts " +
            "  RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) AS roll, " +
            "COUNT(*) OVER (PARTITION BY cust ORDER BY ts " +
            "  RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW) AS cnt FROM txn",
            workers,
            [
                Ins("txn", 100L, new Timestamp(0L), 10L),
                Ins("txn", 100L, new Timestamp(500_000L), 20L),
                Ins("txn", 100L, new Timestamp(1_800_000L), 40L),
                Ins("txn", 200L, new Timestamp(0L), 5L),
                Ins("txn", 200L, new Timestamp(2_000_000L), 7L),
            ],
            [
                Ins("txn", 100L, new Timestamp(1_200_000L), 16L),
                Del("txn", 100L, new Timestamp(500_000L), 20L),
            ]);

    // ---- DATE order key (the daily_market DATE bucket) -----------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void RunningSum_DateKey_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (sym INT NOT NULL, d DATE NOT NULL, v BIGINT NOT NULL)",
            "SELECT sym, d, SUM(v) OVER (PARTITION BY sym ORDER BY d) AS run, " +
            "MAX(v) OVER (PARTITION BY sym ORDER BY d) AS hi FROM t",
            workers,
            [
                Ins("t", 1, new Date32(10), 5L),
                Ins("t", 1, new Date32(12), 9L),
                Ins("t", 1, new Date32(11), 3L),
                Ins("t", 2, new Date32(10), 100L),
            ],
            [Ins("t", 1, new Date32(13), 7L), Del("t", 1, new Date32(12), 9L)]);

    // ---- Nullable order key (exercises absolute NULL positioning) ------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void RunningSum_NullableKey_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts INT, v BIGINT NOT NULL)",
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM t",
            workers,
            [Ins("t", 1, 1, 10L), Ins("t", 1, null, 30L), Ins("t", 1, 2, 20L), Ins("t", 2, null, 5L)],
            [Ins("t", 1, 3, 40L), Del("t", 1, null, 30L)]);

    // ---- Mixed OVER specs in one query (chained typed window ops) ------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void MultipleSpecs_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL)",
            "SELECT g, ts, v, " +
            "SUM(v) OVER (PARTITION BY g) AS tot, " +
            "SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run, " +
            "COUNT(*) OVER (PARTITION BY ts ORDER BY g) AS per_ts FROM t",
            workers,
            [Ins("t", 1, 1, 10L), Ins("t", 1, 2, 20L), Ins("t", 2, 1, 5L), Ins("t", 2, 2, 7L)],
            [Ins("t", 1, 3, 1L), Del("t", 2, 1, 5L)]);

    // ---- Randomized incremental ≡ batch on the mono path (the law of record) --

    private const string W = "CREATE TABLE w (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)";

    [Theory]
    [InlineData(1, "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS s FROM w")]
    [InlineData(4, "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS s FROM w")]
    [InlineData(1, "SELECT g, ts, COUNT(*) OVER (PARTITION BY g ORDER BY ts) AS c FROM w")]
    [InlineData(4, "SELECT g, ts, v, MIN(v) OVER (PARTITION BY g ORDER BY ts) AS lo, MAX(v) OVER (PARTITION BY g ORDER BY ts) AS hi FROM w")]
    [InlineData(1, "SELECT g, ts, v, MIN(v) OVER (PARTITION BY g ORDER BY ts DESC) AS lo FROM w")]
    [InlineData(4, "SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM w")]
    [InlineData(1, "SELECT g, ts, v, MAX(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM w")]
    public void MonoIncrementalEqualsBatch_Random(int workers, string query)
    {
        for (var seed = 0; seed < 12; seed++)
        {
            AssertMonoIncrementalEqualsBatch(W, query, workers, seed, monotonicTs: false);
        }
    }

    [Theory]
    [InlineData(1, "SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM w")]
    [InlineData(4, "SELECT g, ts, v, MAX(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM w")]
    public void MonoIncrementalEqualsBatch_UnderLatenessGc(int workers, string query)
    {
        const string ddl = "CREATE TABLE w (g INT NOT NULL, ts BIGINT NOT NULL LATENESS 3, v INT NOT NULL)";
        for (var seed = 0; seed < 12; seed++)
        {
            AssertMonoIncrementalEqualsBatch(ddl, query, workers, seed, monotonicTs: true);
        }
    }

    private static void AssertMonoIncrementalEqualsBatch(
        string ddl, string query, int workers, int seed, bool monotonicTs)
    {
        // Generate the randomized tick script (identical shape to
        // WindowAggregateTests.AssertIncrementalEqualsBatch), then drive it through
        // the typed mono parallel query and assert the accumulated output deltas
        // equal the batch recomputation.
        var rng = new Random(seed);
        var present = new List<object?[]>();
        var ticks = new List<List<InputEvent>>();
        long nextTs = 0;
        for (var t = 0; t < 14; t++)
        {
            var tick = new List<InputEvent>();
            var ops = rng.Next(1, 4);
            for (var o = 0; o < ops; o++)
            {
                var del = !monotonicTs && present.Count > 0 && rng.NextDouble() < 0.35;
                if (del)
                {
                    var idx = rng.Next(present.Count);
                    var row = present[idx];
                    present.RemoveAt(idx);
                    tick.Add(new InputEvent("w", row, -1));
                }
                else
                {
                    var g = rng.Next(0, 3);
                    object tsVal = monotonicTs ? nextTs++ : (object)rng.Next(0, 8);
                    var v = rng.Next(0, 5);
                    var row = new object?[] { g, tsVal, v };
                    present.Add(row);
                    tick.Add(new InputEvent("w", row, 1));
                }
            }

            ticks.Add(tick);
        }

        var plan = CompilePlan(ddl, query);
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(
                plan, workers, out var mono, null, new CompileOptions { MonomorphizeWindowOrderKey = true }),
            "typed mono parallel compile failed");

        var accumulated = ZSet<StructuralRow, Z64>.Empty;
        using (mono)
        {
            foreach (var tick in ticks)
            {
                foreach (var ev in tick)
                {
                    if (ev.Weight > 0)
                    {
                        mono!.Table(ev.Table).Insert(ev.Row);
                    }
                    else
                    {
                        mono!.Table(ev.Table).Delete(ev.Row);
                    }
                }

                mono!.Step();
                accumulated += ToZSet(mono.Current);
            }
        }

        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["w"] = IncrementalOracle.NetTable(ticks.SelectMany(x => x), "w"),
        };
        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        Assert.True(
            accumulated.Equals(batch),
            $"seed={seed} workers={workers} query={query}\n  accumulated={accumulated}\n  batch={batch}");
    }

    private static ZSet<StructuralRow, Z64> ToZSet(IEnumerable<(object?[] Values, long Weight)> current)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (values, weight) in current)
        {
            b.Add(new StructuralRow(values), new Z64(weight));
        }

        return b.Build();
    }
}
