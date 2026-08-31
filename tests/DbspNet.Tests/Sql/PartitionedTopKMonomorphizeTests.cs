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

namespace DbspNet.Tests.Sql;

/// <summary>
/// End-to-end validation of the monomorphized partitioned-TOP-K order key
/// (design §26, <see cref="CompileOptions.MonomorphizeTopKOrderKey"/>) — the TOP-K
/// twin of <see cref="WindowAggregateMonomorphizeTests"/>. The gate keys the
/// operator's per-partition <c>SortedDictionary</c> on <b>unboxed</b> monotone longs
/// (<see cref="LongKeyComparer{TRow}"/> / <see cref="MultiLongKeyComparer{TRow}"/>)
/// instead of the boxed <c>SortKeyComparer</c>; it must be output-equivalent.
/// <para>Each test drives the <b>same</b> incremental op-script (inserts AND
/// retractions, including tie groups that RANK / DENSE_RANK must keep or drop whole)
/// through the structural circuit — the ground-truth oracle, itself proven ≡ batch in
/// <see cref="PartitionedTopKTests"/> — plus the typed boxed and typed monomorphized
/// circuits at W ∈ {1,2,4,8}, and asserts all outputs agree after every tick.</para>
/// </summary>
public class PartitionedTopKMonomorphizeTests
{
    private const long Sec = 1_000_000L;

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
    /// Compile three ways — structural oracle, typed boxed comparer, typed
    /// monomorphized comparer — and assert all agree after every tick.
    /// </summary>
    private static void AssertAllPathsAgree(string ddl, string query, int workers, params Mut[][] ticks)
    {
        var plan = CompilePlan(ddl, query);
        var structural = PlanToCircuit.Compile(plan);

        // Explicit false/true on both arms so the A/B survives the default (on).
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(
                plan, workers, out var boxed, null, new CompileOptions { MonomorphizeTopKOrderKey = false }),
            "typed boxed parallel compile failed");
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(
                plan, workers, out var mono, null, new CompileOptions { MonomorphizeTopKOrderKey = true }),
            "typed mono parallel compile failed");

        using (boxed)
        using (mono)
        {
            Assert.Equal(workers, mono!.Workers);
            foreach (var tick in ticks)
            {
                Apply(structural, tick);
                Apply(boxed!.Table, tick);
                Apply(mono.Table, tick);
                structural.Step();
                boxed.Step();
                mono.Step();

                var expected = Canon(structural.Current);
                Assert.Equal(expected, Canon(boxed.Current));
                Assert.Equal(expected, Canon(mono.Current));
            }
        }
    }

    private const string Emp = "CREATE TABLE emp (dept INT NOT NULL, sal INT NOT NULL)";

    private static string RowNumber(string order = "sal DESC", int k = 2) =>
        $"SELECT dept, sal FROM (SELECT dept, sal, ROW_NUMBER() OVER " +
        $"(PARTITION BY dept ORDER BY {order}) AS rn FROM emp) s WHERE rn <= {k}";

    // ---- Engagement: the flag wires the unboxed comparer, and only for carriers ----

    [Fact]
    public void MonomorphizeGate_Engages_ForCarrierOrderKey()
    {
        var plan = CompilePlan(Emp, RowNumber());
        var before = TypedPlanCompiler.MonomorphizedTopKOrderKeyCount;
        Assert.True(TypedPlanCompiler.TryCompileParallel(
            plan, 1, out var q, null, new CompileOptions { MonomorphizeTopKOrderKey = true }));
        q!.Dispose();
        Assert.True(TypedPlanCompiler.MonomorphizedTopKOrderKeyCount > before);
    }

    [Fact]
    public void MonomorphizeGate_NoOp_WhenDisabled()
    {
        var plan = CompilePlan(Emp, RowNumber());
        var before = TypedPlanCompiler.MonomorphizedTopKOrderKeyCount;
        Assert.True(TypedPlanCompiler.TryCompileParallel(
            plan, 1, out var q, null, new CompileOptions { MonomorphizeTopKOrderKey = false }));
        q!.Dispose();
        Assert.Equal(before, TypedPlanCompiler.MonomorphizedTopKOrderKeyCount);
    }

    [Fact]
    public void MonomorphizeGate_DeclinesNonCarrierKey_AndStaysCorrect()
    {
        // A VARCHAR ORDER BY key is not a monotone-long carrier: the whole operator
        // must keep the boxed comparer (all-or-nothing), and stay correct.
        const string ddl = "CREATE TABLE t (g INT NOT NULL, name VARCHAR NOT NULL, v INT NOT NULL)";
        const string sql =
            "SELECT g, name, v FROM (SELECT g, name, v, ROW_NUMBER() OVER " +
            "(PARTITION BY g ORDER BY name) AS rn FROM t) s WHERE rn <= 2";

        var plan = CompilePlan(ddl, sql);
        var before = TypedPlanCompiler.MonomorphizedTopKOrderKeyCount;
        if (TypedPlanCompiler.TryCompileParallel(
            plan, 1, out var q, null, new CompileOptions { MonomorphizeTopKOrderKey = true }))
        {
            q!.Dispose();
            Assert.Equal(before, TypedPlanCompiler.MonomorphizedTopKOrderKeyCount);
        }

        AssertAllPathsAgree(ddl, sql, 1,
            [Ins("t", 1, "carol", 3), Ins("t", 1, "alice", 1), Ins("t", 1, "bob", 2)],
            [Del("t", 1, "alice", 1)]);
    }

    [Fact]
    public void MonomorphizeGate_DeclinesMixedKeys_WhenOneIsNonCarrier()
    {
        // One carrier + one non-carrier ⇒ all-or-nothing keeps the boxed comparer.
        const string ddl = "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, name VARCHAR NOT NULL)";
        const string sql =
            "SELECT g, v, name FROM (SELECT g, v, name, ROW_NUMBER() OVER " +
            "(PARTITION BY g ORDER BY v DESC, name) AS rn FROM t) s WHERE rn <= 2";

        var plan = CompilePlan(ddl, sql);
        var before = TypedPlanCompiler.MonomorphizedTopKOrderKeyCount;
        if (TypedPlanCompiler.TryCompileParallel(
            plan, 1, out var q, null, new CompileOptions { MonomorphizeTopKOrderKey = true }))
        {
            q!.Dispose();
            Assert.Equal(before, TypedPlanCompiler.MonomorphizedTopKOrderKeyCount);
        }

        AssertAllPathsAgree(ddl, sql, 1,
            [Ins("t", 1, 10, "b"), Ins("t", 1, 10, "a"), Ins("t", 1, 20, "c")],
            [Del("t", 1, 20, "c")]);
    }

    // ---- Output equivalence across shapes -----------------------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void RowNumber_SingleDescKey_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(Emp, RowNumber(), workers,
            [Ins("emp", 1, 100), Ins("emp", 1, 90), Ins("emp", 1, 80), Ins("emp", 2, 5)],
            [Ins("emp", 1, 95), Del("emp", 1, 100)],
            [Ins("emp", 2, 7), Del("emp", 1, 90), Ins("emp", 3, 1)]);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void RowNumber_MultiKey_TheQ9Shape_AllPathsAgree(int workers) =>
        // Two carrier keys of different types with opposite directions — the exact
        // shape that drove §26 (Nexmark q9: price BIGINT DESC, date_time TIMESTAMP ASC).
        AssertAllPathsAgree(
            "CREATE TABLE bid (auction BIGINT NOT NULL, price BIGINT NOT NULL, ts TIMESTAMP NOT NULL)",
            "SELECT auction, price, ts FROM (SELECT auction, price, ts, ROW_NUMBER() OVER " +
            "(PARTITION BY auction ORDER BY price DESC, ts ASC) AS rn FROM bid) s WHERE rn <= 1",
            workers,
            [
                Ins("bid", 1L, 100L, new Timestamp(1 * Sec)),
                Ins("bid", 1L, 100L, new Timestamp(2 * Sec)),
                Ins("bid", 1L, 90L, new Timestamp(0)),
                Ins("bid", 2L, 50L, new Timestamp(5 * Sec)),
            ],
            // Retract the current winner: the tie-broken runner-up must take over.
            [Del("bid", 1L, 100L, new Timestamp(1 * Sec))],
            [Ins("bid", 1L, 500L, new Timestamp(9 * Sec)), Del("bid", 2L, 50L, new Timestamp(5 * Sec))]);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void Rank_TieGroups_AllPathsAgree(int workers) =>
        // RANK keeps a whole tie group; the zero-tiebreak comparer decides group
        // boundaries, so the monomorphized twin must reproduce it exactly.
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)",
            "SELECT g, v, id FROM (SELECT g, v, id, RANK() OVER " +
            "(PARTITION BY g ORDER BY v) AS rn FROM t) s WHERE rn <= 2",
            workers,
            [Ins("t", 1, 10, 1), Ins("t", 1, 10, 2), Ins("t", 1, 10, 3)],
            [Ins("t", 1, 5, 4)],
            [Del("t", 1, 10, 2), Ins("t", 1, 5, 5)]);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void DenseRank_TieGroups_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)",
            "SELECT g, v, id FROM (SELECT g, v, id, DENSE_RANK() OVER " +
            "(PARTITION BY g ORDER BY v) AS rn FROM t) s WHERE rn <= 2",
            workers,
            [Ins("t", 1, 10, 1), Ins("t", 1, 10, 2), Ins("t", 1, 20, 3), Ins("t", 1, 30, 4)],
            [Del("t", 1, 10, 1)],
            [Ins("t", 1, 5, 5), Ins("t", 1, 20, 6)]);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void NullableOrderKey_NullPositionPreserved(int workers) =>
        // NULL position is absolute (never flipped by DESC) — the one place the two
        // comparers could silently disagree.
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, v INT)",
            "SELECT g, v FROM (SELECT g, v, ROW_NUMBER() OVER " +
            "(PARTITION BY g ORDER BY v DESC) AS rn FROM t) s WHERE rn <= 2",
            workers,
            [Ins("t", 1, null), Ins("t", 1, 10), Ins("t", 1, 20), Ins("t", 2, null)],
            [Del("t", 1, 20)],
            [Ins("t", 1, null), Ins("t", 2, 5)]);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void DateKey_AllPathsAgree(int workers) =>
        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, d DATE NOT NULL)",
            "SELECT g, d FROM (SELECT g, d, ROW_NUMBER() OVER " +
            "(PARTITION BY g ORDER BY d DESC) AS rn FROM t) s WHERE rn <= 2",
            workers,
            [Ins("t", 1, new Date32(120)), Ins("t", 1, new Date32(1)), Ins("t", 1, new Date32(240))],
            [Del("t", 1, new Date32(240))],
            [Ins("t", 1, new Date32(-365)), Ins("t", 2, new Date32(33))]);

    [Fact]
    public void Randomized_MonoEqualsBoxedAndStructural()
    {
        // Randomized insert/delete churn over small partitions with heavy tie
        // pressure (v drawn from a tiny domain), the case most likely to expose a
        // tiebreak divergence between the two comparers.
        var rng = new Random(20260831);
        var live = new List<object?[]>();
        var ticks = new List<Mut[]>();
        for (var t = 0; t < 25; t++)
        {
            var tick = new List<Mut>();
            for (var i = 0; i < 6; i++)
            {
                if (live.Count > 0 && rng.Next(100) < 35)
                {
                    var idx = rng.Next(live.Count);
                    tick.Add(new Mut("t", live[idx], -1));
                    live.RemoveAt(idx);
                }
                else
                {
                    var row = new object?[] { rng.Next(1, 4), rng.Next(1, 5), rng.Next(1, 1000) };
                    live.Add(row);
                    tick.Add(new Mut("t", row, 1));
                }
            }

            ticks.Add(tick.ToArray());
        }

        AssertAllPathsAgree(
            "CREATE TABLE t (g INT NOT NULL, v INT NOT NULL, id INT NOT NULL)",
            "SELECT g, v, id FROM (SELECT g, v, id, ROW_NUMBER() OVER " +
            "(PARTITION BY g ORDER BY v DESC, id ASC) AS rn FROM t) s WHERE rn <= 3",
            1,
            ticks.ToArray());
    }
}
