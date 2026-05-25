// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// End-to-end Phase-3 tests: <c>LATENESS</c> parsed on a column drives the
/// monotonicity analyzer and wires the aggregate's frontier-driven GC, bounding
/// state on a monotone stream. Covers both the BIGINT logical-time carrier and
/// the headline TIMESTAMP carrier, frontier propagation through a WHERE, the
/// input late-row drop, and resolver validation.
/// </summary>
public class LatenessSqlTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    // Structural compile is forced by LATENESS, so the aggregate is the
    // StructuralRow-keyed flat operator; finding it also confirms structural
    // compile engaged (the typed path would produce a different operator type).
    private static int RetainedGroups(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single()
            .RetainedGroupCount;

    [Fact]
    public void GroupByMonotoneBigint_BoundsAggregateState()
    {
        var q = Compile(
            ["CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)"],
            "SELECT ts, COUNT(*) FROM events GROUP BY ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("events").Insert(t, 1);
            q.Step();
        }

        // 201 distinct group keys streamed, but the frontier (max − 10) keeps
        // only the trailing window [190, 200] = 11 groups.
        Assert.Equal(11, RetainedGroups(q));
        // The just-emitted final group is in the latest delta.
        Assert.Equal(1, q.WeightOf(200L, 1L).Value);
    }

    [Fact]
    public void GroupByMonotoneTimestamp_BoundsState()
    {
        // LATENESS 10_000_000 µs = 10 s; timestamps one second apart.
        var q = Compile(
            ["CREATE TABLE events (ts TIMESTAMP NOT NULL LATENESS 10000000, v INT NOT NULL)"],
            "SELECT ts, COUNT(*) FROM events GROUP BY ts");

        for (long i = 0; i < 100; i++)
        {
            q.Table("events").Insert(new Timestamp(i * 1_000_000L), 1);
            q.Step();
        }

        // max = 99 s; frontier = 89 s; retained ts ∈ [89 s, 99 s] = 11 groups.
        Assert.Equal(11, RetainedGroups(q));
        Assert.Equal(1, q.WeightOf(new Timestamp(99_000_000L), 1L).Value);
    }

    [Fact]
    public void FrontierPropagatesThroughWhere()
    {
        var q = Compile(
            ["CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)"],
            "SELECT ts, COUNT(*) FROM events WHERE v > 0 GROUP BY ts");

        for (long t = 0; t <= 50; t++)
        {
            q.Table("events").Insert(t, 1);
            q.Step();
        }

        // The frontier survived the intervening filter, so the GROUP BY still GCs.
        Assert.Equal(11, RetainedGroups(q));
    }

    [Fact]
    public void LateRowDroppedAtInput_DoesNotResurrectGroup()
    {
        var q = Compile(
            ["CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)"],
            "SELECT ts, COUNT(*) FROM events GROUP BY ts");

        for (long t = 0; t <= 30; t++)
        {
            q.Table("events").Insert(t, 1);
            q.Step();
        }

        Assert.Equal(11, RetainedGroups(q)); // [20, 30]

        // A late row (ts = 5 < frontier 20) is dropped at the input: no output,
        // and the already-collected group 5 is not resurrected.
        q.Table("events").Insert(5, 1);
        q.Step();
        Assert.True(q.Current.IsEmpty);
        Assert.Equal(11, RetainedGroups(q));
    }

    [Fact]
    public void Lateness_OnNullableColumn_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Compile(["CREATE TABLE t (ts BIGINT LATENESS 10)"], "SELECT ts FROM t"));
        Assert.Contains("NOT NULL", ex.Message, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Lateness_OnVarcharColumn_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Compile(["CREATE TABLE t (s VARCHAR NOT NULL LATENESS 10)"], "SELECT s FROM t"));
        Assert.Contains("integer or temporal", ex.Message, System.StringComparison.OrdinalIgnoreCase);
    }
}
