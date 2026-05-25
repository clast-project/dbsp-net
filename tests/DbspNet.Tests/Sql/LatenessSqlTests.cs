// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;
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

    private static CompiledQuery CompileSpine(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, snapshotCodecs: null, new CompileOptions { TraceFamily = TraceFamily.Spine });
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

    private static int JoinRetainedKeys(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>()
            .Single()
            .RetainedKeyCount;

    [Fact]
    public void InnerJoinOnMonotoneKey_BoundsBothTraces()
    {
        // Both join inputs declare LATENESS on the equi-key, so the join may GC
        // both traces (no future delta arrives below the frontier on either side).
        var q = Compile(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        // Both traces keep only the trailing window [190, 200] = 11 keys each.
        Assert.Equal(22, JoinRetainedKeys(q));
        Assert.Equal(1, q.WeightOf(200, 200).Value);
    }

    [Fact]
    public void InnerJoinOnMonotoneKey_BoundsBothTraces_Spine()
    {
        var q = CompileSpine(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        var op = q.Circuit.Operators
            .OfType<SpineIncrementalJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>()
            .Single();
        Assert.Equal(22, op.RetainedKeyCount);
        Assert.Equal(1, q.WeightOf(200, 200).Value);
    }

    [Fact]
    public void InnerJoin_OneSideNotMonotone_DoesNotGc()
    {
        // Only a.ts declares LATENESS; b.ts does not. A future b row could arrive
        // at any key, so neither trace can be collected — state grows with input.
        var q = Compile(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 50; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        // No GC: both traces retain all 51 keys (102 total).
        Assert.Equal(102, JoinRetainedKeys(q));
    }

    private static int LeftJoinRetainedKeys(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>()
            .Single()
            .RetainedKeyCount;

    [Fact]
    public void LeftJoinOnMonotoneKey_BoundsBothTraces()
    {
        // Both sides declare LATENESS on the equi-key. For an OUTER join GC needs
        // BOTH sides monotone (not just the preserved left side): a future row on
        // a non-monotone right side could still flip a left key from unmatched to
        // matched below the frontier. With both bounded, the join GCs both traces.
        var q = Compile(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a LEFT JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        // Trailing window [190, 200] = 11 keys per trace.
        Assert.Equal(22, LeftJoinRetainedKeys(q));
        Assert.Equal(1, q.WeightOf(200, 200).Value); // matched row in the final delta
    }

    [Fact]
    public void LeftJoinOnMonotoneKey_BoundsBothTraces_Spine()
    {
        var q = CompileSpine(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a LEFT JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        var op = q.Circuit.Operators
            .OfType<SpineIncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>>()
            .Single();
        Assert.Equal(22, op.RetainedKeyCount);
        Assert.Equal(1, q.WeightOf(200, 200).Value);
    }

    [Fact]
    public void LeftJoin_UnmatchedRowsEmittedNullPadded_StateStillBounded()
    {
        // a keys are even, b keys are odd — they never match, so every left row is
        // emitted NULL-padded. b still receives rows, so its frontier advances and
        // the both-sides license holds. Each NULL-padded row, once below the
        // frontier, is final; GC drops its key from the left trace and emits
        // nothing — the already-emitted NULL-pad survives downstream.
        var q = Compile(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a LEFT JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 25; t++)
        {
            q.Table("a").Insert(2 * t, (int)(2 * t));     // even key
            q.Table("b").Insert(2 * t + 1, (int)(2 * t)); // odd key — never matches a
            q.Step();
        }

        // Final tick (t=25): a.ts=50, b.ts=51, frontier = min(50,51) − 10 = 40.
        // Left trace keeps even keys ≥ 40: {40,42,44,46,48,50} = 6; right trace
        // keeps odd keys ≥ 40: {41,43,45,47,49,51} = 6. Total 12 (vs 52 ungc'd).
        Assert.Equal(12, LeftJoinRetainedKeys(q));

        // The final left row (x=50) is unmatched → NULL-padded, with no spurious
        // matched row. The GC of older keys emits no retraction into this delta.
        Assert.Equal(1, q.WeightOf(50, null).Value);
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void RightJoinOnMonotoneKey_BoundsBothTraces()
    {
        // RIGHT JOIN compiles to IncrementalLeftJoinOp with the physical sides
        // swapped; the both-sides-monotone license and join-key frontier apply
        // identically. The same operator GCs both (swapped) traces.
        var q = Compile(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a RIGHT JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        Assert.Equal(22, LeftJoinRetainedKeys(q));
        Assert.Equal(1, q.WeightOf(200, 200).Value);
    }

    [Fact]
    public void LeftJoin_OneSideNotMonotone_DoesNotGc()
    {
        // Only the preserved (left) side declares LATENESS. The analyzer would
        // mark the left key monotone, but the stricter join license needs BOTH
        // sides — a future b row could flip an old left key from unmatched to
        // matched — so neither trace is collected and state grows with input.
        var q = Compile(
            [
                "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
                "CREATE TABLE b (ts BIGINT NOT NULL, y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a LEFT JOIN b ON a.ts = b.ts");

        for (long t = 0; t <= 50; t++)
        {
            q.Table("a").Insert(t, (int)t);
            q.Table("b").Insert(t, (int)t);
            q.Step();
        }

        Assert.Equal(102, LeftJoinRetainedKeys(q)); // 51 keys per trace, no GC
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
