// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for window aggregates — <c>SUM/COUNT/AVG/MIN/MAX(x) OVER
/// (PARTITION BY p [ORDER BY o RANGE …])</c> emitted as a new output column.
/// Split into parser (grammar), resolver (recognition / rejection), and
/// behavioural (incremental) sections.
/// </summary>
public class WindowAggregateTests
{
    // ---- Parser: window frame grammar ---------------------------------------

    private static WindowFunctionExpression ParseWindow(string query)
    {
        var stmt = (SelectStatement)Parser.ParseStatement(query);
        var item = (ExpressionSelectItem)stmt.Items[^1];
        return (WindowFunctionExpression)item.Expression;
    }

    private static WindowSpec ParseWindowSpec(string query) => ParseWindow(query).Over;

    [Fact]
    public void Parse_NoFrame_LeavesFrameNull()
    {
        var over = ParseWindowSpec("SELECT a, SUM(b) OVER (PARTITION BY a ORDER BY ts) FROM t");
        Assert.Null(over.Frame);
        Assert.Single(over.PartitionBy);
        Assert.Single(over.OrderBy);
    }

    [Fact]
    public void Parse_RangeBetweenUnboundedAndCurrentRow()
    {
        var over = ParseWindowSpec(
            "SELECT a, SUM(b) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t");
        Assert.NotNull(over.Frame);
        Assert.Equal(WindowFrameMode.Range, over.Frame!.Mode);
        Assert.Equal(FrameBoundKind.UnboundedPreceding, over.Frame.Start.Kind);
        Assert.Equal(FrameBoundKind.CurrentRow, over.Frame.End.Kind);
    }

    [Fact]
    public void Parse_RangeIntervalPrecedingShorthand()
    {
        var over = ParseWindowSpec(
            "SELECT a, MAX(b) OVER (ORDER BY ts RANGE INTERVAL '1' DAY PRECEDING) FROM t");
        Assert.NotNull(over.Frame);
        Assert.Equal(FrameBoundKind.Preceding, over.Frame!.Start.Kind);
        // INTERVAL '1' DAY desugars to CAST('1' AS INTERVAL DAY).
        Assert.IsType<CastExpression>(over.Frame.Start.Offset);
        // Single-bound shorthand implies CURRENT ROW upper bound.
        Assert.Equal(FrameBoundKind.CurrentRow, over.Frame.End.Kind);
    }

    [Fact]
    public void Parse_RangeConstPreceding()
    {
        var over = ParseWindowSpec(
            "SELECT a, COUNT(*) OVER (ORDER BY n RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) FROM t");
        Assert.Equal(FrameBoundKind.Preceding, over.Frame!.Start.Kind);
        Assert.IsType<LiteralExpression>(over.Frame.Start.Offset);
    }

    [Fact]
    public void Parse_RowsModeIsCarried()
    {
        var over = ParseWindowSpec(
            "SELECT a, SUM(b) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM t");
        Assert.Equal(WindowFrameMode.Rows, over.Frame!.Mode);
    }

    [Fact]
    public void Parse_CountStarOver()
    {
        var win = ParseWindow("SELECT a, COUNT(*) OVER (PARTITION BY a) FROM t");
        Assert.True(win.IsStar);
        Assert.Equal("count", win.FunctionName);
    }

    [Fact]
    public void Parse_BadFrameBound_Throws() =>
        Assert.ThrowsAny<System.Exception>(() => ParseWindowSpec(
            "SELECT a, SUM(b) OVER (ORDER BY ts RANGE BETWEEN UNBOUNDED AND CURRENT ROW) FROM t"));

    // ---- Resolver: recognition + rejection ----------------------------------

    private const string Orders =
        "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL, ts TIMESTAMP NOT NULL)";

    private static LogicalPlan ResolvePlan(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static WindowAggregatePlan WindowPlanOf(string ddl, string query)
    {
        var plan = ResolvePlan(ddl, query);
        var proj = Assert.IsType<ProjectPlan>(plan);
        return Assert.IsType<WindowAggregatePlan>(proj.Input);
    }

    private static void Rejects(string query) =>
        Assert.Throws<ResolveException>(() => ResolvePlan(Orders, query));

    [Fact]
    public void Resolve_WholePartition_NoOrderKey()
    {
        var wp = WindowPlanOf(Orders, "SELECT cust, SUM(amount) OVER (PARTITION BY cust) AS t FROM orders");
        Assert.Null(wp.OrderKey);
        Assert.Null(wp.Frame);
        Assert.Single(wp.PartitionKeys);
        Assert.Single(wp.Aggregates);
        Assert.Equal(AggregateKind.Sum, wp.Aggregates[0].Kind);
    }

    [Fact]
    public void Resolve_Running_DefaultFrameIsUnboundedPreceding()
    {
        var wp = WindowPlanOf(Orders,
            "SELECT ts, SUM(amount) OVER (PARTITION BY cust ORDER BY ts) AS run FROM orders");
        Assert.NotNull(wp.OrderKey);
        Assert.NotNull(wp.Frame);
        Assert.Null(wp.Frame!.Preceding); // UNBOUNDED PRECEDING (running).
    }

    [Fact]
    public void Resolve_BoundedInterval_OffsetInMicros()
    {
        var wp = WindowPlanOf(Orders,
            "SELECT ts, MAX(amount) OVER (PARTITION BY cust ORDER BY ts " +
            "RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS m FROM orders");
        Assert.Equal(86_400_000_000L, wp.Frame!.Preceding);
        Assert.Equal(AggregateKind.Max, wp.Aggregates[0].Kind);
    }

    [Fact]
    public void Resolve_MultipleAggregates_SharedSpec()
    {
        var wp = WindowPlanOf(Orders,
            "SELECT cust, SUM(amount) OVER (PARTITION BY cust) AS s, " +
            "COUNT(*) OVER (PARTITION BY cust) AS c FROM orders");
        Assert.Equal(2, wp.Aggregates.Count);
        Assert.Equal(AggregateKind.Sum, wp.Aggregates[0].Kind);
        Assert.Equal(AggregateKind.CountStar, wp.Aggregates[1].Kind);
    }

    [Fact]
    public void Rejects_RowsFrame() => Rejects(
        "SELECT ts, SUM(amount) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM orders");

    [Fact]
    public void Rejects_FollowingBound() => Rejects(
        "SELECT ts, SUM(amount) OVER (ORDER BY ts RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM orders");

    [Fact]
    public void Rejects_YearIntervalOffset() => Rejects(
        "SELECT ts, SUM(amount) OVER (ORDER BY ts RANGE INTERVAL '1' YEAR PRECEDING) FROM orders");

    [Fact]
    public void Resolve_StarWithWindow_ExpandsBaseColumns()
    {
        // `SELECT *, agg OVER (…)` is now supported — the star covers the base
        // columns (cust, amount, ts), the window result is appended.
        var plan = ResolvePlan(Orders,
            "SELECT *, SUM(amount) OVER (PARTITION BY cust) AS s FROM orders");
        var proj = Assert.IsType<ProjectPlan>(plan);
        Assert.IsType<WindowAggregatePlan>(proj.Input);
        // 3 base columns + 1 window result.
        Assert.Equal(4, plan.Schema.Count);
    }

    [Fact]
    public void Rejects_WindowInWhere() => Rejects(
        "SELECT cust FROM orders WHERE SUM(amount) OVER (PARTITION BY cust) > 5");

    [Fact]
    public void Rejects_DeferredWindowFunction() => Rejects(
        "SELECT ts, NTILE(4) OVER (PARTITION BY cust ORDER BY ts) FROM orders");

    [Fact]
    public void Resolve_DifferentSpecs_ChainsTwoNodes()
    {
        // Two distinct OVER specs (PARTITION BY cust vs PARTITION BY ts) lower to
        // two chained WindowAggregatePlan nodes — one operator per spec.
        var plan = ResolvePlan(Orders,
            "SELECT SUM(amount) OVER (PARTITION BY cust) AS s, " +
            "SUM(amount) OVER (PARTITION BY ts) AS s2 FROM orders");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var outer = Assert.IsType<WindowAggregatePlan>(proj.Input);
        var inner = Assert.IsType<WindowAggregatePlan>(outer.Input);
        Assert.Single(outer.Aggregates);
        Assert.Single(inner.Aggregates);
        Assert.Single(inner.PartitionKeys); // first group: PARTITION BY cust.
        Assert.Single(outer.PartitionKeys); // second group: PARTITION BY ts.
    }

    [Fact]
    public void Resolve_MixedFamilies_ChainsAggregateAndOffset()
    {
        // An aggregate and an offset function may now coexist — each becomes its
        // own node. SUM occurs first (aggregate group), LAG second (offset group),
        // so the offset node is chained outermost.
        var plan = ResolvePlan(Orders,
            "SELECT SUM(amount) OVER (PARTITION BY cust ORDER BY ts) AS s, " +
            "LAG(amount) OVER (PARTITION BY cust ORDER BY ts) AS p FROM orders");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var off = Assert.IsType<WindowOffsetPlan>(proj.Input);
        var agg = Assert.IsType<WindowAggregatePlan>(off.Input);
        Assert.Single(off.Functions);
        Assert.Single(agg.Aggregates);
    }

    [Fact]
    public void Resolve_NestedWindow_LiftsToHiddenColumn()
    {
        // A window function nested in an expression (here arithmetic) is lifted
        // to a hidden column; the outer ProjectPlan wraps the WindowAggregatePlan.
        var plan = ResolvePlan(Orders,
            "SELECT cust, 1 + SUM(amount) OVER (PARTITION BY cust) AS s FROM orders");
        var proj = Assert.IsType<ProjectPlan>(plan);
        Assert.IsType<WindowAggregatePlan>(proj.Input);
    }

    // ---- Behavioural: incremental evaluation ---------------------------------

    private const string S = "CREATE TABLE s (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)";

    private static CompiledQuery Compile(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan); // window aggregates compile structurally.
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---- SELECT * alongside a window function ----

    [Fact]
    public void SelectStar_WithWindow_ExpandsBaseColumnsOnly()
    {
        // dim_customer shape: `SELECT *, agg(x) OVER w`. The * expands to the
        // BASE columns (g, ts, v), NOT the hidden window-result column.
        var q = Compile(S,
            "SELECT *, COUNT(v) OVER (PARTITION BY g) AS cnt FROM s");
        q.Table("s").Insert(1, 10, 100);
        q.Table("s").Insert(1, 20, 200);
        q.Step();
        // Output = base (g, ts, v) + cnt; two rows in g=1 → cnt 2.
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 100, 2L));
        Assert.Equal(1, WeightOf(q.Current, 1, 20, 200, 2L));
    }

    [Fact]
    public void SelectStar_WithNamedWindow_DimCustomerShape()
    {
        // The exact dim_customer form: SELECT *, COUNT(col) OVER w … WINDOW w AS (…).
        var q = Compile(S,
            "SELECT *, COUNT(v) OVER w AS grp FROM s WINDOW w AS (PARTITION BY g ORDER BY ts)");
        q.Table("s").Insert(1, 10, 100);
        q.Table("s").Insert(1, 20, 200);
        q.Step();
        // Running COUNT over ts: 1, then 2. Star gives base cols only.
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 100, 1L));
        Assert.Equal(1, WeightOf(q.Current, 1, 20, 200, 2L));
    }

    // ---- Named WINDOW clause (parser substitutes the definition) ----

    [Fact]
    public void NamedWindow_SubstitutesDefinition()
    {
        // `OVER w` resolves to the WINDOW-clause definition, even though the
        // clause is lexically after the SELECT list that references it.
        var stmt = (SelectStatement)Parser.ParseStatement(
            "SELECT g, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi FROM s " +
            "WINDOW w AS (PARTITION BY g ORDER BY ts)");
        var lo = (WindowFunctionExpression)((ExpressionSelectItem)stmt.Items[1]).Expression;
        Assert.Null(lo.Over.Name);                    // substituted, not a placeholder
        Assert.Single(lo.Over.PartitionBy);
        Assert.Single(lo.Over.OrderBy);
    }

    [Fact]
    public void NamedWindow_EndToEnd_MatchesInlineSpec()
    {
        // daily_market shape: cumulative MIN/MAX over a named window.
        var q = Compile(S,
            "SELECT g, ts, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi FROM s " +
            "WINDOW w AS (PARTITION BY g ORDER BY ts)");
        q.Table("s").Insert(1, 1, 50);   // g, ts, v
        q.Table("s").Insert(1, 2, 30);
        q.Table("s").Insert(1, 3, 70);
        q.Step();
        // Output (g, ts, lo, hi); running MIN/MAX over ts order.
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 50, 50));
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 30, 50));
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 30, 70));
    }

    [Fact]
    public void NamedWindow_NestedInExpression()
    {
        // Named window referenced from inside a CASE (combines with the lift).
        var q = Compile(S,
            "SELECT g, v, CASE WHEN v = MAX(v) OVER w THEN 1 ELSE 0 END AS is_max FROM s " +
            "WINDOW w AS (PARTITION BY g)");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 2, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 0));
        Assert.Equal(1, WeightOf(q.Current, 1, 30, 1));
    }

    [Fact]
    public void NamedWindow_UndefinedName_Throws()
    {
        Assert.Throws<ParseException>(() => Parser.ParseStatement(
            "SELECT g, MIN(v) OVER w FROM s WINDOW x AS (PARTITION BY g)"));
    }

    [Fact]
    public void NamedWindow_DuplicateDefinition_Throws()
    {
        Assert.Throws<ParseException>(() => Parser.ParseStatement(
            "SELECT g, MIN(v) OVER w FROM s WINDOW w AS (PARTITION BY g), w AS (PARTITION BY ts)"));
    }

    // ---- Window function nested in an expression (lifted to a hidden column) ----

    [Fact]
    public void WindowAggregate_NestedInCase_IsCurrentMarker()
    {
        // The TPC-DI SCD2 `is_current` shape: MAX(ts) OVER (PARTITION BY g)
        // nested inside a CASE, compared to the row's own ts.
        var q = Compile(S,
            "SELECT g, v, CASE WHEN ts = MAX(ts) OVER (PARTITION BY g) THEN 1 ELSE 0 END AS is_cur FROM s");
        q.Table("s").Insert(1, 10, 100);   // ts 10
        q.Table("s").Insert(1, 30, 300);   // ts 30 = max for g=1 → current
        q.Table("s").Insert(2, 5, 50);     // ts 5 = max for g=2 → current
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100, 0));   // ts 10 ≠ max 30
        Assert.Equal(1, WeightOf(q.Current, 1, 300, 1));   // ts 30 = max
        Assert.Equal(1, WeightOf(q.Current, 2, 50, 1));    // ts 5 = max
    }

    [Fact]
    public void OffsetFunction_NestedInCoalesce_EndTimestampShape()
    {
        // The SCD2 `end_timestamp` shape: COALESCE(LAG(ts) OVER (...), default).
        var q = Compile(S,
            "SELECT g, ts, COALESCE(LAG(ts) OVER (PARTITION BY g ORDER BY ts DESC), -1) AS end_ts FROM s");
        q.Table("s").Insert(1, 10, 0);
        q.Table("s").Insert(1, 20, 0);
        q.Table("s").Insert(1, 30, 0);
        q.Step();
        // DESC order: 30, 20, 10. LAG = the previous (later-ts) row, or -1 at the top.
        Assert.Equal(1, WeightOf(q.Current, 1, 30, -1));
        Assert.Equal(1, WeightOf(q.Current, 1, 20, 30));
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 20));
    }

    [Fact]
    public void OffsetFunction_NestedInIsNull_TradesHistoryShape()
    {
        // trades_history is-current: LAG(id) OVER (...) IS NULL marks the newest.
        var q = Compile(S,
            "SELECT g, ts, CASE WHEN LAG(v) OVER (PARTITION BY g ORDER BY ts DESC) IS NULL THEN 1 ELSE 0 END AS newest FROM s");
        q.Table("s").Insert(1, 10, 111);
        q.Table("s").Insert(1, 30, 333);   // newest (highest ts) → LAG is null
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 30, 1));
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 0));
    }

    [Fact]
    public void WindowFunctions_TwoNestedInOneQuery_SharedBase()
    {
        // The full SCD2 pair: an offset (end_ts) and an aggregate (is_current),
        // each nested in its own expression, in one SELECT.
        var q = Compile(S,
            "SELECT g, ts, " +
            "COALESCE(LAG(ts) OVER (PARTITION BY g ORDER BY ts DESC), -1) AS end_ts, " +
            "CASE WHEN ts = MAX(ts) OVER (PARTITION BY g) THEN 1 ELSE 0 END AS is_cur FROM s");
        q.Table("s").Insert(1, 10, 0);
        q.Table("s").Insert(1, 30, 0);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 30, -1, 1));   // newest: end_ts -1, current
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 30, 0));   // older: end_ts 30, not current
    }

    [Fact]
    public void RankFunction_NestedInExpression_LiftsToRankColumn()
    {
        // financials shape: ROW_NUMBER() OVER (...) = 1 in a CASE. Rank-in-output
        // is now supported generally — the rank is lifted to a hidden column and
        // the CASE reads it (see PartitionedRankTests for the full coverage).
        var q = Compile(S,
            "SELECT g, CASE WHEN ROW_NUMBER() OVER (PARTITION BY g ORDER BY ts) = 1 THEN 1 ELSE 0 END AS c FROM s");
        q.Table("s").Insert(1, 10, 0);   // earliest ts → row_number 1 → c = 1
        q.Table("s").Insert(1, 30, 0);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 1, 0));
    }

    [Fact]
    public void WindowFunction_InWhere_StillRejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(S,
            "SELECT g, v FROM s WHERE MAX(ts) OVER (PARTITION BY g) > 5"));
        Assert.Contains("WHERE", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WholePartition_Sum_BroadcastsAndUpdates()
    {
        var q = Compile(S, "SELECT g, v, SUM(v) OVER (PARTITION BY g) AS t FROM s");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 2, 20);
        q.Table("s").Insert(2, 9, 5);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 10, 30L));
        Assert.Equal(1, WeightOf(q.Current, 1, 20, 30L));
        Assert.Equal(1, WeightOf(q.Current, 2, 5, 5L));

        // A new row in partition 1 retracts both old broadcasts and re-emits.
        q.Table("s").Insert(1, 3, 5);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 10, 30L));
        Assert.Equal(-1, WeightOf(q.Current, 1, 20, 30L));
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 35L));
        Assert.Equal(1, WeightOf(q.Current, 1, 20, 35L));
        Assert.Equal(1, WeightOf(q.Current, 1, 5, 35L));
        // Partition 2 is untouched — no delta for it.
        Assert.Equal(0, WeightOf(q.Current, 2, 5, 5L));
    }

    [Fact]
    public void Running_Sum_CumulativeByOrder()
    {
        var q = Compile(S, "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM s");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 2, 20);
        q.Table("s").Insert(1, 3, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10L));
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 30L));
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 60L));
    }

    [Fact]
    public void BoundedRange_Sum_SlidingWindow()
    {
        var q = Compile(S,
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts " +
            "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll FROM s");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 2, 20);
        q.Table("s").Insert(1, 3, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10L)); // frame [0,1] = {10}
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 30L)); // frame [1,2] = {10,20}
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 50L)); // frame [2,3] = {20,30}
    }

    [Fact]
    public void BoundedRange_Peers_ShareFrame()
    {
        var q = Compile(S,
            "SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts " +
            "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll FROM s");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 1, 5); // peer of the first row (equal ts)
        q.Table("s").Insert(1, 2, 20);
        q.Step();
        // ts=1 peers both see frame [0,1] = {10,5} = 15.
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10, 15L));
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 5, 15L));
        // ts=2 sees [1,2] = {10,5,20} = 35.
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 20, 35L));
    }

    [Fact]
    public void WholePartition_CountStar()
    {
        var q = Compile(S, "SELECT g, COUNT(*) OVER (PARTITION BY g) AS c FROM s");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 2, 20);
        q.Step();
        // Both base rows project to (g=1, c=2), so they collapse to weight 2.
        Assert.Equal(2, WeightOf(q.Current, 1, 2L));
    }

    [Fact]
    public void TwoDistinctSpecs_BothColumnsComputed()
    {
        // A whole-partition SUM and a running SUM over the same partition are two
        // distinct OVER specs — each its own chained operator, both columns landing
        // on every row.
        var q = Compile(S,
            "SELECT g, ts, SUM(v) OVER (PARTITION BY g) AS tot, " +
            "SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM s");
        q.Table("s").Insert(1, 1, 10);
        q.Table("s").Insert(1, 2, 20);
        q.Table("s").Insert(1, 3, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 60L, 10L)); // tot=60, run=10
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 60L, 30L)); // tot=60, run=30
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 60L, 60L)); // tot=60, run=60
    }

    // ---- Bounded-frame LATENESS GC -------------------------------------------

    private static int RetainedRows(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<PartitionedWindowAggregateOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow>>()
            .Single()
            .RetainedRowCount;

    [Fact]
    public void BoundedRange_OverLatenessKey_BoundsState()
    {
        // ORDER BY a LATENESS column with a bounded RANGE frame: the operator GCs
        // rows below (frontier − preceding). Frontier = max_seen − 10; preceding
        // = 5, so after streaming ts 0..200 only ts in [185, 200] (16 rows) remain.
        var q = Compile(
            "CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)",
            "SELECT ts, SUM(v) OVER (ORDER BY ts RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS roll FROM events");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("events").Insert(t, 1);
            q.Step();
        }

        Assert.Equal(16, RetainedRows(q));
    }

    // ---- Randomized incremental ≡ batch (the test of record) -----------------

    private const string W = "CREATE TABLE w (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)";

    [Theory]
    [InlineData("SELECT g, v, SUM(v) OVER (PARTITION BY g) AS s FROM w")]
    [InlineData("SELECT g, COUNT(*) OVER (PARTITION BY g) AS c FROM w")]
    [InlineData("SELECT g, ts, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS s FROM w")]
    [InlineData("SELECT g, ts, COUNT(*) OVER (PARTITION BY g ORDER BY ts) AS c FROM w")]
    [InlineData("SELECT ts, AVG(v) OVER (ORDER BY ts) AS a FROM w")]
    [InlineData("SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM w")]
    [InlineData("SELECT g, ts, v, MIN(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM w")]
    [InlineData("SELECT g, ts, v, MAX(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM w")]
    // Multiple distinct OVER specs in one query — each a chained operator.
    [InlineData("SELECT g, ts, v, SUM(v) OVER (PARTITION BY g) AS tot, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run FROM w")]
    [InlineData("SELECT g, ts, v, SUM(v) OVER (PARTITION BY g) AS sg, COUNT(*) OVER (PARTITION BY ts) AS ct FROM w")]
    [InlineData("SELECT g, ts, v, MAX(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS m, SUM(v) OVER (PARTITION BY g) AS s FROM w")]
    public void IncrementalEqualsBatch_RandomInsertsAndDeletes(string query)
    {
        for (var seed = 0; seed < 12; seed++)
        {
            AssertIncrementalEqualsBatch(W, query, seed, monotonicTs: false);
        }
    }

    [Theory]
    // Bounded RANGE over a LATENESS key exercises frontier-driven GC: the
    // accumulated incremental output must still equal the batch over all rows.
    [InlineData("SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM w")]
    [InlineData("SELECT g, ts, v, MAX(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM w")]
    public void IncrementalEqualsBatch_UnderLatenessGc(string query)
    {
        const string ddl = "CREATE TABLE w (g INT NOT NULL, ts BIGINT NOT NULL LATENESS 3, v INT NOT NULL)";
        for (var seed = 0; seed < 12; seed++)
        {
            AssertIncrementalEqualsBatch(ddl, query, seed, monotonicTs: true);
        }
    }

    private static void AssertIncrementalEqualsBatch(string ddl, string query, int seed, bool monotonicTs)
    {
        var rng = new Random(seed);
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        var compiled = PlanToCircuit.Compile(plan);

        var present = new List<object?[]>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
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
                    // Box ts at its declared column type: BIGINT (long) when the
                    // monotonic-LATENESS DDL is used, INT otherwise. A bare ternary
                    // would unify both arms to long and box an INT column as long —
                    // harmless on the untyped structural path, but the typed scan
                    // lift (now reached for PARTITION BY windows) enforces the schema.
                    object tsVal = monotonicTs ? nextTs++ : (object)rng.Next(0, 8);
                    var v = rng.Next(0, 5);
                    var row = new object?[] { g, tsVal, v };
                    present.Add(row);
                    tick.Add(new InputEvent("w", row, 1));
                }
            }

            ticks.Add(tick);
        }

        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, ticks);

        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["w"] = IncrementalOracle.NetTable(ticks.SelectMany(x => x), "w"),
        };
        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        Assert.True(
            accumulated.Equals(batch),
            $"seed={seed} query={query}\n  accumulated={accumulated}\n  batch={batch}");
    }
}
