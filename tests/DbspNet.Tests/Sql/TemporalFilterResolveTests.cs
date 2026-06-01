// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase-1 (resolve-only) coverage for the temporal-filter feature: NOW() /
/// CURRENT_TIMESTAMP recognised in sanctioned WHERE predicates and folded into
/// a <see cref="TemporalFilterPlan"/>, and rejected everywhere else. Operator
/// compilation and incremental semantics land in later phases.
/// </summary>
public class TemporalFilterResolveTests
{
    private const long MicrosPerHour = 3_600_000_000L;
    private const long MicrosPerDay = 86_400_000_000L;

    private static LogicalPlan Resolve(string where)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL)"));
        var stmt = Parser.ParseStatement($"SELECT id, ts FROM events WHERE {where}");
        return ((SelectPlan)resolver.Resolve(stmt)).Query;
    }

    private static ResolveException ResolveError(string where) =>
        Assert.Throws<ResolveException>(() => Resolve(where));

    // The query is `SELECT id, ts ...`, so a ProjectPlan sits on top of the
    // temporal filter(s). Peel it to reach the (possibly stacked) filters.
    private static TemporalFilterPlan SingleTemporalFilter(LogicalPlan plan)
    {
        var inner = plan is ProjectPlan p ? p.Input : plan;
        return Assert.IsType<TemporalFilterPlan>(inner);
    }

    // ---------- Parser ----------

    [Fact]
    public void Parser_Now_ParsesToNowExpression()
    {
        var expr = new Parser(Lexer.Tokenize("NOW()")).ParseExpression();
        Assert.Equal(NowFunction.Now, Assert.IsType<NowExpression>(expr).Function);
    }

    [Fact]
    public void Parser_CurrentTimestamp_ParsesToNowExpression()
    {
        var expr = new Parser(Lexer.Tokenize("CURRENT_TIMESTAMP")).ParseExpression();
        Assert.Equal(NowFunction.CurrentTimestamp, Assert.IsType<NowExpression>(expr).Function);
    }

    [Fact]
    public void Parser_NowWithoutParens_StaysAColumnReference()
    {
        // A bare `now` (no arg list) is still a column reference, so existing
        // identifiers named "now" keep working.
        var expr = new Parser(Lexer.Tokenize("now")).ParseExpression();
        var col = Assert.IsType<ColumnReference>(expr);
        Assert.Equal("now", col.Name);
    }

    // ---------- Single-bound recognition ----------

    [Fact]
    public void UpperBoundOnNow_BecomesAppearBound()
    {
        // ts <= NOW(): the row appears (inclusive) at its own timestamp and
        // never disappears.
        var tf = SingleTemporalFilter(Resolve("ts <= NOW()"));
        Assert.Equal(0, tf.AppearOffsetMicros);
        Assert.True(tf.AppearInclusive);
        Assert.Null(tf.DisappearOffsetMicros);
        Assert.IsType<ScanPlan>(tf.Input);
    }

    [Fact]
    public void LowerBoundOnNow_BecomesDisappearBound()
    {
        // ts > NOW() - INTERVAL '1' HOUR  <=>  NOW() < ts + 1h: disappears
        // (exclusive) at ts + 1 hour.
        var tf = SingleTemporalFilter(Resolve("ts > NOW() - INTERVAL '1' HOUR"));
        Assert.Null(tf.AppearOffsetMicros);
        Assert.Equal(MicrosPerHour, tf.DisappearOffsetMicros);
        Assert.False(tf.DisappearInclusive);
    }

    [Fact]
    public void NowOnLeft_FlipsTheComparison()
    {
        // NOW() >= ts  <=>  ts <= NOW(): appear inclusive at ts.
        var tf = SingleTemporalFilter(Resolve("NOW() >= ts"));
        Assert.Equal(0, tf.AppearOffsetMicros);
        Assert.True(tf.AppearInclusive);
        Assert.Null(tf.DisappearOffsetMicros);
    }

    [Fact]
    public void CurrentTimestampSpelling_IsEquivalentToNow()
    {
        var tf = SingleTemporalFilter(Resolve("ts <= CURRENT_TIMESTAMP"));
        Assert.Equal(0, tf.AppearOffsetMicros);
        Assert.True(tf.AppearInclusive);
    }

    [Fact]
    public void NowPlusInterval_GivesPositiveAppearOffset()
    {
        // ts < NOW() + INTERVAL '1' DAY  <=>  NOW() > ts - 1d: appears
        // (exclusive) at ts - 1 day.
        var tf = SingleTemporalFilter(Resolve("ts < NOW() + INTERVAL '1' DAY"));
        Assert.Equal(-MicrosPerDay, tf.AppearOffsetMicros);
        Assert.False(tf.AppearInclusive);
    }

    // ---------- Window (two bounds on the same key) ----------

    [Fact]
    public void TwoBoundsOnSameKey_MergeIntoOneNode()
    {
        var tf = SingleTemporalFilter(
            Resolve("ts > NOW() - INTERVAL '1' HOUR AND ts <= NOW()"));
        Assert.Equal(0, tf.AppearOffsetMicros);
        Assert.True(tf.AppearInclusive);
        Assert.Equal(MicrosPerHour, tf.DisappearOffsetMicros);
        Assert.False(tf.DisappearInclusive);
        // A single node, not two stacked filters.
        Assert.IsType<ScanPlan>(tf.Input);
    }

    [Fact]
    public void Between_DesugarsIntoOneTemporalWindow()
    {
        // ts BETWEEN NOW() - INTERVAL '1' HOUR AND NOW()
        //   => ts >= NOW()-1h  (disappear incl at ts+1h)
        //    AND ts <= NOW()    (appear incl at ts)
        var tf = SingleTemporalFilter(
            Resolve("ts BETWEEN NOW() - INTERVAL '1' HOUR AND NOW()"));
        Assert.Equal(0, tf.AppearOffsetMicros);
        Assert.True(tf.AppearInclusive);
        Assert.Equal(MicrosPerHour, tf.DisappearOffsetMicros);
        Assert.True(tf.DisappearInclusive);
    }

    [Fact]
    public void TemporalAndScalarConjuncts_Coexist()
    {
        // The scalar predicate stays a FilterPlan; the temporal predicate is a
        // TemporalFilterPlan below it.
        var plan = Resolve("ts <= NOW() AND id > 0");
        var project = Assert.IsType<ProjectPlan>(plan);
        var filter = Assert.IsType<FilterPlan>(project.Input);
        Assert.IsType<TemporalFilterPlan>(filter.Input);
    }

    // ---------- Position restriction ----------

    [Fact]
    public void Now_InSelectList_IsRejected()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL)"));
        var ex = Assert.Throws<ResolveException>(() =>
            resolver.Resolve(Parser.ParseStatement("SELECT NOW() FROM events")));
        Assert.Contains("temporal-filter", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Now_InProjectionArithmetic_IsRejected()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL)"));
        Assert.Throws<ResolveException>(() =>
            resolver.Resolve(Parser.ParseStatement("SELECT ts > NOW() AS recent FROM events")));
    }

    [Fact]
    public void Now_WithDisjunction_IsRejected()
    {
        var ex = ResolveError("ts > NOW() OR id = 1");
        Assert.Contains("temporal-filter comparison", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Now_AgainstNonTimestampKey_IsRejected()
    {
        var ex = ResolveError("id > NOW()");
        Assert.Contains("TIMESTAMP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MonthIntervalOffset_IsRejected()
    {
        var ex = ResolveError("ts > NOW() - INTERVAL '1' MONTH");
        Assert.Contains("day-time", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void NowOnBothSides_IsRejected()
    {
        var ex = ResolveError("NOW() < NOW()");
        Assert.Contains("only one side", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Equality_AgainstNow_IsRejected()
    {
        // `=` is not a sanctioned temporal-filter operator (a zero-width
        // window valid only at one instant); reject it.
        var ex = ResolveError("ts = NOW()");
        Assert.Contains("<, <=, >, >=", ex.Message, StringComparison.Ordinal);
    }
}
