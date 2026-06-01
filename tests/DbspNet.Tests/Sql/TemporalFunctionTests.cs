// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Temporal scalar functions (EXTRACT / DATE_PART / DATE_TRUNC / DATEADD /
/// DATEDIFF) routed through the <c>ScalarFunctionRegistry</c>, plus a couple of
/// dispatch checks confirming non-registry builtins still resolve via the
/// legacy fallthrough.
/// </summary>
public class TemporalFunctionTests
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

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---- EXTRACT / DATE_PART ----

    [Fact]
    public void Extract_FieldsFromTimestamp()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT EXTRACT(YEAR FROM ts) AS y, EXTRACT(MONTH FROM ts) AS mo, " +
            "EXTRACT(DAY FROM ts) AS d, EXTRACT(HOUR FROM ts) AS h, " +
            "EXTRACT(MINUTE FROM ts) AS mi, EXTRACT(QUARTER FROM ts) AS qq FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 13:45:30"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 2026L, 4L, 27L, 13L, 45L, 2L));
    }

    [Fact]
    public void Extract_SecondIsDouble()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT EXTRACT(SECOND FROM ts) AS s FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 13:45:30.500000"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 30.5));
    }

    [Fact]
    public void DatePart_QuarterFromDate()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT DATE_PART('quarter', d) AS qq FROM t");

        q.Table("t").Insert(Date32.Parse("2026-11-15"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 4L));
    }

    // ---- DATE_TRUNC ----

    [Fact]
    public void DateTrunc_MonthOnTimestamp()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT DATE_TRUNC('month', ts) AS m FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 13:45:30"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Timestamp.Parse("2026-04-01 00:00:00")));
    }

    [Fact]
    public void DateTrunc_DayOnTimestamp()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT DATE_TRUNC('day', ts) AS m FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 13:45:30"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Timestamp.Parse("2026-04-27 00:00:00")));
    }

    [Fact]
    public void DateTrunc_MonthOnDate()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT DATE_TRUNC('month', d) AS m FROM t");

        q.Table("t").Insert(Date32.Parse("2026-04-27"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Date32.Parse("2026-04-01")));
    }

    // ---- DATEADD ----

    [Fact]
    public void DateAdd_DaysToDate()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT DATEADD('day', 5, d) AS d2 FROM t");

        q.Table("t").Insert(Date32.Parse("2026-04-27"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Date32.Parse("2026-05-02")));
    }

    [Fact]
    public void DateAdd_MonthToTimestamp_CalendarAware()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT DATEADD('month', 1, ts) AS ts2 FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-01-31 12:00:00"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Timestamp.Parse("2026-02-28 12:00:00")));
    }

    // ---- DATEDIFF ----

    [Theory]
    [InlineData("day", "2026-04-20", "2026-04-27", 7L)]
    [InlineData("year", "2019-12-31", "2020-01-01", 1L)]
    [InlineData("month", "2026-01-15", "2026-04-10", 3L)]
    public void DateDiff_OnDates(string unit, string a, string b, long expected)
    {
        var q = Compile(
            ["CREATE TABLE t (a DATE NOT NULL, b DATE NOT NULL)"],
            $"SELECT DATEDIFF('{unit}', a, b) AS n FROM t");

        q.Table("t").Insert(Date32.Parse(a), Date32.Parse(b));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, expected));
    }

    // ---- NULL propagation ----

    [Fact]
    public void Extract_NullPropagates()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, ts TIMESTAMP)"],
            "SELECT id, EXTRACT(YEAR FROM ts) AS y FROM t");

        q.Table("t").Insert(1, null);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, null));
    }

    // ---- filter using temporal functions ----

    [Fact]
    public void Filter_ByExtractedYear()
    {
        var q = Compile(
            ["CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL)"],
            "SELECT id FROM events WHERE EXTRACT(YEAR FROM ts) = 2026");

        q.Table("events").Insert(1, Timestamp.Parse("2025-12-31 23:59:59"));
        q.Table("events").Insert(2, Timestamp.Parse("2026-01-01 00:00:00"));
        q.Table("events").Insert(3, Timestamp.Parse("2026-07-04 12:00:00"));
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    // ---- resolver rejections ----

    [Fact]
    public void Extract_HourFromDate_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT EXTRACT(HOUR FROM d) AS x FROM t"));
        Assert.Contains("not valid", ex.Message);
    }

    [Fact]
    public void Extract_UnknownField_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT EXTRACT(fortnight FROM ts) AS x FROM t"));
        Assert.Contains("unknown field", ex.Message);
    }

    [Fact]
    public void DateTrunc_NonTemporal_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (n INT NOT NULL)"],
            "SELECT DATE_TRUNC('day', n) AS x FROM t"));
        Assert.Contains("DATE/TIME/TIMESTAMP", ex.Message);
    }

    // ---- registry dispatch: legacy builtins still resolve via fallthrough ----

    [Fact]
    public void Registry_FallsThroughToLegacyBuiltin()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT UPPER(s) AS u FROM t");

        q.Table("t").Insert("hello");
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "HELLO"));
    }

    [Fact]
    public void Registry_UnknownFunction_StillErrors()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (n INT NOT NULL)"],
            "SELECT bogus_fn(n) AS x FROM t"));
        Assert.Contains("unknown function", ex.Message);
    }
}
