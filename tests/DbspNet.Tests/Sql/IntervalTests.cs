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
/// INTERVAL literals, the <see cref="Interval"/> value type, and date/time
/// arithmetic (date ± interval, ts − ts, interval ± interval, interval scaling).
/// </summary>
public class IntervalTests
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

    // ---- Interval value type ----

    [Theory]
    [InlineData("1", IntervalQualifier.Year, 12, 0L)]
    [InlineData("3", IntervalQualifier.Month, 3, 0L)]
    [InlineData("1-6", IntervalQualifier.YearToMonth, 18, 0L)]
    [InlineData("90", IntervalQualifier.Day, 0, 90L * 86_400_000_000L)]
    [InlineData("2", IntervalQualifier.Hour, 0, 2L * 3_600_000_000L)]
    [InlineData("1.5", IntervalQualifier.Second, 0, 1_500_000L)]
    [InlineData("-5", IntervalQualifier.Day, 0, -5L * 86_400_000_000L)]
    public void IntervalParse_ByQualifier(string text, IntervalQualifier q, int months, long micros)
    {
        var iv = Interval.Parse(text, q);
        Assert.Equal(months, iv.Months);
        Assert.Equal(micros, iv.Micros);
    }

    [Theory]
    [InlineData(14, 0L, "P1Y2M")]
    [InlineData(0, 90L * 86_400_000_000L, "P90D")]
    [InlineData(0, 90L * 60_000_000L, "PT1H30M")]
    [InlineData(0, 0L, "PT0S")]
    public void IntervalToString_Iso8601(int months, long micros, string expected) =>
        Assert.Equal(expected, new Interval(months, micros).ToString());

    // ---- Parse / resolve ----

    [Fact]
    public void IntervalColumnType_Resolves()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE t (span INTERVAL DAY NOT NULL)"));
        var col = catalog.Get("t")[0].Type;
        var interval = Assert.IsType<SqlIntervalType>(col);
        Assert.Equal(IntervalQualifier.Day, interval.Qualifier);
        Assert.Equal(typeof(Interval), interval.ClrType);
    }

    // ---- date ± interval → date ----

    [Fact]
    public void Date_PlusIntervalDay()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT d + INTERVAL '5' DAY AS d2 FROM t");

        q.Table("t").Insert(Date32.Parse("2026-04-27"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Date32.Parse("2026-05-02")));
    }

    [Fact]
    public void Date_MinusIntervalMonth_CalendarAware()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT d - INTERVAL '1' MONTH AS d2 FROM t");

        // Mar-31 − 1 month clamps to Feb-28 (calendar-aware month subtraction).
        q.Table("t").Insert(Date32.Parse("2026-03-31"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Date32.Parse("2026-02-28")));
    }

    [Fact]
    public void Timestamp_PlusIntervalHour()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT ts + INTERVAL '90' MINUTE AS ts2 FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 12:00:00"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, Timestamp.Parse("2026-04-27 13:30:00")));
    }

    // ---- ts − ts / date − date → interval ----

    [Fact]
    public void DateMinusDate_ProducesIntervalDay()
    {
        var q = Compile(
            ["CREATE TABLE t (a DATE NOT NULL, b DATE NOT NULL)"],
            "SELECT CAST(a - b AS VARCHAR) AS span FROM t");

        q.Table("t").Insert(Date32.Parse("2026-04-27"), Date32.Parse("2026-04-20"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "P7D"));
    }

    [Fact]
    public void TimestampMinusTimestamp_ProducesInterval()
    {
        var q = Compile(
            ["CREATE TABLE t (a TIMESTAMP NOT NULL, b TIMESTAMP NOT NULL)"],
            "SELECT CAST(a - b AS VARCHAR) AS span FROM t");

        q.Table("t").Insert(
            Timestamp.Parse("2026-04-27 12:30:00"),
            Timestamp.Parse("2026-04-27 12:00:00"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "PT30M"));
    }

    // ---- interval ± interval, interval scaling ----

    [Fact]
    public void IntervalArithmetic_AddAndScale()
    {
        var q = Compile(
            ["CREATE TABLE t (n INT NOT NULL)"],
            "SELECT CAST(INTERVAL '1' DAY * n + INTERVAL '2' DAY AS VARCHAR) AS span FROM t");

        q.Table("t").Insert(3);
        q.Step();

        // 1 day * 3 + 2 days = 5 days
        Assert.Equal(1, WeightOf(q.Current, "P5D"));
    }

    // ---- filter using date arithmetic (forces structural fallback) ----

    [Fact]
    public void Filter_DateMinusInterval()
    {
        var q = Compile(
            ["CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)"],
            "SELECT id FROM events WHERE d > CAST('2026-04-30' AS DATE) - INTERVAL '7' DAY");

        q.Table("events").Insert(1, Date32.Parse("2026-04-22"));  // == 04-23 boundary? 04-30 - 7 = 04-23
        q.Table("events").Insert(2, Date32.Parse("2026-04-24"));
        q.Table("events").Insert(3, Date32.Parse("2026-04-30"));
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    // ---- NULL propagation ----

    [Fact]
    public void Date_PlusInterval_NullPropagates()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, d DATE)"],
            "SELECT id, CAST(d + INTERVAL '1' DAY AS VARCHAR) AS s FROM t");

        q.Table("t").Insert(1, null);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, null));
    }

    // ---- resolver rejections ----

    [Fact]
    public void DatePlusDate_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (a DATE NOT NULL, b DATE NOT NULL)"],
            "SELECT a + b AS x FROM t"));
        Assert.Contains("not defined", ex.Message);
    }

    [Fact]
    public void MonthIntervalOnTime_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (tm TIME NOT NULL)"],
            "SELECT tm + INTERVAL '1' MONTH AS x FROM t"));
        Assert.Contains("TIME", ex.Message);
    }

    [Fact]
    public void MixedIntervalClasses_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (n INT NOT NULL)"],
            "SELECT INTERVAL '1' MONTH + INTERVAL '1' DAY AS x FROM t"));
        Assert.Contains("year-month", ex.Message);
    }
}
