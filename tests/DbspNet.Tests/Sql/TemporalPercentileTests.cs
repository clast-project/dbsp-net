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
/// Temporal-typed quantiles: <c>MEDIAN</c> / <c>PERCENTILE_CONT</c> /
/// <c>PERCENTILE_DISC</c> / <c>APPROX_PERCENTILE</c> over DATE / TIMESTAMP
/// (answered <b>exactly</b> via <see cref="DbspNet.Core.Operators.Stateful.Aggregators.OrderedQuantileSketch"/>)
/// and INTERVAL (DDSketch-approximated). Exact results are asserted exactly; the
/// INTERVAL estimates within the sketch's ~1% relative-error bound.
/// </summary>
public class TemporalPercentileTests
{
    private static CompiledQuery Compile(string[] ddl, string query, CompileMode mode = CompileMode.Typed)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return mode switch
        {
            CompileMode.Structural => PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance),
            CompileMode.Spine => PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine }),
            _ => PlanToCircuit.Compile(plan),
        };
    }

    public enum CompileMode { Typed, Structural, Spine }

    /// <summary>The boxed value of the single positive-weight output row's column 0.</summary>
    private static object? ScalarResult(CompiledQuery q)
    {
        object? found = null;
        var any = false;
        foreach (var (row, weight) in q.Current)
        {
            if (weight.Value <= 0)
            {
                continue;
            }

            Assert.False(any);
            any = true;
            found = row[0];
        }

        Assert.True(any);
        return found;
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---- DATE (exact) ----

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Date_Median_OddCount(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT MEDIAN(d) AS m FROM t",
            mode);

        foreach (var d in new[] { "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05" })
        {
            q.Table("t").Insert(Date32.Parse(d));
        }

        q.Step();
        Assert.Equal(Date32.Parse("2026-01-03"), ScalarResult(q));
    }

    [Fact]
    public void Date_ContInterpolatesToMidpointDay()
    {
        // Median of Jan-01 and Jan-03 interpolates to the exact midpoint, Jan-02.
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT MEDIAN(d) AS m FROM t");

        q.Table("t").Insert(Date32.Parse("2026-01-01"));
        q.Table("t").Insert(Date32.Parse("2026-01-03"));
        q.Step();

        Assert.Equal(Date32.Parse("2026-01-02"), ScalarResult(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Date_ContVsDisc_DifferOnEvenCount(CompileMode mode)
    {
        // {Jan-01..Jan-04}: CONT(0.5)=Jan-02.5 → rounds to Jan-03; DISC(0.5) picks
        // the 2nd ordered member = Jan-02 (a true member).
        var cont = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d) AS m FROM t",
            mode);
        var disc = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY d) AS m FROM t",
            mode);

        foreach (var d in new[] { "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04" })
        {
            cont.Table("t").Insert(Date32.Parse(d));
            disc.Table("t").Insert(Date32.Parse(d));
        }

        cont.Step();
        disc.Step();
        Assert.Equal(Date32.Parse("2026-01-03"), ScalarResult(cont));
        Assert.Equal(Date32.Parse("2026-01-02"), ScalarResult(disc));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Date_GroupBy_PerGroupMedians(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, d DATE NOT NULL)"],
            "SELECT g, MEDIAN(d) AS m FROM t GROUP BY g",
            mode);

        q.Table("t").Insert(1, Date32.Parse("2026-01-01"));
        q.Table("t").Insert(1, Date32.Parse("2026-01-02"));
        q.Table("t").Insert(1, Date32.Parse("2026-01-03"));
        q.Table("t").Insert(2, Date32.Parse("2026-06-10"));
        q.Table("t").Insert(2, Date32.Parse("2026-06-20"));
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, Date32.Parse("2026-01-02")));
        Assert.Equal(1, WeightOf(q.Current, 2, Date32.Parse("2026-06-15")));
    }

    [Fact]
    public void Date_IgnoresNullArguments()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE)"],
            "SELECT MEDIAN(d) AS m FROM t");

        q.Table("t").Insert(Date32.Parse("2026-01-01"));
        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(Date32.Parse("2026-01-03"));
        q.Step();

        Assert.Equal(Date32.Parse("2026-01-02"), ScalarResult(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Date_AllNullGroup_ReturnsNull(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, d DATE)"],
            "SELECT g, MEDIAN(d) AS m FROM t GROUP BY g",
            mode);

        q.Table("t").Insert(1, null);
        q.Table("t").Insert(1, null);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, null));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Date_Delete_ShiftsTheQuantile(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT MEDIAN(d) AS m FROM t",
            mode);

        q.Table("t").Insert(Date32.Parse("2026-01-01"));
        q.Table("t").Insert(Date32.Parse("2026-01-03"));
        q.Table("t").Insert(Date32.Parse("2026-01-05"));
        q.Step();
        Assert.Equal(Date32.Parse("2026-01-03"), ScalarResult(q));

        // Remove the top value — the exact sketch retracts that key and the
        // median of {Jan-01, Jan-03} interpolates to the midpoint Jan-02.
        q.Table("t").Delete(Date32.Parse("2026-01-05"));
        q.Step();
        Assert.Equal(Date32.Parse("2026-01-02"), ScalarResult(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Date_PercentileDisc_Descending_InvertsFraction(CompileMode mode)
    {
        // 100 consecutive days; PERCENTILE_DISC(0.9) DESC == the 10th from the
        // bottom = day 10.
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY d DESC) AS p FROM t",
            mode);

        var start = DateOnly.ParseExact("2026-01-01", "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        for (var i = 0; i < 100; i++)
        {
            q.Table("t").Insert(Date32.FromDateOnly(start.AddDays(i)));
        }

        q.Step();
        // DESC lowers the fraction to 1−0.9=0.1 → DISC(0.1)=ceil(0.1·100)=10th
        // ordered day = 2026-01-10.
        Assert.Equal(Date32.FromDateOnly(start.AddDays(9)), ScalarResult(q));
    }

    // ---- TIMESTAMP (exact) ----

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Timestamp_Median_OddCount(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT MEDIAN(ts) AS m FROM t",
            mode);

        foreach (var ts in new[] { "2026-04-27 12:00:00", "2026-04-27 12:00:10", "2026-04-27 12:00:20" })
        {
            q.Table("t").Insert(Timestamp.Parse(ts));
        }

        q.Step();
        Assert.Equal(Timestamp.Parse("2026-04-27 12:00:10"), ScalarResult(q));
    }

    [Fact]
    public void Timestamp_ContInterpolatesToMicrosecond()
    {
        // Median of 12:00:00 and 12:00:01 = 12:00:00.500000 (exact half-second).
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT MEDIAN(ts) AS m FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 12:00:00"));
        q.Table("t").Insert(Timestamp.Parse("2026-04-27 12:00:01"));
        q.Step();

        Assert.Equal(Timestamp.Parse("2026-04-27 12:00:00.500000"), ScalarResult(q));
    }

    [Fact]
    public void Timestamp_IncrementalEqualsBatch_OverRandomInsertsAndDeletes()
    {
        // The exact sketch is a deterministic function of the present multiset,
        // so the incremental estimate must equal a from-scratch batch recompute
        // exactly (and, being exact, it is the true median).
        var rng = new Random(20260602);
        var live = new Dictionary<long, int>(); // micros-key -> multiplicity
        var baseTs = Timestamp.Parse("2026-04-27 12:00:00").Microseconds;

        var incremental = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT MEDIAN(ts) AS m FROM t");

        var net = new Dictionary<StructuralRow, long>();
        for (var tick = 0; tick < 30; tick++)
        {
            for (var op = 0; op < 20; op++)
            {
                var sec = rng.Next(0, 300);
                var micros = baseTs + (long)sec * 1_000_000L;
                var present = live.GetValueOrDefault(micros, 0);
                if (present > 0 && rng.Next(2) == 0)
                {
                    incremental.Table("t").Delete(new Timestamp(micros));
                    live[micros] = present - 1;
                }
                else
                {
                    incremental.Table("t").Insert(new Timestamp(micros));
                    live[micros] = present + 1;
                }
            }

            incremental.Step();
            foreach (var (row, weight) in incremental.Current)
            {
                net[row] = net.GetValueOrDefault(row, 0) + weight.Value;
            }
        }

        var survivors = net.Where(kv => kv.Value > 0).ToList();
        Assert.Single(survivors);
        var netIncremental = (Timestamp)survivors[0].Key[0]!;

        var batch = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT MEDIAN(ts) AS m FROM t");
        foreach (var (micros, mult) in live)
        {
            for (var i = 0; i < mult; i++)
            {
                batch.Table("t").Insert(new Timestamp(micros));
            }
        }

        batch.Step();
        Assert.Equal(netIncremental, (Timestamp)ScalarResult(batch)!);
    }

    // ---- INTERVAL (DDSketch-approximate) ----

    [Fact]
    public void Interval_DayTime_Median_LatencyDistribution()
    {
        // 99 durations 1s, 2s, …, 99s expressed as (ts − base); median ≈ 50s.
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT MEDIAN(ts - CAST('2026-01-01 00:00:00' AS TIMESTAMP)) AS m FROM t");

        var baseTs = Timestamp.Parse("2026-01-01 00:00:00");
        for (var sec = 1; sec <= 99; sec++)
        {
            q.Table("t").Insert(new Timestamp(baseTs.Microseconds + (long)sec * 1_000_000L));
        }

        q.Step();
        var micros = ((Interval)ScalarResult(q)!).Micros;
        var seconds = micros / (double)Interval.MicrosPerSecond;
        Assert.True(Math.Abs(seconds - 50.0) / 50.0 <= 0.02, $"expected ≈50s, got {seconds}s");
    }

    [Fact]
    public void Interval_YearMonth_Median()
    {
        // INTERVAL '1' MONTH * n is a year-month interval of n months; median of
        // n=1..99 months ≈ 50 months.
        var q = Compile(
            ["CREATE TABLE t (n INT NOT NULL)"],
            "SELECT MEDIAN(INTERVAL '1' MONTH * n) AS m FROM t");

        for (var n = 1; n <= 99; n++)
        {
            q.Table("t").Insert(n);
        }

        q.Step();
        var months = ((Interval)ScalarResult(q)!).Months;
        Assert.True(Math.Abs(months - 50) <= 1, $"expected ≈50 months, got {months}");
    }

    // ---- Resolver ----

    [Theory]
    [InlineData("CREATE TABLE t (d DATE NOT NULL)", "SELECT MEDIAN(d) AS m FROM t", "DATE")]
    [InlineData("CREATE TABLE t (ts TIMESTAMP NOT NULL)", "SELECT MEDIAN(ts) AS m FROM t", "TIMESTAMP")]
    public void Resolver_TemporalResultType_MatchesArgAndIsNullable(string ddl, string query, string typeName)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;

        Assert.Equal(typeName, plan.Schema[0].Type.Name);
        Assert.True(plan.Schema[0].Type.Nullable);
    }

    [Fact]
    public void Resolver_IntervalResultType_IsIntervalNullable()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (a TIMESTAMP NOT NULL, b TIMESTAMP NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT MEDIAN(a - b) AS m FROM t"))).Query;

        Assert.IsType<SqlIntervalType>(plan.Schema[0].Type);
        Assert.True(plan.Schema[0].Type.Nullable);
    }

    [Theory]
    [InlineData("CREATE TABLE t (tm TIME NOT NULL)", "SELECT MEDIAN(tm) FROM t")]
    [InlineData("CREATE TABLE t (s VARCHAR NOT NULL)", "SELECT MEDIAN(s) FROM t")]
    [InlineData("CREATE TABLE t (b BOOLEAN NOT NULL)", "SELECT MEDIAN(b) FROM t")]
    public void Resolver_UnsupportedArgType_Throws(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));

        var ex = Assert.Throws<ResolveException>(() => resolver.Resolve(Parser.ParseStatement(query)));
        Assert.Contains("DATE, TIMESTAMP, or INTERVAL", ex.Message, StringComparison.Ordinal);
    }
}
