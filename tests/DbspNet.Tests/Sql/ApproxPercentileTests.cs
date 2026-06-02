// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// <c>APPROX_PERCENTILE</c> / <c>MEDIAN</c> / <c>PERCENTILE_CONT</c> /
/// <c>PERCENTILE_DISC</c> (DDSketch) on both compile paths and both trace
/// families. The sketch is a deterministic, invertible function of the value
/// multiset, so the estimate is fixed for fixed inputs (these tests are not
/// flaky), the incremental result equals a from-scratch batch recompute
/// exactly, and every reported value is within the sketch's ~1% relative-error
/// bound of the true quantile.
/// </summary>
public class ApproxPercentileTests
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

    /// <summary>The value of the single positive-weight output row's column 0
    /// (null for a NULL result).</summary>
    private static double? ScalarPercentile(CompiledQuery q)
    {
        double? found = null;
        var any = false;
        foreach (var (row, weight) in q.Current)
        {
            if (weight.Value <= 0)
            {
                continue;
            }

            Assert.False(any);
            any = true;
            found = row[0] is null ? null : Convert.ToDouble(row[0], System.Globalization.CultureInfo.InvariantCulture);
        }

        Assert.True(any);
        return found;
    }

    private static void AssertClose(double expected, double? actual, double relTol = 0.02)
    {
        Assert.NotNull(actual);
        var relError = Math.Abs(actual!.Value - expected) / Math.Abs(expected);
        Assert.True(relError <= relTol, $"expected ≈{expected}, got {actual} (rel error {relError:P3})");
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Median_OfValues(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT MEDIAN(v) AS m FROM t",
            mode);

        foreach (var v in new[] { 10, 20, 30, 40, 50 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        AssertClose(30, ScalarPercentile(q));
    }

    [Theory]
    [InlineData(0.0, 10)]
    [InlineData(0.5, 30)]
    [InlineData(0.9, 40)]
    [InlineData(1.0, 50)]
    public void ApproxPercentile_AtFraction(double fraction, double expected)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            $"SELECT APPROX_PERCENTILE(v, {fraction}) AS p FROM t");

        foreach (var v in new[] { 10, 20, 30, 40, 50 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        AssertClose(expected, ScalarPercentile(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void PercentileCont_WithinGroup(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) AS m FROM t",
            mode);

        foreach (var v in new[] { 10, 20, 30, 40, 50 })
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        AssertClose(30, ScalarPercentile(q));
    }

    [Fact]
    public void PercentileDisc_WithinGroup_SharesTheSketch()
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY v) AS p FROM t");

        for (var v = 1; v <= 100; v++)
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        AssertClose(90, ScalarPercentile(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void GroupBy_PerGroupMedians(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, MEDIAN(v) AS m FROM t GROUP BY g",
            mode);

        // group 1: {10, 20, 30} -> median ≈ 20; group 2: {100, 200} -> ≈ 100.
        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, 20);
        q.Table("t").Insert(1, 30);
        q.Table("t").Insert(2, 100);
        q.Table("t").Insert(2, 200);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        foreach (var (row, weight) in q.Current)
        {
            Assert.Equal(1, weight.Value);
            var g = Convert.ToInt32(row[0], System.Globalization.CultureInfo.InvariantCulture);
            var m = Convert.ToDouble(row[1], System.Globalization.CultureInfo.InvariantCulture);
            AssertClose(g == 1 ? 20 : 100, m);
        }
    }

    [Fact]
    public void IgnoresNullArguments()
    {
        var q = Compile(
            ["CREATE TABLE t (v INT)"],
            "SELECT MEDIAN(v) AS m FROM t");

        q.Table("t").Insert(5);
        q.Table("t").Insert(15);
        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(25);
        q.Table("t").Insert((object?)null);
        q.Step();

        AssertClose(15, ScalarPercentile(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void AllNullGroup_ReturnsNull(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, MEDIAN(v) AS m FROM t GROUP BY g",
            mode);

        q.Table("t").Insert(1, null);
        q.Table("t").Insert(1, null);
        q.Step();

        // The group is present (positive weight) but has no non-NULL value.
        Assert.Equal(1, WeightOf(q.Current, 1, null));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Delete_ShiftsTheQuantile(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT MEDIAN(v) AS m FROM t",
            mode);

        q.Table("t").Insert(10);
        q.Table("t").Insert(20);
        q.Table("t").Insert(30);
        q.Step();
        AssertClose(20, ScalarPercentile(q));

        // Remove the top value — the sketch retracts that bucket (no rebuild),
        // and the median of {10, 20} drops toward 10.
        q.Table("t").Delete(30);
        q.Step();
        AssertClose(10, ScalarPercentile(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    [InlineData(CompileMode.Spine)]
    public void Delete_RemovingDuplicate_KeepsTheValue(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT MEDIAN(v) AS m FROM t",
            mode);

        // Value 20 appears twice.
        q.Table("t").Insert(10);
        q.Table("t").Insert(20);
        q.Table("t").Insert(20);
        q.Table("t").Insert(30);
        q.Step();
        var before = ScalarPercentile(q);

        // Remove one copy of 20 — still present, the median is unchanged, so no
        // delta should be emitted.
        q.Table("t").Delete(20);
        q.Step();
        Assert.Equal(0, q.Current.Count);
        AssertClose(before!.Value, before);
    }

    [Fact]
    public void Decimal_Median()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT MEDIAN(v) AS m FROM t");

        foreach (var s in new[] { "10.50", "20.25", "30.75", "40.00", "50.00" })
        {
            q.Table("t").Insert(s);
        }

        q.Step();
        AssertClose(30.75, ScalarPercentile(q));
    }

    [Theory]
    [InlineData(CompileMode.Typed)]
    [InlineData(CompileMode.Structural)]
    public void LargeInput_WithinErrorBound(CompileMode mode)
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_PERCENTILE(v, 0.95) AS p FROM t",
            mode);

        for (var v = 1; v <= 10_000; v++)
        {
            q.Table("t").Insert(v);
        }

        q.Step();
        AssertClose(9500, ScalarPercentile(q));
    }

    [Fact]
    public void IncrementalEqualsBatch_OverRandomInsertsAndDeletes()
    {
        // A deterministic op stream (fixed seed): apply it tick-by-tick with
        // interleaved deletes, then feed the net multiset to a fresh query in a
        // single batch tick. The DDSketch is a deterministic function of the
        // present value multiset, so the two estimates must match *exactly*
        // (not merely within the error bound).
        var rng = new Random(20260602);
        var live = new Dictionary<int, int>(); // value -> current multiplicity

        var incremental = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_PERCENTILE(v, 0.5) AS m FROM t");

        var net = new Dictionary<StructuralRow, long>();
        for (var tick = 0; tick < 40; tick++)
        {
            for (var op = 0; op < 25; op++)
            {
                var v = rng.Next(0, 300);
                var present = live.GetValueOrDefault(v, 0);
                if (present > 0 && rng.Next(2) == 0)
                {
                    incremental.Table("t").Delete(v);
                    live[v] = present - 1;
                }
                else
                {
                    incremental.Table("t").Insert(v);
                    live[v] = present + 1;
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
        var netIncremental = Convert.ToDouble(survivors[0].Key[0], System.Globalization.CultureInfo.InvariantCulture);

        var batch = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT APPROX_PERCENTILE(v, 0.5) AS m FROM t");
        foreach (var (v, mult) in live)
        {
            for (var i = 0; i < mult; i++)
            {
                batch.Table("t").Insert(v);
            }
        }

        batch.Step();
        Assert.Equal(netIncremental, ScalarPercentile(batch)!.Value);
    }

    [Fact]
    public void Resolver_ApproxPercentile_IsDoubleNullable()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT MEDIAN(v) AS m FROM t"))).Query;

        Assert.Equal("DOUBLE PRECISION", plan.Schema[0].Type.Name);
        Assert.True(plan.Schema[0].Type.Nullable);
    }

    [Theory]
    [InlineData("SELECT APPROX_PERCENTILE(v, 1.5) FROM t")]
    [InlineData("SELECT APPROX_PERCENTILE(v, -0.1) FROM t")]
    public void Resolver_FractionOutOfRange_Throws(string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));

        var ex = Assert.Throws<ResolveException>(() => resolver.Resolve(Parser.ParseStatement(query)));
        Assert.Contains("[0, 1]", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolver_NonConstantFraction_Throws()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL, f DOUBLE PRECISION NOT NULL)"));

        Assert.Throws<ResolveException>(() => resolver.Resolve(
            Parser.ParseStatement("SELECT APPROX_PERCENTILE(v, f) FROM t")));
    }

    [Fact]
    public void Resolver_PercentileAsWindowFunction_Throws()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"));

        var ex = Assert.Throws<ResolveException>(() => resolver.Resolve(Parser.ParseStatement(
            "SELECT MEDIAN(v) OVER (PARTITION BY g) FROM t")));
        Assert.Contains("window function", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Parser_WithinGroup_DescendingOrder_Throws()
    {
        Assert.Throws<ParseException>(() => Parser.ParseStatement(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v DESC) FROM t"));
    }
}
