// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Text;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Smoke tests for the Decimal128 integration. Exercise end-to-end flows
/// (literal arithmetic, column arithmetic, SUM/AVG/MIN/MAX, CAST round-trip)
/// to surface runtime breakage as we wire up the kernel-based decimal path.
/// </summary>
public class Decimal128CompilerTests
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

    private static Decimal128 D(string text, int precision, int scale) =>
        DecimalText.ParseDecimal128(text, DecimalType.Numeric(precision, scale));

    [Fact]
    public void Project_DecimalColumn_RoundTrips()
    {
        var q = Compile(
            ["CREATE TABLE t (price DECIMAL(10, 2) NOT NULL)"],
            "SELECT price FROM t");
        q.Table("t").Insert("12.34");
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, q.WeightOf(D("12.34", 10, 2)).Value);
    }

    [Fact]
    public void Project_AddDecimalColumns()
    {
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 2) NOT NULL)"],
            "SELECT a + b FROM t");
        q.Table("t").Insert("1.50", "2.75");
        q.Step();

        Assert.Equal(1, q.Current.Count);
        // 1.50 + 2.75 = 4.25, scale=2 → mantissa 425
        Assert.Equal(1, q.WeightOf(D("4.25", 11, 2)).Value);
    }

    [Fact]
    public void Sum_OverDecimalColumn()
    {
        var q = Compile(
            ["CREATE TABLE t (amount DECIMAL(10, 2) NOT NULL)"],
            "SELECT SUM(amount) FROM t");
        q.Table("t").Insert("10.50");
        q.Table("t").Insert("20.25");
        q.Step();

        // SUM result: 30.75 at scale 2.
        Assert.Equal(1, q.WeightOf(D("30.75", 10, 2)).Value);
    }

    [Fact]
    public void Avg_OverDecimalColumn()
    {
        var q = Compile(
            ["CREATE TABLE t (amount DECIMAL(10, 2) NOT NULL)"],
            "SELECT AVG(amount) FROM t");
        q.Table("t").Insert("10.00");
        q.Table("t").Insert("20.00");
        q.Step();

        // AVG of (10.00, 20.00) = 15.00 at scale 2.
        Assert.Equal(1, q.WeightOf(D("15.00", 10, 2)).Value);
    }

    [Fact]
    public void Min_OverDecimalColumn()
    {
        var q = Compile(
            ["CREATE TABLE t (amount DECIMAL(10, 2) NOT NULL)"],
            "SELECT MIN(amount) FROM t");
        q.Table("t").Insert("10.50");
        q.Table("t").Insert("5.25");
        q.Table("t").Insert("20.75");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("5.25", 10, 2)).Value);
    }

    [Fact]
    public void Filter_DecimalLiteralComparison()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, price DECIMAL(10, 2) NOT NULL)"],
            "SELECT id FROM t WHERE price > 5.00");
        q.Table("t").Insert(1, "3.50");
        q.Table("t").Insert(2, "7.25");
        q.Table("t").Insert(3, "5.00");
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, q.WeightOf(2).Value);
    }

    [Fact]
    public void Cast_StringToDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT CAST(s AS DECIMAL(10, 2)) FROM t");
        q.Table("t").Insert("12.34");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("12.34", 10, 2)).Value);
    }

    [Fact]
    public void Cast_DecimalToString()
    {
        var q = Compile(
            ["CREATE TABLE t (price DECIMAL(10, 2) NOT NULL)"],
            "SELECT CAST(price AS VARCHAR) FROM t");
        q.Table("t").Insert("12.34");
        q.Step();

        Assert.Equal(1, q.WeightOf("12.34").Value);
    }

    [Fact]
    public void Cast_IntToDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (n INT NOT NULL)"],
            "SELECT CAST(n AS DECIMAL(10, 2)) FROM t");
        q.Table("t").Insert(42);
        q.Step();

        Assert.Equal(1, q.WeightOf(D("42.00", 10, 2)).Value);
    }

    [Fact]
    public void Cast_DoubleToDecimal()
    {
        // The ivm-bench analytics models do CAST(ROUND(price, 4) AS DECIMAL(38, 4)) over
        // DOUBLE prices — scale by 10^s and round half-to-even.
        var q = Compile(
            ["CREATE TABLE t (v DOUBLE PRECISION NOT NULL)"],
            "SELECT CAST(v AS DECIMAL(10, 2)) FROM t");
        q.Table("t").Insert(12.345);   // rounds half-to-even at scale 2 → 12.34
        q.Table("t").Insert(3.14159);  // → 3.14
        q.Step();

        Assert.Equal(1, q.WeightOf(D("12.34", 10, 2)).Value);
        Assert.Equal(1, q.WeightOf(D("3.14", 10, 2)).Value);
    }

    [Fact]
    public void Cast_RealToDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (v REAL NOT NULL)"],
            "SELECT CAST(v AS DECIMAL(10, 2)) FROM t");
        q.Table("t").Insert(2.5f);
        q.Step();

        Assert.Equal(1, q.WeightOf(D("2.50", 10, 2)).Value);
    }

    // ---- ABS / FLOOR / CEIL / ROUND on Decimal128 ----

    [Fact]
    public void Abs_OnDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT ABS(v) FROM t");
        q.Table("t").Insert("-12.50");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("12.50", 10, 2)).Value);
    }

    [Fact]
    public void Abs_OnDecimal_PreservesPositive()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT ABS(v) FROM t");
        q.Table("t").Insert("3.14");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("3.14", 10, 2)).Value);
    }

    [Fact]
    public void Floor_OnPositiveDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT FLOOR(v) FROM t");
        q.Table("t").Insert("1.75");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("1.00", 10, 2)).Value);
    }

    [Fact]
    public void Floor_OnNegativeDecimal_RoundsTowardNegativeInfinity()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT FLOOR(v) FROM t");
        q.Table("t").Insert("-1.75");
        q.Step();

        // Floor of -1.75 is -2 (not -1, which would be truncation).
        Assert.Equal(1, q.WeightOf(D("-2.00", 10, 2)).Value);
    }

    [Fact]
    public void Floor_OnIntegerValuedDecimal_Unchanged()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT FLOOR(v) FROM t");
        q.Table("t").Insert("5.00");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("5.00", 10, 2)).Value);
    }

    [Fact]
    public void Ceil_OnPositiveDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT CEIL(v) FROM t");
        q.Table("t").Insert("1.25");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("2.00", 10, 2)).Value);
    }

    [Fact]
    public void Ceil_OnNegativeDecimal_RoundsTowardPositiveInfinity()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT CEIL(v) FROM t");
        q.Table("t").Insert("-1.25");
        q.Step();

        // Ceil of -1.25 is -1 (not -2).
        Assert.Equal(1, q.WeightOf(D("-1.00", 10, 2)).Value);
    }

    [Fact]
    public void Round_NoDigits_RoundsToInteger()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT ROUND(v) FROM t");
        q.Table("t").Insert("1.50");
        q.Step();

        // Banker's rounding: 1.50 rounds to 2 (even).
        Assert.Equal(1, q.WeightOf(D("2.00", 10, 2)).Value);
    }

    [Fact]
    public void Round_BankersRoundingHalfToEven()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT ROUND(v) FROM t");
        // Round half-to-even: 0.50 → 0 (even), 2.50 → 2 (even),
        // 3.50 → 4 (even). Standard half-up would give 0/3/4 — the test
        // verifies banker's rounding rejects the 0.50/2.50 half-up cases.
        q.Table("t").Insert("0.50");
        q.Table("t").Insert("2.50");
        q.Table("t").Insert("3.50");
        q.Step();

        Assert.Equal(1, q.WeightOf(D("0.00", 10, 2)).Value);
        Assert.Equal(1, q.WeightOf(D("2.00", 10, 2)).Value);
        Assert.Equal(1, q.WeightOf(D("4.00", 10, 2)).Value);
    }

    [Fact]
    public void Round_WithDigits_DropsToTargetPrecision()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 3) NOT NULL)"],
            "SELECT ROUND(v, 1) FROM t");
        q.Table("t").Insert("1.236");
        q.Step();

        // 1.236 rounded to 1 digit = 1.2 (round half-to-even on 0.36 → 0.4
        // would be standard half-up; banker's says round 1.236 to 1.2
        // since the dropped 36 > 50 is false). Result at scale 3 is 1.200.
        Assert.Equal(1, q.WeightOf(D("1.200", 10, 3)).Value);
    }

    [Fact]
    public void Round_DigitsBeyondScale_NoChange()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2) NOT NULL)"],
            "SELECT ROUND(v, 5) FROM t");
        q.Table("t").Insert("1.25");
        q.Step();

        // Asking for more digits than the column has: input passes through.
        Assert.Equal(1, q.WeightOf(D("1.25", 10, 2)).Value);
    }

    [Fact]
    public void Round_NegativeDigits_RoundsBeforeDecimalPoint()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 1) NOT NULL)"],
            "SELECT ROUND(v, -1) FROM t");
        q.Table("t").Insert("123.4");
        q.Step();

        // ROUND(123.4, -1) → 120, padded back to scale 1 → 120.0.
        Assert.Equal(1, q.WeightOf(D("120.0", 10, 1)).Value);
    }

    [Fact]
    public void Abs_PropagatesNull()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 2))"],
            "SELECT ABS(v) FROM t");
        q.Table("t").Insert((object?)null);
        q.Step();

        Assert.Equal(1, q.WeightOf(new object?[] { null }).Value);
    }

    [Fact]
    public void IntPlusDecimal_PromotesToDecimal()
    {
        var q = Compile(
            ["CREATE TABLE t (qty INT NOT NULL, unit DECIMAL(10, 2) NOT NULL)"],
            "SELECT qty + unit FROM t");
        q.Table("t").Insert(3, "5.50");
        q.Step();

        // INT promoted to DECIMAL(10, 0) by the new SQL Server / Substrait
        // rule (was DECIMAL(38, 0) under the simpler max-of-precisions
        // rule). qty + unit → DECIMAL(max(10,8)+1+max(0,2), max(0,2))
        // = DECIMAL(13, 2). Value 8.50.
        Assert.Equal(1, q.WeightOf(D("8.50", 13, 2)).Value);
        Assert.Equal(new SqlDecimalType(13, 2, false), q.OutputSchema[0].Type);
    }

    // ---- DecimalTypeRules swap-in: per-op precision/scale growth ----

    [Fact]
    public void Add_ResultPrecisionGrowsByOne_SqlServerRule()
    {
        // DECIMAL(10,2) + DECIMAL(10,2) → DECIMAL(11, 2). The "+1" digit
        // accounts for carry: 99999999.99 + 99999999.99 = 199999999.98
        // (9 integer digits) needs DECIMAL(11, 2) to hold without
        // exceeding declared precision.
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 2) NOT NULL)"],
            "SELECT a + b FROM t");
        Assert.Equal(new SqlDecimalType(11, 2, false), q.OutputSchema[0].Type);

        q.Table("t").Insert("99999999.99", "99999999.99");
        q.Step();
        Assert.Equal(1, q.WeightOf(D("199999999.98", 11, 2)).Value);
    }

    [Fact]
    public void Multiply_ScaleAndPrecisionGrow()
    {
        // DECIMAL(10,2) * DECIMAL(10,2) → DECIMAL(21, 4): scale = 2+2,
        // precision = 10+10+1 (full product can need 21 digits). Old
        // max-of-precisions rule produced DECIMAL(10, 2), which silently
        // returned values exceeding the declared precision.
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 2) NOT NULL)"],
            "SELECT a * b FROM t");
        Assert.Equal(new SqlDecimalType(21, 4, false), q.OutputSchema[0].Type);

        q.Table("t").Insert("1.50", "2.50");
        q.Step();
        // 1.50 * 2.50 = 3.7500 at scale 4.
        Assert.Equal(1, q.WeightOf(D("3.7500", 21, 4)).Value);
    }

    [Fact]
    public void Divide_ScaleExtendsToPreservePrecision()
    {
        // DECIMAL(10,2) / DECIMAL(10,2): scale = max(6, 2 + 10 + 1) = 13;
        // precision = int_a + p_b + s = 8 + 10 + 13 = 31. Old rule kept
        // result at scale 2 which truncated to whole-cents.
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 2) NOT NULL)"],
            "SELECT a / b FROM t");
        var resultType = (SqlDecimalType)q.OutputSchema[0].Type;
        Assert.Equal(31, resultType.Precision);
        Assert.Equal(13, resultType.Scale);

        q.Table("t").Insert("1.00", "3.00");
        q.Step();
        // 1.00 / 3.00 with banker's rounding at scale 13 = 0.0769230769231
        // (last digit rounded). The exact value depends on the kernel's
        // half-even rule on the 14th-digit residual — we just verify the
        // first several digits are present.
        var entry = Assert.Single(q.Current);
        var result = (Clast.DatabaseDecimal.Values.Decimal128)entry.Key[0]!;
        Assert.StartsWith("0.333333333333", result.ToString(13));
    }

    [Fact]
    public void Subtract_ResultPrecisionGrowsByOne()
    {
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 2) NOT NULL)"],
            "SELECT a - b FROM t");
        Assert.Equal(new SqlDecimalType(11, 2, false), q.OutputSchema[0].Type);
    }

    [Fact]
    public void Add_DifferentScales_PromotesToMaxScale()
    {
        // DECIMAL(10,2) + DECIMAL(10,3): integer digits max(8,7)=8,
        // scale max(2,3)=3, p = 8 + 1 + 3 = 12.
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(10, 2) NOT NULL, b DECIMAL(10, 3) NOT NULL)"],
            "SELECT a + b FROM t");
        Assert.Equal(new SqlDecimalType(12, 3, false), q.OutputSchema[0].Type);

        q.Table("t").Insert("1.50", "2.250");
        q.Step();
        // 1.500 + 2.250 = 3.750 at scale 3.
        Assert.Equal(1, q.WeightOf(D("3.750", 12, 3)).Value);
    }

    // ---- Decimal256 SUM widening: intermediate per-row × weight no longer
    // ---- silently wraps when it crosses Int128 capacity.

    [Fact]
    public void Sum_IntermediateExceedsInt128_FinalFits()
    {
        // Single Step with two delta rows whose intermediate weighted sum
        // exceeds Int128 capacity (≈ 1.7e38) but whose final sum fits both
        // Int128 and DECIMAL(38, 0) (i.e., < 10^38).
        // Row A: 10^37 with weight 18 → 1.8e38 (overflows Int128).
        // Row B: −10^37 with weight 10 → −10^38.
        // Final: 1.8e38 − 1.0e38 = 8 × 10^37, fits both bounds.
        // Old Int128-state SUM would silently wrap after row A;
        // Int256 state holds the full intermediate and lets the final
        // narrow succeed.
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(38, 0) NOT NULL)"],
            "SELECT SUM(v) FROM t");

        for (var i = 0; i < 18; i++)
        {
            q.Table("t").Insert("10000000000000000000000000000000000000");
        }

        for (var i = 0; i < 10; i++)
        {
            q.Table("t").Insert("-10000000000000000000000000000000000000");
        }

        q.Step();

        // Expected sum: 8 × 10^37 = "8" + 37 zeros (38 digits, fits prec 38).
        var expected = D("80000000000000000000000000000000000000", 38, 0);
        Assert.Equal(1, q.WeightOf(expected).Value);
    }

    [Fact]
    public void Sum_FinalOverflowsInt128_ThrowsOverflow()
    {
        // Two values of 10^38 — 1 sum to ≈ 2 × 10^38, exceeds Int128
        // capacity. Output narrow throws (was a silent wrap before).
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(38, 0) NOT NULL)"],
            "SELECT SUM(v) FROM t");
        q.Table("t").Insert("99999999999999999999999999999999999999");
        q.Table("t").Insert("99999999999999999999999999999999999999");

        Assert.Throws<OverflowException>(() => q.Step());
    }

    [Fact]
    public void Multiply_PrecisionClampsAt38()
    {
        // DECIMAL(20, 5) * DECIMAL(20, 5) → raw precision 41, scale 10;
        // clamps to (38, 7) — scale loses 3 digits, precision capped at 38.
        var q = Compile(
            ["CREATE TABLE t (a DECIMAL(20, 5) NOT NULL, b DECIMAL(20, 5) NOT NULL)"],
            "SELECT a * b FROM t");
        var resultType = (SqlDecimalType)q.OutputSchema[0].Type;
        Assert.Equal(38, resultType.Precision);
        Assert.Equal(7, resultType.Scale);
    }
}
