// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Text;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Regression tests for <c>CAST(DECIMAL AS DOUBLE / REAL)</c>. The cast must
/// preserve the fractional digits (<c>CAST(60.7834 AS DOUBLE)</c> → 60.7834),
/// not drop them by rescaling the mantissa to scale 0. This was the root cause
/// of the ivm-bench <c>trade_volume_stats</c> / <c>broker_performance</c>
/// divergence: every <c>avg_*</c> column is
/// <c>CAST(SUM(DECIMAL(38,4)) AS DOUBLE) / COUNT(col)</c>, and the truncating
/// cast made all of them wrong while the pure-DOUBLE columns matched Feldera.
/// </summary>
public class DecimalToDoubleCastTests
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

    // ---- Structural expression path (PlanToCircuit) ----

    [Fact]
    public void Cast_DecimalToDouble_PreservesFraction()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 4) NOT NULL)"],
            "SELECT CAST(v AS DOUBLE) FROM t");
        q.Table("t").Insert("60.7834");
        q.Step();

        Assert.Equal(1, q.WeightOf(60.7834).Value);
    }

    [Fact]
    public void Cast_DecimalToDouble_NegativeFraction()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 4) NOT NULL)"],
            "SELECT CAST(v AS DOUBLE) FROM t");
        q.Table("t").Insert("-12.3456");
        q.Step();

        Assert.Equal(1, q.WeightOf(-12.3456).Value);
    }

    [Fact]
    public void Cast_DecimalToReal_PreservesFraction()
    {
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 4) NOT NULL)"],
            "SELECT CAST(v AS REAL) FROM t");
        q.Table("t").Insert("2.7500");
        q.Step();

        Assert.Equal(1, q.WeightOf(2.75f).Value);
    }

    [Fact]
    public void Cast_DecimalToBigint_StillTruncatesFraction()
    {
        // Integer targets keep the fraction-dropping behavior (unchanged).
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(10, 4) NOT NULL)"],
            "SELECT CAST(v AS BIGINT) FROM t");
        q.Table("t").Insert("60.7834");
        q.Step();

        Assert.Equal(1, q.WeightOf(61L).Value);
    }

    [Fact]
    public void AvgViaDecimalSumCastToDouble_MatchesExactAverage()
    {
        // The exact ivm-bench shape:
        //   ROUND(CAST(SUM(CAST(ROUND(fee,4) AS DECIMAL(38,4))) AS DOUBLE)
        //         / NULLIF(COUNT(fee), 0), 4)
        var q = Compile(
            ["CREATE TABLE trades (broker INT NOT NULL, fee DOUBLE PRECISION)"],
            @"SELECT broker,
                     ROUND(CAST(SUM(CAST(ROUND(fee, 4) AS DECIMAL(38,4))) AS DOUBLE)
                           / NULLIF(COUNT(fee), 0), 4) AS avg_fee
              FROM trades GROUP BY broker");

        // fees 12.3456, 20.5, NULL, 30.9999 → sum 63.8455, count 3
        //   avg = 63.8455 / 3 = 21.281833... → ROUND 4 = 21.2818
        q.Table("trades").Insert(1, 12.3456);
        q.Table("trades").Insert(1, 20.5);
        q.Table("trades").Insert(1, (object?)null);
        q.Table("trades").Insert(1, 30.9999);
        q.Step();

        Assert.Equal(1, q.WeightOf(1, 21.2818).Value);
    }

    // ---- Typed expression path (TypedExpressionCompiler) ----

    [Fact]
    public void Typed_Cast_DecimalToDouble_PreservesFraction()
    {
        var schema = new Schema([new SchemaColumn("v", new SqlDecimalType(10, 4, false))]);
        var rowType = TypedRowEmitter.EmitRowType(schema)!;
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)!;

        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (v DECIMAL(10,4) NOT NULL)"));
        var plan = (SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT CAST(v AS DOUBLE) FROM t"));
        var expr = ((ProjectPlan)plan.Query).Projections[0].Expression;

        var del = TypedExpressionCompiler.TryCompile(expr, rowType);
        Assert.NotNull(del);

        var row = factory(new object?[] { D("60.7834", 10, 4) });
        Assert.Equal(60.7834, (double)del!.DynamicInvoke(row)!, 6);
    }
}
