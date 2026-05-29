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
/// End-to-end coverage for anti-semi-join — the primitive backing
/// <c>NOT IN (subquery)</c> (uncorrelated + correlated) and correlated
/// <c>NOT EXISTS</c>. The compiler emits <c>outer − SemiJoin(outer, sq)</c>
/// via the existing <c>builder.Difference</c> Z-set subtraction; the
/// resolver lifts each shape to a <see cref="SemiJoinPlan"/> with
/// <c>IsAnti=true</c>.
///
/// NOT NULL operands only in v1: nullable probe or subquery column
/// rejects at resolve time. Uncorrelated NOT EXISTS still flows through
/// the existing COALESCE desugar via UnaryNot.
/// </summary>
public class AntiSemiJoinTests
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

    // ---------- NOT IN (uncorrelated) ----------

    [Fact]
    public void NotIn_Uncorrelated_EmitsOuterRowsWithNoMatch()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("orders").Insert(2, 50);
        q.Table("orders").Insert(3, 30);
        q.Table("vips").Insert(1);
        q.Table("vips").Insert(3);
        q.Step();

        // Customers 1 and 3 are in the VIP list — filtered out by NOT IN.
        // Customer 2 is not — passes.
        Assert.Equal(1, WeightOf(q.Current, 2, 50));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void NotIn_Uncorrelated_AddingVipRetractsMatchingOrder()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100));

        // Cust 1 becomes a VIP — order retracts.
        q.Table("vips").Insert(1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 100));

        // VIP retracted — order re-emerges.
        q.Table("vips").Delete(1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
    }

    [Fact]
    public void NotIn_Uncorrelated_EquivalentToExceptSpelling()
    {
        // outer NOT IN (sq) ≡ (outer EXCEPT outer-matching-sq). We use
        // INNER JOIN + EXCEPT to spell the equivalent comparison plan
        // (SQL surface has no DISTINCT, so the IN-side dedups via UNION).
        var sq = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        var eq = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders " +
            "EXCEPT " +
            "SELECT o.cust, o.amt FROM orders o INNER JOIN vips v ON o.cust = v.vid");

        void Tick(Action<CompiledQuery> apply)
        {
            apply(sq); apply(eq);
            sq.Step(); eq.Step();
            Assert.Equal(eq.Current, sq.Current);
        }

        Tick(q => { q.Table("orders").Insert(1, 100); q.Table("orders").Insert(2, 50); });
        Tick(q => { q.Table("vips").Insert(1); });
        Tick(q => { q.Table("orders").Insert(3, 25); });
        Tick(q => { q.Table("vips").Delete(1); });
    }

    // ---------- NOT IN (correlated) ----------

    [Fact]
    public void NotIn_Correlated_FiltersByRegionMembership()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE cust NOT IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        // Region 1: cust 1 IS a VIP same-region → filtered. Cust 2 isn't.
        // Region 2: cust 3 IS a VIP same-region → filtered.
        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 1);
        q.Table("orders").Insert(3, 2);
        q.Table("vips").Insert(1, 1);
        q.Table("vips").Insert(3, 2);
        q.Table("vips").Insert(1, 2); // ID 1 in region 2 — doesn't affect cust 1 in region 1.
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 2, 1));
        Assert.Equal(1, q.Current.Count);
    }

    // ---------- Correlated NOT EXISTS ----------

    [Fact]
    public void NotExists_Correlated_KeepsOuterRowsWithNoMatchingGroup()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE NOT EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 1);
        q.Table("orders").Insert(3, 2);
        q.Table("vips").Insert(99, 1);
        q.Step();

        // Region 1 has a VIP → both orders in region 1 are filtered.
        // Region 2 has no VIPs → order 3 passes.
        Assert.Equal(1, WeightOf(q.Current, 3, 2));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void NotExists_Correlated_FirstVipInRegionRetractsAllRegionOrders()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE NOT EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 2, 1));

        // Insert a VIP in region 1 — both orders retract.
        q.Table("vips").Insert(99, 1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 1));
        Assert.Equal(-1, WeightOf(q.Current, 2, 1));

        // Retract the VIP — orders come back.
        q.Table("vips").Delete(99, 1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 2, 1));
    }

    // ---------- Regression for uncorrelated NOT EXISTS ----------

    [Fact]
    public void Regression_UncorrelatedNotExists_StillFlowsThroughCoalesceDesugar()
    {
        // Uncorrelated NOT EXISTS doesn't go through the new anti-semi-join
        // path — it desugars as NOT(uncorrelated_EXISTS_desugar) at the
        // unary-NOT arm in ParseNot. This test pins that the existing
        // behaviour still works after enabling correlated NOT EXISTS.
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL, name VARCHAR NOT NULL)",
                "CREATE TABLE u (flag INT NOT NULL)",
            ],
            "SELECT name FROM t WHERE NOT EXISTS (SELECT 1 FROM u)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, WeightOf(q.Current, "b"));

        q.Table("u").Insert(42);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, "a"));
        Assert.Equal(-1, WeightOf(q.Current, "b"));
    }
}
