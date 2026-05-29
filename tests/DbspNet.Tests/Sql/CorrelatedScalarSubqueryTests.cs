// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for correlated scalar subqueries — e.g.
/// <c>WHERE x &gt; (SELECT MAX(y) FROM t WHERE t.k = outer.k)</c>. The
/// resolver decorrelates by stripping the equi-correlation conjunct from
/// the inner WHERE, prepending the correlation columns to the inner
/// aggregate's GROUP BY, and emitting a
/// <see cref="CorrelatedScalarSubqueryJoinPlan"/> that compiles to a
/// multi-column-key LEFT JOIN. Outer rows whose correlation key matches no
/// inner group get NULL appended (LEFT JOIN semantics).
/// </summary>
public class CorrelatedScalarSubqueryTests
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

    private static LogicalPlan Plan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---------- Resolver ----------

    [Fact]
    public void Resolver_CorrelatedScalar_LiftsToCorrelatedJoinPlan()
    {
        // ProjectPlan → FilterPlan(x > $sub) → CorrelatedScalarSubqueryJoinPlan(...).
        var plan = Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE amt > (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region)");

        var project = Assert.IsType<ProjectPlan>(plan);
        var filter = Assert.IsType<FilterPlan>(project.Input);
        var csp = Assert.IsType<CorrelatedScalarSubqueryJoinPlan>(filter.Input);
        Assert.Single(csp.CorrelationKeys);
        // Scalar column is the last column of the inner Subquery's schema.
        Assert.Equal(csp.Subquery.Schema.Count - 1, csp.ScalarColumnIndex);
    }

    [Fact]
    public void Resolver_UncorrelatedScalar_StillUsesUncorrelatedBatch()
    {
        // Sanity: the WrapWithScalarSubqueries refactor didn't change the
        // uncorrelated path. Uncorrelated subqueries stay in a
        // ScalarSubqueryJoinPlan.
        var plan = Plan(
            [
                "CREATE TABLE orders (amt INT NOT NULL)",
                "CREATE TABLE thresh (v INT NOT NULL)",
            ],
            "SELECT amt FROM orders WHERE amt > (SELECT MAX(v) FROM thresh)");

        var project = Assert.IsType<ProjectPlan>(plan);
        var filter = Assert.IsType<FilterPlan>(project.Input);
        Assert.IsType<ScalarSubqueryJoinPlan>(filter.Input);
    }

    [Fact]
    public void Resolver_CorrelatedNonAggregate_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE cust > (SELECT vid FROM vips WHERE vips.region = orders.region)"));
        Assert.Contains("aggregate", ex.Message);
    }

    [Fact]
    public void Resolver_CorrelationInsideAggregate_Rejected()
    {
        // Correlation refs inside an aggregate argument fall out as
        // "unknown column" because CollectAggregatesInto doesn't thread the
        // outerSchema (correlation in aggregates isn't supported in v1
        // anyway). Either error shape is acceptable as a rejection signal.
        Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE cust > (SELECT MAX(amt + orders.region) FROM vips WHERE vips.region = orders.region)"));
    }

    [Fact]
    public void Resolver_NonEquiCorrelation_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE amt > (SELECT MAX(amt) FROM vips WHERE vips.region > orders.region)"));
        Assert.Contains("equi-correlation", ex.Message);
    }

    // ---------- End-to-end ----------

    [Fact]
    public void CorrelatedScalar_InWhere_FiltersByPerRegionMax()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders " +
            "WHERE amt > (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region)");

        // Region 1: max VIP amt = 100. Orders 200 passes; 50 doesn't.
        q.Table("orders").Insert(1, 1, 200);
        q.Table("orders").Insert(2, 1, 50);
        // Region 2: no VIP. The subquery is NULL → predicate NULL → row dropped.
        q.Table("orders").Insert(3, 2, 999);
        q.Table("vips").Insert(99, 1, 100);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 200));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void CorrelatedScalar_InSelect_AppendsPerRegionMax()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust, (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region) AS max_vip " +
            "FROM orders");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 2);
        q.Table("vips").Insert(99, 1, 100);
        q.Table("vips").Insert(98, 1, 50);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        // Region 2: no VIP → NULL.
        Assert.Equal(1, WeightOf(q.Current, 2, null));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void CorrelatedScalar_Retracts_WhenInnerGroupLosesItsMax()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust, (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region) AS max_vip " +
            "FROM orders");

        q.Table("orders").Insert(1, 1);
        q.Table("vips").Insert(99, 1, 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100));

        // Insert a higher VIP — max flips from 100 to 200.
        q.Table("vips").Insert(98, 1, 200);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 1, 200));

        // Retract the lone region-1 VIPs entirely — max → NULL.
        q.Table("vips").Delete(99, 1, 100);
        q.Table("vips").Delete(98, 1, 200);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 200));
        Assert.Equal(1, WeightOf(q.Current, 1, null));
    }

    [Fact]
    public void CorrelatedScalar_NullOuterKey_AppendsNull()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust, (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region) AS max_vip " +
            "FROM orders");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, null);  // NULL outer key — joins to nothing.
        q.Table("vips").Insert(99, 1, 100);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 2, null));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void CorrelatedScalar_DuplicateInExpression_DedupedAcrossSemiJoinLayers()
    {
        // Reference-equal SubqueryExpression appearing twice in WHERE
        // shouldn't add the same correlated-LEFT-JOIN layer twice. The
        // resolver's dedup map keys by reference, so two parsed-once
        // subquery references share a binding.
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders " +
            "WHERE amt > (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region) " +
            "AND amt < (SELECT MAX(amt) FROM vips WHERE vips.region = orders.region) + 1000");

        q.Table("orders").Insert(1, 1, 150);  // > 100, < 1100 — passes.
        q.Table("orders").Insert(2, 1, 50);   // ≤ 100 — fails first predicate.
        q.Table("vips").Insert(99, 1, 100);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 150));
        Assert.Equal(1, q.Current.Count);
    }
}
