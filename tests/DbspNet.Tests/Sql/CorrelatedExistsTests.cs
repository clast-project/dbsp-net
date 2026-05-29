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
/// Coverage for the correlated form of <c>EXISTS (subquery)</c> in WHERE.
/// The resolver lifts EXISTS conjuncts at the top-level AND chain: if the
/// inner subquery has no <see cref="ResolvedCorrelationRef"/>, the existing
/// <c>COALESCE((SELECT COUNT(*) FROM (sq)), 0) &gt; 0</c> desugar runs as
/// before; if it does, <c>DecorrelateSubqueryPlan</c> strips correlation
/// equi-predicates and emits a <see cref="SemiJoinPlan"/> whose EquiKeys
/// cover only the correlation columns (no IN-probe key).
/// </summary>
public class CorrelatedExistsTests
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
    public void Resolver_CorrelatedExists_LiftsToCorrelationOnlySemiJoin()
    {
        var plan = Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        // ProjectPlan → SemiJoinPlan(EquiKeys.Count == 1) → Scan(orders).
        // The single equi-key is the correlation: outer.region = inner.region.
        // There is no IN-probe key (unlike correlated IN).
        var project = Assert.IsType<ProjectPlan>(plan);
        var semi = Assert.IsType<SemiJoinPlan>(project.Input);
        Assert.Single(semi.EquiKeys);
        Assert.False(semi.IsAnti);
    }

    [Fact]
    public void Resolver_UncorrelatedExists_StillUsesScalarSubqueryPath()
    {
        // Sanity: the AST move from parser-time desugar to resolver-time
        // desugar didn't change the uncorrelated plan shape — it's still
        // ScalarSubqueryJoinPlan + FilterPlan, no SemiJoinPlan.
        var plan = Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE EXISTS (SELECT 1 FROM vips)");

        var project = Assert.IsType<ProjectPlan>(plan);
        // Filter over ScalarSubqueryJoinPlan over Scan — no SemiJoinPlan here.
        var filter = Assert.IsType<FilterPlan>(project.Input);
        Assert.IsType<ScalarSubqueryJoinPlan>(filter.Input);
    }

    [Fact]
    public void Resolver_CorrelatedNotExists_LiftsToAntiSemiJoin()
    {
        var plan = Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE NOT EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        var project = Assert.IsType<ProjectPlan>(plan);
        var semi = Assert.IsType<SemiJoinPlan>(project.Input);
        Assert.True(semi.IsAnti);
        Assert.Single(semi.EquiKeys);
    }

    [Fact]
    public void Resolver_NonEquiCorrelation_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE EXISTS (SELECT 1 FROM vips WHERE vips.region > orders.region)"));
        Assert.Contains("equi-correlation", ex.Message);
    }

    // ---------- End-to-end ----------

    [Fact]
    public void CorrelatedExists_EmitsOuterRowsThatHaveAMatch()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 1);
        q.Table("orders").Insert(3, 2);
        q.Table("vips").Insert(99, 1);  // any VIP in region 1
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 2, 1));
        // Region 2 has no VIP — order 3 doesn't pass EXISTS.
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void CorrelatedExists_RetractsWhenInnerEmptiesItsRegion()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("vips").Insert(99, 1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1));

        // Retract the lone VIP in region 1 — the order should retract.
        q.Table("vips").Delete(99, 1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 1));
    }

    [Fact]
    public void CorrelatedExists_EquivalentToInnerJoinPlusUnion()
    {
        // Semi-join semantics: outer rows that match at least one inner row
        // on the correlation key, deduped. We compare against
        // `outer ⋈ inner ON outer.region = inner.region` followed by
        // UNION-with-self (the dedup proxy SQL surface today gives us).
        var sq = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT o.cust, o.region FROM orders o " +
            "WHERE EXISTS (SELECT 1 FROM vips v WHERE v.region = o.region)");

        var jq = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT o.cust, o.region FROM orders o INNER JOIN vips v ON v.region = o.region " +
            "UNION " +
            "SELECT o.cust, o.region FROM orders o INNER JOIN vips v ON v.region = o.region");

        void Tick(Action<CompiledQuery> apply)
        {
            apply(sq); apply(jq);
            sq.Step(); jq.Step();
            Assert.Equal(jq.Current, sq.Current);
        }

        Tick(q => { q.Table("orders").Insert(1, 1); q.Table("vips").Insert(99, 1); });
        Tick(q => { q.Table("orders").Insert(2, 1); q.Table("vips").Insert(98, 1); });
        Tick(q => { q.Table("orders").Insert(3, 2); });
        Tick(q => { q.Table("vips").Insert(97, 2); });
        Tick(q => { q.Table("vips").Delete(99, 1); });
    }

    [Fact]
    public void CorrelatedExists_AndedWithScalarPredicate()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders " +
            "WHERE EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region) " +
            "AND amt > 100");

        q.Table("orders").Insert(1, 1, 200); // same-region VIP, large
        q.Table("orders").Insert(2, 1, 50);  // same-region VIP, small — filtered by amt
        q.Table("orders").Insert(3, 2, 500); // no VIP in region 2 — filtered by EXISTS
        q.Table("vips").Insert(99, 1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 200));
        Assert.Equal(1, q.Current.Count);
    }

    // ---------- Regressions for the AST move ----------

    [Fact]
    public void Regression_UncorrelatedExistsInSelect_StillWorks()
    {
        // The parser-time desugar moved to the resolver. SELECT-position
        // EXISTS used to ride directly on the COALESCE shape the parser
        // built; now ResolveExistsAsScalar synthesises the same shape from
        // the cached CountSubquery.
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (flag INT NOT NULL)",
            ],
            "SELECT x, EXISTS (SELECT 1 FROM u) AS has_u FROM t");

        q.Table("t").Insert(1);
        q.Table("u").Insert(42);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
    }

    [Fact]
    public void Regression_UncorrelatedNotExists_StillWorks()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL, name VARCHAR NOT NULL)",
                "CREATE TABLE u (flag INT NOT NULL)",
            ],
            "SELECT name FROM t WHERE NOT EXISTS (SELECT 1 FROM u)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        q.Step();
        // u is empty → NOT EXISTS = TRUE → all rows pass.
        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, WeightOf(q.Current, "b"));
    }
}
