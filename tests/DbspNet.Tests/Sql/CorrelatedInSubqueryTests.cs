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
/// Coverage for the correlated form of <c>x IN (subquery)</c> in WHERE.
/// The resolver threads an outer-schema reference into the subquery's
/// resolution; any <see cref="ResolvedCorrelationRef"/> the inner produces
/// is then decorrelated into additional <see cref="SemiJoinEqui"/> entries
/// against the outer columns, with the matching equi-predicate stripped
/// from the inner WHERE. The result is a multi-key <see cref="SemiJoinPlan"/>
/// equivalent to <c>outer ⋈ Distinct(subquery) ON probe = inner_probe AND
/// outer.corr_i = inner.corr_i</c>.
/// </summary>
public class CorrelatedInSubqueryTests
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
    public void Resolver_CorrelatedIn_LiftsToMultiKeySemiJoin()
    {
        var plan = Plan(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust FROM orders WHERE cust IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        // ProjectPlan → SemiJoinPlan(EquiKeys.Count == 2) → Scan(orders)
        var project = Assert.IsType<ProjectPlan>(plan);
        var semi = Assert.IsType<SemiJoinPlan>(project.Input);
        Assert.Equal(2, semi.EquiKeys.Count);
        // EquiKeys[0] = probe (outer.cust = inner.last_col); EquiKeys[1] = correlation (outer.region = inner.0)
        Assert.False(semi.IsAnti);
    }

    [Fact]
    public void Resolver_UncorrelatedIn_StillProducesSingleKeySemiJoin()
    {
        // Sanity: the multi-key generalization didn't break the uncorrelated path.
        var plan = Plan(
            [
                "CREATE TABLE t (id INT NOT NULL)",
                "CREATE TABLE u (uid INT NOT NULL)",
            ],
            "SELECT id FROM t WHERE id IN (SELECT uid FROM u)");

        var project = Assert.IsType<ProjectPlan>(plan);
        var semi = Assert.IsType<SemiJoinPlan>(project.Input);
        Assert.Single(semi.EquiKeys);
    }

    [Fact]
    public void Resolver_NonEquiCorrelation_Rejected()
    {
        // Inequality correlation (outer.col > inner.col) is not supported in v1.
        var ex = Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE t (id INT NOT NULL, k INT NOT NULL)",
                "CREATE TABLE u (uid INT NOT NULL, k INT NOT NULL)",
            ],
            "SELECT id FROM t WHERE id IN (SELECT uid FROM u WHERE u.k > t.k)"));
        Assert.Contains("equi-correlation", ex.Message);
    }

    [Fact]
    public void Resolver_BareCorrelationWithoutEqui_Rejected()
    {
        // Correlation reference appearing without an equi-predicate (here, in
        // an arithmetic expression) is rejected — the decorrelator needs a
        // clean (outer.col = inner.col) to lift.
        var ex = Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE t (id INT NOT NULL, k INT NOT NULL)",
                "CREATE TABLE u (uid INT NOT NULL, n INT NOT NULL)",
            ],
            "SELECT id FROM t WHERE id IN (SELECT uid FROM u WHERE u.n + t.k > 0)"));
        Assert.Contains("equi-correlation", ex.Message);
    }

    // ---------- End-to-end ----------

    [Fact]
    public void Correlated_SameRegionVips_FiltersByRegionAndMembership()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE cust IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        // Region 1: cust 1 is a VIP (same region); cust 2 is not.
        // Region 2: cust 1 is a VIP in region 2 (different person, same id).
        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 1);
        q.Table("orders").Insert(3, 2);
        q.Table("vips").Insert(1, 1);
        q.Table("vips").Insert(3, 2);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 3, 2));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Correlated_CrossRegionMatch_FilteredOut()
    {
        // Outer (cust=1, region=1); VIP (vid=1, region=2) — same ID but different
        // regions, so the correlated predicate filters the row out.
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE cust IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("vips").Insert(1, 2); // ID matches but region doesn't.
        q.Step();

        Assert.True(q.Current.IsEmpty);
    }

    [Fact]
    public void Correlated_RetractsWhenInnerLosesMatch()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE cust IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("vips").Insert(1, 1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1));

        // Retract the VIP — the outer row should retract.
        q.Table("vips").Delete(1, 1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 1));
    }

    [Fact]
    public void Correlated_EquivalentToInnerJoinPlusUnion()
    {
        // The decorrelated SemiJoinPlan should produce identical deltas to an
        // INNER JOIN spelled directly with deduplication. SQL surface today
        // has no SELECT DISTINCT — we use UNION-with-self as the dedup proxy.
        var sq = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT o.cust, o.region FROM orders o " +
            "WHERE o.cust IN (SELECT v.vid FROM vips v WHERE v.region = o.region)");

        var jq = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT o.cust, o.region FROM orders o INNER JOIN vips v " +
            "ON o.cust = v.vid AND o.region = v.region " +
            "UNION SELECT o.cust, o.region FROM orders o INNER JOIN vips v " +
            "ON o.cust = v.vid AND o.region = v.region");

        void Tick(Action<CompiledQuery> apply)
        {
            apply(sq); apply(jq);
            sq.Step(); jq.Step();
            Assert.Equal(jq.Current, sq.Current);
        }

        Tick(q => { q.Table("orders").Insert(1, 1); q.Table("vips").Insert(1, 1); });
        Tick(q => { q.Table("orders").Insert(2, 1); q.Table("vips").Insert(3, 2); });
        Tick(q => { q.Table("orders").Insert(3, 2); });
        Tick(q => { q.Table("vips").Delete(1, 1); });
    }

    [Fact]
    public void Correlated_WithExtraScalarPredicate()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders " +
            "WHERE cust IN (SELECT vid FROM vips WHERE vips.region = orders.region) " +
            "AND amt > 100");

        q.Table("orders").Insert(1, 1, 200);  // VIP, large
        q.Table("orders").Insert(1, 1, 50);   // VIP, small — filtered by amt
        q.Table("orders").Insert(2, 1, 500);  // not VIP — filtered by IN
        q.Table("vips").Insert(1, 1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 200));
        Assert.Equal(1, q.Current.Count);
    }
}
