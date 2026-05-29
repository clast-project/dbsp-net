// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for <c>probe IN (subquery)</c> in WHERE — uncorrelated only.
/// At resolve time the WHERE clause's top-level AND chain is split; each
/// <see cref="InSubqueryExpression"/> conjunct lifts to a
/// <see cref="SemiJoinPlan"/> over the input. The remaining scalar
/// predicates stay in a <see cref="FilterPlan"/>. Compiles to
/// <c>Distinct(sq) ⋈ outer</c> on the equi-key; the inner join's NULL-key
/// filter gives SQL three-valued semantics at the WHERE boundary.
/// </summary>
public class InSubqueryTests
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
    public void Resolver_WhereInSubquery_LiftsToSemiJoinPlan()
    {
        var plan = Plan(
            [
                "CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT name FROM t WHERE id IN (SELECT vid FROM vips)");

        // ProjectPlan over SemiJoinPlan over ScanPlan(t) / ScanPlan(vips).
        var project = Assert.IsType<ProjectPlan>(plan);
        Assert.IsType<SemiJoinPlan>(project.Input);
    }

    [Fact]
    public void Resolver_WhereInSubquery_AndedWithScalar_LiftsBoth()
    {
        // Mixed conjunct: IN-subquery + a scalar predicate. The semi-join
        // wraps the input; the scalar predicate stays in a FilterPlan.
        var plan = Plan(
            [
                "CREATE TABLE t (id INT NOT NULL, x INT NOT NULL)",
                "CREATE TABLE u (uid INT NOT NULL)",
            ],
            "SELECT id FROM t WHERE id IN (SELECT uid FROM u) AND x > 0");

        // ProjectPlan → FilterPlan(x>0) → SemiJoinPlan → ScanPlan(t)
        var project = Assert.IsType<ProjectPlan>(plan);
        var filter = Assert.IsType<FilterPlan>(project.Input);
        Assert.IsType<SemiJoinPlan>(filter.Input);
    }

    [Fact]
    public void Resolver_InSubqueryInSelect_Rejected()
    {
        // IN-subquery as a scalar boolean expression (SELECT-position) is
        // deferred; resolver rejects with a clear message.
        var ex = Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE t (id INT NOT NULL)",
                "CREATE TABLE u (uid INT NOT NULL)",
            ],
            "SELECT id IN (SELECT uid FROM u) AS hit FROM t"));
        Assert.Contains("IN (subquery)", ex.Message);
    }

    [Fact]
    public void Resolver_NotInSubquery_NotNullOperands_LiftsToAntiSemiJoin()
    {
        var plan = Plan(
            [
                "CREATE TABLE t (id INT NOT NULL)",
                "CREATE TABLE u (uid INT NOT NULL)",
            ],
            "SELECT id FROM t WHERE id NOT IN (SELECT uid FROM u)");

        var project = Assert.IsType<ProjectPlan>(plan);
        var semi = Assert.IsType<SemiJoinPlan>(project.Input);
        Assert.True(semi.IsAnti);
        Assert.Single(semi.EquiKeys);
    }

    // The previous nullable-NOT-IN rejection tests were replaced once
    // full 3VL ships — see NullableNotInTests.cs for the positive coverage.

    [Fact]
    public void Resolver_MultiColumnSubquery_Rejected()
    {
        Assert.Throws<ResolveException>(() => Plan(
            [
                "CREATE TABLE t (id INT NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL, b INT NOT NULL)",
            ],
            "SELECT id FROM t WHERE id IN (SELECT a, b FROM u)"));
    }

    // Note: correlated IN-subquery is no longer rejected — it's decorrelated.
    // See CorrelatedInSubqueryTests.cs for the positive coverage.

    // ---------- End-to-end ----------

    [Fact]
    public void WhereInSubquery_FiltersByMembership()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("orders").Insert(2, 50);
        q.Table("orders").Insert(3, 30);
        q.Table("vips").Insert(1);
        q.Table("vips").Insert(3);
        q.Step();

        // Customers 1 and 3 are VIPs; 2 is not.
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, WeightOf(q.Current, 3, 30));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void WhereInSubquery_DuplicateSubqueryValuesDoNotMultiply()
    {
        // The Distinct in front of the subquery matters: even though `vips`
        // has VID=1 twice, the outer order(1, 100) emits exactly once.
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("vips").Insert(1);
        q.Table("vips").Insert(1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void WhereInSubquery_RetractsWhenMatchValueLeavesSubquery()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("vips").Insert(1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100));

        // Retract the VIP — the order should retract from the view.
        q.Table("vips").Delete(1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 100));
    }

    [Fact]
    public void WhereInSubquery_AndedWithScalarPredicate()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust IN (SELECT vid FROM vips) AND amt > 50");

        q.Table("orders").Insert(1, 100); // VIP, large
        q.Table("orders").Insert(1, 30);  // VIP, small — filtered by amt
        q.Table("orders").Insert(2, 200); // not VIP — filtered by IN
        q.Table("vips").Insert(1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void WhereInSubquery_NullOuterKey_Filtered()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT amt FROM orders WHERE cust IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("orders").Insert(null, 50); // NULL cust — never matches.
        q.Table("vips").Insert(1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 100));
        Assert.Equal(1, q.Current.Count);
    }
}
