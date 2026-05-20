// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class DerivedTableTests
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

    // ---- Parser ----

    [Fact]
    public void Parser_ParsesDerivedTableWithAs()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "SELECT x.k FROM (SELECT k, v FROM t) AS x");
        var from = Assert.IsType<DerivedTableReference>(stmt.From);
        Assert.Equal("x", from.Alias);
        Assert.IsType<SelectStatement>(from.Query);
    }

    [Fact]
    public void Parser_ParsesDerivedTableWithoutAs()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "SELECT x.k FROM (SELECT k FROM t) x");
        var from = Assert.IsType<DerivedTableReference>(stmt.From);
        Assert.Equal("x", from.Alias);
    }

    [Fact]
    public void Parser_DerivedTableRequiresAlias()
    {
        Assert.Throws<ParseException>(() => Parser.ParseStatement(
            "SELECT k FROM (SELECT k FROM t)"));
    }

    [Fact]
    public void Parser_DerivedTableInJoin()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "SELECT x.k, y.k FROM (SELECT k FROM t) x JOIN (SELECT k FROM u) y ON x.k = y.k");
        var join = Assert.IsType<JoinClause>(stmt.From);
        Assert.IsType<DerivedTableReference>(join.Left);
        Assert.IsType<DerivedTableReference>(join.Right);
    }

    [Fact]
    public void Parser_DerivedTableCanContainUnionAll()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "SELECT x.k FROM (SELECT k FROM t UNION ALL SELECT k FROM u) x");
        var from = Assert.IsType<DerivedTableReference>(stmt.From);
        Assert.IsType<SetOpQuery>(from.Query);
    }

    // ---- Resolver ----

    [Fact]
    public void Resolver_DerivedTableQualifiesColumnsWithAlias()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"));

        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "SELECT x.k, x.v FROM (SELECT k, v FROM t) x"))).Query;

        Assert.Equal(2, plan.Schema.Count);
        Assert.Equal("k", plan.Schema[0].Name);
        Assert.Equal("v", plan.Schema[1].Name);
    }

    [Fact]
    public void Resolver_DerivedTableUnknownColumn_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"));

        // `x.hidden` — the derived table projects only `k`, so `hidden` doesn't exist.
        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "SELECT x.hidden FROM (SELECT k FROM t) x")));
    }

    [Fact]
    public void Resolver_DerivedTableWrongAlias_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (k INT NOT NULL)"));

        // The alias is `x`, but the outer query uses `y`.
        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "SELECT y.k FROM (SELECT k FROM t) x")));
    }

    // ---- Runtime ----

    [Fact]
    public void Runtime_SimpleDerivedTable()
    {
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT x.k, x.v FROM (SELECT k, v FROM t WHERE v > 5) x");

        q.Table("t").Insert(1, 3);
        q.Table("t").Insert(2, 10);
        q.Table("t").Insert(3, 7);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 2, 10));
        Assert.Equal(1, WeightOf(q.Current, 3, 7));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Runtime_DerivedTableWithFilter()
    {
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k FROM (SELECT k, v FROM t) x WHERE x.v < 10");

        q.Table("t").Insert(1, 3);
        q.Table("t").Insert(2, 20);
        q.Table("t").Insert(3, 7);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Runtime_DerivedTableWithGroupBy()
    {
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT x.k, x.s FROM " +
            "  (SELECT k, SUM(v) AS s FROM t GROUP BY k) x " +
            "WHERE x.s > 10");

        q.Table("t").Insert(1, 5);
        q.Table("t").Insert(1, 7);   // k=1 sum=12 → passes
        q.Table("t").Insert(2, 3);   // k=2 sum=3 → filtered out
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 12L));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Runtime_JoinOfTwoDerivedTables()
    {
        var q = Compile(
            [
                "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE u (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT x.v, y.w FROM " +
            "  (SELECT k, v FROM t WHERE v > 0) x " +
            "JOIN (SELECT k, w FROM u) y ON x.k = y.k");

        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(2, -5);  // filtered out by inner derived table
        q.Table("u").Insert(1, 100);
        q.Table("u").Insert(2, 200);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 10, 100));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Runtime_DerivedTableInUnionAll()
    {
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT x.k FROM (SELECT k FROM t UNION ALL SELECT v FROM t) x");

        q.Table("t").Insert(1, 100);
        q.Table("t").Insert(2, 200);
        q.Step();

        // Each tuple contributes two rows through the UNION ALL.
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 100));
        Assert.Equal(1, WeightOf(q.Current, 200));
        Assert.Equal(4, q.Current.Count);
    }

    [Fact]
    public void Runtime_NestedDerivedTables()
    {
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT y.k FROM " +
            "  (SELECT x.k FROM (SELECT k, v FROM t WHERE v > 0) x WHERE x.k < 5) y");

        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(2, -5); // filtered by innermost (v > 0)
        q.Table("t").Insert(99, 10); // filtered by middle (k < 5)
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Runtime_DerivedTable_Incremental()
    {
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT x.k, x.s FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k) x");

        q.Table("t").Insert(1, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 10L));

        // Add to the same group: delta should retract old sum and emit new.
        q.Table("t").Insert(1, 5);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 10L));
        Assert.Equal(1, WeightOf(q.Current, 1, 15L));
    }

    [Fact]
    public void Runtime_DerivedTableSeesCtesInScope()
    {
        // A derived table inside a query with a CTE in scope should be able
        // to reference the CTE (via the query-level scope threaded through
        // the resolver).
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "WITH big AS (SELECT k, v FROM t WHERE v > 5) " +
            "SELECT x.k FROM (SELECT k FROM big) x");

        q.Table("t").Insert(1, 3);  // filtered by CTE
        q.Table("t").Insert(2, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, q.Current.Count);
    }
}
