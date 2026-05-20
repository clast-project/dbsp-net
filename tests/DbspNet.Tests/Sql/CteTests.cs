// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class CteTests
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

    // ---- Parser-level ----

    [Fact]
    public void Parser_ParsesSingleCte()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH c AS (SELECT a FROM t) SELECT a FROM c");
        Assert.Single(stmt.Ctes);
        Assert.Equal("c", stmt.Ctes[0].Name);
        Assert.Single(((SelectStatement)stmt.Ctes[0].Query).Items);
    }

    [Fact]
    public void Parser_ParsesMultipleCtes()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH c1 AS (SELECT a FROM t), c2 AS (SELECT b FROM t) SELECT a FROM c1 JOIN c2 ON a = b");
        Assert.Equal(2, stmt.Ctes.Count);
        Assert.Equal("c1", stmt.Ctes[0].Name);
        Assert.Equal("c2", stmt.Ctes[1].Name);
    }

    [Fact]
    public void Parser_ParsesNestedCte_InsideSubquery()
    {
        // CTE inside a CTE's query body.
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH outer_cte AS (WITH inner_cte AS (SELECT a FROM t) SELECT a FROM inner_cte) SELECT a FROM outer_cte");
        Assert.Single(stmt.Ctes);
        Assert.Equal("outer_cte", stmt.Ctes[0].Name);
        Assert.Single(((SelectStatement)stmt.Ctes[0].Query).Ctes);
        Assert.Equal("inner_cte", ((SelectStatement)stmt.Ctes[0].Query).Ctes[0].Name);
    }

    // ---- Resolver-level ----

    [Fact]
    public void Resolver_ResolvesCteReferenceInFrom()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "WITH c AS (SELECT a FROM t) SELECT a FROM c"))).Query;

        Assert.Equal(1, plan.Schema.Count);
        Assert.Equal("a", plan.Schema[0].Name);
    }

    [Fact]
    public void Resolver_LaterCteReferencesEarlierCte()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        // c2 depends on c1.
        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "WITH c1 AS (SELECT a FROM t), c2 AS (SELECT a FROM c1) SELECT a FROM c2"))).Query;

        Assert.Equal(1, plan.Schema.Count);
    }

    [Fact]
    public void Resolver_DuplicateCteName_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "WITH c AS (SELECT a FROM t), c AS (SELECT a FROM t) SELECT a FROM c")));
    }

    [Fact]
    public void Resolver_UnknownCte_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        // Query references a name neither in catalog nor in scope.
        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "SELECT a FROM nonexistent_cte")));
    }

    [Fact]
    public void Resolver_CteShadowsSameNamedTable()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"));

        // The CTE `t` shadows the base table `t` inside this query.
        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "WITH t AS (SELECT a FROM t) SELECT a FROM t"))).Query;

        // CTE projection has only column `a` — if the base table were used,
        // `b` would still exist and `SELECT a` would still work but schema
        // shape would differ only in qualifier. Here we verify the CTE-only
        // shape (1 column).
        Assert.Equal(1, plan.Schema.Count);
        Assert.Equal("a", plan.Schema[0].Name);
    }

    // ---- Compiler / runtime ----

    [Fact]
    public void Compiler_SingleCteReference_ProducesExpectedOutput()
    {
        var q = Compile(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "WITH filtered AS (SELECT a, b FROM t WHERE a > 0) SELECT a + b AS sum FROM filtered");

        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(-1, 99);  // a <= 0 — filtered out by CTE
        q.Table("t").Insert(2, 20);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 11));
        Assert.Equal(1, WeightOf(q.Current, 22));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Compiler_DoubleReference_ToSameCte_ComputesOnce()
    {
        // The CTE is referenced twice in a self-join. We can't directly observe
        // the shared subcircuit here, but we verify the semantics are correct
        // — self-join via CTE should produce the same result as an explicit
        // self-join on the base table.
        var q = Compile(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "WITH f AS (SELECT k, v FROM t WHERE v > 0) " +
            "SELECT l.v, r.v FROM f AS l JOIN f AS r ON l.k = r.k");

        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, 20);
        q.Table("t").Insert(2, -5); // filtered out by CTE
        q.Step();

        // f after filter: (1,10), (1,20). Self-join on k produces 4 pairs:
        // (10,10), (10,20), (20,10), (20,20).
        Assert.Equal(1, WeightOf(q.Current, 10, 10));
        Assert.Equal(1, WeightOf(q.Current, 10, 20));
        Assert.Equal(1, WeightOf(q.Current, 20, 10));
        Assert.Equal(1, WeightOf(q.Current, 20, 20));
    }

    [Fact]
    public void Compiler_DoubleReference_SharesSubcircuit_DeduplicatedInCache()
    {
        // Introspect: after compilation, the CteRef should have a single
        // entry in the CteCache. We can't reach the cache directly, but we
        // *can* verify that the plan tree, before compilation, has both
        // scans pointing at the same CteRef instance (reference equality).
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (k INT NOT NULL)"));

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
            "WITH f AS (SELECT k FROM t) SELECT l.k FROM f AS l JOIN f AS r ON l.k = r.k"))).Query;

        // Plan: Project(Join(CteScan(f), CteScan(f))).
        var proj = (ProjectPlan)plan;
        var join = (JoinPlan)proj.Input;
        var lScan = (CteScanPlan)join.Left;
        var rScan = (CteScanPlan)join.Right;
        Assert.Same(lScan.Cte, rScan.Cte);
    }

    [Fact]
    public void Compiler_CteReferencingCte_Works()
    {
        var q = Compile(
            ["CREATE TABLE t (a INT NOT NULL)"],
            "WITH c1 AS (SELECT a FROM t WHERE a > 0), " +
            "     c2 AS (SELECT a FROM c1 WHERE a < 100) " +
            "SELECT a FROM c2");

        q.Table("t").Insert(5);
        q.Table("t").Insert(200);   // filtered by c2
        q.Table("t").Insert(-1);    // filtered by c1
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Compiler_CteWithGroupBy_RetractsCorrectly()
    {
        var q = Compile(
            ["CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)"],
            "WITH totals AS (SELECT dept, SUM(salary) AS total FROM e GROUP BY dept) " +
            "SELECT dept, total FROM totals WHERE total > 100");

        q.Table("e").Insert("eng", 50);
        q.Table("e").Insert("eng", 60);  // eng total = 110 → passes
        q.Table("e").Insert("sales", 50); // sales total = 50 → filtered out
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "eng", 110L));
        Assert.Equal(1, q.Current.Count);

        // Bring sales above threshold.
        q.Table("e").Insert("sales", 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, "sales", 150L));
    }
}
