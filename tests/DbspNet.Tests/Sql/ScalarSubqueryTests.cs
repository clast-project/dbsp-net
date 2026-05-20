// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class ScalarSubqueryTests
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
    public void Parser_ParsesScalarSubqueryInWhere()
    {
        var stmt = Parser.ParseStatement(
            "SELECT x FROM t WHERE x > (SELECT MAX(y) FROM u)");
        Assert.NotNull(stmt);
    }

    [Fact]
    public void Parser_ParenthesizedExprStillWorks()
    {
        var stmt = Parser.ParseStatement("SELECT (1 + 2) * 3 AS x FROM t");
        Assert.NotNull(stmt);
    }

    // ---- Resolver ----

    [Fact]
    public void Resolver_RejectsMultiColumnSubquery()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"));

        Assert.Throws<ResolveException>(() =>
            r.Resolve(Parser.ParseStatement("SELECT a FROM t WHERE a > (SELECT a, b FROM t)")));
    }

    // ---- Compiler / runtime: WHERE ----

    [Fact]
    public void WhereScalarSubquery_FiltersByAggregate()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE thresh (v INT NOT NULL)",
            ],
            "SELECT x FROM t WHERE x > (SELECT MAX(v) FROM thresh)");

        q.Table("t").Insert(1);
        q.Table("t").Insert(5);
        q.Table("t").Insert(10);
        q.Table("thresh").Insert(4);
        q.Step();

        // MAX(v) = 4, so rows with x > 4 pass.
        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, WeightOf(q.Current, 10));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void WhereScalarSubquery_EmptySubquery_ComparisonYieldsNull_FiltersOut()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE thresh (v INT NOT NULL)",
            ],
            "SELECT x FROM t WHERE x > (SELECT MAX(v) FROM thresh)");

        q.Table("t").Insert(1);
        q.Table("t").Insert(100);
        // thresh is empty → MAX(v) returns NULL → x > NULL is NULL → WHERE filters out.
        q.Step();

        Assert.Empty(q.Current);
    }

    [Fact]
    public void WhereScalarSubquery_ChangingValue_RetractsOldRows_EmitsNew()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE thresh (v INT NOT NULL)",
            ],
            "SELECT x FROM t WHERE x > (SELECT MAX(v) FROM thresh)");

        q.Table("t").Insert(1);
        q.Table("t").Insert(5);
        q.Table("t").Insert(10);
        q.Table("thresh").Insert(4);
        q.Step();
        Assert.Equal(2, q.Current.Count);

        // Raise threshold: MAX becomes 7, so only x > 7 passes.
        q.Table("thresh").Insert(7);
        q.Step();

        // Row (5) should be retracted; (10) still matches, so no change.
        Assert.Equal(-1, WeightOf(q.Current, 5));
        Assert.Equal(0, WeightOf(q.Current, 10));
    }

    // ---- Compiler / runtime: SELECT ----

    [Fact]
    public void SelectScalarSubquery_AppendsColumn()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (y INT NOT NULL)",
            ],
            "SELECT x, (SELECT SUM(y) FROM u) AS total FROM t");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("u").Insert(10);
        q.Table("u").Insert(20);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 30L));
        Assert.Equal(1, WeightOf(q.Current, 2, 30L));
    }

    [Fact]
    public void SelectScalarSubquery_EmptySubquery_YieldsNull()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (y INT NOT NULL)",
            ],
            "SELECT x, (SELECT SUM(y) FROM u) AS total FROM t");

        q.Table("t").Insert(1);
        q.Step();

        // u is empty → SUM(y) is NULL.
        Assert.Equal(1, WeightOf(q.Current, 1, null));
    }

    // ---- Compiler / runtime: HAVING ----

    [Fact]
    public void HavingScalarSubquery_FiltersGroups()
    {
        var q = Compile(
            [
                "CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)",
                "CREATE TABLE avg_threshold (v INT NOT NULL)",
            ],
            "SELECT dept, SUM(salary) AS total FROM e " +
            "GROUP BY dept HAVING SUM(salary) > (SELECT MAX(v) FROM avg_threshold)");

        q.Table("e").Insert("eng", 100);
        q.Table("e").Insert("eng", 200);
        q.Table("e").Insert("sales", 50);
        q.Table("avg_threshold").Insert(100);
        q.Step();

        // eng total = 300 > 100 → passes; sales total = 50 < 100 → filtered out.
        Assert.Equal(1, WeightOf(q.Current, "eng", 300L));
        Assert.Equal(1, q.Current.Count);
    }

    // ---- Deduplication ----

    [Fact]
    public void DuplicateSubqueryInExpression_ProducesCorrectResults()
    {
        // The same subquery text appears twice in the predicate. The parser
        // produces two independent SubqueryExpression ASTs that compare
        // unequal (C# record equality on their embedded collection fields is
        // by reference), so the resolver currently compiles two separate
        // subcircuits. That's a missed optimisation but not a correctness
        // issue — semantics still hold. Use a CTE if you want guaranteed
        // sharing: `WITH sq AS (SELECT MAX(y) FROM u) SELECT … FROM sq`.
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (y INT NOT NULL)",
            ],
            "SELECT x FROM t WHERE x > (SELECT MAX(y) FROM u) AND x < (SELECT MAX(y) FROM u) + 100");

        q.Table("t").Insert(1);
        q.Table("t").Insert(50);
        q.Table("t").Insert(200);
        q.Table("u").Insert(40);
        q.Step();

        // MAX(y) = 40; predicate = x > 40 AND x < 140. Only x=50 qualifies.
        Assert.Equal(1, WeightOf(q.Current, 50));
        Assert.Equal(1, q.Current.Count);
    }

    // ---- CTE + subquery interaction ----

    [Fact]
    public void CteAndSubqueryTogether_Work()
    {
        var q = Compile(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "WITH big AS (SELECT x FROM t WHERE x > 5) " +
            "SELECT x FROM big WHERE x > (SELECT MIN(x) FROM big)");

        q.Table("t").Insert(3);   // filtered by CTE
        q.Table("t").Insert(7);   // in CTE; MIN in CTE is 7
        q.Table("t").Insert(10);
        q.Table("t").Insert(15);
        q.Step();

        // big = {7, 10, 15}, MIN = 7, so x > 7 passes: {10, 15}.
        Assert.Equal(1, WeightOf(q.Current, 10));
        Assert.Equal(1, WeightOf(q.Current, 15));
        Assert.Equal(2, q.Current.Count);
    }
}
