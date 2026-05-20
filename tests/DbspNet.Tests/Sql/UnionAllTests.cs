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

public class UnionAllTests
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
    public void Parser_ParsesTwoBranchUnionAll()
    {
        var stmt = (SetOpQuery)Parser.ParseStatement(
            "SELECT a FROM t UNION ALL SELECT b FROM u");
        Assert.Equal(2, stmt.Branches.Count);
        Assert.IsType<SelectStatement>(stmt.Branches[0]);
        Assert.IsType<SelectStatement>(stmt.Branches[1]);
    }

    [Fact]
    public void Parser_ParsesThreeBranchChainFlat()
    {
        var stmt = (SetOpQuery)Parser.ParseStatement(
            "SELECT a FROM t UNION ALL SELECT b FROM u UNION ALL SELECT c FROM v");
        Assert.Equal(3, stmt.Branches.Count);
    }

    [Fact]
    public void Parser_PlainUnion_ParsesAsSetUnion()
    {
        // Plain UNION is now supported as set-semantics (DISTINCT). The
        // SetOpKind distinguishes it from UNION ALL.
        var stmt = (SetOpQuery)Parser.ParseStatement(
            "SELECT a FROM t UNION SELECT b FROM u");
        Assert.Equal(SetOpKind.Union, stmt.Kind);
    }

    [Fact]
    public void Parser_WithClauseAppliesToUnionAll()
    {
        var stmt = (SetOpQuery)Parser.ParseStatement(
            "WITH c AS (SELECT a FROM t) SELECT a FROM c UNION ALL SELECT a FROM c");
        Assert.Single(stmt.Ctes);
        Assert.Equal(2, stmt.Branches.Count);
    }

    [Fact]
    public void Parser_UnionAllInsideCteBody()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH both AS (SELECT a FROM t UNION ALL SELECT b FROM u) SELECT a FROM both");
        Assert.Single(stmt.Ctes);
        Assert.IsType<SetOpQuery>(stmt.Ctes[0].Query);
    }

    [Fact]
    public void Parser_UnionAllInScalarSubquery()
    {
        var stmt = Parser.ParseStatement(
            "SELECT x FROM t WHERE x > (SELECT v FROM a UNION ALL SELECT v FROM b)");
        Assert.NotNull(stmt);
    }

    // ---- Resolver ----

    [Fact]
    public void Resolver_RejectsArityMismatch()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"));
        r.Resolve(Parser.ParseStatement("CREATE TABLE u (x INT NOT NULL)"));

        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "SELECT a, b FROM t UNION ALL SELECT x FROM u")));
    }

    [Fact]
    public void Resolver_RejectsIncompatibleTypes()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));
        r.Resolve(Parser.ParseStatement("CREATE TABLE u (s VARCHAR NOT NULL)"));

        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "SELECT a FROM t UNION ALL SELECT s FROM u")));
    }

    [Fact]
    public void Resolver_UnifiesCompatibleNumericTypes()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));
        r.Resolve(Parser.ParseStatement("CREATE TABLE u (b BIGINT NOT NULL)"));

        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "SELECT a FROM t UNION ALL SELECT b FROM u"))).Query;

        Assert.IsType<SqlBigintType>(plan.Schema[0].Type);
    }

    [Fact]
    public void Resolver_NullabilityIsUnionOfBranches()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));
        r.Resolve(Parser.ParseStatement("CREATE TABLE u (a INT)"));

        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "SELECT a FROM t UNION ALL SELECT a FROM u"))).Query;

        Assert.True(plan.Schema[0].Type.Nullable);
    }

    [Fact]
    public void Resolver_OutputColumnNamesComeFromFirstBranch()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));
        r.Resolve(Parser.ParseStatement("CREATE TABLE u (b INT NOT NULL)"));

        var plan = ((SelectPlan)r.Resolve(Parser.ParseStatement(
            "SELECT a FROM t UNION ALL SELECT b FROM u"))).Query;

        Assert.Equal("a", plan.Schema[0].Name);
    }

    // ---- Compiler / runtime ----

    [Fact]
    public void Compiler_TwoBranchUnionAll_ConcatenatesRows()
    {
        var q = Compile(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (b INT NOT NULL)",
            ],
            "SELECT a FROM t UNION ALL SELECT b FROM u");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("u").Insert(3);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void Compiler_UnionAll_KeepsDuplicates()
    {
        var q = Compile(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (b INT NOT NULL)",
            ],
            "SELECT a FROM t UNION ALL SELECT b FROM u");

        q.Table("t").Insert(1);
        q.Table("u").Insert(1);  // same value as in t
        q.Step();

        // UNION ALL preserves multiplicities — (1) appears twice (weight 2).
        Assert.Equal(2, WeightOf(q.Current, 1));
    }

    [Fact]
    public void Compiler_UnionAll_EmitsIncrementally()
    {
        var q = Compile(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (b INT NOT NULL)",
            ],
            "SELECT a FROM t UNION ALL SELECT b FROM u");

        q.Table("t").Insert(1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1));

        // New row on the other branch — delta should contain only the new row.
        q.Table("u").Insert(2);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(0, WeightOf(q.Current, 1));  // pre-existing row not re-emitted

        // Retract on first branch — delta contains negative-weight row.
        q.Table("t").Delete(1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1));
    }

    [Fact]
    public void Compiler_ThreeBranchUnionAll_Works()
    {
        var q = Compile(
            [
                "CREATE TABLE a (x INT NOT NULL)",
                "CREATE TABLE b (x INT NOT NULL)",
                "CREATE TABLE c (x INT NOT NULL)",
            ],
            "SELECT x FROM a UNION ALL SELECT x FROM b UNION ALL SELECT x FROM c");

        q.Table("a").Insert(1);
        q.Table("b").Insert(2);
        q.Table("c").Insert(3);
        q.Step();

        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void Compiler_UnionAll_WithTypeCoercion()
    {
        var q = Compile(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (b BIGINT NOT NULL)",
            ],
            "SELECT a FROM t UNION ALL SELECT b FROM u");

        q.Table("t").Insert(1);          // INT (boxed int)
        q.Table("u").Insert(2L);          // BIGINT (boxed long)
        q.Step();

        // Both rows should have been cast to BIGINT (long) in the output.
        Assert.Equal(1, WeightOf(q.Current, 1L));
        Assert.Equal(1, WeightOf(q.Current, 2L));
    }

    [Fact]
    public void Compiler_UnionAllInsideCte()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (x INT NOT NULL)",
            ],
            "WITH all_x AS (SELECT x FROM t UNION ALL SELECT x FROM u) " +
            "SELECT x FROM all_x WHERE x > 0");

        q.Table("t").Insert(-1);
        q.Table("t").Insert(5);
        q.Table("u").Insert(-2);
        q.Table("u").Insert(10);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, WeightOf(q.Current, 10));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Compiler_UnionAll_WithCtesVisibleInBothBranches()
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "WITH nums AS (SELECT v FROM t) " +
            "SELECT v FROM nums WHERE v < 10 UNION ALL SELECT v FROM nums WHERE v > 100");

        q.Table("t").Insert(5);
        q.Table("t").Insert(50);    // filtered out by both branches
        q.Table("t").Insert(200);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, WeightOf(q.Current, 200));
        Assert.Equal(2, q.Current.Count);
    }
}
