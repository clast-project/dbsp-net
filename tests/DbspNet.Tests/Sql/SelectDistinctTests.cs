// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for <c>SELECT DISTINCT</c> (and the default <c>SELECT ALL</c>).
/// DISTINCT wraps the projection in a <c>DistinctPlan</c>, so every surviving
/// row carries weight 1 regardless of input multiplicity.
/// </summary>
public class SelectDistinctTests
{
    private static CompiledQuery Compile(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Fact]
    public void Distinct_CollapsesDuplicateRows()
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT DISTINCT a FROM t");
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(2);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Step();

        Assert.Equal(3, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    [Fact]
    public void All_KeepsBagMultiplicity()
    {
        // SELECT ALL (the default) preserves duplicate weights.
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT ALL a FROM t");
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Step();

        Assert.Equal(2, WeightOf(q.Current, 1));
    }

    [Fact]
    public void Distinct_MultiColumn()
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT DISTINCT a, b FROM t");
        q.Table("t").Insert(1, 1);
        q.Table("t").Insert(1, 1);
        q.Table("t").Insert(1, 2);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 1, 2));
    }

    [Fact]
    public void Distinct_OverExpression()
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL)",
            "SELECT DISTINCT a % 2 AS p FROM t");
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Table("t").Insert(4);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 0));
        Assert.Equal(1, WeightOf(q.Current, 1));
    }

    [Fact]
    public void Distinct_SurvivesOptimizer()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT DISTINCT a FROM t WHERE a > 0"))).Query;
        var q = PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Fact]
    public void Distinct_IsIncremental()
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT DISTINCT a FROM t");
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1));   // first copy emits the row

        // A second copy of an already-present value emits nothing.
        q.Table("t").Insert(1);
        q.Step();
        Assert.Equal(0, q.Current.Count);

        // A new value emits exactly one row.
        q.Table("t").Insert(2);
        q.Step();
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
    }
}
