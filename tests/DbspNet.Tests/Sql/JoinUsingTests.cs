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
/// Coverage for <c>JOIN ... USING (c1, …)</c>. USING resolves to an equi-join
/// on the named columns plus a projection that merges each shared column to a
/// single unqualified copy (taken from the preserved side for outer joins).
/// Output order is: merged USING columns, then remaining left, then remaining
/// right.
/// </summary>
public class JoinUsingTests
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

    private static readonly string[] TwoTables =
    [
        "CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
        "CREATE TABLE b (k INT NOT NULL, y INT NOT NULL)",
    ];

    [Fact]
    public void InnerUsing_MergesJoinColumn()
    {
        var q = Compile(TwoTables, "SELECT * FROM a JOIN b USING (k)");
        q.Table("a").Insert(1, 10);
        q.Table("a").Insert(2, 20);
        q.Table("b").Insert(1, 100);
        q.Table("b").Insert(3, 300);
        q.Step();

        // Only k=1 matches; output is the merged [k, x, y].
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 100));
    }

    [Fact]
    public void Using_MergedColumnReferencedUnqualified()
    {
        // The merged column is unqualified; selecting it by bare name works,
        // and the per-side columns remain reachable.
        var q = Compile(TwoTables, "SELECT k, x, y FROM a JOIN b USING (k)");
        q.Table("a").Insert(5, 50);
        q.Table("b").Insert(5, 500);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5, 50, 500));
    }

    [Fact]
    public void LeftJoinUsing_PreservesUnmatchedLeftRow()
    {
        var q = Compile(TwoTables, "SELECT * FROM a LEFT JOIN b USING (k)");
        q.Table("a").Insert(1, 10);
        q.Table("a").Insert(2, 20);
        q.Table("b").Insert(1, 100);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 100));
        Assert.Equal(1, WeightOf(q.Current, 2, 20, null));   // unmatched: y is NULL
    }

    [Fact]
    public void MultiColumnUsing()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k1 INT NOT NULL, k2 INT NOT NULL, x INT NOT NULL)",
                "CREATE TABLE b (k1 INT NOT NULL, k2 INT NOT NULL, y INT NOT NULL)",
            ],
            "SELECT * FROM a JOIN b USING (k1, k2)");
        q.Table("a").Insert(1, 2, 10);
        q.Table("a").Insert(1, 9, 11);
        q.Table("b").Insert(1, 2, 100);
        q.Step();

        // Merged [k1, k2, x, y] for the (1,2) match only.
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 10, 100));
    }

    [Fact]
    public void InnerUsing_SurvivesOptimizer()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in TwoTables)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT k, x, y FROM a JOIN b USING (k)"))).Query;
        var q = PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
        q.Table("a").Insert(1, 10);
        q.Table("b").Insert(1, 100);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 10, 100));
    }

    [Fact]
    public void QualifiedStar_IncludesMergedUsingKey()
    {
        // `a.*` must include the merged USING key (attributed to the source
        // side), matching PostgreSQL. Regression for the ivm-bench SCD2 models
        // (`select s1.* … using (symbol)`), which lost the key when it was
        // null-qualified.
        var q = Compile(TwoTables, "SELECT a.* FROM a JOIN b USING (k)");
        q.Table("a").Insert(3, 30);
        q.Table("b").Insert(3, 300);
        q.Step();
        // a.* = (k, x); the merged k survives.
        Assert.Equal(1, WeightOf(q.Current, 3, 30));
    }

    [Fact]
    public void UsingKey_SurvivesCteStar_AndDownstreamBareReference()
    {
        // The exact watches_history → watches shape: a CTE joins USING (k) and
        // projects `cte.*`; a downstream query then selects the bare key.
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in TwoTables)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        // A view whose body is `SELECT s1.* FROM a s1 JOIN b USING (k)` must
        // expose `k` in its output schema.
        var viewPlan = ((CreateViewPlan)resolver.Resolve(Parser.ParseStatement(
            "CREATE VIEW v AS SELECT s1.* FROM a s1 JOIN b USING (k)"))).Query;
        var names = viewPlan.Schema.Columns.Select(c => c.Name).ToArray();
        Assert.Contains("k", names);   // the merged key reached the view output
        Assert.Contains("x", names);
    }

    [Fact]
    public void Using_UnknownColumn_Throws()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in TwoTables)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        Assert.ThrowsAny<Exception>(() =>
            resolver.Resolve(Parser.ParseStatement("SELECT * FROM a JOIN b USING (nope)")));
    }
}
