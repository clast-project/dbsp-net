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
/// Coverage for <c>b IS [NOT] TRUE / FALSE / UNKNOWN</c> — boolean tests that
/// always yield a definite boolean (never NULL), following SQL three-valued
/// logic. Implemented as a parse-time desugar (COALESCE for TRUE/FALSE,
/// IS [NOT] NULL for UNKNOWN), so they ride the existing resolver / compilers.
/// </summary>
public class BooleanTestTests
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

    // Rows: id=1 → TRUE, id=2 → FALSE, id=3 → NULL.
    private static CompiledQuery Filter(string whereClause)
    {
        var q = Compile(
            "CREATE TABLE t (id INT NOT NULL, b BOOLEAN)",
            $"SELECT id FROM t WHERE {whereClause}");
        q.Table("t").Insert(1, true);
        q.Table("t").Insert(2, false);
        q.Table("t").Insert(3, (object?)null);
        q.Step();
        return q;
    }

    [Fact]
    public void IsTrue_MatchesOnlyTrue()
    {
        var q = Filter("b IS TRUE");
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
    }

    [Fact]
    public void IsNotTrue_MatchesFalseAndNull()
    {
        var q = Filter("b IS NOT TRUE");
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    [Fact]
    public void IsFalse_MatchesOnlyFalse()
    {
        var q = Filter("b IS FALSE");
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Fact]
    public void IsNotFalse_MatchesTrueAndNull()
    {
        var q = Filter("b IS NOT FALSE");
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    [Fact]
    public void IsUnknown_MatchesOnlyNull()
    {
        var q = Filter("b IS UNKNOWN");
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    [Fact]
    public void IsNotUnknown_MatchesTrueAndFalse()
    {
        var q = Filter("b IS NOT UNKNOWN");
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Fact]
    public void IsTrue_ProjectsDefiniteBoolean()
    {
        // A NULL operand must produce a definite FALSE in projection position,
        // not NULL.
        var q = Compile(
            "CREATE TABLE t (id INT NOT NULL, b BOOLEAN)",
            "SELECT id, b IS TRUE AS bt FROM t");
        q.Table("t").Insert(1, true);
        q.Table("t").Insert(3, (object?)null);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 3, false));
    }
}
