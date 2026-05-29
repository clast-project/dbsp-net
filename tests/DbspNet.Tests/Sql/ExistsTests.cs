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
/// Coverage for the uncorrelated <c>EXISTS (subquery)</c> predicate.
/// EXISTS desugars at parse time to <c>(SELECT COUNT(*) FROM (subquery)) > 0</c>,
/// which rides on the existing scalar-subquery machinery — no new resolver
/// case, no new operator. <c>NOT EXISTS (sq)</c> is just <c>NOT (count > 0)</c>
/// via the unary-NOT path; <c>COUNT(*)</c> is never NULL so three-valued
/// logic is trivial.
/// </summary>
public class ExistsTests
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

    // ---------- Parser ----------

    [Fact]
    public void Parser_Exists_DesugarsToCoalescedCountGreaterThanZero()
    {
        // EXISTS (SELECT 1 FROM t) → COALESCE((SELECT COUNT(*) FROM (SELECT 1 FROM t)), 0) > 0
        var expr = new Parser(Lexer.Tokenize("EXISTS (SELECT 1 FROM t)")).ParseExpression();
        var cmp = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.Greater, cmp.Operator);

        // Left: COALESCE(subquery, 0).
        var coalesce = Assert.IsType<FunctionCallExpression>(cmp.Left);
        Assert.Equal("coalesce", coalesce.FunctionName);
        Assert.Equal(2, coalesce.Arguments.Count);
        Assert.IsType<SubqueryExpression>(coalesce.Arguments[0]);

        // Right: literal 0.
        var lit = Assert.IsType<LiteralExpression>(cmp.Right);
        Assert.Equal(LiteralKind.Integer, lit.Kind);
        Assert.Equal(0L, lit.Value);
    }

    [Fact]
    public void Parser_ExistsRequiresParenSubquery()
    {
        Assert.Throws<ParseException>(() =>
            new Parser(Lexer.Tokenize("EXISTS")).ParseExpression());
        Assert.Throws<ParseException>(() =>
            new Parser(Lexer.Tokenize("EXISTS (1, 2)")).ParseExpression());
    }

    [Fact]
    public void Parser_NotExists_FlowsThroughUnaryNot()
    {
        // No special NOT EXISTS AST — `NOT EXISTS (...)` parses to
        // `NOT (COALESCE(count, 0) > 0)` via the existing unary-not arm.
        var expr = new Parser(Lexer.Tokenize("NOT EXISTS (SELECT 1 FROM t)")).ParseExpression();
        var not = Assert.IsType<UnaryExpression>(expr);
        Assert.Equal(UnaryOperator.Not, not.Operator);
        var cmp = Assert.IsType<BinaryExpression>(not.Operand);
        Assert.Equal(BinaryOperator.Greater, cmp.Operator);
    }

    // ---------- End-to-end ----------

    [Fact]
    public void WhereExists_NonEmptySubquery_EmitsAllRows()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL, name VARCHAR NOT NULL)",
                "CREATE TABLE u (flag INT NOT NULL)",
            ],
            "SELECT name FROM t WHERE EXISTS (SELECT 1 FROM u)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        q.Table("u").Insert(42);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, WeightOf(q.Current, "b"));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void WhereExists_EmptySubquery_EmitsNothing()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL, name VARCHAR NOT NULL)",
                "CREATE TABLE u (flag INT NOT NULL)",
            ],
            "SELECT name FROM t WHERE EXISTS (SELECT 1 FROM u)");

        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        // u stays empty.
        q.Step();

        Assert.True(q.Current.IsEmpty);
    }

    [Fact]
    public void WhereNotExists_EmptySubquery_EmitsAllRows()
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

        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, WeightOf(q.Current, "b"));
    }

    [Fact]
    public void WhereExists_DynamicallyFlips_WhenSubqueryGainsAndLosesRows()
    {
        var q = Compile(
            [
                "CREATE TABLE t (x INT NOT NULL, name VARCHAR NOT NULL)",
                "CREATE TABLE u (flag INT NOT NULL)",
            ],
            "SELECT name FROM t WHERE EXISTS (SELECT 1 FROM u WHERE flag > 0)");

        // Tick 1: t has rows, u is empty. Output empty.
        q.Table("t").Insert(1, Utf8String.Of("a"));
        q.Table("t").Insert(2, Utf8String.Of("b"));
        q.Step();
        Assert.True(q.Current.IsEmpty);

        // Tick 2: insert a row into u that satisfies the predicate. Output
        // gains all rows of t.
        q.Table("u").Insert(7);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, "a"));
        Assert.Equal(1, WeightOf(q.Current, "b"));

        // Tick 3: retract the row from u. Output retracts all rows of t.
        q.Table("u").Delete(7);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, "a"));
        Assert.Equal(-1, WeightOf(q.Current, "b"));
    }

    [Fact]
    public void SelectExists_AppendsBooleanColumn()
    {
        // EXISTS works in SELECT position too — desugar gives a scalar
        // boolean per row (constant per tick since uncorrelated).
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
}
