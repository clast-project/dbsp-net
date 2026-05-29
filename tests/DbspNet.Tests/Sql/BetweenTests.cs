// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for <c>probe [NOT] BETWEEN lo AND hi</c>, a parse-time desugar
/// to a comparison conjunction: <c>BETWEEN</c> ≡ <c>probe &gt;= lo AND
/// probe &lt;= hi</c>; <c>NOT BETWEEN</c> ≡ <c>probe &lt; lo OR probe &gt; hi</c>.
/// No new AST node, resolver, or compiler support — it rides the existing
/// comparison/boolean machinery (incl. SQL three-valued NULL handling).
/// </summary>
public class BetweenTests
{
    // ---------- Parser ----------

    [Fact]
    public void Parser_Between_DesugarsToConjunction()
    {
        var expr = new Parser(Lexer.Tokenize("x BETWEEN 1 AND 5")).ParseExpression();
        var and = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.And, and.Operator);
        Assert.Equal(BinaryOperator.GreaterEqual, Assert.IsType<BinaryExpression>(and.Left).Operator);
        Assert.Equal(BinaryOperator.LessEqual, Assert.IsType<BinaryExpression>(and.Right).Operator);
    }

    [Fact]
    public void Parser_NotBetween_DesugarsToDisjunction()
    {
        var expr = new Parser(Lexer.Tokenize("x NOT BETWEEN 1 AND 5")).ParseExpression();
        var or = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.Or, or.Operator);
        Assert.Equal(BinaryOperator.Less, Assert.IsType<BinaryExpression>(or.Left).Operator);
        Assert.Equal(BinaryOperator.Greater, Assert.IsType<BinaryExpression>(or.Right).Operator);
    }

    [Fact]
    public void Parser_Between_AndChainsToOuterBoolean()
    {
        // `x BETWEEN 1 AND 5 AND y > 0`: the first AND is the BETWEEN's, the
        // second is the boolean AND. Result is (between) AND (y > 0).
        var expr = new Parser(Lexer.Tokenize("x BETWEEN 1 AND 5 AND y > 0")).ParseExpression();
        var top = Assert.IsType<BinaryExpression>(expr);
        Assert.Equal(BinaryOperator.And, top.Operator);

        // Right is the y > 0 comparison.
        Assert.Equal(BinaryOperator.Greater, Assert.IsType<BinaryExpression>(top.Right).Operator);

        // Left is the BETWEEN conjunction (>= AND <=).
        var between = Assert.IsType<BinaryExpression>(top.Left);
        Assert.Equal(BinaryOperator.And, between.Operator);
        Assert.Equal(BinaryOperator.GreaterEqual, Assert.IsType<BinaryExpression>(between.Left).Operator);
        Assert.Equal(BinaryOperator.LessEqual, Assert.IsType<BinaryExpression>(between.Right).Operator);
    }

    [Fact]
    public void Parser_SubqueryOperand_Throws()
    {
        Assert.ThrowsAny<Exception>(() =>
            new Parser(Lexer.Tokenize("(SELECT 1) BETWEEN 1 AND 5")).ParseExpression());
    }

    // ---------- Eval ----------

    [Fact]
    public void Eval_Between_InclusiveBounds()
    {
        var f = CompileExpr("a BETWEEN 2 AND 4", ("a", new SqlIntegerType(false)));
        Assert.Equal(false, f([1]));
        Assert.Equal(true, f([2]));   // lower bound inclusive
        Assert.Equal(true, f([3]));
        Assert.Equal(true, f([4]));   // upper bound inclusive
        Assert.Equal(false, f([5]));
    }

    [Fact]
    public void Eval_NotBetween_IsComplement()
    {
        var f = CompileExpr("a NOT BETWEEN 2 AND 4", ("a", new SqlIntegerType(false)));
        Assert.Equal(true, f([1]));
        Assert.Equal(false, f([2]));
        Assert.Equal(false, f([4]));
        Assert.Equal(true, f([5]));
    }

    [Fact]
    public void Eval_NullProbe_YieldsNull()
    {
        var f = CompileExpr("a BETWEEN 2 AND 4", ("a", new SqlIntegerType(true)));
        Assert.Null(f([null]));
    }

    [Fact]
    public void Eval_NotBetween_NullProbe_YieldsNull()
    {
        var f = CompileExpr("a NOT BETWEEN 2 AND 4", ("a", new SqlIntegerType(true)));
        Assert.Null(f([null]));
    }

    [Fact]
    public void Eval_ColumnBounds()
    {
        // Bounds may be arbitrary expressions, not just literals.
        var f = CompileExpr(
            "a BETWEEN lo AND hi",
            ("a", new SqlIntegerType(false)),
            ("lo", new SqlIntegerType(false)),
            ("hi", new SqlIntegerType(false)));
        Assert.Equal(true, f([5, 1, 10]));
        Assert.Equal(false, f([5, 6, 10]));
    }

    // ---------- End-to-end ----------

    [Fact]
    public void EndToEnd_WhereBetween_FiltersRange()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
            "SELECT id FROM t WHERE v BETWEEN 2 AND 4");

        q.Table("t").Insert(1, 1);
        q.Table("t").Insert(2, 2);
        q.Table("t").Insert(3, 3);
        q.Table("t").Insert(4, 4);
        q.Table("t").Insert(5, 5);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(1, WeightOf(q.Current, 4));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_SelectBetween_ProjectsBoolean()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
            "SELECT id, v BETWEEN 2 AND 4 AS in_range FROM t");

        q.Table("t").Insert(1, 1);
        q.Table("t").Insert(2, 3);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, false));
        Assert.Equal(1, WeightOf(q.Current, 2, true));
        Assert.Equal(2, q.Current.Count);
    }

    // ---------- Helpers ----------

    private static Func<object?[], object?> CompileExpr(string exprText, params (string Name, SqlType Type)[] cols)
    {
        var ast = new Parser(Lexer.Tokenize(exprText)).ParseExpression();
        var resolved = ResolveViaPublicApi(ast, cols);
        var fn = ExpressionCompiler.CompileScalar(resolved);
        return arr => fn(arr);
    }

    private static ResolvedExpression ResolveViaPublicApi(Expression ast, (string Name, SqlType Type)[] cols)
    {
        var cat = new Catalog();
        var colDefs = new List<ColumnDefinition>();
        foreach (var c in cols)
        {
            colDefs.Add(new ColumnDefinition(c.Name, SqlTypeSpecOf(c.Type), !c.Type.Nullable, PrimaryKey: false));
        }

        var resolver = new Resolver(cat);
        resolver.Resolve(new CreateTableStatement("t", colDefs));

        var select = new SelectStatement(
            Items: [new ExpressionSelectItem(ast, Alias: null)],
            From: new TableReference("t", Alias: null),
            Where: null,
            GroupBy: Array.Empty<Expression>(),
            Having: null,
            Ctes: Array.Empty<CteDefinition>());
        var proj = (ProjectPlan)((SelectPlan)resolver.Resolve(select)).Query;
        return proj.Projections[0].Expression;
    }

    private static CompiledQuery CompileView(string[] ddl, string query)
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

    private static SqlTypeSpec SqlTypeSpecOf(SqlType t) => t switch
    {
        SqlIntegerType => new SqlTypeSpec("INTEGER"),
        SqlBigintType => new SqlTypeSpec("BIGINT"),
        SqlVarcharType v => new SqlTypeSpec("VARCHAR", v.MaxLength),
        SqlBooleanType => new SqlTypeSpec("BOOLEAN"),
        _ => throw new NotSupportedException(),
    };
}
