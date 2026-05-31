// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;

namespace DbspNet.Tests.Sql;

public class ParserTests
{
    private static SqlStatement Parse(string src) => Parser.ParseStatement(src);

    // --- CREATE TABLE ---

    [Fact]
    public void CreateTable_SimpleColumns()
    {
        var stmt = (CreateTableStatement)Parse("CREATE TABLE emp (id INTEGER NOT NULL, name VARCHAR(50))");
        Assert.Equal("emp", stmt.TableName);
        Assert.Equal(2, stmt.Columns.Count);

        Assert.Equal("id", stmt.Columns[0].Name);
        Assert.Equal("INTEGER", stmt.Columns[0].Type.Name);
        Assert.True(stmt.Columns[0].NotNull);

        Assert.Equal("name", stmt.Columns[1].Name);
        Assert.Equal("VARCHAR", stmt.Columns[1].Type.Name);
        Assert.Equal(50, stmt.Columns[1].Type.Parameter1);
        Assert.False(stmt.Columns[1].NotNull);
    }

    [Fact]
    public void CreateTable_DoublePrecision()
    {
        var stmt = (CreateTableStatement)Parse("CREATE TABLE t (x DOUBLE PRECISION, y DOUBLE)");
        Assert.Equal("DOUBLE PRECISION", stmt.Columns[0].Type.Name);
        Assert.Equal("DOUBLE PRECISION", stmt.Columns[1].Type.Name);
    }

    [Fact]
    public void CreateTable_Decimal_WithPrecisionAndScale()
    {
        var stmt = (CreateTableStatement)Parse("CREATE TABLE t (x DECIMAL(10,2), y DECIMAL(18))");
        Assert.Equal("DECIMAL", stmt.Columns[0].Type.Name);
        Assert.Equal(10, stmt.Columns[0].Type.Parameter1);
        Assert.Equal(2, stmt.Columns[0].Type.Parameter2);
        Assert.Equal(18, stmt.Columns[1].Type.Parameter1);
        Assert.Null(stmt.Columns[1].Type.Parameter2);
    }

    [Fact]
    public void CreateTable_PrimaryKey_IsCapturedButInert()
    {
        var stmt = (CreateTableStatement)Parse("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        Assert.True(stmt.Columns[0].PrimaryKey);
    }

    // --- CREATE VIEW ---

    [Fact]
    public void CreateView_BindsSelectQuery()
    {
        var stmt = (CreateViewStatement)Parse("CREATE VIEW v AS SELECT a FROM t");
        Assert.Equal("v", stmt.ViewName);
        Assert.NotNull(stmt.Query);
        Assert.Single(((SelectStatement)stmt.Query).Items);
    }

    // --- SELECT plumbing ---

    [Fact]
    public void Select_Star()
    {
        var s = (SelectStatement)Parse("SELECT * FROM t");
        Assert.Single(s.Items);
        var star = Assert.IsType<StarSelectItem>(s.Items[0]);
        Assert.Null(star.TableQualifier);
    }

    [Fact]
    public void Select_QualifiedStar()
    {
        var s = (SelectStatement)Parse("SELECT t.* FROM t");
        var star = Assert.IsType<StarSelectItem>(s.Items[0]);
        Assert.Equal("t", star.TableQualifier);
    }

    [Fact]
    public void Select_AliasWithAndWithoutAs()
    {
        var s = (SelectStatement)Parse("SELECT a AS aa, b bb FROM t");
        Assert.Equal("aa", ((ExpressionSelectItem)s.Items[0]).Alias);
        Assert.Equal("bb", ((ExpressionSelectItem)s.Items[1]).Alias);
    }

    [Fact]
    public void Select_WhereGroupByHaving()
    {
        var s = (SelectStatement)Parse(
            "SELECT dept, SUM(salary) AS total FROM emp WHERE active = TRUE GROUP BY dept HAVING SUM(salary) > 100");
        Assert.NotNull(s.Where);
        Assert.Single(s.GroupBy);
        Assert.NotNull(s.Having);
    }

    [Fact]
    public void Select_InnerJoinOn_BuildsJoinClause()
    {
        var s = (SelectStatement)Parse("SELECT * FROM a INNER JOIN b ON a.k = b.k");
        var join = Assert.IsType<JoinClause>(s.From);
        Assert.Equal(JoinType.Inner, join.Type);
        Assert.IsType<TableReference>(join.Left);
        Assert.IsType<TableReference>(join.Right);
    }

    [Fact]
    public void Select_JoinWithoutInner_DefaultsToInner()
    {
        var s = (SelectStatement)Parse("SELECT * FROM a JOIN b ON a.k = b.k");
        var join = Assert.IsType<JoinClause>(s.From);
        Assert.Equal(JoinType.Inner, join.Type);
    }

    [Fact]
    public void Select_CrossJoin_DesugarsToInnerWithTrueOn()
    {
        var s = (SelectStatement)Parse("SELECT * FROM a CROSS JOIN b");
        var join = Assert.IsType<JoinClause>(s.From);
        Assert.Equal(JoinType.Inner, join.Type);
        Assert.Null(join.UsingColumns);
        var lit = Assert.IsType<LiteralExpression>(join.OnCondition);
        Assert.Equal(LiteralKind.Boolean, lit.Kind);
        Assert.Equal(true, lit.Value);
    }

    // --- Pratt expression parser: precedence and associativity ---

    private static Expression ParseExpr(string src)
    {
        var p = new Parser(Lexer.Tokenize(src));
        return p.ParseExpression();
    }

    [Fact]
    public void Pratt_MultBindsTighterThanAdd()
    {
        // 1 + 2 * 3  =>  (1 + (2 * 3))
        var e = (BinaryExpression)ParseExpr("1 + 2 * 3");
        Assert.Equal(BinaryOperator.Add, e.Operator);
        var right = Assert.IsType<BinaryExpression>(e.Right);
        Assert.Equal(BinaryOperator.Multiply, right.Operator);
    }

    [Fact]
    public void Pratt_ParensOverridePrecedence()
    {
        var e = (BinaryExpression)ParseExpr("(1 + 2) * 3");
        Assert.Equal(BinaryOperator.Multiply, e.Operator);
        var left = Assert.IsType<BinaryExpression>(e.Left);
        Assert.Equal(BinaryOperator.Add, left.Operator);
    }

    [Fact]
    public void Pratt_AddIsLeftAssociative()
    {
        // 1 - 2 - 3  =>  ((1 - 2) - 3)
        var e = (BinaryExpression)ParseExpr("1 - 2 - 3");
        Assert.Equal(BinaryOperator.Subtract, e.Operator);
        var left = Assert.IsType<BinaryExpression>(e.Left);
        Assert.Equal(BinaryOperator.Subtract, left.Operator);
    }

    [Fact]
    public void Pratt_UnaryMinusBeatsMultiply()
    {
        // -a * b  =>  (-a) * b
        var e = (BinaryExpression)ParseExpr("-a * b");
        Assert.Equal(BinaryOperator.Multiply, e.Operator);
        Assert.IsType<UnaryExpression>(e.Left);
    }

    [Fact]
    public void Pratt_AndBindsTighterThanOr()
    {
        // a OR b AND c  =>  (a OR (b AND c))
        var e = (BinaryExpression)ParseExpr("a OR b AND c");
        Assert.Equal(BinaryOperator.Or, e.Operator);
        var right = Assert.IsType<BinaryExpression>(e.Right);
        Assert.Equal(BinaryOperator.And, right.Operator);
    }

    [Fact]
    public void Pratt_NotBindsLooserThanComparison()
    {
        // NOT a = b  =>  NOT (a = b)
        var e = (UnaryExpression)ParseExpr("NOT a = b");
        Assert.Equal(UnaryOperator.Not, e.Operator);
        Assert.IsType<BinaryExpression>(e.Operand);
    }

    [Fact]
    public void Pratt_IsNullAndIsNotNull()
    {
        var e1 = (IsNullExpression)ParseExpr("x IS NULL");
        Assert.False(e1.Negated);

        var e2 = (IsNullExpression)ParseExpr("x IS NOT NULL");
        Assert.True(e2.Negated);
    }

    [Fact]
    public void Pratt_QualifiedColumn()
    {
        var e = (ColumnReference)ParseExpr("t.c");
        Assert.Equal("t", e.Qualifier);
        Assert.Equal("c", e.Name);
    }

    [Fact]
    public void Pratt_UnqualifiedColumn()
    {
        var e = (ColumnReference)ParseExpr("c");
        Assert.Null(e.Qualifier);
        Assert.Equal("c", e.Name);
    }

    [Fact]
    public void Pratt_FunctionCallWithArgs()
    {
        var e = (FunctionCallExpression)ParseExpr("sum(salary + bonus)");
        Assert.Equal("sum", e.FunctionName);
        Assert.Single(e.Arguments);
        Assert.False(e.IsStar);
    }

    [Fact]
    public void Pratt_CountStar()
    {
        var e = (FunctionCallExpression)ParseExpr("COUNT(*)");
        Assert.Equal("count", e.FunctionName);
        Assert.True(e.IsStar);
        Assert.Empty(e.Arguments);
    }

    [Fact]
    public void Pratt_Coalesce_MultipleArgs()
    {
        var e = (FunctionCallExpression)ParseExpr("COALESCE(a, b, c)");
        Assert.Equal("coalesce", e.FunctionName);
        Assert.Equal(3, e.Arguments.Count);
    }

    [Fact]
    public void Pratt_CastExpression()
    {
        var e = (CastExpression)ParseExpr("CAST(x AS BIGINT)");
        Assert.Equal("BIGINT", e.TargetType.Name);
    }

    [Fact]
    public void Pratt_LiteralKinds()
    {
        Assert.Equal(LiteralKind.Integer, ((LiteralExpression)ParseExpr("42")).Kind);
        Assert.Equal(LiteralKind.Decimal, ((LiteralExpression)ParseExpr("3.14")).Kind);
        Assert.Equal(LiteralKind.Float, ((LiteralExpression)ParseExpr("1e3")).Kind);
        Assert.Equal(LiteralKind.String, ((LiteralExpression)ParseExpr("'hi'")).Kind);
        Assert.Equal(LiteralKind.Boolean, ((LiteralExpression)ParseExpr("TRUE")).Kind);
        Assert.Equal(LiteralKind.Null, ((LiteralExpression)ParseExpr("NULL")).Kind);
    }

    // --- Error paths ---

    [Fact]
    public void UnexpectedToken_ThrowsParseException()
    {
        Assert.Throws<ParseException>(() => Parse("SELECT * FROM"));
    }

    [Fact]
    public void MissingRParen_ThrowsParseException()
    {
        Assert.Throws<ParseException>(() => Parse("SELECT (1 + 2 FROM t"));
    }
}
