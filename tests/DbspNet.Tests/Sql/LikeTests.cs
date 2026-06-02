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
/// Coverage for <c>value [NOT] {LIKE|ILIKE|SIMILAR TO} pattern [ESCAPE esc]</c>.
/// These are parse-time desugars to boolean scalar-function calls
/// (<c>like</c> / <c>ilike</c> / <c>similar_to</c>) resolved and lowered through
/// <see cref="ScalarFunctionRegistry"/>; the negated form is wrapped in a unary
/// <c>NOT</c> so SQL three-valued NULL handling is inherited. The escape
/// character defaults to backslash (PostgreSQL-aligned); <c>ESCAPE ''</c>
/// disables it.
/// </summary>
public class LikeTests
{
    // ---------- Parser ----------

    [Fact]
    public void Parser_Like_DesugarsToFunctionCall()
    {
        var call = Assert.IsType<FunctionCallExpression>(Parse("x LIKE 'a%'"));
        Assert.Equal("like", call.FunctionName);
        Assert.Equal(2, call.Arguments.Count);
    }

    [Fact]
    public void Parser_ILike_And_SimilarTo_Names()
    {
        Assert.Equal("ilike", Assert.IsType<FunctionCallExpression>(Parse("x ILIKE 'a%'")).FunctionName);
        Assert.Equal("similar_to", Assert.IsType<FunctionCallExpression>(Parse("x SIMILAR TO 'a%'")).FunctionName);
    }

    [Fact]
    public void Parser_NotLike_WrapsInUnaryNot()
    {
        var not = Assert.IsType<UnaryExpression>(Parse("x NOT LIKE 'a%'"));
        Assert.Equal(UnaryOperator.Not, not.Operator);
        Assert.Equal("like", Assert.IsType<FunctionCallExpression>(not.Operand).FunctionName);
    }

    [Fact]
    public void Parser_NotSimilarTo_WrapsInUnaryNot()
    {
        var not = Assert.IsType<UnaryExpression>(Parse("x NOT SIMILAR TO 'a%'"));
        Assert.Equal(UnaryOperator.Not, not.Operator);
        Assert.Equal("similar_to", Assert.IsType<FunctionCallExpression>(not.Operand).FunctionName);
    }

    [Fact]
    public void Parser_Escape_AddsThirdArgument()
    {
        var call = Assert.IsType<FunctionCallExpression>(Parse("x LIKE 'a!%' ESCAPE '!'"));
        Assert.Equal(3, call.Arguments.Count);
    }

    [Fact]
    public void Parser_Like_ClosesAtBooleanAnd()
    {
        // `x LIKE 'a%' AND y > 0` → (x LIKE 'a%') AND (y > 0): the pattern does
        // not swallow the boolean AND.
        var and = Assert.IsType<BinaryExpression>(Parse("x LIKE 'a%' AND y > 0"));
        Assert.Equal(BinaryOperator.And, and.Operator);
        Assert.Equal("like", Assert.IsType<FunctionCallExpression>(and.Left).FunctionName);
        Assert.Equal(BinaryOperator.Greater, Assert.IsType<BinaryExpression>(and.Right).Operator);
    }

    [Fact]
    public void Parser_LikeAndTo_StayUsableAsIdentifiers()
    {
        // The LIKE-family keywords are contextual, so a column named e.g. "to"
        // or "like" still resolves as an ordinary identifier elsewhere.
        Assert.IsType<ColumnReference>(Parse("to"));
        Assert.IsType<ColumnReference>(Parse("like"));
        Assert.IsType<ColumnReference>(Parse("similar"));
    }

    // ---------- Eval: LIKE ----------

    [Fact]
    public void Eval_Like_Wildcards()
    {
        var f = Like("a%");
        Assert.Equal(true, f("abc"));
        Assert.Equal(true, f("a"));       // % matches the empty string
        Assert.Equal(false, f("xabc"));

        var g = Like("a_c");
        Assert.Equal(true, g("abc"));
        Assert.Equal(false, g("ac"));     // _ requires exactly one character
        Assert.Equal(false, g("abbc"));
    }

    [Fact]
    public void Eval_Like_IsCaseSensitive()
    {
        Assert.Equal(false, Like("a%")("ABC"));
        Assert.Equal(true, Like("A%")("ABC"));
    }

    [Fact]
    public void Eval_Like_MatchesWholeString()
    {
        // LIKE is implicitly anchored: 'b' alone does not match 'abc'.
        Assert.Equal(false, Like("b")("abc"));
        Assert.Equal(true, Like("abc")("abc"));
    }

    [Fact]
    public void Eval_Like_NewlineUnderscore()
    {
        // _ / % match any character including newline.
        Assert.Equal(true, Like("a_c")("a\nc"));
        Assert.Equal(true, Like("a%c")("a\n\nc"));
    }

    [Fact]
    public void Eval_ILike_IsCaseInsensitive()
    {
        var f = ILike("a%");
        Assert.Equal(true, f("ABC"));
        Assert.Equal(true, f("abc"));
    }

    // ---------- Eval: escape ----------

    [Fact]
    public void Eval_DefaultBackslashEscape_MatchesLiteralWildcard()
    {
        // Default escape is backslash: '\%' matches a literal percent sign.
        var f = Like(@"100\%");
        Assert.Equal(true, f("100%"));
        Assert.Equal(false, f("100x"));
    }

    [Fact]
    public void Eval_CustomEscape()
    {
        var f = Like("a!%b", escape: "!");
        Assert.Equal(true, f("a%b"));     // !% is a literal percent
        Assert.Equal(false, f("axb"));
    }

    [Fact]
    public void Eval_EmptyEscape_DisablesEscaping()
    {
        // ESCAPE '' means backslash is an ordinary literal character.
        var f = Like(@"a\b", escape: "");
        Assert.Equal(true, f(@"a\b"));
        Assert.Equal(false, f("ab"));
    }

    // ---------- Eval: NULL (three-valued) ----------

    [Fact]
    public void Eval_NullValue_OrNullPattern_YieldsNull()
    {
        Assert.Null(LikeNullable("a%")(null));
        Assert.Null(EvalExpr("v LIKE p", ("v", Str(false), "abc"), ("p", Str(true), null)));
    }

    [Fact]
    public void Eval_NotLike_NullValue_YieldsNull()
    {
        Assert.Null(EvalExpr("v NOT LIKE 'a%'", ("v", Str(true), null)));
    }

    [Fact]
    public void Eval_NotLike_IsComplementForNonNull()
    {
        Assert.Equal(false, EvalExpr("v NOT LIKE 'a%'", ("v", Str(false), "abc")));
        Assert.Equal(true, EvalExpr("v NOT LIKE 'a%'", ("v", Str(false), "xyz")));
    }

    // ---------- Eval: SIMILAR TO ----------

    [Fact]
    public void Eval_SimilarTo_Wildcards()
    {
        Assert.Equal(true, Similar("a_c")("abc"));
        Assert.Equal(true, Similar("%b%")("aXbY"));
    }

    [Fact]
    public void Eval_SimilarTo_RegexMetacharacters()
    {
        Assert.Equal(true, Similar("a(b|c)c")("abc"));
        Assert.Equal(true, Similar("a(b|c)c")("acc"));
        Assert.Equal(false, Similar("a(b|c)c")("adc"));

        Assert.Equal(true, Similar("a{2,3}")("aaa"));
        Assert.Equal(false, Similar("a{2,3}")("a"));

        Assert.Equal(true, Similar("ab+")("abbb"));
        Assert.Equal(true, Similar("[abc]+")("cabba"));
        Assert.Equal(false, Similar("[abc]+")("cabxa"));
    }

    [Fact]
    public void Eval_SimilarTo_DotIsLiteral()
    {
        // Unlike POSIX regex, '.' is a literal in SIMILAR TO.
        Assert.Equal(true, Similar("a.c")("a.c"));
        Assert.Equal(false, Similar("a.c")("abc"));
    }

    [Fact]
    public void Eval_SimilarTo_MatchesWholeString()
    {
        Assert.Equal(false, Similar("b")("abc"));
        Assert.Equal(true, Similar("(a|b)bc")("abc"));
    }

    // ---------- End-to-end ----------

    [Fact]
    public void EndToEnd_WhereLike_Filters()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, s VARCHAR NOT NULL)"],
            "SELECT id FROM t WHERE s LIKE 'a%'");

        q.Table("t").Insert(1, "apple");
        q.Table("t").Insert(2, "banana");
        q.Table("t").Insert(3, "avocado");
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_SelectLike_ProjectsBoolean()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, s VARCHAR NOT NULL)"],
            "SELECT id, s ILIKE '%CAT%' AS has_cat FROM t");

        q.Table("t").Insert(1, "a CATalog");
        q.Table("t").Insert(2, "dog");
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 2, false));
        Assert.Equal(2, q.Current.Count);
    }

    // ---------- Helpers ----------

    private static Expression Parse(string text) => new Parser(Lexer.Tokenize(text)).ParseExpression();

    private static SqlVarcharType Str(bool nullable) => new SqlVarcharType(null, nullable);

    // Convenience: compile `s <op> 'pattern' [ESCAPE 'esc']` over a single
    // non-null VARCHAR column `s` and return a string→bool? probe.
    private static Func<string?, object?> Like(string pattern, string? escape = null) =>
        BuildProbe("like", pattern, escape, nullable: false);

    private static Func<string?, object?> ILike(string pattern, string? escape = null) =>
        BuildProbe("ilike", pattern, escape, nullable: false);

    private static Func<string?, object?> Similar(string pattern, string? escape = null) =>
        BuildProbe("similar", pattern, escape, nullable: false, similarTo: true);

    private static Func<string?, object?> LikeNullable(string pattern) =>
        BuildProbe("like", pattern, escape: null, nullable: true);

    private static Func<string?, object?> BuildProbe(
        string keyword, string pattern, string? escape, bool nullable, bool similarTo = false)
    {
        var op = similarTo ? "SIMILAR TO" : keyword.ToUpperInvariant();
        var escapeClause = escape is null ? "" : $" ESCAPE '{escape}'";
        var f = CompileExpr($"s {op} '{pattern}'{escapeClause}", ("s", Str(nullable)));
        return v => f([v is null ? null : Utf8String.Of(v)]);
    }

    // Compile an expression over the given columns and evaluate one row of
    // (string) inputs, encoding VARCHAR inputs as Utf8String at the boundary.
    private static object? EvalExpr(string exprText, params (string Name, SqlType Type, string? Value)[] cols)
    {
        var f = CompileExpr(exprText, cols.Select(c => (c.Name, c.Type)).ToArray());
        var row = cols.Select(c =>
            c.Value is null ? (object?)null : c.Type is SqlVarcharType ? Utf8String.Of(c.Value) : c.Value).ToArray();
        return f(row);
    }

    private static Func<object?[], object?> CompileExpr(string exprText, params (string Name, SqlType Type)[] cols)
    {
        var ast = Parse(exprText);
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
