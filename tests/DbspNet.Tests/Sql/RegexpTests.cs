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
/// Coverage for the POSIX-regex function family — <c>REGEXP_LIKE</c>,
/// <c>REGEXP_REPLACE</c>, <c>REGEXP_SUBSTR</c> — built on the same
/// <see cref="ScalarFunctionRegistry"/> + regex-cache machinery as
/// <see cref="LikeTests"/>. Unlike LIKE these are <b>substring</b> matches
/// (not whole-string anchored). PostgreSQL semantics: REGEXP_REPLACE replaces
/// the first match by default and all matches with the <c>g</c> flag;
/// replacement uses <c>\1</c>/<c>\&amp;</c> backreferences.
/// </summary>
public class RegexpTests
{
    // ---------- REGEXP_LIKE ----------

    [Fact]
    public void RegexpLike_IsSubstringMatch_NotAnchored()
    {
        // Differs from LIKE: a bare 'b' matches anywhere in 'abc'.
        Assert.Equal(true, Like("b")("abc"));
        Assert.Equal(true, Like("[0-9]+")("abc123"));
        Assert.Equal(false, Like("[0-9]+")("abc"));
    }

    [Fact]
    public void RegexpLike_HonoursAnchors()
    {
        Assert.Equal(true, Like("^a")("abc"));
        Assert.Equal(false, Like("^b")("abc"));
        Assert.Equal(true, Like("c$")("abc"));
    }

    [Fact]
    public void RegexpLike_IsCaseSensitiveByDefault_FlagOverrides()
    {
        Assert.Equal(false, Like("abc")("ABC"));
        Assert.Equal(true, LikeF("abc", "i")("ABC"));
        Assert.Equal(false, LikeF("abc", "ic")("ABC")); // trailing 'c' clears 'i'
    }

    [Fact]
    public void RegexpLike_NullPropagates()
    {
        Assert.Null(EvalExpr("regexp_like(v, 'a')", ("v", Str(true), null)));
        Assert.Null(EvalExpr("regexp_like('a', 'a', f)", ("f", Str(true), null)));
    }

    [Fact]
    public void RegexpLike_UnknownFlag_Throws()
    {
        Assert.ThrowsAny<Exception>(() => LikeF("a", "z")("abc"));
    }

    // ---------- REGEXP_REPLACE ----------

    [Fact]
    public void RegexpReplace_FirstMatchByDefault()
    {
        Assert.Equal("baa", Replace("aaa", "a", "b"));
    }

    [Fact]
    public void RegexpReplace_GlobalFlag_ReplacesAll()
    {
        Assert.Equal("bbb", ReplaceF("aaa", "a", "b", "g"));
    }

    [Fact]
    public void RegexpReplace_Backreferences()
    {
        // \1 / \2 reorder captured groups; the SQL literal carries the
        // backslashes through verbatim (the lexer does no backslash escaping).
        Assert.Equal("Smith John", Replace("John Smith", @"(\w+)\s+(\w+)", @"\2 \1"));
    }

    [Fact]
    public void RegexpReplace_WholeMatchBackreference()
    {
        Assert.Equal("a[b]c", Replace("abc", "b", @"[\&]"));
    }

    [Fact]
    public void RegexpReplace_LiteralDollarIsPreserved()
    {
        // A '$' in the replacement is not a .NET group reference.
        Assert.Equal("$b", Replace("ab", "a", "$"));
    }

    [Fact]
    public void RegexpReplace_CaseInsensitiveFlag()
    {
        Assert.Equal("x", ReplaceF("ABC", "abc", "x", "i"));
    }

    [Fact]
    public void RegexpReplace_NullPropagates()
    {
        Assert.Null(EvalExpr("regexp_replace(v, 'a', 'b')", ("v", Str(true), null)));
    }

    // ---------- REGEXP_SUBSTR ----------

    [Fact]
    public void RegexpSubstr_ReturnsFirstMatch()
    {
        Assert.Equal("123", Substr("abc123def456", "[0-9]+"));
    }

    [Fact]
    public void RegexpSubstr_NoMatch_IsNull()
    {
        Assert.Null(Substr("abc", "[0-9]+"));
    }

    [Fact]
    public void RegexpSubstr_CaseInsensitiveFlag()
    {
        Assert.Equal("ABC", SubstrF("xxABCxx", "abc", "i"));
    }

    // ---------- End-to-end ----------

    [Fact]
    public void EndToEnd_WhereRegexpLike_Filters()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, s VARCHAR NOT NULL)"],
            "SELECT id FROM t WHERE regexp_like(s, '^a')");

        q.Table("t").Insert(1, "apple");
        q.Table("t").Insert(2, "banana");
        q.Table("t").Insert(3, "apricot");
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void EndToEnd_SelectRegexpReplace_Projects()
    {
        var q = CompileView(
            ["CREATE TABLE t (id INT NOT NULL, s VARCHAR NOT NULL)"],
            "SELECT id, regexp_replace(s, '[0-9]+', '#', 'g') AS masked FROM t");

        q.Table("t").Insert(1, "a1b22c");
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, "a#b#c"));
        Assert.Equal(1, q.Current.Count);
    }

    // ---------- Helpers ----------

    private static SqlVarcharType Str(bool nullable) => new SqlVarcharType(null, nullable);

    private static Func<string?, object?> Like(string pattern) =>
        v => EvalExpr("regexp_like(s, $p)".Replace("$p", Lit(pattern)), ("s", Str(false), v));

    private static Func<string?, object?> LikeF(string pattern, string flags) =>
        v => EvalExpr($"regexp_like(s, {Lit(pattern)}, {Lit(flags)})", ("s", Str(false), v));

    private static string? Replace(string value, string pattern, string replacement) =>
        AsString(EvalExpr($"regexp_replace(s, {Lit(pattern)}, {Lit(replacement)})", ("s", Str(false), value)));

    private static string? ReplaceF(string value, string pattern, string replacement, string flags) =>
        AsString(EvalExpr(
            $"regexp_replace(s, {Lit(pattern)}, {Lit(replacement)}, {Lit(flags)})", ("s", Str(false), value)));

    private static string? Substr(string value, string pattern) =>
        AsString(EvalExpr($"regexp_substr(s, {Lit(pattern)})", ("s", Str(false), value)));

    private static string? SubstrF(string value, string pattern, string flags) =>
        AsString(EvalExpr($"regexp_substr(s, {Lit(pattern)}, {Lit(flags)})", ("s", Str(false), value)));

    // SQL single-quoted literal ('' escapes an embedded quote).
    private static string Lit(string s) => "'" + s.Replace("'", "''") + "'";

    private static string? AsString(object? v) => v is Utf8String u ? u.ToStringDecoded() : (string?)v;

    private static object? EvalExpr(string exprText, params (string Name, SqlType Type, string? Value)[] cols)
    {
        var f = CompileExpr(exprText, cols.Select(c => (c.Name, c.Type)).ToArray());
        var row = cols.Select(c =>
            c.Value is null ? (object?)null : c.Type is SqlVarcharType ? Utf8String.Of(c.Value) : c.Value).ToArray();
        return f(row);
    }

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
        SqlVarcharType v => new SqlTypeSpec("VARCHAR", v.MaxLength),
        SqlBooleanType => new SqlTypeSpec("BOOLEAN"),
        _ => throw new NotSupportedException(),
    };
}
