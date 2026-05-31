// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class ScalarFunctionTests
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

    // Compile a single-row scenario and return the projected object-value.
    private static object? EvalOne(string[] ddl, string query, Action<CompiledQuery> push)
    {
        var q = Compile(ddl, query);
        push(q);
        q.Step();
        Assert.Equal(1, q.Current.Count);
        var entry = Assert.Single(q.Current);
        Assert.Equal(1, entry.Value.Value);  // weight
        var raw = entry.Key[0];               // one-column row
        // Decode UTF-8 to .NET string at the test boundary so assertions
        // can compare against string literals without knowing about
        // the internal Utf8String storage.
        return raw is Utf8String u ? u.ToStringDecoded() : raw;
    }

    // ---- UPPER / LOWER ----

    [Fact]
    public void Upper_AndLower_WorkOnStrings()
    {
        Assert.Equal("HELLO", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT UPPER(s) FROM t",
            q => q.Table("t").Insert("Hello")));

        Assert.Equal("hello", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT LOWER(s) FROM t",
            q => q.Table("t").Insert("Hello")));
    }

    [Fact]
    public void Upper_PropagatesNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (s VARCHAR)"],
            "SELECT UPPER(s) FROM t",
            q => q.Table("t").Insert((object?)null)));
    }

    [Fact]
    public void Upper_AndLower_WorkOnMultibyte()
    {
        // Native UTF-8 case folding: 'café' → 'CAFÉ' via Rune-based fold,
        // no .NET string round-trip.
        Assert.Equal("CAFÉ", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT UPPER(s) FROM t",
            q => q.Table("t").Insert("café")));

        Assert.Equal("éclair", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT LOWER(s) FROM t",
            q => q.Table("t").Insert("ÉCLAIR")));
    }

    [Fact]
    public void Length_ReturnsIntegerCodeUnitCount()
    {
        Assert.Equal(5, EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT LENGTH(s) FROM t",
            q => q.Table("t").Insert("hello")));
    }

    // ---- CONCAT ----

    [Fact]
    public void Concat_ConcatenatesStrings()
    {
        Assert.Equal("abcdef", EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR NOT NULL)"],
            "SELECT CONCAT(a, b) FROM t",
            q => q.Table("t").Insert("abc", "def")));
    }

    [Fact]
    public void Concat_SkipsNullArgs()
    {
        // PG semantics: NULL args are skipped, result never NULL.
        Assert.Equal("abcdef", EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR, c VARCHAR NOT NULL)"],
            "SELECT CONCAT(a, b, c) FROM t",
            q => q.Table("t").Insert("abc", null, "def")));
    }

    [Fact]
    public void Concat_AllNullArgs_ReturnsEmptyString()
    {
        Assert.Equal(string.Empty, EvalOne(
            ["CREATE TABLE t (a VARCHAR, b VARCHAR)"],
            "SELECT CONCAT(a, b) FROM t",
            q => q.Table("t").Insert(null, null)));
    }

    // ---- ABS ----

    [Fact]
    public void Abs_OnMultipleNumericTypes()
    {
        Assert.Equal(7, EvalOne(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "SELECT ABS(x) FROM t",
            q => q.Table("t").Insert(-7)));

        Assert.Equal(7L, EvalOne(
            ["CREATE TABLE t (x BIGINT NOT NULL)"],
            "SELECT ABS(x) FROM t",
            q => q.Table("t").Insert(-7L)));

        Assert.Equal(7.5, EvalOne(
            ["CREATE TABLE t (x DOUBLE PRECISION NOT NULL)"],
            "SELECT ABS(x) FROM t",
            q => q.Table("t").Insert(-7.5)));
    }

    [Fact]
    public void Abs_PropagatesNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (x INT)"],
            "SELECT ABS(x) FROM t",
            q => q.Table("t").Insert((object?)null)));
    }

    // ---- FLOOR / CEIL ----

    [Fact]
    public void Floor_AndCeil_OnDouble()
    {
        Assert.Equal(3.0, EvalOne(
            ["CREATE TABLE t (x DOUBLE PRECISION NOT NULL)"],
            "SELECT FLOOR(x) FROM t",
            q => q.Table("t").Insert(3.7)));

        Assert.Equal(4.0, EvalOne(
            ["CREATE TABLE t (x DOUBLE PRECISION NOT NULL)"],
            "SELECT CEIL(x) FROM t",
            q => q.Table("t").Insert(3.2)));
    }

    [Fact]
    public void CeilingAlias_WorksLikeCeil()
    {
        Assert.Equal(4.0, EvalOne(
            ["CREATE TABLE t (x DOUBLE PRECISION NOT NULL)"],
            "SELECT CEILING(x) FROM t",
            q => q.Table("t").Insert(3.2)));
    }

    [Fact]
    public void Floor_OnInteger_IsIdentity()
    {
        Assert.Equal(5, EvalOne(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "SELECT FLOOR(x) FROM t",
            q => q.Table("t").Insert(5)));
    }

    // ---- ROUND ----

    [Fact]
    public void Round_WithoutDigits_RoundsToNearestInteger()
    {
        Assert.Equal(4.0, EvalOne(
            ["CREATE TABLE t (x DOUBLE PRECISION NOT NULL)"],
            "SELECT ROUND(x) FROM t",
            q => q.Table("t").Insert(3.6)));
    }

    [Fact]
    public void Round_WithDigits()
    {
        Assert.Equal(3.14, EvalOne(
            ["CREATE TABLE t (x DOUBLE PRECISION NOT NULL)"],
            "SELECT ROUND(x, 2) FROM t",
            q => q.Table("t").Insert(3.14159)));
    }

    // ---- POWER / SQRT ----

    [Fact]
    public void Power_ReturnsDouble()
    {
        Assert.Equal(8.0, EvalOne(
            ["CREATE TABLE t (b INT NOT NULL, e INT NOT NULL)"],
            "SELECT POWER(b, e) FROM t",
            q => q.Table("t").Insert(2, 3)));
    }

    [Fact]
    public void Sqrt_ReturnsDouble()
    {
        Assert.Equal(3.0, EvalOne(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "SELECT SQRT(x) FROM t",
            q => q.Table("t").Insert(9)));
    }

    // ---- GREATEST / LEAST ----

    [Fact]
    public void Greatest_ReturnsMaxSkippingNulls()
    {
        // a=1, b=NULL, c=7 → GREATEST = 7
        Assert.Equal(7, EvalOne(
            ["CREATE TABLE t (a INT NOT NULL, b INT, c INT NOT NULL)"],
            "SELECT GREATEST(a, b, c) FROM t",
            q => q.Table("t").Insert(1, null, 7)));
    }

    [Fact]
    public void Least_ReturnsMinSkippingNulls()
    {
        Assert.Equal(1, EvalOne(
            ["CREATE TABLE t (a INT NOT NULL, b INT, c INT NOT NULL)"],
            "SELECT LEAST(a, b, c) FROM t",
            q => q.Table("t").Insert(5, null, 1)));
    }

    [Fact]
    public void Greatest_AllNull_ReturnsNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (a INT, b INT)"],
            "SELECT GREATEST(a, b) FROM t",
            q => q.Table("t").Insert(null, null)));
    }

    [Fact]
    public void Greatest_OnStrings()
    {
        Assert.Equal("zebra", EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR NOT NULL)"],
            "SELECT GREATEST(a, b) FROM t",
            q => q.Table("t").Insert("apple", "zebra")));
    }

    // ---- NULLIF ----

    [Fact]
    public void NullIf_ReturnsNullOnMatch()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT NULLIF(a, b) FROM t",
            q => q.Table("t").Insert(5, 5)));
    }

    [Fact]
    public void NullIf_ReturnsFirstArgWhenDifferent()
    {
        Assert.Equal(5, EvalOne(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT NULLIF(a, b) FROM t",
            q => q.Table("t").Insert(5, 7)));
    }

    [Fact]
    public void NullIf_FirstArgNull_ReturnsNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (a INT, b INT NOT NULL)"],
            "SELECT NULLIF(a, b) FROM t",
            q => q.Table("t").Insert(null, 5)));
    }

    [Fact]
    public void NullIf_SecondArgNull_ReturnsFirst()
    {
        Assert.Equal(5, EvalOne(
            ["CREATE TABLE t (a INT NOT NULL, b INT)"],
            "SELECT NULLIF(a, b) FROM t",
            q => q.Table("t").Insert(5, null)));
    }

    // ---- Resolver-level errors ----

    [Fact]
    public void Resolver_UnknownFunction_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (x INT NOT NULL)"));
        Assert.Throws<ResolveException>(() =>
            r.Resolve(Parser.ParseStatement("SELECT NOPE(x) FROM t")));
    }

    [Fact]
    public void Resolver_WrongArity_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (x INT NOT NULL)"));
        Assert.Throws<ResolveException>(() =>
            r.Resolve(Parser.ParseStatement("SELECT ABS(x, 1) FROM t")));
    }

    [Fact]
    public void Resolver_WrongArgType_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (s VARCHAR NOT NULL)"));
        Assert.Throws<ResolveException>(() =>
            r.Resolve(Parser.ParseStatement("SELECT ABS(s) FROM t")));
    }

    // ---- Composition ----

    [Fact]
    public void Functions_ComposeInExpressions()
    {
        // LENGTH(UPPER(s)) + ABS(x)
        Assert.Equal(8, EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL, x INT NOT NULL)"],
            "SELECT LENGTH(UPPER(s)) + ABS(x) FROM t",
            q => q.Table("t").Insert("hello", -3)));  // 5 + 3 = 8
    }

    [Fact]
    public void Functions_WorkInWhere()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t WHERE LENGTH(s) > 3");
        q.Table("t").Insert("hi");
        q.Table("t").Insert("hello");
        q.Table("t").Insert("world");
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, "hello"));
        Assert.Equal(1, WeightOf(q.Current, "world"));
    }

    [Fact]
    public void Functions_InAggregateArgument_WorkWithGroupBy()
    {
        // GROUP BY is still restricted to bare column refs, but functions in
        // aggregate arguments work normally.
        var q = Compile(
            ["CREATE TABLE t (bucket INT NOT NULL, s VARCHAR NOT NULL)"],
            "SELECT bucket, SUM(LENGTH(s)) AS total_len FROM t GROUP BY bucket");
        q.Table("t").Insert(1, "aa");
        q.Table("t").Insert(1, "bbb");
        q.Table("t").Insert(2, "cccc");
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 5L));  // 2 + 3
        Assert.Equal(1, WeightOf(q.Current, 2, 4L));
    }

    // ---- SUBSTRING ----

    private static object? EvalStr(string expr, string value) => EvalOne(
        ["CREATE TABLE t (s VARCHAR NOT NULL)"], $"SELECT {expr} FROM t",
        q => q.Table("t").Insert(value));

    [Fact]
    public void Substring_StartAndLength()
    {
        Assert.Equal("ell", EvalStr("SUBSTRING(s, 2, 3)", "hello"));
        Assert.Equal("ello", EvalStr("SUBSTRING(s, 2)", "hello"));
        Assert.Equal("hello", EvalStr("SUBSTR(s, 1, 99)", "hello"));
    }

    [Fact]
    public void Substring_ClipsLowStart()
    {
        // SQL standard: positions below 1 count toward the length window.
        // start=-1, length=4 → positions -1..2 → code points at 1,2 = "he".
        Assert.Equal("he", EvalStr("SUBSTRING(s, -1, 4)", "hello"));
        Assert.Equal(string.Empty, EvalStr("SUBSTRING(s, 2, 0)", "hello"));
    }

    [Fact]
    public void Substring_IsCodePointAware()
    {
        // 'café' = c,a,f,é (1..4); SUBSTRING(2,2) = "af"; SUBSTRING(4,1) = "é".
        Assert.Equal("af", EvalStr("SUBSTRING(s, 2, 2)", "café"));
        Assert.Equal("é", EvalStr("SUBSTRING(s, 4, 1)", "café"));
    }

    [Fact]
    public void Substring_PropagatesNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (s VARCHAR)"],
            "SELECT SUBSTRING(s, 1, 2) FROM t",
            q => q.Table("t").Insert((object?)null)));
    }

    // ---- TRIM / LTRIM / RTRIM ----

    [Fact]
    public void Trim_StripsSpaces()
    {
        Assert.Equal("hi", EvalStr("TRIM(s)", "  hi  "));
        Assert.Equal("hi  ", EvalStr("LTRIM(s)", "  hi  "));
        Assert.Equal("  hi", EvalStr("RTRIM(s)", "  hi  "));
    }

    [Fact]
    public void Trim_WithCharSet()
    {
        Assert.Equal("hi", EvalStr("TRIM(s, 'xy')", "xyxhiyx"));
        Assert.Equal("hiyx", EvalStr("LTRIM(s, 'xy')", "xyxhiyx"));
        Assert.Equal("xyxhi", EvalStr("RTRIM(s, 'xy')", "xyxhiyx"));
    }

    // ---- REPLACE ----

    [Fact]
    public void Replace_ReplacesAllOccurrences()
    {
        Assert.Equal("aXaX", EvalStr("REPLACE(s, 'bc', 'X')", "abcabc"));
        Assert.Equal("cafe", EvalStr("REPLACE(s, 'é', 'e')", "café"));
        Assert.Equal("abc", EvalStr("REPLACE(s, 'z', 'Q')", "abc"));
    }

    // ---- POSITION / STRPOS ----

    [Fact]
    public void Position_ReturnsOneBasedCodePointIndex()
    {
        Assert.Equal(4, EvalStr("POSITION('lo' IN s)", "hello"));
        Assert.Equal(0, EvalStr("POSITION('z' IN s)", "hello"));
        Assert.Equal(1, EvalStr("POSITION('' IN s)", "hello"));
        Assert.Equal(4, EvalStr("POSITION('é' IN s)", "café"));
    }

    [Fact]
    public void Strpos_IsPositionWithSwappedArgs()
    {
        Assert.Equal(4, EvalStr("STRPOS(s, 'lo')", "hello"));
        Assert.Equal(0, EvalStr("STRPOS(s, 'z')", "hello"));
    }

    // ---- SIGN ----

    [Fact]
    public void Sign_ReturnsMinusZeroOrPlusOne()
    {
        object? EvalSignInt(int v) => EvalOne(
            ["CREATE TABLE t (x INT NOT NULL)"], "SELECT SIGN(x) FROM t",
            q => q.Table("t").Insert(v));

        Assert.Equal(-1, EvalSignInt(-5));
        Assert.Equal(0, EvalSignInt(0));
        Assert.Equal(1, EvalSignInt(7));
    }

    // ---- LN / EXP / LOG ----

    private static double EvalDouble(string expr) => (double)EvalOne(
        ["CREATE TABLE t (x INT NOT NULL)"], $"SELECT {expr} FROM t",
        q => q.Table("t").Insert(1))!;

    [Fact]
    public void Ln_Exp_Log_ComputeExpectedValues()
    {
        Assert.Equal(0.0, EvalDouble("LN(1)"), 10);
        Assert.Equal(1.0, EvalDouble("EXP(0)"), 10);
        Assert.Equal(2.0, EvalDouble("LOG(100)"), 10);     // base-10
        Assert.Equal(3.0, EvalDouble("LOG(2, 8)"), 10);    // log base 2 of 8
        Assert.Equal(1.0, EvalDouble("LN(EXP(1))"), 10);
    }

    [Fact]
    public void Numeric_Functions_PropagateNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (x INT)"],
            "SELECT LN(x) FROM t",
            q => q.Table("t").Insert((object?)null)));
        Assert.Null(EvalOne(
            ["CREATE TABLE t (x INT)"],
            "SELECT SIGN(x) FROM t",
            q => q.Table("t").Insert((object?)null)));
    }
}
