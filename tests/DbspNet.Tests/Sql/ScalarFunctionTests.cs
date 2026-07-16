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

    private static SelectPlan ResolvePlan(string query, params string[] ddl)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return (SelectPlan)resolver.Resolve(Parser.ParseStatement(query));
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

    // ---- SPLIT_INDEX ----

    [Fact]
    public void SplitIndex_ReturnsZeroBasedPart()
    {
        Assert.Equal("a", EvalStr("SPLIT_INDEX(s, '/', 0)", "a/b/c"));
        Assert.Equal("b", EvalStr("SPLIT_INDEX(s, '/', 1)", "a/b/c"));
        Assert.Equal("c", EvalStr("SPLIT_INDEX(s, '/', 2)", "a/b/c"));
        // Empty part between consecutive delimiters (the "//" in a URL).
        Assert.Equal("", EvalStr("SPLIT_INDEX(s, '/', 1)", "http:////host"));
        Assert.Equal("host", EvalStr("SPLIT_INDEX(s, '/', 4)", "http:////host"));
    }

    [Fact]
    public void SplitIndex_OutOfRangeOrNegative_IsNull()
    {
        Assert.Null(EvalStr("SPLIT_INDEX(s, '/', 3)", "a/b/c"));
        Assert.Null(EvalStr("SPLIT_INDEX(s, '/', -1)", "a/b/c"));
    }

    [Fact]
    public void SplitIndex_MultibyteContentAndDelimiter()
    {
        // Byte-wise split must still be code-point-correct for valid UTF-8:
        // multi-byte parts and a multi-byte ('—', U+2014, 3 bytes) delimiter.
        Assert.Equal("café", EvalStr("SPLIT_INDEX(s, '—', 0)", "café—latté—τ"));
        Assert.Equal("latté", EvalStr("SPLIT_INDEX(s, '—', 1)", "café—latté—τ"));
        Assert.Equal("τ", EvalStr("SPLIT_INDEX(s, '—', 2)", "café—latté—τ"));
        Assert.Equal("latté", EvalStr("SPLIT_PART(s, '—', 2)", "café—latté—τ"));
    }

    [Fact]
    public void SplitIndex_PropagatesNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (s VARCHAR)"],
            "SELECT SPLIT_INDEX(s, '/', 0) FROM t",
            q => q.Table("t").Insert((object?)null)));
    }

    // ---- SPLIT_PART ----

    [Fact]
    public void SplitPart_ReturnsOneBasedPart()
    {
        Assert.Equal("a", EvalStr("SPLIT_PART(s, '/', 1)", "a/b/c"));
        Assert.Equal("c", EvalStr("SPLIT_PART(s, '/', 3)", "a/b/c"));
        // Negative counts from the end (PostgreSQL 14+).
        Assert.Equal("c", EvalStr("SPLIT_PART(s, '/', -1)", "a/b/c"));
        Assert.Equal("a", EvalStr("SPLIT_PART(s, '/', -3)", "a/b/c"));
    }

    [Fact]
    public void SplitPart_OutOfRangeOrZero_IsEmptyString()
    {
        Assert.Equal("", EvalStr("SPLIT_PART(s, '/', 4)", "a/b/c"));
        Assert.Equal("", EvalStr("SPLIT_PART(s, '/', 0)", "a/b/c"));
        Assert.Equal("", EvalStr("SPLIT_PART(s, '/', -4)", "a/b/c"));
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

    // ---- Typeless NULL (bare null adopts context type) ----

    [Fact]
    public void TypelessNull_CaseBranch_UnifiesWithDate()
    {
        // finwire_company shape: CASE WHEN ... THEN null ELSE CAST('...' AS DATE).
        // Bare null in one branch must unify with DATE, not fail as INTEGER.
        var q = Compile(
            ["CREATE TABLE t (flag INT NOT NULL, d VARCHAR NOT NULL)"],
            "SELECT CASE WHEN flag = 0 THEN null ELSE CAST(d AS DATE) END AS x FROM t");
        q.Table("t").Insert(0, "2020-01-01");   // → null
        q.Table("t").Insert(1, "2020-06-15");   // → date
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, (object?)null));
        Assert.Equal(1, WeightOf(q.Current, Date32.Parse("2020-06-15")));
    }

    [Fact]
    public void TypelessNull_SimpleCaseBranch_UnifiesWithVarchar()
    {
        // watches_history shape: simple CASE with a null ELSE among VARCHAR arms.
        Assert.Equal("Activate", EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL)"],
            "SELECT CASE a WHEN 'ACTV' THEN 'Activate' ELSE null END AS x FROM t",
            q => q.Table("t").Insert("ACTV")));
        Assert.Null(EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL)"],
            "SELECT CASE a WHEN 'ACTV' THEN 'Activate' ELSE null END AS x FROM t",
            q => q.Table("t").Insert("OTHER")));
    }

    [Fact]
    public void TypelessNull_CastToDate()
    {
        // crm_customer_mgmt shape: CAST(null AS DATE) — a typed DATE null, no
        // INTEGER→DATE cast path needed.
        var q = Compile(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "SELECT CAST(null AS DATE) AS d FROM t");
        q.Table("t").Insert(1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, (object?)null));
        // The column is typed DATE (nullable), not INTEGER.
        Assert.IsType<DbspNet.Sql.TypeSystem.SqlDateType>(
            ((SelectPlan)ResolvePlan("SELECT CAST(null AS DATE) AS d FROM t",
                "CREATE TABLE t (x INT NOT NULL)")).Query.Schema[0].Type);
    }

    [Fact]
    public void TypelessNull_ComparisonWithString()
    {
        // null = 'x' — previously required CAST(NULL AS VARCHAR). NULL comparison
        // is UNKNOWN (never TRUE), so the row is filtered out.
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t WHERE null = s");
        q.Table("t").Insert("a");
        q.Step();
        Assert.Equal(0, q.Current.Count);
    }

    [Fact]
    public void TypelessNull_ArithmeticStillWorks()
    {
        // Regression guard: null + 1 unified as INTEGER before this change and
        // must keep working (adopts INTEGER, result null).
        Assert.Null(EvalOne(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "SELECT null + 1 AS y FROM t",
            q => q.Table("t").Insert(1)));
    }

    [Fact]
    public void TypelessNull_Coalesce()
    {
        Assert.Equal("fallback", EvalOne(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "SELECT COALESCE(null, 'fallback') AS y FROM t",
            q => q.Table("t").Insert(1)));
    }

    // ---- CONCAT_WS ----

    [Fact]
    public void ConcatWs_JoinsWithSeparator()
    {
        Assert.Equal("a-b-c", EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR NOT NULL, c VARCHAR NOT NULL)"],
            "SELECT CONCAT_WS('-', a, b, c) FROM t",
            q => q.Table("t").Insert("a", "b", "c")));
    }

    [Fact]
    public void ConcatWs_SkipsNullValues_NoDoubledSeparator()
    {
        // NULL value args collapse: no leading/trailing/doubled separator.
        Assert.Equal("a-c", EvalOne(
            ["CREATE TABLE t (a VARCHAR NOT NULL, b VARCHAR, c VARCHAR NOT NULL)"],
            "SELECT CONCAT_WS('-', a, b, c) FROM t",
            q => q.Table("t").Insert("a", null, "c")));
    }

    [Fact]
    public void ConcatWs_AllNullValues_EmptyString()
    {
        Assert.Equal("", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL, a VARCHAR, b VARCHAR)"],
            "SELECT CONCAT_WS(s, a, b) FROM t",
            q => q.Table("t").Insert("-", null, null)));
    }

    [Fact]
    public void ConcatWs_NullSeparator_ReturnsNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (s VARCHAR, a VARCHAR NOT NULL)"],
            "SELECT CONCAT_WS(s, a) FROM t",
            q => q.Table("t").Insert(new object?[] { null, "a" })));
    }

    // ---- MD5 ----

    [Fact]
    public void Md5_MatchesKnownDigests()
    {
        // Reference MD5 hex digests (as PostgreSQL / Spark emit).
        Assert.Equal("d41d8cd98f00b204e9800998ecf8427e", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT MD5(s) FROM t",
            q => q.Table("t").Insert("")));
        Assert.Equal("0cc175b9c0f1b6a831c399e269772661", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT MD5(s) FROM t",
            q => q.Table("t").Insert("a")));
        Assert.Equal("900150983cd24fb0d6963f7d28e17f72", EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT MD5(s) FROM t",
            q => q.Table("t").Insert("abc")));
    }

    [Fact]
    public void Md5_PropagatesNull()
    {
        Assert.Null(EvalOne(
            ["CREATE TABLE t (s VARCHAR)"],
            "SELECT MD5(s) FROM t",
            q => q.Table("t").Insert((object?)null)));
    }

    // ---- RLIKE (Spark infix, desugars to REGEXP_LIKE) ----

    [Fact]
    public void Rlike_MatchesPosixRegex()
    {
        Assert.Equal(true, EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s RLIKE '^[0-9]+$' FROM t",
            q => q.Table("t").Insert("12345")));
        Assert.Equal(false, EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s RLIKE '^[0-9]+$' FROM t",
            q => q.Table("t").Insert("12a45")));
    }

    [Fact]
    public void NotRlike_Negates()
    {
        Assert.Equal(true, EvalOne(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s NOT RLIKE '^[0-9]+$' FROM t",
            q => q.Table("t").Insert("x")));
    }

    // ---- Typed temporal literals ----

    [Fact]
    public void TimestampLiteral_ParsesAndEqualsCast()
    {
        // TIMESTAMP '…' ≡ CAST('…' AS TIMESTAMP); both feed a filter here.
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT ts FROM t WHERE ts < TIMESTAMP '9999-12-31 23:59:59.999'");
        q.Table("t").Insert(Timestamp.Parse("2020-01-01 00:00:00"));
        q.Step();
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void DateLiteral_Parses()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT d FROM t WHERE d >= DATE '1900-01-01'");
        q.Table("t").Insert(Date32.Parse("2020-06-15"));
        q.Step();
        Assert.Equal(1, q.Current.Count);
    }

    // ---- TINYINT / SMALLINT (parse as INTEGER) ----

    [Fact]
    public void TinyIntSmallInt_ParseAsInteger()
    {
        var q = Compile(
            ["CREATE TABLE t (flag TINYINT NOT NULL, n SMALLINT NOT NULL)"],
            "SELECT flag + n AS s FROM t");
        q.Table("t").Insert(1, 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 101));
    }

    // ---- CAST(bigint AS timestamp) ----

    [Fact]
    public void CastBigintToTimestamp_InterpretsAsMicroseconds()
    {
        // 1_600_000_000_000_000 µs since epoch = 2020-09-13 12:26:40 UTC.
        var q = Compile(
            ["CREATE TABLE t (n BIGINT NOT NULL)"],
            "SELECT CAST(n AS TIMESTAMP) AS ts FROM t");
        q.Table("t").Insert(1_600_000_000_000_000L);
        q.Step();
        var entry = Assert.Single(q.Current);
        var ts = Assert.IsType<Timestamp>(entry.Key[0]);
        Assert.Equal(1_600_000_000_000_000L, ts.Microseconds);
    }
}
