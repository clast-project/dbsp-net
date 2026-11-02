// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class NullOnErrorAndJsonTests
{
    private static CompiledQuery Compile(string ddl, string query)
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(ddl));
        return PlanToCircuit.Compile(((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query);
    }

    private static object? Eval(string expression, string? value)
    {
        var q = Compile("CREATE TABLE t (s VARCHAR)", $"SELECT {expression} FROM t");
        q.Table("t").Insert(new object?[] { value });
        q.Step();
        var result = Assert.Single(q.Current).Key[0];
        return result is Utf8String text ? text.ToStringDecoded() : result;
    }

    [Theory]
    [InlineData("42", 42)]
    [InlineData("bad", null)]
    [InlineData("2147483648", null)]
    [InlineData(null, null)]
    public void TryCastReturnsNullForInvalidOrOverflowingStrings(string? value, int? expected) =>
        Assert.Equal(expected, Eval("TRY_CAST(s AS INTEGER)", value));

    [Fact]
    public void TryCastChecksNumericNarrowingAndPreservesRetractions()
    {
        var q = Compile("CREATE TABLE t (n BIGINT)", "SELECT TRY_CAST(n AS INTEGER) AS n FROM t");
        q.Table("t").Insert(2147483648L);
        q.Step();
        Assert.Null(Assert.Single(q.Current).Key[0]);
        q.Table("t").Delete(2147483648L);
        q.Table("t").Insert(42L);
        q.Step();
        Assert.Equal(2, q.Current.Count);
        Assert.Null(Assert.Single(q.Current, p => p.Value.Value == -1).Key[0]);
        Assert.Equal(42, Assert.Single(q.Current, p => p.Value.Value == 1).Key[0]);
    }

    [Fact]
    public void PostAggregateNullCastsKeepTheirDeclaredType()
    {
        var q = Compile("CREATE TABLE t (s VARCHAR)", "SELECT TRY_CAST(NULL AS INTEGER) AS n, COUNT(*) AS c FROM t");
        q.Table("t").Insert("a");
        q.Step();
        Assert.Null(Assert.Single(q.Current).Key[0]);
    }

    [Theory]
    [InlineData("2002-12-31 00:00:00", "2002-12-31 00:00:00")]
    [InlineData("2002-12-31 00:00:00.120000", "2002-12-31 00:00:00.12")]
    [InlineData("2002-12-31 00:00:00.123456", "2002-12-31 00:00:00.123456")]
    public void TimestampTextOmitsInsignificantFractionalZeros(string input, string expected) =>
        Assert.Equal(expected, Eval("CAST(CAST(s AS TIMESTAMP) AS VARCHAR)", input));

    [Theory]
    [InlineData("0", false)]
    [InlineData("-1", true)]
    [InlineData(null, null)]
    public void NumericBooleanCastsPreserveZeroAndNull(string? value, bool? expected) =>
        Assert.Equal(expected, Eval("CAST(CAST(s AS INTEGER) AS BOOLEAN)", value));

    [Fact]
    public void CastOfAnUntypedNullIsNull()
    {
        var expression = new ResolvedCast(
            new ResolvedLiteral(LiteralKind.Null, null, SqlNullType.Instance), new SqlIntegerType(true));
        Assert.Null(DbspNet.Sql.Expressions.ExpressionCompiler.CompileScalar(expression)(Array.Empty<object?>()));
    }

    [Fact]
    public void StrictCastStillFailsOnBadInput() =>
        Assert.Throws<FormatException>(() => Eval("CAST(s AS INTEGER)", "bad"));

    [Theory]
    [InlineData("20000229")]
    [InlineData("20000229 ")]
    public void CompactDateParsingPreservesLeapDays(string value) =>
        Assert.Equal(Timestamp.FromDateTime(new DateTime(2000, 2, 29)), Eval("TRY_STRPTIME(s, '%Y%m%d')", value));

    [Fact]
    public void CompactDateErrorsAreNullOnlyForTheSafeVariant()
    {
        Assert.Null(Eval("TRY_STRPTIME(s, '%Y%m%d')", "20010229"));
        Assert.Null(Eval("TRY_STRPTIME(s, '%Y%m%d')", null));
        Assert.Throws<FormatException>(() => Eval("STRPTIME(s, '%Y%m%d')", "20010229"));
    }

    [Theory]
    [InlineData("{\"x\":\"hello\"}", "hello")]
    [InlineData("{\"x\":12}", "12")]
    [InlineData("{\"x\":false}", "false")]
    [InlineData("{\"x\":null}", null)]
    [InlineData("{}", null)]
    [InlineData(null, null)]
    public void JsonScalarExtractionPreservesValuesAndNulls(string? value, string? expected) =>
        Assert.Equal(expected, Eval("JSON_EXTRACT_STRING(s, '$.x')", value));

    [Theory]
    [InlineData("SELECT FIRST_VALUE(s IGNORE NULLS) FROM t")]
    [InlineData("SELECT SUM(s) IGNORE NULLS OVER (ORDER BY s) FROM t")]
    public void IgnoreNullsRejectsNonWindowOrUnsupportedFunctions(string sql) =>
        Assert.Throws<ParseException>(() => Parser.ParseStatement(sql));

    [Fact]
    public void IgnoreNullsUsesEachColumnsFirstAndLastNonNullValuesAfterRetraction()
    {
        const string frame = "OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
        var q = Compile("CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, a INT, b INT)",
            $"SELECT g, ts, FIRST_VALUE(a IGNORE NULLS) {frame} AS f, LAST_VALUE(b) IGNORE NULLS {frame} AS l FROM t");
        q.Table("t").Insert(1, 1, null, 9);
        q.Table("t").Insert(1, 2, 7, null);
        q.Table("t").Insert(1, 3, 8, 10);
        q.Table("t").Insert(2, 1, null, null);
        q.Step();
        foreach (var pair in q.Current)
        {
            Assert.Equal((int)pair.Key[0]! == 1 ? 7 : null, pair.Key[2]);
            Assert.Equal((int)pair.Key[0]! == 1 ? 10 : null, pair.Key[3]);
        }
        q.Table("t").Delete(1, 2, 7, null);
        q.Table("t").Delete(1, 3, 8, 10);
        q.Step();
        var inserted = Assert.Single(q.Current, p => p.Value.Value == 1);
        Assert.Equal(1, inserted.Key[0]);
        Assert.Equal(1, inserted.Key[1]);
        Assert.Null(inserted.Key[2]);
        Assert.Equal(9, inserted.Key[3]);
        Assert.Equal(3, q.Current.Count(p => p.Value.Value == -1));
    }
}
