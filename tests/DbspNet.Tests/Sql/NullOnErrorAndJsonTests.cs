// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.InteropServices;
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
    public void TryCastRemainsAnOrdinaryColumnName()
    {
        var q = Compile("CREATE TABLE t (try_cast INTEGER)", "SELECT try_cast, t.try_cast + 1 FROM t");
        q.Table("t").Insert(7);
        q.Step();
        var row = Assert.Single(q.Current).Key;
        Assert.Equal(7, row[0]);
        Assert.Equal(8, row[1]);
    }

    [Theory]
    [InlineData("2.9", 3)]
    [InlineData("-2.9", -3)]
    [InlineData("2.5", 2)]
    [InlineData("-2.5", -2)]
    [InlineData("3.5", 4)]
    public void FloatingIntegralCastsRoundNearestWithEvenTies(string value, int expected)
    {
        foreach (var type in new[] { "REAL", "DOUBLE" })
        {
            Assert.Equal(expected, Eval($"CAST(CAST(s AS {type}) AS INTEGER)", value));
            Assert.Equal(expected, Eval($"TRY_CAST(CAST(s AS {type}) AS INTEGER)", value));
            Assert.Equal((long)expected, Eval($"TRY_CAST(CAST(s AS {type}) AS BIGINT)", value));
        }
    }

    [Theory]
    [InlineData("2.5", 3)]
    [InlineData("-2.5", -3)]
    [InlineData("2.4999999999999999999999999999999999999999", 2)]
    [InlineData("1e2", 100)]
    [InlineData("1.5e1", 15)]
    [InlineData(".5", 1)]
    [InlineData("-.5", -1)]
    [InlineData("000.5", 1)]
    [InlineData("1e-32768", 0)]
    [InlineData("0e100", 0)]
    public void TextIntegralCastsRoundExactDecimalDigitsAwayFromZero(string value, int expected)
    {
        Assert.Equal(expected, Eval("CAST(s AS INTEGER)", value));
        Assert.Equal(expected, Eval("TRY_CAST(s AS INTEGER)", value));
        Assert.Equal((long)expected, Eval("TRY_CAST(s AS BIGINT)", value));
    }

    [Theory]
    [InlineData("2.5", 3)]
    [InlineData("-2.5", -3)]
    public void DecimalCastsRoundTiesAwayFromZero(string value, int expected)
    {
        foreach (var cast in new[] { "CAST", "TRY_CAST" })
        {
            Assert.Equal(expected, Eval($"{cast}(CAST(s AS DECIMAL(3,1)) AS INTEGER)", value));
            Assert.Equal(new Clast.DatabaseDecimal.Values.Decimal128(expected),
                Eval($"{cast}(CAST(s AS DECIMAL(3,1)) AS DECIMAL(2,0))", value));
            Assert.Equal(new Clast.DatabaseDecimal.Values.Decimal128(expected),
                Eval($"{cast}(CAST(s AS DOUBLE) AS DECIMAL(2,0))", value));
            Assert.Equal(new Clast.DatabaseDecimal.Values.Decimal128(expected),
                Eval($"{cast}(s AS DECIMAL(2,0))", value));
        }
    }

    [Theory]
    [InlineData("2147483647.49", 2147483647)]
    [InlineData("-2147483648.49", -2147483648)]
    [InlineData("2147483647.5", null)]
    [InlineData("-2147483648.5", null)]
    [InlineData("1e9999999999999", null)]
    [InlineData("1e-9999999999999", null)]
    [InlineData("0e9999999999999", null)]
    [InlineData("1e 2", null)]
    [InlineData("1.2.3", null)]
    [InlineData("3814585150461789134281.40e-15", null)]
    public void TextIntegralCastChecksRangeAfterRoundingAndRejectsMalformedNumbers(string value, int? expected) =>
        Assert.Equal(expected, Eval("TRY_CAST(s AS INTEGER)", value));

    [Theory]
    [InlineData("DOUBLE", "INTEGER", "2147483647.6")]
    [InlineData("DOUBLE", "INTEGER", "-2147483648.6")]
    [InlineData("DOUBLE", "BIGINT", "9.23e18")]
    [InlineData("DOUBLE", "INTEGER", "NaN")]
    [InlineData("DOUBLE", "REAL", "1e100")]
    [InlineData("BIGINT", "INTEGER", "2147483648")]
    [InlineData("DECIMAL(12,0)", "INTEGER", "4294967296")]
    [InlineData("DECIMAL(12,0)", "DECIMAL(3,0)", "1000")]
    [InlineData("BIGINT", "DECIMAL(38,37)", "9223372036854775807")]
    [InlineData("DOUBLE", "DECIMAL(3,0)", "1000")]
    [InlineData("DOUBLE", "REAL", "-1e100")]
    [InlineData("DECIMAL(12,0)", "INTEGER", "-2147483649")]
    [InlineData("DECIMAL(20,0)", "BIGINT", "9223372036854775808")]
    [InlineData("DECIMAL(20,0)", "BIGINT", "-9223372036854775809")]
    [InlineData("DECIMAL(12,0)", "DECIMAL(3,0)", "-1000")]
    [InlineData("DECIMAL(8,2)", "DECIMAL(3,1)", "99.96")]
    [InlineData("DECIMAL(38,0)", "DECIMAL(38,37)", "99999999999999999999999999999999999999")]
    [InlineData("BIGINT", "DECIMAL(3,0)", "1000")]
    [InlineData("DOUBLE", "DECIMAL(38,0)", "1e100")]
    [InlineData("DOUBLE", "DECIMAL(38,0)", "NaN")]
    public void StrictAndSafeCastsShareRangeChecks(string source, string target, string value)
    {
        Assert.Throws<OverflowException>(() => Eval($"CAST(CAST(s AS {source}) AS {target})", value));
        Assert.Null(Eval($"TRY_CAST(CAST(s AS {source}) AS {target})", value));
    }

    [Fact]
    public void TryCastDoesNotSuppressOperandErrors() =>
        Assert.Throws<FormatException>(() => Eval("TRY_CAST(CAST(s AS INTEGER) AS VARCHAR)", "bad"));

    [Fact]
    public void TryCastDoesNotSuppressOperandOverflow() =>
        Assert.Throws<OverflowException>(() => Eval("TRY_CAST(CAST(s AS BIGINT) AS VARCHAR)", "9223372036854775808"));

    [Fact]
    public void NestedTryCastRetainsNullOnErrorBehavior() =>
        Assert.Null(Eval("TRY_CAST(TRY_CAST(s AS INTEGER) AS VARCHAR)", "bad"));

    [Theory]
    [InlineData("2147483647", int.MaxValue)]
    [InlineData("-2147483648", int.MinValue)]
    public void TryCastPreservesDecimalToIntegerBoundaries(string value, int expected) =>
        Assert.Equal(expected, Eval("TRY_CAST(CAST(s AS DECIMAL(12,0)) AS INTEGER)", value));

    [Theory]
    [InlineData("999", 999)]
    [InlineData("-999", -999)]
    public void TryCastPreservesDecimalPrecisionBoundaries(string value, int mantissa) =>
        Assert.Equal(new Clast.DatabaseDecimal.Values.Decimal128(mantissa),
            Eval("TRY_CAST(CAST(s AS DECIMAL(12,0)) AS DECIMAL(3,0))", value));

    [Theory]
    [InlineData("3.4e38")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData("NaN")]
    public void TryCastPreservesRepresentableAndAlreadyNonfiniteFloatingValues(string value) =>
        Assert.Equal((float)double.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            Eval("TRY_CAST(CAST(s AS DOUBLE) AS REAL)", value));

    [Fact]
    public void TryCastIntervalLiteralsAndColumnsShareNullOnErrorSemantics()
    {
        Assert.Null(Eval("TRY_CAST('bad' AS INTERVAL DAY)", "unused"));
        Assert.Null(Eval("TRY_CAST(s AS INTERVAL DAY)", "bad"));
        Assert.Equal(Eval("TRY_CAST(s AS INTERVAL DAY)", "2"),
            Eval("TRY_CAST('2' AS INTERVAL DAY)", "unused"));
        Assert.Throws<FormatException>(() => Eval("CAST('bad' AS INTERVAL DAY)", "unused"));
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
    [InlineData("{\"x\":1,\"x\":2}", "$.x", "1")]
    [InlineData("{\"x\":null,\"x\":2}", "$.x", null)]
    [InlineData("{\"x\":{\"a\":1,\"a\":2},\"x\":{\"a\":3}}", "$.x.a", "1")]
    [InlineData("{\"x\":null,\"x\":{\"a\":2}}", "$.x.a", null)]
    public void JsonExtractionSelectsTheFirstDuplicateKeyAtEachLevel(string json, string path, string? expected) =>
        Assert.Equal(expected, Eval($"JSON_EXTRACT_STRING(s, '{path}')", json));

    [Theory]
    [InlineData("1e2", "100.0")]
    [InlineData("1.2300", "1.23")]
    [InlineData("-0", "0")]
    [InlineData("-0.0", "-0.0")]
    [InlineData("1e-6", "0.000001")]
    [InlineData("1e-7", "1e-7")]
    [InlineData("1.23e-5", "0.0000123")]
    [InlineData("1e20", "100000000000000000000.0")]
    [InlineData("1e21", "1e21")]
    [InlineData("-1.23e20", "-123000000000000000000.0")]
    [InlineData("18446744073709551616", "18446744073709551616")]
    [InlineData("1e400", "1e400")]
    [InlineData("1e-400", "0.0")]
    [InlineData("1.234567890123456789", "1.2345678901234567")]
    public void JsonNumbersUseReferenceTextAtEveryNestingLevel(string number, string expected)
    {
        Assert.Equal(expected, Eval("JSON_EXTRACT_STRING(s, '$.x')", $"{{\"x\":{number}}}"));
        Assert.Equal($"{{\"a\":[{expected}]}}", Eval("JSON_EXTRACT_STRING(s, '$.x')", $"{{\"x\":{{\"a\":[{number}]}}}}"));
    }

    [Theory]
    [InlineData("SELECT FIRST_VALUE(s IGNORE NULLS) FROM t")]
    [InlineData("SELECT SUM(s) IGNORE NULLS OVER (ORDER BY s) FROM t")]
    public void IgnoreNullsRejectsNonWindowOrUnsupportedFunctions(string sql) =>
        Assert.Throws<ParseException>(() => Parser.ParseStatement(sql));

    [Theory]
    [InlineData("FIRST_VALUE(s IGNORE NULLS) OVER (ORDER BY s)")]
    [InlineData("LAST_VALUE(s IGNORE NULLS) OVER (ORDER BY s)")]
    [InlineData("FIRST_VALUE(s) OVER (ORDER BY s), LAST_VALUE(s IGNORE NULLS) OVER (ORDER BY s)")]
    [InlineData("LAST_VALUE(s IGNORE NULLS) OVER (ORDER BY s), FIRST_VALUE(s) OVER (ORDER BY s)")]
    [InlineData("FIRST_VALUE(s IGNORE NULLS) OVER (ORDER BY s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")]
    public void IgnoreNullsRejectsImplicitAndPartialFrames(string expressions) =>
        Assert.Throws<ResolveException>(() => Compile("CREATE TABLE t(s VARCHAR)", $"SELECT {expressions} FROM t"));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ExplicitFrameValidationDoesNotDependOnSelectOrder(bool lagFirst)
    {
        const string frame = "OVER (ORDER BY s ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";
        var first = $"FIRST_VALUE(s IGNORE NULLS) {frame}";
        var lag = $"LAG(s) {frame}";
        var expressions = lagFirst ? lag + ", " + first : first + ", " + lag;
        Assert.Throws<ResolveException>(() => Compile("CREATE TABLE t(s VARCHAR)", $"SELECT {expressions} FROM t"));
    }

    [Theory]
    [InlineData("{\"x\":12}", "12")]
    [InlineData("{\"x\":true}", "true")]
    [InlineData("{\"x\":false}", "false")]
    [InlineData("{\"x\":\"abc\"}", "abc")]
    [InlineData("{\"x\":{\"a\":[1e2]}}", "{\"a\":[100.0]}")]
    [InlineData("{\"x\":[1e2]}", "[100.0]")]
    public void JsonResultsRetainOnlyTheirUtf8Payload(string source, string expected)
    {
        var q = Compile("CREATE TABLE t(s VARCHAR)", "SELECT JSON_EXTRACT_STRING(s, '$.x') FROM t");
        q.Table("t").Insert(source);
        q.Step();
        var value = Assert.IsType<Utf8String>(Assert.Single(q.Current).Key[0]);
        Assert.Equal(expected, value.ToStringDecoded());
        Assert.True(MemoryMarshal.TryGetArray(value.Memory, out var bytes));
        Assert.NotNull(bytes.Array);
        Assert.Equal(value.ByteLength, bytes.Array!.Length);
    }

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
