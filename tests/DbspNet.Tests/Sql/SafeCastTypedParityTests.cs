// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// TRY_CAST, STRPTIME / TRY_STRPTIME and JSON_EXTRACT_STRING lower on the typed path, so a view
// using them is not thrown back to the structural pipeline (and out of the parallel compile). The
// oracle is the structural compiler: for every expression and every input row, both paths give
// the same value, or both throw the same exception type.
using System.Reflection;
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Sql;

public class SafeCastTypedParityTests
{
    private const string Ddl = "CREATE TABLE t (s VARCHAR, n BIGINT, f DOUBLE, x DECIMAL(10,2), i INT)";

    public static TheoryData<string> Expressions => new()
    {
        "TRY_CAST(s AS INTEGER)",
        "TRY_CAST(s AS BIGINT)",
        "TRY_CAST(s AS DOUBLE)",
        "TRY_CAST(s AS REAL)",
        "TRY_CAST(s AS DECIMAL(5,2))",
        "TRY_CAST(s AS DATE)",
        "TRY_CAST(s AS TIMESTAMP)",
        "TRY_CAST(n AS INTEGER)",
        "TRY_CAST(n AS VARCHAR)",
        "TRY_CAST(n AS DECIMAL(4,1))",
        "TRY_CAST(f AS INTEGER)",
        "TRY_CAST(f AS BIGINT)",
        "TRY_CAST(f AS REAL)",
        "TRY_CAST(f AS DECIMAL(4,1))",
        "TRY_CAST(x AS DECIMAL(3,1))",
        "TRY_CAST(x AS DECIMAL(12,2))",
        "TRY_CAST(x AS INTEGER)",
        "TRY_CAST(x AS VARCHAR)",
        "TRY_CAST(x AS DOUBLE)",
        "TRY_CAST(i AS DECIMAL(3,0))",
        "TRY_CAST(i AS BIGINT)",
        "TRY_CAST(CAST(s AS INTEGER) AS BIGINT)", // the inner strict cast's error propagates
        "TRY_CAST(TRY_CAST(s AS DOUBLE) AS INTEGER)",
        "COALESCE(TRY_CAST(s AS INTEGER), -1)",
        "STRPTIME(s, '%Y%m%d')",
        "TRY_STRPTIME(s, '%Y%m%d')",
        "JSON_EXTRACT_STRING(s, '$.a')",
        "JSON_EXTRACT_STRING(s, '$.a.b')",
    };

    private static readonly object?[][] Rows = Inputs().ToArray();

    private static IEnumerable<object?[]> Inputs()
    {
        string?[] strings =
        [
            "42", "bad", "2147483648", "2.5", "-2.5", " 7 ", "1e2", "20240131", "2024-01-31",
            "2024-01-31 10:11:12", "{\"a\":1.50,\"a\":2}", "{\"a\":{\"b\":[1,2e3]}}", "{\"a\":\"x\"}", "", null,
        ];
        long?[] longs = [0, -5, 2147483648L, long.MaxValue, null];
        double?[] doubles = [2.5, -2.5, 3.5, 1e300, double.NaN, double.PositiveInfinity, null];
        Decimal128?[] decimals = [new((Int128)12345), new((Int128)(-25)), new((Int128)99999999), null];
        int?[] ints = [0, 999, 1000, -1000, null];
        for (var k = 0; k < strings.Length * 3; k++)
        {
            yield return
            [
                strings[k % strings.Length] is { } s ? Utf8String.Of(s) : null,
                longs[k % longs.Length],
                doubles[k % doubles.Length],
                decimals[k % decimals.Length],
                ints[k % ints.Length],
            ];
        }
    }

    private static readonly (Type RowType, Func<object?[], object> Factory, Schema Schema) Row = MakeRow();

    private static (Type, Func<object?[], object>, Schema) MakeRow()
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = (SelectPlan)resolver.Resolve(Parser.ParseStatement("SELECT * FROM t"));
        var schema = plan.Query.Schema;
        return (TypedRowEmitter.EmitRowType(schema)!, TypedRowEmitter.BuildBoxedFactory(schema)!, schema);
    }

    private static ResolvedExpression Resolve(string expression)
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = (SelectPlan)resolver.Resolve(Parser.ParseStatement($"SELECT {expression} FROM t"));
        return ((ProjectPlan)plan.Query).Projections[0].Expression;
    }

    private static (object? Value, Type? Error) Run(Func<object?> eval)
    {
        try
        {
            return (eval(), null);
        }
        catch (TargetInvocationException e) when (e.InnerException is { } inner)
        {
            return (null, inner.GetType());
        }
        catch (Exception e)
        {
            return (null, e.GetType());
        }
    }

    [Theory]
    [MemberData(nameof(Expressions))]
    public void TheTypedPathLowersIt_AndAgreesWithTheStructuralPath(string expression)
    {
        var expr = Resolve(expression);
        var structural = ExpressionCompiler.CompileScalar(expr);
        var typed = TypedExpressionCompiler.TryCompile(expr, Row.RowType);
        Assert.True(typed is not null, $"{expression} fell back to the structural path");
        var nulls = 0;
        foreach (var cells in Rows)
        {
            var a = Run(() => structural(cells));
            var row = Row.Factory(cells);
            var b = Run(() => typed!.DynamicInvoke(row));
            var shown = string.Join(", ", cells.Select(c => c?.ToString() ?? "NULL"));
            Assert.True(a.Error == b.Error, $"{expression} on ({shown}): structural {a.Error?.Name ?? "ok"}, typed {b.Error?.Name ?? "ok"}");
            Assert.True(Equals(a.Value, b.Value), $"{expression} on ({shown}): structural {a.Value ?? "NULL"}, typed {b.Value ?? "NULL"}");
            nulls += a.Error is null && a.Value is null ? 1 : 0;
        }

        Assert.True(nulls < Rows.Length, $"{expression} is NULL on every input: the corpus does not exercise it");
    }
}
