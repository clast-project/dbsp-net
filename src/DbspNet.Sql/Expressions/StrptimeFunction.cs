// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Linq.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>Compact calendar-date parsing with strict and null-on-error variants.</summary>
internal sealed class StrptimeFunction(string name, bool safe) : IScalarFunction
{
    private static readonly string[] Formats = ["yyyyMMdd", "yyyyMMd"];

    public string Name => name;

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 2 || args[0].Type is not (SqlVarcharType or SqlNullType)
            || args[1] is not ResolvedLiteral { Value: Utf8String format }
            || format.ToStringDecoded() != "%Y%m%d")
        {
            throw new ResolveException($"{name} requires a string and the constant format '%Y%m%d'");
        }

        return new ResolvedFunctionCall(name, args, new SqlTimestampType(safe || args[0].Type.Nullable));
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        Expression.Call(typeof(StrptimeFunction).GetMethod(nameof(Parse))!,
            buildArg(fn.Arguments[0]), Expression.Constant(safe));

    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t)
    {
        if (TypedExpressionCompiler.UnderlyingType(t[0].Type) != typeof(Utf8String))
        {
            return null;
        }

        var input = Expression.Convert(t[0], typeof(Utf8String?));

        return fn.Type.Nullable
            ? Expression.Call(typeof(StrptimeFunction).GetMethod(nameof(ParseNullable))!, input, Expression.Constant(safe))
            : Expression.Call(typeof(StrptimeFunction).GetMethod(nameof(ParseStrict))!, input);
    }

    /// <summary>The typed path's nullable result: NULL input, or (when safe) an unparsable one, is NULL.</summary>
    public static Timestamp? ParseNullable(Utf8String? value, bool safe) => Parse(value, safe) is Timestamp ts ? ts : null;

    /// <summary>The typed path's strict, non-null result.</summary>
    public static Timestamp ParseStrict(Utf8String? value) => (Timestamp)Parse(value, safe: false)!;

    public static object? Parse(object? value, bool safe)
    {
        if (value is null) return null;
        var text = ((Utf8String)value).ToStringDecoded().Trim();
        if (DateTime.TryParseExact(text, Formats, CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var date))
        {
            return Timestamp.FromDateTime(date);
        }

        if (safe) return null;
        throw new FormatException($"Invalid compact date '{text}'");
    }
}
