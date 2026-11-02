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

    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;

    public static object? Parse(object? value, bool safe)
    {
        if (value is null) return null;
        var text = ((Utf8String)value).ToStringDecoded().Trim();
        if (DateTime.TryParseExact(text, new[] { "yyyyMMdd", "yyyyMMd" }, CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var date))
        {
            return Timestamp.FromDateTime(date);
        }

        if (safe) return null;
        throw new FormatException($"Invalid compact date '{text}'");
    }
}
