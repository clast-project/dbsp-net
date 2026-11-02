// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Text.Encodings.Web;
using System.Text.Json;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>Extract a JSON value through a constant dotted member path.</summary>
internal sealed class JsonExtractStringFunction : IScalarFunction
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public string Name => "json_extract_string";

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 2 || args[0].Type is not (SqlVarcharType or SqlNullType)
            || args[1] is not ResolvedLiteral { Value: Utf8String path })
        {
            throw new ResolveException("JSON_EXTRACT_STRING requires a string and a constant member path");
        }

        var text = path.ToStringDecoded();
        if (!text.StartsWith("$.", StringComparison.Ordinal)
            || text[2..].Split('.').Any(part => part.Length == 0 || part.Any(c => !char.IsLetterOrDigit(c) && c != '_')))
        {
            throw new ResolveException("JSON_EXTRACT_STRING supports dotted member paths such as '$.key'");
        }

        return new ResolvedFunctionCall(Name, args, new SqlVarcharType(null, true));
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var path = ((Utf8String)((ResolvedLiteral)fn.Arguments[1]).Value!).ToStringDecoded()[2..].Split('.');
        return Expression.Call(typeof(JsonExtractStringFunction).GetMethod(nameof(Extract))!,
            buildArg(fn.Arguments[0]), Expression.Constant(path));
    }

    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;

    public static object? Extract(object? source, string[] path)
    {
        if (source is null) return null;
        using var document = JsonDocument.Parse(((Utf8String)source).ToStringDecoded());
        var value = document.RootElement;
        foreach (var key in path)
        {
            if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty(key, out value)) return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => Utf8String.Of(value.GetString()!),
            _ => Utf8String.Of(JsonSerializer.Serialize(value, JsonOptions)),
        };
    }
}
