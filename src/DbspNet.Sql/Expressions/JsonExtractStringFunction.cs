// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Buffers;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>Extract a JSON value through a constant dotted member path.</summary>
internal sealed class JsonExtractStringFunction : IScalarFunction
{
    private static readonly JsonWriterOptions JsonOptions = new()
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

    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t)
    {
        if (TypedExpressionCompiler.UnderlyingType(t[0].Type) != typeof(Utf8String))
        {
            return null;
        }

        var input = Expression.Convert(t[0], typeof(Utf8String?));

        var path = ((Utf8String)((ResolvedLiteral)fn.Arguments[1]).Value!).ToStringDecoded()[2..].Split('.');
        return Expression.Call(typeof(JsonExtractStringFunction).GetMethod(nameof(ExtractTyped))!, input, Expression.Constant(path));
    }

    /// <summary>The typed path's form of <see cref="Extract"/>.</summary>
    public static Utf8String? ExtractTyped(Utf8String? source, string[] path) => (Utf8String?)Extract(source, path);

    public static object? Extract(object? source, string[] path)
    {
        if (source is null) return null;
        // Valid UTF-8 parses from the bytes: the string overload transcodes back to exactly these
        // bytes. Invalid UTF-8 goes through the string, which turns bad bytes into U+FFFD where the
        // byte parser would throw.
        var utf8 = ((Utf8String)source).Memory;
        using var document = System.Text.Unicode.Utf8.IsValid(utf8.Span)
            ? JsonDocument.Parse(utf8)
            : JsonDocument.Parse(((Utf8String)source).ToStringDecoded());
        var value = document.RootElement;
        foreach (var key in path)
        {
            if (value.ValueKind != JsonValueKind.Object) return null;
            // DuckDB selects the first duplicate key; TryGetProperty selects the last.
            var found = false;
            foreach (var property in value.EnumerateObject())
            {
                if (!property.NameEquals(key)) continue;
                value = property.Value;
                found = true;
                break;
            }
            if (!found) return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => StringValue(value),
            JsonValueKind.Number => Utf8String.Of(FormatNumber(value)),
            JsonValueKind.True => Utf8String.Of("true"),
            JsonValueKind.False => Utf8String.Of("false"),
            _ => Serialize(value),
        };
    }

    // A JSON string's value: its raw bytes between the quotes when it has no escape sequence (the
    // parser has validated them), else the unescaped string.
    private static Utf8String StringValue(JsonElement value)
    {
        var raw = System.Runtime.InteropServices.JsonMarshal.GetRawUtf8Value(value);
        return raw.Length >= 2 && raw.IndexOf((byte)'\\') < 0
            ? Utf8String.FromBytes(raw[1..^1].ToArray())
            : Utf8String.Of(value.GetString()!);
    }

    private static Utf8String Serialize(JsonElement value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer, JsonOptions);
        Write(value, writer);
        writer.Flush();
        return Utf8String.FromBytes(buffer.WrittenSpan.ToArray());
    }

    private static void Write(JsonElement value, Utf8JsonWriter writer)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var property in value.EnumerateObject())
                {
                    writer.WritePropertyName(property.Name);
                    Write(property.Value, writer);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in value.EnumerateArray()) Write(item, writer);
                writer.WriteEndArray();
                break;
            case JsonValueKind.Number:
                writer.WriteRawValue(FormatNumber(value), skipInputValidation: true);
                break;
            default:
                value.WriteTo(writer);
                break;
        }
    }

    private static string FormatNumber(JsonElement value)
    {
        var raw = value.GetRawText();
        // Preserve integer precision and numbers beyond DOUBLE's finite range.
        if (raw.IndexOfAny(['.', 'e', 'E']) < 0) return raw == "-0" ? "0" : raw;
        var number = value.GetDouble();
        if (!double.IsFinite(number)) return raw;
        var text = number.ToString("R", CultureInfo.InvariantCulture);
        var e = text.IndexOf('E');
        if (e < 0) return text.Contains('.') ? text : text + ".0";

        var exponent = int.Parse(text.AsSpan(e + 1), CultureInfo.InvariantCulture);
        var mantissa = text[..e];
        // DuckDB uses fixed notation for exponents -6 through 20.
        if (exponent < -6 || exponent >= 21)
            return mantissa + "e" + exponent.ToString(CultureInfo.InvariantCulture);
        var negative = mantissa[0] == '-';
        if (negative) mantissa = mantissa[1..];
        var dot = mantissa.IndexOf('.');
        var point = (dot < 0 ? mantissa.Length : dot) + exponent;
        var digits = mantissa.Replace(".", "");
        text = point <= 0 ? "0." + new string('0', -point) + digits
            : point >= digits.Length ? digits + new string('0', point - digits.Length) + ".0"
            : digits.Insert(point, ".");
        return negative ? "-" + text : text;
    }

}
