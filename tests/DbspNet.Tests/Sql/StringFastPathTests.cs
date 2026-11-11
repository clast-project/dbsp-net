// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using System.Text.RegularExpressions;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The byte-level fast paths in <c>TRIM</c> and <c>SUBSTRING</c> (docs/design-tpcdi-batch1.md §4d)
/// agree with the code-point walks they skip: <c>TRIM</c> with an ASCII character set on any bytes,
/// valid UTF-8 or not, <c>SUBSTRING</c> on ASCII and multi-byte strings at every window, and the
/// VARCHAR casts that parse UTF-8 bytes directly, against the string parsers they bypass.
/// </summary>
public class StringFastPathTests
{
    // Fragments to build strings from: trim candidates, multi-byte code points (2, 3 and 4 bytes,
    // including U+2003, a non-ASCII space), and malformed bytes (lone continuation, truncated lead,
    // a byte never valid in UTF-8).
    private static readonly byte[][] Fragments =
    [
        " "u8.ToArray(), "a"u8.ToArray(), "x"u8.ToArray(), "-"u8.ToArray(), "0"u8.ToArray(),
        "é"u8.ToArray(), " "u8.ToArray(), "😀"u8.ToArray(),
        [0x80], [0xC3], [0xE2, 0x82], [0xFF],
    ];

    private static readonly string?[] CharSets = [null, " ", "ax", " -0", "x", "a x-"];

    private static Utf8String RandomString(Random rng, bool validOnly)
    {
        var bytes = new List<byte>();
        var n = rng.Next(0, 9);
        for (var i = 0; i < n; i++)
        {
            var f = Fragments[rng.Next(validOnly ? 8 : Fragments.Length)];
            bytes.AddRange(f);
        }

        return Utf8String.FromBytes(bytes.ToArray());
    }

    [Fact]
    public void Trim_WithAnAsciiSet_MatchesTheDecodingTrim_OnAnyBytes()
    {
        var rng = new Random(20261111);
        for (var i = 0; i < 20_000; i++)
        {
            var s = RandomString(rng, validOnly: false);
            var set = CharSets[rng.Next(CharSets.Length)];
            Utf8String? chars = set is null ? null : Utf8String.Of(set);
            var side = rng.Next(3);
            var fast = SqlBuiltinRuntime.TrimCore(s, chars, side);
            var slow = SqlBuiltinRuntime.TrimDecoded(s, chars, side);
            Assert.True(
                fast.Span.SequenceEqual(slow.Span),
                $"TRIM side {side} chars '{set}' of [{Convert.ToHexString(s.Span)}]: " +
                $"{Convert.ToHexString(fast.Span)} vs {Convert.ToHexString(slow.Span)}");
        }
    }

    [Fact]
    public void Trim_LeavesAnUntrimmedString_AsTheSameValue()
    {
        var s = Utf8String.Of("abc");
        Assert.True(SqlBuiltinRuntime.TrimCore(s, null, 2).Memory.Equals(s.Memory));
    }

    [Fact]
    public void Substring_MatchesACodePointReference_OnAsciiAndMultiByteStrings()
    {
        var rng = new Random(11112026);
        for (var i = 0; i < 20_000; i++)
        {
            var s = RandomString(rng, validOnly: true);
            var start = rng.Next(-3, 12);
            int? length = rng.Next(4) == 0 ? null : rng.Next(0, 12);
            var actual = SqlBuiltinRuntime.SubstringCore(s, start, length).ToStringDecoded();
            Assert.Equal(Reference(s.ToStringDecoded(), start, length), actual);
        }

        // SQL SUBSTRING over code points: positions start .. start+length-1, clipped to 1..n.
        static string Reference(string value, int start, int? length)
        {
            var runes = value.EnumerateRunes().ToList();
            var first = Math.Max(1, start);
            var lastExclusive = length is null ? runes.Count + 1 : Math.Min(start + length.Value, runes.Count + 1);
            var sb = new StringBuilder();
            for (var p = first; p < lastExclusive; p++)
            {
                sb.Append(runes[p - 1].ToString());
            }

            return sb.ToString();
        }
    }

    private static readonly byte[][] NumberFragments =
    [
        "0"u8.ToArray(), "1"u8.ToArray(), "7"u8.ToArray(), "9"u8.ToArray(), "-"u8.ToArray(), "+"u8.ToArray(),
        "."u8.ToArray(), "e"u8.ToArray(), "E"u8.ToArray(), ","u8.ToArray(), " "u8.ToArray(), "a"u8.ToArray(),
        "Infinity"u8.ToArray(), "NaN"u8.ToArray(), "\t"u8.ToArray(), "٣"u8.ToArray(), " "u8.ToArray(),
        [0xFF], [0xC3], "99999999999"u8.ToArray(),
    ];

    private static Utf8String RandomNumber(Random rng)
    {
        var bytes = new List<byte>();
        var n = rng.Next(0, 7);
        for (var i = 0; i < n; i++)
        {
            // Mostly digits, so plenty of inputs parse.
            bytes.AddRange(NumberFragments[rng.Next(2) == 0 ? rng.Next(4) : rng.Next(NumberFragments.Length)]);
        }

        return Utf8String.FromBytes(bytes.ToArray());
    }

    // The value, or the exception type, a parse gives.
    private static string Outcome<T>(Func<T> parse)
    {
        try
        {
            return "ok:" + Convert.ToString(parse(), System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (Exception e)
        {
            return "throws:" + e.GetType().Name;
        }
    }

    [Fact]
    public void NumericCasts_FromUtf8_MatchTheStringParsers()
    {
        var rng = new Random(4242);
        var parsed = 0;
        for (var i = 0; i < 40_000; i++)
        {
            var s = RandomNumber(rng);
            var text = s.ToStringDecoded();
            var hex = Convert.ToHexString(s.Span);
            var int32 = Outcome(() => SqlCasts.ParseInt32Utf8(s));
            Assert.True(int32 == Outcome(() => SqlCasts.ParseInt32(text)), $"INT [{hex}] {int32}");
            Assert.True(Outcome(() => SqlCasts.ParseInt64Utf8(s)) == Outcome(() => SqlCasts.ParseInt64(text)), $"BIGINT [{hex}]");
            Assert.True(Outcome(() => SqlCasts.ParseSingleUtf8(s)) == Outcome(() => SqlCasts.ParseSingle(text)), $"REAL [{hex}]");
            var dbl = Outcome(() => SqlCasts.ParseDoubleUtf8(s));
            Assert.True(dbl == Outcome(() => SqlCasts.ParseDouble(text)), $"DOUBLE [{hex}] {dbl}");
            parsed += dbl.StartsWith("ok:", StringComparison.Ordinal) ? 1 : 0;
        }

        Assert.True(parsed > 5_000, $"only {parsed} inputs parsed");
    }

    [Fact]
    public void DateCast_FromUtf8_MatchesTheStringParser()
    {
        var rng = new Random(77);
        string[] samples = ["2020-02-29", "2021-02-29", "0000-01-01", "0001-01-01", "9999-12-31", "2020-13-01", "2020-00-10",
            "2020-1-05", " 2020-01-05", "2020-01-05 ", "2020/01/05", "20200105", "", "2020-01-0x", "２020-01-05"];
        var inputs = samples.Select(Utf8String.Of).ToList();
        for (var i = 0; i < 20_000; i++)
        {
            var y = rng.Next(0, 10_000);
            var m = rng.Next(0, 14);
            var d = rng.Next(0, 33);
            inputs.Add(Utf8String.Of($"{y:D4}-{m:D2}-{d:D2}"));
        }

        inputs.Add(Utf8String.FromBytes(new byte[] { 0x32, 0x30, 0x32, 0x30, 0x2D, 0x30, 0x31, 0x2D, 0x30, 0xFF }));
        var valid = 0;
        foreach (var s in inputs)
        {
            var fast = Outcome(() => SqlCasts.ParseDateUtf8(s));
            Assert.True(fast == Outcome(() => Date32.Parse(s.ToStringDecoded())), $"DATE '{s.ToStringDecoded()}' {fast}");
            valid += fast.StartsWith("ok:", StringComparison.Ordinal) ? 1 : 0;
        }

        Assert.True(valid > 5_000, $"only {valid} dates parsed");
    }
    [Fact]
    public void NumberAndBoolToVarchar_FormatTheSameBytesAsTheStringPath()
    {
        var rng = new Random(9001);
        static void Same(Utf8String fast, string slow, string what) =>
            Assert.True(fast.Span.SequenceEqual(Utf8String.Of(slow).Span), $"{what}: {fast.ToStringDecoded()} vs {slow}");

        int[] ints = [0, -1, 1, int.MinValue, int.MaxValue];
        long[] longs = [0, -1, long.MinValue, long.MaxValue];
        double[] doubles = [0.0, -0.0, double.NaN, double.PositiveInfinity, double.NegativeInfinity, double.Epsilon,
            double.MaxValue, double.MinValue, 0.1, 1e21, 1e-7, 123456789.125];
        float[] floats = [0f, -0f, float.NaN, float.PositiveInfinity, float.Epsilon, float.MaxValue, 0.1f, 3e-7f];
        foreach (var v in ints.Concat(Enumerable.Range(0, 5000).Select(_ => rng.Next(int.MinValue, int.MaxValue))))
        {
            Same(SqlCasts.NumericToUtf8(v), SqlCasts.NumericToString(v), "INT");
        }

        foreach (var v in longs.Concat(Enumerable.Range(0, 5000).Select(_ => rng.NextInt64(long.MinValue, long.MaxValue))))
        {
            Same(SqlCasts.NumericToUtf8(v), SqlCasts.NumericToString(v), "BIGINT");
        }

        foreach (var v in doubles.Concat(Enumerable.Range(0, 5000).Select(_ => BitConverter.Int64BitsToDouble(rng.NextInt64()))))
        {
            Same(SqlCasts.NumericToUtf8(v), SqlCasts.NumericToString(v), "DOUBLE");
        }

        foreach (var v in floats.Concat(Enumerable.Range(0, 5000).Select(_ => BitConverter.Int32BitsToSingle(rng.Next(int.MinValue, int.MaxValue)))))
        {
            Same(SqlCasts.NumericToUtf8(v), SqlCasts.NumericToString(v), "REAL");
        }

        Same(SqlCasts.BoolToUtf8(true), true.ToString(), "BOOLEAN");
        Same(SqlCasts.BoolToUtf8(false), false.ToString(), "BOOLEAN");
    }

    [Fact]
    public void DecimalCast_FromUtf8_MatchesTheStringParser()
    {
        var rng = new Random(31337);
        byte[][] fragments =
        [
            "0"u8.ToArray(), "5"u8.ToArray(), "9"u8.ToArray(), "-"u8.ToArray(), "."u8.ToArray(), "e"u8.ToArray(),
            " "u8.ToArray(), "x"u8.ToArray(), "\u0663"u8.ToArray(), [0xFF], "12345678901234567890"u8.ToArray(),
        ];
        var types = new[] { (10, 2), (18, 0), (38, 10), (5, 5) }
            .Select(t => Clast.DatabaseDecimal.DecimalType.Numeric((byte)t.Item1, (byte)t.Item2)).ToArray();
        var parsed = 0;
        for (var i = 0; i < 20_000; i++)
        {
            var bytes = new List<byte>();
            var n = rng.Next(0, i % 50 == 0 ? 20 : 7); // now and then past the 128-byte stack buffer
            for (var k = 0; k < n; k++)
            {
                bytes.AddRange(fragments[rng.Next(2) == 0 ? rng.Next(3) : rng.Next(fragments.Length)]);
            }

            var s = Utf8String.FromBytes(bytes.ToArray());
            var type = types[rng.Next(types.Length)];
            var fast = Outcome(() => SqlCasts.ParseDecimal128Utf8(s, type, Clast.DatabaseDecimal.DecimalRounding.HalfUp));
            var slow = Outcome(() => Clast.DatabaseDecimal.Text.DecimalText.ParseDecimal128(
                s.ToStringDecoded().AsSpan(), type, Clast.DatabaseDecimal.DecimalRounding.HalfUp));
            Assert.True(fast == slow, $"DECIMAL [{Convert.ToHexString(s.Span)}] {fast} vs {slow}");
            parsed += fast.StartsWith("ok:", StringComparison.Ordinal) ? 1 : 0;
        }

        Assert.True(parsed > 2_000, $"only {parsed} decimals parsed");
    }
    /// <summary>
    /// JSON_EXTRACT_STRING parses valid UTF-8 from the bytes and returns unescaped strings as their raw
    /// bytes. Against the old path (decode to a string, parse, GetString): the same value or the same
    /// exception type, over escapes, non-ASCII text, malformed bytes, duplicate keys and nesting.
    /// </summary>
    [Fact]
    public void JsonExtract_FromUtf8_MatchesTheStringPath()
    {
        var rng = new Random(2718);
        string[] strings = ["\"plain\"", "\"\"", "\"a\\\"b\"", "\"tab\\tnew\\nline\"", "\"\\u00e9t\\u00e9\"", "\"\\ud83d\\ude00\"",
            "\"été 😀\"", "\"back\\\\slash\"", "\"\\/\""];
        string[] others = ["null", "true", "false", "12", "-0.5e3", "{\"a\":\"in\"}", "[1,\"x\"]"];
        string[] paths = ["a", "b", "a.a", "b.a"];
        var compared = 0;
        for (var i = 0; i < 20_000; i++)
        {
            string Value(int depth) => depth < 2 && rng.Next(4) == 0
                ? "{" + string.Join(",", Enumerable.Range(0, rng.Next(1, 3)).Select(_ => $"\"{(rng.Next(2) == 0 ? "a" : "b")}\":{Value(depth + 1)}")) + "}"
                : rng.Next(2) == 0 ? strings[rng.Next(strings.Length)] : others[rng.Next(others.Length)];

            var bytes = new List<byte>(System.Text.Encoding.UTF8.GetBytes(Value(0)));
            if (rng.Next(10) == 0)
            {
                bytes.Insert(rng.Next(bytes.Count + 1), 0xFF); // malformed UTF-8 somewhere
            }

            var source = Utf8String.FromBytes(bytes.ToArray());
            var path = paths[rng.Next(paths.Length)].Split('.');
            var fast = Outcome(() => (JsonExtractStringFunction.Extract(source, path) as Utf8String?) is { } u ? Convert.ToHexString(u.Span) : "NULL");
            var slow = Outcome(() => Reference(source, path));
            if (slow == "ok:skip")
            {
                continue;
            }

            compared++;
            Assert.True(fast == slow, $"JSON [{source.ToStringDecoded()}] {string.Join('.', path)}: {fast} vs {slow}");
        }

        Assert.True(compared > 5_000, $"only {compared} compared");

        // The old path for the kinds the change touches (string, null, booleans, missing); others "skip".
        static string Reference(Utf8String source, string[] path)
        {
            using var document = System.Text.Json.JsonDocument.Parse(source.ToStringDecoded());
            var value = document.RootElement;
            foreach (var key in path)
            {
                if (value.ValueKind != System.Text.Json.JsonValueKind.Object) return "NULL";
                var found = false;
                foreach (var property in value.EnumerateObject())
                {
                    if (!property.NameEquals(key)) continue;
                    value = property.Value;
                    found = true;
                    break;
                }

                if (!found) return "NULL";
            }

            return value.ValueKind switch
            {
                System.Text.Json.JsonValueKind.Null => "NULL",
                System.Text.Json.JsonValueKind.String => Convert.ToHexString(Utf8String.Of(value.GetString()!).Span),
                System.Text.Json.JsonValueKind.True => Convert.ToHexString("true"u8),
                System.Text.Json.JsonValueKind.False => Convert.ToHexString("false"u8),
                _ => "skip",
            };
        }
    }
    /// <summary>
    /// A constant LIKE of literals and '%' matches on bytes; everything else decodes into a buffer for
    /// the regex. Both against the regex on the decoded string, for patterns with '%', '_', escapes,
    /// multi-byte and astral characters, and values that are malformed or past the stack buffer.
    /// </summary>
    [Fact]
    public void Like_OnBytes_AndRegexOnSpans_MatchTheStringRegex()
    {
        var rng = new Random(1618);
        string[] patternParts = ["a", "b", "ab", "é", "😀", "%", "%", "_", "\\%", "\\_", "\\\\", "x", "\uFFFD"];
        byte[][] valueParts = ["a"u8.ToArray(), "b"u8.ToArray(), "é"u8.ToArray(), "😀"u8.ToArray(), "%"u8.ToArray(),
            "_"u8.ToArray(), "\\"u8.ToArray(), "x"u8.ToArray(), [0xFF], [0xC3]];
        var literal = 0;
        for (var i = 0; i < 4_000; i++)
        {
            var pattern = string.Concat(Enumerable.Range(0, rng.Next(0, 6)).Select(_ => patternParts[rng.Next(patternParts.Length)]));
            Regex regex;
            try
            {
                regex = SqlPatternMatch.Compile(pattern, SqlPatternMatch.DefaultEscape, caseInsensitive: false, similar: false);
            }
            catch (InvalidOperationException)
            {
                Assert.Null(SqlPatternMatch.LiteralLike(pattern, SqlPatternMatch.DefaultEscape, null!));
                continue;
            }

            var bytesMatcher = SqlPatternMatch.LiteralLike(pattern, SqlPatternMatch.DefaultEscape, regex);
            literal += bytesMatcher is null ? 0 : 1;
            for (var j = 0; j < 10; j++)
            {
                var bytes = new List<byte>();
                var n = rng.Next(0, j == 0 ? 120 : 6); // one long value per pattern: past the 256-byte buffer
                for (var k = 0; k < n; k++)
                {
                    bytes.AddRange(valueParts[rng.Next(rng.Next(3) == 0 ? valueParts.Length : valueParts.Length - 2)]);
                }

                var value = Utf8String.FromBytes(bytes.ToArray());
                var expected = regex.IsMatch(value.ToStringDecoded());
                Assert.Equal(expected, SqlBuiltinRuntime.IsMatchDecoded(regex, value));
                if (bytesMatcher is not null)
                {
                    Assert.True(
                        expected == bytesMatcher.IsMatch(value),
                        $"LIKE '{pattern}' on [{Convert.ToHexString(value.Span)}]: regex {expected}");
                }
            }
        }

        Assert.True(literal > 1_000, $"only {literal} literal patterns");
    }
    /// <summary>Middle pieces must be found in order without reusing bytes, and the first and last
    /// pieces may not overlap.</summary>
    [Theory]
    [InlineData("%a%a%", "a", false)]
    [InlineData("%a%a%", "aa", true)]
    [InlineData("%ab%b%", "ab", false)]
    [InlineData("%ab%b%", "abb", true)]
    [InlineData("%b%a%", "ab", false)]
    [InlineData("a%a", "a", false)]
    [InlineData("a%a", "aa", true)]
    [InlineData("ab%bc", "abc", false)]
    [InlineData("%😀%😀%", "x😀y", false)]
    [InlineData("%😀%😀%", "😀😀", true)]
    [InlineData("%\uFFFD%", "a\uFFFDb", true)]
    public void Like_OnBytes_RepeatedAndOverlappingPieces(string pattern, string value, bool expected)
    {
        var regex = SqlPatternMatch.Compile(pattern, SqlPatternMatch.DefaultEscape, caseInsensitive: false, similar: false);
        Assert.Equal(expected, regex.IsMatch(value));
        Assert.Equal(expected, SqlPatternMatch.LiteralLike(pattern, SqlPatternMatch.DefaultEscape, regex)!.IsMatch(Utf8String.Of(value)));
    }
    /// <summary>A pattern with U+FFFD matches a malformed value's bad bytes, as the regex sees them.</summary>
    [Fact]
    public void Like_WithAReplacementChar_MatchesMalformedBytes()
    {
        var regex = SqlPatternMatch.Compile("a%�", SqlPatternMatch.DefaultEscape, caseInsensitive: false, similar: false);
        var value = Utf8String.FromBytes(new byte[] { 0x61, 0x62, 0xFF });
        Assert.Matches(regex, value.ToStringDecoded());
        Assert.True(SqlPatternMatch.LiteralLike("a%�", SqlPatternMatch.DefaultEscape, regex)!.IsMatch(value));
    }
}
