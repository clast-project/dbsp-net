// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
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
}
