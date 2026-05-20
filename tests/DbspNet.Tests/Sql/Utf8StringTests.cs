// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class Utf8StringTests
{
    // ---- Construction / round-trip ----

    [Fact]
    public void EmptyAndDefaultAreEqual()
    {
        Assert.Equal(Utf8String.Empty, default);
        Assert.Equal(0, Utf8String.Empty.ByteLength);
    }

    [Fact]
    public void RoundTripsAsciiString()
    {
        var u = Utf8String.Of("hello");
        Assert.Equal(5, u.ByteLength);
        Assert.Equal("hello", u.ToStringDecoded());
        Assert.Equal("hello", u.ToString());
    }

    [Fact]
    public void RoundTripsMultibyteString()
    {
        // "café" is c-a-f-é where é is U+00E9 → 0xC3 0xA9 in UTF-8 (2 bytes).
        var u = Utf8String.Of("café");
        Assert.Equal(5, u.ByteLength);
        Assert.Equal("café", u.ToStringDecoded());
    }

    [Fact]
    public void RoundTripsBmpAndAstral()
    {
        // "🎉" is U+1F389 → 4 bytes in UTF-8 (surrogate pair in UTF-16).
        var u = Utf8String.Of("a🎉b");
        Assert.Equal(6, u.ByteLength);
        Assert.Equal("a🎉b", u.ToStringDecoded());
    }

    [Fact]
    public void FromBytesIsZeroCopyAndDoesNotValidate()
    {
        var bytes = new byte[] { 0x68, 0x69 };  // "hi"
        var u = Utf8String.FromBytes(bytes);
        Assert.Equal(2, u.ByteLength);
        Assert.Equal("hi", u.ToStringDecoded());
    }

    // ---- Equality / hashing ----

    [Fact]
    public void EqualValuesShareHash()
    {
        var a = Utf8String.Of("hello");
        var b = Utf8String.Of("hello");
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.True(a == b);
        Assert.False(a != b);
    }

    [Fact]
    public void DifferentValuesUnequal()
    {
        Assert.NotEqual(Utf8String.Of("hello"), Utf8String.Of("world"));
        Assert.NotEqual(Utf8String.Of("a"), Utf8String.Of("b"));
    }

    [Fact]
    public void HashesAreStableForSameInput()
    {
        // XxHash3 is deterministic — same bytes always produce same hash.
        var h1 = Utf8String.Of("the quick brown fox").GetHashCode();
        var h2 = Utf8String.Of("the quick brown fox").GetHashCode();
        Assert.Equal(h1, h2);
    }

    [Fact]
    public void UsableAsDictionaryKey()
    {
        var dict = new Dictionary<Utf8String, int>
        {
            [Utf8String.Of("alice")] = 1,
            [Utf8String.Of("bob")] = 2,
        };

        Assert.Equal(1, dict[Utf8String.Of("alice")]);
        Assert.Equal(2, dict[Utf8String.Of("bob")]);
        Assert.False(dict.ContainsKey(Utf8String.Of("charlie")));
    }

    // ---- Ordering ----

    [Fact]
    public void OrdersByteWise()
    {
        // For valid UTF-8 byte-wise == code-point order. ASCII case here.
        Assert.True(Utf8String.Of("abc") < Utf8String.Of("abd"));
        Assert.True(Utf8String.Of("ab") < Utf8String.Of("abc"));
        Assert.True(Utf8String.Of("abc").CompareTo(Utf8String.Of("abc")) == 0);
    }

    [Fact]
    public void OrderingMatchesCodepointForMultibyte()
    {
        // 'a' (U+0061) < 'é' (U+00E9). UTF-8 encoding preserves this.
        Assert.True(Utf8String.Of("a") < Utf8String.Of("é"));
    }

    // ---- Code-point counting (PG LENGTH semantics) ----

    [Fact]
    public void CodePointCountForAscii()
    {
        Assert.Equal(0, Utf8String.Empty.CodePointCount());
        Assert.Equal(5, Utf8String.Of("hello").CodePointCount());
    }

    [Fact]
    public void CodePointCountForMultibyte()
    {
        // "café" is 4 code points but 5 bytes.
        Assert.Equal(4, Utf8String.Of("café").CodePointCount());
        Assert.Equal(5, Utf8String.Of("café").ByteLength);
    }

    [Fact]
    public void CodePointCountForAstralCharacter()
    {
        // "🎉" is one code point (U+1F389), 4 bytes in UTF-8.
        Assert.Equal(1, Utf8String.Of("🎉").CodePointCount());
        Assert.Equal(4, Utf8String.Of("🎉").ByteLength);
    }

    // ---- Concat ----

    [Fact]
    public void ConcatProducesByteWiseConcatenation()
    {
        var result = Utf8String.Concat([
            Utf8String.Of("abc"),
            Utf8String.Of("def"),
            Utf8String.Of("ghi"),
        ]);
        Assert.Equal("abcdefghi", result.ToStringDecoded());
    }

    [Fact]
    public void ConcatEmptyArrayReturnsEmpty()
    {
        var result = Utf8String.Concat(ReadOnlySpan<Utf8String>.Empty);
        Assert.Equal(Utf8String.Empty, result);
    }

    [Fact]
    public void ConcatPreservesMultibyteEncoding()
    {
        var result = Utf8String.Concat([Utf8String.Of("café"), Utf8String.Of("⚡")]);
        Assert.Equal("café⚡", result.ToStringDecoded());
    }

    // ---- Case folding (native UTF-8) ----

    [Fact]
    public void ToUpperInvariant_AsciiBasic()
    {
        Assert.Equal(Utf8String.Of("HELLO"), Utf8String.Of("hello").ToUpperInvariant());
        Assert.Equal(Utf8String.Of("HELLO"), Utf8String.Of("Hello").ToUpperInvariant());
    }

    [Fact]
    public void ToLowerInvariant_AsciiBasic()
    {
        Assert.Equal(Utf8String.Of("world"), Utf8String.Of("WORLD").ToLowerInvariant());
        Assert.Equal(Utf8String.Of("world"), Utf8String.Of("World").ToLowerInvariant());
    }

    [Fact]
    public void ToUpperInvariant_PreservesNonLetters()
    {
        Assert.Equal(
            Utf8String.Of("ABC 123 XYZ-456!"),
            Utf8String.Of("abc 123 xyz-456!").ToUpperInvariant());
    }

    [Fact]
    public void ToUpperInvariant_AlreadyUpperCase_IsZeroAlloc()
    {
        // The contract: when no fold is needed, return the same backing
        // bytes. Verify by checking that the underlying ReadOnlyMemory
        // identity is preserved (same buffer, same offset, same length).
        var original = Utf8String.Of("HELLO");
        var folded = original.ToUpperInvariant();
        // Same Memory reference shape — strict equality on the wrapper.
        Assert.True(folded.Memory.Equals(original.Memory));
    }

    [Fact]
    public void ToLowerInvariant_AlreadyLowerCase_IsZeroAlloc()
    {
        var original = Utf8String.Of("café");
        var folded = original.ToLowerInvariant();
        Assert.True(folded.Memory.Equals(original.Memory));
    }

    [Fact]
    public void ToUpperInvariant_Empty()
    {
        Assert.Equal(Utf8String.Empty, Utf8String.Empty.ToUpperInvariant());
        Assert.Equal(Utf8String.Empty, Utf8String.Of("").ToUpperInvariant());
    }

    [Fact]
    public void CaseFold_Latin1Multibyte()
    {
        // 'é' (U+00E9, 2 bytes) ↔ 'É' (U+00C9, 2 bytes) — same length.
        Assert.Equal(Utf8String.Of("CAFÉ"), Utf8String.Of("café").ToUpperInvariant());
        Assert.Equal(Utf8String.Of("éclair"), Utf8String.Of("ÉCLAIR").ToLowerInvariant());
    }

    [Fact]
    public void CaseFold_MixedAsciiAndMultibyte()
    {
        Assert.Equal(Utf8String.Of("HÉLLO"), Utf8String.Of("héllo").ToUpperInvariant());
    }

    [Fact]
    public void ToLowerInvariant_KelvinSign_ShrinksByteCount()
    {
        // U+212A KELVIN SIGN: 3 bytes UTF-8 ('\xE2\x84\xAA').
        // Invariant ToLower → 'k' (U+006B, 1 byte). Output is shorter.
        var kelvin = Utf8String.Of("K");
        Assert.Equal(3, kelvin.ByteLength);

        var lower = kelvin.ToLowerInvariant();
        Assert.Equal(1, lower.ByteLength);
        Assert.Equal(Utf8String.Of("k"), lower);
    }

    [Fact]
    public void CaseFold_IsIdempotent()
    {
        var s = Utf8String.Of("Hello, café");
        var upper = s.ToUpperInvariant();
        Assert.Equal(upper, upper.ToUpperInvariant());

        var lower = s.ToLowerInvariant();
        Assert.Equal(lower, lower.ToLowerInvariant());
    }

    [Fact]
    public void CaseFold_AstralPlaneUntouched()
    {
        // 🎉 (U+1F389) has no case mapping — round-trips unchanged.
        var party = Utf8String.Of("a🎉b");
        Assert.Equal(Utf8String.Of("A🎉B"), party.ToUpperInvariant());
    }

    // ---- Codec smoke test ----

    [Fact]
    public void Utf8StringInDictionaryWithStringField_RoundTrips()
    {
        // Sanity: a Utf8String key works in a Dictionary just like string would.
        var dict = new Dictionary<(int, Utf8String), int>();
        dict[(1, Utf8String.Of("hello"))] = 10;
        dict[(2, Utf8String.Of("hello"))] = 20;

        Assert.Equal(10, dict[(1, Utf8String.Of("hello"))]);
        Assert.Equal(20, dict[(2, Utf8String.Of("hello"))]);
    }
}
