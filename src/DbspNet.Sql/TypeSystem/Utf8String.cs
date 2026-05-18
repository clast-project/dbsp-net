using System.IO.Hashing;
using System.Text;

namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// Arrow-aligned UTF-8 string value: a <see cref="ReadOnlyMemory{Byte}"/>
/// of validly-encoded UTF-8 bytes. Equality and ordering are byte-wise (which
/// for valid UTF-8 is identical to code-point comparison — a designed
/// property of the encoding). The layout matches Arrow's <c>Utf8</c> /
/// <c>LargeUtf8</c> column body so values can round-trip into / out of an
/// Arrow buffer without copying.
/// </summary>
/// <remarks>
/// Construction:
/// <list type="bullet">
///   <item><see cref="Of(string)"/> — encode a .NET <see cref="string"/> as
///     UTF-8 (allocates).</item>
///   <item><see cref="FromBytes(ReadOnlyMemory{byte})"/> — wrap an existing
///     buffer (no copy, no validation; caller must guarantee valid UTF-8).
///     Used by the Arrow ingest path and any caller already holding bytes.</item>
/// </list>
/// </remarks>
public readonly struct Utf8String : IEquatable<Utf8String>, IComparable<Utf8String>, IComparable
{
    private readonly ReadOnlyMemory<byte> _bytes;

    public static Utf8String Empty => default;

    private Utf8String(ReadOnlyMemory<byte> bytes)
    {
        _bytes = bytes;
    }

    /// <summary>The raw UTF-8 byte buffer.</summary>
    public ReadOnlyMemory<byte> Memory => _bytes;

    /// <summary>The raw UTF-8 byte span.</summary>
    public ReadOnlySpan<byte> Span => _bytes.Span;

    /// <summary>Number of UTF-8 bytes (length of the storage buffer).</summary>
    public int ByteLength => _bytes.Length;

    /// <summary>
    /// Encode a .NET <see cref="string"/> as UTF-8. Allocates a fresh byte
    /// buffer; for ASCII data the result is half the byte size of the
    /// source. Use <see cref="FromBytes"/> when you already have UTF-8 bytes.
    /// </summary>
    public static Utf8String Of(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length == 0)
        {
            return Empty;
        }

        return new Utf8String(Encoding.UTF8.GetBytes(value));
    }

    /// <summary>
    /// Wrap an existing UTF-8 buffer without copying or validating. Caller
    /// must guarantee the bytes are valid UTF-8; behaviour is undefined
    /// otherwise (notably <see cref="CodePointCount"/> may report nonsense).
    /// </summary>
    public static Utf8String FromBytes(ReadOnlyMemory<byte> bytes) => new(bytes);

    /// <summary>Decode the bytes back to a .NET <see cref="string"/>.</summary>
    public string ToStringDecoded() =>
        _bytes.IsEmpty ? string.Empty : Encoding.UTF8.GetString(_bytes.Span);

    public override string ToString() => ToStringDecoded();

    /// <summary>
    /// PG-style string length: number of Unicode code points. A UTF-8
    /// continuation byte has its high two bits as <c>10</c>; non-continuation
    /// bytes mark the start of a code point.
    /// </summary>
    public int CodePointCount()
    {
        var span = _bytes.Span;
        var count = 0;
        for (var i = 0; i < span.Length; i++)
        {
            if ((span[i] & 0xC0) != 0x80)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Invariant-culture upper-case fold. Operates directly on the UTF-8
    /// byte stream — no detour through a .NET <see cref="string"/>. ASCII-
    /// only inputs take a byte-level fast path; otherwise the result is
    /// computed code-point by code-point via <see cref="Rune"/>. Returns
    /// the input unchanged (zero allocation) when no code point would
    /// fold, so already-cased strings are free.
    /// </summary>
    public Utf8String ToUpperInvariant() => FoldCase(toUpper: true);

    /// <summary>Invariant-culture lower-case fold. See <see cref="ToUpperInvariant"/>.</summary>
    public Utf8String ToLowerInvariant() => FoldCase(toUpper: false);

    private Utf8String FoldCase(bool toUpper)
    {
        if (_bytes.IsEmpty)
        {
            return this;
        }

        var src = _bytes.Span;

        // ASCII fast path. Most strings are ASCII, and ASCII case fold is
        // a one-byte-per-byte operation: no Rune machinery needed.
        var allAscii = true;
        for (var i = 0; i < src.Length; i++)
        {
            if (src[i] >= 0x80)
            {
                allAscii = false;
                break;
            }
        }

        return allAscii ? FoldAscii(toUpper) : FoldRunes(toUpper);
    }

    private Utf8String FoldAscii(bool toUpper)
    {
        var src = _bytes.Span;
        var firstChange = -1;
        for (var i = 0; i < src.Length; i++)
        {
            var b = src[i];
            var needs = toUpper
                ? b >= (byte)'a' && b <= (byte)'z'
                : b >= (byte)'A' && b <= (byte)'Z';
            if (needs)
            {
                firstChange = i;
                break;
            }
        }

        if (firstChange == -1)
        {
            return this;
        }

        var buf = new byte[src.Length];
        src[..firstChange].CopyTo(buf);
        for (var i = firstChange; i < src.Length; i++)
        {
            var b = src[i];
            buf[i] = toUpper
                ? b >= (byte)'a' && b <= (byte)'z' ? (byte)(b - 0x20) : b
                : b >= (byte)'A' && b <= (byte)'Z' ? (byte)(b + 0x20) : b;
        }

        return new Utf8String(buf);
    }

    private Utf8String FoldRunes(bool toUpper)
    {
        var src = _bytes.Span;

        // Pass 1: walk runes; sum the folded byte length and detect any
        // change. Skips allocation entirely when the input is already in
        // the target case (e.g. "café".ToLowerInvariant()).
        var outputBytes = 0;
        var anyChange = false;
        var pos = 0;
        while (pos < src.Length)
        {
            Rune.DecodeFromUtf8(src[pos..], out var rune, out var consumed);
            var folded = toUpper
                ? Rune.ToUpperInvariant(rune)
                : Rune.ToLowerInvariant(rune);
            outputBytes += folded.Utf8SequenceLength;
            if (folded != rune)
            {
                anyChange = true;
            }

            pos += consumed;
        }

        if (!anyChange)
        {
            return this;
        }

        // Pass 2: write into an exact-size buffer. Output may be longer
        // or shorter than input: e.g. U+212A (KELVIN SIGN, 3 bytes UTF-8)
        // ToLowerInvariant → 'k' (1 byte).
        var buf = new byte[outputBytes];
        var written = 0;
        pos = 0;
        while (pos < src.Length)
        {
            Rune.DecodeFromUtf8(src[pos..], out var rune, out var consumed);
            var folded = toUpper
                ? Rune.ToUpperInvariant(rune)
                : Rune.ToLowerInvariant(rune);
            written += folded.EncodeToUtf8(buf.AsSpan(written));
            pos += consumed;
        }

        return new Utf8String(buf);
    }

    /// <summary>
    /// Concatenate UTF-8 strings byte-wise. Result is the concatenation of
    /// the input buffers — no decoding pass.
    /// </summary>
    public static Utf8String Concat(ReadOnlySpan<Utf8String> parts)
    {
        var totalLen = 0;
        foreach (var p in parts)
        {
            totalLen += p.ByteLength;
        }

        if (totalLen == 0)
        {
            return Empty;
        }

        var buf = new byte[totalLen];
        var offset = 0;
        foreach (var p in parts)
        {
            p._bytes.Span.CopyTo(buf.AsSpan(offset));
            offset += p.ByteLength;
        }

        return new Utf8String(buf);
    }

    public bool Equals(Utf8String other) => _bytes.Span.SequenceEqual(other._bytes.Span);

    public override bool Equals(object? obj) => obj is Utf8String u && Equals(u);

    public override int GetHashCode()
    {
        // XxHash3 64-bit; fold to 32 bits for the int contract.
        var h = XxHash3.HashToUInt64(_bytes.Span);
        return (int)(h ^ (h >> 32));
    }

    /// <summary>
    /// Byte-wise comparison. For valid UTF-8 this is identical to
    /// code-point comparison (a designed property of the encoding) and
    /// matches the SQL standard default collation.
    /// </summary>
    public int CompareTo(Utf8String other) => _bytes.Span.SequenceCompareTo(other._bytes.Span);

    int IComparable.CompareTo(object? obj)
    {
        if (obj is null)
        {
            return 1;
        }

        if (obj is Utf8String other)
        {
            return CompareTo(other);
        }

        throw new ArgumentException($"object must be of type {nameof(Utf8String)}");
    }

    public static bool operator ==(Utf8String a, Utf8String b) => a.Equals(b);

    public static bool operator !=(Utf8String a, Utf8String b) => !a.Equals(b);

    public static bool operator <(Utf8String a, Utf8String b) => a.CompareTo(b) < 0;

    public static bool operator <=(Utf8String a, Utf8String b) => a.CompareTo(b) <= 0;

    public static bool operator >(Utf8String a, Utf8String b) => a.CompareTo(b) > 0;

    public static bool operator >=(Utf8String a, Utf8String b) => a.CompareTo(b) >= 0;
}
