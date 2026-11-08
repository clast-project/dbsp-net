// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Globalization;
using System.Security.Cryptography;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// A per-thread UTF-8 buffer that a <c>CONCAT_WS</c> with a literal separator writes its arguments' text
/// into (docs/design-bounded-memory.md §5.36). A <c>CAST(x AS TEXT)</c> argument of a number, date, or
/// boolean formats straight into it, as the cast would format it, rather than building a .NET string,
/// its UTF-8 copy and a <see cref="Utf8String"/> per argument; under <c>MD5</c> the concatenation is
/// hashed where it lies, so only the 32-byte digest is allocated. Extended TPC-DS's surrogate keys,
/// <c>MD5(CONCAT_WS('-', COALESCE(TRY_CAST(c AS TEXT), ''), ...))</c> over eleven columns, allocated
/// ~80% of their projection in those casts.
/// </summary>
internal sealed class Utf8TextBuffer
{
    [ThreadStatic]
    private static Utf8TextBuffer? t_free;

    private Utf8TextBuffer? _next;
    private byte[] _bytes = new byte[256];
    private int _length;
    private int _items;

    /// <summary>A cleared buffer: this thread's free one, or a new one (a nested <c>CONCAT_WS</c> takes another).</summary>
    public static Utf8TextBuffer Rent()
    {
        var b = t_free;
        if (b is null)
        {
            return new Utf8TextBuffer();
        }

        t_free = b._next;
        b._next = null;
        b._length = 0;
        b._items = 0;
        return b;
    }

    /// <summary>Give the buffer back to this thread (one that grew past 64 KiB is shrunk).</summary>
    public void Return()
    {
        if (_bytes.Length > 1 << 16)
        {
            _bytes = new byte[256];
        }

        _next = t_free;
        t_free = this;
    }

    /// <summary>Begin an argument: the separator, if an argument was kept before. Returns the mark to roll back to.</summary>
    public int Open(Utf8String separator)
    {
        var mark = _length;
        if (_items > 0)
        {
            Write(separator.Span);
        }

        return mark;
    }

    /// <summary>End an argument: kept, or (NULL) rolled back with its separator.</summary>
    public void Close(int mark, bool kept)
    {
        if (kept)
        {
            _items++;
        }
        else
        {
            _length = mark;
        }
    }

    /// <summary>A string value's bytes; false (nothing written) for NULL, as <c>CONCAT_WS</c> skips it.</summary>
    public bool AppendUtf8(object? value)
    {
        if (value is not Utf8String u)
        {
            return false;
        }

        Write(u.Span);
        return true;
    }

    /// <summary>
    /// <c>CAST(value AS TEXT)</c>'s text, as the cast formats it (invariant culture; dates <c>yyyy-MM-dd</c>;
    /// booleans <c>True</c>/<c>False</c>); false for NULL. A <paramref name="safe"/> cast (<c>TRY_CAST</c>)
    /// that fails is NULL, as the cast's own handler makes it.
    /// </summary>
    public bool AppendCast(object? value, bool safe)
    {
        var mark = _length;
        try
        {
            switch (value)
            {
                case null:
                    return false;
                case Utf8String u:
                    Write(u.Span);
                    return true;
                case int i:
                    Format(i, default);
                    return true;
                case long l:
                    Format(l, default);
                    return true;
                case float f:
                    Format(f, default);
                    return true;
                case double d:
                    Format(d, default);
                    return true;
                case bool b:
                    Write(b ? "True"u8 : "False"u8);
                    return true;
                case Date32 date:
                    Format(date.ToDateOnly(), "yyyy-MM-dd");
                    return true;
                default:
                    throw new InvalidOperationException($"no direct text for {value.GetType()}");
            }
        }
        catch (Exception e) when (safe && e is FormatException or OverflowException or ArgumentException)
        {
            _length = mark;
            return false;
        }
    }

    /// <summary>The text written, as a new string.</summary>
    public Utf8String ToUtf8String() =>
        _length == 0 ? Utf8String.Empty : Utf8String.FromBytes(_bytes.AsSpan(0, _length).ToArray());

    /// <summary>The 32-character lowercase hex MD5 of the text written (<see cref="SqlBuiltinRuntime.Md5"/>'s digest).</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security", "CA5351:Do Not Use Broken Cryptographic Algorithms",
        Justification = "SQL MD5() is a deterministic content hash (dbt surrogate keys), not a security primitive.")]
    public Utf8String Md5Hex()
    {
        Span<byte> digest = stackalloc byte[16];
        MD5.HashData(_bytes.AsSpan(0, _length), digest);
        return SqlBuiltinRuntime.HexOf(digest);
    }

    private void Format<T>(T value, ReadOnlySpan<char> format)
        where T : IUtf8SpanFormattable
    {
        int written;
        while (!value.TryFormat(_bytes.AsSpan(_length), out written, format, CultureInfo.InvariantCulture))
        {
            Grow(_bytes.Length);
        }

        _length += written;
    }

    private void Write(ReadOnlySpan<byte> bytes)
    {
        if (_length + bytes.Length > _bytes.Length)
        {
            Grow(bytes.Length);
        }

        bytes.CopyTo(_bytes.AsSpan(_length));
        _length += bytes.Length;
    }

    private void Grow(int atLeast) =>
        Array.Resize(ref _bytes, Math.Max(_bytes.Length * 2, _length + atLeast));
}
