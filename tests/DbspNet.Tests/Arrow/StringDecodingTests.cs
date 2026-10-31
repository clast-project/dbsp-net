// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Every StringDecoding mode must produce the same VARCHAR values as Transcode — they differ only
// in how the bytes are owned (docs/design-connectors.md, G5).
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Arrow;

public class StringDecodingTests
{
    private static readonly SqlType Varchar = new SqlVarcharType(null, true);

    private static readonly string?[] Values = ["", "a", null, "héllo wörld", "日本語", null, new string('x', 5000), "🙂 emoji"];

    private static StringArray Build()
    {
        var b = new StringArray.Builder();
        foreach (var v in Values)
        {
            if (v is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append(v);
            }
        }

        return b.Build();
    }

    [Theory]
    [InlineData(StringDecoding.Copy)]
    [InlineData(StringDecoding.Arena)]
    [InlineData(StringDecoding.Alias)]
    public void Mode_DecodesTheSameValuesAsTranscode(StringDecoding mode)
    {
        var array = Build();
        for (var from = 0; from < Values.Length; from++)
        {
            var n = Values.Length - from;
            var expected = ArrowColumns.Extract(array, Varchar, from, n, StringDecoding.Transcode);
            var actual = ArrowColumns.Extract(array, Varchar, from, n, mode);
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void Copy_OwnsEachCellsBytes()
    {
        var actual = ArrowColumns.Extract(Build(), Varchar, 0, Values.Length, StringDecoding.Copy);
        var a = (Utf8String)actual[1]!;
        var b = (Utf8String)actual[3]!;
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(a.Memory, out var sa));
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(b.Memory, out var sb));
        Assert.NotSame(sa.Array, sb.Array);
        Assert.Equal(a.ByteLength, sa.Array!.Length);
    }

    private static StringArray FromBytes(params byte[][] cells)
    {
        var b = new StringArray.Builder();
        foreach (var c in cells)
        {
            b.Append(c.AsSpan());
        }

        return b.Build();
    }

    [Fact]
    public void EnsureValidUtf8_AcceptsValidText()
    {
        var a = Build();
        ArrowColumns.EnsureValidUtf8(a, 0, a.Length);
        ArrowColumns.EnsureValidUtf8(a, 3, a.Length - 3);
    }

    [Theory]
    [InlineData(new byte[] { 0x80 })]
    [InlineData(new byte[] { 0x61, 0xC3 })]
    [InlineData(new byte[] { 0xC0, 0xAF })]
    [InlineData(new byte[] { 0xED, 0xA0, 0x80 })]
    [InlineData(new byte[] { 0xFF })]
    public void EnsureValidUtf8_RejectsAMalformedCell(byte[] bad)
    {
        var a = FromBytes("ok"u8.ToArray(), bad, "ok"u8.ToArray());
        Assert.Throws<InvalidDataException>(() => ArrowColumns.EnsureValidUtf8(a, 0, 3));
    }

    [Fact]
    public void EnsureValidUtf8_RejectsCellsThatAreOnlyValidWhenJoined()
    {
        // 0xC3 0xA9 is "é": the column's bytes are valid, but neither cell is.
        var a = FromBytes("x"u8.ToArray(), [0xC3], [0xA9]);
        Assert.Throws<InvalidDataException>(() => ArrowColumns.EnsureValidUtf8(a, 0, 3));
        Assert.Throws<InvalidDataException>(() => ArrowColumns.EnsureValidUtf8(a, 2, 1));
    }
}
