// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Text;
using Clast.DatabaseDecimal.Values;
using DbspNet.Arrow;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Arrow;

/// <summary>
/// The three places that hand-rolled what Clast.DatabaseDecimal 0.4.0 now provides: Arrow decimal
/// column decode (<c>DecimalBinary</c>), CAST(VARCHAR AS DECIMAL) from UTF-8, and DECIMAL → VARCHAR
/// straight to UTF-8. Each is checked against an independent reference.
/// </summary>
public class DecimalLibraryAdoptionTests
{
    private static readonly long[] Mantissas =
        [0, 1, -1, 6789, -6789, int.MaxValue, int.MinValue, 123_456_789, -987_654_321];

    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void Extract_DecimalColumn_SignExtendsAndHonoursNullsOffsetAndFrom(int width)
    {
        // Every third row null; the array itself is sliced (ArrayData offset 2) and we read from
        // row 1 of the slice, so the decode must add both offsets to find its first byte.
        const int arrayOffset = 2;
        const int from = 1;
        var total = Mantissas.Length + arrayOffset;
        var values = new byte[total * width];
        var validity = new ArrowBuffer.BitmapBuilder();
        var expected = new List<Int128?>();
        for (var i = 0; i < total; i++)
        {
            var m = i < arrayOffset ? 4242L : Mantissas[i - arrayOffset];
            var slot = values.AsSpan(i * width, width);
            switch (width)
            {
                case 4: BinaryPrimitives.WriteInt32LittleEndian(slot, (int)m); break;
                case 8: BinaryPrimitives.WriteInt64LittleEndian(slot, m); break;
                default: BinaryPrimitives.WriteInt128LittleEndian(slot, m); break;
            }

            var isNull = i % 3 == 0;
            validity.Append(!isNull);
            if (i >= arrayOffset + from)
            {
                expected.Add(isNull ? null : (Int128)m);
            }
        }

        var length = total - arrayOffset;
        IArrowType arrowType = width switch
        {
            4 => new Decimal32Type(9, 2),
            8 => new Decimal64Type(18, 2),
            _ => new Decimal128Type(38, 2),
        };
        var data = new ArrayData(arrowType, length, nullCount: -1, offset: arrayOffset,
            [validity.Build(), new ArrowBuffer(values)]);
        IArrowArray array = width switch
        {
            4 => new Decimal32Array(data),
            8 => new Decimal64Array(data),
            _ => new Decimal128Array(data),
        };

        var cells = ArrowColumns.Extract(array, new SqlDecimalType(38, 2, Nullable: true), from, length - from);

        Assert.Equal(expected.Count, cells.Length);
        for (var i = 0; i < cells.Length; i++)
        {
            if (expected[i] is { } m)
            {
                Assert.Equal(new Decimal128(m), Assert.IsType<Decimal128>(cells[i]));
            }
            else
            {
                Assert.Null(cells[i]);
            }
        }
    }

    [Theory]
    [InlineData("0", 10, 2)]
    [InlineData("-0.01", 10, 2)]
    [InlineData("123.45", 10, 2)]
    [InlineData("-98765.4321", 18, 4)]
    [InlineData("42", 5, 0)]
    [InlineData("99999999999999999999999999999999999999", 38, 0)]
    [InlineData("-0.99999999999999999999999999999999999999", 38, 38)]
    public void ToUtf8_MatchesTheStringFormatter(string text, int precision, int scale)
    {
        var type = new DecimalType((byte)precision, (byte)scale);
        var value = DecimalText.ParseDecimal128(text.AsSpan(), type);

        Assert.Equal(DecimalText.Format(value, type), DecimalRuntime.ToUtf8(value, type).ToStringDecoded());
    }

    [Fact]
    public void ParseDecimal128Utf8_ParsesInputLongerThanTheOldStackBuffer()
    {
        // The hand-rolled decode switched to a heap string past 128 bytes; the library bounds its
        // own buffer. 200 leading zeros keep the value small.
        var type = new DecimalType(10, 2);
        var text = new string('0', 200) + "123.45";

        var parsed = SqlCasts.ParseDecimal128Utf8(Utf8String.Of(text), type, DecimalRounding.HalfUp);

        Assert.Equal(new Decimal128(12345), parsed);
    }

    [Theory]
    [InlineData("123.455", "12346")] // half-up at the dropped digit
    [InlineData("-123.455", "-12346")]
    [InlineData("7", "700")]
    public void ParseDecimal128Utf8_MatchesTheCharParser(string text, string expectedMantissa)
    {
        var type = new DecimalType(10, 2);

        var parsed = SqlCasts.ParseDecimal128Utf8(Utf8String.Of(text), type, DecimalRounding.HalfUp);

        Assert.Equal(DecimalText.ParseDecimal128(text.AsSpan(), type, DecimalRounding.HalfUp), parsed);
        Assert.Equal(Int128.Parse(expectedMantissa, System.Globalization.CultureInfo.InvariantCulture), parsed.Mantissa);
    }
}
