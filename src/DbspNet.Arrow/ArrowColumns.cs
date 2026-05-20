// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Arrow;

/// <summary>
/// Column-major boundary helpers. Each per-type method walks an entire
/// Arrow column (or DbspNet column buffer) in a single tight typed loop —
/// type-dispatch is hoisted out of the row loop, so the JIT can inline the
/// concrete <c>IArrowArray</c> reads / <c>IArrowArrayBuilder</c> writes
/// without per-cell virtual dispatch.
/// </summary>
internal static class ArrowColumns
{
    // ---- Extraction: Arrow column → DbspNet typed object?[] ----

    public static object?[] Extract(
        IArrowArray array, SqlType type, int rowCount, bool zeroCopyStrings = false) => type switch
    {
        SqlIntegerType => ExtractInt32((Int32Array)array, rowCount),
        SqlBigintType => ExtractInt64((Int64Array)array, rowCount),
        SqlRealType => ExtractFloat((FloatArray)array, rowCount),
        SqlDoubleType => ExtractDouble((DoubleArray)array, rowCount),
        SqlBooleanType => ExtractBool((BooleanArray)array, rowCount),
        SqlVarcharType => zeroCopyStrings
            ? ExtractStringAlias((StringArray)array, rowCount)
            : ExtractString((StringArray)array, rowCount),
        SqlDateType => ExtractDate((Date32Array)array, rowCount),
        SqlTimeType => ExtractTime((Time64Array)array, rowCount),
        SqlTimestampType => ExtractTimestamp((TimestampArray)array, rowCount),
        SqlDecimalType => ExtractDecimal((Decimal128Array)array, rowCount),
        _ => throw new NotSupportedException($"no Arrow extractor for {type.Display}"),
    };

    private static object?[] ExtractInt32(Int32Array a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)values[i];
        }

        return result;
    }

    private static object?[] ExtractInt64(Int64Array a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)values[i];
        }

        return result;
    }

    private static object?[] ExtractFloat(FloatArray a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)values[i];
        }

        return result;
    }

    private static object?[] ExtractDouble(DoubleArray a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)values[i];
        }

        return result;
    }

    private static object?[] ExtractBool(BooleanArray a, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object?)a.GetValue(i);
        }

        return result;
    }

    private static object?[] ExtractString(StringArray a, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)Utf8String.Of(a.GetString(i));
        }

        return result;
    }

    /// <summary>
    /// Zero-copy string extraction: each row's <see cref="Utf8String"/> aliases
    /// a slice of the Arrow <c>ValueBuffer.Memory</c>. The buffer must outlive
    /// the engine's reference to the data — typically that means the caller
    /// holds the <see cref="RecordBatch"/> for as long as the engine retains
    /// rows from it (in DBSP, that's "indefinitely" for state-bearing
    /// operators). For managed-array-backed buffers (the typical builder
    /// path), the GC keeps the bytes alive via the <see cref="ReadOnlyMemory{Byte}"/>
    /// owner reference even if the batch is disposed; for native-backed
    /// buffers, dispose-after-Push would dangle.
    /// </summary>
    private static object?[] ExtractStringAlias(StringArray a, int n)
    {
        var memory = a.ValueBuffer.Memory;
        var offsets = a.ValueOffsets;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(i))
            {
                result[i] = null;
                continue;
            }

            var start = offsets[i];
            var end = offsets[i + 1];
            result[i] = Utf8String.FromBytes(memory.Slice(start, end - start));
        }

        return result;
    }

    private static object?[] ExtractDate(Date32Array a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)new Date32(values[i]);
        }

        return result;
    }

    private static object?[] ExtractTime(Time64Array a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)new Time64(values[i]);
        }

        return result;
    }

    private static object?[] ExtractTimestamp(TimestampArray a, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(i) ? null : (object)new Timestamp(values[i]);
        }

        return result;
    }

    private static object?[] ExtractDecimal(Decimal128Array a, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(i))
            {
                result[i] = null;
                continue;
            }

            // Reinterpret the 16-byte Arrow Decimal128 buffer slot directly
            // as Int128. Arrow stores Decimal128 little-endian; .NET's Int128
            // in-memory layout is also little-endian on every supported
            // platform (x64, ARM64), so the bit pattern matches with no
            // shuffling. The previous BinaryPrimitives + Int128 ctor path
            // produced the same value with extra arithmetic.
            result[i] = new Decimal128(MemoryMarshal.Read<Int128>(a.GetBytes(i)));
        }

        return result;
    }

    // ---- Build: DbspNet typed object?[] → Arrow column ----

    public static IArrowArray Build(SqlType type, object?[] values) => type switch
    {
        SqlIntegerType => BuildInt32(values),
        SqlBigintType => BuildInt64(values),
        SqlRealType => BuildFloat(values),
        SqlDoubleType => BuildDouble(values),
        SqlBooleanType => BuildBool(values),
        SqlVarcharType => BuildString(values),
        SqlDateType => BuildDate(values),
        SqlTimeType => BuildTime(values),
        SqlTimestampType => BuildTimestamp(values),
        SqlDecimalType d => BuildDecimal(values, d.Precision, d.Scale),
        _ => throw new NotSupportedException($"no Arrow builder for {type.Display}"),
    };

    private static IArrowArray BuildInt32(object?[] vs)
    {
        var b = new Int32Array.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append((int)vs[i]!);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildInt64(object?[] vs)
    {
        var b = new Int64Array.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append((long)vs[i]!);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildFloat(object?[] vs)
    {
        var b = new FloatArray.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append((float)vs[i]!);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildDouble(object?[] vs)
    {
        var b = new DoubleArray.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append((double)vs[i]!);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildBool(object?[] vs)
    {
        var b = new BooleanArray.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append((bool)vs[i]!);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildString(object?[] vs)
    {
        var b = new StringArray.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                // Direct UTF-8 byte append — skips the Utf8String → .NET
                // string → UTF-8 round-trip. Builder still copies into its
                // internal contiguous buffer but only once.
                b.Append(((Utf8String)vs[i]!).Span);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildDate(object?[] vs)
    {
        var b = new Date32Array.Builder();
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append(DateTime.UnixEpoch.AddDays(((Date32)vs[i]!).Days));
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildTime(object?[] vs)
    {
        var b = new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond));
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append(((Time64)vs[i]!).Microseconds);
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildTimestamp(object?[] vs)
    {
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null));
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                var micros = ((Timestamp)vs[i]!).Microseconds;
                b.Append(new DateTimeOffset(
                    DateTime.UnixEpoch.AddTicks(micros * 10), TimeSpan.Zero));
            }
        }

        return b.Build();
    }

    private static IArrowArray BuildDecimal(object?[] vs, int precision, int scale)
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
        for (var i = 0; i < vs.Length; i++)
        {
            if (vs[i] is null)
            {
                b.AppendNull();
            }
            else
            {
                // The mantissa's 16 bytes are passed straight through to
                // the builder via a span-reinterpret on the stack local.
                // Same little-endian layout as Arrow Decimal128; no
                // BinaryPrimitives shuffling, no scratch buffer.
                var mantissa = ((Decimal128)vs[i]!).Mantissa;
                var bytes = MemoryMarshal.AsBytes(
                    MemoryMarshal.CreateReadOnlySpan(ref mantissa, 1));
                b.Append(bytes);
            }
        }

        return b.Build();
    }
}
