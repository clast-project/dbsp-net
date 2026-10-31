// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
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
/// <summary>How VARCHAR columns are turned into <see cref="Utf8String"/> values.</summary>
public enum StringDecoding
{
    /// <summary>Decode to a .NET string and re-encode to UTF-8 — two allocations and two
    /// transcodes per cell. The pre-2026-10-31 ingest default, kept as an A/B arm; prefer
    /// <see cref="Copy"/>, which has the same retention without the round trip.</summary>
    Transcode,

    /// <summary>Alias the Arrow buffer with no copy. Requires the caller to keep the
    /// <c>RecordBatch</c> alive for as long as any row survives; on a native-backed buffer
    /// (what the IPC reader produces) a disposed batch leaves the values dangling.</summary>
    Alias,

    /// <summary>Copy each cell's bytes into its own exact-size array: one allocation per cell, no
    /// transcode, and each cell owns its bytes, so no retained row pins the source batch. The
    /// ingest default (docs/design-connectors.md, G5).</summary>
    Copy,

    /// <summary>Copy the column's bytes once into a managed arena and slice out of it: one
    /// allocation per column, no transcode, ordinary GC ownership. Every surviving row pins the
    /// whole arena, so use it where rows live and die together.</summary>
    Arena,
}

internal static class ArrowColumns
{
    // ---- Extraction: Arrow column → DbspNet typed object?[] ----

    public static object?[] Extract(
        IArrowArray array, SqlType type, int rowCount,
        StringDecoding strings = StringDecoding.Copy) =>
        Extract(array, type, 0, rowCount, strings);

    /// <summary>Rows <c>[from, from + rowCount)</c> of <paramref name="array"/>, so a caller can
    /// decode a large batch in blocks whose staging arrays stay off the large-object heap
    /// (docs/design-chunked-deltas.md §8).</summary>
    public static object?[] Extract(
        IArrowArray array, SqlType type, int from, int rowCount,
        StringDecoding strings = StringDecoding.Copy) => type switch
    {
        // INTEGER binds against any narrow signed int (FromArrowType widens Int8/Int16/Int32
        // → INTEGER), so the source array can be any of the three widths; widen to int here.
        SqlIntegerType => array switch
        {
            Int32Array a32 => ExtractInt32(a32, from, rowCount),
            Int16Array a16 => ExtractInt16(a16, from, rowCount),
            Int8Array a8 => ExtractInt8(a8, from, rowCount),
            _ => throw new NotSupportedException(
                $"INTEGER source column is {array.GetType().Name}; expected Int8/Int16/Int32Array"),
        },
        SqlBigintType => ExtractInt64((Int64Array)array, from, rowCount),
        SqlRealType => ExtractFloat((FloatArray)array, from, rowCount),
        SqlDoubleType => ExtractDouble((DoubleArray)array, from, rowCount),
        SqlBooleanType => ExtractBool((BooleanArray)array, from, rowCount),
        SqlVarcharType => strings switch
        {
            StringDecoding.Alias => ExtractStringAlias((StringArray)array, from, rowCount),
            StringDecoding.Arena => ExtractStringArena((StringArray)array, from, rowCount),
            StringDecoding.Copy => ExtractStringCopy((StringArray)array, from, rowCount),
            _ => ExtractString((StringArray)array, from, rowCount),
        },
        SqlDateType => ExtractDate((Date32Array)array, from, rowCount),
        SqlTimeType => ExtractTime((Time64Array)array, from, rowCount),
        // A Delta table's log schema can report TIMESTAMP while the physical Parquet stores
        // the legacy INT96 encoding, which engineered-wood surfaces as raw FixedSizeBinary(12)
        // rather than converting it. Decode INT96 here (driven by the declared type) so the
        // source's data conforms to the schema it advertises.
        SqlTimestampType => array is Apache.Arrow.Arrays.FixedSizeBinaryArray int96
            ? ExtractTimestampInt96(int96, from, rowCount)
            : ExtractTimestamp((TimestampArray)array, from, rowCount),
        // A DECIMAL binds against the Delta log's canonical Decimal128, but the Parquet
        // physical can pack small precisions into INT32/INT64, which engineered-wood surfaces
        // as Decimal32/Decimal64Array. The mantissa is the same scaled integer at every width;
        // widen it to Int128.
        SqlDecimalType => array switch
        {
            Decimal128Array d128 => ExtractDecimal(d128, from, rowCount),
            Decimal64Array d64 => ExtractDecimal64(d64, from, rowCount),
            Decimal32Array d32 => ExtractDecimal32(d32, from, rowCount),
            _ => throw new NotSupportedException(
                $"DECIMAL source column is {array.GetType().Name}; expected Decimal32/64/128Array"),
        },
        _ => throw new NotSupportedException($"no Arrow extractor for {type.Display}"),
    };

    private static object?[] ExtractInt32(Int32Array a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)values[from + i];
        }

        return result;
    }

    private static object?[] ExtractInt16(Int16Array a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)(int)values[from + i];
        }

        return result;
    }

    private static object?[] ExtractInt8(Int8Array a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)(int)values[from + i];
        }

        return result;
    }

    private static object?[] ExtractInt64(Int64Array a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)values[from + i];
        }

        return result;
    }

    private static object?[] ExtractFloat(FloatArray a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)values[from + i];
        }

        return result;
    }

    private static object?[] ExtractDouble(DoubleArray a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)values[from + i];
        }

        return result;
    }

    private static object?[] ExtractBool(BooleanArray a, int from, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object?)a.GetValue(from + i);
        }

        return result;
    }

    private static object?[] ExtractString(StringArray a, int from, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)Utf8String.Of(a.GetString(from + i));
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
    private static object?[] ExtractStringAlias(StringArray a, int from, int n)
    {
        var memory = a.ValueBuffer.Memory;
        var offsets = a.ValueOffsets;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(from + i))
            {
                result[i] = null;
                continue;
            }

            var start = offsets[from + i];
            var end = offsets[from + i + 1];
            result[i] = Utf8String.FromBytes(memory.Slice(start, end - start));
        }

        return result;
    }

    /// <summary>
    /// Copies the column's UTF-8 bytes once into a single managed arena and slices each row's
    /// <see cref="Utf8String"/> out of it: one allocation per column instead of two per cell, and no
    /// UTF-8 → UTF-16 → UTF-8 round trip (which <see cref="ExtractString"/> pays via
    /// <c>Utf8String.Of(a.GetString(from + i))</c>).
    /// </summary>
    /// <remarks>
    /// <para>The middle ground between <see cref="ExtractString"/> and
    /// <see cref="ExtractStringAlias"/>, and it exists because pure aliasing turned out to be
    /// unusable here: an Arrow IPC read buffer is <b>native-backed</b>, so an aliased value both
    /// dangles once the batch is disposed and pays a virtual <c>MemoryManager</c> call on every
    /// <c>Span</c> access — measured at +152% on the hash pass in
    /// docs/design-incremental-persistence.md §11.4. A managed arena has neither problem: ownership
    /// is ordinary GC, and <c>Span</c> is a plain array slice.</para>
    /// <para><b>Retention.</b> Every surviving row pins the whole arena, so this suits a caller
    /// where the rows live or die together — restore, where all of them survive. It is the wrong
    /// choice where a small fraction of a batch is retained (an ingest path that filters), because
    /// one surviving row would pin every string in the batch.</para>
    /// </remarks>
    private static object?[] ExtractStringArena(StringArray a, int from, int n)
    {
        var result = new object?[n];
        if (n == 0)
        {
            return result;
        }

        var offsets = a.ValueOffsets;
        var first = offsets[from];
        var arena = new byte[offsets[from + n] - first];
        a.ValueBuffer.Memory.Span.Slice(first, arena.Length).CopyTo(arena);

        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(from + i))
            {
                result[i] = null;
                continue;
            }

            result[i] = Utf8String.FromBytes(
                new ReadOnlyMemory<byte>(arena, offsets[from + i] - first, offsets[from + i + 1] - offsets[from + i]));
        }

        return result;
    }

    /// <summary>Each cell's UTF-8 bytes copied into an exact-size array: <see cref="ExtractString"/>
    /// without the UTF-8 → UTF-16 → UTF-8 round trip and its intermediate <c>string</c>.</summary>
    private static object?[] ExtractStringCopy(StringArray a, int from, int n)
    {
        var bytes = a.ValueBuffer.Memory.Span;
        var offsets = a.ValueOffsets;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(from + i))
            {
                result[i] = null;
                continue;
            }

            var start = offsets[from + i];
            result[i] = Utf8String.FromBytes(bytes[start..offsets[from + i + 1]].ToArray());
        }

        return result;
    }

    private static object?[] ExtractDate(Date32Array a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)new Date32(values[from + i]);
        }

        return result;
    }

    private static object?[] ExtractTime(Time64Array a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)new Time64(values[from + i]);
        }

        return result;
    }

    private static object?[] ExtractTimestamp(TimestampArray a, int from, int n)
    {
        var values = a.Values;
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            result[i] = a.IsNull(from + i) ? null : (object)new Timestamp(values[from + i]);
        }

        return result;
    }

    // Parquet INT96 timestamp: 12 bytes little-endian = int64 nanoseconds-of-day followed by
    // int32 Julian day. Convert to the µs-since-Unix-epoch DbspNet Timestamp. (Interpreted as
    // an instant / UTC, matching how arrow-rs and pyarrow read INT96 — self-consistent with
    // how the data was written.)
    private static object?[] ExtractTimestampInt96(Apache.Arrow.Arrays.FixedSizeBinaryArray a, int from, int n)
    {
        const long julianUnixEpochDay = 2440588L; // Julian day number of 1970-01-01
        const long microsPerDay = 86_400_000_000L;

        var width = ((FixedSizeBinaryType)a.Data.DataType).ByteWidth;
        if (width != 12)
        {
            throw new NotSupportedException(
                $"TIMESTAMP source column is FixedSizeBinary({width}); only INT96 (12 bytes) is decodable");
        }

        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(from + i))
            {
                result[i] = null;
                continue;
            }

            var bytes = a.GetBytes(from + i);
            var nanosOfDay = BinaryPrimitives.ReadInt64LittleEndian(bytes);
            var julianDay = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(8));
            var micros = ((julianDay - julianUnixEpochDay) * microsPerDay) + (nanosOfDay / 1000L);
            result[i] = new Timestamp(micros);
        }

        return result;
    }

    private static object?[] ExtractDecimal64(Decimal64Array a, int from, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            // 8-byte little-endian signed mantissa; sign-extend to Int128 (same scaled value).
            result[i] = a.IsNull(from + i)
                ? null
                : new Decimal128((Int128)BinaryPrimitives.ReadInt64LittleEndian(a.GetBytes(from + i)));
        }

        return result;
    }

    private static object?[] ExtractDecimal32(Decimal32Array a, int from, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            // 4-byte little-endian signed mantissa; sign-extend to Int128.
            result[i] = a.IsNull(from + i)
                ? null
                : new Decimal128((Int128)BinaryPrimitives.ReadInt32LittleEndian(a.GetBytes(from + i)));
        }

        return result;
    }

    private static object?[] ExtractDecimal(Decimal128Array a, int from, int n)
    {
        var result = new object?[n];
        for (var i = 0; i < n; i++)
        {
            if (a.IsNull(from + i))
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
            result[i] = new Decimal128(MemoryMarshal.Read<Int128>(a.GetBytes(from + i)));
        }

        return result;
    }

    // ---- Build: DbspNet typed object?[] → Arrow column ----

    public static IArrowArray Build(SqlType type, object?[] values) => Build(type, new ArrayCells(values));

    /// <summary>Column <paramref name="column"/> of <paramref name="rows"/> (each a
    /// <c>StructuralRow</c>), built straight from the rows — no boxed per-column staging array,
    /// which for a large output is an <c>object?[]</c> on the large-object heap
    /// (docs/design-chunked-deltas.md §8).</summary>
    public static IArrowArray Build(SqlType type, IReadOnlyList<object> rows, int column) =>
        Build(type, new RowCells(rows, column));

    private static IArrowArray Build<TCells>(SqlType type, TCells values)
        where TCells : struct, ICells => type switch
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

    private static IArrowArray BuildInt32<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new Int32Array.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildInt64<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new Int64Array.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildFloat<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new FloatArray.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildDouble<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new DoubleArray.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildBool<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new BooleanArray.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildString<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new StringArray.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildDate<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new Date32Array.Builder();
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildTime<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond));
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildTimestamp<TCells>(TCells vs)
        where TCells : struct, ICells
    {
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null));
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    private static IArrowArray BuildDecimal<TCells>(TCells vs, int precision, int scale)
        where TCells : struct, ICells
    {
        var b = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
        b.Reserve(vs.Count);
        for (var i = 0; i < vs.Count; i++)
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

    /// <summary>Where a builder reads its column's cells from; a struct, so each builder is
    /// specialised per source with no per-cell delegate or interface call.</summary>
    private interface ICells
    {
        int Count { get; }

        object? this[int i] { get; }
    }

    private readonly struct ArrayCells(object?[] values) : ICells
    {
        public int Count => values.Length;

        public object? this[int i] => values[i];
    }

    private readonly struct RowCells(IReadOnlyList<object> rows, int column) : ICells
    {
        public int Count => rows.Count;

        public object? this[int i] => ((DbspNet.Core.Collections.StructuralRow)rows[i])[column];
    }
}
