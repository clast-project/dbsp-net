// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow.Ipc;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// One immutable block of an indexed trace, held reference-free: the v3 run block's Arrow IPC
/// record-batch message in one managed <c>byte[]</c>, read through column views
/// (docs/design-flat-trace-state.md §2.2). Entries are in ascending unsigned key-hash order
/// (<c>__hash</c>), a key's entries contiguous.
/// </summary>
/// <remarks>
/// <para><b>Equality.</b> <see cref="KeyEquals"/> compares a probe key with a stored key without
/// decoding it, and must agree exactly with <see cref="StructuralRow.Equals(StructuralRow)"/> over
/// the decoded key — cell by cell, <c>object.Equals</c>, so the probe cell's CLR type must be the
/// column's (an <see cref="int"/> never equals a stored BIGINT), doubles compare with
/// <see cref="double.Equals(double)"/> (NaN equals NaN, 0.0 equals −0.0) and NULL equals only NULL.
/// It is the fourth implementation of row identity next to the three row-hash ones
/// (StructuralRowHash), so TraceBlockTests holds it against the production decoder.</para>
/// <para><b>Strings.</b> Cells read out copy their bytes unless asked to alias: an aliased
/// <see cref="Utf8String"/> keeps the whole block alive, and a row kept downstream (a view, an
/// aggregate key) would then pin blocks the trace has merged away.</para>
/// </remarks>
internal sealed class TraceBlock : ITraceBlock
{
    private readonly byte[] _bytes;
    private readonly int[] _validity;
    private readonly int[] _values;
    private readonly int[] _data;

    internal TraceBlock(BlockSchema schema, byte[] bytes, int count, int[] validity, int[] values, int[] data)
    {
        Schema = schema;
        _bytes = bytes;
        Count = count;
        _validity = validity;
        _values = values;
        _data = data;
        var hashes = Hashes;
        FirstHash = count == 0 ? 0 : hashes[0];
        LastHash = count == 0 ? 0 : hashes[^1];
    }

    // Raw layout, for BlockWriter.Concat: offsets into Bytes (−1: absent).
    internal byte[] Bytes => _bytes;

    internal int ValidityAt(int column) => _validity[column];

    internal int ValuesAt(int column) => _values[column];

    internal int DataAt(int column) => _data[column];

    /// <summary>The trace's block layout.</summary>
    public BlockSchema Schema { get; }

    /// <summary>Entries in the block.</summary>
    public int Count { get; }

    /// <summary>The smallest key hash (unsigned).</summary>
    public uint FirstHash { get; }

    /// <summary>The largest key hash (unsigned).</summary>
    public uint LastHash { get; }

    /// <summary>The record-batch message exactly as a run file stores it after the schema.</summary>
    public ReadOnlyMemory<byte> Message => _bytes;

    /// <summary>Each entry's key hash, ascending as unsigned.</summary>
    public ReadOnlySpan<uint> Hashes => Count == 0
        ? default
        : MemoryMarshal.Cast<byte, uint>(_bytes.AsSpan(_values[Schema.HashColumn], Count * 4));

    /// <summary>Entry <paramref name="i"/>'s weight.</summary>
    public long Weight(int i) => BinaryPrimitives.ReadInt64LittleEndian(_bytes.AsSpan(_values[Schema.WeightColumn] + (i * 8)));

    /// <summary>The first entry whose key hash is not below <paramref name="hash"/>
    /// (<see cref="Count"/> if none).</summary>
    public int LowerBound(uint hash)
    {
        var hashes = Hashes;
        int lo = 0, hi = hashes.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (hashes[mid] < hash)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    /// <summary>Whether entry <paramref name="i"/>'s key equals <paramref name="key"/>, without
    /// decoding it (see the remarks for the exact contract).</summary>
    public bool KeyEquals(int i, StructuralRow key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var kinds = Schema.KeyKinds;
        if (key.Count != kinds.Length)
        {
            return false;
        }

        for (var c = 0; c < kinds.Length; c++)
        {
            if (!CellEquals(c, kinds[c], i, key[c]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public object? ValueCell(int i, int column)
    {
        var first = Schema.KeyKinds.Length;
        return Cell(first + column, Schema.ValueKinds[column], i, aliasStrings: false);
    }

    StructuralRow ITraceBlock.Key(int i) => Key(i);

    StructuralRow ITraceBlock.Value(int i) => Value(i);

    void ITraceBlock.ValueCellsInto(int i, object?[] destination, int at) => ValueCellsInto(i, destination, at);

    /// <summary>Entry <paramref name="i"/>'s key, decoded.</summary>
    public StructuralRow Key(int i, bool aliasStrings = false)
    {
        var kinds = Schema.KeyKinds;
        var cells = new object?[kinds.Length];
        for (var c = 0; c < kinds.Length; c++)
        {
            cells[c] = Cell(c, kinds[c], i, aliasStrings);
        }

        return new StructuralRow(cells);
    }

    /// <summary>Entry <paramref name="i"/>'s value, decoded.</summary>
    public StructuralRow Value(int i, bool aliasStrings = false)
    {
        var cells = new object?[Schema.ValueKinds.Length];
        ValueCellsInto(i, cells, 0, aliasStrings);
        return new StructuralRow(cells);
    }

    /// <summary>Box entry <paramref name="i"/>'s value cells into <paramref name="destination"/>
    /// from <paramref name="at"/> — the join writes them straight into its output row.</summary>
    public void ValueCellsInto(int i, object?[] destination, int at, bool aliasStrings = false)
    {
        ArgumentNullException.ThrowIfNull(destination);
        var kinds = Schema.ValueKinds;
        var first = Schema.KeyKinds.Length;
        for (var c = 0; c < kinds.Length; c++)
        {
            destination[at + c] = Cell(first + c, kinds[c], i, aliasStrings);
        }
    }

    private bool IsNull(int column, int i)
    {
        var v = _validity[column];
        return v >= 0 && (_bytes[v + (i >> 3)] & (1 << (i & 7))) == 0;
    }

    private ReadOnlySpan<byte> At(int column, int i, int width) => _bytes.AsSpan(_values[column] + (i * width), width);

    private (int Start, int Length) StringRange(int column, int i)
    {
        var offsets = _values[column];
        var start = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(offsets + (i * 4)));
        var end = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(offsets + ((i + 1) * 4)));
        // An all-empty column's heap is absent (offset −1), and every range in it is empty.
        return end == start ? (0, 0) : (_data[column] + start, end - start);
    }

    private object? Cell(int column, CellKind kind, int i, bool aliasStrings)
    {
        if (IsNull(column, i))
        {
            return null;
        }

        switch (kind)
        {
            case CellKind.Int32: return BinaryPrimitives.ReadInt32LittleEndian(At(column, i, 4));
            case CellKind.Int64: return BinaryPrimitives.ReadInt64LittleEndian(At(column, i, 8));
            case CellKind.Float32: return BinaryPrimitives.ReadSingleLittleEndian(At(column, i, 4));
            case CellKind.Float64: return BinaryPrimitives.ReadDoubleLittleEndian(At(column, i, 8));
            case CellKind.Bool: return (_bytes[_values[column] + (i >> 3)] & (1 << (i & 7))) != 0;
            case CellKind.Date32: return new Date32(BinaryPrimitives.ReadInt32LittleEndian(At(column, i, 4)));
            case CellKind.Time64: return new Time64(BinaryPrimitives.ReadInt64LittleEndian(At(column, i, 8)));
            case CellKind.Timestamp: return new Timestamp(BinaryPrimitives.ReadInt64LittleEndian(At(column, i, 8)));
            case CellKind.Decimal128: return new Decimal128(MemoryMarshal.Read<Int128>(At(column, i, 16)));
            default:
                var (start, length) = StringRange(column, i);
                return Utf8String.FromBytes(aliasStrings
                    ? new ReadOnlyMemory<byte>(_bytes, start, length)
                    : _bytes.AsSpan(start, length).ToArray());
        }
    }

    private bool CellEquals(int column, CellKind kind, int i, object? probe)
    {
        if (IsNull(column, i))
        {
            return probe is null;
        }

        return kind switch
        {
            CellKind.Int32 => probe is int v && v == BinaryPrimitives.ReadInt32LittleEndian(At(column, i, 4)),
            CellKind.Int64 => probe is long v && v == BinaryPrimitives.ReadInt64LittleEndian(At(column, i, 8)),
            CellKind.Float32 => probe is float v && v.Equals(BinaryPrimitives.ReadSingleLittleEndian(At(column, i, 4))),
            CellKind.Float64 => probe is double v && v.Equals(BinaryPrimitives.ReadDoubleLittleEndian(At(column, i, 8))),
            CellKind.Bool => probe is bool v && v == ((_bytes[_values[column] + (i >> 3)] & (1 << (i & 7))) != 0),
            CellKind.Date32 => probe is Date32 v && v.Days == BinaryPrimitives.ReadInt32LittleEndian(At(column, i, 4)),
            CellKind.Time64 => probe is Time64 v && v.Microseconds == BinaryPrimitives.ReadInt64LittleEndian(At(column, i, 8)),
            CellKind.Timestamp => probe is Timestamp v && v.Microseconds == BinaryPrimitives.ReadInt64LittleEndian(At(column, i, 8)),
            CellKind.Decimal128 => probe is Decimal128 v && v.Equals(new Decimal128(MemoryMarshal.Read<Int128>(At(column, i, 16)))),
            _ => probe is Utf8String v && StringEquals(column, i, v),
        };
    }

    private bool StringEquals(int column, int i, Utf8String probe)
    {
        var (start, length) = StringRange(column, i);
        return probe.Span.SequenceEqual(_bytes.AsSpan(start, length));
    }

    // Read column buffer offsets out of an IPC stream — schema message, then one record batch over
    // [messageStart, end) — and keep only that record-batch message. Arrow's reader over a
    // ReadOnlyMemory aliases a managed array, so its buffers are offsets into `stream`.
    internal static TraceBlock Parse(BlockSchema schema, byte[] stream, int messageStart, int end)
    {
        using var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(stream, 0, end));
        using var batch = reader.ReadNextRecordBatch()
            ?? throw new InvalidDataException("block holds no record batch");
        var n = batch.ColumnCount;
        if (n != schema.HashColumn + 1)
        {
            throw new InvalidDataException($"block has {n} columns; its schema has {schema.HashColumn + 1}");
        }

        var validity = new int[n];
        var values = new int[n];
        var data = new int[n];
        for (var c = 0; c < n; c++)
        {
            var array = ((Apache.Arrow.Array)batch.Column(c)).Data;
            if (array.Offset != 0)
            {
                throw new InvalidDataException("block column has a non-zero array offset");
            }

            int Offset(int b)
            {
                if (b >= array.Buffers.Length || array.Buffers[b].Length == 0)
                {
                    return -1;
                }

                if (!MemoryMarshal.TryGetArray(array.Buffers[b].Memory, out var segment) || !ReferenceEquals(segment.Array, stream))
                {
                    throw new InvalidDataException("Arrow did not alias the block's bytes");
                }

                return segment.Offset - messageStart;
            }

            validity[c] = array.NullCount == 0 ? -1 : Offset(0);
            values[c] = Offset(1);
            data[c] = Offset(2);
        }

        var message = messageStart == 0 && end == stream.Length ? stream : stream.AsSpan(messageStart, end - messageStart).ToArray();
        return new TraceBlock(schema, message, batch.Length, validity, values, data);
    }
}
