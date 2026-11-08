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

    // Each column's buffer offsets into _bytes (−1: absent), as one array: validity for every column,
    // then values, then data (docs/design-flat-trace-state.md §3.33: one object per block, not three).
    private readonly int[] _offsets;
    private readonly int _columns;

    private int[]? _valueHashes;

    internal TraceBlock(BlockSchema schema, byte[] bytes, int count, int[] validity, int[] values, int[] data, int[]? valueHashes = null)
    {
        _valueHashes = valueHashes;
        Schema = schema;
        _bytes = bytes;
        Count = count;
        _columns = validity.Length;
        _offsets = new int[_columns * 3];
        validity.CopyTo(_offsets, 0);
        values.CopyTo(_offsets, _columns);
        data.CopyTo(_offsets, _columns * 2);
        // Not through Hashes: building a block is not reading it (the access census).
        var hashes = count == 0 ? default : MemoryMarshal.Cast<byte, uint>(bytes.AsSpan(values[schema.HashColumn], count * 4));
        FirstHash = count == 0 ? 0 : hashes[0];
        LastHash = count == 0 ? 0 : hashes[^1];
        if (BlockAccess.Enabled)
        {
            _stamp.Born();
        }
    }

    // Raw layout, for BlockWriter.Concat: offsets into Bytes (−1: absent).
    internal byte[] Bytes => _bytes;

    internal int ValidityAt(int column) => _offsets[column];

    internal int ValuesAt(int column) => _offsets[_columns + column];

    internal int DataAt(int column) => _offsets[(_columns * 2) + column];

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
    public ReadOnlySpan<uint> Hashes
    {
        get
        {
            Touch();
            return Count == 0
                ? default
                : MemoryMarshal.Cast<byte, uint>(_bytes.AsSpan(ValuesAt(Schema.HashColumn), Count * 4));
        }
    }

    /// <summary>Entry <paramref name="i"/>'s weight.</summary>
    public long Weight(int i)
    {
        Touch();
        return BinaryPrimitives.ReadInt64LittleEndian(_bytes.AsSpan(ValuesAt(Schema.WeightColumn) + (i * 8)));
    }

    // The access census (DBSPNET_BLOCK_ACCESS=1, docs/design-bounded-memory.md §1).
    private BlockStamp _stamp;

    internal BlockStamp Stamp => _stamp;

    /// <summary>The block's bytes as held in memory: the message and the offsets.</summary>
    internal long ResidentBytes => _bytes.Length + ((long)_offsets.Length * 4) + ((long)(_valueHashes?.Length ?? 0) * 4);

    private void Touch()
    {
        if (BlockAccess.Enabled && _stamp.Touch())
        {
            BlockAccess.RecordStack(ResidentBytes);
        }
    }

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

    /// <summary>The value hashes, if known (BlockWriter.Concat carries them over).</summary>
    internal int[]? KnownValueHashes => _valueHashes;

    /// <summary>Take value hashes kept from this block's earlier body (a lazy block reloaded, §5.35).</summary>
    internal void AdoptValueHashes(int[] hashes)
    {
        if (hashes.Length == Count)
        {
            Interlocked.CompareExchange(ref _valueHashes, hashes, null);
        }
    }

    /// <inheritdoc/>
    public int ValueHash(int i)
    {
        Touch();
        return (_valueHashes ?? ComputeValueHashes())[i];
    }

    // A block adopted from a file: decode each value once (strings aliased: only hashed) and keep
    // only the hashes. Two threads may both compute; the arrays are equal.
    private int[] ComputeValueHashes()
    {
        var t0 = BlockCache.Profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        try
        {
            return ComputeValueHashesCore();
        }
        finally
        {
            if (BlockCache.Profile)
            {
                BlockCache.RecordValueHashes(Count, System.Diagnostics.Stopwatch.GetTimestamp() - t0);
            }
        }
    }

    /// <summary>Blocks whose value hashes this thread computed by decoding (tests, §5.35).</summary>
    [ThreadStatic]
    internal static long ThreadValueHashComputes;

    private int[] ComputeValueHashesCore()
    {
        ThreadValueHashComputes++;
        var hashes = new int[Count];
        for (var i = 0; i < Count; i++)
        {
            hashes[i] = Value(i, aliasStrings: true).GetHashCode();
        }

        Interlocked.CompareExchange(ref _valueHashes, hashes, null);
        return _valueHashes!;
    }

    /// <inheritdoc/>
    public bool ValueEquals(int i, StructuralRow value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var kinds = Schema.ValueKinds;
        if (value.Count != kinds.Length)
        {
            return false;
        }

        var first = Schema.KeyKinds.Length;
        for (var c = 0; c < kinds.Length; c++)
        {
            if (!CellEquals(first + c, kinds[c], i, value[c]))
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

    StructuralRow ITraceBlock.KeyAliased(int i) => Key(i, aliasStrings: true);

    /// <inheritdoc/>
    public StructuralRow[] KeysAliased()
    {
        var kinds = Schema.KeyKinds;
        var cells = new object?[Count][];
        for (var i = 0; i < cells.Length; i++)
        {
            cells[i] = new object?[kinds.Length];
        }

        for (var c = 0; c < kinds.Length; c++)
        {
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i][c] = Cell(c, kinds[c], i, aliasStrings: true);
            }
        }

        var rows = new StructuralRow[cells.Length];
        for (var i = 0; i < rows.Length; i++)
        {
            rows[i] = new StructuralRow(cells[i]);
        }

        return rows;
    }

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
        var v = ValidityAt(column);
        return v >= 0 && (_bytes[v + (i >> 3)] & (1 << (i & 7))) == 0;
    }

    private ReadOnlySpan<byte> At(int column, int i, int width) => _bytes.AsSpan(ValuesAt(column) + (i * width), width);

    private (int Start, int Length) StringRange(int column, int i)
    {
        var offsets = ValuesAt(column);
        var start = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(offsets + (i * 4)));
        var end = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(offsets + ((i + 1) * 4)));
        // An all-empty column's heap is absent (offset −1), and every range in it is empty.
        return end == start ? (0, 0) : (DataAt(column) + start, end - start);
    }

    private object? Cell(int column, CellKind kind, int i, bool aliasStrings)
    {
        Touch();
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
            case CellKind.Bool: return (_bytes[ValuesAt(column) + (i >> 3)] & (1 << (i & 7))) != 0;
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
        Touch();
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
            CellKind.Bool => probe is bool v && v == ((_bytes[ValuesAt(column) + (i >> 3)] & (1 << (i & 7))) != 0),
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
    /// <summary>
    /// Adopt a record-batch <paramref name="message"/> (as a run file stores a block: no schema in
    /// front) by reading its flatbuffer header directly — field nodes and buffer offsets — instead of
    /// building an Arrow reader, schema and arrays (docs/design-bounded-memory.md §3.6: that was 95%
    /// of a fault's parse). Null when the header is not the plain layout every block is written in
    /// (compression, another header type); the caller then takes the Arrow path.
    /// </summary>
    internal static TraceBlock? TryParseHeader(BlockSchema schema, byte[] message)
    {
        var m = message.AsSpan();
        if (m.Length < 8)
        {
            return null;
        }

        int pos = 0;
        if (BinaryPrimitives.ReadInt32LittleEndian(m) == -1)
        {
            pos = 4; // continuation marker
        }

        var metaSize = BinaryPrimitives.ReadInt32LittleEndian(m[pos..]);
        var fb = pos + 4;
        var bodyStart = fb + metaSize;
        if (metaSize <= 0 || bodyStart > m.Length)
        {
            return null;
        }

        // Flatbuffer: the root table, then fields by vtable slot.
        static int Field(ReadOnlySpan<byte> m, int table, int slot)
        {
            var vtable = table - BinaryPrimitives.ReadInt32LittleEndian(m[table..]);
            var vtSize = BinaryPrimitives.ReadUInt16LittleEndian(m[vtable..]);
            var entry = 4 + (slot * 2);
            if (entry >= vtSize)
            {
                return 0;
            }

            var off = BinaryPrimitives.ReadUInt16LittleEndian(m[(vtable + entry)..]);
            return off == 0 ? 0 : table + off;
        }

        static int Deref(ReadOnlySpan<byte> m, int at) => at + (int)BinaryPrimitives.ReadUInt32LittleEndian(m[at..]);

        var root = Deref(m, fb);
        var headerType = Field(m, root, 1);
        var header = Field(m, root, 2);
        if (headerType == 0 || m[headerType] != 3 || header == 0)
        {
            return null; // not a RecordBatch
        }

        var batch = Deref(m, header);
        if (Field(m, batch, 3) != 0)
        {
            return null; // compressed
        }

        var lengthAt = Field(m, batch, 0);
        var count = lengthAt == 0 ? 0L : BinaryPrimitives.ReadInt64LittleEndian(m[lengthAt..]);
        var nodesAt = Field(m, batch, 1);
        var buffersAt = Field(m, batch, 2);
        if (nodesAt == 0 || buffersAt == 0 || count > int.MaxValue)
        {
            return null;
        }

        var nodes = Deref(m, nodesAt);
        var buffers = Deref(m, buffersAt);
        var n = schema.HashColumn + 1;
        if (BinaryPrimitives.ReadInt32LittleEndian(m[nodes..]) != n)
        {
            return null;
        }

        var bufferCount = BinaryPrimitives.ReadInt32LittleEndian(m[buffers..]);
        var validity = new int[n];
        var values = new int[n];
        var data = new int[n];
        var b = 0;
        static int Next(ReadOnlySpan<byte> m, int buffers, int bufferCount, int bodyStart, ref int b)
        {
            if (b >= bufferCount)
            {
                throw new InvalidDataException("block header lists too few buffers");
            }

            var at = buffers + 4 + (b++ * 16);
            var offset = BinaryPrimitives.ReadInt64LittleEndian(m[at..]);
            var length = BinaryPrimitives.ReadInt64LittleEndian(m[(at + 8)..]);
            return length == 0 ? -1 : checked((int)(bodyStart + offset));
        }

        for (var c = 0; c < n; c++)
        {
            var nullCount = BinaryPrimitives.ReadInt64LittleEndian(m[(nodes + 4 + (c * 16) + 8)..]);
            var kind = c < schema.KeyKinds.Length ? schema.KeyKinds[c]
                : c < schema.WeightColumn ? schema.ValueKinds[c - schema.KeyKinds.Length]
                : CellKind.Int64; // weight, hash: fixed width
            var v = Next(m, buffers, bufferCount, bodyStart, ref b);
            validity[c] = nullCount == 0 ? -1 : v;
            values[c] = Next(m, buffers, bufferCount, bodyStart, ref b);
            data[c] = kind == CellKind.Utf8 ? Next(m, buffers, bufferCount, bodyStart, ref b) : -1;
        }

        return b == bufferCount ? new TraceBlock(schema, message, (int)count, validity, values, data) : null;
    }

    internal static TraceBlock Parse(BlockSchema schema, byte[] stream, int messageStart, int end)
    {
        var p0 = BlockCache.Profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        try
        {
            return ParseCore(schema, stream, messageStart, end);
        }
        finally
        {
            if (BlockCache.Profile)
            {
                BlockCache.RecordParse(System.Diagnostics.Stopwatch.GetTimestamp() - p0);
            }
        }
    }

    private static TraceBlock ParseCore(BlockSchema schema, byte[] stream, int messageStart, int end)
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

        var c0 = BlockCache.Profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;
        var message = messageStart == 0 && end == stream.Length ? stream : stream.AsSpan(messageStart, end - messageStart).ToArray();
        if (BlockCache.Profile)
        {
            BlockCache.RecordCopy(System.Diagnostics.Stopwatch.GetTimestamp() - c0);
        }

        return new TraceBlock(schema, message, batch.Length, validity, values, data);
    }
}
