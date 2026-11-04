// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Core.Operators.Stateful.Blocks;

namespace DbspNet.Persistence.Blocks;

/// <summary>A view's rows as the block entries holding them, one per copy, in the order a full
/// write's Arrow batches hold them: the keys an output row index takes without decoding a row
/// (docs/design-bounded-memory.md §3h).</summary>
internal sealed class ViewEntries(TraceBlock[] blocks, int[] indexes)
{
    public TraceBlock[] Blocks { get; } = blocks;

    public int[] Indexes { get; } = indexes;

    public int Count => Indexes.Length;
}

/// <summary>
/// A stored view's full contents as Arrow batches built from its blocks (docs/design-bounded-memory.md
/// §3h): a block's columns already are Arrow buffers, so each output column is copied from them, a
/// run of consecutive entries at a time, and no row is decoded. Each worker's blocks are read in
/// turn; a row a worker holds n times is written n times.
/// </summary>
internal static class ViewArrow
{
    /// <summary>Rows per batch.</summary>
    internal const int BatchRows = 1 << 16;

    /// <summary>
    /// The batches and their entries, or null when the view cannot be read this way: a worker's
    /// trace has a managed level or a GC floor, a block is not an in-memory body of the expected
    /// layout, or a row's count is negative on some worker (the workers' integrals must then be
    /// summed, which takes rows).
    /// </summary>
    public static (List<RecordBatch> Batches, ViewEntries Entries)? TryBuild(
        Schema schema, IReadOnlyList<BlockedIndexedTrace> shards, int batchRows = BatchRows)
    {
        var blocks = new List<TraceBlock>();
        var indexes = new List<int>();
        foreach (var shard in shards)
        {
            if (shard.StreamEntries() is not { } entries)
            {
                return null;
            }

            foreach (var (block, index, weight) in entries)
            {
                if (weight < 0 || block is not TraceBlock body || body.Schema.KeyKinds.Length != schema.FieldsList.Count || body.Schema.ValueKinds.Length != 0)
                {
                    return null;
                }

                for (var k = 0; k < weight; k++)
                {
                    blocks.Add(body);
                    indexes.Add(index);
                }
            }
        }

        var all = new ViewEntries([.. blocks], [.. indexes]);
        blocks.Clear();
        indexes.Clear();
        var batches = new List<RecordBatch>();
        for (var from = 0; from < all.Count; from += batchRows)
        {
            var to = Math.Min(all.Count, from + batchRows);
            var arrays = new IArrowArray[schema.FieldsList.Count];
            for (var c = 0; c < arrays.Length; c++)
            {
                if (Column(schema.FieldsList[c].DataType, all, c, from, to) is not { } array)
                {
                    return null;
                }

                arrays[c] = array;
            }

            batches.Add(new RecordBatch(schema, arrays, to - from));
        }

        return (batches, all);
    }

    // Column c of entries [from, to), or null when its type is not the block's kind.
    private static IArrowArray? Column(IArrowType type, ViewEntries entries, int c, int from, int to)
    {
        var n = to - from;
        var kind = entries.Blocks[from].Schema.KeyKinds[c];
        var width = (kind, type) switch
        {
            (CellKind.Int32, Int32Type) or (CellKind.Float32, FloatType) or (CellKind.Date32, Date32Type) => 4,
            (CellKind.Int64, Int64Type) or (CellKind.Float64, DoubleType) or (CellKind.Time64, Time64Type) or (CellKind.Timestamp, TimestampType) => 8,
            (CellKind.Decimal128, Decimal128Type) => 16,
            (CellKind.Bool, BooleanType) => -1,
            (CellKind.Utf8, StringType) => 0,
            _ => int.MinValue,
        };
        if (width == int.MinValue)
        {
            return null;
        }

        var (validity, nulls) = Validity(entries, c, from, to);
        ArrowBuffer[] buffers;
        if (width > 0)
        {
            var values = new byte[(long)n * width];
            ForRuns(entries, from, to, (block, first, count, at) =>
                block.Bytes.AsSpan(block.ValuesAt(c) + (first * width), count * width).CopyTo(values.AsSpan(at * width)));
            buffers = [validity, new ArrowBuffer(values)];
        }
        else if (width < 0)
        {
            var bits = new byte[(n + 7) / 8];
            for (var i = 0; i < n; i++)
            {
                var block = entries.Blocks[from + i];
                var j = entries.Indexes[from + i];
                if ((block.Bytes[block.ValuesAt(c) + (j >> 3)] & (1 << (j & 7))) != 0)
                {
                    bits[i >> 3] |= (byte)(1 << (i & 7));
                }
            }

            buffers = [validity, new ArrowBuffer(bits)];
        }
        else
        {
            var offsets = new byte[(n + 1) * 4];
            long heap = 0;
            for (var i = 0; i < n; i++)
            {
                var block = entries.Blocks[from + i];
                var j = entries.Indexes[from + i];
                heap += Offset(block, c, j + 1) - Offset(block, c, j);
            }

            var data = new byte[checked((int)heap)];
            var position = 0;
            ForRuns(entries, from, to, (block, first, count, at) =>
            {
                var start = Offset(block, c, first);
                var length = Offset(block, c, first + count) - start;
                if (length > 0)
                {
                    block.Bytes.AsSpan(block.DataAt(c) + start, length).CopyTo(data.AsSpan(position));
                }

                for (var k = 1; k <= count; k++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan((at + k) * 4), position + Offset(block, c, first + k) - start);
                }

                position += length;
            });
            buffers = [validity, new ArrowBuffer(offsets), new ArrowBuffer(data)];
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(type, n, nulls, 0, buffers));
    }

    // The validity bitmap of column c over entries [from, to): empty when nothing is null.
    private static (ArrowBuffer Bitmap, int Nulls) Validity(ViewEntries entries, int c, int from, int to)
    {
        var n = to - from;
        byte[]? bits = null;
        var nulls = 0;
        for (var i = 0; i < n; i++)
        {
            var block = entries.Blocks[from + i];
            var v = block.ValidityAt(c);
            var j = entries.Indexes[from + i];
            if (v < 0 || (block.Bytes[v + (j >> 3)] & (1 << (j & 7))) != 0)
            {
                continue;
            }

            if (bits is null)
            {
                bits = new byte[(n + 7) / 8];
                System.Array.Fill(bits, (byte)0xFF);
            }

            bits[i >> 3] &= (byte)~(1 << (i & 7));
            nulls++;
        }

        return (bits is null ? ArrowBuffer.Empty : new ArrowBuffer(bits), nulls);
    }

    // Maximal runs of consecutive indexes in one block: (block, first index, count, position in the batch).
    private static void ForRuns(ViewEntries entries, int from, int to, Action<TraceBlock, int, int, int> run)
    {
        var i = from;
        while (i < to)
        {
            var block = entries.Blocks[i];
            var first = entries.Indexes[i];
            var end = i + 1;
            while (end < to && ReferenceEquals(entries.Blocks[end], block) && entries.Indexes[end] == first + (end - i))
            {
                end++;
            }

            run(block, first, end - i, i - from);
            i = end;
        }
    }

    // A string column's int32 offset i (i = Count: its heap length).
    private static int Offset(TraceBlock block, int column, int i) =>
        BinaryPrimitives.ReadInt32LittleEndian(block.Bytes.AsSpan(block.ValuesAt(column) + (i * 4)));
}
