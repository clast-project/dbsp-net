// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// Writes one block — an Arrow IPC record-batch message in the v3 run schema — straight from rows
/// into a single exactly-sized <c>byte[]</c> (docs/design-flat-trace-state.md §3.1). Freezing
/// through Arrow's builders and stream writer allocated per column, per buffer and per stream, and
/// under twelve workers freezing at once that allocation made encoding seven times slower; this
/// writer allocates the message and nothing else.
/// </summary>
/// <remarks>
/// <para><b>Message.</b> <c>0xFFFFFFFF</c>, the metadata length, a FlatBuffers <c>Message</c>
/// (version V5, header <c>RecordBatch</c>: length, one field node per column, the buffers, the body
/// length) padded to 8 bytes, then the body. Each column's buffers follow Arrow's layout — validity
/// (empty when the column has no null), then values (a bitmap for BOOLEAN, int32 offsets for
/// VARCHAR), then the UTF-8 heap for VARCHAR — each 8-byte aligned. Any Arrow reader reads it with
/// the run's schema message; TraceBlockTests decode every block through Arrow and the run codec.</para>
/// <para><b>Cells</b> must already be the column's canonical CLR type, as for the run codec.</para>
/// </remarks>
internal static class BlockWriter
{
    private const int Prefix = 8;            // continuation marker + metadata length
    private const int MessageTable = 16;     // see WriteHeader for the fixed header layout
    private const int RecordBatchTable = 56;
    private const int NodesVector = 84;      // its uint32 length; elements from 88

    public static TraceBlock Write(BlockSchema schema, StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int n)
    {
        var keyKinds = schema.KeyKinds;
        var valueKinds = schema.ValueKinds;
        var columns = keyKinds.Length + valueKinds.Length + 2;
        var t = Core.Operators.Stateful.Blocks.BlockTraceProfile.Now();
        var nulls = new int[columns];
        var heap = new long[columns];
        var bufferCount = 0;
        for (var c = 0; c < columns; c++)
        {
            var kind = KindOf(schema, c);
            bufferCount += kind == CellKind.Utf8 ? 3 : 2;
            if (c >= schema.WeightColumn)
            {
                continue;
            }

            var (rows, column) = Source(schema, keys, values, c);
            for (var i = 0; i < n; i++)
            {
                var cell = rows[i][column];
                if (cell is null)
                {
                    nulls[c]++;
                }
                else if (kind == CellKind.Utf8)
                {
                    heap[c] += ((Utf8String)cell).Span.Length;
                }
            }
        }

        // Body layout: each buffer 8-byte aligned.
        var buffers = new (long Offset, long Length)[bufferCount];
        var validity = new int[columns];
        var valueAt = new int[columns];
        var dataAt = new int[columns];
        long body = 0;
        var b = 0;
        (long, long) Next(long length)
        {
            var at = body;
            body += Pad8(length);
            return (at, length);
        }

        for (var c = 0; c < columns; c++)
        {
            var kind = KindOf(schema, c);
            buffers[b] = Next(nulls[c] > 0 ? (n + 7) / 8 : 0);
            validity[c] = nulls[c] > 0 ? (int)buffers[b].Offset : -1;
            b++;
            buffers[b] = Next(kind switch
            {
                CellKind.Bool => (n + 7) / 8,
                CellKind.Utf8 => (n + 1) * 4L,
                _ => (long)Width(kind) * n,
            });
            valueAt[c] = n == 0 ? -1 : (int)buffers[b].Offset;
            b++;
            if (kind == CellKind.Utf8)
            {
                buffers[b] = Next(heap[c]);
                dataAt[c] = heap[c] > 0 ? (int)buffers[b].Offset : -1;
                b++;
            }
            else
            {
                dataAt[c] = -1;
            }
        }

        var buffersVector = NodesVector + 4 + (16 * columns) + 4; // the buffers vector's uint32 length
        var metadata = (int)Pad8(buffersVector + 4 + (16 * bufferCount));
        var bodyStart = Prefix + metadata;
        if (Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
        {
            Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(0, t, n);
            t = Core.Operators.Stateful.Blocks.BlockTraceProfile.Now();
        }

        var message = new byte[checked(bodyStart + body)];
        if (Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
        {
            Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(1, t, message.Length);
            t = Core.Operators.Stateful.Blocks.BlockTraceProfile.Now();
        }

        var span = message.AsSpan();
        BinaryPrimitives.WriteUInt32LittleEndian(span, 0xFFFFFFFF);
        BinaryPrimitives.WriteInt32LittleEndian(span[4..], metadata);
        WriteHeader(span.Slice(Prefix, metadata), n, nulls, buffers, buffersVector, body);

        var bodySpan = span[bodyStart..];
        for (var c = 0; c < columns && n > 0; c++)
        {
            var kind = KindOf(schema, c);
            if (c == schema.WeightColumn)
            {
                MemoryMarshal.AsBytes(weights.AsSpan(0, n)).CopyTo(bodySpan[valueAt[c]..]);
                continue;
            }

            if (c == schema.HashColumn)
            {
                MemoryMarshal.AsBytes(hashes.AsSpan(0, n)).CopyTo(bodySpan[valueAt[c]..]);
                continue;
            }

            var (rows, column) = Source(schema, keys, values, c);
            WriteColumn(bodySpan, kind, rows, column, n, validity[c], valueAt[c], dataAt[c]);
        }

        if (Core.Operators.Stateful.Blocks.BlockTraceProfile.Enabled)
        {
            Core.Operators.Stateful.Blocks.BlockTraceProfile.Section(2, t, columns);
        }

        for (var c = 0; c < columns; c++)
        {
            validity[c] = validity[c] < 0 ? -1 : validity[c] + bodyStart;
            valueAt[c] = valueAt[c] < 0 ? -1 : valueAt[c] + bodyStart;
            dataAt[c] = dataAt[c] < 0 ? -1 : dataAt[c] + bodyStart;
        }

        // Value hashes for group lookups (§3.5): free here, each row caches its hash.
        var valueHashes = new int[n];
        for (var i = 0; i < n; i++)
        {
            valueHashes[i] = values[i].GetHashCode();
        }

        return new TraceBlock(schema, message, n, validity, valueAt, dataAt, valueHashes);
    }

    /// <summary>
    /// One block holding <paramref name="parts"/>' entries in order, built from their bytes: fixed
    /// widths and string heaps copied, string offsets rebased, validity and BOOLEAN bits copied —
    /// no cell decoded. Checkpoints use it to write a giant group whose hash spans blocks in memory
    /// as the single block the v3 format requires (design §3.2): decoding and re-encoding those
    /// spans cost 36 s of summed checkpoint time on ivm-bench.
    /// </summary>
    public static TraceBlock Concat(BlockSchema schema, IReadOnlyList<TraceBlock> parts)
    {
        var layout = ConcatLayout.Of(schema, parts);
        var (columns, n, validity, valueAt, dataAt) = (layout.Columns, layout.N, layout.Validity, layout.ValueAt, layout.DataAt);
        var bodyStart = layout.BodyStart;
        var message = new byte[checked(bodyStart + layout.Body)];
        var span = message.AsSpan();
        layout.WriteHead(span);
        var out_ = span[bodyStart..];

        for (var c = 0; c < columns && n > 0; c++)
        {
            var kind = KindOf(schema, c);
            var at = 0;       // entries written so far
            var heapAt = 0;   // heap bytes written so far
            if (kind == CellKind.Utf8)
            {
                BinaryPrimitives.WriteInt32LittleEndian(out_[valueAt[c]..], 0);
            }

            foreach (var part in parts)
            {
                var src = part.Bytes.AsSpan();
                var count = part.Count;
                if (count == 0)
                {
                    continue;
                }

                if (validity[c] >= 0)
                {
                    var v = part.ValidityAt(c);
                    for (var i = 0; i < count; i++)
                    {
                        if (v < 0 || ((src[v + (i >> 3)] >> (i & 7)) & 1) != 0)
                        {
                            var bit = at + i;
                            out_[validity[c] + (bit >> 3)] |= (byte)(1 << (bit & 7));
                        }
                    }
                }

                switch (kind)
                {
                    case CellKind.Bool:
                        var bits = part.ValuesAt(c);
                        for (var i = 0; i < count; i++)
                        {
                            if (((src[bits + (i >> 3)] >> (i & 7)) & 1) != 0)
                            {
                                var bit = at + i;
                                out_[valueAt[c] + (bit >> 3)] |= (byte)(1 << (bit & 7));
                            }
                        }

                        break;
                    case CellKind.Utf8:
                        var length = Offset(part, c, count);
                        if (length > 0)
                        {
                            src.Slice(part.DataAt(c), length).CopyTo(out_[(dataAt[c] + heapAt)..]);
                        }

                        for (var i = 1; i <= count; i++)
                        {
                            BinaryPrimitives.WriteInt32LittleEndian(out_[(valueAt[c] + ((at + i) * 4))..], heapAt + Offset(part, c, i));
                        }

                        heapAt += length;
                        break;
                    default:
                        var width = c == schema.WeightColumn ? 8 : c == schema.HashColumn ? 4 : Width(kind);
                        src.Slice(part.ValuesAt(c), count * width).CopyTo(out_[(valueAt[c] + (at * width))..]);
                        break;
                }

                at += count;
            }
        }

        for (var c = 0; c < columns; c++)
        {
            validity[c] = validity[c] < 0 ? -1 : validity[c] + bodyStart;
            valueAt[c] = valueAt[c] < 0 ? -1 : valueAt[c] + bodyStart;
            dataAt[c] = dataAt[c] < 0 ? -1 : dataAt[c] + bodyStart;
        }

        int[]? valueHashes = null;
        if (parts.All(p => p.KnownValueHashes is not null))
        {
            valueHashes = new int[n];
            var at = 0;
            foreach (var part in parts)
            {
                part.KnownValueHashes!.AsSpan(0, part.Count).CopyTo(valueHashes.AsSpan(at));
                at += part.Count;
            }
        }

        return new TraceBlock(schema, message, n, validity, valueAt, dataAt, valueHashes);
    }

    /// <summary>
    /// Write <paramref name="parts"/> as one block straight to <paramref name="file"/>, byte for byte
    /// the message <see cref="Concat"/> builds, without building it (docs/design-bounded-memory.md
    /// §3d): fixed widths and string heaps are written from the parts' own bytes, and only bitmaps and
    /// rebased string offsets pass through a scratch buffer of at most one part's size. A giant group
    /// of 175 MB costs its checkpoint ~1 MB instead. Returns the bytes written.
    /// </summary>
    public static async ValueTask<long> ConcatToAsync(
        BlockSchema schema, IReadOnlyList<TraceBlock> parts, Core.IO.ISequentialFile file, CancellationToken cancellationToken)
    {
        var layout = ConcatLayout.Of(schema, parts);
        var n = layout.N;
        var head = new byte[layout.BodyStart];
        layout.WriteHead(head);
        await file.WriteAsync(head, cancellationToken).ConfigureAwait(false);
        if (n == 0)
        {
            await file.WriteAsync(new byte[layout.Body], cancellationToken).ConfigureAwait(false);
            return layout.BodyStart + layout.Body;
        }

        var largest = 0;
        foreach (var part in parts)
        {
            largest = Math.Max(largest, part.Count);
        }

        var scratch = new byte[Math.Max((n + 7) / 8, (largest * 4) + 4)];
        var padding = new byte[8];
        var b = 0;
        long written = 0;
        for (var c = 0; c < layout.Columns; c++)
        {
            var kind = KindOf(schema, c);

            // Validity: empty unless the column has a null somewhere.
            if (layout.Buffers[b].Length > 0)
            {
                await Bits(c, valid: true).ConfigureAwait(false);
            }

            await Pad(layout.Buffers[b++].Length).ConfigureAwait(false);

            // Values.
            switch (kind)
            {
                case CellKind.Bool:
                    await Bits(c, valid: false).ConfigureAwait(false);
                    break;
                case CellKind.Utf8:
                    BinaryPrimitives.WriteInt32LittleEndian(scratch, 0);
                    await Write(scratch.AsMemory(0, 4)).ConfigureAwait(false);
                    var heapAt = 0;
                    foreach (var part in parts)
                    {
                        if (part.Count == 0)
                        {
                            continue;
                        }

                        for (var i = 1; i <= part.Count; i++)
                        {
                            BinaryPrimitives.WriteInt32LittleEndian(scratch.AsSpan((i - 1) * 4), heapAt + Offset(part, c, i));
                        }

                        await Write(scratch.AsMemory(0, part.Count * 4)).ConfigureAwait(false);
                        heapAt += Offset(part, c, part.Count);
                    }

                    break;
                default:
                    var width = c == schema.WeightColumn ? 8 : c == schema.HashColumn ? 4 : Width(kind);
                    foreach (var part in parts)
                    {
                        if (part.Count > 0)
                        {
                            await Write(part.Bytes.AsMemory(part.ValuesAt(c), part.Count * width)).ConfigureAwait(false);
                        }
                    }

                    break;
            }

            await Pad(layout.Buffers[b++].Length).ConfigureAwait(false);

            // A string column's heap.
            if (kind == CellKind.Utf8)
            {
                foreach (var part in parts)
                {
                    var length = part.Count == 0 ? 0 : Offset(part, c, part.Count);
                    if (length > 0)
                    {
                        await Write(part.Bytes.AsMemory(part.DataAt(c), length)).ConfigureAwait(false);
                    }
                }

                await Pad(layout.Buffers[b++].Length).ConfigureAwait(false);
            }
        }

        if (written != layout.Body)
        {
            throw new InvalidOperationException($"streamed block body is {written} bytes, its layout {layout.Body}");
        }

        return layout.BodyStart + written;

        ValueTask Write(ReadOnlyMemory<byte> data)
        {
            written += data.Length;
            return file.WriteAsync(data, cancellationToken);
        }

        // The buffer just written was `length` bytes: bring the body to the next 8-byte boundary.
        ValueTask Pad(long length)
        {
            var pad = (int)(Pad8(length) - length);
            return pad == 0 ? ValueTask.CompletedTask : Write(padding.AsMemory(0, pad));
        }

        // A validity bitmap (valid) or BOOLEAN values over all parts, through the scratch buffer.
        ValueTask Bits(int c, bool valid)
        {
            var bytes = (n + 7) / 8;
            var out_ = scratch.AsSpan(0, bytes);
            out_.Clear();
            var at = 0;
            foreach (var part in parts)
            {
                var src = part.Bytes;
                var at0 = valid ? part.ValidityAt(c) : part.ValuesAt(c);
                for (var i = 0; i < part.Count; i++)
                {
                    if (valid ? at0 < 0 || ((src[at0 + (i >> 3)] >> (i & 7)) & 1) != 0 : ((src[at0 + (i >> 3)] >> (i & 7)) & 1) != 0)
                    {
                        var bit = at + i;
                        out_[bit >> 3] |= (byte)(1 << (bit & 7));
                    }
                }

                at += part.Count;
            }

            return Write(scratch.AsMemory(0, bytes));
        }
    }

    // A string column's int32 offset i (i = Count: its heap length).
    private static int Offset(TraceBlock part, int column, int i) =>
        BinaryPrimitives.ReadInt32LittleEndian(part.Bytes.AsSpan(part.ValuesAt(column) + (i * 4)));

    /// <summary>Where each buffer of <see cref="Concat"/>'s message goes, from the parts alone.</summary>
    private sealed class ConcatLayout
    {
        public int Columns, N, Metadata, BuffersVector;
        public long Body;
        public int[] Nulls = [], Validity = [], ValueAt = [], DataAt = [];
        public (long Offset, long Length)[] Buffers = [];

        public int BodyStart => Prefix + Metadata;

        public void WriteHead(Span<byte> span)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(span, 0xFFFFFFFF);
            BinaryPrimitives.WriteInt32LittleEndian(span[4..], Metadata);
            WriteHeader(span.Slice(Prefix, Metadata), N, Nulls, Buffers, BuffersVector, Body);
        }

        public static ConcatLayout Of(BlockSchema schema, IReadOnlyList<TraceBlock> parts)
        {
            var columns = schema.HashColumn + 1;
            var n = 0;
            foreach (var part in parts)
            {
                n = checked(n + part.Count);
            }

            var nulls = new int[columns];
            var heap = new long[columns];
            var bufferCount = 0;
            for (var c = 0; c < columns; c++)
            {
                var kind = KindOf(schema, c);
                bufferCount += kind == CellKind.Utf8 ? 3 : 2;
                foreach (var part in parts)
                {
                    var v = part.ValidityAt(c);
                    if (v >= 0)
                    {
                        for (var i = 0; i < part.Count; i++)
                        {
                            nulls[c] += (part.Bytes[v + (i >> 3)] >> (i & 7)) & 1 ^ 1;
                        }
                    }

                    if (kind == CellKind.Utf8 && part.Count > 0)
                    {
                        heap[c] += Offset(part, c, part.Count);
                    }
                }
            }

            var buffers = new (long Offset, long Length)[bufferCount];
            var validity = new int[columns];
            var valueAt = new int[columns];
            var dataAt = new int[columns];
            long body = 0;
            var b = 0;
            (long, long) Next(long length)
            {
                var at = body;
                body += Pad8(length);
                return (at, length);
            }

            for (var c = 0; c < columns; c++)
            {
                var kind = KindOf(schema, c);
                buffers[b] = Next(nulls[c] > 0 ? (n + 7) / 8 : 0);
                validity[c] = nulls[c] > 0 ? (int)buffers[b].Offset : -1;
                b++;
                buffers[b] = Next(kind switch
                {
                    CellKind.Bool => (n + 7) / 8,
                    CellKind.Utf8 => (n + 1) * 4L,
                    _ => (long)Width(kind) * n,
                });
                valueAt[c] = n == 0 ? -1 : (int)buffers[b].Offset;
                b++;
                if (kind == CellKind.Utf8)
                {
                    buffers[b] = Next(heap[c]);
                    dataAt[c] = heap[c] > 0 ? (int)buffers[b].Offset : -1;
                    b++;
                }
                else
                {
                    dataAt[c] = -1;
                }
            }

            var buffersVector = NodesVector + 4 + (16 * columns) + 4;
            return new ConcatLayout
            {
                Columns = columns,
                N = n,
                Nulls = nulls,
                Buffers = buffers,
                Validity = validity,
                ValueAt = valueAt,
                DataAt = dataAt,
                Body = body,
                BuffersVector = buffersVector,
                Metadata = (int)Pad8(buffersVector + 4 + (16 * bufferCount)),
            };
        }
    }

    /// <summary>
    /// One block of entries gathered from other blocks (docs/design-bounded-memory.md §3h): entry i's
    /// key columns are copied from <paramref name="sources"/>[i] at <paramref name="indexes"/>[i],
    /// whose key columns must be <paramref name="schema"/>'s; its one value column, BIGINT, is
    /// <paramref name="values"/>[i]. Nothing is decoded: an output row index takes a view's rows so.
    /// </summary>
    public static TraceBlock Gather(
        BlockSchema schema, TraceBlock[] sources, int[] indexes, long[] values, long[] weights, int[] hashes, int n)
    {
        if (schema.ValueKinds is not [CellKind.Int64])
        {
            throw new InvalidOperationException("a gathered block's one value column is a BIGINT");
        }

        return GatherCore(schema, sources, indexes, values, weights, hashes, n, schema.KeyKinds.Length, null);
    }

    /// <summary>
    /// One block of whole entries copied from other blocks of <paramref name="schema"/>'s layout
    /// (docs/design-bounded-memory.md §5.35): entry i is <paramref name="sources"/>[i]'s entry
    /// <paramref name="indexes"/>[i], every key and value column copied, nothing decoded, with its
    /// weight and key hash given and its value hash the source's.
    /// </summary>
    public static TraceBlock Copy(BlockSchema schema, TraceBlock[] sources, int[] indexes, long[] weights, int[] hashes, int n)
    {
        var valueHashes = new int[n];
        for (var i = 0; i < n; i++)
        {
            valueHashes[i] = sources[i].ValueHash(indexes[i]);
        }

        return GatherCore(schema, sources, indexes, null, weights, hashes, n, schema.WeightColumn, valueHashes);
    }

    // Columns [0, copied) copied from the sources; with values, the one column after them is that BIGINT.
    private static TraceBlock GatherCore(
        BlockSchema schema, TraceBlock[] sources, int[] indexes, long[]? values, long[] weights, int[] hashes, int n,
        int copied, int[]? valueHashes)
    {
        var keyColumns = copied;
        var columns = schema.HashColumn + 1;

        var nulls = new int[columns];
        var heap = new long[columns];
        var bufferCount = 0;
        for (var c = 0; c < columns; c++)
        {
            var kind = KindOf(schema, c);
            bufferCount += kind == CellKind.Utf8 ? 3 : 2;
            if (c >= keyColumns)
            {
                continue;
            }

            for (var i = 0; i < n; i++)
            {
                var src = sources[i];
                var j = indexes[i];
                var v = src.ValidityAt(c);
                if (v >= 0 && ((src.Bytes[v + (j >> 3)] >> (j & 7)) & 1) == 0)
                {
                    nulls[c]++;
                }

                if (kind == CellKind.Utf8)
                {
                    heap[c] += Offset(src, c, j + 1) - Offset(src, c, j);
                }
            }
        }

        var buffers = new (long Offset, long Length)[bufferCount];
        var validity = new int[columns];
        var valueAt = new int[columns];
        var dataAt = new int[columns];
        long body = 0;
        var b = 0;
        (long, long) Next(long length)
        {
            var at = body;
            body += Pad8(length);
            return (at, length);
        }

        for (var c = 0; c < columns; c++)
        {
            var kind = KindOf(schema, c);
            buffers[b] = Next(nulls[c] > 0 ? (n + 7) / 8 : 0);
            validity[c] = nulls[c] > 0 ? (int)buffers[b].Offset : -1;
            b++;
            buffers[b] = Next(kind switch
            {
                CellKind.Bool => (n + 7) / 8,
                CellKind.Utf8 => (n + 1) * 4L,
                _ => (long)Width(kind) * n,
            });
            valueAt[c] = n == 0 ? -1 : (int)buffers[b].Offset;
            b++;
            if (kind == CellKind.Utf8)
            {
                buffers[b] = Next(heap[c]);
                dataAt[c] = heap[c] > 0 ? (int)buffers[b].Offset : -1;
                b++;
            }
            else
            {
                dataAt[c] = -1;
            }
        }

        var buffersVector = NodesVector + 4 + (16 * columns) + 4;
        var metadata = (int)Pad8(buffersVector + 4 + (16 * bufferCount));
        var bodyStart = Prefix + metadata;
        var message = new byte[checked(bodyStart + body)];
        var span = message.AsSpan();
        BinaryPrimitives.WriteUInt32LittleEndian(span, 0xFFFFFFFF);
        BinaryPrimitives.WriteInt32LittleEndian(span[4..], metadata);
        WriteHeader(span.Slice(Prefix, metadata), n, nulls, buffers, buffersVector, body);
        var out_ = span[bodyStart..];

        for (var c = 0; c < keyColumns && n > 0; c++)
        {
            var kind = KindOf(schema, c);
            var heapAt = 0;
            for (var i = 0; i < n; i++)
            {
                var src = sources[i];
                var bytes = src.Bytes.AsSpan();
                var j = indexes[i];
                if (validity[c] >= 0)
                {
                    var v = src.ValidityAt(c);
                    if (v < 0 || ((bytes[v + (j >> 3)] >> (j & 7)) & 1) != 0)
                    {
                        out_[validity[c] + (i >> 3)] |= (byte)(1 << (i & 7));
                    }
                }

                switch (kind)
                {
                    case CellKind.Bool:
                        if (((bytes[src.ValuesAt(c) + (j >> 3)] >> (j & 7)) & 1) != 0)
                        {
                            out_[valueAt[c] + (i >> 3)] |= (byte)(1 << (i & 7));
                        }

                        break;
                    case CellKind.Utf8:
                        var start = Offset(src, c, j);
                        var length = Offset(src, c, j + 1) - start;
                        if (length > 0)
                        {
                            bytes.Slice(src.DataAt(c) + start, length).CopyTo(out_[(dataAt[c] + heapAt)..]);
                        }

                        heapAt += length;
                        BinaryPrimitives.WriteInt32LittleEndian(out_[(valueAt[c] + ((i + 1) * 4))..], heapAt);
                        break;
                    default:
                        var width = Width(kind);
                        bytes.Slice(src.ValuesAt(c) + (j * width), width).CopyTo(out_[(valueAt[c] + (i * width))..]);
                        break;
                }
            }
        }

        for (var i = 0; i < n; i++)
        {
            if (values is not null)
            {
                BinaryPrimitives.WriteInt64LittleEndian(out_[(valueAt[keyColumns] + (i * 8))..], values[i]);
            }

            BinaryPrimitives.WriteInt64LittleEndian(out_[(valueAt[schema.WeightColumn] + (i * 8))..], weights[i]);
            BinaryPrimitives.WriteInt32LittleEndian(out_[(valueAt[schema.HashColumn] + (i * 4))..], hashes[i]);
        }

        for (var c = 0; c < columns; c++)
        {
            validity[c] = validity[c] < 0 ? -1 : validity[c] + bodyStart;
            valueAt[c] = valueAt[c] < 0 ? -1 : valueAt[c] + bodyStart;
            dataAt[c] = dataAt[c] < 0 ? -1 : dataAt[c] + bodyStart;
        }

        return new TraceBlock(schema, message, n, validity, valueAt, dataAt, valueHashes);
    }

    private static CellKind KindOf(BlockSchema schema, int c) =>
        c < schema.KeyKinds.Length ? schema.KeyKinds[c]
        : c < schema.WeightColumn ? schema.ValueKinds[c - schema.KeyKinds.Length]
        : c == schema.WeightColumn ? CellKind.Int64
        : CellKind.Int32;

    private static (StructuralRow[] Rows, int Column) Source(BlockSchema schema, StructuralRow[] keys, StructuralRow[] values, int c) =>
        c < schema.KeyKinds.Length ? (keys, c) : (values, c - schema.KeyKinds.Length);

    private static int Width(CellKind kind) => kind switch
    {
        CellKind.Int32 or CellKind.Float32 or CellKind.Date32 => 4,
        CellKind.Decimal128 => 16,
        _ => 8,
    };

    private static long Pad8(long length) => (length + 7) & ~7L;

    private static void WriteColumn(
        Span<byte> body, CellKind kind, StructuralRow[] rows, int column, int n, int validity, int values, int data)
    {
        var offset = 0;
        if (kind == CellKind.Utf8)
        {
            BinaryPrimitives.WriteInt32LittleEndian(body[values..], 0);
        }

        for (var i = 0; i < n; i++)
        {
            var cell = rows[i][column];
            if (cell is null)
            {
                if (kind == CellKind.Utf8)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(body[(values + ((i + 1) * 4))..], offset);
                }

                continue;
            }

            if (validity >= 0)
            {
                body[validity + (i >> 3)] |= (byte)(1 << (i & 7));
            }

            switch (kind)
            {
                case CellKind.Int32: BinaryPrimitives.WriteInt32LittleEndian(body[(values + (i * 4))..], (int)cell); break;
                case CellKind.Int64: BinaryPrimitives.WriteInt64LittleEndian(body[(values + (i * 8))..], (long)cell); break;
                case CellKind.Float32: BinaryPrimitives.WriteSingleLittleEndian(body[(values + (i * 4))..], (float)cell); break;
                case CellKind.Float64: BinaryPrimitives.WriteDoubleLittleEndian(body[(values + (i * 8))..], (double)cell); break;
                case CellKind.Date32: BinaryPrimitives.WriteInt32LittleEndian(body[(values + (i * 4))..], ((Date32)cell).Days); break;
                case CellKind.Time64: BinaryPrimitives.WriteInt64LittleEndian(body[(values + (i * 8))..], ((Time64)cell).Microseconds); break;
                case CellKind.Timestamp: BinaryPrimitives.WriteInt64LittleEndian(body[(values + (i * 8))..], ((Timestamp)cell).Microseconds); break;
                case CellKind.Decimal128:
                    var mantissa = ((Decimal128)cell).Mantissa;
                    MemoryMarshal.Write(body[(values + (i * 16))..], in mantissa);
                    break;
                case CellKind.Bool:
                    if ((bool)cell)
                    {
                        body[values + (i >> 3)] |= (byte)(1 << (i & 7));
                    }

                    break;
                default:
                    var bytes = ((Utf8String)cell).Span;
                    if (bytes.Length > 0)
                    {
                        bytes.CopyTo(body[(data + offset)..]);
                    }

                    offset += bytes.Length;
                    BinaryPrimitives.WriteInt32LittleEndian(body[(values + ((i + 1) * 4))..], offset);
                    break;
            }
        }
    }

    // The FlatBuffers Message for a RecordBatch, at fixed positions (all offsets forward, every
    // scalar aligned to its size):
    //   0  root offset → Message table (16)
    //   4  Message vtable: size 12, table size 24, fields version@4 header_type@6 header@8 bodyLength@16
    //  16  Message table: soffset 12, version V5 (4), header_type RecordBatch (3), header → 56, bodyLength
    //  40  RecordBatch vtable: size 10, table size 24, fields length@8 nodes@4 buffers@16
    //  56  RecordBatch table: soffset 16, nodes → 84, length, buffers → buffersVector
    //  84  nodes vector: count, then 16-byte FieldNodes (length, null_count) from 88
    //      buffers vector: count at buffersVector, then 16-byte Buffers (offset, length)
    private static void WriteHeader(
        Span<byte> fb, int n, int[] nulls, (long Offset, long Length)[] buffers, int buffersVector, long bodyLength)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(fb, MessageTable);

        BinaryPrimitives.WriteUInt16LittleEndian(fb[4..], 12);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[6..], 24);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[8..], 4);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[10..], 6);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[12..], 8);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[14..], 16);

        BinaryPrimitives.WriteInt32LittleEndian(fb[16..], MessageTable - 4);
        BinaryPrimitives.WriteInt16LittleEndian(fb[20..], 4);
        fb[22] = 3;
        BinaryPrimitives.WriteUInt32LittleEndian(fb[24..], RecordBatchTable - 24);
        BinaryPrimitives.WriteInt64LittleEndian(fb[32..], bodyLength);

        BinaryPrimitives.WriteUInt16LittleEndian(fb[40..], 10);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[42..], 24);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[44..], 8);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[46..], 4);
        BinaryPrimitives.WriteUInt16LittleEndian(fb[48..], 16);

        BinaryPrimitives.WriteInt32LittleEndian(fb[RecordBatchTable..], RecordBatchTable - 40);
        BinaryPrimitives.WriteUInt32LittleEndian(fb[(RecordBatchTable + 4)..], (uint)(NodesVector - (RecordBatchTable + 4)));
        BinaryPrimitives.WriteInt64LittleEndian(fb[(RecordBatchTable + 8)..], n);
        BinaryPrimitives.WriteUInt32LittleEndian(fb[(RecordBatchTable + 16)..], (uint)(buffersVector - (RecordBatchTable + 16)));

        BinaryPrimitives.WriteUInt32LittleEndian(fb[NodesVector..], (uint)nulls.Length);
        for (var c = 0; c < nulls.Length; c++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(fb[(NodesVector + 4 + (c * 16))..], n);
            BinaryPrimitives.WriteInt64LittleEndian(fb[(NodesVector + 12 + (c * 16))..], nulls[c]);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(fb[buffersVector..], (uint)buffers.Length);
        for (var i = 0; i < buffers.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(fb[(buffersVector + 4 + (i * 16))..], buffers[i].Offset);
            BinaryPrimitives.WriteInt64LittleEndian(fb[(buffersVector + 12 + (i * 16))..], buffers[i].Length);
        }
    }
}
