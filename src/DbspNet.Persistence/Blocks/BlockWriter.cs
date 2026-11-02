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

        return new TraceBlock(schema, message, n, validity, valueAt, dataAt);
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
