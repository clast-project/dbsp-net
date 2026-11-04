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

/// <summary>A full write's Arrow batches, their entries, and the native memory their buffers live in:
/// dispose once the sink has written them (docs/design-bounded-memory.md §3h).</summary>
internal sealed class ViewBatches(List<RecordBatch> batches, ViewEntries entries, ViewArrow.NativeBuffers memory) : IDisposable
{
    public List<RecordBatch> Batches { get; } = batches;

    public ViewEntries Entries { get; } = entries;

    /// <summary>Free the buffers. The batches must not be read after.</summary>
    public void Dispose() => memory.Dispose();
}

/// <summary>
/// A stored view's full contents as Arrow batches built from its blocks (docs/design-bounded-memory.md
/// §3h): a block's columns already are Arrow buffers, so each output column is copied from them, a
/// run of consecutive entries at a time, and no row is decoded. Each worker's blocks are read in
/// turn; a row a worker holds n times is written n times.
/// </summary>
internal static class ViewArrow
{
    /// <summary>Rows per batch (<c>DBSPNET_OUTPUT_BATCH_ROWS</c>). Large: engineered-wood writes
    /// one big batch with less than half the allocation of the same rows in 64K-row batches (§3h).</summary>
    internal static readonly int BatchRows =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_OUTPUT_BATCH_ROWS"), out var rows) && rows > 0 ? rows : 1 << 20;

    /// <summary>
    /// The batches and their entries, or null when the view cannot be read this way: a worker's
    /// trace has a managed level or a GC floor, a block is not an in-memory body of the expected
    /// layout, or a row's count is negative on some worker (the workers' integrals must then be
    /// summed, which takes rows).
    /// </summary>
    public static ViewBatches? TryBuild(
        Schema schema, IReadOnlyList<BlockedIndexedTrace> shards, int batchRows = 0)
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

        batchRows = batchRows > 0 ? batchRows : BatchRows;
        var all = new ViewEntries([.. blocks], [.. indexes]);
        blocks.Clear();
        indexes.Clear();
        var batches = new List<RecordBatch>();
        var memory = new NativeBuffers();
        for (var from = 0; from < all.Count; from += batchRows)
        {
            var to = Math.Min(all.Count, from + batchRows);
            var arrays = new IArrowArray[schema.FieldsList.Count];
            for (var c = 0; c < arrays.Length; c++)
            {
                if (Column(schema.FieldsList[c].DataType, all, c, from, to, memory) is not { } array)
                {
                    memory.Dispose();
                    return null;
                }

                arrays[c] = array;
            }

            batches.Add(new RecordBatch(schema, arrays, to - from));
        }

        return new ViewBatches(batches, all, memory);
    }

    /// <summary>
    /// Whether output buffers come from Arrow's native allocator (docs/design-bounded-memory.md §3h).
    /// Under a GC heap hard limit native memory is outside the limit, as Arrow's own builders'
    /// buffers are, and a 10 GiB limit holds only so (run39). Without one, managed buffers reuse the
    /// heap the GC already holds, and native ones cost ~1 GB of peak RSS (run40, run41). So: native
    /// when a heap hard limit is set; <c>DBSPNET_OUTPUT_NATIVE=on|off</c> overrides.
    /// </summary>
    internal static bool Native => NativeOverride.Value ?? DefaultNative;

    private static readonly bool DefaultNative = Environment.GetEnvironmentVariable("DBSPNET_OUTPUT_NATIVE") switch
    {
        "on" => true,
        "off" => false,
        _ => HeapHardLimitSet(),
    };

    private static readonly AsyncLocal<bool?> NativeOverride = new();

    /// <summary><see cref="Native"/> in the current async context until disposed (tests).</summary>
    internal static IDisposable UseNative(bool native)
    {
        var previous = NativeOverride.Value;
        NativeOverride.Value = native;
        return new Restore(() => NativeOverride.Value = previous);
    }

    private sealed class Restore(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }

    // A GC heap hard limit, as the runtime takes it: DOTNET_ variables or runtimeconfig settings.
    private static bool HeapHardLimitSet()
    {
        foreach (var name in (string[])["GCHeapHardLimit", "GCHeapHardLimitPercent"])
        {
            if (Environment.GetEnvironmentVariable("DOTNET_" + name) is { Length: > 0 } || Environment.GetEnvironmentVariable("COMPlus_" + name) is { Length: > 0 })
            {
                return true;
            }
        }

        return AppContext.GetData("GCHeapHardLimit") is not null || AppContext.GetData("System.GC.HeapHardLimit") is not null
            || AppContext.GetData("GCHeapHardLimitPercent") is not null || AppContext.GetData("System.GC.HeapHardLimitPercent") is not null;
    }

    /// <summary>
    /// The output's buffers: from Arrow's native allocator when <see cref="Native"/> (freed together
    /// as soon as the sink has written the batches), otherwise managed arrays.
    /// </summary>
    internal sealed class NativeBuffers : IDisposable
    {
        private readonly List<System.Buffers.IMemoryOwner<byte>> _owners = [];
        private readonly bool _native = Native;

        public Memory<byte> Allocate(long length)
        {
            if (!_native)
            {
                return new byte[length];
            }

            var owner = Apache.Arrow.Memory.MemoryAllocator.Default.Value.Allocate(checked((int)length));
            _owners.Add(owner);
            var memory = owner.Memory[..(int)length];
            memory.Span.Clear();
            return memory;
        }

        public void Dispose()
        {
            foreach (var owner in _owners)
            {
                owner.Dispose();
            }

            _owners.Clear();
        }
    }

    // Column c of entries [from, to), or null when its type is not the block's kind.
    private static IArrowArray? Column(IArrowType type, ViewEntries entries, int c, int from, int to, NativeBuffers memory)
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

        var (validity, nulls) = Validity(entries, c, from, to, memory);
        ArrowBuffer[] buffers;
        if (width > 0)
        {
            var values = memory.Allocate((long)n * width);
            ForRuns(entries, from, to, (block, first, count, at) =>
                block.Bytes.AsSpan(block.ValuesAt(c) + (first * width), count * width).CopyTo(values.Span[(at * width)..]));
            buffers = [validity, new ArrowBuffer(values)];
        }
        else if (width < 0)
        {
            var bits = memory.Allocate((n + 7) / 8);
            var span = bits.Span;
            for (var i = 0; i < n; i++)
            {
                var block = entries.Blocks[from + i];
                var j = entries.Indexes[from + i];
                if ((block.Bytes[block.ValuesAt(c) + (j >> 3)] & (1 << (j & 7))) != 0)
                {
                    span[i >> 3] |= (byte)(1 << (i & 7));
                }
            }

            buffers = [validity, new ArrowBuffer(bits)];
        }
        else
        {
            var offsets = memory.Allocate((n + 1) * 4L);
            long heap = 0;
            for (var i = 0; i < n; i++)
            {
                var block = entries.Blocks[from + i];
                var j = entries.Indexes[from + i];
                heap += Offset(block, c, j + 1) - Offset(block, c, j);
            }

            var data = memory.Allocate(heap);
            var position = 0;
            ForRuns(entries, from, to, (block, first, count, at) =>
            {
                var start = Offset(block, c, first);
                var length = Offset(block, c, first + count) - start;
                if (length > 0)
                {
                    block.Bytes.AsSpan(block.DataAt(c) + start, length).CopyTo(data.Span[position..]);
                }

                for (var k = 1; k <= count; k++)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(offsets.Span[((at + k) * 4)..], position + Offset(block, c, first + k) - start);
                }

                position += length;
            });
            buffers = [validity, new ArrowBuffer(offsets), new ArrowBuffer(data)];
        }

        return ArrowArrayFactory.BuildArray(new ArrayData(type, n, nulls, 0, buffers));
    }

    // The validity bitmap of column c over entries [from, to): empty when nothing is null.
    private static (ArrowBuffer Bitmap, int Nulls) Validity(ViewEntries entries, int c, int from, int to, NativeBuffers memory)
    {
        var n = to - from;
        Memory<byte>? bits = null;
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
                bits = memory.Allocate((n + 7) / 8);
                bits.Value.Span.Fill(0xFF);
            }

            bits.Value.Span[i >> 3] &= (byte)~(1 << (i & 7));
            nulls++;
        }

        return (bits is { } b ? new ArrowBuffer(b) : ArrowBuffer.Empty, nulls);
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
