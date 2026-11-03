// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// One immutable, reference-free block of an indexed trace (docs/design-flat-trace-state.md §2.2):
/// entries in ascending unsigned key-hash order (a giant group's hash can continue into the next
/// block of its run). Core reads it
/// through this interface; the encoding (the v3 run block) lives in DbspNet.Persistence, which
/// knows Arrow and the SQL cell types.
/// </summary>
internal interface ITraceBlock
{
    /// <summary>The block with its bytes in memory, to read many times: itself, or a file-backed
    /// block's loaded body (docs/design-bounded-memory.md §3.1) — which a cache may otherwise drop
    /// between two reads.</summary>
    ITraceBlock Body => this;

    /// <summary>Entries in the block.</summary>
    int Count { get; }

    /// <summary>The smallest key hash (unsigned).</summary>
    uint FirstHash { get; }

    /// <summary>The largest key hash (unsigned).</summary>
    uint LastHash { get; }

    /// <summary>Each entry's key hash, ascending as unsigned.</summary>
    ReadOnlySpan<uint> Hashes { get; }

    /// <summary>Entry <paramref name="i"/>'s weight.</summary>
    long Weight(int i);

    /// <summary>The first entry whose key hash is not below <paramref name="hash"/>.</summary>
    int LowerBound(uint hash);

    /// <summary>Whether entry <paramref name="i"/>'s key equals <paramref name="key"/> cell by cell
    /// (<c>object.Equals</c>), without decoding it.</summary>
    bool KeyEquals(int i, StructuralRow key);

    /// <summary>Entry <paramref name="i"/>'s value row hash (<see cref="StructuralRow.GetHashCode"/>
    /// of its decoded value), without decoding it on the step path: written at freeze, computed
    /// once for a block restored from a file.</summary>
    int ValueHash(int i);

    /// <summary>Whether entry <paramref name="i"/>'s value equals <paramref name="value"/> cell by
    /// cell (<c>object.Equals</c>), without decoding it. Row equality also needs equal hashes —
    /// compare <see cref="ValueHash"/> first.</summary>
    bool ValueEquals(int i, StructuralRow value);

    /// <summary>Entry <paramref name="i"/>'s key, decoded (strings copied).</summary>
    StructuralRow Key(int i);

    /// <summary>Entry <paramref name="i"/>'s key, decoded with its strings aliasing the block's bytes
    /// (immutable, so safe; the row keeps the block's bytes alive while it lives).</summary>
    StructuralRow KeyAliased(int i);

    /// <summary>Every entry's key, as <see cref="KeyAliased"/>, decoded a column at a time so each
    /// column's cells are allocated together — what a column-wise reader (an Arrow write) reads fastest.</summary>
    StructuralRow[] KeysAliased();

    /// <summary>Entry <paramref name="i"/>'s value, decoded (strings copied).</summary>
    StructuralRow Value(int i);

    /// <summary>Entry <paramref name="i"/>'s value cell <paramref name="column"/>, boxed (strings
    /// copied) — one cell, for a predicate that reads a few.</summary>
    object? ValueCell(int i, int column);

    /// <summary>Box entry <paramref name="i"/>'s value cells into <paramref name="destination"/>
    /// from <paramref name="at"/> (strings copied).</summary>
    void ValueCellsInto(int i, object?[] destination, int at);
}

/// <summary>Encodes a trace's entries as <see cref="ITraceBlock"/>s: one per trace, carrying its
/// key and value schema.</summary>
internal interface ITraceBlockFormat
{
    /// <summary>Columns in a value row.</summary>
    int ValueColumns { get; }

    /// <summary>Writes a trace of this format as a run chain only a block cache reads back
    /// (docs/design-bounded-memory.md §3.7); null if the format cannot.</summary>
    IBlockedTraceCodec? BackingCodec => null;

    /// <summary>About how many bytes one entry adds to a block (fixed widths plus string bytes), so
    /// blocks can be cut below the large-object-heap threshold.</summary>
    int EntryBytes(StructuralRow key, StructuralRow value);

    /// <summary>Encode entries <c>[0, count)</c> — ascending unsigned <paramref name="hashes"/>,
    /// each the hash of its key — as one block.</summary>
    ITraceBlock Encode(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count);
}

/// <summary>Checkpoints a <see cref="BlockedIndexedTrace"/> as run files written from its blocks and
/// restores it by adopting them (docs/design-flat-trace-state.md §2.6–§2.7). Implemented in
/// DbspNet.Persistence by the trace's ordinary snapshot codec.</summary>
internal interface IBlockedTraceCodec
{
    ValueTask SaveBlockedAsync(Circuit.ISnapshotWriter writer, string fileName, BlockedIndexedTrace trace, CancellationToken cancellationToken);

    ValueTask LoadBlockedAsync(Circuit.ISnapshotReader reader, string fileName, BlockedIndexedTrace trace, CancellationToken cancellationToken);

    /// <summary>Write <paramref name="trace"/> as run files a block cache can evict to, and nothing
    /// else: a no-op unless the snapshot layout is runs. Restore never reads it.</summary>
    ValueTask SaveBackingAsync(Circuit.ISnapshotWriter writer, string fileName, BlockedIndexedTrace trace, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;

    /// <summary>Write <paramref name="blocks"/> (in any hash order) to one backing file; once the
    /// checkpoint commits, <paramref name="onCommit"/> receives their file-backed twins, in order. A
    /// no-op unless the snapshot layout is runs.</summary>
    ValueTask SaveBackingBlocksAsync(
        Circuit.ISnapshotWriter writer, string fileName, IReadOnlyList<ITraceBlock> blocks, Action<ITraceBlock[]> onCommit, CancellationToken cancellationToken) =>
        ValueTask.CompletedTask;
}
