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

    /// <summary>Entry <paramref name="i"/>'s key, decoded (strings copied).</summary>
    StructuralRow Key(int i);

    /// <summary>Entry <paramref name="i"/>'s value, decoded (strings copied).</summary>
    StructuralRow Value(int i);

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

    /// <summary>About how many bytes one entry adds to a block (fixed widths plus string bytes), so
    /// blocks can be cut below the large-object-heap threshold.</summary>
    int EntryBytes(StructuralRow key, StructuralRow value);

    /// <summary>Encode entries <c>[0, count)</c> — ascending unsigned <paramref name="hashes"/>,
    /// each the hash of its key — as one block.</summary>
    ITraceBlock Encode(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count);
}
