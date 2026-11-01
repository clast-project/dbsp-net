// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// What a run needs from a Z-set trace codec: the Arrow schema every block shares, and a block
/// encoded straight from rows already in hash order — no intermediate Z-set
/// (docs/design-durable-state.md §1f, increment 2 write fix).
/// </summary>
internal interface IZSetRunCodec
{
    /// <summary>The schema of every block: the row's columns, <c>__weight : Int64</c>, then
    /// <c>__hash : Int32</c> (each entry's order hash, ascending — see <see cref="RunFile"/>).</summary>
    ArrowSchema RunSchema { get; }

    /// <summary>One block: <paramref name="count"/> rows, their weights and order hashes.</summary>
    RecordBatch EncodeBlock(StructuralRow[] rows, long[] weights, int[] hashes, int count);

    /// <summary>Add every row of a decoded block to <paramref name="into"/>.</summary>
    void DecodeBlock(RecordBatch batch, ZSetBuilder<StructuralRow, Z64> into);

    /// <summary>Add rows <c>[from, from + count)</c> of a decoded block to <paramref name="into"/>.</summary>
    void DecodeRange(RecordBatch batch, int from, int count, ZSetBuilder<StructuralRow, Z64> into);
}

/// <summary>The indexed-trace twin of <see cref="IZSetRunCodec"/>: a block row is (key, value, weight).</summary>
internal interface IIndexedRunCodec
{
    /// <summary>The schema of every block: key columns, value columns, <c>__weight : Int64</c>,
    /// <c>__hash : Int32</c> (the key's order hash).</summary>
    ArrowSchema RunSchema { get; }

    RecordBatch EncodeBlock(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count);

    void DecodeBlock(RecordBatch batch, IndexedZSetBuilder<StructuralRow, StructuralRow, Z64> into);

    void DecodeRange(RecordBatch batch, int from, int count, IndexedZSetBuilder<StructuralRow, StructuralRow, Z64> into);
}
