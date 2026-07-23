// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Per-trace serialisation contract for state-bearing operators that hold
/// an <see cref="IndexedZSet{TKey,TValue,TWeight}"/>. Counterpart to
/// <see cref="IZSetTraceCodec{TKey,TWeight}"/> for grouped traces — used
/// by <c>IncrementalAggregateOp</c>, where the trace is keyed by the
/// GROUP BY columns and each per-key Z-set is the multiset of input rows
/// in that group.
/// </summary>
public interface IIndexedZSetTraceCodec<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>
    /// Persist every <c>(key, value, weight)</c> triple in the running
    /// indexed trace through <paramref name="writer"/> under
    /// <paramref name="fileName"/>. The filename is operator-chosen —
    /// operators with a single trace pass <c>"trace.arrows"</c>;
    /// operators with multiple (e.g. join's left/right) pass
    /// disambiguating names.
    /// </summary>
    ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        IndexedZSet<TKey, TValue, TWeight> trace,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Read the saved entries from <paramref name="fileName"/> and return
    /// them as an indexed Z-set ready to be folded into a fresh trace
    /// via <c>IndexedZSetTrace.Integrate</c>.
    /// </summary>
    ValueTask<IndexedZSet<TKey, TValue, TWeight>> LoadAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Stable hash of the codec's key + value schemas. See
    /// <see cref="IZSetTraceCodec{TKey,TWeight}.SchemaFingerprint"/> —
    /// same role, just over a (key schema, value schema) pair.
    /// </summary>
    string SchemaFingerprint { get; }

    /// <summary>
    /// True when this codec can persist an opaque byte blob per key alongside
    /// the trace. Default <c>false</c>: a codec that does not implement it simply
    /// forces its operator down whatever reconstruction path it used before.
    /// </summary>
    /// <remarks>
    /// <para>This exists because per-key derived state cannot be persisted by the
    /// operator alone — the operator is generic in <typeparamref name="TKey"/> and
    /// only the codec knows how to encode one. <c>IncrementalAggregateOp</c> is the
    /// motivating consumer: it must persist each group's aggregator scratch state,
    /// because rebuilding that state by re-folding the restored trace does not
    /// reproduce what an uninterrupted run held whenever the fold is not associative
    /// (float SUM/AVG/STDDEV). See <c>docs/design-incremental-persistence.md</c> §7.2.</para>
    /// <para>The blob is opaque to the codec: it round-trips bytes and nothing more.
    /// What is inside them, and whether they are still meaningful, is the caller's
    /// problem.</para>
    /// </remarks>
    bool SupportsKeyedBlobs => false;

    /// <summary>
    /// Persist <paramref name="entries"/> as (key, blob) pairs under
    /// <paramref name="fileName"/>. Keys need not appear in the trace, and order
    /// is not significant — <see cref="LoadKeyedBlobsAsync"/> is free to return
    /// them in any order.
    /// </summary>
    ValueTask SaveKeyedBlobsAsync(
        ISnapshotWriter writer,
        string fileName,
        IReadOnlyList<(TKey Key, byte[] Blob)> entries,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            $"{GetType().Name} does not support keyed blobs; check SupportsKeyedBlobs first.");

    /// <summary>
    /// Read back what <see cref="SaveKeyedBlobsAsync"/> wrote. Returns an empty
    /// list when <paramref name="fileName"/> does not exist, so a snapshot taken
    /// before the operator started writing blobs loads as "no blobs" rather than
    /// failing.
    /// </summary>
    ValueTask<IReadOnlyList<(TKey Key, byte[] Blob)>> LoadKeyedBlobsAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            $"{GetType().Name} does not support keyed blobs; check SupportsKeyedBlobs first.");
}
