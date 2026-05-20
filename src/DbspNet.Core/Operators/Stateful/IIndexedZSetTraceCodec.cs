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
}
