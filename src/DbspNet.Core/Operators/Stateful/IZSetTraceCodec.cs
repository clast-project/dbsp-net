// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Per-trace serialisation contract for state-bearing operators that hold
/// a <see cref="ZSetTrace{TKey,TWeight}"/>. The trace itself is generic
/// over <typeparamref name="TKey"/> and <typeparamref name="TWeight"/>; the
/// concrete codec injected at construction is responsible for serialising
/// keys and weights in the format the storage layer expects (e.g. Arrow
/// IPC for <see cref="DbspNet.Core.Collections.StructuralRow"/>-keyed
/// traces in the SQL layer).
/// </summary>
public interface IZSetTraceCodec<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>
    /// Persist every <c>(key, weight)</c> entry in the running trace
    /// through <paramref name="writer"/> under <paramref name="fileName"/>.
    /// The filename is operator-chosen — operators that hold a single
    /// trace pass <c>"trace.arrows"</c>; operators that hold multiple
    /// (e.g. join's left/right) pass disambiguating names.
    /// </summary>
    ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        ZSet<TKey, TWeight> trace,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Read the saved entries from <paramref name="fileName"/> and return
    /// them as a Z-set ready to be folded into a fresh trace via
    /// <c>ZSetTrace.Integrate</c>.
    /// </summary>
    ValueTask<ZSet<TKey, TWeight>> LoadAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Stable hash of the codec's schema(s). Operators concatenate their
    /// codecs' fingerprints into <see cref="ISnapshotable"/>'s
    /// <c>SchemaFingerprint</c>; the snapshot manifest aggregates that
    /// across the circuit so a load can detect schema drift that
    /// wouldn't show up in the operator-type fingerprint (e.g. VARCHAR
    /// length change, intermediate column reorder). Implementations
    /// derive the hash from column names + types only — independent of
    /// row data.
    /// </summary>
    string SchemaFingerprint { get; }
}
