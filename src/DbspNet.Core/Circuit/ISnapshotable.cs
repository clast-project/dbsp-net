// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.IO;

namespace DbspNet.Core.Circuit;

/// <summary>
/// Optional snapshot contract for operators that hold state across ticks.
/// The persistence layer enumerates the circuit's operators, picks out
/// those implementing this interface, and orchestrates serialisation /
/// restoration through the supplied <see cref="ISnapshotWriter"/> /
/// <see cref="ISnapshotReader"/> contexts.
/// </summary>
/// <remarks>
/// Operator identity is positional: the snapshot is keyed by the
/// operator's index in <c>RootCircuit.Operators</c>. A plan fingerprint
/// stored alongside the snapshot guards against structural drift —
/// adding, removing, or reordering operators changes the fingerprint and
/// makes old snapshots refuse to load.
/// </remarks>
public interface ISnapshotable
{
    /// <summary>Persist the operator's state through <paramref name="writer"/>.</summary>
    ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default);

    /// <summary>Restore the operator's state from <paramref name="reader"/>.</summary>
    ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stable hash of the operator's row/key/value schemas, derived from
    /// its snapshot codec(s). The snapshot manifest aggregates these
    /// across the circuit so a load can detect schema drift that the
    /// operator-type fingerprint alone wouldn't see — e.g. VARCHAR
    /// length changes (Arrow's StringType carries no length), DECIMAL
    /// precision/scale changes, or column reorders that leave operator
    /// generic args unchanged. Operators without snapshot codecs return
    /// the empty string; their <see cref="SaveAsync"/> already throws.
    /// </summary>
    string SchemaFingerprint { get; }
}

/// <summary>
/// Output side of a snapshot context. Implementations are typically
/// directory-backed (one subdirectory per operator); operators open
/// named files via <see cref="CreateAsync"/> and write whatever format
/// is natural for their state. A small operator might write a single
/// <c>"state.json"</c>; a trace-backed operator might write
/// <c>"keys.arrows"</c>, <c>"values.arrows"</c>, and <c>"manifest.json"</c>.
/// </summary>
public interface ISnapshotWriter
{
    /// <summary>
    /// Create a new file for writing one named artifact. Caller disposes
    /// the returned <see cref="ISequentialFile"/>.
    /// </summary>
    ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default);
}

/// <summary>
/// Input side of a snapshot context. Mirror of <see cref="ISnapshotWriter"/>.
/// </summary>
public interface ISnapshotReader
{
    /// <summary>
    /// Open a file for random-access reading. Caller disposes the returned
    /// <see cref="IRandomAccessFile"/>.
    /// </summary>
    ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default);

    /// <summary>True if a file with the given name exists in this snapshot.</summary>
    ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default);
}
