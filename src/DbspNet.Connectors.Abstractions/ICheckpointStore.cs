// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text.Json;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Persistence;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// The recovered checkpoint: the engine tick it was taken at and each source's
/// committed offset at that tick.
/// </summary>
public sealed record CheckpointState(long Tick, IReadOnlyList<SourceCheckpoint> Offsets);

/// <summary>
/// Persists engine state and per-source offsets together so that, on recovery,
/// engine-tick T and each source's offset restore consistently — the invariant that
/// gives at-least-once (exactly-once with an idempotent sink). See
/// <c>docs/design-connectors.md</c>.
/// </summary>
public interface ICheckpointStore
{
    /// <summary>Snapshot the circuit's state and record <paramref name="offsets"/> as
    /// consumed through <c>circuit.TickCount</c>.</summary>
    ValueTask SaveAsync(RootCircuit circuit, IReadOnlyList<SourceCheckpoint> offsets, CancellationToken cancellationToken);

    /// <summary>Restore the circuit to the last committed checkpoint and return its tick
    /// + offsets, or <c>null</c> if no checkpoint exists (a fresh start).</summary>
    ValueTask<CheckpointState?> TryRestoreAsync(RootCircuit circuit, CancellationToken cancellationToken);
}

/// <summary>
/// <see cref="ICheckpointStore"/> backed by the engine's <see cref="Snapshot"/>. The
/// per-source offsets ride in the snapshot's <b>manifest metadata</b> (a single JSON
/// value under <see cref="OffsetsMetadataKey"/>), so they commit <b>atomically</b> with
/// the operator state — the manifest is written before the <c>current.txt</c> pointer
/// that makes the snapshot visible. Engine-tick T and the source offsets can therefore
/// never diverge (the exactly-once alignment invariant): a crash mid-checkpoint leaves
/// the prior snapshot intact, offsets and all.
/// </summary>
public sealed class SnapshotCheckpointStore : ICheckpointStore
{
    /// <summary>Manifest-metadata key under which the offsets JSON is stored.</summary>
    public const string OffsetsMetadataKey = "connector.offsets";

    private readonly ITableFileSystem _fs;
    private readonly int _retainCount;

    public SnapshotCheckpointStore(ITableFileSystem fs, int retainCount = 1)
    {
        _fs = fs ?? throw new ArgumentNullException(nameof(fs));
        _retainCount = retainCount;
    }

    public async ValueTask SaveAsync(
        RootCircuit circuit, IReadOnlyList<SourceCheckpoint> offsets, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(offsets);

        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [OffsetsMetadataKey] = JsonSerializer.Serialize(offsets),
        };

        await Snapshot.WriteAsync(circuit, _fs, metadata, _retainCount, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<CheckpointState?> TryRestoreAsync(RootCircuit circuit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);

        if (!await Snapshot.ExistsAsync(_fs, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        await Snapshot.ReadAsync(circuit, _fs, cancellationToken).ConfigureAwait(false);

        var metadata = await Snapshot.ReadMetadataAsync(_fs, cancellationToken).ConfigureAwait(false);
        if (metadata is null || !metadata.TryGetValue(OffsetsMetadataKey, out var json))
        {
            // A snapshot without connector offsets (e.g. one written by a non-connector
            // path) — engine state restores, but there is no source cursor to resume.
            return new CheckpointState(circuit.TickCount, Array.Empty<SourceCheckpoint>());
        }

        var offsets = JsonSerializer.Deserialize<SourceCheckpoint[]>(json)
            ?? throw new InvalidDataException("connector.offsets metadata deserialized to null");
        return new CheckpointState(circuit.TickCount, offsets);
    }
}
