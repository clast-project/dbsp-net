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
/// <see cref="ICheckpointStore"/> for a data-parallel program: all <c>W</c> replicas and the
/// per-source offsets commit as one checkpoint.
/// </summary>
public interface IParallelCheckpointStore
{
    /// <summary>Snapshot every replica and record <paramref name="offsets"/> as consumed through
    /// <c>circuit.TickCount</c>, atomically.</summary>
    ValueTask SaveAsync(ParallelCircuit circuit, IReadOnlyList<SourceCheckpoint> offsets, CancellationToken cancellationToken);

    /// <summary>Restore every replica to the last committed checkpoint and return its tick +
    /// offsets, or <c>null</c> if no checkpoint exists (a fresh start).</summary>
    ValueTask<CheckpointState?> TryRestoreAsync(ParallelCircuit circuit, CancellationToken cancellationToken);
}

/// <summary>
/// <see cref="ICheckpointStore"/> backed by the engine's <see cref="Snapshot"/>. The
/// per-source offsets ride in the snapshot's <b>manifest metadata</b> (a single JSON
/// value under <see cref="OffsetsMetadataKey"/>), so they commit <b>atomically</b> with
/// the operator state — the manifest is written before the <c>current.txt</c> pointer
/// that makes the snapshot visible. Engine-tick T and the source offsets can therefore
/// never diverge (the exactly-once alignment invariant): a crash mid-checkpoint leaves
/// the prior snapshot intact, offsets and all.
/// <para>
/// It also stores parallel programs (<see cref="IParallelCheckpointStore"/>) through
/// <see cref="ParallelSnapshot"/>, with the offsets in the one <c>parallel.json</c> that commits
/// every worker. A checkpoint is restorable only at the worker count it was written with;
/// <see cref="RecordedWorkersAsync"/> reports it.
/// </para>
/// </summary>
public sealed class SnapshotCheckpointStore : ICheckpointStore, IParallelCheckpointStore
{
    /// <summary>Manifest-metadata key under which the offsets JSON is stored.</summary>
    public const string OffsetsMetadataKey = "connector.offsets";

    private readonly ITableFileSystem _fs;
    private readonly int _retainCount;

    /// <param name="fs">Where checkpoints live.</param>
    /// <param name="retainCount">Committed checkpoints to keep. Default 2 (Feldera keeps two): the
    /// previous one survives until the next is durable and committed.</param>
    public SnapshotCheckpointStore(ITableFileSystem fs, int retainCount = 2)
    {
        _fs = fs ?? throw new ArgumentNullException(nameof(fs));
        _retainCount = retainCount;
    }

    /// <summary>The worker count of the current checkpoint (1 for a serial program), or
    /// <c>null</c> when none has been written.</summary>
    public ValueTask<int?> RecordedWorkersAsync(CancellationToken cancellationToken = default) =>
        ParallelSnapshot.RecordedWorkersAsync(_fs, cancellationToken);

    public async ValueTask SaveAsync(
        RootCircuit circuit, IReadOnlyList<SourceCheckpoint> offsets, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        await Snapshot.WriteAsync(circuit, _fs, OffsetsMetadata(offsets), _retainCount, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask SaveAsync(
        ParallelCircuit circuit, IReadOnlyList<SourceCheckpoint> offsets, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        await ParallelSnapshot.WriteAsync(circuit, _fs, OffsetsMetadata(offsets), _retainCount, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask<CheckpointState?> TryRestoreAsync(ParallelCircuit circuit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);

        // As for the serial program: only a missing pointer is a fresh start.
        if (!await Snapshot.PointerExistsAsync(_fs, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        await ParallelSnapshot.ReadAsync(circuit, _fs, cancellationToken).ConfigureAwait(false);
        var metadata = await ParallelSnapshot.ReadMetadataAsync(_fs, cancellationToken).ConfigureAwait(false);
        return new CheckpointState(circuit.TickCount, ParseOffsets(metadata));
    }

    private static Dictionary<string, string> OffsetsMetadata(IReadOnlyList<SourceCheckpoint> offsets)
    {
        ArgumentNullException.ThrowIfNull(offsets);
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [OffsetsMetadataKey] = JsonSerializer.Serialize(offsets),
        };
    }

    // A snapshot without connector offsets (e.g. one written by a non-connector path): engine
    // state restores, but there is no source cursor to resume.
    private static SourceCheckpoint[] ParseOffsets(IReadOnlyDictionary<string, string>? metadata) =>
        metadata is null || !metadata.TryGetValue(OffsetsMetadataKey, out var json)
            ? []
            : JsonSerializer.Deserialize<SourceCheckpoint[]>(json)
                ?? throw new InvalidDataException("connector.offsets metadata deserialized to null");

    public async ValueTask<CheckpointState?> TryRestoreAsync(RootCircuit circuit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);

        // Only a missing pointer means "never checkpointed". A pointer naming a snapshot that
        // cannot be read must fail the restore, not silently restart from the sources' initial
        // offsets — ReadAsync throws for it.
        if (!await Snapshot.PointerExistsAsync(_fs, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        await Snapshot.ReadAsync(circuit, _fs, cancellationToken).ConfigureAwait(false);

        var metadata = await Snapshot.ReadMetadataAsync(_fs, cancellationToken).ConfigureAwait(false);
        return new CheckpointState(circuit.TickCount, ParseOffsets(metadata));
    }
}
