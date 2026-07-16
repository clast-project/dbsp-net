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
/// <see cref="ICheckpointStore"/> backed by the engine's <see cref="Snapshot"/> (for
/// operator state) plus an <c>offsets.json</c> sidecar on the same
/// <see cref="ITableFileSystem"/>. The two are written back to back; making them a
/// single atomic manifest commit is gap G3 in the design (a hardening — clean
/// checkpoints round-trip correctly today, and a crash between the two writes falls
/// back to the prior checkpoint, i.e. at-least-once).
/// </summary>
public sealed class SnapshotCheckpointStore : ICheckpointStore
{
    private const string OffsetsKey = "offsets.json";
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

        await Snapshot.WriteAsync(circuit, _fs, _retainCount, cancellationToken).ConfigureAwait(false);

        var record = new OffsetRecord(circuit.TickCount, [.. offsets]);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(record);
        await _fs.WriteAllBytesAsync(OffsetsKey, bytes, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<CheckpointState?> TryRestoreAsync(RootCircuit circuit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(circuit);

        if (!await Snapshot.ExistsAsync(_fs, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        await Snapshot.ReadAsync(circuit, _fs, cancellationToken).ConfigureAwait(false);

        // Read the offsets written alongside this snapshot. If the sidecar is missing
        // or names a different tick than the restored circuit, the checkpoint is torn
        // (a crash between the two writes) — treat it as no usable checkpoint.
        if (!await _fs.ExistsAsync(OffsetsKey, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var bytes = await _fs.ReadAllBytesAsync(OffsetsKey, cancellationToken).ConfigureAwait(false);
        var record = JsonSerializer.Deserialize<OffsetRecord>(bytes)
            ?? throw new InvalidDataException("offsets.json deserialized to null");
        if (record.Tick != circuit.TickCount)
        {
            return null;
        }

        return new CheckpointState(record.Tick, record.Offsets);
    }

    private sealed record OffsetRecord(long Tick, IReadOnlyList<SourceCheckpoint> Offsets);
}
