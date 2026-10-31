// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// When a <see cref="ProgramRunner"/> with a checkpoint store makes a batch durable
/// (docs/design-durable-state.md §1c). With <see cref="Journal"/> off, every batch ends in a full
/// snapshot. With it on, every batch ends in a small durable tick journal entry, and a snapshot is
/// taken only when <see cref="SnapshotInterval"/> has passed or <see cref="SnapshotEveryBatches"/>
/// batches have run since the last one — whichever comes first. A restart restores the last
/// snapshot and replays the journal after it.
/// </summary>
public sealed record CheckpointPolicy
{
    /// <summary>Feldera's bounds on <c>checkpoint_interval_secs</c>, applied the same way.</summary>
    public static readonly TimeSpan MinInterval = TimeSpan.FromSeconds(1);

    public static readonly TimeSpan MaxInterval = TimeSpan.FromSeconds(3600);

    /// <summary>A full snapshot at the end of every batch, no journal: the behaviour before A2,
    /// and what a runner created without a policy does.</summary>
    public static CheckpointPolicy EveryBatch { get; } = new() { Journal = false };

    /// <summary>Journal every batch, snapshot every 60 s — Feldera's default
    /// <c>checkpoint_interval_secs</c>.</summary>
    public static CheckpointPolicy Default { get; } = new() { Journal = true, SnapshotInterval = TimeSpan.FromSeconds(60) };

    /// <summary>Whether each batch commits through the tick journal instead of a snapshot.</summary>
    public bool Journal { get; init; }

    /// <summary>With <see cref="Journal"/>: snapshot once this long has passed since the last
    /// snapshot (or since the runner started). Clamped to [1 s, 1 h]; null = no time trigger.</summary>
    public TimeSpan? SnapshotInterval
    {
        get => _interval;
        init => _interval = value is { } v ? TimeSpan.FromTicks(Math.Clamp(v.Ticks, MinInterval.Ticks, MaxInterval.Ticks)) : null;
    }

    /// <summary>With <see cref="Journal"/>: snapshot once this many batches have committed since
    /// the last snapshot; null = no batch-count trigger.</summary>
    public int? SnapshotEveryBatches { get; init; }

    private readonly TimeSpan? _interval;

    /// <summary>
    /// Parse a policy setting, as <c>DBSPNET_CHECKPOINT_INTERVAL_SECS</c> carries it: unset or empty
    /// is <see cref="Default"/>; <c>batch</c> is <see cref="EveryBatch"/>; <c>none</c> journals every
    /// batch and never snapshots on its own; a number of seconds journals with that snapshot
    /// interval (clamped like Feldera's).
    /// </summary>
    public static CheckpointPolicy Parse(string? setting) => setting?.Trim() switch
    {
        null or "" => Default,
        "batch" => EveryBatch,
        "none" => new CheckpointPolicy { Journal = true },
        var s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var secs) && secs > 0 =>
            new CheckpointPolicy { Journal = true, SnapshotInterval = TimeSpan.FromSeconds(secs) },
        var s => throw new ArgumentException(
            $"checkpoint interval '{s}' is not a positive number of seconds, 'batch' or 'none'", nameof(setting)),
    };
}
