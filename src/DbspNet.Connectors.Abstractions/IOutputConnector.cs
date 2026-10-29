// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Schema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.Abstractions;

/// <summary>How an <see cref="IOutputConnector"/> writes results.</summary>
public enum OutputMode
{
    /// <summary>Replace the sink's contents with the full current view each tick
    /// (materialized / full-state). Naturally idempotent, so exactly-once needs
    /// nothing extra — the ivm-bench path. Requires the query to be compiled with
    /// <c>StoredOutput</c>.</summary>
    Truncate,

    /// <summary>Append the tick's delta as insert/delete rows (a changelog). Idempotent
    /// only via a per-tick dedup key at the sink.</summary>
    Changelog,

    /// <summary>Keep the sink equal to the current view, like <see cref="Truncate"/>, but
    /// write only each batch's net change: rows whose multiplicity fell are deleted where
    /// they sit, new copies appended (<see cref="IOutputConnector.ApplyChangesAsync"/>). The
    /// sink is truncated once, at its first write in a process, which then carries the
    /// whole current view. Feldera's Delta "truncate" mode works the same way.</summary>
    Incremental,
}

/// <summary>
/// A result sink. The runner binds it to the view's schema once, then hands it each
/// tick's output — the full view (<see cref="OutputMode.Truncate"/>) or the delta
/// (<see cref="OutputMode.Changelog"/>). The engine <c>tick</c> is passed as the batch
/// id so a sink can dedup a replayed tick after recovery.
/// </summary>
public interface IOutputConnector : IAsyncDisposable
{
    /// <summary>The view name this sink materialises (for diagnostics / binding).</summary>
    string ViewName { get; }

    /// <summary>Which write shape this sink uses.</summary>
    OutputMode Mode { get; }

    /// <summary>Bind the sink to the view's output schema (from
    /// <c>CompiledQuery.OutputSchema</c>) — called once before any write.</summary>
    ValueTask BindSchemaAsync(Schema viewSchema, CancellationToken cancellationToken);

    /// <summary>Replace the sink's contents with the full current <paramref name="view"/>
    /// (Truncate mode). <paramref name="weights"/>[i] is row i's multiplicity.</summary>
    ValueTask WriteViewAsync(RecordBatch view, long[] weights, long tick, CancellationToken cancellationToken);

    /// <summary>Append the tick's <paramref name="delta"/> with signed
    /// <paramref name="weights"/> (Changelog mode).</summary>
    ValueTask WriteDeltaAsync(RecordBatch delta, long[] weights, long tick, CancellationToken cancellationToken);

    /// <summary>
    /// Apply a batch's net change to a sink holding the view (Incremental mode).
    /// <paramref name="inserts"/> has one row per added copy, and
    /// <paramref name="insertKeys"/>[i] is row i's equality key (equal rows have equal keys).
    /// <paramref name="deletes"/> names rows to remove by key and copy count. On the
    /// connector's first call in a process the sink is truncated first, and
    /// <paramref name="inserts"/> is the whole current view.
    /// </summary>
    ValueTask ApplyChangesAsync(
        RecordBatch inserts,
        IReadOnlyList<object> insertKeys,
        IReadOnlyList<(object Key, long Count)> deletes,
        long tick,
        CancellationToken cancellationToken) =>
        throw new NotSupportedException($"{GetType().Name} does not support OutputMode.Incremental.");
}
