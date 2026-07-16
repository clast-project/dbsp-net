// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Schema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// A pull-based, <b>replayable</b> input source. The runner asks for the changes after
/// a given offset; because the source is replayable (e.g. Delta by version), recovery
/// re-reads from a committed offset and no input write-ahead log is needed. The
/// connector never pushes on its own thread.
/// </summary>
/// <remarks>
/// One tick per version: <see cref="NextAsync"/> returns exactly one source unit
/// (one Delta version's changes) so the runner Steps once per version. Weights encode
/// the change kind: insert <c>+1</c>, delete <c>-1</c>, update <c>-1</c> (preimage) and
/// <c>+1</c> (postimage). See <c>docs/design-connectors.md</c>.
/// </remarks>
public interface IInputConnector : IAsyncDisposable
{
    /// <summary>The logical table name this source feeds (its key in the catalog).</summary>
    string Name { get; }

    /// <summary>
    /// Schema handshake. If <paramref name="declared"/> is non-null the connector
    /// validates/coerces the source onto it and returns it; if null it infers a schema
    /// from the source and returns that (the runner then registers it in the catalog).
    /// </summary>
    ValueTask<Schema> ResolveSchemaAsync(Schema? declared, CancellationToken cancellationToken);

    /// <summary>The "before any data" offset — pass to <see cref="NextAsync"/> to read
    /// from the beginning.</summary>
    IConnectorOffset InitialOffset { get; }

    /// <summary>Rehydrate an offset from its serialized checkpoint form.</summary>
    IConnectorOffset ParseOffset(string serialized);

    /// <summary>The newest offset currently available at the source, or <c>null</c> if
    /// the source has produced nothing yet.</summary>
    ValueTask<IConnectorOffset?> LatestOffsetAsync(CancellationToken cancellationToken);

    /// <summary>
    /// The next unit of change after <paramref name="from"/> (exclusive) — one source
    /// version's changes, tagged with the offset it advances to — or <c>null</c> if
    /// nothing new is available. A version may span several Arrow batches (several
    /// Parquet files); all are applied within the one tick the runner Steps for it. A
    /// bounded source (e.g. a Parquet file) returns its whole contents once with
    /// <see cref="InputBatch.Completed"/> set.
    /// </summary>
    ValueTask<InputBatch?> NextAsync(IConnectorOffset from, CancellationToken cancellationToken);
}

/// <summary>
/// One tick's worth of change from a source: the changes of one version as a
/// <b>lazy stream</b> of Arrow batches (<paramref name="Content"/>), tagged with the
/// <paramref name="Offset"/> this version advances the source to. The runner pulls the
/// stream one <see cref="VersionBatch"/> at a time, pushing each before the single
/// <c>Step</c> for this version — so only one Arrow batch is materialised at a time (a
/// large version is not double-buffered; <c>PushArrow</c> copies each batch's values
/// into the engine input, then it is releasable). <paramref name="Completed"/> signals a
/// bounded source has been fully read.
/// </summary>
public sealed record InputBatch(
    IAsyncEnumerable<VersionBatch> Content,
    IConnectorOffset Offset,
    bool Completed = false);

/// <summary>One Arrow batch of a version, with its matching signed per-row weights
/// (<c>Weights.Length == Batch.Length</c>).</summary>
public readonly record struct VersionBatch(RecordBatch Batch, long[] Weights);
