// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Persistence;

/// <summary>
/// Arrow IPC implementation of <see cref="IZSetTraceCodec{TKey,TWeight}"/>
/// for <see cref="StructuralRow"/> + <see cref="Z64"/> traces. The trace
/// is serialised as a single-batch Arrow IPC stream — same wire format
/// the Arrow boundary uses for deltas, with a trailing
/// <c>__weight : Int64</c> column carrying multiplicities.
/// </summary>
/// <remarks>
/// The on-disk file name is operator-chosen and passed to
/// <see cref="SaveAsync"/> / <see cref="LoadAsync"/> per call — operators
/// with a single trace pick <c>"trace.arrows"</c>; multi-trace operators
/// (e.g. joins) disambiguate. A consumer that reads the snapshot tree as
/// raw Arrow streams sees a well-formed IPC file with the data columns +
/// <c>__weight</c> — same convention as <c>WalRecorder</c> uses for
/// input replay.
/// </remarks>
internal sealed class ArrowZSetTraceCodec : IZSetTraceCodec<StructuralRow, Z64>, Runs.IZSetRunCodec, IColdRowLoader<StructuralRow>, IColdZSetLoader<StructuralRow, Z64>
{
    private readonly SqlSchema _rowSchema;
    private readonly ArrowSchema _arrowDataSchema;
    private readonly ArrowSchema _arrowSchemaWithWeight;

    public ArrowZSetTraceCodec(SqlSchema rowSchema)
    {
        ArgumentNullException.ThrowIfNull(rowSchema);
        _rowSchema = rowSchema;
        _arrowDataSchema = ArrowSchemaBridge.ToArrow(rowSchema);
        _arrowSchemaWithWeight = ArrowIpcExtensions.AppendWeightField(_arrowDataSchema);
        _runSchema = new ArrowSchema(
            [.. _arrowSchemaWithWeight.FieldsList, new Field("__hash", Apache.Arrow.Types.Int32Type.Default, nullable: false)], metadata: null);
        SchemaFingerprint = Persistence.SchemaFingerprint.Of(rowSchema);
    }

    public string SchemaFingerprint { get; }

    /// <summary>Write <paramref name="trace"/>: as a run when <see cref="Runs.StateLayout.Runs"/>
    /// is on (docs/design-durable-state.md §1f) — inside a checkpoint, as an incremental chain of
    /// runs recorded in the snapshot directory's run index — otherwise as one Arrow stream.</summary>
    public async ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        ZSet<StructuralRow, Z64> trace,
        CancellationToken cancellationToken = default)
    {
        if (Runs.StateLayout.Runs)
        {
            ArgumentNullException.ThrowIfNull(trace);
            await Runs.RunCheckpoint.SaveZSetAsync(writer, fileName, trace, this, Hash, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        var t = Runs.RunWriteProfile.Now();
        // A partly cold partitioned operator attaches a partial snapshot: write all of it.
        var whole = (CheckpointTracking.TraceOf(trace) as IZSetCheckpointTracked<StructuralRow, Z64>)?.FullState?.Invoke() ?? trace;
        await SaveMonolithicAsync(writer, fileName, whole, cancellationToken).ConfigureAwait(false);
        Runs.RunWriteProfile.Add(6, t);
    }

    /// <summary>The saved rows, cold (increment 5), when <paramref name="fileName"/> is a chain the
    /// run index records in partition order and lazy restore is on; otherwise null.</summary>
    public async ValueTask<IColdRows<StructuralRow>?> LoadColdAsync(
        ISnapshotReader reader, string fileName, CancellationToken cancellationToken)
    {
        if (!Runs.RunColdGroups.Enabled
            || await Runs.RunCheckpoint.FindChainAsync(reader, fileName, cancellationToken).ConfigureAwait(false) is not
                { PartitionOrder: true, Count: >= 0 } chain)
        {
            return null;
        }

        var cold = await Runs.RunColdRows.OpenAsync(chain, this, cancellationToken).ConfigureAwait(false);
        CheckpointTracking.MarkRestored(cold, chain);
        return cold;
    }

    /// <summary>The saved trace, cold (increment 6), when <paramref name="fileName"/> is a chain the
    /// run index records in row order and lazy restore is on; otherwise null.</summary>
    public async ValueTask<IColdZSet<StructuralRow, Z64>?> LoadColdZSetAsync(
        ISnapshotReader reader, string fileName, CancellationToken cancellationToken)
    {
        if (!Runs.RunColdGroups.Enabled
            || await Runs.RunCheckpoint.FindChainAsync(reader, fileName, cancellationToken).ConfigureAwait(false) is not
                { PartitionOrder: false, Count: >= 0 } chain)
        {
            return null;
        }

        var cold = await Runs.RunColdZSet.OpenAsync(chain, this, cancellationToken).ConfigureAwait(false);
        CheckpointTracking.MarkRestored(cold, chain);
        return cold;
    }

    // The run's order: the row's own cached hash (StructuralRowHash — deterministic across
    // processes, and already computed, so a run write hashes nothing).
    private static int Hash(StructuralRow row) => row.GetHashCode();

    /// <inheritdoc/>
    public ArrowSchema RunSchema => _runSchema;

    private readonly ArrowSchema _runSchema;

    /// <inheritdoc/>
    public RecordBatch EncodeBlock(StructuralRow[] rows, long[] weights, int[] hashes, int count)
    {
        var slice = new ArraySegment<StructuralRow>(rows, 0, count);
        var columnCount = _rowSchema.Count;
        var arrays = new IArrowArray[columnCount + 2];
        for (var c = 0; c < columnCount; c++)
        {
            arrays[c] = ArrowColumns.Build(_rowSchema[c].Type, slice, c);
        }

        var w = new Int64Array.Builder().Reserve(count);
        for (var i = 0; i < count; i++)
        {
            w.Append(weights[i]);
        }

        arrays[columnCount] = w.Build();
        var h = new Int32Array.Builder().Reserve(count);
        for (var i = 0; i < count; i++)
        {
            h.Append(hashes[i]);
        }

        arrays[columnCount + 1] = h.Build();
        return new RecordBatch(_runSchema, arrays, count);
    }

    /// <inheritdoc/>
    public void DecodeBlock(RecordBatch batch, ZSetBuilder<StructuralRow, Z64> into) => AddBatch(batch, into);

    /// <inheritdoc/>
    /// <remarks>A cold read: the rows of one hash (a row, or a partition), retained together, so its
    /// strings share one arena (and, opted in, equal cells share one box: <see cref="CellInterner"/>).</remarks>
    public void DecodeRange(RecordBatch batch, int from, int count, ZSetBuilder<StructuralRow, Z64> into) =>
        AddRange(batch, from, count, into, ShippingStrings, intern: true);

    /// <inheritdoc/>
    public (StructuralRow Row, long Weight)[] DecodeEntries(RecordBatch batch)
    {
        var columnCount = _rowSchema.Count;
        var rowCount = batch.Length;
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = ArrowColumns.Extract(batch.Column(c), _rowSchema[c].Type, 0, rowCount, StringDecoding.Copy);
        }

        var weights = ((Int64Array)batch.Column(columnCount)).Values;
        var entries = new (StructuralRow, long)[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var values = new object?[columnCount];
            for (var c = 0; c < columnCount; c++)
            {
                values[c] = perColumn[c][i];
            }

            entries[i] = (new StructuralRow(values), weights[i]);
        }

        return entries;
    }

    private async ValueTask SaveMonolithicAsync(
        ISnapshotWriter writer,
        string fileName,
        ZSet<StructuralRow, Z64> trace,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(trace);

        var rowCount = trace.Count;
        var columnCount = _rowSchema.Count;
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = new object?[rowCount];
        }

        var weights = new long[rowCount];
        var i = 0;
        foreach (var (row, weight) in trace)
        {
            for (var c = 0; c < columnCount; c++)
            {
                perColumn[c][i] = row[c];
            }

            weights[i] = weight.Value;
            i++;
        }

        var arrays = new IArrowArray[columnCount];
        for (var c = 0; c < columnCount; c++)
        {
            arrays[c] = ArrowColumns.Build(_rowSchema[c].Type, perColumn[c]);
        }

        using var batch = new RecordBatch(_arrowDataSchema, arrays, rowCount);
        var delta = new ArrowDelta(batch, weights);

        await using var file = await writer.CreateAsync(fileName, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        using var deltaWriter = new ArrowDeltaWriter(stream, _arrowSchemaWithWeight, leaveOpen: true);
        deltaWriter.WriteDelta(delta);
    }

    public async ValueTask<ZSet<StructuralRow, Z64>> LoadAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(fileName);
        // A trace file a checkpoint recorded in its run index (increment 3): sum the chain.
        if (await Runs.RunCheckpoint.FindChainAsync(reader, fileName, cancellationToken).ConfigureAwait(false) is { } chain)
        {
            return await Runs.RunCheckpoint.LoadZSetAsync(chain, this, Hash, cancellationToken).ConfigureAwait(false);
        }

        if (!await reader.ExistsAsync(fileName, cancellationToken).ConfigureAwait(false))
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        // Either layout reads back, whatever StateLayout says now: a run ends in its trailer.
        var probe = await reader.OpenReadAsync(fileName, cancellationToken).ConfigureAwait(false);
        if (await Runs.RunReader.IsRunAsync(probe, cancellationToken).ConfigureAwait(false))
        {
            await using var run = await Runs.RunReader.OpenAsync(probe, Hash, fileName, null, cancellationToken).ConfigureAwait(false);
            return await run.ReadAllZSetAsync(this, cancellationToken).ConfigureAwait(false);
        }

        await probe.DisposeAsync().ConfigureAwait(false);
        return await LoadMonolithicAsync(reader, fileName, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<ZSet<StructuralRow, Z64>> LoadMonolithicAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken)
    {
        if (!await reader.ExistsAsync(fileName, cancellationToken).ConfigureAwait(false))
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        if (SnapshotRestoreProfile.Enabled)
        {
            return await LoadProfiledAsync(reader, fileName, cancellationToken).ConfigureAwait(false);
        }

        await using var file = await reader.OpenReadAsync(fileName, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        using var ipcReader = new ArrowStreamReader(stream, leaveOpen: true);
        var batch = ipcReader.ReadNextRecordBatch();
        if (batch is null)
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        using (batch)
        {
            return BuildZSet(batch);
        }
    }

    // EXPERIMENT (§11): decode VARCHAR columns as aliases into the Arrow buffer instead of
    // Utf8 -> string -> Utf8 round-tripping every cell. Aliasing means the batch's buffers must
    // outlive the restored state, so the batches are retained deliberately for the duration of
    // the measurement (this path is the profiled one, never a shipping restore).
    // Restore is the case a string arena is built for: every row of a snapshot survives, so the
    // arena is pinned exactly as long as the data it holds, and one allocation per column replaces
    // two per cell plus a UTF-8 -> UTF-16 -> UTF-8 round trip
    // (docs/design-incremental-persistence.md §11.4b).
    private static readonly StringDecoding RestoreStrings =
        Environment.GetEnvironmentVariable("DBSPNET_RESTORE_STRINGS") switch
        {
            "transcode" => StringDecoding.Transcode,
            "alias" => StringDecoding.Alias,
            _ => StringDecoding.Arena,
        };

    /// <summary>
    /// What the <b>shipping</b> loader may use. <see cref="StringDecoding.Alias"/> is downgraded
    /// here on purpose: it points values into the batch's native buffer, and this path disposes the
    /// batch — the values would dangle, silently. Only the profiled loader can use it, because only
    /// that one retains the batch, and it exists solely to reproduce the §11.4b measurement.
    /// </summary>
    private static readonly StringDecoding ShippingStrings =
        RestoreStrings == StringDecoding.Alias ? StringDecoding.Arena : RestoreStrings;

    private static readonly List<object> RetainedBatches = new();

    private static void Retain(object batch)
    {
        lock (RetainedBatches)
        {
            RetainedBatches.Add(batch);
        }
    }

    // Stage-split twin of LoadAsync (docs/design-incremental-persistence.md §11). Reads the
    // file into memory first so the I/O leg is separable from the decode, then times each of
    // extract / materialize / index individually.
    private async ValueTask<ZSet<StructuralRow, Z64>> LoadProfiledAsync(
        ISnapshotReader reader, string fileName, CancellationToken cancellationToken)
    {
        var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
        MemoryStream memory;
        await using (var file = await reader.OpenReadAsync(fileName, cancellationToken).ConfigureAwait(false))
        {
            await using var stream = file.AsStream();
            memory = new MemoryStream(stream.CanSeek ? (int)stream.Length : 0);
            await stream.CopyToAsync(memory, cancellationToken).ConfigureAwait(false);
        }

        memory.Position = 0;
        var bytes = memory.Length;
        var t1 = System.Diagnostics.Stopwatch.GetTimestamp();

        using var ipcReader = new ArrowStreamReader(memory, leaveOpen: true);
        var batch = ipcReader.ReadNextRecordBatch();
        var t2 = System.Diagnostics.Stopwatch.GetTimestamp();
        if (batch is null)
        {
            SnapshotRestoreProfile.AddCodec(
                SnapshotRestoreProfile.Ms(t0, t1), SnapshotRestoreProfile.Ms(t1, t2),
                0, 0, 0, bytes, 0, 0);
            return ZSet<StructuralRow, Z64>.Empty;
        }

        // Disposed below unless the alias experiment retains it.
        {
            var columnCount = _rowSchema.Count;
            var rowCount = batch.Length;
            var perColumn = new object?[columnCount][];
            var stringMs = 0.0;
            var stringCols = 0L;
            for (var c = 0; c < columnCount; c++)
            {
                var isVarchar = _rowSchema[c].Type is DbspNet.Sql.TypeSystem.SqlVarcharType;
                var tc = System.Diagnostics.Stopwatch.GetTimestamp();
                perColumn[c] = ArrowColumns.Extract(batch.Column(c), _rowSchema[c].Type, rowCount, RestoreStrings);
                if (isVarchar)
                {
                    stringMs += SnapshotRestoreProfile.MsSince(tc);
                    stringCols++;
                }
            }

            var weightValues = ((Int64Array)batch.Column(columnCount)).Values;
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();

            var rows = new StructuralRow[rowCount];
            for (var i = 0; i < rowCount; i++)
            {
                var values = new object?[columnCount];
                for (var c = 0; c < columnCount; c++)
                {
                    values[c] = perColumn[c][i];
                }

                rows[i] = new StructuralRow(values);
            }

            var t4 = System.Diagnostics.Stopwatch.GetTimestamp();

            var b = new ZSetBuilder<StructuralRow, Z64>(rowCount);
            for (var i = 0; i < rowCount; i++)
            {
                b.Add(rows[i], new Z64(weightValues[i]));
            }

            var result = b.Build();
            var t5 = System.Diagnostics.Stopwatch.GetTimestamp();
            if (RestoreStrings == StringDecoding.Alias)
            {
                // Only the alias mode needs this, and only because an IPC read buffer is
                // native-backed: disposing frees the memory the values point at. It leaks by
                // design and exists to reproduce the §11.4b measurement, nothing more.
                Retain(batch);
            }
            else
            {
                batch.Dispose();
            }


            SnapshotRestoreProfile.AddCodec(
                SnapshotRestoreProfile.Ms(t0, t1),
                SnapshotRestoreProfile.Ms(t1, t2),
                SnapshotRestoreProfile.Ms(t2, t3),
                SnapshotRestoreProfile.Ms(t3, t4),
                SnapshotRestoreProfile.Ms(t4, t5),
                bytes, rowCount, columnCount, stringMs, stringCols);
            return result;
        }
    }

    private ZSet<StructuralRow, Z64> BuildZSet(RecordBatch batch)
    {
        // One output row per Arrow row — rowCount is exact.
        var b = new ZSetBuilder<StructuralRow, Z64>(batch.Length);
        AddBatch(batch, b);
        return b.Build();
    }

    private void AddBatch(RecordBatch batch, ZSetBuilder<StructuralRow, Z64> b) =>
        AddRange(batch, 0, batch.Length, b, ShippingStrings);

    private void AddRange(RecordBatch batch, int from, int rowCount, ZSetBuilder<StructuralRow, Z64> b, StringDecoding strings, bool intern = false)
    {
        var columnCount = _rowSchema.Count;
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = ArrowColumns.Extract(
                batch.Column(c), _rowSchema[c].Type, from, rowCount, strings);
            if (intern)
            {
                CellInterner.Intern(perColumn[c]);
            }
        }

        var weightArray = (Int64Array)batch.Column(columnCount);
        var weightValues = weightArray.Values;

        for (var i = 0; i < rowCount; i++)
        {
            var values = new object?[columnCount];
            for (var c = 0; c < columnCount; c++)
            {
                values[c] = perColumn[c][i];
            }

            b.Add(new StructuralRow(values), new Z64(weightValues[from + i]));
        }
    }
}
