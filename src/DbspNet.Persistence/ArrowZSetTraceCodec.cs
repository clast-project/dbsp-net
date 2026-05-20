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
internal sealed class ArrowZSetTraceCodec : IZSetTraceCodec<StructuralRow, Z64>
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
        SchemaFingerprint = Persistence.SchemaFingerprint.Of(rowSchema);
    }

    public string SchemaFingerprint { get; }

    public async ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        ZSet<StructuralRow, Z64> trace,
        CancellationToken cancellationToken = default)
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
        if (!await reader.ExistsAsync(fileName, cancellationToken).ConfigureAwait(false))
        {
            return ZSet<StructuralRow, Z64>.Empty;
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

    private ZSet<StructuralRow, Z64> BuildZSet(RecordBatch batch)
    {
        var columnCount = _rowSchema.Count;
        var rowCount = batch.Length;
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = ArrowColumns.Extract(
                batch.Column(c), _rowSchema[c].Type, rowCount);
        }

        var weightArray = (Int64Array)batch.Column(columnCount);
        var weightValues = weightArray.Values;

        var b = new ZSetBuilder<StructuralRow, Z64>();
        for (var i = 0; i < rowCount; i++)
        {
            var values = new object?[columnCount];
            for (var c = 0; c < columnCount; c++)
            {
                values[c] = perColumn[c][i];
            }

            b.Add(new StructuralRow(values), new Z64(weightValues[i]));
        }

        return b.Build();
    }
}
