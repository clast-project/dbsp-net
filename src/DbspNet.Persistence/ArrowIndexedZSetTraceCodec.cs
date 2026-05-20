// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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
/// Arrow IPC implementation of
/// <see cref="IIndexedZSetTraceCodec{TKey,TValue,TWeight}"/> for
/// <see cref="StructuralRow"/>-keyed traces holding per-group multisets
/// of <see cref="StructuralRow"/> values + <see cref="Z64"/> weights.
/// Used by <c>IncrementalAggregateOp</c> snapshot integration.
/// </summary>
/// <remarks>
/// The trace is serialised as a single-batch Arrow IPC stream whose
/// schema is the concatenation of the GROUP BY key columns, the input-row
/// value columns, and a trailing <c>__weight : Int64</c> column carrying
/// signed multiplicities. Key columns are renamed to <c>__k{i}_*</c> and
/// value columns to <c>__v{i}_*</c> to avoid collisions when GROUP BY
/// keys share names with the underlying row columns; column types and
/// data are otherwise unchanged. Loaders use positional access, so the
/// rename is purely cosmetic on the wire.
/// </remarks>
internal sealed class ArrowIndexedZSetTraceCodec
    : IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>
{
    private readonly SqlSchema _keySchema;
    private readonly SqlSchema _valueSchema;
    private readonly ArrowSchema _arrowSchemaWithWeight;

    public ArrowIndexedZSetTraceCodec(SqlSchema keySchema, SqlSchema valueSchema)
    {
        ArgumentNullException.ThrowIfNull(keySchema);
        ArgumentNullException.ThrowIfNull(valueSchema);
        _keySchema = keySchema;
        _valueSchema = valueSchema;
        _arrowSchemaWithWeight = BuildArrowSchema(keySchema, valueSchema);
        SchemaFingerprint = Persistence.SchemaFingerprint.Of(keySchema, valueSchema);
    }

    public string SchemaFingerprint { get; }

    public async ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        IndexedZSet<StructuralRow, StructuralRow, Z64> trace,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(trace);

        var rowCount = 0;
        foreach (var (_, group) in trace)
        {
            rowCount += group.Count;
        }

        var keyCount = _keySchema.Count;
        var valueCount = _valueSchema.Count;
        var keyColumns = new object?[keyCount][];
        var valueColumns = new object?[valueCount][];
        for (var c = 0; c < keyCount; c++)
        {
            keyColumns[c] = new object?[rowCount];
        }

        for (var c = 0; c < valueCount; c++)
        {
            valueColumns[c] = new object?[rowCount];
        }

        var weights = new long[rowCount];
        var i = 0;
        foreach (var (key, group) in trace)
        {
            foreach (var (value, w) in group)
            {
                for (var c = 0; c < keyCount; c++)
                {
                    keyColumns[c][i] = key[c];
                }

                for (var c = 0; c < valueCount; c++)
                {
                    valueColumns[c][i] = value[c];
                }

                weights[i] = w.Value;
                i++;
            }
        }

        var arrays = new IArrowArray[keyCount + valueCount + 1];
        for (var c = 0; c < keyCount; c++)
        {
            arrays[c] = ArrowColumns.Build(_keySchema[c].Type, keyColumns[c]);
        }

        for (var c = 0; c < valueCount; c++)
        {
            arrays[keyCount + c] = ArrowColumns.Build(_valueSchema[c].Type, valueColumns[c]);
        }

        var weightBuilder = new Int64Array.Builder().Reserve(rowCount);
        for (var k = 0; k < rowCount; k++)
        {
            weightBuilder.Append(weights[k]);
        }

        arrays[keyCount + valueCount] = weightBuilder.Build();

        using var batch = new RecordBatch(_arrowSchemaWithWeight, arrays, rowCount);
        await using var file = await writer.CreateAsync(fileName, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        using var ipcWriter = new ArrowStreamWriter(stream, _arrowSchemaWithWeight, leaveOpen: true);
        ipcWriter.WriteRecordBatch(batch);
        ipcWriter.WriteEnd();
    }

    public async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> LoadAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(fileName);
        if (!await reader.ExistsAsync(fileName, cancellationToken).ConfigureAwait(false))
        {
            return IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
        }

        await using var file = await reader.OpenReadAsync(fileName, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        using var ipcReader = new ArrowStreamReader(stream, leaveOpen: true);
        var batch = ipcReader.ReadNextRecordBatch();
        if (batch is null)
        {
            return IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
        }

        using (batch)
        {
            return BuildIndexedZSet(batch);
        }
    }

    private IndexedZSet<StructuralRow, StructuralRow, Z64> BuildIndexedZSet(RecordBatch batch)
    {
        var keyCount = _keySchema.Count;
        var valueCount = _valueSchema.Count;
        var rowCount = batch.Length;

        var keyColumns = new object?[keyCount][];
        for (var c = 0; c < keyCount; c++)
        {
            keyColumns[c] = ArrowColumns.Extract(
                batch.Column(c), _keySchema[c].Type, rowCount);
        }

        var valueColumns = new object?[valueCount][];
        for (var c = 0; c < valueCount; c++)
        {
            valueColumns[c] = ArrowColumns.Extract(
                batch.Column(keyCount + c), _valueSchema[c].Type, rowCount);
        }

        var weightArray = (Int64Array)batch.Column(keyCount + valueCount);
        var weightValues = weightArray.Values;

        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        for (var i = 0; i < rowCount; i++)
        {
            var keyValues = new object?[keyCount];
            for (var c = 0; c < keyCount; c++)
            {
                keyValues[c] = keyColumns[c][i];
            }

            var rowValues = new object?[valueCount];
            for (var c = 0; c < valueCount; c++)
            {
                rowValues[c] = valueColumns[c][i];
            }

            b.Add(
                new StructuralRow(keyValues),
                new StructuralRow(rowValues),
                new Z64(weightValues[i]));
        }

        return b.Build();
    }

    private static ArrowSchema BuildArrowSchema(SqlSchema keySchema, SqlSchema valueSchema)
    {
        var fields = new Field[keySchema.Count + valueSchema.Count + 1];
        for (var c = 0; c < keySchema.Count; c++)
        {
            var col = keySchema[c];
            fields[c] = new Field(
                "__k" + c + "_" + col.Name,
                ArrowSchemaBridge.ToArrowType(col.Type),
                col.Type.Nullable);
        }

        for (var c = 0; c < valueSchema.Count; c++)
        {
            var col = valueSchema[c];
            fields[keySchema.Count + c] = new Field(
                "__v" + c + "_" + col.Name,
                ArrowSchemaBridge.ToArrowType(col.Type),
                col.Type.Nullable);
        }

        fields[^1] = new Field("__weight", Int64Type.Default, nullable: false);
        return new ArrowSchema(fields, metadata: null);
    }
}
