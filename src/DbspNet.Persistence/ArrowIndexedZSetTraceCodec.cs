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
            return IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
        }

        using (batch)
        {
            return BuildIndexedZSet(batch);
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

    // Stage-split twin of LoadAsync (docs/design-incremental-persistence.md §11); see the
    // matching comment in ArrowZSetTraceCodec.
    private async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> LoadProfiledAsync(
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
            return IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
        }

        // Disposed below unless the alias experiment retains it.
        {
            var keyCount = _keySchema.Count;
            var valueCount = _valueSchema.Count;
            var rowCount = batch.Length;

            var stringMs = 0.0;
            var stringCols = 0L;
            var keyColumns = new object?[keyCount][];
            for (var c = 0; c < keyCount; c++)
            {
                var isVarchar = _keySchema[c].Type is DbspNet.Sql.TypeSystem.SqlVarcharType;
                var tc = System.Diagnostics.Stopwatch.GetTimestamp();
                keyColumns[c] = ArrowColumns.Extract(batch.Column(c), _keySchema[c].Type, rowCount, RestoreStrings);
                if (isVarchar)
                {
                    stringMs += SnapshotRestoreProfile.MsSince(tc);
                    stringCols++;
                }
            }

            var valueColumns = new object?[valueCount][];
            for (var c = 0; c < valueCount; c++)
            {
                var isVarchar = _valueSchema[c].Type is DbspNet.Sql.TypeSystem.SqlVarcharType;
                var tc = System.Diagnostics.Stopwatch.GetTimestamp();
                valueColumns[c] = ArrowColumns.Extract(
                    batch.Column(keyCount + c), _valueSchema[c].Type, rowCount, ShippingStrings);
                if (isVarchar)
                {
                    stringMs += SnapshotRestoreProfile.MsSince(tc);
                    stringCols++;
                }
            }

            var weightValues = ((Int64Array)batch.Column(keyCount + valueCount)).Values;
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();

            var keys = new StructuralRow[rowCount];
            var values = new StructuralRow[rowCount];
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

                keys[i] = new StructuralRow(keyValues);
                values[i] = new StructuralRow(rowValues);
            }

            var t4 = System.Diagnostics.Stopwatch.GetTimestamp();

            var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            for (var i = 0; i < rowCount; i++)
            {
                b.Add(keys[i], values[i], new Z64(weightValues[i]));
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
                bytes, rowCount, keyCount + valueCount, stringMs, stringCols);
            return result;
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
                batch.Column(c), _keySchema[c].Type, rowCount, ShippingStrings);
        }

        var valueColumns = new object?[valueCount][];
        for (var c = 0; c < valueCount; c++)
        {
            valueColumns[c] = ArrowColumns.Extract(
                batch.Column(keyCount + c), _valueSchema[c].Type, rowCount, ShippingStrings);
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

    /// <inheritdoc/>
    /// <remarks>Reuses exactly the key-column encoding the trace file already uses,
    /// with a single trailing <c>__blob : Binary</c> column.</remarks>
    public bool SupportsKeyedBlobs => true;

    public async ValueTask SaveKeyedBlobsAsync(
        ISnapshotWriter writer,
        string fileName,
        IReadOnlyList<(StructuralRow Key, byte[] Blob)> entries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(entries);

        var keyCount = _keySchema.Count;
        var rowCount = entries.Count;
        var keyColumns = new object?[keyCount][];
        for (var c = 0; c < keyCount; c++)
        {
            keyColumns[c] = new object?[rowCount];
        }

        var blobBuilder = new BinaryArray.Builder().Reserve(rowCount);
        for (var i = 0; i < rowCount; i++)
        {
            var (key, blob) = entries[i];
            for (var c = 0; c < keyCount; c++)
            {
                keyColumns[c][i] = key[c];
            }

            blobBuilder.Append(blob ?? System.Array.Empty<byte>());
        }

        var arrays = new IArrowArray[keyCount + 1];
        for (var c = 0; c < keyCount; c++)
        {
            arrays[c] = ArrowColumns.Build(_keySchema[c].Type, keyColumns[c]);
        }

        arrays[keyCount] = blobBuilder.Build();

        var schema = BuildBlobArrowSchema(_keySchema);
        using var batch = new RecordBatch(schema, arrays, rowCount);
        await using var file = await writer.CreateAsync(fileName, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        using var ipcWriter = new ArrowStreamWriter(stream, schema, leaveOpen: true);
        ipcWriter.WriteRecordBatch(batch);
        ipcWriter.WriteEnd();
    }

    public async ValueTask<IReadOnlyList<(StructuralRow Key, byte[] Blob)>> LoadKeyedBlobsAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(fileName);

        // A snapshot written before the operator started emitting blobs has no such
        // file; that is "no blobs", not a failure.
        if (!await reader.ExistsAsync(fileName, cancellationToken).ConfigureAwait(false))
        {
            return System.Array.Empty<(StructuralRow, byte[])>();
        }

        await using var file = await reader.OpenReadAsync(fileName, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        using var ipcReader = new ArrowStreamReader(stream, leaveOpen: true);
        var batch = ipcReader.ReadNextRecordBatch();
        if (batch is null)
        {
            return System.Array.Empty<(StructuralRow, byte[])>();
        }

        using (batch)
        {
            var keyCount = _keySchema.Count;
            var rowCount = batch.Length;
            var keyColumns = new object?[keyCount][];
            for (var c = 0; c < keyCount; c++)
            {
                keyColumns[c] = ArrowColumns.Extract(batch.Column(c), _keySchema[c].Type, rowCount);
            }

            var blobs = (BinaryArray)batch.Column(keyCount);
            var result = new List<(StructuralRow, byte[])>(rowCount);
            for (var i = 0; i < rowCount; i++)
            {
                var keyValues = new object?[keyCount];
                for (var c = 0; c < keyCount; c++)
                {
                    keyValues[c] = keyColumns[c][i];
                }

                result.Add((new StructuralRow(keyValues), blobs.GetBytes(i).ToArray()));
            }

            return result;
        }
    }

    private static ArrowSchema BuildBlobArrowSchema(SqlSchema keySchema)
    {
        var fields = new Field[keySchema.Count + 1];
        for (var c = 0; c < keySchema.Count; c++)
        {
            var col = keySchema[c];
            fields[c] = new Field(
                "__k" + c + "_" + col.Name,
                ArrowSchemaBridge.ToArrowType(col.Type),
                col.Type.Nullable);
        }

        fields[^1] = new Field("__blob", BinaryType.Default, nullable: false);
        return new ArrowSchema(fields, metadata: null);
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
