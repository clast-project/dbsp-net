using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DbspNet.Sql.Compiler;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Arrow;

/// <summary>
/// Apache Arrow IPC streaming for <see cref="CompiledQuery"/> output and
/// <see cref="TableInput"/> ingest. Z-set weights are encoded inline as a
/// trailing <c>__weight</c> Int64 column on the streamed schema, so a
/// recipient that doesn't know about weights still sees a well-formed
/// Arrow stream (with one extra column it can ignore).
/// </summary>
/// <remarks>
/// <para><b>Wire format.</b> A standard Arrow IPC stream: one schema header
/// followed by one or more record batches. The schema is the table /
/// query schema with one extra <c>__weight : Int64 (not null)</c> field at
/// the end. A reader that doesn't expect weights can read everything
/// except the trailing column as the data; a reader that does (this one)
/// strips it and applies signed multiplicities.</para>
/// <para>For one-shot snapshots use <see cref="ArrowIpcExtensions.WriteArrowStream"/>;
/// for multi-batch streaming (e.g. one batch per Step) keep an
/// <see cref="ArrowDeltaWriter"/> open across calls.</para>
/// </remarks>
public static class ArrowIpcExtensions
{
    internal const string WeightFieldName = "__weight";

    /// <summary>
    /// Write the query's current output Z-set delta as a single-batch
    /// Arrow IPC stream. The destination is closed when this method
    /// returns unless <paramref name="leaveOpen"/> is true.
    /// </summary>
    public static void WriteArrowStream(
        this CompiledQuery query, Stream destination, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(destination);
        using var writer = query.OpenArrowDeltaWriter(destination, leaveOpen);
        writer.WriteDelta(query.ToArrowDelta());
    }

    /// <summary>
    /// Open a multi-batch IPC writer bound to the query's output schema.
    /// Each <see cref="ArrowDeltaWriter.WriteDelta"/> call appends one
    /// batch; <see cref="IDisposable.Dispose"/> writes the IPC stream
    /// terminator. Use when streaming a sequence of deltas (one per
    /// <c>Step()</c>) to the same destination.
    /// </summary>
    public static ArrowDeltaWriter OpenArrowDeltaWriter(
        this CompiledQuery query, Stream destination, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(destination);
        var dataSchema = ArrowSchemaBridge.ToArrow(query.OutputSchema);
        var schemaWithWeight = AppendWeightField(dataSchema);
        return new ArrowDeltaWriter(destination, schemaWithWeight, leaveOpen);
    }

    /// <summary>
    /// Read every batch from an Arrow IPC stream and push each into the
    /// table. If a batch's trailing column is named <c>__weight</c> and is
    /// Int64, it is consumed as signed Z-set multiplicities; otherwise
    /// every row is pushed at weight +1. Returns the total row count
    /// across all batches (sum of <c>RecordBatch.Length</c>, before weight
    /// expansion).
    /// </summary>
    /// <remarks>
    /// Does not call <c>Step()</c> between batches — caller controls
    /// commit timing. To process each batch as its own tick, use
    /// <see cref="ReadArrowStreamBatches"/> instead.
    /// </remarks>
    public static int ReadArrowStream(
        this TableInput input, Stream source, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(source);
        using var reader = new ArrowStreamReader(source, leaveOpen);
        var total = 0;
        while (reader.ReadNextRecordBatch() is { } batch)
        {
            using (batch)
            {
                IngestBatch(input, batch);
                total += batch.Length;
            }
        }

        return total;
    }

    /// <summary>
    /// Read batches lazily — yields the row count of each batch after
    /// pushing it, so callers can <c>Step()</c> between batches and react
    /// to per-tick output. The reader and the source stream are disposed
    /// when enumeration completes.
    /// </summary>
    public static IEnumerable<int> ReadArrowStreamBatches(
        this TableInput input, Stream source, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(source);
        using var reader = new ArrowStreamReader(source, leaveOpen);
        while (reader.ReadNextRecordBatch() is { } batch)
        {
            using (batch)
            {
                IngestBatch(input, batch);
                yield return batch.Length;
            }
        }
    }

    private static void IngestBatch(TableInput input, RecordBatch batch)
    {
        if (TryStripWeightColumn(batch, out var rowsBatch, out var weights))
        {
            using (rowsBatch)
            {
                input.PushArrow(rowsBatch, weights);
            }
        }
        else
        {
            input.PushArrow(batch);
        }
    }

    private static bool TryStripWeightColumn(
        RecordBatch batch, out RecordBatch rowsBatch, out long[] weights)
    {
        var schema = batch.Schema;
        var lastIdx = schema.FieldsList.Count - 1;
        if (lastIdx < 0
            || schema.FieldsList[lastIdx].Name != WeightFieldName
            || schema.FieldsList[lastIdx].DataType is not Int64Type)
        {
            rowsBatch = null!;
            weights = null!;
            return false;
        }

        var dataFields = new Field[lastIdx];
        for (var i = 0; i < lastIdx; i++)
        {
            dataFields[i] = schema.FieldsList[i];
        }

        var dataSchema = new ArrowSchema(dataFields, schema.Metadata);
        var dataArrays = new IArrowArray[lastIdx];
        for (var i = 0; i < lastIdx; i++)
        {
            dataArrays[i] = batch.Column(i);
        }

        rowsBatch = new RecordBatch(dataSchema, dataArrays, batch.Length);

        var weightArray = (Int64Array)batch.Column(lastIdx);
        weights = new long[batch.Length];
        var values = weightArray.Values;
        for (var i = 0; i < batch.Length; i++)
        {
            weights[i] = values[i];
        }

        return true;
    }

    internal static ArrowSchema AppendWeightField(ArrowSchema dataSchema)
    {
        var fields = new Field[dataSchema.FieldsList.Count + 1];
        for (var i = 0; i < dataSchema.FieldsList.Count; i++)
        {
            fields[i] = dataSchema.FieldsList[i];
        }

        fields[^1] = new Field(WeightFieldName, Int64Type.Default, nullable: false);
        return new ArrowSchema(fields, dataSchema.Metadata);
    }
}

/// <summary>
/// Multi-batch IPC writer. One instance is bound to a destination stream
/// and a fixed schema (data columns + trailing <c>__weight</c>); each
/// call to <see cref="WriteDelta"/> appends a record batch. Disposing
/// writes the IPC stream terminator.
/// </summary>
public sealed class ArrowDeltaWriter : IDisposable
{
    private readonly ArrowStreamWriter _writer;
    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private readonly ArrowSchema _schemaWithWeight;
    private bool _disposed;

    internal ArrowDeltaWriter(Stream destination, ArrowSchema schemaWithWeight, bool leaveOpen)
    {
        _stream = destination;
        _leaveOpen = leaveOpen;
        _schemaWithWeight = schemaWithWeight;
        _writer = new ArrowStreamWriter(destination, schemaWithWeight, leaveOpen: true);
    }

    /// <summary>
    /// Append <paramref name="delta"/> as one record batch. The delta's
    /// <c>Rows.Schema</c> must match the data columns this writer was
    /// opened with (i.e. the producing query's output schema).
    /// </summary>
    public void WriteDelta(ArrowDelta delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Build the per-batch weight column, then concat the data arrays.
        var n = delta.Rows.Length;
        var weightBuilder = new Int64Array.Builder().Reserve(n);
        for (var i = 0; i < n; i++)
        {
            weightBuilder.Append(delta.Weights[i]);
        }

        var dataArrayCount = delta.Rows.ColumnCount;
        var arrays = new IArrowArray[dataArrayCount + 1];
        for (var i = 0; i < dataArrayCount; i++)
        {
            arrays[i] = delta.Rows.Column(i);
        }

        arrays[dataArrayCount] = weightBuilder.Build();

        using var batch = new RecordBatch(_schemaWithWeight, arrays, n);
        _writer.WriteRecordBatch(batch);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _writer.WriteEnd();
        _writer.Dispose();
        if (!_leaveOpen)
        {
            _stream.Dispose();
        }

        _disposed = true;
    }
}
