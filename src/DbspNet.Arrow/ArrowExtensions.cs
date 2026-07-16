// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using DbspNet.Sql.Compiler;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Arrow;

/// <summary>
/// Arrow boundary for a <see cref="CompiledQuery"/>. Output materialises
/// the current Z-set delta into an Arrow <see cref="RecordBatch"/> plus a
/// parallel weights array. Input ingests an Arrow batch into a
/// <see cref="TableInput"/>, expanding rows by their (signed) weight.
/// </summary>
/// <remarks>
/// Conversion is column-major: each column is walked in a single tight
/// typed loop with type-dispatch hoisted out of the per-row inner loop.
/// Strings still copy through .NET <see cref="string"/> (zero-copy via
/// <c>Utf8String.FromBytes(arrowBuffer)</c> is a follow-up that needs
/// lifetime discipline from the caller).
/// </remarks>
public static class ArrowExtensions
{
    /// <summary>
    /// Materialise the query's current output Z-set as an Arrow batch plus a
    /// parallel weights array. <c>weights[i]</c> is the multiplicity of
    /// row <c>i</c> in the underlying Z-set; positive for inserts, negative
    /// for retractions.
    /// </summary>
    public static ArrowDelta ToArrowDelta(this CompiledQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        return BuildArrow(query.OutputSchema, query.Current);
    }

    /// <summary>
    /// Materialise the query's full current <b>view</b> (every delta integrated so
    /// far) as an Arrow batch plus a parallel weights array — the mirror of
    /// <see cref="ToArrowDelta"/> for a truncate-mode sink. Requires the query to have
    /// been compiled with <see cref="DbspNet.Sql.Compiler.CompileOptions.StoredOutput"/>
    /// (throws otherwise, via <see cref="CompiledQuery.CurrentView"/>). <c>weights[i]</c>
    /// is row <c>i</c>'s multiplicity in the view (≥ 1 for a well-formed query; a set
    /// after DISTINCT/aggregation, a bag under UNION ALL).
    /// </summary>
    public static ArrowDelta ToArrowView(this CompiledQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        return BuildArrow(query.OutputSchema, query.CurrentView);
    }

    private static ArrowDelta BuildArrow(
        SqlSchema schema, DbspNet.Core.Collections.ZSet<DbspNet.Core.Collections.StructuralRow, DbspNet.Core.Algebra.Z64> zset)
    {
        var rowCount = zset.Count;
        var columnCount = schema.Count;

        // Phase 1: walk the Z-set once, splitting per-cell values into
        // column-major buffers + collecting weights. This is the only pass
        // that touches the row-major Z-set.
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = new object?[rowCount];
        }

        var weights = new long[rowCount];
        var rowIndex = 0;
        foreach (var (row, weight) in zset)
        {
            for (var c = 0; c < columnCount; c++)
            {
                perColumn[c][rowIndex] = row[c];
            }

            weights[rowIndex] = weight.Value;
            rowIndex++;
        }

        // Phase 2: build each Arrow array with a tight typed loop. Type
        // dispatch happens once per column, not once per cell.
        var arrays = new IArrowArray[columnCount];
        for (var c = 0; c < columnCount; c++)
        {
            arrays[c] = ArrowColumns.Build(schema[c].Type, perColumn[c]);
        }

        var arrowSchema = ArrowSchemaBridge.ToArrow(schema);
        var batch = new RecordBatch(arrowSchema, arrays, rowCount);
        return new ArrowDelta(batch, weights);
    }

    /// <summary>
    /// Push every row of an Arrow <see cref="RecordBatch"/> as an insert
    /// (weight +1) into the table. String columns are copied — the batch's
    /// buffers may be disposed after this call returns.
    /// </summary>
    public static void PushArrow(this TableInput input, RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(batch);
        PushArrowCore(input, batch, weights: null, zeroCopyStrings: false);
    }

    /// <summary>
    /// Push an Arrow batch with explicit per-row weights. <c>weights[i]</c>
    /// is the signed multiplicity of row <c>i</c>: positive for inserts,
    /// negative for retractions, zero to skip. Lets a single batch carry a
    /// mixed-direction Z-set delta (e.g., when replaying a stream). Strings
    /// are copied — the batch's buffers may be disposed after this call.
    /// </summary>
    public static void PushArrow(
        this TableInput input, RecordBatch batch, ReadOnlySpan<long> weights)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(batch);
        ValidateWeights(batch, weights);
        PushArrowCore(input, batch, weights.ToArray(), zeroCopyStrings: false);
    }

    /// <summary>
    /// Zero-copy variant of <see cref="PushArrow(TableInput, RecordBatch)"/>:
    /// string columns alias the Arrow <c>ValueBuffer</c> via
    /// <see cref="Utf8String.FromBytes"/> instead of decoding + re-encoding.
    /// Eliminates per-cell allocation for VARCHAR data on ingest.
    /// </summary>
    /// <remarks>
    /// <para><b>Lifetime contract.</b> Aliased strings carry a
    /// <see cref="ReadOnlyMemory{Byte}"/> reference to the batch's buffer.
    /// The buffer must remain valid for as long as the engine retains data
    /// from this push — in DBSP, state-bearing operators hold rows
    /// indefinitely, so <em>do not dispose the <see cref="RecordBatch"/>
    /// while the engine still holds rows from it</em>.</para>
    /// <para>For managed-array-backed Arrow buffers (the typical in-process
    /// builder path), the GC keeps the bytes alive via the
    /// <see cref="ReadOnlyMemory{Byte}"/> owner reference even if the batch
    /// itself is disposed. For native-memory buffers (e.g., from an IPC
    /// stream with a custom allocator), disposing frees the bytes — be
    /// conservative and keep the batch alive.</para>
    /// </remarks>
    public static void PushArrowZeroCopy(this TableInput input, RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(batch);
        PushArrowCore(input, batch, weights: null, zeroCopyStrings: true);
    }

    /// <inheritdoc cref="PushArrowZeroCopy(TableInput, RecordBatch)"/>
    public static void PushArrowZeroCopy(
        this TableInput input, RecordBatch batch, ReadOnlySpan<long> weights)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(batch);
        ValidateWeights(batch, weights);
        PushArrowCore(input, batch, weights.ToArray(), zeroCopyStrings: true);
    }

    private static void ValidateWeights(RecordBatch batch, ReadOnlySpan<long> weights)
    {
        if (weights.Length != batch.Length)
        {
            throw new ArgumentException(
                $"weights length {weights.Length} does not match batch row count {batch.Length}",
                nameof(weights));
        }
    }

    private static void PushArrowCore(
        TableInput input, RecordBatch batch, long[]? weights, bool zeroCopyStrings)
    {
        var schema = input.Schema;
        var columnCount = schema.Count;
        if (batch.ColumnCount != columnCount)
        {
            throw new ArgumentException(
                $"batch arity {batch.ColumnCount} does not match table arity {columnCount}",
                nameof(batch));
        }

        var rowCount = batch.Length;

        // Phase 1: per-column extraction. Each column walks its Arrow
        // array in a single typed loop. Per-cell virtual dispatch through
        // an abstract reader is gone.
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = ArrowColumns.Extract(
                batch.Column(c), schema[c].Type, rowCount, zeroCopyStrings);
        }

        // Phase 2: assemble row-major deltas from the column-major buffers.
        // This is unavoidable while StructuralRow stores object?[] internally
        // — typed-row support would let us skip the per-row allocation.
        var deltas = new List<(object?[] Values, long Weight)>(rowCount);
        var hasWeights = weights is not null;
        for (var i = 0; i < rowCount; i++)
        {
            var weight = hasWeights ? weights![i] : 1L;
            if (weight == 0)
            {
                continue;
            }

            var row = new object?[columnCount];
            for (var c = 0; c < columnCount; c++)
            {
                row[c] = perColumn[c][i];
            }

            deltas.Add((row, weight));
        }

        input.Push(deltas);
    }
}

/// <summary>
/// A delta produced by <see cref="ArrowExtensions.ToArrowDelta"/>: an Arrow
/// <see cref="Apache.Arrow.RecordBatch"/> of rows alongside the matching
/// Z-set weights. <c>Weights.Length == Rows.Length</c>; positive weights are
/// inserts, negative are retractions.
/// </summary>
public sealed record ArrowDelta(RecordBatch Rows, long[] Weights);
