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
        // Deltas carry signed weights that the consumer interprets — never expand.
        return BuildArrow(query.OutputSchema, query.Current, expandByWeight: false);
    }

    /// <summary>
    /// Materialise the query's full current <b>view</b> (every delta integrated so far)
    /// as an Arrow batch for a truncate-mode sink — the mirror of <see cref="ToArrowDelta"/>.
    /// A row whose view multiplicity is <c>w</c> is emitted <c>w</c> times, so a bag view
    /// (e.g. under <c>UNION ALL</c>) writes the correct number of rows; a set view (after
    /// DISTINCT/aggregation) is unchanged. The returned <c>Weights</c> are therefore all
    /// <c>1</c>. Requires the query to have been compiled with
    /// <see cref="DbspNet.Sql.Compiler.CompileOptions.StoredOutput"/> (throws otherwise,
    /// via <see cref="CompiledQuery.CurrentView"/>).
    /// </summary>
    public static ArrowDelta ToArrowView(this CompiledQuery query)
    {
        ArgumentNullException.ThrowIfNull(query);
        return BuildArrow(query.OutputSchema, query.CurrentView, expandByWeight: true);
    }

    /// <summary>
    /// Materialise an arbitrary integrated view (schema + Z-set) as an Arrow batch for a
    /// truncate-mode sink, expanding each row by its multiplicity — the shape used by a
    /// multi-output program's <c>ProgramOutput</c>. See <see cref="ToArrowView(CompiledQuery)"/>.
    /// </summary>
    public static ArrowDelta ToArrowView(
        SqlSchema schema,
        DbspNet.Core.Collections.ZSet<DbspNet.Core.Collections.StructuralRow, DbspNet.Core.Algebra.Z64> view)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(view);
        return BuildArrow(schema, view, expandByWeight: true);
    }

    /// <summary>
    /// A net change to an integrated view, shaped for an incremental (Delta-style) sink:
    /// <c>Inserts</c> carries one row per added copy (a row with weight <c>w &gt; 0</c> is
    /// emitted <c>w</c> times), <c>InsertKeys</c>[i] is that row's equality key (the
    /// <see cref="DbspNet.Core.Collections.StructuralRow"/> itself), and <c>Deletes</c> names
    /// each row whose multiplicity fell, with the number of copies to remove.
    /// </summary>
    public static ArrowChanges ToArrowChanges(
        SqlSchema schema,
        DbspNet.Core.Collections.ZSet<DbspNet.Core.Collections.StructuralRow, DbspNet.Core.Algebra.Z64> changes)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(changes);
        // The inserted rows are kept anyway (they are InsertKeys), so the Arrow columns are built
        // straight from them — no boxed per-column staging copy (docs/design-chunked-deltas.md §8).
        var keys = new List<object>(changes.Count);
        var deletes = new List<(object Key, long Count)>();
        foreach (var (row, weight) in changes)
        {
            var w = weight.Value;
            if (w < 0)
            {
                deletes.Add((row, -w));
                continue;
            }

            for (long k = 0; k < w; k++)
            {
                keys.Add(row);
            }
        }

        var ones = new long[keys.Count];
        System.Array.Fill(ones, 1L);
        return new ArrowChanges(AssembleRows(schema, keys, ones).Rows, keys, deletes);
    }

    private static ArrowDelta BuildArrow(
        SqlSchema schema,
        DbspNet.Core.Collections.ZSet<DbspNet.Core.Collections.StructuralRow, DbspNet.Core.Algebra.Z64> zset,
        bool expandByWeight)
    {
        var columnCount = schema.Count;

        if (!expandByWeight)
        {
            // Delta path: one Arrow row per distinct Z-set row, carrying its signed weight.
            var rowCount = zset.Count;
            var rows = new List<object>(rowCount);
            var weights = new long[rowCount];
            foreach (var (row, weight) in zset)
            {
                weights[rows.Count] = weight.Value;
                rows.Add(row);
            }

            return AssembleRows(schema, rows, weights);
        }

        // View path: emit each row w times (its view multiplicity). A materialized view
        // never carries a non-positive accumulated weight, so those are dropped.
        var expanded = new List<object>(zset.Count);
        foreach (var (row, weight) in zset)
        {
            var w = weight.Value;
            if (w <= 0)
            {
                continue;
            }

            for (long k = 0; k < w; k++)
            {
                expanded.Add(row);
            }
        }

        var ones = new long[expanded.Count];
        System.Array.Fill(ones, 1L);
        return AssembleRows(schema, expanded, ones);
    }

    // Each Arrow column built straight from the rows.
    private static ArrowDelta AssembleRows(SqlSchema schema, IReadOnlyList<object> rows, long[] weights)
    {
        var arrays = new IArrowArray[schema.Count];
        for (var c = 0; c < schema.Count; c++)
        {
            arrays[c] = ArrowColumns.Build(schema[c].Type, rows, c);
        }

        var arrowSchema = ArrowSchemaBridge.ToArrow(schema);
        return new ArrowDelta(new RecordBatch(arrowSchema, arrays, rows.Count), weights);
    }

    private static ArrowDelta Assemble(SqlSchema schema, object?[][] perColumn, long[] weights, int rowCount)
    {
        var arrays = new IArrowArray[schema.Count];
        for (var c = 0; c < schema.Count; c++)
        {
            arrays[c] = ArrowColumns.Build(schema[c].Type, perColumn[c]);
        }

        var arrowSchema = ArrowSchemaBridge.ToArrow(schema);
        return new ArrowDelta(new RecordBatch(arrowSchema, arrays, rowCount), weights);
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

    /// <summary>
    /// Decode an Arrow batch into row-major <c>(values, weight)</c> deltas <b>without
    /// pushing</b> — the expensive per-column Arrow extraction, separated from the
    /// (cheap, engine-serialized) push. A pipelined runner calls this on a reader thread
    /// to decode the next version while the engine processes the current one, then pushes
    /// the result via <see cref="TableInput.Push"/> on the engine thread (two versions
    /// must never merge into one tick). Reads only the table's schema — safe to call
    /// off-thread. Strings are copied (no zero-copy aliasing).
    /// </summary>
    public static List<(object?[] Values, long Weight)> DecodeArrowDeltas(
        this TableInput input, RecordBatch batch, ReadOnlySpan<long> weights)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(batch);
        ValidateWeights(batch, weights);
        return DecodeCore(input.Schema, batch, weights.ToArray(), zeroCopyStrings: false);
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

    /// <summary>
    /// Sharded counterpart of <see cref="PushArrow(TableInput, RecordBatch, ReadOnlySpan{long})"/>
    /// for a parallel program's source table: the same per-column decode (one column per
    /// thread on a large batch), then <see cref="ShardedTableInput.PushColumns"/>, which
    /// builds the rows and splits them across the replicas by whole-row hash in parallel.
    /// Strings are copied.
    /// </summary>
    public static void PushArrow(
        this ShardedTableInput input, RecordBatch batch, ReadOnlySpan<long> weights)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(batch);
        ValidateWeights(batch, weights);
        var schema = input.Schema;
        if (batch.ColumnCount != schema.Count)
        {
            throw new ArgumentException(
                $"batch arity {batch.ColumnCount} does not match table arity {schema.Count}",
                nameof(batch));
        }

        // Decoded in row blocks inside the parallel row build, not as whole-batch boxed columns:
        // a whole column is an object?[rowCount] on the large-object heap (design-chunked-deltas §8).
        object?[][] Decode(int from, int count)
        {
            var columns = new object?[schema.Count][];
            for (var c = 0; c < columns.Length; c++)
            {
                columns[c] = ArrowColumns.Extract(batch.Column(c), schema[c].Type, from, count, StringDecoding.Transcode);
            }

            return columns;
        }

        input.PushColumnBlocks(batch.Length, weights.ToArray(), Decode);
    }

    private static void PushArrowCore(
        TableInput input, RecordBatch batch, long[]? weights, bool zeroCopyStrings) =>
        input.Push(DecodeCore(input.Schema, batch, weights, zeroCopyStrings));

    private static List<(object?[] Values, long Weight)> DecodeCore(
        SqlSchema schema, RecordBatch batch, long[]? weights, bool zeroCopyStrings)
    {
        var columnCount = schema.Count;
        if (batch.ColumnCount != columnCount)
        {
            throw new ArgumentException(
                $"batch arity {batch.ColumnCount} does not match table arity {columnCount}",
                nameof(batch));
        }

        var rowCount = batch.Length;

        var deltas = new List<(object?[] Values, long Weight)>(rowCount);
        var hasWeights = weights is not null;
        var perColumn = new object?[columnCount][];
        var strings = zeroCopyStrings ? StringDecoding.Alias : StringDecoding.Transcode;

        // Per block of rows: each column walks its Arrow array in one typed loop (no per-cell
        // virtual dispatch), then the block's rows are assembled row-major. Blocks keep the boxed
        // column buffers off the large-object heap (docs/design-chunked-deltas.md §8).
        const int BlockRows = 4096;
        for (var block = 0; block < rowCount; block += BlockRows)
        {
            var count = Math.Min(BlockRows, rowCount - block);
            for (var c = 0; c < columnCount; c++)
            {
                perColumn[c] = ArrowColumns.Extract(batch.Column(c), schema[c].Type, block, count, strings);
            }

            for (var r = 0; r < count; r++)
            {
                var i = block + r;
                var weight = hasWeights ? weights![i] : 1L;
                if (weight == 0)
                {
                    continue;
                }

                var row = new object?[columnCount];
                for (var c = 0; c < columnCount; c++)
                {
                    row[c] = perColumn[c][r];
                }

                deltas.Add((row, weight));
            }
        }

        return deltas;
    }
}

/// <summary>
/// A delta produced by <see cref="ArrowExtensions.ToArrowDelta"/>: an Arrow
/// <see cref="Apache.Arrow.RecordBatch"/> of rows alongside the matching
/// Z-set weights. <c>Weights.Length == Rows.Length</c>; positive weights are
/// inserts, negative are retractions.
/// </summary>
public sealed record ArrowDelta(RecordBatch Rows, long[] Weights);

/// <summary>A view's net change for an incremental sink (see <see cref="ArrowExtensions.ToArrowChanges"/>).</summary>
public sealed record ArrowChanges(
    RecordBatch Inserts,
    IReadOnlyList<object> InsertKeys,
    IReadOnlyList<(object Key, long Count)> Deletes);
