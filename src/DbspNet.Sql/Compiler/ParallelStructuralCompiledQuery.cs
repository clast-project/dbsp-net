// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Data-parallel variant of <see cref="CompiledQuery"/>: the same structural
/// query replicated across a <see cref="ParallelCircuit"/>'s W workers. Inputs
/// are sharded across replicas (whole-row hash) and the output is gathered
/// (Z-set sum), so the boundary stays <c>ZSet&lt;StructuralRow, Z64&gt;</c>-shaped
/// and the observable result is identical to the single-circuit query for every
/// W — the exchange-insertion correctness invariant. At W=1 every exchange the
/// compiler emitted degrades to the identical <c>GroupProject</c> of the serial
/// path, so W=1 reproduces <see cref="CompiledQuery"/>'s circuit bit-for-bit.
/// </summary>
/// <remarks>
/// Owns the parallel circuit's worker threads — callers must <see cref="Dispose"/>.
/// </remarks>
public sealed class ParallelStructuralCompiledQuery : IDisposable
{
    private readonly ShardedOutputHandle<StructuralRow, Z64> _output;

    internal ParallelStructuralCompiledQuery(
        ParallelCircuit circuit,
        IReadOnlyDictionary<string, ShardedTableInput> inputs,
        ShardedOutputHandle<StructuralRow, Z64> output,
        Schema outputSchema)
    {
        Circuit = circuit;
        Inputs = inputs;
        _output = output;
        OutputSchema = outputSchema;
    }

    public ParallelCircuit Circuit { get; }

    /// <summary>The replica count W the query runs across.</summary>
    public int Workers => Circuit.Workers;

    /// <summary>Sharded inputs by table name; a push splits across the replicas.</summary>
    public IReadOnlyDictionary<string, ShardedTableInput> Inputs { get; }

    public Schema OutputSchema { get; }

    public ShardedTableInput Table(string name) => Inputs[name];

    /// <summary>Push the queued deltas on every replica, then commit one tick.</summary>
    public void Step() => Circuit.Step();

    /// <summary>Advance the logical clock before the next <see cref="Step"/>.</summary>
    public void AdvanceClock(long microsSinceEpoch) => Circuit.AdvanceTime(microsSinceEpoch);

    /// <summary>
    /// The current gathered output <b>delta</b> Z-set — the sum of the replicas'
    /// per-worker shards, equal to the single-circuit query's output delta.
    /// </summary>
    public ZSet<StructuralRow, Z64> Current => _output.Current;

    /// <summary>Look up the weight of a specific row in the current gathered output.</summary>
    public Z64 WeightOf(params object?[] values) =>
        Current.WeightOf(new StructuralRow(BoundaryEncoder.Encode(OutputSchema, values)));

    public void Dispose() => Circuit.Dispose();
}

/// <summary>
/// Data-parallel variant of <see cref="CompiledProgram"/>: a whole multi-view DAG
/// replicated across a <see cref="ParallelCircuit"/>'s W workers. Source tables are
/// sharded (whole-row hash); each output view's per-tick delta is gathered (Z-set
/// sum) and integrated on the driver into the full materialised view, so
/// <see cref="ParallelProgramOutput.CurrentView"/> equals the serial
/// <see cref="ProgramOutput.CurrentView"/> for every W. Only valid when every
/// reachable view is shardable (<c>PlanToCircuit.TryCompileProgramParallel</c>
/// returns <c>null</c> otherwise, and the caller uses the serial program).
/// </summary>
/// <remarks>Owns the parallel circuit's worker threads — callers must <see cref="Dispose"/>.</remarks>
public sealed class ParallelCompiledProgram : IDisposable
{
    internal ParallelCompiledProgram(
        ParallelCircuit circuit,
        IReadOnlyDictionary<string, ShardedTableInput> inputs,
        IReadOnlyDictionary<string, ParallelProgramOutput> outputs)
    {
        Circuit = circuit;
        Inputs = inputs;
        Outputs = outputs;
    }

    public ParallelCircuit Circuit { get; }

    /// <summary>The replica count W the program runs across.</summary>
    public int Workers => Circuit.Workers;

    public IReadOnlyDictionary<string, ShardedTableInput> Inputs { get; }

    public IReadOnlyDictionary<string, ParallelProgramOutput> Outputs { get; }

    public ShardedTableInput Table(string name) => Inputs[name];

    /// <summary>
    /// Commit queued input deltas and fire the whole circuit one tick. Output views
    /// are integrated in-circuit, per replica (§10.4), so there is no driver-side
    /// per-tick view work here.
    /// </summary>
    public void Step()
    {
        Circuit.Step();
        foreach (var output in Outputs.Values)
        {
            output.AfterStep();
        }
    }

    public void Dispose() => Circuit.Dispose();
}

/// <summary>
/// One output view of a <see cref="ParallelCompiledProgram"/>: its schema, its
/// per-tick gathered delta, and the full materialised view contents.
/// </summary>
/// <remarks>
/// <para><b>The view is integrated in-circuit, one integral per replica</b>
/// (docs/design-structural-parallel.md §10.4). Each worker folds its own shard of
/// the delta in place — O(|delta|) per tick, sharded across W — and
/// <see cref="CurrentView"/> is the sum of those W shard integrals, formed on
/// read. A row produced by two workers has its weights summed, exactly as the
/// delta gather does.</para>
/// <para>This replaced a driver-side <c>_view += delta</c>, which copied the whole
/// view on every tick (<c>ZSet.Plus</c> rebuilds the dictionary), making a run
/// quadratic in tick count. The cost moved from every tick to every read — and it
/// also put the view inside the per-worker snapshot subtrees, which is what makes
/// restore reproduce it (§10.1's gap).</para>
/// <para>Reads are cached per tick, so repeated reads between steps are free, and
/// at <c>W == 1</c> the single shard integral is returned directly with no copy.</para>
/// </remarks>
public sealed class ParallelProgramOutput
{
    private readonly ShardedOutputHandle<StructuralRow, Z64> _delta;
    private readonly IReadOnlyList<IntegratedViewHandle<StructuralRow>> _shards;
    private readonly ParallelCircuit _circuit;
    private ZSet<StructuralRow, Z64>? _cached;
    private long _cachedTick = -1;
    private ZSet<StructuralRow, Z64>? _cachedDelta;
    private long _cachedDeltaTick = -1;
    private long _rowCount;
    private long _rowCountTick = -1;

    internal ParallelProgramOutput(
        Schema schema,
        ShardedOutputHandle<StructuralRow, Z64> delta,
        IReadOnlyList<IntegratedViewHandle<StructuralRow>> shards,
        ParallelCircuit circuit)
    {
        Schema = schema;
        _delta = delta;
        _shards = shards;
        _circuit = circuit;
    }

    public Schema Schema { get; }

    /// <summary>This tick's gathered output delta (summed across workers; cached per tick).</summary>
    public ZSet<StructuralRow, Z64> CurrentDelta
    {
        get
        {
            var tick = _circuit.TickCount;
            if (_cachedDelta is null || _cachedDeltaTick != tick)
            {
                _cachedDelta = _delta.Current;
                _cachedDeltaTick = tick;
            }

            return _cachedDelta;
        }
    }

    /// <summary>
    /// The number of distinct rows in <see cref="CurrentView"/>, without forming it on
    /// every tick. The first read counts the formed view; from then on
    /// <see cref="ParallelCompiledProgram.Step"/> keeps the count current from each tick's
    /// delta — O(|delta| · W) lookups in the shard integrals instead of an O(|view|) sum
    /// per read. A program nobody asks for counts pays nothing.
    /// </summary>
    public long RowCount
    {
        get
        {
            if (_shards.Count == 1)
            {
                return _shards[0].Count;
            }

            var tick = _circuit.TickCount;
            if (_rowCountTick != tick)
            {
                _rowCount = CurrentView.Count;
                _rowCountTick = tick;
            }

            return _rowCount;
        }
    }

    /// <summary>
    /// Advance <see cref="RowCount"/> past the tick just stepped: each row the delta
    /// touched entered the view if its summed weight is now non-zero and was zero before
    /// (<c>after - delta</c>), and left it in the opposite case. Skipped until the count is
    /// first read, and when it is stale (a tick was missed), which leaves the next read to
    /// recount; at <c>W == 1</c> the read is already O(1).
    /// </summary>
    internal void AfterStep()
    {
        var tick = _circuit.TickCount;
        if (_shards.Count == 1 || _rowCountTick < 0 || _rowCountTick != tick - 1)
        {
            return;
        }

        var count = _rowCount;
        foreach (var (row, delta) in CurrentDelta)
        {
            var after = 0L;
            foreach (var shard in _shards)
            {
                after += shard.WeightOf(row).Value;
            }

            if (after != 0)
            {
                count++;
            }

            if (after - delta.Value != 0)
            {
                count--;
            }
        }

        _rowCount = count;
        _rowCountTick = tick;
    }

    /// <summary>The full current view contents (valid until the next
    /// <see cref="ParallelCompiledProgram.Step"/>).</summary>
    public ZSet<StructuralRow, Z64> CurrentView
    {
        get
        {
            // One shard: its integral *is* the view — no sum, no copy.
            if (_shards.Count == 1)
            {
                return _shards[0].Current;
            }

            var tick = _circuit.TickCount;
            if (_cached is not null && _cachedTick == tick)
            {
                return _cached;
            }

            var builder = new ZSetBuilder<StructuralRow, Z64>();
            foreach (var shard in _shards)
            {
                foreach (var (row, weight) in shard.Current)
                {
                    builder.Add(row, weight);
                }
            }

            _cached = builder.Build();
            _cachedTick = tick;
            return _cached;
        }
    }
}

/// <summary>
/// Ergonomic input handle for a source table in a
/// <see cref="ParallelStructuralCompiledQuery"/>. Mirrors <see cref="TableInput"/>
/// but pushes to a <see cref="ShardedInputHandle{TKey,TWeight}"/>, which splits
/// each delta across the replicas by the table's whole-row partition hash.
/// </summary>
public sealed class ShardedTableInput
{
    private readonly ShardedInputHandle<StructuralRow, Z64> _handle;
    private readonly IRowCodec<StructuralRow> _codec;

    internal ShardedTableInput(
        ShardedInputHandle<StructuralRow, Z64> handle,
        Schema schema,
        IRowCodec<StructuralRow> codec)
    {
        _handle = handle;
        _codec = codec;
        Schema = schema;
    }

    public Schema Schema { get; }

    public void Insert(params object?[] values) => PushSingle(values, weight: 1);

    public void Delete(params object?[] values) => PushSingle(values, weight: -1);

    public void Push(IEnumerable<(object?[] Values, long Weight)> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        var b = deltas.TryGetNonEnumeratedCount(out var n)
            ? new ZSetBuilder<StructuralRow, Z64>(n)
            : new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (vs, w) in deltas)
        {
            ValidateArity(vs);
            b.Add(_codec.BuildRow(Schema, BoundaryEncoder.Encode(Schema, vs)), new Z64(w));
        }

        _handle.Push(b.Build());
    }

    /// <summary>
    /// Push <paramref name="rowCount"/> rows given column-major (<c>columns[c][i]</c> is
    /// row <c>i</c>'s column <c>c</c>, boundary-typed as <see cref="Push"/> takes them),
    /// with <paramref name="weights"/> (all 1 when null; weight-0 rows are dropped). Same
    /// result as <see cref="Push"/>, but a large batch builds its rows and splits them
    /// across the replicas on up to <see cref="ParallelCompiledProgram.Workers"/> threads
    /// rather than on the caller's (the bulk-load ingest path).
    /// </summary>
    public void PushColumns(object?[][] columns, long[]? weights, int rowCount)
    {
        ArgumentNullException.ThrowIfNull(columns);
        if (columns.Length != Schema.Count)
        {
            throw new ArgumentException(
                $"column count {columns.Length} does not match schema arity {Schema.Count}", nameof(columns));
        }

        // Source row of every pushed row: all of them unless some weight is zero.
        int[]? source = null;
        var count = rowCount;
        if (weights is not null && Array.IndexOf(weights, 0L, 0, rowCount) >= 0)
        {
            source = new int[rowCount];
            count = 0;
            for (var i = 0; i < rowCount; i++)
            {
                if (weights[i] != 0)
                {
                    source[count++] = i;
                }
            }
        }

        var keys = new StructuralRow[count];
        var zs = new Z64[count];
        void Build(int from, int to)
        {
            for (var k = from; k < to; k++)
            {
                var i = source is null ? k : source[k];
                var values = new object?[columns.Length];
                for (var c = 0; c < values.Length; c++)
                {
                    values[c] = columns[c][i];
                }

                keys[k] = _codec.BuildRow(Schema, BoundaryEncoder.Encode(Schema, values));
                zs[k] = new Z64(weights is null ? 1 : weights[i]);
            }
        }

        if (_handle.Workers == 1 || count < ShardedInputHandle<StructuralRow, Z64>.ParallelSplitThreshold)
        {
            Build(0, count);
        }
        else
        {
            Parallel.ForEach(
                System.Collections.Concurrent.Partitioner.Create(0, count, Math.Max(4096, count / (_handle.Workers * 4))),
                new ParallelOptions { MaxDegreeOfParallelism = _handle.Workers },
                range => Build(range.Item1, range.Item2));
        }

        _handle.PushRows(keys, zs, count);
    }

    private void PushSingle(object?[] values, long weight)
    {
        ValidateArity(values);
        var b = new ZSetBuilder<StructuralRow, Z64>(1);
        b.Add(_codec.BuildRow(Schema, BoundaryEncoder.Encode(Schema, values)), new Z64(weight));
        _handle.Push(b.Build());
    }

    private void ValidateArity(object?[] values)
    {
        if (values.Length != Schema.Count)
        {
            throw new ArgumentException(
                $"row arity {values.Length} does not match schema arity {Schema.Count}",
                nameof(values));
        }
    }
}
