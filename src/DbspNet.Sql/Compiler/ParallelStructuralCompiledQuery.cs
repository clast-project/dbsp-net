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
    /// Commit queued input deltas, fire the whole circuit one tick, then integrate
    /// each output view's gathered delta into its materialised full-view state.
    /// </summary>
    public void Step()
    {
        Circuit.Step();
        foreach (var output in Outputs.Values)
        {
            output.Accumulate();
        }
    }

    public void Dispose() => Circuit.Dispose();
}

/// <summary>
/// One output view of a <see cref="ParallelCompiledProgram"/>: its schema and the
/// full materialised view contents, integrated on the driver from the per-tick
/// gathered (summed-across-workers) delta. Mirrors <see cref="ProgramOutput"/>.
/// </summary>
public sealed class ParallelProgramOutput
{
    private readonly ShardedOutputHandle<StructuralRow, Z64> _delta;
    private ZSet<StructuralRow, Z64> _view = ZSet<StructuralRow, Z64>.Empty;

    internal ParallelProgramOutput(Schema schema, ShardedOutputHandle<StructuralRow, Z64> delta)
    {
        Schema = schema;
        _delta = delta;
    }

    public Schema Schema { get; }

    /// <summary>The full current view contents (valid until the next
    /// <see cref="ParallelCompiledProgram.Step"/>).</summary>
    public ZSet<StructuralRow, Z64> CurrentView => _view;

    /// <summary>Add this tick's gathered output delta into the integrated view.</summary>
    internal void Accumulate() => _view += _delta.Current;
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
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (vs, w) in deltas)
        {
            ValidateArity(vs);
            b.Add(_codec.BuildRow(Schema, BoundaryEncoder.Encode(Schema, vs)), new Z64(w));
        }

        _handle.Push(b.Build());
    }

    private void PushSingle(object?[] values, long weight)
    {
        ValidateArity(values);
        var b = new ZSetBuilder<StructuralRow, Z64>();
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
