using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// The fully-compiled output of a SQL query: a <see cref="RootCircuit"/>
/// with typed handles for each source table (push <c>INSERT</c>/<c>DELETE</c>
/// deltas via <see cref="TableInput"/>) and one output handle exposing the
/// result view as a <see cref="ZSet{StructuralRow, Z64}"/>.
/// </summary>
public sealed class CompiledQuery
{
    internal CompiledQuery(
        RootCircuit circuit,
        IReadOnlyDictionary<string, TableInput> inputs,
        OutputHandle<ZSet<StructuralRow, Z64>> output,
        Schema outputSchema)
    {
        Circuit = circuit;
        Inputs = inputs;
        Output = output;
        OutputSchema = outputSchema;
    }

    public RootCircuit Circuit { get; }

    public IReadOnlyDictionary<string, TableInput> Inputs { get; }

    public OutputHandle<ZSet<StructuralRow, Z64>> Output { get; }

    public Schema OutputSchema { get; }

    /// <summary>
    /// Convenience: push the queued deltas on every input, then commit one tick.
    /// </summary>
    public void Step() => Circuit.Step();

    public TableInput Table(string name) => Inputs[name];

    /// <summary>The current output Z-set (only meaningful after <see cref="Step"/>).</summary>
    public ZSet<StructuralRow, Z64> Current => Output.Current;
}

/// <summary>
/// Ergonomic input handle for a source table in a <see cref="CompiledQuery"/>.
/// Buffers <c>INSERT</c> / <c>DELETE</c> deltas and pushes them to the
/// circuit as a single <see cref="ZSet{StructuralRow, Z64}"/> on every call
/// — so each public method corresponds to one <see cref="InputHandle{T}.Push"/>.
/// </summary>
public sealed class TableInput
{
    private readonly InputHandle<ZSet<StructuralRow, Z64>> _handle;
    private readonly IRowCodec<StructuralRow> _codec;

    internal TableInput(
        InputHandle<ZSet<StructuralRow, Z64>> handle,
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
            b.Add(_codec.BuildRow(Schema, vs), new Z64(w));
        }

        _handle.Push(b.Build());
    }

    private void PushSingle(object?[] values, long weight)
    {
        ValidateArity(values);
        var z = ZSet.Singleton(_codec.BuildRow(Schema, values), new Z64(weight));
        _handle.Push(z);
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
