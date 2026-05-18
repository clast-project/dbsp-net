using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

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

    /// <summary>
    /// Look up the weight of a specific row in the current output. Hides
    /// the internal storage representation: callers pass <see cref="string"/>
    /// values for VARCHAR columns and they are encoded to
    /// <see cref="Utf8String"/> against the output schema. Other types pass
    /// through unchanged.
    /// </summary>
    public Z64 WeightOf(params object?[] values)
    {
        var encoded = BoundaryEncoder.Encode(OutputSchema, values);
        return Current.WeightOf(new StructuralRow(encoded));
    }
}

/// <summary>
/// Boundary string-encoder shared by <see cref="TableInput"/> (input side)
/// and <see cref="CompiledQuery.WeightOf"/> (observation side). Lets callers
/// stay in .NET <see cref="string"/> idioms without knowing the internal
/// storage shape:
/// <list type="bullet">
///   <item>VARCHAR columns: <c>string</c> → <see cref="Utf8String"/></item>
///   <item>DECIMAL columns: <c>string</c> → <see cref="Clast.DatabaseDecimal.Values.Decimal128"/>
///         using the column's declared precision and scale</item>
/// </list>
/// Other types pass through unchanged.
/// </summary>
internal static class BoundaryEncoder
{
    public static object?[] Encode(Schema schema, object?[] values)
    {
        object?[]? encoded = null;
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is not string s)
            {
                continue;
            }

            switch (schema[i].Type)
            {
                case SqlVarcharType:
                    encoded ??= (object?[])values.Clone();
                    encoded[i] = Utf8String.Of(s);
                    break;
                case SqlDecimalType d:
                    encoded ??= (object?[])values.Clone();
                    encoded[i] = Clast.DatabaseDecimal.Text.DecimalText.ParseDecimal128(
                        s, Clast.DatabaseDecimal.DecimalType.Numeric(d.Precision, d.Scale));
                    break;
            }
        }

        return encoded ?? values;
    }
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

    /// <summary>
    /// Fires after every <see cref="Push"/> / <see cref="Insert"/> /
    /// <see cref="Delete"/> with the resulting Z-set delta. Used by the
    /// persistence layer (<c>DbspNet.Persistence.WalRecorder</c>) to
    /// capture per-tick inputs for the WAL. Internal — consumers use
    /// <c>WalRecorder</c>, not this event directly.
    /// </summary>
    internal event Action<ZSet<StructuralRow, Z64>>? OnPushed;

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

        var zset = b.Build();
        OnPushed?.Invoke(zset);
        _handle.Push(zset);
    }

    private void PushSingle(object?[] values, long weight)
    {
        ValidateArity(values);
        var z = ZSet.Singleton(
            _codec.BuildRow(Schema, BoundaryEncoder.Encode(Schema, values)),
            new Z64(weight));
        OnPushed?.Invoke(z);
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
