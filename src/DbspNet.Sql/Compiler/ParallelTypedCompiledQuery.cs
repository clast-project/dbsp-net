// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Data-parallel variant of <see cref="TypedCompiledQuery"/>: the same compiled
/// query replicated across a <see cref="ParallelCircuit"/>'s W workers. Inputs
/// are sharded across replicas and the output is gathered (Z-set sum), so the
/// boundary stays <c>object?[]</c>-shaped and the observable result is identical
/// to the single-circuit query for every W.
/// </summary>
/// <remarks>
/// Owns the parallel circuit's worker threads — callers must <see cref="Dispose"/>.
/// </remarks>
public sealed class ParallelTypedCompiledQuery : IDisposable
{
    private readonly Func<object> _currentZSetGetter;
    private readonly Func<object, IEnumerable<KeyValuePair<object, Z64>>> _currentReader;
    private readonly Func<object?[], Z64> _outputWeightOf;
    private readonly IOutputGather? _disjointGather;

    internal ParallelTypedCompiledQuery(
        ParallelCircuit circuit,
        IReadOnlyDictionary<string, TypedTableInput> inputs,
        Schema outputSchema,
        Type outputRowType,
        Func<object> currentZSetGetter,
        Func<object, IEnumerable<KeyValuePair<object, Z64>>> currentReader,
        Func<object?[], Z64> outputWeightOf,
        IOutputGather? disjointGather)
    {
        Circuit = circuit;
        Inputs = inputs;
        OutputSchema = outputSchema;
        OutputRowType = outputRowType;
        _currentZSetGetter = currentZSetGetter;
        _currentReader = currentReader;
        _outputWeightOf = outputWeightOf;
        _disjointGather = disjointGather;
    }

    public ParallelCircuit Circuit { get; }

    /// <summary>The replica count W the query runs across.</summary>
    public int Workers => Circuit.Workers;

    /// <summary>Sharded inputs by table name; a push splits across the replicas.</summary>
    public IReadOnlyDictionary<string, TypedTableInput> Inputs { get; }

    public Schema OutputSchema { get; }

    /// <summary>The closed emitted row type used at the output stage.</summary>
    public Type OutputRowType { get; }

    public TypedTableInput Table(string name) => Inputs[name];

    public void Step() => Circuit.Step();

    /// <summary>
    /// The current gathered output as <c>(values, weight)</c> tuples, decoded to
    /// the public CLR representation — the union of the replicas' shards, equal to
    /// the single-circuit query's output.
    /// </summary>
    /// <remarks>
    /// When the compiler proved the per-worker output shards are key-disjoint (the
    /// stream is partitioned by columns the output still carries — e.g. a filter or
    /// an injective projection), the gather is a parallel per-worker decode +
    /// concat: each worker decodes its own shard on its own thread, no serial
    /// Z-set combine. Otherwise (e.g. a column-dropping projection that can land
    /// equal output rows on different workers) the gather falls back to the serial
    /// Z-set sum, which merges those duplicates — correctness over speed.
    /// </remarks>
    public IEnumerable<(object?[] Values, long Weight)> Current =>
        _disjointGather is not null
            ? _disjointGather.Gather()
            : TypedOutputDecoder.Decode(OutputSchema, _currentZSetGetter, _currentReader);

    /// <summary>Look up the weight of a specific row in the current gathered output.</summary>
    public Z64 WeightOf(params object?[] values) =>
        _outputWeightOf(BoundaryEncoder.Encode(OutputSchema, values));

    public void Dispose() => Circuit.Dispose();
}

/// <summary>
/// Shared output decode: turn a boxed output Z-set (per-tick) into
/// <c>(values, weight)</c> tuples via the schema's cached field getters. Used by
/// both <see cref="TypedCompiledQuery"/> and <see cref="ParallelTypedCompiledQuery"/>.
/// </summary>
internal static class TypedOutputDecoder
{
    public static IEnumerable<(object?[] Values, long Weight)> Decode(
        Schema schema,
        Func<object> currentZSetGetter,
        Func<object, IEnumerable<KeyValuePair<object, Z64>>> currentReader)
    {
        var getters = TypedRowEmitter.BuildFieldGetters(schema)
            ?? throw new InvalidOperationException("output schema unexpectedly unsupported by TypedRowEmitter");

        foreach (var kv in currentReader(currentZSetGetter()))
        {
            var values = new object?[schema.Count];
            for (var i = 0; i < schema.Count; i++)
            {
                values[i] = getters[i](kv.Key);
            }

            yield return (values, kv.Value.Value);
        }
    }
}
