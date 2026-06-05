// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Gathers a <see cref="ParallelCircuit"/>'s per-worker output shards into the
/// single decoded result. Hides the emitted row type so
/// <see cref="ParallelTypedCompiledQuery"/> can stay non-generic.
/// </summary>
internal interface IOutputGather
{
    /// <summary>The gathered output as decoded <c>(values, weight)</c> tuples.</summary>
    IReadOnlyList<(object?[] Values, long Weight)> Gather();
}

/// <summary>
/// The disjoint-output gather: used only when the compiler proved the per-worker
/// output shards share no keys (the output stream is partitioned by columns the
/// output row still carries, so equal output rows always sit on one worker). Each
/// worker decodes its <em>own</em> shard on its own thread and the controller
/// concatenates — no serial Z-set combine, so egest scales with W. The boundary
/// decode (Utf8String→string, Decimal128→decimal), the dominant cost on wide
/// string/decimal outputs, is what parallelizes.
/// </summary>
/// <remarks>
/// Soundness rests on the disjointness proof at compile time (see the
/// <c>ShardDisjoint</c> propagation in <c>TypedPlanCompiler</c>): if two equal
/// output rows could land on different workers, concatenation would emit the key
/// twice instead of summing the weights, so this path must never be selected for
/// such plans. The non-disjoint fallback is the serial Z-set sum.
/// </remarks>
internal sealed class DisjointOutputGather<TRow> : IOutputGather
    where TRow : notnull
{
    private readonly ParallelCircuit _circuit;
    private readonly OutputHandle<ZSet<TRow, Z64>>[] _handles;
    private readonly Func<object, object?>[] _getters;

    public DisjointOutputGather(ParallelCircuit circuit, string outputName, Func<object, object?>[] getters)
    {
        _circuit = circuit;
        _getters = getters;
        _handles = new OutputHandle<ZSet<TRow, Z64>>[circuit.Workers];
        for (var w = 0; w < _handles.Length; w++)
        {
            _handles[w] = circuit.WorkerOutput<ZSet<TRow, Z64>>(outputName, w);
        }
    }

    public IReadOnlyList<(object?[] Values, long Weight)> Gather()
    {
        var workers = _handles.Length;
        var perWorker = new List<(object?[] Values, long Weight)>[workers];

        // Each worker decodes its own shard on its own thread (W == 1 runs inline).
        // Workers touch disjoint slots, so there is no contention.
        _circuit.RunDataParallel((worker, _) =>
        {
            var width = _getters.Length;
            var list = new List<(object?[] Values, long Weight)>();
            foreach (var entry in _handles[worker].Current)
            {
                object boxedRow = entry.Key;
                var values = new object?[width];
                for (var c = 0; c < width; c++)
                {
                    values[c] = _getters[c](boxedRow);
                }

                list.Add((values, entry.Value.Value));
            }

            perWorker[worker] = list;
        });

        var total = 0;
        for (var w = 0; w < workers; w++)
        {
            total += perWorker[w].Count;
        }

        var result = new List<(object?[] Values, long Weight)>(total);
        for (var w = 0; w < workers; w++)
        {
            result.AddRange(perWorker[w]);
        }

        return result;
    }
}
