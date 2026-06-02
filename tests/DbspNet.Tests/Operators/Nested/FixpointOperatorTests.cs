// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Nested;

namespace DbspNet.Tests.Operators.Nested;

/// <summary>
/// Exercises the nested-circuit (fixpoint) primitive with the canonical
/// recursive query — transitive closure — wired from real Z-set operators, and
/// checks the circuit output against an independent batch fixpoint oracle, both
/// in a single tick and accumulated across multiple incremental ticks.
/// </summary>
public class FixpointOperatorTests
{
    // edges(b, c) ; closure(a, b). Step: R(a, b) ⋈ edges(b, c) → (a, c).
    private static (RootCircuit Circuit,
                    InputHandle<ZSet<(int, int), Z64>> Input,
                    OutputHandle<ZSet<(int, int), Z64>> Output) BuildTransitiveClosure()
    {
        InputHandle<ZSet<(int, int), Z64>>? ih = null;
        OutputHandle<ZSet<(int, int), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (handle, edges) = b.ZSetInput<(int, int), Z64>();
            ih = handle;

            var closure = b.Fixpoint<(int, int)>((scope, recRef) =>
            {
                var e = scope.Import(edges);
                var joined = scope.Join(
                    recRef,
                    e,
                    leftKey: path => path.Item2,
                    rightKey: edge => edge.Item1,
                    combine: (path, edge) => (path.Item1, edge.Item2));
                return scope.Distinct(scope.Union(e, joined));
            });

            oh = b.Output(closure);
        });

        return (circuit, ih!, oh!);
    }

    private static ZSet<(int, int), Z64> Edges(params (int, int)[] edges) =>
        ZSet.FromEntries(edges.Select(e => (e, Z64.One)));

    private static HashSet<(int, int)> BatchTransitiveClosure(IEnumerable<(int, int)> edges)
    {
        var e = edges.ToHashSet();
        var tc = new HashSet<(int, int)>(e);
        var changed = true;
        while (changed)
        {
            changed = false;
            foreach (var (a, b) in tc.ToList())
            {
                foreach (var (c, d) in e)
                {
                    if (b == c && tc.Add((a, d)))
                    {
                        changed = true;
                    }
                }
            }
        }

        return tc;
    }

    [Fact]
    public void TransitiveClosure_SingleTick_MatchesBatchOracle()
    {
        var (circuit, input, output) = BuildTransitiveClosure();

        var edges = new[] { (1, 2), (2, 3), (3, 4) };
        input.Push(Edges(edges));
        circuit.Step();

        var expected = BatchTransitiveClosure(edges);
        Assert.Equal(expected.Count, output.Current.Count);
        foreach (var row in expected)
        {
            Assert.Equal(1, output.Current.WeightOf(row).Value);
        }

        // 1→4 is only reachable transitively (1→2→3→4): proves the loop iterated.
        Assert.Equal(1, output.Current.WeightOf((1, 4)).Value);
    }

    [Fact]
    public void TransitiveClosure_MultiTickInserts_AccumulateToBatchOracle()
    {
        var (circuit, input, output) = BuildTransitiveClosure();

        var allEdges = new List<(int, int)>();
        var accumulated = new Dictionary<(int, int), long>();

        void Tick(params (int, int)[] newEdges)
        {
            input.Push(Edges(newEdges));
            circuit.Step();
            foreach (var (row, w) in output.Current)
            {
                accumulated[row] = accumulated.GetValueOrDefault(row) + w.Value;
            }

            allEdges.AddRange(newEdges);
        }

        // Adding (3,4) then (4,5) extends the chain — each tick emits only the
        // newly-reachable pairs as a +1 delta against the prior fixpoint.
        Tick((1, 2), (2, 3));
        Tick((3, 4));
        Tick((4, 5), (5, 6));

        // Every accumulated row settles at weight 0 or 1 (set semantics), and
        // the live set equals the batch transitive closure of all edges so far.
        Assert.All(accumulated.Values, v => Assert.True(v is 0 or 1, $"row weight {v} not in {{0,1}}"));
        var live = accumulated.Where(kv => kv.Value == 1).Select(kv => kv.Key).ToHashSet();
        Assert.Equal(BatchTransitiveClosure(allEdges), live);
    }

    [Fact]
    public void EmptyInput_ProducesEmptyFixpoint()
    {
        var (circuit, _, output) = BuildTransitiveClosure();

        circuit.Step();

        Assert.Equal(0, output.Current.Count);
    }
}
