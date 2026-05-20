// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Spike;

/// <summary>
/// Phase 1 spike — proves that a statically-defined typed <c>readonly record
/// struct</c> row works end-to-end through <c>IRowCodec&lt;TRow&gt;</c> and the
/// Core Z-set operators, with no <see cref="StructuralRow"/> anywhere in the
/// pipeline. A prerequisite to the Reflection.Emit follow-up (spike B).
/// </summary>
public class TypedRowSpikeTests
{
    /// <summary>
    /// The "emitted" row for schema (a INT NOT NULL, b INT NOT NULL). A
    /// <c>readonly record struct</c> gives us fields, structural equality,
    /// <see cref="IEquatable{T}"/>, and a good <see cref="object.GetHashCode"/>
    /// automatically — exactly the surface a Reflection.Emit'd type needs to
    /// mimic.
    /// </summary>
    private readonly record struct TwoIntRow(int A, int B);

    private sealed class TwoIntRowCodec : IRowCodec<TwoIntRow>
    {
        public static TwoIntRowCodec Instance { get; } = new();

        public TwoIntRow BuildRow(Schema? schema, ReadOnlySpan<object?> values)
        {
            return new TwoIntRow((int)values[0]!, (int)values[1]!);
        }
    }

    [Fact]
    public void TypedRow_FlowsThroughZSetAndFilter()
    {
        InputHandle<ZSet<TwoIntRow, Z64>>? ih = null;
        OutputHandle<ZSet<TwoIntRow, Z64>>? oh = null;

        var circuit = RootCircuit.Build(b =>
        {
            var (handle, stream) = b.ZSetInput<TwoIntRow, Z64>();
            ih = handle;
            var filtered = b.Filter(stream, row => row.A > 5);
            oh = b.Output(filtered);
        });

        // Build rows via the codec — the codec call path is what TableInput
        // uses in the real pipeline, so we exercise it here too.
        var codec = TwoIntRowCodec.Instance;
        var inputBuilder = new ZSetBuilder<TwoIntRow, Z64>();
        inputBuilder.Add(codec.BuildRow(null, new object?[] { 3, 100 }), new Z64(1));
        inputBuilder.Add(codec.BuildRow(null, new object?[] { 7, 200 }), new Z64(1));
        inputBuilder.Add(codec.BuildRow(null, new object?[] { 10, 300 }), new Z64(2));
        ih!.Push(inputBuilder.Build());

        circuit.Step();

        Assert.Equal(2, oh!.Current.Count); // two distinct rows with A > 5
        Assert.Equal(new Z64(1), oh.Current.WeightOf(new TwoIntRow(7, 200)));
        Assert.Equal(new Z64(2), oh.Current.WeightOf(new TwoIntRow(10, 300)));
        Assert.Equal(Z64.Zero, oh.Current.WeightOf(new TwoIntRow(3, 100)));
    }

    [Fact]
    public void TypedRow_IncrementalRetractionAndReinsertCancel()
    {
        InputHandle<ZSet<TwoIntRow, Z64>>? ih = null;
        OutputHandle<ZSet<TwoIntRow, Z64>>? oh = null;

        var circuit = RootCircuit.Build(b =>
        {
            var (handle, stream) = b.ZSetInput<TwoIntRow, Z64>();
            ih = handle;
            oh = b.Output(stream);
        });

        var codec = TwoIntRowCodec.Instance;

        // Tick 1: insert (7, 42).
        var tick1 = new ZSetBuilder<TwoIntRow, Z64>();
        tick1.Add(codec.BuildRow(null, new object?[] { 7, 42 }), new Z64(1));
        ih!.Push(tick1.Build());
        circuit.Step();
        Assert.Equal(new Z64(1), oh!.Current.WeightOf(new TwoIntRow(7, 42)));

        // Tick 2: retract (7, 42), insert (7, 43). Equality must treat the
        // two as distinct even though A matches.
        var tick2 = new ZSetBuilder<TwoIntRow, Z64>();
        tick2.Add(codec.BuildRow(null, new object?[] { 7, 42 }), new Z64(-1));
        tick2.Add(codec.BuildRow(null, new object?[] { 7, 43 }), new Z64(1));
        ih.Push(tick2.Build());
        circuit.Step();
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new TwoIntRow(7, 42)));
        Assert.Equal(new Z64(1), oh.Current.WeightOf(new TwoIntRow(7, 43)));
    }

    [Fact]
    public void TypedRow_AggregationKeyBehavesAsStructural()
    {
        // Sanity check the GetHashCode / Equals pair by packing rows into a
        // Dictionary with the row as key. This is exactly what ZSet does
        // internally, so failure here would surface as silent weight
        // accumulation bugs in a circuit.
        var codec = TwoIntRowCodec.Instance;
        var dict = new Dictionary<TwoIntRow, int>();
        for (var i = 0; i < 1000; i++)
        {
            // Two rows per unique (a, b) pair — dict count should equal
            // distinct (a, b) pairs, not total inserts.
            var row = codec.BuildRow(null, new object?[] { i % 10, i % 7 });
            dict[row] = dict.TryGetValue(row, out var c) ? c + 1 : 1;
        }

        // 10 × 7 = 70 distinct (a, b) pairs reachable via i ∈ [0, 999).
        Assert.Equal(70, dict.Count);
    }
}
