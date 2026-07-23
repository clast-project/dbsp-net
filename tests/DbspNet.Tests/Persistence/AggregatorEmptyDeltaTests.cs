// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Pins the invariant that the fix for docs/design-incremental-persistence.md §7.2 rests on.
//
// After a snapshot restore, IncrementalAggregateOp needs each group's EMITTED value so its
// next tick retracts something that actually cancels against the materialised view. Rather
// than persist that value alongside the scratch state, the operator recovers it by calling
//
//     Update(ref state, oldValue: None, delta: <empty>, after: <restored group>)
//
// which is only sound if every aggregator, handed an empty delta, returns the value implied
// by its current state and leaves that state alone. That happens to be true of all ten
// shipped aggregators — each folds only over `delta` and derives its return from state — but
// nothing enforces it. It is an assumption spread across ten independent implementations, and
// the bug it is standing in for was itself a silent disagreement between two code paths.
//
// So: assert it directly, per aggregate kind. A future aggregator that recomputes from
// `after`, or that mutates state on an empty delta, fails here instead of silently corrupting
// a view after a restore.
//
// One aggregator is a deliberate documented exception to the *state-independent* reading:
// SqlApproxCountDistinct rebuilds its sketch from `after` when state is null. That is fine —
// we only ever call this after restoring state — so the test pins the behaviour that matters
// (non-null state ⇒ no rebuild, no mutation) and pins the null-state fallback separately.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Persistence;

public class AggregatorEmptyDeltaTests
{
    private static readonly Func<StructuralRow, object?> Arg = r => r[0];

    private static StructuralRow Row(object? v) => new(new object?[] { v });

    private static readonly ZSet<StructuralRow, Z64> EmptyDelta = ZSet<StructuralRow, Z64>.Empty;

    /// <summary>A group of rows with the given values, all at weight +1.</summary>
    private static ZSet<StructuralRow, Z64> Group(params object?[] values)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var v in values)
        {
            b.Add(Row(v), new Z64(1));
        }

        return b.Build();
    }

    // Cases are named rather than passed as objects: SqlAggregator is internal, so it cannot
    // appear in a public [Theory] signature. Each name resolves to a fresh aggregator and a
    // group whose values it can actually aggregate.
    public static TheoryData<string> Cases() => new()
    {
        "COUNT(*)", "COUNT(x)", "SUM(bigint)", "SUM(double)", "MIN", "MAX",
        "AVG(double)", "STDDEV_SAMP", "VAR_POP", "APPROX_COUNT_DISTINCT",
        "COUNT(DISTINCT x)", "APPROX_PERCENTILE", "PERCENTILE_DISC",
    };

    private static (SqlAggregator Agg, ZSet<StructuralRow, Z64> Group) Case(string name) => name switch
    {
        "COUNT(*)" => (new SqlCountStarAggregator(), Group(1L, 2L, 3L)),
        "COUNT(x)" => (new SqlCountAggregator(Arg), Group(1L, null, 3L)),
        "SUM(bigint)" => (new SqlSumAggregator(Arg, new SqlBigintType(true)), Group(1L, 2L, 3L)),
        "SUM(double)" => (new SqlSumAggregator(Arg, new SqlDoubleType(true)), Group(1.5, 2.5)),
        "MIN" => (new SqlMinMaxAggregator(Arg, wantMin: true), Group(3L, 1L, 2L)),
        "MAX" => (new SqlMinMaxAggregator(Arg, wantMin: false), Group(3L, 1L, 2L)),
        "AVG(double)" => (new SqlAvgAggregator(Arg, new SqlDoubleType(true)), Group(1.0, 2.0, 4.0)),
        "STDDEV_SAMP" => (new SqlStddevAggregator(Arg, sample: true, sqrt: true), Group(1.0, 2.0, 4.0)),
        "VAR_POP" => (new SqlStddevAggregator(Arg, sample: false, sqrt: false), Group(1.0, 2.0, 4.0)),
        "APPROX_COUNT_DISTINCT" => (new SqlApproxCountDistinctAggregator(Arg), Group(1L, 2L, 2L, 3L)),
        "COUNT(DISTINCT x)" => (new SqlCountDistinctAggregator(Arg), Group(1L, 2L, 2L, 3L)),
        "APPROX_PERCENTILE" => (
            new SqlApproxPercentileAggregator(
                Arg, 0.5, v => Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture), d => d),
            Group(1.0, 2.0, 3.0, 4.0)),
        "PERCENTILE_DISC" => (
            new SqlExactQuantileAggregator(
                Arg, 0.5, discrete: true,
                v => Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture),
                k => k),
            Group(10L, 20L, 30L, 40L)),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "unknown aggregate case"),
    };

    /// <summary>
    /// Build the aggregator's steady state the way the live path does — folding the group in
    /// one tick — then assert an empty-delta Update reproduces the value and changes nothing.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public void EmptyDeltaUpdate_ReturnsStateImpliedValue_AndDoesNotMutateState(string name)
    {
        var (agg, group) = Case(name);
        object? state = null;
        var live = agg.Update(ref state, oldValue: null, delta: group, after: group);
        Assert.NotNull(state);
        var stateAfterLive = state;

        // The call IncrementalAggregateOp.LoadAsync would make once state is restored.
        var recovered = agg.Update(ref state, oldValue: null, delta: EmptyDelta, after: group);

        Assert.Equal(live, recovered);

        // Same state object, not a rebuilt one — a rebuild is exactly what we are avoiding.
        Assert.Same(stateAfterLive, state);

        // Idempotent: calling it again must not drift either.
        var again = agg.Update(ref state, oldValue: null, delta: EmptyDelta, after: group);
        Assert.Equal(live, again);
        Assert.Same(stateAfterLive, state);
    }

    /// <summary>
    /// The recovered value must equal a from-scratch batch recompute too. This is the property
    /// that makes "restore, then emit deltas" agree with "never restarted" — and the one that
    /// float SUM/AVG/STDDEV break when the state is re-derived by folding rather than restored.
    /// </summary>
    [Theory]
    [MemberData(nameof(Cases))]
    public void EmptyDeltaUpdate_AgreesWithBatchCompute(string name)
    {
        var (agg, group) = Case(name);
        object? state = null;
        agg.Update(ref state, oldValue: null, delta: group, after: group);
        var recovered = agg.Update(ref state, oldValue: null, delta: EmptyDelta, after: group);

        Assert.Equal(agg.Compute(group), recovered);
    }

    /// <summary>
    /// The documented exception, pinned so it stays deliberate: with a NULL state,
    /// APPROX_COUNT_DISTINCT rebuilds its sketch from <c>after</c> rather than returning a
    /// state-implied value. Sound (the sketch is a deterministic function of the present value
    /// set), but it means the empty-delta contract above holds only once state is restored.
    /// </summary>
    [Fact]
    public void ApproxCountDistinct_WithNullState_RebuildsFromAfterRatherThanReturningZero()
    {
        var agg = new SqlApproxCountDistinctAggregator(Arg);
        var group = Group(1L, 2L, 2L, 3L);

        object? state = null;
        var fromNullState = agg.Update(ref state, oldValue: null, delta: EmptyDelta, after: group);

        Assert.Equal(3L, fromNullState);
        Assert.Equal(agg.Compute(group), fromNullState);
    }
}
