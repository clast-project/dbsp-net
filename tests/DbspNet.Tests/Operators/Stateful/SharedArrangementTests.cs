// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// A fan-out of joins reading ONE shared arrangement (flat
/// <see cref="IArrangement{TKey,TValue,TWeight}"/> or spine
/// <see cref="ISpineArrangement{TKey,TValue,TWeight}"/>) must produce
/// byte-identical per-tick output to the same joins each owning a private right
/// trace. Verifies the cross-operator shared-arrangement increment
/// (docs/design-row-representation.md §6.2 / §9): build the index once, read it
/// from many operators, no change to results — on both substrates.
/// </summary>
public class SharedArrangementTests
{
    // IComparable so the spine traces (sorted-columnar batches) can order rows.
    private sealed record Fact(int Key, long Value) : IComparable<Fact>
    {
        public int CompareTo(Fact? other) =>
            other is null ? 1 : Key != other.Key ? Key.CompareTo(other.Key) : Value.CompareTo(other.Value);
    }

    private sealed record LeftA(int Key, string Tag) : IComparable<LeftA>
    {
        public int CompareTo(LeftA? other) =>
            other is null ? 1 : Key != other.Key ? Key.CompareTo(other.Key) : string.CompareOrdinal(Tag, other.Tag);
    }

    private sealed record LeftB(int Key, string Tag) : IComparable<LeftB>
    {
        public int CompareTo(LeftB? other) =>
            other is null ? 1 : Key != other.Key ? Key.CompareTo(other.Key) : string.CompareOrdinal(Tag, other.Tag);
    }

    private sealed record OutA(int Key, string Tag, long Value);

    private sealed record OutB(int Key, string Tag, long Value);

    /// <summary>
    /// One circuit holding BOTH pipelines over the SAME input streams: two
    /// independent (unshared) joins, and two joins reading a single shared
    /// arrangement. Per tick the shared output must equal the unshared output
    /// for each branch.
    /// </summary>
    private sealed class Harness
    {
        public required RootCircuit Circuit { get; init; }

        public required InputHandle<ZSet<LeftA, Z64>> A { get; init; }

        public required InputHandle<ZSet<LeftB, Z64>> B { get; init; }

        public required InputHandle<ZSet<Fact, Z64>> R { get; init; }

        public required OutputHandle<ZSet<OutA, Z64>> UnsharedA { get; init; }

        public required OutputHandle<ZSet<OutB, Z64>> UnsharedB { get; init; }

        public required OutputHandle<ZSet<OutA, Z64>> SharedA { get; init; }

        public required OutputHandle<ZSet<OutB, Z64>> SharedB { get; init; }
    }

    private static Harness Build(bool spine = false)
    {
        InputHandle<ZSet<LeftA, Z64>>? a = null;
        InputHandle<ZSet<LeftB, Z64>>? b = null;
        InputHandle<ZSet<Fact, Z64>>? r = null;
        OutputHandle<ZSet<OutA, Z64>>? ua = null, sa = null;
        OutputHandle<ZSet<OutB, Z64>>? ub = null, sb = null;

        var circuit = RootCircuit.Build(builder =>
        {
            var (ah, asx) = builder.ZSetInput<LeftA, Z64>();
            var (bh, bsx) = builder.ZSetInput<LeftB, Z64>();
            var (rh, rsx) = builder.ZSetInput<Fact, Z64>();
            a = ah;
            b = bh;
            r = rh;

            var aIdx = builder.IndexBy(asx, x => x.Key);
            var bIdx = builder.IndexBy(bsx, x => x.Key);
            var rIdx = builder.IndexBy(rsx, x => x.Key);

            if (spine)
            {
                // Unshared: each spine join builds its own copy of R's trace.
                ua = builder.Output(builder.SpineIncrementalInnerJoin(
                    aIdx, rIdx, (_, l, f) => new OutA(l.Key, l.Tag, f.Value)));
                ub = builder.Output(builder.SpineIncrementalInnerJoin(
                    bIdx, rIdx, (_, l, f) => new OutB(l.Key, l.Tag, f.Value)));

                // Shared: one spine arrangement of R, probed by both joins.
                var arr = builder.SpineArrange(rIdx);
                sa = builder.Output(builder.SpineIncrementalInnerJoinSharedRight(
                    aIdx, rIdx, arr, (_, l, f) => new OutA(l.Key, l.Tag, f.Value)));
                sb = builder.Output(builder.SpineIncrementalInnerJoinSharedRight(
                    bIdx, rIdx, arr, (_, l, f) => new OutB(l.Key, l.Tag, f.Value)));
            }
            else
            {
                // Unshared: each join integrates its own copy of R.
                ua = builder.Output(builder.IncrementalInnerJoin(
                    aIdx, rIdx, (_, l, f) => new OutA(l.Key, l.Tag, f.Value)));
                ub = builder.Output(builder.IncrementalInnerJoin(
                    bIdx, rIdx, (_, l, f) => new OutB(l.Key, l.Tag, f.Value)));

                // Shared: one arrangement of R, read by both joins. Built AFTER the
                // unshared joins, but BEFORE its own consumers — the only ordering
                // that matters is arrange-before-shared-joins.
                var arr = builder.Arrange(rIdx);
                sa = builder.Output(builder.IncrementalInnerJoinSharedRight(
                    aIdx, rIdx, arr, (_, l, f) => new OutA(l.Key, l.Tag, f.Value)));
                sb = builder.Output(builder.IncrementalInnerJoinSharedRight(
                    bIdx, rIdx, arr, (_, l, f) => new OutB(l.Key, l.Tag, f.Value)));
            }
        });

        return new Harness
        {
            Circuit = circuit,
            A = a!,
            B = b!,
            R = r!,
            UnsharedA = ua!,
            UnsharedB = ub!,
            SharedA = sa!,
            SharedB = sb!,
        };
    }

    private static void AssertZSetEqual<T>(ZSet<T, Z64> expected, ZSet<T, Z64> actual)
        where T : notnull
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (row, w) in expected)
        {
            Assert.Equal(w, actual.WeightOf(row));
        }
    }

    private static void StepAndAssertEqual(Harness h)
    {
        h.Circuit.Step();
        AssertZSetEqual(h.UnsharedA.Current, h.SharedA.Current);
        AssertZSetEqual(h.UnsharedB.Current, h.SharedB.Current);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BothSidesSameTick_SharedMatchesUnshared(bool spine)
    {
        var h = Build(spine);
        h.A.Push(ZSet.Singleton(new LeftA(1, "a"), Z64.One));
        h.B.Push(ZSet.Singleton(new LeftB(1, "b"), Z64.One));
        h.R.Push(ZSet.Singleton(new Fact(1, 100), Z64.One));
        StepAndAssertEqual(h);

        Assert.Equal(Z64.One, h.SharedA.Current.WeightOf(new OutA(1, "a", 100)));
        Assert.Equal(Z64.One, h.SharedB.Current.WeightOf(new OutB(1, "b", 100)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LateRightArrival_SharedMatchesUnshared(bool spine)
    {
        var h = Build(spine);

        // Tick 1: left rows only, R empty — both branches empty.
        h.A.Push(ZSet.Singleton(new LeftA(7, "a"), Z64.One));
        h.B.Push(ZSet.Singleton(new LeftB(7, "b"), Z64.One));
        StepAndAssertEqual(h);
        Assert.True(h.SharedA.Current.IsEmpty);

        // Tick 2: R arrives late — must join against the historical left rows
        // through the SHARED trace exactly as the unshared joins do.
        h.R.Push(ZSet.Singleton(new Fact(7, 42), Z64.One));
        StepAndAssertEqual(h);
        Assert.Equal(Z64.One, h.SharedA.Current.WeightOf(new OutA(7, "a", 42)));
        Assert.Equal(Z64.One, h.SharedB.Current.WeightOf(new OutB(7, "b", 42)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RetractRightFromSharedTrace_SharedMatchesUnshared(bool spine)
    {
        var h = Build(spine);
        h.A.Push(ZSet.Singleton(new LeftA(3, "a"), Z64.One));
        h.B.Push(ZSet.Singleton(new LeftB(3, "b"), Z64.One));
        h.R.Push(ZSet.Singleton(new Fact(3, 9), Z64.One));
        StepAndAssertEqual(h);

        // Retract the shared right row — both branches must emit retractions.
        h.R.Push(ZSet.Singleton(new Fact(3, 9), new Z64(-1)));
        StepAndAssertEqual(h);
        Assert.True(h.SharedA.Current.WeightOf(new OutA(3, "a", 9)).Value < 0);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(2, false)]
    [InlineData(17, false)]
    [InlineData(99, false)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    [InlineData(17, true)]
    [InlineData(99, true)]
    public void RandomDeltaSequence_SharedMatchesUnshared(int seed, bool spine)
    {
        var h = Build(spine);
        var rng = new Random(seed);
        const int keyspace = 12;

        for (var tick = 0; tick < 60; tick++)
        {
            h.A.Push(RandomA(rng, keyspace));
            h.B.Push(RandomB(rng, keyspace));
            h.R.Push(RandomR(rng, keyspace));
            StepAndAssertEqual(h);
        }
    }

    private static ZSet<LeftA, Z64> RandomA(Random rng, int keyspace)
    {
        var entries = new List<(LeftA, Z64)>();
        var n = rng.Next(0, 4);
        for (var i = 0; i < n; i++)
        {
            var key = rng.Next(0, keyspace);
            var w = rng.Next(0, 2) == 0 ? Z64.One : new Z64(-1);
            entries.Add((new LeftA(key, "a" + (key % 3)), w));
        }

        return ZSet.FromEntries(entries);
    }

    private static ZSet<LeftB, Z64> RandomB(Random rng, int keyspace)
    {
        var entries = new List<(LeftB, Z64)>();
        var n = rng.Next(0, 4);
        for (var i = 0; i < n; i++)
        {
            var key = rng.Next(0, keyspace);
            var w = rng.Next(0, 2) == 0 ? Z64.One : new Z64(-1);
            entries.Add((new LeftB(key, "b" + (key % 5)), w));
        }

        return ZSet.FromEntries(entries);
    }

    private static ZSet<Fact, Z64> RandomR(Random rng, int keyspace)
    {
        var entries = new List<(Fact, Z64)>();
        var n = rng.Next(0, 5);
        for (var i = 0; i < n; i++)
        {
            var key = rng.Next(0, keyspace);
            var w = rng.Next(0, 2) == 0 ? Z64.One : new Z64(-1);
            entries.Add((new Fact(key, key * 10L + rng.Next(0, 3)), w));
        }

        return ZSet.FromEntries(entries);
    }
}
