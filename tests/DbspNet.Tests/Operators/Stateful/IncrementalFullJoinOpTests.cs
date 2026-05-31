// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful;

public class IncrementalFullJoinOpTests
{
    private sealed record L(int K, int V) : IComparable<L>
    {
        public int CompareTo(L? other) =>
            other is null ? 1 : K != other.K ? K.CompareTo(other.K) : V.CompareTo(other.V);
    }

    private sealed record R(int K, int V) : IComparable<R>
    {
        public int CompareTo(R? other) =>
            other is null ? 1 : K != other.K ? K.CompareTo(other.K) : V.CompareTo(other.V);
    }

    // Left cols (LK, LV) null when the row is a right-only (NULL-padded-left)
    // row; right cols (RK, RV) null when the row is left-only.
    private sealed record OutRow(int? LK, int? LV, int? RK, int? RV);

    private static OutRow Joined(int _, L l, R r) => new(l.K, l.V, r.K, r.V);
    private static OutRow PadRight(int _, L l) => new(l.K, l.V, null, null);
    private static OutRow PadLeft(int _, R r) => new(null, null, r.K, r.V);

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<L, Z64>> Left,
        InputHandle<ZSet<R, Z64>> Right,
        OutputHandle<ZSet<OutRow, Z64>> Output)
        BuildFlat()
    {
        InputHandle<ZSet<L, Z64>>? li = null;
        InputHandle<ZSet<R, Z64>>? ri = null;
        OutputHandle<ZSet<OutRow, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.ZSetInput<L, Z64>();
            var (rh, rs) = b.ZSetInput<R, Z64>();
            li = lh;
            ri = rh;
            var lIdx = b.IndexBy(ls, x => x.K);
            var rIdx = b.IndexBy(rs, x => x.K);
            oh = b.Output(b.IncrementalFullJoin(
                lIdx, rIdx, Joined, PadRight, PadLeft));
        });
        return (c, li!, ri!, oh!);
    }

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<L, Z64>> Left,
        InputHandle<ZSet<R, Z64>> Right,
        OutputHandle<ZSet<OutRow, Z64>> Output)
        BuildSpine(ICompactionStrategy? strategy = null)
    {
        InputHandle<ZSet<L, Z64>>? li = null;
        InputHandle<ZSet<R, Z64>>? ri = null;
        OutputHandle<ZSet<OutRow, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.ZSetInput<L, Z64>();
            var (rh, rs) = b.ZSetInput<R, Z64>();
            li = lh;
            ri = rh;
            var lIdx = b.IndexBy(ls, x => x.K);
            var rIdx = b.IndexBy(rs, x => x.K);
            oh = b.Output(b.SpineIncrementalFullJoin(
                lIdx, rIdx, Joined, PadRight, PadLeft, compactionStrategy: strategy));
        });
        return (c, li!, ri!, oh!);
    }

    [Fact]
    public void LeftOnly_EmitsNullPaddedRight()
    {
        var (c, left, _, oh) = BuildFlat();
        left.Push(ZSet.Singleton(new L(1, 100), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, 100, null, null)));
        Assert.Equal(1, oh.Current.Count);
    }

    [Fact]
    public void RightOnly_EmitsNullPaddedLeft()
    {
        var (c, _, right, oh) = BuildFlat();
        right.Push(ZSet.Singleton(new R(2, 200), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(null, null, 2, 200)));
        Assert.Equal(1, oh.Current.Count);
    }

    [Fact]
    public void Matched_EmitsJoinedOnly()
    {
        var (c, left, right, oh) = BuildFlat();
        left.Push(ZSet.Singleton(new L(1, 100), Z64.One));
        right.Push(ZSet.Singleton(new R(1, 10), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, 100, 1, 10)));
        Assert.Equal(1, oh.Current.Count);
    }

    [Fact]
    public void GainedMatch_RetractsBothPads_EmitsJoined()
    {
        var (c, left, right, oh) = BuildFlat();

        // Left and right rows on the same key, but arriving on different ticks
        // so each is unmatched first: left-only → pad-right, right-only →
        // pad-left.
        left.Push(ZSet.Singleton(new L(1, 100), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, 100, null, null)));

        right.Push(ZSet.Singleton(new R(1, 10), Z64.One));
        c.Step();
        // The pad-right row retracts; the joined row appears. (No pad-left was
        // ever emitted since the left side was present this whole time.)
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, 100, null, null)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, 100, 1, 10)));
    }

    [Fact]
    public void LostLeft_RetractsJoined_EmitsNullPaddedLeft()
    {
        var (c, left, right, oh) = BuildFlat();
        left.Push(ZSet.Singleton(new L(1, 100), Z64.One));
        right.Push(ZSet.Singleton(new R(1, 10), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, 100, 1, 10)));

        // Remove the only left row: the joined row retracts and the right row
        // becomes NULL-padded-left.
        left.Push(ZSet.Singleton(new L(1, 100), new Z64(-1)));
        c.Step();
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, 100, 1, 10)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(null, null, 1, 10)));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void MatchesBatchOracleAndSpine(int batchesPerLevel)
    {
        var rng = new Random(Seed: 91 + batchesPerLevel);

        var (flat, flatL, flatR, flatOut) = BuildFlat();
        var (spine, spineL, spineR, spineOut) = BuildSpine(new TieredCompactionStrategy(batchesPerLevel));

        var intL = new ZSetBuilder<L, Z64>().Build();
        var intR = new ZSetBuilder<R, Z64>().Build();
        var flatCumulative = new ZSetBuilder<OutRow, Z64>().Build();
        var spineCumulative = new ZSetBuilder<OutRow, Z64>().Build();

        for (var step = 0; step < 200; step++)
        {
            var ld = RandomLeftDelta(rng, keySpace: 5, maxEntries: 3);
            var rd = RandomRightDelta(rng, keySpace: 5, maxEntries: 3);

            flatL.Push(ld);
            flatR.Push(rd);
            spineL.Push(ld);
            spineR.Push(rd);
            flat.Step();
            spine.Step();

            // Spine and flat operators must emit identical deltas every tick.
            Assert.Equal(flatOut.Current, spineOut.Current);

            // Integrate inputs and the flat operator's delta output.
            intL += ld;
            intR += rd;
            flatCumulative += flatOut.Current;
            spineCumulative += spineOut.Current;

            // Compare against a from-scratch batch full-outer oracle.
            Assert.Equal(BatchFullOuter(intL, intR), flatCumulative);
        }

        Assert.Equal(flatCumulative, spineCumulative);
    }

    private static ZSet<OutRow, Z64> BatchFullOuter(ZSet<L, Z64> left, ZSet<R, Z64> right)
    {
        var lByKey = new Dictionary<int, List<(L V, Z64 W)>>();
        foreach (var (l, w) in left)
        {
            if (!lByKey.TryGetValue(l.K, out var list)) lByKey[l.K] = list = new();
            list.Add((l, w));
        }

        var rByKey = new Dictionary<int, List<(R V, Z64 W)>>();
        foreach (var (r, w) in right)
        {
            if (!rByKey.TryGetValue(r.K, out var list)) rByKey[r.K] = list = new();
            list.Add((r, w));
        }

        var b = new ZSetBuilder<OutRow, Z64>();
        var keys = new HashSet<int>(lByKey.Keys);
        keys.UnionWith(rByKey.Keys);
        foreach (var key in keys)
        {
            lByKey.TryGetValue(key, out var ls);
            rByKey.TryGetValue(key, out var rs);
            var lHas = ls is { Count: > 0 };
            var rHas = rs is { Count: > 0 };

            if (lHas && rHas)
            {
                foreach (var (lv, lw) in ls!)
                {
                    foreach (var (rv, rw) in rs!)
                    {
                        b.Add(Joined(key, lv, rv), Z64.Multiply(lw, rw));
                    }
                }
            }
            else if (lHas)
            {
                foreach (var (lv, lw) in ls!)
                {
                    b.Add(PadRight(key, lv), lw);
                }
            }
            else if (rHas)
            {
                foreach (var (rv, rw) in rs!)
                {
                    b.Add(PadLeft(key, rv), rw);
                }
            }
        }

        return b.Build();
    }

    private static ZSet<L, Z64> RandomLeftDelta(Random rng, int keySpace, int maxEntries)
    {
        var n = rng.Next(maxEntries + 1);
        if (n == 0) return ZSet<L, Z64>.Empty;
        var b = new ZSetBuilder<L, Z64>();
        for (var i = 0; i < n; i++)
        {
            var w = rng.Next(-2, 3);
            if (w == 0) continue;
            b.Add(new L(rng.Next(keySpace), rng.Next(3)), new Z64(w));
        }
        return b.Build();
    }

    private static ZSet<R, Z64> RandomRightDelta(Random rng, int keySpace, int maxEntries)
    {
        var n = rng.Next(maxEntries + 1);
        if (n == 0) return ZSet<R, Z64>.Empty;
        var b = new ZSetBuilder<R, Z64>();
        for (var i = 0; i < n; i++)
        {
            var w = rng.Next(-2, 3);
            if (w == 0) continue;
            b.Add(new R(rng.Next(keySpace), rng.Next(3)), new Z64(w));
        }
        return b.Build();
    }
}
