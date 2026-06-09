// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Cross-tick amortisation for the non-indexed trace (docs §13): a
/// <see cref="SpineZSetTrace{TKey,TWeight}"/> (DISTINCT / recursive-CTE import
/// state) with the memtable enabled (<c>stagingCapacity &gt; 0</c>) must read
/// identically to the flat oracle — un-flushed memtable deltas merge with the
/// sorted batches on every read path (WeightOf, Materialize, Entries, IsEmpty),
/// GC, and snapshot. The staging-OFF path is covered (unchanged) by
/// <see cref="SpineZSetTraceTests"/>. Counterpart to <see cref="SpineMemtableTests"/>.
/// </summary>
public class SpineZSetMemtableTests
{
    private static SpineZSetTrace<int, Z64> Staged(int capacity, int batchesPerLevel = 4) =>
        new(new TieredCompactionStrategy(batchesPerLevel), comparer: null, spillConfig: null,
            monotoneKey: k => k, stagingCapacity: capacity);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(64)]
    public void Pbt_Staged_MatchesFlatOracle(int capacity)
    {
        var rng = new Random(Seed: 23 + capacity);
        var spine = Staged(capacity);
        var oracle = new Dictionary<int, long>(); // key -> net weight, zero pruned

        for (var step = 0; step < 250; step++)
        {
            var delta = RandomDelta(rng, keySpace: 24, maxEntries: 8);
            spine.Integrate(delta);
            foreach (var (k, w) in delta)
            {
                var sum = (oracle.TryGetValue(k, out var cur) ? cur : 0) + w.Value;
                if (sum == 0)
                {
                    oracle.Remove(k);
                }
                else
                {
                    oracle[k] = sum;
                }
            }

            // Read mid-stream every 25 ticks: DISTINCT/import probe the trace
            // before any flush, so reads must merge the memtable in place.
            if (step % 25 == 0)
            {
                AssertReadsMatch(spine, oracle);
            }
        }

        AssertReadsMatch(spine, oracle);
    }

    private static void AssertReadsMatch(SpineZSetTrace<int, Z64> spine, Dictionary<int, long> oracle)
    {
        // Materialize matches the oracle.
        var mat = spine.Materialize();
        Assert.Equal(oracle.Count, mat.Count);
        foreach (var (k, w) in oracle)
        {
            Assert.Equal(w, mat.WeightOf(k).Value);
        }

        // Per-key WeightOf, including absent keys.
        for (var k = -4; k < 28; k++)
        {
            var expected = oracle.TryGetValue(k, out var w) ? w : 0;
            Assert.Equal(expected, spine.WeightOf(k).Value);
        }

        // Entries() agrees with the oracle.
        var fromEntries = new Dictionary<int, long>();
        foreach (var (k, w) in spine.Entries())
        {
            fromEntries[k] = w.Value;
        }

        Assert.Equal(oracle.Count, fromEntries.Count);
        foreach (var (k, w) in oracle)
        {
            Assert.True(fromEntries.TryGetValue(k, out var w2) && w2 == w, $"Entries disagree on key {k}");
        }

        Assert.Equal(oracle.Count == 0, spine.IsEmpty);
    }

    [Fact]
    public void Staged_MemtableOnlyKey_IsVisibleBeforeFlush()
    {
        // Capacity 100 with two keys never flushes — they live only in the
        // memtable, yet all reads must see them.
        var spine = Staged(capacity: 100);
        spine.Integrate(ZSet.Singleton(7, new Z64(2)));
        spine.Integrate(ZSet.Singleton(7, new Z64(1)));   // sums to 3 in the memtable
        spine.Integrate(ZSet.Singleton(9, new Z64(5)));

        Assert.Equal(0, spine.BatchCount);                // nothing flushed yet
        Assert.False(spine.IsEmpty);
        Assert.Equal(new Z64(3), spine.WeightOf(7));
        Assert.Equal(new Z64(5), spine.WeightOf(9));
        Assert.Equal(Z64.Zero, spine.WeightOf(8));
        Assert.Equal(2, spine.KeyCount);
    }

    [Fact]
    public void Staged_GetBatches_FlushesMemtable()
    {
        var spine = Staged(capacity: 100);
        spine.Integrate(ZSet.Singleton(1, Z64.One));
        spine.Integrate(ZSet.Singleton(2, Z64.One));
        Assert.Equal(0, spine.BatchCount);   // buffered, not flushed

        var batches = spine.GetBatches();    // snapshot path must flush
        Assert.NotEqual(0, spine.BatchCount);
        var total = 0;
        foreach (var b in batches)
        {
            total += b.Count;
        }

        Assert.Equal(2, total);
    }

    [Fact]
    public void Staged_DropKeysBelow_DropsFromMemtable()
    {
        var spine = Staged(capacity: 100);
        spine.Integrate(ZSet.Singleton(1, Z64.One));
        spine.Integrate(ZSet.Singleton(5, Z64.One));
        spine.Integrate(ZSet.Singleton(9, Z64.One));

        var dropped = spine.DropKeysBelow(threshold: 5, monotoneKey: k => k);
        Assert.Equal(1, dropped);                    // key 1 dropped from the memtable
        Assert.Equal(Z64.Zero, spine.WeightOf(1));
        Assert.Equal(Z64.One, spine.WeightOf(5));    // at threshold → retained
        Assert.Equal(Z64.One, spine.WeightOf(9));
    }

    [Fact]
    public void Staged_CancellationWithinMemtable_NetsToZero()
    {
        var spine = Staged(capacity: 100);
        spine.Integrate(ZSet.Singleton(3, new Z64(2)));
        spine.Integrate(ZSet.Singleton(3, new Z64(-2)));   // cancels inside the memtable

        Assert.Equal(Z64.Zero, spine.WeightOf(3));
        Assert.True(spine.Materialize().IsEmpty);
        // A fully-cancelled memtable holds no keys, so the trace reports empty.
        Assert.True(spine.IsEmpty);
    }

    private static ZSet<int, Z64> RandomDelta(Random rng, int keySpace, int maxEntries)
    {
        var entries = rng.Next(maxEntries + 1);
        if (entries == 0)
        {
            return ZSet<int, Z64>.Empty;
        }

        var b = new ZSetBuilder<int, Z64>();
        for (var i = 0; i < entries; i++)
        {
            var w = rng.Next(-3, 4);
            if (w == 0)
            {
                continue;
            }

            b.Add(rng.Next(keySpace), new Z64(w));
        }

        return b.Build();
    }
}
