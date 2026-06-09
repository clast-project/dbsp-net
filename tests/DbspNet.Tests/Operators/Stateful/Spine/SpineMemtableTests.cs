// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Cross-tick amortisation (docs §9.7): <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/>
/// with the in-memory memtable enabled (<c>stagingCapacity &gt; 0</c>) must read
/// identically to the flat oracle — un-flushed memtable deltas merge with the
/// sorted batches on every read path (GroupFor, GroupForManySorted, Materialize,
/// Entries), GC, and snapshot — across a range of flush thresholds. The
/// staging-OFF path is covered (unchanged) by <see cref="SpineIndexedZSetTraceTests"/>.
/// </summary>
public class SpineMemtableTests
{
    private static SpineIndexedZSetTrace<int, int, Z64> Staged(int capacity, int batchesPerLevel = 4) =>
        new(new TieredCompactionStrategy(batchesPerLevel), monotoneKey: k => k, stagingCapacity: capacity);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(64)]
    public void Pbt_Staged_MatchesFlatOracle(int capacity)
    {
        var rng = new Random(Seed: 31 + capacity);
        var spine = Staged(capacity);
        // Readable oracle: outer key -> (value -> net weight), zero/empty pruned.
        var oracle = new Dictionary<int, Dictionary<int, long>>();

        for (var step = 0; step < 250; step++)
        {
            var delta = RandomDelta(rng, keySpace: 16, valueSpace: 6, maxEntries: 8);
            spine.Integrate(delta);
            foreach (var (k, group) in delta)
            {
                foreach (var (v, w) in group)
                {
                    var inner = oracle.TryGetValue(k, out var ex) ? ex : (oracle[k] = new Dictionary<int, long>());
                    var sum = (inner.TryGetValue(v, out var cur) ? cur : 0) + w.Value;
                    if (sum == 0)
                    {
                        inner.Remove(v);
                    }
                    else
                    {
                        inner[v] = sum;
                    }

                    if (inner.Count == 0)
                    {
                        oracle.Remove(k);
                    }
                }
            }

            // Read EVERY 25th tick mid-stream: the join/aggregate probe the trace
            // before any flush, so reads must merge the memtable in place.
            if (step % 25 == 0)
            {
                AssertReadsMatch(spine, oracle);
            }
        }

        AssertReadsMatch(spine, oracle);
    }

    private static void AssertReadsMatch(SpineIndexedZSetTrace<int, int, Z64> spine, Dictionary<int, Dictionary<int, long>> oracle)
    {
        // Materialize matches the oracle group-for-group.
        var mat = spine.Materialize();
        Assert.Equal(oracle.Count, mat.GroupCount);
        foreach (var (k, inner) in oracle)
        {
            AssertGroupMatches(mat.GroupFor(k), inner);
        }

        // Per-key point probe, incl. absent keys, against the oracle.
        for (var k = -4; k < 20; k++)
        {
            AssertGroupMatches(spine.GroupFor(k), oracle.GetValueOrDefault(k));
        }

        // Batched galloping probe agrees with the (oracle-validated) point probe.
        var probe = Enumerable.Range(-4, 26).ToArray();
        var byKey = spine.GroupForManySorted(probe).ToDictionary(g => g.Key, g => g.Group);
        foreach (var k in probe)
        {
            var pointGroup = spine.GroupFor(k);
            if (pointGroup.IsEmpty)
            {
                Assert.DoesNotContain(k, byKey.Keys);
            }
            else
            {
                Assert.True(byKey.TryGetValue(k, out var mergeGroup), $"key {k} missing from merge");
                var pairs = new Dictionary<int, Z64>();
                foreach (var (v, w) in pointGroup)
                {
                    pairs[v] = w;
                }

                Assert.Equal(pairs.Count, mergeGroup!.Length);
                foreach (var (v, w) in mergeGroup)
                {
                    Assert.Equal(pairs[v], w);
                }
            }
        }
    }

    private static void AssertGroupMatches(ZSet<int, Z64> actual, Dictionary<int, long>? expected)
    {
        expected ??= new Dictionary<int, long>();
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (v, w) in expected)
        {
            Assert.Equal(w, actual.WeightOf(v).Value);
        }
    }

    [Fact]
    public void Staged_MemtableOnlyKey_IsVisibleBeforeFlush()
    {
        // Capacity 100 with one key never flushes — the key lives only in the
        // memtable, yet all reads must see it.
        var spine = Staged(capacity: 100);
        spine.Integrate(Singleton(7, 1, new Z64(2)));
        spine.Integrate(Singleton(7, 1, new Z64(1)));   // sums to 3 in the memtable
        spine.Integrate(Singleton(7, 2, new Z64(5)));

        Assert.Equal(0, spine.BatchCount);              // nothing flushed yet
        Assert.False(spine.IsEmpty);
        var g = spine.GroupFor(7);
        Assert.Equal(new Z64(3), g.WeightOf(1));
        Assert.Equal(new Z64(5), g.WeightOf(2));

        var sorted = spine.GroupForManySorted(new[] { 7 });
        Assert.Single(sorted);
        Assert.Equal(7, sorted[0].Key);
        Assert.Equal(2, sorted[0].Group.Length);

        Assert.Equal(1, spine.Materialize().GroupCount);
    }

    [Fact]
    public void Staged_GetBatches_FlushesMemtable()
    {
        var spine = Staged(capacity: 100);
        spine.Integrate(Singleton(1, 1, new Z64(1)));
        spine.Integrate(Singleton(2, 1, new Z64(1)));
        Assert.Equal(0, spine.BatchCount);   // buffered, not flushed

        var batches = spine.GetBatches();    // snapshot path must flush
        Assert.NotEqual(0, spine.BatchCount);
        Assert.Equal(2, batches.Sum(b => b.GroupCount));
    }

    [Fact]
    public void Staged_DropKeysBelow_DropsFromMemtable()
    {
        var spine = Staged(capacity: 100);
        spine.Integrate(Singleton(1, 1, new Z64(1)));
        spine.Integrate(Singleton(5, 1, new Z64(1)));
        spine.Integrate(Singleton(9, 1, new Z64(1)));

        var dropped = spine.DropKeysBelow(threshold: 5, monotoneKey: k => k);
        Assert.Contains(1, dropped);              // below 5 → dropped from memtable
        Assert.True(spine.GroupFor(1).IsEmpty);
        Assert.False(spine.GroupFor(5).IsEmpty);  // at threshold → retained
        Assert.False(spine.GroupFor(9).IsEmpty);
    }

    [Fact]
    public void Staged_CancellationWithinMemtable_DropsKey()
    {
        var spine = Staged(capacity: 100);
        spine.Integrate(Singleton(3, 1, new Z64(2)));
        spine.Integrate(Singleton(3, 1, new Z64(-2)));   // cancels inside the memtable

        Assert.True(spine.GroupFor(3).IsEmpty);
        Assert.True(spine.Materialize().IsEmpty);
        Assert.True(spine.IsEmpty);
    }

    private static IndexedZSet<int, int, Z64> Singleton(int key, int value, Z64 weight)
    {
        var b = new IndexedZSetBuilder<int, int, Z64>();
        b.Add(key, value, weight);
        return b.Build();
    }

    private static IndexedZSet<int, int, Z64> RandomDelta(Random rng, int keySpace, int valueSpace, int maxEntries)
    {
        var entries = rng.Next(maxEntries + 1);
        if (entries == 0)
        {
            return IndexedZSet<int, int, Z64>.Empty;
        }

        var b = new IndexedZSetBuilder<int, int, Z64>();
        for (var i = 0; i < entries; i++)
        {
            var w = rng.Next(-3, 4);
            if (w == 0)
            {
                continue;
            }

            b.Add(rng.Next(keySpace), rng.Next(valueSpace), new Z64(w));
        }

        return b.Build();
    }
}
