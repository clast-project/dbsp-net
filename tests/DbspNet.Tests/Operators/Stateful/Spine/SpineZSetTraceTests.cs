// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// <see cref="SpineZSetTrace{TKey,TWeight}"/> phase-1 correctness suite.
/// Most tests are property-based: build a flat oracle by summing all
/// integrated deltas with <c>ZSetBuilder</c>, integrate the same delta
/// sequence into a spine, then assert every observable
/// (<c>WeightOf</c>, <c>Materialize</c>, <c>Entries</c>,
/// <c>IsEmpty</c>) agrees with the oracle.
/// </summary>
public class SpineZSetTraceTests
{
    [Fact]
    public void Empty_HasNoBatchesAndIsEmpty()
    {
        var spine = new SpineZSetTrace<string, Z64>();
        Assert.True(spine.IsEmpty);
        Assert.Equal(0, spine.BatchCount);
        Assert.Equal(Z64.Zero, spine.WeightOf("nope"));
        Assert.Empty(spine.Entries());
        Assert.True(spine.Materialize().IsEmpty);
    }

    [Fact]
    public void IntegratingEmptyDelta_IsNoOp()
    {
        var spine = new SpineZSetTrace<string, Z64>();
        spine.Integrate(ZSet<string, Z64>.Empty);
        Assert.Equal(0, spine.BatchCount);
        Assert.True(spine.IsEmpty);
    }

    [Fact]
    public void SingleIntegrate_RoundTripsViaMaterialize()
    {
        var spine = new SpineZSetTrace<string, Z64>();
        spine.Integrate(ZSet.Singleton("a", new Z64(3)));
        Assert.Equal(new Z64(3), spine.WeightOf("a"));
        Assert.Equal(1, spine.BatchCount);
        Assert.Equal(
            ZSet.Singleton("a", new Z64(3)),
            spine.Materialize());
    }

    [Fact]
    public void WeightsSumAcrossBatches()
    {
        var spine = new SpineZSetTrace<string, Z64>();
        spine.Integrate(ZSet.Singleton("a", new Z64(2)));
        spine.Integrate(ZSet.Singleton("a", new Z64(5)));
        spine.Integrate(ZSet.Singleton("a", new Z64(-3)));
        Assert.Equal(new Z64(4), spine.WeightOf("a"));
        // Three batches in flight (tiered default = 4 per level: no compaction yet).
        Assert.Equal(3, spine.BatchCount);
    }

    [Fact]
    public void RetractionsCancellingExactly_DropFromMaterialisedEntries()
    {
        var spine = new SpineZSetTrace<string, Z64>();
        spine.Integrate(ZSet.Singleton("a", new Z64(5)));
        spine.Integrate(ZSet.Singleton("a", new Z64(-5)));
        Assert.Equal(Z64.Zero, spine.WeightOf("a"));
        Assert.Empty(spine.Entries());
        Assert.True(spine.Materialize().IsEmpty);
        // IsEmpty is about batches, not net contents — the cancelling
        // weights still live in their respective immutable batches until
        // a compaction round folds them out.
        Assert.False(spine.IsEmpty);
    }

    [Fact]
    public void TieredCompaction_TriggersAtThreshold_AndPromotesToNextLevel()
    {
        // 4 single-key inserts at distinct keys → 4 batches at L0 → trigger:
        // merge into one batch at L1, leaving L0 empty.
        var spine = new SpineZSetTrace<string, Z64>(new TieredCompactionStrategy(4));
        for (var i = 0; i < 4; i++)
        {
            spine.Integrate(ZSet.Singleton("k" + i, new Z64(1)));
        }

        Assert.Equal(1, spine.BatchCount);
        Assert.Equal(2, spine.LevelCount);
        Assert.Empty(spine.State.BatchSizes[0]);
        Assert.Single(spine.State.BatchSizes[1]);
        Assert.Equal(4, spine.State.BatchSizes[1][0]);
    }

    [Fact]
    public void CompactionCascades_AcrossLevels()
    {
        // 16 inserts with threshold 4 → ladder fills:
        // 4 at L0 → 1 at L1.  Repeat 4 times → 4 at L1 → 1 at L2.
        var spine = new SpineZSetTrace<int, Z64>(new TieredCompactionStrategy(4));
        for (var i = 0; i < 16; i++)
        {
            spine.Integrate(ZSet.Singleton(i, new Z64(1)));
        }

        Assert.Equal(3, spine.LevelCount);
        Assert.Empty(spine.State.BatchSizes[0]);
        Assert.Empty(spine.State.BatchSizes[1]);
        Assert.Single(spine.State.BatchSizes[2]);
        Assert.Equal(16, spine.State.BatchSizes[2][0]);
    }

    [Fact]
    public void EmptyMergedBatch_DoesNotPromoteToNextLevel()
    {
        // Pairwise cancelling integrations produce empty merged batches.
        // The strategy still asks for compaction (it counts non-empty
        // batches), but the spine drops the empty result rather than
        // promoting it.
        var spine = new SpineZSetTrace<string, Z64>(new TieredCompactionStrategy(2));
        spine.Integrate(ZSet.Singleton("a", new Z64(1)));
        spine.Integrate(ZSet.Singleton("a", new Z64(-1)));
        // 2 batches at L0 → merge → empty → dropped.
        Assert.Equal(0, spine.BatchCount);
        Assert.True(spine.IsEmpty);
    }

    [Fact]
    public void Strategy_BelowTwo_Rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new TieredCompactionStrategy(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new TieredCompactionStrategy(0));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    public void Pbt_MatchesFlatOracle_AcrossThresholds(int batchesPerLevel)
    {
        // Property: regardless of compaction threshold, the spine's
        // observable state must equal a single flat ZSet built by
        // summing the same delta sequence.
        var rng = new Random(Seed: 17 + batchesPerLevel);
        var spine = new SpineZSetTrace<int, Z64>(new TieredCompactionStrategy(batchesPerLevel));
        var oracle = new ZSetBuilder<int, Z64>();

        for (var step = 0; step < 200; step++)
        {
            var delta = RandomDelta(rng, keySpace: 30, maxEntriesPerBatch: 6);
            spine.Integrate(delta);
            foreach (var (k, w) in delta)
            {
                oracle.Add(k, w);
            }
        }

        var expected = oracle.Build();
        Assert.Equal(expected, spine.Materialize());

        // Per-key probe agreement on every key in the oracle plus a
        // sample of keys not in it (to cover the "absent" path).
        foreach (var (k, w) in expected)
        {
            Assert.Equal(w, spine.WeightOf(k));
        }

        for (var k = -5; k < 0; k++)
        {
            Assert.Equal(Z64.Zero, spine.WeightOf(k));
        }
    }

    [Fact]
    public void Pbt_BatchCountRemainsBounded()
    {
        // After K integrations with threshold N, batch count must be
        // bounded by N × ceil(log_N(K)). The bound is loose — what we
        // really want to catch is "spine never compacts" pathology
        // (batch count growing linearly with integrations).
        var spine = new SpineZSetTrace<int, Z64>(new TieredCompactionStrategy(4));
        var rng = new Random(Seed: 99);

        for (var step = 0; step < 500; step++)
        {
            spine.Integrate(RandomDelta(rng, keySpace: 50, maxEntriesPerBatch: 8));
        }

        // 500 inserts at threshold 4: ceil(log_4(500)) ≈ 5 levels;
        // at most 4 batches per level → at most ~20 batches.
        Assert.True(spine.BatchCount <= 4 * spine.LevelCount,
            $"unbounded growth: {spine.BatchCount} batches across {spine.LevelCount} levels");
    }

    [Fact]
    public void Pbt_EntriesAgreesWithMaterialize()
    {
        var rng = new Random(Seed: 33);
        var spine = new SpineZSetTrace<int, Z64>();
        for (var step = 0; step < 100; step++)
        {
            spine.Integrate(RandomDelta(rng, keySpace: 20, maxEntriesPerBatch: 5));
        }

        // Entries() and Materialize() must agree key-for-key and weight-for-weight.
        var fromEntries = new Dictionary<int, Z64>();
        foreach (var (k, w) in spine.Entries())
        {
            fromEntries[k] = w;
        }

        var fromMaterialize = new Dictionary<int, Z64>();
        foreach (var (k, w) in spine.Materialize())
        {
            fromMaterialize[k] = w;
        }

        Assert.Equal(fromMaterialize.Count, fromEntries.Count);
        foreach (var (k, w) in fromMaterialize)
        {
            Assert.True(fromEntries.TryGetValue(k, out var w2),
                $"key {k} present in Materialize but missing from Entries");
            Assert.Equal(w, w2);
        }
    }

    private static ZSet<int, Z64> RandomDelta(Random rng, int keySpace, int maxEntriesPerBatch)
    {
        var entries = rng.Next(maxEntriesPerBatch + 1);
        if (entries == 0)
        {
            return ZSet<int, Z64>.Empty;
        }

        var b = new ZSetBuilder<int, Z64>();
        for (var i = 0; i < entries; i++)
        {
            var k = rng.Next(keySpace);
            var w = rng.Next(-3, 4);
            if (w == 0)
            {
                continue;
            }

            b.Add(k, new Z64(w));
        }

        return b.Build();
    }
}
