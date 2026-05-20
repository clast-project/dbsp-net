// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/> phase-1
/// correctness suite. Mirrors <see cref="SpineZSetTraceTests"/> — flat
/// <see cref="IndexedZSetBuilder{TKey,TValue,TWeight}"/> as oracle,
/// integrate the same sequence into a spine, assert
/// <c>GroupFor</c> / <c>Materialize</c> / <c>Entries</c> agree.
/// </summary>
public class SpineIndexedZSetTraceTests
{
    [Fact]
    public void Empty_HasNoBatchesAndIsEmpty()
    {
        var spine = new SpineIndexedZSetTrace<string, string, Z64>();
        Assert.True(spine.IsEmpty);
        Assert.Equal(0, spine.BatchCount);
        Assert.True(spine.GroupFor("nope").IsEmpty);
        Assert.Empty(spine.Entries());
        Assert.True(spine.Materialize().IsEmpty);
    }

    [Fact]
    public void IntegratingEmptyDelta_IsNoOp()
    {
        var spine = new SpineIndexedZSetTrace<string, string, Z64>();
        spine.Integrate(IndexedZSet<string, string, Z64>.Empty);
        Assert.Equal(0, spine.BatchCount);
    }

    [Fact]
    public void GroupFor_UnionsAcrossBatches()
    {
        var spine = new SpineIndexedZSetTrace<string, string, Z64>();
        spine.Integrate(IndexedSingleton("region:west", "alice", new Z64(2)));
        spine.Integrate(IndexedSingleton("region:west", "bob", new Z64(1)));
        spine.Integrate(IndexedSingleton("region:east", "carol", new Z64(1)));

        var west = spine.GroupFor("region:west");
        Assert.Equal(new Z64(2), west.WeightOf("alice"));
        Assert.Equal(new Z64(1), west.WeightOf("bob"));
        Assert.Equal(2, west.Count);

        var east = spine.GroupFor("region:east");
        Assert.Equal(new Z64(1), east.WeightOf("carol"));
        Assert.Single(east);

        Assert.True(spine.GroupFor("region:south").IsEmpty);
    }

    [Fact]
    public void GroupFor_SumsValueWeightsAcrossBatches()
    {
        var spine = new SpineIndexedZSetTrace<int, string, Z64>();
        spine.Integrate(IndexedSingleton(1, "x", new Z64(3)));
        spine.Integrate(IndexedSingleton(1, "x", new Z64(-1)));
        spine.Integrate(IndexedSingleton(1, "y", new Z64(2)));

        var g = spine.GroupFor(1);
        Assert.Equal(new Z64(2), g.WeightOf("x"));
        Assert.Equal(new Z64(2), g.WeightOf("y"));
    }

    [Fact]
    public void EntriesAndMaterialize_DropEntriesThatCancelToZero()
    {
        var spine = new SpineIndexedZSetTrace<int, string, Z64>();
        spine.Integrate(IndexedSingleton(1, "x", new Z64(2)));
        spine.Integrate(IndexedSingleton(1, "x", new Z64(-2)));

        Assert.Empty(spine.Entries());
        Assert.True(spine.Materialize().IsEmpty);
        Assert.True(spine.GroupFor(1).IsEmpty);
    }

    [Fact]
    public void TieredCompaction_CascadesAsExpected()
    {
        var spine = new SpineIndexedZSetTrace<int, string, Z64>(new TieredCompactionStrategy(4));
        for (var i = 0; i < 16; i++)
        {
            spine.Integrate(IndexedSingleton(i, "v", new Z64(1)));
        }

        Assert.Equal(3, spine.LevelCount);
        Assert.Single(spine.State.BatchSizes[2]);
        Assert.Equal(16, spine.State.BatchSizes[2][0]);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Pbt_MatchesFlatOracle(int batchesPerLevel)
    {
        var rng = new Random(Seed: 41 + batchesPerLevel);
        var spine = new SpineIndexedZSetTrace<int, int, Z64>(new TieredCompactionStrategy(batchesPerLevel));
        var oracle = new IndexedZSetBuilder<int, int, Z64>();

        for (var step = 0; step < 200; step++)
        {
            var delta = RandomDelta(rng, keySpace: 12, valueSpace: 6, maxEntries: 8);
            spine.Integrate(delta);
            foreach (var (k, group) in delta)
            {
                foreach (var (v, w) in group)
                {
                    oracle.Add(k, v, w);
                }
            }
        }

        var expected = oracle.Build();
        Assert.Equal(expected, spine.Materialize());

        // Per-key group lookups agree.
        foreach (var (k, expectedGroup) in expected)
        {
            Assert.Equal(expectedGroup, spine.GroupFor(k));
        }

        // Keys not in oracle return empty groups.
        for (var k = -5; k < 0; k++)
        {
            Assert.True(spine.GroupFor(k).IsEmpty);
        }
    }

    [Fact]
    public void Pbt_BatchCountRemainsBounded()
    {
        var spine = new SpineIndexedZSetTrace<int, int, Z64>(new TieredCompactionStrategy(4));
        var rng = new Random(Seed: 7);
        for (var step = 0; step < 500; step++)
        {
            spine.Integrate(RandomDelta(rng, keySpace: 20, valueSpace: 5, maxEntries: 6));
        }

        Assert.True(spine.BatchCount <= 4 * spine.LevelCount,
            $"unbounded growth: {spine.BatchCount} batches across {spine.LevelCount} levels");
    }

    [Fact]
    public void Pbt_EntriesAgreesWithMaterialize()
    {
        var rng = new Random(Seed: 55);
        var spine = new SpineIndexedZSetTrace<int, int, Z64>();
        for (var step = 0; step < 100; step++)
        {
            spine.Integrate(RandomDelta(rng, keySpace: 10, valueSpace: 5, maxEntries: 6));
        }

        var fromEntries = new Dictionary<int, ZSet<int, Z64>>();
        foreach (var (k, group) in spine.Entries())
        {
            fromEntries[k] = group;
        }

        foreach (var (k, expectedGroup) in spine.Materialize())
        {
            Assert.True(fromEntries.TryGetValue(k, out var actualGroup),
                $"key {k} present in Materialize but missing from Entries");
            Assert.Equal(expectedGroup, actualGroup);
        }

        Assert.Equal(spine.Materialize().GroupCount, fromEntries.Count);
    }

    private static IndexedZSet<TKey, TValue, Z64> IndexedSingleton<TKey, TValue>(TKey key, TValue value, Z64 weight)
        where TKey : notnull
        where TValue : notnull
    {
        var b = new IndexedZSetBuilder<TKey, TValue, Z64>();
        b.Add(key, value, weight);
        return b.Build();
    }

    private static IndexedZSet<int, int, Z64> RandomDelta(
        Random rng, int keySpace, int valueSpace, int maxEntries)
    {
        var entries = rng.Next(maxEntries + 1);
        if (entries == 0)
        {
            return IndexedZSet<int, int, Z64>.Empty;
        }

        var b = new IndexedZSetBuilder<int, int, Z64>();
        for (var i = 0; i < entries; i++)
        {
            var k = rng.Next(keySpace);
            var v = rng.Next(valueSpace);
            var w = rng.Next(-3, 4);
            if (w == 0)
            {
                continue;
            }

            b.Add(k, v, new Z64(w));
        }

        return b.Build();
    }
}
