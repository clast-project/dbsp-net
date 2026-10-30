// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.Collections;

public class ZSetTests
{
    private static readonly Gen<Z64> GenWeight =
        Gen.Long[-5, 5].Select(v => new Z64(v));

    // keys drawn from a small alphabet so property tests exercise merging
    private static readonly Gen<ZSet<string, Z64>> GenZSet =
        Gen.Select(Gen.OneOfConst("a", "b", "c", "d"), GenWeight)
           .Array[0, 8]
           .Select(entries => ZSet.FromEntries(entries));

    [Fact]
    public void Empty_IsEmpty()
    {
        var z = ZSet.Empty<string, Z64>();
        Assert.True(z.IsEmpty);
        Assert.Equal(0, z.Count);
    }

    [Fact]
    public void Singleton_PreservesKeyAndWeight()
    {
        var z = ZSet.Singleton("a", new Z64(3));
        Assert.Equal(1, z.Count);
        Assert.Equal(new Z64(3), z.WeightOf("a"));
        Assert.Equal(Z64.Zero, z.WeightOf("b"));
    }

    [Fact]
    public void Singleton_WithZeroWeight_IsEmpty()
    {
        var z = ZSet.Singleton("a", Z64.Zero);
        Assert.True(z.IsEmpty);
    }

    [Fact]
    public void Builder_AccumulatesWeightsForSameKey()
    {
        var b = new ZSetBuilder<string, Z64>();
        b.Add("a", new Z64(2));
        b.Add("a", new Z64(3));
        b.Add("b", new Z64(1));
        var z = b.Build();

        Assert.Equal(2, z.Count);
        Assert.Equal(new Z64(5), z.WeightOf("a"));
        Assert.Equal(new Z64(1), z.WeightOf("b"));
    }

    [Fact]
    public void Builder_OppositeWeights_CancelToRemoval()
    {
        var b = new ZSetBuilder<string, Z64>();
        b.Add("a", new Z64(3));
        b.Add("a", new Z64(-3));
        var z = b.Build();

        Assert.True(z.IsEmpty);
        Assert.False(z.Contains("a"));
    }

    [Fact]
    public void Builder_NeverStoresZeroWeight_AfterAdds()
    {
        var b = new ZSetBuilder<string, Z64>();
        b.Add("a", Z64.Zero);
        var z = b.Build();
        Assert.True(z.IsEmpty);
    }

    [Fact]
    public void Builder_UseAfterBuild_Throws()
    {
        var b = new ZSetBuilder<string, Z64>();
        b.Add("a", new Z64(1));
        _ = b.Build();
        Assert.Throws<InvalidOperationException>(() => b.Add("b", new Z64(1)));
    }

    [Fact]
    public void Plus_AccumulatesWeights()
    {
        var a = ZSet.FromEntries(new[] { ("x", new Z64(1)), ("y", new Z64(2)) });
        var b = ZSet.FromEntries(new[] { ("x", new Z64(3)), ("z", new Z64(4)) });
        var sum = a + b;

        Assert.Equal(new Z64(4), sum.WeightOf("x"));
        Assert.Equal(new Z64(2), sum.WeightOf("y"));
        Assert.Equal(new Z64(4), sum.WeightOf("z"));
    }

    [Fact]
    public void Minus_Negates()
    {
        var a = ZSet.FromEntries(new[] { ("x", new Z64(3)) });
        var b = ZSet.FromEntries(new[] { ("x", new Z64(1)) });
        Assert.Equal(ZSet.Singleton("x", new Z64(2)), a - b);
    }

    [Fact]
    public void Plus_IsCommutative()
    {
        Gen.Select(GenZSet, GenZSet).Sample((a, b) => (a + b).Equals(b + a));
    }

    [Fact]
    public void Plus_IsAssociative()
    {
        Gen.Select(GenZSet, GenZSet, GenZSet)
           .Sample((a, b, c) => ((a + b) + c).Equals(a + (b + c)));
    }

    [Fact]
    public void Empty_IsAdditiveIdentity()
    {
        GenZSet.Sample(a =>
            (a + ZSet.Empty<string, Z64>()).Equals(a) &&
            (ZSet.Empty<string, Z64>() + a).Equals(a));
    }

    [Fact]
    public void Negation_IsAdditiveInverse()
    {
        GenZSet.Sample(a => (a + (-a)).IsEmpty);
    }

    [Fact]
    public void ScalarMultiply_ByZero_IsEmpty()
    {
        GenZSet.Sample(a => a.ScalarMultiply(Z64.Zero).IsEmpty);
    }

    [Fact]
    public void ScalarMultiply_ByOne_IsIdentity()
    {
        GenZSet.Sample(a => a.ScalarMultiply(Z64.One).Equals(a));
    }

    [Fact]
    public void ScalarMultiply_DistributesOverPlus()
    {
        var smallWeight = Gen.Long[-3, 3].Select(v => new Z64(v));
        Gen.Select(GenZSet, GenZSet, smallWeight).Sample((a, b, s) =>
            (a + b).ScalarMultiply(s).Equals(a.ScalarMultiply(s) + b.ScalarMultiply(s)));
    }

    [Fact]
    public void Filter_KeepsMatchingKeys()
    {
        var a = ZSet.FromEntries(new[] { ("x", new Z64(1)), ("y", new Z64(2)), ("z", new Z64(3)) });
        var filtered = a.Filter(k => k != "y");

        Assert.Equal(2, filtered.Count);
        Assert.Equal(new Z64(1), filtered.WeightOf("x"));
        Assert.Equal(Z64.Zero, filtered.WeightOf("y"));
        Assert.Equal(new Z64(3), filtered.WeightOf("z"));
    }

    [Fact]
    public void MapKeys_MergesCollidedKeys()
    {
        var a = ZSet.FromEntries(new[] { (1, new Z64(1)), (2, new Z64(2)), (11, new Z64(3)) });
        // Map to tens digit — keys 1 and 11 both map to "1"
        var mapped = a.MapKeys(i => (i % 10).ToString(System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(new Z64(4), mapped.WeightOf("1")); // 1 + 3
        Assert.Equal(new Z64(2), mapped.WeightOf("2"));
    }

    [Fact]
    public void Equality_IsStructural()
    {
        var a = ZSet.FromEntries(new[] { ("x", new Z64(1)), ("y", new Z64(2)) });
        var b = ZSet.FromEntries(new[] { ("y", new Z64(2)), ("x", new Z64(1)) });
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void FromKeys_GivesEachKeyWeightOne()
    {
        var z = ZSet.FromKeys<string, Z64>(new[] { "a", "b", "a" });
        Assert.Equal(new Z64(2), z.WeightOf("a"));
        Assert.Equal(new Z64(1), z.WeightOf("b"));
    }

    [Fact]
    public void ToString_IsOrderIndependent()
    {
        var a = ZSet.FromEntries(new[] { ("x", new Z64(1)), ("y", new Z64(2)) });
        var b = ZSet.FromEntries(new[] { ("y", new Z64(2)), ("x", new Z64(1)) });
        Assert.Equal(a.ToString(), b.ToString());
    }

    // A key and a weight, the weight possibly zero (a Singleton of zero is empty).
    private static readonly Gen<(string Key, Z64 Weight)> GenEntry =
        Gen.Select(Gen.OneOfConst("a", "b", "c", "d"), GenWeight);

    [Fact]
    public void InlineGroups_MergeAndReadLikeADictionary()
    {
        // A group born inline (Singleton, or Clone of a one-entry set — how an indexed
        // trace stores a new key) then merged with arbitrary deltas must behave exactly
        // like a dictionary-backed set built from the same entries, in every operation.
        Gen.Select(GenEntry, GenZSet.Array[0, 6], Gen.Bool).Sample(t =>
        {
            var (first, deltas, viaClone) = t;
            // Singleton of a zero weight is the shared Empty, never merged into in place.
            var group = viaClone || first.Weight.Value == 0
                ? ZSet.FromEntries(new[] { first }).Clone()
                : ZSet.Singleton(first.Key, first.Weight);
            var reference = new Dictionary<string, long>();
            void Fold(string k, long w)
            {
                var v = reference.GetValueOrDefault(k) + w;
                if (v == 0) reference.Remove(k);
                else reference[k] = v;
            }

            Fold(first.Key, first.Weight.Value);
            AssertSame(reference, group);
            foreach (var delta in deltas)
            {
                group.MergeInPlace(delta);
                foreach (var (k, w) in delta)
                {
                    Fold(k, w.Value);
                }

                AssertSame(reference, group);
            }

            var dictionaryBacked = ZSet.FromEntries(reference.Select(kv => (kv.Key, new Z64(kv.Value))));
            Assert.True(group.Equals(dictionaryBacked) && dictionaryBacked.Equals(group));
            Assert.Equal(dictionaryBacked.GetHashCode(), group.GetHashCode());
            Assert.Equal(dictionaryBacked.ToString(), group.ToString());
            foreach (var other in deltas)
            {
                Assert.Equal(dictionaryBacked + other, group + other);
                Assert.Equal(other + dictionaryBacked, other + group);
                Assert.Equal(dictionaryBacked - other, group - other);
            }

            Assert.Equal(-dictionaryBacked, -group);
            Assert.Equal(dictionaryBacked.Clone(), group.Clone());
            var removed = group.RemoveKeysBelow(1, k => k[0] - 'a');
            Assert.Equal(reference.ContainsKey("a") ? 1 : 0, removed);
            Assert.False(group.Contains("a"));
        });
    }

    private static void AssertSame(Dictionary<string, long> reference, ZSet<string, Z64> set)
    {
        Assert.Equal(reference.Count, set.Count);
        Assert.Equal(reference.Count == 0, set.IsEmpty);
        Assert.Equal(reference.Values.Sum(), set.SumWeights().Value);
        Assert.Equal(reference.OrderBy(kv => kv.Key), set.Select(kv => KeyValuePair.Create(kv.Key, kv.Value.Value)).OrderBy(kv => kv.Key));
        foreach (var k in new[] { "a", "b", "c", "d" })
        {
            Assert.Equal(reference.GetValueOrDefault(k), set.WeightOf(k).Value);
            Assert.Equal(reference.ContainsKey(k), set.Contains(k));
        }

        Assert.Equal(reference.Keys.OrderBy(k => k), set.Keys.OrderBy(k => k));
    }
}
