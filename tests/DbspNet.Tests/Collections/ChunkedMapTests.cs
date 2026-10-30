// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using Xunit;

namespace DbspNet.Tests.Collections;

/// <summary>
/// <see cref="ChunkedMap{TKey,TValue}"/> against <see cref="Dictionary{TKey,TValue}"/>
/// (docs/design-chunked-deltas.md): for any sequence of operations the two hold the same entries
/// and — the load-bearing part — enumerate them in the same order, including across the chunk
/// boundary, after removals that leave holes, and through a compacting copy.
/// </summary>
public class ChunkedMapTests
{
    [Theory]
    [InlineData(1, 0, 50)]
    [InlineData(2, 0, 5_000)]
    [InlineData(3, 3_000, 9_000)]
    [InlineData(4, 0, 40_000)]
    [InlineData(5, 100_000, 30_000)]
    public void RandomOperations_MatchDictionary_IncludingOrder(int seed, int capacity, int keySpace)
    {
        var random = new Random(seed);
        var reference = new Dictionary<StructuralRow, long>(capacity);
        var map = new ChunkedMap<StructuralRow, long>(capacity);
        StructuralRow Key() => new((long)random.Next(keySpace), random.Next(3) == 0 ? null : "s" + random.Next(4));

        for (var step = 0; step < 120_000; step++)
        {
            var key = Key();
            switch (random.Next(10))
            {
                case < 5:
                    reference[key] = step;
                    map[key] = step;
                    break;
                case < 8:
                    Assert.Equal(reference.Remove(key), map.Remove(key));
                    break;
                case 8:
                    Assert.Equal(reference.TryGetValue(key, out var a), map.TryGetValue(key, out var b));
                    Assert.Equal(a, b);
                    break;
                default:
                    if (random.Next(400) == 0)
                    {
                        reference.Clear();
                        map.Clear();
                    }

                    break;
            }

            if (step % 4_000 == 0)
            {
                AssertSameOrder(reference, map);
            }
        }

        AssertSameOrder(reference, map);
        AssertSameOrder(new Dictionary<StructuralRow, long>(reference), ChunkedMap<StructuralRow, long>.CopyOf(map));
        Assert.Equal(reference.Keys, map.Keys);
    }

    [Fact]
    public void LargeMap_KeepsEveryArrayUnderTheLohThreshold()
    {
        var map = new ChunkedMap<StructuralRow, long>();
        for (var i = 0; i < 1_000_000; i++)
        {
            map[new StructuralRow((long)i)] = i;
        }

        Assert.Equal(1_000_000, map.Count);
        Assert.True(map.LargestArrayBytes < 85_000, $"largest array {map.LargestArrayBytes} B");
        Assert.True(map.TryGetValue(new StructuralRow(123_456L), out var v));
        Assert.Equal(123_456, v);
    }

    private static void AssertSameOrder(Dictionary<StructuralRow, long> expected, ChunkedMap<StructuralRow, long> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        using var e = expected.GetEnumerator();
        foreach (var kv in actual)
        {
            Assert.True(e.MoveNext());
            Assert.Equal(e.Current.Key, kv.Key);
            Assert.Equal(e.Current.Value, kv.Value);
        }

        Assert.False(e.MoveNext());
    }
}
