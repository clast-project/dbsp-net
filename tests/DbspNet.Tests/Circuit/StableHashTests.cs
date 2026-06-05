// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;

namespace DbspNet.Tests.Circuit;

public class StableHashTests
{
    [Fact]
    public void Of_IsDeterministic_ForEachKeyType()
    {
        Assert.Equal(StableHash.Of(12345), StableHash.Of(12345));
        Assert.Equal(StableHash.Of(9_000_000_000L), StableHash.Of(9_000_000_000L));
        Assert.Equal(StableHash.Of("orders"), StableHash.Of("orders"));
        Assert.Equal(StableHash.Combine(1, 2, 3), StableHash.Combine(1, 2, 3));
    }

    [Fact]
    public void Of_PinnedValues_GuardAgainstAccidentalAlgorithmChange()
    {
        // Golden values, computed independently (not echoed from the impl): if
        // these change, sharding of a recovered circuit would shift, so the
        // change must be deliberate (and a migration considered).
        Assert.Equal(0, StableHash.Of(0)); // fmix32(0) == 0
        Assert.Equal(1_364_076_727, StableHash.Of(1));
        Assert.Equal(142_593_372, StableHash.Of(42));
        Assert.Equal(-138_115_112, StableHash.Of(9_000_000_000L));
        Assert.Equal(1_597_159_832, StableHash.Of("orders"));
        Assert.Equal(-1_215_916_109, StableHash.Combine(1, 2, 3));
    }

    [Fact]
    public void Combine_IsOrderSensitive()
    {
        Assert.NotEqual(StableHash.Combine(1, 2), StableHash.Combine(2, 1));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void Of_DistributesIntKeysAcrossBuckets(int workers)
    {
        // Deterministic input + deterministic hash, so this never flakes: a fixed
        // run that simply asserts no bucket is starved or swamped.
        const int n = 8000;
        var counts = new int[workers];
        for (var k = 0; k < n; k++)
        {
            counts[((StableHash.Of(k) % workers) + workers) % workers]++;
        }

        var ideal = (double)n / workers;
        foreach (var count in counts)
        {
            Assert.InRange(count, ideal * 0.6, ideal * 1.4);
        }
    }

    [Fact]
    public void Of_String_DependsOnContent()
    {
        Assert.NotEqual(StableHash.Of("abc"), StableHash.Of("abd"));
        Assert.NotEqual(StableHash.Of("ab"), StableHash.Of("ba"));
    }

    [Fact]
    public void Of_ByteSpan_IsDeterministicAndContentSensitive()
    {
        ReadOnlySpan<byte> a = "orders"u8;
        ReadOnlySpan<byte> b = "orders"u8;
        ReadOnlySpan<byte> c = "orderz"u8;
        Assert.Equal(StableHash.Of(a), StableHash.Of(b));
        Assert.NotEqual(StableHash.Of(a), StableHash.Of(c));
    }
}
