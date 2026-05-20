// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;

namespace Clast.BloomFilter.Tests;

public class SplitBlockBloomFilterTests
{
    [Fact]
    public void NoFalseNegatives_ForInsertedValues()
    {
        var builder = new SplitBlockBloomFilterBuilder(SplitBlockBloomFilterBuilder.OptimalNumBytes(1024, 0.01, 1 << 20));
        for (int i = 0; i < 1024; i++)
        {
            builder.Add(Encoding.UTF8.GetBytes($"value-{i}"));
        }

        var filter = builder.Build();
        for (int i = 0; i < 1024; i++)
        {
            Assert.True(filter.MightContain(Encoding.UTF8.GetBytes($"value-{i}")));
        }
    }

    [Fact]
    public void FalsePositiveRate_BelowTarget()
    {
        const int distinct = 4096;
        const double targetFpp = 0.01;
        var builder = new SplitBlockBloomFilterBuilder(
            SplitBlockBloomFilterBuilder.OptimalNumBytes(distinct, targetFpp, 1 << 20));

        for (int i = 0; i < distinct; i++)
        {
            builder.Add(BitConverter.GetBytes(i));
        }

        var filter = builder.Build();

        int falsePositives = 0;
        const int probes = 10_000;
        for (int i = distinct; i < distinct + probes; i++)
        {
            if (filter.MightContain(BitConverter.GetBytes(i)))
                falsePositives++;
        }

        // Allow 3× headroom — SBBF FPP can run a bit higher than the
        // classic bloom formula predicts (single-block design trades
        // some FPP for cache locality).
        var observedFpp = (double)falsePositives / probes;
        Assert.True(observedFpp < targetFpp * 3,
            $"Observed FPP {observedFpp:F4} exceeds 3× target {targetFpp:F4}");
    }

    [Fact]
    public void RawDataRoundTrips()
    {
        var builder = new SplitBlockBloomFilterBuilder(64);
        builder.Add(Encoding.UTF8.GetBytes("hello"));
        var bytes = builder.ToArray();

        var filter = new SplitBlockBloomFilter(bytes);
        Assert.True(filter.MightContain(Encoding.UTF8.GetBytes("hello")));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(33)]
    public void InvalidSize_Throws(int numBytes)
    {
        Assert.Throws<ArgumentException>(() => new SplitBlockBloomFilterBuilder(numBytes));
    }

    [Theory]
    [InlineData(100, 0.01)]
    [InlineData(1000, 0.01)]
    [InlineData(10_000, 0.01)]
    public void OptimalNumBytes_IsBlockAligned(int ndv, double fpp)
    {
        int n = SplitBlockBloomFilterBuilder.OptimalNumBytes(ndv, fpp, 1 << 20);
        Assert.Equal(0, n % 32);
        // Classic bloom: -n·ln(p) / ln(2)² bits. Allow 25% slack
        // either way to keep the test pinned to algebra, not to a
        // specific rounding rule.
        var expectedBits = -ndv * Math.Log(fpp) / (Math.Log(2) * Math.Log(2));
        var expectedBytes = expectedBits / 8.0;
        Assert.InRange(n, expectedBytes * 0.75, expectedBytes * 1.25 + 32);
    }

    [Fact]
    public void OptimalNumBytes_RespectsMaxBytes()
    {
        int n = SplitBlockBloomFilterBuilder.OptimalNumBytes(1_000_000, 0.0001, 1024);
        Assert.Equal(1024, n);
    }
}
