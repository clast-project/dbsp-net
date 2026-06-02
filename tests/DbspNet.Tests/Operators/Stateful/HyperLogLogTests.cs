// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// Direct coverage of the <see cref="HyperLogLog"/> cardinality sketch — the
/// estimator the SQL <c>APPROX_COUNT_DISTINCT</c> aggregators are built on.
/// </summary>
public class HyperLogLogTests
{
    // A fixed splitmix64-style spreader so the tests fold well-distributed
    // hashes (the SQL layer's HllHashing does the same finalize step).
    private static ulong Spread(ulong x)
    {
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9UL;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBUL;
        x ^= x >> 31;
        return x;
    }

    [Fact]
    public void EmptySketch_EstimatesZero()
    {
        Assert.Equal(0, new HyperLogLog().EstimateCardinality());
    }

    [Fact]
    public void SmallCardinality_IsExact_ViaLinearCounting()
    {
        var hll = new HyperLogLog();
        for (var i = 0; i < 10; i++)
        {
            hll.AddHash(Spread((ulong)i));
        }

        Assert.Equal(10, hll.EstimateCardinality());
    }

    [Fact]
    public void DuplicateHashes_DoNotInflate()
    {
        var hll = new HyperLogLog();
        for (var rep = 0; rep < 100; rep++)
        {
            for (var i = 0; i < 5; i++)
            {
                hll.AddHash(Spread((ulong)i));
            }
        }

        Assert.Equal(5, hll.EstimateCardinality());
    }

    [Fact]
    public void LargeCardinality_WithinErrorBound()
    {
        var hll = new HyperLogLog();
        const int distinct = 100_000;
        for (var i = 0; i < distinct; i++)
        {
            hll.AddHash(Spread((ulong)i));
        }

        var estimate = hll.EstimateCardinality();
        var error = Math.Abs(estimate - distinct) / (double)distinct;
        Assert.True(error <= 0.03, $"estimate {estimate}, error {error:P2}");
    }

    [Fact]
    public void OrderAndRepetitionIndependent_SameRegisters()
    {
        // Determinism guarantee the incremental≡batch equivalence relies on:
        // the estimate depends only on the set of hashes, not their order or
        // multiplicity.
        var forward = new HyperLogLog();
        for (var i = 0; i < 2000; i++)
        {
            forward.AddHash(Spread((ulong)i));
        }

        var shuffled = new HyperLogLog();
        for (var i = 1999; i >= 0; i--)
        {
            shuffled.AddHash(Spread((ulong)i));
            shuffled.AddHash(Spread((ulong)i)); // repeated — must be idempotent
        }

        Assert.Equal(forward.EstimateCardinality(), shuffled.EstimateCardinality());
    }

    [Fact]
    public void Clear_ResetsToEmpty()
    {
        var hll = new HyperLogLog();
        for (var i = 0; i < 50; i++)
        {
            hll.AddHash(Spread((ulong)i));
        }

        hll.Clear();
        Assert.Equal(0, hll.EstimateCardinality());
    }

    [Theory]
    [InlineData(3)]
    [InlineData(19)]
    public void Precision_OutOfRange_Throws(int precision)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new HyperLogLog(precision));
    }
}
