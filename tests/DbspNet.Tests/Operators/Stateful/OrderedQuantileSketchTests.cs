// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// Direct coverage of the <see cref="OrderedQuantileSketch"/> — the exact,
/// invertible estimator behind the DATE/TIMESTAMP quantile aggregates. Unlike
/// <see cref="DdSketch"/> the answers are the <i>true</i> quantile of the folded
/// multiset, so they are asserted exactly; determinism and invertibility (the
/// properties the incremental≡batch equivalence relies on) are likewise exact.
/// </summary>
public class OrderedQuantileSketchTests
{
    private static OrderedQuantileSketch Of(params long[] values)
    {
        var sketch = new OrderedQuantileSketch();
        foreach (var v in values)
        {
            sketch.Add(v, 1);
        }

        return sketch;
    }

    [Fact]
    public void EmptySketch_ReturnsNull()
    {
        Assert.Null(new OrderedQuantileSketch().EstimateQuantile(0.5, discrete: false));
        Assert.Null(new OrderedQuantileSketch().EstimateQuantile(0.5, discrete: true));
    }

    [Fact]
    public void Continuous_OddCount_HitsTheMiddleMember()
    {
        var s = Of(10, 20, 30, 40, 50);
        Assert.Equal(10, s.EstimateQuantile(0.0, discrete: false));
        Assert.Equal(30, s.EstimateQuantile(0.5, discrete: false));
        Assert.Equal(50, s.EstimateQuantile(1.0, discrete: false));
    }

    [Fact]
    public void Continuous_EvenCount_InterpolatesAndRounds()
    {
        // {10, 30}: rank 0.5 lands halfway → 20 (exact midpoint).
        Assert.Equal(20, Of(10, 30).EstimateQuantile(0.5, discrete: false));

        // {1, 2, 3, 4}: PERCENTILE_CONT(0.5) = 2.5 → rounds away from zero to 3.
        Assert.Equal(3, Of(1, 2, 3, 4).EstimateQuantile(0.5, discrete: false));
    }

    [Fact]
    public void Discrete_ReturnsAMember_DifferingFromContinuous()
    {
        // {1, 2, 3, 4}: DISC(0.5) = ceil(0.5·4)=2nd value = 2; CONT(0.5)=3.
        var s = Of(1, 2, 3, 4);
        Assert.Equal(2, s.EstimateQuantile(0.5, discrete: true));
        Assert.Equal(3, s.EstimateQuantile(0.5, discrete: false));

        // DISC always lands on an actual member.
        Assert.Equal(1, s.EstimateQuantile(0.0, discrete: true));
        Assert.Equal(4, s.EstimateQuantile(1.0, discrete: true));
    }

    [Fact]
    public void NegativeKeys_OrderBelowPositives()
    {
        var s = Of(-30, -10, 0, 10, 30);
        Assert.Equal(-30, s.EstimateQuantile(0.0, discrete: false));
        Assert.Equal(0, s.EstimateQuantile(0.5, discrete: false));
        Assert.Equal(30, s.EstimateQuantile(1.0, discrete: false));
    }

    [Fact]
    public void Multiplicity_IsHonoured()
    {
        var s = new OrderedQuantileSketch();
        s.Add(10, 1);
        s.Add(20, 3); // 20 appears three times → median of {10,20,20,20} = 20
        Assert.Equal(4, s.Count);
        Assert.Equal(20, s.EstimateQuantile(0.5, discrete: true));
    }

    [Fact]
    public void Invertible_RetractionDropsKeyAtZero()
    {
        var s = Of(10, 20, 30);
        s.Add(30, -1);
        s.Add(20, -1);
        Assert.Equal(1, s.Count);
        Assert.Equal(1, s.DistinctCount);
        Assert.Equal(10, s.EstimateQuantile(0.5, discrete: false));

        s.Add(10, -1);
        Assert.Equal(0, s.Count);
        Assert.Null(s.EstimateQuantile(0.5, discrete: false));
    }

    [Fact]
    public void OrderIndependent_SameMultisetSameAnswer()
    {
        var forward = Of(5, 1, 9, 3, 7);
        var shuffled = Of(9, 7, 5, 3, 1);
        for (var q = 0.0; q <= 1.0; q += 0.1)
        {
            Assert.Equal(
                forward.EstimateQuantile(q, discrete: false),
                shuffled.EstimateQuantile(q, discrete: false));
            Assert.Equal(
                forward.EstimateQuantile(q, discrete: true),
                shuffled.EstimateQuantile(q, discrete: true));
        }
    }

    [Fact]
    public void Merge_CombinesMultisets()
    {
        var a = Of(1, 2, 3);
        var b = Of(4, 5, 6);
        a.Merge(b);
        Assert.Equal(6, a.Count);
        // {1..6}: CONT(0.5)=3.5 → rounds to 4.
        Assert.Equal(4, a.EstimateQuantile(0.5, discrete: false));
    }
}
