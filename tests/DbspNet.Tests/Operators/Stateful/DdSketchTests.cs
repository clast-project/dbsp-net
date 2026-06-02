// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// Direct coverage of the <see cref="DdSketch"/> quantile summary — the
/// estimator the SQL <c>APPROX_PERCENTILE</c> / <c>MEDIAN</c> /
/// <c>PERCENTILE_CONT</c> aggregators are built on. The sketch's relative-error
/// contract is asserted within its <see cref="DdSketch.Alpha"/> bound; its
/// determinism and invertibility (the properties the incremental≡batch
/// equivalence relies on) are asserted exactly.
/// </summary>
public class DdSketchTests
{
    private static void Fill(DdSketch sketch, IEnumerable<double> values)
    {
        foreach (var v in values)
        {
            sketch.Add(v, 1);
        }
    }

    private static void AssertWithinAlpha(double expected, double? actual, double alpha = DdSketch.DefaultAlpha)
    {
        Assert.NotNull(actual);
        if (expected == 0.0)
        {
            Assert.Equal(0.0, actual!.Value, 9);
            return;
        }

        // The α bound is inclusive; allow a hair over for exact-boundary values
        // (e.g. the bucket edge) where floating-point pushes relError to α+ε.
        var relError = Math.Abs(actual!.Value - expected) / Math.Abs(expected);
        Assert.True(relError <= alpha + 1e-9, $"expected {expected}, got {actual} (rel error {relError:P3})");
    }

    [Fact]
    public void EmptySketch_ReturnsNull()
    {
        Assert.Null(new DdSketch().EstimateQuantile(0.5));
    }

    [Fact]
    public void Quantiles_OfRamp_AreWithinBound()
    {
        var sketch = new DdSketch();
        Fill(sketch, Enumerable.Range(1, 1000).Select(i => (double)i));

        AssertWithinAlpha(1, sketch.EstimateQuantile(0.0));
        AssertWithinAlpha(500, sketch.EstimateQuantile(0.5));
        AssertWithinAlpha(900, sketch.EstimateQuantile(0.9));
        AssertWithinAlpha(1000, sketch.EstimateQuantile(1.0));
    }

    [Fact]
    public void NegativeValues_AreOrderedBelowPositives()
    {
        var sketch = new DdSketch();
        Fill(sketch, Enumerable.Range(-500, 1001).Select(i => (double)i)); // -500..500

        AssertWithinAlpha(-500, sketch.EstimateQuantile(0.0));
        Assert.Equal(0.0, sketch.EstimateQuantile(0.5)!.Value, 9); // median of a symmetric ramp
        AssertWithinAlpha(500, sketch.EstimateQuantile(1.0));
    }

    [Fact]
    public void Multiplicity_IsHonored()
    {
        // 1 appears 100×, 100 appears once: the median sits firmly at 1.
        var sketch = new DdSketch();
        sketch.Add(1.0, 100);
        sketch.Add(100.0, 1);

        AssertWithinAlpha(1, sketch.EstimateQuantile(0.5));
    }

    [Fact]
    public void OrderAndRepetitionIndependent_SameEstimate()
    {
        // Determinism guarantee behind incremental≡batch: the estimate depends
        // only on the value multiset, not the order it was folded in.
        var forward = new DdSketch();
        Fill(forward, Enumerable.Range(1, 2000).Select(i => (double)i));

        var backward = new DdSketch();
        Fill(backward, Enumerable.Range(1, 2000).Reverse().Select(i => (double)i));

        foreach (var q in new[] { 0.0, 0.1, 0.5, 0.75, 0.99, 1.0 })
        {
            Assert.Equal(forward.EstimateQuantile(q), backward.EstimateQuantile(q));
        }
    }

    [Fact]
    public void Invertible_RetractionUndoesInsertion()
    {
        // Build {1..100}, then add and fully retract an outlier — the sketch
        // returns to byte-identical estimates (the buckets the outlier touched
        // are dropped back to zero).
        var sketch = new DdSketch();
        Fill(sketch, Enumerable.Range(1, 100).Select(i => (double)i));
        var before = sketch.EstimateQuantile(0.9);

        sketch.Add(1_000_000.0, 5);
        sketch.Add(1_000_000.0, -5);

        Assert.Equal(before, sketch.EstimateQuantile(0.9));
        Assert.Equal(100, sketch.Count);
    }

    [Fact]
    public void Merge_EqualsFoldingBothDirectly()
    {
        var a = new DdSketch();
        Fill(a, Enumerable.Range(1, 600).Select(i => (double)i));
        var b = new DdSketch();
        Fill(b, Enumerable.Range(601, 400).Select(i => (double)i));
        a.Merge(b);

        var combined = new DdSketch();
        Fill(combined, Enumerable.Range(1, 1000).Select(i => (double)i));

        foreach (var q in new[] { 0.0, 0.25, 0.5, 0.9, 1.0 })
        {
            Assert.Equal(combined.EstimateQuantile(q), a.EstimateQuantile(q));
        }
    }

    [Fact]
    public void Clear_ResetsToEmpty()
    {
        var sketch = new DdSketch();
        Fill(sketch, Enumerable.Range(1, 50).Select(i => (double)i));

        sketch.Clear();
        Assert.Null(sketch.EstimateQuantile(0.5));
        Assert.Equal(0, sketch.Count);
    }

    [Fact]
    public void BucketCount_IsBoundedByDynamicRange_NotCardinality()
    {
        // 100k values across a narrow range occupy far fewer buckets than rows.
        var sketch = new DdSketch();
        for (var i = 0; i < 100_000; i++)
        {
            sketch.Add(1.0 + (i % 1000) / 1000.0, 1); // values in [1, 2)
        }

        Assert.True(sketch.BucketCount < 200, $"buckets {sketch.BucketCount}");
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(1.0)]
    [InlineData(-0.5)]
    public void Alpha_OutOfRange_Throws(double alpha)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DdSketch(alpha));
    }

    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    public void Quantile_OutOfRange_Throws(double q)
    {
        var sketch = new DdSketch();
        sketch.Add(1.0, 1);
        Assert.Throws<ArgumentOutOfRangeException>(() => sketch.EstimateQuantile(q));
    }
}
