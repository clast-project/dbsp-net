// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// A DDSketch quantile summary — the bounded-state, relative-error estimator
/// behind SQL <c>APPROX_PERCENTILE</c> / <c>MEDIAN</c> /
/// <c>PERCENTILE_CONT</c>. Values are folded in with <see cref="Add"/> (a
/// signed multiplicity, so deletions just pass a negative count) and a quantile
/// is read back with <see cref="EstimateQuantile"/>.
/// </summary>
/// <remarks>
/// <para><b>How it works.</b> A value <c>x &gt; 0</c> is mapped to the bucket
/// index <c>ceil(log_γ x)</c> with <c>γ = (1+α)/(1−α)</c>; every value in a
/// bucket is reported as that bucket's representative
/// <c>2·γ^i/(γ+1)</c>, which is within relative error <see cref="Alpha"/> of
/// any member. Negative values mirror into a second store keyed on their
/// magnitude; exact zeros are counted separately. The sketch therefore keeps,
/// per bucket, an integer count — and nothing else.</para>
/// <para><b>Determinism &amp; incrementality.</b> The bucket→count map is a
/// pure, order-independent function of the value <i>multiset</i>: folding is
/// commutative integer addition, so the same multiset always yields the same
/// counts and hence the same estimate. That is what lets the incremental
/// aggregator match a from-scratch batch recompute <i>exactly</i> (not merely
/// within the error bound). Because the counts are signed, the sketch is also
/// fully <b>invertible</b> — a retraction folds in a negative count and a
/// bucket that returns to zero is dropped — so unlike a HyperLogLog there is no
/// need to rebuild on deletes.</para>
/// <para><b>State size.</b> One <c>(int,long)</c> entry per occupied bucket.
/// The occupied-bucket count is bounded by the data's dynamic range —
/// <c>log_γ(max/min)</c> — independent of cardinality (≈ a few thousand
/// buckets even across a 1e±9 magnitude span at the default 1% accuracy). The
/// sketch does not collapse buckets, so within that naturally-bounded regime
/// the exact incremental≡batch equality above holds unconditionally.</para>
/// </remarks>
public sealed class DdSketch
{
    /// <summary>Default relative accuracy (1%).</summary>
    public const double DefaultAlpha = 0.01;

    private readonly double _logGamma;
    private readonly double _representativeFactor;

    // value > 0 keyed on ceil(log_γ value); value < 0 keyed on ceil(log_γ |value|).
    private readonly Dictionary<int, long> _positive = new();
    private readonly Dictionary<int, long> _negative = new();
    private long _zeroCount;
    private long _totalCount;

    public DdSketch(double alpha = DefaultAlpha)
    {
        // α in (0, 1): the relative-error target. Smaller α ⇒ γ closer to 1 ⇒
        // more, finer buckets.
        if (alpha is <= 0 or >= 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(alpha), alpha, "DDSketch relative accuracy must be in (0, 1).");
        }

        Alpha = alpha;
        var gamma = (1.0 + alpha) / (1.0 - alpha);
        _logGamma = Math.Log(gamma);

        // A bucket with index i covers (γ^(i-1), γ^i]; its representative
        // 2·γ^i/(γ+1) is the value within relative error α of every member.
        _representativeFactor = 2.0 / (gamma + 1.0);
    }

    /// <summary>The relative-accuracy target this sketch was built with.</summary>
    public double Alpha { get; }

    /// <summary>Total multiplicity folded in (sum of all signed counts).</summary>
    public long Count => _totalCount;

    /// <summary>Number of occupied buckets — the dominant term in the state size.</summary>
    public int BucketCount => _positive.Count + _negative.Count + (_zeroCount != 0 ? 1 : 0);

    /// <summary>Reset the sketch to empty, reusing the backing maps.</summary>
    public void Clear()
    {
        _positive.Clear();
        _negative.Clear();
        _zeroCount = 0;
        _totalCount = 0;
    }

    /// <summary>
    /// Fold <paramref name="value"/> in with multiplicity <paramref name="count"/>.
    /// A negative <paramref name="count"/> retracts; a bucket whose running count
    /// returns to zero is removed so the state stays a function of the present
    /// multiset.
    /// </summary>
    public void Add(double value, long count)
    {
        if (count == 0)
        {
            return;
        }

        if (value == 0.0)
        {
            _zeroCount += count;
        }
        else if (value > 0.0)
        {
            Bump(_positive, KeyOf(value), count);
        }
        else
        {
            Bump(_negative, KeyOf(-value), count);
        }

        _totalCount += count;
    }

    /// <summary>Merge every bucket of <paramref name="other"/> into this sketch.</summary>
    public void Merge(DdSketch other)
    {
        ArgumentNullException.ThrowIfNull(other);
        foreach (var (key, count) in other._positive)
        {
            Bump(_positive, key, count);
        }

        foreach (var (key, count) in other._negative)
        {
            Bump(_negative, key, count);
        }

        _zeroCount += other._zeroCount;
        _totalCount += other._totalCount;
    }

    /// <summary>
    /// Estimate the value at quantile <paramref name="q"/> (0 = minimum,
    /// 0.5 = median, 1 = maximum). Returns <c>null</c> for an empty sketch.
    /// The result is within relative error <see cref="Alpha"/> of the true
    /// quantile of the folded multiset.
    /// </summary>
    public double? EstimateQuantile(double q)
    {
        if (q is < 0.0 or > 1.0)
        {
            throw new ArgumentOutOfRangeException(nameof(q), q, "Quantile must be in [0, 1].");
        }

        if (_totalCount <= 0)
        {
            return null;
        }

        // Target rank on a 0-based axis spanning the whole multiset.
        var rank = q * (_totalCount - 1);

        // Smallest to largest: most-negative magnitudes first (negative store in
        // descending key order), then zeros, then the positive store ascending.
        var cumulative = 0L;

        if (_negative.Count > 0)
        {
            foreach (var key in SortedKeys(_negative, ascending: false))
            {
                cumulative += _negative[key];
                if (cumulative > rank)
                {
                    return -Representative(key);
                }
            }
        }

        cumulative += _zeroCount;
        if (cumulative > rank)
        {
            return 0.0;
        }

        var lastKey = int.MinValue;
        foreach (var key in SortedKeys(_positive, ascending: true))
        {
            cumulative += _positive[key];
            lastKey = key;
            if (cumulative > rank)
            {
                return Representative(key);
            }
        }

        // q == 1 (or floating-point slack at the top): the maximum bucket.
        return Representative(lastKey);
    }

    private int KeyOf(double magnitude) => (int)Math.Ceiling(Math.Log(magnitude) / _logGamma);

    private double Representative(int key) => _representativeFactor * Math.Exp(key * _logGamma);

    private static void Bump(Dictionary<int, long> buckets, int key, long count)
    {
        // Track the running count; drop the bucket when it returns to zero so a
        // fully-retracted value leaves no trace (keeps state ≡ present multiset).
        if (buckets.TryGetValue(key, out var existing))
        {
            var updated = existing + count;
            if (updated == 0)
            {
                buckets.Remove(key);
            }
            else
            {
                buckets[key] = updated;
            }
        }
        else
        {
            buckets[key] = count;
        }
    }

    private static int[] SortedKeys(Dictionary<int, long> buckets, bool ascending)
    {
        var keys = new int[buckets.Count];
        buckets.Keys.CopyTo(keys, 0);
        Array.Sort(keys);
        if (!ascending)
        {
            Array.Reverse(keys);
        }

        return keys;
    }
}
