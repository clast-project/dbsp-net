// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// An <b>exact</b>, fully invertible quantile summary over <see cref="long"/>
/// keys — the sibling of <see cref="DdSketch"/> used for the temporal quantile
/// aggregates (DATE / TIMESTAMP, where the value is an integer day-/microsecond
/// count and DDSketch's relative-error bound on the epoch-offset magnitude would
/// be useless). Values are folded in with <see cref="Add"/> (a signed
/// multiplicity, so deletions pass a negative count) and a quantile is read back
/// with <see cref="EstimateQuantile"/>.
/// </summary>
/// <remarks>
/// <para><b>How it works.</b> The sketch keeps one signed integer count per
/// distinct value in a <see cref="SortedDictionary{TKey,TValue}"/>. A quantile
/// is answered by an ordered cumulative scan, so the result is the <i>true</i>
/// quantile of the folded multiset, not an approximation — <c>discrete</c>
/// returns an actual member (ANSI <c>PERCENTILE_DISC</c>), <c>continuous</c>
/// linearly interpolates between the two neighbouring members (ANSI
/// <c>PERCENTILE_CONT</c>) and rounds to the nearest key.</para>
/// <para><b>Determinism &amp; incrementality.</b> The value→count map is a pure,
/// order-independent function of the value <i>multiset</i> (folding is
/// commutative integer addition), so the same multiset always yields the same
/// counts and hence the same estimate. That is what lets the incremental
/// aggregator match a from-scratch batch recompute <i>exactly</i>. Because the
/// counts are signed the sketch is fully <b>invertible</b> — a retraction folds
/// in a negative count and a key that returns to zero is dropped — so unlike a
/// HyperLogLog there is no need to rebuild on deletes.</para>
/// <para><b>State size.</b> One <c>(long,long)</c> entry per <i>distinct</i>
/// value. Unlike <see cref="DdSketch"/> this is not bounded by dynamic range, so
/// state grows with the number of distinct values — acceptable for the temporal
/// columns this backs, and the price of exactness.</para>
/// </remarks>
public sealed class OrderedQuantileSketch
{
    // Signed count per distinct value; a key whose running count returns to zero
    // is removed so the map stays a function of the present multiset.
    private readonly SortedDictionary<long, long> _counts = new();
    private long _totalCount;

    /// <summary>Total multiplicity folded in (sum of all signed counts).</summary>
    public long Count => _totalCount;

    /// <summary>Number of distinct values retained — the state size.</summary>
    public int DistinctCount => _counts.Count;

    /// <summary>Reset the sketch to empty, reusing the backing map.</summary>
    public void Clear()
    {
        _counts.Clear();
        _totalCount = 0;
    }

    /// <summary>
    /// Fold <paramref name="value"/> in with multiplicity <paramref name="count"/>.
    /// A negative <paramref name="count"/> retracts; a value whose running count
    /// returns to zero is removed so the state stays a function of the present
    /// multiset.
    /// </summary>
    public void Add(long value, long count)
    {
        if (count == 0)
        {
            return;
        }

        if (_counts.TryGetValue(value, out var existing))
        {
            var updated = existing + count;
            if (updated == 0)
            {
                _counts.Remove(value);
            }
            else
            {
                _counts[value] = updated;
            }
        }
        else
        {
            _counts[value] = count;
        }

        _totalCount += count;
    }

    /// <summary>Merge every value of <paramref name="other"/> into this sketch.</summary>
    public void Merge(OrderedQuantileSketch other)
    {
        ArgumentNullException.ThrowIfNull(other);
        foreach (var (value, count) in other._counts)
        {
            Add(value, count);
        }
    }

    /// <summary>
    /// The exact value at quantile <paramref name="q"/> (0 = minimum,
    /// 0.5 = median, 1 = maximum) of the folded multiset, or <c>null</c> if the
    /// sketch is empty. When <paramref name="discrete"/> is true the result is an
    /// actual member of the multiset (ANSI <c>PERCENTILE_DISC</c>: the
    /// <c>ceil(q·N)</c>-th value in ascending order); otherwise it is the linear
    /// interpolation between the two neighbouring members (ANSI
    /// <c>PERCENTILE_CONT</c>) rounded to the nearest key.
    /// </summary>
    public long? EstimateQuantile(double q, bool discrete)
    {
        if (q is < 0.0 or > 1.0)
        {
            throw new ArgumentOutOfRangeException(nameof(q), q, "Quantile must be in [0, 1].");
        }

        if (_totalCount <= 0)
        {
            return null;
        }

        return discrete ? Discrete(q) : Continuous(q);
    }

    // PERCENTILE_DISC: the value at 1-based ordinal ceil(q·N) (at least 1).
    private long Discrete(double q)
    {
        var target = (long)Math.Ceiling(q * _totalCount);
        if (target < 1)
        {
            target = 1;
        }

        var cumulative = 0L;
        foreach (var (value, count) in _counts)
        {
            cumulative += count;
            if (cumulative >= target)
            {
                return value;
            }
        }

        // q == 1 (or floating-point slack at the top): the maximum value.
        return MaxValue();
    }

    // PERCENTILE_CONT: rank = q·(N-1) on the 0-based axis spanning the expanded
    // multiset; interpolate between the values at floor(rank) and ceil(rank).
    private long Continuous(double q)
    {
        var rank = q * (_totalCount - 1);
        var lowIndex = (long)Math.Floor(rank);
        var highIndex = (long)Math.Ceiling(rank);
        var frac = rank - lowIndex;

        var low = ValueAtIndex(lowIndex);
        if (highIndex == lowIndex)
        {
            return low;
        }

        var high = ValueAtIndex(highIndex);
        // Round to the nearest key (away from zero) so the result lands on the
        // type's granularity; deterministic ⇒ incremental ≡ batch.
        return low + (long)Math.Round(frac * (high - low), MidpointRounding.AwayFromZero);
    }

    // The value at a 0-based position in the ascending expanded multiset.
    private long ValueAtIndex(long index)
    {
        var cumulative = 0L;
        foreach (var (value, count) in _counts)
        {
            cumulative += count;
            if (cumulative > index)
            {
                return value;
            }
        }

        return MaxValue();
    }

    private long MaxValue()
    {
        var max = 0L;
        foreach (var key in _counts.Keys)
        {
            max = key;
        }

        return max;
    }
}
