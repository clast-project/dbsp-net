// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// SQL MIN / MAX. Non-invertible under retractions — the operator scans the
/// full per-group value multiset to find the smallest/largest value with
/// positive weight. Empty groups return <see cref="Optional{T}.None"/>. NULL
/// input values are not supported at this layer; the caller must pre-filter.
/// </summary>
public sealed class MinMaxAggregator<T> : IAggregator<T, T>
    where T : notnull
{
    private readonly bool _wantMin;
    private readonly IComparer<T> _comparer;

    private MinMaxAggregator(bool wantMin, IComparer<T> comparer)
    {
        _wantMin = wantMin;
        _comparer = comparer;
    }

    public static MinMaxAggregator<T> Min(IComparer<T>? comparer = null)
        => new(wantMin: true, comparer ?? Comparer<T>.Default);

    public static MinMaxAggregator<T> Max(IComparer<T>? comparer = null)
        => new(wantMin: false, comparer ?? Comparer<T>.Default);

    public Optional<T> Compute(IMultiset<T, Z64> multiset)
    {
        ArgumentNullException.ThrowIfNull(multiset);

        var found = false;
        T best = default!;
        foreach (var (v, w) in multiset)
        {
            if (!Z64.IsPositive(w))
            {
                continue;
            }

            if (!found)
            {
                best = v;
                found = true;
                continue;
            }

            var cmp = _comparer.Compare(v, best);
            if (_wantMin ? cmp < 0 : cmp > 0)
            {
                best = v;
            }
        }

        return found ? Optional<T>.Some(best) : Optional<T>.None;
    }
}
