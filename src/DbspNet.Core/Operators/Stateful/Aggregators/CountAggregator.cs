// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// SQL <c>COUNT(*)</c>. Always returns <see cref="Optional{T}.Some"/> —
/// empty groups yield 0. SQL <c>COUNT(col)</c> semantics (skip NULL) is
/// handled one layer up by pre-filtering the input multiset.
/// </summary>
public sealed class CountStarAggregator<T> : IAggregator<T, long>
    where T : notnull
{
    public Optional<long> Compute(IMultiset<T, Z64> multiset)
    {
        ArgumentNullException.ThrowIfNull(multiset);
        var total = 0L;
        foreach (var (_, w) in multiset)
        {
            total += w.Value;
        }

        return Optional<long>.Some(total);
    }
}
