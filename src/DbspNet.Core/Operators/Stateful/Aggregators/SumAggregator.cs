// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Numerics;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// SQL SUM. Returns <see cref="Optional{T}.None"/> for an empty group.
/// NULL input values are not supported at this layer; the caller must
/// pre-filter (or wrap with nullable handling in Phase 5).
/// </summary>
public sealed class SumAggregator<T> : IAggregator<T, T>
    where T : struct, INumber<T>
{
    public Optional<T> Compute(IMultiset<T, Z64> multiset)
    {
        ArgumentNullException.ThrowIfNull(multiset);
        if (multiset.IsEmpty)
        {
            return Optional<T>.None;
        }

        var sum = T.Zero;
        foreach (var (v, w) in multiset)
        {
            sum += v * T.CreateChecked(w.Value);
        }

        return Optional<T>.Some(sum);
    }
}
