// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Numerics;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// SQL AVG. Computed as SUM / COUNT in <see cref="double"/>. Returns
/// <see cref="Optional{T}.None"/> for empty groups or groups whose net
/// weight is zero (SQL's COUNT = 0 → NULL average).
/// </summary>
public sealed class AvgAggregator<T> : IAggregator<T, double>
    where T : struct, INumber<T>
{
    public Optional<double> Compute(IMultiset<T, Z64> multiset)
    {
        ArgumentNullException.ThrowIfNull(multiset);
        if (multiset.IsEmpty)
        {
            return Optional<double>.None;
        }

        var sum = 0.0;
        var count = 0L;
        foreach (var (v, w) in multiset)
        {
            var weight = w.Value;
            sum += double.CreateChecked(v) * weight;
            count += weight;
        }

        if (count == 0)
        {
            return Optional<double>.None;
        }

        return Optional<double>.Some(sum / count);
    }
}
