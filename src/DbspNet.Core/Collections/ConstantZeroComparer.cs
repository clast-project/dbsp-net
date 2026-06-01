// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Collections;

/// <summary>
/// An <see cref="IComparer{T}"/> that reports every pair as equal. Used as the
/// tiebreak of a <see cref="SortKeyComparer{TRow}"/> to obtain a comparer that
/// orders by the sort keys alone — so <c>Compare(x, y) == 0</c> means "x and y
/// have equal ORDER BY keys", the tie-group test <c>RANK</c> / <c>DENSE_RANK</c>
/// need (as opposed to the full-row total order used to sort the trace).
/// </summary>
public sealed class ConstantZeroComparer<T> : IComparer<T>
{
    public static readonly ConstantZeroComparer<T> Instance = new();

    private ConstantZeroComparer()
    {
    }

    public int Compare(T? x, T? y) => 0;
}
