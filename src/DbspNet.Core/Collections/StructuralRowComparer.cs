// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Collections;

/// <summary>
/// A total order over <see cref="StructuralRow"/>, supplied to the spine trace
/// family (<c>SpineZSetTrace</c> / <c>SpineIndexedZSetTrace</c>), whose batches
/// store keys (and inner values) in sorted order and therefore require an
/// <see cref="IComparer{T}"/>. The flat dictionary-backed traces only need
/// equality + hashing, so <see cref="StructuralRow"/> itself does not implement
/// <see cref="IComparable{T}"/>; this comparer fills that gap for the spine path.
/// </summary>
/// <remarks>
/// <para>
/// Ordering is lexicographic: columns are compared left to right; the first
/// differing column decides. Within a column slot, <c>null</c> (SQL NULL) sorts
/// before any non-null value, and two non-null values are compared through their
/// runtime non-generic <see cref="IComparable"/> implementation — the same
/// primitive the MIN/MAX aggregator relies on. If all shared columns are equal,
/// the shorter row sorts first (an arity tiebreak that, in practice, only ever
/// fires for the zero-column unit key used by scalar-subquery fan-out, where all
/// keys are the empty row).
/// </para>
/// <para>
/// This order is consistent with <see cref="StructuralRow.Equals(StructuralRow)"/>:
/// <c>Compare(x, y) == 0</c> exactly when <c>x.Equals(y)</c>. Every value in a
/// given column shares one SQL type (and, for DECIMAL, one scale), so
/// <see cref="IComparable.CompareTo"/> returning zero coincides with element
/// equality there — which is what the spine relies on to consolidate same-key
/// weights.
/// </para>
/// <para>
/// Non-null elements must implement non-generic <see cref="IComparable"/>; every
/// SQL scalar value type (<c>long</c>, <c>double</c>, <c>bool</c>, <c>Decimal128</c>,
/// <c>Utf8String</c>, the temporal types) does. A non-comparable element throws
/// <see cref="InvalidCastException"/> — that is a compiler wiring bug, not a data
/// condition.
/// </para>
/// </remarks>
public sealed class StructuralRowComparer : IComparer<StructuralRow>
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly StructuralRowComparer Instance = new();

    private StructuralRowComparer()
    {
    }

    public int Compare(StructuralRow? x, StructuralRow? y)
    {
        if (ReferenceEquals(x, y))
        {
            return 0;
        }

        // Defensive: SQL rows are never null references, but IComparer must
        // still define a total order over the nullable parameter type.
        if (x is null)
        {
            return -1;
        }

        if (y is null)
        {
            return 1;
        }

        var nx = x.Count;
        var ny = y.Count;
        var shared = nx < ny ? nx : ny;
        for (var i = 0; i < shared; i++)
        {
            var c = CompareElement(x[i], y[i]);
            if (c != 0)
            {
                return c;
            }
        }

        // Every shared column is equal — the shorter row sorts first.
        return nx.CompareTo(ny);
    }

    private static int CompareElement(object? a, object? b)
    {
        if (a is null)
        {
            return b is null ? 0 : -1;
        }

        if (b is null)
        {
            return 1;
        }

        return ((IComparable)a).CompareTo(b);
    }
}
