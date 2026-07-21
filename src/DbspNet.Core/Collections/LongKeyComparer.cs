// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Collections;

/// <summary>
/// A monomorphized single-key order comparer — the unboxed twin of a
/// one-key <see cref="SortKeyComparer{TRow}"/>. Where <see cref="SortKeyComparer{TRow}"/>
/// pulls the sort key as a boxed <see cref="object"/> and compares it through
/// the non-generic <see cref="System.IComparable"/> (one heap box per key per
/// comparison for a value-type key), this reads the key as an <b>unboxed</b>
/// <see cref="long"/> (via <paramref name="keyOf"/> returning <c>long?</c> — a
/// value, no heap allocation) and compares longs directly.
/// </summary>
/// <remarks>
/// Semantics mirror <see cref="SortKeyComparer{TRow}"/> exactly for a single key:
/// NULL position is absolute (decided by <c>nullsFirst</c>, not flipped by
/// <c>descending</c>); non-null values compare then negate for <c>DESC</c>; ties
/// fall through to <c>tieBreak</c> (a row-level total order) so
/// <c>Compare(x, y) == 0 ⟺ x equals y</c>, which the window operator's
/// <c>SortedDictionary</c> keying relies on. Callers supply a <paramref name="keyOf"/>
/// whose <c>long</c> ordering is monotone in the original key's ordering (e.g. a
/// <c>Date32</c>'s day number, a <c>Timestamp</c>'s microseconds — see the SQL
/// compiler's monotone-key extraction), so the induced order is identical to the
/// boxed comparer's.
/// </remarks>
public sealed class LongKeyComparer<TRow> : IComparer<TRow>
    where TRow : notnull
{
    private readonly System.Func<TRow, long?> _keyOf;
    private readonly bool _descending;
    private readonly bool _nullsFirst;
    private readonly IComparer<TRow> _tieBreak;

    public LongKeyComparer(
        System.Func<TRow, long?> keyOf, bool descending, bool nullsFirst, IComparer<TRow> tieBreak)
    {
        System.ArgumentNullException.ThrowIfNull(keyOf);
        System.ArgumentNullException.ThrowIfNull(tieBreak);
        _keyOf = keyOf;
        _descending = descending;
        _nullsFirst = nullsFirst;
        _tieBreak = tieBreak;
    }

    public int Compare(TRow? x, TRow? y)
    {
        if (ReferenceEquals(x, y))
        {
            return 0;
        }

        if (x is null)
        {
            return -1;
        }

        if (y is null)
        {
            return 1;
        }

        var a = _keyOf(x);
        var b = _keyOf(y);

        int c;
        if (a is null || b is null)
        {
            // NULL position is absolute — independent of ASC/DESC.
            if (a is null && b is null)
            {
                c = 0;
            }
            else if (a is null)
            {
                c = _nullsFirst ? -1 : 1;
            }
            else
            {
                c = _nullsFirst ? 1 : -1;
            }
        }
        else
        {
            c = a.Value.CompareTo(b.Value);
            if (_descending)
            {
                c = -c;
            }
        }

        return c != 0 ? c : _tieBreak.Compare(x, y);
    }
}
