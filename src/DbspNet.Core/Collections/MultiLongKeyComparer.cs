// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Collections;

/// <summary>
/// The multi-key generalisation of <see cref="LongKeyComparer{TRow}"/> — the unboxed
/// twin of an N-key <see cref="SortKeyComparer{TRow}"/>. Where
/// <see cref="SortKeyComparer{TRow}"/> pulls every sort key as a boxed
/// <see cref="object"/> and compares through the non-generic
/// <see cref="System.IComparable"/> (one heap box per key per comparison for a
/// value-type key on a typed struct row), this reads each key as an <b>unboxed</b>
/// <see cref="long"/> and compares longs directly.
/// </summary>
/// <remarks>
/// <para>Semantics mirror <see cref="SortKeyComparer{TRow}"/> exactly, key by key:
/// NULL position is absolute (decided by that key's <c>nullsFirst</c>, never flipped
/// by <c>descending</c>); non-null values compare then negate for <c>DESC</c>; the
/// first non-zero key decides; an all-equal row pair falls through to
/// <paramref name="tieBreak"/>. With a row-level total order as the tiebreak this
/// keeps <c>Compare(x, y) == 0 ⟺ x equals y</c>, which the TOP-K operator's
/// <c>SortedDictionary</c> keying relies on.</para>
/// <para>Callers supply <paramref name="keysOf"/> whose <c>long</c> ordering is
/// monotone in each original key's ordering (see the SQL compiler's monotone-key
/// extraction: <c>BIGINT</c>/<c>INT</c> directly, <c>TIMESTAMP</c> microseconds,
/// <c>DATE</c> day number), so the induced order is identical to the boxed
/// comparer's.</para>
/// </remarks>
public sealed class MultiLongKeyComparer<TRow> : IComparer<TRow>
    where TRow : notnull
{
    private readonly System.Func<TRow, long?>[] _keysOf;
    private readonly bool[] _descending;
    private readonly bool[] _nullsFirst;
    private readonly IComparer<TRow> _tieBreak;

    public MultiLongKeyComparer(
        System.Func<TRow, long?>[] keysOf, bool[] descending, bool[] nullsFirst, IComparer<TRow> tieBreak)
    {
        System.ArgumentNullException.ThrowIfNull(keysOf);
        System.ArgumentNullException.ThrowIfNull(descending);
        System.ArgumentNullException.ThrowIfNull(nullsFirst);
        System.ArgumentNullException.ThrowIfNull(tieBreak);
        if (descending.Length != keysOf.Length || nullsFirst.Length != keysOf.Length)
        {
            throw new System.ArgumentException("keysOf, descending, and nullsFirst must have equal length");
        }

        _keysOf = keysOf;
        _descending = descending;
        _nullsFirst = nullsFirst;
        _tieBreak = tieBreak;
    }

    public int Compare(TRow? x, TRow? y)
    {
        // Reference identity and null are meaningful only for a reference row. On a
        // typed STRUCT row `ReferenceEquals(x, y)` and `x is null` box both operands
        // on every comparison — precisely the per-comparison boxing this comparer
        // exists to remove. `typeof(TRow).IsValueType` is a JIT-time constant in the
        // struct specialisation, so the whole branch (and its boxing) is folded away
        // there, while reference rows keep the fast identity/null path. Skipping it
        // for value types is semantics-preserving: two boxes are never
        // reference-equal, and a value-type row is never null.
        if (!typeof(TRow).IsValueType)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            // IComparer must define a total order over the nullable parameter type;
            // SQL rows are never null references.
            if (x is null)
            {
                return -1;
            }

            if (y is null)
            {
                return 1;
            }
        }

        for (var i = 0; i < _keysOf.Length; i++)
        {
            var a = _keysOf[i](x!);
            var b = _keysOf[i](y!);

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
                    c = _nullsFirst[i] ? -1 : 1;
                }
                else
                {
                    c = _nullsFirst[i] ? 1 : -1;
                }
            }
            else
            {
                c = a.Value.CompareTo(b.Value);
                if (_descending[i])
                {
                    c = -c;
                }
            }

            if (c != 0)
            {
                return c;
            }
        }

        return _tieBreak.Compare(x!, y!);
    }
}
