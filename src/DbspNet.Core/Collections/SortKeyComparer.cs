// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Collections;

/// <summary>
/// A total order over rows for incremental TOP-K (<c>ORDER BY … LIMIT</c>).
/// Compares by a list of <c>ORDER BY</c> sort keys (each an extractor that
/// pulls the key value out of a row, a direction, and a NULL ordering), then
/// breaks remaining ties with <see cref="_tieBreak"/> — a row-level total order
/// (<see cref="StructuralRowComparer"/> on the structural path,
/// <c>Comparer&lt;TRow&gt;.Default</c> for the emitted typed structs). The
/// tiebreak guarantees <c>Compare(x, y) == 0 ⟺ x equals y</c>, which the
/// operator's <c>SortedDictionary</c> keying relies on, and makes the window
/// boundary deterministic so incremental output doesn't flicker between equal
/// sort keys.
/// </summary>
/// <remarks>
/// NULL ordering is absolute: <see cref="_nullsFirst"/> decides a NULL's final
/// position directly and is <b>not</b> flipped by a descending key (only the
/// non-null value comparison is negated for <c>DESC</c>). Non-null key values
/// are compared through the runtime non-generic <see cref="IComparable"/> —
/// the same primitive <see cref="StructuralRowComparer"/> and the MIN/MAX
/// aggregators use; every SQL scalar value type implements it.
/// </remarks>
public sealed class SortKeyComparer<TRow> : IComparer<TRow>
    where TRow : notnull
{
    private readonly Func<TRow, object?>[] _keys;
    private readonly bool[] _descending;
    private readonly bool[] _nullsFirst;
    private readonly IComparer<TRow> _tieBreak;

    public SortKeyComparer(
        Func<TRow, object?>[] keys,
        bool[] descending,
        bool[] nullsFirst,
        IComparer<TRow> tieBreak)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(descending);
        ArgumentNullException.ThrowIfNull(nullsFirst);
        ArgumentNullException.ThrowIfNull(tieBreak);
        if (descending.Length != keys.Length || nullsFirst.Length != keys.Length)
        {
            throw new ArgumentException("keys, descending, and nullsFirst must have equal length");
        }

        _keys = keys;
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

        for (var i = 0; i < _keys.Length; i++)
        {
            var a = _keys[i](x);
            var b = _keys[i](y);

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
                c = ((IComparable)a).CompareTo(b);
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

        return _tieBreak.Compare(x, y);
    }
}
