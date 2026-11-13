// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// The bulk-load fast path's sort (docs/design-bounded-memory.md §5.52.2): new partitions' rows ordered
/// once so that each partition is a contiguous run in its window order. Rows are ordered by partition-key
/// hash (one primitive sort), then, within a stretch of equal hashes, by partition key (grouped by the
/// store's own key equality, <see cref="StructuralRow.Equals(object)"/>, so two keys that collide stay apart)
/// and by <c>order</c>. Shared by TOP-K and rank.
/// </summary>
internal static class PartitionRuns
{
    /// <summary>Sorts <paramref name="rows"/> in place; returns each run's partition key, with the runs
    /// as (start, length) in the same order.</summary>
    internal static List<StructuralRow> Sort(
        List<(StructuralRow Key, StructuralRow Row, long Weight)> rows,
        IComparer<StructuralRow> order,
        out List<(int Start, int Length)> runs)
    {
        var n = rows.Count;
        var byHash = new ulong[n];
        for (var i = 0; i < n; i++)
        {
            byHash[i] = ((ulong)(uint)rows[i].Key.GetHashCode() << 32) | (uint)i;
        }

        Array.Sort(byHash);
        var sorted = new (StructuralRow Key, StructuralRow Row, long Weight)[n];
        for (var i = 0; i < n; i++)
        {
            sorted[i] = rows[(int)(uint)byHash[i]];
        }

        var keys = new List<StructuralRow>();
        runs = new List<(int, int)>();
        var comparer = new StretchComparer(order);
        for (var from = 0; from < n;)
        {
            var hash = byHash[from] >> 32;
            var to = from + 1;
            while (to < n && byHash[to] >> 32 == hash)
            {
                to++;
            }

            if (to - from > 1)
            {
                // Group ids by key equality (a stretch holds one key unless hashes collide), then order.
                var distinct = new List<StructuralRow>(1) { sorted[from].Key };
                var ids = new int[to - from];
                for (var i = from + 1; i < to; i++)
                {
                    var id = distinct.FindIndex(k => k.Equals(sorted[i].Key));
                    if (id < 0)
                    {
                        id = distinct.Count;
                        distinct.Add(sorted[i].Key);
                    }

                    ids[i - from] = id;
                }

                var items = new (int Id, StructuralRow Row, StructuralRow Key, long Weight)[to - from];
                for (var i = from; i < to; i++)
                {
                    items[i - from] = (ids[i - from], sorted[i].Row, sorted[i].Key, sorted[i].Weight);
                }

                Array.Sort(items, comparer);
                for (var i = from; i < to; i++)
                {
                    var (_, row, key, weight) = items[i - from];
                    sorted[i] = (key, row, weight);
                }

                var start = from;
                for (var i = from + 1; i <= to; i++)
                {
                    if (i == to || items[i - from].Id != items[i - from - 1].Id)
                    {
                        keys.Add(sorted[start].Key);
                        runs.Add((start, i - start));
                        start = i;
                    }
                }
            }
            else
            {
                keys.Add(sorted[from].Key);
                runs.Add((from, 1));
            }

            from = to;
        }

        rows.Clear();
        rows.AddRange(sorted);
        return keys;
    }

    private sealed class StretchComparer(IComparer<StructuralRow> order) : IComparer<(int Id, StructuralRow Row, StructuralRow Key, long Weight)>
    {
        public int Compare((int Id, StructuralRow Row, StructuralRow Key, long Weight) x, (int Id, StructuralRow Row, StructuralRow Key, long Weight) y)
        {
            var c = x.Id.CompareTo(y.Id);
            return c != 0 ? c : order.Compare(x.Row, y.Row);
        }
    }
}
