// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// A window operator's per-partition input, reference-free, read in order and only as far as a
/// change reaches (docs/design-flat-trace-state.md §3.15).
/// </summary>
/// <remarks>
/// <para>Two homes. A <b>small</b> partition lives in a shared <see cref="BlockedIndexedTrace"/>
/// (partition key → row), where reading it whole is cheap: one block's header serves many small
/// partitions. A partition read with more than <c>promoteAt</c> rows becomes <b>large</b>: a list of
/// chunks, each one block of rows sorted by (order value, total order), with its first and last
/// order value, its rows' order values, and — when the window's aggregate combines exactly — its
/// partial aggregate. A peer group (equal order value) never spans chunks, so a value routes to one
/// chunk. A change rewrites only the chunks it lands in; appends touch only the last.</para>
/// <para>Reads see the state as of the last <see cref="Integrate"/>, consolidated, in order.</para>
/// </remarks>
internal sealed class OrderedPartitionStore
{
    private readonly ITraceBlockFormat _chunkFormat;
    private readonly Func<StructuralRow, long>? _valueOf;
    private readonly IComparer<StructuralRow> _order;
    private readonly IPartialAggregator<StructuralRow, StructuralRow>? _partial;
    private readonly int _promoteAt;
    private readonly int _chunkRows;
    private readonly BlockedIndexedTrace _small;
    private readonly Dictionary<StructuralRow, List<Chunk>> _large = new();

    private static readonly StructuralRow NoKey = new([]);

    /// <param name="smallFormat">Blocks of the shared trace: the partition key, then the row.</param>
    /// <param name="chunkFormat">Blocks of a large partition's chunks: no key, the row.</param>
    /// <param name="valueOf">The ORDER BY value; null for a whole-partition frame, whose partitions
    /// stay small (with every row one peer group there is nothing to cut at).</param>
    /// <param name="order">The total order (ORDER BY, then the whole row).</param>
    /// <param name="partial">The window's aggregate when it combines exactly: chunks then carry partials.</param>
    public OrderedPartitionStore(
        ITraceBlockFormat smallFormat,
        ITraceBlockFormat chunkFormat,
        Func<StructuralRow, long>? valueOf,
        IComparer<StructuralRow> order,
        IPartialAggregator<StructuralRow, StructuralRow>? partial,
        int freezeEntries,
        int blockEntries,
        int promoteAt = 512,
        int chunkRows = 512)
    {
        _chunkFormat = chunkFormat;
        _valueOf = valueOf;
        _order = order;
        _partial = partial is { IsExactPartial: true } ? partial : null;
        _promoteAt = valueOf is null ? int.MaxValue : promoteAt;
        _chunkRows = Math.Max(1, chunkRows);
        _small = new BlockedIndexedTrace(smallFormat, freezeEntries, blockEntries: blockEntries);
    }

    /// <summary>Whether chunks carry partial aggregates (the aggregate combines exactly).</summary>
    public bool HasPartials => _partial is not null;

    /// <summary>Large partitions (tests).</summary>
    internal int LargePartitions => _large.Count;

    /// <summary>Entries held, an upper bound on the rows (cancelling small entries both count).</summary>
    public long Entries
    {
        get
        {
            var n = _small.Entries;
            foreach (var chunks in _large.Values)
            {
                foreach (var c in chunks)
                {
                    n += c.Count;
                }
            }

            return n;
        }
    }

    private sealed class Chunk(ITraceBlock block, long[] values, object? partial)
    {
        public readonly ITraceBlock Block = block;
        public readonly long[] Values = values;
        public readonly object? Partial = partial;

        public int Count => Values.Length;

        public long First => Values[0];

        public long Last => Values[^1];
    }

    private long ValueOf(StructuralRow row) => _valueOf?.Invoke(row) ?? 0L;

    private int Compare((StructuralRow Row, long Weight, long Value) a, (StructuralRow Row, long Weight, long Value) b)
    {
        var c = a.Value.CompareTo(b.Value);
        return c != 0 ? c : _order.Compare(a.Row, b.Row);
    }

    /// <summary>Apply a tick's change (or restored rows): (partition key, row, weight).</summary>
    public void Integrate(List<(StructuralRow Key, StructuralRow Row, long Weight)> rows)
    {
        if (rows.Count == 0)
        {
            return;
        }

        List<(StructuralRow Key, StructuralRow Row, long Weight)>? small = null;
        Dictionary<StructuralRow, List<(StructuralRow Row, long Weight, long Value)>>? large = null;
        foreach (var e in rows)
        {
            if (_large.ContainsKey(e.Key))
            {
                large ??= new();
                if (!large.TryGetValue(e.Key, out var list))
                {
                    large[e.Key] = list = new();
                }

                list.Add((e.Row, e.Weight, ValueOf(e.Row)));
            }
            else
            {
                (small ??= new()).Add(e);
            }
        }

        if (small is not null)
        {
            if (small.Count >= TraceBlocksMode.FreezeEntries)
            {
                _small.IntegrateEntries(small.Count, i => small[i].Key, i => small[i].Row, i => small[i].Weight);
            }
            else
            {
                var indexed = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                foreach (var (key, row, weight) in small)
                {
                    indexed.Add(key, row, new Z64(weight));
                }

                _small.Integrate(indexed.Build());
            }
        }

        if (large is not null)
        {
            foreach (var (key, delta) in large)
            {
                ApplyLarge(key, delta);
            }
        }
    }

    /// <summary>Move the shared trace's managed levels into runs (an idle tick).</summary>
    public void Freeze() => _small.Freeze();

    /// <summary>Forget everything (a restore replaces the state).</summary>
    public void Clear()
    {
        _small.Clear();
        _large.Clear();
    }

    /// <summary>Every row and weight (a whole-state write).</summary>
    public IEnumerable<(StructuralRow Row, long Weight)> All()
    {
        foreach (var (_, group) in _small.MaterializeUncached())
        {
            foreach (var (row, weight) in group)
            {
                yield return (row, weight.Value);
            }
        }

        foreach (var chunks in _large.Values)
        {
            foreach (var c in chunks)
            {
                for (var i = 0; i < c.Count; i++)
                {
                    yield return (c.Block.Value(i), c.Block.Weight(i));
                }
            }
        }
    }

    /// <summary>
    /// A partition's rows in order (order value, then total order), consolidated: of a large
    /// partition only the chunks overlapping [<paramref name="from"/>, <paramref name="to"/>]. With
    /// <paramref name="needPrefix"/> (a running frame folds from the start), <paramref name="before"/>
    /// is the partial of the chunks below them (folded in ascending order), and without partials
    /// the read starts at the first chunk. Of a small partition, every row (<paramref name="before"/>
    /// null: the caller folds from the start). A small partition found larger than the threshold is
    /// promoted to chunks here.
    /// </summary>
    public List<(StructuralRow Row, long Weight, long Value)> Read(
        StructuralRow key, long from, long to, bool needPrefix, out object? before)
    {
        before = null;
        if (_large.TryGetValue(key, out var chunks))
        {
            var first = FirstChunkReaching(chunks, from);
            if (!needPrefix)
            {
                // The caller needs only the range (a bounded frame).
            }
            else if (_partial is not null && first > 0)
            {
                var acc = _partial.Clone(chunks[0].Partial!);
                for (var c = 1; c < first; c++)
                {
                    _partial.CombineInto(acc, chunks[c].Partial!);
                }

                before = acc;
            }
            else
            {
                first = 0; // no partials: the caller folds from the start
            }

            var rows = new List<(StructuralRow Row, long Weight, long Value)>();
            for (var c = first; c < chunks.Count && chunks[c].First <= to; c++)
            {
                var chunk = chunks[c];
                for (var i = 0; i < chunk.Count; i++)
                {
                    rows.Add((chunk.Block.Value(i), chunk.Block.Weight(i), chunk.Values[i]));
                }
            }

            return rows;
        }

        var all = new List<(StructuralRow Row, long Weight, long Value)>();
        foreach (var (row, weight) in _small.GroupView(key).Decoded())
        {
            all.Add((row, weight.Value, ValueOf(row)));
        }

        all.Sort(Compare);
        if (all.Count > _promoteAt)
        {
            Promote(key, all);
        }

        return all;
    }

    // The first chunk whose last value is not below `from` (the chunk a change at `from` lands in).
    private static int FirstChunkReaching(List<Chunk> chunks, long from)
    {
        int lo = 0, hi = chunks.Count;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (chunks[mid].Last < from)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return Math.Min(lo, chunks.Count - 1);
    }

    // A small partition's sorted rows become chunks; the shared trace forgets them.
    private void Promote(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> sorted)
    {
        var retract = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (row, weight, _) in sorted)
        {
            retract.Add(key, row, new Z64(-weight));
        }

        _small.Integrate(retract.Build());
        _large[key] = Cut(sorted);
    }

    // A large partition's change: each touched chunk is decoded, merged with its share of the
    // change (consolidating equal rows) and re-cut.
    private void ApplyLarge(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> delta)
    {
        var chunks = _large[key];
        delta.Sort(Compare);

        // Route by value: a value lands in the first chunk whose last value is not below it.
        var byChunk = new SortedDictionary<int, List<(StructuralRow Row, long Weight, long Value)>>();
        foreach (var e in delta)
        {
            var c = FirstChunkReaching(chunks, e.Value);
            if (!byChunk.TryGetValue(c, out var list))
            {
                byChunk[c] = list = new();
            }

            list.Add(e);
        }

        // Highest chunk first, so replacing one with several leaves the lower indexes valid.
        foreach (var (c, share) in byChunk.Reverse())
        {
            var chunk = chunks[c];
            var merged = new List<(StructuralRow Row, long Weight, long Value)>(chunk.Count + share.Count);
            int i = 0, j = 0;
            while (i < chunk.Count || j < share.Count)
            {
                var a = i < chunk.Count ? (chunk.Block.Value(i), chunk.Block.Weight(i), chunk.Values[i]) : default;
                var cmp = i >= chunk.Count ? 1 : j >= share.Count ? -1 : Compare(a, share[j]);
                if (cmp < 0)
                {
                    merged.Add(a);
                    i++;
                }
                else if (cmp > 0)
                {
                    merged.Add(share[j++]);
                }
                else
                {
                    var w = a.Item2 + share[j].Weight;
                    if (w != 0)
                    {
                        merged.Add((a.Item1, w, a.Item3));
                    }

                    i++;
                    j++;
                }
            }

            // A row the change left at zero weight (only in the share) drops out here.
            merged.RemoveAll(e => e.Weight == 0);
            chunks.RemoveAt(c);
            chunks.InsertRange(c, Cut(merged));
        }

        if (chunks.Count == 0)
        {
            _large.Remove(key);
        }
    }

    // Sorted rows cut into chunks of about _chunkRows, never inside a peer group.
    private List<Chunk> Cut(List<(StructuralRow Row, long Weight, long Value)> sorted)
    {
        var chunks = new List<Chunk>();
        var start = 0;
        while (start < sorted.Count)
        {
            var end = Math.Min(sorted.Count, start + _chunkRows);
            if (sorted.Count - end < _chunkRows / 2)
            {
                end = sorted.Count; // no runt at the end
            }

            while (end < sorted.Count && sorted[end].Value == sorted[end - 1].Value)
            {
                end++;
            }

            chunks.Add(Build(sorted, start, end));
            start = end;
        }

        return chunks;
    }

    private Chunk Build(List<(StructuralRow Row, long Weight, long Value)> sorted, int start, int end)
    {
        var n = end - start;
        var keys = new StructuralRow[n];
        var rows = new StructuralRow[n];
        var weights = new long[n];
        var values = new long[n];
        var hashes = new int[n];
        object? partial = _partial?.NewAccumulator();
        for (var i = 0; i < n; i++)
        {
            var (row, weight, value) = sorted[start + i];
            keys[i] = NoKey;
            rows[i] = row;
            weights[i] = weight;
            values[i] = value;
            if (partial is not null)
            {
                _partial!.Add(partial, row, weight);
            }
        }

        return new Chunk(_chunkFormat.Encode(keys, rows, weights, hashes, n), values, partial);
    }
}
