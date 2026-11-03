// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// A window operator's per-partition input, read in order and only as far as a change reaches
/// (docs/design-flat-trace-state.md §3.15, §3.18).
/// </summary>
/// <remarks>
/// <para>A partition's representation follows its size. A <b>small</b> partition is managed: a
/// sorted dictionary of its rows, so touching it decodes nothing (on blocks every touch decoded
/// the partition, and small partitions are touched often relative to their size, §3.17). A
/// partition that grows past <c>promoteAt</c> rows becomes <b>large</b>: a list of reference-free
/// chunks, each one block of rows sorted by (order value, total order), with its first and last
/// order value, its rows' order values, and, when the window's aggregate combines exactly, its
/// partial aggregate. A peer group (equal order value) never spans chunks, so a value routes to one
/// chunk. A change rewrites only the chunks it lands in; appends touch only the last. A large
/// partition stays large as it shrinks, since its chunks shrink with it, and leaves the store when
/// empty.</para>
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
    private readonly IComparer<StructuralRow> _rowOrder;
    private readonly Dictionary<StructuralRow, SortedDictionary<StructuralRow, long>> _small = new();
    private readonly Dictionary<StructuralRow, List<Chunk>> _large = new();
    private long _smallRows;

    // With a cold format (§3.22): small partitions' record is this shared trace (partition key →
    // row), always current, and _small is only a read cache of the partitions touched since the
    // last idle tick, dropped then (EndBatch). Without one, _small is the record.
    private readonly BlockedIndexedTrace? _cold;

    // Keys the cold trace has ever held (a Bloom filter: no false negatives), so a partition new
    // this tick is read from the tick's own rows instead of probed for (§3.17).
    private KeyFilter _keys = new();

    // This tick's new partitions (the filter had never seen them): their whole content.
    private Dictionary<StructuralRow, List<(StructuralRow Row, long Weight, long Value)>>? _fresh;

    private static readonly StructuralRow NoKey = new([]);

    /// <param name="chunkFormat">Blocks of a large partition's chunks: no key, the row.</param>
    /// <param name="valueOf">The ORDER BY value; null for a whole-partition frame, whose partitions
    /// stay small (with every row one peer group there is nothing to cut at).</param>
    /// <param name="order">The total order (ORDER BY, then the whole row).</param>
    /// <param name="partial">The window's aggregate when it combines exactly: chunks then carry partials.</param>
    public OrderedPartitionStore(
        ITraceBlockFormat chunkFormat,
        Func<StructuralRow, long>? valueOf,
        IComparer<StructuralRow> order,
        IPartialAggregator<StructuralRow, StructuralRow>? partial,
        int promoteAt = 512,
        int chunkRows = 512,
        ITraceBlockFormat? coldFormat = null)
    {
        _chunkFormat = chunkFormat;
        _valueOf = valueOf;
        _order = order;
        _partial = partial is { IsExactPartial: true } ? partial : null;
        _promoteAt = valueOf is null ? int.MaxValue : promoteAt;
        _chunkRows = Math.Max(1, chunkRows);
        _cold = coldFormat is null ? null : new BlockedIndexedTrace(coldFormat, TraceBlocksMode.FreezeEntries, blockEntries: TraceBlocksMode.BlockEntries);
        _rowOrder = Comparer<StructuralRow>.Create((a, b) =>
        {
            var c = ValueOf(a).CompareTo(ValueOf(b));
            return c != 0 ? c : _order.Compare(a, b);
        });
    }

    /// <summary>Whether chunks carry partial aggregates (the aggregate combines exactly).</summary>
    public bool HasPartials => _partial is not null;

    /// <summary>Large partitions (tests).</summary>
    internal int LargePartitions => _large.Count;

    /// <summary>Rows held.</summary>
    public long Entries
    {
        get
        {
            var n = _cold?.Entries ?? _smallRows;
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

    /// <summary>The order value the store routes a row by.</summary>
    public Func<StructuralRow, long> ValueOfRow => ValueOf;

    // Compare as a delegate built once (a method group would allocate one per sort).
    private Comparison<(StructuralRow Row, long Weight, long Value)> _compare => _compareCache ??= Compare;

    private Comparison<(StructuralRow Row, long Weight, long Value)>? _compareCache;

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

        var byKey = new Dictionary<StructuralRow, List<(StructuralRow Row, long Weight, long Value)>>();
        foreach (var (key, row, weight) in rows)
        {
            if (!byKey.TryGetValue(key, out var list))
            {
                byKey[key] = list = new();
            }

            list.Add((row, weight, ValueOf(row)));
        }

        _fresh = null;
        IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>? cold = null;
        List<StructuralRow>? seen = null;
        foreach (var (key, change) in byKey)
        {
            if (_large.ContainsKey(key))
            {
                ApplyLarge(key, change);
            }
            else if (change.Count > _promoteAt)
            {
                // A bulk change alone past the threshold (a load, a restored partition): straight to
                // chunks, never built as a sorted dictionary first.
                PromoteWith(key, change);
            }
            else if (_cold is null)
            {
                ApplySmall(key, change);
            }
            else
            {
                // The record takes the change; a cached copy follows it; a partition the filter has
                // never seen is all in this change.
                cold ??= new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                foreach (var (row, weight, _) in change)
                {
                    cold.Add(key, row, new Z64(weight));
                }

                if (_small.ContainsKey(key))
                {
                    ApplySmall(key, change);
                }
                else if (!_keys.MayContain(key))
                {
                    (_fresh ??= new())[key] = change;
                }

                (seen ??= new()).Add(key);
            }
        }

        if (cold is not null)
        {
            _cold!.Integrate(cold.Build());
            foreach (var key in seen!)
            {
                _keys.Add(key);
            }
        }
    }

    private void ApplySmall(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> change)
    {
        if (!_small.TryGetValue(key, out var rows))
        {
            _small[key] = rows = new SortedDictionary<StructuralRow, long>(_rowOrder);
        }

        foreach (var (row, weight, _) in change)
        {
            rows.TryGetValue(row, out var current);
            var next = current + weight;
            if (next == 0)
            {
                if (rows.Remove(row))
                {
                    _smallRows--;
                }
            }
            else
            {
                if (current == 0)
                {
                    _smallRows++;
                }

                rows[row] = next;
            }
        }

        if (rows.Count == 0)
        {
            _small.Remove(key);
        }
    }

    /// <summary>An idle tick: freeze the cold trace. The read cache stays, since an operator's idle
    /// tick is not the end of a batch (an empty delta between two that touch the same partitions),
    /// unless it has grown past <see cref="CacheCap"/> partitions. A checkpoint drops it
    /// (<see cref="DropCache"/>).</summary>
    public void Freeze()
    {
        if (_cold is not null)
        {
            if (_small.Count > CacheCap)
            {
                _small.Clear();
            }

            _cold.Freeze();
        }
    }

    /// <summary>Partitions the read cache may hold past an idle tick (a run without checkpoints).</summary>
    internal const int CacheCap = 1 << 18;

    /// <summary>Drop the read cache (a checkpoint: the heap it leaves holds only the record).</summary>
    public void DropCache()
    {
        if (_cold is not null)
        {
            _small.Clear();
        }
    }

    /// <summary>The tick's reads are done: drop its new partitions' rows.</summary>
    public void EndTick() => _fresh = null;

    /// <summary>Forget everything (a restore replaces the state).</summary>
    public void Clear()
    {
        _small.Clear();
        _large.Clear();
        _smallRows = 0;
        _cold?.Clear();
        _keys = new KeyFilter();
        _fresh = null;
    }

    /// <summary>Every row and weight (a whole-state write).</summary>
    public IEnumerable<(StructuralRow Row, long Weight)> All()
    {
        if (_cold is not null)
        {
            foreach (var (_, group) in _cold.MaterializeUncached())
            {
                foreach (var (row, weight) in group)
                {
                    yield return (row, weight.Value);
                }
            }
        }
        else
        {
            foreach (var rows in _small.Values)
            {
                foreach (var (row, weight) in rows)
                {
                    yield return (row, weight);
                }
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
        var t0 = BlockTraceProfile.Now();
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

            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Section(14, t0, rows.Count);
            }

            return rows;
        }

        // A small partition: already ordered and consolidated — from the cache, or (on blocks) read
        // once into it: the tick's own rows when new, otherwise probed and consolidated by sort.
        var all = new List<(StructuralRow Row, long Weight, long Value)>();
        if (_small.TryGetValue(key, out var small))
        {
            all.Capacity = small.Count;
            foreach (var (row, weight) in small)
            {
                all.Add((row, weight, ValueOf(row)));
            }
        }
        else if (_cold is not null)
        {
            List<(StructuralRow Row, long Weight, long Value)> raw;
            if (_fresh is not null && _fresh.TryGetValue(key, out var own))
            {
                raw = own;
            }
            else
            {
                raw = new List<(StructuralRow Row, long Weight, long Value)>();
                var visitor = new Collect(raw, this);
                _cold.Probe(key, ref visitor);
            }

            raw.Sort(_compare);
            foreach (var e in raw)
            {
                if (all.Count > 0 && Compare(all[^1], e) == 0)
                {
                    all[^1] = (all[^1].Row, all[^1].Weight + e.Weight, e.Value);
                }
                else
                {
                    all.Add(e);
                }
            }

            all.RemoveAll(e => e.Weight == 0);
            if (all.Count > 0 && all.Count <= _promoteAt)
            {
                var cached = new SortedDictionary<StructuralRow, long>(_rowOrder);
                foreach (var (row, weight, _) in all)
                {
                    cached[row] = weight;
                }

                _small[key] = cached;
            }
        }

        if (BlockTraceProfile.Enabled)
        {
            BlockTraceProfile.Section(12, t0, all.Count);
        }

        if (all.Count > _promoteAt)
        {
            var tPromote = BlockTraceProfile.Now();
            if (_cold is not null)
            {
                RetractCold(key, all);
            }
            else
            {
                _smallRows -= all.Count;
            }

            _small.Remove(key);
            _large[key] = Cut(all);
            if (BlockTraceProfile.Enabled)
            {
                BlockTraceProfile.Section(13, tPromote, all.Count);
            }
        }

        return all;
    }

    /// <summary>
    /// For positional (offset) windows: a partition's rows in order, consolidated. Of a large
    /// partition, the chunks overlapping [<paramref name="from"/>, <paramref name="to"/>] widened by
    /// whole chunks until at least <paramref name="marginRows"/> rows lie on each side, or the
    /// partition ends; of a small one, every row. <paramref name="atStart"/> and
    /// <paramref name="atEnd"/> say whether the rows read begin and end the partition.
    /// </summary>
    public List<(StructuralRow Row, long Weight, long Value)> ReadAround(
        StructuralRow key, long from, long to, long marginRows, out bool atStart, out bool atEnd)
    {
        if (!_large.TryGetValue(key, out var chunks))
        {
            atStart = atEnd = true;
            return Read(key, long.MinValue, long.MaxValue, needPrefix: false, out _);
        }

        var first = FirstChunkReaching(chunks, from);
        var last = first;
        while (last + 1 < chunks.Count && chunks[last + 1].First <= to)
        {
            last++;
        }

        for (long margin = 0; first > 0 && margin < marginRows;)
        {
            margin += chunks[--first].Count;
        }

        for (long margin = 0; last + 1 < chunks.Count && margin < marginRows;)
        {
            margin += chunks[++last].Count;
        }

        atStart = first == 0;
        atEnd = last == chunks.Count - 1;
        var rows = new List<(StructuralRow Row, long Weight, long Value)>();
        for (var c = first; c <= last; c++)
        {
            var chunk = chunks[c];
            for (var i = 0; i < chunk.Count; i++)
            {
                rows.Add((chunk.Block.Value(i), chunk.Block.Weight(i), chunk.Values[i]));
            }
        }

        return rows;
    }

    // Take a partition's rows (as read, any form) out of the cold trace: it is moving to chunks.
    private void RetractCold(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> rows)
    {
        var retract = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (row, weight, _) in rows)
        {
            retract.Add(key, row, new Z64(-weight));
        }

        _cold!.Integrate(retract.Build());
    }

    private readonly struct Collect(List<(StructuralRow Row, long Weight, long Value)> into, OrderedPartitionStore store) : ITraceVisitor
    {
        public void Row(StructuralRow value, Z64 weight) => into.Add((value, weight.Value, store.ValueOf(value)));

        public void Entry(ITraceBlock block, int index, Z64 weight)
        {
            var row = block.Value(index);
            into.Add((row, weight.Value, store.ValueOf(row)));
        }
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

    // A small partition (if any) and a change to it, together past the threshold: the partition
    // leaves the managed map, and the sum becomes chunks.
    private void PromoteWith(StructuralRow key, List<(StructuralRow Row, long Weight, long Value)> change)
    {
        if (_cold is not null)
        {
            // The partition's rows so far leave the record (the cache, if it holds them, is the same).
            _small.Remove(key);
            var before = new List<(StructuralRow Row, long Weight, long Value)>();
            if (_keys.MayContain(key))
            {
                var visitor = new Collect(before, this);
                _cold.Probe(key, ref visitor);
            }

            if (before.Count > 0)
            {
                RetractCold(key, before);
                change.AddRange(before);
            }

            _keys.Add(key);
        }
        else if (_small.Remove(key, out var existing))
        {
            _smallRows -= existing.Count;
            foreach (var (row, weight) in existing)
            {
                change.Add((row, weight, ValueOf(row)));
            }
        }

        change.Sort(_compare);
        var merged = new List<(StructuralRow Row, long Weight, long Value)>(change.Count);
        foreach (var e in change)
        {
            if (merged.Count > 0 && Compare(merged[^1], e) == 0)
            {
                merged[^1] = (merged[^1].Row, merged[^1].Weight + e.Weight, e.Value);
            }
            else
            {
                merged.Add(e);
            }
        }

        merged.RemoveAll(e => e.Weight == 0);
        if (merged.Count > 0)
        {
            _large[key] = Cut(merged);
        }
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

    // A scalable Bloom filter over partition-key hashes: reference-free bit arrays, a new one twice
    // as large once the newest is full; a key is absent only if every filter says so.
    private sealed class KeyFilter
    {
        private const int BitsPerKey = 10;
        private const int Probes = 4;
        private readonly List<(ulong[] Bits, long Capacity)> _filters = [(new ulong[(1024 * BitsPerKey + 63) / 64], 1024)];
        private long _inNewest;

        public void Add(StructuralRow key)
        {
            var (bits, capacity) = _filters[^1];
            if (_inNewest >= capacity)
            {
                capacity *= 2;
                bits = new ulong[(capacity * BitsPerKey + 63) / 64];
                _filters.Add((bits, capacity));
                _inNewest = 0;
            }

            var m = (ulong)bits.Length * 64;
            var (h1, h2) = Hashes(key);
            for (var i = 0UL; i < Probes; i++)
            {
                var bit = (h1 + (i * h2)) % m;
                bits[bit >> 6] |= 1UL << (int)(bit & 63);
            }

            _inNewest++;
        }

        public bool MayContain(StructuralRow key)
        {
            var (h1, h2) = Hashes(key);
            foreach (var (bits, _) in _filters)
            {
                var m = (ulong)bits.Length * 64;
                var all = true;
                for (var i = 0UL; i < Probes && all; i++)
                {
                    var bit = (h1 + (i * h2)) % m;
                    all = (bits[bit >> 6] & (1UL << (int)(bit & 63))) != 0;
                }

                if (all)
                {
                    return true;
                }
            }

            return false;
        }

        private static (ulong, ulong) Hashes(StructuralRow key)
        {
            var x = (ulong)(uint)key.GetHashCode() * 0x9E3779B97F4A7C15UL;
            x ^= x >> 29;
            return (x, (x >> 32) | 1);
        }
    }
}
