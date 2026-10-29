// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.InteropServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Compact-state partitioned TOP-K (docs/design-state-representation.md §3 row A, §4).
/// Semantically identical to <see cref="PartitionedTopKOp{TRow,TKey}"/> — the same
/// per-partition <c>ROW_NUMBER</c> / <c>RANK</c> / <c>DENSE_RANK</c> &lt;= k window
/// maintained under retraction, over the full integrated per-partition multiset — but
/// with a much smaller per-partition fixed cost:
/// <list type="bullet">
/// <item>one flat <see cref="Dictionary{TKey,TValue}"/> whose value is a
/// <see langword="struct"/>: a partition holding a single row stores it <b>inline</b>
/// (no per-partition heap object at all); 2..<see cref="SmallMax"/> rows live in one
/// sorted <c>Entry[]</c>; only larger partitions fall back to a
/// <see cref="SortedDictionary{TKey,TValue}"/> (keeping inserts O(log n));</item>
/// <item><b>no materialised last-emitted window.</b> The window is a pure function of
/// the accumulated state, so the old window of a partition is recomputed from its
/// pre-delta state the first time a tick touches it, into a reusable scratch buffer,
/// and diffed against the post-delta window by a sorted merge;</item>
/// <item>no per-tick allocation beyond the output delta (touched set and window
/// buffers are reused across ticks).</item>
/// </list>
/// The row-level total order <paramref name="order"/> must satisfy
/// <c>Compare(x, y) == 0 ⟺ x equals y</c> (as for the legacy operator's
/// <c>SortedDictionary</c>); both windows are sorted by it, which makes the merge diff
/// exact. Snapshot format is byte-compatible with the legacy operator.
/// </summary>
internal sealed class PartitionedTopKCompactOp<TRow, TKey> : IOperator, ISnapshotable, IIntrospectable, IFingerprintAlias
    where TRow : notnull
    where TKey : notnull
{
    /// <summary>The largest partition kept as a sorted array; beyond it a partition
    /// moves to a <see cref="SortedDictionary{TKey,TValue}"/> so a hot partition's
    /// insert stays O(log n) instead of an O(n) array shift.</summary>
    internal const int SmallMax = 32;

    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly Func<TRow, TKey> _partitionOf;
    private readonly IComparer<TRow> _order;
    private readonly IComparer<TRow> _sortKeyOnly;
    private readonly RankFunction _function;
    private readonly long _limit;
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    // The whole state: partition key -> its integrated multiset (inline / array / tree).
    private readonly Dictionary<TKey, Partition> _parts;

    // Per-tick scratch, reused: touched partition -> its pre-delta window's range in
    // _oldWindows; and the post-delta window of the partition being diffed.
    private readonly Dictionary<TKey, (int Start, int Length)> _touched;
    private readonly List<Entry> _oldWindows = new();
    private readonly List<Entry> _newWindow = new();

    private int _lastOutputSize;

    public PartitionedTopKCompactOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        Func<TRow, TKey> partitionOf,
        IComparer<TRow> order,
        IComparer<TRow> sortKeyOnly,
        RankFunction function,
        long limit,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(sortKeyOnly);

        _input = input;
        _output = output;
        _partitionOf = partitionOf;
        _order = order;
        _sortKeyOnly = sortKeyOnly;
        _function = function;
        _limit = limit;
        _snapshotCodec = snapshotCodec;
        _parts = new Dictionary<TKey, Partition>(partitionComparer);
        _touched = new Dictionary<TKey, (int, int)>(partitionComparer);
    }

    public string MetricName => "PartitionedTopK";

    public long RetainedRows
    {
        get
        {
            long n = 0;
            foreach (var p in _parts.Values)
            {
                n += p.Count;
            }

            return n;
        }
    }

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    /// <summary>Number of partitions (test / introspection hook).</summary>
    internal int PartitionCount => _parts.Count;

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _lastOutputSize = 0;
            _output.SetCurrent(new ZSetBuilder<TRow, Z64>(0).Build());
            return;
        }

        _touched.Clear();
        _oldWindows.Clear();
        foreach (var (row, dw) in delta)
        {
            var key = _partitionOf(row);
            ref var p = ref CollectionsMarshal.GetValueRefOrAddDefault(_parts, key, out _);
            ref var range = ref CollectionsMarshal.GetValueRefOrAddDefault(_touched, key, out var seen);
            if (!seen)
            {
                // First touch this tick: record the window the previous tick emitted
                // (a pure function of the still-unmodified state).
                var start = _oldWindows.Count;
                ComputeWindow(ref p, _oldWindows);
                range = (start, _oldWindows.Count - start);
            }

            if (Apply(ref p, row, dw.Value))
            {
                _parts.Remove(key);
            }
        }

        var builder = new ZSetBuilder<TRow, Z64>(_lastOutputSize);
        var oldAll = CollectionsMarshal.AsSpan(_oldWindows);
        foreach (var (key, range) in _touched)
        {
            _newWindow.Clear();
            ref var p = ref CollectionsMarshal.GetValueRefOrNullRef(_parts, key);
            if (!System.Runtime.CompilerServices.Unsafe.IsNullRef(ref p))
            {
                ComputeWindow(ref p, _newWindow);
            }

            EmitDiff(builder, oldAll.Slice(range.Start, range.Length), CollectionsMarshal.AsSpan(_newWindow));
        }

        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
    }

    /// <summary>Sorted-merge diff of two windows (both ascending by <see cref="_order"/>).</summary>
    private void EmitDiff(ZSetBuilder<TRow, Z64> builder, ReadOnlySpan<Entry> oldWin, ReadOnlySpan<Entry> newWin)
    {
        int i = 0, j = 0;
        while (i < oldWin.Length && j < newWin.Length)
        {
            var c = _order.Compare(oldWin[i].Row, newWin[j].Row);
            if (c < 0)
            {
                builder.Add(oldWin[i].Row, new Z64(-oldWin[i].Weight));
                i++;
            }
            else if (c > 0)
            {
                builder.Add(newWin[j].Row, new Z64(newWin[j].Weight));
                j++;
            }
            else
            {
                if (oldWin[i].Weight != newWin[j].Weight)
                {
                    builder.Add(newWin[j].Row, new Z64(newWin[j].Weight - oldWin[i].Weight));
                }

                i++;
                j++;
            }
        }

        for (; i < oldWin.Length; i++)
        {
            builder.Add(oldWin[i].Row, new Z64(-oldWin[i].Weight));
        }

        for (; j < newWin.Length; j++)
        {
            builder.Add(newWin[j].Row, new Z64(newWin[j].Weight));
        }
    }

    /// <summary>
    /// Adds <paramref name="dw"/> to <paramref name="row"/>'s accumulated weight in
    /// partition <paramref name="p"/> (a default-initialised <paramref name="p"/> is a
    /// new, empty partition). Returns <see langword="true"/> when the partition became
    /// empty and must be removed by the caller.
    /// </summary>
    private bool Apply(ref Partition p, TRow row, long dw)
    {
        if (dw == 0)
        {
            return p.Count == 0;
        }

        switch (p.Store)
        {
            case null when p.Count == 0:
                p.Row0 = row;
                p.Weight0 = dw;
                p.Count = 1;
                return false;

            case null:
            {
                var c = _order.Compare(row, p.Row0);
                if (c == 0)
                {
                    var next = p.Weight0 + dw;
                    if (next == 0)
                    {
                        p = default;
                        return true;
                    }

                    p.Weight0 = next;
                    return false;
                }

                // Inline -> sorted array of two.
                var arr = new Entry[2];
                if (c < 0)
                {
                    arr[0] = new Entry(row, dw);
                    arr[1] = new Entry(p.Row0, p.Weight0);
                }
                else
                {
                    arr[0] = new Entry(p.Row0, p.Weight0);
                    arr[1] = new Entry(row, dw);
                }

                p.Row0 = default!;
                p.Weight0 = 0;
                p.Store = arr;
                p.Count = 2;
                return false;
            }

            case Entry[] arr:
            {
                var idx = Search(arr, p.Count, row);
                if (idx >= 0)
                {
                    var next = arr[idx].Weight + dw;
                    if (next != 0)
                    {
                        arr[idx].Weight = next;
                        return false;
                    }

                    var count = p.Count - 1;
                    Array.Copy(arr, idx + 1, arr, idx, count - idx);
                    arr[count] = default;
                    if (count == 1)
                    {
                        // Back to inline: drop the array.
                        p.Row0 = arr[0].Row;
                        p.Weight0 = arr[0].Weight;
                        p.Store = null;
                    }

                    p.Count = count;
                    return false;
                }

                var at = ~idx;
                if (p.Count == SmallMax)
                {
                    var tree = new SortedDictionary<TRow, long>(_order);
                    for (var k = 0; k < p.Count; k++)
                    {
                        tree.Add(arr[k].Row, arr[k].Weight);
                    }

                    tree.Add(row, dw);
                    p.Store = tree;
                    p.Count = tree.Count;
                    return false;
                }

                if (p.Count == arr.Length)
                {
                    var grown = new Entry[Math.Min(arr.Length * 2, SmallMax)];
                    Array.Copy(arr, grown, p.Count);
                    arr = grown;
                    p.Store = grown;
                }

                Array.Copy(arr, at, arr, at + 1, p.Count - at);
                arr[at] = new Entry(row, dw);
                p.Count++;
                return false;
            }

            default:
            {
                var tree = (SortedDictionary<TRow, long>)p.Store;
                tree.TryGetValue(row, out var current);
                var next = current + dw;
                if (next == 0)
                {
                    tree.Remove(row);
                }
                else
                {
                    tree[row] = next;
                }

                p.Count = tree.Count;
                if (p.Count == 0)
                {
                    p = default;
                    return true;
                }

                return false;
            }
        }
    }

    private int Search(Entry[] arr, int count, TRow row)
    {
        int lo = 0, hi = count - 1;
        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            var c = _order.Compare(arr[mid].Row, row);
            if (c == 0)
            {
                return mid;
            }

            if (c < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }

    /// <summary>
    /// Appends partition <paramref name="p"/>'s window — the rows kept by the rank cut,
    /// with their in-window weight, ascending by <see cref="_order"/> — to
    /// <paramref name="dst"/>. Identical rank arithmetic to the legacy operator.
    /// </summary>
    private void ComputeWindow(ref Partition p, List<Entry> dst)
    {
        if (p.Count == 0 || _limit <= 0)
        {
            return;
        }

        var acc = new WindowCut(_function, _limit, _sortKeyOnly, dst);
        switch (p.Store)
        {
            case null:
                acc.Offer(p.Row0, p.Weight0);
                break;

            case Entry[] arr:
                for (var i = 0; i < p.Count; i++)
                {
                    if (!acc.Offer(arr[i].Row, arr[i].Weight))
                    {
                        break;
                    }
                }

                break;

            default:
                foreach (var (row, weight) in (SortedDictionary<TRow, long>)p.Store)
                {
                    if (!acc.Offer(row, weight))
                    {
                        break;
                    }
                }

                break;
        }
    }

    private IEnumerable<(TRow Row, long Weight)> AllEntries()
    {
        foreach (var p in _parts.Values)
        {
            switch (p.Store)
            {
                case null:
                    yield return (p.Row0, p.Weight0);
                    break;
                case Entry[] arr:
                    for (var i = 0; i < p.Count; i++)
                    {
                        yield return (arr[i].Row, arr[i].Weight);
                    }

                    break;
                default:
                    foreach (var (row, weight) in (SortedDictionary<TRow, long>)p.Store)
                    {
                        yield return (row, weight);
                    }

                    break;
            }
        }
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedTopKCompactOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedTopK to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // Byte-compatible with PartitionedTopKOp: one flattened Z-set; the partition
        // key is recovered from each row on load.
        var snapshot = ZSet.FromEntries(AllEntries().Select(e => (e.Row, new Z64(e.Weight))));
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedTopKCompactOp was constructed without a snapshot codec.");
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _parts.Clear();
        foreach (var (row, weight) in loaded)
        {
            if (weight.Value == 0)
            {
                continue;
            }

            var key = _partitionOf(row);
            ref var p = ref CollectionsMarshal.GetValueRefOrAddDefault(_parts, key, out _);
            if (Apply(ref p, row, weight.Value))
            {
                _parts.Remove(key);
            }
        }

        // No window to record: it is recomputed from the state on the next touch.
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    /// <summary>
    /// The legacy operator this one replaced for this plan (whole-row or §22 narrow,
    /// keyed by a boxed <see cref="StructuralRow"/>); both save the same integrated
    /// input Z-set, so their snapshots restore here. Set by the builder.
    /// </summary>
    public Type FingerprintAliasType { get; init; } = typeof(PartitionedTopKOp<TRow, StructuralRow>);

    private struct Entry
    {
        public TRow Row;
        public long Weight;

        public Entry(TRow row, long weight)
        {
            Row = row;
            Weight = weight;
        }
    }

    /// <summary>
    /// One partition's integrated multiset. <see cref="Store"/> is <see langword="null"/>
    /// for 0..1 rows (the row inline in <see cref="Row0"/>), an <c>Entry[]</c> sorted by
    /// the order for 2..<see cref="SmallMax"/> rows (first <see cref="Count"/> slots
    /// live), or a <see cref="SortedDictionary{TKey,TValue}"/> beyond that.
    /// </summary>
    private struct Partition
    {
        public TRow Row0;
        public long Weight0;
        public object? Store;
        public int Count;
    }

    /// <summary>The rank-cut state machine, fed rows in ascending order.</summary>
    private struct WindowCut
    {
        private readonly RankFunction _function;
        private readonly long _limit;
        private readonly IComparer<TRow> _sortKeyOnly;
        private readonly List<Entry> _dst;
        private long _rowsBefore;
        private long _denseRank;
        private TRow? _groupKey;
        private bool _inGroup;

        public WindowCut(RankFunction function, long limit, IComparer<TRow> sortKeyOnly, List<Entry> dst)
        {
            _function = function;
            _limit = limit;
            _sortKeyOnly = sortKeyOnly;
            _dst = dst;
        }

        /// <summary>Offers the next row; returns <see langword="false"/> once no later
        /// row can be in the window.</summary>
        public bool Offer(TRow row, long weight)
        {
            if (weight <= 0)
            {
                return true;
            }

            if (_function == RankFunction.RowNumber)
            {
                var take = Math.Min(weight, _limit - _rowsBefore);
                if (take > 0)
                {
                    _dst.Add(new Entry(row, take));
                }

                _rowsBefore += weight;
                return _rowsBefore < _limit;
            }

            // RANK / DENSE_RANK: keep or drop whole tie groups.
            if (!_inGroup || _sortKeyOnly.Compare(_groupKey!, row) != 0)
            {
                _denseRank++;
                var kept = _function == RankFunction.DenseRank ? _denseRank <= _limit : _rowsBefore < _limit;
                if (!kept)
                {
                    return false;
                }

                _groupKey = row;
                _inGroup = true;
            }

            _dst.Add(new Entry(row, weight));
            _rowsBefore += weight;
            return true;
        }
    }
}
