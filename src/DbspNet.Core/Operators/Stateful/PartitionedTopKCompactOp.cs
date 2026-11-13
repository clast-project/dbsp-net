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
internal sealed class PartitionedTopKCompactOp<TRow, TKey> : IOperator, ISnapshotable, IIntrospectable, IFingerprintAlias, IStoreSnapshot
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
    private Dictionary<TKey, (int Start, int Length)> _touched;
    private List<Entry> _oldWindows = new();
    private List<Entry> _newWindow = new();

    private int _lastOutputSize;

    // With block formats (docs/design-bounded-memory.md §5.12): the whole state is a blocked
    // OrderedPartitionStore instead of _parts — partitions ordered by the total order and read whole
    // (valueOf null: they never move to chunks), the managed write-back cache bounded by S2's budget
    // and the record spilled by S1. A partition's window is read from it before and after the delta.
    private readonly Blocks.OrderedPartitionStore? _store;
    private readonly Func<Blocks.OrderedPartitionStore>? _newStore;

    // Scratch past this many touched partitions is dropped after the tick: Clear keeps a dictionary's
    // capacity, and one bulk tick otherwise left ~0.9 GB of empty entries behind (§5.11).
    private const int ScratchKeep = 1 << 16;

    public PartitionedTopKCompactOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        Func<TRow, TKey> partitionOf,
        IComparer<TRow> order,
        IComparer<TRow> sortKeyOnly,
        RankFunction function,
        long limit,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null,
        Blocks.ITraceBlockFormat? chunkFormat = null,
        Blocks.ITraceBlockFormat? blockFormat = null)
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
        _bulk = PartitionedBulkMode.Enabled;
        _parts = new Dictionary<TKey, Partition>(partitionComparer);
        _touched = new Dictionary<TKey, (int, int)>(partitionComparer);
        _partitionComparer = partitionComparer;
        if (chunkFormat is not null)
        {
            if (typeof(TRow) != typeof(StructuralRow) || typeof(TKey) != typeof(StructuralRow))
            {
                throw new ArgumentException("Blocked TOP-K state needs structural rows and partition keys.", nameof(chunkFormat));
            }

            _newStore = () => new Blocks.OrderedPartitionStore(
                chunkFormat,
                valueOf: null,
                (IComparer<StructuralRow>)(object)_order,
                partial: null,
                coldFormat: blockFormat,
                keyOf: row => S(_partitionOf(As<TRow>(row))));
            _store = _newStore();
        }
    }

    private readonly IEqualityComparer<TKey>? _partitionComparer;

    // The bulk-load fast path (docs/design-bounded-memory.md §5.52), blocked state only: this step's
    // newness test, taken before the step changes the store; null when off or between steps.
    private readonly bool _bulk;
    private Blocks.OrderedPartitionStore.NewPartitions? _fresh;

    private static StructuralRow S<T>(T value) => (StructuralRow)(object)value!;

    private static T As<T>(StructuralRow row) => (T)(object)row;

    /// <summary>Whether the state is held as blocks (tests).</summary>
    internal bool IsBlocked => _store is not null;

    /// <summary>Whether the bulk-load fast path is on for this operator (§5.52; tests).</summary>
    internal bool BulkEnabled => _bulk && _store is not null;

    /// <summary>Partitions the fast path has written since the operator was built (tests).</summary>
    internal long BulkPartitions => _store?.BulkPartitions ?? 0;

    public string MetricName => "PartitionedTopK";

    public long RetainedRows
    {
        get
        {
            if (_store is not null)
            {
                return _store.Entries;
            }

            long n = 0;
            foreach (var p in _parts.Values)
            {
                n += p.Count;
            }

            return n;
        }
    }

    public long LastOutputRows => _output.LastCount;

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

        if (_store is not null)
        {
            StepBlocked(delta);
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

        var builder = Blocks.DeltaSpill.For<TRow, Z64>(_output.DeltaFormat, _lastOutputSize);
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
        TrimScratch();
    }

    // The blocked step: each touched partition's window read from the store before the delta and
    // after it, then diffed. A delta past the step chunk is stepped a slice of partitions at a time
    // (§5.47, D5 as for rank, §5.43): on issue #2's extended TPC-DS a 36M-row tick-18 delta collected
    // whole and every touched partition loaded at once ran the TOP-K out of memory at the 48 GiB cap.
    private void StepBlocked(ZSet<TRow, Z64> delta)
    {
        _touched.Clear();
        _oldWindows.Clear();
        var builder = Blocks.DeltaSpill.For<TRow, Z64>(_output.DeltaFormat, _lastOutputSize);
        var slices = Blocks.PartitionSlicing.Count(blocked: true, delta.Count);
        Phase[6] += delta.Count;
        Phase[7] += slices;
        var probes0 = (_store!.ColdProbes, _store.ColdProbesEmpty, _store.Flushes, _store.FlushedEntries);
        var bulk0 = (_store.BulkPartitions, _store.BulkEntries);
        _fresh = _bulk ? _store.BeginStep() : null;
        if (slices <= 1)
        {
            StepBlockedPartitions(delta, delta.Count, builder);
        }
        else
        {
            SlicedSteps++;
            var b0 = GC.GetAllocatedBytesForCurrentThread();
            var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            var inner0 = InnerPhases();

            // One pass splits the delta, each slice a builder that spills its share; each slice is
            // decoded once, into a list (a spilled slice would decode again on every pass).
            var parts = new ZSetBuilder<TRow, Z64>?[slices];
            foreach (var (row, weight) in delta)
            {
                var key = _partitionOf(row);
                var s = Blocks.PartitionSlicing.Of(_partitionComparer?.GetHashCode(key) ?? key!.GetHashCode(), slices);
                (parts[s] ??= Blocks.DeltaSpill.For<TRow, Z64>(_input.DeltaFormat, 0, slices)).Add(row, weight);
            }

            var rows = new List<KeyValuePair<TRow, Z64>>();
            for (var s = 0; s < slices; s++)
            {
                if (parts[s] is { } part)
                {
                    parts[s] = null;
                    SliceSteps++;
                    rows.Clear();
                    rows.AddRange(part.Build());
                    StepBlockedPartitions(rows, rows.Count, builder);
                    _touched.Clear();
                    _oldWindows.Clear();
                }
            }

            var inner = InnerPhases();
            Phase[0] += GC.GetAllocatedBytesForCurrentThread() - b0 - (inner.Bytes - inner0.Bytes);
            Phase[10] += System.Diagnostics.Stopwatch.GetTimestamp() - t0 - (inner.Ticks - inner0.Ticks);
        }

        Phase[16] += _store.ColdProbes - probes0.ColdProbes;
        Phase[17] += _store.ColdProbesEmpty - probes0.ColdProbesEmpty;
        Phase[18] += _store.Flushes - probes0.Flushes;
        Phase[19] += _store.FlushedEntries - probes0.FlushedEntries;
        Phase[20] += _store.BulkPartitions - bulk0.BulkPartitions;
        Phase[21] += _store.BulkEntries - bulk0.BulkEntries;
        _fresh = null;
        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
        TrimScratch();
    }

    /// <summary>This operator's steps that ran in partition-hash slices (diagnostics, tests).</summary>
    internal long SlicedSteps;

    /// <summary>This operator's non-empty slices stepped (diagnostics, tests).</summary>
    internal long SliceSteps;

    /// <summary>Diagnostics (the xt harness, TOPKPHASE; blocked state only), summed over steps until reset.
    /// Bytes allocated by phase: 0 slicing passes, 1 old windows (and the change list), 2 integrate,
    /// 3 new windows, 4 emit, 5 end tick; 6 delta rows, 7 slices, 8 touched partitions, 9 rows read
    /// (old + new windows' rows); 10–15 stopwatch ticks of phases 0–5; 16 cold-record probes, 17 of them
    /// that found nothing (key-filter false positives), 18 write-back flushes, 19 entries flushed; 20
    /// partitions and 21 rows the bulk-load fast path took (§5.52).</summary>
    internal readonly long[] Phase = new long[22];

    private (long Bytes, long Ticks) InnerPhases() =>
        (Phase[1] + Phase[2] + Phase[3] + Phase[4] + Phase[5], Phase[11] + Phase[12] + Phase[13] + Phase[14] + Phase[15]);

    // Bytes and stopwatch ticks since the last mark, into phase `p`.
    private void Mark(int p, ref long bytes, ref long ticks)
    {
        var b = GC.GetAllocatedBytesForCurrentThread();
        var t = System.Diagnostics.Stopwatch.GetTimestamp();
        Phase[p] += b - bytes;
        Phase[p + 10] += t - ticks;
        bytes = b;
        ticks = t;
    }

    // A set of partitions' rows of the delta (all of it, or one slice): integrate them and emit the
    // change in their windows; the store may then drop its cache (EndTick, per slice).
    private void StepBlockedPartitions(IEnumerable<KeyValuePair<TRow, Z64>> delta, int count, ZSetBuilder<TRow, Z64> builder)
    {
        var bytes = GC.GetAllocatedBytesForCurrentThread();
        var ticks = System.Diagnostics.Stopwatch.GetTimestamp();
        var changes = new List<(StructuralRow Key, StructuralRow Row, long Weight)>(_fresh is { AllNew: true } ? 0 : count);
        List<(StructuralRow Key, StructuralRow Row, long Weight)>? fresh = null;
        foreach (var (row, dw) in delta)
        {
            var key = _partitionOf(row);
            if (_fresh is not null && _fresh.IsNew(S(key)))
            {
                // New this step (§5.52): no old window, no probe; sorted and written below.
                (fresh ??= new List<(StructuralRow, StructuralRow, long)>(count)).Add((S(key), S(row), dw.Value));
                continue;
            }

            ref var range = ref CollectionsMarshal.GetValueRefOrAddDefault(_touched, key, out var seen);
            if (!seen)
            {
                var start = _oldWindows.Count;
                StoreWindow(key, _oldWindows);
                range = (start, _oldWindows.Count - start);
            }

            changes.Add((S(key), S(row), dw.Value));
        }

        Phase[8] += _touched.Count;
        Phase[9] += _oldWindows.Count;
        Mark(1, ref bytes, ref ticks);
        _store!.Integrate(changes);
        Mark(2, ref bytes, ref ticks);
        var oldAll = CollectionsMarshal.AsSpan(_oldWindows);
        long readNew = 0, readTicks = 0;
        foreach (var (key, range) in _touched)
        {
            _newWindow.Clear();
            var b = GC.GetAllocatedBytesForCurrentThread();
            var t = System.Diagnostics.Stopwatch.GetTimestamp();
            StoreWindow(key, _newWindow);
            readNew += GC.GetAllocatedBytesForCurrentThread() - b;
            readTicks += System.Diagnostics.Stopwatch.GetTimestamp() - t;
            Phase[9] += _newWindow.Count;
            EmitDiff(builder, oldAll.Slice(range.Start, range.Length), CollectionsMarshal.AsSpan(_newWindow));
        }

        Mark(4, ref bytes, ref ticks);
        if (fresh is not null)
        {
            StepNew(fresh, builder, ref bytes, ref ticks);
        }

        Phase[3] += readNew;
        Phase[13] += readTicks;
        Phase[4] -= readNew;
        Phase[14] -= readTicks;
        _store.EndTick();
        Mark(5, ref bytes, ref ticks);
    }

    // The fast path's partitions (§5.52.2), all new this step: sorted once by (key hash, row order), each
    // partition a contiguous run whose window is emitted whole (its old window is empty), and the rows
    // written to the record as one run.
    private void StepNew(List<(StructuralRow Key, StructuralRow Row, long Weight)> rows, ZSetBuilder<TRow, Z64> builder, ref long bytes, ref long ticks)
    {
        var keys = Blocks.PartitionRuns.Sort(rows, (IComparer<StructuralRow>)(object)_order, out var runs);
        Mark(2, ref bytes, ref ticks);
        foreach (var (start, length) in runs)
        {
            _newWindow.Clear();
            if (_limit > 0)
            {
                var acc = new WindowCut(_function, _limit, _sortKeyOnly, _newWindow);
                for (var i = start; i < start + length && acc.Offer(As<TRow>(rows[i].Row), rows[i].Weight); i++)
                {
                }
            }

            EmitDiff(builder, ReadOnlySpan<Entry>.Empty, CollectionsMarshal.AsSpan(_newWindow));
        }

        Phase[8] += keys.Count;
        Mark(4, ref bytes, ref ticks);
        _store!.AppendNew(rows, keys);
        Mark(2, ref bytes, ref ticks);
    }

    // A partition's window from the store: its rows in order, cut by the rank function.
    private void StoreWindow(TKey key, List<Entry> dst)
    {
        if (_limit <= 0)
        {
            return;
        }

        var acc = new WindowCut(_function, _limit, _sortKeyOnly, dst);
        foreach (var (row, weight, _) in _store!.Read(S(key), long.MinValue, long.MaxValue, needPrefix: false, out _))
        {
            if (!acc.Offer(As<TRow>(row), weight))
            {
                break;
            }
        }
    }

    // The tick's scratch: kept for reuse unless a bulk tick grew it (Clear keeps capacity).
    private void TrimScratch()
    {
        if (_touched.Count > ScratchKeep)
        {
            _touched = new Dictionary<TKey, (int Start, int Length)>(_partitionComparer);
        }
        else
        {
            _touched.Clear();
        }

        if (_oldWindows.Count > ScratchKeep)
        {
            _oldWindows = new List<Entry>();
        }
        else
        {
            _oldWindows.Clear();
        }

        _newWindow.Clear();
        if (_newWindow.Capacity > ScratchKeep)
        {
            _newWindow = new List<Entry>();
        }
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

        if (_store is not null && _store.CanSave(writer))
        {
            // The store's own files, nothing decoded (as the rank operator, §3g).
            return _store.SaveAsync(writer, StoreFiles, cancellationToken);
        }

        if (_store is not null)
        {
            _store.DropCache();
            var rows = ZSet.FromEntries(_store.All().Select(e => (As<TRow>(e.Row), new Z64(e.Weight))));
            return SaveStoreAsync(writer, rows, cancellationToken);
        }

        // Byte-compatible with PartitionedTopKOp: one flattened Z-set; the partition
        // key is recovered from each row on load.
        var snapshot = ZSet.FromEntries(AllEntries().Select(e => (e.Row, new Z64(e.Weight))));
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    /// <summary>The store's checkpoint files' base name (§3g).</summary>
    internal const string StoreFiles = "store";

    // The whole state, and with an evicting block cache the record's backing runs (§3.7).
    private async ValueTask SaveStoreAsync(ISnapshotWriter writer, ZSet<TRow, Z64> rows, CancellationToken cancellationToken)
    {
        await _store!.SaveBackingAsync(writer, "record.backing", cancellationToken).ConfigureAwait(false);
        await _snapshotCodec!.SaveAsync(writer, "trace.arrows", rows, cancellationToken).ConfigureAwait(false);
    }

    async ValueTask<ZSet<StructuralRow, Z64>?> IStoreSnapshot.ReadStoreRowsAsync(ISnapshotReader reader, CancellationToken cancellationToken)
    {
        if (_newStore is null)
        {
            return null;
        }

        var scratch = _newStore();
        return await scratch.TryLoadAsync(reader, StoreFiles, cancellationToken).ConfigureAwait(false)
            ? ZSet.FromEntries(scratch.All().Select(e => (e.Row, new Z64(e.Weight))))
            : null;
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedTopKCompactOp was constructed without a snapshot codec.");
        }

        if (_store is not null && await _store.TryLoadAsync(reader, StoreFiles, cancellationToken).ConfigureAwait(false))
        {
            return; // the store's own files (§3g), adopted lazily
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _parts.Clear();
        if (_store is not null)
        {
            _store.Clear();
            _store.Integrate([.. loaded.Where(kv => kv.Value.Value != 0).Select(kv => (S(_partitionOf(kv.Key)), S(kv.Key), kv.Value.Value))]);
            return;
        }

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
