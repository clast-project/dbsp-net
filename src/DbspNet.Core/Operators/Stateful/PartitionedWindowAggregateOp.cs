// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental partitioned window aggregate — <c>SUM/COUNT/AVG/MIN/MAX(x) OVER
/// (PARTITION BY p [ORDER BY o RANGE …])</c> emitted as new column(s) appended to
/// every input row.
/// </summary>
/// <remarks>
/// <para>Three frame shapes share one operator, selected by the constructor
/// arguments:</para>
/// <list type="bullet">
/// <item><b>Whole partition</b> (<paramref name="orderValueOf"/> null): every row
/// gets the partition-wide aggregate.</item>
/// <item><b>Running</b> (<paramref name="orderValueOf"/> set,
/// <paramref name="preceding"/> null): each row's frame is every row at or before
/// it in ORDER BY value (<c>UNBOUNDED PRECEDING AND CURRENT ROW</c>).</item>
/// <item><b>Bounded</b> (<paramref name="preceding"/> set): each row's frame is
/// the rows whose ORDER BY value is within <paramref name="preceding"/> of the
/// current row's value. Peers (equal value) share a frame.</item>
/// </list>
/// <para>A tick recomputes only the output rows whose frame the delta could have
/// changed (a bounded value range for bounded frames; the suffix from the
/// earliest changed value for running; the whole partition for whole-partition
/// frames), diffing each against the last emitted output. The frame multiset is
/// handed to the supplied <see cref="IAggregator{TInRow,TAgg}"/> (a SQL
/// <c>CompositeAggregator</c>), and the per-row aggregate is fused onto the base
/// row by the supplied <c>widen</c> delegate — the only row-shape-specific seam,
/// which lets the same operator run over structural or typed rows.</para>
/// <para>Because finalized rows (value below the frontier) are never recomputed,
/// a bounded ascending frame over a monotone key supports frontier-driven GC:
/// rows whose value is below <c>frontier − preceding</c> can neither enter a
/// future row's backward frame nor be recomputed, so they are dropped from state
/// silently.</para>
/// </remarks>
/// <typeparam name="TInRow">The input (base) row type.</typeparam>
/// <typeparam name="TAgg">The aggregator's per-frame output (the new columns).</typeparam>
/// <typeparam name="TOutRow">The widened output row type (base ⧺ aggregate columns).</typeparam>
/// <typeparam name="TKey">The PARTITION BY key type.</typeparam>
internal sealed class PartitionedWindowAggregateOp<TInRow, TAgg, TOutRow, TKey>
    : IOperator, ISnapshotable, IIntrospectable, ISnapshotOrder
    where TInRow : notnull
    where TAgg : notnull
    where TOutRow : notnull
    where TKey : notnull
{
    private readonly Stream<ZSet<TInRow, Z64>> _input;
    private readonly Stream<ZSet<TOutRow, Z64>> _output;
    private readonly Func<TInRow, TKey> _partitionOf;
    private readonly IComparer<TInRow> _order;
    private readonly Func<TInRow, long>? _orderValueOf;
    private readonly long? _preceding;
    private readonly bool _descending;
    private readonly IAggregator<TInRow, TAgg> _aggregator;
    private readonly Func<TInRow, Optional<TAgg>, TOutRow> _widen;
    private readonly IZSetTraceCodec<TInRow, Z64>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;
    // Last tick's output count, used to pre-size this tick's delta builder
    // (docs/design-row-representation.md §16.8). The emitted diff spans only
    // the touched partitions, so no cheap exact bound exists up front.
    private int _lastOutputSize;

    // Per-partition integrated input, sorted by the total-order comparer.
    private readonly Dictionary<TKey, SortedDictionary<TInRow, long>> _accum;

    // Or, under DBSPNET_TRACE_BLOCKS=window (docs/design-flat-trace-state.md §3.13, §3.15), the same
    // integral reference-free: small partitions in a shared trace, large ones as ordered chunks
    // with partial aggregates; _accum then stays empty. Only for structural rows and keys
    // (TInRow = TKey = StructuralRow), without frontier GC.
    private readonly Blocks.OrderedPartitionStore? _store;

    // Incremental checkpoints (docs/design-durable-state.md §1f, increment 3): the snapshot is
    // the integral of the input, so the change since the last checkpoint is the inputs since.
    private readonly ZSetChangeTracker<TInRow, Z64> _changes = new();

    // Lazy restore (docs/design-durable-state.md §1f, increment 5): partitions still cold, read the
    // first time a delta touches them; null when everything is in memory.
    private LazyPartitions<TKey, TInRow>? _lazy;

    /// <inheritdoc/>
    public object? OrderFor(string fileName) =>
        fileName == "trace.arrows" ? LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf) : null;

    // The emitted output is not stored: a row's previous output is recomputed from the partition
    // as it stood before the delta (docs/design-flat-trace-state.md §3.11).

    public PartitionedWindowAggregateOp(
        Stream<ZSet<TInRow, Z64>> input,
        Stream<ZSet<TOutRow, Z64>> output,
        Func<TInRow, TKey> partitionOf,
        IComparer<TInRow> order,
        Func<TInRow, long>? orderValueOf,
        long? preceding,
        bool descending,
        IAggregator<TInRow, TAgg> aggregator,
        Func<TInRow, Optional<TAgg>, TOutRow> widen,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TInRow, Z64>? snapshotCodec = null,
        IFrontier? frontier = null,
        Blocks.ITraceBlockFormat? blockFormat = null,
        Blocks.ITraceBlockFormat? chunkFormat = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(aggregator);
        ArgumentNullException.ThrowIfNull(widen);

        _input = input;
        _output = output;
        _partitionOf = partitionOf;
        _order = order;
        _orderValueOf = orderValueOf;
        _preceding = preceding;
        _descending = descending;
        _aggregator = aggregator;
        _widen = widen;
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _accum = new Dictionary<TKey, SortedDictionary<TInRow, long>>(partitionComparer);
        if (chunkFormat is not null)
        {
            if (typeof(TInRow) != typeof(StructuralRow) || typeof(TKey) != typeof(StructuralRow))
            {
                throw new ArgumentException("Blocked window state needs structural rows and partition keys.", nameof(chunkFormat));
            }

            if (GcApplies)
            {
                throw new ArgumentException("Blocked window state does not support frontier GC.", nameof(chunkFormat));
            }

            _store = new Blocks.OrderedPartitionStore(
                chunkFormat ?? throw new ArgumentNullException(nameof(chunkFormat)),
                _orderValueOf is null ? null : row => _orderValueOf(As<TInRow>(row)),
                (IComparer<StructuralRow>)(object)_order,
                aggregator as Aggregators.IPartialAggregator<StructuralRow, StructuralRow>,
                promoteAt: Blocks.TraceBlocksMode.PromoteRows,
                chunkRows: Blocks.TraceBlocksMode.ChunkRows,
                coldFormat: blockFormat); // small partitions' record as blocks, when keys encode (§3.22)
        }
    }

    // Whether frontier-driven GC can drop rows (bounded ascending frames over a frontier).
    private bool GcApplies => _frontier is not null && _orderValueOf is not null && _preceding is not null && !_descending;

    private static StructuralRow S<T>(T value) => (StructuralRow)(object)value!;

    private static T As<T>(StructuralRow row) => (T)(object)row;

    // A tick's delta (or restored rows) into the ordered store (§3.15).
    private void IntegrateBlocks(List<(TInRow Row, long Weight)> rows) =>
        _store!.Integrate([.. rows.Select(e => (S(_partitionOf(e.Row)), S(e.Row), e.Weight))]);

    /// <summary>Total input rows currently retained across all partitions —
    /// exposed for tests that assert frontier-driven GC keeps state bounded.</summary>
    internal int RetainedRowCount
    {
        get
        {
            if (_store is not null)
            {
                // Entries held (an upper bound): counting rows exactly would decode the state, and a
                // profiling run reads this every tick.
                return (int)Math.Min(int.MaxValue, _store.Entries);
            }

            var n = 0;
            foreach (var bucket in _accum.Values)
            {
                n += bucket.Count;
            }

            return n;
        }
    }

    public string MetricName => "WindowAggregate";

    public long RetainedRows => RetainedRowCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;

    public void Step()
    {
        var delta = _input.Current;
        _changes.Record(delta);
        var builder = new ZSetBuilder<TOutRow, Z64>(_lastOutputSize);

        if (!delta.IsEmpty)
        {
            // Per touched partition: the value range the delta spans (it bounds which output rows
            // must be recomputed) and the delta's rows; fault in cold partitions.
            var touched = new Dictionary<TKey, PartitionDelta>(_accum.Comparer);
            foreach (var (row, dw) in delta)
            {
                var key = _partitionOf(row);
                if (_lazy is not null)
                {
                    Fault(key);
                }

                var v = _orderValueOf?.Invoke(row) ?? 0L;
                if (!touched.TryGetValue(key, out var pd))
                {
                    touched[key] = pd = new PartitionDelta(v);
                }

                pd.Add(row, dw.Value, v);
            }

            // Integrate.
            var tIntegrate = Blocks.BlockTraceProfile.Now();
            if (_store is not null)
            {
                IntegrateBlocks([.. delta.Select(kv => (kv.Key, kv.Value.Value))]);
            }
            else
            foreach (var (row, dw) in delta)
            {
                var key = _partitionOf(row);
                if (!_accum.TryGetValue(key, out var bucket))
                {
                    bucket = new SortedDictionary<TInRow, long>(_order);
                    _accum[key] = bucket;
                }

                bucket.TryGetValue(row, out var current);
                var next = current + dw.Value;
                if (next == 0)
                {
                    bucket.Remove(row);
                    if (bucket.Count == 0)
                    {
                        _accum.Remove(key);
                    }
                }
                else
                {
                    bucket[row] = next;
                }
            }

            var tRecompute = Blocks.BlockTraceProfile.Now();
            if (Blocks.BlockTraceProfile.Enabled)
            {
                Blocks.BlockTraceProfile.Section(8, tIntegrate, delta.Count);
            }

            foreach (var (key, pd) in touched)
            {
                RecomputePartition(builder, key, pd);
            }

            _store?.EndTick();
            if (Blocks.BlockTraceProfile.Enabled)
            {
                Blocks.BlockTraceProfile.Section(9, tRecompute, touched.Count);
            }
        }

        else
        {
            // An idle tick moves the blocked integral's managed levels into runs off the
            // checkpoint's path (§3.2).
            _store?.Freeze();
        }

        CollectGarbage();
        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
    }

    // One partition's share of a tick's delta.
    private sealed class PartitionDelta(long first)
    {
        public long Min = first;
        public long Max = first;
        public readonly List<(TInRow Row, long Dw)> Rows = new();

        public void Add(TInRow row, long dw, long v)
        {
            Rows.Add((row, dw));
            Min = Math.Min(Min, v);
            Max = Math.Max(Max, v);
        }
    }

    // A row of a touched partition before (Old) and after (New) the delta; a weight of zero is
    // absence on that side.
    private readonly record struct Entry(TInRow Row, long Old, long New, long Value);

    /// <summary>The union of a partition's rows before and after the delta, each with both
    /// weights, ascending by order value with ties in the total order — the order every output
    /// row of either side was folded in, so the old side reproduces exactly what was emitted
    /// (docs/design-flat-trace-state.md §3.11).</summary>
    private List<Entry> Union(SortedDictionary<TInRow, long>? bucket, PartitionDelta pd)
    {
        var dws = new Dictionary<TInRow, long>(pd.Rows.Count);
        foreach (var (row, dw) in pd.Rows)
        {
            dws[row] = dw;
        }

        // Rows the delta removed entirely, in the total order, merged into the bucket's sequence.
        List<TInRow>? removed = null;
        foreach (var (row, _) in pd.Rows)
        {
            if (bucket is null || !bucket.ContainsKey(row))
            {
                (removed ??= new List<TInRow>()).Add(row);
            }
        }

        removed?.Sort(_order);
        var merged = new List<Entry>((bucket?.Count ?? 0) + (removed?.Count ?? 0));
        var r = 0;
        if (bucket is not null)
        {
            foreach (var (row, weight) in bucket)
            {
                while (removed is not null && r < removed.Count && _order.Compare(removed[r], row) < 0)
                {
                    merged.Add(Gone(removed[r++]));
                }

                merged.Add(new Entry(row, weight - dws.GetValueOrDefault(row), weight, _orderValueOf?.Invoke(row) ?? 0L));
            }
        }

        while (removed is not null && r < removed.Count)
        {
            merged.Add(Gone(removed[r++]));
        }

        if (_orderValueOf is null)
        {
            return merged;
        }

        // Stable: equal values keep the total order.
        return [.. merged.OrderBy(e => e.Value)];

        Entry Gone(TInRow row) => new(row, -dws[row], 0, _orderValueOf?.Invoke(row) ?? 0L);
    }

    /// <summary>As <see cref="Union"/>, for a partition in the ordered store (§3.15): the rows it
    /// returns for [<paramref name="from"/>, <paramref name="to"/>] (already ordered and
    /// consolidated, after the delta), the delta subtracted by lookup, and the rows the delta
    /// removed merged in by (order value, total order). No sort of the partition.</summary>
    private List<Entry> ReadUnion(StructuralRow key, PartitionDelta pd, long from, long to, bool needPrefix, out object? before)
    {
        var current = _store!.Read(key, from, to, needPrefix, out before);

        // The common shape of a bulk load: a one-row partition whose row the delta brought.
        if (current.Count == 1 && pd.Rows.Count == 1 && As<TInRow>(current[0].Row).Equals(pd.Rows[0].Row))
        {
            var (row, weight, value) = current[0];
            return [new Entry(As<TInRow>(row), weight - pd.Rows[0].Dw, weight, value)];
        }

        // One lookup per row read: the delta's weight, and whether the row was found — a delta row
        // never found was taken out entirely.
        var dws = new Dictionary<TInRow, (long Dw, bool Seen)>(pd.Rows.Count);
        foreach (var (row, dw) in pd.Rows)
        {
            dws[row] = (dw, false);
        }

        var olds = new long[current.Count];
        for (var i = 0; i < current.Count; i++)
        {
            ref var slot = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrNullRef(dws, As<TInRow>(current[i].Row));
            if (!System.Runtime.CompilerServices.Unsafe.IsNullRef(ref slot))
            {
                slot.Seen = true;
                olds[i] = current[i].Weight - slot.Dw;
            }
            else
            {
                olds[i] = current[i].Weight;
            }
        }

        var gone = new List<Entry>();
        foreach (var (row, (dw, seen)) in dws)
        {
            if (!seen)
            {
                gone.Add(new Entry(row, -dw, 0, _orderValueOf?.Invoke(row) ?? 0L));
            }
        }

        if (gone.Count > 1)
        {
            gone.Sort(CompareEntries);
        }

        var rows = new List<Entry>(current.Count + gone.Count);
        var g = 0;
        for (var i = 0; i < current.Count; i++)
        {
            var (row, weight, value) = current[i];
            var e = new Entry(As<TInRow>(row), olds[i], weight, value);
            while (g < gone.Count && CompareEntries(gone[g], e) < 0)
            {
                rows.Add(gone[g++]);
            }

            rows.Add(e);
        }

        while (g < gone.Count)
        {
            rows.Add(gone[g++]);
        }

        return rows;
    }

    private int CompareEntries(Entry a, Entry b)
    {
        var c = a.Value.CompareTo(b.Value);
        return c != 0 ? c : _order.Compare(a.Row, b.Row);
    }

    private static long Saturating(long a, long b) =>
        b > 0 && a > long.MaxValue - b ? long.MaxValue : b < 0 && a < long.MinValue - b ? long.MinValue : a + b;

    /// <summary>Recompute the output rows of one partition whose frame the delta could have
    /// changed, on both sides of the delta, and emit the difference.</summary>
    private void RecomputePartition(ZSetBuilder<TOutRow, Z64> builder, TKey key, PartitionDelta pd)
    {
        var whole = _orderValueOf is null;

        // The affected output value range. For whole-partition every row is affected;
        // otherwise it is bounded by the frame geometry.
        long lo, hi;
        if (whole)
        {
            lo = long.MinValue;
            hi = long.MaxValue;
        }
        else if (_preceding is null)
        {
            // Running: a change at value d shifts every row at/after d (ASC) or at/before d (DESC).
            lo = _descending ? long.MinValue : pd.Min;
            hi = _descending ? pd.Max : long.MaxValue;
        }
        else
        {
            // Bounded: a change at d affects rows whose frame contains d.
            lo = _descending ? Saturating(pd.Min, -_preceding.Value) : pd.Min;
            hi = _descending ? pd.Max : Saturating(pd.Max, _preceding.Value);
        }

        var tUnion = Blocks.BlockTraceProfile.Now();
        List<Entry> rows;
        object? before = null;
        if (_store is not null)
        {
            // What the change reaches: an ascending running frame from its lowest value (the
            // chunks below as one partial); a bounded frame its frames' span; otherwise everything.
            if (!whole && _preceding is null && !_descending)
            {
                rows = ReadUnion(S(key), pd, pd.Min, long.MaxValue, needPrefix: true, out before);
            }
            else if (!whole && _preceding is not null)
            {
                rows = ReadUnion(
                    S(key), pd, Saturating(pd.Min, -_preceding.Value), Saturating(pd.Max, _preceding.Value), needPrefix: false, out _);
            }
            else
            {
                rows = ReadUnion(S(key), pd, long.MinValue, long.MaxValue, needPrefix: false, out _);
            }
        }
        else
        {
            _accum.TryGetValue(key, out var bucket);
            rows = Union(bucket, pd);
        }

        if (Blocks.BlockTraceProfile.Enabled)
        {
            Blocks.BlockTraceProfile.Section(10, tUnion, rows.Count);
        }

        // Running frame (ORDER BY present, UNBOUNDED PRECEDING): one ordered pass folding each
        // side into its running aggregate — linear in what was read (a per-row prefix rebuild was
        // O(partition²) and dominated ivm-bench batch 1).
        if (!whole && _preceding is null)
        {
            if (_store is { HasPartials: true } && !_descending)
            {
                RecomputeRunningPartial(builder, rows, lo, hi, before);
            }
            else
            {
                RecomputeRunningRange(builder, rows, lo, hi);
            }

            return;
        }

        // For whole-partition the aggregate is shared, so compute it once per side.
        Optional<TAgg> wholeOld = default, wholeNew = default;
        if (whole)
        {
            wholeOld = _aggregator.Compute(ZSet.FromEntries(rows.Where(e => e.Old != 0).Select(e => (e.Row, new Z64(e.Old)))));
            wholeNew = _aggregator.Compute(ZSet.FromEntries(rows.Where(e => e.New != 0).Select(e => (e.Row, new Z64(e.New)))));
        }

        foreach (var e in rows)
        {
            if (e.Value < lo || e.Value > hi)
            {
                continue;
            }

            var old = e.Old != 0 ? _widen(e.Row, whole ? wholeOld : _aggregator.Compute(FrameFor(rows, e.Value, old: true))) : default;
            var now = e.New != 0 ? _widen(e.Row, whole ? wholeNew : _aggregator.Compute(FrameFor(rows, e.Value, old: false))) : default;
            EmitRowDiff(builder, old, e.Old, now, e.New);
        }
    }

    /// <summary>As <see cref="RecomputeRunningRange"/> (ascending only), folding exact partial
    /// accumulators (§3.15) from <paramref name="before"/>, the partial of the chunks below the
    /// rows read: both sides start from it, since the delta does not reach below them.</summary>
    private void RecomputeRunningPartial(ZSetBuilder<TOutRow, Z64> builder, List<Entry> rows, long lo, long hi, object? before)
    {
        var partial = (Aggregators.IPartialAggregator<TInRow, TAgg>)_aggregator;
        var oldAcc = before is null ? partial.NewAccumulator() : partial.Clone(before);
        var newAcc = before is null ? partial.NewAccumulator() : partial.Clone(before);
        var i = 0;
        while (i < rows.Count)
        {
            var v = rows[i].Value;
            var j = i;
            while (j < rows.Count && rows[j].Value == v)
            {
                var e = rows[j];
                if (e.Old != 0)
                {
                    partial.Add(oldAcc, e.Row, e.Old);
                }

                if (e.New != 0)
                {
                    partial.Add(newAcc, e.Row, e.New);
                }

                j++;
            }

            if (v >= lo && v <= hi)
            {
                var oldValue = partial.Result(oldAcc);
                var newValue = partial.Result(newAcc);
                for (var k = i; k < j; k++)
                {
                    var e = rows[k];
                    EmitRowDiff(
                        builder,
                        e.Old != 0 ? _widen(e.Row, oldValue) : default,
                        e.Old,
                        e.New != 0 ? _widen(e.Row, newValue) : default,
                        e.New);
                }
            }

            i = j;
        }
    }

    /// <summary>Recompute a partition's running-frame (UNBOUNDED PRECEDING → CURRENT ROW)
    /// output on both sides of the delta in a single ordered pass. <paramref name="rows"/> is
    /// ascending by order value; for ASC the frame is the growing prefix (scan ascending), for
    /// DESC the growing suffix (scan descending). Each side folds its rows into its own running
    /// aggregate via the incremental <see cref="IAggregator{TInRow,TAgg}.Update"/>; a whole peer
    /// group (equal order value) is folded before any of its rows is emitted, so peers share one
    /// frame (RANGE semantics). Only rows whose order value is in [<paramref name="lo"/>,
    /// <paramref name="hi"/>] are diffed; earlier rows are folded but not re-emitted.</summary>
    private void RecomputeRunningRange(ZSetBuilder<TOutRow, Z64> builder, List<Entry> rows, long lo, long hi)
    {
        var seq = _descending ? Enumerable.Reverse(rows).ToList() : rows;
        object? oldState = null, newState = null;
        Optional<TAgg> oldRunning = Optional<TAgg>.None, newRunning = Optional<TAgg>.None;
        var oldFrame = new GrowingMultiset<TInRow>();
        var newFrame = new GrowingMultiset<TInRow>();

        var i = 0;
        while (i < seq.Count)
        {
            var v = seq[i].Value;
            var j = i;
            while (j < seq.Count && seq[j].Value == v)
            {
                var e = seq[j];
                if (e.Old != 0)
                {
                    oldFrame.Add(e.Row, e.Old);
                    oldRunning = _aggregator.Update(ref oldState, oldRunning, ZSet.FromEntries(new[] { (e.Row, new Z64(e.Old)) }), oldFrame);
                }

                if (e.New != 0)
                {
                    newFrame.Add(e.Row, e.New);
                    newRunning = _aggregator.Update(ref newState, newRunning, ZSet.FromEntries(new[] { (e.Row, new Z64(e.New)) }), newFrame);
                }

                j++;
            }

            if (v >= lo && v <= hi)
            {
                var tEmit = Blocks.BlockTraceProfile.Now();
                for (var k = i; k < j; k++)
                {
                    var e = seq[k];
                    EmitRowDiff(
                        builder,
                        e.Old != 0 ? _widen(e.Row, oldRunning) : default,
                        e.Old,
                        e.New != 0 ? _widen(e.Row, newRunning) : default,
                        e.New);
                }

                if (Blocks.BlockTraceProfile.Enabled)
                {
                    Blocks.BlockTraceProfile.Section(11, tEmit, j - i);
                }
            }

            i = j;
        }
    }

    /// <summary>The frame multiset (one side of the delta) for a row at order value
    /// <paramref name="v"/> over the ascending-by-value <paramref name="rows"/>.</summary>
    private ZSet<TInRow, Z64> FrameFor(List<Entry> rows, long v, bool old)
    {
        long lo, hi;
        if (_preceding is null)
        {
            // Running.
            lo = _descending ? v : long.MinValue;
            hi = _descending ? long.MaxValue : v;
        }
        else
        {
            // Bounded.
            lo = _descending ? v : v - _preceding.Value;
            hi = _descending ? v + _preceding.Value : v;
        }

        var entries = new List<(TInRow, Z64)>();
        foreach (var r in rows)
        {
            var w = old ? r.Old : r.New;
            if (w != 0 && r.Value >= lo && r.Value <= hi)
            {
                entries.Add((r.Row, new Z64(w)));
            }
        }

        return ZSet.FromEntries(entries);
    }

    /// <summary>Emit the change of one base row's output from (<paramref name="before"/>,
    /// <paramref name="oldWeight"/>) to (<paramref name="after"/>, <paramref name="newWeight"/>);
    /// a zero weight means no row on that side.</summary>
    private static void EmitRowDiff(ZSetBuilder<TOutRow, Z64> builder, TOutRow? before, long oldWeight, TOutRow? after, long newWeight)
    {
        if (oldWeight != 0 && newWeight != 0 && before!.Equals(after))
        {
            if (oldWeight != newWeight)
            {
                builder.Add(after!, new Z64(newWeight - oldWeight));
            }

            return;
        }

        if (oldWeight != 0)
        {
            builder.Add(before!, new Z64(-oldWeight));
        }

        if (newWeight != 0)
        {
            builder.Add(after!, new Z64(newWeight));
        }
    }

    /// <summary>Frontier-driven GC for bounded ascending frames: drop rows whose
    /// order value is below <c>frontier − preceding</c> from <see cref="_accum"/>.
    /// Such rows can neither enter a future row's backward frame nor be recomputed
    /// (future deltas land at or above the frontier), nor can any row whose frame
    /// holds them, so their already-emitted output is final and its old side is
    /// never recomputed without them — the drop emits nothing.</summary>
    private void CollectGarbage()
    {
        if (_frontier is null || _orderValueOf is null || _preceding is null || _descending)
        {
            return;
        }

        var frontier = _frontier.Value;
        if (frontier == long.MinValue || frontier <= _lastGcFrontier)
        {
            return;
        }

        _lastGcFrontier = frontier;
        var threshold = frontier - _preceding.Value;
        PromoteAll(); // GC scans every partition

        var emptyPartitions = new List<TKey>();
        foreach (var (key, bucket) in _accum)
        {
            List<TInRow>? drop = null;
            foreach (var (row, _) in bucket)
            {
                // bucket is ascending by order value (ascending frame ⇒ _order
                // refines the value), so stop once we reach the threshold.
                if (_orderValueOf(row) >= threshold)
                {
                    break;
                }

                (drop ??= new List<TInRow>()).Add(row);
            }

            if (drop is null)
            {
                continue;
            }

            _gcDropped += drop.Count;

            foreach (var row in drop)
            {
                _changes.RecordDropped(row, new Z64(bucket[row]));
                bucket.Remove(row);
            }

            if (bucket.Count == 0)
            {
                emptyPartitions.Add(key);
            }
        }

        foreach (var key in emptyPartitions)
        {
            _accum.Remove(key);
        }
    }

    // A fresh object even when empty: the shared ZSet.Empty must never carry checkpoint tracking.
    private ZSet<TInRow, Z64> SnapshotNow() =>
        _store is not null
            ? ZSet.FromEntries(_store.All().Select(e => (As<TInRow>(e.Row), new Z64(e.Weight))))
        : _accum.Count == 0
            ? ZSet<TInRow, Z64>.NewMutable()
            : ZSet.FromEntries(_accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value)))));

    // A partition a lazily restored operator has not touched yet: read its rows from cold before
    // the delta lands. Nothing else is rebuilt: the emitted output is recomputed when needed.
    private void Fault(TKey key)
    {
        if (_lazy!.Take(key) is not { Count: > 0 } rows)
        {
            return;
        }

        if (_store is not null)
        {
            IntegrateBlocks(rows);
            return;
        }

        var bucket = new SortedDictionary<TInRow, long>(_order);
        foreach (var (row, weight) in rows)
        {
            bucket[row] = weight;
        }

        _accum[key] = bucket;
    }

    // Read every partition still cold (a whole-state write, or GC).
    private void PromoteAll()
    {
        if (_lazy is null)
        {
            return;
        }

        if (_store is not null)
        {
            IntegrateBlocks([.. _lazy.TakeAll().Select(e => (e.Row, e.Weight))]);
            _lazy = null;
            return;
        }

        foreach (var (key, row, weight) in _lazy.TakeAll())
        {
            if (!_accum.TryGetValue(key, out var bucket))
            {
                bucket = new SortedDictionary<TInRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight;
        }

        _lazy = null;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedWindowAggregateOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedWindowAggregate to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // Saved in partition order (when the partition hash is stable), so a lazy restore reads a
        // partition as one block. While partitions are still cold, the snapshot holds only those in
        // memory, and FullState reads the rest for a write that needs every row.
        if (_store is not null)
        {
            _store.DropCache();

            // Blocks are not decoded for a checkpoint that writes only the changes since the last:
            // nothing is "in memory" as rows, and FullState decodes everything for a whole write.
            var empty = ZSet<TInRow, Z64>.NewMutable();
            _changes.Attach(
                empty,
                LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf),
                () => { PromoteAll(); return SnapshotNow(); },
                _store.Entries + (_lazy?.ColdCount ?? 0));
            return _snapshotCodec.SaveAsync(writer, "trace.arrows", empty, cancellationToken);
        }

        var snapshot = SnapshotNow();
        _changes.Attach(
            snapshot,
            LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf),
            _lazy is null ? null : () => { PromoteAll(); return SnapshotNow(); },
            _lazy is null ? null : snapshot.Count + _lazy.ColdCount);
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedWindowAggregateOp was constructed without a snapshot codec.");
        }

        var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
        if (_snapshotCodec is IColdRowLoader<TInRow> coldLoader
            && LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf) is not null
            && await coldLoader.LoadColdAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false) is { } cold)
        {
            // Lazily restored: partitions are read as deltas touch them (Fault).
            _accum.Clear();
            _store?.Clear();
            _lazy = new LazyPartitions<TKey, TInRow>(cold, _partitionOf, _accum.Comparer);
            _changes.Restored(cold, cold.Count);
            return;
        }

        _lazy = null;
        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _changes.Restored(loaded);
        var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
        _accum.Clear();
        if (_store is not null)
        {
            _store.Clear();
            IntegrateBlocks([.. loaded.Where(kv => kv.Value.Value != 0).Select(kv => (kv.Key, kv.Value.Value))]);
        }
        else
        foreach (var (row, weight) in loaded)
        {
            if (weight.Value == 0)
            {
                continue;
            }

            var key = _partitionOf(row);
            if (!_accum.TryGetValue(key, out var bucket))
            {
                bucket = new SortedDictionary<TInRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight.Value;
        }

        // Diagnostic (design-incremental-persistence §10): split this operator's restore into
        // the part any trace-shaped operator pays — deserialize — and the part that exists only
        // because the runtime state is a partition-keyed SortedDictionary, which must be
        // re-partitioned and re-sorted from the flat Z-set.
        // Two Stopwatch reads per restore; not on any hot path.
        var freq = (double)System.Diagnostics.Stopwatch.Frequency;
        var rebuildMs = (System.Diagnostics.Stopwatch.GetTimestamp() - t1) * 1000.0 / freq;
        PartitionedWindowAggregateLoadProfile.Add((t1 - t0) * 1000.0 / freq, rebuildMs);
        if (SnapshotRestoreProfile.Enabled)
        {
            SnapshotRestoreProfile.AddRebuild(rebuildMs);
        }
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    /// <summary>A dictionary-backed weighted multiset grown one entry at a time,
    /// used as the running frame in <see cref="RecomputeRunningRange"/>. Add is
    /// O(1) and <see cref="SumWeights"/> / <see cref="WeightOf"/> are O(1) (the
    /// two things an incremental <see cref="IAggregator{TInRow,TAgg}.Update"/>
    /// reads), so folding a whole partition is linear — unlike rebuilding a fresh
    /// Z-set per row.</summary>
    private sealed class GrowingMultiset<TRow> : IMultiset<TRow, Z64>
        where TRow : notnull
    {
        private readonly Dictionary<TRow, long> _entries = new();
        private long _sum;

        public void Add(TRow row, long weight)
        {
            _entries.TryGetValue(row, out var current);
            var next = current + weight;
            if (next == 0)
            {
                _entries.Remove(row);
            }
            else
            {
                _entries[row] = next;
            }

            _sum += weight;
        }

        public bool IsEmpty => _entries.Count == 0;

        public Z64 WeightOf(TRow key) => _entries.TryGetValue(key, out var w) ? new Z64(w) : new Z64(0);

        public Z64 SumWeights() => new Z64(_sum);

        public IEnumerator<KeyValuePair<TRow, Z64>> GetEnumerator()
        {
            foreach (var (row, weight) in _entries)
            {
                yield return new KeyValuePair<TRow, Z64>(row, new Z64(weight));
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

/// <summary>
/// Restore-cost split for <see cref="PartitionedWindowAggregateOp{TInRow,TAgg,TOutRow,TKey}"/>,
/// accumulated across every generic instantiation (a static on the generic type would be per
/// closed type). <see cref="DeserializeMs"/> is the codec read every trace-shaped operator also
/// pays; <see cref="RebuildMs"/> is the re-partition + re-sort + recompute that exists only
/// because the runtime state is not itself an indexed Z-set.
/// </summary>
internal static class PartitionedWindowAggregateLoadProfile
{
    private static double _deserializeMs;
    private static double _rebuildMs;
    private static int _count;

    public static double DeserializeMs => _deserializeMs;

    public static double RebuildMs => _rebuildMs;

    public static int Count => _count;

    public static void Add(double deserializeMs, double rebuildMs)
    {
        _deserializeMs += deserializeMs;
        _rebuildMs += rebuildMs;
        _count++;
    }

    public static void Reset()
    {
        _deserializeMs = 0;
        _rebuildMs = 0;
        _count = 0;
    }
}
