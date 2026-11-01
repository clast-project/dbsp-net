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

    // Incremental checkpoints (docs/design-durable-state.md §1f, increment 3): the snapshot is
    // the integral of the input, so the change since the last checkpoint is the inputs since.
    private readonly ZSetChangeTracker<TInRow, Z64> _changes = new();

    // Lazy restore (docs/design-durable-state.md §1f, increment 5): partitions still cold, read the
    // first time a delta touches them; null when everything is in memory.
    private LazyPartitions<TKey, TInRow>? _lazy;

    /// <inheritdoc/>
    public object? OrderFor(string fileName) => fileName switch
    {
        "trace.arrows" => LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf),
        WindowFile => LazyPartitions<TKey, TOutRow>.PartitionOrder(WindowPartitionOf),
        _ => null,
    };

    // Last-emitted output per partition, keyed by the input (base) row →
    // (widened row, weight). Keying by base row lets GC drop a finalized row
    // from both _accum and here without emitting a retraction.
    private readonly Dictionary<TKey, Dictionary<TInRow, (TOutRow Widened, long Weight)>> _window;

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
        IZSetTraceCodec<TOutRow, Z64>? windowCodec = null,
        Func<TOutRow, TInRow>? baseOf = null)
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
        _window = new Dictionary<TKey, Dictionary<TInRow, (TOutRow, long)>>(partitionComparer);
        _windowCodec = windowCodec;
        _baseOf = baseOf;
    }

    // Persisted window outputs (window.arrows): the output rows last emitted, weighted as emitted,
    // in partition order, so a restore reads a partition's old outputs instead of recomputing them
    // (docs/design-durable-state.md §1f). Their change per step is exactly the step's output delta,
    // plus the rows GC drops from _window without emitting. Needs the output → base row map
    // (baseOf); without it — or without the file, an older snapshot — the windows are recomputed.
    private readonly IZSetTraceCodec<TOutRow, Z64>? _windowCodec;
    private readonly Func<TOutRow, TInRow>? _baseOf;
    private readonly ZSetChangeTracker<TOutRow, Z64> _windowChanges = new();
    private LazyPartitions<TKey, TOutRow>? _lazyWindow;

    // The window outputs were restored from the window file (eagerly, or cold in _lazyWindow):
    // a faulted partition reads its window rather than recomputing it.
    private bool _windowRestored;

    private bool PersistsWindow => _windowCodec is not null && _baseOf is not null;

    private TKey WindowPartitionOf(TOutRow row) => _partitionOf(_baseOf!(row));

    private const string WindowFile = "window.arrows";

    /// <summary>Total input rows currently retained across all partitions —
    /// exposed for tests that assert frontier-driven GC keeps state bounded.</summary>
    internal int RetainedRowCount
    {
        get
        {
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
            // Track per touched partition the value range the delta spans (used to bound
            // which output rows must be recomputed); fault in cold partitions.
            var touched = new Dictionary<TKey, (long Min, long Max)>(_accum.Comparer);
            foreach (var (row, _) in delta)
            {
                var key = _partitionOf(row);
                if (_lazy is not null)
                {
                    Fault(key);
                }

                var v = _orderValueOf?.Invoke(row) ?? 0L;
                touched[key] = touched.TryGetValue(key, out var range) ? (Math.Min(range.Min, v), Math.Max(range.Max, v)) : (v, v);
            }

            // A faulted partition's window is known only where a step has needed it: before the
            // delta lands, rebuild it over this delta's range from the partition as it stands.
            if (_partial is not null)
            {
                foreach (var (key, range) in touched)
                {
                    EnsureWindow(key, range);
                }
            }

            // Integrate.
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

            foreach (var (key, range) in touched)
            {
                RecomputePartition(builder, key, range);
            }
        }

        CollectGarbage();
        var result = builder.Build();
        if (PersistsWindow)
        {
            _windowChanges.Record(result); // what was emitted is what changed in _window
        }

        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
    }

    /// <summary>Recompute the output rows of one partition whose frame the delta
    /// (spanning value range <paramref name="deltaRange"/>) could have changed, and
    /// emit the +/- diffs against the previously emitted output.</summary>
    private void RecomputePartition(ZSetBuilder<TOutRow, Z64> builder, TKey key, (long Min, long Max) deltaRange)
    {
        _accum.TryGetValue(key, out var bucket);
        _window.TryGetValue(key, out var win);
        win ??= new Dictionary<TInRow, (TOutRow, long)>();

        // Materialise the current partition ascending by order value; the frame
        // for any row is then a contiguous slice.
        var rows = bucket is null
            ? new List<(TInRow Row, long Weight, long Value)>()
            : bucket.Select(kv => (Row: kv.Key, Weight: kv.Value, Value: _orderValueOf?.Invoke(kv.Key) ?? 0L))
                    .OrderBy(r => r.Value).ToList();

        var whole = _orderValueOf is null;

        // A full (MinValue, MaxValue) delta range is the "recompute everything"
        // sentinel used by Load; treat it (and whole-partition) as all-rows,
        // short-circuiting the frame arithmetic to avoid overflow.
        var recomputeAll = whole
            || (deltaRange.Min == long.MinValue && deltaRange.Max == long.MaxValue);

        // The affected output value range. For whole-partition every row is
        // affected; otherwise it is bounded by the frame geometry.
        long lo, hi;
        if (recomputeAll)
        {
            lo = long.MinValue;
            hi = long.MaxValue;
        }
        else if (_preceding is null)
        {
            // Running: a change at value d shifts every row at/after d (ASC) or
            // at/before d (DESC).
            lo = _descending ? long.MinValue : deltaRange.Min;
            hi = _descending ? deltaRange.Max : long.MaxValue;
        }
        else
        {
            // Bounded: a change at d affects rows whose frame contains d.
            lo = _descending ? deltaRange.Min - _preceding.Value : deltaRange.Min;
            hi = _descending ? deltaRange.Max : deltaRange.Max + _preceding.Value;
        }

        // Running frame (ORDER BY present, UNBOUNDED PRECEDING): recompute the
        // affected rows in one ordered pass that folds each row into a running
        // aggregate via the incremental IAggregator.Update, instead of rebuilding
        // and re-aggregating every row's whole prefix from scratch. The old
        // per-row FrameFor + Compute was O(partition²) in both time and
        // allocation and dominated ivm-bench batch-1 (the running MIN/MAX 52-week
        // windows over daily_market). Linear per touched partition.
        if (!whole && _preceding is null)
        {
            RecomputeRunningRange(builder, bucket, win, key, rows, lo, hi);
            return;
        }

        // For whole-partition the aggregate is shared, so compute it once.
        var wholeAgg = whole
            ? _aggregator.Compute(ZSet.FromEntries(rows.Select(r => (r.Row, new Z64(r.Weight)))))
            : default;

        // Candidate base rows = current rows in range ∪ previously-emitted rows in
        // range (the latter catches rows the delta retracted).
        var candidates = new HashSet<TInRow>();
        foreach (var r in rows)
        {
            if (r.Value >= lo && r.Value <= hi)
            {
                candidates.Add(r.Row);
            }
        }

        foreach (var baseRow in win.Keys)
        {
            var v = _orderValueOf?.Invoke(baseRow) ?? 0L;
            if (v >= lo && v <= hi)
            {
                candidates.Add(baseRow);
            }
        }

        foreach (var baseRow in candidates)
        {
            if (bucket is not null && bucket.TryGetValue(baseRow, out var weight))
            {
                var agg = whole ? wholeAgg : _aggregator.Compute(FrameFor(rows, _orderValueOf!(baseRow)));
                var widened = _widen(baseRow, agg);
                EmitRowDiff(builder, win, baseRow, widened, weight);
            }
            else if (win.TryGetValue(baseRow, out var old))
            {
                builder.Add(old.Widened, new Z64(-old.Weight));
                win.Remove(baseRow);
            }
        }

        if (win.Count == 0)
        {
            _window.Remove(key);
        }
        else
        {
            _window[key] = win;
        }
    }

    /// <summary>Recompute a partition's running-frame (UNBOUNDED PRECEDING →
    /// CURRENT ROW) output in a single ordered pass. <paramref name="rows"/> is
    /// ascending by order value; for ASC the frame is the growing prefix (scan
    /// ascending), for DESC the growing suffix (scan descending). Each row is
    /// folded into a running aggregate via the incremental
    /// <see cref="IAggregator{TInRow,TAgg}.Update"/>; a whole peer group (equal
    /// order value) is folded before any of its rows is emitted, so peers share
    /// one frame (RANGE semantics). Only rows whose order value is in the affected
    /// range [<paramref name="lo"/>, <paramref name="hi"/>] are diffed against the
    /// last emitted output; earlier rows are folded but not re-emitted (their
    /// frame is unchanged by a delta at or after them).</summary>
    private void RecomputeRunningRange(
        ZSetBuilder<TOutRow, Z64> builder,
        SortedDictionary<TInRow, long>? bucket,
        Dictionary<TInRow, (TOutRow Widened, long Weight)> win,
        TKey key,
        List<(TInRow Row, long Weight, long Value)> rows,
        long lo,
        long hi)
    {
        var seq = _descending ? Enumerable.Reverse(rows).ToList() : rows;
        object? state = null;
        var running = Optional<TAgg>.None;
        var frame = new GrowingMultiset<TInRow>();
        var emitted = new HashSet<TInRow>();

        var i = 0;
        while (i < seq.Count)
        {
            var v = seq[i].Value;

            // Fold the whole peer group (equal order value) before emitting, so
            // peers see the same frame.
            var j = i;
            while (j < seq.Count && seq[j].Value == v)
            {
                var (row, weight, _) = seq[j];
                frame.Add(row, weight);
                running = _aggregator.Update(
                    ref state, running, ZSet.FromEntries(new[] { (row, new Z64(weight)) }), frame);
                j++;
            }

            if (v >= lo && v <= hi)
            {
                for (var k = i; k < j; k++)
                {
                    var (row, weight, _) = seq[k];
                    EmitRowDiff(builder, win, row, _widen(row, running), weight);
                    emitted.Add(row);
                }
            }

            i = j;
        }

        // Retract previously-emitted rows in the affected range that the delta
        // removed (present in the last output, no longer in the partition).
        if (win.Count > 0)
        {
            List<TInRow>? gone = null;
            foreach (var baseRow in win.Keys)
            {
                if (emitted.Contains(baseRow))
                {
                    continue;
                }

                var v = _orderValueOf!(baseRow);
                if (v < lo || v > hi || (bucket is not null && bucket.ContainsKey(baseRow)))
                {
                    continue;
                }

                (gone ??= new List<TInRow>()).Add(baseRow);
            }

            if (gone is not null)
            {
                foreach (var baseRow in gone)
                {
                    var old = win[baseRow];
                    builder.Add(old.Widened, new Z64(-old.Weight));
                    win.Remove(baseRow);
                }
            }
        }

        if (win.Count == 0)
        {
            _window.Remove(key);
        }
        else
        {
            _window[key] = win;
        }
    }

    /// <summary>The frame multiset for a row at order value <paramref name="v"/>
    /// over the ascending-by-value <paramref name="rows"/>.</summary>
    private ZSet<TInRow, Z64> FrameFor(List<(TInRow Row, long Weight, long Value)> rows, long v)
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
            if (r.Value >= lo && r.Value <= hi)
            {
                entries.Add((r.Row, new Z64(r.Weight)));
            }
        }

        return ZSet.FromEntries(entries);
    }

    /// <summary>Emit the diff moving base row <paramref name="baseRow"/>'s previous
    /// output (in <paramref name="win"/>) to (<paramref name="widened"/>,
    /// <paramref name="weight"/>), and update <paramref name="win"/>.</summary>
    private static void EmitRowDiff(
        ZSetBuilder<TOutRow, Z64> builder,
        Dictionary<TInRow, (TOutRow Widened, long Weight)> win,
        TInRow baseRow,
        TOutRow widened,
        long weight)
    {
        if (win.TryGetValue(baseRow, out var old))
        {
            if (old.Widened.Equals(widened))
            {
                if (old.Weight != weight)
                {
                    builder.Add(widened, new Z64(weight - old.Weight));
                }
            }
            else
            {
                builder.Add(old.Widened, new Z64(-old.Weight));
                builder.Add(widened, new Z64(weight));
            }
        }
        else
        {
            builder.Add(widened, new Z64(weight));
        }

        win[baseRow] = (widened, weight);
    }

    /// <summary>Frontier-driven GC for bounded ascending frames: drop rows whose
    /// order value is below <c>frontier − preceding</c> from both <see cref="_accum"/>
    /// and <see cref="_window"/>. Such rows can neither enter a future row's
    /// backward frame nor be recomputed (future deltas land at or above the
    /// frontier), and their already-emitted output is final — so the drop emits
    /// nothing.</summary>
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

            if (_window.TryGetValue(key, out var win))
            {
                foreach (var row in drop)
                {
                    if (PersistsWindow && win.TryGetValue(row, out var dropped))
                    {
                        _windowChanges.RecordDropped(dropped.Widened, new Z64(dropped.Weight)); // dropped, not retracted
                    }

                    win.Remove(row);
                }

                if (win.Count == 0)
                {
                    _window.Remove(key);
                }
            }

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

    // The persisted window outputs: every emitted row, weighted as emitted (fresh even when empty).
    private ZSet<TOutRow, Z64> WindowSnapshotNow()
    {
        var b = new ZSetBuilder<TOutRow, Z64>();
        foreach (var win in _window.Values)
        {
            foreach (var (widened, weight) in win.Values)
            {
                b.Add(widened, new Z64(weight));
            }
        }

        var z = b.Build();
        return z.IsEmpty ? ZSet<TOutRow, Z64>.NewMutable() : z;
    }

    // A fresh object even when empty: the shared ZSet.Empty must never carry checkpoint tracking.
    private ZSet<TInRow, Z64> SnapshotNow() =>
        _accum.Count == 0
            ? ZSet<TInRow, Z64>.NewMutable()
            : ZSet.FromEntries(_accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value)))));

    // A partition a lazily restored operator has not touched yet: read its rows from cold and
    // rebuild its window — what the eager load does for every partition — before the delta lands.
    private void Fault(TKey key)
    {
        if (_lazy!.Take(key) is not { Count: > 0 } rows)
        {
            return;
        }

        var bucket = new SortedDictionary<TInRow, long>(_order);
        foreach (var (row, weight) in rows)
        {
            bucket[row] = weight;
        }

        _accum[key] = bucket;

        // Its window was persisted: read it (the outputs emitted before the restore) rather than
        // recompute it.
        if (_windowRestored)
        {
            if (_lazyWindow?.Take(key) is { Count: > 0 } outs)
            {
                var win = new Dictionary<TInRow, (TOutRow, long)>(outs.Count);
                foreach (var (widened, weight) in outs)
                {
                    win[_baseOf!(widened)] = (widened, weight);
                }

                _window[key] = win;
            }

            return;
        }

        // Its window is rebuilt range by range as steps need it (EnsureWindow), not whole: a
        // partition with a long frame costs O(rows x frame) to rebuild whole, and a step reads
        // only the rows its delta's frame reaches.
        _partial ??= new Dictionary<TKey, (long Min, long Max)>(_accum.Comparer);
        _partial[key] = (long.MaxValue, long.MinValue); // nothing known yet
    }

    // Faulted partitions whose window (_window) is known only over a hull of delta ranges; a
    // partition absent here has its whole window.
    private Dictionary<TKey, (long Min, long Max)>? _partial;

    // Before a delta over `range` lands in a faulted partition: rebuild its window over the
    // rows that delta's frame reaches, from the partition as it stands (the outputs emitted
    // before the restore). Known spans are recomputed to the same values, so widening the hull
    // is safe; a whole-partition frame (no ORDER BY) is rebuilt whole.
    private void EnsureWindow(TKey key, (long Min, long Max) range)
    {
        if (!_partial!.TryGetValue(key, out var known))
        {
            return;
        }

        if (_orderValueOf is null)
        {
            _partial.Remove(key);
            if (_accum.TryGetValue(key, out var all))
            {
                InitWindow(key, all);
            }

            return;
        }

        if (range.Min >= known.Min && range.Max <= known.Max)
        {
            return;
        }

        var hull = (Math.Min(range.Min, known.Min), Math.Max(range.Max, known.Max));
        RecomputePartition(new ZSetBuilder<TOutRow, Z64>(), key, hull);
        _partial[key] = hull;
    }

    // Read every partition still cold (a whole-state write, or GC).
    private void PromoteAll()
    {
        if (_lazy is null)
        {
            return;
        }

        var filled = new HashSet<TKey>(_accum.Comparer);
        foreach (var (key, row, weight) in _lazy.TakeAll())
        {
            if (!_accum.TryGetValue(key, out var bucket))
            {
                bucket = new SortedDictionary<TInRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight;
            filled.Add(key);
        }

        _lazy = null;
        if (_windowRestored)
        {
            if (_lazyWindow is { } lazyWindow)
            {
                _lazyWindow = null;
                foreach (var (key, widened, weight) in lazyWindow.TakeAll())
                {
                    if (!_window.TryGetValue(key, out var win))
                    {
                        win = new Dictionary<TInRow, (TOutRow, long)>();
                        _window[key] = win;
                    }

                    win[_baseOf!(widened)] = (widened, weight);
                }
            }

            return;
        }

        foreach (var key in filled)
        {
            InitWindow(key, _accum[key]);
        }
    }

    // Record a partition's window as already materialised downstream, emitting nothing.
    private void InitWindow(TKey key, SortedDictionary<TInRow, long> bucket)
    {
        _ = bucket; // the window aggregate recomputes from _accum[key]
        RecomputePartition(new ZSetBuilder<TOutRow, Z64>(), key, (long.MinValue, long.MaxValue));
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
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
        var snapshot = SnapshotNow();
        _changes.Attach(
            snapshot,
            LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf),
            _lazy is null ? null : () => { PromoteAll(); return SnapshotNow(); },
            _lazy is null ? null : snapshot.Count + _lazy.ColdCount);
        await _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken).ConfigureAwait(false);
        if (PersistsWindow)
        {
            // Restored from a snapshot without window outputs: the first write of them needs every
            // partition's whole window, so complete what was faulted only in part (once).
            if (!_windowRestored)
            {
                PromoteAll();
                if (_partial is not null)
                {
                    foreach (var key in _partial.Keys.ToList())
                    {
                        if (_accum.TryGetValue(key, out var all))
                        {
                            InitWindow(key, all);
                        }
                    }

                    _partial = null;
                }

                _windowRestored = true; // _window is now whole
            }

            var window = WindowSnapshotNow();
            _windowChanges.Attach(
                window,
                LazyPartitions<TKey, TOutRow>.PartitionOrder(WindowPartitionOf),
                _lazy is null && _lazyWindow is null ? null : () => { PromoteAll(); return WindowSnapshotNow(); },
                _lazyWindow is null ? null : window.Count + _lazyWindow.ColdCount);
            await _windowCodec!.SaveAsync(writer, WindowFile, window, cancellationToken).ConfigureAwait(false);
        }
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
            _window.Clear();
            _lazy = new LazyPartitions<TKey, TInRow>(cold, _partitionOf, _accum.Comparer);
            _changes.Restored(cold, cold.Count);
            _partial = null;
            _lazyWindow = null;
            _windowRestored = false;
            if (PersistsWindow && await reader.ExistsAsync(WindowFile, cancellationToken).ConfigureAwait(false))
            {
                if (_windowCodec is IColdRowLoader<TOutRow> windowLoader
                    && await windowLoader.LoadColdAsync(reader, WindowFile, cancellationToken).ConfigureAwait(false) is { } coldWindow)
                {
                    _lazyWindow = new LazyPartitions<TKey, TOutRow>(coldWindow, WindowPartitionOf, _accum.Comparer);
                    _windowChanges.Restored(coldWindow, coldWindow.Count);
                }
                else
                {
                    LoadWindow(await _windowCodec!.LoadAsync(reader, WindowFile, cancellationToken).ConfigureAwait(false));
                }

                _windowRestored = true;
            }

            return;
        }

        _lazy = null;
        _lazyWindow = null;
        _partial = null;
        _windowRestored = false;
        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _changes.Restored(loaded);
        var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
        _accum.Clear();
        _window.Clear();
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

        // The restored accumulation is current; the windows it implies are already
        // materialised downstream. Read them when they were persisted; otherwise record them
        // without emitting (recompute the whole partition by passing a full delta range).
        if (PersistsWindow && await reader.ExistsAsync(WindowFile, cancellationToken).ConfigureAwait(false))
        {
            LoadWindow(await _windowCodec!.LoadAsync(reader, WindowFile, cancellationToken).ConfigureAwait(false));
            _windowRestored = true;
        }
        else
        {
            var sink = new ZSetBuilder<TOutRow, Z64>();
            foreach (var key in _accum.Keys.ToList())
            {
                RecomputePartition(sink, key, (long.MinValue, long.MaxValue));
            }
        }

        // Diagnostic (design-incremental-persistence §10): split this operator's restore into
        // the part any trace-shaped operator pays — deserialize — and the part that exists only
        // because the runtime state is a partition-keyed SortedDictionary plus a materialised
        // window, which must be re-partitioned, re-sorted and recomputed from the flat Z-set.
        // Two Stopwatch reads per restore; not on any hot path.
        var freq = (double)System.Diagnostics.Stopwatch.Frequency;
        var rebuildMs = (System.Diagnostics.Stopwatch.GetTimestamp() - t1) * 1000.0 / freq;
        PartitionedWindowAggregateLoadProfile.Add((t1 - t0) * 1000.0 / freq, rebuildMs);
        if (SnapshotRestoreProfile.Enabled)
        {
            SnapshotRestoreProfile.AddRebuild(rebuildMs);
        }
    }

    // The window outputs read whole from the window file.
    private void LoadWindow(ZSet<TOutRow, Z64> loaded)
    {
        _window.Clear();
        foreach (var (widened, weight) in loaded)
        {
            if (weight.Value == 0)
            {
                continue;
            }

            var key = WindowPartitionOf(widened);
            if (!_window.TryGetValue(key, out var win))
            {
                win = new Dictionary<TInRow, (TOutRow, long)>();
                _window[key] = win;
            }

            win[_baseOf!(widened)] = (widened, weight.Value);
        }

        _windowChanges.Restored(loaded);
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
