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
    : IOperator, ISnapshotable, IIntrospectable
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

    // Per-partition integrated input, sorted by the total-order comparer.
    private readonly Dictionary<TKey, SortedDictionary<TInRow, long>> _accum;

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
        IFrontier? frontier = null)
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
    }

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
        var builder = new ZSetBuilder<TOutRow, Z64>();

        if (!delta.IsEmpty)
        {
            // Integrate, and track per touched partition the value range the delta
            // spans (used to bound which output rows must be recomputed).
            var touched = new Dictionary<TKey, (long Min, long Max)>(_accum.Comparer);
            foreach (var (row, dw) in delta)
            {
                var key = _partitionOf(row);
                var v = _orderValueOf?.Invoke(row) ?? 0L;
                if (touched.TryGetValue(key, out var range))
                {
                    touched[key] = (Math.Min(range.Min, v), Math.Max(range.Max, v));
                }
                else
                {
                    touched[key] = (v, v);
                }

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
        _output.SetCurrent(builder.Build());
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
                    win.Remove(row);
                }

                if (win.Count == 0)
                {
                    _window.Remove(key);
                }
            }

            foreach (var row in drop)
            {
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

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedWindowAggregateOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedWindowAggregate to enable Snapshot.WriteAsync/ReadAsync.");
        }

        var entries = _accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value))));
        var snapshot = ZSet.FromEntries(entries);
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedWindowAggregateOp was constructed without a snapshot codec.");
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
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
        // materialised downstream, so record them without emitting (recompute the
        // whole partition by passing a full delta range).
        var sink = new ZSetBuilder<TOutRow, Z64>();
        foreach (var key in _accum.Keys.ToList())
        {
            RecomputePartition(sink, key, (long.MinValue, long.MaxValue));
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
