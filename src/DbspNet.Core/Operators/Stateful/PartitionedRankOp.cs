// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental rank-in-output — the <c>ROW_NUMBER</c> / <c>RANK</c> /
/// <c>DENSE_RANK</c> window functions emitted as a new column appended to every
/// row (the general form, not the <c>… OVER (…) &lt;= k</c> filter pattern of
/// <see cref="PartitionedTopKOp{TRow,TKey}"/>). Each partition keeps its own
/// integrated multiset, sorted by a total-order <paramref name="order"/> comparer;
/// every row is re-emitted widened by its rank within the partition.
/// </summary>
/// <remarks>
/// <para>This shares the per-partition sorted trace and rank-assignment logic of
/// <see cref="PartitionedTopKOp{TRow,TKey}"/> and the widened-output diffing of
/// <see cref="PartitionedWindowAggregateOp{TInRow,TAgg,TOutRow,TKey}"/>, but is
/// neither: TOP-K filters (never widens) and window aggregates recompute a
/// value-bounded range. Rank is <em>positional</em> — an insert shifts the rank of
/// every later row with no value-arithmetic bound — so a touched partition is
/// recomputed in full and diffed against its last emitted output.</para>
/// <para>Multiplicity is honoured. <c>ROW_NUMBER</c> gives a weight-<c>w</c> row
/// <c>w</c> distinct consecutive ranks (one output row of weight 1 each);
/// <c>RANK</c> / <c>DENSE_RANK</c> give every copy the group's shared rank (one
/// output row of weight <c>w</c>). Tie groups — contiguous because
/// <paramref name="order"/> refines <paramref name="sortKeyOnly"/> — are detected
/// through <paramref name="sortKeyOnly"/>, a comparer over the ORDER BY keys alone
/// (<see cref="ConstantZeroComparer{T}"/> tiebreak).</para>
/// <para>No emitted output is stored (docs/design-flat-trace-state.md §3.29): a touched partition's
/// previous ranking is its rows before the tick's delta, so the operator walks the old and new
/// rankings together and widens only the rows whose rank or weight changed. With block formats the
/// partitions live in an <see cref="Blocks.OrderedPartitionStore"/> (a blocked record and a
/// write-back cache dropped at each checkpoint, §3.22).</para>
/// <para>Because a future row with a smaller ORDER BY value re-ranks everyone, no
/// row is ever finalizable: <see cref="GcFrontier"/> is <c>null</c> and the whole
/// integrated input is retained per partition (inherent to positional ranking
/// under retraction, as in <see cref="PartitionedTopKOp{TRow,TKey}"/>).</para>
/// </remarks>
/// <typeparam name="TInRow">The input (base) row type.</typeparam>
/// <typeparam name="TOutRow">The widened output row type (base ⧺ rank column).</typeparam>
/// <typeparam name="TKey">The PARTITION BY key type.</typeparam>
internal sealed class PartitionedRankOp<TInRow, TOutRow, TKey> : IOperator, ISnapshotable, IIntrospectable, ISnapshotOrder
    where TInRow : notnull
    where TOutRow : notnull
    where TKey : notnull
{
    private readonly Stream<ZSet<TInRow, Z64>> _input;
    private readonly Stream<ZSet<TOutRow, Z64>> _output;
    private readonly Func<TInRow, TKey> _partitionOf;
    private readonly IComparer<TInRow> _order;
    private readonly IComparer<TInRow> _sortKeyOnly;
    private readonly RankFunction _function;
    private readonly Func<TInRow, long, TOutRow> _widen;
    private readonly IZSetTraceCodec<TInRow, Z64>? _snapshotCodec;
    private readonly IEqualityComparer<TKey>? _partitionComparer;

    // Per-partition integrated input, each sorted by the total-order comparer (managed state).
    private readonly Dictionary<TKey, SortedDictionary<TInRow, long>> _accum;

    // The same, as blocks (§3.29): whole-partition reads, so every partition stays "small" — a
    // blocked record with a write-back cache. Null: managed state.
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

    // Previous tick's output size — pre-sizes this tick's delta builder to avoid
    // dictionary-resize churn (fresh alloc, no reuse; §16.7). Perf hint only.
    private int _lastOutputSize;

    public PartitionedRankOp(
        Stream<ZSet<TInRow, Z64>> input,
        Stream<ZSet<TOutRow, Z64>> output,
        Func<TInRow, TKey> partitionOf,
        IComparer<TInRow> order,
        IComparer<TInRow> sortKeyOnly,
        RankFunction function,
        Func<TInRow, long, TOutRow> widen,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TInRow, Z64>? snapshotCodec = null,
        Blocks.ITraceBlockFormat? chunkFormat = null,
        Blocks.ITraceBlockFormat? blockFormat = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(sortKeyOnly);
        ArgumentNullException.ThrowIfNull(widen);

        _input = input;
        _output = output;
        _partitionOf = partitionOf;
        _order = order;
        _sortKeyOnly = sortKeyOnly;
        _function = function;
        _widen = widen;
        _snapshotCodec = snapshotCodec;
        _partitionComparer = partitionComparer;
        _accum = new Dictionary<TKey, SortedDictionary<TInRow, long>>(partitionComparer);
        if (chunkFormat is not null)
        {
            if (typeof(TInRow) != typeof(StructuralRow) || typeof(TKey) != typeof(StructuralRow))
            {
                throw new ArgumentException("Blocked rank state needs structural rows and partition keys.", nameof(chunkFormat));
            }

            _store = new Blocks.OrderedPartitionStore(
                chunkFormat,
                valueOf: null, // whole-partition reads: partitions stay small
                (IComparer<StructuralRow>)(object)_order,
                partial: null,
                coldFormat: blockFormat);
        }
    }

    private static StructuralRow S<T>(T value) => (StructuralRow)(object)value!;

    private static T As<T>(StructuralRow row) => (T)(object)row;

    /// <summary>Whether the partitions are held as blocks (tests).</summary>
    internal bool IsBlocked => _store is not null;

    public string MetricName => "PartitionedRank";

    public long RetainedRows
    {
        get
        {
            if (_store is not null)
            {
                return _store.Entries;
            }

            long n = 0;
            foreach (var bucket in _accum.Values)
            {
                n += bucket.Count;
            }

            return n;
        }
    }

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    // A tick's delta (or restored rows) into the blocked store.
    private void IntegrateBlocks(IEnumerable<(TInRow Row, long Weight)> rows) =>
        _store!.Integrate([.. rows.Select(e => (S(_partitionOf(e.Row)), S(e.Row), e.Weight))]);

    public void Step()
    {
        var delta = _input.Current;
        _changes.Record(delta);
        if (delta.IsEmpty)
        {
            // An idle tick: the store may freeze its record (§3.22).
            _store?.Freeze();
            _output.SetCurrent(ZSet<TOutRow, Z64>.Empty);
            return;
        }

        // Only the partitions a tick touches can change their ranking: each one's rows before the
        // delta, read now (in order, as the next read after the delta will be).
        var touched = new Dictionary<TKey, List<(TInRow Row, long Weight)>>(_partitionComparer);
        foreach (var (row, _) in delta)
        {
            var key = _partitionOf(row);
            if (_lazy is not null)
            {
                Fault(key);
            }

            if (!touched.ContainsKey(key))
            {
                touched[key] = Current(key);
            }
        }

        if (_store is not null)
        {
            IntegrateBlocks(delta.Select(kv => (kv.Key, kv.Value.Value)));
        }
        else
        {
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
        }

        var builder = new ZSetBuilder<TOutRow, Z64>(_lastOutputSize);
        foreach (var (key, before) in touched)
        {
            EmitDiff(builder, before, Current(key));
        }

        _store?.EndTick();
        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
    }

    // A partition's rows after this tick, in order, consolidated.
    private List<(TInRow Row, long Weight)> Current(TKey key)
    {
        var rows = new List<(TInRow Row, long Weight)>();
        if (_store is not null)
        {
            foreach (var (row, weight, _) in _store.Read(S(key), long.MinValue, long.MaxValue, needPrefix: false, out _))
            {
                rows.Add((As<TInRow>(row), weight));
            }
        }
        else if (_accum.TryGetValue(key, out var bucket))
        {
            foreach (var (row, weight) in bucket)
            {
                rows.Add((row, weight));
            }
        }

        return rows;
    }

    // Rank assignment over one version of a partition, a row at a time in order. ROW_NUMBER gives a
    // weight-w row the w positions after the one returned; RANK / DENSE_RANK give every row of a tie
    // group (equal ORDER BY keys, contiguous in the total order) one shared rank. A group starts
    // where a row's ORDER BY keys differ from the previous row's (the caller decides, so the old and
    // new versions can share one comparison when their previous rows are the same).
    private struct Ranker
    {
        private long _rowsBefore;
        private long _dense;
        private long _groupRank;

        public TInRow? Previous;

        public long Next(RankFunction function, TInRow row, long weight, bool startsGroup)
        {
            Previous = row;
            if (function == RankFunction.RowNumber)
            {
                var start = _rowsBefore;
                _rowsBefore += weight;
                return start;
            }

            if (startsGroup)
            {
                _dense++;
                _groupRank = function == RankFunction.DenseRank ? _dense : _rowsBefore + 1;
            }

            _rowsBefore += weight;
            return _groupRank;
        }
    }

    /// <summary>
    /// Emit the change in one partition's widened output: its rows before the tick
    /// (<paramref name="before"/>) against its rows now (<paramref name="after"/>), both in order,
    /// walked together. Rows unchanged by the tick are the same objects in both, so most steps need no
    /// comparison. A row whose rank and weight are unchanged emits nothing and is never widened.
    /// </summary>
    private void EmitDiff(ZSetBuilder<TOutRow, Z64> builder, List<(TInRow Row, long Weight)> before, List<(TInRow Row, long Weight)> after)
    {
        var old = default(Ranker);
        var now = default(Ranker);
        var ranks = _function != RankFunction.RowNumber;
        int i = 0, j = 0;
        while (i < before.Count || j < after.Count)
        {
            int c;
            if (i >= before.Count)
            {
                c = 1;
            }
            else if (j >= after.Count)
            {
                c = -1;
            }
            else
            {
                c = ReferenceEquals(before[i].Row, after[j].Row) ? 0 : _order.Compare(before[i].Row, after[j].Row);
            }

            TInRow row;
            long was = 0, isNow = 0;
            if (c <= 0)
            {
                (row, was) = before[i++];
                if (c == 0)
                {
                    isNow = after[j++].Weight;
                }
            }
            else
            {
                (row, isNow) = after[j++];
            }

            // Tie-group boundaries, one comparison per version unless both versions' previous rows
            // are the same row.
            bool oldStarts = false, newStarts = false;
            if (ranks)
            {
                if (was > 0)
                {
                    oldStarts = old.Previous is null || _sortKeyOnly.Compare(old.Previous, row) != 0;
                }

                if (isNow > 0)
                {
                    newStarts = was > 0 && ReferenceEquals(old.Previous, now.Previous) && old.Previous is not null
                        ? oldStarts
                        : now.Previous is null || _sortKeyOnly.Compare(now.Previous, row) != 0;
                }
            }

            // (Previous is read above before Next moves it on.)
            var oldRank = was > 0 ? old.Next(_function, row, was, oldStarts) : 0;
            var newRank = isNow > 0 ? now.Next(_function, row, isNow, newStarts) : 0;
            if (was == isNow && oldRank == newRank)
            {
                continue;
            }

            if (!ranks)
            {
                // Positions (oldRank, oldRank + was] become (newRank, newRank + isNow]; the overlap stays.
                for (var p = oldRank + 1; p <= oldRank + was; p++)
                {
                    if (p <= newRank || p > newRank + isNow)
                    {
                        builder.Add(_widen(row, p), new Z64(-1));
                    }
                }

                for (var p = newRank + 1; p <= newRank + isNow; p++)
                {
                    if (p <= oldRank || p > oldRank + was)
                    {
                        builder.Add(_widen(row, p), new Z64(1));
                    }
                }

                continue;
            }

            if (was > 0 && isNow > 0 && oldRank == newRank)
            {
                builder.Add(_widen(row, newRank), new Z64(isNow - was));
                continue;
            }

            if (was > 0)
            {
                builder.Add(_widen(row, oldRank), new Z64(-was));
            }

            if (isNow > 0)
            {
                builder.Add(_widen(row, newRank), new Z64(isNow));
            }
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
    // the delta lands. Nothing else is rebuilt: the emitted ranking is its rows' ranking.
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

    // Read every partition still cold (a whole-state write).
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
                "PartitionedRankOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedRank to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // Flatten every partition into one Z-set; the partition key is recovered
        // from each row on load, so it need not be stored separately.
        // Saved in partition order (when the partition hash is stable), so a lazy restore reads a
        // partition as one block. While partitions are still cold, the snapshot holds only those in
        // memory, and FullState reads the rest for a write that needs every row.
        if (_store is not null)
        {
            _store.DropCache();

            // As the window operators (§3.22): nothing is decoded for a checkpoint that writes only
            // the changes since the last; FullState decodes everything for a whole write.
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
            throw new NotSupportedException("PartitionedRankOp was constructed without a snapshot codec.");
        }

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
        _accum.Clear();
        if (_store is not null)
        {
            _store.Clear();
            IntegrateBlocks([.. loaded.Where(kv => kv.Value.Value != 0).Select(kv => (kv.Key, kv.Value.Value))]);
            return;
        }

        // The loaded accumulation is the current state; the ranking it implies is already
        // materialised downstream, and is recomputed from it when a partition is next touched.
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
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
