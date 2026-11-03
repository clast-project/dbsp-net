// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Which SQL ranking window function a <see cref="PartitionedTopKOp{TRow,TKey}"/>
/// emulates. All three keep the rows whose rank is <c>&lt;= limit</c> within each
/// partition; they differ only in how ties (rows with equal <c>ORDER BY</c> keys)
/// are counted.
/// </summary>
public enum RankFunction
{
    /// <summary><c>ROW_NUMBER()</c>: every row gets a distinct position
    /// (1, 2, 3, 4 …) — ties are broken by the full-row order, so the cut at
    /// <c>limit</c> can split a tie group.</summary>
    RowNumber,

    /// <summary><c>RANK()</c>: tied rows share the smaller rank and the next
    /// distinct key skips (1, 1, 3, 4 …). A tie group is kept in full iff its
    /// first position is <c>&lt;= limit</c>.</summary>
    Rank,

    /// <summary><c>DENSE_RANK()</c>: tied rows share a rank with no gaps
    /// (1, 1, 2, 3 …). The first <c>limit</c> distinct key groups are kept in
    /// full.</summary>
    DenseRank,
}

/// <summary>
/// Incremental partitioned TOP-K — the <c>ROW_NUMBER</c> / <c>RANK</c> /
/// <c>DENSE_RANK</c> window functions restricted to the filter pattern
/// <c>… OVER (PARTITION BY p ORDER BY o) &lt;= k</c>. Each partition keeps its
/// own integrated multiset, sorted by a total-order <paramref name="order"/>
/// comparer, and emits the per-partition rows whose rank is <c>&lt;= limit</c>,
/// maintained as rows enter and leave that window under retraction.
/// </summary>
/// <remarks>
/// <para>This is the per-partition generalisation of <see cref="TopKOp{TRow}"/>:
/// rather than one global window it keeps one window per <typeparamref name="TKey"/>
/// partition and only recomputes the partitions a tick actually touches.</para>
/// <para>The rank value itself is never materialised — it exists only to drive
/// the cut at <c>limit</c>, so the output schema equals the input schema (rows
/// are filtered, never widened). <c>RANK</c> / <c>DENSE_RANK</c> detect tie
/// groups through <paramref name="sortKeyOnly"/>, a comparer that orders by the
/// <c>ORDER BY</c> keys alone (<see cref="ConstantZeroComparer{T}"/> tiebreak),
/// so equal-key rows — which are contiguous because <paramref name="order"/>
/// refines <paramref name="sortKeyOnly"/> — are kept or dropped together.</para>
/// <para>Multiplicity is honoured: a row with accumulated weight <c>w</c>
/// occupies <c>w</c> consecutive positions. As in <see cref="TopKOp{TRow}"/>,
/// retaining the full integrated input per partition is inherent to incremental
/// TOP-K under retraction — when a windowed row is retracted, the next row must
/// already be known.</para>
/// </remarks>
internal sealed class PartitionedTopKOp<TRow, TKey> : IOperator, ISnapshotable, IIntrospectable, ISnapshotOrder
    where TRow : notnull
    where TKey : notnull
{
    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly Func<TRow, TKey> _partitionOf;
    private readonly IComparer<TRow> _order;
    private readonly IComparer<TRow> _sortKeyOnly;
    private readonly RankFunction _function;
    private readonly long _limit;
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    // Per-partition integrated input, each sorted by the total-order comparer.
    private readonly Dictionary<TKey, SortedDictionary<TRow, long>> _accum;

    // Incremental checkpoints (docs/design-durable-state.md §1f, increment 3): the snapshot is
    // the integral of the input, so the change since the last checkpoint is the inputs since.
    private readonly ZSetChangeTracker<TRow, Z64> _changes = new();

    // Lazy restore (docs/design-durable-state.md §1f, increment 5): partitions still cold, read the
    // first time a delta touches them; null when everything is in memory.
    private LazyPartitions<TKey, TRow>? _lazy;

    /// <inheritdoc/>
    public object? OrderFor(string fileName) =>
        fileName == "trace.arrows" ? LazyPartitions<TKey, TRow>.PartitionOrder(_partitionOf) : null;

    // The window emitted last tick, per partition: row -> in-window weight (> 0).
    private readonly Dictionary<TKey, Dictionary<TRow, long>> _window;

    // Previous tick's output size — pre-sizes this tick's delta builder to avoid
    // dictionary-resize churn (fresh alloc, no reuse; §16.7). Perf hint only.
    private int _lastOutputSize;

    public PartitionedTopKOp(
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
        _accum = new Dictionary<TKey, SortedDictionary<TRow, long>>(partitionComparer);
        _window = new Dictionary<TKey, Dictionary<TRow, long>>(partitionComparer);
    }

    public string MetricName => "PartitionedTopK";

    public long RetainedRows
    {
        get
        {
            long n = 0;
            foreach (var bucket in _accum.Values)
            {
                n += bucket.Count;
            }

            return n;
        }
    }

    public long LastOutputRows => (_output.Current?.Count ?? 0);

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;
        _changes.Record(delta);

        // Only the partitions a tick touches can change their window; collect
        // them so we recompute (and re-diff) just those.
        HashSet<TKey>? touched = null;
        if (!delta.IsEmpty)
        {
            touched = new HashSet<TKey>(_accum.Comparer);
            foreach (var (row, dw) in delta)
            {
                var key = _partitionOf(row);
                if (_lazy is not null)
                {
                    Fault(key);
                }

                touched.Add(key);
                if (!_accum.TryGetValue(key, out var bucket))
                {
                    bucket = new SortedDictionary<TRow, long>(_order);
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

        var builder = new ZSetBuilder<TRow, Z64>(_lastOutputSize);
        if (touched is not null)
        {
            foreach (var key in touched)
            {
                var newWindow = _accum.TryGetValue(key, out var bucket)
                    ? ComputeWindow(bucket)
                    : new Dictionary<TRow, long>();
                _window.TryGetValue(key, out var oldWindow);
                EmitDiff(builder, newWindow, oldWindow);

                if (newWindow.Count == 0)
                {
                    _window.Remove(key);
                }
                else
                {
                    _window[key] = newWindow;
                }
            }
        }

        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
    }

    /// <summary>Emit the +/- weights moving the previously-emitted window for a
    /// partition (<paramref name="oldWindow"/>, possibly null) to
    /// <paramref name="newWindow"/>.</summary>
    private static void EmitDiff(
        ZSetBuilder<TRow, Z64> builder,
        Dictionary<TRow, long> newWindow,
        Dictionary<TRow, long>? oldWindow)
    {
        foreach (var (row, weight) in newWindow)
        {
            var old = 0L;
            oldWindow?.TryGetValue(row, out old);
            if (weight != old)
            {
                builder.Add(row, new Z64(weight - old));
            }
        }

        if (oldWindow is not null)
        {
            foreach (var (row, old) in oldWindow)
            {
                if (!newWindow.ContainsKey(row))
                {
                    builder.Add(row, new Z64(-old));
                }
            }
        }
    }

    /// <summary>
    /// The rows of one partition kept by the rank cut, with their in-window
    /// weight. <c>ROW_NUMBER</c> keeps the first <c>limit</c> positions
    /// (multiplicity-counted, so the cut can split a tie group);
    /// <c>RANK</c> / <c>DENSE_RANK</c> keep whole tie groups.
    /// </summary>
    private Dictionary<TRow, long> ComputeWindow(SortedDictionary<TRow, long> bucket)
    {
        var result = new Dictionary<TRow, long>();
        if (_limit <= 0)
        {
            return result;
        }

        if (_function == RankFunction.RowNumber)
        {
            long pos = 0;
            foreach (var (row, weight) in bucket)
            {
                if (weight <= 0)
                {
                    continue;
                }

                var take = Math.Min(weight, _limit - pos);
                if (take > 0)
                {
                    result[row] = take;
                }

                pos += weight;
                if (pos >= _limit)
                {
                    break;
                }
            }

            return result;
        }

        // RANK / DENSE_RANK: walk maximal runs of equal-ORDER-BY-key rows and
        // keep or drop each run as a unit. `rowsBefore` counts row multiplicity
        // (RANK's gap); `denseRank` counts distinct key groups (DENSE_RANK).
        var dense = _function == RankFunction.DenseRank;
        long rowsBefore = 0;
        long denseRank = 0;
        TRow? groupKey = default;
        var inGroup = false;
        var groupKept = false;

        foreach (var (row, weight) in bucket)
        {
            if (weight <= 0)
            {
                continue;
            }

            if (!inGroup || _sortKeyOnly.Compare(groupKey!, row) != 0)
            {
                // Starting a new tie group. Its rank is decided up front.
                denseRank++;
                groupKept = dense ? denseRank <= _limit : rowsBefore < _limit;
                if (!groupKept)
                {
                    break; // ranks only increase — nothing further qualifies.
                }

                groupKey = row;
                inGroup = true;
            }

            result[row] = weight;
            rowsBefore += weight;
        }

        return result;
    }

    // A fresh object even when empty: the shared ZSet.Empty must never carry checkpoint tracking.
    private ZSet<TRow, Z64> SnapshotNow() =>
        _accum.Count == 0
            ? ZSet<TRow, Z64>.NewMutable()
            : ZSet.FromEntries(_accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value)))));

    // A partition a lazily restored operator has not touched yet: read its rows from cold and
    // rebuild its window — what the eager load does for every partition — before the delta lands.
    private void Fault(TKey key)
    {
        if (_lazy!.Take(key) is not { Count: > 0 } rows)
        {
            return;
        }

        var bucket = new SortedDictionary<TRow, long>(_order);
        foreach (var (row, weight) in rows)
        {
            bucket[row] = weight;
        }

        _accum[key] = bucket;
        InitWindow(key, bucket);
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
                bucket = new SortedDictionary<TRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight;
            filled.Add(key);
        }

        _lazy = null;
        foreach (var key in filled)
        {
            InitWindow(key, _accum[key]);
        }
    }

    // Record a partition's window as already materialised downstream, emitting nothing.
    private void InitWindow(TKey key, SortedDictionary<TRow, long> bucket)
    {
        var window = ComputeWindow(bucket);
        if (window.Count > 0)
        {
            _window[key] = window;
        }
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedTopKOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedTopK to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // Flatten every partition into one Z-set; the partition key is recovered
        // from each row on load, so it need not be stored separately.
        // Saved in partition order (when the partition hash is stable), so a lazy restore reads a
        // partition as one block. While partitions are still cold, the snapshot holds only those in
        // memory, and FullState reads the rest for a write that needs every row.
        var snapshot = SnapshotNow();
        _changes.Attach(
            snapshot,
            LazyPartitions<TKey, TRow>.PartitionOrder(_partitionOf),
            _lazy is null ? null : () => { PromoteAll(); return SnapshotNow(); },
            _lazy is null ? null : snapshot.Count + _lazy.ColdCount);
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedTopKOp was constructed without a snapshot codec.");
        }

        if (_snapshotCodec is IColdRowLoader<TRow> coldLoader
            && LazyPartitions<TKey, TRow>.PartitionOrder(_partitionOf) is not null
            && await coldLoader.LoadColdAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false) is { } cold)
        {
            // Lazily restored: partitions are read as deltas touch them (Fault).
            _accum.Clear();
            _window.Clear();
            _lazy = new LazyPartitions<TKey, TRow>(cold, _partitionOf, _accum.Comparer);
            _changes.Restored(cold, cold.Count);
            return;
        }

        _lazy = null;
        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _changes.Restored(loaded);
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
                bucket = new SortedDictionary<TRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight.Value;
        }

        // The loaded accumulation is the current state; the windows it implies
        // are already materialised downstream, so record them without emitting.
        foreach (var (key, bucket) in _accum)
        {
            var window = ComputeWindow(bucket);
            if (window.Count > 0)
            {
                _window[key] = window;
            }
        }
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
