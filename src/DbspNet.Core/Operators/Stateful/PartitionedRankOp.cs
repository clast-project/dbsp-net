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
/// <para>Because a future row with a smaller ORDER BY value re-ranks everyone, no
/// row is ever finalizable: <see cref="GcFrontier"/> is <c>null</c> and the whole
/// integrated input is retained per partition (inherent to positional ranking
/// under retraction, as in <see cref="PartitionedTopKOp{TRow,TKey}"/>).</para>
/// </remarks>
/// <typeparam name="TInRow">The input (base) row type.</typeparam>
/// <typeparam name="TOutRow">The widened output row type (base ⧺ rank column).</typeparam>
/// <typeparam name="TKey">The PARTITION BY key type.</typeparam>
internal sealed class PartitionedRankOp<TInRow, TOutRow, TKey> : IOperator, ISnapshotable, IIntrospectable
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

    // Per-partition integrated input, each sorted by the total-order comparer.
    private readonly Dictionary<TKey, SortedDictionary<TInRow, long>> _accum;

    // The widened output emitted last tick, per partition: widened row -> weight
    // (> 0). Keyed by the *widened* row (rank included) so ROW_NUMBER's one-row-per-
    // position expansion and RANK/DENSE_RANK's shared-rank groups both diff cleanly.
    private readonly Dictionary<TKey, Dictionary<TOutRow, long>> _window;

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
        IZSetTraceCodec<TInRow, Z64>? snapshotCodec = null)
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
        _accum = new Dictionary<TKey, SortedDictionary<TInRow, long>>(partitionComparer);
        _window = new Dictionary<TKey, Dictionary<TOutRow, long>>(partitionComparer);
    }

    public string MetricName => "PartitionedRank";

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

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;

        // Only the partitions a tick touches can change their ranking; collect them
        // so we recompute (and re-diff) just those.
        HashSet<TKey>? touched = null;
        if (!delta.IsEmpty)
        {
            touched = new HashSet<TKey>(_accum.Comparer);
            foreach (var (row, dw) in delta)
            {
                var key = _partitionOf(row);
                touched.Add(key);
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
        if (touched is not null)
        {
            foreach (var key in touched)
            {
                var newWindow = _accum.TryGetValue(key, out var bucket)
                    ? ComputeWindow(bucket)
                    : new Dictionary<TOutRow, long>();
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

    /// <summary>Emit the +/- weights moving the previously-emitted widened output
    /// for a partition (<paramref name="oldWindow"/>, possibly null) to
    /// <paramref name="newWindow"/>.</summary>
    private static void EmitDiff(
        ZSetBuilder<TOutRow, Z64> builder,
        Dictionary<TOutRow, long> newWindow,
        Dictionary<TOutRow, long>? oldWindow)
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
    /// The widened output rows of one partition — every base row re-emitted with
    /// its rank appended, keyed by the widened row. <c>ROW_NUMBER</c> gives a
    /// weight-<c>w</c> base row <c>w</c> distinct consecutive ranks (each output
    /// row weight 1); <c>RANK</c> / <c>DENSE_RANK</c> assign the whole tie group a
    /// single shared rank (each output row keeps its full weight).
    /// </summary>
    private Dictionary<TOutRow, long> ComputeWindow(SortedDictionary<TInRow, long> bucket)
    {
        var result = new Dictionary<TOutRow, long>();

        if (_function == RankFunction.RowNumber)
        {
            long pos = 0;
            foreach (var (row, weight) in bucket)
            {
                if (weight <= 0)
                {
                    continue;
                }

                // Each of the `weight` copies takes the next sequential position.
                for (long i = 0; i < weight; i++)
                {
                    result[_widen(row, pos + i + 1)] = 1;
                }

                pos += weight;
            }

            return result;
        }

        // RANK / DENSE_RANK: walk maximal runs of equal-ORDER-BY-key rows and give
        // each run one shared rank. `rowsBefore` counts row multiplicity (RANK's
        // gap: 1 + rows strictly before the group); `denseRank` counts distinct key
        // groups (DENSE_RANK: 1, 2, 3 with no gaps).
        var dense = _function == RankFunction.DenseRank;
        long rowsBefore = 0;
        long denseRank = 0;
        long groupRank = 0;
        TInRow? groupKey = default;
        var inGroup = false;

        foreach (var (row, weight) in bucket)
        {
            if (weight <= 0)
            {
                continue;
            }

            if (!inGroup || _sortKeyOnly.Compare(groupKey!, row) != 0)
            {
                // Starting a new tie group. Its rank is decided up front and shared
                // by every row in the group.
                denseRank++;
                groupRank = dense ? denseRank : rowsBefore + 1;
                groupKey = row;
                inGroup = true;
            }

            result[_widen(row, groupRank)] = weight;
            rowsBefore += weight;
        }

        return result;
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
        var entries = _accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value))));
        var snapshot = ZSet.FromEntries(entries);
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedRankOp was constructed without a snapshot codec.");
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

        // The loaded accumulation is the current state; the widened outputs it
        // implies are already materialised downstream, so record them without
        // emitting.
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
