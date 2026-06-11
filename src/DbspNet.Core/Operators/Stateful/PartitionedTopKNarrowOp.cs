// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Narrow-key variant of <see cref="PartitionedTopKOp{TRow,TKey}"/> for a
/// <b>single-column</b> <c>ORDER BY</c> (docs/design-row-representation.md §22).
/// Functionally identical — same per-partition <c>ROW_NUMBER</c> / <c>RANK</c> /
/// <c>DENSE_RANK</c> &lt;= k window maintained under retraction — but its per-partition
/// <c>_accum</c> / <c>_window</c> state is keyed by a narrow <see cref="OrderRowKey{TRow}"/>
/// (<c>{order value, wide row}</c>) instead of the whole row. The window
/// <see cref="Dictionary{TKey,TValue}"/> therefore hashes on the order value alone
/// (the §22.3 "kill-hash" half of the prize); the whole row is compared only to
/// disambiguate an equal-order tie group (size 1 for q18's unique <c>date_time</c>).
/// The wide row is carried in the key and recovered directly when emitting output —
/// the §22 "wide-row recovery store" with no separate buffer/index.
/// </summary>
/// <remarks>
/// Default-off behind <see cref="PartitionedTopKNarrowingMode"/>; the whole-row
/// <see cref="PartitionedTopKOp{TRow,TKey}"/> is the byte-identical default. The
/// order comparer reuses the operator's full <c>order</c> total order
/// (<see cref="SortKeyComparer{TRow}"/>) as its tiebreak, so the sorted-trace
/// iteration order is identical to the whole-row op and output is value-equivalent.
/// Snapshot format is byte-compatible — the flattened <c>(wideRow, weight)</c> Z-set
/// matches <see cref="PartitionedTopKOp{TRow,TKey}"/>'s.
/// </remarks>
internal sealed class PartitionedTopKNarrowOp<TRow, TKey> : IOperator, ISnapshotable, IIntrospectable
    where TRow : notnull
    where TKey : notnull
{
    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly Func<TRow, TKey> _partitionOf;
    private readonly Func<TRow, object?> _orderOf;
    private readonly bool _orderDescending;
    private readonly bool _orderNullsFirst;
    private readonly RankFunction _function;
    private readonly long _limit;
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    private readonly OrderRowComparer<TRow> _orderComparer;
    private readonly OrderRowEquality<TRow> _windowEquality;

    // Per-partition integrated input, each sorted by the order comparer (order
    // value, then the wide-row tiebreak — identical total order to the whole-row op).
    private readonly Dictionary<TKey, SortedDictionary<OrderRowKey<TRow>, long>> _accum;

    // The window emitted last tick, per partition: narrow key -> in-window weight (> 0).
    private readonly Dictionary<TKey, Dictionary<OrderRowKey<TRow>, long>> _window;

    // Previous tick's output size — pre-sizes this tick's delta builder (§16.8).
    private int _lastOutputSize;

    public PartitionedTopKNarrowOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        Func<TRow, TKey> partitionOf,
        IComparer<TRow> order,
        Func<TRow, object?> orderOf,
        bool orderDescending,
        bool orderNullsFirst,
        RankFunction function,
        long limit,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(orderOf);

        _input = input;
        _output = output;
        _partitionOf = partitionOf;
        _orderOf = orderOf;
        _orderDescending = orderDescending;
        _orderNullsFirst = orderNullsFirst;
        _function = function;
        _limit = limit;
        _snapshotCodec = snapshotCodec;

        // Tiebreak through the operator's full order: when two keys share the order
        // value, `order.Compare(rowA, rowB)` resolves them exactly as the whole-row
        // op's SortedDictionary would, keeping the sorted iteration order identical.
        _orderComparer = new OrderRowComparer<TRow>(orderDescending, orderNullsFirst, order);
        _windowEquality = new OrderRowEquality<TRow>(EqualityComparer<TRow>.Default);
        _accum = new Dictionary<TKey, SortedDictionary<OrderRowKey<TRow>, long>>(partitionComparer);
        _window = new Dictionary<TKey, Dictionary<OrderRowKey<TRow>, long>>(partitionComparer);
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

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;

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
                    bucket = new SortedDictionary<OrderRowKey<TRow>, long>(_orderComparer);
                    _accum[key] = bucket;
                }

                var entry = new OrderRowKey<TRow>(_orderOf(row), row);
                bucket.TryGetValue(entry, out var current);
                var next = current + dw.Value;
                if (next == 0)
                {
                    bucket.Remove(entry);
                    if (bucket.Count == 0)
                    {
                        _accum.Remove(key);
                    }
                }
                else
                {
                    bucket[entry] = next;
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
                    : new Dictionary<OrderRowKey<TRow>, long>(_windowEquality);
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

    /// <summary>Emit the +/- weights moving <paramref name="oldWindow"/> (possibly
    /// null) to <paramref name="newWindow"/>, recovering the wide row from each
    /// narrow key.</summary>
    private static void EmitDiff(
        ZSetBuilder<TRow, Z64> builder,
        Dictionary<OrderRowKey<TRow>, long> newWindow,
        Dictionary<OrderRowKey<TRow>, long>? oldWindow)
    {
        foreach (var (entry, weight) in newWindow)
        {
            var old = 0L;
            oldWindow?.TryGetValue(entry, out old);
            if (weight != old)
            {
                builder.Add(entry.Row, new Z64(weight - old));
            }
        }

        if (oldWindow is not null)
        {
            foreach (var (entry, old) in oldWindow)
            {
                if (!newWindow.ContainsKey(entry))
                {
                    builder.Add(entry.Row, new Z64(-old));
                }
            }
        }
    }

    /// <summary>
    /// The narrow keys of one partition kept by the rank cut, with their in-window
    /// weight. Identical logic to <see cref="PartitionedTopKOp{TRow,TKey}"/>'s
    /// whole-row version, with tie groups detected by equal <i>order value</i>.
    /// </summary>
    private Dictionary<OrderRowKey<TRow>, long> ComputeWindow(SortedDictionary<OrderRowKey<TRow>, long> bucket)
    {
        var result = new Dictionary<OrderRowKey<TRow>, long>(_windowEquality);
        if (_limit <= 0)
        {
            return result;
        }

        if (_function == RankFunction.RowNumber)
        {
            long pos = 0;
            foreach (var (entry, weight) in bucket)
            {
                if (weight <= 0)
                {
                    continue;
                }

                var take = Math.Min(weight, _limit - pos);
                if (take > 0)
                {
                    result[entry] = take;
                }

                pos += weight;
                if (pos >= _limit)
                {
                    break;
                }
            }

            return result;
        }

        // RANK / DENSE_RANK: walk maximal runs of equal-order-value rows and keep or
        // drop each run as a unit (see the whole-row op for the rank arithmetic).
        var dense = _function == RankFunction.DenseRank;
        long rowsBefore = 0;
        long denseRank = 0;
        OrderRowKey<TRow> groupKey = default;
        var inGroup = false;

        foreach (var (entry, weight) in bucket)
        {
            if (weight <= 0)
            {
                continue;
            }

            if (!inGroup || !OrderRowComparer<TRow>.OrderTie(groupKey.Order, entry.Order))
            {
                denseRank++;
                var groupKept = dense ? denseRank <= _limit : rowsBefore < _limit;
                if (!groupKept)
                {
                    break; // ranks only increase — nothing further qualifies.
                }

                groupKey = entry;
                inGroup = true;
            }

            result[entry] = weight;
            rowsBefore += weight;
        }

        return result;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedTopKNarrowOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedTopK to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // Byte-compatible with PartitionedTopKOp: flatten every partition into one
        // wide-row Z-set; the partition key is recovered from each row on load.
        var entries = _accum.SelectMany(p => p.Value.Select(kv => (kv.Key.Row, new Z64(kv.Value))));
        var snapshot = ZSet.FromEntries(entries);
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedTopKNarrowOp was constructed without a snapshot codec.");
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
                bucket = new SortedDictionary<OrderRowKey<TRow>, long>(_orderComparer);
                _accum[key] = bucket;
            }

            bucket[new OrderRowKey<TRow>(_orderOf(row), row)] = weight.Value;
        }

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

/// <summary>
/// A narrow partitioned-TOP-K trace key: a single <c>ORDER BY</c> value plus the
/// wide row it came from (docs/design-row-representation.md §22). A
/// <see langword="readonly"/> <see langword="struct"/> so it incurs no per-row heap
/// allocation — the order value is the already-boxed extractor result (the same box
/// the whole-row comparer produced on every compare), and the row is carried by
/// reference (structural path) or value (typed path).
/// </summary>
internal readonly struct OrderRowKey<TRow>
    where TRow : notnull
{
    public readonly object? Order;
    public readonly TRow Row;

    public OrderRowKey(object? order, TRow row)
    {
        Order = order;
        Row = row;
    }
}

/// <summary>
/// Orders <see cref="OrderRowKey{TRow}"/> by the single <c>ORDER BY</c> value
/// (replicating <see cref="SortKeyComparer{TRow}"/>'s per-key NULL/direction logic),
/// then by <see cref="_tieBreak"/> — the operator's full total order — so the trace
/// iteration order matches the whole-row operator exactly and
/// <c>Compare(x, y) == 0 ⟺ x equals y</c> (required for the <c>SortedDictionary</c>).
/// </summary>
internal sealed class OrderRowComparer<TRow> : IComparer<OrderRowKey<TRow>>
    where TRow : notnull
{
    private readonly bool _descending;
    private readonly bool _nullsFirst;
    private readonly IComparer<TRow> _tieBreak;

    public OrderRowComparer(bool descending, bool nullsFirst, IComparer<TRow> tieBreak)
    {
        _descending = descending;
        _nullsFirst = nullsFirst;
        _tieBreak = tieBreak;
    }

    public int Compare(OrderRowKey<TRow> x, OrderRowKey<TRow> y)
    {
        var a = x.Order;
        var b = y.Order;
        int c;
        if (a is null || b is null)
        {
            // NULL position is absolute — independent of ASC/DESC (mirrors SortKeyComparer).
            if (a is null && b is null)
            {
                c = 0;
            }
            else if (a is null)
            {
                c = _nullsFirst ? -1 : 1;
            }
            else
            {
                c = _nullsFirst ? 1 : -1;
            }
        }
        else
        {
            c = ((IComparable)a).CompareTo(b);
            if (_descending)
            {
                c = -c;
            }
        }

        return c != 0 ? c : _tieBreak.Compare(x.Row, y.Row);
    }

    /// <summary>Whether two order values fall in the same tie group — mirrors the
    /// whole-row op's <c>sortKeyOnly.Compare == 0</c> (CompareTo-based, both-NULL is a
    /// tie, one-NULL is not).</summary>
    public static bool OrderTie(object? a, object? b)
    {
        if (a is null || b is null)
        {
            return a is null && b is null;
        }

        return ((IComparable)a).CompareTo(b) == 0;
    }
}

/// <summary>
/// Equality over <see cref="OrderRowKey{TRow}"/> for the per-partition window
/// <see cref="Dictionary{TKey,TValue}"/>: hashes on the order value alone (the §22.3
/// "kill-hash"), and on a hash collision compares the order value (by-value) and then
/// the wide row through <paramref name="rowEquality"/> — the same whole-row equality
/// the whole-row operator's window dictionary used, but reached only within an
/// equal-order bucket.
/// </summary>
internal sealed class OrderRowEquality<TRow> : IEqualityComparer<OrderRowKey<TRow>>
    where TRow : notnull
{
    private readonly IEqualityComparer<TRow> _rowEquality;

    public OrderRowEquality(IEqualityComparer<TRow> rowEquality)
    {
        _rowEquality = rowEquality;
    }

    public bool Equals(OrderRowKey<TRow> x, OrderRowKey<TRow> y)
        => Equals(x.Order, y.Order) && _rowEquality.Equals(x.Row, y.Row);

    public int GetHashCode(OrderRowKey<TRow> key) => key.Order?.GetHashCode() ?? 0;
}
