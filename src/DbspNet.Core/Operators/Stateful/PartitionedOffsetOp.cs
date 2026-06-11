// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>Which positional row a <see cref="OffsetSpec"/> reads from.</summary>
public enum OffsetKind
{
    /// <summary><c>LAG</c>: the row <c>Offset</c> positions before the current one.</summary>
    Lag,

    /// <summary><c>LEAD</c>: the row <c>Offset</c> positions after the current one.</summary>
    Lead,

    /// <summary><c>FIRST_VALUE</c>: the first row of the partition (UNLIMITED RANGE).</summary>
    FirstValue,

    /// <summary><c>LAST_VALUE</c>: the last row of the partition (UNLIMITED RANGE).</summary>
    LastValue,
}

/// <summary>
/// One <c>LAG</c> / <c>LEAD</c> / <c>FIRST_VALUE</c> / <c>LAST_VALUE</c> output
/// column: read <see cref="Value"/> from the row (of type
/// <typeparamref name="TRow"/>) selected by <see cref="Kind"/> (using
/// <see cref="Offset"/> for the relative LAG/LEAD kinds), falling back to
/// <see cref="Default"/> when that row is outside the partition.
/// </summary>
public readonly record struct OffsetSpec<TRow>(
    Func<TRow, object?> Value,
    OffsetKind Kind,
    long Offset,
    object? Default);

/// <summary>
/// Incremental partitioned <c>LAG</c> / <c>LEAD</c> — positional offset functions
/// emitted as new column(s) appended to every input row. The per-partition
/// generalisation of an ordered offset lookup, maintained by recompute-and-diff
/// like <see cref="PartitionedTopKOp{TRow,TKey}"/>.
/// </summary>
/// <remarks>
/// <para>Each partition keeps its integrated rows sorted by a total order
/// (<paramref name="order"/> = ORDER BY keys then a full-row tiebreak). A
/// weight-<c>w</c> row occupies <c>w</c> consecutive positions (bag semantics);
/// for each position the operator reads each spec's value from the position
/// <see cref="OffsetSpec.Offset"/> away. The whole touched partition is recomputed
/// each tick and the widened-row multiset diffed against the last emitted one —
/// inserting one row shifts only the O(offset) neighbours' results, but the
/// recompute is currently whole-partition (a neighbourhood-only recompute and
/// frontier GC are deferred).</para>
/// <para>Row-opaque: the only row-shape-specific seam is the supplied
/// <c>widen</c> delegate, which fuses the base row with the per-spec offset values
/// into the widened output row — letting the same operator run over structural or
/// typed rows.</para>
/// </remarks>
/// <typeparam name="TInRow">The input (base) row type.</typeparam>
/// <typeparam name="TOutRow">The widened output row type (base ⧺ offset columns).</typeparam>
/// <typeparam name="TKey">The PARTITION BY key type.</typeparam>
internal sealed class PartitionedOffsetOp<TInRow, TOutRow, TKey> : IOperator, ISnapshotable, IIntrospectable
    where TInRow : notnull
    where TOutRow : notnull
    where TKey : notnull
{
    private readonly Stream<ZSet<TInRow, Z64>> _input;
    private readonly Stream<ZSet<TOutRow, Z64>> _output;
    private readonly Func<TInRow, TKey> _partitionOf;
    private readonly IComparer<TInRow> _order;
    private readonly OffsetSpec<TInRow>[] _specs;
    private readonly Func<TInRow, object?[], TOutRow> _widen;
    private readonly IZSetTraceCodec<TInRow, Z64>? _snapshotCodec;

    private readonly Dictionary<TKey, SortedDictionary<TInRow, long>> _accum;
    private readonly Dictionary<TKey, Dictionary<TOutRow, long>> _window;

    public PartitionedOffsetOp(
        Stream<ZSet<TInRow, Z64>> input,
        Stream<ZSet<TOutRow, Z64>> output,
        Func<TInRow, TKey> partitionOf,
        IComparer<TInRow> order,
        OffsetSpec<TInRow>[] specs,
        Func<TInRow, object?[], TOutRow> widen,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TInRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(specs);
        ArgumentNullException.ThrowIfNull(widen);

        _input = input;
        _output = output;
        _partitionOf = partitionOf;
        _order = order;
        _specs = specs;
        _widen = widen;
        _snapshotCodec = snapshotCodec;
        _accum = new Dictionary<TKey, SortedDictionary<TInRow, long>>(partitionComparer);
        _window = new Dictionary<TKey, Dictionary<TOutRow, long>>(partitionComparer);
    }

    public string MetricName => "WindowOffset";

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

        var builder = new ZSetBuilder<TOutRow, Z64>();
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

        _output.SetCurrent(builder.Build());
    }

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

    /// <summary>Recompute one partition's widened output multiset: expand the
    /// sorted rows into positional slots (weight-aware), then for each slot read
    /// each spec's value from its offset slot.</summary>
    private Dictionary<TOutRow, long> ComputeWindow(SortedDictionary<TInRow, long> bucket)
    {
        // Positional slots in total order; a weight-w row fills w consecutive
        // slots. Negative-weight rows (an invalid multiset state) contribute none.
        var slots = new List<TInRow>();
        foreach (var (row, weight) in bucket)
        {
            for (var c = 0L; c < weight; c++)
            {
                slots.Add(row);
            }
        }

        var result = new Dictionary<TOutRow, long>();
        for (var j = 0; j < slots.Count; j++)
        {
            var baseRow = slots[j];
            var vs = new object?[_specs.Length];
            for (var s = 0; s < _specs.Length; s++)
            {
                var spec = _specs[s];
                var src = spec.Kind switch
                {
                    OffsetKind.Lag => j - spec.Offset,
                    OffsetKind.Lead => j + spec.Offset,
                    OffsetKind.FirstValue => 0,
                    OffsetKind.LastValue => slots.Count - 1,
                    _ => j,
                };
                vs[s] = src >= 0 && src < slots.Count ? spec.Value(slots[(int)src]) : spec.Default;
            }

            var widened = _widen(baseRow, vs);
            result[widened] = result.GetValueOrDefault(widened) + 1;
        }

        return result;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedOffsetOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedOffset to enable Snapshot.WriteAsync/ReadAsync.");
        }

        var entries = _accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value))));
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", ZSet.FromEntries(entries), cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedOffsetOp was constructed without a snapshot codec.");
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
