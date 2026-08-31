// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental TOP-K (<c>ORDER BY … LIMIT [OFFSET]</c>). Integrates the input
/// Z-set into a multiset kept sorted by a total-order <see cref="IComparer{T}"/>
/// (the <c>ORDER BY</c> keys plus a full-row tiebreak), recomputes the rows
/// occupying sort positions <c>[offset, offset + limit)</c> each tick, and emits
/// the delta against the window it emitted last tick — so rows entering and
/// leaving the window under retraction produce the right +1/−1 weights.
/// </summary>
/// <remarks>
/// <para>Row order is not observable in the output Z-set; this operator only
/// restricts <em>which</em> rows (and with what multiplicity) survive the
/// limit. Multiplicity is honoured: a row with accumulated weight <c>w</c>
/// occupies <c>w</c> consecutive positions, so it can straddle the window edge
/// and contribute a partial weight.</para>
/// <para>Only strictly-positive accumulated weights occupy positions (a TOP-K
/// input is a non-negative multiset). Entries that reach weight 0 are pruned;
/// the rare negative entry is retained so a later positive delta restores the
/// correct count. Retaining the full integrated input is inherent to
/// incremental TOP-K under retraction — when the current top row is retracted,
/// the next row must already be known.</para>
/// </remarks>
internal sealed class TopKOp<TRow> : IOperator, ISnapshotable, IIntrospectable
    where TRow : notnull
{
    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly long _offset;
    private readonly long? _limit;
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    // The full integrated input, sorted by the total-order comparer.
    private readonly SortedDictionary<TRow, long> _accum;

    // The window emitted last tick: row -> in-window weight (every value > 0).
    private Dictionary<TRow, long> _window = new();

    public TopKOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        IComparer<TRow> comparer,
        long offset,
        long? limit,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(comparer);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (limit is { } lim)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(lim);
        }

        _input = input;
        _output = output;
        _offset = offset;
        _limit = limit;
        _snapshotCodec = snapshotCodec;
        _accum = new SortedDictionary<TRow, long>(comparer);
    }

    public string MetricName => "TopK";

    public long RetainedRows => _accum.Count;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;
        if (!delta.IsEmpty)
        {
            foreach (var (row, dw) in delta)
            {
                _accum.TryGetValue(row, out var current);
                var next = current + dw.Value;
                if (next == 0)
                {
                    _accum.Remove(row);
                }
                else
                {
                    _accum[row] = next;
                }
            }
        }

        var newWindow = ComputeWindow();
        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, weight) in newWindow)
        {
            _window.TryGetValue(row, out var old);
            if (weight != old)
            {
                builder.Add(row, new Z64(weight - old));
            }
        }

        foreach (var (row, old) in _window)
        {
            if (!newWindow.ContainsKey(row))
            {
                builder.Add(row, new Z64(-old));
            }
        }

        _output.SetCurrent(builder.Build());
        _window = newWindow;
    }

    /// <summary>
    /// The rows occupying sort positions <c>[offset, offset + limit)</c> with
    /// their in-window weight. A row spanning positions <c>[start, start + w)</c>
    /// contributes the length of that span's overlap with the window.
    /// </summary>
    private Dictionary<TRow, long> ComputeWindow()
    {
        var result = new Dictionary<TRow, long>();
        if (_limit is 0)
        {
            return result;
        }

        var windowEnd = _limit is { } lim ? _offset + lim : long.MaxValue;
        long pos = 0;
        foreach (var (row, weight) in _accum)
        {
            if (weight <= 0)
            {
                continue;
            }

            var start = pos;
            var end = pos + weight;
            pos = end;

            var lo = Math.Max(start, _offset);
            var hi = Math.Min(end, windowEnd);
            if (hi > lo)
            {
                result[row] = hi - lo;
            }

            if (pos >= windowEnd)
            {
                break;
            }
        }

        return result;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "TopKOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.TopK to enable Snapshot.WriteAsync/ReadAsync.");
        }

        var snapshot = ZSet.FromEntries(_accum.Select(kv => (kv.Key, new Z64(kv.Value))));
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("TopKOp was constructed without a snapshot codec.");
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _accum.Clear();
        foreach (var (row, weight) in loaded)
        {
            if (weight.Value != 0)
            {
                _accum[row] = weight.Value;
            }
        }

        // The loaded accumulation is the current state; the window it implies is
        // already materialised downstream, so record it without emitting.
        _window = ComputeWindow();
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
