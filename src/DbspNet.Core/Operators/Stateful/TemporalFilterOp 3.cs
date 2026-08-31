// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The time-driven temporal filter (the <c>mz_now()</c>-style advancing-clock
/// predicate). Integrates the input into a multiset and, each tick, emits the
/// set of rows currently valid under an advancing logical clock — the delta
/// against the set it emitted last tick. Validity is a per-row window affine in
/// the row's time-key <c>e</c>: a row is valid at logical time <c>now</c> iff
/// <c>now {&gt;|&gt;=} e + appearOffset</c> (lower bound) and
/// <c>now {&lt;|&lt;=} e + disappearOffset</c> (upper bound).
/// </summary>
/// <remarks>
/// <para>Unlike a stateless filter, validity changes <i>as the clock advances,
/// with no new input</i>: recomputing the valid set against the current clock
/// each tick and diffing it against the previously-emitted set yields exactly
/// the inserts (rows aging in) and retractions (rows aging out) — the same
/// recompute-and-diff structure as <see cref="TopKOp{TRow}"/>. The clock is
/// read from a monotone <see cref="IFrontier"/> the host advances before each
/// <see cref="RootCircuit.Step"/>.</para>
/// <para><b>State GC.</b> Once the clock passes a row's upper bound the row can
/// never be valid again (the clock only moves forward), so it is dropped from
/// the integral after it has been retracted. Rows with no upper bound are
/// retained — a filter like <c>e &lt;= NOW()</c> keeps everything that has ever
/// appeared, which is inherent to its semantics.</para>
/// <para>The per-tick recompute is O(integral size); a time-ordered transition
/// index (only touch rows that crossed a bound) is a deferred optimisation.</para>
/// </remarks>
internal sealed class TemporalFilterOp<TRow> : IOperator, ISnapshotable, IIntrospectable
    where TRow : notnull
{
    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly Func<TRow, long?> _timeKey;
    private readonly long? _appearOffset;
    private readonly bool _appearInclusive;
    private readonly long? _disappearOffset;
    private readonly bool _disappearInclusive;
    private readonly IFrontier _clock;
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    // The integrated input: row -> accumulated weight (entries kept even when
    // currently invalid, since the clock may later bring them into the window;
    // pruned only once permanently past their upper bound).
    private readonly Dictionary<TRow, long> _accum = new();

    // The valid set emitted last tick: row -> weight (every value != 0).
    private Dictionary<TRow, long> _emitted = new();

    // Scratch: rows found permanently expired during the current ComputeValid,
    // removed from _accum after the tick's delta has been emitted.
    private readonly List<TRow> _expired = new();

    public TemporalFilterOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        Func<TRow, long?> timeKey,
        long? appearOffsetMicros,
        bool appearInclusive,
        long? disappearOffsetMicros,
        bool disappearInclusive,
        IFrontier clock,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(timeKey);
        ArgumentNullException.ThrowIfNull(clock);

        _input = input;
        _output = output;
        _timeKey = timeKey;
        _appearOffset = appearOffsetMicros;
        _appearInclusive = appearInclusive;
        _disappearOffset = disappearOffsetMicros;
        _disappearInclusive = disappearInclusive;
        _clock = clock;
        _snapshotCodec = snapshotCodec;
    }

    public string MetricName => "TemporalFilter";

    public long RetainedRows => _accum.Count;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_clock);

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

        var now = _clock.Value;
        var valid = ComputeValid(now);

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, weight) in valid)
        {
            _emitted.TryGetValue(row, out var old);
            if (weight != old)
            {
                builder.Add(row, new Z64(weight - old));
            }
        }

        foreach (var (row, old) in _emitted)
        {
            if (!valid.ContainsKey(row))
            {
                builder.Add(row, new Z64(-old));
            }
        }

        _output.SetCurrent(builder.Build());
        _emitted = valid;

        // GC rows the clock has moved permanently past their upper bound. They
        // are guaranteed absent from `valid` (so already retracted above) and
        // can never re-enter the window.
        if (_expired.Count > 0)
        {
            foreach (var row in _expired)
            {
                _accum.Remove(row);
            }

            _expired.Clear();
        }
    }

    /// <summary>Rows currently valid at <paramref name="now"/>, with their full
    /// (multiplicity-preserving) weight. Also records permanently-expired rows
    /// in <see cref="_expired"/> for post-emit GC.</summary>
    private Dictionary<TRow, long> ComputeValid(long now)
    {
        var result = new Dictionary<TRow, long>();
        foreach (var (row, weight) in _accum)
        {
            if (weight <= 0)
            {
                continue;
            }

            var key = _timeKey(row);
            if (key is null)
            {
                continue; // a NULL time key is never valid
            }

            if (IsValidAt(now, key.Value))
            {
                result[row] = weight;
            }
            else if (IsPermanentlyExpired(now, key.Value))
            {
                _expired.Add(row);
            }
        }

        return result;
    }

    private bool IsValidAt(long now, long key)
    {
        if (_appearOffset is { } a)
        {
            var lower = SaturatingAdd(key, a);
            if (_appearInclusive ? now < lower : now <= lower)
            {
                return false;
            }
        }

        if (_disappearOffset is { } d)
        {
            var upper = SaturatingAdd(key, d);
            if (_disappearInclusive ? now > upper : now >= upper)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>True once the clock has moved past the row's upper bound for
    /// good (so it can be GC'd). Rows with no upper bound never expire.</summary>
    private bool IsPermanentlyExpired(long now, long key)
    {
        if (_disappearOffset is not { } d)
        {
            return false;
        }

        var upper = SaturatingAdd(key, d);
        return _disappearInclusive ? now > upper : now >= upper;
    }

    private static long SaturatingAdd(long a, long b)
    {
        var sum = unchecked(a + b);
        // Overflow iff the operands share a sign that differs from the result's.
        if (((a ^ sum) & (b ^ sum)) < 0)
        {
            return b > 0 ? long.MaxValue : long.MinValue;
        }

        return sum;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "TemporalFilterOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.TemporalFilter to enable Snapshot.WriteAsync/ReadAsync.");
        }

        var snapshot = ZSet.FromEntries(_accum.Select(kv => (kv.Key, new Z64(kv.Value))));
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("TemporalFilterOp was constructed without a snapshot codec.");
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

        // The snapshot is end-of-tick T; the clock has already been restored to
        // its value at T (Snapshot.ReadAsync restores it before loading
        // operators), so the valid set we record here matches what downstream
        // operators were restored with — the next Step emits only the genuine
        // delta, not a re-insert of the whole window.
        _emitted = ComputeValid(_clock.Value);
        _expired.Clear();
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
