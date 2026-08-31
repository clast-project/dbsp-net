// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental <c>distinct</c>: receives a stream of Z-set deltas and emits a
/// stream of Z-set deltas such that the cumulative output for every key is
/// either <c>One</c> (if the cumulative input weight is strictly positive)
/// or <c>Zero</c>. A row is emitted with weight +1 the first tick it becomes
/// present, and -1 the tick it becomes absent — exactly mirroring SQL
/// <c>DISTINCT</c> semantics under retractions.
/// </summary>
internal sealed class DistinctOp<TKey, TWeight> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TKey, TWeight>> _input;
    private readonly Stream<ZSet<TKey, TWeight>> _output;
    private readonly ZSetTrace<TKey, TWeight> _trace = new();
    private readonly IZSetTraceCodec<TKey, TWeight>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public DistinctOp(
        Stream<ZSet<TKey, TWeight>> input,
        Stream<ZSet<TKey, TWeight>> output,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _input = input;
        _output = output;
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Distinct keys retained in the trace. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _trace.Current.Count;

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "DistinctOp was constructed without a snapshot codec; pass one " +
                "to CircuitBuilder.Distinct to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return _snapshotCodec.SaveAsync(writer, "trace.arrows", _trace.Current, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "DistinctOp was constructed without a snapshot codec.");
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _trace.Integrate(loaded);
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _output.SetCurrent(ZSet<TKey, TWeight>.Empty);
            CollectGarbage();
            return;
        }

        var outputBuilder = new ZSetBuilder<TKey, TWeight>();
        foreach (var (key, dw) in delta)
        {
            var before = _trace.Current.WeightOf(key);
            var after = TWeight.Add(before, dw);

            var wasPresent = TWeight.IsPositive(before);
            var isPresent = TWeight.IsPositive(after);

            if (!wasPresent && isPresent)
            {
                outputBuilder.Add(key, TWeight.One);
            }
            else if (wasPresent && !isPresent)
            {
                outputBuilder.Add(key, TWeight.Negate(TWeight.One));
            }
        }

        _output.SetCurrent(outputBuilder.Build());
        _trace.Integrate(delta);
        CollectGarbage();
    }

    // Frontier-driven GC: when the row carries a monotone column (so no future
    // delta arrives below the frontier — the input late-drop enforces this), drop
    // sub-frontier rows from the trace. Emits nothing; the already-emitted +1 for
    // a collected row is final, and no future delta can resurrect it (which would
    // otherwise re-emit a spurious +1, since the trace would read before=0).
    private void CollectGarbage()
    {
        if (_frontier is null || _monotoneKey is null)
        {
            return;
        }

        var frontier = _frontier.Value;
        if (frontier == long.MinValue || frontier <= _lastGcFrontier)
        {
            return;
        }

        _lastGcFrontier = frontier;
        _gcDropped += _trace.DropKeysBelow(frontier, _monotoneKey);
    }

    public string MetricName => "Distinct";

    public long RetainedRows => _trace.Current.Count;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
