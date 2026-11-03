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
    private readonly IRowTrace<TKey, TWeight> _trace;
    private readonly IZSetTraceCodec<TKey, TWeight>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    // Last tick's output count, used to pre-size this tick's delta builder.
    // Only presence flips emit, so this is far tighter than the delta count
    // (docs/design-row-representation.md §16.8 chose last-output for exactly
    // this selective shape).
    private int _lastOutputSize;

    public DistinctOp(
        Stream<ZSet<TKey, TWeight>> input,
        Stream<ZSet<TKey, TWeight>> output,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null,
        IRowTrace<TKey, TWeight>? trace = null)
    {
        _trace = trace ?? new ManagedRowTrace<TKey, TWeight>();
        _input = input;
        _output = output;
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Distinct keys retained in the trace. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _trace.Count;

    /// <summary>Whether the trace is held as blocks (DBSPNET_TRACE_BLOCKS).</summary>
    internal bool IsBlocked => _trace is Blocks.BlockedZSetTrace;

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "DistinctOp was constructed without a snapshot codec; pass one " +
                "to CircuitBuilder.Distinct to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return _trace.SaveAsync(_snapshotCodec, writer, "trace.arrows", cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "DistinctOp was constructed without a snapshot codec.");
        }

        // Lazily (increment 6): a row-ordered chain stays cold; Step reads the rows deltas touch.
        await _trace.LoadAsync(_snapshotCodec, reader, "trace.arrows", lazy: true, cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _trace.Idle();
            _output.SetCurrent(ZSet<TKey, TWeight>.Empty);
            CollectGarbage();
            return;
        }

        var outputBuilder = new ZSetBuilder<TKey, TWeight>(_lastOutputSize);
        foreach (var (key, dw) in delta)
        {
            var before = _trace.WeightOf(key);
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

        var result = outputBuilder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
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

    public long RetainedRows => _trace.RetainedRows;

    public long LastOutputRows => (_output.Current?.Count ?? 0);

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
