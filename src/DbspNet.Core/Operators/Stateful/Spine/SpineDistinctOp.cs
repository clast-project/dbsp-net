// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Spine-backed incremental <c>distinct</c>. Identical observable
/// semantics to <see cref="DistinctOp{TKey,TWeight}"/> — emits +1 the
/// first tick a key's cumulative weight becomes positive, -1 the tick
/// it returns to zero — but holds its trace in an LSM-style
/// <see cref="SpineZSetTrace{TKey,TWeight}"/> rather than a single flat
/// dictionary. Phase 2 of the spine migration: first operator wired
/// through the spine so we can A/B against the flat baseline on the
/// real Step() path.
/// </summary>
/// <remarks>
/// Trade-off vs the flat <see cref="DistinctOp{TKey,TWeight}"/>:
/// <list type="bullet">
///   <item><b>Probe cost</b> (<c>WeightOf</c> per delta-key) climbs from
///   one dict lookup to one lookup per batch — 10–30× slower in
///   isolation per <c>PureTraceBenchmark</c>. Distinct probes
///   <c>O(|delta|)</c> keys per step.</item>
///   <item><b>Integrate cost</b> is O(1) (append a sealed batch) vs
///   O(|delta|) for the flat trace's merge-in-place. Distinct integrates
///   the whole input delta once per step.</item>
/// </list>
/// Whether this is a win in aggregate depends on the trace size and
/// per-step delta size; see <see cref="DistinctBenchmark"/>.
/// </remarks>
/// <remarks>
/// Sibling spine operators wired through the same trace family:
/// <see cref="SpineIncrementalAggregateOp{TKey,TValue,TOut}"/>,
/// <see cref="SpineIncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>,
/// <see cref="SpineIncrementalLeftJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>.
/// </remarks>
internal sealed class SpineDistinctOp<TKey, TWeight> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TKey, TWeight>> _input;
    private readonly Stream<ZSet<TKey, TWeight>> _output;
    private readonly SpineZSetTrace<TKey, TWeight> _trace;
    private readonly IZSetTraceCodec<TKey, TWeight>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public SpineDistinctOp(
        Stream<ZSet<TKey, TWeight>> input,
        Stream<ZSet<TKey, TWeight>> output,
        ICompactionStrategy? compactionStrategy = null,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null,
        IComparer<TKey>? keyComparer = null,
        SpineSpillConfig<TKey, TWeight>? spillConfig = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _input = input;
        _output = output;
        _trace = new SpineZSetTrace<TKey, TWeight>(
            compactionStrategy ?? TieredCompactionStrategy.Default,
            keyComparer,
            spillConfig,
            monotoneKey);
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Distinct keys retained in the trace. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _trace.KeyCount;

    /// <summary>Underlying trace — exposed for GC-shape tests.</summary>
    internal SpineZSetTrace<TKey, TWeight> Trace => _trace;

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineDistinctOp was constructed without a snapshot codec; pass one " +
                "to CircuitBuilder.SpineDistinct to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return SpineSnapshot.SaveAsync(
            writer,
            prefix: "trace",
            batches: _trace.GetBatches(),
            saveOne: (name, batch) => _snapshotCodec.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken);
    }

    public ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineDistinctOp was constructed without a snapshot codec.");
        }

        return SpineSnapshot.LoadAsync(
            reader,
            prefix: "trace",
            loadOne: async name =>
            {
                var batch = await _snapshotCodec.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false);
                _trace.Integrate(batch);
            },
            cancellationToken);
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

        _output.SetCurrent(outputBuilder.Build());
        _trace.Integrate(delta);
        CollectGarbage();
    }

    // Frontier-driven GC mirroring DistinctOp.CollectGarbage: drop sub-frontier
    // rows from the trace (filter-rebuild). Emits nothing. See the flat op for
    // the correctness argument (no future delta resurrects a collected row).
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

    public string MetricName => "SpineDistinct";

    public long RetainedRows => _trace.KeyCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
