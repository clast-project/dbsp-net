// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Spine-backed incremental FULL OUTER equi-join. Observable behaviour matches
/// <see cref="IncrementalFullJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>; only the
/// storage of the two integrated traces changes.
/// </summary>
/// <remarks>
/// The per-key analysis — left-join cases (keyed on right-presence) plus the
/// symmetric right-pad cases (keyed on left-presence) — is identical to the flat
/// operator and only touches keys that appear in <c>dl</c> or <c>dr</c>.
/// </remarks>
internal sealed class SpineIncrementalFullJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightIn;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _joinCombine;
    private readonly Func<TKey, TLeft, TOut> _nullPadRightCombine;
    private readonly Func<TKey, TRight, TOut> _nullPadLeftCombine;
    private readonly SpineIndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace;
    private readonly SpineIndexedZSetTrace<TKey, TRight, TWeight> _rightTrace;
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public SpineIncrementalFullJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadRightCombine,
        Func<TKey, TRight, TOut> nullPadLeftCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TLeft>? leftValueComparer = null,
        IComparer<TRight>? rightValueComparer = null,
        SpineIndexedSpillConfig<TKey, TLeft, TWeight>? leftSpillConfig = null,
        SpineIndexedSpillConfig<TKey, TRight, TWeight>? rightSpillConfig = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _joinCombine = joinCombine;
        _nullPadRightCombine = nullPadRightCombine;
        _nullPadLeftCombine = nullPadLeftCombine;
        var strategy = compactionStrategy ?? TieredCompactionStrategy.Default;
        _leftTrace = new SpineIndexedZSetTrace<TKey, TLeft, TWeight>(strategy, keyComparer, leftValueComparer, leftSpillConfig, monotoneKey);
        _rightTrace = new SpineIndexedZSetTrace<TKey, TRight, TWeight>(strategy, keyComparer, rightValueComparer, rightSpillConfig, monotoneKey);
        _leftSnapshotCodec = leftSnapshotCodec;
        _rightSnapshotCodec = rightSnapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>Total keys retained across both traces. Exposed for GC-bound tests.</summary>
    internal int RetainedKeyCount => _leftTrace.GroupCount + _rightTrace.GroupCount;

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalFullJoinOp was constructed without snapshot codecs.");
        }

        await SpineSnapshot.SaveAsync(
            writer, prefix: "left",
            batches: _leftTrace.GetBatches(),
            saveOne: (name, batch) => _leftSnapshotCodec.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        await SpineSnapshot.SaveAsync(
            writer, prefix: "right",
            batches: _rightTrace.GetBatches(),
            saveOne: (name, batch) => _rightSnapshotCodec.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalFullJoinOp was constructed without snapshot codecs.");
        }

        await SpineSnapshot.LoadAsync(
            reader, prefix: "left",
            loadOne: async name =>
            {
                var batch = await _leftSnapshotCodec.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false);
                _leftTrace.Integrate(batch);
            },
            cancellationToken).ConfigureAwait(false);

        await SpineSnapshot.LoadAsync(
            reader, prefix: "right",
            loadOne: async name =>
            {
                var batch = await _rightSnapshotCodec.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false);
                _rightTrace.Integrate(batch);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint =>
        $"left={_leftSnapshotCodec?.SchemaFingerprint ?? ""};right={_rightSnapshotCodec?.SchemaFingerprint ?? ""}";

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightIn.Current;

        var builder = new ZSetBuilder<TOut, TWeight>();

        var touched = new HashSet<TKey>();
        foreach (var k in dl.Keys)
        {
            touched.Add(k);
        }

        foreach (var k in dr.Keys)
        {
            touched.Add(k);
        }

        foreach (var key in touched)
        {
            var oldL = _leftTrace.GroupFor(key);
            var oldR = _rightTrace.GroupFor(key);
            var dlK = dl.GroupFor(key);
            var drK = dr.GroupFor(key);
            var newL = oldL + dlK;
            var newR = oldR + drK;

            var oldRMatched = !oldR.IsEmpty;
            var newRMatched = !newR.IsEmpty;

            // Part A — inner + left-pad, keyed on right-presence.
            if (oldRMatched && newRMatched)
            {
                JoinInto(builder, key, dlK, newR);
                JoinInto(builder, key, oldL, drK);
            }
            else if (!oldRMatched && !newRMatched)
            {
                NullPadRightInto(builder, key, dlK);
            }
            else if (!oldRMatched && newRMatched)
            {
                NullPadRightInto(builder, key, oldL.Negate());
                JoinInto(builder, key, newL, newR);
            }
            else
            {
                JoinInto(builder, key, oldL.Negate(), oldR);
                NullPadRightInto(builder, key, newL);
            }

            // Part B — right-pad, keyed on left-presence.
            var oldLMatched = !oldL.IsEmpty;
            var newLMatched = !newL.IsEmpty;
            if (oldLMatched && newLMatched)
            {
                // rightPad ∅ both ticks — nothing.
            }
            else if (!oldLMatched && !newLMatched)
            {
                NullPadLeftInto(builder, key, drK);
            }
            else if (!oldLMatched && newLMatched)
            {
                NullPadLeftInto(builder, key, oldR.Negate());
            }
            else
            {
                NullPadLeftInto(builder, key, newR);
            }
        }

        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
        _rightTrace.Integrate(dr);
        CollectGarbage();
    }

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
        _gcDropped += _leftTrace.DropKeysBelow(frontier, _monotoneKey).Count;
        _gcDropped += _rightTrace.DropKeysBelow(frontier, _monotoneKey).Count;
    }

    public string MetricName => "SpineIncrementalFullJoin";

    public long RetainedRows => _leftTrace.GroupCount + _rightTrace.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;

    private void JoinInto(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TLeft, TWeight> left,
        ZSet<TRight, TWeight> right)
    {
        if (left.IsEmpty || right.IsEmpty)
        {
            return;
        }

        foreach (var (lv, lw) in left)
        {
            foreach (var (rv, rw) in right)
            {
                output.Add(_joinCombine(key, lv, rv), TWeight.Multiply(lw, rw));
            }
        }
    }

    private void NullPadRightInto(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TLeft, TWeight> left)
    {
        if (left.IsEmpty)
        {
            return;
        }

        foreach (var (lv, lw) in left)
        {
            output.Add(_nullPadRightCombine(key, lv), lw);
        }
    }

    private void NullPadLeftInto(
        ZSetBuilder<TOut, TWeight> output,
        TKey key,
        ZSet<TRight, TWeight> right)
    {
        if (right.IsEmpty)
        {
            return;
        }

        foreach (var (rv, rw) in right)
        {
            output.Add(_nullPadLeftCombine(key, rv), rw);
        }
    }
}
