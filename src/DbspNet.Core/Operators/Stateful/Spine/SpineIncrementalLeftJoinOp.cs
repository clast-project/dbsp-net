using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Spine-backed incremental LEFT OUTER equi-join. Observable behaviour
/// matches <see cref="IncrementalLeftJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>;
/// only the storage of the two integrated traces changes.
/// </summary>
/// <remarks>
/// The per-key case analysis (stayed-matched / stayed-unmatched /
/// gained-match / lost-match) is identical to the flat operator and
/// only touches keys that appear in <c>dl</c> or <c>dr</c> — so the
/// spine version pays the per-key probe (bloom + binary search) and
/// nothing more.
/// </remarks>
internal sealed class SpineIncrementalLeftJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable
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
    private readonly Func<TKey, TLeft, TOut> _nullPadCombine;
    private readonly SpineIndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace;
    private readonly SpineIndexedZSetTrace<TKey, TRight, TWeight> _rightTrace;
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;

    public SpineIncrementalLeftJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TLeft>? leftValueComparer = null,
        IComparer<TRight>? rightValueComparer = null)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _joinCombine = joinCombine;
        _nullPadCombine = nullPadCombine;
        var strategy = compactionStrategy ?? TieredCompactionStrategy.Default;
        _leftTrace = new SpineIndexedZSetTrace<TKey, TLeft, TWeight>(strategy, keyComparer, leftValueComparer);
        _rightTrace = new SpineIndexedZSetTrace<TKey, TRight, TWeight>(strategy, keyComparer, rightValueComparer);
        _leftSnapshotCodec = leftSnapshotCodec;
        _rightSnapshotCodec = rightSnapshotCodec;
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "SpineIncrementalLeftJoinOp was constructed without snapshot codecs.");
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
                "SpineIncrementalLeftJoinOp was constructed without snapshot codecs.");
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

            var oldMatched = !oldR.IsEmpty;
            var newMatched = !newR.IsEmpty;

            if (oldMatched && newMatched)
            {
                JoinInto(builder, key, dlK, newR);
                JoinInto(builder, key, oldL, drK);
            }
            else if (!oldMatched && !newMatched)
            {
                NullPadInto(builder, key, dlK);
            }
            else if (!oldMatched && newMatched)
            {
                NullPadInto(builder, key, oldL.Negate());
                JoinInto(builder, key, newL, newR);
            }
            else
            {
                JoinInto(builder, key, oldL.Negate(), oldR);
                NullPadInto(builder, key, newL);
            }
        }

        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
        _rightTrace.Integrate(dr);
    }

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

    private void NullPadInto(
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
            output.Add(_nullPadCombine(key, lv), lw);
        }
    }
}
