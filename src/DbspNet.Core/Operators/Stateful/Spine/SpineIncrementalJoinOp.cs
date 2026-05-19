using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Spine-backed incremental inner equi-join. Observable behaviour
/// matches <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>;
/// the only difference is that the two integrated traces live in
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/> rather than
/// the flat <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>.
/// </summary>
/// <remarks>
/// <para>The asymmetric two-pass form — <c>dl ⋈ R_t + L_{t-1} ⋈ dr</c>
/// — is unchanged. The integrate ordering (right before, left after)
/// matters identically; the bilinearity argument still holds.</para>
/// <para><b>Probe direction.</b> Each <see cref="JoinInto"/> call
/// iterates the delta side and probes the spine trace via
/// <c>GroupFor</c>. This is the steady-state shape (delta &lt;&lt;
/// trace); workloads where the delta is larger than the trace will
/// pay the wrong-side iteration cost. The flat operator would pick the
/// smaller side dynamically.</para>
/// </remarks>
internal sealed class SpineIncrementalJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightIn;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _combine;
    private readonly SpineIndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace;
    private readonly SpineIndexedZSetTrace<TKey, TRight, TWeight> _rightTrace;
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;

    public SpineIncrementalJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> combine,
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
        _combine = combine;
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
                "SpineIncrementalJoinOp was constructed without snapshot codecs.");
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
                "SpineIncrementalJoinOp was constructed without snapshot codecs.");
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

        // Right delta integrated BEFORE the joins so the first pass sees
        // R_t = R_{t-1} + dr — absorbs the dl ⋈ dr cross term. The left
        // trace stays at L_{t-1} for the second pass.
        _rightTrace.Integrate(dr);

        var builder = new ZSetBuilder<TOut, TWeight>();

        // dl ⋈ R_t — iterate dl, probe right spine.
        if (!dl.IsEmpty)
        {
            foreach (var (key, lGroup) in dl)
            {
                var rGroup = _rightTrace.GroupFor(key);
                if (rGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (lv, lw) in lGroup)
                {
                    foreach (var (rv, rw) in rGroup)
                    {
                        builder.Add(_combine(key, lv, rv), TWeight.Multiply(lw, rw));
                    }
                }
            }
        }

        // L_{t-1} ⋈ dr — iterate dr, probe left spine.
        if (!dr.IsEmpty)
        {
            foreach (var (key, rGroup) in dr)
            {
                var lGroup = _leftTrace.GroupFor(key);
                if (lGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (lv, lw) in lGroup)
                {
                    foreach (var (rv, rw) in rGroup)
                    {
                        builder.Add(_combine(key, lv, rv), TWeight.Multiply(lw, rw));
                    }
                }
            }
        }

        _output.SetCurrent(builder.Build());

        _leftTrace.Integrate(dl);
    }
}
