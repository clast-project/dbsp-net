using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental inner equi-join of two indexed Z-set streams. Emits a flat
/// Z-set of combined rows. State: the integrated traces of both inputs. Per
/// tick, the output is the bilinear incremental form
/// <code>
/// out_t = dl ⋈ L_{t-1} + L_{t-1} ⋈ dr + dl ⋈ dr
///       = dl ⋈ (L_{t-1} + dr) + L_{t-1} ⋈ dr         (equivalent factoring)
/// </code>
/// where <c>dl, dr</c> are this tick's deltas and <c>L, R</c> are the
/// integrated traces. Equivalent to the <c>D(↑Q(I, I))</c> sandwich on the
/// bilinear operator <c>Q(a,b) = a ⋈ b</c> by the DBSP bilinearity theorem.
/// </summary>
internal sealed class IncrementalJoinOp<TKey, TLeft, TRight, TOut, TWeight> : IOperator, ISnapshotable
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private const string LeftTraceFile = "left.arrows";
    private const string RightTraceFile = "right.arrows";

    private readonly Stream<IndexedZSet<TKey, TLeft, TWeight>> _leftIn;
    private readonly Stream<IndexedZSet<TKey, TRight, TWeight>> _rightIn;
    private readonly Stream<ZSet<TOut, TWeight>> _output;
    private readonly Func<TKey, TLeft, TRight, TOut> _combine;
    private readonly IndexedZSetTrace<TKey, TLeft, TWeight> _leftTrace = new();
    private readonly IndexedZSetTrace<TKey, TRight, TWeight> _rightTrace = new();
    private readonly IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? _leftSnapshotCodec;
    private readonly IIndexedZSetTraceCodec<TKey, TRight, TWeight>? _rightSnapshotCodec;

    public IncrementalJoinOp(
        Stream<IndexedZSet<TKey, TLeft, TWeight>> leftIn,
        Stream<IndexedZSet<TKey, TRight, TWeight>> rightIn,
        Stream<ZSet<TOut, TWeight>> output,
        Func<TKey, TLeft, TRight, TOut> combine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _combine = combine;
        _leftSnapshotCodec = leftSnapshotCodec;
        _rightSnapshotCodec = rightSnapshotCodec;
    }

    public void Save(ISnapshotWriter writer)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalJoinOp was constructed without snapshot codecs; pass them " +
                "to CircuitBuilder.IncrementalInnerJoin to enable Snapshot.Write/Read.");
        }

        _leftSnapshotCodec.Save(writer, LeftTraceFile, _leftTrace.Current);
        _rightSnapshotCodec.Save(writer, RightTraceFile, _rightTrace.Current);
    }

    public void Load(ISnapshotReader reader)
    {
        if (_leftSnapshotCodec is null || _rightSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalJoinOp was constructed without snapshot codecs.");
        }

        _leftTrace.Integrate(_leftSnapshotCodec.Load(reader, LeftTraceFile));
        _rightTrace.Integrate(_rightSnapshotCodec.Load(reader, RightTraceFile));
    }

    public string SchemaFingerprint =>
        $"left={_leftSnapshotCodec?.SchemaFingerprint ?? ""};right={_rightSnapshotCodec?.SchemaFingerprint ?? ""}";

    public void Step()
    {
        var dl = _leftIn.Current;
        var dr = _rightIn.Current;

        var builder = new ZSetBuilder<TOut, TWeight>();

        // dl ⋈ R_{t-1}
        JoinInto(dl, _rightTrace.Current, builder);
        // L_{t-1} ⋈ dr
        JoinInto(_leftTrace.Current, dr, builder);
        // dl ⋈ dr
        JoinInto(dl, dr, builder);

        _output.SetCurrent(builder.Build());

        // Update integrated traces for next tick.
        _leftTrace.Integrate(dl);
        _rightTrace.Integrate(dr);
    }

    private void JoinInto(
        IndexedZSet<TKey, TLeft, TWeight> a,
        IndexedZSet<TKey, TRight, TWeight> b,
        ZSetBuilder<TOut, TWeight> output)
    {
        if (a.IsEmpty || b.IsEmpty)
        {
            return;
        }

        // Iterate the smaller side (by group count) to amortize probing cost.
        if (a.GroupCount <= b.GroupCount)
        {
            foreach (var (key, aGroup) in a)
            {
                var bGroup = b.GroupFor(key);
                if (bGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (av, aw) in aGroup)
                {
                    foreach (var (bv, bw) in bGroup)
                    {
                        output.Add(_combine(key, av, bv), TWeight.Multiply(aw, bw));
                    }
                }
            }
        }
        else
        {
            foreach (var (key, bGroup) in b)
            {
                var aGroup = a.GroupFor(key);
                if (aGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (av, aw) in aGroup)
                {
                    foreach (var (bv, bw) in bGroup)
                    {
                        output.Add(_combine(key, av, bv), TWeight.Multiply(aw, bw));
                    }
                }
            }
        }
    }
}
