// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The DBSP integration operator <c>I</c> at the output boundary: folds every
/// output delta into a running Z-set so <see cref="View"/> holds the <b>full
/// current view contents</b> (a materialized view), while the delta still flows
/// through unchanged on the output stream. This is what lets a truncate-mode sink
/// write a full snapshot per batch — the analogue of Feldera's <c>+stored</c>
/// materialized view.
/// </summary>
/// <remarks>
/// <para>Pass-through, not replacement: the operator re-emits the input delta on
/// its output stream, so every existing delta consumer (and the
/// <see cref="OutputHandle{T}"/>) is unchanged — the integrated view is an
/// <em>addition</em>, read on demand through <see cref="View"/>. The per-tick cost
/// is therefore <c>O(|delta|)</c> (the integrate); the <c>O(|view|)</c> full-relation
/// cost is paid only when a consumer enumerates <see cref="View"/> (a truncate
/// write), which is exactly the cost a materializing engine is charged for.</para>
/// <para>State is the whole materialized output, so it is inherently unbounded —
/// <see cref="GcFrontier"/> is <c>null</c> (this is the point of a stored view, not
/// a leak). Implementing <see cref="ISnapshotable"/> puts the view inside the
/// circuit's snapshot/commit, matching a benchmark timer that includes state
/// persistence and giving crash-recovery of the view for free.</para>
/// <para><see cref="View"/> is a live reference whose contents change on the next
/// <see cref="Step"/> — read it before stepping again, the same lifetime contract
/// as <see cref="OutputHandle{T}.Current"/>.</para>
/// </remarks>
/// <typeparam name="TRow">The output row type (<c>StructuralRow</c> at the SQL
/// output boundary; kept generic for reuse and testing).</typeparam>
internal sealed class IntegrateOp<TRow> : IOperator, ISnapshotable, IIntrospectable
    where TRow : notnull
{
    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly ZSetTrace<TRow, Z64> _view = new();
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    public IntegrateOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        _input = input;
        _output = output;
        _snapshotCodec = snapshotCodec;
    }

    /// <summary>The full current view contents — every delta folded in so far —
    /// valid until the next <see cref="Step"/>.</summary>
    public ZSet<TRow, Z64> View => _view.Current;

    public string MetricName => "IntegratedView";

    public long RetainedRows => _view.Current.Count;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;
        _view.Integrate(delta);      // O(|delta|): fold the tick's delta into the view.
        _output.SetCurrent(delta);   // the delta flows through unchanged.
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "IntegrateOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.Integrate to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return _snapshotCodec.SaveAsync(writer, "view.arrows", _view.Current, cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("IntegrateOp was constructed without a snapshot codec.");
        }

        var loaded = await _snapshotCodec.LoadAsync(reader, "view.arrows", cancellationToken).ConfigureAwait(false);

        // Reset the (in-place-mutated) view to the persisted contents. Integrating
        // into a freshly-cleared trace re-establishes exactly the saved multiset.
        _view.Current.MergeInPlace(_view.Current.Negate());
        _view.Integrate(loaded);
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
