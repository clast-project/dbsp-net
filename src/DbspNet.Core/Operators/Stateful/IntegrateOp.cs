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
    private readonly IRowTrace<TRow, Z64> _view;
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    // A view handle reads the tick's delta after the step (§3b). The op keeps it, block backed when large
    // (§5.33), rather than pinning the input stream's managed value to the end of the tick.
    private bool _keepDelta;
    private ZSet<TRow, Z64>? _lastDelta;

    public IntegrateOp(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null,
        IRowTrace<TRow, Z64>? view = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        _input = input;
        _output = output;
        _snapshotCodec = snapshotCodec;
        _view = view ?? new ManagedRowTrace<TRow, Z64>();
    }

    /// <summary>Whether the view is held as blocks (DBSPNET_TRACE_BLOCKS).</summary>
    internal bool IsBlocked => _view is Blocks.BlockedZSetTrace;

    /// <summary>The view's blocks, when it is held as blocks (a full write builds Arrow from them).</summary>
    internal Blocks.BlockedIndexedTrace? ViewTrace => (_view as Blocks.BlockedZSetTrace)?.Trace;

    /// <summary>The full current view contents — every delta folded in so far —
    /// valid until the next <see cref="Step"/>.</summary>
    /// <summary>Restore output views lazily (increment 6); <c>DBSPNET_LAZY_VIEWS=0</c> reads them
    /// whole at restore instead.</summary>
    internal static bool LazyViews { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_LAZY_VIEWS") != "0";

    public ZSet<TRow, Z64> View => _view.Current;

    /// <summary>Every row of the view, without reading a lazily restored view into memory
    /// (docs/design-durable-state.md §1f, increment 6): what a sink's first full write streams.</summary>
    public IEnumerable<KeyValuePair<TRow, Z64>> EnumerateView() => _view.EnumerateWhole();

    /// <summary>True while the view holds rows not yet read from a lazy restore (tests).</summary>
    internal bool IsCold => _view.IsCold;

    /// <summary>Whether enumerating the view decodes rows (cold, or held as blocks).</summary>
    internal bool EnumerationDecodes => _view.EnumerationDecodes;

    /// <summary>The number of rows in <see cref="View"/>.</summary>
    public int ViewCount => _view.Count;

    /// <summary>A row's weight in <see cref="View"/>.</summary>
    public Z64 WeightOf(TRow row) => _view.WeightOf(row);

    /// <summary>The delta the most recent step integrated.</summary>
    public ZSet<TRow, Z64> LastDelta => _keepDelta ? _lastDelta ?? ZSet<TRow, Z64>.Empty : _input.Current;

    /// <summary>Keep the tick's delta after the step: a view handle reads it (docs/design-bounded-memory.md
    /// §3b). Kept by the op, so the input stream's value is released as any other (§5.33).</summary>
    internal void PinInput() => _keepDelta = true;

    /// <summary>Deltas re-stored block backed for a view handle, and their rows (diagnostics, tests).</summary>
    internal static long SpilledDeltas, SpilledDeltaRows;

    public string MetricName => "IntegratedView";

    public long RetainedRows => _view.RetainedRows;

    public long LastOutputRows => _output.LastCount;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _view.Idle();
        }

        if (_keepDelta)
        {
            delta = BlockBacked(delta);
            _lastDelta = delta;
        }

        _view.Integrate(delta);      // O(|delta|): fold the tick's delta into the view.
        _output.SetCurrent(delta);   // the delta flows through unchanged.
    }

    // §5.33: a kept delta lives to the end of the tick, so a large managed one (a view's whole tick of
    // output, ~20 GB over extended TPC-DS tick 18) is re-stored as block runs. A blocked view adopts the
    // runs, so the rows are encoded once either way. The floor is the Critical chunk, not the step chunk:
    // kept deltas add up over a tick (a worker's share of a 5.8M-row view is ~480k, under 1M).
    private ZSet<TRow, Z64> BlockBacked(ZSet<TRow, Z64> delta)
    {
        if (delta.IsExternal
            || _view is not Blocks.BlockedZSetTrace blocked
            || Blocks.SpillSettings.StepSpillRows is not { } rows
            || delta.Count < Math.Max(1, Math.Min(rows, Blocks.MemoryPressure.CriticalStepRows))
            || delta is not ZSet<StructuralRow, Z64> structural)
        {
            return delta;
        }

        // Always blocked: a delta under one step chunk never freezes on its own, and would build a managed copy.
        var builder = new ZSetBuilder<StructuralRow, Z64>(
            Blocks.DeltaSpill.Store(blocked.Trace.Format, rows, alwaysBlocked: true, structural.Count));
        foreach (var (row, weight) in structural)
        {
            builder.Add(row, weight);
        }

        var spilled = builder.Build();
        Interlocked.Increment(ref SpilledDeltas);
        Interlocked.Add(ref SpilledDeltaRows, spilled.Count);
        return (ZSet<TRow, Z64>)(object)spilled;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "IntegrateOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.Integrate to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return _view.SaveAsync(_snapshotCodec, writer, "view.arrows", cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("IntegrateOp was constructed without a snapshot codec.");
        }

        // Lazily (increment 6): a row-ordered chain stays cold until rows are asked for.
        await _view.LoadAsync(_snapshotCodec, reader, "view.arrows", LazyViews, cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
