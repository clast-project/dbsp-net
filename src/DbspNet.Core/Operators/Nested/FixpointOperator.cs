// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Operators.Nested;

/// <summary>
/// The reusable nested-circuit (fixpoint) primitive: a single outer operator
/// that owns a body sub-graph wired through a <see cref="NestedScopeBuilder{TRow}"/>
/// and drives it to a least fixpoint on an inner iteration clock, once per outer
/// tick. This is the DBSP "nested circuit" construction extracted from the
/// special-cased recursive-CTE operator into Core, so any caller (and the SQL
/// <c>WITH RECURSIVE</c> compiler) can build recursion from real operators
/// rather than a bespoke evaluation loop.
/// </summary>
/// <remarks>
/// <para><b>Brackets.</b> Imported outer streams (<c>δ₀</c>) are integrated into
/// per-stream traces so the loop sees the full relation, not just this tick's
/// delta; the feedback stream (the inner <c>z⁻¹</c>) carries <c>R</c> from the
/// previous iteration; the converged <c>R</c> is exported as the outer-tick
/// delta against the previous tick's fixpoint.</para>
/// <para><b>Iteration.</b> Naive evaluation — <c>R₀ = ∅</c>,
/// <c>Rₙ₊₁ = body(I, Rₙ)</c>, stop when <c>Rₙ₊₁ = Rₙ</c>. Because it recomputes
/// the whole fixpoint from the integrated inputs each tick, it is correct under
/// retractions (the input traces reflect them) but not yet cross-tick
/// incremental; semi-naive iteration and persistent inner traces are the next
/// stage. Behaviourally this matches the recompute path of the prior recursive-
/// CTE operator.</para>
/// <para><b>Snapshot.</b> Only the import traces and the previous-tick fixpoint
/// need persisting — the fixpoint itself is recomputed on the next tick. When
/// every import carries a name and codec and a result codec is supplied, the
/// operator round-trips through <see cref="ISnapshotable"/>; otherwise
/// <see cref="SaveAsync"/> throws, matching the codec-less contract of the other
/// stateful operators.</para>
/// </remarks>
/// <typeparam name="TRow">The row type flowing on every inner stream.</typeparam>
internal sealed class FixpointOperator<TRow> : IOperator, ISnapshotable
    where TRow : notnull
{
    private const string PreviousResultFileName = "prev.arrows";

    private readonly IReadOnlyList<IOperator> _body;
    private readonly Import[] _imports;
    private readonly Stream<ZSet<TRow, Z64>> _recRef;
    private readonly Stream<ZSet<TRow, Z64>> _next;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly IZSetTraceCodec<TRow, Z64>? _resultCodec;
    private readonly int _maxIterations;
    private ZSet<TRow, Z64> _previousResult = ZSet<TRow, Z64>.Empty;

    public FixpointOperator(
        IReadOnlyList<IOperator> body,
        IReadOnlyList<ImportBinding<TRow>> imports,
        Stream<ZSet<TRow, Z64>> recRef,
        Stream<ZSet<TRow, Z64>> next,
        Stream<ZSet<TRow, Z64>> output,
        IZSetTraceCodec<TRow, Z64>? resultCodec,
        int maxIterations)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(imports);
        ArgumentNullException.ThrowIfNull(recRef);
        ArgumentNullException.ThrowIfNull(next);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxIterations);

        _body = body;
        _imports = imports.Select(i => new Import(i.Outer, i.Inner, new ZSetTrace<TRow, Z64>(), i.Name, i.SnapshotCodec)).ToArray();
        _recRef = recRef;
        _next = next;
        _output = output;
        _resultCodec = resultCodec;
        _maxIterations = maxIterations;
    }

    public void Step()
    {
        // δ₀ import: fold this tick's deltas into the traces and expose the
        // running integral to the loop (constant across inner iterations).
        foreach (var import in _imports)
        {
            import.Trace.Integrate(import.Outer.Current);
            import.Inner.SetCurrent(import.Trace.Current);
        }

        // Naive fixpoint: R₀ = ∅; Rₙ₊₁ = body(I, Rₙ); stop at the first fixpoint.
        var r = ZSet<TRow, Z64>.Empty;
        var converged = false;
        for (var i = 0; i < _maxIterations; i++)
        {
            _recRef.SetCurrent(r);
            foreach (var op in _body)
            {
                op.Step();
            }

            var next = _next.Current;
            if (next.Equals(r))
            {
                converged = true;
                break;
            }

            r = next;
        }

        if (!converged)
        {
            throw new InvalidOperationException(
                $"nested fixpoint did not converge after {_maxIterations} iterations");
        }

        // Export: emit the delta of the fixpoint against the previous tick's.
        _output.SetCurrent(r - _previousResult);
        _previousResult = r;
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        EnsureSnapshottable();

        foreach (var import in _imports)
        {
            await import.SnapshotCodec!.SaveAsync(
                writer, ImportFileName(import.Name!), import.Trace.Current, cancellationToken).ConfigureAwait(false);
        }

        await _resultCodec!.SaveAsync(writer, PreviousResultFileName, _previousResult, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        EnsureSnapshottable();

        foreach (var import in _imports)
        {
            import.Trace.Integrate(
                await import.SnapshotCodec!.LoadAsync(reader, ImportFileName(import.Name!), cancellationToken).ConfigureAwait(false));
        }

        _previousResult = await _resultCodec!.LoadAsync(reader, PreviousResultFileName, cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint
    {
        get
        {
            if (!IsSnapshottable)
            {
                return string.Empty;
            }

            var sb = new StringBuilder();
            foreach (var import in _imports.OrderBy(i => i.Name, StringComparer.Ordinal))
            {
                sb.Append(import.Name).Append('=').Append(import.SnapshotCodec!.SchemaFingerprint).Append(';');
            }

            sb.Append("result=").Append(_resultCodec!.SchemaFingerprint);
            return sb.ToString();
        }
    }

    private bool IsSnapshottable =>
        _resultCodec is not null && _imports.All(i => i.Name is not null && i.SnapshotCodec is not null);

    private void EnsureSnapshottable()
    {
        if (!IsSnapshottable)
        {
            throw new NotSupportedException(
                "FixpointOperator was constructed without snapshot codecs; supply a result codec and " +
                "name + codec for every import to enable snapshot save/load.");
        }
    }

    private static string ImportFileName(string name) => "import_" + name + ".arrows";

    private sealed record Import(
        Stream<ZSet<TRow, Z64>> Outer,
        Stream<ZSet<TRow, Z64>> Inner,
        ZSetTrace<TRow, Z64> Trace,
        string? Name,
        IZSetTraceCodec<TRow, Z64>? SnapshotCodec);
}
