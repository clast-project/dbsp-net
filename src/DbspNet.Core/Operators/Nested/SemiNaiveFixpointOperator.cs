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
/// Cross-tick-incremental nested fixpoint for a <em>linear</em> recursion
/// <c>R = distinct(base(I) ∪ step(I, R))</c> — the shape of a SQL
/// <c>WITH RECURSIVE</c> whose recursive branch is linear in the self-reference
/// (Filter / Project / Inner-Join / UnionAll). Unlike the naive
/// <see cref="FixpointOperator{TRow}"/>, it preserves the materialised fixpoint
/// <c>R</c> across outer ticks and, on an insert-only tick, extends it
/// semi-naively — cost proportional to the newly-derivable rows rather than to
/// the whole closure. Any tick containing a retraction falls back to a
/// from-scratch recompute (still correct; the integrated import traces reflect
/// the deletion). Removing that fallback — fully incremental retraction
/// propagation (DRED) — is the next stage.
/// </summary>
/// <remarks>
/// <para>
/// The body is wired once into a single scope and exposes two output streams:
/// <c>base</c> (the non-recursive branch) and <c>step</c> (the recursive branch,
/// reading the feedback). The operator fires the whole body and reads whichever
/// streams a pass needs, switching each import between this tick's delta
/// (<c>ΔI</c>) and the running integral (<c>I</c>):
/// </para>
/// <list type="bullet">
/// <item><b>δ-pass</b> (imports = ΔI, self = R): the rows this tick's input
/// change newly admits — <c>base(ΔI) ∪ step(R, ΔI)</c> — seed the frontier.</item>
/// <item><b>iteration</b> (imports = I, self = frontier): feed only the frontier
/// back through <c>step</c> to a fixpoint, by linearity of <c>step</c> in the
/// self-reference.</item>
/// </list>
/// <para>This is the algorithm the prior recursive-CTE operator ran via batch
/// re-evaluation, now expressed through wired operators on the Core nested
/// primitive. Correctness is held by the incremental≡batch recursive PBT.</para>
/// </remarks>
/// <typeparam name="TRow">The row type flowing on every inner stream.</typeparam>
internal sealed class SemiNaiveFixpointOperator<TRow> : IOperator, ISnapshotable
    where TRow : notnull
{
    private const string ResultFileName = "r.arrows";

    private readonly IReadOnlyList<IOperator> _body;
    private readonly Import[] _imports;
    private readonly Stream<ZSet<TRow, Z64>> _recRef;
    private readonly Stream<ZSet<TRow, Z64>> _baseStream;
    private readonly Stream<ZSet<TRow, Z64>> _stepStream;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly IZSetTraceCodec<TRow, Z64>? _resultCodec;
    private readonly int _maxIterations;
    private ZSet<TRow, Z64> _r = ZSet<TRow, Z64>.Empty;

    public SemiNaiveFixpointOperator(
        IReadOnlyList<IOperator> body,
        IReadOnlyList<ImportBinding<TRow>> imports,
        Stream<ZSet<TRow, Z64>> recRef,
        Stream<ZSet<TRow, Z64>> baseStream,
        Stream<ZSet<TRow, Z64>> stepStream,
        Stream<ZSet<TRow, Z64>> output,
        IZSetTraceCodec<TRow, Z64>? resultCodec,
        int maxIterations)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(imports);
        ArgumentNullException.ThrowIfNull(recRef);
        ArgumentNullException.ThrowIfNull(baseStream);
        ArgumentNullException.ThrowIfNull(stepStream);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxIterations);

        _body = body;
        _imports = imports.Select(i => new Import(i.Outer, i.Inner, new ZSetTrace<TRow, Z64>(), i.Name, i.SnapshotCodec)).ToArray();
        _recRef = recRef;
        _baseStream = baseStream;
        _stepStream = stepStream;
        _output = output;
        _resultCodec = resultCodec;
        _maxIterations = maxIterations;
    }

    public void Step()
    {
        var hasRetraction = _imports.Any(i => ContainsNegativeWeight(i.Outer.Current));

        // Integrate this tick's deltas; ΔI stays on the outer streams, I is the
        // trace integral. Both are needed below.
        foreach (var import in _imports)
        {
            import.Trace.Integrate(import.Outer.Current);
        }

        var previousResult = _r;
        if (hasRetraction || _r.IsEmpty)
        {
            FullRecompute();
        }
        else
        {
            IncrementalExtend();
        }

        _output.SetCurrent(_r - previousResult);
    }

    // R₀ = ∅; Rₙ₊₁ = distinct(base(I) ∪ step(I, Rₙ)); stop at the first fixpoint.
    private void FullRecompute()
    {
        var r = ZSet<TRow, Z64>.Empty;
        for (var i = 0; i < _maxIterations; i++)
        {
            EvalBody(r, useDelta: false);
            var next = AsSet(_baseStream.Current + _stepStream.Current);
            if (next.Equals(r))
            {
                _r = r;
                return;
            }

            r = next;
        }

        throw new InvalidOperationException(
            $"recursive fixpoint did not converge after {_maxIterations} iterations");
    }

    // Semi-naive extension from the preserved R on an insert-only tick.
    private void IncrementalExtend()
    {
        // δ-pass: rows newly admitted by this tick's input change.
        EvalBody(_r, useDelta: true);
        var frontier = SetDifference(AsSet(_baseStream.Current + _stepStream.Current), _r);

        // Iteration: feed only the frontier back through step against full I.
        for (var i = 0; i < _maxIterations; i++)
        {
            if (frontier.IsEmpty)
            {
                return;
            }

            _r = AsSet(_r + frontier);
            EvalBody(frontier, useDelta: false);
            frontier = SetDifference(AsSet(_stepStream.Current), _r);
        }

        throw new InvalidOperationException(
            $"recursive fixpoint did not converge after {_maxIterations} iterations");
    }

    // Present each import as ΔI or the running integral I, bind the feedback,
    // then fire the whole body so both base and step streams are current.
    private void EvalBody(ZSet<TRow, Z64> self, bool useDelta)
    {
        foreach (var import in _imports)
        {
            import.Inner.SetCurrent(useDelta ? import.Outer.Current : import.Trace.Current);
        }

        _recRef.SetCurrent(self);
        foreach (var op in _body)
        {
            op.Step();
        }
    }

    private static ZSet<TRow, Z64> AsSet(ZSet<TRow, Z64> z)
    {
        if (z.IsEmpty)
        {
            return z;
        }

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, w) in z)
        {
            if (Z64.IsPositive(w))
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    // A − B as sets: positive-weight rows of A absent (zero-weight) from B.
    private static ZSet<TRow, Z64> SetDifference(ZSet<TRow, Z64> a, ZSet<TRow, Z64> b)
    {
        if (a.IsEmpty)
        {
            return a;
        }

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, w) in a)
        {
            if (Z64.IsPositive(w) && !Z64.IsPositive(b.WeightOf(row)))
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    private static bool ContainsNegativeWeight(ZSet<TRow, Z64> z)
    {
        foreach (var (_, w) in z)
        {
            if (w.Value < 0)
            {
                return true;
            }
        }

        return false;
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

        await _resultCodec!.SaveAsync(writer, ResultFileName, _r, cancellationToken).ConfigureAwait(false);
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

        _r = await _resultCodec!.LoadAsync(reader, ResultFileName, cancellationToken).ConfigureAwait(false);
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
                "SemiNaiveFixpointOperator was constructed without snapshot codecs; supply a result codec and " +
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
