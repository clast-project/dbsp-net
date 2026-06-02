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
/// (Filter / Project / Inner-Join / UnionAll). It preserves the materialised
/// fixpoint <c>R</c> across outer ticks and updates it incrementally — cost
/// proportional to the affected region rather than to the whole closure — for
/// both insertions (semi-naive extension) and deletions (Delete-and-Re-Derive).
/// </summary>
/// <remarks>
/// <para>
/// The body is wired once into a single scope and exposes two output streams:
/// <c>base</c> (the non-recursive branch) and <c>step</c> (the recursive branch,
/// reading the feedback). The operator fires the whole body and reads whichever
/// streams a pass needs, presenting each import as a computed Z-set — this
/// tick's insertions (ΔI⁺), the running integral (I), the surviving edges
/// (I∖ΔI⁺), the pre-tick edges (I_old), or the deletions (ΔI⁻).
/// </para>
/// <list type="bullet">
/// <item><b>Insertion</b> (semi-naive): seed <c>base(ΔI⁺) ∪ step(R, ΔI⁺)</c>,
/// then iterate the frontier through <c>step</c> against full <c>I</c>, by
/// linearity of <c>step</c> in the self-reference.</item>
/// <item><b>Deletion</b> (DRED): <i>over-delete</i> the R-tuples whose
/// derivation used a deleted input, propagated transitively through the old
/// graph; then <i>re-derive</i> the ones still reachable from the survivors via
/// surviving edges. Correct under alternative derivation paths and cycles.</item>
/// </list>
/// <para>A tick is processed delete-then-insert, each from a correct fixpoint to
/// the next. Multiset input deltas (a weight magnitude &gt; 1) fall outside the
/// set model and trigger a from-scratch recompute. Correctness is held by the
/// incremental≡batch recursive PBT over random insert/delete sequences.</para>
/// </remarks>
/// <typeparam name="TRow">The row type flowing on every inner stream.</typeparam>
internal sealed class SemiNaiveFixpointOperator<TRow> : IOperator, ISnapshotable, IIntrospectable
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
        SpineImportConfig<TRow>? spineConfig,
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
        _imports = imports.Select(i => new Import(
            i.Outer,
            i.Inner,
            spineConfig is null
                ? new FlatImportTrace<TRow>(i.SnapshotCodec)
                : new SpineImportTrace<TRow>(spineConfig, i.SnapshotCodec),
            i.Name)).ToArray();
        _recRef = recRef;
        _baseStream = baseStream;
        _stepStream = stepStream;
        _output = output;
        _resultCodec = resultCodec;
        _maxIterations = maxIterations;
    }

    // The materialised fixpoint R; it grows unbounded (the recursive retain-keys
    // story is deferred — see docs/skipped.md), so its size is worth watching.
    public string MetricName => "RecursiveCte";

    public long RetainedRows => _r.Count;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var n = _imports.Length;

        // Multiset input deltas are outside DRED's set model — recompute instead.
        var multiset = false;
        for (var i = 0; i < n; i++)
        {
            if (AnyWeightMagnitudeAboveOne(_imports[i].Outer.Current))
            {
                multiset = true;
            }
        }

        foreach (var import in _imports)
        {
            import.Trace.Integrate(import.Outer.Current);
        }

        var previousResult = _r;

        if (multiset)
        {
            var raw = new ZSet<TRow, Z64>[n];
            for (var i = 0; i < n; i++)
            {
                raw[i] = _imports[i].Trace.Current;
            }

            FullRecompute(raw);
        }
        else
        {
            // Per-import set views: inserted (ΔI⁺), deleted (ΔI⁻), the integral
            // I, the survivors I∖ΔI⁺, and the pre-tick edges I_old = survivors∪ΔI⁻.
            var inserted = new ZSet<TRow, Z64>[n];
            var deleted = new ZSet<TRow, Z64>[n];
            var integral = new ZSet<TRow, Z64>[n];
            var surviving = new ZSet<TRow, Z64>[n];
            var old = new ZSet<TRow, Z64>[n];
            var hasDeletion = false;
            for (var i = 0; i < n; i++)
            {
                var delta = _imports[i].Outer.Current;
                inserted[i] = AsSet(delta);
                deleted[i] = NegativePartAsSet(delta);
                integral[i] = AsSet(_imports[i].Trace.Current);
                surviving[i] = SetDifference(integral[i], inserted[i]);
                old[i] = SetUnion(surviving[i], deleted[i]);
                hasDeletion |= !deleted[i].IsEmpty;
            }

            if (hasDeletion)
            {
                DredDelete(deleted, old, surviving);
            }

            InsertExtend(inserted, integral);
        }

        _output.SetCurrent(_r - previousResult);
    }

    // R₀ = ∅; Rₙ₊₁ = distinct(base(I) ∪ step(I, Rₙ)); stop at the first fixpoint.
    private void FullRecompute(ZSet<TRow, Z64>[] importValues)
    {
        var r = ZSet<TRow, Z64>.Empty;
        for (var i = 0; i < _maxIterations; i++)
        {
            EvalBody(r, importValues);
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

    // Semi-naive extension of the preserved R by this tick's insertions.
    private void InsertExtend(ZSet<TRow, Z64>[] inserted, ZSet<TRow, Z64>[] integral)
    {
        // Seed: rows newly admitted by the inserted inputs.
        var frontier = SetDifference(BaseUnionStep(_r, inserted), _r);

        // Iterate the frontier through step against the full integral.
        for (var i = 0; i < _maxIterations; i++)
        {
            if (frontier.IsEmpty)
            {
                return;
            }

            _r = SetUnion(_r, frontier);
            frontier = SetDifference(StepOnly(frontier, integral), _r);
        }

        throw new InvalidOperationException(
            $"recursive fixpoint did not converge after {_maxIterations} iterations");
    }

    // Delete-and-Re-Derive: bring R from the fixpoint over I_old to the fixpoint
    // over the surviving edges, incrementally.
    private void DredDelete(ZSet<TRow, Z64>[] deleted, ZSet<TRow, Z64>[] old, ZSet<TRow, Z64>[] surviving)
    {
        // Over-delete: every R-tuple whose derivation used a deleted input,
        // propagated transitively through the old graph (tentatively removed).
        var overDeleted = ZSet<TRow, Z64>.Empty;
        var frontier = SetIntersection(BaseUnionStep(_r, deleted), _r);
        for (var i = 0; i < _maxIterations; i++)
        {
            frontier = SetDifference(frontier, overDeleted);
            if (frontier.IsEmpty)
            {
                break;
            }

            overDeleted = SetUnion(overDeleted, frontier);
            frontier = SetIntersection(StepOnly(frontier, old), _r);
            if (i == _maxIterations - 1)
            {
                throw new InvalidOperationException(
                    $"recursive over-deletion did not converge after {_maxIterations} iterations");
            }
        }

        // Re-derive: over-deleted tuples still reachable from the survivors via
        // surviving edges are restored.
        var survivors = SetDifference(_r, overDeleted);
        var rederived = ZSet<TRow, Z64>.Empty;
        var rfront = SetIntersection(BaseUnionStep(survivors, surviving), overDeleted);
        for (var i = 0; i < _maxIterations; i++)
        {
            rfront = SetDifference(rfront, rederived);
            if (rfront.IsEmpty)
            {
                break;
            }

            rederived = SetUnion(rederived, rfront);
            rfront = SetIntersection(StepOnly(SetUnion(survivors, rederived), surviving), overDeleted);
            if (i == _maxIterations - 1)
            {
                throw new InvalidOperationException(
                    $"recursive re-derivation did not converge after {_maxIterations} iterations");
            }
        }

        _r = SetUnion(survivors, rederived);
    }

    // base(imports) ∪ step(imports, self), as a set.
    private ZSet<TRow, Z64> BaseUnionStep(ZSet<TRow, Z64> self, ZSet<TRow, Z64>[] importValues)
    {
        EvalBody(self, importValues);
        return AsSet(_baseStream.Current + _stepStream.Current);
    }

    // step(imports, self), as a set.
    private ZSet<TRow, Z64> StepOnly(ZSet<TRow, Z64> self, ZSet<TRow, Z64>[] importValues)
    {
        EvalBody(self, importValues);
        return AsSet(_stepStream.Current);
    }

    // Present each import as the supplied value, bind the feedback, then fire the
    // whole body so both base and step streams are current.
    private void EvalBody(ZSet<TRow, Z64> self, ZSet<TRow, Z64>[] importValues)
    {
        for (var i = 0; i < _imports.Length; i++)
        {
            _imports[i].Inner.SetCurrent(importValues[i]);
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

    // Deleted rows (negative weight in the signed delta) as a positive set.
    private static ZSet<TRow, Z64> NegativePartAsSet(ZSet<TRow, Z64> z)
    {
        if (z.IsEmpty)
        {
            return z;
        }

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, w) in z)
        {
            if (w.Value < 0)
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    // A ∪ B as sets.
    private static ZSet<TRow, Z64> SetUnion(ZSet<TRow, Z64> a, ZSet<TRow, Z64> b) => AsSet(a + b);

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

    // A ∩ B as sets: positive-weight rows of A that are also positive in B.
    private static ZSet<TRow, Z64> SetIntersection(ZSet<TRow, Z64> a, ZSet<TRow, Z64> b)
    {
        if (a.IsEmpty || b.IsEmpty)
        {
            return ZSet<TRow, Z64>.Empty;
        }

        var builder = new ZSetBuilder<TRow, Z64>();
        foreach (var (row, w) in a)
        {
            if (Z64.IsPositive(w) && Z64.IsPositive(b.WeightOf(row)))
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    private static bool AnyWeightMagnitudeAboveOne(ZSet<TRow, Z64> z)
    {
        foreach (var (_, w) in z)
        {
            if (w.Value is > 1 or < -1)
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
            await import.Trace.SaveAsync(writer, ImportPrefix(import.Name!), cancellationToken).ConfigureAwait(false);
        }

        await _resultCodec!.SaveAsync(writer, ResultFileName, _r, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        EnsureSnapshottable();

        foreach (var import in _imports)
        {
            await import.Trace.LoadAsync(reader, ImportPrefix(import.Name!), cancellationToken).ConfigureAwait(false);
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
                sb.Append(import.Name).Append('=').Append(import.Trace.SchemaFingerprint).Append(';');
            }

            sb.Append("result=").Append(_resultCodec!.SchemaFingerprint);
            return sb.ToString();
        }
    }

    private bool IsSnapshottable =>
        _resultCodec is not null && _imports.All(i => i.Name is not null && i.Trace.CanSnapshot);

    private void EnsureSnapshottable()
    {
        if (!IsSnapshottable)
        {
            throw new NotSupportedException(
                "SemiNaiveFixpointOperator was constructed without snapshot codecs; supply a result codec and " +
                "name + codec for every import to enable snapshot save/load.");
        }
    }

    private static string ImportPrefix(string name) => "import_" + name;

    private sealed record Import(
        Stream<ZSet<TRow, Z64>> Outer,
        Stream<ZSet<TRow, Z64>> Inner,
        IImportTrace<TRow> Trace,
        string? Name);
}
