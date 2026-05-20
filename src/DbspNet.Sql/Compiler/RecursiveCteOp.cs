// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Runtime operator for a recursive CTE, evaluated with semi-naïve
/// incremental recursion. Preserves the materialised CTE result
/// <c>R</c> across outer ticks and propagates only the newly-derivable rows
/// induced by each tick's external-input delta — this is what the DBSP paper
/// calls a <i>nested circuit</i>, implemented here directly inside the
/// operator rather than as a reusable Core primitive.
/// </summary>
/// <remarks>
/// <para>
/// <b>Algorithm.</b> On each outer tick, the operator looks at the sign of
/// every external-input delta:
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Any negative weight (retraction).</b> Set semantics don't compose with
/// retraction propagation in a finite-iteration loop (a row removed from
/// <c>R</c> can't be found by "what's new"). We fall back to full batch
/// recomputation, which is correct but pays the same cost as the pre-nested
/// implementation.
/// </item>
/// <item>
/// <b>Pure inserts (or first tick).</b> Semi-naïve:
/// <code>
/// Δ₀ = base(δ_ext) + step(R_prev, δ_ext)       -- derivations enabled by ext change
/// loop:
///   Δ = Δ ∖ R                                   -- truly new rows only
///   if Δ empty: break
///   R = R ∪ Δ
///   Δ = step(Δ, ext_new)                       -- by linearity of step in S
/// </code>
/// Correctness rests on <c>step</c> being linear in the self-reference,
/// which holds for the v1 recursive-body subset (Filter / Project /
/// InnerJoin / UnionAll).
/// </item>
/// </list>
/// <para>
/// <b>Termination.</b> Set semantics plus a finite value domain bound the
/// loop. <see cref="MaxIterations"/> guards against divergent queries.
/// </para>
/// <para>
/// <b>What we gain.</b> For an insert-only workload growing a graph edge at
/// a time, per-tick cost drops from <c>O(|R|)</c> (full closure) to
/// <c>O(|newly reachable|)</c>. For retraction-heavy workloads the cost is
/// the same as before — trading implementation simplicity against the
/// DRED/DBSP-weight-propagation alternative.
/// </para>
/// </remarks>
internal sealed class RecursiveCteOp : IOperator, ISnapshotable
{
    public const int MaxIterations = 10_000;

    private const string ResultFileName = "r.arrows";
    private const string PreviousResultFileName = "prev.arrows";

    private readonly IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> _externalDeltaStreams;
    private readonly Dictionary<string, ZSetTrace<StructuralRow, Z64>> _externalTraces;
    private readonly Stream<ZSet<StructuralRow, Z64>> _output;
    private readonly LogicalPlan _basePlan;
    private readonly LogicalPlan _recursivePlan;
    private readonly CteRef _selfRef;
    private readonly IReadOnlyDictionary<string, IZSetTraceCodec<StructuralRow, Z64>>? _externalSnapshotCodecs;
    private readonly IZSetTraceCodec<StructuralRow, Z64>? _resultSnapshotCodec;
    private ZSet<StructuralRow, Z64> _r = ZSet<StructuralRow, Z64>.Empty;
    private ZSet<StructuralRow, Z64> _previousResult = ZSet<StructuralRow, Z64>.Empty;

    public RecursiveCteOp(
        IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> externalDeltaStreams,
        Stream<ZSet<StructuralRow, Z64>> output,
        LogicalPlan basePlan,
        LogicalPlan recursivePlan,
        CteRef selfRef,
        IReadOnlyDictionary<string, IZSetTraceCodec<StructuralRow, Z64>>? externalSnapshotCodecs = null,
        IZSetTraceCodec<StructuralRow, Z64>? resultSnapshotCodec = null)
    {
        _externalDeltaStreams = externalDeltaStreams;
        _output = output;
        _basePlan = basePlan;
        _recursivePlan = recursivePlan;
        _selfRef = selfRef;
        _externalSnapshotCodecs = externalSnapshotCodecs;
        _resultSnapshotCodec = resultSnapshotCodec;

        _externalTraces = new Dictionary<string, ZSetTrace<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var name in externalDeltaStreams.Keys)
        {
            _externalTraces[name] = new ZSetTrace<StructuralRow, Z64>();
        }
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_externalSnapshotCodecs is null || _resultSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "RecursiveCteOp was constructed without snapshot codecs; compile via " +
                "PlanToCircuit.Compile(plan, snapshotCodecs) to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // External base-table traces — one file per table. Filenames are
        // "ext_{tableName}.arrows".
        foreach (var (name, trace) in _externalTraces)
        {
            if (!_externalSnapshotCodecs.TryGetValue(name, out var codec))
            {
                throw new InvalidOperationException(
                    $"RecursiveCteOp: missing snapshot codec for external table '{name}'");
            }

            await codec.SaveAsync(writer, ExternalTraceFileName(name), trace.Current, cancellationToken).ConfigureAwait(false);
        }

        // The materialised CTE result and prior-tick result share a schema,
        // so a single result codec writes both under separate filenames.
        await _resultSnapshotCodec.SaveAsync(writer, ResultFileName, _r, cancellationToken).ConfigureAwait(false);
        await _resultSnapshotCodec.SaveAsync(writer, PreviousResultFileName, _previousResult, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_externalSnapshotCodecs is null || _resultSnapshotCodec is null)
        {
            throw new NotSupportedException(
                "RecursiveCteOp was constructed without snapshot codecs.");
        }

        foreach (var (name, trace) in _externalTraces)
        {
            if (!_externalSnapshotCodecs.TryGetValue(name, out var codec))
            {
                throw new InvalidOperationException(
                    $"RecursiveCteOp: missing snapshot codec for external table '{name}'");
            }

            trace.Integrate(await codec.LoadAsync(reader, ExternalTraceFileName(name), cancellationToken).ConfigureAwait(false));
        }

        _r = await _resultSnapshotCodec.LoadAsync(reader, ResultFileName, cancellationToken).ConfigureAwait(false);
        _previousResult = await _resultSnapshotCodec.LoadAsync(reader, PreviousResultFileName, cancellationToken).ConfigureAwait(false);
    }

    public string SchemaFingerprint
    {
        get
        {
            if (_externalSnapshotCodecs is null || _resultSnapshotCodec is null)
            {
                return string.Empty;
            }

            // External-table fingerprints in name order so it's stable
            // across reorderings of the underlying dictionary.
            var sb = new System.Text.StringBuilder();
            foreach (var name in _externalSnapshotCodecs.Keys.OrderBy(k => k, StringComparer.Ordinal))
            {
                sb.Append(name).Append('=').Append(_externalSnapshotCodecs[name].SchemaFingerprint).Append(';');
            }

            sb.Append("result=").Append(_resultSnapshotCodec.SchemaFingerprint);
            return sb.ToString();
        }
    }

    private static string ExternalTraceFileName(string tableName) =>
        "ext_" + tableName + ".arrows";

    public void Step()
    {
        // Snapshot this tick's per-table δ BEFORE integrating (we need both
        // the delta-only and the post-integration view).
        var thisTickDeltas = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        var hasRetraction = false;
        foreach (var (name, stream) in _externalDeltaStreams)
        {
            var d = stream.Current;
            thisTickDeltas[name] = d;
            if (ContainsNegativeWeight(d))
            {
                hasRetraction = true;
            }
        }

        // Integrate into traces.
        foreach (var (name, trace) in _externalTraces)
        {
            trace.Integrate(thisTickDeltas[name]);
        }

        var integrated = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var (name, trace) in _externalTraces)
        {
            integrated[name] = trace.Current;
        }

        if (hasRetraction || _r.IsEmpty)
        {
            FullRecompute(integrated);
        }
        else
        {
            IncrementalExtend(integrated, thisTickDeltas);
        }

        _output.SetCurrent(_r - _previousResult);
        _previousResult = _r;
    }

    // --- Full recompute: used on the first tick and on any retraction-containing tick. ---

    private void FullRecompute(Dictionary<string, ZSet<StructuralRow, Z64>> integrated)
    {
        var ctes = new Dictionary<CteRef, ZSet<StructuralRow, Z64>>
        {
            [_selfRef] = ZSet<StructuralRow, Z64>.Empty,
        };
        var ctx = new BatchEvalContext(integrated, ctes);

        var r = AsSet(BatchPlanEvaluator.Evaluate(_basePlan, ctx));
        for (var i = 0; i < MaxIterations; i++)
        {
            ctes[_selfRef] = r;
            var step = AsSet(BatchPlanEvaluator.Evaluate(_recursivePlan, ctx));
            var next = AsSet(r + step);
            if (next.Equals(r))
            {
                _r = r;
                return;
            }

            r = next;
        }

        throw new InvalidOperationException(
            $"recursive CTE did not converge after {MaxIterations} iterations");
    }

    // --- Semi-naïve incremental extension: used on insert-only ticks with a non-empty R. ---

    private void IncrementalExtend(
        Dictionary<string, ZSet<StructuralRow, Z64>> integrated,
        Dictionary<string, ZSet<StructuralRow, Z64>> thisTickDeltas)
    {
        // Δ₀ contributions from this tick's external change:
        //   base(δ_ext): rows the base case newly admits because of δ_ext.
        //   step(R_prev, δ_ext): rows step newly produces because of δ_ext
        //     (keeping self-ref bound at R_prev — linearity of step in ext
        //      means step(R_prev, ext_new) − step(R_prev, ext_prev) = step(R_prev, δ_ext)).
        var deltaCtes = new Dictionary<CteRef, ZSet<StructuralRow, Z64>>
        {
            [_selfRef] = _r,
        };
        var deltaCtx = new BatchEvalContext(thisTickDeltas, deltaCtes);

        var baseDelta = BatchPlanEvaluator.Evaluate(_basePlan, deltaCtx);
        var stepFromExtChange = BatchPlanEvaluator.Evaluate(_recursivePlan, deltaCtx);
        var frontier = SetDifference(AsSet(baseDelta + stepFromExtChange), _r);

        // Semi-naïve inner loop: at each iteration, extend R by the frontier
        // and feed ONLY the frontier (by linearity of step in S) back into
        // step against the full ext_new to discover newly-derivable rows.
        var iterCtes = new Dictionary<CteRef, ZSet<StructuralRow, Z64>>
        {
            [_selfRef] = ZSet<StructuralRow, Z64>.Empty,
        };
        var iterCtx = new BatchEvalContext(integrated, iterCtes);

        for (var i = 0; i < MaxIterations; i++)
        {
            if (frontier.IsEmpty)
            {
                return;
            }

            _r = AsSet(_r + frontier);
            iterCtes[_selfRef] = frontier;
            var stepOut = BatchPlanEvaluator.Evaluate(_recursivePlan, iterCtx);
            frontier = SetDifference(AsSet(stepOut), _r);
        }

        throw new InvalidOperationException(
            $"recursive CTE did not converge after {MaxIterations} iterations");
    }

    // --- Helpers ---

    private static ZSet<StructuralRow, Z64> AsSet(ZSet<StructuralRow, Z64> z)
    {
        if (z.IsEmpty)
        {
            return z;
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in z)
        {
            if (Z64.IsPositive(w))
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    /// <summary>A − B where both are treated as sets; keeps only rows in A
    /// with positive weight that are absent (zero-weight) from B.</summary>
    private static ZSet<StructuralRow, Z64> SetDifference(
        ZSet<StructuralRow, Z64> a,
        ZSet<StructuralRow, Z64> b)
    {
        if (a.IsEmpty)
        {
            return a;
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in a)
        {
            if (!Z64.IsPositive(w))
            {
                continue;
            }

            if (b.WeightOf(row).Value > 0)
            {
                continue;
            }

            builder.Add(row, Z64.One);
        }

        return builder.Build();
    }

    private static bool ContainsNegativeWeight(ZSet<StructuralRow, Z64> z)
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
}
