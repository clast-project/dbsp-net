// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// How <c>CircuitBuilder.PartitionedTopK</c> chooses between the whole-row
/// <see cref="PartitionedTopKOp{TRow,TKey}"/> and the narrow-key
/// <see cref="PartitionedTopKNarrowOp{TRow,TKey}"/> (docs/design-row-representation.md
/// §22.7 — the per-operator selection gate).
/// </summary>
internal enum PartitionedTopKNarrowing
{
    /// <summary>
    /// Per-operator decision (the production default): take the narrow path when a
    /// single-column <c>ORDER BY</c> extractor was plumbed through <i>and</i> the
    /// operator's <c>limit</c> exceeds <see cref="PartitionedTopKNarrowingMode.AutoLimitThreshold"/>.
    /// The narrow path pays only when a partition accumulates enough TOP-K state to
    /// amortise the narrow-key overhead (§22.6/§22.7), so TOP-1 shapes (q18/q9) keep the
    /// whole-row op while accumulating TOP-K windows (q19) take the narrow path.
    /// </summary>
    Auto = 0,

    /// <summary>Force the narrow path whenever a single-column extractor is present —
    /// the A/B "on" arm for the §22 gates (q18narrow, w1profile narrowtopk, the PBT).</summary>
    ForceNarrow,

    /// <summary>Force the whole-row op regardless of limit — the A/B baseline arm, and
    /// the equivalence-PBT reference.</summary>
    ForceWholeRow,
}

/// <summary>
/// Process-wide selection control for the <b>narrow-key</b> representation of
/// <see cref="PartitionedTopKOp{TRow,TKey}"/> (docs/design-row-representation.md §22 —
/// the q18/q19 row-narrowing lever). The narrow path keys a partition's
/// <c>_accum</c> / <c>_window</c> state by a narrow <c>{order, wideRow}</c> key —
/// hashing on the order value alone (the §22.3 "kill-hash" half) and falling back to a
/// whole-row compare only within an equal-order tie group — instead of whole-row-hashing
/// the full bid row.
/// </summary>
/// <remarks>
/// <para><b>Selection is per-operator, not process-wide.</b> §22.6 proved the narrow
/// path is unconditionally <i>correct</i> (≡ batch oracle ≡ whole-row op under
/// retractions/ties) but pays only when a partition holds enough state — q19's
/// accumulating TOP-10 wins (W=1 −27 % time / −55 % alloc, W=8 1.30×), while TOP-1 over
/// size-1 partitions (q18/q9) is flat-to-slightly-negative (q18 +2.3 % alloc — a cached,
/// 1-entry whole-row hash leaves no kill-hash prize). §22.7's crossover sweep set the
/// cheap static predicate to <c>limit &gt; <see cref="AutoLimitThreshold"/></c>, read
/// straight off the plan at both compile sites (no analysis, no reflected-signature
/// change — honouring the typed-compiler reflection gotcha). <see cref="Override"/>
/// defaults to <see cref="PartitionedTopKNarrowing.Auto"/>, so the production default is
/// this gate.</para>
/// <para><see cref="Override"/> is a thread-static force-on/off knob the §22 gates and
/// the equivalence PBT use to pin one arm regardless of limit; production never sets it.
/// Read once at operator construction, so it needs no builder-signature change visible to
/// the typed compiler's reflected <c>BuildPartitionedTopK</c> (which only gains an
/// optional extractor argument at its own call site) — mirroring the <c>DeltaPoolMode</c>
/// / <c>NonLinearNarrowingMode</c> gated-seam discipline.</para>
/// </remarks>
internal static class PartitionedTopKNarrowingMode
{
    /// <summary>
    /// The smallest <c>limit</c> for which the <see cref="PartitionedTopKNarrowing.Auto"/>
    /// gate takes the narrow path. <c>limit &gt; 1</c> — i.e. any genuine TOP-K (k ≥ 2) —
    /// leaving every TOP-1 dedup shape (q18/q9) on the whole-row op, byte-identical to the
    /// pre-§22.7 default. §22.7's real-query sweep: the win tracks per-partition state
    /// size, for which <c>limit</c> is the only cheap static proxy; <c>&gt; 1</c> is the
    /// threshold that provably leaves the measured-flat TOP-1 shapes untouched while
    /// capturing q19.
    /// </summary>
    internal const long AutoLimitThreshold = 1;

    /// <summary>
    /// Force-on/off override for the §22 A/B gates and the equivalence PBT. Default
    /// <see cref="PartitionedTopKNarrowing.Auto"/> (the per-operator limit gate — the
    /// production path). Thread-static: each thread's operators read that thread's
    /// compile-scoped value.
    /// </summary>
    [ThreadStatic]
    internal static PartitionedTopKNarrowing Override;

    /// <summary>
    /// Whether <c>CircuitBuilder.PartitionedTopK</c> should build the narrow-key operator
    /// for an operator with the given <paramref name="limit"/>, assuming a single-column
    /// order extractor is present (the caller checks that). Honours <see cref="Override"/>;
    /// under <see cref="PartitionedTopKNarrowing.Auto"/> applies the
    /// <see cref="AutoLimitThreshold"/> gate.
    /// </summary>
    internal static bool ShouldNarrow(long limit) => Override switch
    {
        PartitionedTopKNarrowing.ForceNarrow => true,
        PartitionedTopKNarrowing.ForceWholeRow => false,
        _ => limit > AutoLimitThreshold,
    };
}
