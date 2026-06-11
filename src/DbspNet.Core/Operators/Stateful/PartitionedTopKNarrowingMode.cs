// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Process-wide opt-in for the <b>narrow-key</b> representation of
/// <see cref="PartitionedTopKOp{TRow,TKey}"/> (docs/design-row-representation.md §22 —
/// the q18/q19 row-narrowing lever). When enabled <i>and</i> a single-column
/// <c>ORDER BY</c> extractor was plumbed through, the partitioned TOP-K operator keys
/// its per-partition <c>_accum</c> / <c>_window</c> state by a narrow
/// <c>{order, wideRow}</c> key — hashing on the order value alone (the §22.3
/// "kill-hash" half) and falling back to a whole-row compare only within an
/// equal-order tie group (size 1 for q18's unique <c>date_time</c>) — instead of
/// whole-row-hashing the full 7-column bid row.
/// </summary>
/// <remarks>
/// <para>Default <see langword="false"/> — byte-identical to the whole-row keyed
/// behaviour (<see cref="CircuitBuilder"/>'s <c>PartitionedTopK</c> instantiates the
/// unchanged <see cref="PartitionedTopKOp{TRow,TKey}"/>). Read once at operator
/// construction, so it needs no builder-signature change visible to the typed
/// compiler's reflected <c>BuildPartitionedTopK</c> (which only gains an optional
/// extractor argument at its own call site) — honouring the typed-compiler reflection
/// gotcha — and mirrors the <c>DeltaPoolMode</c> / <c>NonLinearNarrowingMode</c>
/// gated-seam discipline.</para>
/// <para><b>Correctness surface.</b> Unlike join column pruning (§21,
/// unconditionally sound), the narrow path is an incremental operator rewrite under
/// retraction and ties — so it is default-off behind this seam and gated on an
/// equivalence PBT (seam-on ≡ batch oracle and ≡ the whole-row op per tick) plus a
/// q9 non-regression check (q9 shares this operator), matching the §18/§20 envelope
/// discipline. The narrow output is value-equivalent to the whole-row output: the
/// narrow key carries the wide row, recovered directly for the ≤k survivors the
/// output needs.</para>
/// </remarks>
internal static class PartitionedTopKNarrowingMode
{
    /// <summary>
    /// When true, <c>CircuitBuilder.PartitionedTopK</c> builds the narrow-key
    /// <see cref="PartitionedTopKNarrowOp{TRow,TKey}"/> whenever a single-column
    /// order extractor was supplied. Default false (whole-row keyed, byte-identical).
    /// Thread-static: each thread's operators read that thread's compile-scoped value.
    /// </summary>
    [ThreadStatic]
    internal static bool Enabled;
}
