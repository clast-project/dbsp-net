// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Process-wide opt-in for narrowing the input of <b>non-linear</b> aggregates
/// (MIN / MAX / APPROX_COUNT_DISTINCT / COUNT(DISTINCT)) in
/// <see cref="PlanOptimizer"/>'s <c>NarrowAggregateInput</c> rule
/// (docs/design-row-representation.md §18 — the term-2 / whole-row-hash lever).
/// </summary>
/// <remarks>
/// <para>By default the narrowing rule <b>bails</b> on these aggregates: it keeps
/// the full input row in the aggregate's inner multiset so that, over an arbitrary
/// <i>signed</i> Z-set, two rows sharing the kept columns but differing on dropped
/// ones cannot cancel to a single zero-weight entry and hide a value the aggregate
/// would have seen.</para>
/// <para>When <see cref="Enabled"/> is set, the rule narrows the input to
/// <c>{group keys, aggregate-argument columns}</c> even for these aggregates. This
/// is sound whenever the per-group consolidated multiset is <b>non-negative</b> —
/// i.e. for any <i>well-formed</i> insert/delete stream (a row is only deleted if
/// previously inserted, so the per-group integral is a valid bag). Under that
/// (universal in practice) condition, every retained entry has weight ≥ 0, so the
/// narrowed entry weight <c>Σ</c> over rows sharing the kept columns is &gt; 0 iff
/// some positive-weight row exists — exactly the value-presence MIN/MAX/DISTINCT
/// read. The narrowing keeps the aggregate's <i>argument</i> columns, so collapsing
/// two rows that share them is invariant for the aggregate (it reads only those
/// columns). The bail (default) remains the safe choice for callers that cannot
/// assume well-formed input.</para>
/// <para>Read at <see cref="PlanOptimizer.Optimize"/> time. The field is
/// <see cref="ThreadStaticAttribute"/> so concurrent compiles cannot observe each
/// other's value; a caller sets it around its own <c>Optimize</c> call. This
/// mirrors the <c>SpineStagingConfig</c> / <c>FlatAggregateMode</c> gated-seam
/// discipline and needs no plan/compiler signature change.</para>
/// </remarks>
internal static class NonLinearNarrowingMode
{
    /// <summary>
    /// When true, <c>NarrowAggregateInput</c> narrows the input of non-linear
    /// aggregates too (sound for well-formed / non-negative streams). Default
    /// false — byte-identical to the conservative behaviour. Thread-static.
    /// </summary>
    [ThreadStatic]
    internal static bool Enabled;
}
