// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Optimizer;

/// <summary>
/// How <c>PlanOptimizer.NarrowAggregateInput</c> treats <b>non-linear</b> aggregates
/// (MIN / MAX / APPROX_COUNT_DISTINCT / COUNT(DISTINCT)).
/// </summary>
internal enum NonLinearNarrowing
{
    /// <summary>
    /// Narrow when — and only when — the aggregate's input is <i>provably</i>
    /// non-negative (<see cref="PlanWeightPositivity"/>): every base table in its
    /// lineage declared <c>WITH ('append_only' = 'true')</c>, or the lineage passes
    /// through a sign-laundering operator (DISTINCT, an aggregate). The default.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Narrow unconditionally — the caller asserts globally that its streams are
    /// well-formed (no delete of a row that was never inserted), without declaring it
    /// per table. Retained for the A/B benchmarks and for callers whose schemas
    /// predate the <c>append_only</c> property.
    /// </summary>
    Always,

    /// <summary>
    /// Never narrow a non-linear aggregate, whatever the lineage says. This is the
    /// pre-analysis behaviour, kept as the baseline arm of the narrowing benchmarks.
    /// </summary>
    Never,
}

/// <summary>
/// Process-wide (thread-static) selector for the non-linear narrowing rule in
/// <see cref="PlanOptimizer"/> (docs/design-row-representation.md §18 — the term-2 /
/// whole-row-hash lever).
/// </summary>
/// <remarks>
/// <para><b>What the rule does.</b> <c>NarrowAggregateInput</c> projects an
/// aggregate's input down to <c>{group keys, aggregate-argument columns}</c> so the
/// per-group inner multiset stores and whole-row-hashes only the columns the
/// aggregate reads. For <i>linear</i> aggregates (COUNT / SUM / AVG) this is
/// unconditionally sound and always on.</para>
/// <para><b>Why the non-linear case needs a gate.</b> MIN / MAX / DISTINCT read which
/// distinct values have positive weight, not the weight sum. Over an arbitrary
/// <i>signed</i> Z-set, two rows that share the kept columns but differ on a dropped
/// one can cancel to a single zero-weight entry and hide a value the aggregate would
/// have seen. Over a <b>non-negative</b> Z-set they cannot: the narrowed entry's
/// weight is a sum of non-negatives, so it is &gt; 0 iff some positive-weight row
/// exists — exactly the value-presence the aggregate reads. The narrowing keeps the
/// aggregate's <i>argument</i> columns, so collapsing rows that agree on them is
/// invariant for the aggregate itself.</para>
/// <para><b>What changed.</b> Non-negativity used to be a global, unprovable
/// assumption, so the rule was default-off. It is now <i>derived per aggregate</i>
/// from the plan's lineage (<see cref="PlanWeightPositivity"/>) — hence
/// <see cref="NonLinearNarrowing.Auto"/> as the default: on where it is provable,
/// off everywhere else, with no global promise required from the caller.</para>
/// <para>Read at <c>PlanOptimizer.Optimize</c> time. The field is
/// <see cref="ThreadStaticAttribute"/> so concurrent compiles cannot observe each
/// other's value; a caller sets it around its own <c>Optimize</c> call. This mirrors
/// the <c>SpineStagingConfig</c> / <c>FlatAggregateMode</c> gated-seam discipline and
/// needs no plan/compiler signature change.</para>
/// </remarks>
internal static class NonLinearNarrowingMode
{
    /// <summary>
    /// The active policy. Thread-static; defaults to
    /// <see cref="NonLinearNarrowing.Auto"/>.
    /// </summary>
    [ThreadStatic]
    internal static NonLinearNarrowing Mode;
}
