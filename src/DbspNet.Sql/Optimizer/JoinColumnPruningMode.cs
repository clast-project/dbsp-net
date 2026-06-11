// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Process-wide opt-in for <b>projection pushdown through an INNER join</b> — the
/// column-liveness rule <see cref="PlanOptimizer"/> documents as "not yet applied:
/// general top-down column liveness across joins" (docs/design-row-representation.md
/// §21 — the term-2 / whole-row-hash lever the §18 aggregate-input narrowing cannot
/// reach because it sits <i>above</i> the join).
/// </summary>
/// <remarks>
/// <para><b>The cost it attacks.</b> An <c>IncrementalJoinOp</c> stores each input
/// side as an <c>IndexedZSet&lt;joinKey, storedRow&gt;</c> whose inner Z-set is keyed
/// by the <b>whole stored row</b>, re-hashed on every <c>MergeInPlace</c> integrate
/// and re-touched by the cross-product probe (§14.2). With no column-liveness rule the
/// full source row is stored even when only a few of its columns are live above the
/// join — e.g. q4's join stores all ~10 auction and ~7 bid columns when only
/// <c>{id,category,date_time,expires}</c> and <c>{auction,price,date_time}</c> are read
/// by the aggregate, residual, and equi-key. The <c>reprbench</c> idx microbench prices
/// this at 40–58% of the join-trace per-row cost (§21).</para>
/// <para><b>Why it is unconditionally sound</b> (unlike <see cref="NonLinearNarrowingMode"/>'s
/// MIN/MAX non-negativity envelope). The rule drops only columns <i>no</i> consumer reads
/// (not the parent, not the join's combine/residual/equi-key). Two stored rows that are
/// identical on the kept columns but differ on a dropped one produce <b>identical</b> join
/// output rows — the dropped column appears in no output — so they consolidate to the same
/// weight in the output Z-set whether collapsed before the join (pruned) or after. The join
/// is bilinear and no aggregator reads the dropped column <i>through</i> the join (the
/// aggregate's argument columns are kept, being referenced above), so there is no
/// value-presence hazard. Holds for arbitrary <i>signed</i> Z-sets — this is ordinary
/// relational projection pushdown, valid by construction.</para>
/// <para>Kept default-off behind this seam while the rule is benchmark-gated, joining the
/// <c>NonLinearNarrowingMode</c> / <c>DeltaPoolMode</c> / <c>FlatAggregateMode</c> family.
/// Read at <see cref="PlanOptimizer.Optimize"/> time; <see cref="ThreadStaticAttribute"/>
/// so concurrent compiles cannot observe each other's value. No plan/compiler signature
/// change (dodges the typed-compiler reflection gotcha). Scoped to INNER joins in v1.</para>
/// </remarks>
internal static class JoinColumnPruningMode
{
    /// <summary>
    /// When true, <c>PruneJoinInputs</c> inserts narrowing projections on an INNER
    /// join's inputs, dropping every column no consumer (parent, combine, residual,
    /// equi-key) reads. Default false — byte-identical to today. Thread-static.
    /// </summary>
    [ThreadStatic]
    internal static bool Enabled;
}
