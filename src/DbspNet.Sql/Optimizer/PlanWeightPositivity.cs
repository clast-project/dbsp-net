// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Static lineage analysis: is a plan's cumulative Z-set guaranteed
/// <b>non-negative</b> at every tick — i.e. can no row ever hold a negative
/// weight in the integrated relation?
/// </summary>
/// <remarks>
/// <para><b>Why this exists.</b> Several rewrites are sound over a bag but not over
/// an arbitrary <i>signed</i> Z-set, because a signed Z-set lets two rows cancel and
/// hide a value the operator would otherwise have seen. The first consumer is
/// <c>PlanOptimizer.NarrowAggregateInput</c>, which without this analysis has to bail
/// on MIN / MAX / COUNT(DISTINCT) / APPROX_COUNT_DISTINCT and keep the full input row
/// in the aggregate's inner multiset (docs/design-row-representation.md §18).</para>
/// <para><b>Two sources of non-negativity.</b>
/// <list type="number">
/// <item><b>Declared</b> — a base table carrying <c>WITH ('append_only' = 'true')</c>.
/// This is a contract the user asserts and the engine does not police, exactly like
/// <c>NOT NULL</c>.</item>
/// <item><b>Derived</b> — an operator whose output is non-negative <i>whatever</i> its
/// input signs. <c>DISTINCT</c> is the canonical one: its cumulative output weight per
/// row is 1 when the cumulative input weight is positive and 0 otherwise
/// (<c>DistinctOp</c>), so it launders sign. An aggregate is another: it emits one row
/// per live group, so its integral is 1 per group. Everything below such a node is
/// irrelevant, which is why those cases do not recurse.</item>
/// </list></para>
/// <para><b>Conservative by construction.</b> Every node this analysis does not
/// explicitly understand answers <c>false</c>. <see cref="DifferencePlan"/> answers
/// <c>false</c> because <c>a − b</c> is precisely how a negative weight is created;
/// <see cref="RecursiveCtePlan"/> answers <c>false</c> rather than reason about a
/// fixpoint. A wrong <c>false</c> costs an optimization; a wrong <c>true</c> costs
/// correctness.</para>
/// </remarks>
internal static class PlanWeightPositivity
{
    /// <summary>
    /// True when <paramref name="plan"/>'s integrated Z-set provably has no negative
    /// weights, given the <c>append_only</c> declarations its scans carry.
    /// </summary>
    public static bool IsNonNegative(LogicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        return IsNonNegative(plan, new HashSet<CteRef>());
    }

    private static bool IsNonNegative(LogicalPlan plan, HashSet<CteRef> activeCtes) => plan switch
    {
        // --- declared ---
        ScanPlan s => s.AppendOnly,

        // --- sign-laundering: non-negative regardless of what feeds them ---

        // Cumulative output weight is 1 (input cumulative weight positive) or 0.
        DistinctPlan => true,

        // One row per live group, weight 1 — a group whose rows cancel away emits a
        // retraction and disappears; it never goes negative.
        AggregatePlan => true,

        // --- weight-preserving / multiplicative: non-negative iff the inputs are ---

        // Filter drops rows; Project may sum the weights of rows that collapse
        // together. Sums of non-negatives stay non-negative.
        FilterPlan f => IsNonNegative(f.Input, activeCtes),
        TemporalFilterPlan tf => IsNonNegative(tf.Input, activeCtes),
        ProjectPlan p => IsNonNegative(p.Input, activeCtes),

        // Join weights multiply; UnionAll sums.
        JoinPlan j => IsNonNegative(j.Left, activeCtes) && IsNonNegative(j.Right, activeCtes),
        UnionAllPlan u => AllNonNegative(u.Branches, activeCtes),

        // Row-preserving windowing: one output row per input row.
        WindowAggregatePlan wa => IsNonNegative(wa.Input, activeCtes),
        WindowOffsetPlan wo => IsNonNegative(wo.Input, activeCtes),

        // Rank / TOP-K retain a subset of input rows with their weights.
        TopKPlan t => IsNonNegative(t.Input, activeCtes),
        PartitionedTopKPlan pt => IsNonNegative(pt.Input, activeCtes),
        PartitionedRankPlan pr => IsNonNegative(pr.Input, activeCtes),

        // Semi-join compiles to Distinct(Subquery) ⋈ Input: the subquery side is
        // laundered to 0/1 by the Distinct, so only the outer side's sign matters.
        SemiJoinPlan sj => IsNonNegative(sj.Input, activeCtes),

        // Scalar-subquery joins append columns from an aggregated subquery; the outer
        // row's weight carries through, so require both sides to be safe.
        ScalarSubqueryJoinPlan ss =>
            IsNonNegative(ss.Input, activeCtes) && AllNonNegative(ss.Subqueries, activeCtes),
        CorrelatedScalarSubqueryJoinPlan cs =>
            IsNonNegative(cs.Input, activeCtes) && IsNonNegative(cs.Subquery, activeCtes),

        // A CTE reference is its body.
        CteScanPlan c => CteBodyIsNonNegative(c.Cte, activeCtes),

        // --- creates negative weights, or not understood ---
        _ => false,
    };

    private static bool AllNonNegative(IReadOnlyList<LogicalPlan> plans, HashSet<CteRef> activeCtes)
    {
        foreach (var p in plans)
        {
            if (!IsNonNegative(p, activeCtes))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Walk a CTE body, guarding the recursive back-edge: a <see cref="CteRef"/>
    /// already on the walk means a cycle, which only a recursive CTE produces — and
    /// those answer <c>false</c> anyway. The ref is popped afterwards so a CTE
    /// referenced twice in sibling positions is not mistaken for a cycle.
    /// </summary>
    private static bool CteBodyIsNonNegative(CteRef cte, HashSet<CteRef> activeCtes)
    {
        if (!activeCtes.Add(cte))
        {
            return false;
        }

        try
        {
            return IsNonNegative(cte.Plan, activeCtes);
        }
        finally
        {
            activeCtes.Remove(cte);
        }
    }
}
