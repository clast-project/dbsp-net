// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Program-level column-liveness analysis: for each view in a resolved program,
/// which of its <em>output</em> columns are actually read by some output
/// connector or by a downstream live view. A view can produce columns no live
/// consumer reads — most sharply, a window/aggregate column whose only reader is
/// a dead-pruned leaf view — and computing those columns (and the operators that
/// produce them) is pure waste.
///
/// <para>This is the analysis half of the deferred "general top-down column
/// liveness across joins" pass (<c>PlanOptimizer.cs</c>). It does NOT rewrite
/// plans — it computes the live-column sets a rewrite would prune to. The rewrite
/// is a separate, gated step; keeping the analysis standalone lets us validate the
/// column bookkeeping against the real compiler before any plan is mutated.</para>
///
/// <para><b>Soundness by conservative fallback.</b> The per-node rules
/// <em>over-approximate</em> liveness: a node type this pass does not model
/// precisely marks all of its input columns live. Over-approximating liveness can
/// only cause fewer columns to be pruned, never an unsound prune — so the pass can
/// model just the hot-path node kinds precisely and treat the rest conservatively,
/// then be generalised node-by-node without ever risking correctness.</para>
/// </summary>
public static class PlanColumnLiveness
{
    /// <summary>
    /// For every view/table name in <paramref name="views"/>, the set of its
    /// output-column indices that are live. Output views seed as fully live (their
    /// connector writes the whole declared schema); liveness then flows backward
    /// over the dependency-ordered view list to each referenced relation. A name
    /// absent from the result is unreached (dead view) — no live consumer.
    /// </summary>
    public static Dictionary<string, HashSet<int>> ComputeProgramLiveColumns(
        IReadOnlyList<ProgramView> views)
    {
        System.ArgumentNullException.ThrowIfNull(views);

        var live = new Dictionary<string, HashSet<int>>(System.StringComparer.Ordinal);

        // Backward over the topologically-ordered views (each view precedes every
        // view that references it), mirroring CompileProgram's dead-view pass. By
        // the time we process view i, all its consumers (indices > i) have already
        // contributed their column demand into live[view i].
        for (var i = views.Count - 1; i >= 0; i--)
        {
            var v = views[i];
            if (v.IsOutput)
            {
                Demand(live, v.ViewName, FullSet(v.Query.Schema.Count));
            }

            if (!live.TryGetValue(v.ViewName, out var liveOut))
            {
                continue; // unreached: nothing downstream reads this view
            }

            foreach (var (name, cols) in LiveScanColumns(v.Query, liveOut))
            {
                Demand(live, name, cols);
            }
        }

        return live;
    }

    /// <summary>
    /// Given a view's plan and the set of its output-column indices that are live,
    /// the live column indices demanded at each named <see cref="ScanPlan"/> leaf
    /// (a base table or another view). Recurses through CTE bodies so a scan of a
    /// view inside a <c>WITH</c> clause is attributed to that view.
    /// </summary>
    public static Dictionary<string, HashSet<int>> LiveScanColumns(
        LogicalPlan plan, IReadOnlySet<int> liveOut)
    {
        System.ArgumentNullException.ThrowIfNull(plan);
        System.ArgumentNullException.ThrowIfNull(liveOut);

        var acc = new Dictionary<string, HashSet<int>>(System.StringComparer.Ordinal);
        Walk(plan, new HashSet<int>(liveOut), acc, new HashSet<CteRef>());
        return acc;
    }

    // Push the set of live output columns of `p` down to its inputs, accumulating
    // per-scan-name demand into `acc`. `active` guards against CTE self-reference
    // cycles (recursive CTEs).
    private static void Walk(
        LogicalPlan p, HashSet<int> liveOut,
        Dictionary<string, HashSet<int>> acc, HashSet<CteRef> active)
    {
        switch (p)
        {
            case ScanPlan s:
                Demand(acc, s.TableName, Clamp(liveOut, s.Schema.Count));
                break;

            case CteScanPlan c:
                // A CTE referenced more than once accumulates the UNION of its
                // references' demands — walking the body per reference and unioning
                // into `acc` yields exactly that. Skip on cycle (recursive back-edge).
                if (active.Add(c.Cte))
                {
                    Walk(c.Cte.Plan, new HashSet<int>(liveOut), acc, active);
                    active.Remove(c.Cte);
                }

                break;

            case ProjectPlan pr:
            {
                var liveIn = new HashSet<int>();
                foreach (var i in liveOut)
                {
                    if (i < pr.Projections.Count)
                    {
                        liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(pr.Projections[i].Expression));
                    }
                }

                Walk(pr.Input, liveIn, acc, active);
                break;
            }

            case FilterPlan f:
            {
                // Filter preserves schema (output index == input index) but the
                // predicate reads columns that must stay live.
                var liveIn = new HashSet<int>(liveOut);
                liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(f.Predicate));
                Walk(f.Input, liveIn, acc, active);
                break;
            }

            case JoinPlan j:
            {
                var leftCount = j.Left.Schema.Count;
                var leftLive = new HashSet<int>();
                var rightLive = new HashSet<int>();
                foreach (var i in liveOut)
                {
                    if (i < leftCount)
                    {
                        leftLive.Add(i);
                    }
                    else
                    {
                        rightLive.Add(i - leftCount);
                    }
                }

                // Equi-keys and the cross-side residual are always live: they decide
                // which rows match, independent of what the parent projects.
                foreach (var eq in j.EquiKeys)
                {
                    leftLive.Add(eq.LeftIndex);
                    rightLive.Add(eq.RightIndex);
                }

                if (j.Residual is { } residual)
                {
                    foreach (var idx in ExpressionRewriter.CollectColumnIndices(residual))
                    {
                        if (idx < leftCount)
                        {
                            leftLive.Add(idx);
                        }
                        else
                        {
                            rightLive.Add(idx - leftCount);
                        }
                    }
                }

                Walk(j.Left, leftLive, acc, active);
                Walk(j.Right, rightLive, acc, active);
                break;
            }

            case AggregatePlan a:
            {
                // Conservative (sound): group keys plus EVERY aggregate argument.
                // (A refinement would drop a dead aggregate's args; not needed for
                // the win this pass targets and kept simple for correctness.)
                var liveIn = new HashSet<int>();
                foreach (var g in a.GroupKeys)
                {
                    liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(g));
                }

                foreach (var agg in a.Aggregates)
                {
                    if (agg.Argument is { } arg)
                    {
                        liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(arg));
                    }
                }

                Walk(a.Input, liveIn, acc, active);
                break;
            }

            case WindowAggregatePlan wa:
            {
                var inCount = wa.Input.Schema.Count;
                var liveIn = new HashSet<int>();
                var anyAggLive = false;
                foreach (var i in liveOut)
                {
                    if (i < inCount)
                    {
                        liveIn.Add(i); // passthrough column
                    }
                    else
                    {
                        anyAggLive = true; // a computed window column is read
                    }
                }

                // Partition/order/argument columns are needed ONLY to compute the
                // window's own output. If no produced window column is live, the
                // whole operator is dead — do not force its key columns live (this
                // is what lets a fully-dead window op be eliminated upstream).
                if (anyAggLive)
                {
                    AddWindowKeys(liveIn, wa.PartitionKeys, wa.OrderKey is { } ok ? new[] { ok } : null);
                    for (var k = 0; k < wa.Aggregates.Count; k++)
                    {
                        if (liveOut.Contains(inCount + k) && wa.Aggregates[k].Argument is { } arg)
                        {
                            liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(arg));
                        }
                    }
                }

                Walk(wa.Input, liveIn, acc, active);
                break;
            }

            case WindowOffsetPlan wo:
            {
                var inCount = wo.Input.Schema.Count;
                var liveIn = new HashSet<int>();
                var anyFnLive = false;
                foreach (var i in liveOut)
                {
                    if (i < inCount)
                    {
                        liveIn.Add(i);
                    }
                    else
                    {
                        anyFnLive = true;
                    }
                }

                if (anyFnLive)
                {
                    AddWindowKeys(liveIn, wo.PartitionKeys, wo.OrderKeys);
                    for (var k = 0; k < wo.Functions.Count; k++)
                    {
                        if (liveOut.Contains(inCount + k))
                        {
                            liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(wo.Functions[k].Value));
                        }
                    }
                }

                Walk(wo.Input, liveIn, acc, active);
                break;
            }

            case UnionAllPlan u:
                // Branches are position-aligned to the same schema; the same live
                // set applies to every branch.
                foreach (var branch in u.Branches)
                {
                    Walk(branch, new HashSet<int>(liveOut), acc, active);
                }

                break;

            case DistinctPlan d:
                // Every input column is part of the dedup key — all live.
                Walk(d.Input, FullSet(d.Input.Schema.Count), acc, active);
                break;

            case DifferencePlan diff:
                Walk(diff.Left, FullSet(diff.Left.Schema.Count), acc, active);
                Walk(diff.Right, FullSet(diff.Right.Schema.Count), acc, active);
                break;

            // --- conservative (all-input-live) for node kinds not yet modelled ---
            case TemporalFilterPlan tf:
                MarkAllLive(tf.Input, acc, active);
                break;
            case SemiJoinPlan sj:
                MarkAllLive(sj.Input, acc, active);
                MarkAllLive(sj.Subquery, acc, active);
                break;
            case ScalarSubqueryJoinPlan s:
                MarkAllLive(s.Input, acc, active);
                foreach (var sub in s.Subqueries)
                {
                    MarkAllLive(sub, acc, active);
                }

                break;
            case CorrelatedScalarSubqueryJoinPlan csp:
                MarkAllLive(csp.Input, acc, active);
                MarkAllLive(csp.Subquery, acc, active);
                break;
            case TopKPlan t:
                MarkAllLive(t.Input, acc, active);
                break;
            case PartitionedTopKPlan pt:
                MarkAllLive(pt.Input, acc, active);
                break;
            case PartitionedRankPlan prk:
                MarkAllLive(prk.Input, acc, active);
                break;
            case RecursiveCtePlan r:
                active.Add(r.SelfRef);
                MarkAllLive(r.BasePlan, acc, active);
                MarkAllLive(r.RecursivePlan, acc, active);
                break;

            default:
                throw new System.InvalidOperationException($"unsupported plan node {p.GetType().Name}");
        }
    }

    // Walk `p` with every one of its output columns live — the conservative path.
    private static void MarkAllLive(
        LogicalPlan p, Dictionary<string, HashSet<int>> acc, HashSet<CteRef> active) =>
        Walk(p, FullSet(p.Schema.Count), acc, active);

    private static void AddWindowKeys(
        HashSet<int> liveIn,
        IReadOnlyList<ResolvedExpression> partitionKeys,
        IReadOnlyList<SortKey>? orderKeys)
    {
        foreach (var pk in partitionKeys)
        {
            liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(pk));
        }

        if (orderKeys is not null)
        {
            foreach (var ok in orderKeys)
            {
                liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(ok.Expression));
            }
        }
    }

    private static HashSet<int> FullSet(int count)
    {
        var s = new HashSet<int>(count);
        for (var i = 0; i < count; i++)
        {
            s.Add(i);
        }

        return s;
    }

    private static HashSet<int> Clamp(IReadOnlySet<int> cols, int count)
    {
        var s = new HashSet<int>();
        foreach (var c in cols)
        {
            if (c >= 0 && c < count)
            {
                s.Add(c);
            }
        }

        return s;
    }

    private static void Demand(Dictionary<string, HashSet<int>> map, string name, IEnumerable<int> cols)
    {
        if (!map.TryGetValue(name, out var set))
        {
            set = new HashSet<int>();
            map[name] = set;
        }

        set.UnionWith(cols);
    }
}
