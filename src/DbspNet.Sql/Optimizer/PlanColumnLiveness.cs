// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

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

    /// <summary>
    /// Arity-preserving dead-column rewrite: given a view plan and the set of its
    /// output columns that are live, return a plan with the SAME output schema in
    /// which (i) a fully-dead <see cref="WindowAggregatePlan"/>/<see cref="WindowOffsetPlan"/>
    /// (no produced column live) is replaced by a projection that reproduces its
    /// schema with the produced columns as NULL constants — eliminating the operator —
    /// and (ii) a dead projection item becomes a NULL constant. Output arity is
    /// preserved at every node, so no downstream view's column indices shift.
    /// GROUP BY / DISTINCT / join keys stay live (never NULLed at their producer), so
    /// row multiplicity is unchanged. Single-reference CTE bodies are pruned; a CTE
    /// referenced more than once is left intact (sound — conservative).
    /// </summary>
    public static LogicalPlan PruneDeadColumns(LogicalPlan plan, IReadOnlySet<int> liveOut)
    {
        System.ArgumentNullException.ThrowIfNull(plan);
        System.ArgumentNullException.ThrowIfNull(liveOut);
        var refCount = new Dictionary<CteRef, int>();
        CountCteRefs(plan, refCount);
        return Prune(plan, new HashSet<int>(liveOut), refCount);
    }

    private static void CountCteRefs(LogicalPlan p, Dictionary<CteRef, int> refCount)
    {
        if (p is CteScanPlan c)
        {
            if (refCount.TryGetValue(c.Cte, out var n))
            {
                refCount[c.Cte] = n + 1;
                return; // body already counted on first sight
            }

            refCount[c.Cte] = 1;
            CountCteRefs(c.Cte.Plan, refCount);
            return;
        }

        foreach (var child in Children(p))
        {
            CountCteRefs(child, refCount);
        }
    }

    private static LogicalPlan Prune(
        LogicalPlan p, HashSet<int> liveOut, Dictionary<CteRef, int> refCount)
    {
        switch (p)
        {
            case ScanPlan:
                return p;

            case CteScanPlan c:
            {
                // Only single-reference CTE bodies are pruned per-reference; a shared
                // CTE would need the union of its references' demands (deferred).
                if (refCount.GetValueOrDefault(c.Cte) != 1)
                {
                    return p;
                }

                var body = Prune(c.Cte.Plan, liveOut, refCount);
                return ReferenceEquals(body, c.Cte.Plan)
                    ? p
                    : new CteScanPlan(new CteRef(c.Cte.Name, body), c.Schema);
            }

            case ProjectPlan pr:
            {
                var items = new ProjectionItem[pr.Projections.Count];
                var liveIn = new HashSet<int>();
                var changed = false;
                for (var i = 0; i < pr.Projections.Count; i++)
                {
                    if (liveOut.Contains(i))
                    {
                        items[i] = pr.Projections[i];
                        liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(pr.Projections[i].Expression));
                    }
                    else
                    {
                        items[i] = NullItem(pr.Projections[i].Name, pr.Projections[i].Qualifier, pr.Schema.Columns[i].Type);
                        changed = true;
                    }
                }

                var input = Prune(pr.Input, liveIn, refCount);
                return !changed && ReferenceEquals(input, pr.Input)
                    ? pr
                    : new ProjectPlan(input, items, pr.Schema);
            }

            case FilterPlan f:
            {
                var liveIn = new HashSet<int>(liveOut);
                liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(f.Predicate));
                var input = Prune(f.Input, liveIn, refCount);
                return ReferenceEquals(input, f.Input) ? f : new FilterPlan(input, f.Predicate);
            }

            case WindowAggregatePlan wa:
            {
                var inCount = wa.Input.Schema.Count;
                if (!AnyProducedLive(liveOut, inCount))
                {
                    return EliminateWindow(wa, wa.Input, inCount, liveOut, refCount);
                }

                var liveIn = WindowLiveIn(
                    liveOut, inCount, wa.PartitionKeys, wa.OrderKey is { } ok ? new[] { ok } : null,
                    k => wa.Aggregates[k].Argument);
                var input = Prune(wa.Input, liveIn, refCount);
                return ReferenceEquals(input, wa.Input) ? wa : wa with { Input = input };
            }

            case WindowOffsetPlan wo:
            {
                var inCount = wo.Input.Schema.Count;
                if (!AnyProducedLive(liveOut, inCount))
                {
                    return EliminateWindow(wo, wo.Input, inCount, liveOut, refCount);
                }

                var liveIn = WindowLiveIn(
                    liveOut, inCount, wo.PartitionKeys, wo.OrderKeys, k => wo.Functions[k].Value);
                var input = Prune(wo.Input, liveIn, refCount);
                return ReferenceEquals(input, wo.Input) ? wo : wo with { Input = input };
            }

            case JoinPlan j:
            {
                var leftCount = j.Left.Schema.Count;
                var leftLive = new HashSet<int>();
                var rightLive = new HashSet<int>();
                foreach (var i in liveOut)
                {
                    (i < leftCount ? leftLive : rightLive).Add(i < leftCount ? i : i - leftCount);
                }

                foreach (var eq in j.EquiKeys)
                {
                    leftLive.Add(eq.LeftIndex);
                    rightLive.Add(eq.RightIndex);
                }

                if (j.Residual is { } residual)
                {
                    foreach (var idx in ExpressionRewriter.CollectColumnIndices(residual))
                    {
                        (idx < leftCount ? leftLive : rightLive).Add(idx < leftCount ? idx : idx - leftCount);
                    }
                }

                var left = Prune(j.Left, leftLive, refCount);
                var right = Prune(j.Right, rightLive, refCount);
                return ReferenceEquals(left, j.Left) && ReferenceEquals(right, j.Right)
                    ? j
                    : j with { Left = left, Right = right };
            }

            case AggregatePlan a:
            {
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

                var input = Prune(a.Input, liveIn, refCount);
                return ReferenceEquals(input, a.Input) ? a : a with { Input = input };
            }

            case UnionAllPlan u:
            {
                var branches = u.Branches.Select(b => Prune(b, new HashSet<int>(liveOut), refCount)).ToList();
                return branches.Zip(u.Branches).All(t => ReferenceEquals(t.First, t.Second))
                    ? u
                    : u with { Branches = branches };
            }

            case DistinctPlan d:
            {
                var input = Prune(d.Input, FullSet(d.Input.Schema.Count), refCount);
                return ReferenceEquals(input, d.Input) ? d : new DistinctPlan(input);
            }

            case DifferencePlan diff:
            {
                var left = Prune(diff.Left, FullSet(diff.Left.Schema.Count), refCount);
                var right = Prune(diff.Right, FullSet(diff.Right.Schema.Count), refCount);
                return ReferenceEquals(left, diff.Left) && ReferenceEquals(right, diff.Right)
                    ? diff
                    : new DifferencePlan(left, right);
            }

            // Conservative for the remaining node kinds (TopK / semi / scalar /
            // correlated / temporal / recursive): leave them intact. Pruning below
            // them would use full liveness (their inputs are all demanded), under
            // which nothing is dead — so the subtree is unchanged anyway. Returning
            // the node as-is is the same result without reconstructing it.
            default:
                return p;
        }
    }

    private static IEnumerable<LogicalPlan> Children(LogicalPlan p) => p switch
    {
        ScanPlan => System.Array.Empty<LogicalPlan>(),
        CteScanPlan => System.Array.Empty<LogicalPlan>(), // body handled by the caller
        FilterPlan f => new[] { f.Input },
        TemporalFilterPlan tf => new[] { tf.Input },
        ProjectPlan pr => new[] { pr.Input },
        JoinPlan j => new[] { j.Left, j.Right },
        AggregatePlan a => new[] { a.Input },
        WindowAggregatePlan wa => new[] { wa.Input },
        WindowOffsetPlan wo => new[] { wo.Input },
        UnionAllPlan u => u.Branches,
        DistinctPlan d => new[] { d.Input },
        DifferencePlan diff => new[] { diff.Left, diff.Right },
        ScalarSubqueryJoinPlan s => new[] { s.Input }.Concat(s.Subqueries),
        SemiJoinPlan sj => new[] { sj.Input, sj.Subquery },
        CorrelatedScalarSubqueryJoinPlan csp => new[] { csp.Input, csp.Subquery },
        TopKPlan t => new[] { t.Input },
        PartitionedTopKPlan pt => new[] { pt.Input },
        PartitionedRankPlan prk => new[] { prk.Input },
        RecursiveCtePlan r => new[] { r.BasePlan, r.RecursivePlan },
        _ => System.Array.Empty<LogicalPlan>(),
    };

    // Replace a fully-dead window/offset op with a projection reproducing its schema:
    // input columns pass through by identity, the produced (window) columns become
    // NULL constants. The op — and its partition/order/argument work — is gone.
    private static LogicalPlan EliminateWindow(
        LogicalPlan windowOp, LogicalPlan windowInput, int inCount,
        HashSet<int> liveOut, Dictionary<CteRef, int> refCount)
    {
        var passLive = new HashSet<int>();
        foreach (var i in liveOut)
        {
            if (i < inCount)
            {
                passLive.Add(i);
            }
        }

        var input = Prune(windowInput, passLive, refCount);
        var schema = windowOp.Schema;
        var items = new ProjectionItem[schema.Count];
        for (var i = 0; i < inCount; i++)
        {
            var col = windowInput.Schema.Columns[i];
            items[i] = new ProjectionItem(new ResolvedColumn(i, col.Type), schema.Columns[i].Name, schema.Columns[i].Qualifier);
        }

        for (var i = inCount; i < schema.Count; i++)
        {
            items[i] = NullItem(schema.Columns[i].Name, schema.Columns[i].Qualifier, schema.Columns[i].Type);
        }

        return new ProjectPlan(input, items, schema);
    }

    private static bool AnyProducedLive(HashSet<int> liveOut, int inCount)
    {
        foreach (var i in liveOut)
        {
            if (i >= inCount)
            {
                return true;
            }
        }

        return false;
    }

    private static HashSet<int> WindowLiveIn(
        HashSet<int> liveOut, int inCount,
        IReadOnlyList<ResolvedExpression> partitionKeys, IReadOnlyList<SortKey>? orderKeys,
        System.Func<int, ResolvedExpression?> argAt)
    {
        var liveIn = new HashSet<int>();
        foreach (var i in liveOut)
        {
            if (i < inCount)
            {
                liveIn.Add(i);
            }
            else if (argAt(i - inCount) is { } arg)
            {
                liveIn.UnionWith(ExpressionRewriter.CollectColumnIndices(arg));
            }
        }

        AddWindowKeys(liveIn, partitionKeys, orderKeys);
        return liveIn;
    }

    private static ProjectionItem NullItem(string name, string? qualifier, SqlType type) =>
        new(new ResolvedLiteral(LiteralKind.Null, null, type.WithNullable(true)), name, qualifier);

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
