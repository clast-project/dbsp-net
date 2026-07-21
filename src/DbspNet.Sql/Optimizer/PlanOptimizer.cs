// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Rule-based <see cref="LogicalPlan"/> rewriter. Implements three
/// families of optimizations:
/// <list type="bullet">
/// <item>
/// <b>Predicate pushdown.</b> Filters are pushed as close to the scans as
/// semantically safe — past projections (via expression substitution), into
/// join inputs (per-conjunct, respecting outer-join restrictions), into
/// union branches, past Distinct, and into both sides of Difference.
/// Adjacent filters merge. The win is smaller intermediate Z-sets in the
/// stateful operators (join traces, aggregate multisets).
/// </item>
/// <item>
/// <b>Projection composition.</b> Adjacent
/// <see cref="ProjectPlan"/>s fuse via expression substitution, eliminating
/// the intermediate <c>MapRows</c>.
/// </item>
/// <item>
/// <b>Aggregate-input column pruning.</b> Inserts a narrowing
/// <see cref="ProjectPlan"/> between an <see cref="AggregatePlan"/> and
/// its input when the input has more columns than the aggregate
/// references (group keys + aggregate arguments). Shrinks the
/// per-group multiset that <c>IncrementalAggregateOp</c> integrates
/// per step. Sound under the linear emission gate landed alongside
/// <c>CompositeAggregator.SumWeights()</c>; without that, the
/// narrowing could change which groups emit (see
/// project_projection_pushdown_blocked memory note).
/// </item>
/// </list>
/// Not yet applied: general top-down column liveness across joins,
/// constant folding, join reordering. See <c>docs/skipped.md</c>.
/// </summary>
/// <remarks>
/// The optimizer is an explicit pass — <see cref="PlanToCircuit.Compile"/>
/// does <b>not</b> invoke it automatically. Callers that want optimized
/// queries wrap: <c>PlanToCircuit.Compile(PlanOptimizer.Optimize(plan))</c>.
/// The random-query PBT does this and tests semantic equivalence against a
/// batch evaluation over the <i>unoptimized</i> plan — any divergence is an
/// optimizer bug.
/// </remarks>
public static class PlanOptimizer
{
    private const int MaxIterations = 50;

    public static LogicalPlan Optimize(LogicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);

        // Apply rules iteratively to a fixed point. Most queries converge in
        // 1–3 iterations; the guard prevents any accidental infinite rewrite
        // cycle from hanging the compiler.
        var current = plan;
        for (var i = 0; i < MaxIterations; i++)
        {
            var next = OptimizeNode(current);
            if (ReferenceEquals(next, current))
            {
                current = next;
                break;
            }

            current = next;
        }

        // Final pass: common-subexpression elimination by hash-consing. The rewrite
        // rules above produce fresh instances each iteration and never share, so CSE
        // runs once at the fixed point — it collapses structurally-identical subtrees
        // (e.g. a subquery spelled twice, like Nexmark q5's windowed count) to a
        // single shared instance the plan→circuit compiler compiles once.
        return PlanCse.Eliminate(current);
    }

    private static LogicalPlan OptimizeNode(LogicalPlan plan)
    {
        // Bottom-up: optimize children first, then apply local rules at this node.
        var withOptChildren = plan switch
        {
            FilterPlan f => new FilterPlan(OptimizeNode(f.Input), f.Predicate),
            ProjectPlan p => new ProjectPlan(OptimizeNode(p.Input), p.Projections, p.Schema),
            JoinPlan j => j with
            {
                Left = OptimizeNode(j.Left),
                Right = OptimizeNode(j.Right),
            },
            AggregatePlan a => a with { Input = OptimizeNode(a.Input) },
            UnionAllPlan u => u with { Branches = OptimizeBranches(u.Branches) },
            DistinctPlan d => new DistinctPlan(OptimizeNode(d.Input)),
            // TOP-K is a pushdown barrier: optimize the subtree below it, but no
            // rule ever moves a filter/projection across it (that would change
            // which rows fall inside the [offset, offset+limit) window).
            TopKPlan t => t with { Input = OptimizeNode(t.Input) },
            // Partitioned TOP-K is likewise a pushdown barrier — a filter or
            // projection moved across it would change per-partition rank.
            PartitionedTopKPlan pt => pt with { Input = OptimizeNode(pt.Input) },
            // Window aggregate is likewise a pushdown barrier — a filter or
            // projection moved across it would change frame membership.
            WindowAggregatePlan wa => wa with { Input = OptimizeNode(wa.Input) },
            // LAG/LEAD is likewise a pushdown barrier — a filter or projection
            // moved across it would change per-partition row positions.
            WindowOffsetPlan wo => wo with { Input = OptimizeNode(wo.Input) },
            DifferencePlan diff => new DifferencePlan(
                OptimizeNode(diff.Left), OptimizeNode(diff.Right)),
            ScalarSubqueryJoinPlan s => s with
            {
                Input = OptimizeNode(s.Input),
                Subqueries = OptimizeBranches(s.Subqueries),
            },
            SemiJoinPlan sj => sj with
            {
                Input = OptimizeNode(sj.Input),
                Subquery = OptimizeNode(sj.Subquery),
            },
            CorrelatedScalarSubqueryJoinPlan csp => csp with
            {
                Input = OptimizeNode(csp.Input),
                Subquery = OptimizeNode(csp.Subquery),
            },
            // ScanPlan / CteScanPlan: leaves (CTE body was already optimized
            // at its defining site or we skip to avoid breaking sharing).
            // RecursiveCtePlan: the base/step subplans are executed by a
            // dedicated runtime that handles its own semantics; leaving them
            // unoptimized is the safe default for v1.
            _ => plan,
        };

        return withOptChildren switch
        {
            FilterPlan f => SimplifyFilter(f),
            ProjectPlan p => PruneJoinInputs(SimplifyProject(p)),
            AggregatePlan a => PruneJoinInputs(NarrowAggregateInput(a)),
            SemiJoinPlan sj => NarrowSemiJoinSubquery(sj),
            _ => withOptChildren,
        };
    }

    // ---------- Semi-join subquery narrowing ----------

    /// <summary>
    /// A semi-/anti-join reads its <see cref="SemiJoinPlan.Subquery"/> purely as a
    /// <em>set</em> of its equi-key columns — the subquery's row multiplicity never
    /// affects the match. So when the subquery is <c>Project</c>-over-inner-<c>Join</c>
    /// and every projection the subquery keeps references only the join's LEFT
    /// columns (the RIGHT side contributes existence, not data), the inner join is
    /// soundly a semi-join, which never materialises the join's product. This is the
    /// classic <c>EXISTS/NOT EXISTS</c> shape where the subquery body joins two
    /// tables only to test existence — e.g. ivm-bench <c>broker_performance</c>'s
    /// <c>NOT EXISTS (SELECT 1 FROM fact_trade JOIN fact_cash_transactions …)</c>,
    /// where the inner join was an ~8M-row many-to-many product for a boolean.
    /// </summary>
    private static LogicalPlan NarrowSemiJoinSubquery(SemiJoinPlan sj)
    {
        if (sj.Subquery is not ProjectPlan p || p.Input is not JoinPlan j)
        {
            return sj;
        }

        // Inner equi-join only: an outer join keeps unmatched left rows (a semi-join
        // drops them); a residual or null-keyed join can reference the right side or
        // change match semantics; a keyless join has no semi-join form.
        if (j.JoinType != DbspNet.Sql.Parser.Ast.JoinType.Inner
            || j.Residual is not null || j.AllowNullKeys || j.EquiKeys.Count == 0)
        {
            return sj;
        }

        // Every kept projection must reference only left columns (or constants), so
        // repointing the project from the join (Left⧺Right) to a semi-join (Left
        // only) leaves each projected index valid — and proves the right side
        // contributes no data the subquery keeps.
        var leftCount = j.Left.Schema.Count;
        foreach (var item in p.Projections)
        {
            foreach (var idx in ExpressionRewriter.CollectColumnIndices(item.Expression))
            {
                if (idx >= leftCount)
                {
                    return sj;
                }
            }
        }

        var semiKeys = new List<SemiJoinEqui>(j.EquiKeys.Count);
        foreach (var eq in j.EquiKeys)
        {
            semiKeys.Add(new SemiJoinEqui(
                new ResolvedColumn(eq.LeftIndex, eq.KeyType), eq.RightIndex, eq.KeyType));
        }

        var innerSemi = new SemiJoinPlan(j.Left, j.Right, semiKeys, IsAnti: false);
        return sj with { Subquery = new ProjectPlan(innerSemi, p.Projections, p.Schema) };
    }

    private static List<LogicalPlan> OptimizeBranches(IReadOnlyList<LogicalPlan> branches)
    {
        var result = new List<LogicalPlan>(branches.Count);
        foreach (var b in branches)
        {
            result.Add(OptimizeNode(b));
        }

        return result;
    }

    // ---------- Filter simplification ----------

    private static LogicalPlan SimplifyFilter(FilterPlan filter)
    {
        switch (filter.Input)
        {
            case FilterPlan inner:
                // Filter(Filter(x, p2), p1) → Filter(x, p1 AND p2).
                return OptimizeNode(new FilterPlan(
                    inner.Input,
                    ExpressionRewriter.AndAll(new[] { filter.Predicate, inner.Predicate })));

            case ProjectPlan proj:
                // Push past a projection via expression substitution. We can
                // always substitute — at worst, complex projection expressions
                // get re-evaluated in the filter, but that's still cheaper than
                // materializing the projected stream first.
                var pushed = ExpressionRewriter.SubstituteViaProjection(filter.Predicate, proj.Projections);
                return OptimizeNode(proj with
                {
                    Input = OptimizeNode(new FilterPlan(proj.Input, pushed)),
                });

            case JoinPlan join:
                return PushFilterPastJoin(filter.Predicate, join);

            case UnionAllPlan union:
                var newBranches = new List<LogicalPlan>(union.Branches.Count);
                foreach (var b in union.Branches)
                {
                    newBranches.Add(OptimizeNode(new FilterPlan(b, filter.Predicate)));
                }

                return union with { Branches = newBranches };

            case DistinctPlan dist:
                // Filter(Distinct(x), p) ≡ Distinct(Filter(x, p)).
                return new DistinctPlan(OptimizeNode(new FilterPlan(dist.Input, filter.Predicate)));

            case DifferencePlan diff:
                // Filter is linear over Z-set subtraction:
                //   filter(a − b, p) = filter(a, p) − filter(b, p).
                return new DifferencePlan(
                    OptimizeNode(new FilterPlan(diff.Left, filter.Predicate)),
                    OptimizeNode(new FilterPlan(diff.Right, filter.Predicate)));

            default:
                return filter;
        }
    }

    private static LogicalPlan PushFilterPastJoin(ResolvedExpression predicate, JoinPlan join)
    {
        // Decompose the predicate on AND and classify each conjunct by which
        // side's columns it references. Push side-only conjuncts down;
        // cross-cutting conjuncts remain as a filter above the join.
        var conjuncts = ExpressionRewriter.SplitAnd(predicate);
        var leftOnly = new List<ResolvedExpression>();
        var rightOnly = new List<ResolvedExpression>();
        var cross = new List<ResolvedExpression>();

        var leftCount = join.Left.Schema.Count;
        foreach (var c in conjuncts)
        {
            var refs = ExpressionRewriter.CollectColumnIndices(c);
            var usesLeft = false;
            var usesRight = false;
            foreach (var r in refs)
            {
                if (r < leftCount)
                {
                    usesLeft = true;
                }
                else
                {
                    usesRight = true;
                }
            }

            if (usesLeft && usesRight)
            {
                cross.Add(c);
            }
            else if (usesRight)
            {
                rightOnly.Add(c);
            }
            else
            {
                // No refs OR left-only → classify as left (pure-constant
                // predicates are valid on either side; left is arbitrary).
                leftOnly.Add(c);
            }
        }

        // Outer-join restrictions: pushing a predicate on the non-preserved
        // side would wrongly eliminate NULL-padded rows that the outer-join
        // machinery is supposed to produce. Keep those conjuncts above.
        var pushLeft = leftOnly;
        var pushRight = rightOnly;
        var keepAbove = new List<ResolvedExpression>(cross);

        if (join.JoinType == JoinType.LeftOuter)
        {
            keepAbove.AddRange(pushRight);
            pushRight = new List<ResolvedExpression>();
        }
        else if (join.JoinType == JoinType.RightOuter)
        {
            keepAbove.AddRange(pushLeft);
            pushLeft = new List<ResolvedExpression>();
        }

        // Wrap sides with their pushed-down filters.
        var newLeft = join.Left;
        if (pushLeft.Count > 0)
        {
            newLeft = OptimizeNode(new FilterPlan(newLeft, ExpressionRewriter.AndAll(pushLeft)));
        }

        var newRight = join.Right;
        if (pushRight.Count > 0)
        {
            // Shift indices: right-only conjuncts reference columns
            // [leftCount, leftCount + rightCount) in the join output; on
            // the right input itself they need to be [0, rightCount).
            var shifted = new List<ResolvedExpression>(pushRight.Count);
            foreach (var c in pushRight)
            {
                shifted.Add(ExpressionRewriter.ShiftColumnIndices(c, -leftCount));
            }

            newRight = OptimizeNode(new FilterPlan(newRight, ExpressionRewriter.AndAll(shifted)));
        }

        var newJoin = join with { Left = newLeft, Right = newRight };

        if (keepAbove.Count == 0)
        {
            return newJoin;
        }

        // For an INNER join a cross-cutting predicate above it is exactly a join
        // residual, so fold it into the join: the in-memory join op applies it
        // during the cross-product enumeration, never materializing the rows it
        // rejects (the typed/structural compilers fall back to a post-join filter
        // when their join variant can't take a residual — same result). Outer
        // joins must keep the filter above: a post-join predicate there is not a
        // residual — it would drop NULL-padded rows the outer join must emit.
        if (join.JoinType == JoinType.Inner)
        {
            var folded = ExpressionRewriter.AndAll(keepAbove);
            var combined = newJoin.Residual is null
                ? folded
                : ExpressionRewriter.AndAll(new List<ResolvedExpression> { newJoin.Residual, folded });
            return newJoin with { Residual = combined };
        }

        return new FilterPlan(newJoin, ExpressionRewriter.AndAll(keepAbove));
    }

    // ---------- Project simplification ----------

    private static LogicalPlan SimplifyProject(ProjectPlan plan)
    {
        if (plan.Input is ProjectPlan inner)
        {
            // Project(Project(x, inner), outer) → Project(x, composed) where
            // each composed[i] is outer[i] with every ResolvedColumn(j)
            // replaced by inner[j].Expression.
            var composed = new List<ProjectionItem>(plan.Projections.Count);
            foreach (var outer in plan.Projections)
            {
                var substExpr = ExpressionRewriter.SubstituteViaProjection(
                    outer.Expression, inner.Projections);
                composed.Add(new ProjectionItem(substExpr, outer.Name, outer.Qualifier));
            }

            return OptimizeNode(new ProjectPlan(inner.Input, composed, plan.Schema));
        }

        return plan;
    }

    // ---------- Aggregate-input column pruning ----------

    /// <summary>
    /// Insert a narrowing <see cref="ProjectPlan"/> between
    /// <paramref name="agg"/> and its input when the input carries
    /// more columns than the aggregate references (group keys +
    /// aggregate arguments). The aggregate's per-group multiset is
    /// then built over those narrower rows, shrinking the per-tick
    /// cost in <c>IncrementalAggregateOp</c> at large N.
    /// </summary>
    /// <remarks>
    /// Sound only when every aggregate in the call list is
    /// <i>linear</i> in the Z-set weights: COUNT(*), COUNT(col),
    /// SUM, AVG. <c>MIN</c> and <c>MAX</c> are non-linear — they
    /// depend on which distinct values have positive weight, not on
    /// the weight sum — so narrowing can drop value-presence (two
    /// rows that share the kept columns but differ on dropped ones
    /// and have cancelling weights project to a single zero-weight
    /// entry, removing a value MIN/MAX would otherwise have seen).
    /// We skip the rewrite when any aggregate is MIN/MAX to keep
    /// optimizer-vs-batch parity.
    /// </remarks>
    private static LogicalPlan NarrowAggregateInput(AggregatePlan agg)
    {
        // Bail on the non-linear aggregates (MIN / MAX / APPROX_COUNT_DISTINCT /
        // COUNT(DISTINCT)): they depend on which distinct values have positive
        // weight, not on the weight sum, so narrowing can collapse two rows that
        // share the kept columns into a single zero-weight entry and drop a
        // value the aggregate would otherwise have seen — over an arbitrary
        // *signed* Z-set. This bail is the safe default. When
        // NonLinearNarrowingMode is enabled the rule narrows these too: keeping
        // the aggregate's argument columns makes collapsing rows that share them
        // invariant for the aggregate, and over a *well-formed* (non-negative)
        // per-group integral the narrowed weight is > 0 iff some positive-weight
        // row exists — exactly the value-presence MIN/MAX/DISTINCT read
        // (docs/design-row-representation.md §18).
        if (!NonLinearNarrowingMode.Enabled)
        {
            foreach (var call in agg.Aggregates)
            {
                if (call.Kind is AggregateKind.Min or AggregateKind.Max
                    or AggregateKind.ApproxCountDistinct or AggregateKind.CountDistinct)
                {
                    return agg;
                }
            }
        }

        // Collect every input column the aggregate references.
        var used = new HashSet<int>();
        foreach (var g in agg.GroupKeys)
        {
            used.UnionWith(ExpressionRewriter.CollectColumnIndices(g));
        }

        foreach (var call in agg.Aggregates)
        {
            if (call.Argument is not null)
            {
                used.UnionWith(ExpressionRewriter.CollectColumnIndices(call.Argument));
            }
        }

        var inputCount = agg.Input.Schema.Count;
        if (used.Count >= inputCount)
        {
            // Aggregate already uses every input column.
            return agg;
        }

        // Canonical kept-cols order: ascending by original index.
        // Deterministic, easy to inspect.
        var keepCols = used.OrderBy(i => i).ToList();

        var remap = new int[inputCount];
        for (var i = 0; i < remap.Length; i++)
        {
            remap[i] = -1;
        }

        for (var newIdx = 0; newIdx < keepCols.Count; newIdx++)
        {
            remap[keepCols[newIdx]] = newIdx;
        }

        // Build the narrowing projection: each kept column becomes
        // an identity ResolvedColumn at its new index.
        var keptColumns = new List<SchemaColumn>(keepCols.Count);
        var projections = new List<ProjectionItem>(keepCols.Count);
        foreach (var origIdx in keepCols)
        {
            var col = agg.Input.Schema[origIdx];
            keptColumns.Add(col);
            projections.Add(new ProjectionItem(
                new ResolvedColumn(origIdx, col.Type), col.Name, col.Qualifier));
        }

        var narrowingProject = new ProjectPlan(agg.Input, projections, new Schema(keptColumns));

        // Remap the aggregate's column references into the new
        // input column space.
        var newGroupKeys = new List<ResolvedExpression>(agg.GroupKeys.Count);
        foreach (var g in agg.GroupKeys)
        {
            newGroupKeys.Add(ExpressionRewriter.RemapColumnIndices(g, remap));
        }

        var newAggregates = new List<AggregateCall>(agg.Aggregates.Count);
        foreach (var call in agg.Aggregates)
        {
            var newArg = call.Argument is null
                ? null
                : ExpressionRewriter.RemapColumnIndices(call.Argument, remap);
            newAggregates.Add(new AggregateCall(call.Kind, newArg, call.ResultType));
        }

        // The aggregate's output schema is unchanged — group-key
        // and aggregate-result types are preserved by the remap.
        return new AggregatePlan(narrowingProject, newGroupKeys, newAggregates, agg.Schema);
    }

    // ---------- Join-input column pruning (projection pushdown through join) ----------

    /// <summary>
    /// Push column liveness through an INNER join: when <paramref name="parent"/>
    /// (a Project or Aggregate) plus the join's own equi-key / residual reference a
    /// strict subset of an input side's columns, insert a narrowing
    /// <see cref="ProjectPlan"/> on that side so the join stores and whole-row-hashes
    /// only the live columns (docs/design-row-representation.md §21 — the term-2 lever
    /// the §18 aggregate-input narrowing cannot reach, sitting above the join).
    /// </summary>
    /// <remarks>
    /// Unconditionally sound (ordinary relational projection pushdown), unlike
    /// <see cref="NonLinearNarrowingMode"/>: only columns <i>no</i> consumer reads
    /// (parent, combine via concatenation, residual, equi-key) are dropped, so two
    /// stored rows identical on the kept columns produce identical join output rows and
    /// consolidate the same way before or after the join — valid for arbitrary signed
    /// Z-sets. Gated behind <see cref="JoinColumnPruningMode"/>; INNER joins only in v1.
    /// </remarks>
    private static LogicalPlan PruneJoinInputs(LogicalPlan parent)
    {
        if (!JoinColumnPruningMode.Enabled)
        {
            return parent;
        }

        // Only Project(Join) and Aggregate(Join) parents in v1 (covers the Nexmark
        // join queries q3/q4/q9/q20). Other parents bail — safe, just no pruning.
        var input = parent switch
        {
            ProjectPlan p => p.Input,
            AggregatePlan a => a.Input,
            _ => parent,
        };

        if (input is not JoinPlan { JoinType: JoinType.Inner } join)
        {
            return parent;
        }

        var leftCount = join.Left.Schema.Count;
        var rightCount = join.Right.Schema.Count;
        var total = leftCount + rightCount;

        // Columns referenced in the join's OUTPUT (concatenated [left | right]) space:
        // by the parent, plus the join's own equi-keys and residual.
        var live = ReferencedOutputColumns(parent);
        foreach (var eq in join.EquiKeys)
        {
            live.Add(eq.LeftIndex);
            live.Add(leftCount + eq.RightIndex);
        }

        if (join.Residual is not null)
        {
            live.UnionWith(ExpressionRewriter.CollectColumnIndices(join.Residual));
        }

        // Per-side live native indices (ascending — deterministic, easy to inspect).
        var liveLeft = new List<int>();
        var liveRight = new List<int>();
        foreach (var i in live)
        {
            if (i < leftCount)
            {
                liveLeft.Add(i);
            }
            else if (i < total)
            {
                liveRight.Add(i - leftCount);
            }
        }

        liveLeft.Sort();
        liveRight.Sort();

        // Never narrow a side to zero columns (a value-less side is a multiplicity-only
        // cross product; keep its full schema to dodge empty-schema edge cases). Only
        // narrow a side that has a strict, non-empty live subset.
        var narrowLeft = liveLeft.Count > 0 && liveLeft.Count < leftCount;
        var narrowRight = liveRight.Count > 0 && liveRight.Count < rightCount;
        if (!narrowLeft && !narrowRight)
        {
            return parent;
        }

        var (newLeft, leftRemap) = narrowLeft
            ? NarrowSide(join.Left, liveLeft)
            : (join.Left, Identity(leftCount));
        var (newRight, rightRemap) = narrowRight
            ? NarrowSide(join.Right, liveRight)
            : (join.Right, Identity(rightCount));

        var newLeftCount = newLeft.Schema.Count;

        // Concatenated remap: old [left | right] output index → new output index.
        var concatRemap = new int[total];
        for (var i = 0; i < leftCount; i++)
        {
            concatRemap[i] = leftRemap[i];
        }

        for (var i = 0; i < rightCount; i++)
        {
            concatRemap[leftCount + i] = rightRemap[i] < 0 ? -1 : newLeftCount + rightRemap[i];
        }

        // Remap the join's equi-keys (native per-side indices) and residual (concat space).
        var newEqui = new List<JoinEquality>(join.EquiKeys.Count);
        foreach (var eq in join.EquiKeys)
        {
            newEqui.Add(new JoinEquality(leftRemap[eq.LeftIndex], rightRemap[eq.RightIndex], eq.KeyType));
        }

        var newResidual = join.Residual is null
            ? null
            : ExpressionRewriter.RemapColumnIndices(join.Residual, concatRemap);

        var newJoin = join with
        {
            Left = newLeft,
            Right = newRight,
            EquiKeys = newEqui,
            Residual = newResidual,
            Schema = newLeft.Schema.Concat(newRight.Schema),
        };

        // Rebuild the parent against the narrowed join, remapping its references into
        // the new output space. Output schema is unchanged (same projected columns).
        return parent switch
        {
            ProjectPlan p => new ProjectPlan(
                newJoin, RemapProjections(p.Projections, concatRemap), p.Schema),
            AggregatePlan a => new AggregatePlan(
                newJoin,
                RemapExpressions(a.GroupKeys, concatRemap),
                RemapAggregates(a.Aggregates, concatRemap),
                a.Schema),
            _ => parent,
        };
    }

    private static HashSet<int> ReferencedOutputColumns(LogicalPlan parent)
    {
        var used = new HashSet<int>();
        switch (parent)
        {
            case ProjectPlan p:
                foreach (var item in p.Projections)
                {
                    used.UnionWith(ExpressionRewriter.CollectColumnIndices(item.Expression));
                }

                break;
            case AggregatePlan a:
                foreach (var g in a.GroupKeys)
                {
                    used.UnionWith(ExpressionRewriter.CollectColumnIndices(g));
                }

                foreach (var call in a.Aggregates)
                {
                    if (call.Argument is not null)
                    {
                        used.UnionWith(ExpressionRewriter.CollectColumnIndices(call.Argument));
                    }
                }

                break;
        }

        return used;
    }

    private static int[] Identity(int count)
    {
        var remap = new int[count];
        for (var i = 0; i < count; i++)
        {
            remap[i] = i;
        }

        return remap;
    }

    // Insert a narrowing projection on one join input, keeping only liveNative columns
    // (ascending). Returns the new side and the old→new native index remap (-1 = dropped).
    private static (LogicalPlan Side, int[] Remap) NarrowSide(LogicalPlan side, List<int> liveNative)
    {
        var remap = new int[side.Schema.Count];
        for (var i = 0; i < remap.Length; i++)
        {
            remap[i] = -1;
        }

        var keptColumns = new List<SchemaColumn>(liveNative.Count);
        var projections = new List<ProjectionItem>(liveNative.Count);
        for (var newIdx = 0; newIdx < liveNative.Count; newIdx++)
        {
            var origIdx = liveNative[newIdx];
            remap[origIdx] = newIdx;
            var col = side.Schema[origIdx];
            keptColumns.Add(col);
            projections.Add(new ProjectionItem(
                new ResolvedColumn(origIdx, col.Type), col.Name, col.Qualifier));
        }

        return (new ProjectPlan(side, projections, new Schema(keptColumns)), remap);
    }

    private static List<ProjectionItem> RemapProjections(IReadOnlyList<ProjectionItem> items, int[] remap)
    {
        var result = new List<ProjectionItem>(items.Count);
        foreach (var item in items)
        {
            result.Add(item with { Expression = ExpressionRewriter.RemapColumnIndices(item.Expression, remap) });
        }

        return result;
    }

    private static List<ResolvedExpression> RemapExpressions(IReadOnlyList<ResolvedExpression> exprs, int[] remap)
    {
        var result = new List<ResolvedExpression>(exprs.Count);
        foreach (var e in exprs)
        {
            result.Add(ExpressionRewriter.RemapColumnIndices(e, remap));
        }

        return result;
    }

    private static List<AggregateCall> RemapAggregates(IReadOnlyList<AggregateCall> calls, int[] remap)
    {
        var result = new List<AggregateCall>(calls.Count);
        foreach (var call in calls)
        {
            result.Add(call.Argument is null
                ? call
                : call with { Argument = ExpressionRewriter.RemapColumnIndices(call.Argument, remap) });
        }

        return result;
    }
}
