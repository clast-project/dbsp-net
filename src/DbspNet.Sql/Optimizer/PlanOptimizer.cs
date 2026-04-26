using System.Collections.Generic;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Rule-based <see cref="LogicalPlan"/> rewriter. Currently implements two
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
/// </list>
/// Not applied here: projection pruning (top-down column liveness),
/// constant folding, and join reordering. See <c>docs/skipped.md</c>.
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
                return next;
            }

            current = next;
        }

        return current;
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
            DifferencePlan diff => new DifferencePlan(
                OptimizeNode(diff.Left), OptimizeNode(diff.Right)),
            ScalarSubqueryJoinPlan s => s with
            {
                Input = OptimizeNode(s.Input),
                Subqueries = OptimizeBranches(s.Subqueries),
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
            ProjectPlan p => SimplifyProject(p),
            _ => withOptChildren,
        };
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
}
