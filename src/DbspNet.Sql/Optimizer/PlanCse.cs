// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Common-subexpression elimination over a <see cref="LogicalPlan"/> tree by
/// <b>hash-consing</b> (interning): structurally-identical subtrees are collapsed
/// to a single shared instance, turning the plan tree into a DAG. The plan→circuit
/// compiler deduplicates by <em>reference</em> (see <c>PlanToCircuit.CompilePlan</c>'s
/// per-reference stream cache and <c>LogicalPlan.cs</c> on CTE sharing), so a shared
/// instance compiles to one subcircuit feeding every consumer — exactly what a
/// hand-written <c>WITH</c> achieves, but automatically.
///
/// <para>
/// Motivating case: Nexmark q5 spells the same windowed per-auction bid count twice
/// (once for the per-auction output, once inside the per-window <c>MAX</c>). Each is
/// a separate parse, so records compare unequal (their list fields compare by
/// reference) and nothing shared them — the 5× HOP fan-out + count aggregate ran
/// twice. Interning collapses the two into one.
/// </para>
///
/// <para>
/// CORRECTNESS. Interning only ever replaces a subplan with a
/// <b>structurally-equal</b> one, so it cannot change results — provided equality is
/// sound. Equality here is conservative: it compares node kind, then children by
/// <em>reference</em> (valid because children are interned bottom-up first, so equal
/// subtrees are already the same instance), then the node's own non-child fields
/// structurally (expressions via <see cref="ExprEqual"/>, which itself falls back to
/// record equality for any node it doesn't special-case — never a false positive).
/// Column <em>names/qualifiers</em> are deliberately ignored (downstream references
/// are positional <see cref="ResolvedColumn"/> indices, so two subplans differing
/// only by alias — q5's <c>B1</c> vs <c>B2</c> — are genuinely interchangeable);
/// column <em>types</em> and <em>lateness</em> are compared. Node kinds not in the
/// intern set below are passed through un-shared (their children are still interned):
/// losing a sharing opportunity is safe, a wrong share is not.
/// </para>
/// </summary>
public static class PlanCse
{
    public static LogicalPlan Eliminate(LogicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var interner = new Dictionary<LogicalPlan, LogicalPlan>(StructuralComparer.Instance);
        return Intern(plan, interner);
    }

    // Bottom-up: rebuild the node with interned children, then canonicalize the
    // rebuilt node itself (for the intern-eligible kinds).
    private static LogicalPlan Intern(LogicalPlan plan, Dictionary<LogicalPlan, LogicalPlan> interner)
    {
        var rebuilt = plan switch
        {
            FilterPlan f => f with { Input = Intern(f.Input, interner) },
            ProjectPlan p => p with { Input = Intern(p.Input, interner) },
            AggregatePlan a => a with { Input = Intern(a.Input, interner) },
            JoinPlan j => j with { Left = Intern(j.Left, interner), Right = Intern(j.Right, interner) },
            UnionAllPlan u => u with { Branches = InternList(u.Branches, interner) },
            DistinctPlan d => d with { Input = Intern(d.Input, interner) },
            DifferencePlan diff => diff with
            {
                Left = Intern(diff.Left, interner),
                Right = Intern(diff.Right, interner),
            },

            // Pass-through kinds: intern their children so sharing still forms
            // underneath, but never share the node itself (conservative — these carry
            // extra ordering/frame/partition state we don't structurally compare, and
            // CteScan/RecursiveCte have identity/back-edge semantics of their own).
            TopKPlan t => t with { Input = Intern(t.Input, interner) },
            PartitionedTopKPlan pt => pt with { Input = Intern(pt.Input, interner) },
            PartitionedRankPlan pr => pr with { Input = Intern(pr.Input, interner) },
            WindowAggregatePlan wa => wa with { Input = Intern(wa.Input, interner) },
            WindowOffsetPlan wo => wo with { Input = Intern(wo.Input, interner) },
            TemporalFilterPlan tf => tf with { Input = Intern(tf.Input, interner) },
            ScalarSubqueryJoinPlan s => s with
            {
                Input = Intern(s.Input, interner),
                Subqueries = InternList(s.Subqueries, interner),
            },
            SemiJoinPlan sj => sj with
            {
                Input = Intern(sj.Input, interner),
                Subquery = Intern(sj.Subquery, interner),
            },
            CorrelatedScalarSubqueryJoinPlan csp => csp with
            {
                Input = Intern(csp.Input, interner),
                Subquery = Intern(csp.Subquery, interner),
            },

            // Leaves / identity-bearing: return as-is (ScanPlan is interned below via
            // Canon so two scans of the same table share; CteScan/RecursiveCte pass
            // through untouched to preserve their own sharing/back-edge invariants).
            _ => plan,
        };

        return IsInternEligible(rebuilt) ? Canon(rebuilt, interner) : rebuilt;
    }

    private static IReadOnlyList<LogicalPlan> InternList(
        IReadOnlyList<LogicalPlan> plans, Dictionary<LogicalPlan, LogicalPlan> interner)
    {
        var result = new LogicalPlan[plans.Count];
        for (var i = 0; i < plans.Count; i++)
        {
            result[i] = Intern(plans[i], interner);
        }

        return result;
    }

    private static LogicalPlan Canon(LogicalPlan plan, Dictionary<LogicalPlan, LogicalPlan> interner)
    {
        if (interner.TryGetValue(plan, out var canonical))
        {
            return canonical;
        }

        interner[plan] = plan;
        return plan;
    }

    // Kinds we are willing to share. ScanPlan is included so identical base-table
    // scans collapse (letting the fan-out branches above them share too). The
    // ranking family (ORDER BY … LIMIT and ROW_NUMBER/RANK/DENSE_RANK windows) is
    // included so a duplicated ranked/top-k subquery shares too — each carries extra
    // sort-key / partition-key state that StructuralComparer compares below. Left
    // pass-through: window aggregates / offset functions (not reachable from the
    // current SQL surface, so interning them would be untested), the scalar-subquery
    // and semi joins (correlation / batched-subquery semantics), TemporalFilter,
    // CteScan (identity-shared already), and RecursiveCte (self-referential back-edge).
    private static bool IsInternEligible(LogicalPlan plan) => plan switch
    {
        ScanPlan => true,
        FilterPlan => true,
        ProjectPlan => true,
        AggregatePlan => true,
        JoinPlan => true,
        UnionAllPlan => true,
        DistinctPlan => true,
        DifferencePlan => true,
        TopKPlan => true,
        PartitionedTopKPlan => true,
        PartitionedRankPlan => true,
        _ => false,
    };

    // ---- structural equality / hash over interned nodes -----------------------
    // Children are compared by REFERENCE (they are already interned); only the
    // node's own non-child fields are compared structurally.

    private sealed class StructuralComparer : IEqualityComparer<LogicalPlan>
    {
        public static readonly StructuralComparer Instance = new();

        public bool Equals(LogicalPlan? a, LogicalPlan? b)
        {
            if (ReferenceEquals(a, b))
            {
                return true;
            }

            if (a is null || b is null || a.GetType() != b.GetType())
            {
                return false;
            }

            return (a, b) switch
            {
                (ScanPlan x, ScanPlan y) =>
                    string.Equals(x.TableName, y.TableName, StringComparison.Ordinal)
                    && SchemaTypesEqual(x.Schema, y.Schema),
                (FilterPlan x, FilterPlan y) =>
                    ReferenceEquals(x.Input, y.Input) && ExprEqual(x.Predicate, y.Predicate),
                (ProjectPlan x, ProjectPlan y) =>
                    ReferenceEquals(x.Input, y.Input)
                    && ProjListEqual(x.Projections, y.Projections)
                    && SchemaTypesEqual(x.Schema, y.Schema),
                (AggregatePlan x, AggregatePlan y) =>
                    ReferenceEquals(x.Input, y.Input)
                    && ExprListEqual(x.GroupKeys, y.GroupKeys)
                    && AggListEqual(x.Aggregates, y.Aggregates)
                    && SchemaTypesEqual(x.Schema, y.Schema),
                (JoinPlan x, JoinPlan y) =>
                    ReferenceEquals(x.Left, y.Left) && ReferenceEquals(x.Right, y.Right)
                    && x.JoinType == y.JoinType && x.AllowNullKeys == y.AllowNullKeys
                    && EquiKeysEqual(x.EquiKeys, y.EquiKeys)
                    && ExprEqual(x.Residual, y.Residual),
                (UnionAllPlan x, UnionAllPlan y) =>
                    ChildrenRefEqual(x.Branches, y.Branches) && SchemaTypesEqual(x.Schema, y.Schema),
                (DistinctPlan x, DistinctPlan y) => ReferenceEquals(x.Input, y.Input),
                (DifferencePlan x, DifferencePlan y) =>
                    ReferenceEquals(x.Left, y.Left) && ReferenceEquals(x.Right, y.Right),
                (TopKPlan x, TopKPlan y) =>
                    ReferenceEquals(x.Input, y.Input)
                    && x.Limit == y.Limit && x.Offset == y.Offset
                    && SortKeysEqual(x.SortKeys, y.SortKeys),
                (PartitionedTopKPlan x, PartitionedTopKPlan y) =>
                    ReferenceEquals(x.Input, y.Input)
                    && x.Function == y.Function && x.Limit == y.Limit
                    && ExprListEqual(x.PartitionKeys, y.PartitionKeys)
                    && SortKeysEqual(x.SortKeys, y.SortKeys),
                (PartitionedRankPlan x, PartitionedRankPlan y) =>
                    ReferenceEquals(x.Input, y.Input) && x.Function == y.Function
                    && ExprListEqual(x.PartitionKeys, y.PartitionKeys)
                    && SortKeysEqual(x.SortKeys, y.SortKeys)
                    && SchemaTypesEqual(x.Schema, y.Schema),
                _ => false,
            };
        }

        public int GetHashCode(LogicalPlan plan)
        {
            var h = new HashCode();
            h.Add(plan.GetType());
            switch (plan)
            {
                case ScanPlan s:
                    h.Add(s.TableName, StringComparer.Ordinal);
                    h.Add(s.Schema.Count);
                    break;
                case FilterPlan f:
                    h.Add(RuntimeHelpers.GetHashCode(f.Input));
                    h.Add(ExprHash(f.Predicate));
                    break;
                case ProjectPlan p:
                    h.Add(RuntimeHelpers.GetHashCode(p.Input));
                    h.Add(p.Projections.Count);
                    foreach (var pi in p.Projections)
                    {
                        h.Add(ExprHash(pi.Expression));
                    }

                    break;
                case AggregatePlan a:
                    h.Add(RuntimeHelpers.GetHashCode(a.Input));
                    h.Add(a.GroupKeys.Count);
                    h.Add(a.Aggregates.Count);
                    foreach (var g in a.GroupKeys)
                    {
                        h.Add(ExprHash(g));
                    }

                    foreach (var ag in a.Aggregates)
                    {
                        h.Add((int)ag.Kind);
                        h.Add(ExprHash(ag.Argument));
                    }

                    break;
                case JoinPlan j:
                    h.Add(RuntimeHelpers.GetHashCode(j.Left));
                    h.Add(RuntimeHelpers.GetHashCode(j.Right));
                    h.Add((int)j.JoinType);
                    h.Add(j.EquiKeys.Count);
                    break;
                case UnionAllPlan u:
                    foreach (var b in u.Branches)
                    {
                        h.Add(RuntimeHelpers.GetHashCode(b));
                    }

                    break;
                case DistinctPlan d:
                    h.Add(RuntimeHelpers.GetHashCode(d.Input));
                    break;
                case DifferencePlan diff:
                    h.Add(RuntimeHelpers.GetHashCode(diff.Left));
                    h.Add(RuntimeHelpers.GetHashCode(diff.Right));
                    break;

                // Ordering/window/rank/temporal/semi-join family: coarse hash on the
                // input reference + counts + a discriminating scalar. Equality settles
                // collisions.
                case TopKPlan t:
                    h.Add(RuntimeHelpers.GetHashCode(t.Input));
                    h.Add(t.Limit);
                    h.Add(t.SortKeys.Count);
                    break;
                case PartitionedTopKPlan pt:
                    h.Add(RuntimeHelpers.GetHashCode(pt.Input));
                    h.Add((int)pt.Function);
                    h.Add(pt.Limit);
                    h.Add(pt.PartitionKeys.Count);
                    h.Add(pt.SortKeys.Count);
                    break;
                case PartitionedRankPlan pr:
                    h.Add(RuntimeHelpers.GetHashCode(pr.Input));
                    h.Add((int)pr.Function);
                    h.Add(pr.PartitionKeys.Count);
                    h.Add(pr.SortKeys.Count);
                    break;
            }

            return h.ToHashCode();
        }

        private static bool ChildrenRefEqual(IReadOnlyList<LogicalPlan> a, IReadOnlyList<LogicalPlan> b)
        {
            if (a.Count != b.Count)
            {
                return false;
            }

            for (var i = 0; i < a.Count; i++)
            {
                if (!ReferenceEquals(a[i], b[i]))
                {
                    return false;
                }
            }

            return true;
        }
    }

    // Compare schemas by column TYPE + LATENESS only (names/qualifiers are cosmetic
    // at this stage — downstream references are positional). Arity is load-bearing.
    private static bool SchemaTypesEqual(Schema a, Schema b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (!a[i].Type.Equals(b[i].Type) || a[i].Lateness != b[i].Lateness)
            {
                return false;
            }
        }

        return true;
    }

    // Compare projections by their EXPRESSION only — the output Name/Qualifier are
    // aliases (cosmetic; downstream references are positional). This is what lets
    // q5's B1-aliased and B2-aliased-but-identical window projections share.
    private static bool ProjListEqual(IReadOnlyList<ProjectionItem> a, IReadOnlyList<ProjectionItem> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (!ExprEqual(a[i].Expression, b[i].Expression))
            {
                return false;
            }
        }

        return true;
    }

    private static bool SortKeysEqual(IReadOnlyList<SortKey> a, IReadOnlyList<SortKey> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (!SortKeyEqual(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool SortKeyEqual(SortKey a, SortKey b) =>
        a.Descending == b.Descending
        && a.NullsFirst == b.NullsFirst
        && ExprEqual(a.Expression, b.Expression);

    private static bool EquiKeysEqual(IReadOnlyList<JoinEquality> a, IReadOnlyList<JoinEquality> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (a[i].LeftIndex != b[i].LeftIndex || a[i].RightIndex != b[i].RightIndex
                || !a[i].KeyType.Equals(b[i].KeyType))
            {
                return false;
            }
        }

        return true;
    }

    private static bool AggListEqual(IReadOnlyList<AggregateCall> a, IReadOnlyList<AggregateCall> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (a[i].Kind != b[i].Kind
                || a[i].Fraction != b[i].Fraction
                || a[i].Discrete != b[i].Discrete
                || !a[i].ResultType.Equals(b[i].ResultType)
                || !ExprEqual(a[i].Argument, b[i].Argument))
            {
                return false;
            }
        }

        return true;
    }

    // ---- structural equality / hash over ResolvedExpression -------------------
    // Mirrors Resolver.ResolvedExprEqual: record auto-equality is unusable for
    // collection-bearing nodes (their lists compare by reference), so recurse
    // explicitly; leaves fall back to record equality (correct, and never a false
    // positive). The hash is coarse — equality settles collisions.

    private static bool ExprEqual(ResolvedExpression? a, ResolvedExpression? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null || !a.Type.Equals(b.Type))
        {
            return false;
        }

        switch (a, b)
        {
            case (ResolvedBinary x, ResolvedBinary y):
                return x.Operator == y.Operator
                    && ExprEqual(x.Left, y.Left) && ExprEqual(x.Right, y.Right);
            case (ResolvedUnary x, ResolvedUnary y):
                return x.Operator == y.Operator && ExprEqual(x.Operand, y.Operand);
            case (ResolvedIsNull x, ResolvedIsNull y):
                return x.Negated == y.Negated && ExprEqual(x.Operand, y.Operand);
            case (ResolvedCast x, ResolvedCast y):
                return ExprEqual(x.Operand, y.Operand);
            case (ResolvedFunctionCall x, ResolvedFunctionCall y):
                return string.Equals(x.FunctionName, y.FunctionName, StringComparison.Ordinal)
                    && ExprListEqual(x.Arguments, y.Arguments);
            case (ResolvedInList x, ResolvedInList y):
                return x.IsNegated == y.IsNegated
                    && ExprEqual(x.Probe, y.Probe) && ExprListEqual(x.Values, y.Values);
            case (ResolvedCaseWhen x, ResolvedCaseWhen y):
                if (x.Whens.Count != y.Whens.Count)
                {
                    return false;
                }

                for (var i = 0; i < x.Whens.Count; i++)
                {
                    if (!ExprEqual(x.Whens[i].Condition, y.Whens[i].Condition)
                        || !ExprEqual(x.Whens[i].Result, y.Whens[i].Result))
                    {
                        return false;
                    }
                }

                return ExprEqual(x.ElseResult, y.ElseResult);
            default:
                return a.Equals(b); // ResolvedColumn / ResolvedLiteral / ResolvedCorrelationRef / …
        }
    }

    private static bool ExprListEqual(IReadOnlyList<ResolvedExpression> a, IReadOnlyList<ResolvedExpression> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var i = 0; i < a.Count; i++)
        {
            if (!ExprEqual(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static int ExprHash(ResolvedExpression? e)
    {
        if (e is null)
        {
            return 0;
        }

        var h = new HashCode();
        h.Add(e.GetType());
        switch (e)
        {
            case ResolvedColumn c:
                h.Add(c.Index);
                break;
            case ResolvedLiteral l:
                h.Add(l.Kind);
                h.Add(l.Value);
                break;
            case ResolvedBinary bin:
                h.Add((int)bin.Operator);
                h.Add(ExprHash(bin.Left));
                h.Add(ExprHash(bin.Right));
                break;
            case ResolvedUnary un:
                h.Add((int)un.Operator);
                h.Add(ExprHash(un.Operand));
                break;
            case ResolvedCast cast:
                h.Add(ExprHash(cast.Operand));
                break;
            case ResolvedFunctionCall fc:
                h.Add(fc.FunctionName, StringComparer.Ordinal);
                h.Add(fc.Arguments.Count);
                break;
            // Other kinds: type-only hash; equality settles the bucket.
        }

        return h.ToHashCode();
    }
}
