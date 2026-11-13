// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using BinOp = DbspNet.Sql.Parser.Ast.BinaryOperator;

namespace DbspNet.Sql.Optimizer;

/// <summary>
/// Whether a rank that a filter bounds (<c>rn = 1</c>, <c>rn &lt;= k</c>) compiles as an incremental
/// TOP-K (docs/design-bounded-memory.md §5.51). On by default; <c>DBSPNET_RANK_TOPK=off</c> or setting
/// <see cref="Enabled"/> keeps the full rank and the filter.
/// </summary>
internal static class RankTopKMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_RANK_TOPK")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Null = the default. A [ThreadStatic] field's zero value is what every thread sees,
    // so a default of "on" has to live behind a nullable.
    [ThreadStatic]
    private static bool? _enabled;

    /// <summary>Ranks rewritten to a TOP-K since start (diagnostics).</summary>
    internal static long Rewritten;

    /// <summary>CTE scans inlined so a filter can reach the rank in their body, since start.</summary>
    internal static long CtesInlined;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }
}

/// <summary>
/// The rank → TOP-K rewrite (docs/design-bounded-memory.md §5.51). With R = Rank(X) and rn its last
/// column:
/// <code>
///   σ[rn = 1 ∧ q](R)  ≡  σ[q](π[X, 1 AS rn](TopK(X, 1)))
///   σ[rn ≤ k ∧ q](R)  ≡  σ[q](Rank(TopK(X, k)))            k &gt; 1
/// </code>
/// Both are local equivalences: rn keeps its value for every row the filter passes, so the rewrite is
/// right whoever else reads rn. Predicate pushdown brings the filter to the rank; a single-use CTE
/// between them is inlined by <see cref="InlineRankCtes"/>.
/// </summary>
internal static class RankTopK
{
    /// <summary>
    /// Rewrite <c>σ[pred](rank)</c> when a conjunct of <paramref name="predicate"/> bounds the rank
    /// column; <see langword="null"/> otherwise.
    /// </summary>
    internal static LogicalPlan? TryRewrite(ResolvedExpression predicate, PartitionedRankPlan rank)
    {
        if (!RankTopKMode.Enabled)
        {
            return null;
        }

        var rankIndex = rank.Schema.Count - 1;
        var conjuncts = ExpressionRewriter.SplitAnd(predicate);
        var at = -1;
        long limit = 0;
        for (var i = 0; i < conjuncts.Count; i++)
        {
            if (TryBound(conjuncts[i], c => c == rankIndex, out _, out limit))
            {
                at = i;
                break;
            }
        }

        if (at < 0)
        {
            return null;
        }

        var topK = new PartitionedTopKPlan(rank.Input, rank.PartitionKeys, rank.SortKeys, rank.Function, limit);
        LogicalPlan replaced;
        if (limit == 1)
        {
            // Every row the filter passes has rn = 1: put that value back.
            var items = new List<ProjectionItem>(rank.Schema.Count);
            for (var i = 0; i < rankIndex; i++)
            {
                var col = rank.Schema[i];
                items.Add(new ProjectionItem(new ResolvedColumn(i, col.Type), col.Name, col.Qualifier));
            }

            var rn = rank.Schema[rankIndex];
            items.Add(new ProjectionItem(OneOf(rn.Type), rn.Name, rn.Qualifier));
            replaced = new ProjectPlan(topK, items, rank.Schema);
        }
        else
        {
            // The kept rows rank as they did among all rows (§5.51.1).
            replaced = rank with { Input = topK };
        }

        Interlocked.Increment(ref RankTopKMode.Rewritten);
        conjuncts.RemoveAt(at);
        return conjuncts.Count == 0 ? replaced : new FilterPlan(replaced, ExpressionRewriter.AndAll(conjuncts));
    }

    private static ResolvedLiteral OneOf(SqlType type) => type switch
    {
        SqlIntegerType => new ResolvedLiteral(LiteralKind.Integer, 1, type),
        _ => new ResolvedLiteral(LiteralKind.Integer, 1L, type),
    };

    /// <summary>
    /// Whether <paramref name="c"/> bounds a column <paramref name="isRank"/> accepts: <c>col = 1</c>,
    /// <c>col &lt;= k</c>, <c>col &lt; k</c> or a reversed form, k an integer literal, limit ≥ 1.
    /// </summary>
    internal static bool TryBound(ResolvedExpression c, Func<int, bool> isRank, out int column, out long limit)
    {
        column = -1;
        limit = 0;
        if (c is not ResolvedBinary b)
        {
            return false;
        }

        BinOp op;
        ResolvedExpression other;
        if (b.Left is ResolvedColumn lc && isRank(lc.Index))
        {
            column = lc.Index;
            op = b.Operator;
            other = b.Right;
        }
        else if (b.Right is ResolvedColumn rc && isRank(rc.Index))
        {
            column = rc.Index;
            op = b.Operator switch
            {
                BinOp.Less => BinOp.Greater,
                BinOp.LessEqual => BinOp.GreaterEqual,
                BinOp.Greater => BinOp.Less,
                BinOp.GreaterEqual => BinOp.LessEqual,
                var same => same,
            };
            other = b.Left;
        }
        else
        {
            return false;
        }

        if (IntegerLiteral(other) is not { } k)
        {
            return false;
        }

        limit = op switch
        {
            BinOp.Equal when k == 1 => 1,
            BinOp.LessEqual => k,
            BinOp.Less => k - 1,
            _ => 0,
        };
        return limit >= 1;
    }

    private static long? IntegerLiteral(ResolvedExpression e) => e switch
    {
        ResolvedLiteral { Kind: LiteralKind.Integer, Value: long v } => v,
        ResolvedLiteral { Kind: LiteralKind.Integer, Value: int v } => v,
        ResolvedLiteral { Kind: LiteralKind.Integer, Value: short v } => v,
        ResolvedCast { Operand: var o } => IntegerLiteral(o),
        _ => null,
    };

    // ---------- CTE inlining (§5.51.2) ----------

    /// <summary>
    /// Inline each single-use, non-recursive CTE scan that a filter bounding a rank column of its body
    /// sits on, so the next optimizer fixpoint pushes the filter onto the rank. Walks into CTE bodies
    /// (optimizing each before looking at it); a body with a change gets a new <see cref="CteRef"/>
    /// that every scan of the old one uses. Returns <paramref name="root"/> itself when nothing changes.
    /// </summary>
    internal static LogicalPlan InlineRankCtes(LogicalPlan root, Func<LogicalPlan, LogicalPlan> optimizeBody)
    {
        if (!RankTopKMode.Enabled)
        {
            return root;
        }

        var (_, counts, recursive) = PlanToCircuit.CountReferences(root);
        var bodies = new Dictionary<CteRef, CteRef>(ReferenceEqualityComparer.Instance);
        return Walk(root);

        LogicalPlan Walk(LogicalPlan p)
        {
            switch (p)
            {
                case FilterPlan { Input: CteScanPlan c } f
                    when counts.GetValueOrDefault(c.Cte) == 1 && !recursive.Contains(c.Cte)
                         && BoundsRank(f.Predicate, c.Cte.Plan):
                {
                    Interlocked.Increment(ref RankTopKMode.CtesInlined);
                    var body = Walk(optimizeBody(c.Cte.Plan));
                    return new FilterPlan(Requalify(body, c.Schema), f.Predicate);
                }

                case CteScanPlan c when !recursive.Contains(c.Cte):
                {
                    if (!bodies.TryGetValue(c.Cte, out var mapped))
                    {
                        var optimized = optimizeBody(c.Cte.Plan);
                        var walked = Walk(optimized);
                        mapped = ReferenceEquals(walked, optimized) ? c.Cte : new CteRef(c.Cte.Name, walked);
                        bodies[c.Cte] = mapped;
                    }

                    return ReferenceEquals(mapped, c.Cte) ? c : new CteScanPlan(mapped, c.Schema);
                }

                default:
                    return MapChildren(p, Walk);
            }
        }
    }

    // The scan's own column names and qualifiers over the body's rows.
    private static LogicalPlan Requalify(LogicalPlan body, Schema scan)
    {
        var items = new List<ProjectionItem>(scan.Count);
        for (var i = 0; i < scan.Count; i++)
        {
            items.Add(new ProjectionItem(new ResolvedColumn(i, scan[i].Type), scan[i].Name, scan[i].Qualifier));
        }

        return new ProjectPlan(body, items, scan);
    }

    // Some conjunct bounds a column of `body` that is a rank's rank column.
    private static bool BoundsRank(ResolvedExpression predicate, LogicalPlan body)
    {
        foreach (var c in ExpressionRewriter.SplitAnd(predicate))
        {
            if (TryBound(c, i => TracesToRank(body, i), out _, out _))
            {
                return true;
            }
        }

        return false;
    }

    // Column `index` of `p` is, through bare column references, the rank column of a PartitionedRankPlan.
    private static bool TracesToRank(LogicalPlan p, int index)
    {
        while (true)
        {
            switch (p)
            {
                case PartitionedRankPlan r:
                    return index == r.Schema.Count - 1;
                case FilterPlan f:
                    p = f.Input;
                    break;
                case ProjectPlan pr when index < pr.Projections.Count
                                         && pr.Projections[index].Expression is ResolvedColumn col:
                    p = pr.Input;
                    index = col.Index;
                    break;
                case CteScanPlan c:
                    p = c.Cte.Plan;
                    break;
                default:
                    return false;
            }
        }
    }

    /// <summary>
    /// <paramref name="p"/> with <paramref name="map"/> applied to each child; <paramref name="p"/>
    /// itself when no child changes. CTE scans and recursive CTEs are leaves here.
    /// </summary>
    private static LogicalPlan MapChildren(LogicalPlan p, Func<LogicalPlan, LogicalPlan> map)
    {
        // Rebuild with mapped children; compare by reference to keep identity when unchanged.
        var changed = false;
        LogicalPlan M(LogicalPlan c)
        {
            var m = map(c);
            changed |= !ReferenceEquals(m, c);
            return m;
        }

        IReadOnlyList<LogicalPlan> ML(IReadOnlyList<LogicalPlan> cs)
        {
            var result = new LogicalPlan[cs.Count];
            for (var i = 0; i < cs.Count; i++)
            {
                result[i] = M(cs[i]);
            }

            return result;
        }

        LogicalPlan rebuilt = p switch
        {
            FilterPlan f => f with { Input = M(f.Input) },
            TemporalFilterPlan tf => tf with { Input = M(tf.Input) },
            ProjectPlan pr => pr with { Input = M(pr.Input) },
            JoinPlan j => j with { Left = M(j.Left), Right = M(j.Right) },
            AggregatePlan a => a with { Input = M(a.Input) },
            UnionAllPlan u => u with { Branches = ML(u.Branches) },
            DistinctPlan d => d with { Input = M(d.Input) },
            DifferencePlan diff => diff with { Left = M(diff.Left), Right = M(diff.Right) },
            TopKPlan t => t with { Input = M(t.Input) },
            PartitionedTopKPlan pt => pt with { Input = M(pt.Input) },
            PartitionedRankPlan prk => prk with { Input = M(prk.Input) },
            WindowAggregatePlan wa => wa with { Input = M(wa.Input) },
            WindowOffsetPlan wo => wo with { Input = M(wo.Input) },
            ScalarSubqueryJoinPlan s => s with { Input = M(s.Input), Subqueries = ML(s.Subqueries) },
            SemiJoinPlan sj => sj with { Input = M(sj.Input), Subquery = M(sj.Subquery) },
            CorrelatedScalarSubqueryJoinPlan csp => csp with { Input = M(csp.Input), Subquery = M(csp.Subquery) },
            _ => p, // ScanPlan, CteScanPlan (handled by the caller), RecursiveCtePlan (left intact)
        };

        return changed ? rebuilt : p;
    }
}
