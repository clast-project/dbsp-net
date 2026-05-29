// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Sql.Parser.Ast;

namespace DbspNet.Sql.Plan;

/// <summary>
/// Identity of a declared <c>LATENESS</c> source: a base-table column. The
/// compiler creates one frontier per source; a derived monotone column carries
/// the set of sources whose frontiers jointly bound it.
/// </summary>
public readonly record struct LatenessSource(string Table, int Column);

/// <summary>
/// Per-node, per-output-column monotonicity verdicts produced by
/// <see cref="MonotonicityAnalyzer"/>. A column maps to the set of
/// <see cref="LatenessSource"/>s whose advancing frontiers bound it; a
/// <c>null</c> entry means the column is not provably monotone (no GC).
/// </summary>
/// <remarks>
/// Keyed by plan-node reference identity — two structurally-equal subplans are
/// distinct entries, and a shared CTE plan is analyzed once.
/// </remarks>
public sealed class MonotonicityInfo
{
    private readonly Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]> _byNode;

    internal MonotonicityInfo(Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]> byNode)
    {
        _byNode = byNode;
    }

    /// <summary>
    /// The <c>LATENESS</c> sources bounding output column <paramref name="column"/>
    /// of <paramref name="node"/>, or <c>null</c> if the column is not provably
    /// monotone.
    /// </summary>
    public IReadOnlySet<LatenessSource>? Sources(LogicalPlan node, int column)
    {
        ArgumentNullException.ThrowIfNull(node);
        return _byNode.TryGetValue(node, out var cols) && column >= 0 && column < cols.Length
            ? cols[column]
            : null;
    }

    /// <summary>True iff output column <paramref name="column"/> of <paramref name="node"/> is provably monotone.</summary>
    public bool IsMonotone(LogicalPlan node, int column) => Sources(node, column) is not null;
}

/// <summary>
/// Propagates each declared <c>LATENESS</c> column's monotonicity (and the
/// advancing frontier behind it) through the logical plan, so that any stateful
/// operator whose key is monotone — directly or via filters, projections,
/// joins, unions, or aggregates — can be wired for frontier-driven GC.
/// </summary>
/// <remarks>
/// <para><b>Soundness over completeness.</b> A column is marked monotone only
/// when it is provably non-decreasing with a known frontier; everything
/// unproven is left unmarked (no GC, always correct). A false positive would
/// collect live state and corrupt output, so every rule is conservative.</para>
/// <para>Run this on the final (post-optimizer) plan that will be compiled.</para>
/// </remarks>
public static class MonotonicityAnalyzer
{
    public static MonotonicityInfo Analyze(LogicalPlan plan)
    {
        ArgumentNullException.ThrowIfNull(plan);
        var memo = new Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]>(ReferenceEqualityComparer.Instance);
        Visit(plan, memo);
        return new MonotonicityInfo(memo);
    }

    private static IReadOnlySet<LatenessSource>?[] Visit(
        LogicalPlan node,
        Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]> memo)
    {
        if (memo.TryGetValue(node, out var cached))
        {
            return cached;
        }

        var result = Compute(node, memo);
        memo[node] = result;
        return result;
    }

    private static IReadOnlySet<LatenessSource>?[] Compute(
        LogicalPlan node,
        Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]> memo)
    {
        switch (node)
        {
            case ScanPlan s:
            {
                var cols = new IReadOnlySet<LatenessSource>?[s.Schema.Count];
                if (s.ColumnLateness is { } lateness)
                {
                    foreach (var i in lateness.Keys)
                    {
                        if (i >= 0 && i < cols.Length)
                        {
                            cols[i] = new HashSet<LatenessSource> { new(s.TableName, i) };
                        }
                    }
                }

                return cols;
            }

            // Filtering only removes rows — never produces a value below the
            // input's frontier.
            case FilterPlan f:
                return Visit(f.Input, memo);

            case ProjectPlan p:
            {
                var input = Visit(p.Input, memo);
                var cols = new IReadOnlySet<LatenessSource>?[p.Projections.Count];
                for (var i = 0; i < p.Projections.Count; i++)
                {
                    // A bare pass-through of a monotone input column stays
                    // monotone. Transforming expressions are conservatively not
                    // monotone — this is the monotone-function catalog extension
                    // point (e.g. date_trunc, ts + const), deferred.
                    if (p.Projections[i].Expression is ResolvedColumn rc
                        && rc.Index >= 0 && rc.Index < input.Length
                        && input[rc.Index] is { } src)
                    {
                        cols[i] = src;
                    }
                }

                return cols;
            }

            case JoinPlan j:
                return ComputeJoin(j, memo);

            case AggregatePlan a:
            {
                var input = Visit(a.Input, memo);
                // Output = [group-key columns..., aggregate-result columns...].
                var cols = new IReadOnlySet<LatenessSource>?[a.Schema.Count];
                for (var g = 0; g < a.GroupKeys.Count && g < cols.Length; g++)
                {
                    if (a.GroupKeys[g] is ResolvedColumn rc
                        && rc.Index >= 0 && rc.Index < input.Length
                        && input[rc.Index] is { } src)
                    {
                        cols[g] = src;
                    }
                }

                // Aggregate-result columns stay null (MIN/MAX-of-monotone is a
                // future extension).
                return cols;
            }

            case UnionAllPlan u:
                return ComputeUnion(u, memo);

            case DistinctPlan d:
                return Visit(d.Input, memo);

            case DifferencePlan diff:
                return Visit(diff.Left, memo);

            case CteScanPlan c:
                // Same arity as the CTE plan (qualifier-only schema differences).
                return ResizeTo(Visit(c.Cte.Plan, memo), c.Schema.Count);

            case ScalarSubqueryJoinPlan s:
                // Input columns pass through; appended subquery columns are null.
                return ResizeTo(Visit(s.Input, memo), s.Schema.Count);

            case SemiJoinPlan sj:
                // Semi-join's output schema is the input's, unchanged — the
                // subquery side is consumed by the match. Outer monotonicity
                // carries through.
                return Visit(sj.Input, memo);

            case CorrelatedScalarSubqueryJoinPlan csp:
                // Input columns pass through; the appended scalar column is
                // non-monotone (LEFT JOIN — value can become NULL or change
                // arbitrarily as the inner aggregate updates).
                return ResizeTo(Visit(csp.Input, memo), csp.Schema.Count);

            // Recursion + frontier is unsolved here; conservatively not monotone.
            case RecursiveCtePlan r:
                return new IReadOnlySet<LatenessSource>?[r.Schema.Count];

            default:
                return new IReadOnlySet<LatenessSource>?[node.Schema.Count];
        }
    }

    private static IReadOnlySet<LatenessSource>?[] ComputeJoin(
        JoinPlan j,
        Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]> memo)
    {
        var left = Visit(j.Left, memo);
        var right = Visit(j.Right, memo);
        var leftCount = j.Left.Schema.Count;
        var cols = new IReadOnlySet<LatenessSource>?[j.Schema.Count]; // left ++ right

        // Only equi-key columns can stay monotone — non-key columns pair with
        // arbitrarily-old rows on the other side, so they lose their bound.
        foreach (var eq in j.EquiKeys)
        {
            var leftOut = eq.LeftIndex;
            var rightOut = leftCount + eq.RightIndex;
            switch (j.JoinType)
            {
                case JoinType.Inner:
                    // Output key value = left.k = right.k; a key dies only once
                    // it is below both frontiers ⇒ frontier = min ⇒ union sources.
                    if (left[eq.LeftIndex] is { } li && right[eq.RightIndex] is { } ri)
                    {
                        var union = Union(li, ri);
                        cols[leftOut] = union;
                        cols[rightOut] = union;
                    }

                    break;

                case JoinType.LeftOuter:
                    // Preserved side = left: its key carries the real value even
                    // for unmatched (NULL-padded) rows. The right key is NULL for
                    // unmatched rows ⇒ not monotone.
                    if (left[eq.LeftIndex] is { } ls)
                    {
                        cols[leftOut] = ls;
                    }

                    break;

                case JoinType.RightOuter:
                    if (right[eq.RightIndex] is { } rs)
                    {
                        cols[rightOut] = rs;
                    }

                    break;
            }
        }

        return cols;
    }

    private static IReadOnlySet<LatenessSource>?[] ComputeUnion(
        UnionAllPlan u,
        Dictionary<LogicalPlan, IReadOnlySet<LatenessSource>?[]> memo)
    {
        var branches = u.Branches.Select(b => Visit(b, memo)).ToList();
        var cols = new IReadOnlySet<LatenessSource>?[u.Schema.Count];
        for (var i = 0; i < cols.Length; i++)
        {
            // Monotone iff monotone in every branch (any branch can emit a small
            // value otherwise); union the contributing sources.
            HashSet<LatenessSource>? union = null;
            var allMonotone = true;
            foreach (var branch in branches)
            {
                if (i >= branch.Length || branch[i] is not { } src)
                {
                    allMonotone = false;
                    break;
                }

                union ??= new HashSet<LatenessSource>();
                union.UnionWith(src);
            }

            if (allMonotone && union is not null)
            {
                cols[i] = union;
            }
        }

        return cols;
    }

    private static IReadOnlySet<LatenessSource> Union(
        IReadOnlySet<LatenessSource> a, IReadOnlySet<LatenessSource> b)
    {
        var s = new HashSet<LatenessSource>(a);
        s.UnionWith(b);
        return s;
    }

    private static IReadOnlySet<LatenessSource>?[] ResizeTo(IReadOnlySet<LatenessSource>?[] src, int length)
    {
        if (src.Length == length)
        {
            return src;
        }

        var resized = new IReadOnlySet<LatenessSource>?[length];
        Array.Copy(src, resized, Math.Min(src.Length, length));
        return resized;
    }
}
