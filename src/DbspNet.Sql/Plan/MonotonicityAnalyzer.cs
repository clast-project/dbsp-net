// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Plan;

/// <summary>
/// Identity of a declared <c>LATENESS</c> source: a base-table column. The
/// compiler creates one frontier per source; a derived monotone column carries
/// the set of sources whose frontiers jointly bound it.
/// </summary>
public readonly record struct LatenessSource(string Table, int Column);

/// <summary>
/// A provably-monotone output column: the <see cref="LatenessSource"/>s whose
/// advancing frontiers bound it, plus an optional <see cref="FrontierTransform"/>
/// mapping a source-space frontier value into this column's value space.
/// <c>null</c> transform means identity — the raw frontier is a sound (if
/// conservative) GC threshold, which holds for forward shifts (<c>ts + interval</c>).
/// A non-null transform (e.g. <c>date_trunc</c>) must be applied to the frontier
/// before it can threshold this column's keys.
/// </summary>
internal sealed record MonotoneColumn(IReadOnlySet<LatenessSource> Sources, Func<long, long>? FrontierTransform);

/// <summary>
/// Per-node, per-output-column monotonicity verdicts produced by
/// <see cref="MonotonicityAnalyzer"/>. A column maps to the set of
/// <see cref="LatenessSource"/>s whose advancing frontiers bound it (and an
/// optional frontier transform); a non-monotone column has no entry (no GC).
/// </summary>
/// <remarks>
/// Keyed by plan-node reference identity — two structurally-equal subplans are
/// distinct entries, and a shared CTE plan is analyzed once.
/// </remarks>
public sealed class MonotonicityInfo
{
    private readonly Dictionary<LogicalPlan, MonotoneColumn?[]> _byNode;

    internal MonotonicityInfo(Dictionary<LogicalPlan, MonotoneColumn?[]> byNode)
    {
        _byNode = byNode;
    }

    private MonotoneColumn? Get(LogicalPlan node, int column)
    {
        ArgumentNullException.ThrowIfNull(node);
        return _byNode.TryGetValue(node, out var cols) && column >= 0 && column < cols.Length
            ? cols[column]
            : null;
    }

    /// <summary>
    /// The <c>LATENESS</c> sources bounding output column <paramref name="column"/>
    /// of <paramref name="node"/>, or <c>null</c> if the column is not provably
    /// monotone.
    /// </summary>
    public IReadOnlySet<LatenessSource>? Sources(LogicalPlan node, int column) => Get(node, column)?.Sources;

    /// <summary>
    /// The frontier transform for output column <paramref name="column"/> of
    /// <paramref name="node"/> (see <see cref="MonotoneColumn"/>), or <c>null</c>
    /// for identity / non-monotone.
    /// </summary>
    internal Func<long, long>? FrontierTransform(LogicalPlan node, int column) => Get(node, column)?.FrontierTransform;

    /// <summary>True iff output column <paramref name="column"/> of <paramref name="node"/> is provably monotone.</summary>
    public bool IsMonotone(LogicalPlan node, int column) => Get(node, column) is not null;
}

/// <summary>
/// Propagates each declared <c>LATENESS</c> column's monotonicity (and the
/// advancing frontier behind it) through the logical plan, so that any stateful
/// operator whose key is monotone — directly or via filters, projections,
/// monotone scalar functions / forward-shift arithmetic, joins, unions, or
/// aggregates — can be wired for frontier-driven GC.
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
        var memo = new Dictionary<LogicalPlan, MonotoneColumn?[]>(ReferenceEqualityComparer.Instance);
        Visit(plan, memo);
        return new MonotonicityInfo(memo);
    }

    private static MonotoneColumn?[] Visit(
        LogicalPlan node,
        Dictionary<LogicalPlan, MonotoneColumn?[]> memo)
    {
        if (memo.TryGetValue(node, out var cached))
        {
            return cached;
        }

        var result = Compute(node, memo);
        memo[node] = result;
        return result;
    }

    private static MonotoneColumn?[] Compute(
        LogicalPlan node,
        Dictionary<LogicalPlan, MonotoneColumn?[]> memo)
    {
        switch (node)
        {
            case ScanPlan s:
            {
                var cols = new MonotoneColumn?[s.Schema.Count];
                if (s.ColumnLateness is { } lateness)
                {
                    foreach (var i in lateness.Keys)
                    {
                        if (i >= 0 && i < cols.Length)
                        {
                            cols[i] = new MonotoneColumn(new HashSet<LatenessSource> { new(s.TableName, i) }, null);
                        }
                    }
                }

                return cols;
            }

            // Filtering only removes rows — never produces a value below the
            // input's frontier.
            case FilterPlan f:
                return Visit(f.Input, memo);

            case TemporalFilterPlan tf:
            {
                // Schema is the input's; monotonicity passes through. A disappear
                // (upper) bound additionally means the advancing clock bounds the
                // time-key column from below — rows whose key falls below
                // clock − offset are expired and never re-emitted — so when the
                // time key is a base-table column scanned directly below, mark it
                // monotone. Its source identity reuses the column's
                // LatenessSource; PlanToCircuit registers the matching clock-driven
                // frontier so downstream GROUP BY / join / DISTINCT can GC.
                var input = Visit(tf.Input, memo);
                var cols = (MonotoneColumn?[])input.Clone();
                if (tf.DisappearOffsetMicros is not null
                    && tf.TimeKey is ResolvedColumn rc
                    && tf.Input is ScanPlan scan
                    && rc.Index >= 0 && rc.Index < cols.Length)
                {
                    cols[rc.Index] = new MonotoneColumn(
                        new HashSet<LatenessSource> { new(scan.TableName, rc.Index) }, null);
                }

                return cols;
            }

            case ProjectPlan p:
            {
                var input = Visit(p.Input, memo);
                var cols = new MonotoneColumn?[p.Projections.Count];
                for (var i = 0; i < p.Projections.Count; i++)
                {
                    cols[i] = FromExpr(p.Projections[i].Expression, input);
                }

                return cols;
            }

            case JoinPlan j:
                return ComputeJoin(j, memo);

            case AggregatePlan a:
            {
                var input = Visit(a.Input, memo);
                // Output = [group-key columns..., aggregate-result columns...].
                var cols = new MonotoneColumn?[a.Schema.Count];
                for (var g = 0; g < a.GroupKeys.Count && g < cols.Length; g++)
                {
                    cols[g] = FromExpr(a.GroupKeys[g], input);
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
                return new MonotoneColumn?[r.Schema.Count];

            default:
                return new MonotoneColumn?[node.Schema.Count];
        }
    }

    /// <summary>
    /// Resolve a projection/group-key expression to a monotone column, if any.
    /// Handles a bare column pass-through, a monotone scalar function
    /// (per <see cref="ScalarFunctionRegistry.Monotonicity"/>, carrying its
    /// frontier transform), and forward-shift arithmetic (monotone column +
    /// non-negative constant). Transforms compose only over an identity carrier,
    /// keeping the model sound and simple.
    /// </summary>
    private static MonotoneColumn? FromExpr(ResolvedExpression expr, MonotoneColumn?[] input)
    {
        switch (expr)
        {
            case ResolvedColumn rc:
                return rc.Index >= 0 && rc.Index < input.Length ? input[rc.Index] : null;

            case ResolvedFunctionCall fc:
            {
                if (ScalarFunctionRegistry.Monotonicity(fc) is { } m
                    && m.CarrierArgIndex >= 0 && m.CarrierArgIndex < fc.Arguments.Count
                    && FromExpr(fc.Arguments[m.CarrierArgIndex], input) is { FrontierTransform: null } carrier)
                {
                    return new MonotoneColumn(carrier.Sources, m.FrontierTransform);
                }

                return null;
            }

            case ResolvedBinary { Operator: BinaryOperator.Add, Left: var l, Right: var r }:
            {
                if (FromExpr(l, input) is { FrontierTransform: null } lc && IsNonNegativeConstant(r))
                {
                    return new MonotoneColumn(lc.Sources, null);
                }

                if (FromExpr(r, input) is { FrontierTransform: null } rc && IsNonNegativeConstant(l))
                {
                    return new MonotoneColumn(rc.Sources, null);
                }

                return null;
            }

            default:
                return null;
        }
    }

    private static bool IsNonNegativeConstant(ResolvedExpression e)
    {
        // See through a numeric-widening cast the resolver inserts on a mixed-type
        // literal (e.g. BIGINT ts + INT 5 ⇒ ts + CAST(5 AS BIGINT)).
        while (e is ResolvedCast c)
        {
            e = c.Operand;
        }

        return e is ResolvedLiteral lit && lit.Value switch
        {
            Interval iv => iv.Months >= 0 && iv.Micros >= 0,
            int i => i >= 0,
            long l => l >= 0,
            float f => f >= 0,
            double d => d >= 0,
            _ => false,
        };
    }

    private static MonotoneColumn?[] ComputeJoin(
        JoinPlan j,
        Dictionary<LogicalPlan, MonotoneColumn?[]> memo)
    {
        var left = Visit(j.Left, memo);
        var right = Visit(j.Right, memo);
        var leftCount = j.Left.Schema.Count;
        var cols = new MonotoneColumn?[j.Schema.Count]; // left ++ right

        // Only equi-key columns can stay monotone — non-key columns pair with
        // arbitrarily-old rows on the other side, so they lose their bound. A
        // transformed (non-identity) key is not propagated through a join (the
        // join GC site doesn't apply transforms); require identity.
        foreach (var eq in j.EquiKeys)
        {
            var leftOut = eq.LeftIndex;
            var rightOut = leftCount + eq.RightIndex;
            switch (j.JoinType)
            {
                case JoinType.Inner:
                    // Output key value = left.k = right.k; a key dies only once
                    // it is below both frontiers ⇒ frontier = min ⇒ union sources.
                    if (left[eq.LeftIndex] is { FrontierTransform: null } li
                        && right[eq.RightIndex] is { FrontierTransform: null } ri)
                    {
                        var union = new MonotoneColumn(Union(li.Sources, ri.Sources), null);
                        cols[leftOut] = union;
                        cols[rightOut] = union;
                    }

                    break;

                case JoinType.LeftOuter:
                    // Preserved side = left: its key carries the real value even
                    // for unmatched (NULL-padded) rows. The right key is NULL for
                    // unmatched rows ⇒ not monotone.
                    if (left[eq.LeftIndex] is { FrontierTransform: null } ls)
                    {
                        cols[leftOut] = new MonotoneColumn(ls.Sources, null);
                    }

                    break;

                case JoinType.RightOuter:
                    if (right[eq.RightIndex] is { FrontierTransform: null } rs)
                    {
                        cols[rightOut] = new MonotoneColumn(rs.Sources, null);
                    }

                    break;

                case JoinType.FullOuter:
                    // Neither output key column is monotone: an unmatched row
                    // NULLs out the absent side's key, so the projection loses its
                    // bound. (GC still works — it reads the input-side sources
                    // directly, both-sides-monotone.)
                    break;
            }
        }

        return cols;
    }

    private static MonotoneColumn?[] ComputeUnion(
        UnionAllPlan u,
        Dictionary<LogicalPlan, MonotoneColumn?[]> memo)
    {
        var branches = u.Branches.Select(b => Visit(b, memo)).ToList();
        var cols = new MonotoneColumn?[u.Schema.Count];
        for (var i = 0; i < cols.Length; i++)
        {
            // Monotone iff monotone (with an identity transform) in every branch;
            // union the contributing sources.
            HashSet<LatenessSource>? union = null;
            var allMonotone = true;
            foreach (var branch in branches)
            {
                if (i >= branch.Length || branch[i] is not { FrontierTransform: null } src)
                {
                    allMonotone = false;
                    break;
                }

                union ??= new HashSet<LatenessSource>();
                union.UnionWith(src.Sources);
            }

            if (allMonotone && union is not null)
            {
                cols[i] = new MonotoneColumn(union, null);
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

    private static MonotoneColumn?[] ResizeTo(MonotoneColumn?[] src, int length)
    {
        if (src.Length == length)
        {
            return src;
        }

        var resized = new MonotoneColumn?[length];
        Array.Copy(src, resized, Math.Min(src.Length, length));
        return resized;
    }
}
