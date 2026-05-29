// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Pure (non-circuit) evaluator for a <see cref="LogicalPlan"/> against a
/// snapshot of base-table Z-sets. Handles every plan node <b>except</b>
/// <see cref="RecursiveCtePlan"/> (fixed-point evaluation lives in
/// <see cref="RecursiveCteOp"/> itself). Two uses:
/// <list type="bullet">
/// <item>Recursive-CTE runtime: the base and step subplans are evaluated
/// here each iteration of the fixed-point loop.</item>
/// <item>Test oracle: a random-query PBT compares the circuit's accumulated
/// output to what this evaluator produces over the accumulated input, for
/// any plan the generator can produce.</item>
/// </list>
/// Semantics are kept deliberately in lockstep with the incremental
/// operators — a divergence between the two is a bug in one or the other.
/// </summary>
internal static class BatchPlanEvaluator
{
    public static ZSet<StructuralRow, Z64> Evaluate(LogicalPlan plan, BatchEvalContext ctx)
    {
        switch (plan)
        {
            case ScanPlan s:
                return ctx.GetTable(s.TableName);

            case CteScanPlan c:
                return ctx.GetCte(c.Cte);

            case FilterPlan f:
                {
                    var input = Evaluate(f.Input, ctx);
                    var pred = ExpressionCompiler.CompilePredicate(f.Predicate);
                    return input.Filter(row => pred(row));
                }

            case ProjectPlan p:
                return BatchProject(p, ctx);

            case JoinPlan j:
                return j.JoinType switch
                {
                    DbspNet.Sql.Parser.Ast.JoinType.Inner => BatchInnerJoin(j, ctx),
                    DbspNet.Sql.Parser.Ast.JoinType.LeftOuter => BatchLeftOuterJoin(j, ctx),
                    DbspNet.Sql.Parser.Ast.JoinType.RightOuter => BatchRightOuterJoin(j, ctx),
                    _ => throw new InvalidOperationException($"unsupported JoinType {j.JoinType}"),
                };

            case UnionAllPlan u:
                {
                    var result = Evaluate(u.Branches[0], ctx);
                    for (var i = 1; i < u.Branches.Count; i++)
                    {
                        result += Evaluate(u.Branches[i], ctx);
                    }

                    return result;
                }

            case DistinctPlan d:
                return ToSet(Evaluate(d.Input, ctx));

            case DifferencePlan diff:
                return Evaluate(diff.Left, ctx) - Evaluate(diff.Right, ctx);

            case AggregatePlan a:
                return BatchAggregate(a, ctx);

            case ScalarSubqueryJoinPlan s:
                return BatchScalarSubqueryJoin(s, ctx);

            case SemiJoinPlan sj:
                return BatchSemiJoin(sj, ctx);

            case RecursiveCtePlan:
                throw new InvalidOperationException(
                    "RecursiveCtePlan cannot be batch-evaluated directly; it's handled by RecursiveCteOp.");

            default:
                throw new InvalidOperationException($"unsupported plan node: {plan.GetType().Name}");
        }
    }

    // ---- Project ----

    private static ZSet<StructuralRow, Z64> BatchProject(ProjectPlan plan, BatchEvalContext ctx)
    {
        var input = Evaluate(plan.Input, ctx);
        var delegates = new Func<IReadOnlyList<object?>, object?>[plan.Projections.Count];
        for (var i = 0; i < plan.Projections.Count; i++)
        {
            delegates[i] = ExpressionCompiler.CompileScalar(plan.Projections[i].Expression);
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        var outSchema = plan.Schema;
        foreach (var (row, w) in input)
        {
            var vs = new object?[delegates.Length];
            for (var i = 0; i < delegates.Length; i++)
            {
                vs[i] = delegates[i](row);
            }

            builder.Add(ctx.Codec.BuildRow(outSchema, vs), w);
        }

        return builder.Build();
    }

    // ---- Joins ----

    private static ZSet<StructuralRow, Z64> BatchInnerJoin(JoinPlan plan, BatchEvalContext ctx)
    {
        var left = Evaluate(plan.Left, ctx);
        var right = Evaluate(plan.Right, ctx);
        var (leftIdx, rightIdx) = KeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

        var rightByKey = IndexByKey(ctx.Codec, right, rightIdx, plan.AllowNullKeys);
        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (lrow, lw) in left)
        {
            if (!plan.AllowNullKeys && HasNullKey(lrow, leftIdx))
            {
                continue;
            }

            var key = ExtractKey(ctx.Codec, lrow, leftIdx);
            if (!rightByKey.TryGetValue(key, out var matches))
            {
                continue;
            }

            foreach (var (rrow, rw) in matches)
            {
                builder.Add(MergeRows(ctx.Codec, lrow, rrow, leftCount, rightCount), Z64.Multiply(lw, rw));
            }
        }

        var joined = builder.Build();
        if (plan.Residual is { } residual)
        {
            var pred = ExpressionCompiler.CompilePredicate(residual);
            return joined.Filter(row => pred(row));
        }

        return joined;
    }

    private static ZSet<StructuralRow, Z64> BatchLeftOuterJoin(JoinPlan plan, BatchEvalContext ctx)
    {
        // Mirrors RecursiveCteOp-free semantics of the circuit's LEFT JOIN:
        //  * null-keyed left rows always emit NULL-padded.
        //  * null-keyed right rows are dropped.
        //  * left rows with no matching right → NULL-padded.
        //  * left rows with matches → cross product per key (inner-join
        //    semantics). Set semantics are NOT applied — bag multiplicity
        //    is preserved, matching the circuit.
        var left = Evaluate(plan.Left, ctx);
        var right = Evaluate(plan.Right, ctx);
        var (leftIdx, rightIdx) = KeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

        // Index the right side only over non-null keys.
        var rightByKey = new Dictionary<StructuralRow, List<(StructuralRow Row, Z64 Weight)>>();
        foreach (var (row, w) in right)
        {
            if (HasNullKey(row, rightIdx))
            {
                continue;
            }

            var key = ExtractKey(ctx.Codec, row, rightIdx);
            if (!rightByKey.TryGetValue(key, out var list))
            {
                list = new List<(StructuralRow, Z64)>();
                rightByKey[key] = list;
            }

            list.Add((row, w));
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (lrow, lw) in left)
        {
            // Null-keyed left row: always NULL-padded.
            if (HasNullKey(lrow, leftIdx))
            {
                builder.Add(NullPadRight(ctx.Codec, lrow, leftCount, rightCount), lw);
                continue;
            }

            var key = ExtractKey(ctx.Codec, lrow, leftIdx);
            if (rightByKey.TryGetValue(key, out var matches) && matches.Count > 0)
            {
                // Circuit LEFT JOIN: when key is matched, emit inner-join
                // output (cross product per key). No extra NULL-padded row.
                foreach (var (rrow, rw) in matches)
                {
                    builder.Add(MergeRows(ctx.Codec, lrow, rrow, leftCount, rightCount), Z64.Multiply(lw, rw));
                }
            }
            else
            {
                builder.Add(NullPadRight(ctx.Codec, lrow, leftCount, rightCount), lw);
            }
        }

        return builder.Build();
    }

    private static ZSet<StructuralRow, Z64> BatchRightOuterJoin(JoinPlan plan, BatchEvalContext ctx)
    {
        // Swap sides: RIGHT JOIN is LEFT JOIN with (left, right) reversed,
        // but the output columns retain the user's written order (a-cols,
        // then b-cols). For unmatched right rows, left-side cols are NULL.
        var left = Evaluate(plan.Left, ctx);
        var right = Evaluate(plan.Right, ctx);
        var (leftIdx, rightIdx) = KeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

        var leftByKey = new Dictionary<StructuralRow, List<(StructuralRow Row, Z64 Weight)>>();
        foreach (var (row, w) in left)
        {
            if (HasNullKey(row, leftIdx))
            {
                continue;
            }

            var key = ExtractKey(ctx.Codec, row, leftIdx);
            if (!leftByKey.TryGetValue(key, out var list))
            {
                list = new List<(StructuralRow, Z64)>();
                leftByKey[key] = list;
            }

            list.Add((row, w));
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (rrow, rw) in right)
        {
            if (HasNullKey(rrow, rightIdx))
            {
                builder.Add(NullPadLeft(ctx.Codec, rrow, leftCount, rightCount), rw);
                continue;
            }

            var key = ExtractKey(ctx.Codec, rrow, rightIdx);
            if (leftByKey.TryGetValue(key, out var matches) && matches.Count > 0)
            {
                foreach (var (lrow, lw) in matches)
                {
                    builder.Add(MergeRows(ctx.Codec, lrow, rrow, leftCount, rightCount), Z64.Multiply(lw, rw));
                }
            }
            else
            {
                builder.Add(NullPadLeft(ctx.Codec, rrow, leftCount, rightCount), rw);
            }
        }

        return builder.Build();
    }

    // ---- Aggregate ----

    private static ZSet<StructuralRow, Z64> BatchAggregate(AggregatePlan plan, BatchEvalContext ctx)
    {
        // Mirrors CompositeAggregator inside IncrementalAggregate: per-group
        // multiset keyed by FULL ROW, fresh aggregate evaluation per group.
        // Linear-preserving emission gate (matches CompositeAggregator): a
        // group is emitted iff the sum of weights in its row-multiset is
        // non-zero, per the DBSP paper §7.2-7.4 and Feldera's Aggregator
        // trait contract.
        var input = Evaluate(plan.Input, ctx);

        var groupIndices = new int[plan.GroupKeys.Count];
        for (var i = 0; i < plan.GroupKeys.Count; i++)
        {
            if (plan.GroupKeys[i] is ResolvedColumn rc)
            {
                groupIndices[i] = rc.Index;
            }
            else
            {
                throw new InvalidOperationException(
                    "GROUP BY expression not a ResolvedColumn (resolver invariant)");
            }
        }

        var sqlAggs = new SqlAggregator[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            sqlAggs[i] = BuildSqlAggregator(plan.Aggregates[i]);
        }

        // Partition input rows by group key into per-group Z-set builders.
        var groups = new Dictionary<StructuralRow, ZSetBuilder<StructuralRow, Z64>>();
        foreach (var (row, w) in input)
        {
            var key = ExtractKey(ctx.Codec, row, groupIndices);
            if (!groups.TryGetValue(key, out var gb))
            {
                gb = new ZSetBuilder<StructuralRow, Z64>();
                groups[key] = gb;
            }

            gb.Add(row, w);
        }

        var result = new ZSetBuilder<StructuralRow, Z64>();
        var groupCount = plan.GroupKeys.Count;
        var aggCount = plan.Aggregates.Count;
        foreach (var (key, gb) in groups)
        {
            var groupZSet = gb.Build();
            if (Z64.IsZero(groupZSet.SumWeights()))
            {
                continue;
            }

            var vs = new object?[groupCount + aggCount];
            for (var i = 0; i < groupCount; i++)
            {
                vs[i] = key[i];
            }

            for (var i = 0; i < aggCount; i++)
            {
                vs[groupCount + i] = sqlAggs[i].Compute(groupZSet);
            }

            result.Add(ctx.Codec.BuildRow(plan.Schema, vs), Z64.One);
        }

        return result.Build();
    }

    // Local copy of the switch in PlanToCircuit, kept in sync by the
    // resolver producing the same AggregateCall shape.
    private static SqlAggregator BuildSqlAggregator(AggregateCall call) => call.Kind switch
    {
        AggregateKind.CountStar => new SqlCountStarAggregator(),
        AggregateKind.Count => new SqlCountAggregator(ExpressionCompiler.CompileScalar(call.Argument!)),
        AggregateKind.Sum => new SqlSumAggregator(ExpressionCompiler.CompileScalar(call.Argument!), call.ResultType),
        AggregateKind.Min => new SqlMinMaxAggregator(ExpressionCompiler.CompileScalar(call.Argument!), wantMin: true),
        AggregateKind.Max => new SqlMinMaxAggregator(ExpressionCompiler.CompileScalar(call.Argument!), wantMin: false),
        AggregateKind.Avg => new SqlAvgAggregator(ExpressionCompiler.CompileScalar(call.Argument!), call.ResultType),
        _ => throw new InvalidOperationException($"unknown aggregate kind {call.Kind}"),
    };

    // ---- Semi-join (uncorrelated IN-subquery) ----

    private static ZSet<StructuralRow, Z64> BatchSemiJoin(SemiJoinPlan plan, BatchEvalContext ctx)
    {
        // Mirrors CompileSemiJoin: keep outer rows whose probe is non-null
        // and matches at least one non-null distinct value in the subquery.
        // Output schema = outer schema, weights preserved.
        var outer = Evaluate(plan.Input, ctx);
        var subquery = Evaluate(plan.Subquery, ctx);

        var matchSet = new HashSet<object>();
        foreach (var (row, w) in subquery)
        {
            if (!Z64.IsPositive(w))
            {
                continue;
            }

            var v = row[0];
            if (v is not null)
            {
                matchSet.Add(v);
            }
        }

        var probeFn = ExpressionCompiler.CompileScalar(plan.OuterKey);
        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in outer)
        {
            var key = probeFn(row);
            if (key is null)
            {
                continue;
            }

            if (matchSet.Contains(key))
            {
                builder.Add(row, w);
            }
        }

        return builder.Build();
    }

    // ---- Scalar subquery cross-join ----

    private static ZSet<StructuralRow, Z64> BatchScalarSubqueryJoin(ScalarSubqueryJoinPlan plan, BatchEvalContext ctx)
    {
        // Mirrors CompileScalarSubqueryJoin's unit-key LEFT JOIN: append one
        // hidden column per subquery. Empty subquery → NULL column. >1 row
        // in the subquery → undefined (we just use the first positive-weight
        // row, matching our compiler's no-validation stance).
        var current = Evaluate(plan.Input, ctx);
        foreach (var sub in plan.Subqueries)
        {
            var subResult = Evaluate(sub, ctx);
            current = AppendScalarColumn(ctx.Codec, current, subResult);
        }

        return current;
    }

    private static ZSet<StructuralRow, Z64> AppendScalarColumn(
        IRowCodec<StructuralRow> codec,
        ZSet<StructuralRow, Z64> outer,
        ZSet<StructuralRow, Z64> subquery)
    {
        // Pick the "scalar" — any positive-weight row's single column, or NULL.
        object? scalar = null;
        foreach (var (row, w) in subquery)
        {
            if (Z64.IsPositive(w))
            {
                scalar = row[0];
                break;
            }
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in outer)
        {
            var vs = new object?[row.Count + 1];
            for (var i = 0; i < row.Count; i++)
            {
                vs[i] = row[i];
            }

            vs[row.Count] = scalar;
            builder.Add(codec.BuildRow(null, vs), w);
        }

        return builder.Build();
    }

    // ---- Helpers ----

    private static (int[] Left, int[] Right) KeyIndices(JoinPlan plan)
    {
        var leftIdx = new int[plan.EquiKeys.Count];
        var rightIdx = new int[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            leftIdx[i] = plan.EquiKeys[i].LeftIndex;
            rightIdx[i] = plan.EquiKeys[i].RightIndex;
        }

        return (leftIdx, rightIdx);
    }

    private static Dictionary<StructuralRow, List<(StructuralRow Row, Z64 Weight)>> IndexByKey(
        IRowCodec<StructuralRow> codec, ZSet<StructuralRow, Z64> z, int[] indices, bool allowNullKeys = false)
    {
        var result = new Dictionary<StructuralRow, List<(StructuralRow, Z64)>>();
        foreach (var (row, w) in z)
        {
            if (!allowNullKeys && HasNullKey(row, indices))
            {
                continue;
            }

            var key = ExtractKey(codec, row, indices);
            if (!result.TryGetValue(key, out var list))
            {
                list = new List<(StructuralRow, Z64)>();
                result[key] = list;
            }

            list.Add((row, w));
        }

        return result;
    }

    /// <summary>Collapse a Z-set to set semantics — weight 1 per distinct
    /// positive-weight row; non-positive-weight rows drop.</summary>
    private static ZSet<StructuralRow, Z64> ToSet(ZSet<StructuralRow, Z64> z)
    {
        if (z.IsEmpty)
        {
            return z;
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in z)
        {
            if (Z64.IsPositive(w))
            {
                builder.Add(row, Z64.One);
            }
        }

        return builder.Build();
    }

    private static bool HasNullKey(StructuralRow row, int[] indices)
    {
        for (var i = 0; i < indices.Length; i++)
        {
            if (row[indices[i]] is null)
            {
                return true;
            }
        }

        return false;
    }

    private static StructuralRow ExtractKey(IRowCodec<StructuralRow> codec, StructuralRow row, int[] indices)
    {
        var vs = new object?[indices.Length];
        for (var i = 0; i < indices.Length; i++)
        {
            vs[i] = row[indices[i]];
        }

        return codec.BuildRow(null, vs);
    }

    private static StructuralRow MergeRows(IRowCodec<StructuralRow> codec, StructuralRow left, StructuralRow right, int leftCount, int rightCount)
    {
        var vs = new object?[leftCount + rightCount];
        for (var i = 0; i < leftCount; i++)
        {
            vs[i] = left[i];
        }

        for (var i = 0; i < rightCount; i++)
        {
            vs[leftCount + i] = right[i];
        }

        return codec.BuildRow(null, vs);
    }

    private static StructuralRow NullPadRight(IRowCodec<StructuralRow> codec, StructuralRow left, int leftCount, int rightCount)
    {
        var vs = new object?[leftCount + rightCount];
        for (var i = 0; i < leftCount; i++)
        {
            vs[i] = left[i];
        }

        return codec.BuildRow(null, vs);
    }

    private static StructuralRow NullPadLeft(IRowCodec<StructuralRow> codec, StructuralRow right, int leftCount, int rightCount)
    {
        var vs = new object?[leftCount + rightCount];
        for (var i = 0; i < rightCount; i++)
        {
            vs[leftCount + i] = right[i];
        }

        return codec.BuildRow(null, vs);
    }
}

/// <summary>
/// Snapshot of "inputs" for <see cref="BatchPlanEvaluator"/>: each referenced
/// base table maps to its accumulated Z-set; CTEs may be pre-bound (for the
/// recursive self-ref) or evaluated lazily on first reference (with
/// memoization so a CTE referenced twice still compiles-and-runs once).
/// </summary>
internal sealed class BatchEvalContext
{
    private readonly IReadOnlyDictionary<string, ZSet<StructuralRow, Z64>> _tables;
    private readonly Dictionary<CteRef, ZSet<StructuralRow, Z64>> _ctes;

    /// <summary>
    /// Construct a context. The <paramref name="ctes"/> dict is stored by
    /// reference — <see cref="RecursiveCteOp"/> mutates it across iterations
    /// to feed the evolving self-reference value, and lazy CTE evaluation
    /// in this context likewise memoizes into the same dict.
    /// </summary>
    public BatchEvalContext(
        IReadOnlyDictionary<string, ZSet<StructuralRow, Z64>> tables,
        Dictionary<CteRef, ZSet<StructuralRow, Z64>> ctes,
        IRowCodec<StructuralRow>? codec = null)
    {
        _tables = tables;
        _ctes = ctes;
        Codec = codec ?? StructuralRowCodec.Instance;
    }

    /// <summary>Row codec for output-row construction across every plan node.</summary>
    public IRowCodec<StructuralRow> Codec { get; }

    public ZSet<StructuralRow, Z64> GetTable(string name) =>
        _tables.TryGetValue(name, out var z) ? z : ZSet<StructuralRow, Z64>.Empty;

    public ZSet<StructuralRow, Z64> GetCte(CteRef cte)
    {
        if (_ctes.TryGetValue(cte, out var cached))
        {
            return cached;
        }

        // Lazily evaluate this CTE's body. Rejects recursive CTEs, which
        // must be pre-bound by RecursiveCteOp.
        if (cte.Plan is RecursiveCtePlan)
        {
            throw new InvalidOperationException(
                $"recursive CTE '{cte.Name}' has no batch binding (should be handled by RecursiveCteOp)");
        }

        var result = BatchPlanEvaluator.Evaluate(cte.Plan, this);
        _ctes[cte] = result;
        return result;
    }
}
