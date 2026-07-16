// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Pure (non-circuit) evaluator for a <see cref="LogicalPlan"/> against a
/// snapshot of base-table Z-sets. Handles every plan node <b>except</b>
/// <see cref="RecursiveCtePlan"/> (which compiles to the nested fixpoint
/// circuit rather than being batch-evaluated). One use:
/// <list type="bullet">
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

            case TemporalFilterPlan tf:
                {
                    // The temporal filter at logical time `now` is the plain
                    // relational filter `validAt(now)` — this is what makes the
                    // incremental-equals-batch oracle hold (with Now = the
                    // circuit's final clock). Validity logic is kept identical to
                    // TemporalFilterOp.IsValidAt.
                    var input = Evaluate(tf.Input, ctx);
                    var timeKey = ExpressionCompiler.CompileScalar(tf.TimeKey);
                    var now = ctx.Now;
                    return input.Filter(row => TemporalValid(timeKey(row), now, tf));
                }

            case ProjectPlan p:
                return BatchProject(p, ctx);

            case JoinPlan j:
                return j.JoinType switch
                {
                    DbspNet.Sql.Parser.Ast.JoinType.Inner => BatchInnerJoin(j, ctx),
                    DbspNet.Sql.Parser.Ast.JoinType.LeftOuter => BatchLeftOuterJoin(j, ctx),
                    DbspNet.Sql.Parser.Ast.JoinType.RightOuter => BatchRightOuterJoin(j, ctx),
                    DbspNet.Sql.Parser.Ast.JoinType.FullOuter => BatchFullOuterJoin(j, ctx),
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

            case WindowAggregatePlan wa:
                return BatchWindowAggregate(wa, ctx);

            case WindowOffsetPlan wo:
                return BatchWindowOffset(wo, ctx);

            case ScalarSubqueryJoinPlan s:
                return BatchScalarSubqueryJoin(s, ctx);

            case SemiJoinPlan sj:
                return BatchSemiJoin(sj, ctx);

            case CorrelatedScalarSubqueryJoinPlan csp:
                return BatchCorrelatedScalarSubqueryJoin(csp, ctx);

            case RecursiveCtePlan:
                throw new InvalidOperationException(
                    "RecursiveCtePlan cannot be batch-evaluated directly; it compiles to the nested fixpoint circuit.");

            default:
                throw new InvalidOperationException($"unsupported plan node: {plan.GetType().Name}");
        }
    }

    // ---- Temporal filter ----

    // Mirror of TemporalFilterOp.IsValidAt (+ its SaturatingAdd), with the same
    // clock/key/offset-unit handling PlanToCircuit.CompileTemporalFilter applies:
    // a TIMESTAMP clock compares raw µs, a DATE clock floors `now` to its
    // day-number and reads the key as a day-number (offsets are then whole days).
    // Kept in lockstep: a divergence here breaks the incremental-equals-batch
    // oracle.
    private static bool TemporalValid(object? keyValue, long now, TemporalFilterPlan tf)
    {
        long key;
        if (tf.Clock == TemporalClock.Date)
        {
            if (keyValue is not Date32 dk)
            {
                return false; // NULL time key is never valid
            }

            key = dk.Days;
            // Floor the clock to its day-number; mirror TransformedFrontier in
            // passing the unset sentinel (logical −∞) through untouched.
            now = now == long.MinValue ? long.MinValue : Date32.DayNumberFloor(now);
        }
        else
        {
            if (keyValue is not Timestamp ts)
            {
                return false; // NULL time key is never valid
            }

            key = ts.Microseconds;
        }

        if (tf.AppearOffset is { } a)
        {
            var lower = SaturatingAdd(key, a);
            if (tf.AppearInclusive ? now < lower : now <= lower)
            {
                return false;
            }
        }

        if (tf.DisappearOffset is { } d)
        {
            var upper = SaturatingAdd(key, d);
            if (tf.DisappearInclusive ? now > upper : now >= upper)
            {
                return false;
            }
        }

        return true;
    }

    private static long SaturatingAdd(long a, long b)
    {
        var sum = unchecked(a + b);
        if (((a ^ sum) & (b ^ sum)) < 0)
        {
            return b > 0 ? long.MaxValue : long.MinValue;
        }

        return sum;
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
        // Mirrors the circuit's LEFT JOIN semantics:
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

    private static ZSet<StructuralRow, Z64> BatchFullOuterJoin(JoinPlan plan, BatchEvalContext ctx)
    {
        // FULL OUTER = inner (both sides present) + NULL-padded-right (left rows
        // whose key has no right match) + NULL-padded-left (right rows whose key
        // has no left match). Null-keyed rows can never match: each emits its own
        // NULL-padded row.
        var left = Evaluate(plan.Left, ctx);
        var right = Evaluate(plan.Right, ctx);
        var (leftIdx, rightIdx) = KeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

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
                rightByKey[key] = list = new List<(StructuralRow, Z64)>();
            }

            list.Add((row, w));
        }

        // Track which right keys found a left match so the right-only pass below
        // skips them.
        var matchedRightKeys = new HashSet<StructuralRow>();
        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (lrow, lw) in left)
        {
            if (HasNullKey(lrow, leftIdx))
            {
                builder.Add(NullPadRight(ctx.Codec, lrow, leftCount, rightCount), lw);
                continue;
            }

            var key = ExtractKey(ctx.Codec, lrow, leftIdx);
            if (rightByKey.TryGetValue(key, out var matches) && matches.Count > 0)
            {
                matchedRightKeys.Add(key);
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

        // Right-only rows: null-keyed always pad; otherwise pad iff the key had
        // no left match.
        foreach (var (rrow, rw) in right)
        {
            if (HasNullKey(rrow, rightIdx))
            {
                builder.Add(NullPadLeft(ctx.Codec, rrow, leftCount, rightCount), rw);
                continue;
            }

            var key = ExtractKey(ctx.Codec, rrow, rightIdx);
            if (!matchedRightKeys.Contains(key))
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

        // Group key = any scalar expression, compiled to a per-row delegate
        // (mirrors PlanToCircuit.CompileAggregate so the oracle keys identically).
        var keyFns = new Func<IReadOnlyList<object?>, object?>[plan.GroupKeys.Count];
        var keyCols = new SchemaColumn[plan.GroupKeys.Count];
        for (var i = 0; i < plan.GroupKeys.Count; i++)
        {
            keyFns[i] = ExpressionCompiler.CompileScalar(plan.GroupKeys[i]);
            keyCols[i] = new SchemaColumn("$gk" + i, plan.GroupKeys[i].Type);
        }

        var groupKeySchema = new Schema(keyCols);

        var sqlAggs = new SqlAggregator[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            sqlAggs[i] = BuildSqlAggregator(plan.Aggregates[i]);
        }

        // Partition input rows by group key into per-group Z-set builders.
        var groups = new Dictionary<StructuralRow, ZSetBuilder<StructuralRow, Z64>>();
        foreach (var (row, w) in input)
        {
            var kvs = new object?[keyFns.Length];
            for (var i = 0; i < keyFns.Length; i++)
            {
                kvs[i] = keyFns[i](row);
            }

            var key = ctx.Codec.BuildRow(groupKeySchema, kvs);
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

    // Batch window aggregate — the from-scratch oracle for
    // PartitionedWindowAggregateOp. For each partition, each row's frame is the
    // rows whose ORDER BY value lies in the (value-space) frame range; the
    // aggregate over that multiset is appended to the row. Mirrors the operator's
    // value-based frame geometry so the incremental run telescopes to this.
    private static ZSet<StructuralRow, Z64> BatchWindowAggregate(WindowAggregatePlan plan, BatchEvalContext ctx)
    {
        var input = Evaluate(plan.Input, ctx);

        var partFns = new Func<IReadOnlyList<object?>, object?>[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            partFns[i] = ExpressionCompiler.CompileScalar(plan.PartitionKeys[i]);
        }

        var sqlAggs = new SqlAggregator[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            sqlAggs[i] = BuildSqlAggregator(plan.Aggregates[i]);
        }

        var whole = plan.OrderKey is null;
        Func<IReadOnlyList<object?>, object?>? orderFn =
            plan.OrderKey is { } sk ? ExpressionCompiler.CompileScalar(sk.Expression) : null;
        var descending = plan.OrderKey?.Descending ?? false;
        var preceding = plan.Frame?.Preceding;
        var aggCount = plan.Aggregates.Count;

        // Partition rows (keyed by the partition tuple); carry each row's order
        // value alongside.
        var parts = new Dictionary<StructuralRow, List<(StructuralRow Row, long Weight, long Value)>>();
        foreach (var (row, w) in input)
        {
            var kvs = new object?[partFns.Length];
            for (var i = 0; i < partFns.Length; i++)
            {
                kvs[i] = partFns[i](row);
            }

            var key = new StructuralRow(kvs);
            var value = orderFn is null ? 0L : orderFn(row) is { } v ? MonotoneKey.Extract(v) : long.MinValue;
            if (!parts.TryGetValue(key, out var lst))
            {
                lst = new List<(StructuralRow, long, long)>();
                parts[key] = lst;
            }

            lst.Add((row, w.Value, value));
        }

        var result = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (_, rows) in parts)
        {
            foreach (var r in rows)
            {
                ZSet<StructuralRow, Z64> frame;
                if (whole)
                {
                    frame = ZSet.FromEntries(rows.Select(x => (x.Row, new Z64(x.Weight))));
                }
                else
                {
                    long lo, hi;
                    if (preceding is null)
                    {
                        lo = descending ? r.Value : long.MinValue;
                        hi = descending ? long.MaxValue : r.Value;
                    }
                    else
                    {
                        lo = descending ? r.Value : r.Value - preceding.Value;
                        hi = descending ? r.Value + preceding.Value : r.Value;
                    }

                    frame = ZSet.FromEntries(rows
                        .Where(x => x.Value >= lo && x.Value <= hi)
                        .Select(x => (x.Row, new Z64(x.Weight))));
                }

                var nonEmpty = !Z64.IsZero(frame.SumWeights());
                var vs = new object?[r.Row.Count + aggCount];
                for (var i = 0; i < r.Row.Count; i++)
                {
                    vs[i] = r.Row[i];
                }

                for (var i = 0; i < aggCount; i++)
                {
                    vs[r.Row.Count + i] = nonEmpty ? sqlAggs[i].Compute(frame) : null;
                }

                result.Add(new StructuralRow(vs), new Z64(r.Weight));
            }
        }

        return result.Build();
    }

    // Batch LAG/LEAD — the from-scratch oracle for PartitionedOffsetOp. Sort each
    // partition by the total order, expand weights into positional slots, and read
    // each function's value from its offset slot. Mirrors the operator exactly.
    private static ZSet<StructuralRow, Z64> BatchWindowOffset(WindowOffsetPlan plan, BatchEvalContext ctx)
    {
        var input = Evaluate(plan.Input, ctx);

        var partFns = new Func<IReadOnlyList<object?>, object?>[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            partFns[i] = ExpressionCompiler.CompileScalar(plan.PartitionKeys[i]);
        }

        var sortKeys = new Func<StructuralRow, object?>[plan.OrderKeys.Count];
        var sortDescending = new bool[plan.OrderKeys.Count];
        var sortNullsFirst = new bool[plan.OrderKeys.Count];
        for (var i = 0; i < plan.OrderKeys.Count; i++)
        {
            var sortScalar = ExpressionCompiler.CompileScalar(plan.OrderKeys[i].Expression);
            sortKeys[i] = row => sortScalar(row);
            sortDescending[i] = plan.OrderKeys[i].Descending;
            sortNullsFirst[i] = plan.OrderKeys[i].NullsFirst;
        }

        var order = new SortKeyComparer<StructuralRow>(
            sortKeys, sortDescending, sortNullsFirst, StructuralRowComparer.Instance);

        var valueFns = new Func<StructuralRow, object?>[plan.Functions.Count];
        for (var i = 0; i < plan.Functions.Count; i++)
        {
            valueFns[i] = ExpressionCompiler.CompileScalar(plan.Functions[i].Value);
        }

        var parts = new Dictionary<StructuralRow, List<(StructuralRow Row, long Weight)>>();
        foreach (var (row, w) in input)
        {
            var kvs = new object?[partFns.Length];
            for (var i = 0; i < partFns.Length; i++)
            {
                kvs[i] = partFns[i](row);
            }

            var key = new StructuralRow(kvs);
            if (!parts.TryGetValue(key, out var lst))
            {
                lst = new List<(StructuralRow, long)>();
                parts[key] = lst;
            }

            lst.Add((row, w.Value));
        }

        var result = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (_, rows) in parts)
        {
            rows.Sort((a, b) => order.Compare(a.Row, b.Row));
            var slots = new List<StructuralRow>();
            foreach (var (row, weight) in rows)
            {
                for (var c = 0L; c < weight; c++)
                {
                    slots.Add(row);
                }
            }

            for (var j = 0; j < slots.Count; j++)
            {
                var baseRow = slots[j];
                var vs = new object?[baseRow.Count + plan.Functions.Count];
                for (var i = 0; i < baseRow.Count; i++)
                {
                    vs[i] = baseRow[i];
                }

                for (var s = 0; s < plan.Functions.Count; s++)
                {
                    var fn = plan.Functions[s];
                    var src = fn.Kind switch
                    {
                        OffsetKind.Lag => j - fn.Offset,
                        OffsetKind.Lead => j + fn.Offset,
                        OffsetKind.FirstValue => 0,
                        OffsetKind.LastValue => slots.Count - 1,
                        _ => j,
                    };
                    vs[baseRow.Count + s] = src >= 0 && src < slots.Count ? valueFns[s](slots[(int)src]) : fn.Default;
                }

                result.Add(new StructuralRow(vs), Z64.One);
            }
        }

        return result.Build();
    }

    // Local copy of the switch in PlanToCircuit, kept in sync by the
    // resolver producing the same AggregateCall shape.
    private static SqlAggregator BuildSqlAggregator(AggregateCall call) => call.Kind switch
    {
        AggregateKind.CountStar => new SqlCountStarAggregator(),
        AggregateKind.Count => new SqlCountAggregator(ExpressionCompiler.CompileScalar(call.Argument!)),
        AggregateKind.CountDistinct => new SqlCountDistinctAggregator(ExpressionCompiler.CompileScalar(call.Argument!)),
        AggregateKind.ApproxCountDistinct => new SqlApproxCountDistinctAggregator(ExpressionCompiler.CompileScalar(call.Argument!)),
        AggregateKind.ApproxPercentile => DdSketchSupport.BuildStructuralPercentile(call),
        AggregateKind.Sum => new SqlSumAggregator(ExpressionCompiler.CompileScalar(call.Argument!), call.ResultType),
        AggregateKind.Min => new SqlMinMaxAggregator(ExpressionCompiler.CompileScalar(call.Argument!), wantMin: true),
        AggregateKind.Max => new SqlMinMaxAggregator(ExpressionCompiler.CompileScalar(call.Argument!), wantMin: false),
        AggregateKind.Avg => new SqlAvgAggregator(ExpressionCompiler.CompileScalar(call.Argument!), call.ResultType),
        _ => throw new InvalidOperationException($"unknown aggregate kind {call.Kind}"),
    };

    // ---- Semi-join (uncorrelated IN-subquery) ----

    private static ZSet<StructuralRow, Z64> BatchSemiJoin(SemiJoinPlan plan, BatchEvalContext ctx)
    {
        // Mirrors CompileSemiJoin: keep outer rows whose composite probe
        // matches at least one inner row's composite key. The key is a tuple
        // of every EquiKeys entry — one for the IN-probe plus one per
        // correlation column. Any NULL component drops the row (NULL = anything
        // is NULL, never TRUE in WHERE).
        if (plan.EquiKeys.Count == 0)
        {
            throw new InvalidOperationException("internal: SemiJoinPlan with no equi-keys");
        }

        var outer = Evaluate(plan.Input, ctx);
        var subquery = Evaluate(plan.Subquery, ctx);

        var innerIndices = new int[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            innerIndices[i] = plan.EquiKeys[i].InnerColumnIndex;
        }

        var matchSet = new HashSet<TupleKey>();
        foreach (var (row, w) in subquery)
        {
            if (!Z64.IsPositive(w))
            {
                continue;
            }

            var key = BuildTupleKey(row, innerIndices);
            if (key is not null)
            {
                matchSet.Add(key.Value);
            }
        }

        var probeFns = new Func<IReadOnlyList<object?>, object?>[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            probeFns[i] = ExpressionCompiler.CompileScalar(plan.EquiKeys[i].OuterKey);
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in outer)
        {
            var probeVals = new object?[probeFns.Length];
            var hasNull = false;
            for (var i = 0; i < probeFns.Length; i++)
            {
                var v = probeFns[i](row);
                if (v is null)
                {
                    hasNull = true;
                    break;
                }

                probeVals[i] = v;
            }

            if (hasNull)
            {
                // NULL probe drops the row in both semi and anti modes —
                // consistent with WHERE's NULL → drop semantics at the
                // conjunct level. (Anti-semi-join callers are always WHERE
                // conjuncts in v1.)
                continue;
            }

            var inMatchSet = matchSet.Contains(new TupleKey(probeVals));
            if (inMatchSet != plan.IsAnti)
            {
                builder.Add(row, w);
            }
        }

        return builder.Build();
    }

    private static TupleKey? BuildTupleKey(StructuralRow row, int[] indices)
    {
        var vs = new object?[indices.Length];
        for (var i = 0; i < indices.Length; i++)
        {
            var v = row[indices[i]];
            if (v is null)
            {
                return null;
            }

            vs[i] = v;
        }

        return new TupleKey(vs);
    }

    /// <summary>Structural-equality tuple key for the batch semi-join's match set.</summary>
    private readonly struct TupleKey : IEquatable<TupleKey>
    {
        private readonly object?[] _vs;

        public TupleKey(object?[] vs) { _vs = vs; }

        public bool Equals(TupleKey other)
        {
            if (_vs.Length != other._vs.Length) return false;
            for (var i = 0; i < _vs.Length; i++)
            {
                if (!object.Equals(_vs[i], other._vs[i])) return false;
            }

            return true;
        }

        public override bool Equals(object? obj) => obj is TupleKey t && Equals(t);

        public override int GetHashCode()
        {
            var h = 17;
            for (var i = 0; i < _vs.Length; i++)
            {
                h = unchecked(h * 31 + (_vs[i]?.GetHashCode() ?? 0));
            }

            return h;
        }
    }

    // ---- Correlated scalar subquery LEFT JOIN ----

    private static ZSet<StructuralRow, Z64> BatchCorrelatedScalarSubqueryJoin(
        CorrelatedScalarSubqueryJoinPlan plan, BatchEvalContext ctx)
    {
        var outer = Evaluate(plan.Input, ctx);
        var subquery = Evaluate(plan.Subquery, ctx);

        var innerIndices = new int[plan.CorrelationKeys.Count];
        for (var i = 0; i < plan.CorrelationKeys.Count; i++)
        {
            innerIndices[i] = plan.CorrelationKeys[i].InnerColumnIndex;
        }

        // Map composite correlation tuple → scalar value. Inner rows with
        // any NULL component drop out (no outer can match a NULL key).
        var lookup = new Dictionary<TupleKey, object?>();
        foreach (var (row, w) in subquery)
        {
            if (!Z64.IsPositive(w)) { continue; }
            var key = BuildTupleKey(row, innerIndices);
            if (key is null) { continue; }
            lookup[key.Value] = row[plan.ScalarColumnIndex];
        }

        var probeFns = new Func<IReadOnlyList<object?>, object?>[plan.CorrelationKeys.Count];
        for (var i = 0; i < plan.CorrelationKeys.Count; i++)
        {
            probeFns[i] = ExpressionCompiler.CompileScalar(plan.CorrelationKeys[i].OuterKey);
        }

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        var schema = plan.Schema;
        foreach (var (row, w) in outer)
        {
            var probeVals = new object?[probeFns.Length];
            var hasNull = false;
            for (var i = 0; i < probeFns.Length; i++)
            {
                var v = probeFns[i](row);
                if (v is null) { hasNull = true; break; }
                probeVals[i] = v;
            }

            object? scalar = null;
            if (!hasNull && lookup.TryGetValue(new TupleKey(probeVals), out var found))
            {
                scalar = found;
            }

            // Append the scalar column to the outer row (NULL on miss).
            var vs = new object?[row.Count + 1];
            for (var i = 0; i < row.Count; i++) { vs[i] = row[i]; }
            vs[row.Count] = scalar;
            builder.Add(ctx.Codec.BuildRow(schema, vs), w);
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
    /// reference — lazy CTE evaluation in this context memoizes into it, and a
    /// caller may pre-bind entries (e.g. a self-reference) before evaluating.
    /// </summary>
    public BatchEvalContext(
        IReadOnlyDictionary<string, ZSet<StructuralRow, Z64>> tables,
        Dictionary<CteRef, ZSet<StructuralRow, Z64>> ctes,
        IRowCodec<StructuralRow>? codec = null,
        long now = long.MinValue)
    {
        _tables = tables;
        _ctes = ctes;
        Codec = codec ?? StructuralRowCodec.Instance;
        Now = now;
    }

    /// <summary>Row codec for output-row construction across every plan node.</summary>
    public IRowCodec<StructuralRow> Codec { get; }

    /// <summary>
    /// The logical clock value (<c>NOW()</c>, microseconds since the epoch) a
    /// <see cref="TemporalFilterPlan"/> is evaluated against. The temporal-filter
    /// correctness oracle holds because the filter at logical time <c>t</c> is
    /// exactly the relational filter with <c>NOW = t</c>: the circuit's
    /// accumulated output at the final clock equals this batch evaluation with
    /// <see cref="Now"/> set to that same final clock. <see cref="long.MinValue"/>
    /// is "unset" (no temporal filters in play).
    /// </summary>
    public long Now { get; }

    public ZSet<StructuralRow, Z64> GetTable(string name) =>
        _tables.TryGetValue(name, out var z) ? z : ZSet<StructuralRow, Z64>.Empty;

    public ZSet<StructuralRow, Z64> GetCte(CteRef cte)
    {
        if (_ctes.TryGetValue(cte, out var cached))
        {
            return cached;
        }

        // Lazily evaluate this CTE's body. Rejects recursive CTEs, which
        // compile to the nested fixpoint circuit rather than batch evaluation.
        if (cte.Plan is RecursiveCtePlan)
        {
            throw new InvalidOperationException(
                $"recursive CTE '{cte.Name}' has no batch binding (it compiles to the nested fixpoint circuit)");
        }

        var result = BatchPlanEvaluator.Evaluate(cte.Plan, this);
        _ctes[cte] = result;
        return result;
    }
}
