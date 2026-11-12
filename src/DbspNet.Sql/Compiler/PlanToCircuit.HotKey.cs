// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

public static partial class PlanToCircuit
{
    /// <summary>
    /// The value a hashed LEFT join's left key takes for every row whose source column is NULL, when the
    /// key is computed from nullable columns by an expression that maps NULL to a constant
    /// (<c>LOWER(COALESCE(lookup.customer_path, 'NOT APPLICABLE'))</c>): every row a lookup padded shares
    /// that key, and a hash exchange sends them all to one worker (docs/design-bounded-memory.md §5.49).
    /// The key column is traced down through projections to the expression that computes it; null when
    /// it does not reach one, when the expression reads no nullable column, or when NULL inputs give NULL.
    /// </summary>
    private static StructuralRow? HotLeftKey(JoinPlan plan, int[] leftIndices, Schema leftKeySchema, CompileContext ctx)
    {
        var values = new object?[leftIndices.Length];
        for (var c = 0; c < leftIndices.Length; c++)
        {
            var (expr, below) = ColumnSource(plan.Left, leftIndices[c], ctx);
            var reads = ExpressionRewriter.CollectColumnIndices(expr);
            if (reads.Count == 0 || !reads.Any(i => below.Schema[i].Type.Nullable))
            {
                return null;
            }

            try
            {
                values[c] = ExpressionCompiler.CompileScalar(expr)(new object?[below.Schema.Count]);
            }
            catch (Exception ex) when (ex is InvalidOperationException or InvalidCastException or ArgumentException or NullReferenceException)
            {
                return null;
            }

            if (values[c] is null)
            {
                return null;
            }
        }

        return ctx.Codec.BuildRow(leftKeySchema, values);
    }

    // Column `column` of `plan` as an expression over the first node below it that is not a projection,
    // looking into CTE bodies (the parallel program compile does not inline them; an analysis only, so
    // the body is read, never compiled here). A CTE seen twice (recursion) ends the walk.
    private static (ResolvedExpression Expr, LogicalPlan Below) ColumnSource(LogicalPlan plan, int column, CompileContext ctx)
    {
        ResolvedExpression expr = new ResolvedColumn(column, plan.Schema[column].Type);
        HashSet<CteRef>? seen = null;
        while (true)
        {
            if (Unwrap(plan, ctx) is { } inlined)
            {
                plan = inlined;
            }
            else if (plan is CteScanPlan c && (seen ??= new(ReferenceEqualityComparer.Instance)).Add(c.Cte)
                     && c.Cte.Plan.Schema.Count == c.Schema.Count)
            {
                plan = c.Cte.Plan;
            }
            else if (plan is ProjectPlan p)
            {
                expr = ExpressionRewriter.SubstituteViaProjection(expr, p.Projections);
                plan = p.Input;
            }
            else
            {
                return (expr, plan);
            }
        }
    }

    /// <summary>
    /// A hashed LEFT join split on its hot key (§5.49): left rows whose key is <paramref name="hot"/> stay
    /// on their worker and join the right side's group for that key, broadcast; every other row takes the
    /// hashed join as before. Equality on the key partitions the join, so the union is the join.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> CompileHotSplitLeftJoin(
        CircuitBuilder builder,
        CompileContext ctx,
        JoinPlan plan,
        Stream<ZSet<StructuralRow, Z64>> left,
        Stream<ZSet<StructuralRow, Z64>> right,
        StructuralRow hot,
        LinearStage? post,
        Func<StructuralRow, StructuralRow, bool>? pairResidual)
    {
        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);
        var leftKeySchema = plan.Left.Schema.SubsetByIndex(leftIndices);
        var rightKeySchema = plan.Right.Schema.SubsetByIndex(rightIndices);
        var codec = ctx.Codec;
        var hotRight = codec.BuildRow(rightKeySchema, Enumerable.Range(0, hot.Count).Select(i => hot[i]).ToArray());
        var (joinCombine, nullPadCombine) = LeftJoinCombines(codec, plan, post);
        var leftCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(leftKeySchema, plan.Left.Schema);
        var rightCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, plan.Right.Schema);
        var residual = HotResidual(pairResidual);
        var leftKey = HotKeyOf(codec, leftKeySchema, leftIndices);
        var rightKey = HotKeyOf(codec, rightKeySchema, rightIndices);

        // Hot: no exchange of the left; the right's hot group on every worker.
        var hotLeft = builder.GroupProject(builder.Filter(left, HotIs(leftKey, hot, true)), leftKey, static row => row);
        var hotRightRows = builder.Filter(builder.Filter(right, StarNoNullKey(rightIndices)), HotIs(rightKey, hotRight, true));
        var hotRightIndexed = builder.GroupProject(builder.BroadcastExchange(hotRightRows), rightKey, static row => row);
        var hotJoined = EmitLeftJoin(
            builder, ctx, hotLeft, hotRightIndexed, joinCombine, nullPadCombine, leftCodec, rightCodec,
            leftReshard: ReshardPlacement.Anywhere, rightReshard: ReshardPlacement.OnEveryWorker,
            leftShape: (leftKeySchema, plan.Left.Schema), rightShape: (rightKeySchema, plan.Right.Schema),
            residual: residual);

        // Cold: the hashed join, NULL keys bypassing it padded, as CompileLeftOuterJoin does.
        var coldLeft = builder.Exchange(
            builder.Filter(left, HotIs(leftKey, hot, false)), HotHash(leftIndices, leftKeySchema));
        var nullKeyLeft = builder.Filter(coldLeft, HotNullKey(leftIndices, true));
        var coldIndexed = builder.GroupProject(builder.Filter(coldLeft, HotNullKey(leftIndices, false)), leftKey, static row => row);
        var coldRight = builder.Filter(builder.Filter(right, StarNoNullKey(rightIndices)), HotIs(rightKey, hotRight, false));
        var coldRightIndexed = builder.ExchangeIndex(coldRight, HotHash(rightIndices, rightKeySchema), rightKey);
        var coldJoined = EmitLeftJoin(
            builder, ctx, coldIndexed, coldRightIndexed, joinCombine, nullPadCombine, leftCodec, rightCodec,
            leftReshard: ReshardPlacement.WholeRow(leftKeySchema), rightReshard: ReshardPlacement.WholeRow(rightKeySchema),
            leftShape: (leftKeySchema, plan.Left.Schema), rightShape: (rightKeySchema, plan.Right.Schema),
            residual: residual);

        var outSchema = post?.OutSchema ?? plan.Schema;
        AttachDeltaFormat(hotJoined, outSchema);
        AttachDeltaFormat(coldJoined, outSchema);
        var pads = builder.MapRows<StructuralRow, StructuralRow, Z64>(nullKeyLeft, HotPad(nullPadCombine));
        AttachDeltaFormat(pads, outSchema);
        var result = builder.Union(builder.Union(hotJoined, coldJoined), pads);
        AttachDeltaFormat(result, outSchema);

        // The hot rows sit where they arrived: no partition claim.
        Interlocked.Increment(ref HotKeySplitMode.Built);
        HotKeySplitMode.LastHotKey = hot;
        return result;
    }

    // The join's two combines, as CompileLeftOuterJoin builds them, made here so their closure holds only
    // what they read (never ctx: §5.47).
    private static (Func<StructuralRow, StructuralRow, StructuralRow, StructuralRow> Join, Func<StructuralRow, StructuralRow, StructuralRow> Pad)
        LeftJoinCombines(IRowCodec<StructuralRow> codec, JoinPlan plan, LinearStage? post)
    {
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;
        var joinedSchema = plan.Schema;
        if (post is { } map)
        {
            return ((_, l, r) => ApplyMapJoined(map, l, r, leftCount, rightCount, codec),
                (_, l) => ApplyMapJoined(map, l, null, leftCount, rightCount, codec));
        }

        return ((_, l, r) => MergeRows(codec, joinedSchema, l, r, leftCount, rightCount),
            (_, l) => NullPadRight(codec, joinedSchema, l, leftCount, rightCount));
    }

    private static Func<StructuralRow, StructuralRow, StructuralRow, bool>? HotResidual(Func<StructuralRow, StructuralRow, bool>? pair) =>
        pair is null ? null : (_, l, r) => pair(l, r);

    private static Func<StructuralRow, StructuralRow> HotKeyOf(IRowCodec<StructuralRow> codec, Schema keySchema, int[] indices) =>
        row => ExtractKey(codec, keySchema, row, indices);

    private static Func<StructuralRow, bool> HotIs(Func<StructuralRow, StructuralRow> key, StructuralRow hot, bool want) =>
        row => key(row).Equals(hot) == want;

    private static Func<StructuralRow, bool> HotNullKey(int[] indices, bool want) => row => !HasNoNullKey(row, indices) == want;

    private static Func<StructuralRow, int> HotHash(int[] indices, Schema keySchema) =>
        row => StablePartitionHash.OfRow(row, indices, keySchema);

    private static Func<StructuralRow, StructuralRow> HotPad(Func<StructuralRow, StructuralRow, StructuralRow> pad) =>
        row => pad(null!, row);
}
