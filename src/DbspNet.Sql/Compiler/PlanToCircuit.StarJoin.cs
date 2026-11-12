// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

public static partial class PlanToCircuit
{
    /// <summary>
    /// A run of LEFT joins against small dimensions, every join key computed from the run's input
    /// row, as one <see cref="StarLeftJoinOp"/> (docs/design-bounded-memory.md §5.48). Walks down the
    /// left spine from <paramref name="top"/> through projections, collecting members: LEFT, a
    /// residual the operator can take, no join-key GC, a small leaf right side (broadcast at W&gt;1),
    /// no node shared with another consumer, and a key that, rewritten through the projections
    /// between, reads only the run's input row (a residual: that row and its own dimension). Every
    /// expression of the run is rewritten over one combined row, the input row and then each
    /// member's dimension row (bottom first), so no intermediate row is built. Null when fewer than
    /// two members qualify.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>>? TryCompileStarJoin(
        CircuitBuilder builder, JoinPlan top, CompileContext ctx, LinearStage? post)
    {
        if (!StarJoinMode.Enabled
            || ctx.Options.TraceFamily == TraceFamily.Spine
            || ctx.SharedPlans is not { } shared
            || !IsStarMember(top, ctx))
        {
            return null;
        }

        // Top-down: the members, and the projections between each member's left input and the next.
        var joins = new List<JoinPlan> { top };
        var paths = new List<List<ProjectPlan>>();
        while (true)
        {
            var path = new List<ProjectPlan>();
            LogicalPlan node = joins[^1].Left;
            var ok = true;
            while (true)
            {
                if (shared.Contains(node) || ctx.PlanCache.ContainsKey(node))
                {
                    ok = false;
                    break;
                }

                if (Unwrap(node, ctx) is { } inlined)
                {
                    node = inlined;
                }
                else if (node is ProjectPlan p)
                {
                    path.Add(p);
                    node = p.Input;
                }
                else
                {
                    break;
                }
            }

            if (!ok || node is not JoinPlan next || !IsStarMember(next, ctx))
            {
                break;
            }

            joins.Add(next);
            paths.Add(path);
        }

        if (joins.Count < 2)
        {
            return null;
        }

        // Bottom-up from here: member 0 is the lowest join; paths[j] lies between member j and j+1.
        joins.Reverse();
        paths.Reverse();
        foreach (var path in paths)
        {
            path.Reverse();
        }

        LogicalPlan input;
        int inCount, k;
        int[] offsets;
        ResolvedExpression[][] keyExprs;
        ResolvedExpression?[] residuals;
        List<ProjectionItem> topLeft, topJoined;
        while (true)
        {
            if (joins.Count < 2)
            {
                return null;
            }

            input = joins[0].Left;
            inCount = input.Schema.Count;
            k = joins.Count;
            offsets = new int[k];
            for (int j = 0, at = inCount; j < k; at += joins[j].Right.Schema.Count, j++)
            {
                offsets[j] = at;
            }

            // The columns of member j's left input, as expressions over the combined row.
            var inSchema = input.Schema;
            var cols = Enumerable.Range(0, inCount)
                .Select(i => new ProjectionItem(new ResolvedColumn(i, inSchema[i].Type), inSchema[i].Name))
                .ToList();
            keyExprs = new ResolvedExpression[k][];
            residuals = new ResolvedExpression?[k];
            topLeft = cols;
            topJoined = cols;
            var runs = k;
            for (var j = 0; j < k; j++)
            {
                var join = joins[j];
                var rightCols = Enumerable.Range(0, join.Right.Schema.Count)
                    .Select(c => new ProjectionItem(new ResolvedColumn(offsets[j] + c, join.Right.Schema[c].Type), join.Right.Schema[c].Name));
                var joined = cols.Concat(rightCols).ToList();
                var keys = join.EquiKeys.Select(eq => cols[eq.LeftIndex].Expression).ToArray();
                ResolvedExpression? residual = join.Residual is null ? null : ExpressionRewriter.SubstituteViaProjection(join.Residual, joined);
                var dimEnd = offsets[j] + join.Right.Schema.Count;
                if (keys.Any(e => ExpressionRewriter.CollectColumnIndices(e).Any(i => i >= inCount))
                    || (residual is not null && ExpressionRewriter.CollectColumnIndices(residual).Any(i => i >= inCount && (i < offsets[j] || i >= dimEnd))))
                {
                    // This member's key reads an earlier member's columns (not v1: §5.48.5 "later"): the
                    // run starts above it, and the joins up to it are the run's input (where a run of
                    // their own may form when they compile).
                    runs = j;
                    break;
                }

                keyExprs[j] = keys;
                residuals[j] = residual;
                topLeft = cols;
                topJoined = joined;
                if (j + 1 < k)
                {
                    cols = joined;
                    foreach (var p in paths[j])
                    {
                        var below = cols;
                        cols = p.Projections
                            .Select(item => new ProjectionItem(ExpressionRewriter.SubstituteViaProjection(item.Expression, below), item.Name, item.Qualifier))
                            .ToList();
                    }
                }
            }

            if (runs == k)
            {
                break;
            }

            joins.RemoveRange(0, runs + 1);
            paths.RemoveRange(0, Math.Min(runs + 1, paths.Count));
        }

        var codec = ctx.Codec;
        var workers = ctx.Workers;

        // The fact: the run's input, its own Filter/Project chain fused into the operator's pass.
        Stream<ZSet<StructuralRow, Z64>> factIn;
        Func<StructuralRow, StructuralRow?>? pre = null;
        if (IndexFusionMode.Enabled)
        {
            (factIn, pre, _) = CompileIndexInputCore(builder, input, ctx, nullKeyIndices: null);
        }
        else
        {
            factIn = CompilePlan(builder, input, ctx);
        }

        // The tuple key: every member's key columns, in member order, nullable (a NULL key pads).
        var tupleCols = new List<SchemaColumn>();
        var keyEval = new List<Func<IReadOnlyList<object?>, object?>>();
        var spans = new (int Start, int Width)[k];
        for (var j = 0; j < k; j++)
        {
            spans[j] = (tupleCols.Count, keyExprs[j].Length);
            for (var c = 0; c < keyExprs[j].Length; c++)
            {
                var leftCol = joins[j].Left.Schema[joins[j].EquiKeys[c].LeftIndex];
                tupleCols.Add(new SchemaColumn($"__star{j}_{c}", leftCol.Type.WithNullable(true)));
                keyEval.Add(ExpressionCompiler.CompileScalar(keyExprs[j][c]));
            }
        }

        var tupleSchema = new Schema(tupleCols);
        var tupleOf = StarTupleOf(codec, tupleSchema, [.. keyEval]);

        var members = new StarMember[k];
        var files = new List<ReshardFile?>();
        for (var j = 0; j < k; j++)
        {
            var join = joins[j];
            var (leftIndices, rightIndices) = ExtractEquiKeyIndices(join);
            var memberKeySchema = join.Left.Schema.SubsetByIndex(leftIndices);
            var rightKeySchema = join.Right.Schema.SubsetByIndex(rightIndices);
            var right = CompilePlan(builder, join.Right, ctx);
            var validKeyRight = builder.Filter(right, StarNoNullKey(rightIndices));
            var rightIndexed = builder.GroupProject(
                workers > 1 ? builder.BroadcastExchange(validKeyRight) : validKeyRight,
                StarRightKey(codec, rightKeySchema, rightIndices),
                static row => row);
            IGroupTrace<StructuralRow, StructuralRow, Z64> rightTrace;
            if (TraceBlocksMode.Join && TraceBlockFormats.Create(rightKeySchema, join.Right.Schema) is { } rightFormat)
            {
                AttachIndexedDeltaFormat(rightIndexed, rightFormat);
                rightTrace = new BlockedGroupTrace(rightFormat);
            }
            else
            {
                rightTrace = new ManagedGroupTrace<StructuralRow, StructuralRow, Z64>();
            }

            var rightCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, join.Right.Schema);
            var residual = residuals[j] is { } residualExpr
                ? StarResidual(ExpressionCompiler.CompilePredicate(residualExpr), new StarRowView(inCount, offsets, j, offsets[j] + join.Right.Schema.Count))
                : null;
            members[j] = new StarMember(
                rightIndexed, rightTrace, rightCodec, StarKeyOfTuple(codec, memberKeySchema, spans[j].Start, spans[j].Width), residual);
            files.Add(IZ(StarLeftJoinOp.DimFile(j), workers <= 1 ? null : ReshardPlacement.OnEveryWorker, rightCodec));
        }

        // The output row, built once: the top member's consumer projection when one folds, otherwise
        // the top join's own row. The top's left columns are read lazily, so a projection that drops
        // most of them never evaluates them.
        var combined = new StarCombinedView(inCount, offsets, offsets[^1] + joins[^1].Right.Schema.Count);
        var build = post is { } map
            ? StarBuild(codec, map.OutSchema!, map.Delegates!, new StarTopView(
                combined, [.. topLeft.Select(item => ExpressionCompiler.CompileScalar(item.Expression))], topLeft.Count, k - 1,
                topLeft.Count + joins[^1].Right.Schema.Count))
            : StarBuild(codec, top.Schema, [.. topJoined.Select(item => ExpressionCompiler.CompileScalar(item.Expression))], combined);

        IGroupTrace<StructuralRow, StructuralRow, Z64> factTrace =
            TraceBlocksMode.Join && TraceBlockFormats.Create(tupleSchema, input.Schema) is { } factFormat
                ? new BlockedGroupTrace(factFormat)
                : new ManagedGroupTrace<StructuralRow, StructuralRow, Z64>();
        var factCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(tupleSchema, input.Schema);
        files.Insert(0, IZ(StarLeftJoinOp.FactFileName, workers <= 1 ? null : ReshardPlacement.Anywhere, factCodec));

        var result = Recorded(
            builder, ctx,
            () => builder.StarLeftJoin(factIn, pre, tupleOf, members, build, factTrace, factCodec),
            [.. files]);
        AttachDeltaFormat(result, post?.OutSchema ?? top.Schema);

        // No partition claim: the projections inside the run (and a folded consumer) may move the
        // columns the input was partitioned by.
        Interlocked.Increment(ref StarJoinMode.Built);
        return result;
    }

    // The operator's delegates are each made in a method of their own, closing over only what they
    // read: a lambda in TryCompileStarJoin would share its closure with the one that captures the
    // builder, and the release planner would find every stream compiled so far through it (§5.47).
    private static Func<StructuralRow, StructuralRow> StarTupleOf(
        IRowCodec<StructuralRow> codec, Schema tupleSchema, Func<IReadOnlyList<object?>, object?>[] keyEvals) =>
        r =>
        {
            var values = new object?[keyEvals.Length];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = keyEvals[i](r);
            }

            return codec.BuildRow(tupleSchema, values);
        };

    private static Func<StructuralRow, bool> StarNoNullKey(int[] indices) => row => HasNoNullKey(row, indices);

    private static Func<StructuralRow, StructuralRow> StarRightKey(IRowCodec<StructuralRow> codec, Schema keySchema, int[] indices) =>
        row => ExtractKey(codec, keySchema, row, indices);

    private static Func<StructuralRow, StructuralRow?> StarKeyOfTuple(
        IRowCodec<StructuralRow> codec, Schema keySchema, int start, int width) =>
        tuple =>
        {
            var values = new object?[width];
            for (var c = 0; c < width; c++)
            {
                if ((values[c] = tuple[start + c]) is null)
                {
                    return null;
                }
            }

            return codec.BuildRow(keySchema, values);
        };

    private static Func<StructuralRow, StructuralRow, bool> StarResidual(
        Func<IReadOnlyList<object?>, bool> predicate, StarRowView view) =>
        (r, d) => predicate(view.Aim(r, d));

    private static Func<StructuralRow, StructuralRow?[], StructuralRow> StarBuild(
        IRowCodec<StructuralRow> codec, Schema schema, Func<IReadOnlyList<object?>, object?>[] columns, IStarView view) =>
        (r, ds) =>
        {
            var row = view.Aim(r, ds);
            var values = new object?[columns.Length];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = columns[i](row);
            }

            return codec.BuildRow(schema, values);
        };

    private interface IStarView
    {
        IReadOnlyList<object?> Aim(StructuralRow input, StructuralRow?[] dims);
    }

    // A LEFT join the star can take: an equi-key, a residual the operator handles, no join-key GC, a
    // small leaf right side (broadcast at W>1, so the fact never moves).
    private static bool IsStarMember(JoinPlan join, CompileContext ctx)
    {
        if (join.JoinType != DbspNet.Sql.Parser.Ast.JoinType.LeftOuter
            || join.EquiKeys.Count == 0
            || join.AllowNullKeys
            || !ResidualInOperator(join, ctx)
            || ResolveJoinKeyFrontier(join, ctx).Frontier is not null
            || !IsLeafRelation(join.Right, out var relation))
        {
            return false;
        }

        if (ctx.Workers > 1)
        {
            return ShouldBroadcastRight(join.Right, ctx);
        }

        // W=1: the same size test as a broadcast, without the worker count.
        if (ctx.Options.BroadcastSmallDimensionJoins)
        {
            return true;
        }

        var counts = ctx.RowCounts ?? ctx.Options.RelationRowCounts;
        if (ctx.Options.BroadcastMaxRows <= 0 || relation is null || counts is null)
        {
            return false;
        }

        var estimate = CardinalityEstimator.Estimate(join.Right, name => counts.TryGetValue(name, out var n) ? n : (long?)null);
        return estimate <= ctx.Options.BroadcastMaxRows;
    }

    /// <summary>The combined row of a star run read in place: the input row's columns, then each
    /// member's dimension row (NULL where it padded). One per compiled operator; not reentrant.</summary>
    private sealed class StarCombinedView(int inCount, int[] offsets, int count) : IReadOnlyList<object?>, IStarView
    {
        private StructuralRow _input = null!;
        private StructuralRow?[] _dims = [];

        public int Count => count;

        public object? this[int index]
        {
            get
            {
                if (index < inCount)
                {
                    return _input[index];
                }

                var j = Array.BinarySearch(offsets, index);
                if (j < 0)
                {
                    j = ~j - 1;
                }

                return _dims[j]?[index - offsets[j]];
            }
        }

        public IReadOnlyList<object?> Aim(StructuralRow input, StructuralRow?[] dims)
        {
            _input = input;
            _dims = dims;
            return this;
        }

        public IEnumerator<object?> GetEnumerator() => throw new NotSupportedException();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>The top member's joined row, read in place: its left input's columns evaluated over
    /// the combined row on demand, then the top dimension's row.</summary>
    private sealed class StarTopView(
        StarCombinedView combined, Func<IReadOnlyList<object?>, object?>[] left, int leftCount, int topMember, int count)
        : IReadOnlyList<object?>, IStarView
    {
        private StructuralRow?[] _dims = [];

        public int Count => count;

        public object? this[int index] =>
            index < leftCount ? left[index](combined) : _dims[topMember]?[index - leftCount];

        public IReadOnlyList<object?> Aim(StructuralRow input, StructuralRow?[] dims)
        {
            combined.Aim(input, dims);
            _dims = dims;
            return this;
        }

        public IEnumerator<object?> GetEnumerator() => throw new NotSupportedException();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A member's residual reads the input row and its own dimension row of the combined
    /// row; every other position is NULL (the residual never reads one).</summary>
    private sealed class StarRowView(int inCount, int[] offsets, int member, int count) : IReadOnlyList<object?>
    {
        private StructuralRow _input = null!;
        private StructuralRow _dim = null!;

        public int Count => count;

        public object? this[int index] =>
            index < inCount ? _input[index]
            : index >= offsets[member] && (member + 1 == offsets.Length || index < offsets[member + 1]) ? _dim[index - offsets[member]]
            : null;

        public StarRowView Aim(StructuralRow input, StructuralRow dim)
        {
            _input = input;
            _dim = dim;
            return this;
        }

        public IEnumerator<object?> GetEnumerator() => throw new NotSupportedException();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
