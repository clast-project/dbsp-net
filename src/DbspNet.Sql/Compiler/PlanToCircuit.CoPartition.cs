// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

public static partial class PlanToCircuit
{
    /// <summary>
    /// Co-partitioning decisions for one compiled plan (docs/design-bounded-memory.md §5.57). A hashed LEFT
    /// join exchanges both sides by its whole key, even when each side comes out of an aggregate (or a
    /// DISTINCT) that has just exchanged by a key containing the join's. Here each such aggregate / DISTINCT
    /// is asked to partition on the join's bare key columns instead (<see cref="Requests"/>), so both sides
    /// arrive co-partitioned and the join elides its exchanges; a request travels on down to an aggregate /
    /// DISTINCT feeding it, which then elides too. Two requested operators reading one input on the same
    /// columns share one exchange of it (<see cref="Shared"/>). Every decision is only a request: an
    /// operator elides only on the partition its input actually carries (<see cref="PartitionInfo"/>).
    /// </summary>
    private sealed class CoPartitionPlan
    {
        /// <summary>Per aggregate (group-key positions) or DISTINCT (column positions): the columns, in
        /// order, to hash-partition its output by.</summary>
        public Dictionary<LogicalPlan, int[]> Requests { get; } = new(ReferenceEqualityComparer.Instance);

        /// <summary>Requested operators whose input is exchanged once, for all of them.</summary>
        public HashSet<LogicalPlan> Shared { get; } = new(ReferenceEqualityComparer.Instance);

        /// <summary>Operators asked for two different keys: neither request holds.</summary>
        public HashSet<LogicalPlan> Conflicted { get; } = new(ReferenceEqualityComparer.Instance);

        public int[]? RequestFor(LogicalPlan node) =>
            !Conflicted.Contains(node) && Requests.TryGetValue(node, out var cols) ? cols : null;
    }

    /// <summary>The co-partitioning requests for <paramref name="root"/>, compiled at W&gt;1 with
    /// <paramref name="ctx"/>; null when there are none (or the mode is off).</summary>
    private static CoPartitionPlan? PlanCoPartition(LogicalPlan root, CompileContext ctx)
    {
        if (ctx.Workers <= 1 || !CoPartitionMode.Enabled)
        {
            return null;
        }

        var result = new CoPartitionPlan();
        var seen = new HashSet<LogicalPlan>(ReferenceEqualityComparer.Instance);

        // Where each requested operator's input chain bottoms out, and on which columns of it.
        var bases = new List<(LogicalPlan Op, LogicalPlan Base, int[] Cols)>();
        Walk(root);
        if (result.Requests.Count == 0)
        {
            return null;
        }

        foreach (var group in bases
                     .Where(b => !result.Conflicted.Contains(b.Op))
                     .GroupBy(b => (Base: (object)b.Base, Key: string.Join(",", b.Cols)), new BaseKeyComparer()))
        {
            if (group.Count() >= 2)
            {
                foreach (var b in group)
                {
                    result.Shared.Add(b.Op);
                }
            }
        }

        return result;

        void Walk(LogicalPlan p)
        {
            if (!seen.Add(p))
            {
                return;
            }

            if (Unwrap(p, ctx) is { } inlined)
            {
                Walk(inlined);
                return;
            }

            if (p is JoinPlan { JoinType: DbspNet.Sql.Parser.Ast.JoinType.LeftOuter } join
                && join.EquiKeys.Count > 0
                && !ShouldBroadcastRight(join.Right, ctx))
            {
                RequestForJoin(join);
            }

            ForEachChild(p, Walk);
        }

        void RequestForJoin(JoinPlan join)
        {
            var left = new List<int>();
            var right = new List<int>();
            LogicalPlan? leftOp = null;
            LogicalPlan? rightOp = null;
            foreach (var eq in join.EquiKeys)
            {
                if (TraceColumn(join.Left, eq.LeftIndex, ctx) is not { } l
                    || TraceColumn(join.Right, eq.RightIndex, ctx) is not { } r
                    || !IsPartitioner(l.Base) || !IsPartitioner(r.Base)
                    || (leftOp is not null && !ReferenceEquals(leftOp, l.Base))
                    || (rightOp is not null && !ReferenceEquals(rightOp, r.Base))
                    || OperatorKeyColumn(l.Base, l.Column) is null
                    || OperatorKeyColumn(r.Base, r.Column) is null)
                {
                    continue;
                }

                leftOp = l.Base;
                rightOp = r.Base;
                left.Add(l.Column);
                right.Add(r.Column);
            }

            if (left.Count == 0 || ReferenceEquals(leftOp, rightOp))
            {
                return;
            }

            Push(leftOp!, [.. left]);
            Push(rightOp!, [.. right]);
        }

        void Push(LogicalPlan op, int[] cols)
        {
            if (result.Requests.TryGetValue(op, out var existing))
            {
                if (!existing.SequenceEqual(cols))
                {
                    result.Conflicted.Add(op);
                }

                return;
            }

            result.Requests[op] = cols;

            // The request in the operator's input columns, then down its Filter/Project chain.
            var input = op switch
            {
                AggregatePlan a => a.Input,
                DistinctPlan d => d.Input,
                _ => throw new InvalidOperationException(op.GetType().Name),
            };
            var inputCols = new int[cols.Length];
            LogicalPlan? chainBase = null;
            for (var i = 0; i < cols.Length; i++)
            {
                if (OperatorKeyColumn(op, cols[i]) is not { } c
                    || TraceColumn(input, c, ctx) is not { } t
                    || (chainBase is not null && !ReferenceEquals(chainBase, t.Base)))
                {
                    return;
                }

                chainBase = t.Base;
                inputCols[i] = t.Column;
            }

            // Fed by another aggregate / DISTINCT: ask it too, and this one elides on what it delivers.
            if (IsPartitioner(chainBase!) && inputCols.All(c => OperatorKeyColumn(chainBase!, c) is not null))
            {
                Push(chainBase!, inputCols);
            }
            else
            {
                bases.Add((op, chainBase!, inputCols));
            }
        }
    }

    private sealed class BaseKeyComparer : IEqualityComparer<(object Base, string Key)>
    {
        public bool Equals((object Base, string Key) x, (object Base, string Key) y) =>
            ReferenceEquals(x.Base, y.Base) && x.Key == y.Key;

        public int GetHashCode((object Base, string Key) obj) =>
            HashCode.Combine(System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj.Base), obj.Key);
    }

    /// <summary>
    /// A DISTINCT a downstream join asked to partition by <paramref name="request"/> (§5.57). It elides its
    /// exchange when its input already arrives hash-partitioned (any columns: a DISTINCT's key is the whole
    /// row); otherwise it exchanges by the requested columns, its chain running in the sender (§5.56), or
    /// on one exchange of its input shared with another requested operator.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> CompileCoPartitionedDistinct(
        CircuitBuilder builder, DistinctPlan d, CompileContext ctx, int[] request)
    {
        var snapshotCodec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(d.Schema);
        var (gcFrontier, gcMonotoneKey) = ResolveDistinctFrontier(d, ctx);
        var (input, pre, _, sources) = CompileIndexInputSources(builder, d.Input, ctx, nullKeyIndices: null);
        var requestSchema = d.Schema.SubsetByIndex(request);
        if (ctx.CoPartition!.Shared.Contains(d) && ThroughSources(request, sources) is { } baseColumns)
        {
            input = SharedExchange(builder, ctx, input, baseColumns, requestSchema);
        }

        var claim = MapPartition(ctx.GetPartition(input), sources);
        Stream<ZSet<StructuralRow, Z64>> distinctInput;
        PartitionInfo output;
        if (claim.PartitionKey is { } pk && claim.KeySchema is not null && !claim.Gathered)
        {
            distinctInput = pre is null
                ? input
                : LinearOperators.MapFilterRows<StructuralRow, StructuralRow, Z64>(builder, 
                    input, row => pre(row) is { } mapped ? (true, mapped) : (false, null!));
            output = claim;
        }
        else
        {
            var partition = ColumnsPartition(request, requestSchema);
            distinctInput = pre is null
                ? builder.Exchange(input, partition)
                : builder.ExchangeStep<StructuralRow, StructuralRow, Z64>(
                    input, row => pre(row) is { } mapped ? (true, mapped) : (false, null!), partition);
            output = new PartitionInfo(ShardDisjoint: false, request, requestSchema);
        }

        AttachDeltaFormat(distinctInput, d.Schema);
        var distinct = EmitDistinct(
            builder, ctx, distinctInput, d.Schema, snapshotCodec, gcFrontier, gcMonotoneKey,
            reshard: HashPlacement(output.PartitionKey!, output.KeySchema!));
        ctx.SetPartition(distinct, output);
        return distinct;
    }

    private sealed class SharedExchangeKeyComparer
        : IEqualityComparer<(Stream<ZSet<StructuralRow, Z64>> Input, string Columns)>
    {
        public bool Equals(
            (Stream<ZSet<StructuralRow, Z64>> Input, string Columns) x,
            (Stream<ZSet<StructuralRow, Z64>> Input, string Columns) y) =>
            ReferenceEquals(x.Input, y.Input) && x.Columns == y.Columns;

        public int GetHashCode((Stream<ZSet<StructuralRow, Z64>> Input, string Columns) obj) =>
            HashCode.Combine(System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj.Input), obj.Columns);
    }

    private static bool IsPartitioner(LogicalPlan p) => p is AggregatePlan or DistinctPlan;

    /// <summary>The input column an aggregate's output column <paramref name="col"/> is, when it is a bare
    /// group-key column (a DISTINCT's columns are its input's); null otherwise.</summary>
    private static int? OperatorKeyColumn(LogicalPlan op, int col) => op switch
    {
        AggregatePlan a when col < a.GroupKeys.Count && a.GroupKeys[col] is ResolvedColumn rc => rc.Index,
        DistinctPlan d when col < d.Schema.Count => col,
        _ => null,
    };

    /// <summary>
    /// Follows output column <paramref name="col"/> of <paramref name="plan"/> down through filters,
    /// projections that pass it as a bare column, and inlined references, to the first other node: that
    /// node and the column there. Null when a projection computes it.
    /// </summary>
    private static (LogicalPlan Base, int Column)? TraceColumn(LogicalPlan plan, int col, CompileContext ctx)
    {
        var node = plan;
        while (true)
        {
            if (Unwrap(node, ctx) is { } inlined)
            {
                node = inlined;
            }
            else if (node is FilterPlan f)
            {
                node = f.Input;
            }
            else if (node is ProjectPlan p)
            {
                if (p.Projections[col].Expression is not ResolvedColumn rc)
                {
                    return null;
                }

                col = rc.Index;
                node = p.Input;
            }
            else
            {
                return (node, col);
            }
        }
    }

    /// <summary>For each column of a fused chain's output, the column of the chain's input it passes
    /// through bare (-1 when computed); null for a chain that projects nothing.</summary>
    private static int[]? ComposeSources(IEnumerable<LinearStage> stages)
    {
        int[]? map = null;
        foreach (var stage in stages)
        {
            if (stage.Kind != LinearStageKind.Map)
            {
                continue;
            }

            var next = new int[stage.Sources!.Length];
            for (var j = 0; j < next.Length; j++)
            {
                var s = stage.Sources[j];
                next[j] = s < 0 ? -1 : map is null ? s : map[s];
            }

            map = next;
        }

        return map;
    }

    /// <summary>
    /// The partition a chain's output carries, given its input's and the chain's column
    /// <paramref name="sources"/>: each partition column moved to where the chain passes it through bare.
    /// Unpartitioned when the chain computes or drops one of them.
    /// </summary>
    private static PartitionInfo MapPartition(PartitionInfo input, int[]? sources)
    {
        if (sources is null || input.PartitionKey is not { } pk)
        {
            return input;
        }

        var moved = new int[pk.Length];
        for (var i = 0; i < pk.Length; i++)
        {
            moved[i] = Array.IndexOf(sources, pk[i]);
            if (moved[i] < 0)
            {
                return default;
            }
        }

        return new PartitionInfo(ShardDisjoint: false, moved, input.KeySchema, input.Gathered);
    }

    /// <summary>
    /// Whether a join's two inputs arrive hash-partitioned alike on some of its equi-key pairs, so equal
    /// keys already share a worker: the same number of columns, each left column paired with its right
    /// column by an equi-key, hashed as the same type. <paramref name="positions"/> are those pairs'
    /// positions in the join's key rows (for the traces' re-shard placement).
    /// </summary>
    private static bool CoPartitioned(
        PartitionInfo left, PartitionInfo right, int[] leftIndices, int[] rightIndices, out int[] positions)
    {
        positions = [];
        if (left.PartitionKey is not { Length: > 0 } lk
            || right.PartitionKey is not { } rk
            || lk.Length != rk.Length
            || left.KeySchema is not { } ls
            || right.KeySchema is not { } rs
            || left.Gathered
            || right.Gathered)
        {
            return false;
        }

        var found = new int[lk.Length];
        for (var i = 0; i < lk.Length; i++)
        {
            found[i] = -1;
            for (var j = 0; j < leftIndices.Length; j++)
            {
                if (leftIndices[j] == lk[i] && rightIndices[j] == rk[i])
                {
                    found[i] = j;
                    break;
                }
            }

            if (found[i] < 0 || !SameHash(ls[i].Type, rs[i].Type))
            {
                return false;
            }
        }

        positions = found;
        return true;
    }

    /// <summary>Whether equal values of the two types hash alike (<see cref="StablePartitionHash.OfBoxed"/>
    /// dispatches on the type's kind; a decimal's digest also depends on its scale).</summary>
    private static bool SameHash(SqlType a, SqlType b) =>
        a.GetType() == b.GetType()
        && (a is not SqlDecimalType da || (b is SqlDecimalType db && da.Scale == db.Scale && da.Precision == db.Precision));

    /// <summary>A hash placement over the given columns of a persisted entry (a row, or a key row).</summary>
    private static ReshardPlacement HashPlacement(int[] columns, Schema keySchema) =>
        new ReshardPlacement.Hash(entry => StablePartitionHash.OfRow(entry, columns, keySchema));

    /// <summary>A partition function over the given columns of a row. Its own method so the delegate
    /// captures only its arguments (never a compiler context).</summary>
    private static Func<StructuralRow, int> ColumnsPartition(int[] columns, Schema keySchema) =>
        row => StablePartitionHash.OfRow(row, columns, keySchema);

    /// <summary>
    /// <paramref name="input"/> exchanged by <paramref name="columns"/>, once per stream and key for every
    /// requested operator that shares it (§5.57).
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> SharedExchange(
        CircuitBuilder builder, CompileContext ctx, Stream<ZSet<StructuralRow, Z64>> input, int[] columns, Schema keySchema)
    {
        var key = (input, string.Join(",", columns));
        if (!ctx.SharedExchanges.TryGetValue(key, out var exchanged))
        {
            exchanged = builder.Exchange(input, ColumnsPartition(columns, keySchema));
            ctx.SetPartition(exchanged, new PartitionInfo(ShardDisjoint: false, columns, keySchema));
            ctx.SharedExchanges[key] = exchanged;
        }

        return exchanged;
    }

    /// <summary>Output columns <paramref name="cols"/> of a fused chain, in the chain's input; null when the
    /// chain computes one of them.</summary>
    private static int[]? ThroughSources(int[] cols, int[]? sources)
    {
        if (sources is null)
        {
            return cols;
        }

        var mapped = new int[cols.Length];
        for (var i = 0; i < cols.Length; i++)
        {
            mapped[i] = sources[cols[i]];
            if (mapped[i] < 0)
            {
                return null;
            }
        }

        return mapped;
    }
}
