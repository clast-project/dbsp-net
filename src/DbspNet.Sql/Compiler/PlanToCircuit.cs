// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Walks a <see cref="LogicalPlan"/> and emits an equivalent
/// <see cref="RootCircuit"/>. Every plan node maps to a small handful of
/// <c>DbspNet.Core</c> operators; scalar expressions compile to LINQ-expression
/// delegates via <see cref="ExpressionCompiler"/>.
/// </summary>
/// <remarks>
/// Runtime representation at every stream: <c>ZSet&lt;StructuralRow, Z64&gt;</c>.
/// Rows are positional; NULL is <c>null</c> in the row slot. Equi-join
/// semantics drop rows whose join-key contains any NULL.
/// </remarks>
public static class PlanToCircuit
{
    public static CompiledQuery Compile(
        LogicalPlan plan,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        return CompileCore(plan, StructuralRowCodec.Instance, snapshotCodecs, options ?? CompileOptions.Default);
    }

    /// <summary>
    /// Opt-in compile path that uses a non-default <see cref="IRowCodec{TRow}"/>
    /// for every stage's output row construction. Used by benchmarks /
    /// experiments to evaluate alternative row representations against the
    /// baseline.
    /// </summary>
    public static CompiledQuery Compile(LogicalPlan plan, IRowCodec<StructuralRow> codec)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(codec);
        return CompileCore(plan, codec, snapshotCodecs: null, CompileOptions.Default);
    }

    public static CompiledQuery Compile(CreateViewPlan view)
    {
        ArgumentNullException.ThrowIfNull(view);
        return CompileCore(view.Query, StructuralRowCodec.Instance, snapshotCodecs: null, CompileOptions.Default);
    }

    private static CompiledQuery CompileCore(
        LogicalPlan plan,
        IRowCodec<StructuralRow> codec,
        ISqlSnapshotCodecs? snapshotCodecs,
        CompileOptions options)
    {
        // Walk the plan to find every scanned table; each becomes a circuit input.
        var tables = CollectScans(plan);
        var hasLateness = tables.Values.Any(t => t.Lateness is { Count: > 0 });
        var monotonicity = MonotonicityAnalyzer.Analyze(plan);

        RootCircuit? circuit = null;
        Dictionary<string, TableInput>? inputs = null;
        OutputHandle<ZSet<StructuralRow, Z64>>? output = null;

        circuit = RootCircuit.Build(builder =>
        {
            var streams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
            inputs = new Dictionary<string, TableInput>(StringComparer.Ordinal);
            var frontiers = new Dictionary<LatenessSource, MutableFrontier>();
            foreach (var (name, info) in tables)
            {
                var (handle, stream) = builder.ZSetInput<StructuralRow, Z64>();
                inputs[name] = new TableInput(handle, info.Schema, codec);

                // For each declared-LATENESS column, interpose an EnforceLateness
                // operator that drops late rows and advances a frontier the
                // downstream stateful operators GC against.
                if (info.Lateness is { } lateness)
                {
                    foreach (var (column, bound) in lateness)
                    {
                        var index = column;
                        var frontier = new MutableFrontier();
                        stream = builder.EnforceLateness(
                            stream, row => MonotoneKey.Extract(row[index]), bound, frontier);
                        frontiers[new LatenessSource(name, column)] = frontier;
                    }
                }

                streams[name] = stream;
            }

            // Fast path: try the typed-row pipeline first. When the
            // plan and every subexpression are within scope, the
            // internal operator stages run typed; only the input /
            // output boundaries pay a structural↔typed conversion.
            // Snapshot codecs flow through: typed stateful operators
            // wrap the SQL codec in a typed adapter that round-trips
            // typed↔structural at save/load time, so the on-disk
            // format stays compatible with the structural pipeline.
            //
            // Disabled when a non-default IRowCodec<StructuralRow> is
            // supplied — the structural compile is the only path that
            // honours an alternative codec on every stage's output row.
            // Also disabled in spine mode (the typed compiler emits the flat
            // operator family) and when LATENESS is present (GC is wired only on
            // the structural path — long-running bounded pipelines use spine +
            // LATENESS structurally; the typed fast path is for short queries).
            Stream<ZSet<StructuralRow, Z64>>? queryStream = null;
            if (options.TraceFamily == TraceFamily.Flat
                && ReferenceEquals(codec, StructuralRowCodec.Instance)
                && !hasLateness)
            {
                queryStream = TypedPlanCompiler.TryCompileWithStructuralBoundary(
                    builder, plan, streams, codec, snapshotCodecs);
            }

            if (queryStream is null)
            {
                var tableSchemas = tables.ToDictionary(kv => kv.Key, kv => kv.Value.Schema, StringComparer.Ordinal);
                var ctx = new CompileContext(
                    streams, tableSchemas, codec, snapshotCodecs, options, monotonicity, frontiers);
                queryStream = CompilePlan(builder, plan, ctx);
            }

            output = builder.Output(queryStream);
        });

        return new CompiledQuery(circuit!, inputs!, output!, plan.Schema);
    }

    private sealed class CompileContext
    {
        public CompileContext(
            IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> scans,
            IReadOnlyDictionary<string, Schema> tableSchemas,
            IRowCodec<StructuralRow> codec,
            ISqlSnapshotCodecs? snapshotCodecs,
            CompileOptions options,
            MonotonicityInfo monotonicity,
            IReadOnlyDictionary<LatenessSource, MutableFrontier> frontiers)
        {
            Scans = scans;
            TableSchemas = tableSchemas;
            Codec = codec;
            SnapshotCodecs = snapshotCodecs;
            Options = options;
            Monotonicity = monotonicity;
            Frontiers = frontiers;
        }

        /// <summary>Stream per declared table — the circuit's inputs.</summary>
        public IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> Scans { get; }

        /// <summary>
        /// Schema per declared base table — used by stateful operators
        /// (currently <see cref="RecursiveCteOp"/>) that need to construct
        /// per-table snapshot codecs at compile time.
        /// </summary>
        public IReadOnlyDictionary<string, Schema> TableSchemas { get; }

        /// <summary>
        /// Per-CTE compiled stream cache. The first <see cref="CteScanPlan"/>
        /// encountered for a given <see cref="CteRef"/> compiles its
        /// underlying subplan; every subsequent scan returns the cached stream.
        /// </summary>
        public Dictionary<CteRef, Stream<ZSet<StructuralRow, Z64>>> CteCache { get; } = new();

        /// <summary>
        /// Row codec used everywhere internally to build the output row of a
        /// pipeline stage.
        /// </summary>
        public IRowCodec<StructuralRow> Codec { get; }

        /// <summary>
        /// Optional snapshot codec factory. When non-null, stateful operators
        /// register a codec at construction so the circuit can be snapshotted
        /// via <c>DbspNet.Persistence.Snapshot</c>.
        /// </summary>
        public ISqlSnapshotCodecs? SnapshotCodecs { get; }

        /// <summary>Compile-time knobs — currently the flat/spine trace toggle.</summary>
        public CompileOptions Options { get; }

        /// <summary>Per-node, per-column monotonicity verdicts for LATENESS GC.</summary>
        public MonotonicityInfo Monotonicity { get; }

        /// <summary>One frontier per declared LATENESS source, advanced by its input operator.</summary>
        public IReadOnlyDictionary<LatenessSource, MutableFrontier> Frontiers { get; }
    }

    // ---- Stateful operator emission: flat vs spine trace family ----
    //
    // Every SQL stateful operator keys / values on StructuralRow with a Z64
    // weight. The spine trace requires a total order over keys and values,
    // supplied by StructuralRowComparer; the flat trace ignores ordering.
    // These helpers are the single place the trace family is selected.

    private static Stream<ZSet<StructuralRow, Z64>> EmitDistinct(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<ZSet<StructuralRow, Z64>> input,
        IZSetTraceCodec<StructuralRow, Z64>? snapshotCodec)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineDistinct(input, ctx.Options.Compaction, snapshotCodec, StructuralRowComparer.Instance)
            : builder.Distinct(input, snapshotCodec);
    }

    private static Stream<ZSet<StructuralRow, Z64>> EmitInnerJoin(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> left,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> right,
        Func<StructuralRow, StructuralRow, StructuralRow, StructuralRow> combine,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? leftCodec,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? rightCodec,
        IFrontier? frontier = null,
        Func<StructuralRow, long>? monotoneKey = null)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineIncrementalInnerJoin(
                left, right, combine, leftCodec, rightCodec,
                ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                leftValueComparer: StructuralRowComparer.Instance,
                rightValueComparer: StructuralRowComparer.Instance,
                frontier: frontier,
                monotoneKey: monotoneKey)
            : builder.IncrementalInnerJoin(left, right, combine, leftCodec, rightCodec, frontier, monotoneKey);
    }

    private static Stream<ZSet<StructuralRow, Z64>> EmitLeftJoin(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> left,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> right,
        Func<StructuralRow, StructuralRow, StructuralRow, StructuralRow> joinCombine,
        Func<StructuralRow, StructuralRow, StructuralRow> nullPadCombine,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? leftCodec,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? rightCodec,
        IFrontier? frontier = null,
        Func<StructuralRow, long>? monotoneKey = null)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineIncrementalLeftJoin(
                left, right, joinCombine, nullPadCombine, leftCodec, rightCodec,
                ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                leftValueComparer: StructuralRowComparer.Instance,
                rightValueComparer: StructuralRowComparer.Instance,
                frontier: frontier,
                monotoneKey: monotoneKey)
            : builder.IncrementalLeftJoin(
                left, right, joinCombine, nullPadCombine, leftCodec, rightCodec, frontier, monotoneKey);
    }

    private static Stream<ZSet<(StructuralRow Key, StructuralRow Value), Z64>> EmitAggregate(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> input,
        IAggregator<StructuralRow, StructuralRow> aggregator,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? snapshotCodec,
        IFrontier? frontier = null,
        Func<StructuralRow, long>? monotoneKey = null)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineIncrementalAggregate(
                input, aggregator, snapshotCodec,
                ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                valueComparer: StructuralRowComparer.Instance,
                frontier: frontier,
                monotoneKey: monotoneKey)
            : builder.IncrementalAggregate(input, aggregator, snapshotCodec, frontier, monotoneKey);
    }

    /// <summary>
    /// If an aggregate's group key has a monotone column (per the monotonicity
    /// analyzer), returns the frontier to GC against and an extractor for the
    /// monotone value from the group-key row. The frontier is the min across all
    /// LATENESS sources bounding that column; GC only fires when every source
    /// has a frontier (a partial set would be unsound).
    /// </summary>
    private static (IFrontier? Frontier, Func<StructuralRow, long>? MonotoneKey) ResolveGroupKeyFrontier(
        AggregatePlan plan, CompileContext ctx)
    {
        for (var g = 0; g < plan.GroupKeys.Count; g++)
        {
            var sources = ctx.Monotonicity.Sources(plan, g);
            if (sources is not { Count: > 0 })
            {
                continue;
            }

            var frontiers = new List<IFrontier>(sources.Count);
            foreach (var source in sources)
            {
                if (ctx.Frontiers.TryGetValue(source, out var f))
                {
                    frontiers.Add(f);
                }
            }

            if (frontiers.Count != sources.Count)
            {
                continue;
            }

            var keyIndex = g;
            IFrontier frontier = frontiers.Count == 1 ? frontiers[0] : new MinFrontier(frontiers);
            return (frontier, keyRow => MonotoneKey.Extract(keyRow[keyIndex]));
        }

        return (null, null);
    }

    /// <summary>
    /// If an equi-key is monotone on <em>both</em> input sides — the condition
    /// for a join to safely GC (no future delta arrives below the frontier on
    /// either input) — returns the min frontier over its sources and an
    /// extractor for the monotone value from the join-key row (which stores the
    /// equi-key columns in EquiKeys order). Stricter than the analyzer's output
    /// marking, which for outer joins flags only the preserved side.
    /// </summary>
    private static (IFrontier? Frontier, Func<StructuralRow, long>? MonotoneKey) ResolveJoinKeyFrontier(
        JoinPlan plan, CompileContext ctx)
    {
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            var eq = plan.EquiKeys[i];
            var leftSources = ctx.Monotonicity.Sources(plan.Left, eq.LeftIndex);
            var rightSources = ctx.Monotonicity.Sources(plan.Right, eq.RightIndex);
            if (leftSources is not { Count: > 0 } || rightSources is not { Count: > 0 })
            {
                continue;
            }

            var allSources = leftSources.Concat(rightSources).Distinct().ToList();
            var frontiers = new List<IFrontier>(allSources.Count);
            foreach (var source in allSources)
            {
                if (ctx.Frontiers.TryGetValue(source, out var f))
                {
                    frontiers.Add(f);
                }
            }

            if (frontiers.Count != allSources.Count)
            {
                continue;
            }

            var keyIndex = i;
            IFrontier frontier = frontiers.Count == 1 ? frontiers[0] : new MinFrontier(frontiers);
            return (frontier, keyRow => MonotoneKey.Extract(keyRow[keyIndex]));
        }

        return (null, null);
    }

    // ---- Scan collection ----

    /// <summary>A scanned base table's declared schema and per-column LATENESS bounds.</summary>
    private sealed record ScanInfo(Schema Schema, IReadOnlyDictionary<int, long>? Lateness);

    private static Dictionary<string, ScanInfo> CollectScans(LogicalPlan plan)
    {
        var result = new Dictionary<string, ScanInfo>(StringComparer.Ordinal);
        var visitedCtes = new HashSet<CteRef>();
        Walk(plan);
        return result;

        void Walk(LogicalPlan p)
        {
            switch (p)
            {
                case ScanPlan s:
                    if (!result.ContainsKey(s.TableName))
                    {
                        // Register the table's *declared* schema (sans alias qualifier)
                        // plus any LATENESS bounds. We rebuild rows without the
                        // qualifier in TableInput.
                        result[s.TableName] = new ScanInfo(s.Schema, s.ColumnLateness);
                    }

                    break;
                case CteScanPlan c:
                    // Recurse into the CTE's underlying plan so any base
                    // tables it scans are registered as circuit inputs. Dedup
                    // via identity so a CTE referenced twice walks once.
                    if (visitedCtes.Add(c.Cte))
                    {
                        Walk(c.Cte.Plan);
                    }

                    break;
                case FilterPlan f:
                    Walk(f.Input);
                    break;
                case ProjectPlan pr:
                    Walk(pr.Input);
                    break;
                case JoinPlan j:
                    Walk(j.Left);
                    Walk(j.Right);
                    break;
                case AggregatePlan a:
                    Walk(a.Input);
                    break;
                case ScalarSubqueryJoinPlan s:
                    Walk(s.Input);
                    foreach (var sub in s.Subqueries)
                    {
                        Walk(sub);
                    }

                    break;
                case UnionAllPlan u:
                    foreach (var branch in u.Branches)
                    {
                        Walk(branch);
                    }

                    break;
                case DistinctPlan d:
                    Walk(d.Input);
                    break;
                case DifferencePlan diff:
                    Walk(diff.Left);
                    Walk(diff.Right);
                    break;
                case RecursiveCtePlan r:
                    // Walk both the base and recursive subplans to register
                    // every base-table scan they depend on as a circuit input.
                    // Mark the self-ref as visited so we don't re-enter the
                    // recursive plan through the self-ref CteScanPlan's back-edge.
                    visitedCtes.Add(r.SelfRef);
                    Walk(r.BasePlan);
                    Walk(r.RecursivePlan);
                    break;
                default:
                    throw new InvalidOperationException($"unsupported plan node {p.GetType().Name}");
            }
        }
    }

    // ---- Plan → stream ----

    private static Stream<ZSet<StructuralRow, Z64>> CompilePlan(
        CircuitBuilder builder,
        LogicalPlan plan,
        CompileContext ctx)
    {
        switch (plan)
        {
            case ScanPlan s:
                return ctx.Scans[s.TableName];

            case CteScanPlan c:
                // First reference compiles the CTE's underlying plan and
                // caches the stream by identity; subsequent references share
                // that stream. Downstream schema (column qualifiers) may
                // differ per scan but row content is the same.
                if (!ctx.CteCache.TryGetValue(c.Cte, out var cached))
                {
                    cached = CompilePlan(builder, c.Cte.Plan, ctx);
                    ctx.CteCache[c.Cte] = cached;
                }

                return cached;

            case FilterPlan f:
                {
                    var input = CompilePlan(builder, f.Input, ctx);
                    var predicate = ExpressionCompiler.CompilePredicate(f.Predicate);
                    return builder.Filter(input, row => predicate(row));
                }

            case ProjectPlan p:
                return CompileProjection(builder, p, ctx);

            case JoinPlan j:
                return CompileJoin(builder, j, ctx);

            case AggregatePlan a:
                return CompileAggregate(builder, a, ctx);

            case ScalarSubqueryJoinPlan s:
                return CompileScalarSubqueryJoin(builder, s, ctx);

            case UnionAllPlan u:
                {
                    // UNION ALL is Z-set addition: successive builder.Union
                    // of pairwise-aligned branches.
                    var result = CompilePlan(builder, u.Branches[0], ctx);
                    for (var i = 1; i < u.Branches.Count; i++)
                    {
                        var next = CompilePlan(builder, u.Branches[i], ctx);
                        result = builder.Union(result, next);
                    }

                    return result;
                }

            case DistinctPlan d:
                {
                    var distinctCodec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(d.Schema);
                    return EmitDistinct(
                        builder, ctx, CompilePlan(builder, d.Input, ctx), distinctCodec);
                }

            case DifferencePlan diff:
                {
                    var l = CompilePlan(builder, diff.Left, ctx);
                    var r = CompilePlan(builder, diff.Right, ctx);
                    return builder.Difference(l, r);
                }

            case RecursiveCtePlan rcp:
                return CompileRecursiveCte(builder, rcp, ctx);

            default:
                throw new InvalidOperationException($"unsupported plan node {plan.GetType().Name}");
        }
    }

    // ---- Projection ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileProjection(
        CircuitBuilder builder,
        ProjectPlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        // Fast path: pure identity projection (same-arity sequential column refs, no expressions).
        // Skip the MapRows — schema is purely a rename, runtime rows are unchanged.
        var identity = plan.Projections.Count == plan.Input.Schema.Count;
        if (identity)
        {
            for (var i = 0; i < plan.Projections.Count; i++)
            {
                if (plan.Projections[i].Expression is not ResolvedColumn rc || rc.Index != i)
                {
                    identity = false;
                    break;
                }
            }
        }

        if (identity)
        {
            return input;
        }

        var delegates = new Func<IReadOnlyList<object?>, object?>[plan.Projections.Count];
        for (var i = 0; i < plan.Projections.Count; i++)
        {
            delegates[i] = ExpressionCompiler.CompileScalar(plan.Projections[i].Expression);
        }

        var codec = ctx.Codec;
        var outSchema = plan.Schema;
        return builder.MapRows(input, row =>
        {
            var values = new object?[delegates.Length];
            for (var i = 0; i < delegates.Length; i++)
            {
                values[i] = delegates[i](row);
            }

            return codec.BuildRow(outSchema, values);
        });
    }

    // ---- Join ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        return plan.JoinType switch
        {
            DbspNet.Sql.Parser.Ast.JoinType.Inner => CompileInnerJoin(builder, plan, ctx),
            DbspNet.Sql.Parser.Ast.JoinType.LeftOuter => CompileLeftOuterJoin(builder, plan, ctx),
            DbspNet.Sql.Parser.Ast.JoinType.RightOuter => CompileRightOuterJoin(builder, plan, ctx),
            _ => throw new InvalidOperationException($"unsupported JoinType {plan.JoinType}"),
        };
    }

    private static Stream<ZSet<StructuralRow, Z64>> CompileInnerJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        var left = CompilePlan(builder, plan.Left, ctx);
        var right = CompilePlan(builder, plan.Right, ctx);

        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);

        // INNER: drop NULL-keyed rows on both sides — the equi-predicate is
        // never true when either key column is NULL. For set-op-synthesised
        // joins (INTERSECT/EXCEPT) this is overridden: NULLs are equal to
        // each other for deduplication-style matching.
        var leftFiltered = plan.AllowNullKeys
            ? left
            : builder.Filter(left, row => HasNoNullKey(row, leftIndices));
        var rightFiltered = plan.AllowNullKeys
            ? right
            : builder.Filter(right, row => HasNoNullKey(row, rightIndices));

        var leftKeySchema = plan.Left.Schema.SubsetByIndex(leftIndices);
        var rightKeySchema = plan.Right.Schema.SubsetByIndex(rightIndices);
        var joinedSchema = plan.Schema;
        var codec = ctx.Codec;
        var leftIndexed = builder.GroupProject(
            leftFiltered,
            row => ExtractKey(codec, leftKeySchema, row, leftIndices),
            row => row);
        var rightIndexed = builder.GroupProject(
            rightFiltered,
            row => ExtractKey(codec, rightKeySchema, row, rightIndices),
            row => row);

        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;
        var leftCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(leftKeySchema, plan.Left.Schema);
        var rightCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, plan.Right.Schema);
        var (gcFrontier, gcMonotoneKey) = ResolveJoinKeyFrontier(plan, ctx);
        var joined = EmitInnerJoin(
            builder, ctx,
            leftIndexed,
            rightIndexed,
            (_, lrow, rrow) => MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount),
            leftCodec, rightCodec, gcFrontier, gcMonotoneKey);

        if (plan.Residual is { } residual)
        {
            var residualPredicate = ExpressionCompiler.CompilePredicate(residual);
            joined = builder.Filter(joined, row => residualPredicate(row));
        }

        return joined;
    }

    private static Stream<ZSet<StructuralRow, Z64>> CompileLeftOuterJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        // Residual predicates on LEFT JOIN are rejected by the resolver — a
        // failing residual would drop the match but retain the left row
        // NULL-padded, and that's not encoded in the operator's semantics.
        if (plan.Residual is not null)
        {
            throw new InvalidOperationException(
                "internal: LEFT JOIN with residual reached PlanToCircuit; resolver should have rejected");
        }

        var left = CompilePlan(builder, plan.Left, ctx);
        var right = CompilePlan(builder, plan.Right, ctx);

        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

        // NULL-keyed left rows can never find a match (NULL = anything is
        // NULL, not TRUE), so they bypass the join and go straight to the
        // NULL-padded branch. NULL-keyed right rows are simply dropped.
        var nullKeyLeft = builder.Filter(left, row => !HasNoNullKey(row, leftIndices));
        var validKeyLeft = builder.Filter(left, row => HasNoNullKey(row, leftIndices));
        var validKeyRight = builder.Filter(right, row => HasNoNullKey(row, rightIndices));

        var leftKeySchema = plan.Left.Schema.SubsetByIndex(leftIndices);
        var rightKeySchema = plan.Right.Schema.SubsetByIndex(rightIndices);
        var joinedSchema = plan.Schema;
        var codec = ctx.Codec;
        var leftIndexed = builder.GroupProject(
            validKeyLeft,
            row => ExtractKey(codec, leftKeySchema, row, leftIndices),
            row => row);
        var rightIndexed = builder.GroupProject(
            validKeyRight,
            row => ExtractKey(codec, rightKeySchema, row, rightIndices),
            row => row);

        var leftCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(leftKeySchema, plan.Left.Schema);
        var rightCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, plan.Right.Schema);
        // GC license requires the equi-key be monotone on BOTH sides (not just
        // the preserved left side): a future row on the non-monotone right side
        // could still flip a left key from unmatched to matched below the frontier.
        var (gcFrontier, gcMonotoneKey) = ResolveJoinKeyFrontier(plan, ctx);
        var joined = EmitLeftJoin(
            builder, ctx,
            leftIndexed,
            rightIndexed,
            joinCombine: (_, lrow, rrow) => MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount),
            nullPadCombine: (_, lrow) => NullPadRight(codec, joinedSchema, lrow, leftCount, rightCount),
            leftCodec, rightCodec, gcFrontier, gcMonotoneKey);

        // NULL-keyed left rows contribute directly to the output, each as
        // a NULL-padded row (never matched).
        var nullKeyPadded = builder.MapRows(
            nullKeyLeft,
            row => NullPadRight(codec, joinedSchema, row, leftCount, rightCount));

        return builder.Union(joined, nullKeyPadded);
    }

    private static Stream<ZSet<StructuralRow, Z64>> CompileRightOuterJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        // Implemented as LEFT JOIN with physical sides swapped: the right
        // stream becomes the "preserved" side fed to IncrementalLeftJoin.
        // Output rows are re-assembled in the user's written column order
        // (left columns first, then right) — for unmatched right rows the
        // left columns are NULL-padded.
        if (plan.Residual is not null)
        {
            throw new InvalidOperationException(
                "internal: RIGHT JOIN with residual reached PlanToCircuit; resolver should have rejected");
        }

        var left = CompilePlan(builder, plan.Left, ctx);
        var right = CompilePlan(builder, plan.Right, ctx);

        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

        // NULL-keyed right rows bypass the join and emit as unmatched.
        // NULL-keyed left rows can never match and are dropped.
        var nullKeyRight = builder.Filter(right, row => !HasNoNullKey(row, rightIndices));
        var validKeyLeft = builder.Filter(left, row => HasNoNullKey(row, leftIndices));
        var validKeyRight = builder.Filter(right, row => HasNoNullKey(row, rightIndices));

        var leftKeySchema = plan.Left.Schema.SubsetByIndex(leftIndices);
        var rightKeySchema = plan.Right.Schema.SubsetByIndex(rightIndices);
        var joinedSchema = plan.Schema;
        var codec = ctx.Codec;
        var leftIndexed = builder.GroupProject(
            validKeyLeft,
            row => ExtractKey(codec, leftKeySchema, row, leftIndices),
            row => row);
        var rightIndexed = builder.GroupProject(
            validKeyRight,
            row => ExtractKey(codec, rightKeySchema, row, rightIndices),
            row => row);

        // IncrementalLeftJoin treats its first arg as the preserved side.
        // Here: preserved = right; probed = left. In the combiners the
        // preserved-side row is the `b`/right row, probed is `a`/left.
        // Codecs follow the same swap: the operator's "left trace" carries
        // right rows, "right trace" carries left rows.
        var preservedCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, plan.Right.Schema);
        var probedCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(leftKeySchema, plan.Left.Schema);
        // Both-sides-monotone GC license, as for LEFT JOIN. ResolveJoinKeyFrontier
        // keys off the EquiKeys position, which the join-key row carries on both
        // physical sides — so the same extractor is correct despite the swap.
        var (gcFrontier, gcMonotoneKey) = ResolveJoinKeyFrontier(plan, ctx);
        var joined = EmitLeftJoin(
            builder, ctx,
            rightIndexed,
            leftIndexed,
            joinCombine: (_, rrow, lrow) => MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount),
            nullPadCombine: (_, rrow) => NullPadLeft(codec, joinedSchema, rrow, leftCount, rightCount),
            preservedCodec, probedCodec, gcFrontier, gcMonotoneKey);

        var nullKeyPadded = builder.MapRows(
            nullKeyRight,
            row => NullPadLeft(codec, joinedSchema, row, leftCount, rightCount));

        return builder.Union(joined, nullKeyPadded);
    }

    private static StructuralRow NullPadLeft(IRowCodec<StructuralRow> codec, Schema schema, StructuralRow right, int leftCount, int rightCount)
    {
        var vs = new object?[leftCount + rightCount];
        // Left-side columns default to null; fill in the right side.
        for (var i = 0; i < rightCount; i++)
        {
            vs[leftCount + i] = right[i];
        }

        return codec.BuildRow(schema, vs);
    }    // ---- Recursive CTE ----
    //
    // The recursive CTE compiles to a single custom operator (RecursiveCteOp)
    // that holds the base and step subplans as LogicalPlan and evaluates
    // them at runtime via BatchPlanEvaluator. The operator receives the
    // delta streams of every base table its body references and maintains an
    // integrated trace per table so it can run a from-scratch batch
    // fixed-point per outer tick.
    //
    // Restrictions (v1):
    //   - body may reference only base tables (ScanPlan) and the self-ref.
    //     Other CTE references are rejected here.
    //   - body may not contain aggregates, subqueries, outer joins, or
    //     nested recursive CTEs. The batch evaluator enforces this on the
    //     fly with clearer errors.

    private static Stream<ZSet<StructuralRow, Z64>> CompileRecursiveCte(
        CircuitBuilder builder,
        RecursiveCtePlan plan,
        CompileContext ctx)
    {
        // Collect external base-table names referenced by either subplan.
        var externalNames = new HashSet<string>(StringComparer.Ordinal);
        CollectRecursiveExternalTables(plan.BasePlan, plan.SelfRef, externalNames);
        CollectRecursiveExternalTables(plan.RecursivePlan, plan.SelfRef, externalNames);

        // Wire the existing input streams for each referenced table. The
        // operator integrates their deltas internally into ZSetTraces.
        var externalStreams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
        foreach (var name in externalNames)
        {
            externalStreams[name] = ctx.Scans[name];
        }

        // Build snapshot codecs: one ZSet trace codec per external base
        // table (using that table's row schema), plus one for the CTE
        // result (used for both _r and _previousResult — they share a
        // schema). When SnapshotCodecs is null, the operator gets null
        // codecs and Snapshot.Write throws NotSupportedException at the
        // operator boundary.
        Dictionary<string, IZSetTraceCodec<StructuralRow, Z64>>? externalCodecs = null;
        IZSetTraceCodec<StructuralRow, Z64>? resultCodec = null;
        if (ctx.SnapshotCodecs is { } codecs)
        {
            externalCodecs = new Dictionary<string, IZSetTraceCodec<StructuralRow, Z64>>(StringComparer.Ordinal);
            foreach (var name in externalNames)
            {
                externalCodecs[name] = codecs.CreateZSetTraceCodec(ctx.TableSchemas[name]);
            }

            resultCodec = codecs.CreateZSetTraceCodec(plan.Schema);
        }

        var output = new Stream<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty);
        var op = new RecursiveCteOp(
            externalStreams,
            output,
            plan.BasePlan,
            plan.RecursivePlan,
            plan.SelfRef,
            externalCodecs,
            resultCodec);
        builder.AddRawOperator(op);
        return output;
    }

    private static void CollectRecursiveExternalTables(
        LogicalPlan plan,
        CteRef selfRef,
        HashSet<string> result)
    {
        switch (plan)
        {
            case ScanPlan s:
                result.Add(s.TableName);
                break;
            case CteScanPlan c:
                if (ReferenceEquals(c.Cte, selfRef))
                {
                    return; // the self-reference — not an external input
                }

                throw new InvalidOperationException(
                    $"recursive CTE body cannot reference other CTE '{c.Cte.Name}' in v1");
            case FilterPlan f:
                CollectRecursiveExternalTables(f.Input, selfRef, result);
                break;
            case ProjectPlan p:
                CollectRecursiveExternalTables(p.Input, selfRef, result);
                break;
            case JoinPlan j:
                CollectRecursiveExternalTables(j.Left, selfRef, result);
                CollectRecursiveExternalTables(j.Right, selfRef, result);
                break;
            case UnionAllPlan u:
                foreach (var b in u.Branches)
                {
                    CollectRecursiveExternalTables(b, selfRef, result);
                }

                break;
            default:
                throw new InvalidOperationException(
                    $"{plan.GetType().Name} is not supported inside a recursive CTE body");
        }
    }

    // ---- Scalar subquery cross-product ----
    //
    // Each scalar subquery contributes one hidden column, appended to the
    // outer plan via an IncrementalLeftJoin on a constant unit key. An
    // empty subquery yields NULL for every outer row; a 1-row subquery
    // broadcasts its single value. More than one row in the subquery at
    // any tick is undefined (v1 does not validate).
    //
    // Runtime shape:  outer ⟕_unit (MapRows(subq, row => (row[0])))
    // The left-join's nullPadCombine handles empty-subquery → NULL; when
    // the subquery's value changes tick-over-tick, the left-join correctly
    // retracts outer×oldScalar and emits outer×newScalar.
    /// <summary>
    /// Singleton zero-column row used as the unit join key in scalar subquery
    /// fan-out. Two empty <see cref="StructuralRow"/>s are equal by value, so
    /// any allocation works — the singleton just avoids per-row allocation.
    /// </summary>
    private static readonly StructuralRow s_unitKey = new(Array.Empty<object?>());

    private static Stream<ZSet<StructuralRow, Z64>> CompileScalarSubqueryJoin(
        CircuitBuilder builder,
        ScalarSubqueryJoinPlan plan,
        CompileContext ctx)
    {
        var current = CompilePlan(builder, plan.Input, ctx);
        var currentSchema = plan.Input.Schema;
        foreach (var subPlan in plan.Subqueries)
        {
            var subStream = CompilePlan(builder, subPlan, ctx);
            // Project subquery rows to their single column wrapped in a 1-col StructuralRow.
            // (The subquery plan's output schema already has Count=1, so row[0] is the scalar.)
            current = AttachScalarColumn(
                builder, ctx, current, subStream, currentSchema, subPlan.Schema);
            // Output of AttachScalarColumn appends the subquery's single column.
            currentSchema = currentSchema.Concat(subPlan.Schema);
        }

        return current;
    }

    private static Stream<ZSet<StructuralRow, Z64>> AttachScalarColumn(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<ZSet<StructuralRow, Z64>> outer,
        Stream<ZSet<StructuralRow, Z64>> subq,
        Schema outerSchema,
        Schema subqSchema)
    {
        var codec = ctx.Codec;
        var snapshotCodecs = ctx.SnapshotCodecs;
        // Unit-key rekey of both sides: all outer rows under the same
        // 0-column key, and the subquery's (expected) single row under the
        // same key. LEFT JOIN ensures outer rows survive when the subquery
        // is empty (NULL-padded). Using a 0-column StructuralRow keeps the
        // operator's TKey aligned with StructuralRow throughout the SQL
        // layer, so the snapshot codec factory works uniformly.
        var outerIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            outer, _ => s_unitKey, r => r);
        var subqIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            subq, _ => s_unitKey, r => r);

        var leftCodec = snapshotCodecs?.CreateIndexedZSetTraceCodec(Schema.Empty, outerSchema);
        var rightCodec = snapshotCodecs?.CreateIndexedZSetTraceCodec(Schema.Empty, subqSchema);

        // ScalarSubqueryJoin appends one column per subquery; the per-iteration
        // schema isn't readily available here, so we let the codec fall back to
        // its untyped path (passing null). Not on the Joined GROUP BY hot path.
        return EmitLeftJoin(
            builder, ctx,
            outerIndexed,
            subqIndexed,
            joinCombine: (_, outerRow, scalarRow) => AppendColumn(codec, null, outerRow, scalarRow[0]),
            nullPadCombine: (_, outerRow) => AppendColumn(codec, null, outerRow, null),
            leftCodec, rightCodec);
    }

    private static StructuralRow AppendColumn(IRowCodec<StructuralRow> codec, Schema? schema, StructuralRow row, object? value)
    {
        var vs = new object?[row.Count + 1];
        for (var i = 0; i < row.Count; i++)
        {
            vs[i] = row[i];
        }

        vs[row.Count] = value;
        return codec.BuildRow(schema, vs);
    }

    private static (int[] LeftIndices, int[] RightIndices) ExtractEquiKeyIndices(JoinPlan plan)
    {
        var leftIndices = new int[plan.EquiKeys.Count];
        var rightIndices = new int[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            leftIndices[i] = plan.EquiKeys[i].LeftIndex;
            rightIndices[i] = plan.EquiKeys[i].RightIndex;
        }

        return (leftIndices, rightIndices);
    }

    private static StructuralRow NullPadRight(IRowCodec<StructuralRow> codec, Schema schema, StructuralRow left, int leftCount, int rightCount)
    {
        var vs = new object?[leftCount + rightCount];
        for (var i = 0; i < leftCount; i++)
        {
            vs[i] = left[i];
        }
        // Right-side columns default to null (object?[] initialises to nulls).
        return codec.BuildRow(schema, vs);
    }

    private static bool HasNoNullKey(StructuralRow row, int[] indices)
    {
        for (var i = 0; i < indices.Length; i++)
        {
            if (row[indices[i]] is null)
            {
                return false;
            }
        }

        return true;
    }

    private static StructuralRow ExtractKey(IRowCodec<StructuralRow> codec, Schema schema, StructuralRow row, int[] indices)
    {
        var vs = new object?[indices.Length];
        for (var i = 0; i < indices.Length; i++)
        {
            vs[i] = row[indices[i]];
        }

        return codec.BuildRow(schema, vs);
    }

    private static StructuralRow MergeRows(IRowCodec<StructuralRow> codec, Schema schema, StructuralRow left, StructuralRow right, int leftCount, int rightCount)
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

        return codec.BuildRow(schema, vs);
    }

    // ---- Aggregate ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileAggregate(
        CircuitBuilder builder,
        AggregatePlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        // Group-key indices. Resolver restricts v1 GROUP BY to bare column refs.
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
                    "internal: GROUP BY expression not reduced to a ResolvedColumn");
            }
        }

        // Build per-aggregate extractor/aggregator pairs in resolver order.
        var aggs = new SqlAggregator[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            aggs[i] = BuildSqlAggregator(plan.Aggregates[i]);
        }

        // Build the composite aggregator's output-row schema directly from
        // plan.Aggregates: the resolver may collect more AggregateCalls than
        // it surfaces as columns in plan.Schema (e.g. when the same aggregate
        // appears in both SELECT and HAVING and dedup happens at the schema
        // level), so plan.Schema may be narrower than the actual runtime row.
        var groupKeySchema = plan.Input.Schema.SubsetByIndex(groupIndices);
        var aggColumns = new SchemaColumn[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            aggColumns[i] = new SchemaColumn("$agg" + i, plan.Aggregates[i].ResultType);
        }

        var aggOnlySchema = new Schema(aggColumns);
        var composite = new CompositeAggregator(aggs, ctx.Codec, aggOnlySchema);

        // Rekey input rows by the group key; value = the entire input row so each
        // aggregator can extract its argument independently.
        var indexed = builder.GroupProject(
            input,
            row => ExtractKey(ctx.Codec, groupKeySchema, row, groupIndices),
            row => row);

        // Snapshot codec for the IndexedZSet trace inside IncrementalAggregateOp.
        // Bootstrap rebuilds aggregator scratch from the trace on Load, so only
        // the trace itself is round-tripped.
        var aggCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(
            groupKeySchema, plan.Input.Schema);

        // LATENESS GC: if a group-key column is monotone, drop groups below its
        // frontier. The group-key row stores keys in GROUP BY order, so the
        // monotone column sits at its group-key index.
        var (gcFrontier, gcMonotoneKey) = ResolveGroupKeyFrontier(plan, ctx);
        var aggregated = EmitAggregate(builder, ctx, indexed, composite, aggCodec, gcFrontier, gcMonotoneKey);

        var groupCount = plan.GroupKeys.Count;
        var aggCount = plan.Aggregates.Count;
        var codec = ctx.Codec;
        // The runtime row width here is groupCount + aggCount, which may
        // exceed plan.Schema.Count (see comment above). The wrapping Project
        // narrows to plan.Schema downstream, so we don't need a typed codec
        // for this intermediate row — pass null and let the codec fall back.
        return builder.MapRows(aggregated, pair =>
        {
            // pair = (GroupKey, CompositeAggregate output)
            var (key, aggRow) = pair;
            var vs = new object?[groupCount + aggCount];
            for (var i = 0; i < groupCount; i++)
            {
                vs[i] = key[i];
            }

            for (var i = 0; i < aggCount; i++)
            {
                vs[groupCount + i] = aggRow[i];
            }

            return codec.BuildRow(null, vs);
        });
    }

    private static SqlAggregator BuildSqlAggregator(AggregateCall call)
    {
        switch (call.Kind)
        {
            case AggregateKind.CountStar:
                return new SqlCountStarAggregator();
            case AggregateKind.Count:
                return new SqlCountAggregator(ExpressionCompiler.CompileScalar(call.Argument!));
            case AggregateKind.Sum:
                return new SqlSumAggregator(
                    ExpressionCompiler.CompileScalar(call.Argument!),
                    call.ResultType);
            case AggregateKind.Min:
                return new SqlMinMaxAggregator(
                    ExpressionCompiler.CompileScalar(call.Argument!),
                    wantMin: true);
            case AggregateKind.Max:
                return new SqlMinMaxAggregator(
                    ExpressionCompiler.CompileScalar(call.Argument!),
                    wantMin: false);
            case AggregateKind.Avg:
                return new SqlAvgAggregator(
                    ExpressionCompiler.CompileScalar(call.Argument!),
                    call.ResultType);
            default:
                throw new InvalidOperationException($"unsupported aggregate kind {call.Kind}");
        }
    }
}
