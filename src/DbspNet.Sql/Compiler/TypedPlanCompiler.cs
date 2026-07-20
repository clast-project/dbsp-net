// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using AstJoinType = DbspNet.Sql.Parser.Ast.JoinType;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Typed-row compile path. Recursively walks a <see cref="LogicalPlan"/>
/// and builds an equivalent circuit whose streams carry per-schema
/// emitted structs from <see cref="TypedRowEmitter"/> instead of
/// <see cref="StructuralRow"/>. Supports <see cref="ScanPlan"/>,
/// <see cref="FilterPlan"/>, <see cref="ProjectPlan"/>, and INNER
/// <see cref="JoinPlan"/>. Expressions are lowered via
/// <see cref="TypedExpressionCompiler"/>. Plans / subexpressions
/// outside the supported subset cause the whole compile to return
/// <c>false</c>; callers fall back to
/// <see cref="PlanToCircuit.Compile(LogicalPlan, ISqlSnapshotCodecs?)"/>.
/// </summary>
public static class TypedPlanCompiler
{
    /// <summary>
    /// Attempts to compile <paramref name="plan"/> into a
    /// <see cref="TypedCompiledQuery"/>. Returns <c>false</c> if any
    /// part of the plan or any referenced schema falls outside the
    /// typed pipeline's scope.
    /// </summary>
    public static bool TryCompile(LogicalPlan plan, out TypedCompiledQuery? compiled)
    {
        ArgumentNullException.ThrowIfNull(plan);
        compiled = null;

        var inputs = new Dictionary<string, TypedTableInput>(StringComparer.Ordinal);
        TypedNode? topNode = null;
        object? outputHandle = null;

        try
        {
            var circuit = RootCircuit.Build(builder =>
            {
                var ctx = new CompileContext(builder, inputs);
                topNode = TryCompileNode(plan, ctx)
                    ?? throw new UnsupportedPlanException();
                outputHandle = InvokeOutput(builder, topNode.RowType, topNode.Stream);
            });

            var factory = TypedRowEmitter.BuildBoxedFactory(topNode!.Schema)
                ?? throw new InvalidOperationException(
                    "TypedRowEmitter accepted the schema but produced no factory");

            var currentGetter = BuildCurrentZSetGetter(topNode.RowType, outputHandle!);
            var currentReader = BuildBoxedEntriesReader(topNode.RowType);
            var weightOf = BuildWeightOf(topNode.RowType, factory, currentGetter);

            compiled = new TypedCompiledQuery(
                circuit, inputs, topNode.Schema, topNode.RowType,
                currentGetter, currentReader, weightOf);
            return true;
        }
        catch (UnsupportedPlanException)
        {
            return false;
        }
    }

    /// <summary>The logical name the single query output is registered under in parallel mode.</summary>
    private const string OutputPortName = "$result";

    /// <summary>
    /// Data-parallel variant of <see cref="TryCompile"/>: compiles
    /// <paramref name="plan"/> into <paramref name="workers"/> identical circuit
    /// replicas driven by a <see cref="ParallelCircuit"/>, inserting an
    /// <c>exchange</c> before each key-sensitive operator so the result is
    /// independent of <paramref name="workers"/>. The same plan-shape support and
    /// fallback contract as <see cref="TryCompile"/>: returns <c>false</c> for any
    /// plan outside the typed pipeline. The caller owns the returned query and
    /// must dispose it (it holds the replica worker threads).
    /// <para>
    /// Pass <paramref name="snapshotCodecs"/> to make the replicas' stateful
    /// operators snapshottable — each replica is a plain <see cref="RootCircuit"/>,
    /// so <see cref="DbspNet.Persistence.ParallelSnapshot"/> snapshots the W
    /// replicas into per-worker subtrees. Recovery requires the same W (the stable
    /// hash partition is part of the persisted state).
    /// </para>
    /// </summary>
    public static bool TryCompileParallel(
        LogicalPlan plan,
        int workers,
        out ParallelTypedCompiledQuery? compiled,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentOutOfRangeException.ThrowIfLessThan(workers, 1);
        compiled = null;
        var compileOptions = options ?? CompileOptions.Default;

        // Shared across replicas; CompileScan fills it idempotently (the row
        // types/schemas are replica-independent — same schema fingerprint).
        var meta = new Dictionary<string, ParallelInputMeta>(StringComparer.Ordinal);
        TypedNode? topNode = null;

        // Realise the spine memtable capacity (CompileOptions.SpineStagingCapacity)
        // through the SpineStagingConfig ambient seam each trace reads at
        // construction. Replicas are built sequentially on this thread inside
        // ParallelCircuit.Build, so a scoped seam captures every trace; restore it
        // after. Flat forces 0 (no spine traces read it). See docs §11.
        var prevStagingCapacity = SpineStagingConfig.Capacity;
        SpineStagingConfig.Capacity =
            compileOptions.TraceFamily == TraceFamily.Spine ? compileOptions.SpineStagingCapacity : 0;

        ParallelCircuit? circuit = null;
        try
        {
            circuit = ParallelCircuit.Build(workers, builder =>
            {
                var ctx = new CompileContext(builder, meta, workers, snapshotCodecs, compileOptions);
                var top = TryCompileNode(plan, ctx) ?? throw new UnsupportedPlanException();
                InvokeNamedOutput(builder, top.RowType, top.Stream, OutputPortName);
                topNode = top;
            });

            var outputRowType = topNode!.RowType;
            var outputSchema = topNode.Schema;

            // One parallel ingestor per table. Input sharding is arbitrary (a
            // downstream exchange re-shards by op key), so split by whole-row hash
            // for balance; the encode runs on the worker threads.
            var inputs = new Dictionary<string, TypedTableInput>(StringComparer.Ordinal);
            foreach (var (tableName, m) in meta)
            {
                var ingestor = BuildParallelIngestor(circuit, tableName, m.RowType, m.Schema, m.Factory);
                inputs[tableName] = new TypedTableInput(m.Schema, m.RowType, ingestor);
            }

            var shardedOutput = InvokeShardedOutput(circuit, outputRowType, OutputPortName);
            var currentGetter = BuildShardedCurrentZSetGetter(outputRowType, shardedOutput);
            var currentReader = BuildBoxedEntriesReader(outputRowType);
            var factory = TypedRowEmitter.BuildBoxedFactory(outputSchema)!;
            var weightOf = BuildWeightOf(outputRowType, factory, currentGetter);

            // When the output shards are provably key-disjoint, gather them in
            // parallel (per-worker decode + concat) instead of the serial Z-set
            // sum. Only worth the parallel hand-off for W > 1; W == 1 keeps the
            // lazy serial decode. A non-disjoint output keeps the summing path.
            IOutputGather? disjointGather = null;
            if (workers > 1 && topNode!.ShardDisjoint)
            {
                var getters = TypedRowEmitter.BuildFieldGetters(outputSchema)
                    ?? throw new UnsupportedPlanException();
                disjointGather = BuildDisjointGather(circuit, outputRowType, OutputPortName, getters);
            }

            compiled = new ParallelTypedCompiledQuery(
                circuit, inputs, outputSchema, outputRowType, currentGetter, currentReader, weightOf, disjointGather);
            return true;
        }
        catch (UnsupportedPlanException)
        {
            circuit?.Dispose();
            return false;
        }
        finally
        {
            SpineStagingConfig.Capacity = prevStagingCapacity;
        }
    }

    /// <summary>
    /// One node's compiled output: the closed CLR type of rows on the
    /// stream, the SQL schema of that row, and the (boxed) typed
    /// stream object.
    /// </summary>
    /// <param name="ShardDisjoint">
    /// In a parallel build, <see langword="true"/> when this stream's per-worker
    /// shards are provably key-disjoint — i.e. the stream is hash-partitioned by
    /// columns that the row still carries by identity, so equal rows always land
    /// on one worker. The output gather can then concatenate the shards in parallel
    /// instead of a serial Z-set sum. Conservatively <see langword="false"/>
    /// (the safe default) for any operator whose sharding we don't track; a wrong
    /// <see langword="true"/> would emit duplicate output keys, so it is only set
    /// where disjointness is certain.
    /// </param>
    /// <param name="PartitionKey">
    /// In a parallel build, the column indices (into this node's <see cref="Schema"/>)
    /// by which the stream is hash-partitioned across workers — i.e. any two rows
    /// agreeing on these columns are guaranteed co-located on one worker. An
    /// exchange sets it to the shuffle key; a join/aggregate inherits or refines
    /// it. <see langword="null"/> means "not usefully partitioned" (e.g. a raw
    /// whole-row-hashed scan, or after a column-dropping projection). Used to elide
    /// a redundant exchange: an operator keyed on <c>K</c> needs no shuffle when the
    /// input is already partitioned by a subset of <c>K</c> (so every <c>K</c>-group
    /// already lives wholly on one worker). Only meaningful for W &gt; 1.
    /// </param>
    private sealed record TypedNode(
        Type RowType, Schema Schema, object Stream,
        bool ShardDisjoint = false, int[]? PartitionKey = null);

    /// <summary>True when every column in <paramref name="sub"/> appears in
    /// <paramref name="super"/> — i.e. the data's partition key is a subset of the
    /// operator's key, so each operator-key group is already on one worker.</summary>
    private static bool IsKeySubset(int[] sub, int[] super)
    {
        foreach (var c in sub)
        {
            if (Array.IndexOf(super, c) < 0) return false;
        }

        return true;
    }

    /// <summary>
    /// Boundary-adapter entry point: compile <paramref name="plan"/>
    /// inside an existing <see cref="CircuitBuilder"/>, lifting each
    /// scan from a caller-supplied structural-row input stream and
    /// projecting the final typed stream back to
    /// <c>ZSet&lt;StructuralRow, Z64&gt;</c>. Returns <c>null</c> if
    /// the plan or any subexpression is outside the typed pipeline's
    /// scope; the caller (PlanToCircuit) then falls back to its
    /// existing structural compile path. The internal operator stages
    /// run typed; only the input/output boundaries pay the conversion
    /// cost.
    /// </summary>
    internal static Stream<ZSet<StructuralRow, Z64>>? TryCompileWithStructuralBoundary(
        CircuitBuilder builder,
        LogicalPlan plan,
        IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> structuralScans,
        IRowCodec<StructuralRow> outputCodec,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(structuralScans);
        ArgumentNullException.ThrowIfNull(outputCodec);

        try
        {
            var ctx = new CompileContext(builder, structuralScans, snapshotCodecs, options ?? CompileOptions.Default);
            var topNode = TryCompileNode(plan, ctx)
                ?? throw new UnsupportedPlanException();

            return AdaptTypedToStructural(builder, topNode, outputCodec);
        }
        catch (UnsupportedPlanException)
        {
            return null;
        }
    }

    /// <summary>
    /// Measurement helper (design §23 A/B — <b>not</b> part of the runtime
    /// contract): compile a chain of dependent views with a <b>true typed seam</b>
    /// between them. Each entry in <paramref name="chain"/> is compiled typed
    /// against the structural scans <em>plus</em> the already-built typed streams of
    /// the earlier entries — so a later view scanning an earlier view's name binds
    /// directly to that view's typed stream (no <c>Adapt→StructuralRow</c> /
    /// <c>lift→typed</c> round-trip at the seam). Only leaf scans of names in
    /// <paramref name="structuralScans"/> pay the structural lift, and only the
    /// <em>last</em> view's output is adapted back to <c>StructuralRow</c>. Returns
    /// <c>null</c> if any view falls outside the typed subset. Isolates the
    /// inter-view seam cost from the state-representation cost.
    /// </summary>
    internal static Stream<ZSet<StructuralRow, Z64>>? TryCompileTypedSeamChain(
        CircuitBuilder builder,
        IReadOnlyList<(string Name, LogicalPlan Plan)> chain,
        IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> structuralScans,
        IRowCodec<StructuralRow> outputCodec,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(structuralScans);
        ArgumentNullException.ThrowIfNull(outputCodec);
        if (chain.Count == 0) return null;

        try
        {
            // One shared context: its LiftedScanCache carries both the structural
            // lifts of leaf tables AND the typed streams of earlier chain views.
            var ctx = new CompileContext(builder, structuralScans, snapshotCodecs, options ?? CompileOptions.Default);
            TypedNode? last = null;
            foreach (var (name, plan) in chain)
            {
                var node = TryCompileNode(plan, ctx) ?? throw new UnsupportedPlanException();
                // Seed the seam: a later view's ScanPlan for `name` hits this typed
                // stream (CompileScan checks LiftedScanCache before lifting).
                ctx.LiftedScanCache[name] = node;
                last = node;
            }

            return AdaptTypedToStructural(builder, last!, outputCodec);
        }
        catch (UnsupportedPlanException)
        {
            return null;
        }
    }

    private sealed class CompileContext
    {
        public CircuitBuilder Builder { get; }

        /// <summary>
        /// Accumulator for the typed input handles when running in
        /// standalone mode (called from <see cref="TryCompile"/>).
        /// <c>null</c> in boundary-adapter mode, where scans are
        /// lifted from caller-supplied structural-row streams instead.
        /// </summary>
        public Dictionary<string, TypedTableInput>? Inputs { get; }

        /// <summary>
        /// Per-table structural-row input streams in boundary-adapter
        /// mode. <c>null</c> in standalone mode.
        /// </summary>
        public IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>>? StructuralScans { get; }

        /// <summary>
        /// Cache of structural-row scans already lifted to their typed
        /// stream — repeat scans on the same table share the lifted
        /// stream rather than re-emitting the MapRows.
        /// </summary>
        public Dictionary<string, TypedNode> LiftedScanCache { get; } = new(StringComparer.Ordinal);

        /// <summary>
        /// Per-CTE compiled typed stream, keyed by
        /// <see cref="CteRef"/> identity. The first
        /// <see cref="CteScanPlan"/> referencing a CTE compiles its
        /// underlying plan; subsequent scans share that stream.
        /// Mirrors <see cref="PlanToCircuit.CompileContext.CteCache"/>.
        /// </summary>
        public Dictionary<CteRef, TypedNode> CteCache { get; } = new();

        /// <summary>
        /// Snapshot codec factory threaded through from the SQL
        /// compiler entry point. Stateful operators (Join, Aggregate)
        /// build typed-adapted codecs from it via the
        /// <see cref="TypedZSetTraceCodecAdapter{TKey}"/> and
        /// <see cref="TypedIndexedZSetTraceCodecAdapter{TKey,TValue}"/>
        /// adapters. <c>null</c> when snapshotting is disabled.
        /// </summary>
        public ISqlSnapshotCodecs? SnapshotCodecs { get; }

        /// <summary>
        /// Compile-time knobs threaded from <see cref="PlanToCircuit.Compile"/>.
        /// In particular, <c>Options.TraceFamily</c> selects between the
        /// flat and spine stateful operators at every site the typed
        /// pipeline emits (Distinct, Aggregate, Join). Defaults to
        /// <see cref="CompileOptions.Default"/> (flat) for standalone
        /// callers via <see cref="TryCompile"/>.
        /// </summary>
        public CompileOptions Options { get; }

        /// <summary>
        /// Replica count when compiling for a <see cref="ParallelCircuit"/>; 1 for
        /// a single circuit. When &gt; 1 the lowering emits an <c>exchange</c>
        /// before each key-sensitive operator (join / aggregate / distinct).
        /// </summary>
        public int Workers { get; }

        /// <summary>
        /// In parallel standalone mode, collects per-table metadata (schema, row
        /// type, boxed factory) as scans are compiled, so the caller can build a
        /// sharded input per table after the replicas are built. <c>null</c>
        /// otherwise; its presence also marks "name the input/output ports".
        /// </summary>
        public Dictionary<string, ParallelInputMeta>? ParallelInputs { get; }

        public CompileContext(CircuitBuilder builder, Dictionary<string, TypedTableInput> inputs)
        {
            Builder = builder;
            Inputs = inputs;
            StructuralScans = null;
            SnapshotCodecs = null;
            Options = CompileOptions.Default;
            Workers = 1;
        }

        public CompileContext(
            CircuitBuilder builder,
            IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> structuralScans,
            ISqlSnapshotCodecs? snapshotCodecs,
            CompileOptions options)
        {
            Builder = builder;
            Inputs = null;
            StructuralScans = structuralScans;
            SnapshotCodecs = snapshotCodecs;
            Options = options;
            Workers = 1;
        }

        public CompileContext(
            CircuitBuilder builder,
            Dictionary<string, ParallelInputMeta> parallelInputs,
            int workers,
            ISqlSnapshotCodecs? snapshotCodecs,
            CompileOptions options)
        {
            Builder = builder;
            Inputs = null;
            ParallelInputs = parallelInputs;
            StructuralScans = null;
            SnapshotCodecs = snapshotCodecs;
            Options = options;
            Workers = workers;
        }
    }

    /// <summary>
    /// Per-table metadata captured during a parallel compile so the
    /// caller can build a <see cref="ShardedInputHandle{TKey,TWeight}"/>-backed
    /// <see cref="TypedTableInput"/> once the replicas exist.
    /// </summary>
    internal sealed record ParallelInputMeta(Schema Schema, Type RowType, Func<object?[], object> Factory);

    private sealed class UnsupportedPlanException : Exception;

    private static TypedNode? TryCompileNode(LogicalPlan plan, CompileContext ctx) => plan switch
    {
        ScanPlan s => CompileScan(s, ctx),
        // Filter / Project are pointwise and stateless; a maximal run of them
        // fuses into one pass (see CompileLinearChain).
        FilterPlan => CompileLinearChain(plan, ctx),
        ProjectPlan => CompileLinearChain(plan, ctx),
        JoinPlan j => CompileJoin(j, ctx),
        AggregatePlan a => CompileAggregate(a, ctx),
        UnionAllPlan u => CompileUnionAll(u, ctx),
        DistinctPlan d => CompileDistinct(d, ctx),
        DifferencePlan diff => CompileDifference(diff, ctx),
        CteScanPlan c => CompileCteScan(c, ctx),
        ScalarSubqueryJoinPlan s => CompileScalarSubqueryJoin(s, ctx),
        RecursiveCtePlan r => CompileRecursiveCte(r, ctx),
        TopKPlan t => CompileTopK(t, ctx),
        PartitionedTopKPlan pt => CompilePartitionedTopK(pt, ctx),
        // Rank-in-output (ROW_NUMBER / RANK / DENSE_RANK as a column) has no typed
        // path — it recomputes whole partitions positionally. Returning null falls
        // the whole query back to the structural compiler (the marquee case is the
        // unpartitioned analytics rank, where a typed/parallel path would not help).
        PartitionedRankPlan => null,
        // PARTITION BY window aggregates take the typed/parallel path (the
        // operator is row-opaque; a struct-fusing widener appends the agg
        // columns). No-partition windows and unsupported aggregators (MIN/MAX,
        // see BuildTypedAggregator) fall back to structural inside the method.
        WindowAggregatePlan w => CompileWindowAggregate(w, ctx),
        // PARTITION BY offset functions (LAG/LEAD/FIRST_VALUE/LAST_VALUE) take the
        // typed/parallel path too (row-opaque op + a struct-fusing widener).
        WindowOffsetPlan wo => CompileWindowOffset(wo, ctx),
        _ => null,
    };

    // ---- Plan node dispatch ----

    private static TypedNode? CompileScan(ScanPlan scan, CompileContext ctx)
    {
        // Phase N3: scan-level nullable gate lifted. TypedRowEmitter
        // (N1.1) emits Nullable<T> fields, the expression compiler
        // (N2) does NULL propagation, and BuildStructuralToTypedDelegate
        // (this phase) handles null entries at the input boundary.
        // Downstream operators (Filter, Join, Aggregate) keep their
        // own gates that bail when they receive a nullable stream
        // until their respective NULL semantics ship in later phases.
        var rowType = TypedRowEmitter.EmitRowType(scan.Schema);
        if (rowType is null) return null;

        if (ctx.StructuralScans is not null)
        {
            // Boundary-adapter mode: lift each scan from the caller's
            // structural-row stream into a typed stream. Cache by
            // table name so repeat scans reuse the lifted stream.
            if (ctx.LiftedScanCache.TryGetValue(scan.TableName, out var cached))
            {
                return cached;
            }

            if (!ctx.StructuralScans.TryGetValue(scan.TableName, out var structuralStream))
            {
                // The plan refers to a scan the caller didn't provide
                // — shouldn't happen because CollectScans walks the
                // plan first, but treat it as unsupported.
                return null;
            }

            var lifter = BuildStructuralToTypedDelegate(scan.Schema, rowType);
            var liftedStream = InvokeMapRows(
                ctx.Builder, typeof(StructuralRow), rowType, structuralStream, lifter);
            var node = new TypedNode(rowType, scan.Schema, liftedStream);
            ctx.LiftedScanCache[scan.TableName] = node;
            return node;
        }

        // Standalone mode: create a typed input handle on first
        // visit and cache the (TableInput, stream) by table name so
        // repeat scans of the same table share both the handle
        // (i.e. caller-pushed deltas reach every consumer) and the
        // stream. Plans like EXCEPT can visit Scan(t) twice via the
        // BuildExcept decomposition.
        if (ctx.LiftedScanCache.TryGetValue(scan.TableName, out var cachedStandalone))
        {
            return cachedStandalone;
        }

        var factory = TypedRowEmitter.BuildBoxedFactory(scan.Schema)!;

        if (ctx.ParallelInputs is not null)
        {
            // Parallel build: name the input port (so the caller can reach every
            // replica's copy via ShardedInput) and record the metadata needed to
            // build the sharded input after the replicas exist. The per-replica
            // handle itself is reached by name, not captured here.
            var (_, parallelStream) = InvokeZSetInput(ctx.Builder, rowType, scan.TableName);
            ctx.ParallelInputs[scan.TableName] = new ParallelInputMeta(scan.Schema, rowType, factory);
            // The sharded input partitions by whole-row hash (BuildParallelIngestor),
            // so equal rows co-locate: the scan stream is shard-disjoint.
            var parallelNode = new TypedNode(rowType, scan.Schema, parallelStream, ShardDisjoint: true);
            ctx.LiftedScanCache[scan.TableName] = parallelNode;
            return parallelNode;
        }

        var (handle, stream) = InvokeZSetInput(ctx.Builder, rowType);
        ctx.Inputs![scan.TableName] = new TypedTableInput(scan.Schema, rowType, factory, handle);
        var standaloneNode = new TypedNode(rowType, scan.Schema, stream);
        ctx.LiftedScanCache[scan.TableName] = standaloneNode;
        return standaloneNode;
    }

    private enum TypedStageKind
    {
        Filter,
        Map,
    }

    /// <summary>One stage of a fused typed linear chain. <see cref="Delegate"/>
    /// is a <c>Func&lt;TCur, bool&gt;</c> (filter) or <c>Func&lt;TCur, TNext&gt;</c>
    /// (map); <see cref="RowTypeAfter"/> / <see cref="SchemaAfter"/> are the row
    /// type and schema the stage leaves the stream in (unchanged for a filter).</summary>
    private readonly record struct TypedStage(
        TypedStageKind Kind, Delegate Delegate, Type RowTypeAfter, Schema SchemaAfter);

    /// <summary>
    /// Compiles a maximal run of consecutive <see cref="FilterPlan"/> /
    /// <see cref="ProjectPlan"/> nodes into a single fused pass. Both are
    /// pointwise and stateless, so chaining them as separate typed operators
    /// materializes an intermediate Z-set (one allocation + one full iteration)
    /// between every stage. Folding the run into one
    /// <see cref="LinearOperators.MapFilterRows{TIn,TOut,TWeight}"/> evaluates
    /// every stage per row in a single iteration with one output allocation.
    /// </summary>
    /// <remarks>
    /// Each stage reuses the exact per-stage typed delegate builders
    /// (<see cref="BuildTypedPredicateDelegate"/> /
    /// <see cref="BuildTypedProjectionDelegate"/>), so fusion succeeds precisely
    /// when the un-fused stages each would have — any stage outside typed scope
    /// returns <c>null</c> and the whole compile falls back to structural, same
    /// as before. A single-stage chain lowers to the original dedicated operator
    /// (<c>Filter</c> / <c>MapRows</c>) so common single-op queries keep their
    /// exact prior layout; only genuine multi-stage chains fuse.
    /// </remarks>
    private static TypedNode? CompileLinearChain(LogicalPlan plan, CompileContext ctx)
    {
        // Walk outermost→innermost collecting the linear nodes; identity
        // projections are pure renames and contribute no runtime stage.
        var nodes = new List<LogicalPlan>();
        var node = plan;
        while (node is FilterPlan or ProjectPlan)
        {
            if (node is ProjectPlan ip
                && IsIdentityProjection(ip.Projections, ip.Input.Schema, ip.Schema))
            {
                node = ip.Input;
                continue;
            }

            nodes.Add(node);
            node = node is FilterPlan f ? f.Input : ((ProjectPlan)node).Input;
        }

        var baseNode = TryCompileNode(node, ctx);
        if (baseNode is null) return null;

        // All-identity (or empty) chain → pass the input through unchanged.
        if (nodes.Count == 0)
        {
            return baseNode;
        }

        // Collected outermost-first; data flows from the input upward, so build
        // the stages innermost-first.
        nodes.Reverse();

        // Disjointness is preserved by filters and by injective projections (those
        // that keep every input column by identity, so the output row still
        // determines the shard key). A projection that drops a shard column breaks
        // it. Identity projections were skipped above and are injective, so they
        // never appear here. (Skipped identity projections preserve it for free.)
        var shardDisjoint = baseNode.ShardDisjoint;
        foreach (var n in nodes)
        {
            if (n is ProjectPlan p && !RetainsAllInputColumns(p.Projections, p.Input.Schema))
            {
                shardDisjoint = false;
            }
        }

        // The upstream partition key (column indices) survives a filter unchanged,
        // but any projection may drop or reorder columns and invalidate the
        // indices. Identity projections are skipped above, so a projection here is
        // always non-identity — conservatively drop the partition tracking.
        var partitionKey = baseNode.PartitionKey;
        foreach (var n in nodes)
        {
            if (n is ProjectPlan) partitionKey = null;
        }

        var stages = new List<TypedStage>(nodes.Count);
        var currentRowType = baseNode.RowType;
        var currentSchema = baseNode.Schema;
        foreach (var n in nodes)
        {
            if (n is FilterPlan f)
            {
                // Nullable<bool> WHERE predicates are coerced to plain bool inside
                // BuildTypedPredicateDelegate (GetValueOrDefault) — TRUE keeps the
                // row; FALSE and NULL both drop it.
                var predicate = BuildTypedPredicateDelegate(f.Predicate, currentRowType);
                if (predicate is null) return null;
                stages.Add(new TypedStage(TypedStageKind.Filter, predicate, currentRowType, currentSchema));
            }
            else
            {
                // Respect the resolver's per-column nullability on the output
                // schema so genuinely-nullable expressions (NULLIF, NULL literal)
                // are carried as Nullable<T> fields downstream.
                var p = (ProjectPlan)n;
                var outRowType = TypedRowEmitter.EmitRowType(p.Schema);
                if (outRowType is null) return null;
                var projDelegate = BuildTypedProjectionDelegate(
                    currentRowType, outRowType, p.Schema, p.Projections);
                if (projDelegate is null) return null;
                stages.Add(new TypedStage(TypedStageKind.Map, projDelegate, outRowType, p.Schema));
                currentRowType = outRowType;
                currentSchema = p.Schema;
            }
        }

        // Single stage: lower to the original dedicated operator so the common
        // single-filter / single-project case keeps its exact prior shape.
        if (stages.Count == 1)
        {
            var only = stages[0];
            if (only.Kind == TypedStageKind.Filter)
            {
                var stream = InvokeFilter(ctx.Builder, baseNode.RowType, baseNode.Stream, only.Delegate);
                return new TypedNode(baseNode.RowType, baseNode.Schema, stream, shardDisjoint, partitionKey);
            }
            else
            {
                var stream = InvokeMapRows(
                    ctx.Builder, baseNode.RowType, only.RowTypeAfter, baseNode.Stream, only.Delegate);
                return new TypedNode(only.RowTypeAfter, only.SchemaAfter, stream, shardDisjoint, partitionKey);
            }
        }

        var fused = BuildFusedTypedDelegate(baseNode.RowType, currentRowType, stages);
        var fusedStream = InvokeMapFilterRows(
            ctx.Builder, baseNode.RowType, currentRowType, baseNode.Stream, fused);
        return new TypedNode(currentRowType, currentSchema, fusedStream, shardDisjoint, partitionKey);
    }

    /// <summary>
    /// Builds the fused per-row delegate
    /// <c>(TInRoot in) =&gt; (Keep, Value)</c> that threads a row through every
    /// stage: a filter that fails short-circuits to <c>(false, default)</c>; a
    /// map assigns the projected row into a local and the next stage reads it.
    /// The pre-compiled per-stage typed delegates are embedded as constants and
    /// invoked, so each stage stays strongly typed (no boxing of the row structs).
    /// </summary>
    private static Delegate BuildFusedTypedDelegate(
        Type inRootType, Type outFinalType, List<TypedStage> stages)
    {
        var tupleType = typeof(ValueTuple<,>).MakeGenericType(typeof(bool), outFinalType);
        var tupleCtor = tupleType.GetConstructor(new[] { typeof(bool), outFinalType })!;
        var returnLabel = Expression.Label(tupleType, "ret");
        var inParam = Expression.Parameter(inRootType, "in");
        var dropTuple = Expression.New(
            tupleCtor, Expression.Constant(false), Expression.Default(outFinalType));

        var vars = new List<ParameterExpression>();
        var body = new List<Expression>();
        Expression current = inParam;
        var idx = 0;
        foreach (var stage in stages)
        {
            if (stage.Kind == TypedStageKind.Filter)
            {
                var predFuncType = typeof(Func<,>).MakeGenericType(current.Type, typeof(bool));
                var test = Expression.Not(
                    Expression.Invoke(Expression.Constant(stage.Delegate, predFuncType), current));
                body.Add(Expression.IfThen(test, Expression.Return(returnLabel, dropTuple)));
            }
            else
            {
                var projFuncType = typeof(Func<,>).MakeGenericType(current.Type, stage.RowTypeAfter);
                var v = Expression.Variable(stage.RowTypeAfter, "r" + idx);
                vars.Add(v);
                body.Add(Expression.Assign(
                    v, Expression.Invoke(Expression.Constant(stage.Delegate, projFuncType), current)));
                current = v;
            }

            idx++;
        }

        body.Add(Expression.Return(
            returnLabel, Expression.New(tupleCtor, Expression.Constant(true), current)));
        body.Add(Expression.Label(returnLabel, Expression.Default(tupleType)));

        var funcType = typeof(Func<,>).MakeGenericType(inRootType, tupleType);
        return Expression.Lambda(funcType, Expression.Block(vars, body), inParam).Compile();
    }

    /// <summary>
    /// Compiles a <see cref="JoinPlan"/> of any supported join type.
    /// INNER joins emit the side-by-side concatenation of matching
    /// rows. LEFT / RIGHT OUTER joins additionally emit a NULL-padded
    /// row for each unmatched preserved-side row; the resolver marks
    /// the unmatched side's columns as nullable on the output schema,
    /// so the emitted output row carries <c>Nullable&lt;T&gt;</c>
    /// fields on that side and the null-pad combine just leaves them
    /// at their default. FULL OUTER and nullable equi-key columns are
    /// out of scope — fall back to structural for those.
    /// </summary>
    private static TypedNode? CompileJoin(JoinPlan plan, CompileContext ctx)
    {
        if (plan.JoinType is not (AstJoinType.Inner or AstJoinType.LeftOuter
            or AstJoinType.RightOuter or AstJoinType.FullOuter))
        {
            return null;
        }

        // A zero-equi-key INNER join is a nested-loop / cross join: broadcast
        // both sides through a single unit (zero-column) key — the same
        // Schema.Empty pattern CompileScalarSubqueryJoin uses — then the
        // residual ON predicate (below) filters the cross product. Outer joins
        // never reach here with zero equi-keys (the resolver rejects them).
        var keyless = plan.EquiKeys.Count == 0;
        if (keyless && plan.JoinType is not AstJoinType.Inner) return null;

        var left = TryCompileNode(plan.Left, ctx);
        if (left is null) return null;
        var right = TryCompileNode(plan.Right, ctx);
        if (right is null) return null;

        // Equi-key columns must be NOT NULL on both sides. For INNER
        // this is correctness via "NULL ≠ anything"; for LEFT / RIGHT
        // it's a scope restriction — handling nullable keys requires
        // bypassing NULL-keyed preserved-side rows directly to the
        // null-pad branch (parallel to PlanToCircuit's
        // nullKeyLeft / nullKeyRight Filter+MapRows). That bypass is
        // out of scope for the typed pipeline today; the structural
        // fallback handles those queries.
        var leftIndices = new int[plan.EquiKeys.Count];
        var rightIndices = new int[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            leftIndices[i] = plan.EquiKeys[i].LeftIndex;
            rightIndices[i] = plan.EquiKeys[i].RightIndex;
            if (left.Schema[leftIndices[i]].Type.Nullable) return null;
            if (right.Schema[rightIndices[i]].Type.Nullable) return null;
        }

        var keySchema = keyless ? Schema.Empty : left.Schema.SubsetByIndex(leftIndices);
        var keyRowType = TypedRowEmitter.EmitRowType(keySchema);
        if (keyRowType is null) return null;

        var leftKeyExtractor = BuildKeyExtractorDelegate(
            left.RowType, keyRowType, keySchema, leftIndices);
        var rightKeyExtractor = BuildKeyExtractorDelegate(
            right.RowType, keyRowType, keySchema, rightIndices);
        if (leftKeyExtractor is null || rightKeyExtractor is null) return null;

        var outputSchema = plan.Schema;
        var outputRowType = TypedRowEmitter.EmitRowType(outputSchema);
        if (outputRowType is null) return null;

        var combineDelegate = BuildJoinCombineDelegate(
            keyRowType, left.RowType, right.RowType, outputRowType,
            outputSchema, left.Schema.Count, right.Schema.Count);

        // Parallel: re-shard both sides by the join key so matching rows
        // co-locate on one worker, then index by that key for the (local) join —
        // both in one fused ExchangeIndex pass (no throwaway flat Z-set between
        // the shuffle and the re-index). Both sides hash the same key type, so
        // equal keys land on the same worker. At W=1 ExchangeIndex is a plain
        // GroupProject, so the single-thread shape is unchanged.
        var leftPartition = BuildKeyHashPartition(left.RowType, keyRowType, leftKeyExtractor);
        var rightPartition = BuildKeyHashPartition(right.RowType, keyRowType, rightKeyExtractor);

        object leftIndexed, rightIndexed;
        if (ctx.Options.CoalesceJoinExchange)
        {
            // Fuse the two key exchanges into one shared barrier (§15): both sides
            // are independent and adjacent (no dependency between them), so a single
            // rendezvous serves both, halving the join's barrier straggler tax.
            (leftIndexed, rightIndexed) = InvokeExchangeIndexJoin(
                ctx.Builder, keyRowType, left.RowType, right.RowType,
                left.Stream, leftPartition, leftKeyExtractor,
                right.Stream, rightPartition, rightKeyExtractor);
        }
        else
        {
            leftIndexed = InvokeExchangeIndex(
                ctx.Builder, left.RowType, keyRowType, left.Stream, leftPartition, leftKeyExtractor);
            rightIndexed = InvokeExchangeIndex(
                ctx.Builder, right.RowType, keyRowType, right.Stream, rightPartition, rightKeyExtractor);
        }

        var leftSnapshotCodec = BuildAdaptedIndexedCodec(
            ctx.SnapshotCodecs, keySchema, left.Schema, keyRowType, left.RowType);
        var rightSnapshotCodec = BuildAdaptedIndexedCodec(
            ctx.SnapshotCodecs, keySchema, right.Schema, keyRowType, right.RowType);

        // An INNER residual is applied during the join enumeration (so rejected
        // rows never enter the output Z-set) on the in-memory join op. The spine
        // join variant has no residual hook, so there it falls back to the
        // post-join filter below. Build the predicate up front; a null means it's
        // outside the typed pipeline — bail so the structural path takes it.
        var fuseResidual = plan.JoinType is AstJoinType.Inner
            && plan.Residual is not null
            && ctx.Options.TraceFamily != TraceFamily.Spine;
        Delegate? joinResidual = null;
        if (fuseResidual)
        {
            joinResidual = BuildTypedPredicateDelegate(plan.Residual!, outputRowType);
            if (joinResidual is null) return null;
        }

        object joined;
        if (plan.JoinType is AstJoinType.Inner)
        {
            joined = EmitInnerJoin(
                ctx, keyRowType, left.RowType, right.RowType, outputRowType,
                leftIndexed, rightIndexed, combineDelegate,
                leftSnapshotCodec, rightSnapshotCodec, joinResidual);
        }
        else if (plan.JoinType is AstJoinType.LeftOuter)
        {
            // Resolver rejects residuals on outer joins (their
            // failure-on-residual semantics don't compose with the
            // null-pad branch); double-check here.
            if (plan.Residual is not null) return null;
            var nullPad = BuildNullPadCombineDelegate(
                keyRowType, left.RowType, outputRowType, outputSchema,
                preservedSideOffset: 0, preservedSideCount: left.Schema.Count);
            if (nullPad is null) return null;
            joined = EmitLeftJoin(
                ctx, keyRowType, left.RowType, right.RowType, outputRowType,
                leftIndexed, rightIndexed, combineDelegate, nullPad,
                leftSnapshotCodec, rightSnapshotCodec);
        }
        else if (plan.JoinType is AstJoinType.RightOuter)
        {
            if (plan.Residual is not null) return null;
            // Swap sides: the right stream becomes the preserved side
            // fed to IncrementalLeftJoin. Output column order stays
            // [left cols, right cols], so the combine reverses its
            // first two args back to (left, right) and the null-pad
            // populates the right cols from the preserved row, leaving
            // the left cols at default(Nullable<T>).
            var swappedCombine = BuildSwappedJoinCombineDelegate(
                keyRowType, left.RowType, right.RowType, outputRowType,
                outputSchema, left.Schema.Count, right.Schema.Count);
            var nullPad = BuildNullPadCombineDelegate(
                keyRowType, right.RowType, outputRowType, outputSchema,
                preservedSideOffset: left.Schema.Count,
                preservedSideCount: right.Schema.Count);
            if (nullPad is null) return null;
            joined = EmitLeftJoin(
                ctx, keyRowType, right.RowType, left.RowType, outputRowType,
                rightIndexed, leftIndexed, swappedCombine, nullPad,
                rightSnapshotCodec, leftSnapshotCodec);
        }
        else // FullOuter
        {
            if (plan.Residual is not null) return null;
            // Nullable equi-keys already bailed above, so every row here has a
            // non-null key — no NULL-key bypass branch is needed (the structural
            // fallback handles nullable-key FULL joins). Pad left rows into the
            // left cols (right default) and right rows into the right cols
            // (left default); output column order is [left, right].
            var nullPadRight = BuildNullPadCombineDelegate(
                keyRowType, left.RowType, outputRowType, outputSchema,
                preservedSideOffset: 0, preservedSideCount: left.Schema.Count);
            var nullPadLeft = BuildNullPadCombineDelegate(
                keyRowType, right.RowType, outputRowType, outputSchema,
                preservedSideOffset: left.Schema.Count,
                preservedSideCount: right.Schema.Count);
            if (nullPadRight is null || nullPadLeft is null) return null;
            joined = EmitFullJoin(
                ctx, keyRowType, left.RowType, right.RowType, outputRowType,
                leftIndexed, rightIndexed, combineDelegate, nullPadRight, nullPadLeft,
                leftSnapshotCodec, rightSnapshotCodec);
        }

        // Output is partitioned by the join key. Left columns lead the output
        // schema, so the left key indices are the key's output positions — a
        // downstream GROUP BY on (those keys ∪ more) needs no re-shuffle.
        var joinPartition = ctx.Workers > 1 ? leftIndices : null;
        var node = new TypedNode(outputRowType, outputSchema, joined, PartitionKey: joinPartition);

        if (plan.Residual is not null && !fuseResidual)
        {
            // Only INNER reaches here (LEFT/RIGHT bail above). This is the spine
            // path, where the join op has no residual hook — apply it as a
            // post-join filter (the non-spine path fused it into the join above).
            var residual = BuildTypedPredicateDelegate(plan.Residual, outputRowType);
            if (residual is null) return null;
            var filtered = InvokeFilter(ctx.Builder, outputRowType, joined, residual);
            node = new TypedNode(outputRowType, outputSchema, filtered, PartitionKey: joinPartition);
        }

        return node;
    }

    /// <summary>
    /// Compiles a <see cref="UnionAllPlan"/>: each branch is typed
    /// recursively, then folded pairwise with
    /// <see cref="LinearOperators.Union{TRow,TWeight}"/>. All branches
    /// must share the same emitted row type (the resolver enforces
    /// schema alignment, so this is a sanity check on the typed
    /// fingerprint cache).
    /// </summary>
    private static TypedNode? CompileUnionAll(UnionAllPlan plan, CompileContext ctx)
    {
        if (plan.Branches.Count == 0) return null;

        var first = TryCompileNode(plan.Branches[0], ctx);
        if (first is null) return null;

        var resultStream = first.Stream;
        for (var i = 1; i < plan.Branches.Count; i++)
        {
            var branch = TryCompileNode(plan.Branches[i], ctx);
            if (branch is null) return null;
            // Same TRow on every branch — the resolver's schema
            // alignment + TypedRowEmitter's fingerprint cache makes
            // this the common case, but guard explicitly.
            if (branch.RowType != first.RowType) return null;
            resultStream = InvokeUnion(ctx.Builder, first.RowType, resultStream, branch.Stream);
        }

        return new TypedNode(first.RowType, plan.Schema, resultStream);
    }

    /// <summary>
    /// Compiles a <see cref="RecursiveCtePlan"/>: hybrid path that
    /// keeps the operator structural-internal (it evaluates its
    /// base / recursive subplans via <see cref="BatchPlanEvaluator"/>
    /// over <see cref="StructuralRow"/> traces) and wraps the output
    /// stream with a typed <see cref="LinearOperators.MapRows{TIn,TOut,TWeight}"/>
    /// so the rest of the typed pipeline can compose with it.
    /// <para>
    /// Only available in boundary-adapter mode — relies on
    /// <see cref="CompileContext.StructuralScans"/> for the external
    /// table deltas the recursive op needs. Standalone mode bails
    /// (its tests can use the structural-fallback path instead).
    /// </para>
    /// </summary>
    private static TypedNode? CompileRecursiveCte(RecursiveCtePlan plan, CompileContext ctx)
    {
        if (ctx.StructuralScans is null) return null;

        // The output schema must be typed-eligible for the
        // downstream MapRows projection to succeed.
        var outputRowType = TypedRowEmitter.EmitRowType(plan.Schema);
        if (outputRowType is null) return null;

        // Walk both subplans to collect (name, schema) for every
        // external base table the body references. ScanPlan carries
        // its own Schema, so no separate plumbing required.
        // Unsupported plan shapes inside the body throw at runtime
        // via BatchPlanEvaluator — same surface as structural.
        var externalSchemas = new Dictionary<string, Schema>(StringComparer.Ordinal);
        try
        {
            CollectExternalScans(plan.BasePlan, plan.SelfRef, externalSchemas);
            CollectExternalScans(plan.RecursivePlan, plan.SelfRef, externalSchemas);
        }
        catch (InvalidOperationException)
        {
            // Body contains a shape we can't statically walk — let
            // structural handle it.
            return null;
        }

        // Wire the existing structural delta streams; bail if any
        // referenced table isn't in the boundary's scan map.
        var externalStreams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
        foreach (var (name, _) in externalSchemas)
        {
            if (!ctx.StructuralScans.TryGetValue(name, out var stream)) return null;
            externalStreams[name] = stream;
        }

        // Compile the recursive body structurally onto the shared nested-circuit
        // (fixpoint) primitive — the same construction the structural path uses,
        // including the spine import-trace family when in spine mode.
        var structuralOutput = PlanToCircuit.CompileRecursiveCteFixpoint(
            ctx.Builder,
            plan,
            externalStreams,
            externalSchemas,
            StructuralRowCodec.Instance,
            ctx.SnapshotCodecs,
            PlanToCircuit.RecursiveSpineConfig(ctx.Options));

        // Lift the structural output to typed via MapRows. Cost is
        // paid only at the boundary between the recursive op and the
        // surrounding typed pipeline — the recursive iteration loop
        // inside the op stays all-structural.
        var lifter = BuildStructuralToTypedDelegate(plan.Schema, outputRowType);
        var typedOutput = InvokeMapRows(
            ctx.Builder, typeof(StructuralRow), outputRowType, structuralOutput, lifter);
        return new TypedNode(outputRowType, plan.Schema, typedOutput);
    }

    /// <summary>
    /// Walks a recursive-CTE subplan and records every external base
    /// table (ScanPlan) along with its declared schema. Mirrors the
    /// structural <c>CollectRecursiveExternalTables</c> but captures
    /// schemas too.
    /// </summary>
    private static void CollectExternalScans(
        LogicalPlan plan, CteRef selfRef, Dictionary<string, Schema> result)
    {
        switch (plan)
        {
            case ScanPlan s:
                if (!result.ContainsKey(s.TableName)) result[s.TableName] = s.Schema;
                break;
            case CteScanPlan c:
                if (ReferenceEquals(c.Cte, selfRef)) return; // self-reference, skip
                throw new InvalidOperationException(
                    $"recursive CTE body cannot reference other CTE '{c.Cte.Name}' in v1");
            case FilterPlan f:
                CollectExternalScans(f.Input, selfRef, result); break;
            case ProjectPlan p:
                CollectExternalScans(p.Input, selfRef, result); break;
            case JoinPlan j:
                CollectExternalScans(j.Left, selfRef, result);
                CollectExternalScans(j.Right, selfRef, result); break;
            case UnionAllPlan u:
                foreach (var b in u.Branches) CollectExternalScans(b, selfRef, result); break;
            default:
                throw new InvalidOperationException(
                    $"{plan.GetType().Name} is not supported inside a recursive CTE body");
        }
    }

    /// <summary>
    /// Compiles a <see cref="CteScanPlan"/>: looks up the typed
    /// stream for the referenced CTE in <see cref="CompileContext.CteCache"/>,
    /// compiling the underlying plan on first reference. Schema
    /// qualifiers may differ per scan but the row CLR fingerprint
    /// matches (TypedRowEmitter ignores qualifier), so the cached
    /// stream is reusable. Returns a fresh TypedNode that pairs the
    /// cached stream with the CteScanPlan's own schema, matching the
    /// structural behaviour at <c>PlanToCircuit.cs:243</c>.
    /// </summary>
    private static TypedNode? CompileCteScan(CteScanPlan plan, CompileContext ctx)
    {
        if (!ctx.CteCache.TryGetValue(plan.Cte, out var cached))
        {
            var compiled = TryCompileNode(plan.Cte.Plan, ctx);
            if (compiled is null) return null;
            cached = compiled;
            ctx.CteCache[plan.Cte] = cached;
        }

        return new TypedNode(cached.RowType, plan.Schema, cached.Stream);
    }

    /// <summary>
    /// Compiles a <see cref="ScalarSubqueryJoinPlan"/>: each
    /// subquery contributes one nullable column to the outer plan
    /// via an <see cref="StatefulOperators.IncrementalLeftJoin{TKey,TLeft,TRight,TOut,TWeight}"/>
    /// on a constant zero-column unit key. Empty subquery → NULL
    /// column for every outer row (the null-pad combine); 1-row
    /// subquery → broadcast its single value; &gt;1-row at runtime
    /// is undefined (resolver doesn't validate). Mirrors structural
    /// <c>CompileScalarSubqueryJoin</c> at <c>PlanToCircuit.cs:687</c>.
    /// </summary>
    private static TypedNode? CompileScalarSubqueryJoin(ScalarSubqueryJoinPlan plan, CompileContext ctx)
    {
        // Broadcast join on a unit key: every outer row must see the whole
        // subquery result. Sharding splits the subquery rows across workers, so a
        // replica would join against only its fraction — wrong. A correct parallel
        // form needs the subquery side replicated (broadcast), not sharded; refuse
        // for now.
        if (ctx.Workers > 1) return null;

        var current = TryCompileNode(plan.Input, ctx);
        if (current is null) return null;

        // Unit key: zero-column emitted struct. Same emitted type
        // every iteration (TypedRowEmitter caches by fingerprint, and
        // the empty Type[] hashes uniformly), so all outer rows and
        // all subquery rows land in the same bucket — broadcast.
        var unitSchema = Schema.Empty;
        var unitKeyType = TypedRowEmitter.EmitRowType(unitSchema);
        if (unitKeyType is null) return null;
        var noIndices = Array.Empty<int>();

        var soFarSchema = plan.Input.Schema;

        foreach (var subPlan in plan.Subqueries)
        {
            var sub = TryCompileNode(subPlan, ctx);
            if (sub is null) return null;

            var newSchema = soFarSchema.Concat(subPlan.Schema);
            var newRowType = TypedRowEmitter.EmitRowType(newSchema);
            if (newRowType is null) return null;

            var outerExtractor = BuildKeyExtractorDelegate(
                current.RowType, unitKeyType, unitSchema, noIndices);
            var subExtractor = BuildKeyExtractorDelegate(
                sub.RowType, unitKeyType, unitSchema, noIndices);
            if (outerExtractor is null || subExtractor is null) return null;

            var combine = BuildJoinCombineDelegate(
                unitKeyType, current.RowType, sub.RowType, newRowType,
                newSchema, leftCount: soFarSchema.Count, rightCount: subPlan.Schema.Count);
            var nullPad = BuildNullPadCombineDelegate(
                unitKeyType, current.RowType, newRowType,
                newSchema, preservedSideOffset: 0, preservedSideCount: soFarSchema.Count);
            if (nullPad is null) return null;

            var outerIndexed = InvokeGroupProject(
                ctx.Builder, current.RowType, unitKeyType, current.Stream, outerExtractor);
            var subIndexed = InvokeGroupProject(
                ctx.Builder, sub.RowType, unitKeyType, sub.Stream, subExtractor);

            var outerCodec = BuildAdaptedIndexedCodec(
                ctx.SnapshotCodecs, unitSchema, soFarSchema, unitKeyType, current.RowType);
            var subCodec = BuildAdaptedIndexedCodec(
                ctx.SnapshotCodecs, unitSchema, subPlan.Schema, unitKeyType, sub.RowType);

            var joined = EmitLeftJoin(
                ctx, unitKeyType, current.RowType, sub.RowType, newRowType,
                outerIndexed, subIndexed, combine, nullPad, outerCodec, subCodec);

            current = new TypedNode(newRowType, newSchema, joined);
            soFarSchema = newSchema;
        }

        return new TypedNode(current.RowType, plan.Schema, current.Stream);
    }

    /// <summary>
    /// Compiles a <see cref="DifferencePlan"/>: <c>left − right</c>
    /// via <see cref="LinearOperators.Difference{TRow,TWeight}"/>.
    /// Pure linear, no codec needed. Generated by EXCEPT (see
    /// <c>Resolver.BuildExcept</c> — <c>(a EXCEPT b) =
    /// Distinct(a) − Intersect(a, b)</c>).
    /// </summary>
    private static TypedNode? CompileDifference(DifferencePlan plan, CompileContext ctx)
    {
        // left − right is per-row key-sensitive: a row's left and right copies must
        // co-locate, which whole-row sharding alone doesn't guarantee once the
        // sub-plans carry their own (key-based) exchanges. Refuse parallel rather
        // than risk a wrong difference; a both-sides whole-row exchange could make
        // this safe later (mirroring DISTINCT).
        if (ctx.Workers > 1) return null;

        var left = TryCompileNode(plan.Left, ctx);
        if (left is null) return null;
        var right = TryCompileNode(plan.Right, ctx);
        if (right is null) return null;
        if (left.RowType != right.RowType) return null;

        var output = InvokeDifference(ctx.Builder, left.RowType, left.Stream, right.Stream);
        return new TypedNode(left.RowType, plan.Schema, output);
    }

    /// <summary>
    /// Compiles a <see cref="DistinctPlan"/>: dedup the input Z-set
    /// to set semantics via the stateful
    /// <see cref="StatefulOperators.Distinct{TKey,TWeight}"/>. The
    /// typed-key snapshot codec adapter mirrors the join/aggregate
    /// codec wiring so on-disk snapshots stay byte-compatible with
    /// the structural path.
    /// </summary>
    private static TypedNode? CompileDistinct(DistinctPlan plan, CompileContext ctx)
    {
        var inner = TryCompileNode(plan.Input, ctx);
        if (inner is null) return null;

        var snapshotCodec = BuildAdaptedZSetCodec(
            ctx.SnapshotCodecs, plan.Schema, inner.RowType);

        // Parallel: re-shard by the whole row (DISTINCT's key) so identical rows
        // co-locate and dedup on one worker; the gather unions disjoint rows.
        var distinctStream = inner.Stream;
        if (ctx.Workers > 1)
        {
            distinctStream = InvokeExchange(
                ctx.Builder, inner.RowType, distinctStream, BuildRowHashPartition(inner.RowType));
        }

        var output = EmitDistinct(ctx, inner.RowType, distinctStream, snapshotCodec);
        return new TypedNode(inner.RowType, plan.Schema, output);
    }

    /// <summary>
    /// Compiles a <see cref="TopKPlan"/> (<c>ORDER BY … LIMIT/OFFSET</c>). Each
    /// sort-key expression is lowered against the emitted row struct and boxed
    /// to <c>object?</c>, then a <see cref="SortKeyComparer{TRow}"/> (tie-broken
    /// by <c>Comparer&lt;TRow&gt;.Default</c> — the emitted struct's
    /// <c>IComparable&lt;TSelf&gt;</c>) drives the incremental TOP-K operator.
    /// Falls back to structural if any sort expression is outside the typed
    /// expression compiler's scope.
    /// </summary>
    private static TypedNode? CompileTopK(TopKPlan plan, CompileContext ctx)
    {
        // A global (un-partitioned) TOP-K is a single ranking over the whole
        // input; sharding + Z-set gather can't reconstruct it (each worker would
        // keep its own local top-k, and Plus would union up to W×k rows rather
        // than re-ranking). Refuse the parallel compile so the caller falls back,
        // rather than emit a silently-wrong circuit.
        if (ctx.Workers > 1) return null;

        var inner = TryCompileNode(plan.Input, ctx);
        if (inner is null) return null;

        var rowType = inner.RowType;
        var count = plan.SortKeys.Count;
        var extractors = new Delegate[count];
        var descending = new bool[count];
        var nullsFirst = new bool[count];
        for (var i = 0; i < count; i++)
        {
            var sortKey = plan.SortKeys[i];
            var rowParam = Expression.Parameter(rowType, "row");
            var body = TypedExpressionCompiler.TryBuildInto(sortKey.Expression, rowParam);
            if (body is null) return null; // unsupported sort expr — structural fallback

            // Box to object? — Convert lifts value types and Nullable<T>
            // correctly (a null Nullable boxes to a null reference), matching
            // the SortKeyComparer's null handling.
            var boxed = Expression.Convert(body, typeof(object));
            var funcType = typeof(Func<,>).MakeGenericType(rowType, typeof(object));
            extractors[i] = Expression.Lambda(funcType, boxed, rowParam).Compile();
            descending[i] = sortKey.Descending;
            nullsFirst[i] = sortKey.NullsFirst;
        }

        var snapshotCodec = BuildAdaptedZSetCodec(ctx.SnapshotCodecs, plan.Schema, rowType);
        var output = InvokeTopK(
            ctx.Builder, rowType, inner.Stream, extractors, descending, nullsFirst,
            plan.Offset ?? 0, plan.Limit, snapshotCodec);
        return new TypedNode(rowType, inner.Schema, output);
    }

    /// <summary>
    /// Compiles a <see cref="PartitionedTopKPlan"/> (windowed
    /// <c>ROW_NUMBER</c> / <c>RANK</c> / <c>DENSE_RANK</c> &lt;= k). Mirrors
    /// <see cref="CompileTopK"/> for the sort keys and additionally lowers the
    /// PARTITION BY expressions to boxed extractors that build a
    /// <see cref="StructuralRow"/> partition key. Falls back to the structural
    /// compile if any sort or partition expression is outside the typed
    /// expression compiler's scope.
    /// </summary>
    private static TypedNode? CompilePartitionedTopK(PartitionedTopKPlan plan, CompileContext ctx)
    {
        var inner = TryCompileNode(plan.Input, ctx);
        if (inner is null) return null;

        var rowType = inner.RowType;

        var count = plan.SortKeys.Count;
        var sortExtractors = new Delegate[count];
        var descending = new bool[count];
        var nullsFirst = new bool[count];
        for (var i = 0; i < count; i++)
        {
            var sortKey = plan.SortKeys[i];
            if (BuildBoxedExtractor(sortKey.Expression, rowType) is not { } ex) return null;
            sortExtractors[i] = ex;
            descending[i] = sortKey.Descending;
            nullsFirst[i] = sortKey.NullsFirst;
        }

        var partitionExtractors = new Delegate[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            if (BuildBoxedExtractor(plan.PartitionKeys[i], rowType) is not { } ex) return null;
            partitionExtractors[i] = ex;
        }

        // Parallel: co-locate each partition's rows on one worker so the
        // per-partition ranking is global, not per-shard. Exchange by the same
        // PARTITION BY key the operator ranks within. A partition key outside the
        // stable-hash type surface refuses the parallel compile (falls back).
        var topKInput = inner.Stream;
        if (ctx.Workers > 1 && plan.PartitionKeys.Count > 0)
        {
            if (BuildExprListHashPartition(rowType, plan.PartitionKeys) is not { } partition)
            {
                return null;
            }

            topKInput = InvokeExchange(ctx.Builder, rowType, inner.Stream, partition);
        }
        else if (ctx.Workers > 1)
        {
            // No PARTITION BY ⇒ a single global ranking; same hazard as CompileTopK.
            return null;
        }

        var snapshotCodec = BuildAdaptedZSetCodec(ctx.SnapshotCodecs, plan.Schema, rowType);
        var output = InvokePartitionedTopK(
            ctx.Builder, rowType, topKInput, sortExtractors, descending, nullsFirst,
            partitionExtractors, plan.Function, plan.Limit, snapshotCodec);
        return new TypedNode(rowType, inner.Schema, output);
    }

    /// <summary>
    /// Compiles a <see cref="WindowAggregatePlan"/> — <c>agg(x) OVER (PARTITION BY
    /// p ORDER BY o RANGE …)</c> — to the row-opaque
    /// <see cref="PartitionedWindowAggregateOp{TInRow,TAgg,TOutRow,TKey}"/>, with a
    /// struct-fusing widener that appends the aggregate columns onto each base row.
    /// The output schema is <c>[input cols…, agg cols…]</c> (= <c>plan.Schema</c>).
    /// </summary>
    /// <remarks>
    /// Falls back to structural (returns <c>null</c>) for a window with no
    /// PARTITION BY (a single global partition, off the parallel target) or an
    /// aggregate the typed aggregators don't cover (MIN/MAX — see
    /// <see cref="BuildTypedAggregator"/>).
    /// <para>No GC frontier is wired here: <see cref="PlanToCircuit"/> gates the
    /// typed path off whenever LATENESS / a temporal filter is present (the only
    /// sources a window frontier can derive from), so a typed window's frontier is
    /// always null anyway — the structural path keeps every GC-eligible query.
    /// Parallel: the operator partitions by the PARTITION BY key, so an exchange on
    /// that key co-locates each partition on one worker (same shape + stable-hash
    /// hazard as <see cref="CompilePartitionedTopK"/>).</para>
    /// </remarks>
    private static TypedNode? CompileWindowAggregate(WindowAggregatePlan plan, CompileContext ctx)
    {
        // The no-PARTITION-BY case is a single global window; leave it structural.
        if (plan.PartitionKeys.Count == 0) return null;

        var inner = TryCompileNode(plan.Input, ctx);
        if (inner is null) return null;
        var rowType = inner.RowType;

        // Partition-key extractors (boxed), as in CompilePartitionedTopK.
        var partitionExtractors = new Delegate[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            if (BuildBoxedExtractor(plan.PartitionKeys[i], rowType) is not { } ex) return null;
            partitionExtractors[i] = ex;
        }

        // ORDER BY key (boxed), if any — its presence + a bounded frame select the
        // operator's three frame shapes (see PartitionedWindowAggregateOp).
        Delegate? orderExtractor = null;
        var descending = false;
        var nullsFirst = false;
        if (plan.OrderKey is { } sk)
        {
            if (BuildBoxedExtractor(sk.Expression, rowType) is not { } ex) return null;
            orderExtractor = ex;
            descending = sk.Descending;
            nullsFirst = sk.NullsFirst;
        }

        // Typed composite aggregator + emitted TAgg ([agg cols…]).
        if (BuildTypedComposite(plan.Aggregates, rowType) is not { } built) return null;
        var (composite, aggRowType) = built;

        // Widened output row = [input cols…, agg cols…] = plan.Schema. Reuse the
        // (TKey, TAgg) → TFinal flatten builder with the *full input row* in the
        // first slot (input col count = the "key" count).
        var outRowType = TypedRowEmitter.EmitRowType(plan.Schema);
        if (outRowType is null) return null;
        var flatten = BuildAggregateFlattenDelegate(
            rowType, aggRowType, outRowType, plan.Schema,
            plan.Input.Schema.Count, plan.Aggregates.Count);

        // Parallel: co-locate each partition on one worker. A partition key
        // outside the stable-hash surface refuses the parallel compile (falls back).
        var opInput = inner.Stream;
        if (ctx.Workers > 1)
        {
            if (BuildExprListHashPartition(rowType, plan.PartitionKeys) is not { } partition) return null;
            opInput = InvokeExchange(ctx.Builder, rowType, inner.Stream, partition);
        }

        // The per-partition integrated input stores base rows (plan.Input.Schema).
        var snapshotCodec = BuildAdaptedZSetCodec(ctx.SnapshotCodecs, plan.Input.Schema, rowType);

        var output = InvokePartitionedWindowAggregate(
            ctx.Builder, rowType, aggRowType, outRowType, opInput,
            partitionExtractors, orderExtractor, descending, nullsFirst,
            plan.Frame?.Preceding, composite, flatten, snapshotCodec);

        // Output is partitioned by the PARTITION BY columns. The widen carries the
        // input columns through at their original indices (they precede the agg
        // cols), so a bare-column partition key keeps its indices; an expression
        // key can't be named as columns ⇒ no inherited partition (downstream
        // re-exchanges if it needs one — sound, just an extra shuffle).
        int[]? outPartition = null;
        if (ctx.Workers > 1)
        {
            var indices = new int[plan.PartitionKeys.Count];
            var allColumns = true;
            for (var i = 0; i < plan.PartitionKeys.Count; i++)
            {
                if (plan.PartitionKeys[i] is ResolvedColumn rc)
                {
                    indices[i] = rc.Index;
                }
                else
                {
                    allColumns = false;
                    break;
                }
            }

            if (allColumns) outPartition = indices;
        }

        return new TypedNode(outRowType, plan.Schema, output, PartitionKey: outPartition);
    }

    /// <summary>
    /// Compiles a <see cref="WindowOffsetPlan"/> — <c>LAG/LEAD/FIRST_VALUE/
    /// LAST_VALUE(expr) OVER (PARTITION BY p ORDER BY o)</c> — to the row-opaque
    /// <see cref="PartitionedOffsetOp{TInRow,TOutRow,TKey}"/>. Mirrors
    /// <see cref="CompileWindowAggregate"/>: boxed partition extractors + a parallel
    /// exchange on the PARTITION BY key, a boxed order extractor + full-row tiebreak
    /// comparer, and a hybrid widener that reads the base columns from the typed row
    /// and casts each boxed offset value onto the appended columns. Output schema is
    /// <c>[input cols…, offset cols…]</c> (= <c>plan.Schema</c>). Falls back to
    /// structural (returns <c>null</c>) for a window with no PARTITION BY or an
    /// offset value the typed expression compiler can't lower.
    /// </summary>
    private static TypedNode? CompileWindowOffset(WindowOffsetPlan plan, CompileContext ctx)
    {
        // The no-PARTITION-BY case is a single global ordering; leave it structural.
        if (plan.PartitionKeys.Count == 0) return null;

        var inner = TryCompileNode(plan.Input, ctx);
        if (inner is null) return null;
        var rowType = inner.RowType;

        var partitionExtractors = new Delegate[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            if (BuildBoxedExtractor(plan.PartitionKeys[i], rowType) is not { } ex) return null;
            partitionExtractors[i] = ex;
        }

        // The ORDER BY keys (at least one) — a total order with a full-row tiebreak
        // makes the positional offsets deterministic. Any comparable key type is
        // allowed, and keys may mix ASC/DESC.
        var sortExtractors = new Delegate[plan.OrderKeys.Count];
        var sortDescending = new bool[plan.OrderKeys.Count];
        var sortNullsFirst = new bool[plan.OrderKeys.Count];
        for (var i = 0; i < plan.OrderKeys.Count; i++)
        {
            if (BuildBoxedExtractor(plan.OrderKeys[i].Expression, rowType) is not { } ex) return null;
            sortExtractors[i] = ex;
            sortDescending[i] = plan.OrderKeys[i].Descending;
            sortNullsFirst[i] = plan.OrderKeys[i].NullsFirst;
        }

        // Per-function value extractors (read from the positionally-selected row).
        var n = plan.Functions.Count;
        var valueExtractors = new Delegate[n];
        var kinds = new OffsetKind[n];
        var offsets = new long[n];
        var defaults = new object?[n];
        for (var i = 0; i < n; i++)
        {
            var fn = plan.Functions[i];
            if (BuildBoxedExtractor(fn.Value, rowType) is not { } ex) return null;
            valueExtractors[i] = ex;
            kinds[i] = fn.Kind;
            offsets[i] = fn.Offset;
            defaults[i] = fn.Default;
        }

        // Widened output row = [input cols…, offset cols…] = plan.Schema.
        var outRowType = TypedRowEmitter.EmitRowType(plan.Schema);
        if (outRowType is null) return null;
        var widen = BuildOffsetWidenDelegate(rowType, outRowType, plan.Schema, plan.Input.Schema.Count);

        // Parallel: co-locate each partition on one worker.
        var opInput = inner.Stream;
        if (ctx.Workers > 1)
        {
            if (BuildExprListHashPartition(rowType, plan.PartitionKeys) is not { } partition) return null;
            opInput = InvokeExchange(ctx.Builder, rowType, inner.Stream, partition);
        }

        var snapshotCodec = BuildAdaptedZSetCodec(ctx.SnapshotCodecs, plan.Input.Schema, rowType);

        var output = InvokePartitionedOffset(
            ctx.Builder, rowType, outRowType, opInput, partitionExtractors, sortExtractors,
            sortDescending, sortNullsFirst,
            valueExtractors, kinds, offsets, defaults, widen, snapshotCodec);

        // Output partitioned by the PARTITION BY columns, carried through at their
        // original indices (they precede the offset cols). Bare-column keys keep
        // their indices; an expression key yields no inherited partition.
        int[]? outPartition = null;
        if (ctx.Workers > 1)
        {
            var indices = new int[plan.PartitionKeys.Count];
            var allColumns = true;
            for (var i = 0; i < plan.PartitionKeys.Count; i++)
            {
                if (plan.PartitionKeys[i] is ResolvedColumn rc)
                {
                    indices[i] = rc.Index;
                }
                else
                {
                    allColumns = false;
                    break;
                }
            }

            if (allColumns) outPartition = indices;
        }

        return new TypedNode(outRowType, plan.Schema, output, PartitionKey: outPartition);
    }

    /// <summary>Lower a resolved expression to a compiled <c>Func&lt;TRow,
    /// object?&gt;</c> over the emitted row struct (boxed so a null
    /// <c>Nullable&lt;T&gt;</c> becomes a null reference, matching
    /// <see cref="SortKeyComparer{TRow}"/>). Returns null when the expression is
    /// outside the typed expression compiler's scope.</summary>
    private static Delegate? BuildBoxedExtractor(ResolvedExpression expr, Type rowType)
    {
        var rowParam = Expression.Parameter(rowType, "row");
        var body = TypedExpressionCompiler.TryBuildInto(expr, rowParam);
        if (body is null) return null;

        var boxed = Expression.Convert(body, typeof(object));
        var funcType = typeof(Func<,>).MakeGenericType(rowType, typeof(object));
        return Expression.Lambda(funcType, boxed, rowParam).Compile();
    }

    /// <summary>
    /// Compiles an <see cref="AggregatePlan"/>. The output stream
    /// carries a row schema of <c>[group-key cols..., agg-result cols...]</c>
    /// (matching <see cref="AggregatePlan.Schema"/>); the typed path
    /// supports COUNT(*) / COUNT(col) / SUM / AVG on int/long/float/double.
    /// MIN/MAX and decimal-result aggregates fall back to structural.
    /// </summary>
    private static TypedNode? CompileAggregate(AggregatePlan plan, CompileContext ctx)
    {
        // Bare-column group keys take the fast path: typed field reads, and the
        // exchange-elision check below needs their column indices. Any other
        // scalar key expression (CAST(ts AS DATE), a + b, …) takes the general
        // path — each key is lowered over the input row into a synthetic key row,
        // mirroring the structural compiler. `allColumns` gates the two.
        var allColumns = true;
        var groupIndices = new int[plan.GroupKeys.Count];
        for (var i = 0; i < plan.GroupKeys.Count; i++)
        {
            if (plan.GroupKeys[i] is ResolvedColumn rc)
            {
                groupIndices[i] = rc.Index;
            }
            else
            {
                allColumns = false;
            }
        }

        var inner = TryCompileNode(plan.Input, ctx);
        if (inner is null) return null;

        // Nullable group-key columns flow through naturally: the
        // emitted TKey carries Nullable<T> fields for nullable key
        // cols (TypedRowEmitter, N1.1), and its IEquatable<TKey> /
        // GetHashCode implementations use Nullable<T>'s HasValue +
        // Value comparison + HashCode.Add<Nullable<T>> — which
        // gives the SQL GROUP BY semantics SQL wants (one bucket
        // for NULL, distinct from any non-null value). No special
        // handling needed at this layer.

        // TKey from the group keys. For bare columns it is the matching
        // input-column subset; for expression keys a synthetic schema of the key
        // expressions' resolved types ($gk{i}). Same schema-fingerprint sharing.
        Schema keySchema;
        if (allColumns)
        {
            keySchema = inner.Schema.SubsetByIndex(groupIndices);
        }
        else
        {
            var keyColumns = new SchemaColumn[plan.GroupKeys.Count];
            for (var i = 0; i < plan.GroupKeys.Count; i++)
            {
                keyColumns[i] = new SchemaColumn("$gk" + i, plan.GroupKeys[i].Type);
            }

            keySchema = new Schema(keyColumns);
        }

        var keyRowType = TypedRowEmitter.EmitRowType(keySchema);
        if (keyRowType is null) return null;

        var keyExtractor = allColumns
            ? BuildKeyExtractorDelegate(inner.RowType, keyRowType, keySchema, groupIndices)
            : BuildExprKeyExtractorDelegate(inner.RowType, keyRowType, keySchema, plan.GroupKeys);
        if (keyExtractor is null) return null;

        // The typed composite aggregator (TAgg = [agg-result cols...]) — shared
        // verbatim with the window-aggregate path.
        if (BuildTypedComposite(plan.Aggregates, inner.RowType) is not { } built)
        {
            return null;
        }

        var (composite, aggRowType) = built;

        // Parallel: re-shard by the group key so every row of a group co-locates
        // on one worker — that worker then computes the group's complete
        // aggregate, and the gather just unions the disjoint groups. No-op for W=1.
        // Elide the shuffle when the input is already partitioned by a subset of
        // the group key (e.g. a join on a.id feeding GROUP BY a.id, a.category):
        // every group already lives wholly on one worker, so a re-shard would just
        // move rows to the same place. This removes a full Barrier(W) + ZSet
        // rebuild over the (often large) input — the dominant exchange tax.
        // When a shuffle is needed, fuse it with the re-index (ExchangeIndex)
        // so the gather builds the indexed Z-set directly. When the input is
        // already co-partitioned (elided) there is no gather, so a plain
        // GroupProject over the inherited stream is all that's needed.
        // Elision is sound only for bare-column keys, where IsKeySubset can prove
        // the input is already partitioned by a subset of the group key. An
        // expression key always re-shards.
        var needsExchange = ctx.Workers > 1
            && !(allColumns && inner.PartitionKey is { } p && IsKeySubset(p, groupIndices));

        object indexed;
        if (needsExchange)
        {
            Delegate partition;
            try
            {
                partition = BuildKeyHashPartition(inner.RowType, keyRowType, keyExtractor);
            }
            catch (NotSupportedException)
            {
                // A key type outside the stable-hash surface (e.g. INTERVAL) can't
                // be sharded deterministically — refuse the parallel compile; the
                // structural path handles it.
                return null;
            }

            indexed = InvokeExchangeIndex(
                ctx.Builder, inner.RowType, keyRowType, inner.Stream, partition, keyExtractor);
        }
        else
        {
            indexed = InvokeGroupProject(
                ctx.Builder, inner.RowType, keyRowType, inner.Stream, keyExtractor);
        }

        // IncrementalAggregate<TKey, TIn, TAgg>(indexed, composite)
        // Output: Stream<ZSet<(TKey, TAgg), Z64>>
        var aggSnapshotCodec = BuildAdaptedIndexedCodec(
            ctx.SnapshotCodecs, keySchema, inner.Schema, keyRowType, inner.RowType);
        var aggregated = EmitAggregate(
            ctx, keyRowType, inner.RowType, aggRowType, indexed, composite,
            aggSnapshotCodec);

        // Flatten (TKey, TAgg) -> TFinal: TFinal columns are
        // [group-key columns..., agg columns...]. We rely on the
        // resolver's plan.Schema for the final shape; any extra
        // aggregator columns past plan.Schema's count are dropped by
        // the caller's wrapping Project (the structural path does the
        // same — see PlanToCircuit notes). Phase N4: the per-call
        // result nullability computed in TypedAggregateResultType
        // already matches plan.Schema for the agg slots (resolver
        // marks SUM/AVG nullable, we keep that iff arg is nullable);
        // group-key cols carry through with their own nullability
        // — which is non-null here because the group-key gate
        // above rejects nullable keys.
        var finalSchema = plan.Schema;
        var finalRowType = TypedRowEmitter.EmitRowType(finalSchema);
        if (finalRowType is null) return null;

        var flattenDelegate = BuildAggregateFlattenDelegate(
            keyRowType, aggRowType, finalRowType, finalSchema,
            keySchema.Count, plan.Aggregates.Count);

        var pairType = typeof(ValueTuple<,>).MakeGenericType(keyRowType, aggRowType);
        var flatStream = InvokeMapRows(ctx.Builder, pairType, finalRowType, aggregated, flattenDelegate);

        // Output is partitioned by the group key, which the flatten lays out as
        // the first keySchema.Count columns — whether we exchanged or inherited a
        // finer partition, equal group keys are co-located (see PartitionKey).
        var outPartition = ctx.Workers > 1 ? BuildIota(keySchema.Count) : null;
        return new TypedNode(finalRowType, finalSchema, flatStream, PartitionKey: outPartition);
    }

    /// <summary><c>[0, 1, …, n-1]</c>.</summary>
    private static int[] BuildIota(int n)
    {
        var a = new int[n];
        for (var i = 0; i < n; i++) a[i] = i;
        return a;
    }

    /// <summary>
    /// Per-call output nullability used to size the <c>TAgg</c>
    /// schema slot. Independent of <see cref="AggregateCall.ResultType"/>
    /// (which the resolver always marks nullable per SQL spec for
    /// SUM/AVG/MIN/MAX) because the typed pipeline's linear-emission
    /// gate guarantees every emitted row corresponds to a non-empty
    /// group — so SUM/AVG/MIN/MAX over a non-null arg cannot be NULL
    /// in the emitted row. For nullable args the nullable-arg
    /// aggregator variants (<see cref="TypedSumLongNullableAggregator{TIn}"/>
    /// etc.) can still emit NULL when every contributing row has a
    /// NULL arg, so the slot stays nullable.
    /// </summary>
    private static SqlType TypedAggregateResultType(AggregateCall call)
    {
        var argNullable = call.Argument is not null && call.Argument.Type.Nullable;
        return call.Kind switch
        {
            AggregateKind.CountStar => call.ResultType.WithNullable(false),
            AggregateKind.Count => call.ResultType.WithNullable(false),
            AggregateKind.CountDistinct => call.ResultType.WithNullable(false),
            AggregateKind.ApproxCountDistinct => call.ResultType.WithNullable(false),
            // APPROX_PERCENTILE returns NULL for an empty / all-NULL group, so
            // the emitted slot is always nullable (like MIN/MAX).
            AggregateKind.ApproxPercentile => call.ResultType.WithNullable(true),
            AggregateKind.Sum => call.ResultType.WithNullable(argNullable),
            AggregateKind.Avg => call.ResultType.WithNullable(argNullable),
            // MIN/MAX is *always* nullable in the emitted slot, even on
            // a non-null arg: the linear gate emits on net non-zero
            // weight, which can be the negative-weight case (no
            // positive-weight row, MIN/MAX returns NULL). Well-formed
            // DBSP streams shouldn't reach that state, but the aggregator
            // is correct under any Z-set input — the schema slot must
            // match.
            AggregateKind.Min or AggregateKind.Max => call.ResultType.WithNullable(true),
            _ => call.ResultType,
        };
    }

    /// <summary>
    /// Builds a typed <c>TypedSqlAggregator&lt;TIn&gt;</c> for one
    /// <see cref="AggregateCall"/>. Returns <c>null</c> if the call
    /// falls outside scope (MIN/MAX, or an argument expression the
    /// typed expression compiler can't lower).
    /// </summary>
    private static object? BuildTypedAggregator(AggregateCall call, Type inputRowType)
    {
        switch (call.Kind)
        {
            case AggregateKind.CountStar:
                {
                    var open = typeof(TypedCountStarAggregator<>);
                    var closed = open.MakeGenericType(inputRowType);
                    return Activator.CreateInstance(closed);
                }

            case AggregateKind.Count:
                {
                    if (call.Argument is null) return null;
                    // For non-null arg, COUNT(col) reduces to COUNT(*).
                    // For nullable arg, build a TypedCountNullableAggregator
                    // with an isPresent extractor.
                    if (!call.Argument.Type.Nullable)
                    {
                        var inParam = Expression.Parameter(inputRowType, "in");
                        if (TypedExpressionCompiler.TryBuildInto(call.Argument, inParam) is null)
                            return null;
                        var open = typeof(TypedCountStarAggregator<>);
                        var closed = open.MakeGenericType(inputRowType);
                        return Activator.CreateInstance(closed);
                    }

                    var presence = BuildHasValueDelegate(call.Argument, inputRowType);
                    if (presence is null) return null;
                    var openNull = typeof(TypedCountNullableAggregator<>);
                    var closedNull = openNull.MakeGenericType(inputRowType);
                    return Activator.CreateInstance(closedNull, presence);
                }

            case AggregateKind.CountDistinct:
                return BuildCountDistinctAggregator(call, inputRowType);

            case AggregateKind.ApproxCountDistinct:
                return BuildApproxCountDistinctAggregator(call, inputRowType);

            case AggregateKind.ApproxPercentile:
                return BuildApproxPercentileAggregator(call, inputRowType);

            case AggregateKind.Sum:
                return BuildSumAggregator(call, inputRowType);

            case AggregateKind.Avg:
                return BuildAvgAggregator(call, inputRowType);

            case AggregateKind.Min:
                return BuildMinMaxAggregator(call, inputRowType, wantMin: true);

            case AggregateKind.Max:
                return BuildMinMaxAggregator(call, inputRowType, wantMin: false);

            default:
                return null;
        }
    }

    private static object? BuildMinMaxAggregator(AggregateCall call, Type inputRowType, bool wantMin)
    {
        if (call.Argument is null) return null;
        var argExtract = BuildTypedScalarDelegate(call.Argument, inputRowType, out var resultClr);
        if (argExtract is null) return null;

        var argNullable = call.Argument.Type.Nullable;
        var underlyingClr = TypedExpressionCompiler.IsNullable(resultClr)
            ? TypedExpressionCompiler.UnderlyingType(resultClr)
            : resultClr;

        // T must be a value type (every numeric / Decimal128 /
        // Utf8String / temporal we extract is a struct) and
        // IComparable<T>. Fall back if either condition fails — the
        // structural path still handles ref-type comparable args.
        if (!underlyingClr.IsValueType) return null;
        var icmp = typeof(IComparable<>).MakeGenericType(underlyingClr);
        if (!icmp.IsAssignableFrom(underlyingClr)) return null;

        if (argNullable)
        {
            var open = typeof(TypedSqlMinMaxNullableAggregator<,>);
            var closed = open.MakeGenericType(inputRowType, underlyingClr);
            return Activator.CreateInstance(closed, argExtract, wantMin);
        }

        var openNonNull = typeof(TypedSqlMinMaxAggregator<,>);
        var closedNonNull = openNonNull.MakeGenericType(inputRowType, underlyingClr);
        return Activator.CreateInstance(closedNonNull, argExtract, wantMin);
    }

    private static object? BuildCountDistinctAggregator(AggregateCall call, Type inputRowType)
    {
        if (call.Argument is null) return null;

        // Same boxing-extractor strategy as APPROX_COUNT_DISTINCT: lower the
        // argument to its CLR type then box (a no-value Nullable<T> boxes to
        // null = SQL NULL). One non-generic extractor covers every arg type,
        // including reference-typed args like Utf8String.
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(call.Argument, inParam);
        if (built is null) return null;

        var boxed = Expression.Convert(built, typeof(object));
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(object));
        var argExtract = Expression.Lambda(delegateType, boxed, inParam).Compile();

        var open = typeof(TypedCountDistinctAggregator<>);
        var closed = open.MakeGenericType(inputRowType);
        return Activator.CreateInstance(closed, argExtract);
    }

    private static object? BuildApproxCountDistinctAggregator(AggregateCall call, Type inputRowType)
    {
        if (call.Argument is null) return null;

        // Build a boxing extractor: the argument lowered to its CLR type, then
        // converted to object. A no-value Nullable<T> boxes to a null
        // reference, which the aggregator reads as SQL NULL — so one
        // non-generic extractor handles both nullable and non-nullable args
        // (and reference-typed args like Utf8String) uniformly.
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(call.Argument, inParam);
        if (built is null) return null;

        var boxed = Expression.Convert(built, typeof(object));
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(object));
        var argExtract = Expression.Lambda(delegateType, boxed, inParam).Compile();

        var open = typeof(TypedApproxCountDistinctAggregator<>);
        var closed = open.MakeGenericType(inputRowType);
        return Activator.CreateInstance(closed, argExtract);
    }

    private static object? BuildApproxPercentileAggregator(AggregateCall call, Type inputRowType)
    {
        if (call.Argument is null) return null;

        // Same boxing-extractor strategy as APPROX_COUNT_DISTINCT: the argument
        // lowered to its CLR type then boxed (a no-value Nullable<T> boxes to
        // null = SQL NULL). The aggregator unboxes and folds into its sketch.
        // INTERVAL has no Arrow row-emit, so this path is reached only for
        // numeric / DATE / TIMESTAMP results (an INTERVAL agg slot makes the
        // typed row emit fail upstream and the whole compile falls back to
        // structural — like INTERVAL arithmetic already does).
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(call.Argument, inParam);
        if (built is null) return null;

        var boxed = Expression.Convert(built, typeof(object));
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(object));
        var argExtract = Expression.Lambda(delegateType, boxed, inParam).Compile();

        var argType = call.Argument.Type;
        if (DdSketchSupport.IsExactQuantileType(argType))
        {
            var exactOpen = typeof(TypedExactQuantileAggregator<>);
            var exactClosed = exactOpen.MakeGenericType(inputRowType);
            return Activator.CreateInstance(
                exactClosed,
                argExtract,
                call.Fraction!.Value,
                call.Discrete,
                DdSketchSupport.ExactToKey(argType),
                DdSketchSupport.ExactFromKey(argType),
                argType.ClrType);
        }

        Func<object, double> toDouble;
        Func<double, object> fromDouble;
        if (argType is SqlIntervalType iv)
        {
            toDouble = DdSketchSupport.IntervalToDouble(iv.Qualifier);
            fromDouble = DdSketchSupport.IntervalFromDouble(iv.Qualifier);
        }
        else
        {
            toDouble = DdSketchSupport.NumericToDouble(argType);
            fromDouble = DdSketchSupport.DoubleIdentity;
        }

        var open = typeof(TypedApproxPercentileAggregator<>);
        var closed = open.MakeGenericType(inputRowType);
        return Activator.CreateInstance(
            closed, argExtract, call.Fraction!.Value, toDouble, fromDouble, argType.ClrType);
    }

    /// <summary>
    /// Compiles an "is the extracted arg non-null?" predicate. The
    /// arg expression is built through <see cref="TypedExpressionCompiler"/>
    /// (which returns either <c>Nullable&lt;T&gt;</c> for nullable
    /// value types or a reference type that can be <c>null</c>). The
    /// returned delegate is <c>Func&lt;TIn, bool&gt;</c>; used by
    /// COUNT(nullable col) and (later) by MIN/MAX. Returns
    /// <c>null</c> if the arg is outside the expression compiler's
    /// scope.
    /// </summary>
    private static Delegate? BuildHasValueDelegate(ResolvedExpression arg, Type inputRowType)
    {
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(arg, inParam);
        if (built is null) return null;

        Expression body;
        if (TypedExpressionCompiler.IsNullable(built.Type))
        {
            body = Expression.Property(built, nameof(Nullable<int>.HasValue));
        }
        else if (!built.Type.IsValueType)
        {
            body = Expression.NotEqual(built, Expression.Constant(null, built.Type));
        }
        else
        {
            // Definite-value arg (non-nullable struct) — always present.
            body = Expression.Constant(true);
        }

        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(bool));
        return Expression.Lambda(delegateType, body, inParam).Compile();
    }

    private static object? BuildSumAggregator(AggregateCall call, Type inputRowType)
    {
        if (call.Argument is null) return null;
        var argNullable = call.Argument.Type.Nullable;
        var argExtract = BuildTypedScalarDelegate(call.Argument, inputRowType, out var resultClr);
        if (argExtract is null) return null;
        var underlyingClr = TypedExpressionCompiler.IsNullable(resultClr)
            ? TypedExpressionCompiler.UnderlyingType(resultClr)
            : resultClr;

        // Map the SQL result type to the running accumulator type.
        if (call.ResultType is SqlBigintType)
        {
            // Widen int → long if necessary. For nullable args, widen
            // inside the Nullable<T> envelope (T → long becomes
            // Nullable<T> → Nullable<long>).
            var targetClr = argNullable ? typeof(long?) : typeof(long);
            var widened = resultClr == targetClr
                ? argExtract
                : RecompileWidened(call.Argument, inputRowType, targetClr);
            if (widened is null) return null;
            var open = argNullable
                ? typeof(TypedSumLongNullableAggregator<>)
                : typeof(TypedSumLongAggregator<>);
            var closed = open.MakeGenericType(inputRowType);
            return Activator.CreateInstance(closed, widened);
        }

        if (call.ResultType is SqlDoubleType)
        {
            var targetClr = argNullable ? typeof(double?) : typeof(double);
            var widened = resultClr == targetClr
                ? argExtract
                : RecompileWidened(call.Argument, inputRowType, targetClr);
            if (widened is null) return null;
            var open = argNullable
                ? typeof(TypedSumDoubleNullableAggregator<>)
                : typeof(TypedSumDoubleAggregator<>);
            var closed = open.MakeGenericType(inputRowType);
            return Activator.CreateInstance(closed, widened);
        }

        if (call.ResultType is SqlDecimalType)
        {
            // Arg must already be Decimal128 — SUM only widens within
            // the decimal family by adjusting precision, not by
            // promoting non-decimal operands.
            if (underlyingClr != typeof(Clast.DatabaseDecimal.Values.Decimal128)) return null;
            var open = argNullable
                ? typeof(TypedSumDecimalNullableAggregator<>)
                : typeof(TypedSumDecimalAggregator<>);
            var closed = open.MakeGenericType(inputRowType);
            return Activator.CreateInstance(closed, argExtract);
        }

        return null;
    }

    private static object? BuildAvgAggregator(AggregateCall call, Type inputRowType)
    {
        if (call.Argument is null) return null;
        var argNullable = call.Argument.Type.Nullable;
        var argExtract = BuildTypedScalarDelegate(call.Argument, inputRowType, out var resultClr);
        if (argExtract is null) return null;
        var underlyingClr = TypedExpressionCompiler.IsNullable(resultClr)
            ? TypedExpressionCompiler.UnderlyingType(resultClr)
            : resultClr;

        if (call.ResultType is SqlDecimalType)
        {
            if (underlyingClr != typeof(Clast.DatabaseDecimal.Values.Decimal128)) return null;
            var open = argNullable
                ? typeof(TypedAvgDecimalNullableAggregator<>)
                : typeof(TypedAvgDecimalAggregator<>);
            var closed = open.MakeGenericType(inputRowType);
            return Activator.CreateInstance(closed, argExtract);
        }

        // Non-decimal AVG always produces a double-running-total
        // aggregator. Recompile the arg expression widened to double
        // (or double? for nullable args).
        var avgTargetClr = argNullable ? typeof(double?) : typeof(double);
        var widened = resultClr == avgTargetClr
            ? argExtract
            : RecompileWidened(call.Argument, inputRowType, avgTargetClr);
        if (widened is null) return null;
        var avgOpen = argNullable
            ? typeof(TypedAvgDoubleNullableAggregator<>)
            : typeof(TypedAvgDoubleAggregator<>);
        var avgClosed = avgOpen.MakeGenericType(inputRowType);
        return Activator.CreateInstance(avgClosed, widened);
    }

    /// <summary>
    /// Lowers <paramref name="expr"/> against an input row parameter
    /// and returns the compiled <c>Func&lt;TIn, T&gt;</c>; also
    /// reports the expression's CLR result type. Returns <c>null</c>
    /// if the expression is outside scope.
    /// </summary>
    private static Delegate? BuildTypedScalarDelegate(
        ResolvedExpression expr, Type inputRowType, out Type resultClr)
    {
        resultClr = null!;
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(expr, inParam);
        if (built is null) return null;
        resultClr = built.Type;
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, built.Type);
        return Expression.Lambda(delegateType, built, inParam).Compile();
    }

    /// <summary>
    /// Like <see cref="BuildTypedScalarDelegate"/> but widens the
    /// compiled body to <paramref name="targetClr"/> via
    /// <see cref="Expression.Convert(Expression, Type)"/>. Used for
    /// SUM(INT)→long and AVG(*)→double accumulator widening.
    /// </summary>
    private static Delegate? RecompileWidened(
        ResolvedExpression expr, Type inputRowType, Type targetClr)
    {
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(expr, inParam);
        if (built is null) return null;
        var widened = built.Type == targetClr ? built : Expression.Convert(built, targetClr);
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, targetClr);
        return Expression.Lambda(delegateType, widened, inParam).Compile();
    }

    /// <summary>
    /// Builds the <c>TypedCompositeAggregator&lt;TIn, TAgg&gt;</c> (boxed) for a
    /// list of aggregate calls over <paramref name="inputRowType"/>, returning it
    /// alongside the emitted <c>TAgg</c> row type (schema
    /// <c>[$agg0, $agg1, …]</c>). Returns <c>null</c> if any call is out of the
    /// typed aggregators' scope (MIN/MAX or an un-lowerable argument) or the agg
    /// row type can't be emitted — the caller then falls back to structural.
    /// Shared by the grouped-aggregate and window-aggregate compile paths.
    /// </summary>
    private static (object Composite, Type AggRowType)? BuildTypedComposite(
        IReadOnlyList<AggregateCall> aggregates, Type inputRowType)
    {
        // Per-aggregate-call schema for TAgg, parallel to the call list. (The
        // resolver may collect more AggregateCalls than it surfaces in a plan's
        // Schema, so we always size TAgg from the calls themselves.)
        var aggColumns = new SchemaColumn[aggregates.Count];
        for (var i = 0; i < aggregates.Count; i++)
        {
            aggColumns[i] = new SchemaColumn("$agg" + i, TypedAggregateResultType(aggregates[i]));
        }

        var aggSchema = new Schema(aggColumns);
        var aggRowType = TypedRowEmitter.EmitRowType(aggSchema);
        if (aggRowType is null) return null;

        // Build the per-aggregator typed objects.
        var aggregators = new object[aggregates.Count];
        for (var i = 0; i < aggregates.Count; i++)
        {
            var agg = BuildTypedAggregator(aggregates[i], inputRowType);
            if (agg is null) return null;
            aggregators[i] = agg;
        }

        var aggArray = CastAggregatorArray(inputRowType, aggregators);

        // packResults returns object; wrap into a typed Func<object?[], TAgg>.
        var packResults = TypedRowEmitter.BuildBoxedFactory(aggSchema)!;
        var packParam = Expression.Parameter(typeof(object?[]), "vs");
        var packCall = Expression.Convert(
            Expression.Invoke(Expression.Constant(packResults), packParam), aggRowType);
        var packDelegateType = typeof(Func<,>).MakeGenericType(typeof(object?[]), aggRowType);
        var typedPack = Expression.Lambda(packDelegateType, packCall, packParam).Compile();

        // new TypedCompositeAggregator<TIn, TAgg>(aggArray, typedPack)
        var compositeClosed = typeof(TypedCompositeAggregator<,>).MakeGenericType(inputRowType, aggRowType);
        var composite = Activator.CreateInstance(compositeClosed, aggArray, typedPack)!;
        return (composite, aggRowType);
    }

    /// <summary>
    /// Repackage <c>object[]</c> of <c>TypedSqlAggregator&lt;TIn&gt;</c>
    /// instances as a strongly-typed <c>TypedSqlAggregator&lt;TIn&gt;[]</c>
    /// so the ctor's array reference is variance-safe.
    /// </summary>
    private static Array CastAggregatorArray(Type inputRowType, object[] aggregators)
    {
        var elementType = typeof(TypedSqlAggregator<>).MakeGenericType(inputRowType);
        var arr = Array.CreateInstance(elementType, aggregators.Length);
        for (var i = 0; i < aggregators.Length; i++)
        {
            arr.SetValue(aggregators[i], i);
        }

        return arr;
    }

    /// <summary>
    /// Builds <c>(ValueTuple&lt;TKey, TAgg&gt; p) =&gt;
    /// new TFinal(p.Item1.F0, ..., p.Item1.F{k-1}, p.Item2.F0, ..., p.Item2.F{a-1})</c>.
    /// </summary>
    private static Delegate BuildAggregateFlattenDelegate(
        Type keyRowType, Type aggRowType, Type finalRowType, Schema finalSchema,
        int keyCount, int aggCount)
    {
        var ctorParamTypes = new Type[finalSchema.Count];
        for (var i = 0; i < finalSchema.Count; i++)
        {
            var field = finalRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted final aggregate row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = finalRowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException(
                "emitted final aggregate row missing typed-fields ctor");

        var pairType = typeof(ValueTuple<,>).MakeGenericType(keyRowType, aggRowType);
        var pairParam = Expression.Parameter(pairType, "p");
        var keyExpr = Expression.Field(pairParam, "Item1");
        var aggExpr = Expression.Field(pairParam, "Item2");

        var args = new Expression[finalSchema.Count];
        var emittedSlots = Math.Min(finalSchema.Count, keyCount + aggCount);
        for (var i = 0; i < emittedSlots; i++)
        {
            Expression source;
            if (i < keyCount)
            {
                var field = keyRowType.GetField("F" + i)!;
                source = Expression.Field(keyExpr, field);
            }
            else
            {
                var field = aggRowType.GetField("F" + (i - keyCount))!;
                source = Expression.Field(aggExpr, field);
            }

            args[i] = source.Type == ctorParamTypes[i]
                ? source
                : Expression.Convert(source, ctorParamTypes[i]);
        }

        // Pad any extra slots (shouldn't happen with the resolver, but
        // guard the ctor arity match).
        for (var i = emittedSlots; i < finalSchema.Count; i++)
        {
            args[i] = Expression.Default(ctorParamTypes[i]);
        }

        var delegateType = typeof(Func<,>).MakeGenericType(pairType, finalRowType);
        return Expression.Lambda(delegateType, Expression.New(ctor, args), pairParam).Compile();
    }

    /// <summary>
    /// Builds the offset-op widener <c>(TInRow row, object?[] vals) =&gt; new
    /// TOutRow(row.F0, …, row.F{inputCount-1}, (T)vals[0], …)</c>: the first
    /// <paramref name="inputCount"/> output columns are read from the typed base
    /// row, and each appended offset column unboxes/casts the corresponding boxed
    /// value from <c>vals</c> (a null boxes to an absent <c>Nullable&lt;T&gt;</c> /
    /// null reference, matching the resolver's nullable offset-result columns).
    /// </summary>
    private static Delegate BuildOffsetWidenDelegate(
        Type inRowType, Type outRowType, Schema outSchema, int inputCount)
    {
        var ctorParamTypes = new Type[outSchema.Count];
        for (var i = 0; i < outSchema.Count; i++)
        {
            var field = outRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted offset output row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = outRowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException("emitted offset output row missing typed-fields ctor");

        var rowParam = Expression.Parameter(inRowType, "row");
        var valsParam = Expression.Parameter(typeof(object?[]), "vals");

        var args = new Expression[outSchema.Count];
        for (var i = 0; i < outSchema.Count; i++)
        {
            Expression source;
            if (i < inputCount)
            {
                var field = inRowType.GetField("F" + i)!;
                source = Expression.Field(rowParam, field);
            }
            else
            {
                // (FieldType) vals[i - inputCount] — unbox/cast the boxed offset value.
                source = Expression.Convert(
                    Expression.ArrayIndex(valsParam, Expression.Constant(i - inputCount)),
                    ctorParamTypes[i]);
            }

            args[i] = source.Type == ctorParamTypes[i]
                ? source
                : Expression.Convert(source, ctorParamTypes[i]);
        }

        var delegateType = typeof(Func<,,>).MakeGenericType(inRowType, typeof(object?[]), outRowType);
        return Expression.Lambda(delegateType, Expression.New(ctor, args), rowParam, valsParam).Compile();
    }

    // ---- Helpers: identity / projection / predicate ----

    /// <summary>
    /// True when <paramref name="projections"/> keeps every input column by
    /// identity (each input column index appears as a bare <see cref="ResolvedColumn"/>
    /// somewhere in the output) — so the projection is injective and the output row
    /// determines the input row. Computed/renamed columns are ignored; they neither
    /// help nor hurt. Used to decide whether a projection preserves shard
    /// disjointness — must stay sound (never report true when a column is dropped).
    /// </summary>
    private static bool RetainsAllInputColumns(IReadOnlyList<ProjectionItem> projections, Schema inputSchema)
    {
        var inputCount = inputSchema.Count;
        if (inputCount == 0)
        {
            return true;
        }

        var seen = new bool[inputCount];
        var remaining = inputCount;
        foreach (var item in projections)
        {
            if (item.Expression is ResolvedColumn rc
                && (uint)rc.Index < (uint)inputCount
                && !seen[rc.Index])
            {
                seen[rc.Index] = true;
                if (--remaining == 0)
                {
                    return true;
                }
            }
        }

        return remaining == 0;
    }

    private static bool IsIdentityProjection(
        IReadOnlyList<ProjectionItem> projections, Schema inputSchema, Schema outputSchema)
    {
        if (projections.Count != inputSchema.Count) return false;
        if (outputSchema.Count != inputSchema.Count) return false;
        for (var i = 0; i < projections.Count; i++)
        {
            if (projections[i].Expression is not ResolvedColumn rc || rc.Index != i)
            {
                return false;
            }
        }

        return true;
    }

    private static Delegate? BuildTypedProjectionDelegate(
        Type inputRowType, Type outputRowType, Schema outputSchema,
        IReadOnlyList<ProjectionItem> projections)
    {
        // Param types come from the emitted typed row's fields so they
        // match Nullable<T> vs T correctly per N1.1's emitter.
        var ctorParamTypes = new Type[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            var field = outputRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted output row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = outputRowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException(
                "emitted output row missing typed-fields ctor");

        var inParam = Expression.Parameter(inputRowType, "in");
        var args = new Expression[projections.Count];
        for (var i = 0; i < projections.Count; i++)
        {
            var built = TypedExpressionCompiler.TryBuildInto(projections[i].Expression, inParam);
            if (built is null) return null;

            // Phase N3: the projection's output column may be
            // nullable (Nullable<T>) while the expression result is
            // raw T — e.g. a ResolvedColumn read from an
            // aggregate's stripped-non-nullable output that flows
            // into a wrapping projection whose resolver-marked
            // column type is nullable. Convert(T, Nullable<T>) does
            // the implicit lift. The opposite direction
            // (Convert(Nullable<T>, T)) is an unsafe unwrap; we
            // refuse to compile it here since a runtime null would
            // throw, and instead fall back to structural.
            if (TypedExpressionCompiler.IsNullable(built.Type)
                && !TypedExpressionCompiler.IsNullable(ctorParamTypes[i]))
            {
                return null;
            }

            args[i] = built.Type == ctorParamTypes[i]
                ? built
                : Expression.Convert(built, ctorParamTypes[i]);
        }

        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, outputRowType);
        return Expression.Lambda(delegateType, Expression.New(ctor, args), inParam).Compile();
    }

    private static Delegate? BuildTypedPredicateDelegate(
        ResolvedExpression predicate, Type inputRowType)
    {
        if (predicate.Type is not SqlBooleanType) return null;

        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(predicate, inParam);
        if (built is null) return null;

        Expression body;
        if (built.Type == typeof(bool))
        {
            body = built;
        }
        else if (built.Type == typeof(bool?))
        {
            // SQL WHERE: TRUE → keep, FALSE → drop, NULL → drop.
            // Nullable<bool>.GetValueOrDefault() returns the
            // underlying value if HasValue and the default(bool)
            // (= false) otherwise — exactly the coercion WHERE
            // wants, with no HasValue branch in the IL.
            var getValueOrDefault = typeof(bool?).GetMethod(
                nameof(Nullable<bool>.GetValueOrDefault), Type.EmptyTypes)!;
            body = Expression.Call(built, getValueOrDefault);
        }
        else
        {
            return null;
        }

        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(bool));
        return Expression.Lambda(delegateType, body, inParam).Compile();
    }

    // ---- Helpers: join key extraction and row combine ----

    /// <summary>
    /// Builds <c>(TIn in) =&gt; new TKey(in.F{idx[0]}, in.F{idx[1]}, ...)</c>.
    /// </summary>
    private static Delegate? BuildKeyExtractorDelegate(
        Type inputRowType, Type keyRowType, Schema keySchema, int[] indices)
    {
        var ctorParamTypes = new Type[keySchema.Count];
        for (var i = 0; i < keySchema.Count; i++)
        {
            var field = keyRowType.GetField("F" + i);
            if (field is null) return null;
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = keyRowType.GetConstructor(ctorParamTypes);
        if (ctor is null) return null;

        var inParam = Expression.Parameter(inputRowType, "in");
        var args = new Expression[indices.Length];
        for (var i = 0; i < indices.Length; i++)
        {
            var field = inputRowType.GetField("F" + indices[i]);
            if (field is null) return null;
            args[i] = Expression.Field(inParam, field);
        }

        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, keyRowType);
        return Expression.Lambda(delegateType, Expression.New(ctor, args), inParam).Compile();
    }

    /// <summary>
    /// Like <see cref="BuildKeyExtractorDelegate"/> but for arbitrary scalar
    /// group-key expressions (e.g. <c>CAST(ts AS DATE)</c>): each key is lowered
    /// over the input row via the typed expression compiler and packed into the
    /// synthetic key row. Returns <c>null</c> if any key falls outside the typed
    /// expression surface (the caller then refuses the typed/parallel compile and
    /// the structural path runs it).
    /// </summary>
    private static Delegate? BuildExprKeyExtractorDelegate(
        Type inputRowType, Type keyRowType, Schema keySchema, IReadOnlyList<ResolvedExpression> keys)
    {
        var ctorParamTypes = new Type[keySchema.Count];
        for (var i = 0; i < keySchema.Count; i++)
        {
            var field = keyRowType.GetField("F" + i);
            if (field is null) return null;
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = keyRowType.GetConstructor(ctorParamTypes);
        if (ctor is null) return null;

        var inParam = Expression.Parameter(inputRowType, "in");
        var args = new Expression[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            var body = TypedExpressionCompiler.TryBuildInto(keys[i], inParam);
            if (body is null) return null;

            // Align the lowered value to the emitted key field type (e.g. lift a
            // non-null value into Nullable<T> when the field is nullable).
            if (body.Type != ctorParamTypes[i])
            {
                body = Expression.Convert(body, ctorParamTypes[i]);
            }

            args[i] = body;
        }

        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, keyRowType);
        return Expression.Lambda(delegateType, Expression.New(ctor, args), inParam).Compile();
    }

    /// <summary>
    /// Builds the inner-join combine
    /// <c>(TKey _, TLeft l, TRight r) =&gt; new TOut(l.F0, ..., l.F{lN-1}, r.F0, ..., r.F{rN-1})</c>.
    /// The output schema is the resolver-built side-by-side concatenation
    /// (left columns first, then right). The TKey arg is ignored since
    /// the key columns are already present in the left row.
    /// </summary>
    private static Delegate BuildJoinCombineDelegate(
        Type keyRowType, Type leftRowType, Type rightRowType, Type outputRowType,
        Schema outputSchema, int leftCount, int rightCount)
    {
        // Ctor param types come from the emitted typed row's fields so
        // Nullable<T> vs T match per N1.1's emitter (same fix as in
        // BuildTypedProjectionDelegate / BuildStructuralToTypedDelegate).
        var ctorParamTypes = new Type[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            var field = outputRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted output row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = outputRowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException(
                "emitted output row missing typed-fields ctor");

        var keyParam = Expression.Parameter(keyRowType, "k");
        var leftParam = Expression.Parameter(leftRowType, "l");
        var rightParam = Expression.Parameter(rightRowType, "r");

        var args = new Expression[leftCount + rightCount];
        for (var i = 0; i < leftCount; i++)
        {
            var field = leftRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("missing left field F" + i);
            args[i] = Expression.Field(leftParam, field);
            if (args[i].Type != ctorParamTypes[i])
                args[i] = Expression.Convert(args[i], ctorParamTypes[i]);
        }

        for (var j = 0; j < rightCount; j++)
        {
            var field = rightRowType.GetField("F" + j)
                ?? throw new InvalidOperationException("missing right field F" + j);
            var arg = (Expression)Expression.Field(rightParam, field);
            if (arg.Type != ctorParamTypes[leftCount + j])
                arg = Expression.Convert(arg, ctorParamTypes[leftCount + j]);
            args[leftCount + j] = arg;
        }

        var delegateType = typeof(Func<,,,>).MakeGenericType(
            keyRowType, leftRowType, rightRowType, outputRowType);
        return Expression.Lambda(
            delegateType, Expression.New(ctor, args),
            keyParam, leftParam, rightParam).Compile();
    }

    /// <summary>
    /// Same as <see cref="BuildJoinCombineDelegate"/> but with the
    /// physical preserved/probed sides swapped — used for RIGHT JOIN
    /// where the right stream is fed to IncrementalLeftJoin as the
    /// preserved side. The delegate's first arg is the preserved
    /// (right-table) row and its second arg is the probed (left-table)
    /// row, but the output column layout stays [left cols, right cols]
    /// (user-written order), so the body pulls fields from the second
    /// param for the left slots and from the first param for the right
    /// slots.
    /// </summary>
    private static Delegate BuildSwappedJoinCombineDelegate(
        Type keyRowType, Type leftRowType, Type rightRowType, Type outputRowType,
        Schema outputSchema, int leftCount, int rightCount)
    {
        var ctorParamTypes = new Type[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            var field = outputRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted output row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = outputRowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException(
                "emitted output row missing typed-fields ctor");

        var keyParam = Expression.Parameter(keyRowType, "k");
        // Preserved side (first non-key arg of IncrementalLeftJoin's
        // combine) is the right-table row; probed is the left-table row.
        var preservedRightParam = Expression.Parameter(rightRowType, "r");
        var probedLeftParam = Expression.Parameter(leftRowType, "l");

        var args = new Expression[leftCount + rightCount];
        for (var i = 0; i < leftCount; i++)
        {
            var field = leftRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("missing left field F" + i);
            args[i] = Expression.Field(probedLeftParam, field);
            if (args[i].Type != ctorParamTypes[i])
                args[i] = Expression.Convert(args[i], ctorParamTypes[i]);
        }

        for (var j = 0; j < rightCount; j++)
        {
            var field = rightRowType.GetField("F" + j)
                ?? throw new InvalidOperationException("missing right field F" + j);
            var arg = (Expression)Expression.Field(preservedRightParam, field);
            if (arg.Type != ctorParamTypes[leftCount + j])
                arg = Expression.Convert(arg, ctorParamTypes[leftCount + j]);
            args[leftCount + j] = arg;
        }

        var delegateType = typeof(Func<,,,>).MakeGenericType(
            keyRowType, rightRowType, leftRowType, outputRowType);
        return Expression.Lambda(
            delegateType, Expression.New(ctor, args),
            keyParam, preservedRightParam, probedLeftParam).Compile();
    }

    /// <summary>
    /// Builds the LEFT-JOIN null-pad combine
    /// <c>(TKey _, TPreserved p) =&gt; new TOut(...)</c> where output
    /// slots in <c>[preservedSideOffset, preservedSideOffset + preservedSideCount)</c>
    /// come from the preserved row's fields and every other output
    /// slot is initialised to <c>default</c> (Nullable&lt;T&gt; with
    /// HasValue=false, or null reference). The resolver marks every
    /// non-preserved-side column as nullable on the output schema —
    /// if some emitted output field doesn't accept the null padding
    /// (i.e. its CLR type is a non-nullable value type), the build
    /// fails and the caller falls back to structural.
    /// </summary>
    private static Delegate? BuildNullPadCombineDelegate(
        Type keyRowType, Type preservedRowType, Type outputRowType,
        Schema outputSchema, int preservedSideOffset, int preservedSideCount)
    {
        var ctorParamTypes = new Type[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            var field = outputRowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted output row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = outputRowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException(
                "emitted output row missing typed-fields ctor");

        var keyParam = Expression.Parameter(keyRowType, "k");
        var preservedParam = Expression.Parameter(preservedRowType, "p");

        var args = new Expression[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            var inPreservedRange = i >= preservedSideOffset
                && i < preservedSideOffset + preservedSideCount;
            if (inPreservedRange)
            {
                var field = preservedRowType.GetField("F" + (i - preservedSideOffset))
                    ?? throw new InvalidOperationException(
                        "missing preserved field F" + (i - preservedSideOffset));
                args[i] = Expression.Field(preservedParam, field);
                if (args[i].Type != ctorParamTypes[i])
                    args[i] = Expression.Convert(args[i], ctorParamTypes[i]);
            }
            else
            {
                // Padding slot. Must be either Nullable<T> or a ref
                // type — otherwise we can't represent SQL NULL here.
                var target = ctorParamTypes[i];
                if (target.IsValueType
                    && !(target.IsGenericType
                         && target.GetGenericTypeDefinition() == typeof(Nullable<>)))
                {
                    return null;
                }

                args[i] = Expression.Default(target);
            }
        }

        var delegateType = typeof(Func<,,>).MakeGenericType(
            keyRowType, preservedRowType, outputRowType);
        return Expression.Lambda(
            delegateType, Expression.New(ctor, args),
            keyParam, preservedParam).Compile();
    }

    // ---- Snapshot codec adapters ----

    /// <summary>
    /// Builds an <c>IIndexedZSetTraceCodec&lt;TKey, TValue, Z64&gt;</c>
    /// for the typed pipeline, wrapping the structural
    /// <see cref="ISqlSnapshotCodecs.CreateIndexedZSetTraceCodec"/>
    /// in a <see cref="TypedIndexedZSetTraceCodecAdapter{TKey,TValue}"/>.
    /// Returns <c>null</c> if <paramref name="snapshotCodecs"/> is
    /// <c>null</c> (snapshotting disabled). Built reflectively because
    /// the adapter's generics close over the emitted struct types.
    /// </summary>
    private static object? BuildAdaptedIndexedCodec(
        ISqlSnapshotCodecs? snapshotCodecs,
        Schema keySchema, Schema valueSchema,
        Type keyRowType, Type valueRowType)
    {
        if (snapshotCodecs is null) return null;

        var structuralCodec = snapshotCodecs.CreateIndexedZSetTraceCodec(keySchema, valueSchema);

        var keyToStruct = BuildTypedToStructuralDelegate(
            keyRowType, keySchema, StructuralRowCodec.Instance);
        var valToStruct = BuildTypedToStructuralDelegate(
            valueRowType, valueSchema, StructuralRowCodec.Instance);
        var structToKey = BuildStructuralToTypedDelegate(keySchema, keyRowType);
        var structToVal = BuildStructuralToTypedDelegate(valueSchema, valueRowType);

        var adapterOpen = typeof(TypedIndexedZSetTraceCodecAdapter<,>);
        var adapterClosed = adapterOpen.MakeGenericType(keyRowType, valueRowType);
        return Activator.CreateInstance(adapterClosed,
            structuralCodec, keyToStruct, valToStruct, structToKey, structToVal)!;
    }

    /// <summary>
    /// Builds an <c>IZSetTraceCodec&lt;TKey, Z64&gt;</c> for the
    /// typed pipeline, wrapping the structural
    /// <see cref="ISqlSnapshotCodecs.CreateZSetTraceCodec"/> in a
    /// <see cref="TypedZSetTraceCodecAdapter{TKey}"/>. Single-key
    /// counterpart to <see cref="BuildAdaptedIndexedCodec"/>; used
    /// by the typed DISTINCT operator.
    /// </summary>
    private static object? BuildAdaptedZSetCodec(
        ISqlSnapshotCodecs? snapshotCodecs,
        Schema keySchema, Type keyRowType)
    {
        if (snapshotCodecs is null) return null;

        var structuralCodec = snapshotCodecs.CreateZSetTraceCodec(keySchema);
        var keyToStruct = BuildTypedToStructuralDelegate(
            keyRowType, keySchema, StructuralRowCodec.Instance);
        var structToKey = BuildStructuralToTypedDelegate(keySchema, keyRowType);

        var adapterOpen = typeof(TypedZSetTraceCodecAdapter<>);
        var adapterClosed = adapterOpen.MakeGenericType(keyRowType);
        return Activator.CreateInstance(adapterClosed,
            structuralCodec, keyToStruct, structToKey)!;
    }

    // ---- Boundary adapters (structural ↔ typed) ----

    /// <summary>
    /// Builds <c>(StructuralRow r) =&gt; new TRow((T0)r[0], (T1)r[1], ...)</c>
    /// as a <see cref="Delegate"/> of type <c>Func&lt;StructuralRow, TRow&gt;</c>.
    /// Used at the input boundary in adapter mode to lift the
    /// caller's structural-row input streams into the typed pipeline.
    /// </summary>
    private static Delegate BuildStructuralToTypedDelegate(Schema schema, Type rowType)
    {
        // Param types come from the emitted typed row's fields so
        // Nullable<T> vs T match per N1.1's emitter — same fix as in
        // BuildTypedProjectionDelegate.
        var ctorParamTypes = new Type[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            var field = rowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted row missing field F" + i);
            ctorParamTypes[i] = field.FieldType;
        }

        var ctor = rowType.GetConstructor(ctorParamTypes)
            ?? throw new InvalidOperationException(
                "emitted row missing typed-fields ctor");

        var rowParam = Expression.Parameter(typeof(StructuralRow), "r");
        var indexer = typeof(StructuralRow).GetMethod("get_Item", [typeof(int)])!;

        var args = new Expression[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            var idx = Expression.Constant(i, typeof(int));
            var boxed = Expression.Call(rowParam, indexer, idx);
            args[i] = BuildCastFromBoxed(boxed, ctorParamTypes[i]);
        }

        var delegateType = typeof(Func<,>).MakeGenericType(typeof(StructuralRow), rowType);
        return Expression.Lambda(delegateType, Expression.New(ctor, args), rowParam).Compile();
    }

    /// <summary>
    /// Builds <c>(targetType)boxed</c> with appropriate handling of
    /// Nullable&lt;T&gt; and null reference values:
    /// <list type="bullet">
    /// <item>For value-type T (non-nullable): direct
    /// <see cref="Expression.Unbox"/>.</item>
    /// <item>For <c>Nullable&lt;T&gt;</c>: branch on boxed == null,
    /// returning <c>default(Nullable&lt;T&gt;)</c> for null and
    /// <c>new Nullable&lt;T&gt;((T)boxed)</c> otherwise.</item>
    /// <item>For reference type T: <see cref="Expression.Convert"/>
    /// (handles null naturally).</item>
    /// </list>
    /// </summary>
    private static Expression BuildCastFromBoxed(Expression boxed, Type targetType)
    {
        if (TypedExpressionCompiler.IsNullable(targetType))
        {
            var underlying = TypedExpressionCompiler.UnderlyingType(targetType);
            var nullableCtor = targetType.GetConstructor([underlying])!;
            // boxed == null ? default(Nullable<T>) : new Nullable<T>((T)boxed)
            return Expression.Condition(
                Expression.Equal(boxed, Expression.Constant(null, typeof(object))),
                Expression.Default(targetType),
                Expression.New(nullableCtor, Expression.Unbox(boxed, underlying)));
        }

        if (targetType.IsValueType)
        {
            return Expression.Unbox(boxed, targetType);
        }

        return Expression.Convert(boxed, targetType);
    }

    /// <summary>
    /// Adds a final <c>MapRows&lt;TRow, StructuralRow, Z64&gt;</c> that
    /// projects each typed output row to a <see cref="StructuralRow"/>
    /// (built via <paramref name="codec"/>). The typed pipeline's
    /// internal stages keep their typed representation; only the
    /// final emission pays this conversion.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> AdaptTypedToStructural(
        CircuitBuilder builder, TypedNode top, IRowCodec<StructuralRow> codec)
    {
        var projection = BuildTypedToStructuralDelegate(top.RowType, top.Schema, codec);
        var streamObj = InvokeMapRows(builder, top.RowType, typeof(StructuralRow), top.Stream, projection);
        return (Stream<ZSet<StructuralRow, Z64>>)streamObj;
    }

    /// <summary>
    /// Builds <c>(TRow r) =&gt; BuildStructuralRow(codec, schema, new object?[] { (object?)r.F0, ... })</c>.
    /// Field reads are typed; one per-field box happens per output
    /// row, plus one array allocation. Goes through a shim because
    /// <see cref="IRowCodec{TRow}.BuildRow"/>'s second arg is a
    /// <c>ReadOnlySpan</c> (ref struct), which can't be constructed
    /// by an expression-tree <see cref="Expression.Call"/>.
    /// </summary>
    private static Delegate BuildTypedToStructuralDelegate(
        Type rowType, Schema schema, IRowCodec<StructuralRow> codec)
    {
        // Lazy-boxing output boundary (docs/design-row-representation.md §16,
        // lever 2): on the default structural codec — the only codec the typed
        // path runs under, since a non-default codec gates the typed path off —
        // emit a TypedStructuralRow<TRow> that holds the struct inline and boxes
        // columns on demand, instead of eagerly allocating an object?[] and boxing
        // every field per output row (the dominant per-output-row allocation on
        // output-heavy queries). The wrapped row is indistinguishable from the
        // backing-array form (same Count / indexer / hash / equals), so this is
        // correctness-equivalent.
        if (ReferenceEquals(codec, StructuralRowCodec.Instance))
        {
            return BuildLazyTypedToStructuralDelegate(rowType, schema);
        }

        // Fallback for any non-default codec: eager object?[] + codec.BuildRow.
        var rowParam = Expression.Parameter(rowType, "r");

        var arrInit = new Expression[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            var field = rowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted row missing field F" + i);
            arrInit[i] = Expression.Convert(Expression.Field(rowParam, field), typeof(object));
        }

        var newArray = Expression.NewArrayInit(typeof(object), arrInit);

        var helper = typeof(TypedPlanCompiler).GetMethod(
            nameof(BuildStructuralRowFromArray),
            BindingFlags.NonPublic | BindingFlags.Static)!;
        var body = Expression.Call(
            null, helper,
            Expression.Constant(codec, typeof(IRowCodec<StructuralRow>)),
            Expression.Constant(schema, typeof(Schema)),
            newArray);

        var delegateType = typeof(Func<,>).MakeGenericType(rowType, typeof(StructuralRow));
        return Expression.Lambda(delegateType, body, rowParam).Compile();
    }

    /// <summary>
    /// Builds <c>(TRow r) =&gt; new TypedStructuralRow&lt;TRow&gt;(r, shape)</c> — the
    /// lever-2 lazy-boxing boundary. The shared <see cref="StructuralRowShape{TRow}"/>
    /// carries the typed hash (reproducing <see cref="StructuralRow.ComputeHash"/>
    /// field-by-field without boxing) and the per-column boxing accessor, both built
    /// once here. Per output row this allocates one wrapper object (the struct sits
    /// inline) and no <c>object?[]</c> or per-field boxes; columns box lazily only
    /// when the indexer is read.
    /// </summary>
    private static Delegate BuildLazyTypedToStructuralDelegate(Type rowType, Schema schema)
    {
        var fields = new FieldInfo[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            fields[i] = rowType.GetField("F" + i)
                ?? throw new InvalidOperationException("emitted row missing field F" + i);
        }

        var hash = BuildTypedHashDelegate(rowType, fields);
        var box = BuildColumnBoxDelegate(rowType, fields);

        var shapeType = typeof(StructuralRowShape<>).MakeGenericType(rowType);
        var shape = Activator.CreateInstance(shapeType, schema.Count, hash, box)!;

        var wrapperType = typeof(TypedStructuralRow<>).MakeGenericType(rowType);
        var ctor = wrapperType.GetConstructor(new[] { rowType, shapeType })!;

        var rowParam = Expression.Parameter(rowType, "r");
        var body = Expression.New(ctor, rowParam, Expression.Constant(shape, shapeType));
        var delegateType = typeof(Func<,>).MakeGenericType(rowType, typeof(StructuralRow));
        return Expression.Lambda(delegateType, body, rowParam).Compile();
    }

    /// <summary>
    /// Builds <c>(TRow r) =&gt; { var hc = default(HashCode); hc.Add(arity);
    /// hc.Add(r.F0); …; return hc.ToHashCode(); }</c>. This reproduces
    /// <see cref="StructuralRow.ComputeHash"/> exactly: that method does
    /// <c>hc.Add(count)</c> then <c>hc.Add((object)values[i])</c>, and
    /// <c>HashCode.Add(typedField)</c> feeds the identical per-element hash as
    /// <c>HashCode.Add((object)boxedField)</c> (both route to the field's
    /// <c>GetHashCode</c>; null → 0), so a wrapped row hashes equal to the
    /// backing-array form — required for output Z-set dedup and cross-type lookups.
    /// </summary>
    private static Delegate BuildTypedHashDelegate(Type rowType, FieldInfo[] fields)
    {
        var addOpen = typeof(HashCode).GetMethods()
            .Single(m => m.Name == nameof(HashCode.Add)
                && m.IsGenericMethodDefinition
                && m.GetParameters().Length == 1);
        var toHash = typeof(HashCode).GetMethod(nameof(HashCode.ToHashCode))!;

        var rowParam = Expression.Parameter(rowType, "r");
        var hc = Expression.Variable(typeof(HashCode), "hc");
        var body = new List<Expression>
        {
            Expression.Assign(hc, Expression.Default(typeof(HashCode))),
            Expression.Call(hc, addOpen.MakeGenericMethod(typeof(int)), Expression.Constant(fields.Length)),
        };
        foreach (var f in fields)
        {
            body.Add(Expression.Call(hc, addOpen.MakeGenericMethod(f.FieldType), Expression.Field(rowParam, f)));
        }

        body.Add(Expression.Call(hc, toHash));
        var block = Expression.Block(typeof(int), new[] { hc }, body);
        var delegateType = typeof(Func<,>).MakeGenericType(rowType, typeof(int));
        return Expression.Lambda(delegateType, block, rowParam).Compile();
    }

    /// <summary>
    /// Builds <c>(TRow r, int i) =&gt; i switch { 0 =&gt; (object?)r.F0, … }</c> — the
    /// per-column boxing accessor the lazy wrapper's indexer calls on demand.
    /// </summary>
    private static Delegate BuildColumnBoxDelegate(Type rowType, FieldInfo[] fields)
    {
        var rowParam = Expression.Parameter(rowType, "r");
        var idxParam = Expression.Parameter(typeof(int), "i");

        var cases = new SwitchCase[fields.Length];
        for (var i = 0; i < fields.Length; i++)
        {
            cases[i] = Expression.SwitchCase(
                Expression.Convert(Expression.Field(rowParam, fields[i]), typeof(object)),
                Expression.Constant(i));
        }

        var defaultBody = Expression.Throw(
            Expression.New(typeof(IndexOutOfRangeException).GetConstructor(Type.EmptyTypes)!),
            typeof(object));
        var sw = Expression.Switch(typeof(object), idxParam, defaultBody, null, cases);
        var delegateType = typeof(Func<,,>).MakeGenericType(rowType, typeof(int), typeof(object));
        return Expression.Lambda(delegateType, sw, rowParam, idxParam).Compile();
    }

    /// <summary>
    /// Shim called from the compiled MapRows lambda — wraps the
    /// boxed-field array as a span and dispatches to
    /// <see cref="IRowCodec{TRow}.BuildRow"/>.
    /// </summary>
    private static StructuralRow BuildStructuralRowFromArray(
        IRowCodec<StructuralRow> codec, Schema schema, object?[] values)
        => codec.BuildRow(schema, values);

    // ---- Stateful op family dispatch: flat vs spine ----
    //
    // Every typed stateful site dispatches through one of these
    // EmitXxx helpers, mirroring PlanToCircuit's EmitDistinct /
    // EmitInnerJoin / EmitLeftJoin / EmitAggregate. The spine path
    // does not pass an externally-supplied IComparer — the emitted
    // struct row types implement IComparable<TSelf> (see
    // TypedRowEmitter.EmitTypedCompareTo), so Comparer<T>.Default
    // works.

    private static object EmitDistinct(
        CompileContext ctx, Type rowType, object input, object? snapshotCodec)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? InvokeSpineDistinct(ctx.Builder, rowType, input, ctx.Options.Compaction, snapshotCodec)
            : InvokeDistinct(ctx.Builder, rowType, input, snapshotCodec);
    }

    private static object EmitInnerJoin(
        CompileContext ctx, Type keyRowType, Type leftRowType, Type rightRowType, Type outputRowType,
        object leftIndexed, object rightIndexed, Delegate combine,
        object? leftSnapshotCodec, object? rightSnapshotCodec, Delegate? residual = null)
    {
        // residual is non-null only on the in-memory path (the caller leaves it
        // null for spine and post-filters instead).
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? InvokeSpineIncrementalInnerJoin(
                ctx.Builder, keyRowType, leftRowType, rightRowType, outputRowType,
                leftIndexed, rightIndexed, combine,
                leftSnapshotCodec, rightSnapshotCodec, ctx.Options.Compaction)
            : InvokeIncrementalInnerJoin(
                ctx.Builder, keyRowType, leftRowType, rightRowType, outputRowType,
                leftIndexed, rightIndexed, combine, leftSnapshotCodec, rightSnapshotCodec, residual);
    }

    private static object EmitLeftJoin(
        CompileContext ctx, Type keyRowType, Type leftRowType, Type rightRowType, Type outputRowType,
        object leftIndexed, object rightIndexed, Delegate joinCombine, Delegate nullPadCombine,
        object? leftSnapshotCodec, object? rightSnapshotCodec)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? InvokeSpineIncrementalLeftJoin(
                ctx.Builder, keyRowType, leftRowType, rightRowType, outputRowType,
                leftIndexed, rightIndexed, joinCombine, nullPadCombine,
                leftSnapshotCodec, rightSnapshotCodec, ctx.Options.Compaction)
            : InvokeIncrementalLeftJoin(
                ctx.Builder, keyRowType, leftRowType, rightRowType, outputRowType,
                leftIndexed, rightIndexed, joinCombine, nullPadCombine,
                leftSnapshotCodec, rightSnapshotCodec);
    }

    private static object EmitFullJoin(
        CompileContext ctx, Type keyRowType, Type leftRowType, Type rightRowType, Type outputRowType,
        object leftIndexed, object rightIndexed, Delegate joinCombine,
        Delegate nullPadRightCombine, Delegate nullPadLeftCombine,
        object? leftSnapshotCodec, object? rightSnapshotCodec)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? InvokeSpineIncrementalFullJoin(
                ctx.Builder, keyRowType, leftRowType, rightRowType, outputRowType,
                leftIndexed, rightIndexed, joinCombine, nullPadRightCombine, nullPadLeftCombine,
                leftSnapshotCodec, rightSnapshotCodec, ctx.Options.Compaction)
            : InvokeIncrementalFullJoin(
                ctx.Builder, keyRowType, leftRowType, rightRowType, outputRowType,
                leftIndexed, rightIndexed, joinCombine, nullPadRightCombine, nullPadLeftCombine,
                leftSnapshotCodec, rightSnapshotCodec);
    }

    private static object EmitAggregate(
        CompileContext ctx, Type keyRowType, Type valueRowType, Type aggRowType,
        object indexed, object aggregator, object? snapshotCodec)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? InvokeSpineIncrementalAggregate(
                ctx.Builder, keyRowType, valueRowType, aggRowType,
                indexed, aggregator, snapshotCodec, ctx.Options.Compaction)
            : InvokeIncrementalAggregate(
                ctx.Builder, keyRowType, valueRowType, aggRowType,
                indexed, aggregator, snapshotCodec);
    }

    // ---- Reflection-built generic calls ----

    /// <summary>builder.ZSetInput&lt;TRow, Z64&gt;()</summary>
    private static (object Handle, object Stream) InvokeZSetInput(
        CircuitBuilder builder, Type rowType, string? name = null)
    {
        // ZSetInput has two generic overloads (with/without a name); select by
        // parameter count so adding the named one didn't make this ambiguous.
        var paramCount = name is null ? 1 : 2;
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.ZSetInput)
                && m.IsGenericMethodDefinition
                && m.GetParameters().Length == paramCount);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        var args = name is null ? new object[] { builder } : new object[] { builder, name };
        var tuple = closed.Invoke(null, args)!;
        var tupleType = tuple.GetType();
        var item1 = tupleType.GetField("Item1")!.GetValue(tuple)!;
        var item2 = tupleType.GetField("Item2")!.GetValue(tuple)!;
        return (item1, item2);
    }

    /// <summary>
    /// <c>builder.Exchange&lt;TRow, Z64&gt;(stream, partition)</c> — re-shards the
    /// row stream so rows with the same key co-locate on one worker. A no-op
    /// (returns the stream unchanged) unless the circuit is a parallel one.
    /// </summary>
    private static object InvokeExchange(
        CircuitBuilder builder, Type rowType, object stream, Delegate partition)
    {
        var openMethod = typeof(CircuitBuilder)
            .GetMethods()
            .Single(m => m.Name == nameof(CircuitBuilder.Exchange) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(builder, new[] { stream, partition })!;
    }

    /// <summary>
    /// Builds the exchange partition <c>(TRow r) =&gt; stableHash(keyExtractor(r))</c>
    /// over an emitted key type — co-locating rows whose key columns are equal.
    /// </summary>
    /// <remarks>
    /// The hash is a per-column <see cref="StablePartitionHash"/> projection rather
    /// than <see cref="object.GetHashCode"/>: it depends only on the column values,
    /// so equal keys map to the same worker across runs and across a
    /// snapshot/recovery cycle (a recovered replica must own the same keys it held
    /// before). <see cref="BuildRowHashPartition"/> is the whole-row counterpart.
    /// </remarks>
    private static Delegate BuildKeyHashPartition(Type rowType, Type keyRowType, Delegate keyExtractor)
    {
        var rowParam = Expression.Parameter(rowType, "r");
        var keyVar = Expression.Variable(keyRowType, "k");
        var assign = Expression.Assign(keyVar, Expression.Invoke(Expression.Constant(keyExtractor), rowParam));
        var body = Expression.Block(new[] { keyVar }, assign, BuildStableRowHash(keyVar, keyRowType));
        var delegateType = typeof(Func<,>).MakeGenericType(rowType, typeof(int));
        return Expression.Lambda(delegateType, body, rowParam).Compile();
    }

    /// <summary>
    /// Builds the whole-row exchange partition <c>(TRow r) =&gt; stableHash(r)</c>,
    /// used by DISTINCT (the entire row is the key) and to balance sharded input.
    /// See <see cref="BuildKeyHashPartition"/> on hashing stability.
    /// </summary>
    private static Delegate BuildRowHashPartition(Type rowType)
    {
        var rowParam = Expression.Parameter(rowType, "r");
        var body = BuildStableRowHash(rowParam, rowType);
        var delegateType = typeof(Func<,>).MakeGenericType(rowType, typeof(int));
        return Expression.Lambda(delegateType, body, rowParam).Compile();
    }

    /// <summary>
    /// Builds <c>(TRow r) =&gt; stableHash(key0(r), key1(r), …)</c> from a list of
    /// resolved partition-key expressions (e.g. an <c>OVER (PARTITION BY …)</c>
    /// key), each lowered over the emitted row and stable-hashed per column, folded
    /// with <see cref="StableHash.Combine"/>. Returns <c>null</c> if any key is
    /// outside the typed expression compiler or the stable-hash type surface — the
    /// caller then refuses the parallel compile.
    /// </summary>
    private static Delegate? BuildExprListHashPartition(Type rowType, IReadOnlyList<ResolvedExpression> keys)
    {
        var rowParam = Expression.Parameter(rowType, "r");
        var hashes = new Expression[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            var body = TypedExpressionCompiler.TryBuildInto(keys[i], rowParam);
            if (body is null) return null;
            try
            {
                hashes[i] = BuildStableFieldHash(body, body.Type);
            }
            catch (NotSupportedException)
            {
                return null;
            }
        }

        Expression combined;
        if (hashes.Length == 1)
        {
            combined = hashes[0];
        }
        else
        {
            var combine = typeof(StableHash).GetMethod(nameof(StableHash.Combine), new[] { typeof(int[]) })!;
            combined = Expression.Call(combine, Expression.NewArrayInit(typeof(int), hashes));
        }

        var delegateType = typeof(Func<,>).MakeGenericType(rowType, typeof(int));
        return Expression.Lambda(delegateType, combined, rowParam).Compile();
    }

    /// <summary>
    /// Builds an <c>int</c>-typed expression hashing every column of an emitted
    /// row (<paramref name="rowValue"/> of type <paramref name="rowType"/>) via
    /// <see cref="StablePartitionHash"/>, then folding the per-column hashes with
    /// <see cref="StableHash.Combine"/>. A single-column row skips the combine.
    /// </summary>
    private static Expression BuildStableRowHash(Expression rowValue, Type rowType)
    {
        var fields = OrderedRowFields(rowType);
        var hashes = new Expression[fields.Length];
        for (var i = 0; i < fields.Length; i++)
        {
            hashes[i] = BuildStableFieldHash(Expression.Field(rowValue, fields[i]), fields[i].FieldType);
        }

        if (hashes.Length == 1)
        {
            return hashes[0];
        }

        var combine = typeof(StableHash).GetMethod(nameof(StableHash.Combine), new[] { typeof(int[]) })!;
        return Expression.Call(combine, Expression.NewArrayInit(typeof(int), hashes));
    }

    /// <summary>
    /// Hashes one emitted-row field via the matching <see cref="StablePartitionHash"/>
    /// overload. A <see cref="Nullable{T}"/> field hashes its value when present and
    /// collapses to <see cref="StablePartitionHash.NullHash"/> when null, so two
    /// null keys co-locate (matching the typed Nullable equality).
    /// </summary>
    private static Expression BuildStableFieldHash(Expression field, Type fieldType)
    {
        var underlying = Nullable.GetUnderlyingType(fieldType);
        if (underlying is not null)
        {
            var hasValue = Expression.Property(field, "HasValue");
            var value = Expression.Property(field, "Value");
            return Expression.Condition(
                hasValue,
                StableHashOfCall(value, underlying),
                Expression.Constant(StablePartitionHash.NullHash));
        }

        return StableHashOfCall(field, fieldType);
    }

    /// <summary>
    /// <c>StablePartitionHash.Of(value)</c>, resolving the overload by the value's
    /// (non-nullable) CLR type. Reference-type columns (e.g. <c>string</c>) bind the
    /// nullable-aware overload, which handles a null value itself.
    /// </summary>
    private static Expression StableHashOfCall(Expression value, Type valueType)
    {
        var method = typeof(StablePartitionHash).GetMethod(
            nameof(StablePartitionHash.Of), BindingFlags.Public | BindingFlags.Static, new[] { valueType })
            ?? throw new NotSupportedException(
                $"no stable partition hash for typed-row column type {valueType}");
        return Expression.Call(method, value);
    }

    /// <summary>
    /// The emitted row's <c>F0..Fn</c> fields in declaration order. Reflection's
    /// field order is unspecified, so sort by the numeric suffix the emitter assigns
    /// (<see cref="TypedRowEmitter"/>).
    /// </summary>
    private static FieldInfo[] OrderedRowFields(Type rowType)
    {
        var fields = rowType.GetFields(BindingFlags.Public | BindingFlags.Instance);
        Array.Sort(fields, static (a, b) =>
            int.Parse(a.Name.AsSpan(1), System.Globalization.CultureInfo.InvariantCulture)
                .CompareTo(int.Parse(b.Name.AsSpan(1), System.Globalization.CultureInfo.InvariantCulture)));
        return fields;
    }

    /// <summary><c>builder.Filter&lt;TRow, Z64&gt;(stream, predicate)</c>.</summary>
    private static object InvokeFilter(
        CircuitBuilder builder, Type rowType, object stream, Delegate predicate)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.Filter) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, stream, predicate })!;
    }

    /// <summary><c>builder.Union&lt;TRow, Z64&gt;(left, right)</c>.</summary>
    private static object InvokeUnion(
        CircuitBuilder builder, Type rowType, object left, object right)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.Union) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, left, right })!;
    }

    /// <summary><c>builder.Difference&lt;TRow, Z64&gt;(left, right)</c>.</summary>
    private static object InvokeDifference(
        CircuitBuilder builder, Type rowType, object left, object right)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.Difference) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, left, right })!;
    }

    /// <summary><c>builder.Distinct&lt;TRow, Z64&gt;(input, snapshotCodec)</c>.</summary>
    private static object InvokeDistinct(
        CircuitBuilder builder, Type rowType, object input, object? snapshotCodec)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.Distinct) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        // Reflection ignores optional-parameter defaults — every parameter must
        // be supplied. The trailing nulls are the LATENESS GC hooks
        // (frontier, monotoneKey); the typed path does not GC (LATENESS forces
        // the structural compile).
        return closed.Invoke(null, new object?[] { builder, input, snapshotCodec, null, null })!;
    }

    /// <summary>
    /// <c>builder.TopK&lt;TRow&gt;(input, comparer, offset, limit, snapshotCodec)</c>,
    /// building the typed <see cref="SortKeyComparer{TRow}"/> from the boxed
    /// sort-key extractors first (TRow is only known at runtime).
    /// </summary>
    private static object InvokeTopK(
        CircuitBuilder builder, Type rowType, object input, Delegate[] extractors,
        bool[] descending, bool[] nullsFirst, long offset, long? limit, object? snapshotCodec)
    {
        var openMethod = typeof(TypedPlanCompiler)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == nameof(BuildTopK) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType);
        return closed.Invoke(null, new object?[]
        {
            builder, input, extractors, descending, nullsFirst, offset, limit, snapshotCodec,
        })!;
    }

    /// <summary>
    /// Generic worker for <see cref="InvokeTopK"/> — closes the open generics so
    /// the comparer, stream cast, and builder call are all statically typed.
    /// </summary>
    private static object BuildTopK<TRow>(
        CircuitBuilder builder, object input, Delegate[] extractors,
        bool[] descending, bool[] nullsFirst, long offset, long? limit, object? snapshotCodec)
        where TRow : notnull
    {
        var keys = new Func<TRow, object?>[extractors.Length];
        for (var i = 0; i < extractors.Length; i++)
        {
            keys[i] = (Func<TRow, object?>)extractors[i];
        }

        var comparer = new SortKeyComparer<TRow>(keys, descending, nullsFirst, Comparer<TRow>.Default);
        var stream = (Stream<ZSet<TRow, Z64>>)input;
        var codec = (IZSetTraceCodec<TRow, Z64>?)snapshotCodec;
        return builder.TopK(stream, comparer, offset, limit, codec);
    }

    /// <summary>
    /// <c>builder.PartitionedTopK&lt;TRow, StructuralRow&gt;(…)</c>, building the
    /// typed order / sort-key-only comparers and the StructuralRow partition-key
    /// extractor from the boxed delegates first (TRow is only known at runtime).
    /// </summary>
    private static object InvokePartitionedTopK(
        CircuitBuilder builder, Type rowType, object input, Delegate[] sortExtractors,
        bool[] descending, bool[] nullsFirst, Delegate[] partitionExtractors,
        RankFunction function, long limit, object? snapshotCodec)
    {
        var openMethod = typeof(TypedPlanCompiler)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == nameof(BuildPartitionedTopK) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType);
        return closed.Invoke(null, new object?[]
        {
            builder, input, sortExtractors, descending, nullsFirst, partitionExtractors, function, limit, snapshotCodec,
        })!;
    }

    /// <summary>Generic worker for <see cref="InvokePartitionedTopK"/>.</summary>
    private static object BuildPartitionedTopK<TRow>(
        CircuitBuilder builder, object input, Delegate[] sortExtractors,
        bool[] descending, bool[] nullsFirst, Delegate[] partitionExtractors,
        RankFunction function, long limit, object? snapshotCodec)
        where TRow : notnull
    {
        var keys = new Func<TRow, object?>[sortExtractors.Length];
        for (var i = 0; i < sortExtractors.Length; i++)
        {
            keys[i] = (Func<TRow, object?>)sortExtractors[i];
        }

        var order = new SortKeyComparer<TRow>(keys, descending, nullsFirst, Comparer<TRow>.Default);
        var sortKeyOnly = new SortKeyComparer<TRow>(keys, descending, nullsFirst, ConstantZeroComparer<TRow>.Instance);

        var partKeys = new Func<TRow, object?>[partitionExtractors.Length];
        for (var i = 0; i < partitionExtractors.Length; i++)
        {
            partKeys[i] = (Func<TRow, object?>)partitionExtractors[i];
        }

        StructuralRow PartitionOf(TRow row)
        {
            var values = new object?[partKeys.Length];
            for (var i = 0; i < partKeys.Length; i++)
            {
                values[i] = partKeys[i](row);
            }

            return new StructuralRow(values);
        }

        // §22 narrow-key path: pass the single ORDER BY extractor (the same boxed
        // delegate `order`/`sortKeyOnly` already use) so the operator can key its trace
        // by {order value, wide row} when the narrowing seam is on. Single-column shape
        // only (q18/q19); multi-column falls back to the whole-row operator. Harmless
        // when the seam is off. No reflected-signature change — `keys`/`descending`/
        // `nullsFirst` are already this method's arguments (the typed reflection gotcha
        // is dodged: only this in-method call site changes).
        Func<TRow, object?>? orderKey = null;
        var orderDescending = false;
        var orderNullsFirst = false;
        if (keys.Length == 1)
        {
            orderKey = keys[0];
            orderDescending = descending[0];
            orderNullsFirst = nullsFirst[0];
        }

        var stream = (Stream<ZSet<TRow, Z64>>)input;
        var codec = (IZSetTraceCodec<TRow, Z64>?)snapshotCodec;
        return builder.PartitionedTopK<TRow, StructuralRow>(
            stream, PartitionOf, order, sortKeyOnly, function, limit, null, codec,
            orderKey, orderDescending, orderNullsFirst);
    }

    private static object InvokePartitionedWindowAggregate(
        CircuitBuilder builder, Type inRowType, Type aggRowType, Type outRowType, object input,
        Delegate[] partitionExtractors, Delegate? orderExtractor, bool descending, bool nullsFirst,
        long? preceding, object aggregator, Delegate flatten, object? snapshotCodec)
    {
        var openMethod = typeof(TypedPlanCompiler)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == nameof(BuildPartitionedWindowAggregate) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(inRowType, aggRowType, outRowType);
        return closed.Invoke(null, new object?[]
        {
            builder, input, partitionExtractors, orderExtractor, descending, nullsFirst,
            preceding, aggregator, flatten, snapshotCodec,
        })!;
    }

    /// <summary>Generic worker for <see cref="InvokePartitionedWindowAggregate"/> —
    /// constructs the strongly-typed partition/order delegates, the numeric
    /// order-value extractor, and the struct-fusing widener, then drives the
    /// row-opaque window-aggregate builder (TKey = StructuralRow over the boxed
    /// partition columns, as in <see cref="BuildPartitionedTopK{TRow}"/>).</summary>
    private static object BuildPartitionedWindowAggregate<TInRow, TAgg, TOutRow>(
        CircuitBuilder builder, object input, Delegate[] partitionExtractors,
        Delegate? orderExtractor, bool descending, bool nullsFirst,
        long? preceding, object aggregator, Delegate flatten, object? snapshotCodec)
        where TInRow : notnull
        where TAgg : notnull
        where TOutRow : notnull
    {
        var partKeys = new Func<TInRow, object?>[partitionExtractors.Length];
        for (var i = 0; i < partitionExtractors.Length; i++)
        {
            partKeys[i] = (Func<TInRow, object?>)partitionExtractors[i];
        }

        StructuralRow PartitionOf(TInRow row)
        {
            var values = new object?[partKeys.Length];
            for (var i = 0; i < partKeys.Length; i++)
            {
                values[i] = partKeys[i](row);
            }

            return new StructuralRow(values);
        }

        IComparer<TInRow> order;
        Func<TInRow, long>? orderValueOf = null;
        if (orderExtractor is Func<TInRow, object?> orderBoxed)
        {
            var keys = new[] { orderBoxed };
            order = new SortKeyComparer<TInRow>(
                keys, new[] { descending }, new[] { nullsFirst }, Comparer<TInRow>.Default);

            // Ordered frames use the numeric order value for the RANGE arithmetic;
            // the resolver constrains the key to an integer or temporal type. A
            // NULL key sorts to the low end of the value space.
            orderValueOf = row => orderBoxed(row) is { } v ? MonotoneKey.Extract(v) : long.MinValue;
        }
        else
        {
            // Whole-partition frame: a deterministic total order over distinct rows.
            order = Comparer<TInRow>.Default;
        }

        var agg = (IAggregator<TInRow, TAgg>)aggregator;
        var fuse = (Func<ValueTuple<TInRow, TAgg>, TOutRow>)flatten;

        // The aggregate is non-empty for any emitted row (the current row is always
        // in its own frame), so HasValue holds in practice; default(TAgg) guards the
        // impossible empty-frame branch (its agg cols are nullable / zero).
        TOutRow Widen(TInRow row, Optional<TAgg> a) => fuse((row, a.HasValue ? a.Value : default!));

        var stream = (Stream<ZSet<TInRow, Z64>>)input;
        var codec = (IZSetTraceCodec<TInRow, Z64>?)snapshotCodec;
        return builder.PartitionedWindowAggregate<TInRow, TAgg, TOutRow, StructuralRow>(
            stream, PartitionOf, order, orderValueOf, preceding, descending, agg, Widen, null, codec, null);
    }

    /// <remarks>
    /// The argument array below is positional and name-matched only — it is NOT
    /// checked at compile time. Any change to <see cref="BuildPartitionedOffset{TInRow,TOutRow}"/>'s
    /// parameter list must be mirrored here in exact positional order, or this fails
    /// at runtime rather than at build.
    /// </remarks>
    private static object InvokePartitionedOffset(
        CircuitBuilder builder, Type inRowType, Type outRowType, object input,
        Delegate[] partitionExtractors, Delegate[] sortExtractors, bool[] descending, bool[] nullsFirst,
        Delegate[] valueExtractors, OffsetKind[] kinds, long[] offsets, object?[] defaults,
        Delegate widen, object? snapshotCodec)
    {
        var openMethod = typeof(TypedPlanCompiler)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .Single(m => m.Name == nameof(BuildPartitionedOffset) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(inRowType, outRowType);
        return closed.Invoke(null, new object?[]
        {
            builder, input, partitionExtractors, sortExtractors, descending, nullsFirst,
            valueExtractors, kinds, offsets, defaults, widen, snapshotCodec,
        })!;
    }

    /// <summary>Generic worker for <see cref="InvokePartitionedOffset"/> — builds
    /// the strongly-typed partition/order delegates and the
    /// <see cref="OffsetSpec{TInRow}"/> array, then drives the row-opaque offset
    /// builder (TKey = StructuralRow over the boxed partition columns).</summary>
    private static object BuildPartitionedOffset<TInRow, TOutRow>(
        CircuitBuilder builder, object input, Delegate[] partitionExtractors,
        Delegate[] sortExtractors, bool[] descending, bool[] nullsFirst,
        Delegate[] valueExtractors, OffsetKind[] kinds, long[] offsets, object?[] defaults,
        Delegate widen, object? snapshotCodec)
        where TInRow : notnull
        where TOutRow : notnull
    {
        var partKeys = new Func<TInRow, object?>[partitionExtractors.Length];
        for (var i = 0; i < partitionExtractors.Length; i++)
        {
            partKeys[i] = (Func<TInRow, object?>)partitionExtractors[i];
        }

        StructuralRow PartitionOf(TInRow row)
        {
            var values = new object?[partKeys.Length];
            for (var i = 0; i < partKeys.Length; i++)
            {
                values[i] = partKeys[i](row);
            }

            return new StructuralRow(values);
        }

        var sortKeys = new Func<TInRow, object?>[sortExtractors.Length];
        for (var i = 0; i < sortExtractors.Length; i++)
        {
            sortKeys[i] = (Func<TInRow, object?>)sortExtractors[i];
        }

        var order = new SortKeyComparer<TInRow>(
            sortKeys, descending, nullsFirst, Comparer<TInRow>.Default);

        var specs = new OffsetSpec<TInRow>[valueExtractors.Length];
        for (var s = 0; s < valueExtractors.Length; s++)
        {
            specs[s] = new OffsetSpec<TInRow>(
                (Func<TInRow, object?>)valueExtractors[s], kinds[s], offsets[s], defaults[s]);
        }

        var fuse = (Func<TInRow, object?[], TOutRow>)widen;

        var stream = (Stream<ZSet<TInRow, Z64>>)input;
        var codec = (IZSetTraceCodec<TInRow, Z64>?)snapshotCodec;
        return builder.PartitionedOffset<TInRow, TOutRow, StructuralRow>(
            stream, PartitionOf, order, specs, fuse, null, codec);
    }

    /// <summary><c>builder.MapRows&lt;TIn, TOut, Z64&gt;(stream, projection)</c>.</summary>
    private static object InvokeMapRows(
        CircuitBuilder builder, Type inputRowType, Type outputRowType,
        object inputStream, Delegate projection)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.MapRows) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(inputRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, inputStream, projection })!;
    }

    /// <summary><c>builder.MapFilterRows&lt;TIn, TOut, Z64&gt;(stream, step)</c> — the
    /// fused map+filter pass for a typed linear chain.</summary>
    private static object InvokeMapFilterRows(
        CircuitBuilder builder, Type inputRowType, Type outputRowType,
        object inputStream, Delegate step)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.MapFilterRows) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(inputRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, inputStream, step })!;
    }

    /// <summary>
    /// <c>builder.GroupProject&lt;TKey, TRow, TRow, Z64&gt;(stream, keyOf, row =&gt; row)</c>.
    /// The value extractor is the identity — every row is indexed by
    /// its key with itself as the value.
    /// </summary>
    private static object InvokeGroupProject(
        CircuitBuilder builder, Type rowType, Type keyRowType,
        object stream, Delegate keyExtractor)
    {
        // (TRow row) => row — identity value extractor.
        var rowParam = Expression.Parameter(rowType, "row");
        var identityDelegateType = typeof(Func<,>).MakeGenericType(rowType, rowType);
        var identity = Expression.Lambda(identityDelegateType, rowParam, rowParam).Compile();

        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.GroupProject) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(keyRowType, rowType, rowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, stream, keyExtractor, identity })!;
    }

    /// <summary>
    /// <c>builder.ExchangeIndex&lt;TKey, TRow, Z64&gt;(stream, partition, keyOf)</c>
    /// — the fused shuffle + re-index that replaces an
    /// <see cref="InvokeExchange"/> immediately followed by an
    /// <see cref="InvokeGroupProject"/>, producing the same
    /// <c>IndexedZSet&lt;TKey, TRow, Z64&gt;</c> in one pass. At W=1 the builder
    /// method degrades to a plain GroupProject, so the single-thread shape is
    /// unchanged.
    /// </summary>
    private static object InvokeExchangeIndex(
        CircuitBuilder builder, Type rowType, Type keyRowType,
        object stream, Delegate partition, Delegate keyExtractor)
    {
        var openMethod = typeof(CircuitBuilder)
            .GetMethods()
            .Single(m => m.Name == nameof(CircuitBuilder.ExchangeIndex) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(keyRowType, rowType, typeof(Z64));
        return closed.Invoke(builder, new[] { stream, partition, keyExtractor })!;
    }

    /// <summary>
    /// <c>builder.ExchangeIndexJoin&lt;TKey, TLeft, TRight, Z64&gt;(...)</c> — the
    /// fused dual shuffle + re-index of a join's two inputs across one shared
    /// barrier (§15). Returns the boxed (left, right) indexed streams, each
    /// identical to a separate <see cref="InvokeExchangeIndex"/> but sharing one
    /// rendezvous. The builder method returns a value tuple; its <c>Item1</c>/
    /// <c>Item2</c> fields are the two streams.
    /// </summary>
    private static (object Left, object Right) InvokeExchangeIndexJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        object leftStream, Delegate leftPartition, Delegate leftKeyExtractor,
        object rightStream, Delegate rightPartition, Delegate rightKeyExtractor)
    {
        var openMethod = typeof(CircuitBuilder)
            .GetMethods()
            .Single(m => m.Name == nameof(CircuitBuilder.ExchangeIndexJoin) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(keyRowType, leftRowType, rightRowType, typeof(Z64));
        var result = closed.Invoke(builder, new[]
        {
            leftStream, leftPartition, leftKeyExtractor,
            rightStream, rightPartition, rightKeyExtractor,
        })!;
        var type = result.GetType();
        var leftOut = type.GetField("Item1")!.GetValue(result)!;
        var rightOut = type.GetField("Item2")!.GetValue(result)!;
        return (leftOut, rightOut);
    }

    /// <summary>
    /// <c>builder.IncrementalInnerJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// </summary>
    private static object InvokeIncrementalInnerJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed, Delegate combine,
        object? leftSnapshotCodec = null, object? rightSnapshotCodec = null, Delegate? residual = null)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.IncrementalInnerJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        // Reflection ignores optional-parameter defaults — every parameter must
        // be supplied. The two nulls after the codecs are the LATENESS GC hooks
        // (frontier, monotoneKey); the typed path does not GC (LATENESS forces
        // the structural compile). The trailing slot is the residual predicate
        // (Func<TOut,bool>), applied during the join enumeration.
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, combine,
            leftSnapshotCodec, rightSnapshotCodec, null, null, residual,
        })!;
    }

    /// <summary>
    /// <c>builder.IncrementalLeftJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// "Left" here is the preserved side regardless of caller's
    /// physical left/right — RIGHT JOIN passes its right-table
    /// stream as <paramref name="leftIndexed"/>.
    /// </summary>
    private static object InvokeIncrementalLeftJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed,
        Delegate joinCombine, Delegate nullPadCombine,
        object? leftSnapshotCodec = null, object? rightSnapshotCodec = null)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.IncrementalLeftJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        // Reflection ignores optional-parameter defaults — every parameter must
        // be supplied. The trailing nulls are the LATENESS GC hooks
        // (frontier, monotoneKey); the typed path does not GC (LATENESS forces
        // the structural compile).
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, joinCombine, nullPadCombine,
            leftSnapshotCodec, rightSnapshotCodec, null, null,
        })!;
    }

    /// <summary>
    /// <c>builder.IncrementalFullJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// </summary>
    private static object InvokeIncrementalFullJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed,
        Delegate joinCombine, Delegate nullPadRightCombine, Delegate nullPadLeftCombine,
        object? leftSnapshotCodec = null, object? rightSnapshotCodec = null)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.IncrementalFullJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, joinCombine, nullPadRightCombine, nullPadLeftCombine,
            leftSnapshotCodec, rightSnapshotCodec, null, null,
        })!;
    }

    /// <summary>
    /// <c>builder.IncrementalAggregate&lt;TKey, TValue, TOut&gt;(indexed, aggregator)</c>.
    /// Output stream carries <c>ZSet&lt;(TKey, TOut), Z64&gt;</c>.
    /// </summary>
    private static object InvokeIncrementalAggregate(
        CircuitBuilder builder, Type keyRowType, Type valueRowType, Type aggRowType,
        object indexed, object aggregator, object? snapshotCodec = null)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.IncrementalAggregate)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(keyRowType, valueRowType, aggRowType);
        // Reflection does not apply C# optional-parameter defaults, so every
        // parameter must be supplied explicitly. frontier / monotoneKey are the
        // LATENESS GC hooks — null here; the typed path does not GC yet.
        return closed.Invoke(null, new object?[]
        {
            builder, indexed, aggregator, snapshotCodec, null, null,
        })!;
    }

    // ---- Spine builder reflection wrappers ----
    //
    // Each method mirrors its flat-trace sibling above but targets the
    // SpineStatefulOperators extensions. Comparers are NOT passed
    // (left as null) — the emitted struct row types implement
    // IComparable<TSelf>, so Comparer<T>.Default takes over. spill
    // configs, frontier, and monotoneKey are all null; LATENESS forces
    // the structural compile, so GC is wired only there.

    /// <summary>
    /// <c>builder.SpineDistinct&lt;TRow, Z64&gt;(input, compaction, snapshotCodec, null, null, null, null)</c>.
    /// </summary>
    private static object InvokeSpineDistinct(
        CircuitBuilder builder, Type rowType, object input,
        ICompactionStrategy? compaction, object? snapshotCodec)
    {
        var openMethod = typeof(SpineStatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(SpineStatefulOperators.SpineDistinct)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(null, new object?[]
        {
            builder, input, compaction, snapshotCodec,
            null,   // keyComparer — Comparer<TRow>.Default via IComparable<TRow>
            null,   // spillConfig
            null,   // frontier
            null,   // monotoneKey
        })!;
    }

    /// <summary>
    /// <c>builder.SpineIncrementalInnerJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// </summary>
    private static object InvokeSpineIncrementalInnerJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed, Delegate combine,
        object? leftSnapshotCodec, object? rightSnapshotCodec,
        ICompactionStrategy? compaction)
    {
        var openMethod = typeof(SpineStatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(SpineStatefulOperators.SpineIncrementalInnerJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, combine,
            leftSnapshotCodec, rightSnapshotCodec,
            compaction,
            null,  // keyComparer
            null,  // leftValueComparer
            null,  // rightValueComparer
            null,  // leftSpillConfig
            null,  // rightSpillConfig
            null,  // frontier
            null,  // monotoneKey
        })!;
    }

    /// <summary>
    /// <c>builder.SpineIncrementalLeftJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// </summary>
    private static object InvokeSpineIncrementalLeftJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed,
        Delegate joinCombine, Delegate nullPadCombine,
        object? leftSnapshotCodec, object? rightSnapshotCodec,
        ICompactionStrategy? compaction)
    {
        var openMethod = typeof(SpineStatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(SpineStatefulOperators.SpineIncrementalLeftJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, joinCombine, nullPadCombine,
            leftSnapshotCodec, rightSnapshotCodec,
            compaction,
            null,  // keyComparer
            null,  // leftValueComparer
            null,  // rightValueComparer
            null,  // leftSpillConfig
            null,  // rightSpillConfig
            null,  // frontier
            null,  // monotoneKey
        })!;
    }

    /// <summary>
    /// <c>builder.SpineIncrementalFullJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// </summary>
    private static object InvokeSpineIncrementalFullJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed,
        Delegate joinCombine, Delegate nullPadRightCombine, Delegate nullPadLeftCombine,
        object? leftSnapshotCodec, object? rightSnapshotCodec,
        ICompactionStrategy? compaction)
    {
        var openMethod = typeof(SpineStatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(SpineStatefulOperators.SpineIncrementalFullJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, joinCombine, nullPadRightCombine, nullPadLeftCombine,
            leftSnapshotCodec, rightSnapshotCodec,
            compaction,
            null,  // keyComparer
            null,  // leftValueComparer
            null,  // rightValueComparer
            null,  // leftSpillConfig
            null,  // rightSpillConfig
            null,  // frontier
            null,  // monotoneKey
        })!;
    }

    /// <summary>
    /// <c>builder.SpineIncrementalAggregate&lt;TKey, TValue, TOut&gt;(indexed, aggregator, ...)</c>.
    /// Output stream carries <c>ZSet&lt;(TKey, TOut), Z64&gt;</c>.
    /// </summary>
    private static object InvokeSpineIncrementalAggregate(
        CircuitBuilder builder, Type keyRowType, Type valueRowType, Type aggRowType,
        object indexed, object aggregator, object? snapshotCodec,
        ICompactionStrategy? compaction)
    {
        var openMethod = typeof(SpineStatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(SpineStatefulOperators.SpineIncrementalAggregate)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(keyRowType, valueRowType, aggRowType);
        return closed.Invoke(null, new object?[]
        {
            builder, indexed, aggregator, snapshotCodec, compaction,
            null,  // keyComparer
            null,  // valueComparer
            null,  // spillConfig
            null,  // frontier
            null,  // monotoneKey
        })!;
    }

    /// <summary>builder.Output(stream) — generic over ZSet&lt;TRow, Z64&gt;</summary>
    private static object InvokeOutput(CircuitBuilder builder, Type rowType, object stream)
    {
        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        var outputOpenMethod = typeof(CircuitBuilder).GetMethods()
            .Single(m => m.Name == nameof(CircuitBuilder.Output)
                && m.IsGenericMethodDefinition
                && m.GetParameters().Length == 1);
        var closed = outputOpenMethod.MakeGenericMethod(zsetType);
        return closed.Invoke(builder, new[] { stream })!;
    }

    /// <summary><c>builder.Output&lt;ZSet&lt;TRow, Z64&gt;&gt;(stream, name)</c> — the named overload.</summary>
    private static object InvokeNamedOutput(CircuitBuilder builder, Type rowType, object stream, string name)
    {
        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        var outputOpenMethod = typeof(CircuitBuilder).GetMethods()
            .Single(m => m.Name == nameof(CircuitBuilder.Output)
                && m.IsGenericMethodDefinition
                && m.GetParameters().Length == 2);
        var closed = outputOpenMethod.MakeGenericMethod(zsetType);
        return closed.Invoke(builder, new[] { stream, (object)name })!;
    }

    /// <summary>
    /// Build a <see cref="ParallelIngestor{TRow}"/> closed over the table's emitted
    /// row type: it shards a pushed batch across the replicas by whole-row hash,
    /// encoding on the worker threads (see <see cref="ParallelIngestor{TRow}"/>).
    /// </summary>
    private static ITableIngestor BuildParallelIngestor(
        ParallelCircuit circuit, string name, Type rowType, Schema schema, Func<object?[], object> factory)
    {
        var ingestorType = typeof(ParallelIngestor<>).MakeGenericType(rowType);
        var partition = BuildRowHashPartition(rowType);   // Func<TRow, int>
        return (ITableIngestor)Activator.CreateInstance(
            ingestorType, circuit, name, schema, factory, partition)!;
    }

    /// <summary><c>circuit.ShardedOutput&lt;TRow, Z64&gt;(name)</c>.</summary>
    private static object InvokeShardedOutput(ParallelCircuit circuit, Type rowType, string name)
    {
        var open = typeof(ParallelCircuit).GetMethods()
            .Single(m => m.Name == nameof(ParallelCircuit.ShardedOutput) && m.IsGenericMethodDefinition);
        var closed = open.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(circuit, new object[] { name })!;
    }

    /// <summary>
    /// Build a <see cref="DisjointOutputGather{TRow}"/> closed over the output row
    /// type: a parallel per-worker decode + concat used only when the output shards
    /// are key-disjoint (see <c>ShardDisjoint</c>).
    /// </summary>
    private static IOutputGather BuildDisjointGather(
        ParallelCircuit circuit, Type rowType, string outputName, Func<object, object?>[] getters)
    {
        var gatherType = typeof(DisjointOutputGather<>).MakeGenericType(rowType);
        return (IOutputGather)Activator.CreateInstance(gatherType, circuit, outputName, getters)!;
    }

    /// <summary>Reads <c>ShardedOutputHandle&lt;TRow, Z64&gt;.Current</c> (the gathered Z-set), boxed.</summary>
    private static Func<object> BuildShardedCurrentZSetGetter(Type rowType, object shardedOutput)
    {
        var handleType = typeof(ShardedOutputHandle<,>).MakeGenericType(rowType, typeof(Z64));
        var currentProp = handleType.GetProperty(nameof(ShardedOutputHandle<int, Z64>.Current))!;
        var getCurrent = currentProp.GetGetMethod()!;
        var call = Expression.Call(Expression.Constant(shardedOutput), getCurrent);
        var boxed = Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<object>>(boxed).Compile();
    }

    private static Func<object> BuildCurrentZSetGetter(Type rowType, object outputHandle)
    {
        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        var handleType = typeof(OutputHandle<>).MakeGenericType(zsetType);
        var currentProp = handleType.GetProperty(nameof(OutputHandle<int>.Current))!;
        var getCurrent = currentProp.GetGetMethod()!;
        var call = Expression.Call(Expression.Constant(outputHandle), getCurrent);
        var boxed = Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<object>>(boxed).Compile();
    }

    private static Func<object, IEnumerable<KeyValuePair<object, Z64>>> BuildBoxedEntriesReader(Type rowType)
    {
        // A typed enumerator (one boxing per key, no per-entry reflection — the old
        // GetProperty-per-row reflection dominated the egest wall-clock on large
        // outputs). ZSet<TRow, Z64> enumerates as KeyValuePair<TRow, Z64>.
        var method = typeof(TypedPlanCompiler)
            .GetMethod(nameof(EnumerateTypedEntries), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(rowType);
        return method.CreateDelegate<Func<object, IEnumerable<KeyValuePair<object, Z64>>>>();
    }

    private static IEnumerable<KeyValuePair<object, Z64>> EnumerateTypedEntries<TRow>(object zset)
        where TRow : notnull
    {
        foreach (var kv in (ZSet<TRow, Z64>)zset)
        {
            yield return new KeyValuePair<object, Z64>(kv.Key!, kv.Value);
        }
    }

    private static Func<object?[], Z64> BuildWeightOf(
        Type rowType, Func<object?[], object> factory, Func<object> currentGetter)
    {
        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        var weightOfMethod = zsetType.GetMethod(nameof(ZSet<int, Z64>.WeightOf), [rowType])!;

        return values =>
        {
            var row = factory(values);
            var current = currentGetter();
            return (Z64)weightOfMethod.Invoke(current, new[] { row })!;
        };
    }
}
