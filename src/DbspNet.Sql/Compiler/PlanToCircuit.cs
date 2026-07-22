// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Nested;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

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

    /// <summary>
    /// Opt-in compile path with both explicit <see cref="CompileOptions"/> and a
    /// non-default <see cref="IRowCodec{TRow}"/>. A non-default codec already pins
    /// the structural pipeline, so this is the apples-to-apples way to A/B a
    /// compile option (e.g. <see cref="CompileOptions.ShareArrangements"/>) on the
    /// structural path with the codec held fixed across arms. Options precede the
    /// codec to disambiguate from
    /// <see cref="Compile(LogicalPlan, ISqlSnapshotCodecs, CompileOptions)"/>.
    /// </summary>
    public static CompiledQuery Compile(LogicalPlan plan, CompileOptions options, IRowCodec<StructuralRow> codec)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(codec);
        return CompileCore(plan, codec, snapshotCodecs: null, options);
    }

    public static CompiledQuery Compile(CreateViewPlan view)
    {
        ArgumentNullException.ThrowIfNull(view);
        return CompileCore(view.Query, StructuralRowCodec.Instance, snapshotCodecs: null, CompileOptions.Default);
    }

    /// <summary>The logical port name the parallel single-query build wires its result to.</summary>
    private const string ParallelResultPort = "$result";

    /// <summary>
    /// Data-parallel compile of a single query onto a <see cref="ParallelCircuit"/>
    /// of <paramref name="workers"/> replicas: the same structural graph runs on
    /// every worker, key-sensitive operators re-shard by their key so equal keys
    /// co-locate, table inputs are sharded by whole-row hash, and the output is
    /// gathered (Z-set sum). The observable result equals the single-circuit
    /// <see cref="Compile(LogicalPlan, ISqlSnapshotCodecs, CompileOptions)"/> for
    /// every W (docs/design-structural-parallel.md). Returns <c>false</c> (and the
    /// caller should fall back to the serial compile) when the plan uses a
    /// construct this pass does not shard soundly — a broadcast / correlated /
    /// scalar-subquery join, a semi-join, a global (un-partitioned) window or
    /// top-K / rank, a set difference, a recursive CTE, a temporal filter, a
    /// LATENESS-GC'd input, or a partition key outside the stable-hash surface.
    /// </summary>
    /// <remarks>
    /// At <paramref name="workers"/> == 1 the emitted graph is byte-identical to
    /// the serial circuit (every <c>Exchange*</c> degrades to the same
    /// <c>GroupProject</c>), which the correctness oracle relies on as a free
    /// structural-identity regression guard.
    /// </remarks>
    public static bool TryCompileParallel(
        LogicalPlan plan,
        int workers,
        out ParallelStructuralCompiledQuery? compiled,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentOutOfRangeException.ThrowIfLessThan(workers, 1);
        compiled = null;
        options ??= CompileOptions.Default;

        var (tables, temporalFrontierSpecs) = CollectScans(plan);

        // The parallel path wires neither LATENESS GC nor temporal-filter
        // frontiers across the replicas yet (a follow-on), and refuses any plan
        // node it cannot shard soundly. In every such case the caller falls back
        // to the serial single circuit.
        if (temporalFrontierSpecs.Count > 0
            || tables.Values.Any(t => t.Lateness is { Count: > 0 })
            || !CanCompileParallel(plan))
        {
            return false;
        }

        var codec = StructuralRowCodec.Instance;
        var monotonicity = MonotonicityAnalyzer.Analyze(plan);
        var tableSchemas = tables.ToDictionary(kv => kv.Key, kv => kv.Value.Schema, StringComparer.Ordinal);
        var emptyFrontiers = (IReadOnlyDictionary<LatenessSource, IFrontier>)
            new Dictionary<LatenessSource, IFrontier>();
        var emptyShareable = (IReadOnlySet<ArrangementKey>)
            System.Collections.Immutable.ImmutableHashSet<ArrangementKey>.Empty;

        var inputSchemas = new Dictionary<string, Schema>(StringComparer.Ordinal);

        var prevStagingCapacity = SpineStagingConfig.Capacity;
        SpineStagingConfig.Capacity = options.TraceFamily == TraceFamily.Spine ? options.SpineStagingCapacity : 0;
        ParallelCircuit circuit;
        try
        {
            circuit = ParallelCircuit.Build(workers, builder =>
            {
                var streams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
                var ctx = new CompileContext(
                    streams, tableSchemas, codec, snapshotCodecs, options, monotonicity,
                    emptyFrontiers, emptyShareable, workers, options.RelationRowCounts);

                foreach (var (name, info) in tables)
                {
                    // Named input port so ShardedInput can find each replica's copy.
                    var (_, stream) = builder.ZSetInput<StructuralRow, Z64>(name);
                    streams[name] = stream;
                    inputSchemas[name] = info.Schema;
                    // The input is split by whole-row hash, so equal rows co-locate:
                    // the scan stream is shard-disjoint (not usefully key-partitioned).
                    ctx.SetPartition(stream, new PartitionInfo(ShardDisjoint: true, PartitionKey: null));
                }

                var queryStream = CompilePlan(builder, plan, ctx);
                builder.Output(queryStream, ParallelResultPort);
            });
        }
        finally
        {
            SpineStagingConfig.Capacity = prevStagingCapacity;
        }

        var inputs = new Dictionary<string, ShardedTableInput>(StringComparer.Ordinal);
        foreach (var (name, schema) in inputSchemas)
        {
            var handle = circuit.ShardedInput<StructuralRow, Z64>(
                name, row => StablePartitionHash.OfWholeRow(row, schema));
            inputs[name] = new ShardedTableInput(handle, schema, codec);
        }

        var output = circuit.ShardedOutput<StructuralRow, Z64>(ParallelResultPort);
        compiled = new ParallelStructuralCompiledQuery(circuit, inputs, output, plan.Schema);
        return true;
    }

    private static CompiledQuery CompileCore(
        LogicalPlan plan,
        IRowCodec<StructuralRow> codec,
        ISqlSnapshotCodecs? snapshotCodecs,
        CompileOptions options)
    {
        // Walk the plan to find every scanned table; each becomes a circuit input.
        var (tables, temporalFrontierSpecs) = CollectScans(plan);
        var hasLateness = tables.Values.Any(t => t.Lateness is { Count: > 0 });
        var hasTemporalFilter = temporalFrontierSpecs.Count > 0;
        var monotonicity = MonotonicityAnalyzer.Analyze(plan);

        RootCircuit? circuit = null;
        Dictionary<string, TableInput>? inputs = null;
        OutputHandle<ZSet<StructuralRow, Z64>>? output = null;
        IntegratedViewHandle<StructuralRow>? view = null;

        // The spine memtable capacity (CompileOptions.SpineStagingCapacity) is
        // realised through the SpineStagingConfig ambient seam, which each trace
        // reads once at construction. Set it for the duration of the build (the
        // graph — and so every trace — is constructed synchronously inside
        // RootCircuit.Build) and restore it after. Flat builds force 0 (no spine
        // traces read it). See docs §11.
        var prevStagingCapacity = SpineStagingConfig.Capacity;
        SpineStagingConfig.Capacity = options.TraceFamily == TraceFamily.Spine ? options.SpineStagingCapacity : 0;
        try
        {

        circuit = RootCircuit.Build(builder =>
        {
            var streams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
            inputs = new Dictionary<string, TableInput>(StringComparer.Ordinal);
            var frontiers = new Dictionary<LatenessSource, IFrontier>();
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

            // Register each temporal filter's clock-driven frontier (clock −
            // offset) on its time-key column, so downstream stateful operators
            // GC the same way they do for a declared LATENESS column. A column
            // already bounded by LATENESS keeps that frontier (skip on collision)
            // — the late-drop bound is sound on its own.
            foreach (var spec in temporalFrontierSpecs)
            {
                if (!frontiers.ContainsKey(spec.Source))
                {
                    frontiers[spec.Source] = new TransformedFrontier(
                        builder.LogicalClock,
                        ClockOffsetTransform(spec.Clock, spec.Offset, spec.CastTimestampToDate));
                }
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
            // Trace family flows through: in spine mode the typed
            // compiler dispatches to the SpineDistinct / SpineIncrementalInnerJoin
            // / SpineIncrementalLeftJoin / SpineIncrementalAggregate builders
            // instead of their flat siblings (see TypedPlanCompiler.Emit*),
            // backed by Comparer<TRow>.Default on the emitted IComparable
            // structs (see TypedRowEmitter.EmitTypedCompareTo).
            //
            // Disabled when a non-default IRowCodec<StructuralRow> is
            // supplied — the structural compile is the only path that
            // honours an alternative codec on every stage's output row.
            // Also disabled when LATENESS is present (GC is wired only on
            // the structural path — long-running bounded pipelines use
            // LATENESS structurally; the typed fast path is for short
            // queries).
            // Arrangement CSE (options.ShareArrangements) is implemented only on
            // the structural path below, so the flag also forces it: skip the
            // typed fast path when sharing is requested.
            Stream<ZSet<StructuralRow, Z64>>? queryStream = null;
            if (ReferenceEquals(codec, StructuralRowCodec.Instance)
                && !hasLateness
                && !hasTemporalFilter
                && !options.ShareArrangements)
            {
                queryStream = TypedPlanCompiler.TryCompileWithStructuralBoundary(
                    builder, plan, streams, codec, snapshotCodecs, options);
            }

            if (queryStream is null)
            {
                var tableSchemas = tables.ToDictionary(kv => kv.Key, kv => kv.Value.Schema, StringComparer.Ordinal);
                var shareable = options.ShareArrangements
                    ? CollectShareableArrangements(plan)
                    : (IReadOnlySet<ArrangementKey>)System.Collections.Immutable.ImmutableHashSet<ArrangementKey>.Empty;
                var ctx = new CompileContext(
                    streams, tableSchemas, codec, snapshotCodecs, options, monotonicity, frontiers, shareable);
                queryStream = CompilePlan(builder, plan, ctx);
            }

            // Opt-in stored output: integrate the final delta at the boundary so the
            // full materialized view is retained (and snapshot-persisted) for a
            // truncate-mode sink. The delta still flows through the returned stream,
            // so the plain OutputHandle is unchanged. See docs/design-stored-output.md.
            if (options.StoredOutput)
            {
                var viewCodec = snapshotCodecs?.CreateZSetTraceCodec(plan.Schema);
                var integrated = builder.Integrate(queryStream, viewCodec);
                queryStream = integrated.Output;
                view = integrated.View;
            }

            output = builder.Output(queryStream);
        });
        }
        finally
        {
            SpineStagingConfig.Capacity = prevStagingCapacity;
        }

        return new CompiledQuery(circuit!, inputs!, output!, plan.Schema, view);
    }

    /// <summary>
    /// Compile a whole program — <paramref name="tables"/> (<c>CREATE TABLE</c> sources)
    /// and <paramref name="views"/> (<c>CREATE VIEW</c>, in dependency order) — into a
    /// single circuit. Each table gets an input handle + stream; each view is compiled
    /// against the shared streams of the tables/views it references (so a view is computed
    /// once and shared by all its consumers), and each output view is integrated to a
    /// materialised <see cref="ProgramOutput"/>. Structural compile path (the typed fast
    /// path is single-query only); see <see cref="CompiledProgram"/>.
    /// </summary>
    public static CompiledProgram CompileProgram(
        IReadOnlyList<CreateTablePlan> tables,
        IReadOnlyList<ProgramView> views,
        ISqlSnapshotCodecs? snapshotCodecs = null,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(views);
        options ??= CompileOptions.Default;
        var codec = StructuralRowCodec.Instance;
        var typedViews = new List<string>();
        var fellBackViews = new List<string>();

        // Dead-view elimination: only compile views reachable from an output. A view with no
        // output connector that no output transitively references is never observed, so
        // building its stream is pure waste — and a large such view (e.g. a +stored-but-
        // unwritten fact that fans a fact table out by a dimension version count) can dominate
        // memory. `views` is in dependency order (a view after everything it references), so a
        // single backward pass marks each output and then every name it transitively scans.
        // CollectScans is the same traversal compilation resolves scans through, so the
        // reachable set is exactly what a kept view will look up in `streams`.
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        for (var i = views.Count - 1; i >= 0; i--)
        {
            var v = views[i];
            if (v.IsOutput)
            {
                reachable.Add(v.ViewName);
            }

            if (reachable.Contains(v.ViewName))
            {
                foreach (var referenced in CollectScans(v.Query).Scans.Keys)
                {
                    reachable.Add(referenced);
                }
            }
        }

        // Program-level dead-column liveness (opt-in): per-view live output columns,
        // used below to prune each view's plan to the columns some output / live view
        // reads (docs/design-column-liveness.md).
        var liveColumns = options.EliminateDeadColumns
            ? DbspNet.Sql.Optimizer.PlanColumnLiveness.ComputeProgramLiveColumns(views)
            : null;

        RootCircuit? circuit = null;
        Dictionary<string, TableInput>? inputs = null;
        Dictionary<string, ProgramOutput>? outputs = null;

        var emptyFrontiers = (IReadOnlyDictionary<LatenessSource, IFrontier>)
            new Dictionary<LatenessSource, IFrontier>();
        var emptyShareable = (IReadOnlySet<ArrangementKey>)
            System.Collections.Immutable.ImmutableHashSet<ArrangementKey>.Empty;

        var prevStagingCapacity = SpineStagingConfig.Capacity;
        SpineStagingConfig.Capacity = options.TraceFamily == TraceFamily.Spine ? options.SpineStagingCapacity : 0;
        try
        {
            circuit = RootCircuit.Build(builder =>
            {
                // Environment: name → circuit stream (tables first, then each view as it
                // is compiled). Scans of a name resolve here, so a view reference is wired
                // to that view's already-built stream — not a fresh input.
                var streams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
                var schemas = new Dictionary<string, Schema>(StringComparer.Ordinal);
                inputs = new Dictionary<string, TableInput>(StringComparer.Ordinal);
                outputs = new Dictionary<string, ProgramOutput>(StringComparer.Ordinal);

                foreach (var t in tables)
                {
                    var (handle, stream) = builder.ZSetInput<StructuralRow, Z64>();
                    inputs[t.TableName] = new TableInput(handle, t.Schema, codec);
                    streams[t.TableName] = stream;
                    schemas[t.TableName] = t.Schema;

                    // NOTE (Phase A): declared LATENESS is not enforced across the program
                    // yet — state is unbounded (sound, just not GC'd), as for unpartitioned
                    // ranks. Cross-view frontier wiring is a follow-on.
                }

                foreach (var v in views)
                {
                    // Skip dead views (no output depends on them) — never built, never stepped.
                    if (!reachable.Contains(v.ViewName))
                    {
                        continue;
                    }

                    // Optimize each view's plan (filter pushdown, projection/column
                    // pruning, semi-join narrowing, …) — the single-query path does
                    // this via its callers; the program path did not, so every view
                    // compiled un-optimized. Optimize is result- and output-schema-
                    // preserving, and each view is a standalone tree whose scans of
                    // other views are untouched leaves, so per-view optimization is
                    // safe across the shared-stream DAG.
                    // Prune columns no live consumer reads BEFORE per-view optimize,
                    // so a narrowed output lets the join/aggregate rules push further.
                    // Arity-preserving, so a fell-back or downstream view's column
                    // indices into this view's stream are unchanged.
                    var viewQuery = v.Query;
                    if (liveColumns is not null
                        && liveColumns.TryGetValue(v.ViewName, out var liveOut))
                    {
                        viewQuery = DbspNet.Sql.Optimizer.PlanColumnLiveness.PruneDeadColumns(viewQuery, liveOut);
                    }

                    var optimizedQuery = DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(viewQuery);
                    var monotonicity = MonotonicityAnalyzer.Analyze(optimizedQuery);
                    var ctx = new CompileContext(
                        streams, schemas, codec, snapshotCodecs, options, monotonicity, emptyFrontiers, emptyShareable);
                    // Tag every operator this view creates with its name, so a runtime
                    // profile can attribute per-operator cost to the view (observability).
                    builder.BuildLabel = v.ViewName;

                    // Measurement gate (docs/design-row-representation.md): try the typed
                    // fast path per view, structural elsewhere. A typed view lifts its
                    // scans from the shared StructuralRow streams, runs its inner ops
                    // typed, and adapts its output back to StructuralRow — so the shared
                    // inter-view streams are byte-identical to the structural path and a
                    // fell-back view downstream is unaffected. Forced off with CSE
                    // (structural-only) and when a non-default codec is in play.
                    Stream<ZSet<StructuralRow, Z64>>? stream = null;
                    if (options.TypeEligibleProgramViews
                        && !options.ShareArrangements
                        && ReferenceEquals(codec, StructuralRowCodec.Instance))
                    {
                        stream = TypedPlanCompiler.TryCompileWithStructuralBoundary(
                            builder, optimizedQuery, streams, codec, snapshotCodecs, options);
                    }

                    if (options.TypeEligibleProgramViews)
                    {
                        (stream is not null ? typedViews : fellBackViews).Add(v.ViewName);
                    }

                    stream ??= CompilePlan(builder, optimizedQuery, ctx);
                    streams[v.ViewName] = stream;
                    schemas[v.ViewName] = v.Query.Schema;

                    if (v.IsOutput)
                    {
                        var viewCodec = snapshotCodecs?.CreateZSetTraceCodec(v.Query.Schema);
                        var integrated = builder.Integrate(stream, viewCodec);
                        outputs[v.ViewName] = new ProgramOutput(v.Query.Schema, integrated.View);
                    }
                }
            });
        }
        finally
        {
            SpineStagingConfig.Capacity = prevStagingCapacity;
        }

        if (options.TypeEligibleProgramViews)
        {
            LastProgramTypedTally = (typedViews, fellBackViews);
        }

        return new CompiledProgram(circuit!, inputs!, outputs!);
    }

    /// <summary>
    /// Data-parallel compile of a whole multi-view program onto a
    /// <see cref="ParallelCircuit"/> of <paramref name="workers"/> replicas — the
    /// program analogue of <see cref="TryCompileParallel"/> and the Increment 2
    /// entry point (docs/design-structural-parallel.md §5). Every reachable view
    /// runs the exchange-inserting structural compile; source tables are sharded
    /// by whole-row hash and each output view's delta is gathered (Z-set sum) and
    /// integrated on the driver. Returns <c>false</c> (caller falls back to the
    /// serial <see cref="CompileProgram"/>) when <em>any</em> reachable view uses a
    /// construct the pass cannot shard soundly — the whole program shares one
    /// circuit and one worker count, so a single un-shardable view forces serial.
    /// </summary>
    public static bool TryCompileProgramParallel(
        IReadOnlyList<CreateTablePlan> tables,
        IReadOnlyList<ProgramView> views,
        int workers,
        out ParallelCompiledProgram? compiled,
        CompileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(views);
        ArgumentOutOfRangeException.ThrowIfLessThan(workers, 1);
        compiled = null;
        options ??= CompileOptions.Default;
        var codec = StructuralRowCodec.Instance;

        // Dead-view elimination (identical to CompileProgram): only compile views
        // reachable from an output.
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        for (var i = views.Count - 1; i >= 0; i--)
        {
            var v = views[i];
            if (v.IsOutput)
            {
                reachable.Add(v.ViewName);
            }

            if (reachable.Contains(v.ViewName))
            {
                foreach (var referenced in CollectScans(v.Query).Scans.Keys)
                {
                    reachable.Add(referenced);
                }
            }
        }

        var liveColumns = options.EliminateDeadColumns
            ? DbspNet.Sql.Optimizer.PlanColumnLiveness.ComputeProgramLiveColumns(views)
            : null;

        // Pre-pass: optimize each reachable view ONCE (the build closure runs W
        // times, so optimizing inside it would repeat the work), refuse the whole
        // parallel program if any reachable view is not shardable, and — for the
        // broadcast-join size gate — estimate each view's row count from the
        // deployment-supplied base-table counts, in dependency order so a view's
        // estimate can consult its already-estimated inputs.
        var rowCounts = new Dictionary<string, long>(StringComparer.Ordinal);
        if (options.RelationRowCounts is { } supplied)
        {
            foreach (var (name, count) in supplied)
            {
                rowCounts[name] = count;
            }
        }

        long? CountLookup(string name) => rowCounts.TryGetValue(name, out var n) ? n : (long?)null;

        var prepared = new List<(ProgramView View, LogicalPlan Optimized)>();
        foreach (var v in views)
        {
            if (!reachable.Contains(v.ViewName))
            {
                continue;
            }

            var viewQuery = v.Query;
            if (liveColumns is not null && liveColumns.TryGetValue(v.ViewName, out var liveOut))
            {
                viewQuery = DbspNet.Sql.Optimizer.PlanColumnLiveness.PruneDeadColumns(viewQuery, liveOut);
            }

            var optimized = DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(viewQuery);
            if (!CanCompileParallel(optimized))
            {
                return false;
            }

            if (options.BroadcastMaxRows > 0)
            {
                rowCounts[v.ViewName] = CardinalityEstimator.Estimate(optimized, CountLookup);
            }

            prepared.Add((v, optimized));
        }

        var emptyFrontiers = (IReadOnlyDictionary<LatenessSource, IFrontier>)
            new Dictionary<LatenessSource, IFrontier>();
        var emptyShareable = (IReadOnlySet<ArrangementKey>)
            System.Collections.Immutable.ImmutableHashSet<ArrangementKey>.Empty;

        var inputSchemas = new Dictionary<string, Schema>(StringComparer.Ordinal);
        var outputSchemas = new Dictionary<string, Schema>(StringComparer.Ordinal);

        var prevStagingCapacity = SpineStagingConfig.Capacity;
        SpineStagingConfig.Capacity = options.TraceFamily == TraceFamily.Spine ? options.SpineStagingCapacity : 0;
        ParallelCircuit circuit;
        try
        {
            circuit = ParallelCircuit.Build(workers, builder =>
            {
                var streams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
                var schemas = new Dictionary<string, Schema>(StringComparer.Ordinal);

                foreach (var t in tables)
                {
                    var (_, stream) = builder.ZSetInput<StructuralRow, Z64>(t.TableName);
                    streams[t.TableName] = stream;
                    schemas[t.TableName] = t.Schema;
                    inputSchemas[t.TableName] = t.Schema;
                }

                foreach (var (v, optimized) in prepared)
                {
                    var monotonicity = MonotonicityAnalyzer.Analyze(optimized);
                    var ctx = new CompileContext(
                        streams, schemas, codec, snapshotCodecs: null, options, monotonicity,
                        emptyFrontiers, emptyShareable, workers, rowCounts);
                    // The scan streams (this program's tables) are whole-row-hashed
                    // inputs → shard-disjoint. A view-scan resolves to that view's
                    // already-built stream, whose partition state was recorded when
                    // it was compiled.
                    foreach (var t in tables)
                    {
                        ctx.SetPartition(streams[t.TableName], new PartitionInfo(ShardDisjoint: true, PartitionKey: null));
                    }

                    builder.BuildLabel = v.ViewName;
                    var stream = CompilePlan(builder, optimized, ctx);
                    streams[v.ViewName] = stream;
                    schemas[v.ViewName] = v.Query.Schema;

                    if (v.IsOutput)
                    {
                        builder.Output(stream, "view:" + v.ViewName);
                        outputSchemas[v.ViewName] = v.Query.Schema;
                    }
                }
            });
        }
        finally
        {
            SpineStagingConfig.Capacity = prevStagingCapacity;
        }

        var inputs = new Dictionary<string, ShardedTableInput>(StringComparer.Ordinal);
        foreach (var (name, schema) in inputSchemas)
        {
            var handle = circuit.ShardedInput<StructuralRow, Z64>(
                name, row => StablePartitionHash.OfWholeRow(row, schema));
            inputs[name] = new ShardedTableInput(handle, schema, codec);
        }

        var outputs = new Dictionary<string, ParallelProgramOutput>(StringComparer.Ordinal);
        foreach (var (name, schema) in outputSchemas)
        {
            var handle = circuit.ShardedOutput<StructuralRow, Z64>("view:" + name);
            outputs[name] = new ParallelProgramOutput(schema, handle);
        }

        compiled = new ParallelCompiledProgram(circuit, inputs, outputs);
        return true;
    }

    /// <summary>
    /// Diagnostic (measurement only): the typed vs fell-back program-view split
    /// from the most recent <see cref="CompileProgram"/> call made with
    /// <see cref="CompileOptions.TypeEligibleProgramViews"/> set. Lets the local
    /// harness confirm the gate typed exactly the views the coverage census
    /// predicted. Not thread-safe; not part of the runtime contract.
    /// </summary>
    public static (IReadOnlyList<string> Typed, IReadOnlyList<string> FellBack) LastProgramTypedTally
    {
        get;
        private set;
    } = (Array.Empty<string>(), Array.Empty<string>());

    /// <summary>
    /// Data-parallel partition state a compiled stream carries (only meaningful
    /// when <see cref="CompileContext.Workers"/> &gt; 1).
    /// </summary>
    /// <param name="ShardDisjoint">
    /// True when the per-worker shards are provably key-disjoint — the stream is
    /// partitioned by columns it still carries by identity, so equal rows always
    /// land on one worker. Conservatively false when unknown.
    /// </param>
    /// <param name="PartitionKey">
    /// The column indices (into the stream's own schema) the stream is
    /// hash-partitioned by: any two rows agreeing on these columns are co-located.
    /// <c>null</c> means "not usefully partitioned". Used to elide a redundant
    /// aggregate exchange via <see cref="IsKeySubset"/>.
    /// </param>
    private readonly record struct PartitionInfo(bool ShardDisjoint, int[]? PartitionKey);

    /// <summary>
    /// True when every column in <paramref name="sub"/> also appears in
    /// <paramref name="super"/> — i.e. the data's partition key is a subset of the
    /// operator's key, so each operator-key group already sits on one worker and
    /// the operator's re-shuffle can be elided. Ported verbatim from
    /// <c>TypedPlanCompiler.IsKeySubset</c>.
    /// </summary>
    private static bool IsKeySubset(int[] sub, int[] super)
    {
        foreach (var c in sub)
        {
            if (Array.IndexOf(super, c) < 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether every node in <paramref name="plan"/> is one the exchange-insertion
    /// pass can shard soundly. A key-sensitive node is admissible only when its
    /// shard key is hashable (<see cref="IsHashableType"/>) and, for windows /
    /// top-K / rank, a PARTITION BY exists (a global window has no key to shard
    /// on). Everything not explicitly handled — a broadcast / correlated /
    /// scalar-subquery join, a semi-join, a set difference, a recursive CTE, a
    /// temporal filter, a non-inner join, a global top-K — is refused so the
    /// caller falls back to the serial single circuit.
    /// </summary>
    private static bool CanCompileParallel(LogicalPlan plan)
    {
        switch (plan)
        {
            case ScanPlan:
                return true;

            case CteScanPlan c:
                return CanCompileParallel(c.Cte.Plan);

            case FilterPlan f:
                return CanCompileParallel(f.Input);

            case ProjectPlan p:
                return CanCompileParallel(p.Input);

            case JoinPlan j:
            {
                if (j.JoinType != DbspNet.Sql.Parser.Ast.JoinType.Inner)
                {
                    return false;
                }

                var (leftIndices, rightIndices) = ExtractEquiKeyIndices(j);
                if (leftIndices.Length == 0)
                {
                    return false; // a cross join has no equi-key to shard on.
                }

                for (var k = 0; k < leftIndices.Length; k++)
                {
                    if (!IsHashableType(j.Left.Schema[leftIndices[k]].Type)
                        || !IsHashableType(j.Right.Schema[rightIndices[k]].Type))
                    {
                        return false;
                    }
                }

                return CanCompileParallel(j.Left) && CanCompileParallel(j.Right);
            }

            case AggregatePlan a:
                foreach (var gk in a.GroupKeys)
                {
                    if (!IsHashableType(gk.Type))
                    {
                        return false;
                    }
                }

                return CanCompileParallel(a.Input);

            case UnionAllPlan u:
                foreach (var b in u.Branches)
                {
                    if (!CanCompileParallel(b))
                    {
                        return false;
                    }
                }

                return true;

            case DistinctPlan d:
                foreach (var col in d.Schema.Columns)
                {
                    if (!IsHashableType(col.Type))
                    {
                        return false;
                    }
                }

                return CanCompileParallel(d.Input);

            case PartitionedTopKPlan pt:
                return pt.PartitionKeys.Count > 0 && AllHashable(pt.PartitionKeys) && CanCompileParallel(pt.Input);

            case PartitionedRankPlan pr:
                return pr.PartitionKeys.Count > 0 && AllHashable(pr.PartitionKeys) && CanCompileParallel(pr.Input);

            case WindowAggregatePlan wa:
                return wa.PartitionKeys.Count > 0 && AllHashable(wa.PartitionKeys) && CanCompileParallel(wa.Input);

            case WindowOffsetPlan wo:
                return wo.PartitionKeys.Count > 0 && AllHashable(wo.PartitionKeys) && CanCompileParallel(wo.Input);

            default:
                return false;
        }
    }

    /// <summary>
    /// True when <paramref name="plan"/> is a plain relation scan — a base table
    /// or a referenced view (through any number of Filter/Project wrappers), i.e. a
    /// dimension-shaped leaf rather than a join / aggregate subtree. The
    /// broadcast-join heuristic replicates such a right side (the star-schema build
    /// side, conventionally the smaller relation) instead of hash-sharding it.
    /// </summary>
    private static bool IsLeafRelation(LogicalPlan plan, out string? relation)
    {
        relation = null;
        while (true)
        {
            switch (plan)
            {
                case FilterPlan f:
                    plan = f.Input;
                    break;
                case ProjectPlan p:
                    plan = p.Input;
                    break;
                case ScanPlan s:
                    relation = s.TableName;
                    return true;
                case CteScanPlan c:
                    relation = c.Cte.Name;
                    return true;
                default:
                    return false;
            }
        }
    }

    /// <summary>
    /// Whether an INNER join with right side <paramref name="right"/> should be
    /// compiled as a broadcast join (replicate the right dimension) rather than a
    /// hash join. True only at W&gt;1 when the right is a leaf relation and either
    /// the unconditional experimental override
    /// (<see cref="CompileOptions.BroadcastSmallDimensionJoins"/>) is set, or the
    /// production size gate (<see cref="CompileOptions.BroadcastMaxRows"/>) is set
    /// and the right's estimated row count is known and within it. An unknown size
    /// is treated as large ⇒ hash join, so a broadcast is never made blindly.
    /// </summary>
    private static bool ShouldBroadcastRight(LogicalPlan right, CompileContext ctx)
    {
        if (ctx.Workers <= 1 || !IsLeafRelation(right, out var relation))
        {
            return false;
        }

        if (ctx.Options.BroadcastSmallDimensionJoins)
        {
            return true;
        }

        if (ctx.Options.BroadcastMaxRows <= 0 || relation is null || ctx.RowCounts is null)
        {
            return false;
        }

        var estimate = CardinalityEstimator.Estimate(
            right, name => ctx.RowCounts.TryGetValue(name, out var n) ? n : (long?)null);
        return estimate <= ctx.Options.BroadcastMaxRows;
    }

    private static bool AllHashable(IReadOnlyList<ResolvedExpression> keys)
    {
        foreach (var k in keys)
        {
            if (!IsHashableType(k.Type))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether a column of this SQL type has a stable partition hash
    /// (<see cref="StablePartitionHash.OfBoxed"/>). REAL, INTERVAL and the untyped
    /// NULL literal have none — a stream keyed on one cannot be sharded
    /// deterministically, mirroring the typed compiler's refuse-parallel guard.
    /// </summary>
    private static bool IsHashableType(SqlType type) => type switch
    {
        SqlIntegerType or SqlBigintType or SqlBooleanType or SqlDoubleType
            or SqlDecimalType or SqlVarcharType or SqlDateType or SqlTimeType
            or SqlTimestampType => true,
        _ => false,
    };

    private sealed class CompileContext
    {
        public CompileContext(
            IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> scans,
            IReadOnlyDictionary<string, Schema> tableSchemas,
            IRowCodec<StructuralRow> codec,
            ISqlSnapshotCodecs? snapshotCodecs,
            CompileOptions options,
            MonotonicityInfo monotonicity,
            IReadOnlyDictionary<LatenessSource, IFrontier> frontiers,
            IReadOnlySet<ArrangementKey> shareableArrangements,
            int workers = 1,
            IReadOnlyDictionary<string, long>? rowCounts = null)
        {
            Scans = scans;
            TableSchemas = tableSchemas;
            Codec = codec;
            SnapshotCodecs = snapshotCodecs;
            Options = options;
            Monotonicity = monotonicity;
            Frontiers = frontiers;
            ShareableArrangements = shareableArrangements;
            Workers = workers;
            RowCounts = rowCounts;
        }

        /// <summary>
        /// Estimated row counts (base relations supplied by the deployment, plus
        /// per-view estimates the program compile derives) for the broadcast-join
        /// size gate. Null when no counts were supplied.
        /// </summary>
        public IReadOnlyDictionary<string, long>? RowCounts { get; }

        /// <summary>
        /// Replica count W when this compile targets a <see cref="ParallelCircuit"/>;
        /// 1 for a plain single circuit. When &gt; 1 the key-sensitive operators
        /// (join / aggregate / distinct / partitioned window / partitioned top-K)
        /// re-shard by their key so equal keys co-locate on one worker; at W&lt;=1
        /// every <c>Exchange*</c> the compiler emits degrades to the identical
        /// <c>GroupProject</c> it emitted before parallelism (a free structural
        /// regression guard — see docs/design-structural-parallel.md §1).
        /// </summary>
        public int Workers { get; }

        /// <summary>
        /// Per-stream partition metadata, keyed by stream identity. An
        /// <c>Exchange*</c> records the shuffle key here; a downstream aggregate
        /// consults it to elide a redundant re-shuffle when the data already
        /// co-locates each of its groups (<see cref="IsKeySubset"/>). Only
        /// consulted when <see cref="Workers"/> &gt; 1; empty / <c>default</c>
        /// (unpartitioned) is always the safe answer.
        /// </summary>
        private readonly Dictionary<Stream<ZSet<StructuralRow, Z64>>, PartitionInfo> _partition =
            new(ReferenceEqualityComparer.Instance);

        /// <summary>Record the partition state a compiled stream carries.</summary>
        public void SetPartition(Stream<ZSet<StructuralRow, Z64>> stream, PartitionInfo info) =>
            _partition[stream] = info;

        /// <summary>
        /// The partition state of a compiled stream, or the unpartitioned default
        /// (<c>ShardDisjoint=false, PartitionKey=null</c>) if none was recorded.
        /// </summary>
        public PartitionInfo GetPartition(Stream<ZSet<StructuralRow, Z64>> stream) =>
            _partition.TryGetValue(stream, out var info) ? info : default;

        /// <summary>Stream per declared table — the circuit's inputs.</summary>
        public IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> Scans { get; }

        /// <summary>
        /// Schema per declared base table — used when constructing per-table
        /// snapshot codecs at compile time (e.g. a recursive CTE's imported
        /// base tables in <see cref="CompileRecursiveCteFixpoint"/>).
        /// </summary>
        public IReadOnlyDictionary<string, Schema> TableSchemas { get; }

        /// <summary>
        /// Per-CTE compiled stream cache. The first <see cref="CteScanPlan"/>
        /// encountered for a given <see cref="CteRef"/> compiles its
        /// underlying subplan; every subsequent scan returns the cached stream.
        /// </summary>
        public Dictionary<CteRef, Stream<ZSet<StructuralRow, Z64>>> CteCache { get; } = new();

        /// <summary>
        /// Arrangement keys (relation + right key columns) that ≥2 INNER joins
        /// share, found by <see cref="CollectShareableArrangements"/>. Empty
        /// unless <see cref="CompileOptions.ShareArrangements"/> is set.
        /// </summary>
        public IReadOnlySet<ArrangementKey> ShareableArrangements { get; }

        /// <summary>
        /// Per-shared-arrangement cache: the first INNER join that references a
        /// shareable (relation, key) builds the indexed delta stream and the
        /// shared <c>Arrange</c>/<c>SpineArrange</c>; subsequent joins reuse them.
        /// </summary>
        public Dictionary<ArrangementKey, SharedArrangement> ArrangementCache { get; } = new();

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

        /// <summary>
        /// One frontier per GC source: a declared LATENESS column (advanced by its
        /// input operator) or a temporal filter's time-key (clock-driven).
        /// </summary>
        public IReadOnlyDictionary<LatenessSource, IFrontier> Frontiers { get; }
    }

    /// <summary>The clock-driven GC frontier transform a temporal filter
    /// advertises on its source column: the µs logical clock mapped into that
    /// column's value space, shifted down by the disappear offset.
    /// <list type="bullet">
    /// <item>TIMESTAMP key over a TIMESTAMP column: raw clock minus the µs
    /// offset.</item>
    /// <item>DATE key over a DATE column: clock floored to its day-number minus
    /// the whole-day offset — matching <see cref="MonotoneKey.Extract"/>, which
    /// reads a DATE column as its day-number.</item>
    /// <item><c>CAST(ts AS DATE)</c> key (<paramref name="castTimestampToDate"/>):
    /// the source column is the µs <em>timestamp</em>, so the day-space frontier
    /// is scaled back to the <em>midnight µs</em> of that day. This is a sound
    /// lower bound on any timestamp the filter can still emit (conservative by at
    /// most one day), so downstream GC never drops a live row.</item>
    /// </list>
    /// <see cref="TransformedFrontier"/> passes the unset sentinel
    /// (<see cref="long.MinValue"/>) through before this runs.</summary>
    private static Func<long, long> ClockOffsetTransform(
        TemporalClock clock, long offset, bool castTimestampToDate = false)
    {
        if (castTimestampToDate)
        {
            // Day-space frontier (day-number) → midnight µs of that day.
            return v => SaturatingMulDay(SaturatingSub(Date32.DayNumberFloor(v), offset));
        }

        return clock == TemporalClock.Date
            ? v => SaturatingSub(Date32.DayNumberFloor(v), offset)
            : v => SaturatingSub(v, offset);
    }

    private static long SaturatingSub(long v, long offset)
    {
        var r = unchecked(v - offset);
        if (((v ^ offset) & (v ^ r)) < 0)
        {
            return offset > 0 ? long.MinValue : long.MaxValue;
        }

        return r;
    }

    /// <summary>Saturating <c>dayNumber × µs_per_day</c> (midnight µs of a day),
    /// for mapping a day-space frontier back into a µs timestamp column's space.</summary>
    private static long SaturatingMulDay(long dayNumber)
    {
        const long Day = Interval.MicrosPerDay;
        if (dayNumber > long.MaxValue / Day)
        {
            return long.MaxValue;
        }

        if (dayNumber < long.MinValue / Day)
        {
            return long.MinValue;
        }

        return dayNumber * Day;
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
        IZSetTraceCodec<StructuralRow, Z64>? snapshotCodec,
        IFrontier? frontier = null,
        Func<StructuralRow, long>? monotoneKey = null)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineDistinct(
                input, ctx.Options.Compaction, snapshotCodec, StructuralRowComparer.Instance,
                frontier: frontier, monotoneKey: monotoneKey)
            : builder.Distinct(input, snapshotCodec, frontier, monotoneKey);
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
        Func<StructuralRow, long>? monotoneKey = null,
        Func<StructuralRow, bool>? residual = null)
    {
        // The residual (a non-equi ON conjunct spanning both sides, e.g. a
        // temporal-SCD `ts BETWEEN lo AND hi`) is pushed INTO the flat join's
        // cross-product enumeration so rows failing it never enter the output
        // Z-set — the intermediate full product is never materialised. The spine
        // join has no residual hook, so there it stays a post-join filter.
        if (ctx.Options.TraceFamily == TraceFamily.Spine)
        {
            var spineJoined = builder.SpineIncrementalInnerJoin(
                left, right, combine, leftCodec, rightCodec,
                ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                leftValueComparer: StructuralRowComparer.Instance,
                rightValueComparer: StructuralRowComparer.Instance,
                frontier: frontier,
                monotoneKey: monotoneKey);
            return residual is null ? spineJoined : builder.Filter(spineJoined, row => residual(row));
        }

        return builder.IncrementalInnerJoin(
            left, right, combine, leftCodec, rightCodec, frontier, monotoneKey, residual);
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

    private static Stream<ZSet<StructuralRow, Z64>> EmitFullJoin(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> left,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> right,
        Func<StructuralRow, StructuralRow, StructuralRow, StructuralRow> joinCombine,
        Func<StructuralRow, StructuralRow, StructuralRow> nullPadRightCombine,
        Func<StructuralRow, StructuralRow, StructuralRow> nullPadLeftCombine,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? leftCodec,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? rightCodec,
        IFrontier? frontier = null,
        Func<StructuralRow, long>? monotoneKey = null)
    {
        return ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineIncrementalFullJoin(
                left, right, joinCombine, nullPadRightCombine, nullPadLeftCombine, leftCodec, rightCodec,
                ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                leftValueComparer: StructuralRowComparer.Instance,
                rightValueComparer: StructuralRowComparer.Instance,
                frontier: frontier,
                monotoneKey: monotoneKey)
            : builder.IncrementalFullJoin(
                left, right, joinCombine, nullPadRightCombine, nullPadLeftCombine,
                leftCodec, rightCodec, frontier, monotoneKey);
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

            // A monotone-function group key (e.g. date_trunc(ts)) lives in a
            // different value space than its source frontier; pass the bound
            // through the same transform before it thresholds the keys.
            if (ctx.Monotonicity.FrontierTransform(plan, g) is { } transform)
            {
                frontier = new TransformedFrontier(frontier, transform);
            }

            return (frontier, keyRow => MonotoneKey.Extract(keyRow[keyIndex]));
        }

        return (null, null);
    }

    /// <summary>
    /// If the DISTINCT's row carries a monotone column (per the analyzer, which
    /// passes monotonicity through DISTINCT), returns the frontier to GC against
    /// and an extractor for the monotone value from the row. As with the other
    /// GC sites, fires only when every source bounding that column has a frontier
    /// (a partial set would be unsound). GC is safe because the input late-drop
    /// removes sub-frontier rows, so no future delta can resurrect a collected
    /// row (which would otherwise re-emit a spurious +1).
    /// </summary>
    private static (IFrontier? Frontier, Func<StructuralRow, long>? MonotoneKey) ResolveDistinctFrontier(
        DistinctPlan plan, CompileContext ctx)
    {
        for (var c = 0; c < plan.Schema.Count; c++)
        {
            var sources = ctx.Monotonicity.Sources(plan, c);
            if (sources is not { Count: > 0 })
            {
                continue;
            }

            // This site GCs against the raw frontier, so it can only use a column
            // whose value IS the frontier value (identity transform). A
            // transformed monotone column (e.g. date_trunc) is skipped here.
            if (ctx.Monotonicity.FrontierTransform(plan, c) is not null)
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

            var colIndex = c;
            IFrontier frontier = frontiers.Count == 1 ? frontiers[0] : new MinFrontier(frontiers);
            return (frontier, row => MonotoneKey.Extract(row[colIndex]));
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

            // Join GC uses the raw frontier; only identity-transform keys qualify
            // (a transformed monotone key would need its bound transformed, which
            // this site doesn't do).
            if (ctx.Monotonicity.FrontierTransform(plan.Left, eq.LeftIndex) is not null
                || ctx.Monotonicity.FrontierTransform(plan.Right, eq.RightIndex) is not null)
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

    /// <summary>
    /// A clock-driven frontier a temporal filter advertises on the base-scan
    /// source column its time-key reduces to: the column's
    /// <see cref="LatenessSource"/>, the disappear offset, the clock value space
    /// (<see cref="Clock"/>), and whether the key was <c>CAST(ts AS DATE)</c> (so
    /// the source column is the µs timestamp while the filter ran in day units).
    /// The GC frontier is <c>transform(clock) − Offset</c> mapped into the source
    /// column's space (see <see cref="ClockOffsetTransform"/>).
    /// </summary>
    private readonly record struct TemporalFrontierSpec(
        LatenessSource Source, long Offset, TemporalClock Clock, bool CastTimestampToDate);

    private static (Dictionary<string, ScanInfo> Scans, List<TemporalFrontierSpec> Temporal) CollectScans(
        LogicalPlan plan)
    {
        var result = new Dictionary<string, ScanInfo>(StringComparer.Ordinal);
        var temporal = new List<TemporalFrontierSpec>();
        var visitedCtes = new HashSet<CteRef>();
        Walk(plan);
        return (result, temporal);

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
                case TemporalFilterPlan tf:
                    // A disappear-bounded temporal filter whose time-key reduces to
                    // a base-scan column (bare, or CAST(ts AS DATE)) advertises a
                    // clock-driven GC frontier on that source column. Matches the
                    // source MonotonicityAnalyzer marks for the same node.
                    if (tf.DisappearOffset is { } off
                        && tf.Input is ScanPlan tfScan
                        && MonotonicityAnalyzer.TemporalKeySource(tf.TimeKey) is { } src)
                    {
                        temporal.Add(new TemporalFrontierSpec(
                            new LatenessSource(tfScan.TableName, src.Column), off, tf.Clock, src.CastTimestampToDate));
                    }

                    Walk(tf.Input);
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
                case SemiJoinPlan sj:
                    Walk(sj.Input);
                    Walk(sj.Subquery);
                    break;
                case CorrelatedScalarSubqueryJoinPlan csp:
                    Walk(csp.Input);
                    Walk(csp.Subquery);
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
                case TopKPlan t:
                    Walk(t.Input);
                    break;
                case PartitionedTopKPlan pt:
                    Walk(pt.Input);
                    break;
                case PartitionedRankPlan pr:
                    Walk(pr.Input);
                    break;
                case WindowAggregatePlan wa:
                    Walk(wa.Input);
                    break;
                case WindowOffsetPlan wo:
                    Walk(wo.Input);
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

    /// <summary>
    /// Identifies a shareable right-side arrangement: the source relation — a
    /// <see cref="CteRef"/> instance (reference identity) or a base-table name
    /// (value identity) — plus the right equi-key column indices. Two INNER
    /// joins with the same source and key compile to one shared arrangement.
    /// Default record equality does the right thing: <see cref="CteRef"/> has no
    /// value equality so it compares by reference; a table-name string compares
    /// by value; <see cref="KeySig"/> is a string.
    /// </summary>
    private readonly record struct ArrangementKey(object Source, string KeySig);

    /// <summary>
    /// A built shared right arrangement: the indexed delta stream (shared as the
    /// <c>dr</c> side) and the flat <see cref="IArrangement{TKey,TValue,TWeight}"/>
    /// or spine <see cref="ISpineArrangement{TKey,TValue,TWeight}"/> handle (the
    /// <c>R_t</c> side) the shared-right joins read.
    /// </summary>
    private sealed record SharedArrangement(
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> RightIndexed,
        object Arrangement);

    /// <summary>
    /// Finds (relation, right-key) pairs used as the right input of ≥2 INNER
    /// joins, so the compiler builds one shared arrangement for them instead of
    /// a private right trace per join (docs/design-row-representation.md §9.6).
    /// Only bare <see cref="ScanPlan"/> / <see cref="CteScanPlan"/> right inputs
    /// qualify: those compile to a single shared stream
    /// (<see cref="CompileContext.Scans"/> / <see cref="CompileContext.CteCache"/>),
    /// so the arrangement built over the first reference is exactly what the
    /// others need. NULL-accepting (set-op-synthesised) joins are excluded; the
    /// per-join GC-frontier and snapshot guards are applied later at the compile
    /// site. Mirrors the <see cref="CollectScans"/> traversal.
    /// </summary>
    private static IReadOnlySet<ArrangementKey> CollectShareableArrangements(LogicalPlan plan)
    {
        var counts = new Dictionary<ArrangementKey, int>();
        var visitedCtes = new HashSet<CteRef>();
        Walk(plan);

        var shareable = new HashSet<ArrangementKey>();
        foreach (var (key, n) in counts)
        {
            if (n >= 2)
            {
                shareable.Add(key);
            }
        }

        return shareable;

        void Tally(JoinPlan j)
        {
            if (j.JoinType != DbspNet.Sql.Parser.Ast.JoinType.Inner || j.AllowNullKeys)
            {
                return;
            }

            if (RightShareSource(j.Right) is not { } source)
            {
                return;
            }

            var (_, rightIndices) = ExtractEquiKeyIndices(j);
            var key = new ArrangementKey(source, string.Join(",", rightIndices));
            counts[key] = counts.TryGetValue(key, out var c) ? c + 1 : 1;
        }

        void Walk(LogicalPlan p)
        {
            switch (p)
            {
                case ScanPlan:
                    break;
                case CteScanPlan c:
                    if (visitedCtes.Add(c.Cte))
                    {
                        Walk(c.Cte.Plan);
                    }

                    break;
                case FilterPlan f:
                    Walk(f.Input);
                    break;
                case TemporalFilterPlan tf:
                    Walk(tf.Input);
                    break;
                case ProjectPlan pr:
                    Walk(pr.Input);
                    break;
                case JoinPlan j:
                    Tally(j);
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
                case SemiJoinPlan sj:
                    Walk(sj.Input);
                    Walk(sj.Subquery);
                    break;
                case CorrelatedScalarSubqueryJoinPlan csp:
                    Walk(csp.Input);
                    Walk(csp.Subquery);
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
                case TopKPlan t:
                    Walk(t.Input);
                    break;
                case PartitionedTopKPlan pt:
                    Walk(pt.Input);
                    break;
                case PartitionedRankPlan pr:
                    Walk(pr.Input);
                    break;
                case WindowAggregatePlan wa:
                    Walk(wa.Input);
                    break;
                case WindowOffsetPlan wo:
                    Walk(wo.Input);
                    break;
                case DifferencePlan diff:
                    Walk(diff.Left);
                    Walk(diff.Right);
                    break;
                case RecursiveCtePlan r:
                    visitedCtes.Add(r.SelfRef);
                    Walk(r.BasePlan);
                    Walk(r.RecursivePlan);
                    break;
                default:
                    // Unknown node: don't recurse. Missing a nested join only
                    // forgoes an optimization; it is never a correctness issue.
                    break;
            }
        }
    }

    private static object? RightShareSource(LogicalPlan right) => right switch
    {
        CteScanPlan c => c.Cte,
        ScanPlan s => s.TableName,
        _ => null,
    };

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
                    // Optimize the CTE body (the top-level view/query plan is
                    // optimized by its caller, but its CTE bodies are CteScan
                    // leaves the optimizer doesn't descend into — so much of a
                    // WITH-heavy query's logic, e.g. an EXISTS/NOT-EXISTS body,
                    // would compile un-optimized). Optimize is a pass-through on
                    // recursive plans, so a recursive CTE body is left intact.
                    cached = CompilePlan(
                        builder, DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(c.Cte.Plan), ctx);
                    ctx.CteCache[c.Cte] = cached;
                }

                return cached;

            case FilterPlan:
            case ProjectPlan:
                // Both lower to pointwise linear passes; fuse a maximal chain of
                // consecutive Filter/Project nodes into one Apply.
                return CompileLinearChain(builder, plan, ctx);

            case TemporalFilterPlan tf:
                return CompileTemporalFilter(builder, tf, ctx);

            case JoinPlan j:
                return CompileJoin(builder, j, ctx);

            case AggregatePlan a:
                return CompileAggregate(builder, a, ctx);

            case ScalarSubqueryJoinPlan s:
                return CompileScalarSubqueryJoin(builder, s, ctx);

            case SemiJoinPlan sj:
                return CompileSemiJoin(builder, sj, ctx);

            case CorrelatedScalarSubqueryJoinPlan csp:
                return CompileCorrelatedScalarSubqueryJoin(builder, csp, ctx);

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
                    var (gcFrontier, gcMonotoneKey) = ResolveDistinctFrontier(d, ctx);
                    var distinctInput = CompilePlan(builder, d.Input, ctx);

                    // Parallel: re-shard by the whole row (DISTINCT's key) so
                    // identical rows co-locate and dedup on one worker; the gather
                    // then unions the disjoint survivors. `Exchange` is an
                    // identity passthrough at W<=1, so W=1 is byte-identical.
                    if (ctx.Workers > 1)
                    {
                        var distinctSchema = d.Schema;
                        distinctInput = builder.Exchange(
                            distinctInput,
                            row => StablePartitionHash.OfWholeRow(row, distinctSchema));
                    }

                    return EmitDistinct(
                        builder, ctx, distinctInput, distinctCodec,
                        gcFrontier, gcMonotoneKey);
                }

            case DifferencePlan diff:
                {
                    var l = CompilePlan(builder, diff.Left, ctx);
                    var r = CompilePlan(builder, diff.Right, ctx);
                    return builder.Difference(l, r);
                }

            case RecursiveCtePlan rcp:
                return CompileRecursiveCte(builder, rcp, ctx);

            case TopKPlan t:
                return CompileTopK(builder, t, ctx);

            case PartitionedTopKPlan pt:
                return CompilePartitionedTopK(builder, pt, ctx);

            case PartitionedRankPlan pr:
                return CompilePartitionedRank(builder, pr, ctx);

            case WindowAggregatePlan wa:
                return CompileWindowAggregate(builder, wa, ctx);

            case WindowOffsetPlan wo:
                return CompileWindowOffset(builder, wo, ctx);

            default:
                throw new InvalidOperationException($"unsupported plan node {plan.GetType().Name}");
        }
    }

    // ---- Temporal filter (NOW() / advancing-clock predicate) ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileTemporalFilter(
        CircuitBuilder builder,
        TemporalFilterPlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        // Time-key extraction and the clock both live in the unit fixed by
        // plan.Clock; the operator is unit-agnostic (it only compares longs), so
        // the whole DATE case is just a different key carrier + a day-floored
        // clock — no operator change. A NULL key maps to null (never valid).
        var timeKeyScalar = ExpressionCompiler.CompileScalar(plan.TimeKey);
        Func<StructuralRow, long?> timeKey;
        IFrontier clock;
        if (plan.Clock == TemporalClock.Date)
        {
            // DATE key: day-number; clock floored to its day (CURRENT_DATE).
            timeKey = row => timeKeyScalar(row) is Date32 d ? d.Days : null;
            clock = new TransformedFrontier(builder.LogicalClock, Date32.DayNumberFloor);
        }
        else
        {
            // TIMESTAMP key: microseconds since epoch; raw µs clock.
            timeKey = row => timeKeyScalar(row) is Timestamp ts ? ts.Microseconds : null;
            clock = builder.LogicalClock;
        }

        var codec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(plan.Schema);
        return builder.TemporalFilter(
            input,
            timeKey,
            plan.AppearOffset,
            plan.AppearInclusive,
            plan.DisappearOffset,
            plan.DisappearInclusive,
            clock,
            codec);
    }

    // ---- TOP-K (ORDER BY ... LIMIT/OFFSET) ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileTopK(
        CircuitBuilder builder,
        TopKPlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);
        var comparer = BuildStructuralSortComparer(plan.SortKeys);
        var codec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(plan.Schema);
        return builder.TopK(input, comparer, plan.Offset ?? 0, plan.Limit, codec);
    }

    private static IComparer<StructuralRow> BuildStructuralSortComparer(IReadOnlyList<SortKey> sortKeys)
    {
        var keys = new Func<StructuralRow, object?>[sortKeys.Count];
        var descending = new bool[sortKeys.Count];
        var nullsFirst = new bool[sortKeys.Count];
        for (var i = 0; i < sortKeys.Count; i++)
        {
            // StructuralRow is IReadOnlyList<object?>, which CompileScalar consumes directly.
            var scalar = ExpressionCompiler.CompileScalar(sortKeys[i].Expression);
            keys[i] = row => scalar(row);
            descending[i] = sortKeys[i].Descending;
            nullsFirst[i] = sortKeys[i].NullsFirst;
        }

        // Full-row tiebreak gives a total order so the window boundary is stable.
        return new SortKeyComparer<StructuralRow>(keys, descending, nullsFirst, StructuralRowComparer.Instance);
    }

    // ---- Partitioned TOP-K (windowed ROW_NUMBER / RANK / DENSE_RANK <= k) ----

    private static Stream<ZSet<StructuralRow, Z64>> CompilePartitionedTopK(
        CircuitBuilder builder,
        PartitionedTopKPlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        var keys = new Func<StructuralRow, object?>[plan.SortKeys.Count];
        var descending = new bool[plan.SortKeys.Count];
        var nullsFirst = new bool[plan.SortKeys.Count];
        for (var i = 0; i < plan.SortKeys.Count; i++)
        {
            var scalar = ExpressionCompiler.CompileScalar(plan.SortKeys[i].Expression);
            keys[i] = row => scalar(row);
            descending[i] = plan.SortKeys[i].Descending;
            nullsFirst[i] = plan.SortKeys[i].NullsFirst;
        }

        // `order` is the total order keeping the per-partition trace sorted;
        // `sortKeyOnly` (zero tiebreak) detects equal-ORDER-BY-key tie groups for
        // RANK / DENSE_RANK.
        var order = new SortKeyComparer<StructuralRow>(
            keys, descending, nullsFirst, StructuralRowComparer.Instance);
        var sortKeyOnly = new SortKeyComparer<StructuralRow>(
            keys, descending, nullsFirst, ConstantZeroComparer<StructuralRow>.Instance);

        // Partition key: project the PARTITION BY expressions into a StructuralRow
        // (an empty list ⇒ a constant key ⇒ a single global partition).
        var partitionExtractors = new Func<IReadOnlyList<object?>, object?>[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            partitionExtractors[i] = ExpressionCompiler.CompileScalar(plan.PartitionKeys[i]);
        }

        StructuralRow PartitionOf(StructuralRow row)
        {
            var values = new object?[partitionExtractors.Length];
            for (var i = 0; i < partitionExtractors.Length; i++)
            {
                values[i] = partitionExtractors[i](row);
            }

            return new StructuralRow(values);
        }

        // Parallel: co-locate each PARTITION BY group on one worker before the
        // per-partition operator (identity passthrough at W<=1). A global (no
        // PARTITION BY) window is refused upstream by CanCompileParallel, so the
        // partition list is non-empty whenever this fires.
        if (ctx.Workers > 1 && plan.PartitionKeys.Count > 0)
        {
            var partitionSchema = SyntheticKeySchema(plan.PartitionKeys);
            input = builder.Exchange(
                input, row => StablePartitionHash.OfWholeRow(PartitionOf(row), partitionSchema));
        }

        // §22 narrow-key path: pass the single ORDER BY extractor so the operator can
        // key its trace by {order value, wide row} when the narrowing seam is on. Only
        // the single-column shape (q18/q19) is plumbed; multi-column ORDER BY falls back
        // to the whole-row operator. Harmless when the seam is off.
        Func<StructuralRow, object?>? orderKey = null;
        var orderDescending = false;
        var orderNullsFirst = false;
        if (plan.SortKeys.Count == 1)
        {
            orderKey = keys[0];
            orderDescending = descending[0];
            orderNullsFirst = nullsFirst[0];
        }

        var codec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(plan.Schema);
        return builder.PartitionedTopK<StructuralRow, StructuralRow>(
            input, PartitionOf, order, sortKeyOnly, plan.Function, plan.Limit, null, codec,
            orderKey, orderDescending, orderNullsFirst);
    }

    // ---- Rank-in-output (ROW_NUMBER / RANK / DENSE_RANK as an output column) ----

    private static Stream<ZSet<StructuralRow, Z64>> CompilePartitionedRank(
        CircuitBuilder builder,
        PartitionedRankPlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        var keys = new Func<StructuralRow, object?>[plan.SortKeys.Count];
        var descending = new bool[plan.SortKeys.Count];
        var nullsFirst = new bool[plan.SortKeys.Count];
        for (var i = 0; i < plan.SortKeys.Count; i++)
        {
            var scalar = ExpressionCompiler.CompileScalar(plan.SortKeys[i].Expression);
            keys[i] = row => scalar(row);
            descending[i] = plan.SortKeys[i].Descending;
            nullsFirst[i] = plan.SortKeys[i].NullsFirst;
        }

        // `order` is the total order keeping the per-partition trace sorted;
        // `sortKeyOnly` (zero tiebreak) detects equal-ORDER-BY-key tie groups for
        // RANK / DENSE_RANK.
        var order = new SortKeyComparer<StructuralRow>(
            keys, descending, nullsFirst, StructuralRowComparer.Instance);
        var sortKeyOnly = new SortKeyComparer<StructuralRow>(
            keys, descending, nullsFirst, ConstantZeroComparer<StructuralRow>.Instance);

        // Partition key: project the PARTITION BY expressions into a StructuralRow
        // (an empty list ⇒ a constant key ⇒ a single global partition, which is the
        // unpartitioned analytics rank).
        var partitionExtractors = new Func<IReadOnlyList<object?>, object?>[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            partitionExtractors[i] = ExpressionCompiler.CompileScalar(plan.PartitionKeys[i]);
        }

        StructuralRow PartitionOf(StructuralRow row)
        {
            var values = new object?[partitionExtractors.Length];
            for (var i = 0; i < partitionExtractors.Length; i++)
            {
                values[i] = partitionExtractors[i](row);
            }

            return new StructuralRow(values);
        }

        // Parallel: co-locate each PARTITION BY group on one worker before the
        // per-partition operator (identity passthrough at W<=1). A global (no
        // PARTITION BY) window is refused upstream by CanCompileParallel, so the
        // partition list is non-empty whenever this fires.
        if (ctx.Workers > 1 && plan.PartitionKeys.Count > 0)
        {
            var partitionSchema = SyntheticKeySchema(plan.PartitionKeys);
            input = builder.Exchange(
                input, row => StablePartitionHash.OfWholeRow(PartitionOf(row), partitionSchema));
        }

        // The snapshot codec is over the base (input) rows; the operator recovers
        // the widened output from them on load.
        var codec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(plan.Input.Schema);
        return builder.PartitionedRank<StructuralRow>(
            input, PartitionOf, order, sortKeyOnly, plan.Function, null, codec);
    }

    // ---- Window aggregates (agg(x) OVER (PARTITION BY p [ORDER BY o RANGE …])) ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileWindowAggregate(
        CircuitBuilder builder,
        WindowAggregatePlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        // Partition key: project the PARTITION BY expressions into a StructuralRow
        // (an empty list ⇒ a single global partition).
        var partitionExtractors = new Func<IReadOnlyList<object?>, object?>[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            partitionExtractors[i] = ExpressionCompiler.CompileScalar(plan.PartitionKeys[i]);
        }

        StructuralRow PartitionOf(StructuralRow row)
        {
            var values = new object?[partitionExtractors.Length];
            for (var i = 0; i < partitionExtractors.Length; i++)
            {
                values[i] = partitionExtractors[i](row);
            }

            return new StructuralRow(values);
        }

        // Parallel: co-locate each PARTITION BY group on one worker before the
        // per-partition operator (identity passthrough at W<=1). A global (no
        // PARTITION BY) window is refused upstream by CanCompileParallel, so the
        // partition list is non-empty whenever this fires.
        if (ctx.Workers > 1 && plan.PartitionKeys.Count > 0)
        {
            var partitionSchema = SyntheticKeySchema(plan.PartitionKeys);
            input = builder.Exchange(
                input, row => StablePartitionHash.OfWholeRow(PartitionOf(row), partitionSchema));
        }

        IComparer<StructuralRow> order;
        Func<StructuralRow, long>? orderValueOf = null;
        var descending = false;
        if (plan.OrderKey is { } sk)
        {
            var scalar = ExpressionCompiler.CompileScalar(sk.Expression);
            var keys = new Func<StructuralRow, object?>[] { row => scalar(row) };
            var desc = new[] { sk.Descending };
            var nullsFirst = new[] { sk.NullsFirst };
            descending = sk.Descending;
            order = new SortKeyComparer<StructuralRow>(keys, desc, nullsFirst, StructuralRowComparer.Instance);

            // Ordered frames (running and bounded) use the numeric order value for
            // the RANGE arithmetic; the resolver constrains the key to an integer
            // or temporal type. A NULL key sorts to the low end of the value space.
            orderValueOf = row => scalar(row) is { } v ? MonotoneKey.Extract(v) : long.MinValue;
        }
        else
        {
            // Whole-partition frame: ordering is irrelevant, but the per-partition
            // store still needs a deterministic total order over distinct rows.
            order = StructuralRowComparer.Instance;
        }

        // Build the composite aggregator over the result columns (the frame
        // multiset is handed to it per row).
        var aggs = new SqlAggregator[plan.Aggregates.Count];
        var aggColumns = new SchemaColumn[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            aggs[i] = BuildSqlAggregator(plan.Aggregates[i]);
            aggColumns[i] = new SchemaColumn("$wagg" + i, plan.Aggregates[i].ResultType);
        }

        var composite = new CompositeAggregator(aggs, ctx.Codec, new Schema(aggColumns));

        // Snapshot codec for the per-partition integrated input (base rows).
        var codec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(plan.Input.Schema);

        var frontier = ResolveWindowFrontier(plan, ctx);

        return builder.PartitionedWindowAggregate<StructuralRow>(
            input,
            PartitionOf,
            order,
            orderValueOf,
            plan.Frame?.Preceding,
            descending,
            composite,
            plan.Aggregates.Count,
            partitionComparer: null,
            snapshotCodec: codec,
            frontier: frontier);
    }

    /// <summary>
    /// The GC frontier for a bounded ascending RANGE frame whose ORDER BY key is a
    /// monotone base column (a LATENESS column or a temporal-filter watermark).
    /// Rows whose order value falls below <c>frontier − preceding</c> can never
    /// enter a future row's backward frame, so the operator drops them. Returns
    /// <c>null</c> (no GC) for running / whole-partition frames, descending order,
    /// a non-column order key, or an order key with no (full) frontier — all sound,
    /// just unbounded.
    /// </summary>
    private static IFrontier? ResolveWindowFrontier(WindowAggregatePlan plan, CompileContext ctx)
    {
        if (plan.Frame?.Preceding is null || plan.OrderKey is not { } sk || sk.Descending)
        {
            return null;
        }

        if (sk.Expression is not ResolvedColumn col)
        {
            return null; // GC only for a bare monotone column in v1.
        }

        var sources = ctx.Monotonicity.Sources(plan.Input, col.Index);
        if (sources is not { Count: > 0 })
        {
            return null;
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
            return null; // a partial frontier set would be unsound.
        }

        IFrontier frontier = frontiers.Count == 1 ? frontiers[0] : new MinFrontier(frontiers);
        if (ctx.Monotonicity.FrontierTransform(plan.Input, col.Index) is { } transform)
        {
            frontier = new TransformedFrontier(frontier, transform);
        }

        return frontier;
    }

    // ---- Window offset functions (LAG / LEAD) ----

    private static Stream<ZSet<StructuralRow, Z64>> CompileWindowOffset(
        CircuitBuilder builder,
        WindowOffsetPlan plan,
        CompileContext ctx)
    {
        var input = CompilePlan(builder, plan.Input, ctx);

        var partitionExtractors = new Func<IReadOnlyList<object?>, object?>[plan.PartitionKeys.Count];
        for (var i = 0; i < plan.PartitionKeys.Count; i++)
        {
            partitionExtractors[i] = ExpressionCompiler.CompileScalar(plan.PartitionKeys[i]);
        }

        StructuralRow PartitionOf(StructuralRow row)
        {
            var values = new object?[partitionExtractors.Length];
            for (var i = 0; i < partitionExtractors.Length; i++)
            {
                values[i] = partitionExtractors[i](row);
            }

            return new StructuralRow(values);
        }

        // Parallel: co-locate each PARTITION BY group on one worker before the
        // per-partition operator (identity passthrough at W<=1). A global (no
        // PARTITION BY) window is refused upstream by CanCompileParallel, so the
        // partition list is non-empty whenever this fires.
        if (ctx.Workers > 1 && plan.PartitionKeys.Count > 0)
        {
            var partitionSchema = SyntheticKeySchema(plan.PartitionKeys);
            input = builder.Exchange(
                input, row => StablePartitionHash.OfWholeRow(PartitionOf(row), partitionSchema));
        }

        // Total order: the ORDER BY keys left to right (any comparable type —
        // LAG/LEAD is positional) then a full-row tiebreak so positions are
        // deterministic.
        var keys = new Func<StructuralRow, object?>[plan.OrderKeys.Count];
        var descending = new bool[plan.OrderKeys.Count];
        var nullsFirst = new bool[plan.OrderKeys.Count];
        for (var i = 0; i < plan.OrderKeys.Count; i++)
        {
            var sortScalar = ExpressionCompiler.CompileScalar(plan.OrderKeys[i].Expression);
            keys[i] = row => sortScalar(row);
            descending[i] = plan.OrderKeys[i].Descending;
            nullsFirst[i] = plan.OrderKeys[i].NullsFirst;
        }

        var order = new SortKeyComparer<StructuralRow>(
            keys, descending, nullsFirst, StructuralRowComparer.Instance);

        var specs = new OffsetSpec<StructuralRow>[plan.Functions.Count];
        for (var i = 0; i < plan.Functions.Count; i++)
        {
            var fn = plan.Functions[i];
            var valueScalar = ExpressionCompiler.CompileScalar(fn.Value);
            specs[i] = new OffsetSpec<StructuralRow>(row => valueScalar(row), fn.Kind, fn.Offset, fn.Default);
        }

        var codec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(plan.Input.Schema);
        return builder.PartitionedOffset<StructuralRow>(input, PartitionOf, order, specs, null, codec);
    }

    // ---- Projection ----

    // ---- Linear chain (fused Filter / Project) ----

    private enum LinearStageKind
    {
        Filter,
        Map,
    }

    /// <summary>One stage of a fused linear chain — either a row-dropping
    /// predicate (<see cref="LinearStageKind.Filter"/>) or a row-rewriting
    /// projection (<see cref="LinearStageKind.Map"/>).</summary>
    private readonly record struct LinearStage(
        LinearStageKind Kind,
        Func<IReadOnlyList<object?>, bool>? Predicate,
        Func<IReadOnlyList<object?>, object?>[]? Delegates,
        Schema? OutSchema);

    /// <summary>
    /// Compiles a maximal run of consecutive <see cref="FilterPlan"/> /
    /// <see cref="ProjectPlan"/> nodes into a single fused pass. Both are
    /// pointwise and stateless, so chaining them as separate operators
    /// materializes an intermediate Z-set (one allocation + one full iteration)
    /// between every stage. Folding the whole run into one
    /// <see cref="LinearOperators.MapFilterRows{TIn,TOut,TWeight}"/> evaluates
    /// every stage per row in a single iteration with one output allocation.
    /// </summary>
    /// <remarks>
    /// The fold is exactly equivalent to staging the operators: filters are pure
    /// row predicates, projections are pure row functions, and accumulating into
    /// one <c>ZSetBuilder</c> matches the staged <c>MapKeys</c>/<c>Filter</c>
    /// because Z-set addition is associative and commutative. A single-stage
    /// chain lowers to the original dedicated operator (<c>Filter</c> /
    /// <c>MapRows</c>) so common single-op queries keep their exact prior
    /// operator layout; only genuine multi-stage chains fuse.
    /// </remarks>
    private static Stream<ZSet<StructuralRow, Z64>> CompileLinearChain(
        CircuitBuilder builder,
        LogicalPlan plan,
        CompileContext ctx)
    {
        // Walk top→down collecting stages; the loop stops at the first
        // non-linear node, which becomes the chain's compiled input.
        var stages = new List<LinearStage>();
        var node = plan;
        while (true)
        {
            if (node is FilterPlan f)
            {
                stages.Add(new LinearStage(
                    LinearStageKind.Filter,
                    ExpressionCompiler.CompilePredicate(f.Predicate),
                    null,
                    null));
                node = f.Input;
            }
            else if (node is ProjectPlan p)
            {
                // An identity projection is a pure rename — no runtime stage.
                if (IsIdentityProjection(p))
                {
                    node = p.Input;
                    continue;
                }

                var delegates = new Func<IReadOnlyList<object?>, object?>[p.Projections.Count];
                for (var i = 0; i < delegates.Length; i++)
                {
                    delegates[i] = ExpressionCompiler.CompileScalar(p.Projections[i].Expression);
                }

                stages.Add(new LinearStage(LinearStageKind.Map, null, delegates, p.Schema));
                node = p.Input;
            }
            else
            {
                break;
            }
        }

        var input = CompilePlan(builder, node, ctx);

        // No runtime stages (e.g. a chain of identity projections) → pass through.
        if (stages.Count == 0)
        {
            return input;
        }

        // Stages were collected outermost-first; data flows from the input
        // upward, so apply them in reverse (innermost-first) order.
        stages.Reverse();

        var codec = ctx.Codec;

        Stream<ZSet<StructuralRow, Z64>> result;

        // Single stage: lower to the original dedicated operator so the common
        // single-filter / single-project case keeps its exact prior shape.
        if (stages.Count == 1)
        {
            var only = stages[0];
            if (only.Kind == LinearStageKind.Filter)
            {
                var predicate = only.Predicate!;
                result = builder.Filter(input, row => predicate(row));
            }
            else
            {
                result = builder.MapRows(input, row => ApplyMap(only, row, codec));
            }
        }
        else
        {
            var chain = stages.ToArray();
            result = builder.MapFilterRows<StructuralRow, StructuralRow, Z64>(input, row =>
            {
                IReadOnlyList<object?> current = row;
                foreach (var stage in chain)
                {
                    if (stage.Kind == LinearStageKind.Filter)
                    {
                        if (!stage.Predicate!(current))
                        {
                            return (false, null!);
                        }
                    }
                    else
                    {
                        current = ApplyMap(stage, current, codec);
                    }
                }

                return (true, (StructuralRow)current);
            });
        }

        // Partition state survives a filters-only chain unchanged (filters touch
        // no columns); any projection may drop or reorder columns and invalidate
        // the partition-key indices, so it drops the tracking (docs §2). Only
        // meaningful at W>1. Identity projections emit no Map stage (stripped
        // above), so a pure rename still propagates.
        if (ctx.Workers > 1)
        {
            var hasMap = false;
            foreach (var s in stages)
            {
                if (s.Kind == LinearStageKind.Map)
                {
                    hasMap = true;
                    break;
                }
            }

            if (!hasMap)
            {
                ctx.SetPartition(result, ctx.GetPartition(input));
            }
        }

        return result;
    }

    private static StructuralRow ApplyMap(
        LinearStage stage, IReadOnlyList<object?> row, IRowCodec<StructuralRow> codec)
    {
        var delegates = stage.Delegates!;
        var values = new object?[delegates.Length];
        for (var i = 0; i < delegates.Length; i++)
        {
            values[i] = delegates[i](row);
        }

        return codec.BuildRow(stage.OutSchema, values);
    }

    /// <summary>
    /// True when <paramref name="plan"/>'s projection is a pure identity — same
    /// arity as its input with every column a sequential <see cref="ResolvedColumn"/>
    /// (a rename only, no value change), so it needs no runtime stage.
    /// </summary>
    private static bool IsIdentityProjection(ProjectPlan plan)
    {
        if (plan.Projections.Count != plan.Input.Schema.Count)
        {
            return false;
        }

        for (var i = 0; i < plan.Projections.Count; i++)
        {
            if (plan.Projections[i].Expression is not ResolvedColumn rc || rc.Index != i)
            {
                return false;
            }
        }

        return true;
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
            DbspNet.Sql.Parser.Ast.JoinType.FullOuter => CompileFullOuterJoin(builder, plan, ctx),
            _ => throw new InvalidOperationException($"unsupported JoinType {plan.JoinType}"),
        };
    }

    private static Stream<ZSet<StructuralRow, Z64>> CompileInnerJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        var left = CompilePlan(builder, plan.Left, ctx);

        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);

        // INNER: drop NULL-keyed rows on both sides — the equi-predicate is
        // never true when either key column is NULL. For set-op-synthesised
        // joins (INTERSECT/EXCEPT) this is overridden: NULLs are equal to
        // each other for deduplication-style matching.
        var leftFiltered = plan.AllowNullKeys
            ? left
            : builder.Filter(left, row => HasNoNullKey(row, leftIndices));

        var leftKeySchema = plan.Left.Schema.SubsetByIndex(leftIndices);
        var rightKeySchema = plan.Right.Schema.SubsetByIndex(rightIndices);
        var joinedSchema = plan.Schema;
        var codec = ctx.Codec;
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;
        // Re-shard each side by the equi-key so matching rows co-locate on one
        // worker, then index by that key. At W<=1 `ExchangeIndex`/`ExchangeIndexJoin`
        // degrade to exactly `GroupProject(side, keyOf, row => row)` — byte-identical
        // to the pre-parallel emit (docs/design-structural-parallel.md §1).
        int LeftPart(StructuralRow row) => StablePartitionHash.OfRow(row, leftIndices, leftKeySchema);
        StructuralRow LeftKeyOf(StructuralRow row) => ExtractKey(codec, leftKeySchema, row, leftIndices);
        StructuralRow Combine(StructuralRow _, StructuralRow lrow, StructuralRow rrow) =>
            MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount);

        var (gcFrontier, gcMonotoneKey) = ResolveJoinKeyFrontier(plan, ctx);

        // Push a residual (non-equi ON conjunct spanning both sides — e.g. a
        // temporal-SCD `ts BETWEEN lo AND hi`) INTO the join combine rather than
        // materialising the full equi cross product and filtering it above: rows
        // failing the residual never enter the output Z-set. The result set is
        // identical (matched = σ_residual(join) either way); only the
        // intermediate shrinks. GC is unaffected — key retention does not depend
        // on which output rows survive the residual.
        Func<StructuralRow, bool>? residualFn = null;
        if (plan.Residual is { } residual)
        {
            var residualPredicate = ExpressionCompiler.CompilePredicate(residual);
            residualFn = row => residualPredicate(row);
        }

        Stream<ZSet<StructuralRow, Z64>> joined;

        // Arrangement CSE: when this relation+key is the right input of ≥2 INNER
        // joins and this join needs neither join-key GC nor snapshot, route the
        // right side through ONE shared arrangement instead of a private right
        // trace (docs/design-row-representation.md §9.6). The shared arrangement
        // is the R_t ("after") side, so the join math is unchanged.
        if (ctx.Options.ShareArrangements
            && !plan.AllowNullKeys
            && gcFrontier is null
            && ctx.SnapshotCodecs is null
            && RightShareSource(plan.Right) is { } shareSource
            && ctx.ShareableArrangements.Contains(
                new ArrangementKey(shareSource, string.Join(",", rightIndices))))
        {
            var shared = GetOrBuildSharedArrangement(
                builder, ctx, plan, rightIndices, rightKeySchema,
                new ArrangementKey(shareSource, string.Join(",", rightIndices)));
            var leftIndexed = builder.ExchangeIndex(leftFiltered, LeftPart, LeftKeyOf);
            joined = EmitSharedRightInnerJoin(
                builder, ctx, leftIndexed, shared.RightIndexed, shared.Arrangement, Combine, residualFn);
        }
        else
        {
            var right = CompilePlan(builder, plan.Right, ctx);
            var rightFiltered = plan.AllowNullKeys
                ? right
                : builder.Filter(right, row => HasNoNullKey(row, rightIndices));
            int RightPart(StructuralRow row) => StablePartitionHash.OfRow(row, rightIndices, rightKeySchema);
            StructuralRow RightKeyOf(StructuralRow row) => ExtractKey(codec, rightKeySchema, row, rightIndices);

            var broadcastRight = ShouldBroadcastRight(plan.Right, ctx);

            Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> leftIndexed;
            Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> rightIndexed;
            if (broadcastRight)
            {
                // Broadcast join: the low-cardinality right dimension would skew a
                // hash shuffle (its whole group lands on one worker), so instead
                // keep the fact (left) on its current balanced partition — index it
                // LOCALLY, no shuffle — and replicate the dimension to every worker.
                // Each worker then joins its local fact shard against the complete
                // dimension; summing the shards reconstructs the full join. At W<=1
                // BroadcastExchange is identity and both GroupProjects are the serial
                // emit, so W=1 stays byte-identical.
                leftIndexed = builder.GroupProject(leftFiltered, LeftKeyOf, static row => row);
                var rightFull = builder.BroadcastExchange(rightFiltered);
                rightIndexed = builder.GroupProject(rightFull, RightKeyOf, static row => row);
            }
            else if (ctx.Workers > 1 && ctx.Options.CoalesceJoinExchange)
            {
                // Fuse both key exchanges into ONE barrier (a single all-to-all
                // rendezvous instead of two): the shuffle is the same, but a 4-way
                // join costs 4 barriers per step instead of 8, cutting the
                // coordination/straggler wait the profiler attributes the W-scaling
                // wall to (docs/design-structural-parallel.md §15). At W<=1 each
                // side still degrades to the identical GroupProject.
                (leftIndexed, rightIndexed) = builder.ExchangeIndexJoin(
                    leftFiltered, LeftPart, LeftKeyOf,
                    rightFiltered, RightPart, RightKeyOf);
            }
            else
            {
                leftIndexed = builder.ExchangeIndex(leftFiltered, LeftPart, LeftKeyOf);
                rightIndexed = builder.ExchangeIndex(rightFiltered, RightPart, RightKeyOf);
            }

            var leftCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(leftKeySchema, plan.Left.Schema);
            var rightCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, plan.Right.Schema);
            joined = EmitInnerJoin(
                builder, ctx, leftIndexed, rightIndexed, Combine,
                leftCodec, rightCodec, gcFrontier, gcMonotoneKey, residualFn);

            // A broadcast join leaves the fact side on its incoming partition (not
            // the join key), so the output is partitioned as the left was — inherit
            // that; a hash join re-shards both sides by the key, so the output is
            // partitioned by the left equi-key indices (left columns lead the row).
            if (ctx.Workers > 1)
            {
                ctx.SetPartition(joined, broadcastRight
                    ? new PartitionInfo(ShardDisjoint: false, PartitionKey: ctx.GetPartition(left).PartitionKey)
                    : new PartitionInfo(ShardDisjoint: false, PartitionKey: leftIndices));
            }

            return joined;
        }

        // Shared-arrangement (CSE) join output: hash-partitioned by the join key.
        if (ctx.Workers > 1)
        {
            ctx.SetPartition(joined, new PartitionInfo(ShardDisjoint: false, PartitionKey: leftIndices));
        }

        return joined;
    }

    /// <summary>
    /// Returns the shared right arrangement for <paramref name="key"/>, building
    /// it on first reference: compile the (shared) right stream once, drop
    /// NULL-keyed rows, index by the right key, and wrap in a flat
    /// <c>Arrange</c> or spine <c>SpineArrange</c> (per trace family). Subsequent
    /// joins with the same key reuse the cached stream + arrangement.
    /// </summary>
    private static SharedArrangement GetOrBuildSharedArrangement(
        CircuitBuilder builder,
        CompileContext ctx,
        JoinPlan plan,
        int[] rightIndices,
        Schema rightKeySchema,
        ArrangementKey key)
    {
        if (ctx.ArrangementCache.TryGetValue(key, out var entry))
        {
            return entry;
        }

        var codec = ctx.Codec;
        var right = CompilePlan(builder, plan.Right, ctx);
        // The pre-pass only marks non-NULL-accepting joins shareable, so the
        // right side always drops NULL keys here.
        var rightFiltered = builder.Filter(right, row => HasNoNullKey(row, rightIndices));
        var rightIndexed = builder.GroupProject(
            rightFiltered,
            row => ExtractKey(codec, rightKeySchema, row, rightIndices),
            row => row);

        object arrangement = ctx.Options.TraceFamily == TraceFamily.Spine
            ? builder.SpineArrange(
                rightIndexed, ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                valueComparer: StructuralRowComparer.Instance)
            : builder.Arrange(rightIndexed);

        entry = new SharedArrangement(rightIndexed, arrangement);
        ctx.ArrangementCache[key] = entry;
        return entry;
    }

    /// <summary>
    /// Emits an INNER join whose right side is a shared arrangement (flat or
    /// spine, per trace family). <paramref name="rightDelta"/> is the shared
    /// indexed delta stream; <paramref name="arrangement"/> is the matching
    /// <see cref="IArrangement{TKey,TValue,TWeight}"/> /
    /// <see cref="ISpineArrangement{TKey,TValue,TWeight}"/> handle.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> EmitSharedRightInnerJoin(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> leftIndexed,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> rightDelta,
        object arrangement,
        Func<StructuralRow, StructuralRow, StructuralRow, StructuralRow> combine,
        Func<StructuralRow, bool>? residual = null)
    {
        // Residual pushdown mirrors EmitInnerJoin: the flat shared-right join
        // folds it into the cross product; the spine variant has no hook and
        // post-filters.
        if (ctx.Options.TraceFamily == TraceFamily.Spine)
        {
            var spineJoined = builder.SpineIncrementalInnerJoinSharedRight(
                leftIndexed, rightDelta,
                (ISpineArrangement<StructuralRow, StructuralRow, Z64>)arrangement,
                combine, ctx.Options.Compaction,
                keyComparer: StructuralRowComparer.Instance,
                leftValueComparer: StructuralRowComparer.Instance);
            return residual is null ? spineJoined : builder.Filter(spineJoined, row => residual(row));
        }

        return builder.IncrementalInnerJoinSharedRight(
            leftIndexed, rightDelta,
            (IArrangement<StructuralRow, StructuralRow, Z64>)arrangement,
            combine, residual);
    }

    private static Stream<ZSet<StructuralRow, Z64>> CompileLeftOuterJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        // A residual on an outer join isn't expressible in IncrementalLeftJoin:
        // its match-presence is a per-KEY emptiness test, but a residual can
        // reference left columns, so two left rows under one key can disagree
        // about whether the key is matched. Lower to the anti-join rewrite.
        if (plan.Residual is not null)
        {
            return CompileOuterJoinWithResidual(builder, plan, ctx);
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

    /// <summary>
    /// Lower an outer join carrying a residual (non-equi) ON conjunct:
    /// <code>
    ///   matched   = σ_residual( L ⋈_key R )
    ///   unmatched = S − antisemi( S, π_S(matched) )      for each preserved side S
    ///   result    = matched ∪ NULLPAD(unmatched…)
    /// </code>
    /// Composed from primitives rather than taught to the operator.
    /// <see cref="IncrementalLeftJoinOp"/> decides padding with a per-key
    /// emptiness test (<c>!oldR.IsEmpty</c>); a residual makes match-presence a
    /// property of the (key, preserved row) pair instead, which that test cannot
    /// express. The rewrite sidesteps the operator entirely — which also means it
    /// does not need an equi-key at all, so a keyless outer join with a residual
    /// lowers here too (as a unit-key cross product: correct, quadratic).
    ///
    /// <para><b>NULL-safety.</b> The anti-join keys on the WHOLE preserved row and
    /// deliberately does NOT filter NULL keys — unlike <c>CompileSemiJoin</c>,
    /// whose keys are SQL probe values where <c>NULL = x</c> is never TRUE. Here
    /// the key is row identity, so <c>NULL</c> must equal <c>NULL</c>: dropping
    /// NULL-bearing rows from the anti-join would let a row that DID match survive
    /// the subtraction and emit a spurious NULL-pad alongside its real join
    /// output.</para>
    ///
    /// <para><b>Sharing.</b> <c>matched</c> is consumed once per preserved side
    /// plus once for the output. There is no CSE at plan level, but this is the
    /// circuit — the stream is a value, so reusing the variable shares the
    /// operator.</para>
    ///
    /// <para>NULL <em>equi-key</em> semantics are inherited from the inner join:
    /// NULL-keyed rows never match, so they fall through to the anti-join and get
    /// padded (preserved side) or dropped (non-preserved side) — exactly the
    /// non-residual operator's behaviour.</para>
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> CompileOuterJoinWithResidual(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        var left = CompilePlan(builder, plan.Left, ctx);
        var right = CompilePlan(builder, plan.Right, ctx);

        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;
        var codec = ctx.Codec;
        var joinedSchema = plan.Schema;

        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);

        // matched = the inner join, with the residual applied. Rows are built in
        // the OUTER join's schema so the padded and matched branches agree.
        var validKeyLeft = builder.Filter(left, row => HasNoNullKey(row, leftIndices));
        var validKeyRight = builder.Filter(right, row => HasNoNullKey(row, rightIndices));

        var leftKeySchema = plan.Left.Schema.SubsetByIndex(leftIndices);
        var rightKeySchema = plan.Right.Schema.SubsetByIndex(rightIndices);
        var leftIndexed = builder.GroupProject(
            validKeyLeft,
            row => ExtractKey(codec, leftKeySchema, row, leftIndices),
            row => row);
        var rightIndexed = builder.GroupProject(
            validKeyRight,
            row => ExtractKey(codec, rightKeySchema, row, rightIndices),
            row => row);

        // Push the residual INTO the inner join's combine (flat path) instead of
        // materialising the full product and filtering above. matched is the same
        // set either way — σ_residual(L ⋈_key R) — so the unmatched anti-joins
        // below and the outer-join match-presence semantics are unchanged; only
        // the intermediate cross product shrinks (a temporal SCD join OOMs
        // building it). On the spine path EmitInnerJoin falls back to a post-join
        // filter internally.
        var residualScalar = ExpressionCompiler.CompileScalar(plan.Residual!);
        var matched = EmitInnerJoin(
            builder, ctx,
            leftIndexed,
            rightIndexed,
            (_, lrow, rrow) => MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount),
            ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(leftKeySchema, plan.Left.Schema),
            ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(rightKeySchema, plan.Right.Schema),
            residual: row => residualScalar(row) is true);

        var parts = new List<Stream<ZSet<StructuralRow, Z64>>> { matched };

        if (plan.JoinType is DbspNet.Sql.Parser.Ast.JoinType.LeftOuter
            or DbspNet.Sql.Parser.Ast.JoinType.FullOuter)
        {
            var unmatchedLeft = UnmatchedPreservedRows(
                builder, ctx, left, matched, joinedSchema, plan.Left.Schema, offsetInCombined: 0);
            parts.Add(builder.MapRows(
                unmatchedLeft,
                row => NullPadRight(codec, joinedSchema, row, leftCount, rightCount)));
        }

        if (plan.JoinType is DbspNet.Sql.Parser.Ast.JoinType.RightOuter
            or DbspNet.Sql.Parser.Ast.JoinType.FullOuter)
        {
            var unmatchedRight = UnmatchedPreservedRows(
                builder, ctx, right, matched, joinedSchema, plan.Right.Schema, offsetInCombined: leftCount);
            parts.Add(builder.MapRows(
                unmatchedRight,
                row => NullPadLeft(codec, joinedSchema, row, leftCount, rightCount)));
        }

        var result = parts[0];
        for (var i = 1; i < parts.Count; i++)
        {
            result = builder.Union(result, parts[i]);
        }

        return result;
    }

    /// <summary>
    /// Set-ify by PRESENCE: every row whose accumulated weight is non-zero gets
    /// weight +1; rows at zero drop out.
    ///
    /// <para><c>Distinct</c> alone will not do. It defines presence as weight
    /// <c>&gt; 0</c> (<c>DistinctOp</c> tests <c>TWeight.IsPositive</c>), but the
    /// join operators define match-presence as weight <c>≠ 0</c>
    /// (<c>!oldR.IsEmpty</c>) — and the PBT feeds arbitrary ±1 streams, so
    /// accumulated weights genuinely go negative and the two notions diverge.
    /// <c>Distinct(x) + Distinct(−x)</c> reconciles them: for accumulated weight
    /// <c>w</c>, the first term fires iff <c>w &gt; 0</c>, the second iff
    /// <c>w &lt; 0</c>, and they cannot both fire — so the sum is exactly
    /// <c>w ≠ 0 ⇒ 1</c>. <c>Negate</c> is linear, so negating the delta stream
    /// negates the accumulation.</para>
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> NonZeroSet(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<ZSet<StructuralRow, Z64>> input,
        Schema schema)
    {
        var positive = EmitDistinct(
            builder, ctx, input,
            snapshotCodec: ctx.SnapshotCodecs?.CreateZSetTraceCodec(schema));
        var negative = EmitDistinct(
            builder, ctx, builder.Negate(input),
            snapshotCodec: ctx.SnapshotCodecs?.CreateZSetTraceCodec(schema));
        return builder.Union(positive, negative);
    }

    /// <summary>
    /// Rows of a preserved side that contributed no <paramref name="matched"/>
    /// row: <c>S − antisemi(S, π_S(matched))</c>. Keys on the whole row, NULLs
    /// included (see <see cref="CompileOuterJoinWithResidual"/>).
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> UnmatchedPreservedRows(
        CircuitBuilder builder,
        CompileContext ctx,
        Stream<ZSet<StructuralRow, Z64>> side,
        Stream<ZSet<StructuralRow, Z64>> matched,
        Schema matchedSchema,
        Schema sideSchema,
        int offsetInCombined)
    {
        var codec = ctx.Codec;
        var width = sideSchema.Count;

        // Set-ify the matched PAIRS before projecting. Projecting first would
        // sum weights across a preserved row's matches, and Z-sets admit
        // negative weights, so those could cancel to zero — a row that matched
        // would look unmatched and emit a spurious NULL-pad. Set-ifying first
        // makes every surviving pair weight +1, so the projection below can only
        // sum upward.
        var matchedPairs = NonZeroSet(builder, ctx, matched, matchedSchema);

        // π_S(matchedPairs), rebuilt in the SIDE's own schema (not a slice of
        // the join's, whose nullability differs) so the rows compare equal to
        // the side's own.
        var projected = builder.MapRows<StructuralRow, StructuralRow, Z64>(
            matchedPairs,
            row =>
            {
                var vs = new object?[width];
                for (var i = 0; i < width; i++)
                {
                    vs[i] = row[offsetInCombined + i];
                }

                return codec.BuildRow(sideSchema, vs);
            });

        // Collapse the per-match multiplicity: a preserved row that matched k
        // rows must be subtracted exactly once, not k times. Weights here are
        // all +1, so plain Distinct is enough.
        var distinct = EmitDistinct(
            builder, ctx, projected,
            snapshotCodec: ctx.SnapshotCodecs?.CreateZSetTraceCodec(sideSchema));

        var sideIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            side, row => row, row => row);
        var distinctIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            distinct, row => row, row => row);

        // Semi-join on row identity: emits each matched preserved row carrying
        // its ORIGINAL weight (distinct side contributes weight 1), so the
        // subtraction below cancels it exactly.
        var semi = EmitInnerJoin(
            builder, ctx,
            sideIndexed,
            distinctIndexed,
            (_, sideRow, _) => sideRow,
            ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(sideSchema, sideSchema),
            ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(sideSchema, sideSchema));

        return builder.Difference(side, semi);
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
            return CompileOuterJoinWithResidual(builder, plan, ctx);
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

    private static Stream<ZSet<StructuralRow, Z64>> CompileFullOuterJoin(
        CircuitBuilder builder,
        JoinPlan plan,
        CompileContext ctx)
    {
        // A residual is not encodable in the operator (same reason as LEFT /
        // RIGHT — a failing residual must retain the preserved rows NULL-padded,
        // and match-presence there is per-key). Lower to the anti-join rewrite,
        // which preserves both sides.
        if (plan.Residual is not null)
        {
            return CompileOuterJoinWithResidual(builder, plan, ctx);
        }

        var left = CompilePlan(builder, plan.Left, ctx);
        var right = CompilePlan(builder, plan.Right, ctx);

        var (leftIndices, rightIndices) = ExtractEquiKeyIndices(plan);
        var leftCount = plan.Left.Schema.Count;
        var rightCount = plan.Right.Schema.Count;

        // NULL-keyed rows can never match. A NULL-keyed left row emits straight
        // to the NULL-padded-right branch; a NULL-keyed right row to the
        // NULL-padded-left branch. Both bypass the keyed operator.
        var nullKeyLeft = builder.Filter(left, row => !HasNoNullKey(row, leftIndices));
        var validKeyLeft = builder.Filter(left, row => HasNoNullKey(row, leftIndices));
        var nullKeyRight = builder.Filter(right, row => !HasNoNullKey(row, rightIndices));
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
        // Both-sides-monotone GC license, as for LEFT / RIGHT.
        var (gcFrontier, gcMonotoneKey) = ResolveJoinKeyFrontier(plan, ctx);
        var joined = EmitFullJoin(
            builder, ctx,
            leftIndexed,
            rightIndexed,
            joinCombine: (_, lrow, rrow) => MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount),
            nullPadRightCombine: (_, lrow) => NullPadRight(codec, joinedSchema, lrow, leftCount, rightCount),
            nullPadLeftCombine: (_, rrow) => NullPadLeft(codec, joinedSchema, rrow, leftCount, rightCount),
            leftCodec, rightCodec, gcFrontier, gcMonotoneKey);

        var nullKeyLeftPadded = builder.MapRows(
            nullKeyLeft,
            row => NullPadRight(codec, joinedSchema, row, leftCount, rightCount));
        var nullKeyRightPadded = builder.MapRows(
            nullKeyRight,
            row => NullPadLeft(codec, joinedSchema, row, leftCount, rightCount));

        return builder.Union(builder.Union(joined, nullKeyLeftPadded), nullKeyRightPadded);
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
    // The recursive CTE compiles onto the Core nested-circuit (fixpoint)
    // primitive: the base and step subplans are wired as real Z-set operators
    // inside a NestedScopeBuilder, the self-reference resolves to the loop's
    // feedback stream, and each referenced base table is imported once. The
    // SemiNaiveFixpointOperator preserves the fixpoint across ticks and updates
    // it incrementally — semi-naive extension on inserts, delete-and-re-derive
    // on deletes — through the reusable Core construct rather than a bespoke
    // evaluation loop.
    //
    // Restrictions (v1):
    //   - body may reference only base tables (ScanPlan) and the self-ref.
    //     Other CTE references are rejected here.
    //   - body may not contain aggregates, subqueries, outer joins, or
    //     nested recursive CTEs. CompileRecursiveBody rejects anything else.

    private static Stream<ZSet<StructuralRow, Z64>> CompileRecursiveCte(
        CircuitBuilder builder,
        RecursiveCtePlan plan,
        CompileContext ctx) =>
        CompileRecursiveCteFixpoint(
            builder, plan, ctx.Scans, ctx.TableSchemas, ctx.Codec, ctx.SnapshotCodecs,
            RecursiveSpineConfig(ctx.Options));

    /// <summary>
    /// Spine import-trace config for a recursive CTE when the query is compiled
    /// in <see cref="TraceFamily.Spine"/> mode — the import integrals get the
    /// same LSM trace family as every other stateful site (structural rows are
    /// ordered by <see cref="StructuralRowComparer"/>); null for flat mode.
    /// </summary>
    internal static SpineImportConfig<StructuralRow>? RecursiveSpineConfig(CompileOptions options) =>
        options.TraceFamily == TraceFamily.Spine
            ? new SpineImportConfig<StructuralRow>(StructuralRowComparer.Instance, options.Compaction)
            : null;

    /// <summary>
    /// Build the recursive CTE on the Core nested-circuit (fixpoint) primitive.
    /// Shared by the structural compile and the typed fast path (which compiles
    /// the recursive body structurally and lifts the output to typed at the
    /// boundary), so the iteration loop is identical on both.
    /// </summary>
    internal static Stream<ZSet<StructuralRow, Z64>> CompileRecursiveCteFixpoint(
        CircuitBuilder builder,
        RecursiveCtePlan plan,
        IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> scans,
        IReadOnlyDictionary<string, Schema> tableSchemas,
        IRowCodec<StructuralRow> codec,
        ISqlSnapshotCodecs? snapshotCodecs,
        SpineImportConfig<StructuralRow>? spineConfig = null)
    {
        var bodyCtx = new RecursiveBodyCtx(scans, tableSchemas, codec, snapshotCodecs);

        // Result codec persists the previous-tick fixpoint; per-import codecs
        // are built lazily as base tables are imported in CompileRecursiveBody.
        var resultCodec = snapshotCodecs?.CreateZSetTraceCodec(plan.Schema);

        // R = distinct(base ∪ step(R)). The driver preserves R across ticks and
        // updates it incrementally (semi-naive inserts, DRED deletes); both
        // subplans share one import per base table, and the operator applies the
        // set-collapse, so base and step are returned raw.
        return builder.SemiNaiveFixpoint<StructuralRow>(
            (scope, recRef) =>
            {
                var imports = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
                var baseStream = CompileRecursiveBody(scope, recRef, plan.BasePlan, plan.SelfRef, bodyCtx, imports);
                var stepStream = CompileRecursiveBody(scope, recRef, plan.RecursivePlan, plan.SelfRef, bodyCtx, imports);
                return (baseStream, stepStream);
            },
            resultCodec,
            spineConfig);
    }

    /// <summary>Inputs the recursive-body compiler threads through its recursion.</summary>
    private sealed record RecursiveBodyCtx(
        IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> Scans,
        IReadOnlyDictionary<string, Schema> TableSchemas,
        IRowCodec<StructuralRow> Codec,
        ISqlSnapshotCodecs? SnapshotCodecs);

    /// <summary>
    /// Wire one recursive-body subplan (base or step) into the nested scope,
    /// reproducing the structural-compile row semantics: <see cref="ScanPlan"/>
    /// imports a base table (once, via <paramref name="imports"/>), the self-ref
    /// <see cref="CteScanPlan"/> resolves to <paramref name="recRef"/>, and
    /// Filter / Project / Inner-Join / UnionAll map to the scope's operators.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> CompileRecursiveBody(
        NestedScopeBuilder<StructuralRow> scope,
        Stream<ZSet<StructuralRow, Z64>> recRef,
        LogicalPlan plan,
        CteRef selfRef,
        RecursiveBodyCtx ctx,
        Dictionary<string, Stream<ZSet<StructuralRow, Z64>>> imports)
    {
        var codec = ctx.Codec;
        switch (plan)
        {
            case ScanPlan s:
            {
                if (imports.TryGetValue(s.TableName, out var cached))
                {
                    return cached;
                }

                var snapshotCodec = ctx.SnapshotCodecs?.CreateZSetTraceCodec(ctx.TableSchemas[s.TableName]);
                var inner = scope.Import(ctx.Scans[s.TableName], s.TableName, snapshotCodec);
                imports[s.TableName] = inner;
                return inner;
            }

            case CteScanPlan c when ReferenceEquals(c.Cte, selfRef):
                return recRef;

            case CteScanPlan c:
                throw new InvalidOperationException(
                    $"recursive CTE body cannot reference other CTE '{c.Cte.Name}' in v1");

            case FilterPlan f:
            {
                var input = CompileRecursiveBody(scope, recRef, f.Input, selfRef, ctx, imports);
                var predicate = ExpressionCompiler.CompilePredicate(f.Predicate);
                return scope.Filter(input, row => predicate(row));
            }

            case ProjectPlan p:
            {
                var input = CompileRecursiveBody(scope, recRef, p.Input, selfRef, ctx, imports);
                var delegates = new Func<IReadOnlyList<object?>, object?>[p.Projections.Count];
                for (var i = 0; i < p.Projections.Count; i++)
                {
                    delegates[i] = ExpressionCompiler.CompileScalar(p.Projections[i].Expression);
                }

                var outSchema = p.Schema;
                return scope.Map(input, row =>
                {
                    var vs = new object?[delegates.Length];
                    for (var i = 0; i < delegates.Length; i++)
                    {
                        vs[i] = delegates[i](row);
                    }

                    return codec.BuildRow(outSchema, vs);
                });
            }

            case JoinPlan { JoinType: DbspNet.Sql.Parser.Ast.JoinType.Inner } j:
            {
                var left = CompileRecursiveBody(scope, recRef, j.Left, selfRef, ctx, imports);
                var right = CompileRecursiveBody(scope, recRef, j.Right, selfRef, ctx, imports);
                var (leftIndices, rightIndices) = ExtractEquiKeyIndices(j);

                // INNER: NULL-keyed rows never match (mirrors CompileInnerJoin).
                var leftFiltered = j.AllowNullKeys ? left : scope.Filter(left, row => HasNoNullKey(row, leftIndices));
                var rightFiltered = j.AllowNullKeys ? right : scope.Filter(right, row => HasNoNullKey(row, rightIndices));

                var leftKeySchema = j.Left.Schema.SubsetByIndex(leftIndices);
                var rightKeySchema = j.Right.Schema.SubsetByIndex(rightIndices);
                var joinedSchema = j.Schema;
                var leftCount = j.Left.Schema.Count;
                var rightCount = j.Right.Schema.Count;

                var joined = scope.Join(
                    leftFiltered,
                    rightFiltered,
                    leftKey: row => ExtractKey(codec, leftKeySchema, row, leftIndices),
                    rightKey: row => ExtractKey(codec, rightKeySchema, row, rightIndices),
                    combine: (lrow, rrow) => MergeRows(codec, joinedSchema, lrow, rrow, leftCount, rightCount));

                if (j.Residual is { } residual)
                {
                    var residualPredicate = ExpressionCompiler.CompilePredicate(residual);
                    joined = scope.Filter(joined, row => residualPredicate(row));
                }

                return joined;
            }

            case UnionAllPlan u:
            {
                var result = CompileRecursiveBody(scope, recRef, u.Branches[0], selfRef, ctx, imports);
                for (var i = 1; i < u.Branches.Count; i++)
                {
                    result = scope.Union(result, CompileRecursiveBody(scope, recRef, u.Branches[i], selfRef, ctx, imports));
                }

                return result;
            }

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

    /// <summary>
    /// Compile <c>WHERE probe IN (subquery)</c>: dedup the subquery's
    /// projection on the join key, equi-join with the outer rows on the
    /// composite key, project the outer columns. The composite key carries
    /// every <see cref="SemiJoinPlan.EquiKeys"/> entry — one for the IN-probe
    /// plus one per correlated reference (for the correlated-IN decorrelation).
    /// Output preserves outer schema and weights; the inner join's NULL-key
    /// filter gives SQL three-valued semantics at the WHERE boundary.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> CompileSemiJoin(
        CircuitBuilder builder,
        SemiJoinPlan plan,
        CompileContext ctx)
    {
        if (plan.EquiKeys.Count == 0)
        {
            throw new InvalidOperationException("internal: SemiJoinPlan with no equi-keys");
        }

        var outer = CompilePlan(builder, plan.Input, ctx);
        var subqueryStream = CompilePlan(builder, plan.Subquery, ctx);
        var codec = ctx.Codec;

        // Compile every outer-side key expression once.
        var probeFns = new Func<IReadOnlyList<object?>, object?>[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            probeFns[i] = ExpressionCompiler.CompileScalar(plan.EquiKeys[i].OuterKey);
        }

        var innerIndices = new int[plan.EquiKeys.Count];
        var keySchemaCols = new SchemaColumn[plan.EquiKeys.Count];
        for (var i = 0; i < plan.EquiKeys.Count; i++)
        {
            innerIndices[i] = plan.EquiKeys[i].InnerColumnIndex;
            keySchemaCols[i] = new SchemaColumn("__semi_key_" + i, plan.EquiKeys[i].Type, null);
        }

        var keySchema = new Schema(keySchemaCols);

        // Drop the subquery columns we don't need on the join — keep only the
        // inner-key columns. This dedup-narrows the right side before Distinct.
        var subqueryProjected = builder.MapRows<StructuralRow, StructuralRow, Z64>(
            subqueryStream,
            row =>
            {
                var vs = new object?[innerIndices.Length];
                for (var i = 0; i < innerIndices.Length; i++)
                {
                    vs[i] = row[innerIndices[i]];
                }

                return codec.BuildRow(keySchema, vs);
            });

        var subqueryDistinct = EmitDistinct(
            builder, ctx, subqueryProjected,
            snapshotCodec: ctx.SnapshotCodecs?.CreateZSetTraceCodec(keySchema));

        // Filter NULL probes (NULL = anything is NULL, never TRUE in WHERE)
        // and NULL inner-key tuples (same reason).
        var outerNonNull = builder.Filter(outer, row => HasNoNullProbe(probeFns, row));
        var subqNonNull = builder.Filter(subqueryDistinct, row => HasNoNullKeyRow(row));

        var outerIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            outerNonNull,
            row =>
            {
                var vs = new object?[probeFns.Length];
                for (var i = 0; i < probeFns.Length; i++)
                {
                    vs[i] = probeFns[i](row);
                }

                return codec.BuildRow(keySchema, vs);
            },
            row => row);
        var subqIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            subqNonNull,
            row => row,
            row => row);

        var leftJoinCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(keySchema, plan.Input.Schema);
        var rightJoinCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(keySchema, keySchema);

        // Combine emits ONLY the outer row — semi-join semantics.
        var matched = EmitInnerJoin(
            builder, ctx,
            outerIndexed,
            subqIndexed,
            (_, outerRow, _) => outerRow,
            leftJoinCodec, rightJoinCodec);

        if (plan.IsAnti)
        {
            // Anti-semi-join via Z-set subtraction: keep outer rows that
            // did NOT match. NULL-key outer rows were filtered above and
            // never entered the join, so they don't appear in `outer` here
            // either; they're consistently dropped, matching SQL's
            // WHERE-NULL-drops semantics at the conjunct level.
            return builder.Difference(outerNonNull, matched);
        }

        return matched;
    }

    private static bool HasNoNullProbe(
        Func<IReadOnlyList<object?>, object?>[] probeFns, StructuralRow row)
    {
        for (var i = 0; i < probeFns.Length; i++)
        {
            if (probeFns[i](row) is null)
            {
                return false;
            }
        }

        return true;
    }

    private static bool HasNoNullKeyRow(StructuralRow row)
    {
        for (var i = 0; i < row.Count; i++)
        {
            if (row[i] is null)
            {
                return false;
            }
        }

        return true;
    }

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

    /// <summary>
    /// Compile a correlated scalar subquery LEFT JOIN: the inner subquery
    /// schema is <c>[__corr_0, ..., __corr_N, scalar]</c>; we equi-join
    /// outer with it on the composite correlation key, append the scalar
    /// (or NULL on no match) to the outer row.
    /// </summary>
    private static Stream<ZSet<StructuralRow, Z64>> CompileCorrelatedScalarSubqueryJoin(
        CircuitBuilder builder,
        CorrelatedScalarSubqueryJoinPlan plan,
        CompileContext ctx)
    {
        if (plan.CorrelationKeys.Count == 0)
        {
            throw new InvalidOperationException(
                "internal: CorrelatedScalarSubqueryJoinPlan with no correlation keys — " +
                "the resolver should have produced a ScalarSubqueryJoinPlan instead");
        }

        var outer = CompilePlan(builder, plan.Input, ctx);
        var subqueryStream = CompilePlan(builder, plan.Subquery, ctx);
        var codec = ctx.Codec;
        var outerSchema = plan.Input.Schema;
        var joinedSchema = plan.Schema;

        var probeFns = new Func<IReadOnlyList<object?>, object?>[plan.CorrelationKeys.Count];
        for (var i = 0; i < plan.CorrelationKeys.Count; i++)
        {
            probeFns[i] = ExpressionCompiler.CompileScalar(plan.CorrelationKeys[i].OuterKey);
        }

        var innerIndices = new int[plan.CorrelationKeys.Count];
        var keyCols = new SchemaColumn[plan.CorrelationKeys.Count];
        for (var i = 0; i < plan.CorrelationKeys.Count; i++)
        {
            innerIndices[i] = plan.CorrelationKeys[i].InnerColumnIndex;
            keyCols[i] = new SchemaColumn("__corr_key_" + i, plan.CorrelationKeys[i].Type, null);
        }

        var keySchema = new Schema(keyCols);
        var scalarColumnIndex = plan.ScalarColumnIndex;

        // Outer rows with a NULL component in the correlation key tuple
        // can never match (NULL = anything is NULL); route them to the
        // NULL-padded branch directly rather than through the join.
        var validKeyOuter = builder.Filter(outer, row => HasNoNullProbe(probeFns, row));
        var nullKeyOuter = builder.Filter(outer, row => !HasNoNullProbe(probeFns, row));

        // Drop inner rows whose correlation tuple has any NULL — they
        // wouldn't match a non-NULL outer key either.
        var validInner = builder.Filter(subqueryStream, row =>
        {
            for (var i = 0; i < innerIndices.Length; i++)
            {
                if (row[innerIndices[i]] is null)
                {
                    return false;
                }
            }

            return true;
        });

        var outerIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            validKeyOuter,
            row =>
            {
                var vs = new object?[probeFns.Length];
                for (var i = 0; i < probeFns.Length; i++)
                {
                    vs[i] = probeFns[i](row);
                }

                return codec.BuildRow(keySchema, vs);
            },
            row => row);

        var innerIndexed = builder.GroupProject<StructuralRow, StructuralRow, StructuralRow, Z64>(
            validInner,
            row =>
            {
                var vs = new object?[innerIndices.Length];
                for (var i = 0; i < innerIndices.Length; i++)
                {
                    vs[i] = row[innerIndices[i]];
                }

                return codec.BuildRow(keySchema, vs);
            },
            row => row);

        var leftCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(keySchema, outerSchema);
        var rightCodec = ctx.SnapshotCodecs?.CreateIndexedZSetTraceCodec(keySchema, plan.Subquery.Schema);

        var matched = EmitLeftJoin(
            builder, ctx,
            outerIndexed,
            innerIndexed,
            joinCombine: (_, outerRow, innerRow) =>
                AppendColumn(codec, joinedSchema, outerRow, innerRow[scalarColumnIndex]),
            nullPadCombine: (_, outerRow) =>
                AppendColumn(codec, joinedSchema, outerRow, null),
            leftCodec, rightCodec);

        // NULL-key outer rows bypass the join — append NULL directly.
        var nullPadded = builder.MapRows<StructuralRow, StructuralRow, Z64>(
            nullKeyOuter,
            row => AppendColumn(codec, joinedSchema, row, null));

        return builder.Union(matched, nullPadded);
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

        // Group-key extractors. A key is any scalar expression (bare column,
        // CAST(ts AS DATE), a + b, …), compiled once to a delegate run per row —
        // the same model JOIN / IN-subquery use for composite equi-keys.
        var keyFns = new Func<IReadOnlyList<object?>, object?>[plan.GroupKeys.Count];
        var groupKeyCols = new SchemaColumn[plan.GroupKeys.Count];
        for (var i = 0; i < plan.GroupKeys.Count; i++)
        {
            keyFns[i] = ExpressionCompiler.CompileScalar(plan.GroupKeys[i]);
            // Synthetic key-row schema (the wrapping Project supplies the
            // user-facing names from plan.Schema); only the types matter here.
            groupKeyCols[i] = new SchemaColumn("$gk" + i, plan.GroupKeys[i].Type);
        }

        var groupKeySchema = new Schema(groupKeyCols);

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
        var aggColumns = new SchemaColumn[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            aggColumns[i] = new SchemaColumn("$agg" + i, plan.Aggregates[i].ResultType);
        }

        var aggOnlySchema = new Schema(aggColumns);
        var composite = new CompositeAggregator(aggs, ctx.Codec, aggOnlySchema);

        // Rekey input rows by the group key; value = the entire input row so each
        // aggregator can extract its argument independently.
        var keyCodec = ctx.Codec;
        StructuralRow KeyRowOf(StructuralRow row)
        {
            var vs = new object?[keyFns.Length];
            for (var i = 0; i < keyFns.Length; i++)
            {
                vs[i] = keyFns[i](row);
            }

            return keyCodec.BuildRow(groupKeySchema, vs);
        }

        // Parallel: re-shard by the group key so every group's rows land on one
        // worker before the incremental aggregate. Elide the shuffle when the
        // input already co-locates each group — i.e. it is hash-partitioned by a
        // subset of the (bare-column) group key (docs/design-structural-parallel.md
        // §2). At W<=1 `ExchangeIndex` degrades to the same `GroupProject` this
        // emitted before parallelism, so W=1 is byte-identical either branch.
        var bareGroupIndices = BareColumnIndices(plan.GroupKeys);
        var needsExchange = ctx.Workers > 1
            && !(bareGroupIndices is { } gi
                 && ctx.GetPartition(input).PartitionKey is { } pk
                 && IsKeySubset(pk, gi));

        var indexed = needsExchange
            ? builder.ExchangeIndex(
                input,
                row => StablePartitionHash.OfWholeRow(KeyRowOf(row), groupKeySchema),
                KeyRowOf)
            : builder.GroupProject(input, KeyRowOf, row => row);

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
        var mapped = builder.MapRows(aggregated, pair =>
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

        // The output lays the group key out as the first groupCount columns and
        // every group is co-located on one worker, so the result is partitioned by
        // iota(groupCount) — record it for a downstream aggregate to elide on.
        if (ctx.Workers > 1)
        {
            ctx.SetPartition(mapped, new PartitionInfo(ShardDisjoint: false, PartitionKey: Iota(groupCount)));
        }

        return mapped;
    }

    /// <summary>
    /// The column indices of a group-by / partition key when <em>every</em> key
    /// expression is a bare column reference (so the key survives in the output by
    /// identity and can be reasoned about for exchange elision); <c>null</c> if any
    /// key is a computed expression.
    /// </summary>
    private static int[]? BareColumnIndices(IReadOnlyList<ResolvedExpression> keys)
    {
        var indices = new int[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            if (keys[i] is not ResolvedColumn rc)
            {
                return null;
            }

            indices[i] = rc.Index;
        }

        return indices;
    }

    /// <summary>
    /// A synthetic key-row schema whose column types are those of a PARTITION BY /
    /// group-key expression list — the types <see cref="StablePartitionHash.OfWholeRow"/>
    /// dispatches on when hashing the projected key row.
    /// </summary>
    private static Schema SyntheticKeySchema(IReadOnlyList<ResolvedExpression> keys)
    {
        var cols = new SchemaColumn[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            cols[i] = new SchemaColumn("$pk" + i, keys[i].Type);
        }

        return new Schema(cols);
    }

    /// <summary>The identity index vector <c>[0, 1, …, n-1]</c>.</summary>
    private static int[] Iota(int n)
    {
        var a = new int[n];
        for (var i = 0; i < n; i++)
        {
            a[i] = i;
        }

        return a;
    }

    private static SqlAggregator BuildSqlAggregator(AggregateCall call)
    {
        switch (call.Kind)
        {
            case AggregateKind.CountStar:
                return new SqlCountStarAggregator();
            case AggregateKind.Count:
                return new SqlCountAggregator(ExpressionCompiler.CompileScalar(call.Argument!));
            case AggregateKind.CountDistinct:
                return new SqlCountDistinctAggregator(
                    ExpressionCompiler.CompileScalar(call.Argument!));
            case AggregateKind.ApproxCountDistinct:
                return new SqlApproxCountDistinctAggregator(
                    ExpressionCompiler.CompileScalar(call.Argument!));
            case AggregateKind.ApproxPercentile:
                return DdSketchSupport.BuildStructuralPercentile(call);
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
            case AggregateKind.VarSamp:
                return new SqlStddevAggregator(ExpressionCompiler.CompileScalar(call.Argument!), sample: true, sqrt: false);
            case AggregateKind.VarPop:
                return new SqlStddevAggregator(ExpressionCompiler.CompileScalar(call.Argument!), sample: false, sqrt: false);
            case AggregateKind.StddevSamp:
                return new SqlStddevAggregator(ExpressionCompiler.CompileScalar(call.Argument!), sample: true, sqrt: true);
            case AggregateKind.StddevPop:
                return new SqlStddevAggregator(ExpressionCompiler.CompileScalar(call.Argument!), sample: false, sqrt: true);
            default:
                throw new InvalidOperationException($"unsupported aggregate kind {call.Kind}");
        }
    }
}
