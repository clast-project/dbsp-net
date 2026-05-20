using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
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

    /// <summary>
    /// One node's compiled output: the closed CLR type of rows on the
    /// stream, the SQL schema of that row, and the (boxed) typed
    /// stream object.
    /// </summary>
    private sealed record TypedNode(Type RowType, Schema Schema, object Stream);

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
        ISqlSnapshotCodecs? snapshotCodecs = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(structuralScans);
        ArgumentNullException.ThrowIfNull(outputCodec);

        try
        {
            var ctx = new CompileContext(builder, structuralScans, snapshotCodecs);
            var topNode = TryCompileNode(plan, ctx)
                ?? throw new UnsupportedPlanException();

            return AdaptTypedToStructural(builder, topNode, outputCodec);
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

        public CompileContext(CircuitBuilder builder, Dictionary<string, TypedTableInput> inputs)
        {
            Builder = builder;
            Inputs = inputs;
            StructuralScans = null;
            SnapshotCodecs = null;
        }

        public CompileContext(
            CircuitBuilder builder,
            IReadOnlyDictionary<string, Stream<ZSet<StructuralRow, Z64>>> structuralScans,
            ISqlSnapshotCodecs? snapshotCodecs)
        {
            Builder = builder;
            Inputs = null;
            StructuralScans = structuralScans;
            SnapshotCodecs = snapshotCodecs;
        }
    }

    private sealed class UnsupportedPlanException : Exception;

    private static TypedNode? TryCompileNode(LogicalPlan plan, CompileContext ctx) => plan switch
    {
        ScanPlan s => CompileScan(s, ctx),
        FilterPlan f => CompileFilter(f, ctx),
        ProjectPlan p => CompileProject(p, ctx),
        JoinPlan j => CompileJoin(j, ctx),
        AggregatePlan a => CompileAggregate(a, ctx),
        UnionAllPlan u => CompileUnionAll(u, ctx),
        DistinctPlan d => CompileDistinct(d, ctx),
        DifferencePlan diff => CompileDifference(diff, ctx),
        CteScanPlan c => CompileCteScan(c, ctx),
        ScalarSubqueryJoinPlan s => CompileScalarSubqueryJoin(s, ctx),
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
        var (handle, stream) = InvokeZSetInput(ctx.Builder, rowType);
        ctx.Inputs![scan.TableName] = new TypedTableInput(scan.Schema, rowType, factory, handle);
        var standaloneNode = new TypedNode(rowType, scan.Schema, stream);
        ctx.LiftedScanCache[scan.TableName] = standaloneNode;
        return standaloneNode;
    }

    private static TypedNode? CompileFilter(FilterPlan filter, CompileContext ctx)
    {
        var inner = TryCompileNode(filter.Input, ctx);
        if (inner is null) return null;

        // Nullable<bool> WHERE predicates are coerced to plain
        // bool inside BuildTypedPredicateDelegate via
        // Nullable<bool>.GetValueOrDefault() — SQL WHERE semantics:
        // TRUE keeps the row; FALSE and NULL both drop it.
        var predicate = BuildTypedPredicateDelegate(filter.Predicate, inner.RowType);
        if (predicate is null) return null;

        var stream = InvokeFilter(ctx.Builder, inner.RowType, inner.Stream, predicate);
        return new TypedNode(inner.RowType, inner.Schema, stream);
    }

    private static TypedNode? CompileProject(ProjectPlan project, CompileContext ctx)
    {
        var inner = TryCompileNode(project.Input, ctx);
        if (inner is null) return null;

        // Respect the resolver's per-column nullability on the
        // projection's output schema so genuinely-nullable
        // expressions (NULLIF, NULL literal) can be carried as
        // Nullable<T> fields downstream. Aggregate output schemas
        // continue to be stripped inside CompileAggregate where the
        // operator's linear-emission gate guarantees non-null
        // values.
        var outputSchema = project.Schema;
        if (IsIdentityProjection(project.Projections, inner.Schema, outputSchema))
        {
            return inner;
        }

        var outputRowType = TypedRowEmitter.EmitRowType(outputSchema);
        if (outputRowType is null) return null;

        var projDelegate = BuildTypedProjectionDelegate(
            inner.RowType, outputRowType, outputSchema, project.Projections);
        if (projDelegate is null) return null;

        var stream = InvokeMapRows(
            ctx.Builder, inner.RowType, outputRowType, inner.Stream, projDelegate);
        return new TypedNode(outputRowType, outputSchema, stream);
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
        if (plan.JoinType is not (AstJoinType.Inner or AstJoinType.LeftOuter or AstJoinType.RightOuter))
        {
            return null;
        }

        if (plan.EquiKeys.Count == 0) return null;

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

        var keySchema = left.Schema.SubsetByIndex(leftIndices);
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

        var leftIndexed = InvokeGroupProject(
            ctx.Builder, left.RowType, keyRowType, left.Stream, leftKeyExtractor);
        var rightIndexed = InvokeGroupProject(
            ctx.Builder, right.RowType, keyRowType, right.Stream, rightKeyExtractor);

        var leftSnapshotCodec = BuildAdaptedIndexedCodec(
            ctx.SnapshotCodecs, keySchema, left.Schema, keyRowType, left.RowType);
        var rightSnapshotCodec = BuildAdaptedIndexedCodec(
            ctx.SnapshotCodecs, keySchema, right.Schema, keyRowType, right.RowType);

        object joined;
        if (plan.JoinType is AstJoinType.Inner)
        {
            joined = InvokeIncrementalInnerJoin(
                ctx.Builder, keyRowType, left.RowType, right.RowType, outputRowType,
                leftIndexed, rightIndexed, combineDelegate,
                leftSnapshotCodec, rightSnapshotCodec);
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
            joined = InvokeIncrementalLeftJoin(
                ctx.Builder, keyRowType, left.RowType, right.RowType, outputRowType,
                leftIndexed, rightIndexed, combineDelegate, nullPad,
                leftSnapshotCodec, rightSnapshotCodec);
        }
        else // RightOuter
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
            joined = InvokeIncrementalLeftJoin(
                ctx.Builder, keyRowType, right.RowType, left.RowType, outputRowType,
                rightIndexed, leftIndexed, swappedCombine, nullPad,
                rightSnapshotCodec, leftSnapshotCodec);
        }

        var node = new TypedNode(outputRowType, outputSchema, joined);

        if (plan.Residual is not null)
        {
            // Only INNER reaches here (LEFT/RIGHT bail above).
            var residual = BuildTypedPredicateDelegate(plan.Residual, outputRowType);
            if (residual is null) return null;
            var filtered = InvokeFilter(ctx.Builder, outputRowType, joined, residual);
            node = new TypedNode(outputRowType, outputSchema, filtered);
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

            var joined = InvokeIncrementalLeftJoin(
                ctx.Builder, unitKeyType, current.RowType, sub.RowType, newRowType,
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

        var output = InvokeDistinct(ctx.Builder, inner.RowType, inner.Stream, snapshotCodec);
        return new TypedNode(inner.RowType, plan.Schema, output);
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
        // Group-by keys must be ResolvedColumn (matches resolver
        // restriction in v1) so we can index by typed field reads.
        var groupIndices = new int[plan.GroupKeys.Count];
        for (var i = 0; i < plan.GroupKeys.Count; i++)
        {
            if (plan.GroupKeys[i] is not ResolvedColumn rc) return null;
            groupIndices[i] = rc.Index;
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

        // TKey from the group-key columns. Same schema-fingerprint
        // sharing as anywhere else.
        var keySchema = inner.Schema.SubsetByIndex(groupIndices);
        var keyRowType = TypedRowEmitter.EmitRowType(keySchema);
        if (keyRowType is null) return null;

        var keyExtractor = BuildKeyExtractorDelegate(inner.RowType, keyRowType, keySchema, groupIndices);
        if (keyExtractor is null) return null;

        // Per-aggregate-call schema for TAgg, parallel to plan.Aggregates.
        // We use plan.Aggregates (not plan.Schema) because the resolver
        // may collect more AggregateCalls than it surfaces in Schema
        // when an aggregate is referenced from both SELECT and HAVING.
        var aggColumns = new SchemaColumn[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            aggColumns[i] = new SchemaColumn(
                "$agg" + i,
                TypedAggregateResultType(plan.Aggregates[i]));
        }

        var aggSchema = new Schema(aggColumns);
        var aggRowType = TypedRowEmitter.EmitRowType(aggSchema);
        if (aggRowType is null) return null;

        // Build the per-aggregator typed objects.
        var aggregators = new object[plan.Aggregates.Count];
        for (var i = 0; i < plan.Aggregates.Count; i++)
        {
            var built = BuildTypedAggregator(plan.Aggregates[i], inner.RowType);
            if (built is null) return null;
            aggregators[i] = built;
        }

        var aggArray = CastAggregatorArray(inner.RowType, aggregators);

        var packResults = TypedRowEmitter.BuildBoxedFactory(aggSchema)!;
        // packResults returns object; wrap into a typed Func<object?[], TAgg>
        // via Expression compilation.
        var packParam = Expression.Parameter(typeof(object?[]), "vs");
        var packCall = Expression.Convert(
            Expression.Invoke(Expression.Constant(packResults), packParam),
            aggRowType);
        var packDelegateType = typeof(Func<,>).MakeGenericType(typeof(object?[]), aggRowType);
        var typedPack = Expression.Lambda(packDelegateType, packCall, packParam).Compile();

        // new TypedCompositeAggregator<TIn, TAgg>(aggArray, typedPack)
        var compositeOpen = typeof(TypedCompositeAggregator<,>);
        var compositeClosed = compositeOpen.MakeGenericType(inner.RowType, aggRowType);
        var composite = Activator.CreateInstance(compositeClosed, aggArray, typedPack)!;

        // GroupProject<TKey, TIn, TIn, Z64>(inner, keyExtractor, identity)
        var indexed = InvokeGroupProject(
            ctx.Builder, inner.RowType, keyRowType, inner.Stream, keyExtractor);

        // IncrementalAggregate<TKey, TIn, TAgg>(indexed, composite)
        // Output: Stream<ZSet<(TKey, TAgg), Z64>>
        var aggSnapshotCodec = BuildAdaptedIndexedCodec(
            ctx.SnapshotCodecs, keySchema, inner.Schema, keyRowType, inner.RowType);
        var aggregated = InvokeIncrementalAggregate(
            ctx.Builder, keyRowType, inner.RowType, aggRowType, indexed, composite,
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

        return new TypedNode(finalRowType, finalSchema, flatStream);
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

    // ---- Helpers: identity / projection / predicate ----

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
    /// Shim called from the compiled MapRows lambda — wraps the
    /// boxed-field array as a span and dispatches to
    /// <see cref="IRowCodec{TRow}.BuildRow"/>.
    /// </summary>
    private static StructuralRow BuildStructuralRowFromArray(
        IRowCodec<StructuralRow> codec, Schema schema, object?[] values)
        => codec.BuildRow(schema, values);

    // ---- Reflection-built generic calls ----

    /// <summary>builder.ZSetInput&lt;TRow, Z64&gt;()</summary>
    private static (object Handle, object Stream) InvokeZSetInput(CircuitBuilder builder, Type rowType)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.ZSetInput) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        var tuple = closed.Invoke(null, new object[] { builder })!;
        var tupleType = tuple.GetType();
        var item1 = tupleType.GetField("Item1")!.GetValue(tuple)!;
        var item2 = tupleType.GetField("Item2")!.GetValue(tuple)!;
        return (item1, item2);
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
        return closed.Invoke(null, new object?[] { builder, input, snapshotCodec })!;
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
    /// <c>builder.IncrementalInnerJoin&lt;TKey, TLeft, TRight, TOut, Z64&gt;(...)</c>.
    /// </summary>
    private static object InvokeIncrementalInnerJoin(
        CircuitBuilder builder, Type keyRowType, Type leftRowType, Type rightRowType,
        Type outputRowType, object leftIndexed, object rightIndexed, Delegate combine,
        object? leftSnapshotCodec = null, object? rightSnapshotCodec = null)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.IncrementalInnerJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, combine,
            leftSnapshotCodec, rightSnapshotCodec,
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
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, joinCombine, nullPadCombine,
            leftSnapshotCodec, rightSnapshotCodec,
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
        return closed.Invoke(null, new object?[]
        {
            builder, indexed, aggregator, snapshotCodec,
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
        => EnumerateBoxedEntries;

    private static IEnumerable<KeyValuePair<object, Z64>> EnumerateBoxedEntries(object zset)
    {
        foreach (var kv in (System.Collections.IEnumerable)zset)
        {
            var kvType = kv!.GetType();
            var key = kvType.GetProperty(nameof(KeyValuePair<int, int>.Key))!.GetValue(kv)!;
            var value = (Z64)kvType.GetProperty(nameof(KeyValuePair<int, int>.Value))!.GetValue(kv)!;
            yield return new KeyValuePair<object, Z64>(key, value);
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
