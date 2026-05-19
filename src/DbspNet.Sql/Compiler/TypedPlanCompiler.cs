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

    private sealed class CompileContext
    {
        public CircuitBuilder Builder { get; }

        public Dictionary<string, TypedTableInput> Inputs { get; }

        public CompileContext(CircuitBuilder builder, Dictionary<string, TypedTableInput> inputs)
        {
            Builder = builder;
            Inputs = inputs;
        }
    }

    private sealed class UnsupportedPlanException : Exception;

    private static TypedNode? TryCompileNode(LogicalPlan plan, CompileContext ctx) => plan switch
    {
        ScanPlan s => CompileScan(s, ctx),
        FilterPlan f => CompileFilter(f, ctx),
        ProjectPlan p => CompileProject(p, ctx),
        JoinPlan { JoinType: AstJoinType.Inner } j => CompileInnerJoin(j, ctx),
        _ => null,
    };

    // ---- Plan node dispatch ----

    private static TypedNode? CompileScan(ScanPlan scan, CompileContext ctx)
    {
        var rowType = TypedRowEmitter.EmitRowType(scan.Schema);
        if (rowType is null) return null;

        var factory = TypedRowEmitter.BuildBoxedFactory(scan.Schema)!;
        var (handle, stream) = InvokeZSetInput(ctx.Builder, rowType);
        ctx.Inputs[scan.TableName] = new TypedTableInput(scan.Schema, rowType, factory, handle);
        return new TypedNode(rowType, scan.Schema, stream);
    }

    private static TypedNode? CompileFilter(FilterPlan filter, CompileContext ctx)
    {
        var inner = TryCompileNode(filter.Input, ctx);
        if (inner is null) return null;

        var predicate = BuildTypedPredicateDelegate(filter.Predicate, inner.RowType);
        if (predicate is null) return null;

        var stream = InvokeFilter(ctx.Builder, inner.RowType, inner.Stream, predicate);
        return new TypedNode(inner.RowType, inner.Schema, stream);
    }

    private static TypedNode? CompileProject(ProjectPlan project, CompileContext ctx)
    {
        var inner = TryCompileNode(project.Input, ctx);
        if (inner is null) return null;

        if (IsIdentityProjection(project.Projections, inner.Schema, project.Schema))
        {
            return inner;
        }

        var outputRowType = TypedRowEmitter.EmitRowType(project.Schema);
        if (outputRowType is null) return null;

        var projDelegate = BuildTypedProjectionDelegate(
            inner.RowType, outputRowType, project.Schema, project.Projections);
        if (projDelegate is null) return null;

        var stream = InvokeMapRows(
            ctx.Builder, inner.RowType, outputRowType, inner.Stream, projDelegate);
        return new TypedNode(outputRowType, project.Schema, stream);
    }

    /// <summary>
    /// Compiles an <see cref="AstJoinType.Inner"/> equi-join. Both
    /// sides are recursively typed; the equi-key columns become a
    /// per-join emitted struct (<c>TKey</c>); the combined row is the
    /// concatenation of the two sides' columns (left first, then
    /// right). A residual is applied as a subsequent typed
    /// <see cref="LinearOperators.Filter"/>.
    /// </summary>
    /// <remarks>
    /// LEFT / RIGHT OUTER joins are out of scope: NULL-padding the
    /// missing side requires nullable output columns, which the typed
    /// row gate rejects.
    /// </remarks>
    private static TypedNode? CompileInnerJoin(JoinPlan plan, CompileContext ctx)
    {
        if (plan.EquiKeys.Count == 0) return null;

        var left = TryCompileNode(plan.Left, ctx);
        if (left is null) return null;
        var right = TryCompileNode(plan.Right, ctx);
        if (right is null) return null;

        // Equi-key columns must be NOT NULL on both sides — the typed
        // row gate enforces this at the scan level, but a key column
        // could in principle come from a derived plan with a nullable
        // result. Belt and braces: reject explicitly.
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

        var outputRowType = TypedRowEmitter.EmitRowType(plan.Schema);
        if (outputRowType is null) return null;

        var combineDelegate = BuildJoinCombineDelegate(
            keyRowType, left.RowType, right.RowType, outputRowType,
            plan.Schema, left.Schema.Count, right.Schema.Count);

        var leftIndexed = InvokeGroupProject(
            ctx.Builder, left.RowType, keyRowType, left.Stream, leftKeyExtractor);
        var rightIndexed = InvokeGroupProject(
            ctx.Builder, right.RowType, keyRowType, right.Stream, rightKeyExtractor);

        var joined = InvokeIncrementalInnerJoin(
            ctx.Builder, keyRowType, left.RowType, right.RowType, outputRowType,
            leftIndexed, rightIndexed, combineDelegate);

        var node = new TypedNode(outputRowType, plan.Schema, joined);

        if (plan.Residual is not null)
        {
            var residual = BuildTypedPredicateDelegate(plan.Residual, outputRowType);
            if (residual is null) return null;
            var filtered = InvokeFilter(ctx.Builder, outputRowType, joined, residual);
            node = new TypedNode(outputRowType, plan.Schema, filtered);
        }

        return node;
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
        var ctorParamTypes = new Type[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            ctorParamTypes[i] = outputSchema[i].Type.ClrType;
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
        if (predicate.Type is not SqlBooleanType { Nullable: false }) return null;

        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(predicate, inParam);
        if (built is null || built.Type != typeof(bool)) return null;

        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(bool));
        return Expression.Lambda(delegateType, built, inParam).Compile();
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
            ctorParamTypes[i] = keySchema[i].Type.ClrType;
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
        var ctorParamTypes = new Type[outputSchema.Count];
        for (var i = 0; i < outputSchema.Count; i++)
        {
            ctorParamTypes[i] = outputSchema[i].Type.ClrType;
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
        Type outputRowType, object leftIndexed, object rightIndexed, Delegate combine)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.IncrementalInnerJoin)
                && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(
            keyRowType, leftRowType, rightRowType, outputRowType, typeof(Z64));
        // Two trailing optional snapshot-codec args default to null; pass null.
        return closed.Invoke(null, new object?[]
        {
            builder, leftIndexed, rightIndexed, combine, null, null,
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
