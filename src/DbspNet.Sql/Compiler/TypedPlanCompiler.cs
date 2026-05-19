using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Typed-row compile path. Recognises a growing set of plan shapes —
/// today: bare table scan, column or arbitrary-expression projections
/// over a scan, and a <see cref="FilterPlan"/> between them — and
/// builds a circuit whose streams carry per-schema emitted structs
/// from <see cref="TypedRowEmitter"/> instead of
/// <see cref="StructuralRow"/>. Projection and filter expressions are
/// lowered via <see cref="TypedExpressionCompiler"/>. Plans outside
/// the supported subset return <c>false</c>; callers fall back to
/// <see cref="PlanToCircuit.Compile(LogicalPlan, ISqlSnapshotCodecs?)"/>.
/// </summary>
public static class TypedPlanCompiler
{
    /// <summary>
    /// Attempts to compile <paramref name="plan"/> into a
    /// <see cref="TypedCompiledQuery"/>. Returns <c>false</c> if the
    /// plan shape isn't supported by this phase or if any referenced
    /// schema falls outside <see cref="TypedRowEmitter"/>'s scope.
    /// </summary>
    public static bool TryCompile(LogicalPlan plan, out TypedCompiledQuery? compiled)
    {
        ArgumentNullException.ThrowIfNull(plan);

        if (!TryUnwrapScanFilterProject(plan,
                out var scan, out var filterPredicate, out var projections, out var outputSchema))
        {
            compiled = null;
            return false;
        }

        var inputRowType = TypedRowEmitter.EmitRowType(scan.Schema);
        if (inputRowType is null)
        {
            compiled = null;
            return false;
        }

        var outputRowType = TypedRowEmitter.EmitRowType(outputSchema);
        if (outputRowType is null)
        {
            compiled = null;
            return false;
        }

        compiled = BuildScanFilterProject(
            scan, filterPredicate, projections, outputSchema,
            inputRowType, outputRowType);
        return compiled is not null;
    }

    /// <summary>
    /// Recognises plan shapes the typed compile path supports.
    /// Accepts (top → bottom):
    /// <list type="bullet">
    /// <item>bare <see cref="ScanPlan"/> (identity projection)</item>
    /// <item><see cref="ProjectPlan"/> over <see cref="ScanPlan"/></item>
    /// <item><see cref="ProjectPlan"/> over <see cref="FilterPlan"/> over <see cref="ScanPlan"/></item>
    /// <item><see cref="FilterPlan"/> over <see cref="ScanPlan"/> (identity projection)</item>
    /// </list>
    /// The projection list is whatever the plan exposes (or the
    /// identity list synthesised for bare scans); whether the
    /// expressions are typed-compilable is decided downstream.
    /// </summary>
    private static bool TryUnwrapScanFilterProject(
        LogicalPlan plan,
        out ScanPlan scan,
        out ResolvedExpression? filterPredicate,
        out IReadOnlyList<ProjectionItem> projections,
        out Schema outputSchema)
    {
        scan = null!;
        filterPredicate = null;
        projections = null!;
        outputSchema = null!;

        LogicalPlan inner;
        if (plan is ProjectPlan project)
        {
            projections = project.Projections;
            outputSchema = project.Schema;
            inner = project.Input;
        }
        else
        {
            inner = plan;
        }

        if (inner is FilterPlan filter)
        {
            filterPredicate = filter.Predicate;
            inner = filter.Input;
        }

        if (inner is not ScanPlan s) return false;
        scan = s;

        if (projections is null)
        {
            projections = IdentityProjections(scan.Schema);
            outputSchema = scan.Schema;
        }

        return true;
    }

    private static IReadOnlyList<ProjectionItem> IdentityProjections(Schema schema)
    {
        var items = new ProjectionItem[schema.Count];
        for (var i = 0; i < schema.Count; i++)
        {
            items[i] = new ProjectionItem(
                new ResolvedColumn(i, schema[i].Type),
                schema[i].Name,
                schema[i].Qualifier);
        }

        return items;
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

    /// <summary>
    /// Builds the typed circuit for a scan + optional filter +
    /// optional non-identity projection. Returns <c>null</c> if the
    /// predicate or any projection expression is outside
    /// <see cref="TypedExpressionCompiler"/>'s scope. The circuit
    /// shape is, in order:
    /// <list type="bullet">
    /// <item><c>typed input</c></item>
    /// <item><c>Filter&lt;TIn, Z64&gt;</c> (only if a predicate is present)</item>
    /// <item><c>MapRows&lt;TIn, TOut, Z64&gt;</c> (only if the projection isn't an identity)</item>
    /// <item><c>typed output</c></item>
    /// </list>
    /// </summary>
    private static TypedCompiledQuery? BuildScanFilterProject(
        ScanPlan scan,
        ResolvedExpression? filterPredicate,
        IReadOnlyList<ProjectionItem> projections,
        Schema outputSchema,
        Type inputRowType,
        Type outputRowType)
    {
        var inputSchema = scan.Schema;
        var inputFactory = TypedRowEmitter.BuildBoxedFactory(inputSchema)
            ?? throw new InvalidOperationException(
                "TypedRowEmitter accepted the input schema but produced no factory");
        var outputFactory = TypedRowEmitter.BuildBoxedFactory(outputSchema)
            ?? throw new InvalidOperationException(
                "TypedRowEmitter accepted the output schema but produced no factory");

        Delegate? predicateDelegate = null;
        if (filterPredicate is not null)
        {
            predicateDelegate = BuildTypedPredicateDelegate(filterPredicate, inputRowType);
            if (predicateDelegate is null) return null;
        }

        var identityProjection = IsIdentityProjection(projections, inputSchema, outputSchema);
        Delegate? projectionDelegate = null;
        if (!identityProjection)
        {
            projectionDelegate = BuildTypedProjectionDelegate(
                inputRowType, outputRowType, outputSchema, projections);
            if (projectionDelegate is null) return null;
        }

        RootCircuit? circuit = null;
        object? handle = null;
        object? outputHandle = null;

        circuit = RootCircuit.Build(builder =>
        {
            var (h, stream) = InvokeZSetInput(builder, inputRowType);
            handle = h;
            if (predicateDelegate is not null)
            {
                stream = InvokeFilter(builder, inputRowType, stream, predicateDelegate);
            }

            if (projectionDelegate is not null)
            {
                stream = InvokeMapRows(builder, inputRowType, outputRowType, stream, projectionDelegate);
                outputHandle = InvokeOutput(builder, outputRowType, stream);
            }
            else
            {
                outputHandle = InvokeOutput(builder, inputRowType, stream);
            }
        });

        var inputs = new Dictionary<string, TypedTableInput>(StringComparer.Ordinal)
        {
            [scan.TableName] = new TypedTableInput(inputSchema, inputRowType, inputFactory, handle!),
        };

        // Output row type / factory depends on whether a non-identity
        // projection was applied. Identity → stream still carries TIn.
        var streamRowType = identityProjection ? inputRowType : outputRowType;
        var streamFactory = identityProjection ? inputFactory : outputFactory;
        var streamSchema = identityProjection ? inputSchema : outputSchema;

        var currentGetter = BuildCurrentZSetGetter(streamRowType, outputHandle!);
        var currentReader = BuildBoxedEntriesReader(streamRowType);
        var weightOf = BuildWeightOf(streamRowType, streamFactory, currentGetter);

        return new TypedCompiledQuery(
            circuit!, inputs, streamSchema, streamRowType,
            currentGetter, currentReader, weightOf);
    }

    /// <summary>
    /// Builds <c>(TIn in) =&gt; new TOut(expr_0(in), expr_1(in), ...)</c>
    /// as a <see cref="Delegate"/> of type <c>Func&lt;TIn, TOut&gt;</c>
    /// by lowering each projection through
    /// <see cref="TypedExpressionCompiler.TryBuildInto"/> and feeding
    /// the results into the emitted typed-fields constructor. Returns
    /// <c>null</c> if any projection expression is outside scope.
    /// </summary>
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
            // Widen to the ctor's expected param type when the
            // expression compiler returned a slightly narrower CLR
            // type (e.g. int → long).
            args[i] = built.Type == ctorParamTypes[i]
                ? built
                : Expression.Convert(built, ctorParamTypes[i]);
        }

        var body = Expression.New(ctor, args);
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, outputRowType);
        return Expression.Lambda(delegateType, body, inParam).Compile();
    }

    /// <summary>
    /// Builds a typed predicate <c>Func&lt;TIn, bool&gt;</c>. Returns
    /// <c>null</c> if the predicate is outside the typed expression
    /// compiler's scope, or if it doesn't reduce to a plain
    /// non-nullable BOOLEAN.
    /// </summary>
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

    // ---- Reflection-built generic calls ----

    /// <summary>builder.ZSetInput&lt;TRow, Z64&gt;()</summary>
    private static (object Handle, object Stream) InvokeZSetInput(CircuitBuilder builder, Type rowType)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.ZSetInput) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        var tuple = closed.Invoke(null, new object[] { builder })!;
        // The tuple type is (InputHandle<ZSet<TRow, Z64>>, Stream<ZSet<TRow, Z64>>)
        var tupleType = tuple.GetType();
        var item1 = tupleType.GetField("Item1")!.GetValue(tuple)!;
        var item2 = tupleType.GetField("Item2")!.GetValue(tuple)!;
        return (item1, item2);
    }

    /// <summary>
    /// <c>builder.Filter&lt;TRow, Z64&gt;(stream, predicate)</c>.
    /// </summary>
    private static object InvokeFilter(
        CircuitBuilder builder, Type rowType, object stream, Delegate predicate)
    {
        var openMethod = typeof(LinearOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(LinearOperators.Filter) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(rowType, typeof(Z64));
        return closed.Invoke(null, new object[] { builder, stream, predicate })!;
    }

    /// <summary>
    /// <c>builder.MapRows&lt;TIn, TOut, Z64&gt;(inputStream, projection)</c>.
    /// </summary>
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
        // () => outputHandle.Current  (where Current : ZSet<TRow, Z64>)
        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        var handleType = typeof(OutputHandle<>).MakeGenericType(zsetType);
        var currentProp = handleType.GetProperty(nameof(OutputHandle<int>.Current))!;
        var getCurrent = currentProp.GetGetMethod()!;
        var call = Expression.Call(Expression.Constant(outputHandle), getCurrent);
        var boxed = Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<object>>(boxed).Compile();
    }

    /// <summary>
    /// Returns a function that, given a boxed
    /// <c>ZSet&lt;TRow, Z64&gt;</c>, enumerates its
    /// <c>(boxed-row, weight)</c> entries.
    /// </summary>
    private static Func<object, IEnumerable<KeyValuePair<object, Z64>>> BuildBoxedEntriesReader(Type rowType)
        => EnumerateBoxedEntries;

    private static IEnumerable<KeyValuePair<object, Z64>> EnumerateBoxedEntries(object zset)
    {
        // ZSet<TRow, Z64> implements IEnumerable<KeyValuePair<TRow, Z64>>.
        // Use the non-generic IEnumerable to avoid building a typed
        // enumerator. Each element is a struct KeyValuePair<TRow, Z64>;
        // reflect it back to (object, Z64).
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
        // (values) => current.WeightOf((TRow)factory(values))
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
