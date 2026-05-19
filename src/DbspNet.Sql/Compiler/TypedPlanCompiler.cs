using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Typed-row compile path. Recognises a growing set of plan shapes —
/// today: bare table scan (<c>SELECT * FROM t</c>) and column-only
/// projections over a scan (<c>SELECT b, a, a FROM t</c>) — and builds
/// a circuit whose streams carry per-schema emitted structs from
/// <see cref="TypedRowEmitter"/> instead of
/// <see cref="StructuralRow"/>. Plans outside the supported subset
/// return <c>false</c>; callers fall back to
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

        if (!TryUnwrapColumnOnlyProject(plan, out var scan, out var columnIndices, out var outputSchema))
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

        if (IsIdentity(columnIndices, scan.Schema, outputSchema))
        {
            compiled = BuildPassThrough(scan, inputRowType);
            return true;
        }

        var outputRowType = TypedRowEmitter.EmitRowType(outputSchema);
        if (outputRowType is null)
        {
            compiled = null;
            return false;
        }

        compiled = BuildScanProject(scan, outputSchema, columnIndices, inputRowType, outputRowType);
        return true;
    }

    /// <summary>
    /// Recognises plan shapes the typed compile path supports:
    /// either a bare <see cref="ScanPlan"/> (treated as
    /// <see cref="ProjectPlan"/> with an identity column list), or a
    /// <see cref="ProjectPlan"/> over a <see cref="ScanPlan"/> whose
    /// projections are all <see cref="ResolvedColumn"/> (no
    /// expressions, literals, or casts — those land in the typed
    /// expression compiler phase). The column indices array gives the
    /// scan-column index for each output column.
    /// </summary>
    private static bool TryUnwrapColumnOnlyProject(
        LogicalPlan plan, out ScanPlan scan, out int[] columnIndices, out Schema outputSchema)
    {
        if (plan is ScanPlan direct)
        {
            scan = direct;
            columnIndices = Enumerable.Range(0, direct.Schema.Count).ToArray();
            outputSchema = direct.Schema;
            return true;
        }

        if (plan is ProjectPlan project && project.Input is ScanPlan inner)
        {
            var indices = new int[project.Projections.Count];
            for (var i = 0; i < project.Projections.Count; i++)
            {
                if (project.Projections[i].Expression is not ResolvedColumn rc)
                {
                    scan = null!;
                    columnIndices = null!;
                    outputSchema = null!;
                    return false;
                }

                indices[i] = rc.Index;
            }

            scan = inner;
            columnIndices = indices;
            outputSchema = project.Schema;
            return true;
        }

        scan = null!;
        columnIndices = null!;
        outputSchema = null!;
        return false;
    }

    private static bool IsIdentity(int[] columnIndices, Schema inputSchema, Schema outputSchema)
    {
        if (columnIndices.Length != inputSchema.Count) return false;
        for (var i = 0; i < columnIndices.Length; i++)
        {
            if (columnIndices[i] != i) return false;
        }

        // Same column types are guaranteed by ResolvedColumn semantics,
        // but the output schema's nullability could in theory differ
        // (projections through a NULL-introducing context, e.g. outer
        // join — not reachable from the ScanPlan path here). Belt and
        // braces: confirm by column count, since type-level differences
        // would already make the output row type emit a different
        // struct that the pass-through path can't satisfy.
        return outputSchema.Count == inputSchema.Count;
    }

    private static TypedCompiledQuery BuildPassThrough(ScanPlan scan, Type rowType)
    {
        var schema = scan.Schema;
        var factory = TypedRowEmitter.BuildBoxedFactory(schema)
            ?? throw new InvalidOperationException(
                "TypedRowEmitter accepted the schema but produced no factory");

        // Build the circuit with a single typed input feeding a single
        // typed output. All reflection-built generic calls close over
        // rowType.
        RootCircuit? circuit = null;
        object? handle = null;
        object? outputHandle = null;

        circuit = RootCircuit.Build(builder =>
        {
            var (h, stream) = InvokeZSetInput(builder, rowType);
            handle = h;
            outputHandle = InvokeOutput(builder, rowType, stream);
        });

        var inputs = new Dictionary<string, TypedTableInput>(StringComparer.Ordinal)
        {
            [scan.TableName] = new TypedTableInput(schema, rowType, factory, handle!),
        };

        var currentGetter = BuildCurrentZSetGetter(rowType, outputHandle!);
        var currentReader = BuildBoxedEntriesReader(rowType);
        var outputFactory = factory;
        var weightOf = BuildWeightOf(rowType, outputFactory, currentGetter);

        return new TypedCompiledQuery(
            circuit!, inputs, schema, rowType,
            currentGetter, currentReader, weightOf);
    }

    /// <summary>
    /// Builds a circuit shape <c>typed-input → MapRows → typed-output</c>:
    /// the input handle carries <typeparamref name="inputRowType"/>
    /// rows, a typed projection delegate (<c>Func&lt;TIn, TOut&gt;</c>,
    /// expression-tree-compiled, no boxing) projects them to
    /// <typeparamref name="outputRowType"/>, and the output handle
    /// carries those.
    /// </summary>
    private static TypedCompiledQuery BuildScanProject(
        ScanPlan scan, Schema outputSchema, int[] columnIndices,
        Type inputRowType, Type outputRowType)
    {
        var inputSchema = scan.Schema;
        var inputFactory = TypedRowEmitter.BuildBoxedFactory(inputSchema)
            ?? throw new InvalidOperationException(
                "TypedRowEmitter accepted the input schema but produced no factory");
        var outputFactory = TypedRowEmitter.BuildBoxedFactory(outputSchema)
            ?? throw new InvalidOperationException(
                "TypedRowEmitter accepted the output schema but produced no factory");

        var projectionDelegate = BuildTypedProjectionDelegate(
            inputRowType, outputRowType, outputSchema, columnIndices);

        RootCircuit? circuit = null;
        object? handle = null;
        object? outputHandle = null;

        circuit = RootCircuit.Build(builder =>
        {
            var (h, inputStream) = InvokeZSetInput(builder, inputRowType);
            handle = h;
            var outputStream = InvokeMapRows(
                builder, inputRowType, outputRowType, inputStream, projectionDelegate);
            outputHandle = InvokeOutput(builder, outputRowType, outputStream);
        });

        var inputs = new Dictionary<string, TypedTableInput>(StringComparer.Ordinal)
        {
            [scan.TableName] = new TypedTableInput(inputSchema, inputRowType, inputFactory, handle!),
        };

        var currentGetter = BuildCurrentZSetGetter(outputRowType, outputHandle!);
        var currentReader = BuildBoxedEntriesReader(outputRowType);
        var weightOf = BuildWeightOf(outputRowType, outputFactory, currentGetter);

        return new TypedCompiledQuery(
            circuit!, inputs, outputSchema, outputRowType,
            currentGetter, currentReader, weightOf);
    }

    /// <summary>
    /// Builds <c>(TIn in) =&gt; new TOut(in.F{idx[0]}, in.F{idx[1]}, ...)</c>
    /// as a <see cref="Delegate"/> of type <c>Func&lt;TIn, TOut&gt;</c>.
    /// Uses the typed-fields constructor emitted by
    /// <see cref="TypedRowEmitter"/>; no boxing or array allocation on
    /// the per-row path.
    /// </summary>
    private static Delegate BuildTypedProjectionDelegate(
        Type inputRowType, Type outputRowType, Schema outputSchema, int[] columnIndices)
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
        var args = new Expression[columnIndices.Length];
        for (var i = 0; i < columnIndices.Length; i++)
        {
            var srcField = inputRowType.GetField("F" + columnIndices[i])
                ?? throw new InvalidOperationException(
                    "emitted input row missing field F" + columnIndices[i]);
            args[i] = Expression.Field(inParam, srcField);
        }

        var body = Expression.New(ctor, args);
        var delegateType = typeof(Func<,>).MakeGenericType(inputRowType, outputRowType);
        return Expression.Lambda(delegateType, body, inParam).Compile();
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
