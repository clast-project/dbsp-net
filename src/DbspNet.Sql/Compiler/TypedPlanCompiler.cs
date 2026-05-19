using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Phase 1.1 of the typed-row lift. Compiles a narrow subset of plans
/// — currently just <c>SELECT * FROM t</c> (a bare
/// <see cref="ScanPlan"/>) — into a circuit whose streams carry the
/// per-schema emitted struct from <see cref="TypedRowEmitter"/>
/// instead of <see cref="StructuralRow"/>. Demonstrates the typed
/// pipeline mechanism end-to-end; subsequent phases extend the
/// supported plan shapes.
/// </summary>
public static class TypedPlanCompiler
{
    /// <summary>
    /// Attempts to compile <paramref name="plan"/> into a
    /// <see cref="TypedCompiledQuery"/>. Returns <c>false</c> if the
    /// plan shape isn't supported by this phase or if any referenced
    /// schema falls outside <see cref="TypedRowEmitter"/>'s scope —
    /// callers should fall back to
    /// <see cref="PlanToCircuit.Compile(LogicalPlan, ISqlSnapshotCodecs?)"/>.
    /// </summary>
    public static bool TryCompile(LogicalPlan plan, out TypedCompiledQuery? compiled)
    {
        ArgumentNullException.ThrowIfNull(plan);

        if (!TryUnwrapBareScan(plan, out var scan))
        {
            compiled = null;
            return false;
        }

        var rowType = TypedRowEmitter.EmitRowType(scan.Schema);
        if (rowType is null)
        {
            compiled = null;
            return false;
        }

        compiled = BuildPassThrough(scan, rowType);
        return true;
    }

    /// <summary>
    /// Recognises plan shapes equivalent to a bare table scan: either a
    /// <see cref="ScanPlan"/> directly, or a <see cref="ProjectPlan"/>
    /// whose projections are an in-order identity over an underlying
    /// <see cref="ScanPlan"/> (<c>SELECT * FROM t</c>).
    /// </summary>
    private static bool TryUnwrapBareScan(LogicalPlan plan, out ScanPlan scan)
    {
        if (plan is ScanPlan direct)
        {
            scan = direct;
            return true;
        }

        if (plan is ProjectPlan project
            && project.Input is ScanPlan inner
            && project.Projections.Count == inner.Schema.Count)
        {
            for (var i = 0; i < project.Projections.Count; i++)
            {
                if (project.Projections[i].Expression is not ResolvedColumn rc || rc.Index != i)
                {
                    scan = null!;
                    return false;
                }
            }

            scan = inner;
            return true;
        }

        scan = null!;
        return false;
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
        var weightOf = BuildWeightOf(rowType, factory, currentGetter);

        return new TypedCompiledQuery(
            circuit!, inputs, schema, rowType,
            currentGetter, currentReader, weightOf);
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
