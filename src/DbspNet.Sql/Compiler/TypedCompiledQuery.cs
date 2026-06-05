// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Typed-row variant of <see cref="CompiledQuery"/>: the operator graph
/// is built over the per-schema struct emitted by
/// <see cref="TypedRowEmitter"/> rather than <see cref="StructuralRow"/>.
/// The public API stays <c>object?[]</c>-shaped at the boundary —
/// callers don't need to know the closed row type.
/// </summary>
/// <remarks>
/// Phase 1.1: only constructed for plans the typed compile path
/// supports (today: bare <c>SELECT * FROM t</c>). Subsequent phases
/// extend the supported plan shapes.
/// </remarks>
public sealed class TypedCompiledQuery
{
    internal TypedCompiledQuery(
        RootCircuit circuit,
        IReadOnlyDictionary<string, TypedTableInput> inputs,
        Schema outputSchema,
        Type outputRowType,
        Func<object> currentZSetGetter,
        Func<object, IEnumerable<KeyValuePair<object, Z64>>> currentReader,
        Func<object?[], Z64> outputWeightOf)
    {
        Circuit = circuit;
        Inputs = inputs;
        OutputSchema = outputSchema;
        OutputRowType = outputRowType;
        _currentZSetGetter = currentZSetGetter;
        _currentReader = currentReader;
        _outputWeightOf = outputWeightOf;
    }

    private readonly Func<object> _currentZSetGetter;
    private readonly Func<object, IEnumerable<KeyValuePair<object, Z64>>> _currentReader;
    private readonly Func<object?[], Z64> _outputWeightOf;

    public RootCircuit Circuit { get; }

    public IReadOnlyDictionary<string, TypedTableInput> Inputs { get; }

    public Schema OutputSchema { get; }

    /// <summary>The closed emitted row type used at the output stage.</summary>
    public Type OutputRowType { get; }

    public TypedTableInput Table(string name) => Inputs[name];

    public void Step() => Circuit.Step();

    /// <summary>
    /// Returns the current output Z-set as a sequence of
    /// <c>(values, weight)</c> tuples. Values are decoded back to the
    /// public CLR representation (e.g. <c>string</c> for VARCHAR),
    /// matching the input shape passed to
    /// <see cref="TypedTableInput.Insert"/>.
    /// </summary>
    public IEnumerable<(object?[] Values, long Weight)> Current =>
        TypedOutputDecoder.Decode(OutputSchema, _currentZSetGetter, _currentReader);

    /// <summary>
    /// Look up the weight of a specific row in the current output. As
    /// with <see cref="CompiledQuery.WeightOf"/>, values are
    /// boundary-encoded against the schema.
    /// </summary>
    public Z64 WeightOf(params object?[] values) =>
        _outputWeightOf(BoundaryEncoder.Encode(OutputSchema, values));
}

/// <summary>
/// Typed-row variant of <see cref="TableInput"/>. Wraps a handle whose
/// <c>Push(ZSet&lt;TEmitted, Z64&gt;)</c> accepts the emitted row type — either an
/// <see cref="InputHandle{T}"/> (single circuit) or a
/// <see cref="ShardedInputHandle{TKey,TWeight}"/> (parallel circuit), which share
/// that signature. Insert / Delete / Push at the boundary take
/// <c>object?[]</c>; internally the typed row struct is constructed
/// once per row and pushed through the handle.
/// </summary>
public sealed class TypedTableInput
{
    private readonly Func<object?[], object>? _factory;
    private readonly Action<object>? _pushOneBoxedRowPositive;
    private readonly Action<object>? _pushOneBoxedRowNegative;
    private readonly Action<IEnumerable<(object Row, long Weight)>>? _pushBatch;
    private readonly ITableIngestor? _ingestor;

    internal TypedTableInput(
        Schema schema,
        Type rowType,
        Func<object?[], object> factory,
        object handle)
    {
        Schema = schema;
        RowType = rowType;
        _factory = factory;
        _pushOneBoxedRowPositive = BuildPushSingleton(rowType, handle, +1L);
        _pushOneBoxedRowNegative = BuildPushSingleton(rowType, handle, -1L);
        _pushBatch = BuildPushBatch(rowType, handle);
    }

    /// <summary>
    /// Parallel-circuit variant: routes Insert / Delete / Push through an
    /// <see cref="ITableIngestor"/> that shards the rows across the replicas, with
    /// the boundary encode running on the worker threads (see
    /// <see cref="ParallelIngestor{TRow}"/>).
    /// </summary>
    internal TypedTableInput(Schema schema, Type rowType, ITableIngestor ingestor)
    {
        Schema = schema;
        RowType = rowType;
        _ingestor = ingestor;
    }

    public Schema Schema { get; }

    /// <summary>The closed emitted row type carried through the input handle.</summary>
    public Type RowType { get; }

    public void Insert(params object?[] values)
    {
        ValidateArity(values);
        if (_ingestor is not null)
        {
            _ingestor.PushOne(values, +1L);
            return;
        }

        var row = _factory!(BoundaryEncoder.Encode(Schema, values));
        _pushOneBoxedRowPositive!(row);
    }

    public void Delete(params object?[] values)
    {
        ValidateArity(values);
        if (_ingestor is not null)
        {
            _ingestor.PushOne(values, -1L);
            return;
        }

        var row = _factory!(BoundaryEncoder.Encode(Schema, values));
        _pushOneBoxedRowNegative!(row);
    }

    public void Push(IEnumerable<(object?[] Values, long Weight)> deltas)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        if (_ingestor is not null)
        {
            // Hand the raw rows straight to the ingestor — it encodes on the
            // worker threads. Reuse the caller's list when it is already one
            // (the common large-batch case), else materialize once.
            var raw = deltas as IReadOnlyList<(object?[] Values, long Weight)> ?? new List<(object?[], long)>(deltas);
            _ingestor.Push(raw);
            return;
        }

        var rows = new List<(object Row, long Weight)>();
        foreach (var (vs, w) in deltas)
        {
            ValidateArity(vs);
            rows.Add((_factory!(BoundaryEncoder.Encode(Schema, vs)), w));
        }

        _pushBatch!(rows);
    }

    private void ValidateArity(object?[] values)
    {
        if (values.Length != Schema.Count)
        {
            throw new ArgumentException(
                $"row arity {values.Length} does not match schema arity {Schema.Count}",
                nameof(values));
        }
    }

    // ---------- Reflection-built delegates ----------

    /// <summary>
    /// Builds <c>(boxedRow) =&gt; handle.Push(ZSet.Singleton((TRow)boxedRow, new Z64(weight)))</c>
    /// for a fixed sign.
    /// </summary>
    private static Action<object> BuildPushSingleton(Type rowType, object handle, long weight)
    {
        var z64Ctor = typeof(Z64).GetConstructor([typeof(long)])!;
        var singletonOpen = typeof(ZSet)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(ZSet.Singleton) && m.IsGenericMethodDefinition);
        var singletonClosed = singletonOpen.MakeGenericMethod(rowType, typeof(Z64));

        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        // Bind against the handle's runtime type, not InputHandle specifically, so
        // a ShardedInputHandle (same Push(ZSet) signature) works unchanged.
        var pushMethod = handle.GetType().GetMethod("Push", [zsetType])!;

        var boxedRowParam = Expression.Parameter(typeof(object), "boxed");
        var castRow = Expression.Convert(boxedRowParam, rowType);
        var weightExpr = Expression.New(z64Ctor, Expression.Constant(weight));
        var singletonCall = Expression.Call(null, singletonClosed, castRow, weightExpr);
        var pushCall = Expression.Call(Expression.Constant(handle), pushMethod, singletonCall);

        return Expression.Lambda<Action<object>>(pushCall, boxedRowParam).Compile();
    }

    /// <summary>
    /// Builds a batch push: iterate the supplied (row, weight) pairs into a
    /// <c>ZSetBuilder&lt;TRow, Z64&gt;</c>, then <c>handle.Push(builder.Build())</c>.
    /// </summary>
    private static Action<IEnumerable<(object Row, long Weight)>> BuildPushBatch(Type rowType, object handle)
    {
        var zsetType = typeof(ZSet<,>).MakeGenericType(rowType, typeof(Z64));
        var builderType = typeof(ZSetBuilder<,>).MakeGenericType(rowType, typeof(Z64));
        var builderCtor = builderType.GetConstructor(Type.EmptyTypes)!;
        var addMethod = builderType.GetMethod(nameof(ZSetBuilder<int, Z64>.Add), [rowType, typeof(Z64)])!;
        var buildMethod = builderType.GetMethod(nameof(ZSetBuilder<int, Z64>.Build))!;
        var z64Ctor = typeof(Z64).GetConstructor([typeof(long)])!;
        // Bind against the handle's runtime type (InputHandle or ShardedInputHandle).
        var pushMethod = handle.GetType().GetMethod("Push", [zsetType])!;

        // (rows) => {
        //   var b = new ZSetBuilder<TRow, Z64>();
        //   foreach (var (row, w) in rows) b.Add((TRow)row, new Z64(w));
        //   handle.Push(b.Build());
        // }
        var rowsParam = Expression.Parameter(typeof(IEnumerable<(object Row, long Weight)>), "rows");
        var builderVar = Expression.Variable(builderType, "b");
        var pairVar = Expression.Variable(typeof((object Row, long Weight)), "p");

        var loop = ForEach(
            rowsParam, pairVar,
            Expression.Call(
                builderVar, addMethod,
                Expression.Convert(Expression.Field(pairVar, nameof(ValueTuple<object, long>.Item1)), rowType),
                Expression.New(z64Ctor, Expression.Field(pairVar, nameof(ValueTuple<object, long>.Item2)))));

        var body = Expression.Block(
            new[] { builderVar },
            Expression.Assign(builderVar, Expression.New(builderCtor)),
            loop,
            Expression.Call(
                Expression.Constant(handle), pushMethod,
                Expression.Call(builderVar, buildMethod)));

        return Expression.Lambda<Action<IEnumerable<(object Row, long Weight)>>>(body, rowsParam).Compile();
    }

    /// <summary>foreach (var item in source) body</summary>
    private static Expression ForEach(Expression source, ParameterExpression item, Expression body)
    {
        var enumerableType = source.Type;
        var enumeratorType = typeof(IEnumerator<>).MakeGenericType(item.Type);
        var getEnumerator = enumerableType.GetMethod(nameof(IEnumerable<int>.GetEnumerator))!;
        var moveNext = typeof(System.Collections.IEnumerator).GetMethod(nameof(System.Collections.IEnumerator.MoveNext))!;
        var currentProp = enumeratorType.GetProperty(nameof(IEnumerator<int>.Current))!;
        var dispose = typeof(IDisposable).GetMethod(nameof(IDisposable.Dispose))!;

        var enumerator = Expression.Variable(enumeratorType, "e");
        var breakLabel = Expression.Label("end");

        return Expression.Block(
            new[] { enumerator, item },
            Expression.Assign(enumerator, Expression.Call(source, getEnumerator)),
            Expression.TryFinally(
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.Call(enumerator, moveNext),
                        Expression.Block(
                            Expression.Assign(item, Expression.Property(enumerator, currentProp)),
                            body),
                        Expression.Break(breakLabel)),
                    breakLabel),
                Expression.Call(enumerator, dispose)));
    }
}
