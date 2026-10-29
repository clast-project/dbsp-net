// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Reflection;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Circuit;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Trace-free linear GROUP BY (docs/design-state-representation.md §4G): when every
/// aggregate is COUNT(*), COUNT(x), SUM, AVG or COUNT(DISTINCT x) (FILTERed forms
/// included), the typed compiler emits <see cref="LinearAggregateOp{TKey,TValue,TOut}"/>
/// over a <see cref="TypedLinearCompositeAggregator{TIn,TAgg}"/> instead of the
/// trace-backed <c>IncrementalAggregateOp</c>.
/// </summary>
public static partial class TypedPlanCompiler
{
    /// <summary>
    /// Builds the linear composite for <paramref name="aggregates"/> with the same
    /// <c>TAgg</c> row type as <see cref="BuildTypedComposite"/>, or <c>null</c> when
    /// some call is not linear (MIN/MAX, sketches, …) or can't be typed — the caller
    /// then keeps the trace-backed operator.
    /// </summary>
    private static object? TryBuildLinearComposite(
        IReadOnlyList<AggregateCall> aggregates, Type inputRowType, Type aggRowType)
    {
        foreach (var call in aggregates)
        {
            if (call.Kind is not (AggregateKind.CountStar or AggregateKind.Count or AggregateKind.Sum
                or AggregateKind.Avg or AggregateKind.CountDistinct))
            {
                return null;
            }
        }

        var units = new List<object>(aggregates.Count);
        var unitSlots = new List<int[]>(aggregates.Count);
        if (!BuildLinearCountDistinct(aggregates, inputRowType, units, unitSlots))
        {
            return null;
        }

        for (var i = 0; i < aggregates.Count; i++)
        {
            if (aggregates[i].Kind == AggregateKind.CountDistinct) continue;
            var unit = BuildLinearUnit(aggregates[i], inputRowType);
            if (unit is null) return null;
            units.Add(unit);
            unitSlots.Add([i]);
        }

        var unitType = typeof(TypedLinearAggregator<>).MakeGenericType(inputRowType);
        var unitArray = Array.CreateInstance(unitType, units.Count);
        for (var i = 0; i < units.Count; i++) unitArray.SetValue(units[i], i);

        var aggColumns = new SchemaColumn[aggregates.Count];
        for (var i = 0; i < aggregates.Count; i++)
        {
            aggColumns[i] = new SchemaColumn("$agg" + i, TypedAggregateResultType(aggregates[i]));
        }

        var packResults = TypedRowEmitter.BuildBoxedFactory(new Schema(aggColumns))!;
        var packParam = Expression.Parameter(typeof(object?[]), "vs");
        var packCall = Expression.Convert(
            Expression.Invoke(Expression.Constant(packResults), packParam), aggRowType);
        var typedPack = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(typeof(object?[]), aggRowType), packCall, packParam).Compile();

        var closed = typeof(TypedLinearCompositeAggregator<,>).MakeGenericType(inputRowType, aggRowType);
        return Activator.CreateInstance(closed, unitArray, unitSlots.ToArray(), aggregates.Count, typedPack);
    }

    private static object? BuildLinearUnit(AggregateCall call, Type inputRowType)
    {
        if (call.Kind == AggregateKind.CountStar)
        {
            return Activator.CreateInstance(
                typeof(LinearCountAggregator<>).MakeGenericType(inputRowType), new object?[] { null });
        }

        if (call.Argument is null) return null;
        var inParam = Expression.Parameter(inputRowType, "in");
        var built = TypedExpressionCompiler.TryBuildInto(call.Argument, inParam);
        if (built is null) return null;
        var underlying = TypedExpressionCompiler.IsNullable(built.Type)
            ? TypedExpressionCompiler.UnderlyingType(built.Type)
            : built.Type;

        switch (call.Kind)
        {
            case AggregateKind.Count:
                {
                    var presence = call.Argument.Type.Nullable
                        ? BuildHasValueDelegate(call.Argument, inputRowType)
                        : null;
                    if (call.Argument.Type.Nullable && presence is null) return null;
                    return Activator.CreateInstance(
                        typeof(LinearCountAggregator<>).MakeGenericType(inputRowType), presence);
                }

            // SUM over a NULLable argument stays on the trace-backed operator: its NULL
            // rule is "no non-NULL row has non-zero weight", a per-row fact that a
            // weight sum cannot reproduce once the integrated input carries negative
            // weights (rows {+1 x=4, -1 x=2} sum to weight 0 yet the result is 2).
            // AVG's NULL rule is already the summed non-NULL weight, so it is linear.
            case AggregateKind.Sum when call.Argument.Type.Nullable:
                return null;

            case AggregateKind.Sum when call.ResultType is SqlBigintType:
                return Linear(typeof(LinearSumLongAggregator<>), typeof(long?));

            case AggregateKind.Sum when call.ResultType is SqlDoubleType:
                return Linear(typeof(LinearDoubleAggregator<>), typeof(double?), false);

            case AggregateKind.Sum when call.ResultType is SqlDecimalType && underlying == typeof(Decimal128):
                return Linear(typeof(LinearDecimalAggregator<>), typeof(Decimal128?), false);

            case AggregateKind.Avg when call.ResultType is SqlDecimalType:
                return underlying == typeof(Decimal128)
                    ? Linear(typeof(LinearDecimalAggregator<>), typeof(Decimal128?), true)
                    : null;

            case AggregateKind.Avg:
                return Linear(typeof(LinearDoubleAggregator<>), typeof(double?), true);

            default:
                return null;
        }

        // Func<TIn, target?> over the argument, widened exactly as the trace-backed
        // builders widen it (int → long, numeric → double), lifted to Nullable.
        object? Linear(Type open, Type target, params object[] extra)
        {
            Expression body;
            try
            {
                body = built.Type == target ? built : Expression.Convert(built, target);
            }
            catch (InvalidOperationException)
            {
                return null;
            }

            var extract = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(inputRowType, target), body, inParam).Compile();
            return Activator.CreateInstance(open.MakeGenericType(inputRowType), [extract, .. extra]);
        }
    }

    /// <summary>
    /// The linear counterpart of <see cref="BuildSharedCountDistinct"/>: same grouping
    /// of COUNT(DISTINCT) calls by base column (FILTERs as member predicates). Returns
    /// false if any COUNT(DISTINCT) call can't be served (reference-typed or
    /// un-serialisable value, un-lowerable predicate).
    /// </summary>
    private static bool BuildLinearCountDistinct(
        IReadOnlyList<AggregateCall> aggregates,
        Type inputRowType,
        List<object> units,
        List<int[]> unitSlots)
    {
        var groups = new List<(ResolvedExpression Base, List<(int Index, ResolvedExpression? Pred)> Members)>();
        for (var i = 0; i < aggregates.Count; i++)
        {
            var call = aggregates[i];
            if (call.Kind != AggregateKind.CountDistinct) continue;
            if (call.Argument is null) return false;
            var (baseExpr, pred) = SplitFilteredArgument(call.Argument);
            var group = pred is null && baseExpr is not ResolvedColumn
                ? -1
                : groups.FindIndex(g => g.Base is ResolvedColumn && g.Base.Equals(baseExpr) && g.Members.Count < 64);
            if (group < 0)
            {
                groups.Add((baseExpr, []));
                group = groups.Count - 1;
            }

            groups[group].Members.Add((i, pred));
        }

        var inParam = Expression.Parameter(inputRowType, "in");
        foreach (var (baseExpr, members) in groups)
        {
            var baseBuilt = TypedExpressionCompiler.TryBuildInto(baseExpr, inParam);
            if (baseBuilt is null) return false;
            var underlying = TypedExpressionCompiler.UnderlyingType(baseBuilt.Type);
            if (!underlying.IsValueType || !IsLinearValueSupported(underlying)) return false;
            var nullableType = typeof(Nullable<>).MakeGenericType(underlying);
            var baseNullable = baseBuilt.Type == nullableType ? baseBuilt : Expression.Convert(baseBuilt, nullableType);
            var baseExtract = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(inputRowType, nullableType), baseNullable, inParam).Compile();

            var predType = typeof(Func<,>).MakeGenericType(inputRowType, typeof(bool));
            var predArray = Array.CreateInstance(predType, members.Count);
            for (var j = 0; j < members.Count; j++)
            {
                if (members[j].Pred is not { } p) continue;
                var built = TypedExpressionCompiler.TryBuildInto(p, inParam);
                if (built is null) return false;
                Expression? definite = built.Type == typeof(bool)
                    ? built
                    : built.Type == typeof(bool?)
                        ? Expression.Equal(built, Expression.Constant(true, typeof(bool?)))
                        : null;
                if (definite is null) return false;
                predArray.SetValue(Expression.Lambda(predType, definite, inParam).Compile(), j);
            }

            var closed = typeof(LinearCountDistinctAggregator<,>).MakeGenericType(inputRowType, underlying);
            units.Add(Activator.CreateInstance(closed, baseExtract, predArray)!);
            var slots = new int[members.Count];
            for (var j = 0; j < members.Count; j++) slots[j] = members[j].Index;
            unitSlots.Add(slots);
        }

        return true;
    }

    private static bool IsLinearValueSupported(Type valueType)
        => (bool)typeof(LinearValueCodec<>).MakeGenericType(valueType)
            .GetField(nameof(LinearValueCodec<int>.Supported), BindingFlags.Public | BindingFlags.Static)!
            .GetValue(null)!;

    /// <summary>True when the (typed adapter) codec can hold the linear op's per-group blobs.</summary>
    private static bool SupportsKeyedBlobs(object? codec)
        => codec is null
            || codec.GetType().GetProperty("SupportsKeyedBlobs")?.GetValue(codec) is true;

    /// <summary><c>builder.LinearAggregate&lt;TKey, TValue, TOut&gt;(indexed, aggregator, codec, null, null)</c>.</summary>
    private static object InvokeLinearAggregate(
        CircuitBuilder builder, Type keyRowType, Type valueRowType, Type aggRowType,
        object indexed, object aggregator, object? snapshotCodec)
    {
        var openMethod = typeof(StatefulOperators)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(StatefulOperators.LinearAggregate) && m.IsGenericMethodDefinition);
        var closed = openMethod.MakeGenericMethod(keyRowType, valueRowType, aggRowType);
        return closed.Invoke(null, new object?[] { builder, indexed, aggregator, snapshotCodec, null, null })!;
    }
}
