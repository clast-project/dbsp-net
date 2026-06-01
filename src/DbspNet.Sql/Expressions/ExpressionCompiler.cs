// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using BinOp = DbspNet.Sql.Parser.Ast.BinaryOperator;
using UnOp = DbspNet.Sql.Parser.Ast.UnaryOperator;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Compiles a resolved scalar expression to a <see cref="Func{T, TResult}"/>
/// that takes a positional <c>object?[]</c> row and returns the expression's
/// value (with SQL NULL represented as <c>null</c>).
/// </summary>
/// <remarks>
/// <para>
/// <b>NULL semantics.</b> Arithmetic and comparison operators propagate NULL
/// (any NULL operand → NULL result). Boolean <c>AND</c>/<c>OR</c>/<c>NOT</c>
/// follow SQL three-valued logic via <see cref="ThreeValuedLogic"/>.
/// <c>IS [NOT] NULL</c> always returns a definite TRUE/FALSE.
/// </para>
/// <para>
/// <b>Row layout.</b> Each row is an <c>object?[]</c> whose entries are
/// either <c>null</c> (SQL NULL) or a boxed CLR value whose runtime type
/// matches the resolver-assigned <see cref="SqlType"/> for that column.
/// The compiler unboxes by the <i>resolved</i> type, so a mismatch between
/// a row's actual boxed CLR type and its declared SQL type is a caller bug.
/// </para>
/// </remarks>
public static class ExpressionCompiler
{
    private static readonly MethodInfo AndMethod =
        typeof(ThreeValuedLogic).GetMethod(nameof(ThreeValuedLogic.And))!;
    private static readonly MethodInfo OrMethod =
        typeof(ThreeValuedLogic).GetMethod(nameof(ThreeValuedLogic.Or))!;
    private static readonly MethodInfo NotMethod =
        typeof(ThreeValuedLogic).GetMethod(nameof(ThreeValuedLogic.Not))!;
    private static readonly MethodInfo StringCompareOrdinalMethod =
        typeof(string).GetMethod(nameof(string.CompareOrdinal), [typeof(string), typeof(string)])!;

    private static readonly ConstantExpression NullObject =
        Expression.Constant(null, typeof(object));

    private static readonly MethodInfo RowIndexer =
        typeof(IReadOnlyList<object?>).GetProperty("Item")!.GetGetMethod()!;

    public static Func<IReadOnlyList<object?>, object?> CompileScalar(ResolvedExpression expr)
    {
        ArgumentNullException.ThrowIfNull(expr);
        var row = Expression.Parameter(typeof(IReadOnlyList<object?>), "row");
        var body = Build(expr, row);
        var lambda = Expression.Lambda<Func<IReadOnlyList<object?>, object?>>(body, row);
        return lambda.Compile();
    }

    /// <summary>
    /// Compile a boolean expression for use at the edge of a WHERE / HAVING /
    /// JOIN ON pipeline: NULL coerces to FALSE, so the predicate is always
    /// a definite <c>bool</c>.
    /// </summary>
    public static Func<IReadOnlyList<object?>, bool> CompilePredicate(ResolvedExpression expr)
    {
        ArgumentNullException.ThrowIfNull(expr);
        if (expr.Type is not SqlBooleanType)
        {
            throw new InvalidOperationException($"predicate must be BOOLEAN, got {expr.Type.Display}");
        }

        var f = CompileScalar(expr);
        return row => f(row) is bool b && b;
    }

    // ---- Expression-tree construction ----

    private static Expression Build(ResolvedExpression expr, ParameterExpression row) => expr switch
    {
        ResolvedLiteral lit => BuildLiteral(lit),
        ResolvedColumn col => BuildColumn(col, row),
        ResolvedUnary un => BuildUnary(un, row),
        ResolvedBinary bin => BuildBinary(bin, row),
        ResolvedIsNull isn => BuildIsNull(isn, row),
        ResolvedCast cast => BuildCast(cast, row),
        ResolvedFunctionCall fn => BuildFunction(fn, row),
        ResolvedInList il => BuildInList(il, row),
        ResolvedCaseWhen ce => BuildCaseWhen(ce, row),
        ResolvedCorrelationRef => throw new InvalidOperationException(
            "internal: ResolvedCorrelationRef reached ExpressionCompiler — " +
            "decorrelator should have rewritten every correlation reference"),
        _ => throw new InvalidOperationException($"unsupported expression: {expr.GetType().Name}"),
    };

    private static readonly MethodInfo InListEvaluateMethod =
        typeof(InListRuntime).GetMethod(nameof(InListRuntime.Evaluate))!;

    private static Expression BuildInList(ResolvedInList il, ParameterExpression row)
    {
        // Each value is evaluated up front and packed into an object?[].
        // The runtime helper does the search iteratively, so the C# stack
        // and the Linq.Expression tree both stay shallow regardless of N.
        var probeExpr = Build(il.Probe, row);
        var valueExprs = new Expression[il.Values.Count];
        for (var i = 0; i < il.Values.Count; i++)
        {
            valueExprs[i] = Build(il.Values[i], row);
        }

        var valuesArr = Expression.NewArrayInit(typeof(object), valueExprs);
        return Expression.Call(
            InListEvaluateMethod,
            probeExpr,
            valuesArr,
            Expression.Constant(il.IsNegated));
    }

    private static readonly MethodInfo CaseConditionIsTrueMethod =
        typeof(CaseRuntime).GetMethod(nameof(CaseRuntime.ConditionIsTrue))!;

    private static Expression BuildCaseWhen(ResolvedCaseWhen ce, ParameterExpression row)
    {
        // Right-to-left fold into a nested conditional. Lazy by construction:
        // a non-taken arm's result subexpression sits in the false branch and
        // is never evaluated. An arm is taken iff its condition is a definite
        // TRUE (NULL / FALSE fall through — SQL three-valued semantics). Each
        // result has already been cast to the common type by the resolver, so
        // every branch of the conditional is typeof(object).
        Expression result = ce.ElseResult is null ? NullObject : Build(ce.ElseResult, row);
        for (var i = ce.Whens.Count - 1; i >= 0; i--)
        {
            var cond = Build(ce.Whens[i].Condition, row);
            var taken = Expression.Call(CaseConditionIsTrueMethod, cond);
            var branch = Build(ce.Whens[i].Result, row);
            result = Expression.Condition(taken, branch, result);
        }

        return result;
    }

    private static Expression BuildLiteral(ResolvedLiteral lit)
    {
        if (lit.Value is null)
        {
            return NullObject;
        }

        return Expression.Constant(lit.Value, typeof(object));
    }

    private static Expression BuildColumn(ResolvedColumn col, ParameterExpression row)
    {
        // row[col.Index] through the IReadOnlyList<object?> indexer.
        return Expression.Call(row, RowIndexer, Expression.Constant(col.Index));
    }

    private static Expression BuildIsNull(ResolvedIsNull isn, ParameterExpression row)
    {
        var inner = Build(isn.Operand, row);
        Expression test = Expression.Equal(inner, NullObject);
        if (isn.Negated)
        {
            test = Expression.Not(test);
        }

        return Expression.Convert(test, typeof(object));
    }

    private static Expression BuildUnary(ResolvedUnary un, ParameterExpression row)
    {
        var operand = Build(un.Operand, row);
        if (un.Operator == UnOp.Not)
        {
            var asBool = Expression.TypeAs(operand, typeof(bool?));
            var result = Expression.Call(NotMethod, asBool);
            return Expression.Convert(result, typeof(object));
        }

        // Unary negate on numeric.
        var clr = ClrOf(un.Type);
        var isNull = Expression.Equal(operand, NullObject);
        var unboxed = Expression.Convert(operand, clr);
        var neg = Expression.Negate(unboxed);
        return Expression.Condition(isNull, NullObject, Expression.Convert(neg, typeof(object)));
    }

    private static Expression BuildBinary(ResolvedBinary bin, ParameterExpression row)
    {
        var l = Build(bin.Left, row);
        var r = Build(bin.Right, row);
        switch (bin.Operator)
        {
            case BinOp.And:
                return Call3VL(AndMethod, l, r);
            case BinOp.Or:
                return Call3VL(OrMethod, l, r);
            case BinOp.Add:
            case BinOp.Subtract:
            case BinOp.Multiply:
            case BinOp.Divide:
            case BinOp.Modulo:
                if (IsTemporalOrInterval(bin.Left.Type) || IsTemporalOrInterval(bin.Right.Type))
                {
                    return BuildTemporalArith(bin, l, r);
                }

                return NullPropagatingNumericOp(
                    bin.Operator, l, r, bin.Left.Type, bin.Right.Type, bin.Type);
            case BinOp.Equal:
            case BinOp.NotEqual:
            case BinOp.Less:
            case BinOp.LessEqual:
            case BinOp.Greater:
            case BinOp.GreaterEqual:
                return NullPropagatingCompareOp(
                    bin.Operator, l, r, bin.Left.Type, bin.Right.Type);
            default:
                throw new InvalidOperationException($"unhandled binary operator {bin.Operator}");
        }
    }

    private static Expression Call3VL(MethodInfo method, Expression l, Expression r)
    {
        var lQ = Expression.TypeAs(l, typeof(bool?));
        var rQ = Expression.TypeAs(r, typeof(bool?));
        var call = Expression.Call(method, lQ, rQ);
        return Expression.Convert(call, typeof(object));
    }

    private static Expression NullPropagatingNumericOp(
        BinOp op, Expression l, Expression r,
        SqlType leftType, SqlType rightType, SqlType resultType)
    {
        var isNull = Expression.OrElse(
            Expression.Equal(l, NullObject),
            Expression.Equal(r, NullObject));

        Expression compute;
        if (resultType is SqlDecimalType decResult)
        {
            // Decimal arithmetic dispatches to a kernel with operand and
            // result DecimalType baked in as constants. The kernel handles
            // cross-scale rescaling internally via ScaleHelper.
            compute = EmitDecimalBinaryOp(op, l, leftType, r, rightType, decResult);
        }
        else
        {
            var clr = ClrOf(resultType);
            var lv = Expression.Convert(l, clr);
            var rv = Expression.Convert(r, clr);
            compute = op switch
            {
                BinOp.Add => Expression.Add(lv, rv),
                BinOp.Subtract => Expression.Subtract(lv, rv),
                BinOp.Multiply => Expression.Multiply(lv, rv),
                BinOp.Divide => Expression.Divide(lv, rv),
                BinOp.Modulo => Expression.Modulo(lv, rv),
                _ => throw new InvalidOperationException(),
            };
        }

        return Expression.Condition(
            isNull,
            NullObject,
            Expression.Convert(compute, typeof(object)));
    }

    /// <summary>
    /// Build a NULL-propagating date/time/interval arithmetic op. Operands are
    /// boxed <c>object</c>; the computed value is a temporal/interval struct
    /// re-boxed to <c>object</c>. Dispatch on the resolved operand types
    /// (which the resolver has already validated).
    /// </summary>
    private static Expression BuildTemporalArith(ResolvedBinary bin, Expression l, Expression r)
    {
        var isNull = Expression.OrElse(
            Expression.Equal(l, NullObject),
            Expression.Equal(r, NullObject));
        var compute = EmitTemporalCompute(bin.Operator, l, bin.Left.Type, r, bin.Right.Type);
        return Expression.Condition(isNull, NullObject, Expression.Convert(compute, typeof(object)));
    }

    private static Expression EmitTemporalCompute(
        BinOp op, Expression l, SqlType lt, Expression r, SqlType rt)
    {
        var lInterval = lt is SqlIntervalType;
        var rInterval = rt is SqlIntervalType;
        var lTemporal = lt is SqlDateType or SqlTimeType or SqlTimestampType;
        var rTemporal = rt is SqlDateType or SqlTimeType or SqlTimestampType;

        Expression Temporal(Expression e, SqlType t) => Expression.Convert(e, ClrOf(t));
        Expression Iv(Expression e) => Expression.Convert(e, typeof(Interval));
        Expression ToDouble(Expression e, SqlType t) =>
            Expression.Convert(Expression.Convert(e, ClrOf(t)), typeof(double));

        switch (op)
        {
            case BinOp.Add:
                if (lTemporal && rInterval) return AddTo(lt, Temporal(l, lt), Iv(r), sub: false);
                if (lInterval && rTemporal) return AddTo(rt, Temporal(r, rt), Iv(l), sub: false);
                if (lInterval && rInterval) return CallTemporal(nameof(TemporalArithmetic.AddIntervals), Iv(l), Iv(r));
                break;
            case BinOp.Subtract:
                if (lTemporal && rInterval) return AddTo(lt, Temporal(l, lt), Iv(r), sub: true);
                if (lTemporal && rTemporal) return Diff(lt, Temporal(l, lt), Temporal(r, rt));
                if (lInterval && rInterval) return CallTemporal(nameof(TemporalArithmetic.SubIntervals), Iv(l), Iv(r));
                break;
            case BinOp.Multiply:
                if (lInterval) return CallTemporal(nameof(TemporalArithmetic.MulInterval), Iv(l), ToDouble(r, rt));
                if (rInterval) return CallTemporal(nameof(TemporalArithmetic.MulInterval), Iv(r), ToDouble(l, lt));
                break;
            case BinOp.Divide:
                if (lInterval) return CallTemporal(nameof(TemporalArithmetic.DivInterval), Iv(l), ToDouble(r, rt));
                break;
        }

        throw new InvalidOperationException(
            $"unsupported temporal arithmetic {lt.Display} {op} {rt.Display}");
    }

    private static Expression AddTo(SqlType temporal, Expression t, Expression iv, bool sub)
    {
        var name = temporal switch
        {
            SqlDateType => sub ? nameof(TemporalArithmetic.SubFromDate) : nameof(TemporalArithmetic.AddToDate),
            SqlTimeType => sub ? nameof(TemporalArithmetic.SubFromTime) : nameof(TemporalArithmetic.AddToTime),
            SqlTimestampType => sub ? nameof(TemporalArithmetic.SubFromTimestamp) : nameof(TemporalArithmetic.AddToTimestamp),
            _ => throw new InvalidOperationException(),
        };
        return CallTemporal(name, t, iv);
    }

    private static Expression Diff(SqlType temporal, Expression a, Expression b)
    {
        var name = temporal switch
        {
            SqlDateType => nameof(TemporalArithmetic.DiffDates),
            SqlTimeType => nameof(TemporalArithmetic.DiffTimes),
            SqlTimestampType => nameof(TemporalArithmetic.DiffTimestamps),
            _ => throw new InvalidOperationException(),
        };
        return CallTemporal(name, a, b);
    }

    private static Expression CallTemporal(string method, params Expression[] args) =>
        Expression.Call(typeof(TemporalArithmetic).GetMethod(method)!, args);

    /// <summary>
    /// Convert a boxed numeric operand expression to a typed
    /// <see cref="Clast.DatabaseDecimal.Values.Decimal128"/> expression, returning
    /// the operand's <see cref="Clast.DatabaseDecimal.DecimalType"/> for the
    /// kernel. Integer source types are promoted to scale-0 decimals (matches
    /// the resolver's <c>CombineDecimal</c> rule of treating INT as
    /// <c>DECIMAL(38, 0)</c>).
    /// </summary>
    private static (Expression Value, Clast.DatabaseDecimal.DecimalType Type) ToDecimalOperand(
        Expression boxedValue, SqlType srcType)
    {
        if (srcType is SqlDecimalType d)
        {
            var unboxed = Expression.Convert(boxedValue, typeof(Clast.DatabaseDecimal.Values.Decimal128));
            return (unboxed, Clast.DatabaseDecimal.DecimalType.Numeric(d.Precision, d.Scale));
        }

        if (srcType is SqlIntegerType)
        {
            var unboxed = Expression.Convert(boxedValue, typeof(int));
            var promote = Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.FromInt32))!,
                unboxed);
            return (promote, Clast.DatabaseDecimal.DecimalType.Numeric(38, 0));
        }

        if (srcType is SqlBigintType)
        {
            var unboxed = Expression.Convert(boxedValue, typeof(long));
            var promote = Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.FromInt64))!,
                unboxed);
            return (promote, Clast.DatabaseDecimal.DecimalType.Numeric(38, 0));
        }

        throw new InvalidOperationException(
            $"cannot promote {srcType.Display} to Decimal128 in arithmetic context");
    }

    private static Expression EmitDecimalBinaryOp(
        BinOp op, Expression l, SqlType leftType, Expression r, SqlType rightType,
        SqlDecimalType resultDec)
    {
        var (lExpr, lType) = ToDecimalOperand(l, leftType);
        var (rExpr, rType) = ToDecimalOperand(r, rightType);
        var resultType = Clast.DatabaseDecimal.DecimalType.Numeric(resultDec.Precision, resultDec.Scale);

        var methodName = op switch
        {
            BinOp.Add => nameof(DecimalRuntime.Add),
            BinOp.Subtract => nameof(DecimalRuntime.Subtract),
            BinOp.Multiply => nameof(DecimalRuntime.Multiply),
            BinOp.Divide => nameof(DecimalRuntime.Divide),
            BinOp.Modulo => nameof(DecimalRuntime.Modulus),
            _ => throw new InvalidOperationException(),
        };
        var method = typeof(DecimalRuntime).GetMethod(methodName)!;

        return Expression.Call(
            method,
            lExpr,
            Expression.Constant(lType),
            rExpr,
            Expression.Constant(rType),
            Expression.Constant(resultType));
    }

    private static Expression NullPropagatingCompareOp(
        BinOp op, Expression l, Expression r, SqlType leftType, SqlType rightType)
    {
        var isNull = Expression.OrElse(
            Expression.Equal(l, NullObject),
            Expression.Equal(r, NullObject));

        Expression compute;
        if (leftType is SqlDecimalType || rightType is SqlDecimalType)
        {
            // Decimal comparison: rescale operands to common scale before
            // comparing Int128 mantissas. Mixed-type comparisons (e.g.
            // INT vs DECIMAL) get the integer side promoted to scale 0.
            var (lExpr, lType) = ToDecimalOperand(l, leftType);
            var (rExpr, rType) = ToDecimalOperand(r, rightType);
            var cmp = Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Compare))!,
                lExpr,
                Expression.Constant(lType),
                rExpr,
                Expression.Constant(rType));
            var zero = Expression.Constant(0);
            compute = op switch
            {
                BinOp.Equal => Expression.Equal(cmp, zero),
                BinOp.NotEqual => Expression.NotEqual(cmp, zero),
                BinOp.Less => Expression.LessThan(cmp, zero),
                BinOp.LessEqual => Expression.LessThanOrEqual(cmp, zero),
                BinOp.Greater => Expression.GreaterThan(cmp, zero),
                BinOp.GreaterEqual => Expression.GreaterThanOrEqual(cmp, zero),
                _ => throw new InvalidOperationException(),
            };
        }
        else
        {
            // Operand types match for non-decimal comparisons (resolver
            // promotes both sides to a common type, but for primitives the
            // common type is the operand type, so leftType / rightType
            // are interchangeable here).
            var clr = ClrOf(leftType);
            var lv = Expression.Convert(l, clr);
            var rv = Expression.Convert(r, clr);
            if (clr == typeof(string))
            {
                var cmp = Expression.Call(StringCompareOrdinalMethod, lv, rv);
                var zero = Expression.Constant(0);
                compute = op switch
                {
                    BinOp.Equal => Expression.Equal(cmp, zero),
                    BinOp.NotEqual => Expression.NotEqual(cmp, zero),
                    BinOp.Less => Expression.LessThan(cmp, zero),
                    BinOp.LessEqual => Expression.LessThanOrEqual(cmp, zero),
                    BinOp.Greater => Expression.GreaterThan(cmp, zero),
                    BinOp.GreaterEqual => Expression.GreaterThanOrEqual(cmp, zero),
                    _ => throw new InvalidOperationException(),
                };
            }
            else
            {
                compute = op switch
                {
                    BinOp.Equal => Expression.Equal(lv, rv),
                    BinOp.NotEqual => Expression.NotEqual(lv, rv),
                    BinOp.Less => Expression.LessThan(lv, rv),
                    BinOp.LessEqual => Expression.LessThanOrEqual(lv, rv),
                    BinOp.Greater => Expression.GreaterThan(lv, rv),
                    BinOp.GreaterEqual => Expression.GreaterThanOrEqual(lv, rv),
                    _ => throw new InvalidOperationException(),
                };
            }
        }

        return Expression.Condition(
            isNull,
            NullObject,
            Expression.Convert(compute, typeof(object)));
    }

    private static Expression BuildCast(ResolvedCast cast, ParameterExpression row)
    {
        var operand = Build(cast.Operand, row);
        var srcClr = ClrOf(cast.Operand.Type);
        var dstClr = ClrOf(cast.Type);

        // Decimal carries scale in the SqlType, not the ClrType — same
        // ClrType alone is not enough to short-circuit. Branch into the
        // decimal-aware paths first, then fall through to primitive logic.
        if (cast.Type is SqlDecimalType targetDec)
        {
            return BuildCastToDecimal(operand, cast.Operand.Type, targetDec);
        }

        if (cast.Operand.Type is SqlDecimalType sourceDec)
        {
            return BuildCastFromDecimal(operand, sourceDec, cast.Type, dstClr);
        }

        if (srcClr == dstClr)
        {
            return operand;
        }

        var isNull = Expression.Equal(operand, NullObject);

        Expression converted;
        if (IsNumeric(srcClr) && IsNumeric(dstClr))
        {
            var unboxed = Expression.Convert(operand, srcClr);
            // Explicit narrowing between numeric CLR types; exceptions surface as runtime errors.
            var widened = Expression.Convert(unboxed, dstClr);
            converted = Expression.Convert(widened, typeof(object));
        }
        else if (srcClr == typeof(bool) && dstClr == typeof(Utf8String))
        {
            var unboxed = Expression.Convert(operand, typeof(bool));
            var toStr = Expression.Call(unboxed,
                typeof(bool).GetMethod(nameof(object.ToString), Type.EmptyTypes)!);
            var toUtf8 = Expression.Call(
                typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
                toStr);
            converted = Expression.Convert(toUtf8, typeof(object));
        }
        else if (IsNumeric(srcClr) && dstClr == typeof(Utf8String))
        {
            var unboxed = Expression.Convert(operand, srcClr);
            // Use invariant culture so CAST(1.5 AS VARCHAR) is locale-stable.
            var method = typeof(SqlCasts).GetMethod(nameof(SqlCasts.NumericToString), [srcClr])
                ?? throw new InvalidOperationException($"no string cast for {srcClr}");
            var toUtf8 = Expression.Call(
                typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
                Expression.Call(method, unboxed));
            converted = Expression.Convert(toUtf8, typeof(object));
        }
        else if (srcClr == typeof(Utf8String) && IsNumeric(dstClr))
        {
            var unboxed = Expression.Convert(operand, typeof(Utf8String));
            var asString = Expression.Call(unboxed,
                typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
            var method = dstClr switch
            {
                var t when t == typeof(int) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseInt32))!,
                var t when t == typeof(long) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseInt64))!,
                var t when t == typeof(float) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseSingle))!,
                var t when t == typeof(double) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseDouble))!,
                _ => throw new InvalidOperationException($"no string-to-{dstClr} parser"),
            };
            converted = Expression.Convert(Expression.Call(method, asString), typeof(object));
        }
        else if (srcClr == typeof(Utf8String) && IsTemporal(dstClr))
        {
            var unboxed = Expression.Convert(operand, typeof(Utf8String));
            var asString = Expression.Call(unboxed,
                typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
            var method = dstClr switch
            {
                var t when t == typeof(Date32) => typeof(Date32).GetMethod(nameof(Date32.Parse), [typeof(string)])!,
                var t when t == typeof(Time64) => typeof(Time64).GetMethod(nameof(Time64.Parse), [typeof(string)])!,
                var t when t == typeof(Timestamp) => typeof(Timestamp).GetMethod(nameof(Timestamp.Parse), [typeof(string)])!,
                _ => throw new InvalidOperationException($"no string-to-{dstClr} parser"),
            };
            converted = Expression.Convert(Expression.Call(method, asString), typeof(object));
        }
        else if (IsTemporal(srcClr) && dstClr == typeof(Utf8String))
        {
            var unboxed = Expression.Convert(operand, srcClr);
            var toStr = Expression.Call(unboxed,
                srcClr.GetMethod(nameof(object.ToString), Type.EmptyTypes)!);
            var toUtf8 = Expression.Call(
                typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
                toStr);
            converted = Expression.Convert(toUtf8, typeof(object));
        }
        else if (srcClr == typeof(Utf8String) && cast.Type is SqlIntervalType targetInterval)
        {
            var unboxed = Expression.Convert(operand, typeof(Utf8String));
            var asString = Expression.Call(unboxed,
                typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
            var parse = Expression.Call(
                typeof(Interval).GetMethod(nameof(Interval.Parse), [typeof(string), typeof(IntervalQualifier)])!,
                asString,
                Expression.Constant(targetInterval.Qualifier));
            converted = Expression.Convert(parse, typeof(object));
        }
        else if (srcClr == typeof(Interval) && dstClr == typeof(Utf8String))
        {
            var unboxed = Expression.Convert(operand, typeof(Interval));
            var toStr = Expression.Call(unboxed,
                typeof(Interval).GetMethod(nameof(object.ToString), Type.EmptyTypes)!);
            var toUtf8 = Expression.Call(
                typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
                toStr);
            converted = Expression.Convert(toUtf8, typeof(object));
        }
        else
        {
            throw new InvalidOperationException(
                $"unsupported CAST from {cast.Operand.Type.Display} to {cast.Type.Display}");
        }

        return Expression.Condition(isNull, NullObject, converted);
    }

    private static Expression BuildFunction(ResolvedFunctionCall fn, ParameterExpression row)
    {
        return ScalarFunctionRegistry.BuildStructural(fn, arg => Build(arg, row));
    }

    /// <summary>
    /// Cast any source type to <see cref="SqlDecimalType"/>. Branches on
    /// the source kind: decimal-to-decimal rescales; integer-to-decimal
    /// promotes the integer with scale-up; string-to-decimal parses via
    /// <c>DecimalText</c> using the target's declared precision/scale.
    /// </summary>
    private static Expression BuildCastToDecimal(
        Expression operand, SqlType srcType, SqlDecimalType target)
    {
        var isNull = Expression.Equal(operand, NullObject);
        var targetType = Clast.DatabaseDecimal.DecimalType.Numeric(target.Precision, target.Scale);

        Expression converted;
        if (srcType is SqlDecimalType srcDec)
        {
            // Cross-scale rescale via ScaleHelper.Rescale128.
            if (srcDec.Scale == target.Scale)
            {
                return operand;
            }

            var unboxed = Expression.Convert(operand, typeof(Clast.DatabaseDecimal.Values.Decimal128));
            var mantissa = Expression.Field(unboxed, nameof(Clast.DatabaseDecimal.Values.Decimal128.Mantissa));
            var rescale = Expression.Call(
                typeof(Clast.DatabaseDecimal.Arithmetic.ScaleHelper).GetMethod(
                    nameof(Clast.DatabaseDecimal.Arithmetic.ScaleHelper.Rescale128))!,
                mantissa,
                Expression.Constant(srcDec.Scale),
                Expression.Constant(target.Scale));
            var ctor = typeof(Clast.DatabaseDecimal.Values.Decimal128).GetConstructor([typeof(Int128)])!;
            converted = Expression.Convert(Expression.New(ctor, rescale), typeof(object));
        }
        else if (srcType is SqlIntegerType or SqlBigintType)
        {
            // FromInteger(value, scale): wraps the integer × 10^scale as
            // a Decimal128 mantissa. Handles INT/BIGINT to any scale.
            var srcClr = ClrOf(srcType);
            var unboxed = Expression.Convert(operand, srcClr);
            var asInt128 = Expression.Convert(unboxed, typeof(Int128));
            var fromInt = Expression.Call(
                typeof(Clast.DatabaseDecimal.Values.Decimal128).GetMethod(
                    nameof(Clast.DatabaseDecimal.Values.Decimal128.FromInteger))!,
                asInt128,
                Expression.Constant((byte)target.Scale));
            converted = Expression.Convert(fromInt, typeof(object));
        }
        else if (srcType is SqlVarcharType)
        {
            // Parse via DecimalText, applying the target type's scale and
            // precision (scale-up / banker's rounding handled inside).
            var unboxed = Expression.Convert(operand, typeof(Utf8String));
            var asString = Expression.Call(
                unboxed,
                typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
            var parse = Expression.Call(
                typeof(Clast.DatabaseDecimal.Text.DecimalText).GetMethod(
                    nameof(Clast.DatabaseDecimal.Text.DecimalText.ParseDecimal128),
                    [typeof(ReadOnlySpan<char>), typeof(Clast.DatabaseDecimal.DecimalType)])!,
                Expression.Call(
                    typeof(MemoryExtensions).GetMethod(nameof(MemoryExtensions.AsSpan), [typeof(string)])!,
                    asString),
                Expression.Constant(targetType));
            converted = Expression.Convert(parse, typeof(object));
        }
        else
        {
            throw new InvalidOperationException(
                $"unsupported CAST from {srcType.Display} to {target.Display}");
        }

        return Expression.Condition(isNull, NullObject, converted);
    }

    /// <summary>
    /// Cast a <see cref="SqlDecimalType"/> source to a non-decimal target.
    /// Decimal → string formats via <c>DecimalText</c>; decimal → integer /
    /// float / double rescales the mantissa to scale 0 then casts.
    /// </summary>
    private static Expression BuildCastFromDecimal(
        Expression operand, SqlDecimalType src, SqlType dstType, Type dstClr)
    {
        var isNull = Expression.Equal(operand, NullObject);
        var sourceType = Clast.DatabaseDecimal.DecimalType.Numeric(src.Precision, src.Scale);
        var unboxed = Expression.Convert(operand, typeof(Clast.DatabaseDecimal.Values.Decimal128));

        Expression converted;
        if (dstType is SqlVarcharType)
        {
            var format = Expression.Call(
                typeof(Clast.DatabaseDecimal.Text.DecimalText).GetMethod(
                    nameof(Clast.DatabaseDecimal.Text.DecimalText.Format),
                    [typeof(Clast.DatabaseDecimal.Values.Decimal128), typeof(Clast.DatabaseDecimal.DecimalType)])!,
                unboxed,
                Expression.Constant(sourceType));
            var toUtf8 = Expression.Call(
                typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
                format);
            converted = Expression.Convert(toUtf8, typeof(object));
        }
        else if (dstClr == typeof(int) || dstClr == typeof(long)
            || dstClr == typeof(float) || dstClr == typeof(double))
        {
            // Truncate to integer mantissa (rescale to scale 0), then cast.
            var mantissa = Expression.Field(unboxed, nameof(Clast.DatabaseDecimal.Values.Decimal128.Mantissa));
            var rescaled = Expression.Call(
                typeof(Clast.DatabaseDecimal.Arithmetic.ScaleHelper).GetMethod(
                    nameof(Clast.DatabaseDecimal.Arithmetic.ScaleHelper.Rescale128))!,
                mantissa,
                Expression.Constant(src.Scale),
                Expression.Constant(0));
            var cast = Expression.Convert(rescaled, dstClr);
            converted = Expression.Convert(cast, typeof(object));
        }
        else
        {
            throw new InvalidOperationException(
                $"unsupported CAST from {src.Display} to {dstType.Display}");
        }

        return Expression.Condition(isNull, NullObject, converted);
    }

    // ---- Type helpers ----

    private static Type ClrOf(SqlType t) => t switch
    {
        SqlIntegerType => typeof(int),
        SqlBigintType => typeof(long),
        SqlRealType => typeof(float),
        SqlDoubleType => typeof(double),
        SqlDecimalType => typeof(Clast.DatabaseDecimal.Values.Decimal128),
        SqlVarcharType => typeof(Utf8String),
        SqlBooleanType => typeof(bool),
        SqlDateType => typeof(Date32),
        SqlTimeType => typeof(Time64),
        SqlTimestampType => typeof(Timestamp),
        SqlIntervalType => typeof(Interval),
        _ => throw new InvalidOperationException($"no CLR mapping for {t.Display}"),
    };

    private static bool IsNumeric(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(float)
        || t == typeof(double) || t == typeof(decimal);

    private static bool IsTemporal(Type t) =>
        t == typeof(Date32) || t == typeof(Time64) || t == typeof(Timestamp);

    private static bool IsTemporalOrInterval(SqlType t) =>
        t is SqlDateType or SqlTimeType or SqlTimestampType or SqlIntervalType;
}

/// <summary>Thin helpers invoked from compiled Expression trees for CAST operations.</summary>
internal static class SqlCasts
{
    public static int ParseInt32(string s) => int.Parse(s, CultureInfo.InvariantCulture);

    public static long ParseInt64(string s) => long.Parse(s, CultureInfo.InvariantCulture);

    public static float ParseSingle(string s) => float.Parse(s, CultureInfo.InvariantCulture);

    public static double ParseDouble(string s) => double.Parse(s, CultureInfo.InvariantCulture);

    public static decimal ParseDecimal(string s) => decimal.Parse(s, CultureInfo.InvariantCulture);

    public static string NumericToString(int v) => v.ToString(CultureInfo.InvariantCulture);

    public static string NumericToString(long v) => v.ToString(CultureInfo.InvariantCulture);

    public static string NumericToString(float v) => v.ToString(CultureInfo.InvariantCulture);

    public static string NumericToString(double v) => v.ToString(CultureInfo.InvariantCulture);

    public static string NumericToString(decimal v) => v.ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Runtime helper for <c>probe [NOT] IN (v1, ..., vN)</c>. Compiles to a
/// single call from the expression tree — the iteration is here, not in the
/// generated Linq.Expression, so the tree depth stays constant in the list
/// size.
/// </summary>
/// <remarks>
/// SQL three-valued NULL semantics:
/// <list type="bullet">
///   <item>Probe is NULL → result is NULL.</item>
///   <item>Match found on a non-NULL value → TRUE (or FALSE if negated).</item>
///   <item>No match and at least one value is NULL → NULL.</item>
///   <item>No match and no NULL values → FALSE (or TRUE if negated).</item>
/// </list>
/// The resolver has cast probe and every value to the same comparable CLR
/// type, so <see cref="object.Equals(object)"/> performs value equality.
/// </remarks>
internal static class CaseRuntime
{
    /// <summary>
    /// A CASE WHEN arm is taken iff its condition evaluates to a definite
    /// TRUE. A NULL (SQL UNKNOWN) or FALSE condition falls through to the
    /// next arm / ELSE — matching SQL three-valued semantics.
    /// </summary>
    public static bool ConditionIsTrue(object? condition) => condition is bool b && b;
}

internal static class InListRuntime
{
    public static object? Evaluate(object? probe, object?[] values, bool isNegated)
    {
        if (probe is null)
        {
            return null;
        }

        var hasNull = false;
        for (var i = 0; i < values.Length; i++)
        {
            var v = values[i];
            if (v is null)
            {
                hasNull = true;
                continue;
            }

            if (probe.Equals(v))
            {
                return !isNegated;
            }
        }

        return hasNull ? null : (object?)isNegated;
    }
}
