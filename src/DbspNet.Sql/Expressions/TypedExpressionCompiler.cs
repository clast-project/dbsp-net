// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Reflection;
using Clast.DatabaseDecimal;
using Clast.DatabaseDecimal.Arithmetic;
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using BinOp = DbspNet.Sql.Parser.Ast.BinaryOperator;
using UnOp = DbspNet.Sql.Parser.Ast.UnaryOperator;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Typed-row counterpart to <see cref="ExpressionCompiler"/>. Compiles a
/// resolved scalar expression to <c>Func&lt;TRow, TResult&gt;</c>, where
/// <c>TRow</c> is the per-schema emitted struct from
/// <see cref="DbspNet.Sql.Compiler.TypedRowEmitter"/> and <c>TResult</c>
/// is the SqlType's CLR representation. Columns are direct field reads
/// (<c>row.F{i}</c>) — no boxing through <see cref="object"/> or
/// <c>IReadOnlyList&lt;object?&gt;</c>.
/// </summary>
/// <remarks>
/// <para><b>Nullability representation.</b> The result <c>.Type</c> of
/// each <see cref="Expression"/> returned by <c>Build</c> matches the
/// SQL nullability of its <see cref="ResolvedExpression.Type"/>:
/// raw <c>T</c> for non-nullable value/ref types,
/// <c>Nullable&lt;T&gt;</c> for nullable value-type results. The
/// per-row gate at <c>TypedRowEmitter</c> emits the matching field
/// layout. Non-nullable subexpressions stay on the fast path
/// (direct arithmetic, plain comparison, plain AndAlso/OrElse);
/// nullable subexpressions go through NULL-propagating wrappers
/// (HasValue + lift) for arithmetic / comparison and three-valued
/// logic (<see cref="ThreeValuedLogic"/>) for AND/OR/NOT.</para>
/// <para><b>Scope gate.</b> Returns <c>null</c> if any subexpression
/// is outside the supported subset (e.g. an unrecognised function,
/// a CAST direction that's not in the matrix). The caller
/// (TypedPlanCompiler) is expected to fall back to the structural
/// pipeline when this returns <c>null</c>.</para>
/// </remarks>
public static class TypedExpressionCompiler
{
    /// <summary>
    /// Compile <paramref name="expr"/> against the emitted struct
    /// <paramref name="rowType"/>. Returns the delegate boxed as
    /// <see cref="Delegate"/> (closed type is
    /// <c>Func&lt;TRow, TResult&gt;</c>), or <c>null</c> if the
    /// expression is outside scope.
    /// </summary>
    public static Delegate? TryCompile(ResolvedExpression expr, Type rowType)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(rowType);

        try
        {
            var rowParam = Expression.Parameter(rowType, "row");
            var body = Build(expr, rowParam, rowType);
            var delegateType = typeof(Func<,>).MakeGenericType(rowType, body.Type);
            return Expression.Lambda(delegateType, body, rowParam).Compile();
        }
        catch (UnsupportedExpressionException)
        {
            return null;
        }
    }

    /// <summary>
    /// True iff <see cref="TryCompile"/> would succeed for
    /// <paramref name="expr"/> — useful for callers that want to test
    /// the gate without paying for compilation.
    /// </summary>
    public static bool IsCompilable(ResolvedExpression expr, Type rowType)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(rowType);

        try
        {
            var rowParam = Expression.Parameter(rowType, "row");
            _ = Build(expr, rowParam, rowType);
            return true;
        }
        catch (UnsupportedExpressionException)
        {
            return false;
        }
    }

    /// <summary>
    /// Lowers <paramref name="expr"/> against a caller-supplied row
    /// parameter, returning the expression-tree fragment (or
    /// <c>null</c> if unsupported). Used by callers that want to
    /// inline several lowered expressions into a single compiled
    /// lambda — e.g. building one <c>Func&lt;TIn, TOut&gt;</c> that
    /// constructs an output row from several projection expressions.
    /// </summary>
    public static Expression? TryBuildInto(
        ResolvedExpression expr, ParameterExpression rowParam)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(rowParam);

        try
        {
            return Build(expr, rowParam, rowParam.Type);
        }
        catch (UnsupportedExpressionException)
        {
            return null;
        }
    }

    private static Expression Build(ResolvedExpression expr, ParameterExpression row, Type rowType)
    {
        return expr switch
        {
            ResolvedLiteral lit => BuildLiteral(lit),
            ResolvedColumn col => BuildColumn(col, row, rowType),
            ResolvedUnary un => BuildUnary(un, row, rowType),
            ResolvedBinary bin => BuildBinary(bin, row, rowType),
            ResolvedIsNull isn => BuildIsNull(isn, row, rowType),
            ResolvedCast cast => BuildCast(cast, row, rowType),
            ResolvedFunctionCall fn => BuildFunction(fn, row, rowType),
            ResolvedCaseWhen ce => BuildCaseWhen(ce, row, rowType),
            _ => throw Unsupported(),
        };
    }

    /// <summary>
    /// Lowers a searched <c>CASE</c> to a right-to-left nested conditional.
    /// Every branch (and the ELSE / implicit NULL) is coerced to the
    /// expression's resolved result CLR type — lifting non-nullable value
    /// branches to <c>Nullable&lt;T&gt;</c> when the overall result is
    /// nullable. An arm is taken iff its BOOLEAN condition is a definite
    /// TRUE; a nullable condition reads <c>GetValueOrDefault()</c> so NULL
    /// falls through. Evaluation stays lazy: non-taken branches sit in the
    /// false arm and are never run.
    /// </summary>
    private static Expression BuildCaseWhen(ResolvedCaseWhen ce, ParameterExpression row, Type rowType)
    {
        var underlying = ce.Type.ClrType;
        var targetType = ce.Type.Nullable && underlying.IsValueType
            ? typeof(Nullable<>).MakeGenericType(underlying)
            : underlying;

        // An ELSE-less CASE must have a nullable result type to carry the
        // unmatched-NULL. The resolver guarantees this; bail to the
        // structural path defensively if it ever doesn't hold.
        if (ce.ElseResult is null && targetType.IsValueType && !IsNullable(targetType))
        {
            throw Unsupported();
        }

        Expression Coerce(Expression e)
        {
            if (e.Type == targetType) return e;
            if (IsNullable(targetType) && !IsNullable(e.Type) && e.Type.IsValueType)
            {
                return Expression.Convert(e, targetType);
            }

            if (!targetType.IsValueType && !e.Type.IsValueType) return e;
            return Expression.Convert(e, targetType);
        }

        Expression result = ce.ElseResult is null
            ? Expression.Constant(null, targetType)
            : Coerce(Build(ce.ElseResult, row, rowType));

        var getValueOrDefault = typeof(bool?).GetMethod(
            nameof(Nullable<bool>.GetValueOrDefault), Type.EmptyTypes)!;

        for (var i = ce.Whens.Count - 1; i >= 0; i--)
        {
            var cond = Build(ce.Whens[i].Condition, row, rowType);
            if (UnderlyingType(cond.Type) != typeof(bool)) throw Unsupported();
            var taken = IsNullable(cond.Type)
                ? (Expression)Expression.Call(cond, getValueOrDefault)
                : cond;
            var branch = Coerce(Build(ce.Whens[i].Result, row, rowType));
            result = Expression.Condition(taken, branch, result);
        }

        return result;
    }

    // ---- Nullability helpers ----

    /// <summary>True iff <paramref name="type"/> is <c>Nullable&lt;T&gt;</c>.</summary>
    internal static bool IsNullable(Type type) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);

    /// <summary>
    /// Returns <c>T</c> for <c>Nullable&lt;T&gt;</c>, otherwise the
    /// type itself. Use to recover the underlying value type of an
    /// expression regardless of nullability.
    /// </summary>
    internal static Type UnderlyingType(Type type) =>
        IsNullable(type) ? type.GenericTypeArguments[0] : type;

    /// <summary>
    /// Wraps <paramref name="raw"/> (of type T) into a
    /// <c>Nullable&lt;T&gt;</c> expression. No-op if already nullable.
    /// </summary>
    internal static Expression LiftToNullable(Expression raw)
    {
        if (IsNullable(raw.Type)) return raw;
        if (!raw.Type.IsValueType) return raw; // reference type is naturally nullable
        var nullableT = typeof(Nullable<>).MakeGenericType(raw.Type);
        return Expression.Convert(raw, nullableT);
    }

    /// <summary>
    /// Build <c>operand.HasValue ? compute(operand.Value) : null</c> for
    /// a unary op that propagates NULL. <paramref name="compute"/> receives
    /// the unwrapped <c>T</c> value and returns an Expression of result type
    /// (typically the same underlying type as the input).
    /// </summary>
    internal static Expression PropagateUnary(Expression operand, Func<Expression, Expression> compute)
    {
        if (!IsNullable(operand.Type))
        {
            return compute(operand);
        }

        var underlying = UnderlyingType(operand.Type);
        var local = Expression.Variable(operand.Type, "x");
        var value = Expression.Property(local, nameof(Nullable<int>.Value));
        var raw = compute(value);
        var resultNullable = typeof(Nullable<>).MakeGenericType(raw.Type);
        var lifted = Expression.Convert(raw, resultNullable);
        var nullConst = Expression.Constant(null, resultNullable);
        return Expression.Block(
            new[] { local },
            Expression.Assign(local, operand),
            Expression.Condition(
                Expression.Property(local, nameof(Nullable<int>.HasValue)),
                lifted,
                nullConst));
    }

    /// <summary>
    /// Build <c>l.HasValue &amp;&amp; r.HasValue ? compute(l.Value, r.Value) : null</c>
    /// for a binary op that propagates NULL. Locals keep operand
    /// types unchanged (Nullable&lt;T&gt; or T); <c>compute</c>
    /// receives the unwrapped underlying values.
    /// </summary>
    internal static Expression PropagateBinary(
        Expression l, Expression r, Func<Expression, Expression, Expression> compute)
    {
        var lNullable = IsNullable(l.Type);
        var rNullable = IsNullable(r.Type);
        if (!lNullable && !rNullable)
        {
            return compute(l, r);
        }

        // Stash both operands into locals (at their original types) so
        // HasValue / Value can be read without re-evaluating.
        var lLocal = Expression.Variable(l.Type, "l");
        var rLocal = Expression.Variable(r.Type, "r");

        Expression lValue = lNullable
            ? Expression.Property(lLocal, nameof(Nullable<int>.Value))
            : lLocal;
        Expression rValue = rNullable
            ? Expression.Property(rLocal, nameof(Nullable<int>.Value))
            : rLocal;

        var raw = compute(lValue, rValue);
        var resultNullable = raw.Type.IsValueType
            ? typeof(Nullable<>).MakeGenericType(raw.Type)
            : raw.Type;
        var lifted = raw.Type == resultNullable ? raw : Expression.Convert(raw, resultNullable);
        var nullConst = Expression.Constant(null, resultNullable);

        Expression bothHaveValue;
        if (lNullable && rNullable)
        {
            bothHaveValue = Expression.AndAlso(
                Expression.Property(lLocal, nameof(Nullable<int>.HasValue)),
                Expression.Property(rLocal, nameof(Nullable<int>.HasValue)));
        }
        else if (lNullable)
        {
            bothHaveValue = Expression.Property(lLocal, nameof(Nullable<int>.HasValue));
        }
        else
        {
            bothHaveValue = Expression.Property(rLocal, nameof(Nullable<int>.HasValue));
        }

        return Expression.Block(
            new[] { lLocal, rLocal },
            Expression.Assign(lLocal, l),
            Expression.Assign(rLocal, r),
            Expression.Condition(bothHaveValue, lifted, nullConst));
    }

    // ---- Build branches ----

    private static Expression BuildLiteral(ResolvedLiteral lit)
    {
        if (lit.Value is null)
        {
            // NULL literal — typed as Nullable<UnderlyingT>. Use a
            // sensible default underlying type for SqlIntegerType
            // (which is what the resolver tags raw NULL with).
            var clr = lit.Type.ClrType;
            if (clr.IsValueType)
            {
                var nt = typeof(Nullable<>).MakeGenericType(clr);
                return Expression.Constant(null, nt);
            }

            return Expression.Constant(null, clr);
        }

        var raw = Expression.Constant(lit.Value, lit.Type.ClrType);
        return lit.Type.Nullable ? LiftToNullable(raw) : raw;
    }

    private static Expression BuildColumn(ResolvedColumn col, ParameterExpression row, Type rowType)
    {
        var field = rowType.GetField("F" + col.Index)
            ?? throw Unsupported();
        return Expression.Field(row, field);
    }

    private static Expression BuildIsNull(ResolvedIsNull isn, ParameterExpression row, Type rowType)
    {
        var operand = Build(isn.Operand, row, rowType);

        // Non-nullable operand: collapses to a constant.
        // Nullable value-type: !operand.HasValue (for IS NULL) or operand.HasValue (for IS NOT NULL).
        // Reference-type nullable: operand == null vs operand != null.
        Expression isNullExpr;
        if (IsNullable(operand.Type))
        {
            var hasValue = Expression.Property(operand, nameof(Nullable<int>.HasValue));
            isNullExpr = Expression.Not(hasValue);
        }
        else if (!operand.Type.IsValueType)
        {
            isNullExpr = Expression.Equal(operand, Expression.Constant(null, operand.Type));
        }
        else
        {
            // Definite value — IS NULL is constant false.
            return Expression.Constant(isn.Negated);
        }

        return isn.Negated ? (Expression)Expression.Not(isNullExpr) : isNullExpr;
    }

    private static Expression BuildUnary(ResolvedUnary un, ParameterExpression row, Type rowType)
    {
        var operand = Build(un.Operand, row, rowType);

        return un.Operator switch
        {
            UnOp.Not when UnderlyingType(operand.Type) == typeof(bool) => BuildNot(operand),
            UnOp.Negate when IsNumericNonDecimal(UnderlyingType(operand.Type))
                => PropagateUnary(operand, Expression.Negate),
            UnOp.Negate when UnderlyingType(operand.Type) == typeof(Decimal128)
                => PropagateUnary(operand, Expression.Negate),
            _ => throw Unsupported(),
        };
    }

    private static Expression BuildNot(Expression operand)
    {
        if (!IsNullable(operand.Type))
        {
            return Expression.Not(operand);
        }

        // 3VL NOT via the runtime helper. operand is bool?.
        var method = typeof(ThreeValuedLogic).GetMethod(nameof(ThreeValuedLogic.Not))!;
        return Expression.Call(method, operand);
    }

    private static Expression BuildBinary(ResolvedBinary bin, ParameterExpression row, Type rowType)
    {
        var l = Build(bin.Left, row, rowType);
        var r = Build(bin.Right, row, rowType);

        switch (bin.Operator)
        {
            case BinOp.And:
            case BinOp.Or:
                return BuildLogical(bin.Operator, l, r);

            case BinOp.Add:
            case BinOp.Subtract:
            case BinOp.Multiply:
            case BinOp.Divide:
            case BinOp.Modulo:
                if (bin.Type is SqlDecimalType decResult)
                {
                    return BuildDecimalArith(bin.Operator, l, bin.Left.Type, r, bin.Right.Type, decResult);
                }

                return BuildNumericArith(bin.Operator, l, r, bin.Type.ClrType);

            case BinOp.Equal:
            case BinOp.NotEqual:
            case BinOp.Less:
            case BinOp.LessEqual:
            case BinOp.Greater:
            case BinOp.GreaterEqual:
                if (bin.Left.Type is SqlDecimalType || bin.Right.Type is SqlDecimalType)
                {
                    return BuildDecimalCompare(bin.Operator, l, bin.Left.Type, r, bin.Right.Type);
                }

                return BuildCompare(bin.Operator, l, r);

            default:
                throw Unsupported();
        }
    }

    private static Expression BuildLogical(BinOp op, Expression l, Expression r)
    {
        if (UnderlyingType(l.Type) != typeof(bool) || UnderlyingType(r.Type) != typeof(bool))
        {
            throw Unsupported();
        }

        // Both definite → plain AndAlso/OrElse.
        if (!IsNullable(l.Type) && !IsNullable(r.Type))
        {
            return op == BinOp.And ? Expression.AndAlso(l, r) : Expression.OrElse(l, r);
        }

        // At least one nullable bool → three-valued logic via runtime helper.
        var lift = (Expression e) => IsNullable(e.Type) ? e : LiftToNullable(e);
        var method = typeof(ThreeValuedLogic).GetMethod(
            op == BinOp.And ? nameof(ThreeValuedLogic.And) : nameof(ThreeValuedLogic.Or))!;
        return Expression.Call(method, lift(l), lift(r));
    }

    private static Expression BuildNumericArith(BinOp op, Expression l, Expression r, Type arithClr)
    {
        if (!IsNumericNonDecimal(arithClr)) throw Unsupported();

        // Widen each operand's underlying type to the result type if
        // necessary. For nullable operands, the widen happens on the
        // unwrapped value inside PropagateBinary's compute closure.
        return PropagateBinary(l, r, (lv, rv) =>
        {
            if (lv.Type != arithClr) lv = Expression.Convert(lv, arithClr);
            if (rv.Type != arithClr) rv = Expression.Convert(rv, arithClr);
            return op switch
            {
                BinOp.Add => Expression.Add(lv, rv),
                BinOp.Subtract => Expression.Subtract(lv, rv),
                BinOp.Multiply => Expression.Multiply(lv, rv),
                BinOp.Divide => Expression.Divide(lv, rv),
                BinOp.Modulo => Expression.Modulo(lv, rv),
                _ => throw Unsupported(),
            };
        });
    }

    private static Expression BuildCompare(BinOp op, Expression l, Expression r)
    {
        var lu = UnderlyingType(l.Type);
        var ru = UnderlyingType(r.Type);
        if (lu != ru) throw Unsupported();
        if (!IsComparable(lu)) throw Unsupported();

        return PropagateBinary(l, r, (lv, rv) => op switch
        {
            BinOp.Equal => Expression.Equal(lv, rv),
            BinOp.NotEqual => Expression.NotEqual(lv, rv),
            BinOp.Less => Expression.LessThan(lv, rv),
            BinOp.LessEqual => Expression.LessThanOrEqual(lv, rv),
            BinOp.Greater => Expression.GreaterThan(lv, rv),
            BinOp.GreaterEqual => Expression.GreaterThanOrEqual(lv, rv),
            _ => throw Unsupported(),
        });
    }

    /// <summary>
    /// Decimal arithmetic dispatches to <see cref="DecimalRuntime"/>
    /// with operand and result <see cref="DecimalType"/>s baked in as
    /// constants. Integer operands are promoted to
    /// <see cref="Decimal128"/> at scale 0. NULL propagation wraps
    /// the call when either operand is nullable.
    /// </summary>
    private static Expression BuildDecimalArith(
        BinOp op, Expression l, SqlType leftType, Expression r, SqlType rightType,
        SqlDecimalType resultDec)
    {
        return PropagateBinary(l, r, (lv, rv) =>
        {
            var (lExpr, lType) = ToDecimalOperand(lv, leftType);
            var (rExpr, rType) = ToDecimalOperand(rv, rightType);
            var resultType = DecimalType.Numeric(resultDec.Precision, resultDec.Scale);
            var methodName = op switch
            {
                BinOp.Add => nameof(DecimalRuntime.Add),
                BinOp.Subtract => nameof(DecimalRuntime.Subtract),
                BinOp.Multiply => nameof(DecimalRuntime.Multiply),
                BinOp.Divide => nameof(DecimalRuntime.Divide),
                BinOp.Modulo => nameof(DecimalRuntime.Modulus),
                _ => throw Unsupported(),
            };
            var method = typeof(DecimalRuntime).GetMethod(methodName)!;
            return Expression.Call(
                method,
                lExpr, Expression.Constant(lType),
                rExpr, Expression.Constant(rType),
                Expression.Constant(resultType));
        });
    }

    /// <summary>
    /// Decimal comparison: rescale both operands to their common
    /// scale, compare mantissas, reduce the int result to the
    /// requested boolean operator. NULL propagation wraps the call
    /// when either operand is nullable.
    /// </summary>
    private static Expression BuildDecimalCompare(
        BinOp op, Expression l, SqlType leftType, Expression r, SqlType rightType)
    {
        return PropagateBinary(l, r, (lv, rv) =>
        {
            var (lExpr, lType) = ToDecimalOperand(lv, leftType);
            var (rExpr, rType) = ToDecimalOperand(rv, rightType);
            var cmp = Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Compare))!,
                lExpr, Expression.Constant(lType),
                rExpr, Expression.Constant(rType));
            var zero = Expression.Constant(0);
            return op switch
            {
                BinOp.Equal => Expression.Equal(cmp, zero),
                BinOp.NotEqual => Expression.NotEqual(cmp, zero),
                BinOp.Less => Expression.LessThan(cmp, zero),
                BinOp.LessEqual => Expression.LessThanOrEqual(cmp, zero),
                BinOp.Greater => Expression.GreaterThan(cmp, zero),
                BinOp.GreaterEqual => Expression.GreaterThanOrEqual(cmp, zero),
                _ => throw Unsupported(),
            };
        });
    }

    private static (Expression Value, DecimalType Type) ToDecimalOperand(
        Expression operand, SqlType srcType)
    {
        if (srcType is SqlDecimalType d)
        {
            if (operand.Type != typeof(Decimal128)) throw Unsupported();
            return (operand, DecimalType.Numeric(d.Precision, d.Scale));
        }

        if (srcType is SqlIntegerType)
        {
            if (operand.Type != typeof(int)) throw Unsupported();
            var promote = Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.FromInt32))!,
                operand);
            return (promote, DecimalType.Numeric(38, 0));
        }

        if (srcType is SqlBigintType)
        {
            if (operand.Type != typeof(long)) throw Unsupported();
            var promote = Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.FromInt64))!,
                operand);
            return (promote, DecimalType.Numeric(38, 0));
        }

        throw Unsupported();
    }

    private static Expression BuildCast(ResolvedCast cast, ParameterExpression row, Type rowType)
    {
        var operand = Build(cast.Operand, row, rowType);
        var srcType = cast.Operand.Type;
        var dstType = cast.Type;

        // Decimal-aware paths first; decimal scale isn't captured in
        // the CLR type alone, so srcClr == dstClr doesn't short-circuit
        // for cross-scale decimal casts.
        if (dstType is SqlDecimalType targetDec)
        {
            return PropagateUnary(operand, op => BuildCastToDecimal(op, srcType, targetDec));
        }

        if (srcType is SqlDecimalType sourceDec)
        {
            return PropagateUnary(operand, op => BuildCastFromDecimal(op, sourceDec, dstType));
        }

        var srcClr = UnderlyingType(operand.Type);
        var dstClr = dstType.ClrType;

        // Identity cast (no representational change).
        if (srcClr == dstClr)
        {
            // If destination is nullable but operand isn't, lift.
            if (dstType.Nullable && !IsNullable(operand.Type) && operand.Type.IsValueType)
            {
                return LiftToNullable(operand);
            }

            return operand;
        }

        if (IsNumericNonDecimal(srcClr) && IsNumericNonDecimal(dstClr))
        {
            return PropagateUnary(operand, op => Expression.Convert(op, dstClr));
        }

        if (srcClr == typeof(bool) && dstClr == typeof(Utf8String))
        {
            return PropagateUnary(operand, BuildBoolToUtf8);
        }

        if (IsNumericNonDecimal(srcClr) && dstClr == typeof(Utf8String))
        {
            return PropagateUnary(operand, op => BuildNumericToUtf8(op, srcClr));
        }

        if (srcClr == typeof(Utf8String) && IsNumericNonDecimal(dstClr))
        {
            return PropagateUnary(operand, op => BuildUtf8ToNumeric(op, dstClr));
        }

        if (srcClr == typeof(Utf8String) && IsTemporal(dstClr))
        {
            return PropagateUnary(operand, op => BuildUtf8ToTemporal(op, dstClr));
        }

        if (IsTemporal(srcClr) && dstClr == typeof(Utf8String))
        {
            return PropagateUnary(operand, op => BuildTemporalToUtf8(op, srcClr));
        }

        if (srcClr == typeof(Timestamp) && dstClr == typeof(Date32))
        {
            // CAST(timestamp AS date): discard the time-of-day (floor to the day).
            return PropagateUnary(operand, op => Expression.Call(
                typeof(Date32).GetMethod(nameof(Date32.FromTimestamp), [typeof(Timestamp)])!, op));
        }

        if (srcClr == typeof(Date32) && dstClr == typeof(Timestamp))
        {
            // CAST(date AS timestamp): midnight (00:00:00) of that day.
            return PropagateUnary(operand, op => Expression.Call(
                typeof(Timestamp).GetMethod(nameof(Timestamp.FromDate), [typeof(Date32)])!, op));
        }

        throw Unsupported();
    }

    private static Expression BuildBoolToUtf8(Expression operand)
    {
        var toStr = Expression.Call(operand,
            typeof(bool).GetMethod(nameof(object.ToString), Type.EmptyTypes)!);
        return Expression.Call(
            typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
            toStr);
    }

    private static Expression BuildNumericToUtf8(Expression operand, Type srcClr)
    {
        var method = typeof(SqlCasts).GetMethod(nameof(SqlCasts.NumericToString), [srcClr])
            ?? throw Unsupported();
        return Expression.Call(
            typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
            Expression.Call(method, operand));
    }

    private static Expression BuildUtf8ToNumeric(Expression operand, Type dstClr)
    {
        var asString = Expression.Call(operand,
            typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
        var method = dstClr switch
        {
            var t when t == typeof(int) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseInt32))!,
            var t when t == typeof(long) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseInt64))!,
            var t when t == typeof(float) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseSingle))!,
            var t when t == typeof(double) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseDouble))!,
            _ => throw Unsupported(),
        };
        return Expression.Call(method, asString);
    }

    private static Expression BuildUtf8ToTemporal(Expression operand, Type dstClr)
    {
        var asString = Expression.Call(operand,
            typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
        var method = dstClr switch
        {
            var t when t == typeof(Date32) =>
                typeof(Date32).GetMethod(nameof(Date32.Parse), [typeof(string)])!,
            var t when t == typeof(Time64) =>
                typeof(Time64).GetMethod(nameof(Time64.Parse), [typeof(string)])!,
            var t when t == typeof(Timestamp) =>
                typeof(Timestamp).GetMethod(nameof(Timestamp.Parse), [typeof(string)])!,
            _ => throw Unsupported(),
        };
        return Expression.Call(method, asString);
    }

    private static Expression BuildTemporalToUtf8(Expression operand, Type srcClr)
    {
        var toStr = Expression.Call(operand,
            srcClr.GetMethod(nameof(object.ToString), Type.EmptyTypes)!);
        return Expression.Call(
            typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
            toStr);
    }

    private static bool IsTemporal(Type t) =>
        t == typeof(Date32) || t == typeof(Time64) || t == typeof(Timestamp);

    private static Expression BuildCastToDecimal(
        Expression operand, SqlType srcType, SqlDecimalType target)
    {
        if (srcType is SqlDecimalType srcDec)
        {
            if (srcDec.Scale == target.Scale)
            {
                return operand;
            }

            var mantissa = Expression.Field(operand, nameof(Decimal128.Mantissa));
            var rescaled = Expression.Call(
                typeof(ScaleHelper).GetMethod(nameof(ScaleHelper.Rescale128))!,
                mantissa,
                Expression.Constant(srcDec.Scale),
                Expression.Constant(target.Scale));
            var ctor = typeof(Decimal128).GetConstructor([typeof(Int128)])!;
            return Expression.New(ctor, rescaled);
        }

        if (srcType is SqlIntegerType or SqlBigintType)
        {
            var srcClr = srcType.ClrType;
            if (operand.Type != srcClr) operand = Expression.Convert(operand, srcClr);
            var asInt128 = Expression.Convert(operand, typeof(Int128));
            return Expression.Call(
                typeof(Decimal128).GetMethod(nameof(Decimal128.FromInteger))!,
                asInt128,
                Expression.Constant((byte)target.Scale));
        }

        if (srcType is SqlVarcharType)
        {
            var targetType = DecimalType.Numeric(target.Precision, target.Scale);
            var asString = Expression.Call(operand,
                typeof(Utf8String).GetMethod(nameof(Utf8String.ToStringDecoded))!);
            var asSpan = Expression.Call(
                typeof(MemoryExtensions).GetMethod(nameof(MemoryExtensions.AsSpan), [typeof(string)])!,
                asString);
            return Expression.Call(
                typeof(Clast.DatabaseDecimal.Text.DecimalText).GetMethod(
                    nameof(Clast.DatabaseDecimal.Text.DecimalText.ParseDecimal128),
                    [typeof(ReadOnlySpan<char>), typeof(DecimalType)])!,
                asSpan,
                Expression.Constant(targetType));
        }

        throw Unsupported();
    }

    private static Expression BuildCastFromDecimal(
        Expression operand, SqlDecimalType src, SqlType dstType)
    {
        var dstClr = dstType.ClrType;
        if (dstType is SqlVarcharType)
        {
            var sourceType = DecimalType.Numeric(src.Precision, src.Scale);
            var format = Expression.Call(
                typeof(Clast.DatabaseDecimal.Text.DecimalText).GetMethod(
                    nameof(Clast.DatabaseDecimal.Text.DecimalText.Format),
                    [typeof(Decimal128), typeof(DecimalType)])!,
                operand,
                Expression.Constant(sourceType));
            return Expression.Call(
                typeof(Utf8String).GetMethod(nameof(Utf8String.Of), [typeof(string)])!,
                format);
        }

        if (!IsNumericNonDecimal(dstClr)) throw Unsupported();

        var mantissa = Expression.Field(operand, nameof(Decimal128.Mantissa));
        var rescaled = Expression.Call(
            typeof(ScaleHelper).GetMethod(nameof(ScaleHelper.Rescale128))!,
            mantissa,
            Expression.Constant(src.Scale),
            Expression.Constant(0));
        return Expression.Convert(rescaled, dstClr);
    }

    /// <summary>
    /// Lowers a builtin scalar function call by recursively compiling
    /// the args and delegating to <see cref="ScalarFunctionRegistry.BuildTyped"/>.
    /// Functions outside the typed pipeline's scope return null and cause the
    /// whole compile to fall back.
    /// </summary>
    private static Expression BuildFunction(ResolvedFunctionCall fn, ParameterExpression row, Type rowType)
    {
        var args = new Expression[fn.Arguments.Count];
        for (var i = 0; i < fn.Arguments.Count; i++)
        {
            args[i] = Build(fn.Arguments[i], row, rowType);
        }

        return ScalarFunctionRegistry.BuildTyped(fn, fn.Arguments, args)
            ?? throw Unsupported();
    }

    private static bool IsNumericNonDecimal(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(float) || t == typeof(double);

    private static bool IsComparable(Type t) =>
        IsNumericNonDecimal(t) || t == typeof(bool) || t == typeof(Utf8String)
        || t == typeof(Decimal128) || IsTemporal(t);

    private static UnsupportedExpressionException Unsupported() => new();

    private sealed class UnsupportedExpressionException : Exception;
}
