using System.Linq.Expressions;
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
/// is the SqlType's CLR type. Columns are direct field reads
/// (<c>row.F{i}</c>) — no boxing through <see cref="object"/> or
/// <c>IReadOnlyList&lt;object?&gt;</c>.
/// </summary>
/// <remarks>
/// <para><b>Scope gate.</b> Returns <c>null</c> if any subexpression
/// has a nullable result type, is a <see cref="SqlDecimalType"/>, is a
/// function call, or is a CAST outside the numeric-numeric matrix.
/// The caller (TypedPlanCompiler) is expected to fall back to the
/// structural pipeline when this returns <c>null</c>.</para>
/// <para><b>NULL handling.</b> Because the typed pipeline rejects
/// nullable columns at the schema gate, every leaf has a definite
/// value. The compiler exploits this: <c>IS NULL</c> reduces to a
/// constant <c>false</c>, <c>AND</c>/<c>OR</c> are plain
/// <see cref="Expression.AndAlso"/> / <see cref="Expression.OrElse"/>
/// (no three-valued logic), and there's no NULL-propagation conditional
/// wrapped around arithmetic / comparison results.</para>
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
        // Note: we don't gate on `expr.Type.Nullable`. In the typed
        // pipeline every value on every stream is non-null by
        // construction — the typed-row gate at the schema level
        // enforces it. Resolver type inference may mark expression
        // results as nullable in coarser cases (e.g. SQL aggregate
        // outputs, which we know are non-null on the typed path
        // because empty groups are dropped before the aggregator
        // runs). Treating the resolver's nullability as advisory and
        // operating on the underlying definite values is correct.
        // Explicit null literals still reject below (BuildLiteral),
        // so user-written NULL still falls back.
        return expr switch
        {
            ResolvedLiteral lit => BuildLiteral(lit),
            ResolvedColumn col => BuildColumn(col, row, rowType),
            ResolvedUnary un => BuildUnary(un, row, rowType),
            ResolvedBinary bin => BuildBinary(bin, row, rowType),
            ResolvedIsNull isn => BuildIsNull(isn, row, rowType),
            ResolvedCast cast => BuildCast(cast, row, rowType),
            ResolvedFunctionCall fn => BuildFunction(fn, row, rowType),
            _ => throw Unsupported(),
        };
    }

    private static Expression BuildLiteral(ResolvedLiteral lit)
    {
        if (lit.Value is null) throw Unsupported();
        return Expression.Constant(lit.Value, lit.Type.ClrType);
    }

    private static Expression BuildColumn(ResolvedColumn col, ParameterExpression row, Type rowType)
    {
        var field = rowType.GetField("F" + col.Index)
            ?? throw Unsupported();
        return Expression.Field(row, field);
    }

    private static Expression BuildUnary(ResolvedUnary un, ParameterExpression row, Type rowType)
    {
        var operand = Build(un.Operand, row, rowType);
        return un.Operator switch
        {
            UnOp.Not when operand.Type == typeof(bool) => Expression.Not(operand),
            UnOp.Negate when IsNumericNonDecimal(operand.Type) => Expression.Negate(operand),
            // Decimal128 has op_UnaryNegation — Expression.Negate
            // resolves it via the user-defined operator lookup.
            UnOp.Negate when operand.Type == typeof(Decimal128) => Expression.Negate(operand),
            _ => throw Unsupported(),
        };
    }

    private static Expression BuildBinary(ResolvedBinary bin, ParameterExpression row, Type rowType)
    {
        var l = Build(bin.Left, row, rowType);
        var r = Build(bin.Right, row, rowType);

        switch (bin.Operator)
        {
            case BinOp.And:
            case BinOp.Or:
                if (l.Type != typeof(bool) || r.Type != typeof(bool)) throw Unsupported();
                return bin.Operator == BinOp.And
                    ? Expression.AndAlso(l, r)
                    : Expression.OrElse(l, r);

            case BinOp.Add:
            case BinOp.Subtract:
            case BinOp.Multiply:
            case BinOp.Divide:
            case BinOp.Modulo:
                if (bin.Type is SqlDecimalType decResult)
                {
                    return BuildDecimalArith(bin.Operator, l, bin.Left.Type, r, bin.Right.Type, decResult);
                }

                // Non-decimal arithmetic: resolver assigns a single
                // result type; widen operands to it (no-op when they
                // already match).
                var arithClr = bin.Type.ClrType;
                if (!IsNumericNonDecimal(arithClr)) throw Unsupported();
                if (l.Type != arithClr) l = Expression.Convert(l, arithClr);
                if (r.Type != arithClr) r = Expression.Convert(r, arithClr);
                return bin.Operator switch
                {
                    BinOp.Add => Expression.Add(l, r),
                    BinOp.Subtract => Expression.Subtract(l, r),
                    BinOp.Multiply => Expression.Multiply(l, r),
                    BinOp.Divide => Expression.Divide(l, r),
                    BinOp.Modulo => Expression.Modulo(l, r),
                    _ => throw Unsupported(),
                };

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

                if (l.Type != r.Type) throw Unsupported();
                if (!IsComparable(l.Type)) throw Unsupported();
                return bin.Operator switch
                {
                    BinOp.Equal => Expression.Equal(l, r),
                    BinOp.NotEqual => Expression.NotEqual(l, r),
                    BinOp.Less => Expression.LessThan(l, r),
                    BinOp.LessEqual => Expression.LessThanOrEqual(l, r),
                    BinOp.Greater => Expression.GreaterThan(l, r),
                    BinOp.GreaterEqual => Expression.GreaterThanOrEqual(l, r),
                    _ => throw Unsupported(),
                };

            default:
                throw Unsupported();
        }
    }

    /// <summary>
    /// Decimal arithmetic dispatches to <see cref="DecimalRuntime"/>
    /// with operand and result <see cref="DecimalType"/>s baked in as
    /// constants — same pattern as the structural compiler, just
    /// without the boxing wrapper. Integer operands are promoted to
    /// <see cref="Decimal128"/> at scale 0 via
    /// <see cref="DecimalRuntime.FromInt32"/> /
    /// <see cref="DecimalRuntime.FromInt64"/>.
    /// </summary>
    private static Expression BuildDecimalArith(
        BinOp op, Expression l, SqlType leftType, Expression r, SqlType rightType,
        SqlDecimalType resultDec)
    {
        var (lExpr, lType) = ToDecimalOperand(l, leftType);
        var (rExpr, rType) = ToDecimalOperand(r, rightType);
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
            lExpr,
            Expression.Constant(lType),
            rExpr,
            Expression.Constant(rType),
            Expression.Constant(resultType));
    }

    /// <summary>
    /// Decimal comparison: rescale both operands to their common
    /// scale, compare mantissas, and reduce the int result to the
    /// requested boolean operator.
    /// </summary>
    private static Expression BuildDecimalCompare(
        BinOp op, Expression l, SqlType leftType, Expression r, SqlType rightType)
    {
        var (lExpr, lType) = ToDecimalOperand(l, leftType);
        var (rExpr, rType) = ToDecimalOperand(r, rightType);
        var cmp = Expression.Call(
            typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Compare))!,
            lExpr,
            Expression.Constant(lType),
            rExpr,
            Expression.Constant(rType));
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
    }

    /// <summary>
    /// Convert <paramref name="operand"/> (which is already at its
    /// CLR type — Decimal128, int, or long) to a typed Decimal128
    /// expression and report its <see cref="DecimalType"/> for the
    /// kernel. Mirrors <c>ExpressionCompiler.ToDecimalOperand</c>
    /// but without unboxing — operands are already typed here.
    /// </summary>
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

    private static Expression BuildIsNull(ResolvedIsNull isn, ParameterExpression row, Type rowType)
    {
        // Walk the operand for gate consistency (rejects nullable /
        // decimal / unsupported subexpressions even though we don't
        // use the result). Then collapse to a constant — every leaf
        // in this compiler is non-null, so IS NULL is always false
        // and IS NOT NULL is always true.
        _ = Build(isn.Operand, row, rowType);
        return Expression.Constant(isn.Negated);
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
            return BuildCastToDecimal(operand, srcType, targetDec);
        }

        if (srcType is SqlDecimalType sourceDec)
        {
            return BuildCastFromDecimal(operand, sourceDec, dstType);
        }

        var srcClr = srcType.ClrType;
        var dstClr = dstType.ClrType;
        if (srcClr == dstClr) return operand;
        if (IsNumericNonDecimal(srcClr) && IsNumericNonDecimal(dstClr))
        {
            return Expression.Convert(operand, dstClr);
        }

        throw Unsupported();
    }

    /// <summary>
    /// CAST any supported numeric source to <see cref="SqlDecimalType"/>.
    /// Decimal-to-decimal rescales; integer-to-decimal promotes and
    /// scales up. Mirrors the structural compiler's path minus the
    /// NULL-propagation wrapper.
    /// </summary>
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

        // Decimal CAST from VARCHAR / temporal etc. lives in the
        // structural cast matrix and isn't covered by Phase 1.8.
        throw Unsupported();
    }

    /// <summary>
    /// CAST a <see cref="SqlDecimalType"/> source to a non-decimal
    /// numeric target. Rescales the mantissa to scale 0, then
    /// converts to the target CLR type.
    /// </summary>
    private static Expression BuildCastFromDecimal(
        Expression operand, SqlDecimalType src, SqlType dstType)
    {
        var dstClr = dstType.ClrType;
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
    /// the args and delegating to <see cref="TypedBuiltinScalarFunctions.TryBuild"/>.
    /// Functions outside the typed pipeline's scope (currently just
    /// <c>NULLIF</c>) cause the whole compile to fall back.
    /// </summary>
    private static Expression BuildFunction(ResolvedFunctionCall fn, ParameterExpression row, Type rowType)
    {
        var args = new Expression[fn.Arguments.Count];
        for (var i = 0; i < fn.Arguments.Count; i++)
        {
            args[i] = Build(fn.Arguments[i], row, rowType);
        }

        return TypedBuiltinScalarFunctions.TryBuild(fn, fn.Arguments, args)
            ?? throw Unsupported();
    }

    private static bool IsNumericNonDecimal(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(float) || t == typeof(double);

    private static bool IsComparable(Type t) =>
        IsNumericNonDecimal(t) || t == typeof(bool) || t == typeof(Utf8String)
        || t == typeof(Decimal128);

    private static UnsupportedExpressionException Unsupported() => new();

    private sealed class UnsupportedExpressionException : Exception;
}
