using System.Linq.Expressions;
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
        if (expr.Type.Nullable) throw Unsupported();
        if (expr.Type is SqlDecimalType) throw Unsupported();

        return expr switch
        {
            ResolvedLiteral lit => BuildLiteral(lit),
            ResolvedColumn col => BuildColumn(col, row, rowType),
            ResolvedUnary un => BuildUnary(un, row, rowType),
            ResolvedBinary bin => BuildBinary(bin, row, rowType),
            ResolvedIsNull isn => BuildIsNull(isn, row, rowType),
            ResolvedCast cast => BuildCast(cast, row, rowType),
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
                // Resolver assigns a single result type; widen operands
                // to it (no-op when they already match).
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
        var srcClr = cast.Operand.Type.ClrType;
        var dstClr = cast.Type.ClrType;

        if (srcClr == dstClr) return operand;
        if (IsNumericNonDecimal(srcClr) && IsNumericNonDecimal(dstClr))
        {
            return Expression.Convert(operand, dstClr);
        }

        throw Unsupported();
    }

    private static bool IsNumericNonDecimal(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(float) || t == typeof(double);

    private static bool IsComparable(Type t) =>
        IsNumericNonDecimal(t) || t == typeof(bool) || t == typeof(Utf8String);

    private static UnsupportedExpressionException Unsupported() => new();

    private sealed class UnsupportedExpressionException : Exception;
}
