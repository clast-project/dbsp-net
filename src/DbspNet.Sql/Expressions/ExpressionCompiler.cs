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
        _ => throw new InvalidOperationException($"unsupported expression: {expr.GetType().Name}"),
    };

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
        else
        {
            throw new InvalidOperationException(
                $"unsupported CAST from {cast.Operand.Type.Display} to {cast.Type.Display}");
        }

        return Expression.Condition(isNull, NullObject, converted);
    }

    private static Expression BuildFunction(ResolvedFunctionCall fn, ParameterExpression row)
    {
        return BuiltinScalarFunctions.Build(fn, arg => Build(arg, row));
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
        _ => throw new InvalidOperationException($"no CLR mapping for {t.Display}"),
    };

    private static bool IsNumeric(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(float)
        || t == typeof(double) || t == typeof(decimal);

    private static bool IsTemporal(Type t) =>
        t == typeof(Date32) || t == typeof(Time64) || t == typeof(Timestamp);
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
