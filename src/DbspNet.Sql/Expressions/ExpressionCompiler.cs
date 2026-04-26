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
                return NullPropagatingNumericOp(bin.Operator, l, r, bin.Type);
            case BinOp.Equal:
            case BinOp.NotEqual:
            case BinOp.Less:
            case BinOp.LessEqual:
            case BinOp.Greater:
            case BinOp.GreaterEqual:
                return NullPropagatingCompareOp(bin.Operator, l, r, bin.Left.Type);
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
        BinOp op, Expression l, Expression r, SqlType resultType)
    {
        var clr = ClrOf(resultType);
        var isNull = Expression.OrElse(
            Expression.Equal(l, NullObject),
            Expression.Equal(r, NullObject));
        var lv = Expression.Convert(l, clr);
        var rv = Expression.Convert(r, clr);
        Expression compute = op switch
        {
            BinOp.Add => Expression.Add(lv, rv),
            BinOp.Subtract => Expression.Subtract(lv, rv),
            BinOp.Multiply => Expression.Multiply(lv, rv),
            BinOp.Divide => Expression.Divide(lv, rv),
            BinOp.Modulo => Expression.Modulo(lv, rv),
            _ => throw new InvalidOperationException(),
        };

        return Expression.Condition(
            isNull,
            NullObject,
            Expression.Convert(compute, typeof(object)));
    }

    private static Expression NullPropagatingCompareOp(
        BinOp op, Expression l, Expression r, SqlType operandType)
    {
        var clr = ClrOf(operandType);
        var isNull = Expression.OrElse(
            Expression.Equal(l, NullObject),
            Expression.Equal(r, NullObject));
        var lv = Expression.Convert(l, clr);
        var rv = Expression.Convert(r, clr);

        Expression compute;
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
        else if (srcClr == typeof(bool) && dstClr == typeof(string))
        {
            var unboxed = Expression.Convert(operand, typeof(bool));
            var toStr = Expression.Call(unboxed,
                typeof(bool).GetMethod(nameof(object.ToString), Type.EmptyTypes)!);
            converted = Expression.Convert(toStr, typeof(object));
        }
        else if (IsNumeric(srcClr) && dstClr == typeof(string))
        {
            var unboxed = Expression.Convert(operand, srcClr);
            // Use invariant culture so CAST(1.5 AS VARCHAR) is locale-stable.
            var method = typeof(SqlCasts).GetMethod(nameof(SqlCasts.NumericToString), [srcClr])
                ?? throw new InvalidOperationException($"no string cast for {srcClr}");
            converted = Expression.Convert(Expression.Call(method, unboxed), typeof(object));
        }
        else if (srcClr == typeof(string) && IsNumeric(dstClr))
        {
            var unboxed = Expression.Convert(operand, typeof(string));
            var method = dstClr switch
            {
                var t when t == typeof(int) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseInt32))!,
                var t when t == typeof(long) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseInt64))!,
                var t when t == typeof(float) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseSingle))!,
                var t when t == typeof(double) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseDouble))!,
                var t when t == typeof(decimal) => typeof(SqlCasts).GetMethod(nameof(SqlCasts.ParseDecimal))!,
                _ => throw new InvalidOperationException($"no string-to-{dstClr} parser"),
            };
            converted = Expression.Convert(Expression.Call(method, unboxed), typeof(object));
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

    // ---- Type helpers ----

    private static Type ClrOf(SqlType t) => t switch
    {
        SqlIntegerType => typeof(int),
        SqlBigintType => typeof(long),
        SqlRealType => typeof(float),
        SqlDoubleType => typeof(double),
        SqlDecimalType => typeof(decimal),
        SqlVarcharType => typeof(string),
        SqlBooleanType => typeof(bool),
        _ => throw new InvalidOperationException($"no CLR mapping for {t.Display}"),
    };

    private static bool IsNumeric(Type t) =>
        t == typeof(int) || t == typeof(long) || t == typeof(float)
        || t == typeof(double) || t == typeof(decimal);
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
