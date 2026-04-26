using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Lookup / dispatch for the v1 scalar function library. Every function's
/// resolver-side type-inference and compiler-side
/// <see cref="System.Linq.Expressions.Expression"/> construction live together
/// here so the two layers can't drift apart. Runtime helpers invoked from the
/// generated expression trees are in <see cref="SqlBuiltinRuntime"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>NULL handling.</b> Most functions propagate NULL (any NULL arg ⇒
/// NULL result). Variadic <c>CONCAT</c>, <c>GREATEST</c>, and <c>LEAST</c>
/// follow PostgreSQL: NULL args are skipped. <c>NULLIF(x, y)</c> returns
/// <c>NULL</c> when the two operands are equal and <c>x</c> otherwise (with
/// its own NULL-handling rules; see the case below).
/// </para>
/// </remarks>
internal static class BuiltinScalarFunctions
{
    private static readonly ConstantExpression NullObject = Expression.Constant(null, typeof(object));

    /// <summary>
    /// <c>true</c> iff <paramref name="name"/> names a builtin scalar
    /// function (so the resolver can skip the aggregate-dispatch path).
    /// </summary>
    public static bool IsKnown(string name) => name switch
    {
        "coalesce" or "upper" or "lower" or "length" or "concat"
            or "abs" or "floor" or "ceil" or "ceiling" or "round" or "power" or "sqrt"
            or "greatest" or "least" or "nullif" => true,
        _ => false,
    };

    /// <summary>
    /// Resolver entry — validate arg counts/types, apply coercions, return
    /// a <see cref="ResolvedFunctionCall"/> with the inferred result type.
    /// </summary>
    public static ResolvedFunctionCall Resolve(string name, IReadOnlyList<ResolvedExpression> args)
    {
        switch (name)
        {
            case "coalesce":
                return ResolveCoalesce(args);
            case "upper":
            case "lower":
                RequireArity(name, args, 1);
                RequireString(name, args[0]);
                return new ResolvedFunctionCall(name, args, args[0].Type);
            case "length":
                RequireArity(name, args, 1);
                RequireString(name, args[0]);
                return new ResolvedFunctionCall(name, args, new SqlIntegerType(args[0].Type.Nullable));
            case "concat":
                if (args.Count < 1)
                {
                    throw new ResolveException("CONCAT requires at least one argument");
                }

                foreach (var a in args)
                {
                    RequireString(name, a);
                }

                // PG semantics: CONCAT never returns NULL. NULL args are skipped.
                return new ResolvedFunctionCall(name, args, new SqlVarcharType(null, Nullable: false));
            case "abs":
                RequireArity(name, args, 1);
                RequireNumeric(name, args[0]);
                return new ResolvedFunctionCall(name, args, args[0].Type);
            case "floor":
            case "ceil":
            case "ceiling":
                RequireArity(name, args, 1);
                RequireNumeric(name, args[0]);
                return new ResolvedFunctionCall(name, args, args[0].Type);
            case "round":
                return ResolveRound(name, args);
            case "power":
                RequireArity(name, args, 2);
                RequireNumeric(name, args[0]);
                RequireNumeric(name, args[1]);
                return new ResolvedFunctionCall(
                    name, args, new SqlDoubleType(args[0].Type.Nullable || args[1].Type.Nullable));
            case "sqrt":
                RequireArity(name, args, 1);
                RequireNumeric(name, args[0]);
                return new ResolvedFunctionCall(name, args, new SqlDoubleType(args[0].Type.Nullable));
            case "greatest":
            case "least":
                return ResolveGreatestLeast(name, args);
            case "nullif":
                return ResolveNullIf(args);
            default:
                throw new ResolveException($"unknown function '{name}'");
        }
    }

    /// <summary>
    /// Compiler entry — emit a LINQ expression that evaluates the function.
    /// <paramref name="buildArg"/> compiles an individual resolved arg
    /// (using the enclosing row parameter) so this module doesn't need to
    /// know about the row-parameter plumbing.
    /// </summary>
    public static Expression Build(
        ResolvedFunctionCall fn,
        Func<ResolvedExpression, Expression> buildArg)
    {
        var compiled = new Expression[fn.Arguments.Count];
        for (var i = 0; i < fn.Arguments.Count; i++)
        {
            compiled[i] = buildArg(fn.Arguments[i]);
        }

        return fn.FunctionName switch
        {
            "coalesce" => BuildCoalesce(compiled),
            "upper" => BuildUpperLower(compiled[0], isUpper: true),
            "lower" => BuildUpperLower(compiled[0], isUpper: false),
            "length" => BuildLength(compiled[0]),
            "concat" => BuildConcat(compiled),
            "abs" => BuildAbs(compiled[0], fn.Arguments[0].Type),
            "floor" => BuildFloorCeil(compiled[0], fn.Arguments[0].Type, floor: true),
            "ceil" or "ceiling" => BuildFloorCeil(compiled[0], fn.Arguments[0].Type, floor: false),
            "round" => BuildRound(compiled, fn.Arguments),
            "power" => BuildPower(compiled[0], compiled[1], fn.Arguments[0].Type, fn.Arguments[1].Type),
            "sqrt" => BuildSqrt(compiled[0], fn.Arguments[0].Type),
            "greatest" => BuildGreatestLeast(compiled, greatest: true),
            "least" => BuildGreatestLeast(compiled, greatest: false),
            "nullif" => BuildNullIf(compiled[0], compiled[1], fn.Arguments[0].Type),
            _ => throw new InvalidOperationException($"unknown function '{fn.FunctionName}'"),
        };
    }

    // ---------------- Resolver helpers ----------------

    private static void RequireArity(string name, IReadOnlyList<ResolvedExpression> args, int expected)
    {
        if (args.Count != expected)
        {
            throw new ResolveException(
                $"{name.ToUpperInvariant()} takes exactly {expected} argument{(expected == 1 ? "" : "s")}");
        }
    }

    private static void RequireString(string name, ResolvedExpression arg)
    {
        if (arg.Type is not SqlVarcharType)
        {
            throw new ResolveException($"{name.ToUpperInvariant()} requires a VARCHAR argument");
        }
    }

    private static void RequireNumeric(string name, ResolvedExpression arg)
    {
        if (!TypeInference.IsNumeric(arg.Type))
        {
            throw new ResolveException($"{name.ToUpperInvariant()} requires a numeric argument");
        }
    }

    private static ResolvedFunctionCall ResolveCoalesce(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count < 1)
        {
            throw new ResolveException("COALESCE requires at least one argument");
        }

        var common = args[0].Type;
        for (var i = 1; i < args.Count; i++)
        {
            common = TypeInference.CommonComparableType(common, args[i].Type);
        }

        // COALESCE is NOT NULL iff any argument is NOT NULL.
        var resultNullable = true;
        foreach (var a in args)
        {
            if (!a.Type.Nullable)
            {
                resultNullable = false;
                break;
            }
        }

        common = common.WithNullable(resultNullable);
        var cast = new List<ResolvedExpression>(args.Count);
        foreach (var a in args)
        {
            cast.Add(MaybeCast(a, common));
        }

        return new ResolvedFunctionCall("coalesce", cast, common);
    }

    private static ResolvedFunctionCall ResolveRound(string name, IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count is < 1 or > 2)
        {
            throw new ResolveException("ROUND takes 1 or 2 arguments");
        }

        RequireNumeric(name, args[0]);
        if (args.Count == 2 && !(args[1].Type is SqlIntegerType or SqlBigintType))
        {
            throw new ResolveException("ROUND second argument must be an integer");
        }

        return new ResolvedFunctionCall(name, args, args[0].Type);
    }

    private static ResolvedFunctionCall ResolveGreatestLeast(string name, IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count < 1)
        {
            throw new ResolveException($"{name.ToUpperInvariant()} requires at least one argument");
        }

        var common = args[0].Type;
        for (var i = 1; i < args.Count; i++)
        {
            common = TypeInference.CommonComparableType(common, args[i].Type);
        }

        // All-NULL input → NULL output, so result is always nullable.
        var resultType = common.WithNullable(true);
        var cast = new List<ResolvedExpression>(args.Count);
        foreach (var a in args)
        {
            cast.Add(MaybeCast(a, resultType));
        }

        return new ResolvedFunctionCall(name, cast, resultType);
    }

    private static ResolvedFunctionCall ResolveNullIf(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("nullif", args, 2);
        var common = TypeInference.CommonComparableType(args[0].Type, args[1].Type);
        var cast = new List<ResolvedExpression>(2)
        {
            MaybeCast(args[0], common),
            MaybeCast(args[1], common),
        };

        // Result is either the first arg (type X) or NULL, so always nullable.
        return new ResolvedFunctionCall("nullif", cast, args[0].Type.WithNullable(true));
    }

    private static ResolvedExpression MaybeCast(ResolvedExpression e, SqlType target)
    {
        if (SameTypeIgnoringNullable(e.Type, target))
        {
            return e;
        }

        return new ResolvedCast(e, target);
    }

    private static bool SameTypeIgnoringNullable(SqlType a, SqlType b) =>
        a.WithNullable(false).Equals(b.WithNullable(false));

    // ---------------- Compiler helpers ----------------

    private static Expression BuildCoalesce(Expression[] args)
    {
        // Right-to-left fold: COALESCE(a, b, c) ≡ a ?? (b ?? c).
        Expression result = NullObject;
        for (var i = args.Length - 1; i >= 0; i--)
        {
            var arg = args[i];
            var isNull = Expression.Equal(arg, NullObject);
            result = Expression.Condition(isNull, result, arg);
        }

        return result;
    }

    private static readonly MethodInfo StringToUpperInvariant =
        typeof(string).GetMethod(nameof(string.ToUpperInvariant), Type.EmptyTypes)!;
    private static readonly MethodInfo StringToLowerInvariant =
        typeof(string).GetMethod(nameof(string.ToLowerInvariant), Type.EmptyTypes)!;

    private static Expression BuildUpperLower(Expression arg, bool isUpper)
    {
        return NullPropagatingUnary(arg, typeof(string), s =>
            Expression.Call(s, isUpper ? StringToUpperInvariant : StringToLowerInvariant));
    }

    private static readonly PropertyInfo StringLength =
        typeof(string).GetProperty(nameof(string.Length))!;

    private static Expression BuildLength(Expression arg)
    {
        return NullPropagatingUnary(arg, typeof(string),
            s => Expression.Property(s, StringLength));
    }

    private static readonly MethodInfo ConcatRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Concat))!;

    private static Expression BuildConcat(Expression[] args)
    {
        // PG-style CONCAT: skip NULL args, concatenate the rest. Dispatched
        // at runtime since arity is variable.
        var argArray = Expression.NewArrayInit(typeof(object), args);
        return Expression.Convert(Expression.Call(ConcatRuntime, argArray), typeof(object));
    }

    private static Expression BuildAbs(Expression arg, SqlType argType)
    {
        var clr = ClrOf(argType);
        var method = typeof(Math).GetMethod(nameof(Math.Abs), new[] { clr })
            ?? throw new InvalidOperationException($"no Math.Abs({clr.Name}) overload");
        return NullPropagatingUnary(arg, clr, unboxed => Expression.Call(method, unboxed));
    }

    private static Expression BuildFloorCeil(Expression arg, SqlType argType, bool floor)
    {
        // Integer types: no-op (FLOOR(5) = 5, CEIL(5) = 5). Float / double /
        // decimal get dispatched to a runtime helper to hide the per-type
        // overload juggling.
        if (argType is SqlIntegerType or SqlBigintType)
        {
            return arg;
        }

        var clr = ClrOf(argType);
        var methodName = floor ? nameof(SqlBuiltinRuntime.Floor) : nameof(SqlBuiltinRuntime.Ceil);
        var method = typeof(SqlBuiltinRuntime).GetMethod(methodName, new[] { clr })
            ?? throw new InvalidOperationException($"no runtime {methodName} for {clr.Name}");
        return NullPropagatingUnary(arg, clr, unboxed => Expression.Call(method, unboxed));
    }

    private static Expression BuildRound(Expression[] args, IReadOnlyList<ResolvedExpression> argTypes)
    {
        var argType = argTypes[0].Type;
        if (argType is SqlIntegerType or SqlBigintType)
        {
            return args[0];
        }

        var clr = ClrOf(argType);
        var method = typeof(SqlBuiltinRuntime).GetMethod(
            nameof(SqlBuiltinRuntime.Round), new[] { clr, typeof(int) })
            ?? throw new InvalidOperationException($"no runtime Round for {clr.Name}");

        // digits: default 0 if absent; else unbox second arg to int (with NULL propagation).
        if (args.Length == 1)
        {
            return NullPropagatingUnary(args[0], clr, unboxed =>
                Expression.Call(method, unboxed, Expression.Constant(0)));
        }

        var isNull = Expression.OrElse(
            Expression.Equal(args[0], NullObject),
            Expression.Equal(args[1], NullObject));
        var unboxedX = Expression.Convert(args[0], clr);
        var digitsClr = ClrOf(argTypes[1].Type);
        var digitsAsInt = digitsClr == typeof(int)
            ? Expression.Convert(args[1], typeof(int))
            : Expression.Convert(Expression.Convert(args[1], digitsClr), typeof(int));
        var call = Expression.Call(method, unboxedX, digitsAsInt);
        return Expression.Condition(isNull, NullObject, Expression.Convert(call, typeof(object)));
    }

    private static Expression BuildPower(Expression x, Expression y, SqlType xType, SqlType yType)
    {
        // Both operands widen to double; Math.Pow returns double.
        var isNull = Expression.OrElse(
            Expression.Equal(x, NullObject),
            Expression.Equal(y, NullObject));
        var xd = Expression.Convert(Expression.Convert(x, ClrOf(xType)), typeof(double));
        var yd = Expression.Convert(Expression.Convert(y, ClrOf(yType)), typeof(double));
        var call = Expression.Call(typeof(Math).GetMethod(nameof(Math.Pow))!, xd, yd);
        return Expression.Condition(isNull, NullObject, Expression.Convert(call, typeof(object)));
    }

    private static Expression BuildSqrt(Expression arg, SqlType argType)
    {
        return NullPropagatingUnary(arg, typeof(double), unboxed =>
            Expression.Call(typeof(Math).GetMethod(nameof(Math.Sqrt))!, unboxed),
            // Non-double CLR types need a widening conversion first.
            intermediateClr: ClrOf(argType) == typeof(double) ? null : ClrOf(argType));
    }

    private static readonly MethodInfo GreatestRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Greatest))!;
    private static readonly MethodInfo LeastRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Least))!;

    private static Expression BuildGreatestLeast(Expression[] args, bool greatest)
    {
        var argArray = Expression.NewArrayInit(typeof(object), args);
        return Expression.Call(greatest ? GreatestRuntime : LeastRuntime, argArray);
    }

    private static Expression BuildNullIf(Expression x, Expression y, SqlType argType)
    {
        // Semantics (matches PG):
        //   x IS NULL            → NULL
        //   y IS NULL            → x
        //   x = y (definite eq)  → NULL
        //   else                 → x
        var clr = ClrOf(argType);
        var xIsNull = Expression.Equal(x, NullObject);
        var yIsNull = Expression.Equal(y, NullObject);

        var xUnboxed = Expression.Convert(x, clr);
        var yUnboxed = Expression.Convert(y, clr);

        Expression equal;
        if (clr == typeof(string))
        {
            var cmp = Expression.Call(
                typeof(string).GetMethod(nameof(string.CompareOrdinal), new[] { typeof(string), typeof(string) })!,
                xUnboxed, yUnboxed);
            equal = Expression.Equal(cmp, Expression.Constant(0));
        }
        else
        {
            equal = Expression.Equal(xUnboxed, yUnboxed);
        }

        var inner = Expression.Condition(equal, NullObject, x);
        var yIsNullBranch = Expression.Condition(yIsNull, x, inner);
        return Expression.Condition(xIsNull, NullObject, yIsNullBranch);
    }

    // ---------------- Misc helpers ----------------

    /// <summary>
    /// Build <c>operand == null ? null : (object)f((clr)operand)</c>.
    /// </summary>
    private static Expression NullPropagatingUnary(
        Expression operand,
        Type clr,
        Func<Expression, Expression> f,
        Type? intermediateClr = null)
    {
        var isNull = Expression.Equal(operand, NullObject);
        Expression unboxed = Expression.Convert(operand, intermediateClr ?? clr);
        if (intermediateClr is not null && intermediateClr != clr)
        {
            unboxed = Expression.Convert(unboxed, clr);
        }

        var result = f(unboxed);
        return Expression.Condition(isNull, NullObject, Expression.Convert(result, typeof(object)));
    }

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
}

/// <summary>
/// Runtime helpers invoked from expression trees built by
/// <see cref="BuiltinScalarFunctions"/>. Kept as plain static methods so
/// <c>Expression.Call</c> can reach them by overload resolution on arg types.
/// </summary>
internal static class SqlBuiltinRuntime
{
    // ---- String ----

    /// <summary>PG-style CONCAT: NULL args are skipped; result is never NULL.</summary>
    public static string Concat(object?[] args)
    {
        var sb = new StringBuilder();
        foreach (var a in args)
        {
            if (a is null)
            {
                continue;
            }

            sb.Append((string)a);
        }

        return sb.ToString();
    }

    // ---- FLOOR / CEIL / ROUND per-type ----

    public static decimal Floor(decimal x) => Math.Floor(x);
    public static double Floor(double x) => Math.Floor(x);
    public static float Floor(float x) => (float)Math.Floor((double)x);

    public static decimal Ceil(decimal x) => Math.Ceiling(x);
    public static double Ceil(double x) => Math.Ceiling(x);
    public static float Ceil(float x) => (float)Math.Ceiling((double)x);

    public static decimal Round(decimal x, int digits) => Math.Round(x, digits);
    public static double Round(double x, int digits) => Math.Round(x, digits);
    public static float Round(float x, int digits) => (float)Math.Round((double)x, digits);

    // ---- GREATEST / LEAST (PG semantics: NULLs skipped, all-NULL → NULL) ----

    public static object? Greatest(object?[] args)
    {
        object? best = null;
        foreach (var a in args)
        {
            if (a is null)
            {
                continue;
            }

            if (best is null || ((IComparable)a).CompareTo(best) > 0)
            {
                best = a;
            }
        }

        return best;
    }

    public static object? Least(object?[] args)
    {
        object? best = null;
        foreach (var a in args)
        {
            if (a is null)
            {
                continue;
            }

            if (best is null || ((IComparable)a).CompareTo(best) < 0)
            {
                best = a;
            }
        }

        return best;
    }

    // Dummy reference so CultureInfo import isn't flagged as unused even when
    // this file's Math.Round / Math.Floor overloads don't directly need it.
    // Left in place for when future number parsing helpers move here.
    internal static CultureInfo Invariant => CultureInfo.InvariantCulture;
}
