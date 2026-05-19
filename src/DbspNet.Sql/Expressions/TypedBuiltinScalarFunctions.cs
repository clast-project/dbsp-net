using System.Linq.Expressions;
using System.Reflection;
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Typed-row counterpart to <see cref="BuiltinScalarFunctions"/>.
/// Lowers a <see cref="ResolvedFunctionCall"/> against pre-compiled
/// typed argument expressions, producing an expression whose CLR
/// type matches the function's result. No boxing, no
/// <c>object</c>-typed intermediate values; NULL-propagation logic
/// collapses since every operand is non-null on the typed path.
/// </summary>
/// <remarks>
/// <para><b>NULL handling.</b> Functions that produce NULL by design
/// (currently just <c>NULLIF</c>) are rejected — the typed-row
/// pipeline can't carry nullable result columns. <c>COALESCE</c> with
/// all non-null inputs collapses to its first argument.</para>
/// <para>The structural counterpart's <c>NullPropagatingUnary</c>
/// wrapper is replaced here by a direct call: <c>f(operand)</c>
/// without the <c>operand == null ?</c> branch.</para>
/// </remarks>
internal static class TypedBuiltinScalarFunctions
{
    /// <summary>
    /// Lower <paramref name="fn"/> with already-compiled typed
    /// argument expressions <paramref name="args"/>. Returns the
    /// result expression or <c>null</c> if the function is outside
    /// the typed pipeline's scope.
    /// </summary>
    public static Expression? TryBuild(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] args)
    {
        return fn.FunctionName switch
        {
            "coalesce" => BuildCoalesce(args),
            "upper" => BuildUpperLower(args[0], isUpper: true),
            "lower" => BuildUpperLower(args[0], isUpper: false),
            "length" => BuildLength(args[0]),
            "concat" => BuildConcat(args),
            "abs" => BuildAbs(args[0], astArgs[0].Type),
            "floor" => BuildFloorCeil(args[0], astArgs[0].Type, floor: true),
            "ceil" or "ceiling" => BuildFloorCeil(args[0], astArgs[0].Type, floor: false),
            "round" => BuildRound(args, astArgs),
            "power" => BuildPower(args[0], args[1]),
            "sqrt" => BuildSqrt(args[0]),
            "greatest" => BuildGreatestLeast(args, greatest: true),
            "least" => BuildGreatestLeast(args, greatest: false),
            // NULLIF can return NULL on a definite-equal match, which
            // the typed-row pipeline can't carry. Always falls back to
            // the structural compile.
            "nullif" => null,
            _ => null,
        };
    }

    // ---- COALESCE ----

    private static Expression BuildCoalesce(Expression[] args)
    {
        // On the typed path every value is non-null. COALESCE's
        // semantics ("first non-null arg") therefore reduce to "first
        // arg". The resolver has already cast every arg to the common
        // result type, so a direct return is well-typed.
        return args[0];
    }

    // ---- String ----

    private static readonly MethodInfo Utf8ToUpperInvariant =
        typeof(Utf8String).GetMethod(nameof(Utf8String.ToUpperInvariant))!;
    private static readonly MethodInfo Utf8ToLowerInvariant =
        typeof(Utf8String).GetMethod(nameof(Utf8String.ToLowerInvariant))!;
    private static readonly MethodInfo Utf8CodePointCount =
        typeof(Utf8String).GetMethod(nameof(Utf8String.CodePointCount))!;

    private static Expression BuildUpperLower(Expression arg, bool isUpper)
    {
        return Expression.Call(arg, isUpper ? Utf8ToUpperInvariant : Utf8ToLowerInvariant);
    }

    private static Expression BuildLength(Expression arg)
    {
        // PG semantics: LENGTH = number of code points.
        return Expression.Call(arg, Utf8CodePointCount);
    }

    private static readonly MethodInfo ConcatTyped =
        typeof(TypedBuiltinRuntime).GetMethod(nameof(TypedBuiltinRuntime.ConcatTyped))!;

    private static Expression BuildConcat(Expression[] args)
    {
        // Variadic — package as a typed Utf8String[] and dispatch to
        // the typed runtime helper. (No null-skip needed: every arg
        // on the typed path is a definite Utf8String.)
        var argArray = Expression.NewArrayInit(typeof(Utf8String), args);
        return Expression.Call(ConcatTyped, argArray);
    }

    // ---- Numeric ----

    private static Expression BuildAbs(Expression arg, SqlType argType)
    {
        if (argType is SqlDecimalType)
        {
            return Expression.Call(
                typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Abs))!,
                arg);
        }

        var clr = argType.ClrType;
        var method = typeof(Math).GetMethod(nameof(Math.Abs), new[] { clr })
            ?? throw new InvalidOperationException($"no Math.Abs({clr.Name}) overload");
        return Expression.Call(method, arg);
    }

    private static Expression BuildFloorCeil(Expression arg, SqlType argType, bool floor)
    {
        if (argType is SqlIntegerType or SqlBigintType)
        {
            return arg;
        }

        if (argType is SqlDecimalType d)
        {
            var name = floor ? nameof(DecimalRuntime.Floor) : nameof(DecimalRuntime.Ceil);
            return Expression.Call(
                typeof(DecimalRuntime).GetMethod(name)!,
                arg, Expression.Constant((byte)d.Scale));
        }

        var clr = argType.ClrType;
        var runtimeName = floor ? nameof(SqlBuiltinRuntime.Floor) : nameof(SqlBuiltinRuntime.Ceil);
        var method = typeof(SqlBuiltinRuntime).GetMethod(runtimeName, new[] { clr })
            ?? throw new InvalidOperationException($"no runtime {runtimeName} for {clr.Name}");
        return Expression.Call(method, arg);
    }

    private static Expression BuildRound(Expression[] args, IReadOnlyList<ResolvedExpression> astArgs)
    {
        var argType = astArgs[0].Type;
        if (argType is SqlIntegerType or SqlBigintType)
        {
            return args[0];
        }

        if (argType is SqlDecimalType decType)
        {
            var method = typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Round))!;
            var scale = Expression.Constant((byte)decType.Scale);
            if (args.Length == 1)
            {
                return Expression.Call(method, args[0], scale, Expression.Constant(0));
            }

            var digits = WidenToInt(args[1]);
            return Expression.Call(method, args[0], scale, digits);
        }

        var clr = argType.ClrType;
        var runtimeMethod = typeof(SqlBuiltinRuntime).GetMethod(
            nameof(SqlBuiltinRuntime.Round), new[] { clr, typeof(int) })
            ?? throw new InvalidOperationException($"no runtime Round for {clr.Name}");

        if (args.Length == 1)
        {
            return Expression.Call(runtimeMethod, args[0], Expression.Constant(0));
        }

        return Expression.Call(runtimeMethod, args[0], WidenToInt(args[1]));
    }

    private static Expression BuildPower(Expression x, Expression y)
    {
        // Both widen to double; Math.Pow returns double.
        var xd = x.Type == typeof(double) ? x : Expression.Convert(x, typeof(double));
        var yd = y.Type == typeof(double) ? y : Expression.Convert(y, typeof(double));
        return Expression.Call(typeof(Math).GetMethod(nameof(Math.Pow))!, xd, yd);
    }

    private static Expression BuildSqrt(Expression arg)
    {
        var x = arg.Type == typeof(double) ? arg : Expression.Convert(arg, typeof(double));
        return Expression.Call(typeof(Math).GetMethod(nameof(Math.Sqrt))!, x);
    }

    // ---- GREATEST / LEAST ----

    private static Expression BuildGreatestLeast(Expression[] args, bool greatest)
    {
        // Resolver casts every arg to a common comparable type.
        // Reflect the element type from the first arg and dispatch
        // to the typed runtime helper.
        var elementType = args[0].Type;
        var helperName = greatest ? nameof(TypedBuiltinRuntime.Greatest) : nameof(TypedBuiltinRuntime.Least);
        var openHelper = typeof(TypedBuiltinRuntime).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == helperName && m.IsGenericMethodDefinition);
        var closedHelper = openHelper.MakeGenericMethod(elementType);
        var argArray = Expression.NewArrayInit(elementType, args);
        return Expression.Call(closedHelper, argArray);
    }

    // ---- Helpers ----

    private static Expression WidenToInt(Expression e)
    {
        if (e.Type == typeof(int)) return e;
        return Expression.Convert(e, typeof(int));
    }
}

/// <summary>
/// Runtime helpers invoked from typed expression trees. Counterpart
/// to <see cref="SqlBuiltinRuntime"/> for the typed pipeline:
/// arguments are typed (not boxed), and there's no NULL-skip
/// bookkeeping.
/// </summary>
internal static class TypedBuiltinRuntime
{
    /// <summary>
    /// Variadic CONCAT over <see cref="Utf8String"/> arguments. No
    /// NULL skip — every arg is a definite Utf8String on the typed
    /// path. Single allocating pass: sum byte lengths, then copy.
    /// </summary>
    public static Utf8String ConcatTyped(Utf8String[] args)
    {
        var totalBytes = 0;
        foreach (var u in args)
        {
            totalBytes += u.ByteLength;
        }

        if (totalBytes == 0)
        {
            return Utf8String.Empty;
        }

        var buf = new byte[totalBytes];
        var offset = 0;
        foreach (var u in args)
        {
            u.Span.CopyTo(buf.AsSpan(offset));
            offset += u.ByteLength;
        }

        return Utf8String.FromBytes(buf);
    }

    /// <summary>
    /// Typed GREATEST over <typeparamref name="T"/> values, where T
    /// is <see cref="IComparable{T}"/>. No NULL skip — every arg is
    /// definite on the typed path. Resolver guarantees at least one
    /// arg.
    /// </summary>
    public static T Greatest<T>(T[] args) where T : IComparable<T>
    {
        var best = args[0];
        for (var i = 1; i < args.Length; i++)
        {
            if (args[i].CompareTo(best) > 0) best = args[i];
        }

        return best;
    }

    /// <summary>Typed LEAST. See <see cref="Greatest{T}"/>.</summary>
    public static T Least<T>(T[] args) where T : IComparable<T>
    {
        var best = args[0];
        for (var i = 1; i < args.Length; i++)
        {
            if (args[i].CompareTo(best) < 0) best = args[i];
        }

        return best;
    }
}
