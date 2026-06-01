// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq.Expressions;
using System.Reflection;
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Typed-row counterpart to <see cref="BuiltinScalarFunctions"/>: the typed
/// (fast-path) lowering for each builtin, against pre-compiled typed argument
/// expressions, producing an expression whose CLR type matches the function's
/// result. No boxing, no <c>object</c>-typed intermediate values. Dispatch
/// lives in <see cref="ScalarFunctionRegistry"/>; each registry entry's
/// <c>BuildTyped</c> delegates to a <c>BuildXxx</c> here (or returns null to
/// fall the query back to the structural pipeline).
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
    /// LN / EXP on the typed path — chosen by <paramref name="exp"/>. The
    /// registry entry delegates here (the multi-arg string functions return
    /// null from their own entries to fall back to the structural pipeline).
    /// </summary>
    internal static Expression BuildLnExp(Expression arg, bool exp) =>
        BuildUnaryDouble(arg, exp ? MathExp : MathLog);

    // ---- Numeric (sign / ln / log / exp) ----
    //
    // The resolver casts every operand to DOUBLE, so on the typed path the
    // argument expression is already double / double?; no per-type dispatch.

    internal static readonly MethodInfo MathSignDouble =
        typeof(Math).GetMethod(nameof(Math.Sign), new[] { typeof(double) })!;
    internal static readonly MethodInfo MathLog =
        typeof(Math).GetMethod(nameof(Math.Log), new[] { typeof(double) })!;
    internal static readonly MethodInfo MathLog2 =
        typeof(Math).GetMethod(nameof(Math.Log), new[] { typeof(double), typeof(double) })!;
    internal static readonly MethodInfo MathLog10 =
        typeof(Math).GetMethod(nameof(Math.Log10), new[] { typeof(double) })!;
    internal static readonly MethodInfo MathExp =
        typeof(Math).GetMethod(nameof(Math.Exp), new[] { typeof(double) })!;

    internal static Expression BuildSign(Expression arg) =>
        TypedExpressionCompiler.PropagateUnary(arg, v => Expression.Call(MathSignDouble, v));

    internal static Expression BuildUnaryDouble(Expression arg, MethodInfo math) =>
        TypedExpressionCompiler.PropagateUnary(arg, v => Expression.Call(math, v));

    internal static Expression BuildLog(Expression[] args)
    {
        if (args.Length == 1)
        {
            return TypedExpressionCompiler.PropagateUnary(args[0], v => Expression.Call(MathLog10, v));
        }

        // LOG(b, x) = log_b(x) = Math.Log(x, b).
        return TypedExpressionCompiler.PropagateBinary(
            args[0], args[1], (b, x) => Expression.Call(MathLog2, x, b));
    }

    /// <summary>
    /// NULLIF(x, y): returns NULL when x is non-null and equals y;
    /// otherwise returns x. Result is always nullable. Unblocked by
    /// Phase N2's NULL-aware typed expression compiler.
    /// </summary>
    internal static Expression BuildNullIf(Expression x, Expression y)
    {
        // Lift both args to nullable to share a single result type.
        var xN = TypedExpressionCompiler.IsNullable(x.Type) || !x.Type.IsValueType
            ? x : TypedExpressionCompiler.LiftToNullable(x);
        var yN = TypedExpressionCompiler.IsNullable(y.Type) || !y.Type.IsValueType
            ? y : TypedExpressionCompiler.LiftToNullable(y);

        var resultType = xN.Type;
        var nullConst = Expression.Constant(null, resultType);

        // Stash into locals so we don't re-evaluate.
        var xLocal = Expression.Variable(xN.Type, "x");
        var yLocal = Expression.Variable(yN.Type, "y");

        // Compute equality at the underlying type. If either is null,
        // not-equal (NULLIF returns x). If both non-null and equal,
        // return null.
        Expression xVal = TypedExpressionCompiler.IsNullable(xN.Type)
            ? Expression.Property(xLocal, nameof(Nullable<int>.Value))
            : xLocal;
        Expression yVal = TypedExpressionCompiler.IsNullable(yN.Type)
            ? Expression.Property(yLocal, nameof(Nullable<int>.Value))
            : yLocal;
        var equal = Expression.Equal(xVal, yVal);

        // bothHaveValue: x is non-null and y is non-null.
        Expression xHasValue = TypedExpressionCompiler.IsNullable(xN.Type)
            ? Expression.Property(xLocal, nameof(Nullable<int>.HasValue))
            : (Expression)Expression.Constant(true);
        Expression yHasValue = TypedExpressionCompiler.IsNullable(yN.Type)
            ? Expression.Property(yLocal, nameof(Nullable<int>.HasValue))
            : (Expression)Expression.Constant(true);
        var bothHaveValue = Expression.AndAlso(xHasValue, yHasValue);

        // If both non-null AND equal → NULL; else → x.
        return Expression.Block(
            new[] { xLocal, yLocal },
            Expression.Assign(xLocal, xN),
            Expression.Assign(yLocal, yN),
            Expression.Condition(
                Expression.AndAlso(bothHaveValue, equal),
                nullConst,
                xLocal));
    }

    // ---- COALESCE ----

    internal static Expression BuildCoalesce(Expression[] args)
    {
        // Right-to-left fold: COALESCE(a, b, c) ≡ a ?? (b ?? c). For
        // non-null args, collapses to args[0]. For nullable args,
        // emits a HasValue check on each and falls through to the
        // next on null. Returns Nullable<T> only if every arg is
        // nullable; if any arg is non-null, the result is that
        // arg's underlying type T (which is what the resolver tagged
        // the COALESCE result, since COALESCE is non-null when any
        // arg is non-null).
        Expression result = args[args.Length - 1];
        for (var i = args.Length - 2; i >= 0; i--)
        {
            result = CoalesceTwo(args[i], result);
        }

        return result;
    }

    /// <summary>
    /// Builds <c>x ?? fallback</c>. If x is non-nullable, returns x.
    /// If x is nullable, returns
    /// <c>x.HasValue ? x.Value : fallback</c> (or
    /// <c>x.HasValue ? x : fallback</c> when fallback is nullable
    /// and we want to keep the nullable wrapper).
    /// </summary>
    internal static Expression CoalesceTwo(Expression x, Expression fallback)
    {
        if (!TypedExpressionCompiler.IsNullable(x.Type))
        {
            // x is non-null → COALESCE short-circuits to x.
            return x;
        }

        var underlying = TypedExpressionCompiler.UnderlyingType(x.Type);
        var fallbackIsNullable = TypedExpressionCompiler.IsNullable(fallback.Type);
        var resultType = fallbackIsNullable || !fallback.Type.IsValueType
            ? fallback.Type
            : underlying;

        var local = Expression.Variable(x.Type, "x");
        Expression xResult = resultType == x.Type
            ? local
            : Expression.Property(local, nameof(Nullable<int>.Value));
        Expression fallbackResult = fallback.Type == resultType
            ? fallback
            : Expression.Convert(fallback, resultType);

        return Expression.Block(
            new[] { local },
            Expression.Assign(local, x),
            Expression.Condition(
                Expression.Property(local, nameof(Nullable<int>.HasValue)),
                xResult,
                fallbackResult));
    }

    // ---- String ----

    internal static readonly MethodInfo Utf8ToUpperInvariant =
        typeof(Utf8String).GetMethod(nameof(Utf8String.ToUpperInvariant))!;
    internal static readonly MethodInfo Utf8ToLowerInvariant =
        typeof(Utf8String).GetMethod(nameof(Utf8String.ToLowerInvariant))!;
    internal static readonly MethodInfo Utf8CodePointCount =
        typeof(Utf8String).GetMethod(nameof(Utf8String.CodePointCount))!;

    internal static Expression BuildUpperLower(Expression arg, bool isUpper)
    {
        return TypedExpressionCompiler.PropagateUnary(arg, v =>
            Expression.Call(v, isUpper ? Utf8ToUpperInvariant : Utf8ToLowerInvariant));
    }

    internal static Expression BuildLength(Expression arg)
    {
        // PG semantics: LENGTH = number of code points.
        return TypedExpressionCompiler.PropagateUnary(arg, v =>
            Expression.Call(v, Utf8CodePointCount));
    }

    internal static readonly MethodInfo ConcatTyped =
        typeof(TypedBuiltinRuntime).GetMethod(nameof(TypedBuiltinRuntime.ConcatTyped))!;
    internal static readonly MethodInfo ConcatTypedNullable =
        typeof(TypedBuiltinRuntime).GetMethod(nameof(TypedBuiltinRuntime.ConcatTypedNullable))!;
    internal static readonly MethodInfo ConcatStrictTypedNullable =
        typeof(TypedBuiltinRuntime).GetMethod(nameof(TypedBuiltinRuntime.ConcatStrictTypedNullable))!;

    internal static Expression BuildConcat(Expression[] args)
    {
        // PG semantics: CONCAT skips NULL args, never returns NULL.
        // Fast path (all args non-null): pack as Utf8String[].
        // Slow path (any nullable): pack as Utf8String?[] and route
        // to the null-skipping helper. Resolver always types CONCAT
        // result as non-null.
        var anyNullable = false;
        foreach (var a in args)
        {
            if (TypedExpressionCompiler.IsNullable(a.Type)) { anyNullable = true; break; }
        }

        if (!anyNullable)
        {
            var argArray = Expression.NewArrayInit(typeof(Utf8String), args);
            return Expression.Call(ConcatTyped, argArray);
        }

        var lifted = new Expression[args.Length];
        for (var i = 0; i < args.Length; i++)
        {
            lifted[i] = TypedExpressionCompiler.IsNullable(args[i].Type)
                ? args[i]
                : Expression.Convert(args[i], typeof(Utf8String?));
        }

        var nullableArray = Expression.NewArrayInit(typeof(Utf8String?), lifted);
        return Expression.Call(ConcatTypedNullable, nullableArray);
    }

    internal static Expression BuildConcatStrict(Expression[] args)
    {
        // The `||` operator: NULL-propagating. All-non-null collapses to the
        // plain concat (non-null Utf8String). If any arg is nullable, route to
        // the propagating helper, which returns Utf8String? (null if any null).
        var anyNullable = false;
        foreach (var a in args)
        {
            if (TypedExpressionCompiler.IsNullable(a.Type)) { anyNullable = true; break; }
        }

        if (!anyNullable)
        {
            var argArray = Expression.NewArrayInit(typeof(Utf8String), args);
            return Expression.Call(ConcatTyped, argArray);
        }

        var lifted = new Expression[args.Length];
        for (var i = 0; i < args.Length; i++)
        {
            lifted[i] = TypedExpressionCompiler.IsNullable(args[i].Type)
                ? args[i]
                : Expression.Convert(args[i], typeof(Utf8String?));
        }

        var nullableArray = Expression.NewArrayInit(typeof(Utf8String?), lifted);
        return Expression.Call(ConcatStrictTypedNullable, nullableArray);
    }

    // ---- Numeric ----

    internal static Expression BuildAbs(Expression arg, SqlType argType)
    {
        if (argType is SqlDecimalType)
        {
            var method = typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Abs))!;
            return TypedExpressionCompiler.PropagateUnary(arg, v => Expression.Call(method, v));
        }

        var clr = argType.ClrType;
        var mathMethod = typeof(Math).GetMethod(nameof(Math.Abs), new[] { clr })
            ?? throw new InvalidOperationException($"no Math.Abs({clr.Name}) overload");
        return TypedExpressionCompiler.PropagateUnary(arg, v => Expression.Call(mathMethod, v));
    }

    internal static Expression BuildFloorCeil(Expression arg, SqlType argType, bool floor)
    {
        if (argType is SqlIntegerType or SqlBigintType)
        {
            // FLOOR(int) = int; nullable input → nullable output, the
            // arg expression already has the right type.
            return arg;
        }

        if (argType is SqlDecimalType d)
        {
            var name = floor ? nameof(DecimalRuntime.Floor) : nameof(DecimalRuntime.Ceil);
            var method = typeof(DecimalRuntime).GetMethod(name)!;
            return TypedExpressionCompiler.PropagateUnary(arg, v =>
                Expression.Call(method, v, Expression.Constant((byte)d.Scale)));
        }

        var clr = argType.ClrType;
        var runtimeName = floor ? nameof(SqlBuiltinRuntime.Floor) : nameof(SqlBuiltinRuntime.Ceil);
        var runtimeMethod = typeof(SqlBuiltinRuntime).GetMethod(runtimeName, new[] { clr })
            ?? throw new InvalidOperationException($"no runtime {runtimeName} for {clr.Name}");
        return TypedExpressionCompiler.PropagateUnary(arg, v => Expression.Call(runtimeMethod, v));
    }

    internal static Expression BuildRound(Expression[] args, IReadOnlyList<ResolvedExpression> astArgs)
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
                return TypedExpressionCompiler.PropagateUnary(args[0], v =>
                    Expression.Call(method, v, scale, Expression.Constant(0)));
            }

            // Two-arg: NULL on either propagates.
            return TypedExpressionCompiler.PropagateBinary(args[0], args[1], (xv, dv) =>
            {
                var digits = dv.Type == typeof(int) ? dv : Expression.Convert(dv, typeof(int));
                return Expression.Call(method, xv, scale, digits);
            });
        }

        var clr = argType.ClrType;
        var runtimeMethod = typeof(SqlBuiltinRuntime).GetMethod(
            nameof(SqlBuiltinRuntime.Round), new[] { clr, typeof(int) })
            ?? throw new InvalidOperationException($"no runtime Round for {clr.Name}");

        if (args.Length == 1)
        {
            return TypedExpressionCompiler.PropagateUnary(args[0], v =>
                Expression.Call(runtimeMethod, v, Expression.Constant(0)));
        }

        return TypedExpressionCompiler.PropagateBinary(args[0], args[1], (xv, dv) =>
        {
            var digits = dv.Type == typeof(int) ? dv : Expression.Convert(dv, typeof(int));
            return Expression.Call(runtimeMethod, xv, digits);
        });
    }

    internal static Expression BuildPower(Expression x, Expression y)
    {
        // Both widen to double inside the propagate closure; result
        // is Nullable<double> iff either operand is nullable.
        return TypedExpressionCompiler.PropagateBinary(x, y, (xv, yv) =>
        {
            var xd = xv.Type == typeof(double) ? xv : Expression.Convert(xv, typeof(double));
            var yd = yv.Type == typeof(double) ? yv : Expression.Convert(yv, typeof(double));
            return Expression.Call(typeof(Math).GetMethod(nameof(Math.Pow))!, xd, yd);
        });
    }

    internal static Expression BuildSqrt(Expression arg)
    {
        return TypedExpressionCompiler.PropagateUnary(arg, v =>
        {
            var x = v.Type == typeof(double) ? v : Expression.Convert(v, typeof(double));
            return Expression.Call(typeof(Math).GetMethod(nameof(Math.Sqrt))!, x);
        });
    }

    // ---- GREATEST / LEAST ----

    internal static Expression? BuildGreatestLeast(Expression[] args, bool greatest)
    {
        // Resolver casts every arg to a common comparable type and
        // marks the result nullable iff any arg is nullable (all-NULL
        // group → NULL). If any arg is nullable, route to the
        // skip-null Nullable<T> variant; otherwise stay on the
        // non-null fast path.
        var anyNullable = false;
        foreach (var a in args)
        {
            if (TypedExpressionCompiler.IsNullable(a.Type)) { anyNullable = true; break; }
        }

        if (!anyNullable)
        {
            var elementType = args[0].Type;
            var helperName = greatest
                ? nameof(TypedBuiltinRuntime.Greatest)
                : nameof(TypedBuiltinRuntime.Least);
            var openHelper = typeof(TypedBuiltinRuntime).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m => m.Name == helperName && m.IsGenericMethodDefinition && m.GetParameters().Length == 1);
            var closedHelper = openHelper.MakeGenericMethod(elementType);
            var argArray = Expression.NewArrayInit(elementType, args);
            return Expression.Call(closedHelper, argArray);
        }

        // Lift every arg to Nullable<T> (resolver guarantees a
        // common underlying T; T must be a value type for the
        // nullable variant to apply).
        var underlying = TypedExpressionCompiler.IsNullable(args[0].Type)
            ? TypedExpressionCompiler.UnderlyingType(args[0].Type)
            : args[0].Type;
        // Ref-type GREATEST/LEAST on nullable args is out of scope
        // for the typed pipeline; fall back to structural.
        if (!underlying.IsValueType) return null;

        var nullableElement = typeof(Nullable<>).MakeGenericType(underlying);
        var lifted = new Expression[args.Length];
        for (var i = 0; i < args.Length; i++)
        {
            lifted[i] = TypedExpressionCompiler.IsNullable(args[i].Type)
                ? args[i]
                : Expression.Convert(args[i], nullableElement);
        }

        var nullHelperName = greatest
            ? nameof(TypedBuiltinRuntime.GreatestNullable)
            : nameof(TypedBuiltinRuntime.LeastNullable);
        var openNullHelper = typeof(TypedBuiltinRuntime).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nullHelperName && m.IsGenericMethodDefinition);
        var closedNullHelper = openNullHelper.MakeGenericMethod(underlying);
        var liftedArray = Expression.NewArrayInit(nullableElement, lifted);
        return Expression.Call(closedNullHelper, liftedArray);
    }

    // ---- Helpers ----

    internal static Expression WidenToInt(Expression e)
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
    /// The <c>||</c> operator on the typed path: concatenate, but PROPAGATE
    /// NULL — return <c>null</c> if any arg is NULL (SQL standard), otherwise
    /// the concatenation of all (definite) args.
    /// </summary>
    public static Utf8String? ConcatStrictTypedNullable(Utf8String?[] args)
    {
        var totalBytes = 0;
        foreach (var a in args)
        {
            if (!a.HasValue) return null;
            totalBytes += a.GetValueOrDefault().ByteLength;
        }

        if (totalBytes == 0)
        {
            return Utf8String.Empty;
        }

        var buf = new byte[totalBytes];
        var offset = 0;
        foreach (var a in args)
        {
            var u = a.GetValueOrDefault();
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

    /// <summary>
    /// Variadic CONCAT with PG NULL semantics (matches structural):
    /// skip args whose <see cref="Nullable{T}.HasValue"/> is false,
    /// concatenate the rest. Never returns NULL — an all-NULL or empty
    /// call returns <see cref="Utf8String.Empty"/>.
    /// </summary>
    public static Utf8String ConcatTypedNullable(Utf8String?[] args)
    {
        var totalBytes = 0;
        foreach (var a in args)
        {
            if (a.HasValue) totalBytes += a.GetValueOrDefault().ByteLength;
        }

        if (totalBytes == 0)
        {
            return Utf8String.Empty;
        }

        var buf = new byte[totalBytes];
        var offset = 0;
        foreach (var a in args)
        {
            if (!a.HasValue) continue;
            var u = a.GetValueOrDefault();
            u.Span.CopyTo(buf.AsSpan(offset));
            offset += u.ByteLength;
        }

        return Utf8String.FromBytes(buf);
    }

    /// <summary>
    /// Typed GREATEST with PG NULL semantics: skip NULL args, return
    /// <c>null</c> if every arg is NULL. Otherwise the maximum
    /// non-null value.
    /// </summary>
    public static T? GreatestNullable<T>(T?[] args) where T : struct, IComparable<T>
    {
        T best = default;
        var hasBest = false;
        foreach (var a in args)
        {
            if (!a.HasValue) continue;
            var v = a.GetValueOrDefault();
            if (!hasBest || v.CompareTo(best) > 0) { best = v; hasBest = true; }
        }

        return hasBest ? best : null;
    }

    /// <summary>Typed LEAST with PG NULL semantics. See <see cref="GreatestNullable{T}"/>.</summary>
    public static T? LeastNullable<T>(T?[] args) where T : struct, IComparable<T>
    {
        T best = default;
        var hasBest = false;
        foreach (var a in args)
        {
            if (!a.HasValue) continue;
            var v = a.GetValueOrDefault();
            if (!hasBest || v.CompareTo(best) < 0) { best = v; hasBest = true; }
        }

        return hasBest ? best : null;
    }
}
