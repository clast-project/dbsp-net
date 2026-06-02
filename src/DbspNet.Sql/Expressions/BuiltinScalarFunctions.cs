// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Implementation library for the non-temporal builtin scalar functions:
/// per-function resolve (type inference) + structural <see cref="Expression"/>
/// construction. Dispatch lives in <see cref="ScalarFunctionRegistry"/> — each
/// registry entry in <c>ScalarFunctionLibrary</c> delegates to the
/// <c>ResolveXxx</c> / <c>BuildXxx</c> helpers here, so resolve and build for a
/// function stay co-located and can't drift. Runtime helpers invoked from the
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
    internal static readonly ConstantExpression NullObject = Expression.Constant(null, typeof(object));

    /// <summary>Compile a function call's arguments against the enclosing row.</summary>
    internal static Expression[] CompileArgs(
        ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var compiled = new Expression[fn.Arguments.Count];
        for (var i = 0; i < fn.Arguments.Count; i++)
        {
            compiled[i] = buildArg(fn.Arguments[i]);
        }

        return compiled;
    }

    // ---- Per-function resolve (one internal method per builtin; the registry
    // entry for each name in ScalarFunctionLibrary delegates here, so the
    // single source of truth for "which functions exist" is the registry). ----

    internal static ResolvedFunctionCall ResolveUpperLower(string name, IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity(name, args, 1);
        RequireString(name, args[0]);
        return new ResolvedFunctionCall(name, args, args[0].Type);
    }

    internal static ResolvedFunctionCall ResolveLength(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("length", args, 1);
        RequireString("length", args[0]);
        return new ResolvedFunctionCall("length", args, new SqlIntegerType(args[0].Type.Nullable));
    }

    internal static ResolvedFunctionCall ResolveConcat(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count < 1)
        {
            throw new ResolveException("CONCAT requires at least one argument");
        }

        foreach (var a in args)
        {
            RequireString("concat", a);
        }

        // PG semantics: CONCAT never returns NULL. NULL args are skipped.
        return new ResolvedFunctionCall("concat", args, new SqlVarcharType(null, Nullable: false));
    }

    internal static ResolvedFunctionCall ResolveConcatStrict(IReadOnlyList<ResolvedExpression> args)
    {
        // The `||` operator: unlike CONCAT it PROPAGATES NULL (any NULL operand
        // → NULL result), per the SQL standard.
        if (args.Count < 2)
        {
            throw new ResolveException("|| requires at least two operands");
        }

        foreach (var a in args)
        {
            RequireString("||", a);
        }

        var concatNullable = false;
        foreach (var a in args)
        {
            concatNullable |= a.Type.Nullable;
        }

        return new ResolvedFunctionCall("||", args, new SqlVarcharType(null, concatNullable));
    }

    /// <summary>
    /// <c>value {LIKE|ILIKE|SIMILAR TO} pattern [ESCAPE esc]</c> — all operands
    /// are VARCHAR; the result is BOOLEAN, NULL iff any operand is NULL (each
    /// arg is a NULL-propagating operand, so the predicate is NULL when any of
    /// value / pattern / escape is NULL).
    /// </summary>
    internal static ResolvedFunctionCall ResolveLike(string name, IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count is not (2 or 3))
        {
            throw new ResolveException($"{LikeDisplayName(name)} takes 2 or 3 arguments");
        }

        foreach (var a in args)
        {
            RequireString(name, a);
        }

        var nullable = false;
        foreach (var a in args)
        {
            nullable |= a.Type.Nullable;
        }

        return new ResolvedFunctionCall(name, args, new SqlBooleanType(nullable));
    }

    private static string LikeDisplayName(string name) =>
        name == "similar_to" ? "SIMILAR TO" : name.ToUpperInvariant();

    internal static ResolvedFunctionCall ResolveAbs(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("abs", args, 1);
        RequireNumeric("abs", args[0]);
        return new ResolvedFunctionCall("abs", args, args[0].Type);
    }

    internal static ResolvedFunctionCall ResolveFloorCeil(string name, IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity(name, args, 1);
        RequireNumeric(name, args[0]);
        return new ResolvedFunctionCall(name, args, args[0].Type);
    }

    internal static ResolvedFunctionCall ResolvePower(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("power", args, 2);
        RequireNumeric("power", args[0]);
        RequireNumeric("power", args[1]);
        return new ResolvedFunctionCall(
            "power", args, new SqlDoubleType(args[0].Type.Nullable || args[1].Type.Nullable));
    }

    internal static ResolvedFunctionCall ResolveSqrt(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("sqrt", args, 1);
        RequireNumeric("sqrt", args[0]);
        return new ResolvedFunctionCall("sqrt", args, new SqlDoubleType(args[0].Type.Nullable));
    }

    internal static ResolvedFunctionCall ResolveReplace(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("replace", args, 3);
        RequireString("replace", args[0]);
        RequireString("replace", args[1]);
        RequireString("replace", args[2]);
        return new ResolvedFunctionCall("replace", args, new SqlVarcharType(null, AnyNullable(args)));
    }

    internal static ResolvedFunctionCall ResolvePosition(string name, IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity(name, args, 2);
        RequireString(name, args[0]);
        RequireString(name, args[1]);
        return new ResolvedFunctionCall(name, args, new SqlIntegerType(AnyNullable(args)));
    }

    // ---- Build wrappers for the arms that were inline in the old dispatch ----

    internal static Expression BuildReplace(Expression[] c) =>
        Expression.Call(ReplaceRuntime, c[0], c[1], c[2]);

    // POSITION(sub IN str) → (needle=sub, haystack=str); STRPOS(str, sub) is the
    // same with the arguments swapped.
    internal static Expression BuildPosition(Expression[] c, bool swapped) =>
        swapped
            ? Expression.Call(PositionRuntime, c[1], c[0])
            : Expression.Call(PositionRuntime, c[0], c[1]);

    internal static readonly MethodInfo RegexMatchRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.RegexMatch))!;
    internal static readonly MethodInfo PatternMatchRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.PatternMatch))!;
    internal static readonly MethodInfo PatternMatchEscRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.PatternMatchEsc))!;

    /// <summary>
    /// Lower a LIKE / ILIKE / SIMILAR TO predicate. The common case — a
    /// constant pattern with a constant (or absent) escape — compiles the
    /// regex once at build time and bakes it in; otherwise the pattern is
    /// translated and matched per row through a cached runtime helper.
    /// </summary>
    internal static Expression BuildLike(
        ResolvedFunctionCall fn, Expression[] c, bool caseInsensitive, bool similar)
    {
        var escapeArg = fn.Arguments.Count == 3 ? fn.Arguments[2] : null;

        // Fast path: constant pattern + constant/absent escape → precompile.
        if (fn.Arguments[1] is ResolvedLiteral { Value: Utf8String patLit }
            && TryConstEscape(escapeArg, out var escChar))
        {
            var regex = SqlPatternMatch.Compile(patLit.ToStringDecoded(), escChar, caseInsensitive, similar);
            return NullPropagatingUnary(c[0], typeof(Utf8String),
                s => Expression.Call(RegexMatchRuntime, Expression.Constant(regex), s));
        }

        // General path: NULL-propagating runtime translate-and-match.
        var ci = Expression.Constant(caseInsensitive);
        var sim = Expression.Constant(similar);
        return escapeArg is null
            ? Expression.Call(PatternMatchRuntime, c[0], c[1], ci, sim)
            : Expression.Call(PatternMatchEscRuntime, c[0], c[1], c[2], ci, sim);
    }

    // The escape clause defaults to backslash (PostgreSQL-aligned); a literal
    // empty escape string disables escaping. Only a single-BMP-char literal (or
    // empty) can be folded at build time — anything else (non-literal, multi
    // char, surrogate pair) falls to the runtime path, which validates it.
    private static bool TryConstEscape(ResolvedExpression? escapeArg, out char? escChar)
    {
        if (escapeArg is null)
        {
            escChar = SqlPatternMatch.DefaultEscape;
            return true;
        }

        if (escapeArg is ResolvedLiteral { Value: Utf8String escLit })
        {
            var s = escLit.ToStringDecoded();
            switch (s.Length)
            {
                case 0: escChar = null; return true;   // ESCAPE '' disables it
                case 1: escChar = s[0]; return true;
            }
        }

        escChar = null;
        return false;
    }

    internal static Expression BuildSign(Expression arg) =>
        NullPropagatingUnary(arg, typeof(double), v => Expression.Call(MathSignDouble, v));

    internal static Expression BuildLnExp(Expression arg, bool exp) =>
        NullPropagatingUnary(arg, typeof(double), v => Expression.Call(exp ? MathExp : MathLog, v));

    // ---------------- Resolver helpers ----------------

    internal static void RequireArity(string name, IReadOnlyList<ResolvedExpression> args, int expected)
    {
        if (args.Count != expected)
        {
            throw new ResolveException(
                $"{name.ToUpperInvariant()} takes exactly {expected} argument{(expected == 1 ? "" : "s")}");
        }
    }

    internal static void RequireString(string name, ResolvedExpression arg)
    {
        if (arg.Type is not SqlVarcharType)
        {
            throw new ResolveException($"{name.ToUpperInvariant()} requires a VARCHAR argument");
        }
    }

    internal static void RequireNumeric(string name, ResolvedExpression arg)
    {
        if (!TypeInference.IsNumeric(arg.Type))
        {
            throw new ResolveException($"{name.ToUpperInvariant()} requires a numeric argument");
        }
    }

    internal static ResolvedFunctionCall ResolveCoalesce(IReadOnlyList<ResolvedExpression> args)
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

    internal static ResolvedFunctionCall ResolveRound(string name, IReadOnlyList<ResolvedExpression> args)
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

    internal static ResolvedFunctionCall ResolveGreatestLeast(string name, IReadOnlyList<ResolvedExpression> args)
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

    internal static ResolvedFunctionCall ResolveNullIf(IReadOnlyList<ResolvedExpression> args)
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

    internal static bool AnyNullable(IReadOnlyList<ResolvedExpression> args)
    {
        foreach (var a in args)
        {
            if (a.Type.Nullable)
            {
                return true;
            }
        }

        return false;
    }

    internal static void RequireInteger(string name, ResolvedExpression arg)
    {
        if (arg.Type is not (SqlIntegerType or SqlBigintType))
        {
            throw new ResolveException($"{name.ToUpperInvariant()} requires integer position/length arguments");
        }
    }

    internal static ResolvedFunctionCall ResolveSubstring(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count is < 2 or > 3)
        {
            throw new ResolveException("SUBSTRING takes 2 or 3 arguments");
        }

        RequireString("substring", args[0]);
        RequireInteger("substring", args[1]);
        if (args.Count == 3)
        {
            RequireInteger("substring", args[2]);
        }

        // Result type is VARCHAR; NULL in any argument yields NULL.
        return new ResolvedFunctionCall("substring", args, new SqlVarcharType(null, AnyNullable(args)));
    }

    internal static ResolvedFunctionCall ResolveTrim(string name, IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count is < 1 or > 2)
        {
            throw new ResolveException($"{name.ToUpperInvariant()} takes 1 or 2 arguments");
        }

        RequireString(name, args[0]);
        if (args.Count == 2)
        {
            // Second argument is the set of characters to strip.
            RequireString(name, args[1]);
        }

        return new ResolvedFunctionCall(name, args, new SqlVarcharType(null, AnyNullable(args)));
    }

    internal static ResolvedFunctionCall ResolveSign(IReadOnlyList<ResolvedExpression> args)
    {
        RequireArity("sign", args, 1);
        RequireNumeric("sign", args[0]);

        // Compute over DOUBLE (sign is exact for every representable value),
        // result is INTEGER (-1 / 0 / 1). Casting up front keeps Build free
        // of per-type dispatch — notably no Decimal128 internals.
        var asDouble = MaybeCast(args[0], new SqlDoubleType(args[0].Type.Nullable));
        return new ResolvedFunctionCall("sign", new[] { asDouble }, new SqlIntegerType(args[0].Type.Nullable));
    }

    internal static ResolvedFunctionCall ResolveUnaryDouble(string name, IReadOnlyList<ResolvedExpression> args)
    {
        // LN / EXP: widen the operand to DOUBLE and return DOUBLE.
        RequireArity(name, args, 1);
        RequireNumeric(name, args[0]);
        var d = new SqlDoubleType(args[0].Type.Nullable);
        return new ResolvedFunctionCall(name, new[] { MaybeCast(args[0], d) }, d);
    }

    internal static ResolvedFunctionCall ResolveLog(IReadOnlyList<ResolvedExpression> args)
    {
        // LOG(x) — base-10 logarithm; LOG(b, x) — logarithm of x to base b.
        if (args.Count is < 1 or > 2)
        {
            throw new ResolveException("LOG takes 1 or 2 arguments");
        }

        var cast = new List<ResolvedExpression>(args.Count);
        foreach (var a in args)
        {
            RequireNumeric("log", a);
            cast.Add(MaybeCast(a, new SqlDoubleType(a.Type.Nullable)));
        }

        return new ResolvedFunctionCall("log", cast, new SqlDoubleType(AnyNullable(args)));
    }

    internal static ResolvedExpression MaybeCast(ResolvedExpression e, SqlType target)
    {
        if (SameTypeIgnoringNullable(e.Type, target))
        {
            return e;
        }

        return new ResolvedCast(e, target);
    }

    internal static bool SameTypeIgnoringNullable(SqlType a, SqlType b) =>
        a.WithNullable(false).Equals(b.WithNullable(false));

    // ---------------- Compiler helpers ----------------

    internal static Expression BuildCoalesce(Expression[] args)
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

    internal static readonly MethodInfo Utf8ToUpperInvariant =
        typeof(Utf8String).GetMethod(nameof(Utf8String.ToUpperInvariant))!;
    internal static readonly MethodInfo Utf8ToLowerInvariant =
        typeof(Utf8String).GetMethod(nameof(Utf8String.ToLowerInvariant))!;

    internal static Expression BuildUpperLower(Expression arg, bool isUpper)
    {
        // Native UTF-8 case fold on Utf8String — no decode/encode round-trip.
        var method = isUpper ? Utf8ToUpperInvariant : Utf8ToLowerInvariant;
        return NullPropagatingUnary(arg, typeof(Utf8String), s =>
            Expression.Call(s, method));
    }

    internal static readonly MethodInfo Utf8CodePointCount =
        typeof(Utf8String).GetMethod(nameof(Utf8String.CodePointCount))!;

    internal static Expression BuildLength(Expression arg)
    {
        // PG semantics: LENGTH = number of code points (not bytes).
        return NullPropagatingUnary(arg, typeof(Utf8String),
            s => Expression.Call(s, Utf8CodePointCount));
    }

    internal static readonly MethodInfo ConcatRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Concat))!;

    internal static Expression BuildConcat(Expression[] args)
    {
        // PG-style CONCAT: skip NULL args, concatenate the rest. Dispatched
        // at runtime since arity is variable.
        var argArray = Expression.NewArrayInit(typeof(object), args);
        return Expression.Convert(Expression.Call(ConcatRuntime, argArray), typeof(object));
    }

    internal static readonly MethodInfo ConcatStrictRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.ConcatStrict))!;

    internal static Expression BuildConcatStrict(Expression[] args)
    {
        // The `||` operator: NULL-propagating concat. The runtime helper
        // already returns object? (null when any arg is null), so no boxing
        // conversion is needed here.
        var argArray = Expression.NewArrayInit(typeof(object), args);
        return Expression.Call(ConcatStrictRuntime, argArray);
    }

    internal static Expression BuildAbs(Expression arg, SqlType argType)
    {
        if (argType is SqlDecimalType)
        {
            var method = typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Abs))!;
            return NullPropagatingUnary(arg, typeof(Clast.DatabaseDecimal.Values.Decimal128),
                unboxed => Expression.Call(method, unboxed));
        }

        var clr = ClrOf(argType);
        var mathMethod = typeof(Math).GetMethod(nameof(Math.Abs), new[] { clr })
            ?? throw new InvalidOperationException($"no Math.Abs({clr.Name}) overload");
        return NullPropagatingUnary(arg, clr, unboxed => Expression.Call(mathMethod, unboxed));
    }

    internal static Expression BuildFloorCeil(Expression arg, SqlType argType, bool floor)
    {
        // Integer types: no-op (FLOOR(5) = 5, CEIL(5) = 5).
        if (argType is SqlIntegerType or SqlBigintType)
        {
            return arg;
        }

        if (argType is SqlDecimalType d)
        {
            // Decimal path: scale baked in as a constant; runtime helper
            // walks the mantissa with sign-aware rounding toward ±∞.
            var methodName = floor ? nameof(DecimalRuntime.Floor) : nameof(DecimalRuntime.Ceil);
            var method = typeof(DecimalRuntime).GetMethod(methodName)!;
            return NullPropagatingUnary(arg, typeof(Clast.DatabaseDecimal.Values.Decimal128),
                unboxed => Expression.Call(method, unboxed, Expression.Constant((byte)d.Scale)));
        }

        var clr = ClrOf(argType);
        var fallbackName = floor ? nameof(SqlBuiltinRuntime.Floor) : nameof(SqlBuiltinRuntime.Ceil);
        var fallback = typeof(SqlBuiltinRuntime).GetMethod(fallbackName, new[] { clr })
            ?? throw new InvalidOperationException($"no runtime {fallbackName} for {clr.Name}");
        return NullPropagatingUnary(arg, clr, unboxed => Expression.Call(fallback, unboxed));
    }

    internal static Expression BuildRound(Expression[] args, IReadOnlyList<ResolvedExpression> argTypes)
    {
        var argType = argTypes[0].Type;
        if (argType is SqlIntegerType or SqlBigintType)
        {
            return args[0];
        }

        if (argType is SqlDecimalType decType)
        {
            // Decimal path: ROUND uses banker's rounding (matches the
            // ScaleHelper.DivideRoundHalfEven semantics) and returns the
            // same DecimalType — fractional digits beyond the requested
            // precision become trailing zeros.
            var roundMethod = typeof(DecimalRuntime).GetMethod(nameof(DecimalRuntime.Round))!;
            var scaleConst = Expression.Constant((byte)decType.Scale);

            if (args.Length == 1)
            {
                return NullPropagatingUnary(args[0], typeof(Clast.DatabaseDecimal.Values.Decimal128),
                    unboxed => Expression.Call(roundMethod, unboxed, scaleConst, Expression.Constant(0)));
            }

            var isNullDec = Expression.OrElse(
                Expression.Equal(args[0], NullObject),
                Expression.Equal(args[1], NullObject));
            var unboxedXDec = Expression.Convert(args[0], typeof(Clast.DatabaseDecimal.Values.Decimal128));
            var digitsClrDec = ClrOf(argTypes[1].Type);
            var digitsAsIntDec = digitsClrDec == typeof(int)
                ? Expression.Convert(args[1], typeof(int))
                : Expression.Convert(Expression.Convert(args[1], digitsClrDec), typeof(int));
            var callDec = Expression.Call(roundMethod, unboxedXDec, scaleConst, digitsAsIntDec);
            return Expression.Condition(isNullDec, NullObject, Expression.Convert(callDec, typeof(object)));
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

    internal static Expression BuildPower(Expression x, Expression y, SqlType xType, SqlType yType)
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

    internal static Expression BuildSqrt(Expression arg, SqlType argType)
    {
        return NullPropagatingUnary(arg, typeof(double), unboxed =>
            Expression.Call(typeof(Math).GetMethod(nameof(Math.Sqrt))!, unboxed),
            // Non-double CLR types need a widening conversion first.
            intermediateClr: ClrOf(argType) == typeof(double) ? null : ClrOf(argType));
    }

    internal static readonly MethodInfo GreatestRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Greatest))!;
    internal static readonly MethodInfo LeastRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Least))!;

    internal static Expression BuildGreatestLeast(Expression[] args, bool greatest)
    {
        var argArray = Expression.NewArrayInit(typeof(object), args);
        return Expression.Call(greatest ? GreatestRuntime : LeastRuntime, argArray);
    }

    internal static Expression BuildNullIf(Expression x, Expression y, SqlType argType)
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

        // All NULLIF-typed operands are value types in the current type
        // system; Expression.Equal resolves to op_Equality on the value
        // type when one is defined.
        var equal = Expression.Equal(xUnboxed, yUnboxed);

        var inner = Expression.Condition(equal, NullObject, x);
        var yIsNullBranch = Expression.Condition(yIsNull, x, inner);
        return Expression.Condition(xIsNull, NullObject, yIsNullBranch);
    }

    // ---------------- String / numeric builders ----------------

    internal enum TrimSide
    {
        Left = 0,
        Right = 1,
        Both = 2,
    }

    internal static readonly MethodInfo SubstringRuntime2 =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Substring), new[] { typeof(object), typeof(object) })!;
    internal static readonly MethodInfo SubstringRuntime3 =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Substring), new[] { typeof(object), typeof(object), typeof(object) })!;
    internal static readonly MethodInfo TrimRuntime1 =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.TrimString), new[] { typeof(object), typeof(int) })!;
    internal static readonly MethodInfo TrimRuntime2 =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.TrimString), new[] { typeof(object), typeof(object), typeof(int) })!;
    internal static readonly MethodInfo ReplaceRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Replace))!;
    internal static readonly MethodInfo PositionRuntime =
        typeof(SqlBuiltinRuntime).GetMethod(nameof(SqlBuiltinRuntime.Position))!;
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

    internal static Expression BuildSubstring(Expression[] args) =>
        args.Length == 2
            ? Expression.Call(SubstringRuntime2, args[0], args[1])
            : Expression.Call(SubstringRuntime3, args[0], args[1], args[2]);

    internal static Expression BuildTrim(Expression[] args, TrimSide side)
    {
        var sideConst = Expression.Constant((int)side);
        return args.Length == 1
            ? Expression.Call(TrimRuntime1, args[0], sideConst)
            : Expression.Call(TrimRuntime2, args[0], args[1], sideConst);
    }

    internal static Expression BuildLog(Expression[] args)
    {
        if (args.Length == 1)
        {
            return NullPropagatingUnary(args[0], typeof(double), v => Expression.Call(MathLog10, v));
        }

        // LOG(b, x) = log_b(x) = Math.Log(x, b). Both operands are already
        // DOUBLE (the resolver cast them); NULL in either propagates.
        var isNull = Expression.OrElse(
            Expression.Equal(args[0], NullObject),
            Expression.Equal(args[1], NullObject));
        var b = Expression.Convert(args[0], typeof(double));
        var x = Expression.Convert(args[1], typeof(double));
        var call = Expression.Call(MathLog2, x, b);
        return Expression.Condition(isNull, NullObject, Expression.Convert(call, typeof(object)));
    }

    // ---------------- Misc helpers ----------------

    /// <summary>
    /// Build <c>operand == null ? null : (object)f((clr)operand)</c>.
    /// </summary>
    internal static Expression NullPropagatingUnary(
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

    internal static Type ClrOf(SqlType t) => t switch
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
}

/// <summary>
/// Runtime helpers invoked from expression trees built by
/// <see cref="BuiltinScalarFunctions"/>. Kept as plain static methods so
/// <c>Expression.Call</c> can reach them by overload resolution on arg types.
/// </summary>
internal static class SqlBuiltinRuntime
{
    // ---- String ----

    /// <summary>
    /// PG-style CONCAT: NULL args are skipped; result is never NULL. Uses
    /// byte-wise concat over UTF-8 — no decode pass.
    /// </summary>
    public static Utf8String Concat(object?[] args)
    {
        // Single allocating pass: sum byte lengths, then copy.
        var totalBytes = 0;
        foreach (var a in args)
        {
            if (a is Utf8String u)
            {
                totalBytes += u.ByteLength;
            }
        }

        if (totalBytes == 0)
        {
            return Utf8String.Empty;
        }

        var buf = new byte[totalBytes];
        var offset = 0;
        foreach (var a in args)
        {
            if (a is not Utf8String u)
            {
                continue;
            }

            u.Span.CopyTo(buf.AsSpan(offset));
            offset += u.ByteLength;
        }

        return Utf8String.FromBytes(buf);
    }

    /// <summary>
    /// The <c>||</c> operator: concatenate UTF-8 args, but PROPAGATE NULL —
    /// if any arg is NULL the whole result is NULL (SQL standard, unlike the
    /// PG-style NULL-skipping <see cref="Concat"/>).
    /// </summary>
    public static object? ConcatStrict(object?[] args)
    {
        foreach (var a in args)
        {
            if (a is null)
            {
                return null;
            }
        }

        return Concat(args);
    }

    /// <summary>
    /// <c>SUBSTRING(s, start)</c> — 1-based, code-point semantics, to the end
    /// of the string. NULL in any argument yields NULL.
    /// </summary>
    public static object? Substring(object? s, object? start) =>
        s is null || start is null ? null : SubstringCore((Utf8String)s, AsInt(start), null);

    /// <summary>
    /// <c>SUBSTRING(s, start, length)</c> — 1-based start, <paramref name="length"/>
    /// code points. Positions below 1 are clipped to the string, counting
    /// toward the length window (SQL standard). NULL in any argument → NULL.
    /// </summary>
    public static object? Substring(object? s, object? start, object? length) =>
        s is null || start is null || length is null
            ? null
            : SubstringCore((Utf8String)s, AsInt(start), AsInt(length));

    /// <summary><c>LTRIM/RTRIM/TRIM(s)</c> — strip leading/trailing spaces.</summary>
    public static object? TrimString(object? s, int side) =>
        s is null ? null : TrimCore((Utf8String)s, chars: null, side);

    /// <summary>
    /// <c>LTRIM/RTRIM/TRIM(s, chars)</c> — strip any leading/trailing code
    /// point that appears in <paramref name="chars"/>.
    /// </summary>
    public static object? TrimString(object? s, object? chars, int side) =>
        s is null || chars is null ? null : TrimCore((Utf8String)s, (Utf8String)chars, side);

    /// <summary><c>REPLACE(s, from, to)</c> — replace every occurrence of
    /// <c>from</c> with <c>to</c>. NULL in any argument → NULL.</summary>
    public static object? Replace(object? s, object? from, object? to) =>
        s is null || from is null || to is null
            ? null
            : ReplaceCore((Utf8String)s, (Utf8String)from, (Utf8String)to);

    /// <summary>
    /// <c>POSITION(needle IN haystack)</c> — 1-based code-point index of the
    /// first occurrence of <paramref name="needle"/>, or 0 if absent. An empty
    /// needle is at position 1. NULL in either argument → NULL.
    /// </summary>
    public static object? Position(object? needle, object? haystack) =>
        needle is null || haystack is null
            ? null
            : (object)PositionCore((Utf8String)haystack, (Utf8String)needle);

    /// <summary>
    /// Match against a pattern compiled at build time (the constant-pattern fast
    /// path). The caller's <see cref="BuiltinScalarFunctions.NullPropagatingUnary"/>
    /// has already guaranteed a non-NULL value.
    /// </summary>
    public static bool RegexMatch(Regex re, Utf8String value) => re.IsMatch(value.ToStringDecoded());

    /// <summary>
    /// LIKE / ILIKE / SIMILAR TO with a runtime (non-constant) pattern and the
    /// default backslash escape. NULL in either operand → NULL.
    /// </summary>
    public static object? PatternMatch(object? value, object? pattern, bool caseInsensitive, bool similar) =>
        value is null || pattern is null
            ? null
            : (object)SqlPatternMatch.Compile(
                ((Utf8String)pattern).ToStringDecoded(), SqlPatternMatch.DefaultEscape, caseInsensitive, similar)
                .IsMatch(((Utf8String)value).ToStringDecoded());

    /// <summary>
    /// LIKE / ILIKE / SIMILAR TO with an explicit ESCAPE clause. The escape
    /// string must be empty (escaping disabled) or a single character; NULL in
    /// any operand → NULL.
    /// </summary>
    public static object? PatternMatchEsc(
        object? value, object? pattern, object? escape, bool caseInsensitive, bool similar)
    {
        if (value is null || pattern is null || escape is null)
        {
            return null;
        }

        return SqlPatternMatch.Compile(
            ((Utf8String)pattern).ToStringDecoded(),
            SqlPatternMatch.EscapeCharOf((Utf8String)escape),
            caseInsensitive,
            similar).IsMatch(((Utf8String)value).ToStringDecoded());
    }

    internal static int AsInt(object o) => o switch
    {
        int i => i,
        long l => checked((int)l),
        _ => Convert.ToInt32(o, CultureInfo.InvariantCulture),
    };

    internal static Utf8String SubstringCore(Utf8String s, int start1, int? length)
    {
        var span = s.Span;
        var n = CodePointCount(span);

        // First selected code point (1-based, clipped to the string start).
        var first = Math.Max(1, start1);
        int lastExclusive;
        if (length is null)
        {
            lastExclusive = n + 1;
        }
        else
        {
            // Positions strictly below start1 + length are in the window.
            lastExclusive = start1 + length.Value;
            lastExclusive = Math.Min(lastExclusive, n + 1);
        }

        if (lastExclusive <= first)
        {
            return Utf8String.Empty;
        }

        var byteStart = CpToByte(span, first - 1);
        var byteEnd = CpToByte(span, lastExclusive - 1);
        if (byteStart >= byteEnd)
        {
            return Utf8String.Empty;
        }

        return Utf8String.FromBytes(span[byteStart..byteEnd].ToArray());
    }

    internal static Utf8String TrimCore(Utf8String s, Utf8String? chars, int side)
    {
        var set = new HashSet<Rune>();
        if (chars is null)
        {
            set.Add(new Rune(' '));
        }
        else
        {
            var cs = chars.Value.Span;
            var cp = 0;
            while (cp < cs.Length)
            {
                Rune.DecodeFromUtf8(cs[cp..], out var r, out var consumed);
                set.Add(r);
                cp += consumed;
            }
        }

        var span = s.Span;
        var offsets = new List<int>();
        var runes = new List<Rune>();
        var p = 0;
        while (p < span.Length)
        {
            offsets.Add(p);
            Rune.DecodeFromUtf8(span[p..], out var r, out var consumed);
            runes.Add(r);
            p += consumed;
        }

        offsets.Add(span.Length);

        var lo = 0;
        var hi = runes.Count;
        if (side is 0 or 2)
        {
            while (lo < hi && set.Contains(runes[lo]))
            {
                lo++;
            }
        }

        if (side is 1 or 2)
        {
            while (hi > lo && set.Contains(runes[hi - 1]))
            {
                hi--;
            }
        }

        if (lo == 0 && hi == runes.Count)
        {
            return s;
        }

        var byteStart = offsets[lo];
        var byteEnd = offsets[hi];
        return byteStart >= byteEnd ? Utf8String.Empty : Utf8String.FromBytes(span[byteStart..byteEnd].ToArray());
    }

    internal static Utf8String ReplaceCore(Utf8String s, Utf8String from, Utf8String to)
    {
        var sp = s.Span;
        var f = from.Span;
        if (f.Length == 0 || f.Length > sp.Length)
        {
            return s;
        }

        var t = to.Span;
        var result = new List<byte>(sp.Length);
        var i = 0;
        while (i <= sp.Length - f.Length)
        {
            if (sp.Slice(i, f.Length).SequenceEqual(f))
            {
                foreach (var b in t)
                {
                    result.Add(b);
                }

                i += f.Length;
            }
            else
            {
                result.Add(sp[i]);
                i++;
            }
        }

        while (i < sp.Length)
        {
            result.Add(sp[i]);
            i++;
        }

        return Utf8String.FromBytes(result.ToArray());
    }

    internal static int PositionCore(Utf8String haystack, Utf8String needle)
    {
        var h = haystack.Span;
        var ndl = needle.Span;
        if (ndl.Length == 0)
        {
            return 1;
        }

        var byteIdx = h.IndexOf(ndl);
        if (byteIdx < 0)
        {
            return 0;
        }

        // Byte offset → 1-based code-point index.
        var cp = 0;
        for (var i = 0; i < byteIdx; i++)
        {
            if ((h[i] & 0xC0) != 0x80)
            {
                cp++;
            }
        }

        return cp + 1;
    }

    internal static int CodePointCount(ReadOnlySpan<byte> span)
    {
        var count = 0;
        for (var i = 0; i < span.Length; i++)
        {
            if ((span[i] & 0xC0) != 0x80)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Byte offset where the <paramref name="cp"/>-th (0-based) code
    /// point starts; <c>span.Length</c> if past the end.</summary>
    internal static int CpToByte(ReadOnlySpan<byte> span, int cp)
    {
        if (cp <= 0)
        {
            return 0;
        }

        var seen = 0;
        for (var i = 0; i < span.Length; i++)
        {
            if ((span[i] & 0xC0) != 0x80)
            {
                if (seen == cp)
                {
                    return i;
                }

                seen++;
            }
        }

        return span.Length;
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

/// <summary>
/// Translates SQL <c>LIKE</c> / <c>ILIKE</c> / <c>SIMILAR TO</c> patterns into
/// .NET regular expressions and caches the compiled result. Both forms are
/// whole-string matches (implicitly anchored), so the body is wrapped in
/// <c>\A(?:…)\z</c>; <see cref="RegexOptions.Singleline"/> makes <c>_</c>
/// (→ <c>.</c>) match any character including newline, matching SQL semantics.
/// </summary>
/// <remarks>
/// The escape character defaults to backslash (<see cref="DefaultEscape"/>),
/// matching PostgreSQL; an explicit <c>ESCAPE ''</c> disables it. The cache is
/// a pure memoization — a translated pattern is a deterministic function of its
/// inputs — so it does not affect incremental correctness.
/// <para>
/// <b>Scope note (v1).</b> <c>SIMILAR TO</c> covers the SQL-regex
/// metacharacters <c>% _ | * + ? ( ) { } [ ]</c>; bracket expressions are
/// passed through to .NET, so POSIX class names (<c>[[:alpha:]]</c>) are not
/// supported yet.
/// </para>
/// </remarks>
internal static class SqlPatternMatch
{
    /// <summary>PostgreSQL-aligned default escape character.</summary>
    public const char DefaultEscape = '\\';

    private static readonly ConcurrentDictionary<(string Pattern, char? Escape, bool Ci, bool Similar), Regex> Cache =
        new();

    public static Regex Compile(string pattern, char? escape, bool caseInsensitive, bool similar) =>
        Cache.GetOrAdd(
            (pattern, escape, caseInsensitive, similar),
            static k => Build(k.Pattern, k.Escape, k.Ci, k.Similar));

    /// <summary>
    /// The single-character escape carried by an explicit <c>ESCAPE</c> clause:
    /// empty disables escaping (<c>null</c>); anything longer than one .NET char
    /// is an error.
    /// </summary>
    public static char? EscapeCharOf(Utf8String escape)
    {
        var s = escape.ToStringDecoded();
        return s.Length switch
        {
            0 => null,
            1 => s[0],
            _ => throw new InvalidOperationException("ESCAPE must be a single-character string"),
        };
    }

    private static Regex Build(string pattern, char? escape, bool caseInsensitive, bool similar)
    {
        var body = similar ? TranslateSimilar(pattern, escape) : TranslateLike(pattern, escape);
        var options = RegexOptions.Singleline | RegexOptions.CultureInvariant;
        if (caseInsensitive)
        {
            options |= RegexOptions.IgnoreCase;
        }

        return new Regex(@"\A(?:" + body + @")\z", options);
    }

    private static string TranslateLike(string pattern, char? escape)
    {
        var sb = new StringBuilder(pattern.Length + 8);
        for (var i = 0; i < pattern.Length; i++)
        {
            var ch = pattern[i];
            if (escape is char e && ch == e)
            {
                i++;
                if (i >= pattern.Length)
                {
                    throw new InvalidOperationException("LIKE pattern may not end with the escape character");
                }

                sb.Append(Regex.Escape(pattern[i].ToString()));
                continue;
            }

            switch (ch)
            {
                case '%': sb.Append(".*"); break;
                case '_': sb.Append('.'); break;
                default: sb.Append(Regex.Escape(ch.ToString())); break;
            }
        }

        return sb.ToString();
    }

    private static string TranslateSimilar(string pattern, char? escape)
    {
        var sb = new StringBuilder(pattern.Length + 8);
        var inBracket = false;
        for (var i = 0; i < pattern.Length; i++)
        {
            var ch = pattern[i];
            if (escape is char e && ch == e)
            {
                i++;
                if (i >= pattern.Length)
                {
                    throw new InvalidOperationException("SIMILAR TO pattern may not end with the escape character");
                }

                sb.Append(Regex.Escape(pattern[i].ToString()));
                continue;
            }

            if (inBracket)
            {
                // Bracket expressions pass through to .NET verbatim until ']'.
                sb.Append(ch);
                if (ch == ']')
                {
                    inBracket = false;
                }

                continue;
            }

            switch (ch)
            {
                case '%': sb.Append(".*"); break;
                case '_': sb.Append('.'); break;
                case '[': sb.Append('['); inBracket = true; break;

                // SQL-regex metacharacters that coincide with .NET's.
                case '|':
                case '*':
                case '+':
                case '?':
                case '(':
                case ')':
                case '{':
                case '}':
                    sb.Append(ch);
                    break;

                // Everything else — including '.', '^', '$', '\' — is literal.
                default:
                    sb.Append(Regex.Escape(ch.ToString()));
                    break;
            }
        }

        if (inBracket)
        {
            throw new InvalidOperationException("SIMILAR TO pattern has an unterminated bracket expression");
        }

        return sb.ToString();
    }
}
