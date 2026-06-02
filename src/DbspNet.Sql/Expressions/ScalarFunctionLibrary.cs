// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Expressions;

// The non-temporal builtin scalar functions as IScalarFunction entries. Each is
// a thin adapter that delegates to the (now internal) resolve/build helpers in
// BuiltinScalarFunctions / TypedBuiltinScalarFunctions — the bodies are
// unchanged from the original dispatch switches; only the name→logic mapping
// moved from four parallel switches into these registry entries. Functions
// without a typed lowering (the multi-arg string family) return null from
// BuildTyped, falling the query back to the structural pipeline.

internal sealed class CoalesceFunction : IScalarFunction
{
    public string Name => "coalesce";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveCoalesce(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildCoalesce(BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildCoalesce(t);
}

internal sealed class UpperLowerFunction(string name, bool isUpper) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveUpperLower(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildUpperLower(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0], isUpper);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildUpperLower(t[0], isUpper);
}

internal sealed class LengthFunction : IScalarFunction
{
    public string Name => "length";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveLength(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildLength(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0]);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildLength(t[0]);
}

internal sealed class ConcatFunction : IScalarFunction
{
    public string Name => "concat";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveConcat(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildConcat(BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildConcat(t);
}

internal sealed class ConcatStrictFunction : IScalarFunction
{
    public string Name => "||";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveConcatStrict(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildConcatStrict(BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildConcatStrict(t);
}

internal sealed class AbsFunction : IScalarFunction
{
    public string Name => "abs";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveAbs(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildAbs(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0], fn.Arguments[0].Type);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildAbs(t[0], a[0].Type);
}

internal sealed class FloorCeilFunction(string name, bool floor) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveFloorCeil(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildFloorCeil(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0], fn.Arguments[0].Type, floor);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildFloorCeil(t[0], a[0].Type, floor);
}

internal sealed class RoundFunction : IScalarFunction
{
    public string Name => "round";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveRound("round", args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildRound(BuiltinScalarFunctions.CompileArgs(fn, buildArg), fn.Arguments);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildRound(t, a);
}

internal sealed class PowerFunction : IScalarFunction
{
    public string Name => "power";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolvePower(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var c = BuiltinScalarFunctions.CompileArgs(fn, buildArg);
        return BuiltinScalarFunctions.BuildPower(c[0], c[1], fn.Arguments[0].Type, fn.Arguments[1].Type);
    }

    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildPower(t[0], t[1]);
}

internal sealed class SqrtFunction : IScalarFunction
{
    public string Name => "sqrt";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveSqrt(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildSqrt(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0], fn.Arguments[0].Type);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildSqrt(t[0]);
}

internal sealed class GreatestLeastFunction(string name, bool greatest) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveGreatestLeast(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildGreatestLeast(BuiltinScalarFunctions.CompileArgs(fn, buildArg), greatest);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildGreatestLeast(t, greatest);
}

internal sealed class NullIfFunction : IScalarFunction
{
    public string Name => "nullif";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveNullIf(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var c = BuiltinScalarFunctions.CompileArgs(fn, buildArg);
        return BuiltinScalarFunctions.BuildNullIf(c[0], c[1], fn.Arguments[0].Type);
    }

    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildNullIf(t[0], t[1]);
}

internal sealed class SubstringFunction : IScalarFunction
{
    public string Name => "substring";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveSubstring(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildSubstring(BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

internal sealed class TrimFunction(string name, BuiltinScalarFunctions.TrimSide side) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveTrim(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildTrim(BuiltinScalarFunctions.CompileArgs(fn, buildArg), side);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

internal sealed class ReplaceFunction : IScalarFunction
{
    public string Name => "replace";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveReplace(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildReplace(BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

/// <summary>POSITION(needle IN haystack); STRPOS(haystack, needle) swaps the args.</summary>
internal sealed class PositionFunction(string name, bool swapped) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolvePosition(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildPosition(BuiltinScalarFunctions.CompileArgs(fn, buildArg), swapped);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

/// <summary>
/// <c>value [NOT] {LIKE|ILIKE} pattern [ESCAPE esc]</c>. <c>ilike</c> sets the
/// case-insensitive flag; both translate to a regex (precompiled when the
/// pattern is constant). No typed fast path — returns null to fall the query
/// back to the structural pipeline, like the other string predicates.
/// </summary>
internal sealed class LikeFunction(string name, bool caseInsensitive) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveLike(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildLike(
            fn, BuiltinScalarFunctions.CompileArgs(fn, buildArg), caseInsensitive, similar: false);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

/// <summary><c>value [NOT] SIMILAR TO pattern [ESCAPE esc]</c> — SQL-regex
/// pattern matching (a superset of LIKE); case-sensitive.</summary>
internal sealed class SimilarToFunction : IScalarFunction
{
    public string Name => "similar_to";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveLike("similar_to", args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildLike(
            fn, BuiltinScalarFunctions.CompileArgs(fn, buildArg), caseInsensitive: false, similar: true);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

/// <summary><c>REGEXP_LIKE(value, pattern [, flags])</c> — POSIX-regex
/// containment test (boolean). Constant patterns are precompiled.</summary>
internal sealed class RegexpLikeFunction : IScalarFunction
{
    public string Name => "regexp_like";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveRegexpLike(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildRegexpLike(fn, BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

/// <summary><c>REGEXP_REPLACE(value, pattern, replacement [, flags])</c> —
/// first match (or all with the <c>g</c> flag), PostgreSQL semantics.</summary>
internal sealed class RegexpReplaceFunction : IScalarFunction
{
    public string Name => "regexp_replace";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveRegexpReplace(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildRegexpReplace(fn, BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

/// <summary><c>REGEXP_SUBSTR(value, pattern [, flags])</c> — first matching
/// substring or NULL.</summary>
internal sealed class RegexpSubstrFunction : IScalarFunction
{
    public string Name => "regexp_substr";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveRegexpSubstr(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildRegexpSubstr(fn, BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) => null;
}

internal sealed class SignFunction : IScalarFunction
{
    public string Name => "sign";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveSign(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildSign(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0]);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildSign(t[0]);
}

internal sealed class LnExpFunction(string name, bool isExp) : IScalarFunction
{
    public string Name => name;
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveUnaryDouble(name, args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildLnExp(BuiltinScalarFunctions.CompileArgs(fn, buildArg)[0], isExp);
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildLnExp(t[0], isExp);
}

internal sealed class LogFunction : IScalarFunction
{
    public string Name => "log";
    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args) =>
        BuiltinScalarFunctions.ResolveLog(args);
    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        BuiltinScalarFunctions.BuildLog(BuiltinScalarFunctions.CompileArgs(fn, buildArg));
    public Expression? BuildTyped(ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> a, Expression[] t) =>
        TypedBuiltinScalarFunctions.BuildLog(t);
}
