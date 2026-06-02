// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// One registry entry per builtin scalar function. Co-locates the three
/// concerns that must stay in lockstep — arity/type inference, structural
/// (boxed) lowering, and typed (fast-path) lowering — so they can't drift
/// across the parallel dispatch surfaces the way the legacy
/// <see cref="BuiltinScalarFunctions"/> / <see cref="TypedBuiltinScalarFunctions"/>
/// switches could. See <c>docs/scalar-function-registry.md</c> for the design.
/// </summary>
/// <remarks>
/// The load-bearing DBSP fact: a scalar function is applied pointwise inside a
/// linear map/filter, so the only property the engine must assume is
/// determinism (purity). Everything else here is type inference or
/// lowering — no algebraic obligations. (The optional monotonicity hook for
/// LATENESS GC is a future addition; not modeled yet.)
/// </remarks>
internal interface IScalarFunction
{
    /// <summary>Canonical lowercased name (aliases are registered separately).</summary>
    string Name { get; }

    /// <summary>
    /// Validate arity / argument types, apply any coercions, and return a
    /// <see cref="ResolvedFunctionCall"/> carrying the (possibly re-cast) args
    /// and the inferred result type. Throws <see cref="ResolveException"/> on
    /// bad arity/types.
    /// </summary>
    ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args);

    /// <summary>
    /// Structural pipeline: emit a LINQ expression over boxed <c>object?</c>
    /// values. <paramref name="buildArg"/> compiles one resolved argument
    /// against the enclosing row.
    /// </summary>
    Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg);

    /// <summary>
    /// Typed fast path: lower against pre-compiled typed argument expressions.
    /// Return <c>null</c> to fall the whole query back to the structural
    /// compile (the established contract for ops outside the typed subset).
    /// </summary>
    Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs);

    /// <summary>
    /// LATENESS-GC hook: if this call is monotone non-decreasing in one of its
    /// arguments, describe which argument carries the frontier and how the
    /// frontier value must be transformed into the result's value space.
    /// Default: not monotone (no GC propagation — always safe). See
    /// <c>docs/scalar-function-registry.md</c> "Monotonicity payoff".
    /// </summary>
    ScalarMonotonicity? Monotonicity(ResolvedFunctionCall fn) => null;
}

/// <summary>
/// Declares that a scalar call is monotone non-decreasing in argument
/// <see cref="CarrierArgIndex"/>. <see cref="FrontierTransform"/> maps a
/// frontier value (in the carrier argument's native-unit space) into the
/// result's value space; <c>null</c> means identity (the raw frontier is a
/// sound — if conservative — threshold, e.g. a forward shift like
/// <c>ts + interval</c>). A non-null transform is required when the function
/// can produce a value below its input (e.g. <c>date_trunc</c>), so the GC
/// threshold must be the transformed bound. The transform must itself be
/// non-decreasing.
/// </summary>
internal readonly record struct ScalarMonotonicity(int CarrierArgIndex, Func<long, long>? FrontierTransform);

/// <summary>
/// Single dispatch authority for builtin scalar functions. Every name lives in
/// exactly one <see cref="IScalarFunction"/> entry, which owns its arity/type
/// inference and both lowerings — replacing the four parallel switches that
/// previously had to be kept in lockstep. The resolver checks
/// <see cref="IsKnown"/> before <see cref="Resolve"/>, and lowering only runs
/// for resolved (hence known) calls, so the dictionary always contains the name
/// by the time Build* is reached.
/// </summary>
internal static class ScalarFunctionRegistry
{
    private static readonly Dictionary<string, IScalarFunction> ByName = Build();

    public static bool IsKnown(string name) => ByName.ContainsKey(name);

    public static ResolvedFunctionCall Resolve(string name, IReadOnlyList<ResolvedExpression> args) =>
        ByName.TryGetValue(name, out var f)
            ? f.Resolve(args)
            : throw new ResolveException($"unknown function '{name}'");

    public static Expression BuildStructural(
        ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        Lookup(fn.FunctionName).BuildStructural(fn, buildArg);

    public static Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) =>
        Lookup(fn.FunctionName).BuildTyped(fn, astArgs, typedArgs);

    public static ScalarMonotonicity? Monotonicity(ResolvedFunctionCall fn) =>
        ByName.TryGetValue(fn.FunctionName, out var f) ? f.Monotonicity(fn) : null;

    private static IScalarFunction Lookup(string name) =>
        ByName.TryGetValue(name, out var f)
            ? f
            : throw new InvalidOperationException($"no registered scalar function '{name}'");

    private static Dictionary<string, IScalarFunction> Build()
    {
        var fns = new IScalarFunction[]
        {
            // Temporal (registry-native).
            new ExtractFunction(),
            new DateTruncFunction(),
            new DateAddFunction(),
            new DateDiffFunction(),

            // General.
            new CoalesceFunction(),
            new NullIfFunction(),
            new GreatestLeastFunction("greatest", greatest: true),
            new GreatestLeastFunction("least", greatest: false),

            // String.
            new UpperLowerFunction("upper", isUpper: true),
            new UpperLowerFunction("lower", isUpper: false),
            new LengthFunction(),
            new ConcatFunction(),
            new ConcatStrictFunction(),
            new SubstringFunction(),
            new TrimFunction("ltrim", BuiltinScalarFunctions.TrimSide.Left),
            new TrimFunction("rtrim", BuiltinScalarFunctions.TrimSide.Right),
            new TrimFunction("trim", BuiltinScalarFunctions.TrimSide.Both),
            new ReplaceFunction(),
            new PositionFunction("position", swapped: false),
            new PositionFunction("strpos", swapped: true),
            new LikeFunction("like", caseInsensitive: false),
            new LikeFunction("ilike", caseInsensitive: true),
            new SimilarToFunction(),

            // Numeric.
            new AbsFunction(),
            new FloorCeilFunction("floor", floor: true),
            new FloorCeilFunction("ceil", floor: false),
            new RoundFunction(),
            new PowerFunction(),
            new SqrtFunction(),
            new SignFunction(),
            new LnExpFunction("ln", isExp: false),
            new LnExpFunction("exp", isExp: true),
            new LogFunction(),
        };

        var map = new Dictionary<string, IScalarFunction>(StringComparer.Ordinal);
        foreach (var f in fns)
        {
            map[f.Name] = f;
        }

        // Aliases (same instance under another spelling).
        map["date_part"] = map["extract"]; // DATE_PART(field, source) ≡ EXTRACT(field FROM source)
        map["substr"] = map["substring"];
        map["ceiling"] = map["ceil"];
        return map;
    }
}
