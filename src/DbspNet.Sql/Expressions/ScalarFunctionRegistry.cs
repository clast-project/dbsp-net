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
}

/// <summary>
/// Single dispatch authority for builtin scalar functions. Registered
/// <see cref="IScalarFunction"/> entries are consulted first; any name not yet
/// ported to the registry falls through to the legacy
/// <see cref="BuiltinScalarFunctions"/> / <see cref="TypedBuiltinScalarFunctions"/>
/// switches. This keeps a single seam while the function library migrates onto
/// the registry incrementally (each ported function deletes its legacy arms).
/// </summary>
internal static class ScalarFunctionRegistry
{
    private static readonly Dictionary<string, IScalarFunction> ByName = Build();

    public static bool IsKnown(string name) =>
        ByName.ContainsKey(name) || BuiltinScalarFunctions.IsKnown(name);

    public static ResolvedFunctionCall Resolve(string name, IReadOnlyList<ResolvedExpression> args) =>
        ByName.TryGetValue(name, out var f)
            ? f.Resolve(args)
            : BuiltinScalarFunctions.Resolve(name, args);

    public static Expression BuildStructural(
        ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg) =>
        ByName.TryGetValue(fn.FunctionName, out var f)
            ? f.BuildStructural(fn, buildArg)
            : BuiltinScalarFunctions.Build(fn, buildArg);

    public static Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) =>
        ByName.TryGetValue(fn.FunctionName, out var f)
            ? f.BuildTyped(fn, astArgs, typedArgs)
            : TypedBuiltinScalarFunctions.TryBuild(fn, astArgs, typedArgs);

    private static Dictionary<string, IScalarFunction> Build()
    {
        var fns = new IScalarFunction[]
        {
            new ExtractFunction(),
            new DateTruncFunction(),
            new DateAddFunction(),
            new DateDiffFunction(),
        };

        var map = new Dictionary<string, IScalarFunction>(StringComparer.Ordinal);
        foreach (var f in fns)
        {
            map[f.Name] = f;
        }

        // DATE_PART(field, source) is EXTRACT(field FROM source) with the field
        // spelled as a string literal — the same registry entry.
        map["date_part"] = map["extract"];
        return map;
    }
}
