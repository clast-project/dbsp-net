# Design note: a scalar-function registry (`IScalarFunction`)

**Status:** *phases 1–3 landed* (2026-05). `IScalarFunction` +
`ScalarFunctionRegistry` are the **single dispatch authority**: all four sites
(resolver scalar + post-aggregate, structural compiler, typed compiler) route
through `ScalarFunctionRegistry`, and **every** builtin — the temporal functions
(`EXTRACT`/`DATE_PART`, `DATE_TRUNC`, `DATEADD`, `DATEDIFF`) plus the original
~25 (COALESCE, the string family, the numeric family, GREATEST/LEAST, NULLIF) —
is now a registry entry in `ScalarFunctionLibrary.cs`. The four parallel
switches are gone. `BuiltinScalarFunctions` / `TypedBuiltinScalarFunctions`
remain as **implementation libraries** of internal `ResolveXxx` / `BuildXxx`
helpers (bodies unchanged from the original switches); each registry entry is a
thin adapter delegating to them, so resolve + build for a function stay
co-located and can't drift. Aliases (`substr`→`substring`, `ceiling`→`ceil`,
`date_part`→`extract`) register the same instance under another key.

**Remaining work:**
- Add the optional `Monotonicity()` hook + wire it into
  `MonotonicityAnalyzer` (phase 4) — the LATENESS-GC payoff. **Not yet
  modeled** on the interface (kept minimal: Resolve / BuildStructural /
  BuildTyped only).
- UDF surface (phase 5, deferred).
- Optional: collapse the implementation helpers into the entry classes so
  each function is fully self-contained (the bodies currently still live in
  `BuiltinScalarFunctions` / `TypedBuiltinScalarFunctions`). Cosmetic — the
  drift surface is already gone.

The original proposal follows; it captures *why* a registry and *what* it
buys us. Read it before wiring the monotone-function catalog for LATENESS, or
before considering user-defined functions (UDFs, tracked P2 in `skipped.md`).

## The load-bearing DBSP fact

A scalar function is applied **pointwise** inside a `map` (projection) or
`filter` (predicate) operator. Those relational operators are **linear** in the
Z-set — they distribute over Z-set addition:

```
filter_p(a + b) = filter_p(a) + filter_p(b)
map_f(a + b)    = map_f(a) + map_f(b)
```

…and this holds for **any** pure `p` / `f`, regardless of what the function
computes. For a linear operator `Q`, the incremental form is just `Q^Δ = ↑Q`
(apply it to the delta). So:

> **A scalar function needs only one property to be incrementally correct in
> any position — including predicates: determinism (purity).** No monotonicity,
> no algebraic laws.

The only thing that breaks the linearity assumption is **non-determinism**
(`NOW()`, `RANDOM()`, stateful state): those aren't pure functions of the row,
so the delta computed at one tick wouldn't match a re-evaluation. Determinism
is therefore the one property the engine must *assume* (and, for UDFs,
*require by contract* — it can't verify it).

Everything else about a scalar function is either **type inference** (resolve
time) or **optional optimization metadata**. Contrast with:

- **Relational operators** — carry the linear / bilinear structure (fixed, in
  the runtime).
- **Aggregate functions** — *do* have real algebraic requirements (a group /
  invertibility for efficient retraction). That's why `SUM`/`MIN`/`MAX`/`AVG`
  are bespoke and `COUNT(DISTINCT …)` is hard. **Aggregates are out of scope
  for this note** — they'd want their own framework.

### Where scalar-function properties *do* matter (all optional, safe defaults)

1. **Monotonicity → LATENESS GC.** `MonotonicityAnalyzer` propagates the
   lateness frontier through operators to garbage-collect trace state. Today
   its `ProjectPlan` case keeps a column's frontier source only for a *bare
   pass-through* (`ResolvedColumn`); any transforming expression is
   conservatively dropped to non-monotone (see the comment at
   `MonotonicityAnalyzer.cs`, `ProjectPlan` case — "the monotone-function
   catalog extension point … deferred"). Result: `… date_trunc(ts) …` simply
   doesn't get bounded-history GC on that column. **Correct, just not
   optimized.** A monotonicity descriptor on each function is what would close
   this.
2. **Predicate pushdown** needs only column-dependency info, read structurally
   off the `ResolvedExpression` tree — *not* per-function metadata.
3. **Constant folding** (deferred) would want a "constant-foldable pure
   function" flag.

None of these is a correctness gate. **Adding functions can never break
correctness or LATENESS — it only leaves optimizations on the table until the
metadata exists.**

## What's actually wrong with the status quo

The `switch`-based dispatch is *not* the cost. The cost is that each function is
**three co-located concerns duplicated across a parallel dispatch surface**:

| Concern | Lives in |
| --- | --- |
| Is-known gate | `BuiltinScalarFunctions.IsKnown` (Resolver.cs:3129, :3290) |
| Arity + type inference (+ coercion casts) | `BuiltinScalarFunctions.Resolve` (Resolver.cs:3140, :3405) |
| Structural `Expression` builder | `BuiltinScalarFunctions.Build` (ExpressionCompiler.cs:513) |
| Typed `Expression` builder (or null ⇒ fall back) | `TypedBuiltinScalarFunctions.TryBuild` (TypedExpressionCompiler.cs:805) |

Adding one function means editing ≥4 sites in ≥2 files; the `IsKnown` switch and
the `Resolve` switch must be kept in lockstep. The file's own doc comment notes
the resolve/build pairing is co-located deliberately "so the two layers can't
drift apart" — a registry formalizes that intent instead of relying on
discipline.

**Critical constraint:** a naïve registry of `Func<object?[], object?>` delegates
would *lose the typed fast path* (it forces boxing) and wouldn't capture type
inference or NULL semantics. Any registry that's actually equivalent to today
must carry all four concerns. This is exactly Calcite's `SqlOperator` /
`SqlOperatorTable` shape (`inferReturnType`, operand type checkers,
`getMonotonicity`) — which is what Feldera leans on.

## Proposed shape

```csharp
// One entry per builtin. Aliases register the same instance under multiple keys.
internal interface IScalarFunction
{
    // Canonical lowercased name (e.g. "substring"). Aliases handled at
    // registration, not here.
    string Name { get; }

    // Arity validation + result-type inference + any argument coercion
    // (DECIMAL scale promotion, VARCHAR length, COALESCE/GREATEST unification,
    // the SIGN/LN/LOG/EXP cast-to-DOUBLE). Returns the resolved call carrying
    // (possibly re-cast) args and the result SqlType. Throws ResolveException
    // on bad arity/types — same behaviour as today.
    ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args);

    // Structural pipeline (boxed object? values). `buildArg` compiles one
    // resolved arg against the enclosing row.
    Expression BuildStructural(
        ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg);

    // Typed fast path. Return null to fall the whole query back to the
    // structural compile (today's contract for multi-arg string functions).
    Expression? BuildTyped(
        ResolvedFunctionCall fn,
        IReadOnlyList<ResolvedExpression> astArgs,
        Expression[] typedArgs);

    // Optional LATENESS hook. Default None ⇒ analyzer treats the result as
    // non-monotone (today's conservative behaviour).
    FunctionMonotonicity Monotonicity(ResolvedFunctionCall fn) => FunctionMonotonicity.None;
}

// Per-argument monotonicity in the frontier sense. The current LATENESS model
// only tracks a lower-bound frontier (max_seen − d), so only non-strict
// Increasing positions can carry a source forward; Decreasing is treated as
// None (anti-monotone isn't representable yet).
internal enum ArgMonotonicity { None, Increasing, Decreasing }

internal readonly record struct FunctionMonotonicity(IReadOnlyList<ArgMonotonicity> PerArg)
{
    public static readonly FunctionMonotonicity None = new(Array.Empty<ArgMonotonicity>());
}
```

```csharp
internal static class ScalarFunctionRegistry
{
    private static readonly Dictionary<string, IScalarFunction> ByName = Build();

    public static bool IsKnown(string name) => ByName.ContainsKey(name);
    public static IScalarFunction? Lookup(string name) => ByName.GetValueOrDefault(name);

    private static Dictionary<string, IScalarFunction> Build()
    {
        var fns = new IScalarFunction[] { new Coalesce(), new Upper(), /* … */ new Substring() };
        var map = new Dictionary<string, IScalarFunction>(StringComparer.Ordinal);
        foreach (var f in fns) map[f.Name] = f;
        // Aliases:
        map["substr"]  = map["substring"];
        map["ceiling"] = map["ceil"];
        map["strpos"]  = new Strpos(map["position"]);  // wraps position with swapped args
        return map;
    }
}
```

The four call sites collapse to registry lookups:

- `BuiltinScalarFunctions.IsKnown(n)` → `ScalarFunctionRegistry.IsKnown(n)`
- `BuiltinScalarFunctions.Resolve(n, args)` → `Lookup(n)!.Resolve(args)`
- `BuiltinScalarFunctions.Build(fn, b)` → `Lookup(fn.Name)!.BuildStructural(fn, b)`
- `TypedBuiltinScalarFunctions.TryBuild(fn, …)` → `Lookup(fn.Name)?.BuildTyped(fn, …)`

### Parser-sugar caveat (do NOT move these into the registry)

Some "functions" are **parse-time desugars** that never reach resolution as a
function call, and must stay in the parser:

- `IIF(...)` / `DECODE(...)` → `CaseExpression`.
- `POSITION(x IN y)` → the `IN`-keyword spelling is parsed specially into a
  two-arg `FunctionCallExpression("position", …)`; the *resolution/build* of
  `position` would live in the registry, but the keyword parsing stays put.
- `||` parses to a flat `FunctionCallExpression("||", …)` — `"||"` *is* a normal
  registry entry, but its run-collapsing parse stays in the parser.
- `IS [NOT] TRUE/FALSE/UNKNOWN` desugar to `COALESCE`/`IS NULL` — no entry.

The registry owns resolution + lowering of *named function calls*; the parser
keeps owning syntax.

## Monotonicity payoff (concrete)

In `MonotonicityAnalyzer`'s `ProjectPlan` case, replace the "bare column only"
check with: if the projection is `f(args…)` and `f.Monotonicity()` marks a
position `Increasing`, and the input column feeding that position carries a
frontier source, propagate the source to the output column. Seed the catalog
with the obvious wins — `ts + const`, `date_trunc`, integer `+` — so
append-mostly pipelines keep GC'ing through light transforms. (Strictly
non-decreasing is sufficient for a lower-bound frontier; strict monotonicity
isn't required.)

## Migration plan (phased, mechanical, test-preserving)

The existing `ScalarFunctionTests` + the random-query PBT are the safety net;
every phase must keep them green.

1. **Introduce `IScalarFunction` + registry, port nothing yet.** Implement the
   interface for 2–3 functions (one numeric, one string, COALESCE for the
   type-unification case). Have the four dispatch sites consult the registry
   *first* and fall through to the existing switches for unported names. Zero
   behaviour change.
2. **Port the rest function-by-function**, deleting each switch arm as its entry
   lands. Mechanical; run the suite after each batch.
3. **Delete the switches** and the now-empty `BuiltinScalarFunctions` /
   `TypedBuiltinScalarFunctions` shells (or keep them as thin facades over the
   registry to minimise churn at the call sites).
4. **Add the `Monotonicity()` hook to the analyzer** and seed the monotone-
   function catalog. New tests: a LATENESS GC test over `date_trunc(ts)` /
   `ts + const` confirming state stays bounded (mirrors the existing LATENESS
   PBT).
5. **(Optional, later) UDF surface** — see below.

No single phase is risky; the win is removing the drift surface and creating the
monotonicity slot.

## What it unlocks — and the UDF contract

- **User-defined functions.** A `RegisterFunction(IScalarFunction)` entry point
  on the catalog/compiler. The binding is the easy 10%; the hard 90% is:
  - **Determinism is an unverifiable contract.** A non-pure UDF silently
    corrupts incremental results. Gate registration behind an explicit
    "I declare this pure" and document it as a hard requirement.
  - **Typed-path boxing.** A generic CLR delegate UDF returns `null` from
    `BuiltinScalar -> BuildTyped`, dropping queries that use it back to the
    structural compile. Acceptable (several features already do this), but
    state it.
  - **Monotonicity is opt-in.** Let the registrant declare it so LATENESS keeps
    working through the UDF.
  Defer until there's a concrete consumer.
- A clean home for the deferred **constant-folding** and **monotone-function
  catalog** work.

## Non-goals

- **Aggregate functions.** Different math (group / invertibility); separate
  framework.
- **Changing NULL semantics.** Each function keeps its current PG-aligned
  propagate-vs-skip behaviour; the registry just relocates it.
- **A pluggable *operator* registry.** This is scalar functions only; relational
  operators stay fixed.

## Touch points (as of this writing)

- `src/DbspNet.Sql/Expressions/BuiltinScalarFunctions.cs` — `IsKnown`, `Resolve`,
  `Build`, `SqlBuiltinRuntime`.
- `src/DbspNet.Sql/Expressions/TypedBuiltinScalarFunctions.cs` — `TryBuild`,
  `TypedBuiltinRuntime`.
- `src/DbspNet.Sql/Plan/Resolver.cs` — dispatch at lines ~3129/3140 (scalar) and
  ~3290/3405 (post-aggregate).
- `src/DbspNet.Sql/Expressions/ExpressionCompiler.cs:513` — structural call.
- `src/DbspNet.Sql/Expressions/TypedExpressionCompiler.cs:805` — typed call.
- `src/DbspNet.Sql/Plan/MonotonicityAnalyzer.cs` — `ProjectPlan` case (the
  monotonicity hook).
- `src/DbspNet.Sql/Parser/Parser.cs` — parser sugar that must NOT move
  (IIF/DECODE, POSITION-IN, `||`, IS TRUE/FALSE/UNKNOWN).
