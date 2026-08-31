---
name: scalar-function-registry-temporal
description: "DONE — IScalarFunction registry (phases 1-3, full builtin port) + temporal scalar functions, on main"
metadata: 
  node_type: memory
  type: project
  originSessionId: 24889398-8fbe-40c3-887a-de443a383fa4
---

UPDATE (main): added `SPLIT_INDEX(s,delim,n)` (0-based; NULL if n<0 or out of range
— always-nullable result) and `SPLIT_PART(s,delim,n)` (1-based; negative counts from
end per PG14; empty string when out of range / n=0; nullable=AnyNullable). Each is a
new `IScalarFunction` entry in ScalarFunctionLibrary.cs delegating to
Resolve/Build/Core helpers in BuiltinScalarFunctions + SqlBuiltinRuntime (registered
in ScalarFunctionRegistry.Build's String section). Both split **byte-wise over the
UTF-8 span** (TryNthPart/CountParts in SqlBuiltinRuntime — correct for valid UTF-8,
like ReplaceCore/PositionCore; NO decode round-trip, only the chosen part is
materialized; empty delim → whole string as one part); typed path returns null →
structural fallback like the other multi-arg string fns. Tests in ScalarFunctionTests.
Unlocked Nexmark q22 (URL dirs); see [[feldera-comparison-benchmarks]].

---

UPDATE (main, commit c801583): phase 4 done — Monotonicity() hook wired into LATENESS GC.
`IScalarFunction.Monotonicity(fn) -> ScalarMonotonicity(carrierArgIndex, Func<long,long>? frontierTransform)`.
DATE_TRUNC declares a non-identity transform (it lowers values); DATEADD(const n>=0) + analyzer
forward-shift arithmetic (`monotone_col + nonneg_const`, incl. `ts + interval`) = identity.
`MonotonicityInfo` now carries `MonotoneColumn(sources, frontierTransform)`; analyzer Project/Aggregate
route through `FromExpr` (bare col / monotone fn / Add-of-nonneg-const); joins/unions only carry
identity-transform columns. New Core `TransformedFrontier(inner, transform)`: the GROUP-BY GC site wraps
the frontier so a `date_trunc(ts)` key is GC'd against `date_trunc(maxSeen-lateness)` (THE soundness crux —
raw frontier would collect a still-live window); join/distinct GC sites skip transformed columns.
Reachable via derived table (`GROUP BY <expr>` still parser-rejected). Tests: analyzer unit + e2e bounded +
soundness witness (in-window row → count 2, not resurrected) + date_trunc incremental≡batch oracle + PBT
shift shape. Full suite 1217 pass. See [[lateness-implementation]]. Deferred: UDFs (phase 5), MIN/MAX-of-monotone,
subtraction transform.

---

UPDATE (main, commit 1a2ba07): phases 2-3 done — ALL builtins ported onto the registry.
Every scalar function is now an `IScalarFunction` entry in `ScalarFunctionLibrary.cs`; the four
parallel switches (IsKnown/Resolve/structural Build/typed TryBuild) are deleted.
`BuiltinScalarFunctions`/`TypedBuiltinScalarFunctions` remain as internal implementation-helper
libraries (ResolveXxx/BuildXxx bodies unchanged) the thin entry adapters delegate to. Aliases
(substr→substring, ceiling→ceil, date_part→extract) = same instance under another key. No
behaviour change: full suite 1207 pass. Still deferred: Monotonicity() hook (phase 4), UDFs (phase 5).

---

DONE (main, commit 90fce4e): scalar-function registry phase 1 + temporal functions.

- `IScalarFunction` (Resolve / BuildStructural / BuildTyped — no Monotonicity hook yet) +
  `ScalarFunctionRegistry` in `src/DbspNet.Sql/Expressions/`. The registry is now the
  **single dispatch authority** for all 4 sites (resolver scalar @ ResolveScalarFunction,
  resolver post-aggregate @ BuildBuiltinCallPost, ExpressionCompiler.BuildFunction,
  TypedExpressionCompiler.BuildFunction). Registry-first, **falls through to legacy
  `BuiltinScalarFunctions`/`TypedBuiltinScalarFunctions` switches** for unported names.
- First registry-native entries (`TemporalScalarFunctions.cs`): EXTRACT(field FROM src) /
  DATE_PART, DATE_TRUNC, DATEADD, DATEDIFF. EXTRACT integer fields → BIGINT, SECOND/EPOCH →
  DOUBLE. DATEADD/DATEDIFF take a **string-literal unit** (not SQL Server bare keyword);
  EXTRACT has a special parser form (new `extract` keyword + FROM). BuildTyped returns null →
  structural fallback (consistent with temporal arithmetic — see [[interval-datetime-arithmetic]]).
- Gotcha hit & fixed: a C# switch-expression returning `object` with mixed `long`/`double`
  arms unifies to `double`, boxing integer EXTRACT fields as double → mismatch the BIGINT
  column. Fix: cast every arm to `(object)`.
- 18 tests (`TemporalFunctionTests.cs`); full suite 1207 pass / 1 skip.

Deferred / next (the doc `docs/scalar-function-registry.md` stages these): port the ~25
existing builtins into registry entries function-by-function + delete legacy switch arms
(phase 2-3, mechanical, keep suite green per batch); add Monotonicity() hook +
MonotonicityAnalyzer wiring (phase 4, LATENESS-GC payoff); NOW/CURRENT_TIMESTAMP needs
once-per-step (not per-row) eval to stay incrementally correct; other math (SIN/COS/MOD/TRUNC).
