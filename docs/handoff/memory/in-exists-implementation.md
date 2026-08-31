---
name: in-exists-implementation
description: "DONE: IN-list, EXISTS, IN-subquery uncorrelated forms shipped; design decisions, what's deferred"
metadata: 
  node_type: memory
  type: project
  originSessionId: 51fc8f20-fa30-40ed-be39-bafeaf2b02a0
---

`IN (literal_list)`, `NOT IN (literal_list)`, `EXISTS (subquery)`, `NOT
EXISTS`, and `IN (subquery)` (uncorrelated, WHERE-only) all ship as of
commits `e9ba89d` (Phase 1, IN-list), `62c56d4` (Phase 2a, EXISTS), and
`539b31f` (Phase 2b, IN-subquery). Full suite 890 green.

## Phase 1: IN-list (`e9ba89d`)

Flat AST node `InListExpression(Probe, Values, IsNegated)`, NOT a parse-time
desugar to OR-of-equalities. See [[flat-ast-for-variadic-syntax]] for the
why — a left-leaning OR chain would build an O(N)-depth tree and risk a C#
stack overflow on large lists. Compiles to a single call into
`InListRuntime.Evaluate` that iterates the values with SQL three-valued NULL
semantics: NULL probe → NULL; match on non-NULL → TRUE (or FALSE if
negated); no match but NULL value present → NULL; no match no NULLs →
FALSE (or TRUE if negated). Common comparable type folded across probe
and all values at resolve time. Tokens: `IN`. New tests: `InListTests.cs`
(18 tests including a 10k-element stack-depth guard).

## Phase 2a: EXISTS / NOT EXISTS (`62c56d4`)

Pure parser-time desugar — `EXISTS (sq)` becomes
`COALESCE((SELECT COUNT(*) FROM (sq) AS __exists_inner), 0) > 0`. No new
AST node, no new resolver case, no new operator. `NOT EXISTS` falls out as
`NOT (...)` via the existing unary-not arm. Token added: `EXISTS`.

**The COALESCE wrap is load-bearing**: DbspNet's incremental aggregate
(see existing `WhereScalarSubquery_EmptySubquery_ComparisonYieldsNull_FiltersOut`)
emits no row for empty input, so the bare scalar `(SELECT COUNT(*) FROM
(sq))` is NULL when sq is empty. Without COALESCE, `NOT (NULL > 0)`
stays NULL and WHERE would drop the row instead of passing it. Standard
SQL has `COUNT(*)` always return one row — the COALESCE is the
engine-quirk shim.

EXISTS works in SELECT/HAVING for free (the desugar is pure expression),
not WHERE-only.

## Phase 2b: IN (uncorrelated subquery) in WHERE (`539b31f`)

New AST node `InSubqueryExpression(Probe, Subquery, IsNegated)`. New
LogicalPlan `SemiJoinPlan(Input, Subquery, OuterKey, IsAnti)` —
`IsAnti=false` always in v1, reserved for future NOT IN.

Resolver pre-pass on WHERE: `SplitAndConjuncts` walks the top-level AND
chain; each `InSubqueryExpression` conjunct lifts to a `SemiJoinPlan`
over the input via `LiftInSubqueryToSemiJoin`. Remaining scalar
predicates re-AND into a `FilterPlan` over the result.
`InSubqueryExpression` in SELECT / HAVING / nested-boolean positions
hits a deferred-error arm in `ResolveScalarExpression`.

`PlanToCircuit.CompileSemiJoin` builds: `Distinct(sq) ⋈ outer` on the
equi-key, with a combine that emits only the outer row. NULL outer-keys
and NULL subquery values are dropped by the inner-join's NULL-key filter
— matches SQL three-valued semantics at the WHERE boundary. Probe and
subquery column are cast to `CommonComparableType` so equi-key compare
is well-defined.

`BatchPlanEvaluator.BatchSemiJoin` is the matching batch oracle for the
PBT. `MonotonicityAnalyzer.SemiJoinPlan` passes outer columns through
(input's monotonicity carries).

Walker cases added across all plan-walking sites:
`Resolver.CollectSubqueriesInto` (no recurse — handled by WHERE pre-pass
or rejected later), `PlanToCircuit.CollectScans`, `PlanOptimizer.OptimizeNode`,
`MonotonicityAnalyzer.Visit`, `BatchPlanEvaluator.Evaluate`.

Tests in `InSubqueryTests.cs` (11): resolver lift to SemiJoinPlan,
mixed-AND with scalar predicate, SELECT-position deferred-error, NOT IN
deferred-error, correlated rejection (falls out as "column not found"),
end-to-end membership / dedup-no-multiplication / retract / mixed AND
scalar / NULL-outer-key.

## Deferred follow-ons (in `docs/skipped.md`)

- **[P1] `NOT IN (subquery)`** — anti-semi-join primitive + three-valued
  NULL handling. `NOT IN` semantics genuinely differ from `NOT (x IN ...)`
  when the subquery contains NULL.
- **[P1] `IN (subquery)` in SELECT / HAVING / nested boolean** —
  per-row boolean rather than row filter; different machinery from the
  semi-join lift.
- **[P1] Subquery decorrelation** — the pass that converts correlated
  subqueries to joins, unblocking correlated `IN` / `EXISTS` / scalar
  subqueries in one go. The resolver's scope plumbing is explicit-parameter
  passing today (no global context); decorrelation would slot in as a
  new bottom-up optimizer pass (existing `PlanOptimizer` shape).

## Reusable patterns this session surfaced

- Forking WHERE into "semi-join lift conjuncts + remaining scalars" via
  `SplitAndConjuncts` / `JoinAnd` is reusable for any future
  predicate-shape lift (e.g. a future NOT EXISTS-as-anti-semi-join).
- Parse-time desugar to existing AST + machinery (the EXISTS pattern)
  beats adding a new resolver/operator path whenever the semantics fit.
- The reflection gotcha [[typed-compiler-reflection-gotcha]] was
  sidestepped trivially: SemiJoinPlan returns `null` from
  `TypedPlanCompiler.TryCompileNode`, falling back to structural.
- `CommonComparableType` folded across N values (existing
  `BuiltinScalarFunctions.cs:182` pattern) was reused for IN-list type
  promotion.
