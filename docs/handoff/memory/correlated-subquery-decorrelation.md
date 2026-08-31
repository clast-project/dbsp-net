---
name: correlated-subquery-decorrelation
description: "DONE: all row-filter shapes (IN/NOT IN/EXISTS/NOT EXISTS/scalar) plus non-WHERE per-row boolean shape (IN/EXISTS/NOT IN/NOT EXISTS in SELECT/HAVING)"
metadata: 
  node_type: memory
  type: project
  originSessionId: 51fc8f20-fa30-40ed-be39-bafeaf2b02a0
---

Correlated subquery decorrelation now covers every row-filter shape:
`IN (subquery)` (`1542b00`), `EXISTS (subquery)` (`9340fb8`), scalar
subquery (`d221968`), `NOT IN` + correlated `NOT EXISTS` via anti-semi-
join with NOT NULL operands (`ae5bacb`), and `NOT IN` with nullable
operands via full SQL three-valued logic (`3fd7287`). Plus the per-row
boolean shape: `IN` / `EXISTS` / `NOT IN` / `NOT EXISTS` in
SELECT / HAVING / nested-boolean positions (this session), 941 tests
green.

## Non-WHERE positions — per-row boolean shape

Lifts `IN` / `EXISTS` / `NOT IN` / `NOT EXISTS` in SELECT / HAVING /
nested boolean to a hidden per-correlation-group `COUNT(*)` column.
`Resolver.cs`:

1. `CollectNonWhereBooleanSubqueries` (a parallel walker, **not**
   `CollectSubqueriesInto`) scans select-items and HAVING for
   `ExistsExpression` and `InSubqueryExpression`, returning two lists.
2. `WrapWithNonWhereBooleanSubqueries(plan, scope, existsList, inList)`
   feeds each through `LayerBooleanSubqueryCount`, returning the
   wrapped plan plus per-AST `BooleanSubqueryBinding(CountColumnIndex,
   IsNegated)` maps.
3. `LayerBooleanSubqueryCount` (the core):
   - Resolves the subquery with `outerSchema = plan.Schema`.
   - `DecorrelateSubqueryPlan` lifts equi-correlations into the
     subquery schema as `__corr_i` columns.
   - GROUP BY = correlation columns. **For IN, the value column joins
     as an additional group key** (it's a synthetic correlation
     against the outer probe). COUNT(*) is the single aggregate.
   - For uncorrelated EXISTS: empty GroupBy → single global count →
     `ScalarSubqueryJoinPlan`.
   - Otherwise → `CorrelatedScalarSubqueryJoinPlan` with augmented
     keys. For IN this is the trick that handles uncorrelated IN
     uniformly: the probe→value-column equi-key drives a per-row
     lookup even when the subquery body has no real correlation.
4. `BuildPreBoundFromBoolMaps` flattens both binding maps into a
   single `IReadOnlyDictionary<Expression, ResolvedExpression>`
   keyed by AST node (uses record value-equality), where the value
   is `COALESCE(count_col, 0) > 0` (or `= 0` for negated). This dict
   threads through `ResolveScalarExpression` /
   `ResolvePostAggregateExpression` as a `preBound` optional
   parameter; at the top of each dispatcher, a hit in `preBound`
   short-circuits and returns the substitution.
5. NOT NULL operands only — nullable non-WHERE IN/NOT IN is deferred
   pending `CASE WHEN` (the WHERE-conjunct form handles nullables
   today via `LayerNullCountAndFilter`).

`CollectSubqueriesIntoExcludingBound` is a sibling of
`CollectSubqueriesInto` used when collecting scalar subqueries for
`WrapWithScalarSubqueries` after the boolean pre-pass — it skips
ExistsExpression / InSubqueryExpression that have already been bound
to hidden match-count columns, preventing the existing CountSubquery
desugar from registering as a regular scalar subquery alongside the
new hidden column.

Wired into both the non-aggregate path of `ResolveSelect` (SELECT
items) and the aggregate path (HAVING + aggregate-SELECT). The
aggregate path also threads `preBound` through the parallel
`BuildXxxPost` chain (Unary, Binary, IsNull, Cast, BuiltinCall).

## Phase 1 (this session): outerSchema through ResolveDerivedTable

Added `outerSchema: Schema? = null` to `ResolveFrom` and
`ResolveDerivedTable`. Threads through to `ResolveQuery(dt.Query,
cteScope, outerSchema: outerSchema)` so correlation refs inside a
derived table (e.g., the COALESCE-COUNT desugar's wrap of the user's
subquery) resolve against the outer scope. Top-level callers pass
`outerSchema: null` so plain `SELECT * FROM (SELECT y FROM t) s`
continues to resolve with no outer scope. Phase 2 of the original
plan — generalising `DecorrelateScalarSubqueryPlan` — was attempted
but pivoted: the COUNT-via-derived-table path can't reach the
correlation column when the user's subquery projects only literals,
so non-WHERE correlated EXISTS goes through the new unified pre-pass
instead.

## Anti-semi-join algebra

`Anti(outer, sq) = outer − SemiJoin(outer, sq)` via the existing
`builder.Difference` Z-set subtraction (used today by `DifferencePlan`
for `EXCEPT`). The compiler in `PlanToCircuit.CompileSemiJoin` builds
`matched = EmitInnerJoin(...)` exactly as for semi mode, then returns
`builder.Difference(outerNonNull, matched)` when `plan.IsAnti=true`.
`BatchPlanEvaluator.BatchSemiJoin` mirrors: keep rows iff
`(inMatchSet != plan.IsAnti)`.

NULL-key outer rows drop in both semi and anti modes — consistent
with WHERE's NULL-drops-row semantics at the conjunct level. For
non-WHERE anti-semi-join callers (deferred), the NULL handling would
need to be revisited.

## NOT IN with nullable operands — full SQL 3VL (`3fd7287`)

Restriction lifted: nullable probe and/or nullable subquery column
now compile. `LayerNullCountAndFilter(plan, decorrelated,
correlationKeys, probe, probeInnerIndex)` in `Resolver.cs`:

1. Filter the decorrelated subquery to rows where the value column
   is NULL: `FilterPlan(decorrelated, IS NULL(value))`.
2. Aggregate `COUNT(*)` grouped by the correlation columns (empty
   GroupKeys for uncorrelated). Result schema:
   `[__null_corr_0..N-1, __null_count]`.
3. Layer the null-count plan as a hidden column on the outer:
   - Uncorrelated → `ScalarSubqueryJoinPlan` (existing batched
     uncorrelated form).
   - Correlated → `CorrelatedScalarSubqueryJoinPlan` (composite-key
     LEFT JOIN; reuses the same `correlationKeys` because the
     null-count plan's correlation columns share the inner index
     layout of the decorrelated subquery).
4. Append a `FilterPlan` whose predicate is
   `probe IS NOT NULL AND (null_count IS NULL OR null_count = 0)`.
   The `null_count IS NULL` arm handles two cases at once:
   correlated outer with no matching inner group (LEFT JOIN null-pad);
   uncorrelated empty subquery (DbspNet's aggregate emits no row for
   empty input → scalar evaluates to NULL).
5. The existing anti-semi-join (`SemiJoinPlan(IsAnti=true)`) stacks
   on top to handle the "no match" part of 3VL.

The probe column index and correlation keys are stable across the
null-count layering because adding a column at the end doesn't shift
earlier indices. Uncorrelated NOT EXISTS still flows through the
existing `UnaryNot(uncorrelated_EXISTS_desugar)` path — unaffected.

Tests in `NullableNotInTests.cs` (6 cases): NULL probe drops; NULL
in uncorrelated sq drops all non-matched; retract semantics
(remove the lone NULL → rows return); nullable-typed-but-no-runtime-
NULLs matches the NOT NULL path; correlated NULL is per-region;
empty correlation group passes.

## EXISTS — how it rides the same machinery

The parser emits a dedicated `ExistsExpression(Subquery, CountSubquery)`
AST node (the cached `CountSubquery` is the
`(SELECT COUNT(*) FROM (sq) AS __exists_inner)` synth that the parser
used to build directly before correlated EXISTS — reusing the same
instance keeps reference-equality dedup in `WrapWithScalarSubqueries`
working). The WHERE pre-pass scans top-level conjuncts for
`ExistsExpression` (or `UnaryExpression(Not, ExistsExpression)`):

- Uncorrelated → build `COALESCE(CountSubquery, 0) > 0` and add to
  the pending scalar-conjunct list (same plan shape as the previous
  parser-time desugar; no behavioural change).
- Correlated, not negated → `DecorrelateSubqueryPlan(subPlan, plan.Schema)`,
  lift to `SemiJoinPlan(plan, decorrelated, correlationKeys, IsAnti: false)`.
  The decorrelator already returns the complete equi-key list ready
  for EXISTS — no IN-probe key to add (the EXISTS lift uses
  `correlationKeys` directly).
- Correlated, negated → reject with `"correlated NOT EXISTS is not
  yet supported in v1 (anti-semi-join + three-valued NULL handling
  deferred)"`. The `IsAnti=true` flag on `SemiJoinPlan` is the future
  hook.

`ResolveScalarExpression`'s `ExistsExpression` arm handles non-WHERE
positions (SELECT / HAVING / nested boolean) by recursively resolving
the cached `CountSubquery` based COALESCE-desugar with
`outerSchema: null` — correlated EXISTS outside top-level WHERE
falls out as the usual "unknown column" error.

## Architecture

**Scope plumbing.** `outerSchema: Schema?` parameter added to
`ResolveQuery` → `ResolveSelect` → `ResolveScalarExpression` and its
helpers (`ResolveBinary` / `ResolveUnary` / `ResolveIsNull` / `ResolveCast`
/ `ResolveScalarFunction` / `ResolveInList` / `ResolveColumn`). Non-subquery
callers pass `null`. The WHERE pre-pass's `LiftInSubqueryToSemiJoin`
passes `plan.Schema` as the `outerSchema` for the inner `ResolveQuery`
call — single level only.

**Correlation detection.** `Schema.TryResolve(qualifier, name)` is the
non-throwing variant. `ResolveColumn` checks the local schema; on miss,
checks `outerSchema`; if found there, returns
`ResolvedCorrelationRef(OuterIndex, Type)` rather than throwing. New
`ResolvedExpression` record alongside `ResolvedColumn`. The expression
compilers throw if they ever see one — defensive assertion that the
decorrelator has stripped them all.

**Multi-key `SemiJoinPlan`.** Replaced single `OuterKey: ResolvedExpression`
with `EquiKeys: IReadOnlyList<SemiJoinEqui>` where each
`SemiJoinEqui = (OuterKey, InnerColumnIndex, Type)`. Uncorrelated `IN` is
the single-key degenerate case; correlated `IN` has N+1 keys (1 for the
IN-probe plus 1 per correlation column). `CompileSemiJoin` /
`BatchSemiJoin` build N-column `StructuralRow` keys via
`codec.BuildRow(keySchema, vs)`. The subquery side is narrowed to just
the key columns *before* `Distinct` so dedup is on the join key, not
the original subquery row.

## Decorrelation algorithm — `DecorrelateSubqueryPlan`

1. Resolve the subquery's `LogicalPlan` with `outerSchema=plan.Schema`.
   Outer column refs become `ResolvedCorrelationRef`.
2. Walk the resolved plan (`FindAllCorrelations` + `WalkResolvedExpressions`)
   to collect every referenced outer index.
3. If empty → uncorrelated, return as-is with empty `correlationKeys`.
4. Locate the outermost `FilterPlan` (the inner `SELECT ... WHERE`):
   `LocateOuterFilter` matches the `Project(Filter(...))` shape the
   resolver always emits at SELECT boundaries.
5. Split the filter's predicate at top-level AND. For each conjunct:
   - `ResolvedCorrelationRef(i) = ResolvedColumn(j)` (or symmetric)
     → record `matchedOuterIndices[i] = j`. The conjunct goes away;
     it becomes a join key.
   - Any other expression containing a correlation ref → reject with
     "only equi-correlation predicates supported in v1".
   - No correlation → keep as a remaining scalar predicate.
6. Validate that every outer index found in step 2 was covered by step 5.
7. Rebuild the inner: filter without correlation conjuncts; new outer
   `Project` whose first columns project the correlation values from
   the filter's schema (named `__corr_0`, `__corr_1`, ...) and whose
   remaining columns are the **original outer Project's projections**
   (extracted from the rebuild callback via `GetOuterProjections`).
8. Return the new plan + `IReadOnlyList<SemiJoinEqui>` with each
   correlation column pointing at its `__corr_i` index in the
   decorrelated subquery schema.

## Restrictions for v1 (rejected with clear errors)

- Non-equi correlation predicates (`outer.col > inner.col`, arithmetic
  involving the outer column).
- Multiple equi-predicates for the same outer column.
- Correlation references outside the inner WHERE — in JOIN ON,
  HAVING, aggregates, GROUP BY, or projections.
- Nested correlation (subquery inside subquery referencing
  grand-outer columns). Falls out naturally because v1 only threads
  one level of `outerSchema`.

## Walker cases for `ResolvedCorrelationRef`

In `ExpressionRewriter`: `CollectColumnIndices` skips (not local
columns), `ShiftColumnIndices` / `RemapColumnIndices` /
`SubstituteViaProjection` pass through unchanged (correlation refs
index a different namespace). New `CollectCorrelationIndices(expr) →
HashSet<int>` helper used by the decorrelator.

In `ExpressionCompiler.Build`: throws
`InvalidOperationException("internal: ResolvedCorrelationRef reached
ExpressionCompiler — decorrelator should have rewritten every
correlation reference")`. Same in `TypedExpressionCompiler.Build`
(which already fell back to structural via `UnsupportedExpressionException`
for unknown nodes; the structural compiler then hits the explicit
throw).

## Foundation for the deferred follow-ons

The `outerSchema` plumbing, `ResolvedCorrelationRef`, and multi-key
`SemiJoinPlan` all generalise:

- **Correlated `EXISTS`** (`9340fb8`) — shipped. The parser-time
  COALESCE desugar moved to the resolver; uncorrelated keeps that
  shape via `BuildExistsCoalesceDesugar(ExistsExpression.CountSubquery)`,
  correlated lifts to `SemiJoinPlan` with the correlation-only
  equi-keys from `DecorrelateSubqueryPlan` (no probe key — EXISTS uses
  the returned `correlationKeys` directly).
- **Correlated scalar subquery** (`d221968`) — shipped. Distinct
  `CorrelatedScalarSubqueryJoinPlan(Input, Subquery, CorrelationKeys,
  ScalarColumnIndex, Schema)` node; new `DecorrelateScalarSubqueryPlan`
  helper expects `Project(Aggregate(Filter(...)))` shape, **prepends
  correlation columns to `AggregatePlan.GroupKeys`** (the scalar
  decorrelation's distinguishing move vs IN/EXISTS, which only project
  correlation columns), and rebuilds the outer `Project` with the
  user's projections shifted via `ExpressionRewriter.ShiftColumnIndices`.
  Compiler builds composite-key LEFT JOIN; the inner schema after
  decorrelation is `[__corr_0..N, original_outputs..., scalar]`. v1
  restriction: inner MUST be an aggregate (no uniqueness guarantee
  otherwise); correlation refs inside aggregate args fall out as
  "unknown column" via the existing column-resolution path because
  `CollectAggregatesInto` doesn't thread `outerSchema` (rejection is
  correct, the message just isn't EXISTS-specific).
- **Correlated `NOT IN` / `NOT EXISTS`** — anti-semi-join. The
  `IsAnti` field on `SemiJoinPlan` is the hook. Three-valued NULL
  semantics are the hard part, not the correlation.

## Test surface

- `tests/DbspNet.Tests/Sql/CorrelatedInSubqueryTests.cs` (9 tests).
- `tests/DbspNet.Tests/Sql/CorrelatedExistsTests.cs` (10 tests).
- `tests/DbspNet.Tests/Sql/CorrelatedScalarSubqueryTests.cs` (10 tests).
- `tests/DbspNet.Tests/Sql/NonWhereSubqueryTests.cs` (10 tests):
  correlated EXISTS / NOT EXISTS in SELECT; uncorrelated /
  correlated IN / NOT IN in SELECT with NOT NULL operands;
  HAVING-position uncorrelated IN + correlated EXISTS;
  aggregate-SELECT-position uncorrelated IN; nullable-probe and
  nullable-subquery-column rejections.

All three row-filter test files follow the same shape: resolver
assertions that correlated → the new node, uncorrelated → unchanged
shape (regression); rejection-test triplet (non-equi /
inside-aggregate / bare); end-to-end classic correlated query;
retract semantics; one composition with a scalar predicate. Existing
`InSubqueryTests`, `ExistsTests`, and `ScalarSubqueryTests`
(uncorrelated) continue to pass — uncorrelated paths remain in their
original plan-node shapes. The uncorrelated `IN`-in-SELECT case is
the one shape change: lifts to `CorrelatedScalarSubqueryJoinPlan`
(via the synthetic probe→value-column equi-key), not
`ScalarSubqueryJoinPlan` — see updated
`Resolver_InSubqueryInSelect_LiftsToCorrelatedScalarSubqueryJoin`.
