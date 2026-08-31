---
name: case-when-implementation
description: "DONE: CASE WHEN (searched + simple) shipped on both compiler paths; flat-AST design, walker sites, lazy lowering"
metadata: 
  node_type: memory
  type: project
  originSessionId: c15359a4-9705-4a58-9fa3-24c406f8e5f9
---

`CASE WHEN ... THEN ... [ELSE ...] END` (searched form) and
`CASE x WHEN v THEN ...` (simple form) ship on both compiler paths.
Full suite green (966+25 new). See [[dbspnet-overview]] for the pipeline.

## Design

- **Flat AST** per [[flat-ast-for-variadic-syntax]]:
  `CaseExpression(IReadOnlyList<CaseWhenClause> Whens, Expression? ElseResult)`
  + `CaseWhenClause(Condition, Result)`. Resolved twin:
  `ResolvedCaseWhen(IReadOnlyList<ResolvedCaseClause>, ElseResult?, Type)`.
  `ResolvedCaseClause`/`CaseWhenClause` are NOT expressions — just pairs the
  walkers descend into.
- **Simple form desugars at parse time** to the searched shape (each arm's
  test becomes `operand = v`), so the resolver/compilers only ever see the
  searched form. The parser **rejects a subquery as the simple-CASE operand**
  — it would be referenced N times and double-counted in the resolver's
  reference-keyed subquery map.
- **Lazy lowering is mandatory** and is the key difference from IN-list:
  CASE must NOT evaluate non-taken THEN/ELSE branches (e.g.
  `CASE WHEN x<>0 THEN 100/x ELSE -1 END` must not divide by zero). So it
  lowers to a right-to-left nested `Expression.Condition` chain (lazy by
  construction), NOT to an eager array-packing runtime helper the way
  IN-list does. The flat AST keeps the *walkers* shallow; only the final
  Linq tree is depth-N, and CASE arm counts are bounded in practice.
- **An arm is taken iff its condition is a definite TRUE** (3VL): NULL/FALSE
  fall through. Structural path: `CaseRuntime.ConditionIsTrue(object?) =>
  c is bool b && b`. Typed path: nullable bool condition reads
  `GetValueOrDefault()` (NULL→false).
- **Result type** = `TypeInference.CommonComparableType` folded across all
  THENs + ELSE (same helper UNION-branch unification uses; gives numeric
  promotion + clean mismatch errors), then forced nullable if ELSE is
  absent. Each branch gets a `ResolvedCast` to the common type via the
  resolver's `MaybeCast`.

## Walker sites touched (the full checklist for a new scalar node)

This is the canonical list for adding any new scalar expression node:

- **Resolver AST walkers (4)**: `ResolveScalarExpression` (dispatch),
  `CollectSubqueriesInto`, `CollectSubqueriesIntoExcludingBound`,
  `CollectNonWhereBooleanSubqueries`, `CollectAggregatesInto`. The
  aggregate walker must descend into conditions AND results so
  `SUM(CASE WHEN ...)` and CASE-containing-aggregates both work.
- **ExpressionRewriter ResolvedExpression walkers (5)** in
  `Optimizer/ExpressionRewriter.cs`: `CollectColumnIndices`,
  `CollectCorrelationIndices`, `ShiftColumnIndices`, `RemapColumnIndices`,
  `SubstituteViaProjection`. The 3 rebuilding ones got a `*Clauses` local
  helper.
- **Both compilers**: `ExpressionCompiler` (structural — also feeds
  `BatchPlanEvaluator` (the PBT oracle) and `PlanToCircuit`, so they get it
  for free) and `TypedExpressionCompiler` (typed fast path; out-of-scope
  branches throw `Unsupported` → structural fallback).
- **NOT touched**: `MonotonicityAnalyzer` and `BatchPlanEvaluator` don't
  switch on resolved-expression subtypes (they reuse `ExpressionCompiler`).
  No [[typed-compiler-reflection-gotcha]] risk — only added switch arms,
  no builder-signature changes.

## Tokens / files

Tokens `Case/When/Then/Else/End` in `Token.cs` + `Lexer.cs` keyword map.
`ParseCaseExpression` in `Parser.cs` (off `ParsePrimary`). Tests in
`CaseWhenTests.cs` (25). Docs: `docs/skipped.md`, `README.md` updated.

## PBT coverage

6 CASE templates (#41-46) added to `RandomQuery.cs`: searched-with-ELSE,
no-ELSE (nullable), simple CASE, boolean CASE in WHERE, `SUM(CASE …)`
conditional aggregation, and a 3VL fall-through over nullable table `n`.
All three PBT variants (flat / optimized / spine, 3000 iters each) green —
CASE is now under the incremental≡batch guarantee.

## First consumer: nullable non-WHERE IN/NOT IN (DONE, same branch)

CASE's first real internal use. `IN`/`NOT IN` in SELECT/HAVING/nested-
boolean with a nullable probe or subquery column now emit full SQL 3VL
instead of being rejected. In `Resolver.cs`:
`WrapWithNonWhereBooleanSubqueries` dispatches on operand nullability; the
nullable path layers THREE hidden per-group counts — match (existing
`LayerBooleanSubqueryCount` with probe), total (same with probe=null,
EXISTS-style), null-value (`LayerNullCountColumn`, new) — and
`BuildNullableInSubqueryRef` builds a `ResolvedCaseWhen` directly:
`match>0→TRUE; total=0→FALSE (empty group); probe IS NULL→NULL;
nullcount>0→NULL; ELSE FALSE`, with `NOT IN` = `ResolvedUnary(Not, …)`.
The non-nullable fast path (`COALESCE(count,0)>0`) is unchanged.
**total_count is load-bearing** only for `NULL probe IN (empty group)`
= FALSE (vs NULL for a non-empty no-match) — the easy edge to miss.
**The PBT can't catch 3VL-rewrite bugs** (batch oracle runs the same
resolved plan), so the hand-computed value tests in
`NonWhereSubqueryTests.cs` carry correctness; PBT templates 47-49 only
prove incremental≡batch *execution* of the new plan shape.

## IIF / DECODE (DONE, same effort)

Both desugar to `CaseExpression` **in the parser** (`Parser.cs`
`BuildIifExpression` / `BuildDecodeExpression`, dispatched from
`ParseIdentifierExpression` on the lowercased function name), so they need
zero resolver/compiler/walker support — they flow through everything as a
hand-written CASE. `IIF(c,a,b)` → `CASE WHEN c THEN a ELSE b END`.
`DECODE(expr, s, r, …, [default])` → simple-CASE-style, but each arm uses
**NULL-safe equality** `(expr = s) OR (expr IS NULL AND s IS NULL)` because
Oracle DECODE matches `NULL = NULL` (the defining quirk vs `=`/simple CASE);
under CASE 3VL fall-through this selects the arm exactly when DECODE would.
Subquery as the DECODE expr/search is rejected (reference-duplication, like
simple CASE operand). Tests in `CaseWhenTests.cs` (+12), PBT templates 50-51.

## BETWEEN (DONE, same parse-desugar pattern)

`[NOT] BETWEEN` parsed in `ParseComparison` (alongside `IN`), desugared in
`ParseBetweenRhs`: `x BETWEEN lo AND hi` → `x>=lo AND x<=hi`;
`NOT BETWEEN` → `x<lo OR x>hi` (De Morgan dual, agrees under 3VL). New
`Between` token. Bounds parsed at `ParseIsNull` level so the separating
`AND` binds to BETWEEN, not the boolean-AND above. Subquery operand
rejected (reference-duplication). Tests `BetweenTests.cs` (11), PBT 52-53.
Establishes the reusable recipe for the remaining expression-surface
polish: **parse-time desugar to existing AST + reject subquery operands
that would be reference-duplicated; no resolver/compiler/walker changes.**

## || string concat (DONE)

`||` is NOT a CONCAT desugar — CONCAT follows PG and *skips* NULLs, but SQL
`||` *propagates* NULL (`'a' || NULL` → NULL). So it's a distinct internal
builtin keyed `"||"` (added to `BuiltinScalarFunctions` IsKnown/Resolve/Build
+ typed `TypedBuiltinScalarFunctions`, with runtime `ConcatStrict` /
`ConcatStrictTypedNullable`). New `BarBar` token (lexer rejects a lone `|`).
A `||` run is parsed (`ParseConcat`, between IS-NULL and additive precedence)
into ONE flat `FunctionCallExpression("||", operands)` — flat so walkers stay
shallow and each operand compiles once (no duplication → subquery operands
are fine, unlike BETWEEN/DECODE). Result VARCHAR, nullable iff any operand
nullable. Tests `ConcatOperatorTests.cs` (12), PBT 54-55. Lesson: when a new
operator's NULL semantics differ from an existing function's, add a distinct
builtin rather than desugaring to the lookalike.

## IS [NOT] DISTINCT FROM (DONE)

NULL-safe (in)equality, always a definite boolean. `skipped.md` had claimed
it "needs a real compiler path" — **wrong**: it's a pure parse-time desugar.
The naive `(a=b) OR (a IS NULL AND b IS NULL)` leaks NULL when exactly one
side is null, but the GUARDED form doesn't:
`a IS NOT DISTINCT FROM b` ≡ `(a IS NULL AND b IS NULL) OR (a IS NOT NULL
AND b IS NOT NULL AND a = b)` — the `IS NOT NULL` guards make 3VL
`FALSE AND (a=b)` collapse to FALSE. `IS DISTINCT FROM` = `NOT(...)`.
Parsed in `ParseIsNull`'s `IS` arm (new `Distinct` token; `From` reused).
Subquery operands rejected (a/b referenced 3× each). Tests
`DistinctFromTests.cs` (11), PBT 56-57. Lesson: a desugar that leaks NULL
as a CASE *condition* (harmless — NULL≡FALSE for branch selection, cf.
DECODE) is NOT automatically safe as a standalone boolean *value*; guard it.

## Deferred follow-ons
