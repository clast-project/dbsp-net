---
name: orderby-limit-topk
description: "DONE — ORDER BY/LIMIT/OFFSET/FETCH as incremental TOP-K, both compiler paths"
metadata: 
  node_type: memory
  type: project
  originSessionId: 40f317c4-6855-4b4f-a5a9-43f503753e95
---

ORDER BY / LIMIT / OFFSET / FETCH FIRST shipped as incremental TOP-K (the last
P1 SQL gap). Implemented on `main`; full suite green (1125 pass), docs updated
(`docs/skipped.md`, `README.md`, `ARCHITECTURE.md`).

**Shape:** parser wraps any query expression in `OrderLimitQuery` (clauses bind
to the whole expression, so they work after set-ops and inside subqueries/
derived tables) → resolver lowers `ORDER BY … LIMIT/OFFSET` to `TopKPlan`
(`Input.Schema` unchanged) → both compiler paths build a total-order
`SortKeyComparer<TRow>` (ORDER BY keys + full-row tiebreak:
`StructuralRowComparer` structurally, `Comparer<TRow>.Default` on the emitted
struct) and wire `TopKOp<TRow>` (`Core/Operators/Stateful/TopKOp.cs`, fixed to
`Z64`). Operator keeps integrated input in a `SortedDictionary`, recomputes the
`[offset, offset+limit)` window each tick honouring multiplicity, emits the diff
vs. last window. Builder: `StatefulOperators.TopK`. Typed path builds boxed
`Func<TRow,object?>` extractors via `TypedExpressionCompiler.TryBuildInto` +
`Expression.Convert(.., object)` and invokes a generic `BuildTopK<TRow>` by
reflection (mirrors `InvokeSpineDistinct`); falls back to structural if a sort
expr is outside typed scope.

**Gotcha that bit me:** there are THREE plan walkers, not just the compiler
switch — `PlanToCircuit.CollectScans` and `PlanToCircuit.CompilePlan` both throw
on unknown nodes (had to add `TopKPlan` arms to each), plus `PlanOptimizer`
(added a recurse-into-Input arm; TopK is a pushdown barrier).
`MonotonicityAnalyzer` has a safe conservative default, so no change there. When
adding any future plan node, grep for `unsupported plan node`.

**Semantics decided:** bare `ORDER BY` (no limit/offset) = validated no-op (row
order unobservable in a Z-set); `LIMIT` without `ORDER BY` uses the implicit
full-row order; ORDER BY scope = output columns/ordinals only; NULL ordering =
PostgreSQL default (ASC→LAST, DESC→FIRST). New reserved keywords added
(order/limit/offset/fetch/first/next/row/rows/only/asc/desc/nulls/last) — no
corpus collisions.

**Non-selected ORDER BY columns** (follow-on, also shipped): `ORDER BY` over a
column/expression not in the select list works via hidden columns — `ResolveOrderLimit`
classifies each term (output-resolvable vs. not), and for non-output terms
re-resolves a copy of the inner `SelectStatement` with the expr appended as
`__orderby_k` (so it resolves against the FROM scope under the resolver's normal
aggregate/non-grouped rules), orders TOP-K by the hidden column, then strips it
with a final `ProjectPlan`. DISTINCT + non-selected order and set-op +
non-selected order both raise explicit resolver errors (standard SQL). Output
scope still wins first (alias precedence).

**Follow-on now DONE:** partitioned/windowed TOP-K (`ROW_NUMBER`/`RANK`/
`DENSE_RANK` with `PARTITION BY`) shipped — see [[partitioned-topk]]. See
[[join-completeness-next]] for the broader TPC-H trajectory.
