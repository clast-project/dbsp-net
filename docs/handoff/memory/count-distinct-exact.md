---
name: count-distinct-exact
description: "DONE: exact COUNT(DISTINCT x) shipped on main — both compile paths + spine; unlocks Nexmark q15/q16"
metadata: 
  node_type: memory
  type: project
  originSessionId: a4b020b6-5826-4371-a771-fe45abd2c5bd
---

DONE (shipped to main): exact `COUNT(DISTINCT x)` — the number of distinct
non-NULL argument values per group. The small-feature item (1) from the Nexmark
gap roadmap in [[feldera-comparison-benchmarks]]; mirrors the wiring of
[[approx-count-distinct]] (HLL) but answers exactly.

**Design — invertible per-value count (MIN/MAX membership model, not HLL).**
State is `Dictionary<object,long> Counts`: each distinct non-NULL value → number
of post-delta rows carrying it with strictly-positive net weight. Result =
`Counts.Count` (non-nullable BIGINT; 0 for an all-NULL / empty group). A value is
"present" iff its per-value count is positive, so a retraction that empties a
value drops it WITHOUT a rebuild — fully invertible (unlike HLL, which rebuilds
on any retraction tick). `Compute` scans positive-weight non-NULL rows into a
`HashSet`. Because the present-value set is a deterministic function of the
multiset, incremental ≡ batch **exactly** — tests assert exact equality (no
tolerance), including a typed/structural/spine random ins/del PBT.

**The DISTINCT flag plumbing (the new bit vs other aggregates):**
- Parser: `bool Distinct` added to `FunctionCallExpression`; parsed in
  `ParseIdentifierExpression` right after `(` (after the `*` check), before the
  arg list. Rejected (`ParseException`) if the call is then a window function
  (`COUNT(DISTINCT …) OVER` unsupported).
- Resolver `ToAggregateKind`: `count` + `Distinct` → new
  `AggregateKind.CountDistinct`; `Distinct` on any other function name throws
  ("DISTINCT is not supported for …"). No `Distinct` flag threaded through
  `AggregateCall`/`AggregateKey` — the distinct enum kind already distinguishes
  them, keeping the downstream switches enum-dispatch like every other kind.

**Wiring checklist (same switch sites as HLL — verified June 2026):** enum in
LogicalPlan.cs (appended `CountDistinct` AFTER `ApproxPercentile` to preserve
existing ordinals / persisted plan fingerprints); Resolver ToAggregateKind +
ComputeAggregateResultType (→ BIGINT non-null, requires arg); new
`SqlCountDistinctAggregator` (SqlAggregators.cs) + `TypedCountDistinctAggregator<TIn>`
(TypedSqlAggregators.cs, boxing `Func<TIn,object?>` extractor like HLL);
PlanToCircuit.BuildSqlAggregator + BatchPlanEvaluator (oracle) + TypedPlanCompiler
(TypedAggregateResultType + BuildTypedAggregator → new
`BuildCountDistinctAggregator`); **PlanOptimizer.NarrowAggregateInput must bail**
(non-linear, like MIN/MAX/HLL). Spine works for free via Composite/TypedComposite
→ IAggregator.

Tests: `tests/.../Sql/CountDistinctTests.cs` (typed/structural/spine; group by,
deletes, nulls, VARCHAR, large-cardinality exact, coexistence with COUNT(*)/SUM,
DISTINCT-on-SUM rejected, incremental≡batch PBT). Full suite green (1713).

**Nexmark q15/q16 enabled** (`NexmarkQueries.All`, validated in
`NexmarkNewQueriesTests`): per-day (q15) and per-channel/day (q16) bid/bidder/
auction stats. DbspNet-dialect rewrites of Feldera's SQL: `to_char(date_time,…)`
→ `CAST(date_time AS DATE)`; `COUNT(*) FILTER(WHERE p)` → `SUM(CASE WHEN p THEN 1
ELSE 0 END)`; **`COUNT(DISTINCT x) FILTER(WHERE p)` → `COUNT(DISTINCT CASE WHEN p
THEN x END)`** (CASE→NULL when false, count ignores NULL = exact FILTER
semantics). q16's cosmetic `minute` column omitted (no minute-format scalar).
Both run end-to-end in the `nexmark` harness, flagged "single-only" like q17
(computed group key + CASE aggregation aren't on the typed PARALLEL exchange
path). **Nexmark coverage now 12 queries** (q0–q4, q9, q15–q20).
