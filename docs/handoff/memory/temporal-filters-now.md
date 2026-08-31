---
name: temporal-filters-now
description: "DONE — NOW()/CURRENT_TIMESTAMP shipped as advancing temporal filters (option B), all 5 phases on main; CURRENT_DATE shipped (day-space clock), CURRENT_TIME rejected as cyclic"
metadata: 
  node_type: memory
  type: project
  originSessionId: 1548043e-a9a2-4fdc-b4ec-bc6bee71c30c
---

DONE (shipped to main, 2026-05/06): `NOW()` / `CURRENT_TIMESTAMP` implemented as
**advancing temporal filters** (the `mz_now()` model — "option B" in
`docs/now-and-temporal-filters.md`), the design note that preceded this work.
Chosen over the frozen-constant (C) and per-tick-stamp (A, a trap) alternatives.

Five phases, each its own commit:
- **Clock primitive** — `RootCircuit.LogicalTime`/`AdvanceTime` (microseconds,
  monotone, host-driven, never wall clock), persisted in the snapshot manifest
  (`logical_time`, schema v2→v3, restored *before* operators load), exposed as
  `IFrontier` via `RootCircuit.Clock`. `CompiledQuery.AdvanceClock` is the host
  convenience.
- **Parse/resolve** — dedicated `NowExpression` AST node (never a
  FunctionCallExpression / registry entry — purity contract). Recognised only in
  WHERE conjuncts `key {<|<=|>|>=} NOW() [± constant day-time INTERVAL]` (both
  operand orders; BETWEEN folds to one window) → `TemporalFilterPlan`. Anywhere
  else = ResolveException.
- **Operator** — `TemporalFilterOp<TRow>` (Core), recompute-and-diff like
  `TopKOp`: emits inserts/retractions as the clock advances with no input;
  self-GCs rows past their upper bound; ISnapshotable. Typed path falls back to
  structural (like [[lateness-implementation]]).
- **Oracle/PBT** — `BatchPlanEvaluator.Now` + TemporalFilterPlan arm;
  `TemporalFilterPbtTests` checks accumulated incremental output == batch at the
  run's final clock (deltas telescope to validAt(finalClock)).
- **Clock-as-watermark GC** — a disappear-bounded filter on a bare time-key over
  a scan advertises a `clock − offset` frontier (TransformedFrontier over the
  clock), reusing the [[lateness-implementation]] frontier/MonotonicityAnalyzer
  plumbing so downstream GROUP BY/join/DISTINCT GC. Frontiers dict generalised
  MutableFrontier→IFrontier.

**Why:** the headline streaming feature; the design note flagged it as
LATENESS-sized and a product call (which the user made: full B).

DONE (2026-06): **`CURRENT_DATE`** as a `DATE`-keyed temporal filter, day-space
clock ("option 1" in the design note). A `TemporalClock {Timestamp, Date}`
discriminator on `TemporalFilterPlan` (set by resolver from the niladic
spelling, which must match the key type) fixes the unit of clock/key/offsets.
Compiler wraps `LogicalClock` in `TransformedFrontier(Date32.DayNumberFloor)`
(`floor(now/µs_per_day)`), extracts key as `Date32.Days`, offsets in whole days
(sub-day INTERVAL rejected). **`TemporalFilterOp` unchanged** — it only compares
`long`s in whatever shared unit it's handed. GC frontier is
`floor(clock/µs_per_day) − offsetDays` (matches `MonotoneKey.Extract(Date32)=Days`,
which is *why* day-space, not µs-space). Batch oracle floors `now` + reads key as
day-number; dedicated CURRENT_DATE PBT uses hour-scale clock jumps.
**`CURRENT_TIME` rejected** as cyclic (`now mod day`, not monotone — no sound
advancing semantics); clear error everywhere. Plan-record offset fields renamed
`Appear/DisappearOffsetMicros`→`Appear/DisappearOffset` (unit now per-Clock).

DONE (2026-06, same session): **`CAST(timestamp ↔ date)`** in both compiler paths
(`Date32.FromTimestamp` = day-floor; `Timestamp.FromDate` = midnight; both
monotone) — makes `CURRENT_DATE` usable on TIMESTAMP event columns
(`WHERE CAST(ts AS DATE) > CURRENT_DATE - INTERVAL '30' DAY`). Plus the
**expression-key downstream-GC frontier**: a `CAST(ts AS DATE)` filter key reduces
to the base `ts` column, advertising a midnight-µs frontier
(`(floor(clock/day)-offDays)*day`, conservative ≤1 day, GC is `key<frontier` so
never drops live rows). `GROUP BY ts` (bare — GROUP BY is bare-column-only in v1)
uses it directly; a projected `CAST(ts AS DATE)` grouped via a **derived table**
picks it up because `MonotonicityAnalyzer` now recognises a monotone temporal CAST
(forward day-floor transform, like date_trunc). Recogniser
`MonotonicityAnalyzer.TemporalKeySource` shared with PlanToCircuit. PBT has a
derived-table-GROUP-BY-over-CAST shape so unsound GC = incremental≠batch failure.

**How to apply:** remaining deferred follow-ons (each independent, all
sound-by-omission): `CURRENT_TIME` frozen-constant feature (separate design);
spine sibling op; per-row transition-time index (per-tick recompute is O(state));
general monotone-key inverse (arbitrary `f(col)`) & non-direct-scan inputs for the
downstream-GC frontier; typed fast path for the temporal-filter op; WAL (approach
A) per-tick clock recording (snapshot path is correct). Relates to
[[scalar-function-registry-temporal]] (NOW is deliberately NOT a registry entry)
and [[interval-datetime-arithmetic]] (day-time interval offsets).
