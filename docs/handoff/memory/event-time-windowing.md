---
name: event-time-windowing
description: "Event-time windowing arc (TUMBLE/HOP/SESSION → Nexmark q7/q8/q5/q11/q12); Phase 1 TUMBLE DONE+pushed, HOP/SESSION gated"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5af21acf-8820-491f-aeab-5aae5b009a15
---

Arc: add event-time windowing TVFs to unlock the last feature-gated Nexmark
queries. Validated the kickoff thesis ("a windowing TVF is mostly lowering, not a
new operator family") and refined it against Feldera's actual Nexmark SQL.

**Key reframe from reading Feldera's benchmark/feldera-sql/.../nexmark/queries/*.sql
(via `gh api`):** Feldera uses TWO surface forms, not one —
- **q7, q8, q12** use the `GROUP BY TUMBLE(date_time, INTERVAL '10' SECOND)` form
  with `TUMBLE_START`/`TUMBLE_END` scalar projections (NOT the TVF form). And
  Feldera's **q12 is event-time** on date_time, not the original Nexmark
  processing-time form — so the kickoff's separate "Phase 4 processing-time q12"
  is MOOT; q12 falls out of TUMBLE for free.
- **q5** uses the TVF form `TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL 2
  SECOND, INTERVAL 10 SECOND))` with `window_start`/`window_end` columns.
- **q11**: Feldera has NO q11.sql — it doesn't support session windows. So SESSION
  has **no apples-to-apples Feldera baseline** (decisive for de-prioritising it).

**Phase 1 — event-time TUMBLE — DONE + pushed (commit b526a18, suite 1775 green).**
Pure lowering, **no new plan node or operator** (cleaner than the kickoff hoped):
- New internal monotone scalar `tumble_start(t, size)` in
  `TemporalScalarFunctions.cs` — a fixed-bucket floor `floor(t/size)*size`, the
  arbitrary-interval generalisation of `DATE_TRUNC`. Resolve: TIMESTAMP or
  whole-day DATE; rejects calendar (month/year) sizes (non-uniform bucket).
  BuildStructural → `TemporalFunctions.TumbleStart`/`FloorToBucket`; BuildTyped =
  null (structural fallback, like the other temporal fns). **Monotonicity** =
  carrier arg 0 + bucket-floor `FrontierTransform` — mirrors `DateTruncFunction`
  exactly, so a `GROUP BY` window-start key is GC-able (drop a window only once
  the frontier passes `start+size`; soundness identical to date_trunc). Registered
  in `ScalarFunctionRegistry.Build`.
- Parser desugar (Parser.cs, just before the generic FunctionCallExpression
  return ~line 1820): `TUMBLE(t,s)`/`TUMBLE_START(t,s)` → `tumble_start(t,s)`;
  `TUMBLE_END(t,s)` → `BinaryExpression(Add, tumble_start(t,s), s)`. Contextual
  (identifier text). **Why this works with zero resolver changes:** GROUP BY
  TUMBLE and SELECT TUMBLE_START both desugar to the same `tumble_start` AST, so
  `ResolvePostAggregateExpression`'s `AstEqual` group-key match (the
  [[group-by-expression-keys]] machinery) resolves the SELECT column to the group
  key; TUMBLE_END resolves post-aggregate as `group_col + interval`.
- Wired q7/q8/q12 (verbatim Feldera form) into `NexmarkQueries.All`; q5/q11 stay
  in `.NotSupported`. Tests: `TumbleWindowTests` (value + q7/q8 shapes + LATENESS
  GC bounds + soundness witness), `TumbleWindowPbtTests` (2000-iter
  incremental≡batch, flat+spine, monotone+GC AND ±1-retraction), q7/q8/q12 e2e in
  `NexmarkNewQueriesTests`.

**DECISION GATE — REAL Feldera head-to-head (user's machine, dbsp-bench-5.txt, 1/10/14c).
ARC JUSTIFIED — TUMBLE landed competitively, q8 a standout:**
- **q8 = MAJOR UNCONDITIONAL WIN** — beats Feldera at EVERY core count incl.
  single-thread: 1c 1.91× (13.0M/s vs 6.8M), 10c 1.97×, 14c 1.86×. Shows q3's
  signature NEGATIVE scaling (W=1 13.0M > W=14 11.8M) — the single-thread path is
  so efficient parallel exchange isn't worth it. **DbspNet's 2nd unconditional win
  alongside q3.** (Helped by the windowed group-by collapsing the 1:3:46 stream to
  tiny person/auction state.)
- **q12 = functional, trails, scales then regresses** — 1c 0.33× (1.62M vs 4.96M)
  → 10c 0.89× → 14c 0.63× (loses ground 10c→14c = exchange overhead). The gap is
  WORST single-core ⇒ it's PER-ROW efficiency, the broad row-rep theme
  ([[per-row-execution-efficiency]] / [[repr-execution-apportionment]]), not a
  windowing-specific cost. Clear optimization target.
- **q7 = weakest in suite** — single-only (~780k/s, 0.21× 1c). Two stacked issues:
  (a) typed-join-with-cross-side-residual doesn't parallelize, (b) unoptimized
  per-row. Odd lagged-window BETWEEN semantics make it lowest-value.
Standings at 14c: DbspNet leads/ties ~10/15 (q1,q3,q4,q8,q9,q15,q16,q17,q20,q22);
gaps cluster in window/join-heavy q12/q18/q19 + new q7. See
[[nexmark-feldera-w14-snapshot]].

**Typed / W>1 unlock — DONE + pushed (commit 2fccbbd).** Measure-first corrected
the framing: typed `tumble_start` ALONE was necessary-but-not-sufficient — q7/q8/q12
ALL contain temporal+INTERVAL arithmetic (TUMBLE_END = tumble_start+size; q7 WHERE
ts−INTERVAL) which `TypedExpressionCompiler.BuildNumericArith` rejected (non-numeric
result → Unsupported → structural fallback). A 7-case parallel-compile diagnostic
(TryCompileParallel on optimized plans) isolated it precisely: tumble_start key
parallelized, but **TUMBLE_END failed because the post-aggregate `CAST(string AS
INTERVAL)` is left UNFOLDED there** (scan-level intervals get constant-folded to a
ResolvedLiteral, post-aggregate ones don't) and typed `BuildCast` had no
string→INTERVAL arm. Fixes: (1) typed temporal/interval arithmetic in
`TypedExpressionCompiler.BuildBinary` (temporal±interval / interval±interval /
interval×÷numeric / temporal−temporal → same `TemporalArithmetic` helpers, unboxed
operands; temporal comparison was already typed via the structs' `<` operators);
(2) typed `CAST(string↔INTERVAL)`; (3) typed `tumble_start` (`FloorToBucket`, made
public). **Result: q8 W=24 1.65×, q12 3.02×** (was single-only; output
cross-checked vs W=1). Also clears the long-deferred "typed-fast-path temporal
arithmetic" item. `TumbleParallelTests` guards it. Full suite 1775 green.

**q7 still single-only** (structural): its inner join carries a cross-side `BETWEEN`
residual the typed JOIN path doesn't parallelize (a separate typed-join gap, not a
windowing one). q7 is the least valuable of the three (odd lagged-window BETWEEN
semantics) — left as a follow-on, not chased.

**Phase 2 — HOP TVF → q5 — DONE + pushed (commit 75512a7, suite 1785 green).**
Added the windowing-TVF surface `TABLE(TUMBLE|HOP(TABLE src, DESCRIPTOR(timecol),
[slide,] size))` → new `WindowTableFunction` FromClause node (parser
`ParseWindowTableFunction`; `TUMBLE`/`HOP`/`DESCRIPTOR` contextual; `TABLE` is the
reserved token) + resolver `ResolveWindowTableFunction`. **No new operator/plan
node** — lowers to `ProjectPlan` (TUMBLE, N=1) / `UnionAllPlan` of N=size/slide
shifted `ProjectPlan`s (HOP), branch k: `window_start = tumble_start(t,slide) −
k·slide`, `window_end = window_start + size`. Rides the Phase-1 tumble_start +
typed temporal arith, so **q5 compiled straight onto the typed→parallel path: W=24
3.49×** (output cross-checked; low absolute throughput 118k W=1 — heaviest query:
5× fan-out + double-agg + self-join). q5 → NexmarkQueries.All. `HopWindowTests` +
q5 e2e + 2000-iter HOP incremental≡batch PBT (±1 retractions). Constant day-time
INTERVAL slide/size (size multiple of slide); TIMESTAMP / whole-day DATE.
**GC DEFERRED for HOP (the landmine the kickoff flagged, now CONFIRMED unsound):**
the per-branch `−k·slide` shift isn't followed by `MonotonicityAnalyzer` (Subtract
unhandled → not monotone → no GC → safe-by-omission, correct-but-unbounded). And
even if it WERE followed, the structurally-derived transform `bucket_floor(v,slide)
− (n−1)·slide` OVER-DROPS by up to a slide (a window s is final only when s+size ≤
frontier; that threshold lands in [frontier−size+1, frontier−size+slide], the upper
part unsound). The SOUND transform is `v → v − size` (independent of branch),
which the structural derivation can't produce — needs a custom per-column
frontier-transform injection mechanism (the real follow-on). HOP state is unbounded
under LATENESS today; TUMBLE GC is unaffected (group key is plain tumble_start).
Subquery TVF data source also deferred (base table only).

**Phase 3 (recommend CUT): SESSION → q11.** The one genuinely stateful case (a
merge operator under retraction). Feldera has no baseline. Build only if the user
wants q11 for its own sake.

Relates to [[lateness-implementation]] (the frontier/GC plumbing reused wholesale),
[[temporal-filters-now]] (clock-as-watermark), [[scalar-function-registry-temporal]]
(date_trunc monotonicity = the exact template), [[flat-ast-for-variadic-syntax]]
(TUMBLE is fixed-arity so parse-time desugar is fine).
