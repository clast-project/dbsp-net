---
name: typed-program-path-scope
description: "Scoping for adopting the typed (non-object[]) compile path in CompileProgram — the ivm-bench program runs entirely on boxed StructuralRow; this is the row-representation lever"
metadata: 
  node_type: memory
  type: project
  originSessionId: 59c84cbd-f811-435b-8b7b-3f6e874d8a8b
  modified: 2026-07-21T03:41:42.517Z
---

**Question (Curt, 2026-07-20):** why hasn't `CompileProgram` adopted the typed compile path, and is
typing it a tractable lever against the ~45 GiB batch-1 allocation floor?

**ANSWERED — MEASURED DECISIVELY NEGATIVE, ARC STOPPED (2026-07-20, design §23, committed).** Wired
`CompileOptions.TypeEligibleProgramViews` (default-off gate) into `CompileProgram`: attempt
`TryCompileWithStructuralBoundary` per view, structural fallback elsewhere; `LastProgramTypedTally`
diagnostic; driven by `IVM_TYPE_VIEWS=1` on `IvmBatchProfile`. Result on SF=3 batch-1 (identical output
row counts across all 16 outputs): **allocation +82% (44.7→81.3 GiB), wall +42% (88→125s)**. Loss is on
the TYPED views themselves (watches agg 4.0→10.5s 2.6×, daily_market window 7.1→13.6s, trades 1.8→4.0s),
not fallbacks. **Mechanism:** the fully-structural program passes the SAME `object[]` by reference at
inter-view boundaries (near-free); typing inserts a decode(lift)+encode(adapt) round-trip, and a
**typed→typed adjacency** (the 3 losers, all downstream of typed `brokerage_*`) forces exactly the column
boxing the lazy `TypedStructuralRow` deferred (§16.9's win DEFEATED by an immediately-re-reading typed
consumer) + stores wide structs BY VALUE in large traces (daily_market 1.28M-row window state). `fact_cash_balances`
(typed downstream of a STRUCTURAL view) was ≈neutral — isolates the mechanism.

**SEAM-ISOLATION A/B/C (design §23.6, `TypedSeamAbBenchmark` + `TryCompileTypedSeamChain`, Curt asked
"was I wrong that a typed seam would avoid the copy?"):** on the daily_market slice (staging→brokerage→
daily window-agg), three configs, Step-only, identical replayed input: **S** structural, **H** typed +
STRUCTURAL seam (round-trip), **T** typed + TYPED seam (Curt's design, no round-trip). Result: S 4044 MiB
/9.4s, H 20902/16.5s, T 20193/15.1s. **H−T (the seam round-trip ALONE) = +3.5% alloc / +9.4% time —
NEGLIGIBLE. T−S (typed-seam design vs structural) = +399% alloc (≈5×) / +60% time.** So Curt's typed-seam
intuition is CORRECT (it does remove the round-trip) but the round-trip ISN'T the cost. **Clean repr
comparison, no algorithm confound:** both paths emit the SAME generic `PartitionedWindowAggregateOp`
(same affected-range recompute); only the row type differs. **Mechanism (code-grounded, the box-cache
inversion):** the op is row-opaque and extracts partition/order/agg keys via BOXED delegates
(`Func<TRow,object>`). `object[]`/StructuralRow boxes each scalar ONCE at ingest and a bare-col extractor
`row=>row[i]` returns that cached box by ref (0 alloc) — StructuralRow is a BOX CACHE shared by every op +
recompute. A typed value struct has NO cache: `BuildBoxedExtractor` re-boxes the field EVERY call, and the
window op re-reads keys/values many times → the boxes typing was meant to eliminate get MULTIPLIED. So
typing the ROW while operators still extract via boxed delegates is STRICTLY WORSE than object[]. Typed
wins only if the WHOLE operator stack is monomorphized (typed comparers/aggregators/dict keys = Feldera
model; ≈ what the typed PARALLEL path does, where data-parallelism outweighs the boxing). **This CORRECTS
§23.4's emphasis — the seam adjacency is minor (~3.5%/9%); the real villain is in-operator boxed extraction,
present with or without a typed seam.** Lever is NOT seam typing but removing boxed extraction from hot ops
(monomorphized typed ops, or columnar/buffer-reuse §17-21).

**MONOMORPHIZATION BUILT + MEASURED (design §23.7, Curt asked "shouldn't a smarter compiler avoid the
boxing?"):** YES. Per-site attribution (`WindowAggAllocProbe`, since removed) pinned the typed cost to the
per-partition `SortedDictionary` insert, whose `SortKeyComparer` runs the BOXED order-key extractor per
comparison (89.8M compares, O(log n)/insert); structural returns cached `object[]` boxes (0 alloc), typed
boxes `dm_date` fresh each compare. Built the fix: `CompileOptions.MonomorphizeWindowOrderKey` →
`LongKeyComparer<TRow>` comparing the UNBOXED monotone `long?` key (`BuildUnboxedOrderKey` mirrors
`MonotoneKey.Extract`: int/long as-is, Timestamp/Time64 µs, Date32 days; NULL/DESC/tiebreak mirrored,
order-equivalent, falls back for non-carriers). **Result (stable top-line, daily_market slice): T typed
20193 MiB/15064ms → T2 monomorphized 8570 MiB/11565ms = −58% alloc / −23% time, OUTPUT BYTE-IDENTICAL to
structural (asserted, 1.28M rows). The order-key boxing was 72% of the entire typed penalty** (T−S gap
16.1 GiB, T−T2 removed 11.6 GiB). Reframes the whole arc: **typed is NOT fundamentally 5× worse — ¾ of the
penalty was ONE fixable inefficiency (boxed key extraction to keep the op row-opaque); with it gone typed
is +112%/2.1×, and the residual is REPRESENTATION** (wide TOutRow/TInRow structs copied by value into the
output ZSetBuilder / per-row 1-elem frame ZSet / recompute list — columnar/§17-21 territory, not boxing).
Landmine: the fine-grained per-call GC probe UNDERCOUNTS (swung 11.9↔4.5 GiB across runs); trust the stable
top-line `GetTotalAllocatedBytes` A/B (build-the-fix-and-measure), not per-site GC deltas at 90M calls.
**`MonomorphizeWindowOrderKey` also helps the single-query + PARALLEL typed window paths = a rare in-Step
per-row cut that carries to W>1 ([[per-row-execution-efficiency]] holy grail).** Same "unbox the key, keep
the op generic over a typed key type" pattern generalizes to join/aggregate/TOP-K keys.

**SHIPPED DEFAULT-ON (2026-07-20, dbsp-net 687c0d6, both validation gates cleared).** Gate (a) correctness:
`WindowOrderKeyMonomorphizeTests` (LongKeyComparer order-equivalent to single-key SortKeyComparer over
int/long/Date32/Timestamp/Time64 × ASC/DESC × nulls × ties; BuildUnboxedOrderKey carrier→unboxed /
non-carrier→null fallback) + `WindowAggregateMonomorphizeTests` (structural≡boxed≡mono≡mono·spine every
tick across running/bounded-RANGE/MIN·MAX/DESC/INTERVAL-over-TIMESTAMP/DATE/nullable/multi-spec at
W=1/2/4/8 + incremental≡batch PBT incl LATENESS GC) — whole suite **2099 pass**. Resolver constrains
window-agg ORDER BY to INT/BIGINT/DATE/TIME/TIMESTAMP (carriers), so the non-carrier fallback is a
defensive guard not a live aggregate path. Gate (b) competitive: **Nexmark has NO window-aggregate-with-
ORDER-BY query** (its OVER queries are all ROW_NUMBER→rank/TOP-K), so the target is the FRAUD rolling-window
feature view (`SUM/COUNT OVER PARTITION BY cust ORDER BY ts RANGE PRECEDING`, TIMESTAMP key). New
`windowmono` benchmark (`dotnet run -- windowmono [txns] [cust] [batch] [W] [runs]`, WindowMonoBenchmark.cs,
docs/window-mono-bench.md) A/Bs boxed-vs-mono byte-identical: **+13–35% step throughput W=1..14, up to
−40% alloc; win carries to W=14 (+21% @200k)**. Flipped `CompileOptions.MonomorphizeWindowOrderKey` default
→ true (boxed stays as fallback via explicit `= false` or non-carrier key). Honest caveat: near-best-case
(value-type key, large sorted partitions); string keys don't box, small partitions do few compares → real
mixed workloads see less; does NOT reach parity on wide rows (struct-copy residual = columnar). Engagement
diagnostic `TypedPlanCompiler.MonomorphizedWindowOrderKeyCount`. Committed+pushed.

**STOP-condition met** (for the program-wide typed adoption; the monomorphization is a separate, live lever)
(the arc's phase-3 gate). NOT "small prize declined" but architecturally wrong: shared-`object[]` is
already near-optimal at inter-view boundaries; closing typed-join residual coverage (§23.2) would type
MORE wide fact views → WORSE; typing never touches the Layer-A dict floor (§16.3) anyway. **Only viable
floor lever stays columnar/buffer-reuse on the Layer-A inner multiset ([[per-row-execution-efficiency]]
§17-21), which keeps the shared-reference model.** Gate+census+harness kept default-off for cheap
re-measure. The scoping below is the pre-run investigation (superseded by the result above).

**Confirmed facts:**
- The ivm-bench program (server + local harness) runs 100% on `StructuralRow` = `object?[]`
  (`StructuralRow.cs:34`). Every scalar is boxed; every op builds a fresh dict-Z-set of StructuralRow
  per tick; joins hash whole object[] rows. `CompileProgram` never invokes `TypedPlanCompiler`.
- A typed path EXISTS but only for: single-query `PlanToCircuit.Compile` (hybrid
  `TryCompileWithStructuralBoundary`, `PlanToCircuit.cs:180`) and the parallel Nexmark path. The
  program circuit is also single-threaded (plain foreach Step), so the parallel typed ingest doesn't
  apply either.
- **NO prior investigation** of typing the program path (docs + memories empty). Looks like a pragmatic
  omission — the ivm-bench arc was about SQL COVERAGE, not perf (same shape as the missing optimizer
  we just found + fixed in `6a8a9e3`).
- `TypedPlanCompiler` (~4000 lines) is **all-or-nothing per view** (any unsupported node →
  `UnsupportedPlanException` → whole view falls back to structural). Bails on: `PartitionedRankPlan`
  (rank-in-output, unconditional — 6 ivm-bench views, all small analytics), nullable equi-keys, various
  residual/outer-join shapes. Invokes stateful-op builders via REFLECTION ([[typed-compiler-reflection-gotcha]]).

**HONEST PRIZE CEILING (from [[per-row-execution-efficiency]] §16.3 apportionment):** typed rows attack
the **boxing / object[] / structural-boundary** cost (Layer B, ~40–45% of per-tuple alloc) but NOT the
**fresh-dict-Z-set-per-op-per-tick** (Layer A, ~55–60%, architectural — needs pooling or columnar). So a
fully-typed program might recover roughly the Layer-B fraction of the 45 GiB (~→ mid-20s GiB), NOT
eliminate the floor. Also unknown: per-view typed-with-structural-boundary pays object[]↔typed
conversion at EVERY view boundary in a 50-view chain — could eat the savings (the arc found the boundary
is a real cost). Nexmark single-query typed INGEST was measured at only +7–16% (§ retired), but the
program is a DEEP DAG where per-row cost compounds — genuinely untested, could differ either way.

**COVERAGE CENSUS RAN (2026-07-20, `TypedCoverageCensus.cs`, gated on IVM_SPEC) — DECISIVE:**
**19/50 views typed, 31 fell back; of the 13 HOT views only 4 typed** (trades, watches, daily_market,
fact_cash_balances). Bail reasons by node type:
- **JOIN coverage = THE hot-view blocker.** Nearly every hot fell-back is `JoinPlan` ALONE:
  fact_holdings, fact_watches, fact_trade, fact_cash_transactions, watches_history, holdings_history.
  Dump of fact_holdings shows a chain of INNER joins where one has **`residual=YES`** → the typed join
  compiler bails on RESIDUAL joins (TypedPlanCompiler.cs ~789-879, bail when residual present and not
  fuseResidual, and all LEFT/RIGHT-with-residual). These residuals are the SCD-2 TEMPORAL joins
  (`key AND ts BETWEEN lo AND hi`) — the same ones [[residual-pushdown-next]] handles structurally. So
  **closing typed-join residual coverage flips ~6 of 9 hot fell-backs.** (nullable equi-keys, line ~730,
  is a secondary join bail.)
- **PartitionedRankPlan** (rank-in-output, unconditional bail line 402): broker_performance,
  customer_concentration, daily_market_pulse, trade_volume_stats, market_volatility, financials — all
  SMALL-output analytics (low value).
- **SemiJoinPlan** (my new narrowing rule emits it; typed doesn't support it): same analytics views —
  but they'd bail on rank anyway.
- **WindowOffsetPlan / WindowAggregatePlan present** in accounts/companies/customers/securities/etc. but
  those ALSO have JoinPlan → join is likely the real bail (window agg/offset ARE typed-supported w/
  PARTITION BY per [[window-aggregates]]).
- **dim_trade** bailed with NO interesting nodes ("only scan/project/filter/cte") — an expression or
  nullable case in the typed expression/scan lift; worth a 10-min drill (should be typeable).
- **UnionAllPlan**: crm_customer_mgmt (branch row-type mismatch, line 910).

**Upshot:** coverage is TRACTABLE and concentrated — the single highest-value gap is **typed-join
RESIDUAL support** (unlocks the fact-table hot views); rank-in-output is the other big bucket but only
on low-value small views. So "typed program path" is not blocked by a scattered long tail; it's ~2-3
coverage features. The open question stays the PRIZE (Layer-B recovery minus per-view boundary cost),
which needs the gated measurement — the census only proves feasibility, not payoff.

**INVESTIGATION PHASES (design-first, measure-first):**
1. **Coverage census (decisive first datum):** instrument `CompileProgram` to ALSO attempt
   `TryCompileWithStructuralBoundary` per view (with other views' structural streams as StructuralScans)
   and log typed/fell-back + reason. Count typed-able views, ESPECIALLY the hot ones (fact_holdings,
   dim_trade, fact_watches, daily_market windows, watches aggregate). Small analytics views bailing on
   rank-in-output is FINE (low value). If the hot views bail too → the lever is small without closing
   gaps first.
2. **Wire typed-with-structural-boundary into CompileProgram** per typed-eligible view, gated. Measure
   batch-1 (local harness) — real prize vs the boundary cost.
3. **Boundary economics:** does typed-inner + per-view structural boundary net positive on the deep DAG,
   or does the boundary churn dominate? Compare vs the columnar alternative (attacks both layers).
4. Close highest-value coverage gaps only if a HOT view bails on one specific construct.

**Meta (session/model):** do this as a FRESH session, design-doc-first (it's a distinct
row-representation arc, not the algorithmic-wins arc). The coverage census (phase 1) is mechanical and
subagent-friendly. Reuse the local harness ([[ivm-bench-batch1-perf-gap]], `IvmBatchProfile` +
`BrokerPlanDump`) — the fast Docker-free loop makes measuring real. Relates to
[[row-representation-design]], [[per-row-execution-efficiency]].
