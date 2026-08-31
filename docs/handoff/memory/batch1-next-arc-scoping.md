---
name: batch1-next-arc-scoping
description: Scoping for the next ivm-bench batch-1 perf arc — DbspNet is now ~3.5x Feldera (was 7x); where the remaining allocation-bound gap is and the ranked levers to try
metadata: 
  node_type: memory
  type: project
  originSessionId: d14a0364-69f8-416a-9281-8409e145e614
  modified: 2026-07-21T23:03:18.781Z
---

**Scoping note for the NEXT arc (design §24, written 2026-07-21). Run as a FRESH, design-first session.**

**State:** after the program-path optimizer (`6a8a9e3`), residual pushdown ([[residual-pushdown-next]]),
and the algorithmic wins, DbspNet is **~3.5× Feldera's time on SF=3 batch-1** (bulk historical load),
down from ~7× ([[ivm-bench-batch1-perf-gap]]). The wins that moved it were ALGORITHMIC; the per-row
micro-levers (typed ingest, barrier coalescing, W-sizing, §19, §20) all held flat/negative on the real
benchmark — the standing measure-first caveat.

**Where the gap lives:** engine is ALLOCATION-bound. Apportioned (§16–§17) as **Layer A ~55–60%** (fresh
dict-Z-set per op per tick + whole-row hash — architectural, untouchable by typing per §23) + **Layer B
~40–45%** (object[]/StructuralRow boxing — already attacked; full removal only reaches ~mid-20s GiB). Real
remaining prize = **Layer A**; only levers that keep the shared-object[]-by-ref model (§23's finding) are
columnar / buffer-reuse on the inner multiset.

**RE-PROFILE DONE (2026-07-21, HEAD 3f2c1a4, local `IvmBatchProfile`).** ServerGC wall **59.5s**
(≈ the 60.1s recorded at 6a8a9e3 → Docker-equiv ~71s → **~3.4:1 vs Feldera 21s, STABLE, no regression**);
alloc **44.74 GiB** (= 44.7 at 6a8a9e3 → optimizer+residual-pushdown intact). Wall is GC-mode-sensitive:
workstation GC = 91.5s / 2283 gen0; ServerGC = 59.5s / 72 gen0 — **always run the local loop with
`DOTNET_gcServer=1` for a Docker-faithful wall; trust ALLOC + op-ranking, not workstation wall.** Phases:
engine step 87%, ingest 2%+7.3%, output ~3.7%. **Fresh ranking (broker_performance's 9.9s blow-up GONE —
semi-join narrowing holds; NO algorithmic blow-ups left):** cost now spread across window/agg/join/apply
over the 4 big fact tables — trades WindowAggregate op210 5.9s (state=982180, whole-partition MIN/MAX per
trade_id), daily_market running windows op348+350 6.8s (residual after the O(n) fix, 1.28M rows),
watches IncrementalAggregate op292 4.3s (state=886K, 6-col composite GROUP BY MIN/MAX) + FIVE watches
ApplyOps ~6.2s, fact_holdings joins ~5.5s, market_volatility WindowOffset op398 2.2s, trades_history
windows+joins ~6.5s. **The ApplyOp/row-map tail ≈ 24% of engine step (top-30 ApplyOps sum ~18.9s)** —
pure whole-row object[] rebuild + ComputeHash-at-construction = the per-row/alloc floor.
**Two levers RE-RANKED by the fresh profile:**
- **Lever #4 (memoized hash) RETIRED** — `StructuralRow._hash` is ALREADY cached at construction, Equals
  short-circuits on it, pure-filter `MapFilterRows` already passes the row through by ref (no realloc/rehash).
  No cheap win. The whole-row-hash cost is paid at row CONSTRUCTION (projections build fresh rows) → this
  REINFORCES columnar/buffer-reuse as the prize, doesn't undercut it.
- **Lever #2 (bulk-load fast path) DE-WEIGHTED** — ingest is only ~10% (read 2% + push 7.3%); the 87%
  engine step is DERIVED-view compute (windows/aggs/joins) that a bulk initial-arrangement build wouldn't
  touch. Limited upside.
→ **Lever #3 (columnar / buffer-reuse inner multiset) is now unambiguously THE prize** (44.74 GiB alloc
  floor + 24% row-rebuild + per-row window/agg/join at 4-6µs/row all point there). See
  [[row-representation-design]]. Mid-effort probe worth a look first: narrow/reuse in the ApplyOp tail.

**PER-OP ALLOC MEASURED (2026-07-21) — instrumented the batch profiler (per-op + per-KIND alloc via
`GC.GetAllocatedBytesForCurrentThread` in `RootCircuit.StepProfiled`, exact bc engine step is serial
single-thread; kept, zero-cost when ProfileOperators off). Batch-1 alloc BY KIND:**
- **ApplyOp x310 = 17.7 GiB = 47.3% of alloc (AND 47.2% of step time) — THE single largest source**, far
  bigger than the top-30-time estimate (24%). It's the head, not the tail.
- IncrementalInnerJoin x37 = 10.3 GiB (27.4%); WindowAggregate x30 = 4.9 GiB (13.1%); IncrementalAggregate
  x11 = 2.8 GiB (7.5%); WindowOffset x6 = 1.36 GiB (3.6%). Rest <1%.

**ApplyOp per-row alloc APPORTIONED (`ApplyOpAllocSplit` microbench, IVM_MICRO=1, reproduces the exact
MapFilterRows inner loop, 14→12 col + 1 computed, differenced E/F/I/P):**
- **(a) output container + dict entries = 31.5 B/row = 15.2%** — columnar STORAGE captures this. SMALL.
- **(b) per-row object[] + StructuralRow = 152 B/row = 73.2%** — the PRIZE. Captured ONLY by vectorized
  column-WRITE (materialize output columns, never build a StructuralRow); columnar STORAGE alone does NOT
  capture it (a row still gets built to feed the row-lambda — with columnar INPUT it'd even add a read-side
  materialize).
- **(c) compute/boxing = 24 B/row = 11.6%** — stranded; needs vectorized expression EVAL (compute over
  column vectors into primitive cols, no boxing).
- enumeration = 0 (struct enumerator, free).

**CORRECTION to the earlier hand-wave: columnar-STORAGE-alone captures only (a)=15% of the ApplyOp tail
≈ ~7% of TOTAL alloc. The 85% that matters (b+c) is behind VECTORIZED EXECUTION — a bigger, different
project than a columnar container.** So "land columnar inner-rep" as STORAGE would NOT obsolete ApplyOp
row-build cost; the two are complementary, and the real ApplyOp win requires vectorized column-write/eval.
**Cheap lever this surfaces (needs NEITHER columnar nor vectorization): dead-column elimination ACROSS the
view DAG** — (b) scales linearly with out_width; today column pruning fires only THROUGH joins
([[join-column-pruning]]), not across view→view→output chains. Narrowing a 14-col row that needs 8
downstream ≈ −40% on the 73% term. Ranked as the next probe to scope.

**INSTRUMENTATION COMMITTED+PUSHED (16e7d76): per-op + per-KIND alloc in the batch profiler.** (Scratch
`ApplyOpAllocSplit.cs` microbench kept LOCAL/uncommitted per Scratch convention, gated IVM_MICRO=1.)

**DAG DEAD-COLUMN INVESTIGATION (2026-07-21) — found a bigger prize than the (b)-term shrink.** Built the
view→consumer graph from the spec. Current pruning reach: (i) `PlanOptimizer` is a PER-VIEW tree rewrite —
local `Project(Join)`/`Aggregate(Join)` narrowing only ([[join-column-pruning]]); window/offset/TOP-K are
HARD barriers (no input-narrowing through them); the general top-down column-liveness pass is STILL
deferred (`PlanOptimizer.cs:38`). (ii) `CompileProgram` prunes dead VIEWS (reachability from outputs) but
**VIEW-granular, not COLUMN-granular**. The gap = column-liveness across the view DAG.
**HEADLINE FINDING — `daily_market`'s `fifty_two_week_low/high` + `..._low_date/high_date` (4 cols) are
DEAD but fully computed.** They're read ONLY by `fact_market_history`, which is a dead-pruned leaf
(consumed_by none, not an output); `daily_market`'s only LIVE consumer `market_volatility` reads just
dm_close/date/high/low/s_symb/vol (verified: market_volatility never mentions fifty_two_week). So two whole
WindowAggregate ops compute nothing live: **op348 (running MIN/MAX 52wk low/high) 4.6s/1620 MiB + op350
(max-flag 52wk dates) 2.3s/1382 MiB + op349 flag-CASE ApplyOp ~1.0s/390 MiB ≈ 6.9–7.9s and ~3.4 GiB =
7.6% of the 44.74 GiB (and 61% of ALL WindowAggregate alloc is these two dead ops).** Pure dead-code
elimination, correctness-preserving (dropping cols no live consumer reads can't change any output),
config-driven exactly like the existing dead-view prune. **This is an ALGORITHMIC-class win, NOT a per-row
shrink — the DAG lever's real value is eliminating whole dead sub-computations (window ops), not just the
(b) row-width term.** Smaller row-narrowing also found: trades_history `trade_timestamp`+`update_status`
dead (but `end_timestamp`/`is_current` LIVE via dim_trade → op206 WindowOffset stays).
**→ Recommended next build: COLUMN-granular liveness pass at program compile — extend CompileProgram's
dead-view reachability (CollectScans) to propagate per-view output-column liveness, then prune a view's
plan expressions/ops that produce ONLY dead columns.** Two sub-levers: (a) column-granular dead-view-output
elim = the daily_market ~3.4 GiB win (high value, clean); (b) live-row narrowing through window/offset
barriers = the (b)-term shrink (smaller). Do (a) first.

**DESIGN + ANALYSIS LANDED (2026-07-21) — `docs/design-column-liveness.md` written; analysis half BUILT +
VALIDATED (rewrite NOT built).** New production `src/DbspNet.Sql/Optimizer/PlanColumnLiveness.cs`:
`LiveScanColumns(plan, liveOut)` = backward visitor mirroring CollectScans' switch (Project/Filter/Join/
Aggregate/Window*/Union/Distinct precise; TopK/Semi/Scalar/Correlated/Temporal/RecursiveCte CONSERVATIVE=
all-input-live) + `ComputeProgramLiveColumns(views)` backward-topo glue. **Conservative fallback = soundness:
over-approximating liveness can only under-prune, never unsound → model hot-path kinds, generalize node-by-
node safely.** Window/offset "if no produced col live → contribute nothing" branch = the producer-dead
elimination signal. Gated diagnostic `tests/.../Scratch/ColumnLivenessProbe.cs` (IVM_SPEC, analysis-only, no
mutation, LOCAL/uncommitted) RAN vs the live compiler: **CONFIRMED daily_market 4/10 dead = exactly
[fifty_two_week_low/high, ..._low_date/high_date]** (the op348+350 ~3.4 GiB). Also: 5 whole dead views
(already CompileProgram-pruned: daily_market_pulse + finwire_financial→financials→wrk_company_financials→
fact_market_history chain); **13 views / 64 dead cols total** (accounts 23/32, syndicated_prospect 14/22,
watches 5/9, trades 4/15, ...); all 16 OUTPUT views fully live (seeding sanity ✓).
**TWO PAYOFF CLASSES (design §4): (i) producer-dead → OP ELIMINATION** (dead col is a produced window/offset
VALUE → drop the op; daily_market = this) vs **(ii) passthrough/key-dead → ROW NARROWING only**
(dead col is passthrough or a GROUP BY/DISTINCT key surfaced to output — e.g. watches' company_id etc. are
dead-for-output but LIVE inside the aggregate; can't drop the key, only stop surfacing it). Analysis handles
both soundly (AggregatePlan keeps all group keys live in the demand).

**REWRITE BUILT + MEASURED — arity-preserving = a WASH; the real prize is arity REDUCTION (design §9/§10).**
Shipped gated `CompileOptions.EliminateDeadColumns` (default OFF) + `PlanColumnLiveness.PruneDeadColumns`
(arity-preserving: dead col → NULL const, producer-dead window/offset → constant-filling Project;
single-ref CTE bodies pruned, multi-ref left intact). Unit tests `ColumnLivenessRewriteTests` GREEN
(producer-dead window elim + GROUP BY-key + DISTINCT-key soundness — the distinct case proves we DON'T
over-dedup). Batch-1 A/B (IVM_DEAD_COLS=1, local, ServerGC): **all 16 output row counts BYTE-IDENTICAL
(sound); daily_market's 2 window ops eliminated as predicted (WindowAggregate 4.9→1.86 GiB, x30→x28). BUT
net alloc only 44.74→43.72 GiB (−1.0 GiB), WALL FLAT (59.5→60.9s) — far below the ~3.4 GiB projection.**
ROOT CAUSE (measure-first correction): arity preservation RE-MATERIALIZES dead cols as NULL constants → row
WIDTH unchanged → the (b) row-materialization term (73% of ApplyOp alloc) is untouched; the replacement
constant-Projects re-pay row materialization (ApplyOp +2.5 GiB, x310→x335, +fusion perturbation), so
removing the window COMPUTE (~3 GiB, mostly its OUTPUT-row materialization not state) nets only ~1 GiB.
**CONFIRMS the a/b/c apportionment empirically: the prize is the (b) row-WIDTH term → captured only by
NARROWING rows = arity REDUCTION (true col removal + downstream ScanPlan reindexing), NOT arity-preserving
elim.** DECISION: don't ship EliminateDeadColumns as perf (correct but marginal); keep gated as foundation.
**ARITY-REDUCING REWRITE — CEILING ESTIMATED, NOT BUILT (design §10, measure-first stop).** Before
building the invasive cross-view reindexing, estimated the (b)-term prize = Σ(view rows × dead-passthrough
cols × ~2 passes × ~12B) = **26.5M cell-passes ≈ 0.3 GiB (≤ ~1 GiB even at 3-4× generous), ~1-2% of the
44.74 GiB batch, WALL-NEUTRAL.** ADVERSARIAL CORRELATION: the wide-dead-col views (accounts 23/32,
syndicated_prospect 14/22) are LOW-VOLUME dimensions (~11-15K rows → 0.5M/0.4M cell-passes); the
high-volume fact views (watches/trades/holdings_history 886K-982K rows) have only 2-5 dead cols each.
Dead cols are a small, adversarially-placed fraction of the row-material → pruning them barely dents (b).
**DECISION: do NOT build it — invasive/risky reindexing for ≤~1 GiB wall-neutral isn't justified.**

**ARC CONCLUSION (column liveness): DONE as a validated tool + modest cleanup, NOT a gap-mover.** Committed:
analysis (7d6b3b7) + gated arity-preserving rewrite/tests/doc (2afa69b). Both forms of dead-col elim top out
~1-2 GiB (~4%), wall-neutral, because the dead fraction of row-material is small + adversarially distributed.
**The re-profile's REAL prize — ApplyOp 47% / (b) row-materialization of ALL (mostly LIVE) rows — is
reachable only by the columnar/vectorized-execution rewrite ([[row-representation-design]]), a big project
with its own ~1.3-2× (not parity) ceiling + the .NET-vs-Rust floor.** Batch-1 (~3.4:1 vs Feldera) is near
its practical floor for cheap/medium levers; further real movement needs the big rewrite or is inherent.

**COLUMNAR-FOR-BATCH-1 DESIGN WRITTEN (2026-07-21, `docs/design-columnar-batch1.md`, design-only).** Answers
"is the big columnar rewrite documented?" → PARTLY. The Nexmark arc (design-row-representation.md §17 +
`next-arc-columnar-prompt.md`) documents target **A = columnar inner-multiset STATE** (join+agg IndexedZSet,
~35% alloc) — but on batch-1 A is LARGELY PRE-CAPTURED: §21 JoinColumnPruning is DEFAULT-ON so join stored
rows already narrowed → columnar-only residual is small (§21.3). The REAL batch-1 prize is **B = columnar
inter-OPERATOR interface** (the ApplyOp 47% row flow; row=universal inter-op interface so B = rewrite EVERY
op = XL). MEASURED B ceiling (extended `ApplyOpAllocSplit` with SoA object-array variant): columnarizing an
ApplyOp removes **38% of its alloc → ~18% of batch (~8 GiB)** — reclaims StructuralRow wrappers+per-row
hash+object[] headers but NOT boxing (object-cols still box; typed long[] cols needed to remove it = even
bigger typed+columnar rewrite). B is undocumented (this doc opens it). PATH MISMATCH: batch-1 is STRUCTURAL
SERIAL, the Nexmark columnar tooling is TYPED PARALLEL (reprbench/w1profile/q4* + ThreadStatic-for-typed
seams) → needs re-scope to StructuralRow + IvmBatchProfile gate. INHERITED CONSTRAINTS (must carry): spine
sorted-merge LOST (§8.3) → columnar=STORAGE+flat-hash-O(1)-probe+buffer-reuse NOT sorted-merge; buffer reuse
proven safe (§20); ceiling ~1.3-2× not parity (§17.5). **RECOMMENDED FIRST INCREMENT (§6): columnarize ONE
hot pure-apply view's chain internally (watches 886K/5-applies or daily_market 1.28M), materialize
StructuralRows only at output boundary, `[ThreadStatic] ColumnarApplyMode` seam, gate = IvmBatchProfile 16
outputs byte-identical + view ApplyOp alloc→−38% + no wall regress, RETIRE-IF-LOSES.** §7 open measurements
first: confirm single-view ceiling on real data, price the output-boundary re-materialization (win may need
ADJACENT views columnar to defer materialization), typed-column upside.

**§7 MEASUREMENTS RAN (2026-07-21, HEAD ce8c7de, local IvmBatchProfile reproduced baseline 44.73 GiB /
60.6s ServerGC — intact, no regression). Spec regen: `python dbt_to_program.py <ivm-bench>/src/containers/
dbt-server/dbt-projects/dbspnet` → scratchpad/ivm_spec.json; dbt_to_program.py lives ONLY on ivm-bench
branch `dbspnet-engine-experiments` (not main); data at D:/ivm-data/raw/3/delta.**
- **§7 #1 single-view ceiling on REAL data — CONFIRMED + RAISED.** Extended ApplyOpAllocSplit with the real
  `watches` s1 shape (8→10, strings/DateTimes, 2 CASE-passthrough cols). Real string-heavy views columnarize
  **−47.5%/ApplyOp** (vs synthetic numeric −38.3%) → ~22.5% batch ceiling, because their computed cols are CASE
  ref-passthroughs (`when 'Activate' then watch_timestamp else null`) = **ZERO (c) boxing**, so object-columns
  strand nothing; (b) row-materialization = 81% of the op. String/SCD/dimension views = the BEST columnar case.
- **§7 #2 output-boundary materialization — RESOLVED NEGATIVE for the single-view increment (STRUCTURAL, not
  measurable-away).** `CompileLinearChain` (PlanToCircuit.cs:1631) fuses a MAXIMAL run of consecutive Filter/
  Project into ONE op → **contiguous pure-apply regions have length 1**; every ApplyOp sits between materializing
  barriers (join/agg/window/output/fork). A lone ApplyOp MUST materialize StructuralRows for its barrier-consumer
  regardless → columnarizing it alone ADDS a col round-trip and saves ZERO = **strictly worse**. The 47% ApplyOp
  alloc is the cost of feeding rows INTO barrier ops; reclaimable only if joins/aggregates/windows consume columns
  directly = whole-engine. **§6's "columnarize one view's apply-chain" first increment is FALSIFIED — RETIRED in
  the design doc; do not build.** Smallest increment that can show a POSITIVE number = one barrier op (e.g. watches
  agg op292=2.0 GiB, or a fact-join) + its neighbors columnar — bigger brick, commits columnar batch as inter-op type.
- **§7 #3 typed-column upside — NOT RUN** (string tail has ~0 boxing residual → typed adds nothing there; numeric
  views' 11.6% boxing residual is typed+columnar engine territory, gated behind the object-column go/no-go).
**BARRIER-SLICE RECLAIMABILITY MICROBENCH RAN (2026-07-21, `JoinBarrierSlice.cs`, IVM_MICRO=1) — DIRECTION-CHANGING,
the "bounded validating increment" Curt chose is shown UN-GATEABLE by its own de-risk step.** Reuses the REAL join
kernel (`IncrementalJoinCore.JoinInto`) + REAL `IndexedZSetTrace` over the watches_history slice (proj→JOIN(securities)
→proj, 900K rows, 20 ticks); ROW vs COL differ ONLY in the output sink; invariant trace floor measured separately.
Result: single barrier slice saves **17.7% (unpooled fused) / 32.7% (pooled cols §20)** — NOT the −47.5% pure-projection
ceiling — because (1) **33% of the slice is representation-invariant trace-integrate** (uncapturable w/o a columnar
TRACE = target A, bigger, mostly pre-captured by §21 pruning) + (2) **boundary re-materializes rows once** (row-based
consumer). Cleanest single slice (1.28 GiB) moves the batch **0.15–0.28% = below wall-noise → UN-GATEABLE as a bounded
increment**; a gateable ~2-3% signal needs ~15% of the batch (~7 GiB joins+projs) columnarized AT ONCE = most of the XL
rewrite. **Whole-engine ceiling REVISED DOWN: ~10-13% alloc (was 18-22%), ~5-10% wall — because join(27%)+agg(13%)+
window(13%) alloc is dominated by internal trace/accumulator state that output-columnarization doesn't touch.**
**RECOMMENDATION (design §8): DECLARE batch-1 at practical floor (~3.4:1), close the columnar arc as
DESIGNED-AND-MEASURED-NOT-BUILT.** No bounded gateable increment exists; the full rewrite is multi-session for ~5-10%
wall at 1.3-2× not parity, on the ONE-TIME batch-1 load (IVM's real value = incremental batches 2/3, already competitive,
cost there = output I/O not engine). Microbench artefacts (`ApplyOpAllocSplit` 2-scenario, `JoinBarrierSlice`) + design
doc §6-§8 are the recorded decision. **AWAITING Curt's final call: stop (recommended) vs commit to the XL rewrite anyway.**

**STRUCTURAL-PARALLEL INVESTIGATION (2026-07-21, Curt's "is competitive plausible?" strategic Q).** Two findings:
- **(1) Exchange/parallel substrate is ALREADY generic over StructuralRow — NO typing needed.** `ExchangeOp<TKey,TWeight>`,
  `CircuitBuilder.Exchange`/`ExchangeIndex`, `ParallelCircuit.Build`/`ShardedInput`/`ShardedOutput` are all `<TKey>`-generic;
  the typed-ness lives ONLY in `TypedPlanCompiler`'s exchange-INSERTION pass. Structural-parallel needs NO new runtime ops —
  only port the insertion strategy (shard scans, shuffle-by-key before joins/aggs/distinct, propagate partitioning) to
  `PlanToCircuit`/`CompileProgram` over StructuralRow with `Func<StructuralRow,int>` key-hash (`StablePartitionHash` exists).
  AVOIDS the §23 typing penalty (Exchange shuffles StructuralRow REFS, no decode/encode). `PlanToCircuit` currently inserts
  ZERO exchanges (single-circuit only).
- **(2) SCALING PROBE (`ParallelScalingProbe.cs`, ServerGC, 24 procs, real JoinInto+trace+StructuralRow over disjoint shards,
  4.5M rows, NO exchange = best-case ceiling): structural join+proj scales only ~3.07× at W=8 (efficiency 100/82/62/38% at
  W=1/2/4/8). MEMORY-BANDWIDTH bound, NOT GC-bound (GC pause only 20-25% of wall).** Output-only columnar sink (pooled cols,
  1.15 vs 1.97 GiB): **1.35-1.65× FASTER absolute at every W but SAME ~2.5× scaling** — reducing OUTPUT alloc does NOT improve
  the scaling factor because the bandwidth bottleneck is the INPUT-side object[] join match (common to both reps). Compound
  thesis (low-alloc→better scaling) REFUTED for output-columnar; the real bandwidth cost is INPUT pointer-chasing/boxing,
  fixable only by typed/columnar INPUT + columnar trace (= full rewrite, untested here).
- **SYNTHESIS: the parallel ceiling is REPRESENTATION-DETERMINED (bandwidth).** object[]/boxed rows waste bandwidth (pointers,
  headers, boxing) → slower serial AND saturate sooner in parallel (~3× wall). Feldera's compact columnar Rust is
  bandwidth-efficient → faster serial AND scales further. So levers aren't independent: bandwidth-efficient rep RAISES the
  parallel ceiling; parallelism MULTIPLIES the rep win. Batch-1 est: engine 51s /3× ≈ 17s → total ~26s vs Feldera 21s = ~80%
  (optimistic, no exchange tax; realized 2.5× → ~72%). **Competitive (~50-80%) is REACHABLE on batch-1 but marginal and needs
  structural-parallel at minimum; comfortable needs rep work too.** PIVOTAL UNKNOWN — **RESOLVED: Feldera's 21s is
  12-WORKER PARALLEL** (ivm-bench `src/containers/dbt-server/dbt-projects/feldera/profiles.yml`: `workers: 12`,
  `compilation_profile: optimized`, `dev_tweaks.adaptive_joins: true`; `threads: 1` = dbt concurrency not runtime). So
  "they're serial, just parallelize and win" is OFF — we race their bandwidth-efficient 12-worker parallel vs our serial;
  structural-parallel ~3× → ~26s ≈ 80% of 21s (optimistic). **NEXT decisive measurement: Feldera batch-1 at workers=1/2/4/8
  for THEIR scaling curve** — if only ~3-4× (bandwidth-saturating, cf Nexmark negative-at-high-W), their single-core is
  ~60-85s ≈ our serial 60s → parallel-vs-parallel winnable; if ~8-10×, rep edge large → need rewrite. Needs Docker/WSL
  Feldera run (edit workers, re-run; ~295s Rust compile if uncached).
  Probes `ParallelScalingProbe`/`JoinBarrierSlice`/`ApplyOpAllocSplit` LOCAL/uncommitted. Windows/aggs (compute-denser) may
  scale better than this bandwidth-heavy join → the ~3× is a lower-ish single-op estimate, needs real-DAG validation.

**FELDERA SCALING CURVE MEASURED — DECISIVE (2026-07-21, WSL session drove Docker directly).** Ran the
faithful OAT harness (`feldera-only.json`, SF=3 batch-1, `duration_s` from `run-feldera-batch1.json`) at
workers=1/2/4/8, this i9-12900K box (8P+8E). `workers` is `runtime_config` → no recompile per-worker, but
harness recompiles ~290s per sweep regardless (no compile-cache volume + teardown); doesn't pollute
`duration_s`. Curve: **W1 56.07s / W2 34.36s (1.63×) / W4 23.93s (2.34×) / W8 18.56s (3.02×, PEAK) / W12
20.47s (2.74× — NEGATIVE past knee, −10% vs W8)**. Efficiency 100/82/59/38/23%.
**TWO decisive facts:** (1) **Feldera single-core 56.07s ≈ dbsp-net serial ~59.5s (local ServerGC)** — its
batch-1 per-row rep edge is only ~1.06×, NOT the 2–5× it holds on Nexmark; the algorithmic wins already
closed the per-row gap → **columnar/rep rewrite buys ~nothing on batch-1, serial is already ~parity.**
(2) Feldera saturates ~3× knee W=8, goes NEGATIVE at W=12 (oversubscribing 8 P-cores) — same synchronous-BSP
bandwidth+coordination wall as our exchange arc (§15); we're W-insensitive (§15.8) so we'd dodge that hit.
**ANSWER to Curt's "is competitive plausible?": YES, and via the cheaper lever.** Batch-1 competitiveness is
a PARALLELISM-implementation gap, not a representation gap. dbsp-net structural-parallel at realized 2.5–3×
→ ~59.5s → ~20–24s = 77–92% of Feldera's peak 18.56s, up to parity vs its configured 20.47s → in the
50–80%+ band, plausibly parity, WITHOUT columnar. **RECOMMENDED BUILD: the structural-parallel
exchange-insertion pass in `PlanToCircuit` (no new runtime ops, avoids §23 typing).** Columnar stays
DESIGNED-AND-MEASURED-NOT-BUILT (only a *further* multiplier once parallel lands — output-columnar alone
does NOT lift the scaling factor per the probe; INPUT-side object[] bandwidth is the bottleneck). OPEN
number the build must validate: real-DAG structural scaling (SCD-2 temporal joins, wide window aggs, skew,
exchange tax) — probe's 3.07× is single best-case disjoint-shard; real may be 2–2.5× → ~24–30s → 62–77%.
Reframe folded into `docs/design-columnar-batch1.md` §8 (pointer) + new §9, plus new
`docs/design-structural-parallel.md` (the build scoping: insertion points in `PlanToCircuit.cs`, the one
new piece = a StructuralRow-slot `StablePartitionHash.OfBoxed`, gate-first 3-increment plan).
**COMMITTED+PUSHED (Curt pushed 2026-07-21):** dbsp-net `548ec4e` on main (both design docs);
ivm-bench `6e4eba4` on `dbspnet-engine-experiments` (`feldera-only.json`). Scaling CSV + per-W result JSONs
were in the WSL session scratchpad (transient). **NEXT ARC = build `design-structural-parallel.md`
Increment 0→2; Increment 2's W-sweep on `IvmBatchProfile` lands the real-DAG scaling factor (the open
number behind the 77–92% projection).**

**Ranked levers (all measure-first):**
1. **RE-PROFILE batch-1 FIRST — DONE (see above).** Docker-free `IvmBatchProfile` loop, `DOTNET_gcServer=1`.
2. **Bulk-load / batch-construction fast path (the NEW idea).** Batch-1 is a one-shot historical load (a
   batch, not a stream) but flows through the incremental machinery paying per-op dict-merge churn for
   millions of rows. Feldera bulk-builds arrangements sorted, once. Design Q: can the initial load build
   traces directly (sorted bulk build) vs dict-merging a giant delta? Workload-shaped, targets the exact
   ivm-bench metric, plausibly lower effort than columnar.
3. **Columnar / buffer-reuse Layer-A inner multiset** — the arc every row-rep memory points at; §20 delta
   pooling de-risked it. Biggest prize/effort. Naive spine SUBSTRATE lost 1.4–2.5× at W=24 → needs a
   genuinely columnar inner rep, NOT a trace swap ([[row-representation-design]]).
4. **Memoized row hash (cheap probe)** — StructuralRow is hashed repeatedly (join probe + agg + distinct).
   If GetHashCode isn't memoized, caching it is a small broad cut against the 40–48% whole-row-hash term.
   ~30 min check + measure before the big arcs.

**Sequencing:** (1) re-profile → weight workload-shaped/quick levers (2, 4) ahead of the columnar rewrite
(3); let the numbers pick. Relates to [[per-row-execution-efficiency]], [[repr-execution-apportionment]],
[[ivm-bench-arc]], [[typed-program-path-scope]].
