# Design: columnar execution for ivm-bench batch-1 — target justification & first increment

Status: **design-only; opens at the A-vs-B target decision with measured numbers.**
Companion to `docs/design-row-representation.md` (the Nexmark row-rep arc, §5–§24) and
`docs/next-arc-columnar-prompt.md` (the columnar/arena kickoff, Nexmark-scoped). This doc
re-justifies the columnar target on the **ivm-bench batch-1** workload specifically — a
different shape (wide SCD rows, 50-view program, **structural single-threaded** path) than
the typed-parallel Nexmark queries the prior arc measured.

## 0. TL;DR

The batch-1 re-profile localized allocation to two candidate columnar targets. Measured:

- **A — columnar inner-multiset state** (join + aggregate `IndexedZSet`): ~35% of alloc, but
  the join stored rows are **already narrowed** (§21 `JoinColumnPruning`, default-on) — so A's
  columnar-only residual is the small sub-narrow-dict part. Contained (op-local), documented,
  but **little batch-1 prize left.**
- **B — columnar inter-operator interface** (the `ZSet<StructuralRow>` row flow the map/
  filter/project ApplyOps produce): 47% of alloc, the biggest single term. Measured ceiling:
  an object-array SoA batch removes **38% of an ApplyOp's allocation → ~18% of the batch
  (~8 GiB)** — the `StructuralRow` wrappers, per-row hashes, and object[] headers. But it is
  the **XL whole-engine rewrite**: the row is the *universal* inter-op interface, so
  columnarizing it means every operator changes.

**Decision: the real batch-1 prize is B, and it is XL.** A is contained but largely
pre-captured. The recommended path is a **single benchmark-gated first increment that
validates B's ceiling on one view's apply-chain before committing to the whole-engine
rewrite** — retire-if-it-loses, exactly as every prior increment in the companion doc.

> **⚠️ Superseded by §9 (2026-07-21):** §7 falsified even that first increment (fusion invariant), and
> §9 reframes the whole arc — the measured Feldera scaling curve shows batch-1 is a **parallelism** gap
> (Feldera single-core ≈ ours), not a representation gap. The recommended build is now the
> structural-parallel exchange pass (§9), not columnar. Columnar = DESIGNED-AND-MEASURED-NOT-BUILT.

## 1. The measurement (batch-1, HEAD `b510343`, per-op alloc instrumentation `16e7d76`)

ServerGC wall ~59.5s, 44.74 GiB allocated, engine step 87%. Allocation by operator kind:

| kind | count | alloc | % |
|---|---|---|---|
| **ApplyOp** (map/filter/project) | 310 | 17.7 GiB | **47.3%** |
| IncrementalInnerJoin | 37 | 10.3 GiB | 27.4% |
| WindowAggregate | 30 | 4.9 GiB | 13.1% |
| IncrementalAggregate | 11 | 2.8 GiB | 7.5% |
| WindowOffset | 6 | 1.36 GiB | 3.6% |

ApplyOp per-row apportionment (`ApplyOpAllocSplit` microbench, 14→12 col + 1 computed):

- (a) fresh output container + dict entries — **15.2%**
- (b) per-row `object[]` + `StructuralRow` + hash — **73.2%**
- (c) compute / boxing — **11.6%**

## 2. The two candidate targets

### 2.1 Target A — columnar inner-multiset state (the documented §17 #3)

`docs/next-arc-columnar-prompt.md` scopes this: fold a stateful op's inner `IndexedZSet`
(join stored side, aggregate per-group multiset) into a per-column arena buffer with an
O(1) hash probe (**not** sorted-merge — see §4). It attacks the join (27%) + aggregate
(7.5%) inner-state term-2 (whole-row hash of stored rows).

**But on batch-1 it is largely pre-captured.** §21's join column-pruning is **default-on** and
already narrows every join's stored rows to the columns the join/residual/equi-key/parent
read. §21.3 measured that narrowing to a still-flat dict recovers the entire 40–58% term-2
prize; *"a columnar SoA inner multiset could only chase the residual below the narrow-dict
line."* So on batch-1 (pruning already applied), A's marginal columnar prize is that small
residual, plus the aggregate inner rows (§18 narrowing is opt-in, not default — a cheaper
lever than columnar if it applies). **A is contained and documented but leaves little batch-1
allocation on the table.**

### 2.2 Target B — columnar inter-operator interface (the ApplyOp row flow)

The 47% ApplyOp term is not inner state — it is the **rows flowing between operators.** Every
operator consumes and produces `ZSet<StructuralRow>`; the (b) sub-term (73% of ApplyOp ≈ 34%
of the whole batch) is the fresh `object[]` + `StructuralRow` + hash a projection builds per
output row. This is the single largest reducible term in the batch.

**The architectural crux: the row is the *universal* inter-op interface.** Attacking (b) means
replacing `ZSet<StructuralRow>` with columnar batches as the interface *between* operators — so
**every** operator (map, filter, project, join, aggregate, window, distinct, output) must read
and write columns. That is the whole execution engine, not one operator. This is the "XL
columnar rewrite" the companion doc repeatedly defers.

### 2.3 B's ceiling, measured

`ApplyOpAllocSplit` was extended with a columnar (SoA) variant of the same projection —
per-column `object?[]` arrays + a `long[]` weight array, no per-row `StructuralRow`, no per-row
hash:

| variant | B/row | |
|---|---|---|
| P (row-wise, current) | 207.5 | |
| COL (columnar SoA, object arrays) | 128.0 | **−38.3%** |

**Columnarizing the inter-op interface with object-array columns removes ~38% of ApplyOp
allocation → ~18% of the batch (~8 GiB).** It reclaims the `StructuralRow` wrappers (~32 B/row),
the per-row hash, and object[] headers — but **not the boxing**: the column cells still hold
boxed values (`object?`), so a computed `long` and every passthrough value stays boxed. Removing
that requires **typed** columns (`long[]`/typed arrays), which is the typed-row path (§23 found
typing the inter-view boundary net-negative because structural shares `object[]` by reference)
combined with columnar — a still larger rewrite whose upside is the residual boxing term.

So the honest ceiling ladder for B:
- object-array columnar interface: **~18% of batch alloc** (~8 GiB), medium-large rewrite.
- typed columnar interface: more (removes boxing) but = the maximal typed+columnar engine.

## 3. The A-vs-B decision

| | A: inner-state columnar | B: columnar inter-op interface |
|---|---|---|
| surface | join 27% + agg 7.5% | ApplyOp 47% (+ all ops carry rows) |
| measured reducible | small residual (join pre-narrowed by §21) | ~18% of batch (object cols) |
| scope | op-local, keeps row interface | every operator (XL) |
| documented | yes (`next-arc-columnar-prompt.md`) | no (this doc) |
| batch-1 prize | modest (pre-captured) | the real prize, but XL |

**The batch-1 allocation prize lives in B — the inter-op row interface — and it is the whole-
engine rewrite.** A is the contained, documented increment, but on batch-1 its prize is largely
already taken by the default-on §21 pruning. Choosing A because it is documented would optimize
the smaller, mostly-captured term.

**Recommendation: pursue B, but gate it.** Do not begin a whole-engine columnar rewrite on a
~1.3–2× ceiling (§4) unrewarded. Build the **smallest increment that validates B's premise and
ceiling on the real workload** (§6), and only generalize if it clears the gate.

## 4. Inherited constraints (from `docs/design-row-representation.md` — do not re-learn)

- **The sorted-columnar spine already lost** (§5–§13, §8.3): 1.4–2.5× slower than the flat dict
  on fine ticks, because per-tick sorted-batch rebuild dominated — it fixed term-2 (hash→compare)
  but worsened term-1 (build). **Columnar here = columnar *storage* + flat-hash O(1) *probe* +
  cross-tick buffer reuse — NOT sorted-merge.**
- **Cross-tick buffer reuse is proven safe** (§20, `DeltaPoolingPbtTests`) on a dead-after-tick
  edge (no `z⁻¹`, non-terminal). A columnar batch reused across ticks obeys the same rule;
  recursive-CTE (`z⁻¹` on deltas) is excluded.
- **Honest ceiling: ~1.3–2×, not parity** (§17.5) — managed bounds checks, no SIMD-by-default,
  object headers vs monomorphized Rust. q3's 2.83× win proves the engine is fast where it
  neither allocates nor whole-row-hashes retained state. On an allocation-bound batch-1, an ~18%
  alloc cut plausibly yields ~10–15% wall — real but not transformative.
- **Codegen/monomorphization is dead** (§17.2, execution 3–10% and free). Surrogate keys are
  closed (§14.9). Coordination is a strength, not a target (§16.11).

## 5. The batch-1 path mismatch (why the documented tooling doesn't transfer directly)

The companion arc is built around the **typed parallel** path: `ThreadStatic` seams to dodge the
typed-compiler reflection gotcha, W=8 gates, `reprbench`/`w1profile`/`q4*` harnesses on typed
struct rows. **Batch-1 runs the structural, single-threaded `CompileProgram` path** — `object[]`
`StructuralRow`s, one `foreach` over operators, no Exchange. Implications:

- The seam pattern still applies (a `[ThreadStatic]` columnar-mode flag read at compile), but the
  representation is `StructuralRow`/`object[]`, not typed structs.
- The gate is the **local `IvmBatchProfile`** loop (docker-free, `DOTNET_gcServer=1`, per-op alloc
  instrumentation) with the 16-output byte-identical cross-check — not the Nexmark harnesses.
- The `ApplyOpAllocSplit` microbench (extended here with the columnar variant) is the
  apportionment gate: an increment must beat the row-wise ApplyOp on both alloc and wall.

## 6. Smallest benchmark-gated first increment — ⚠️ RETIRED by §7.2 (fusion invariant); do not build

> The single-view apply-chain premise below is **falsified**: `CompileLinearChain` already fuses
> consecutive Filter/Project into one op, so a view has no long internal apply chain — every
> ApplyOp is bounded by materializing barriers and must materialize rows for its consumer anyway.
> See §7.2. The real smallest increment is one barrier op + neighbors columnar. Kept for context.

**Columnarize one view's ApplyOp chain internally, end-to-end within the view, and measure.**
Pick a hot pure-apply view — e.g. `watches` (886K rows, 5 ApplyOps) or `daily_market` (1.28M
rows) — and execute its map/filter/project chain over a **columnar batch** (SoA `object?[]`
columns + weight array), materializing `StructuralRow`s only at the view's output boundary (where
downstream views read the shared stream). This tests B's premise on the real workload with the
smallest blast radius: one view, one seam, the rest of the program unchanged (its output stream
stays `ZSet<StructuralRow>`, so consumers are untouched).

- **Seam:** a `[ThreadStatic]` `ColumnarApplyMode` (default off), read at compile; the view's
  fused apply-chain lowers to a columnar micro-pipeline instead of `MapFilterRows`.
- **Gate:** `IvmBatchProfile` (IVM_DEAD_COLS-style env flag), **16 outputs byte-identical**, and
  the view's ApplyOp alloc drops toward the microbench's −38%. Wall must not regress.
- **Retire-if-loses:** if the columnar micro-pipeline does not beat the row-wise chain on the real
  view (materialization at the output boundary eats the saving, or the boxed columns dominate),
  the arc retires here — as the spine did — with the honest number recorded.

Only if the single-view increment clears the gate does generalization to a columnar inter-op
interface (multiple adjacent views/operators sharing columnar batches, deferring materialization
across boundaries) become justified — and that is where the bulk of the ~18% lives.

## 7. Open measurements — RAN (2026-07-21, HEAD `ce8c7de`, local `IvmBatchProfile` reproduced 44.73 GiB / 60.6s ServerGC, baseline intact)

### 7.1 Single-view ceiling on real data — CONFIRMED and RAISED (§7 #1)

Extended `ApplyOpAllocSplit` with a **real `watches` s1 shape** (8→10 col, strings + DateTimes,
two CASE-passthrough computed cols) alongside the synthetic numeric shape:

| shape | (a) container | (b) row-materialization | (c) boxing | **columnar saving / ApplyOp** | batch if all ApplyOps columnar |
|---|---|---|---|---|---|
| NUMERIC 14→12 (boxed long/decimal) | 15.2% | 73.2% | 11.6% | **−38.3%** | ~18.1% |
| **WATCHES 8→10 (string/DateTime)** | 18.8% | **81.2%** | **0.0%** | **−47.5%** | **~22.5%** |

**The real string/DateTime-heavy dimension/SCD views columnarize *better* than the synthetic
microbench.** Their "computed" columns are CASE **reference-passthroughs**
(`case action_type when 'Activate' then watch_timestamp else null`) that reuse an already-boxed
ref → **zero (c) boxing residual**, so object-array columns leave nothing stranded and (b)
row-materialization is 81% of the op. B's object-column alloc ceiling for the string-heavy tail
is **~22%**, above the §2.3 −38%/18% synthetic estimate.

### 7.2 Output-boundary materialization — RESOLVED **NEGATIVE for the single-view increment** (§7 #2)

The §6 "columnarize one view's apply-chain" increment does **not** work as a positive-ROI brick.
Reason is structural, not measurable-away: **`CompileLinearChain` (PlanToCircuit.cs:1631) folds a
*maximal* run of consecutive Filter/Project into ONE `MapFilterRows` op**, stopping at the first
non-linear node. So after fusion:

- **Every surviving ApplyOp sits between materializing barriers** — its input is a source or a
  non-linear op (join/aggregate/window/distinct/union); its output feeds a non-linear op, an
  output boundary, or a fork (shared stream = also materialized). **Contiguous pure-apply regions
  have length 1.**
- A lone ApplyOp between two barriers **must materialize `StructuralRow`s for its barrier-consumer
  regardless of representation.** Columnarizing it in isolation = read rows → scatter to columns →
  compute → **re-materialize rows** for the next barrier: it *adds* a column round-trip and saves
  **zero** of the (b) term. **Strictly worse.**
- Columnar only wins when **both** neighbors are columnar (no scatter in, no materialize out) — which
  chains back to needing the stateful **barrier** ops (join/aggregate/window) to consume+produce
  columns. The 47% ApplyOp materialization alloc is fundamentally **the cost of feeding rows *into*
  the barrier ops**; it is not reclaimable one-op-at-a-time.

**Consequence:** there is no valid "one view's apply-chain" first increment. The smallest increment
that can show a *positive* number is columnarizing **one stateful barrier op + its adjacent applies**
(e.g. the `watches` aggregate op 292 = 2.0 GiB, or a join in the fact-join tier) so the columnar
batch spans the barrier — a substantially bigger brick than §6 proposed, and it commits the columnar
batch as a real inter-op type. §6's "single pure-apply view" premise is **falsified by the fusion
invariant** and should not be built.

### 7.3b Barrier-slice reclaimability — the honest number CORRECTS the ceiling and shows the single-slice increment is UN-GATEABLE

The §6 increment's own de-risking step (the "reclaimability microbench BEFORE wiring" pattern —
poolbench→pooling, reprbench→columnar). `JoinBarrierSlice` reuses the **real** join kernel
(`IncrementalJoinCore.JoinInto`) and the **real** `IndexedZSetTrace` over the `watches_history`
slice (projection → INNER JOIN(securities) → projection, 900K probe rows, 20 ticks), differing
between paths **only in the output sink**: ROW = StructuralRow throughout (current interface); COL =
columnar join output → projection fused into a single boundary materialisation (StructuralRows built
once, because the consumer `watches` is row-based). The invariant trace-integrate + match floor is
measured separately.

| slice variant | alloc | whole-slice saving vs ROW |
|---|---|---|
| ROW (join combine + builder + projection) | 0.382 GiB | — |
| COL (fused projection, unpooled columns) | 0.314 GiB | **−17.7%** |
| COL + **pooled** column buffers (§20) | 0.257 GiB | **−32.7%** |
| invariant floor (trace-integrate + match) | 0.126 GiB | **33.0% uncapturable** |

**The −47.5% pure-projection ceiling (§7.1) does NOT transfer to a real barrier slice.** Two
structural erosions, both permanent for a single slice:
1. **33% of the slice is representation-invariant** — folding the streamed side into the join's left
   trace (`IndexedZSetTrace.Integrate`) allocates the same whether the *output* is rows or columns.
   Reclaiming it needs a **columnar trace** (target A), a separate/larger change that §2.1/§21 already
   showed is mostly pre-captured by column-pruning.
2. **The boundary must materialise rows once** — a slice ending at a row-consuming barrier (every
   consumer here is a row-based join/aggregate/output) pays one full StructuralRow-build pass no
   matter what, halving the interface saving (47.5% → 26.4%).

**Un-gateable as a bounded increment.** This ~1.28 GiB slice is the *cleanest, biggest* single-barrier
target, and columnarizing it removes **0.07–0.13 GiB = 0.15–0.28% of the 44.7 GiB batch** — below the
`IvmBatchProfile` wall-noise floor. A single wired slice cannot be gated on batch wall; clearing a
~2–3% batch signal would require columnarizing slices totalling ~15% of the batch (~7 GiB of joins +
projections across many views) — i.e. **most of the XL rewrite, not a bounded brick.**

**Whole-engine ceiling revised DOWN.** The batch's join (27%) + aggregate (13%) + window (13%) alloc
is dominated by **internal trace/accumulator state**, which output-columnarization does not touch (the
33% floor here is that state for one join). Only the output-combine + projection *interface* is
capturable, and even that keeps its boundary-materialisation cost until *adjacent* ops are columnar
too. Realistic whole-engine reachable alloc ≈ **~10–13%** (not the §2.3/§7.1 ~18–22%), plausibly
**~5–10% wall** — for a multi-session whole-engine rewrite at a ~1.3–2× (not parity) ceiling, on the
**one-time** batch-1 load (the IVM benchmark's actual point is incremental batches 2/3, already
competitive — cost there is output I/O, not engine).

### 7.3 Typed-column upside (§7 #3) — NOT RUN

Deferred: §7.1 already shows the string-heavy tail has ~0 boxing residual, so typed columns add
nothing there; the numeric views (daily_market/trades) keep an 11.6% boxing residual that typed
`long[]` columns would capture — but only as part of the (larger) typed+columnar engine, gated
behind the go/no-go on the object-column engine first.

## 8. Honest bottom line — RECOMMEND STOP (declare the floor)

Batch-1 is allocation-bound at ~3.4:1 vs Feldera. The cheap/medium levers are exhausted (re-profile
found no algorithmic blow-ups; column liveness is a ~4% wash). §7 ran the three decision measurements
and the columnar thesis came out **weaker than the opening estimate, in two compounding ways:**

1. **B is all-or-nothing** (§7.2, fusion invariant): every ApplyOp sits between materialising barriers,
   so the 47% ApplyOp term is not decomposable into a cheap single-view brick.
2. **The realistic ceiling is lower and the smallest wireable unit is un-gateable** (§7.3b, faithful
   barrier-slice microbench reusing the real join kernel + trace): a real barrier slice saves only
   **17–33%** (not the 47.5% pure-projection ceiling) because ~33% is invariant trace/accumulator
   state and the boundary re-materialises rows once. The cleanest single slice moves the batch
   **0.15–0.28%** — below wall-noise; a gateable signal needs ~15% of the batch columnarized at once.
   Whole-engine reachable alloc revised to **~10–13%** (was ~18–22%), plausibly **~5–10% wall**.

**Recommendation: declare batch-1 at its practical floor (~3.4:1) and close the columnar arc as
DESIGNED-AND-MEASURED-NOT-BUILT.** The remaining prize is a multi-session whole-engine columnar
rewrite (columnar joins + aggregates + windows + applies + a columnar trace to reach the 33% floor),
with no bounded gateable increment, a revised-down ~5–10% wall ceiling at ~1.3–2× (not parity), on the
**one-time** historical load — while the IVM benchmark's actual value is the incremental batches 2/3,
which are already competitive (their cost is output I/O, not engine). The measured artefacts
(`ApplyOpAllocSplit`, `JoinBarrierSlice`) and this doc are the recorded decision; revisit only if the
target shifts from batch-1 wall to something the columnar rewrite serves better.

> **⚠️ Framing superseded by §9 (2026-07-21).** The "stop" verdict is correct *for the columnar arc*,
> but §9 shows it answered the wrong strategic question. Batch-1 competitiveness is a **parallelism**
> gap, not a representation gap — the columnar rewrite serves *serial* wall, and serial is already
> ~parity with Feldera single-core. Read §9 before acting on §8.

## 9. The parallel reframe — competitiveness is a PARALLELISM gap, not a representation gap (measured 2026-07-21)

§8's "stop" stands *for columnar*, but the strategic question is Curt's: **is competitive (50–80% of
Feldera) plausible with enough engineering, or a fool's errand?** The columnar rewrite serves batch-1
**serial** wall; the actual gap is **parallelism**. Two findings settle it.

**(1) The Exchange substrate is already generic over `StructuralRow` — structural-parallel needs no new
runtime ops.** `ExchangeOp<TKey,TWeight>`, `CircuitBuilder.Exchange`/`ExchangeIndex`,
`ParallelCircuit.Build`/`ShardedInput`/`ShardedOutput` are all `<TKey>`-generic; the typing lives ONLY
in `TypedPlanCompiler`'s exchange-INSERTION pass. Structural-parallel = port that insertion strategy
(shard scans, shuffle-by-key before joins/aggs/distinct, propagate partitioning) to
`PlanToCircuit`/`CompileProgram` over `StructuralRow` with a `Func<StructuralRow,int>` key-hash
(`StablePartitionHash` exists). It AVOIDS the §23 typing penalty (Exchange shuffles row *refs*, no
decode/encode). `PlanToCircuit` currently inserts ZERO exchanges (single-circuit only) — this is the
whole build.

**(2) Feldera's ~21s batch-1 is 12-worker parallel, and its full scaling curve is now measured**
(SF=3 batch-1, faithful OAT harness, `duration_s` from `run-feldera-batch1.json`, this i9-12900K box,
8P+8E; workers is `runtime_config` → no recompile, but the harness recompiles per sweep regardless):

| Workers | duration_s | vs 1-core | Efficiency |
|--:|--:|--:|--:|
| 1 | 56.07 | 1.00× | 100% |
| 2 | 34.36 | 1.63× | 82% |
| 4 | 23.93 | 2.34× | 59% |
| **8** | **18.56** | **3.02× (peak)** | 38% |
| 12 | 20.47 | 2.74× | 23% |

Two facts, one conclusion:
- **Feldera single-core (56.07s) ≈ dbsp-net serial (~59.5s local ServerGC).** On batch-1 Feldera's
  per-row representation edge is only ~1.06×, NOT the 2–5× it holds on Nexmark — the algorithmic wins
  (O(N²) window fix, semi-join narrowing, program-path optimizer) already closed the per-row gap. **A
  columnar/rep rewrite would buy little on batch-1: serial is already ~parity.**
- **Feldera saturates at ~3× with the knee at W=8, and goes NEGATIVE at W=12** (−10% vs W=8,
  oversubscribing the 8-P-core box) — the same synchronous-BSP bandwidth+coordination wall documented
  in the exchange-scaling arc (§15). dbsp-net is W-insensitive (§15.8), so it would not take that
  oversubscription hit; its configured default (workers=12) is not even Feldera's best here.

**Reframed bottom line:** batch-1 competitiveness is decisively a **parallelism-implementation**
problem. dbsp-net structural-parallel at a realized 2.5–3× (our `ParallelScalingProbe` hit 3.07×
best-case, disjoint-shard join+proj) takes ~59.5s → **~20–24s**: vs Feldera's peak 18.56s = **77–92%**,
vs its configured 20.47s = **up to parity** — squarely in the 50–80%+ "competitive" band, plausibly
parity, WITHOUT the columnar rewrite. **The recommended build is the structural-parallel
exchange-insertion pass in `PlanToCircuit`, not columnar.** Columnar stays
DESIGNED-AND-MEASURED-NOT-BUILT (§8); it re-enters only as a *further* multiplier once parallel lands
(a bandwidth-efficient rep raises the parallel ceiling — the compound thesis; the scaling probe showed
output-columnar alone does NOT lift the factor because the bottleneck is INPUT-side object[] bandwidth),
not as the batch-1 lever. Caveat on the estimate: the 2.5–3× is from a single best-case disjoint-shard
op; the real batch-1 DAG (SCD-2 temporal joins, wide-row window aggs, skew, exchange tax) may realize
lower (2–2.5×) → ~24–30s → 62–77% — still competitive, but the real-DAG factor is the open number the
build must validate.
