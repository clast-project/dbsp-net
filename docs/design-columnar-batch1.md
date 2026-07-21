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

## 6. Smallest benchmark-gated first increment

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

## 7. Open measurements to run first (before writing any operator)

1. **Confirm the single-view ceiling on real data.** The microbench's −38% is a synthetic 12-col
   projection. Instrument `watches`/`daily_market`'s actual apply-chain alloc and confirm the
   columnar SoA saving holds at the real column widths/types (wider rows → bigger StructuralRow-
   wrapper share; string columns don't box, so the boxing residual varies by view).
2. **Price the output-boundary materialization.** A view's columnar chain still materializes
   `StructuralRow`s at its output (consumers read them). Measure whether that boundary cost eats
   the internal saving — i.e. is the win only realized when *adjacent* views also go columnar
   (deferring materialization)? This decides whether the first increment is one view (weaker) or a
   view-pair (stronger but larger).
3. **Typed-column upside.** Measure the additional saving from `long[]`/typed columns over
   object-array columns on a numeric-heavy view, to price whether the typed+columnar end-state is
   worth its larger scope over object-array columnar.

## 8. Honest bottom line

Batch-1 is allocation-bound at ~3.4:1 vs Feldera. The cheap/medium levers are exhausted
(re-profile found no algorithmic blow-ups; column liveness is a ~4% wash). The remaining prize is
the inter-op row-materialization term (B), reachable only by a columnar execution interface —
measured ceiling ~18% alloc (~8 GiB, object columns; more with typed columns), a large rewrite
with a ~1.3–2× (not parity) ceiling. This doc opens the arc at that decision with the numbers; the
first increment (§6) is designed to validate or retire the ceiling on one view before the
whole-engine commitment.
