---
name: repr-execution-apportionment
description: "§17 design arc — apportioned the single-core per-tuple gap; representation is the lever, execution/codegen is measured-dead"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7ff0a153-db82-4e9c-a781-e2c6e965f351
---

DONE (design-only, no engine code): the §17 arc of `docs/design-row-representation.md`
opened the data-representation/per-tuple-execution investigation the §16.11 single-core
proof pointed at, and **apportioned** the per-tuple floor with a new microbench.

**New durable probe:** `reprbench` (`Benchmarks/ReprDecompBenchmark.cs`, `dotnet run --
reprbench [ticks] [delta] [runs]`, report docs/repr-decomp-bench.md). Times the universal
per-tick Layer-A hot loop (build delta dict of D wide-row keys → fold into state →
enumerate) FOUR ways at 3 key widths (W2/W8/WStr): gen·fresh, gen·pooled (kills alloc),
mono·deleg (raw dict, Func transform), mono·inline (no delegate = the floor). The deltas
apportion: alloc tax / abstraction tax / dispatch tax / compute floor.

**The apportionment (the deliverable, decisive):** of the per-tuple floor on wide
(bid-like W8/WStr) rows — **~50–60% is fresh-Dictionary-per-tick ALLOCATION** (term 1,
fixable by pooling, Layer-A, dead-after-tick edges only), **~40–48% is the irreducible
WHOLE-ROW HASH floor** (term 2, scales with width, fixable ONLY by representation —
narrow/extracted keys or columnar per-column; for the aggregate inner multiset keyed by
the whole row it is largely irreducible short of columnar), and **execution/dispatch is
~3–10%** with the generic ZSet/Z64/IZRing abstraction measured **FREE** (.NET
monomorphises value-type generics; abstraction tax ≈ 0 within noise). So **codegen/
monomorphization is the wrong target — demoted a THIRD time, now on apportioned evidence**,
not just the §16 "time tracks allocation" correlation.

**Spine lesson resolved (the central question):** sorted-merge attacked term 2 (hash→
compare) but WORSENED term 1 (per-tick batch build) — traded the smaller term for the
bigger one → that's why it lost on fine ticks, now predicted by the decomposition. Lever
must attack BOTH terms without trading: (i) get the flat-hash model (which WON) off the
per-tick heap — pooled/unboxed/inline — for term 1; (ii) columnar PER-COLUMN inner
multiset with REUSED buffers ONLY for the aggregate/join IndexedZSet where term 2 is large
and irreducible. NOT columnar storage (lost), NOT sorted-merge (lost).

**Ranked design space:** everything with ROI is representation. #1 cross-tick delta
pooling (term 1, lead bounded lever), #2 unboxed pooled flat-hash trace (terms 1+2 broad),
#3 columnar per-column inner multiset (term 2, narrow=q4/q15–q19, XL, the real Feldera
move), #4 typed ingest (Layer B, q18/q19). DEAD: #6 codegen (3–10%, abstraction free),
#7 off-heap/unsafe (pooling gets term 1 safely), #8 global surrogates (closed §14.9).

**Realistic ceiling (honest):** term 1 reclaimable in full; term 2 shrinkable not
erasable (managed bounds-checks/no-SIMD/object-headers). Expect to narrow the 2–5×
single-core gaps on q4(0.21×)/q18(0.33×)/q19(0.35×) toward ~1.3–2×, NOT parity. q3's
2.83× win proves the ceiling is high when a query pays NEITHER term (filter sheds 94%, no
retained agg state, tiny output).

**Smallest gated first increment (next, if pursued):** cross-tick delta pooling on the q4
aggregate (+join) behind a per-edge "no-z⁻¹" guard; gate on q4 W=1 w1profile ns+B/event
down AND W=8 in-Step up (in-Step term-1 wins amplify at W>1, unlike out-of-Step lever 2 —
§16.11). Fall back to #2 purpose-built trace if Build()-ownership fights pooling.

**Landmines:** keep it OPT-IN/seam-gated to stateful ops (don't tax q3's cheap path);
ambient [ThreadStatic] seam not builder-signature change ([[typed-compiler-reflection-gotcha]]);
accommodate not-yet-built TUMBLE/HOP/SESSION (more IndexedZSet inner multisets → #3
generalises) and UDFs (reintroduce scalar-path delegate dispatch → keep a scalar
codegen seam even though operator-loop codegen is demoted).

**§18 (LANDED, gated, opt-in) — term-2 first increment, CORRECTED §17's premise.**
§17 claimed q4's term-2 whole-row hash is "irreducible short of columnar." WRONG:
q4's inner `MAX(b.price) GROUP BY a.id,a.category` stores the full ~17-col join row
because `NarrowAggregateInput` (PlanOptimizer.cs:354) BAILS on MIN/MAX/DISTINCT
(conservative guard) — it only needs 3 cols. Verified against SqlMinMaxAggregator
(keys its own Counts/Active state on the VALUE, probes after.WeightOf(row) per delta):
**narrowing to {keys, agg-args} is SOUND iff the per-group integral is non-negative =
well-formed/append-only streams** (the guard protects arbitrary SIGNED Z-sets, which
the engine supports + the random PBT tests via ±1 weights — so narrowing CAN'T be a
default, only an opt-in for non-negative/insert-only inputs like Nexmark/CDC-free
ingest). Shipped: `NonLinearNarrowingMode` [ThreadStatic] default-off seam +
unblocked rule; `NonLinearNarrowingTests` (300 well-formed seeds narrowed≡full-row +
arity 4→2 non-vacuous check); suite **1749 green** default-off. Gates: `w1profile
narrow` → **q4 W=1 −35% time / −23% alloc**; `q4narrow` → **q4 W=8 step 1.22–1.37×**
(largest W>1 q4 win in the arc; output cross-checked identical = in-envelope correct on
parallel typed path) — confirms §16.11 in-Step amplification. DECISION: narrowing
captures a big chunk of q4 term-2 cheaply → **columnar SoA inner-multiset (§17 #3)
reframed**: now attacks only the RESIDUAL 3-col floor (small) + output-retaining TOP-K
(q18/q19, where narrowing can't help), so deprioritized for q4. Term-1 pooling still the
other front. Productization TODO: clean public opt-in (param on PlanOptimizer.Optimize),
document append-only envelope.

**§19 (RAN, mixed, REVERTED — documented dead-end).** Tried the term-2 attack on
q18/q19 (TOP-K, next-worst gaps 0.33/0.35×, narrowing-immune since TOP-K needs full
rows for output): replace the per-partition window `Dictionary<TRow,long>` with a
compact `(TRow,long)[]` (windows ≤limit: q18=1,q19=10) — correctness-neutral, no
snapshot-format change. Result MIXED on the SAME operator: q18 −15% alloc, q19 −7%,
but **q9 +14% alloc REGRESSION, deterministic + unexplained by the diff** (skip-unchanged
refinement didn't move it). Reverted — a correctness-neutral change that regresses a
structurally-identical query for unexplained reasons isn't shippable, and the q18/q19
wins are modest + W=1-only (their W>1 gap is the OUTPUT BOUNDARY decoded out-of-Step
§16.10 + coordination §15, NOT in-Step window state). CONCLUSION: q18/q19 TOP-K has NO
cheap in-Step representation lever like q4's narrowing; redirect q18/q19 effort to (i)
term-1 cross-tick delta pooling (§17 #1, broad) and (ii) the parallel-path output
materialization boundary. Lesson for next session: don't re-try "narrow the TOP-K state."

**§20 (LANDED behind seam — thin throughput, BUT architectural question ANSWERED).**
Term-1 lever (the ~50-60% allocation half): cross-tick delta-builder POOLING — reuse one
ZSetBuilder across ticks (new `ZSetBuilder.Reset()` + `BuildShared()` = wrap dict WITHOUT
nulling, so reusable) instead of fresh dict per Step. Wired into IncrementalAggregateOp +
IncrementalJoinOp behind `DeltaPoolMode` [ThreadStatic] default-off seam. THROUGHPUT THIN
(as §16.10 predicted): q4 W=1 −7.5% alloc / time-flat; W=8 step 0.86–1.29× = within ±0.5
parallel noise. Thin because §16.8 pre-sizing already took the churn + output builders are
only PART of q4 alloc (re-index/trace/boundary unpooled). **BUT the REAL result: the
load-bearing architectural question is ANSWERED YES — cross-tick buffer reuse IS safe on
dead-after-tick edges** (no z⁻¹/non-terminal): proven by `DeltaPoolingPbtTests` (full random
PBT pooling-on, 3000 iters incl. retractions, = batch oracle) + suite 1750 green seam-off +
q4pool parallel output cross-check identical. Breaking ZSet's "don't retain the dict"
invariant is sound under the guard. DECISION: keep default-off seam (don't productionize for
~7%); the value is **de-risking the columnar end-state** (§17 #3) — columnar/arena buffer
reuse now has the retention rule PROVEN, doesn't re-litigate it. Excluded: recursive CTE
(z⁻¹ on delta) + externally-read terminal output (unless consumer copies/tick). Term-1 now
substantially closed standalone (pre-size shipped + pooling proven-thin); remaining headroom
= other unpooled sites + the columnar restructuring (fold output+reindex+inner-multiset into
one reused columnar buffer).

Builds on [[per-row-execution-efficiency]] (the §16 arc this continues),
[[row-representation-design]], [[surrogate-key-design]], [[exchange-scaling-decomposition]].
