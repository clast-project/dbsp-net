---
name: per-row-execution-efficiency
description: "ACTIVE arc (the §15.8 lever) — W=1 per-tuple cost MEASURED: allocation-bound; q4/q18/q19 gaps = Layer-A dict-Z-set floor + Layer-B object[]/StructuralRow boundary"
metadata: 
  node_type: memory
  type: project
  originSessionId: 013f2c0a-2a3d-451e-aa38-01475b1a8ea2
---

**TYPED-INGEST SCOPING (2026-06-11) — it's SINGLE-CORE-ONLY; the competitive path already does it.** The engine has TWO ingest paths: (1) single-circuit `CompiledQuery` (`PlanToCircuit.Compile`, what `w1profile` AND the comparison's single-core "DbspNet 1c"/"W=1" column use, via `NexmarkBenchmark.MeasureSingle`) = HYBRID: `object?[]`→`StructuralRow` boundary + typed-inner via `TypedPlanCompiler.TryCompileWithStructuralBoundary`, converting StructuralRow↔typed at scan/sink (the Layer-B boundary; §16.9 attacked the OUTPUT side). (2) parallel `ParallelTypedCompiledQuery`/`ParallelIngestor` (what the comparison's W=10/W=14 columns use) = encodes `object?[]`→`ZSet<TRow>` typed struct DIRECTLY via `_factory(BoundaryEncoder.Encode(…))`, **NO StructuralRow** — i.e. typed ingest is ALREADY DONE on the parallel/competitive path. So **typed ingest would only improve the single-core column, NOT the competitive (10c/14c) ratios** that decide "beats Feldera". Run D's single-core boundary gaps (q0 0.51/q2 0.56/q22 0.39/q18 0.34/q19 0.32) are MUCH worse than their multi-core (q0 0.95/q2 0.89/q22 0.79/q18 0.46/q19 0.52) — consistent with the parallel path already shedding the structural boundary. The real competitive q18/q19 gap = TOP-K state over wide rows + out-of-`Step` parallel OUTPUT materialization (§16.10/§19) + coordination. **TYPED-INGEST RETIRED BY MEASUREMENT (2026-06-11, `ingestpath` cmd / docs/ingest-path-bench.md):** A/B'd structural single circuit vs typed W=1 (`TryCompileParallel(plan,1)`) over the SAME 1M stream, this i9 box. typed/struct = **0.85×–1.16×, ~parity** (q0 1.15, q22 1.16, q18 1.08, q19 1.07, but q2 0.92, q3 0.85 — typed W=1 is SLOWER on cheap queries because the W=1 parallel scaffolding overhead outweighs the ingest saving). So the single-core gap is NOT a path artifact — it's a REAL per-row floor (Layer-A dict-Z-set churn + typed-inner work), and switching the single circuit to typed ingest buys only ~7–16% on boundary queries, not the 2–3× needed to close the gap. **Typed ingest is DEAD as a lever** (competitive path already has it; single-core prize too small) — joins surrogates/codegen/spine in the retired-by-measurement pile. Next = q18/q19 out-of-`Step` parallel OUTPUT path (own session, design-first). See [[nexmark-feldera-w14-snapshot]] Run D.

The per-row / columnar execution-efficiency arc — the lever the exchange/scaling
arc concluded with ([[exchange-scaling-decomposition]] §15.8). Started 2026-06-10.
Same discipline: design-first, MEASURE-FIRST, benchmark-gated, honest
fundamental-vs-fixable. Builds on [[row-representation-design]] (the design doc
`docs/design-row-representation.md`, now §16), [[parallel-pipeline-perf]],
[[surrogate-key-design]].

**Target gaps (10-core comparison, `D:\src\dbsp-bench.txt`):** q4 0.49×, q18
0.46×, q19 0.55× vs Feldera. NOT scaling gaps (§15 ruled out 3 ways) — per-tuple
execution cost.

## MEASURED (W=1, this session)
Two durable harnesses shipped (uncommitted as of session end):
- **`w1profile`** (`Benchmarks/W1ProfileBenchmark.cs`, `dotnet run -- w1profile
  [events] [batch] [runs]` → `docs/w1-profile.md`): per stream event ns + managed
  bytes (`GC.GetAllocatedBytesForCurrentThread`, accurate at W=1, single circuit
  Steps on caller thread) + GC, across a differential query ladder.
- **`profile {handwired,typed,structural}`** (`ProfileHotPath.cs`, added
  alloc/step): handwired = pure Core typed ZSet, no SQL/object[]/StructuralRow =
  the ceiling.

**Results (1M events, batch 10k):**
- ns/event and B/event track each other almost exactly → **engine is
  ALLOCATION-bound per tuple**, not dispatch/compute-bound (so codegen stays
  demoted, §6.3 confirmed).
- The 3 gap queries are the 3 heaviest allocators: q4 2.8 KB/ev, q18 3.5 KB/ev,
  q19 5.1 KB/ev (and slowest: 2430/2620/3592 ns/ev). q0 passthrough already
  1.26 KB/ev. **Allocation IS the per-tuple gap.**
- `profile` ceiling: handwired 0.92µs/2843 B/step; typed 3.14µs/4949 B (+2106);
  structural 4.12µs/5467 B (+2624).

## ATTRIBUTION (code-grounded, design §16.3)
- **Layer A (~2843 B floor, ~55-60%, ARCHITECTURAL):** every stateful op's Step
  does `new ZSetBuilder`→fill→`Build()`→`SetCurrent` = a fresh `Dictionary`
  Z-set delta PER OP PER TICK (universal across all 25 stateful-op files;
  `ZSetBuilder.cs:72-82`). Present even hand-wired. Feldera builds ONE reused
  sorted columnar buffer; we build a fresh hash dict every tick. **`Build()`
  transfers dict ownership into the output ZSet → z⁻¹/trace/output-stream may
  retain it → NOT trivially poolable.** Hits q4's internal pipeline (q4 has only
  10 output rows → ~all its alloc is Layer A).
- **Layer B (+2106 B typed boundary, ~40-45%, BOUNDED):** object[]→typed input
  lift + typed→StructuralRow output materialization + per-projection delegate
  rows. q4/q18/q19 run TYPED at W=1 (pay +2106, not structural +2624). Localized
  by q2 (74 out, 818 B) vs q0 (9200 out, 1263 B) = output build ≈ +445 B. Hits
  output-heavy q18/q19 hard; ~zero for tiny-output q4.

## RANKED LEVERS (design §16.5) — no single bounded lever closes all 3
1. **Delta Z-set buffer pooling** — only bounded lever touching q4 (Layer A); but
   H-RISK: fights Build()-ownership + z⁻¹/trace retention (z⁻¹ DOES retain prev
   tick's output dict → pooling needs per-edge safety analysis). Needs a
   reclaimability microbench BEFORE wiring (mirror `surrogatebench`).
2. **Typed ingest + deferred output materialization** — Layer B; low-risk; helps
   q18/q19, ~no help for q4. (Note: in w1profile the input object[] is
   pre-generated+reused, so the measurable Layer B win is OUTPUT-side, not source
   typed-ingest.)
3. **Columnar/vectorized batch operators (OrdZSet analogue, reused buffers)** —
   the real Feldera-parity move, near-rewrite, the end-state. NB "columnar" here =
   buffer-reuse + per-column vectorized work, NOT sorted-merge (the spine arc
   §5-§13 already proved sorted-merge LOSES on our fine-grained ticks).
4. Codegen — demoted again (time tracks alloc not dispatch).

**Honest core:** the gap is substantially the cost of a generic
object[]/hash-Dictionary managed engine vs Feldera's monomorphized columnar Rust.
q4 is floor-bound (needs Layer A); q18/q19 are split.

## LEVER-1 MICROBENCH RAN (design §16.7, user picked this) — prize real but Layer-A-only
`poolbench` (`Benchmarks/PoolBenchmark.cs`, `dotnet run -- poolbench` →
`docs/pool-bench.md`) A/Bs the delta-dict lifecycle fresh/presized/pooled across D.
- **Pooling reclaims 100% of dict BACKING** (Clear keeps arrays → 0 alloc on stable
  refill). Prize scales with D: ~942 KB/dict/tick (long key) / 1.2 MB (pair) at
  D=9216 (bid-only batch-10k tick).
- **`fresh ≈ 3.3× presized` at large D = dictionary RESIZE CHURN** (grow from cap 0
  reallocates backing ~11×). → **pre-sizing the bare `ZSetBuilder()` reclaims ~70%
  with ZERO retention risk** (no cross-tick reuse; just size once). The stateful ops
  use the bare ctor (`IncrementalAggregateOp.cs:121`, `TopKOp.cs:101`);
  `ZSetBuilder.From` already pre-sizes. = a safe mechanical first sub-lever.
- **Retention constraint satisfiable per-edge:** trace `Integrate` folds delta into
  its OWN dict (doesn't retain delta); `Stream.Current` held only to next SetCurrent;
  ONLY cross-tick delta aliasing is `DelayOp` (`_nextOutput=_input.Current`,
  DelayOp.cs:34). q4/q18/q19 put no z⁻¹ on delta edges (delays are trace-internal,
  join L_{t-1}=`_leftTrace.Current`) → their deltas poolable. Recursive/nested have
  explicit z⁻¹ on deltas → exclude (per-edge compiler analysis).
- **HONEST LIMIT — pooling is LAYER-A-ONLY:** poolbench measures dict backing arrays.
  Per-row OBJECTS (StructuralRow/object[]/boxes = Layer B boundary) are separate
  heap allocs the dict only references → NOT reclaimed by pooling. So lever 1 is
  q4's lever (tiny output, value-type inline rows = mostly backing); q18/q19 are
  output-boundary-dominated → need lever 2. Levers 1+2 are COMPLEMENTARY, not
  either/or.

## COMMITTED (2 commits on main)
- `d7c38af` — measurement + design: `w1profile`, `profile` alloc ceiling,
  `poolbench`, §16.1–16.7.
- `ed21c68` — **lever-1 step (a) SHIPPED: adaptive delta-builder pre-sizing**
  (§16.8). Added `ZSetBuilder(int capacity)`; pre-size each tick's delta builder to
  the PREVIOUS tick's output count (last-output sizing self-tunes — 1:1 proj→input,
  selective filter→small result, NO over-alloc; better than §16.7's "input-count"
  wording). Sites: fused `MapFilterRows`/`FlatMapRows`, `ZSet.MapKeys`,
  `IncrementalJoinOp`/`IncrementalAggregateOp`/`PartitionedTopKOp` (instance
  `_lastOutputSize`). **Correctness-neutral by construction** (capacity is a pure
  perf hint → built Z-set byte-identical), fresh-alloc each tick (no reuse → no z⁻¹
  hazard). Suite 1747 green. **Gate (w1profile 1M/10k): alloc −16–35% every query,
  per-event time −10–18% on the gap queries** — q4 2812→2376 B/2430→2197 ns; q18
  3530→2417 B/2620→2249 ns; q19 5130→4059 B/3592→2945 ns; q9 −17%/−18%. Bigger than
  §16.7 predicted (wide value-type rows inline in dict Entry[] → big churned backing
  at batch 10k).

- `140dff9` — **LEVER 2 SHIPPED: lazy-boxing output boundary** (§16.9). Post-(a)
  re-attribution showed the biggest universal remaining term = the
  typed→StructuralRow output boundary (`AdaptTypedToStructural` built
  `new object?[]{(object)r.F0,…}`→StructuralRow every tick). Added
  `TypedStructuralRow<TRow>` + `StructuralRowShape<TRow>` (Core): StructuralRow
  subclass holding the emitted struct INLINE, boxing columns LAZILY (indexer only).
  Shape's typed hash reproduces `StructuralRow.ComputeHash` field-by-field with NO
  boxing — valid because `HashCode.Add(typedField)` == `HashCode.Add((object)boxed)`
  per-element (null→0). **Correctness-equivalent** (same Count/indexer/hash/equals);
  eager object[] kept as fallback for non-default codecs. Suite 1747 green (the SQL
  output-correctness tests compare ZSet<StructuralRow> → would fail on hash
  mismatch). **Gate (w1profile, before=ed21c68): output-heavy queries win big** —
  q0 962→719 B/−39% ns, q1 −23%/−30%, q22 −17%/−31%, **gap query q18 2417→2173 B
  (−10%)/−20% ns**, q19 −6%/−8%. **q4 UNCHANGED** (boundary-light, 10 out rows →
  confirms floor-bound, needs lever b/columnar). Caveat: w1profile consumer doesn't
  read columns so the time win is partly deferred boxing; alloc cut real for all.
  Combined w/ (a): q18 alloc down ~38% from arc start (3530→2173 B/ev).

## NEXT (unchanged sequencing, design §16.9)
(a)+lever-2 done. **q4 remains the holdout** (0.49×, the worst gap) — boundary-light,
so neither (a) nor lever-2 moved it; its remainder is join/aggregate INTERNALS (the
IndexedZSet trace structures + inner Z-sets). Options for q4:
- (b) TRUE cross-tick pooling — reclaims steady backing incl. the join's internal
  IndexedZSet builders; H-risk (breaks ZSet cross-tick immutability; per-edge no-z⁻¹
  guard, double-buffering); post-(a) prize ~5% → user DECLINED in favor of lever 2.
- (3) COLUMNAR end-state — the real q4 fix and the big structural step; near-rewrite,
  own arc.
**REGRESSION CHECK RAN (d09d83f, §16.10):** same-box A/B HEAD vs pre-arc 215fac0
(i9, W=8, 1M, 3 runs). (1) **NO parallel regression** — every query ≥ pre-arc at
W=8 (q18 +6%, q19 −2% noise); the M4 Pro 10c q18 dip was single-run noise. (2)
**KEY CORRECTION — "extend lever 2 to parallel" is MOOT:** lever 2 (typed→
StructuralRow output) is an IN-CIRCUIT operator on the SINGLE circuit, timed every
Step (q18 W=1 +40%), BUT the PARALLEL path decodes output LAZILY on q.Current read
which the throughput bench does AFTER sw.Stop() (MaterializeParallel) → the W>1 hot
loop never eagerly materialises output → nothing for lever 2 to remove at W>1.
Lever 2 = single-thread/latency + the "DbspNet W=1" column ONLY. (3) **W>1
competitive lever = in-Step work only** — (a) helped (q4 W=8 +14%, in-Step join
pre-size); q18's win is out-of-Step at W>1 (+6%). (4) **Amdahl dilution measured** —
per-row wins shrink parallel speedup (q18 2.58→1.96×), so W>1 gains ≪ W=1 gains;
q18/q19 partly coordination-bound (§15). **No cheap/safe/high-W>1-ROI lever remains.**

M4 Pro comparison after (a)+lever2: W=1 wins land (10c q0 +47%, q18 +46%, q19 +14%,
q4 +3%); competitive ratios moved modestly (10c q4 0.49→0.55, q19 0.55→0.62; 14c
q4 0.66, q18 0.58, q19 0.73). q4 W=8 +14% is the best W>1 gain (in-Step).

## SINGLE-CORE COMPARISON — coordination question ANSWERED & INVERTED (§16.11)
1-thread-vs-1-thread, both engines, M4 Pro (D:\src\dbsp-bench-2.txt):
- **DbspNet trails Feldera single-threaded on 11/13, often 2–5×** (q4 0.21×, q15
  0.32×, q18 0.33×, q19 0.35×, q22 0.41×, q0 0.49×, q2 0.59×, q20 0.67×, q1 0.75×).
  Only single-core wins: q3 2.83× (real algorithmic edge), q9 ~1.07×.
- **Multi-core wins come from SCALING, not speed:** DbspNet scales +ve on 12/13;
  **Feldera goes NEGATIVE on q4/q15/q16/q17** (slower at 14c than 1c — q15 −4.5×).
  Even q0 (ingest/egest-bound) scales BETTER for us (2.35×) than Feldera (1.19×) →
  my §16.10 serial-boundary worry ALSO dispelled.
- **CONCLUSION: coordination/scaling is our STRENGTH, not a leak** — we out-scale
  Feldera, never de-parallelize. The whole competitive gap (q4/q18/q19/q22/q0/q2) is
  **PER-ROW** (managed runtime + object[] boundary + alloc), worst on
  aggregate/join/string-heavy. Retires coordination (§15) as a COMPETITIVE target;
  vindicates the per-row arc unambiguously.
- **Dilution caveat REFINED (strengthens columnar):** q18 +40%→+6% W1→W8 was the
  lever-2-out-of-Step artifact, NOT a general law. IN-Step per-row wins translate &
  can AMPLIFY ((a) on q4: +4% W1 → +14% W8, speedup ROSE 2.47→2.70). So columnar
  (in-Step) lifts BOTH single-core (huge 2–5×) and multi-core (amplified on
  join/agg laggards). **Highest-value columnar target = aggregate/join inner repr
  (IndexedZSet trace)** where q4 (0.21×)/q15–19 bleed. Study q3's 2.83× win to
  preserve it.

## NEXT-ARC KICKOFF PROMPT SAVED (commit 2de2282)
`docs/next-arc-representation-prompt.md` — ready-to-paste kickoff. **REFRAMED past
"columnar" per user insight:** the lever is **data representation OFF the managed
heap / unboxed / pooled + per-tuple EXECUTION (monomorphization/vectorization)** —
columnar is ONE candidate, NOT the headline. The cost is allocation throughput +
boxing/indirection + per-tick dict realloc (NOT GC pauses, §15.2). Our own spine
result (sorted-columnar LOST to flat dict on fine ticks) → **leading candidate =
"unboxed pooled flat-hash"** (keep the flat-hash execution that won, strip its
managed-heap costs: value-type keys, open-addressing over pooled arrays, reused),
not literal columnar. Prompt requires measure-first apportioning the 2–5× single-
core gap between representation vs execution before betting.

## DECISION POINT (next)
No remaining cheap/safe per-row lever for W>1. Honest options: (1) COLUMNAR
end-state (lever 3) — the real structural step for the in-Step per-row cost +
q4/q18/q19 W>1 gaps; own arc, near-rewrite. (2) CONSOLIDATE — treat (a)+lever 2 as
the bounded per-row win (W=1 big, W>1 modest) and stop, with the residual W>1 gap
attributed to coordination (§15, substantially fundamental) + the generic-engine
per-tuple cost (columnar territory). Lever (b) pooling = thin ~5% / high-risk,
declined. Still-valid small follow-ups: extend adaptive sizing to ZSet.Filter /
remaining builders (W=1 only); lever 2 is DONE (don't try to parallelise it).
