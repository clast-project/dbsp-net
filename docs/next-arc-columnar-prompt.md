# Starting prompt — next arc: columnar / arena inner-multiset representation

> Ready-to-use kickoff prompt for the columnar end-state arc (§17 #3 of
> `docs/design-row-representation.md`). Written so a fresh session starts at the real
> decision — *where is term-2 still large after the §18 narrowing, and does a reused
> columnar buffer beat the flat dictionary there?* — instead of rediscovering the
> whole row-representation arc. The two prior prototypes (§18 narrowing, §20 buffer
> pooling) deliberately **de-risked** this bet; this prompt hands the fresh session
> those foundations and the honest open question. Paste the block below into a new
> session. Use Opus.

---

**Columnar / arena inner-multiset design + benchmark-gated first increment — the §17 #3 end-state (measure-first; retire-if-it-loses)**

The per-row arc (`docs/design-row-representation.md` §16–§20; memories
`[[per-row-execution-efficiency]]`, `[[repr-execution-apportionment]]`) **apportioned**
DbspNet's single-core per-tuple gap vs Feldera (the whole competitive gap per §16.11:
we trail 1-thread on 11/13, q4 0.21×) into three terms with a direct microbench
(`reprbench`, §17.2): **term-1 allocation ~50–60 %** (fresh `Dictionary` per op per
tick), **term-2 whole-row hash ~40–48 %** (scales with row width), **execution ~3–10 %**
(the generic abstraction measured *free* — codegen is dead, demoted a third time). The
two representation fronts have each had a prototype:

- **§18 (term-2, q4): non-linear aggregate input narrowing** — q4's inner
  `MAX(price) GROUP BY auction` stored the full ~17-column join row; narrowing it to
  `{keys, agg-args}` (3 cols) gave **q4 W=1 −35 % time / −23 % alloc, W=8 step
  1.22–1.37×**. Sound only for non-negative / append-only streams, so it ships
  opt-in behind a seam (`NonLinearNarrowingMode`), default-off, productization deferred.
- **§20 (term-1): cross-tick delta-builder pooling** — reuse one `ZSetBuilder`
  across ticks (`Reset()` + `BuildShared()`). Throughput **thin** (q4 W=1 −7.5 % alloc,
  W=8 noise — pre-sizing §16.8 already took the churn). **But it answered the
  load-bearing architectural question for this arc: cross-tick buffer reuse IS safe
  and feasible** on a dead-after-tick edge (no `z⁻¹`/non-terminal), proven by
  `DeltaPoolingPbtTests` (full random-query oracle, pooling on, 3000 iters incl.
  retractions = batch). Kept default-off behind `DeltaPoolMode`.
- **§19 (term-2, q18/q19): TOP-K window narrowing — DEAD-END, reverted.** TOP-K is
  narrowing-immune (needs full rows for output); its window is already tiny; the
  attempt regressed q9. **q18/q19's gap is the output boundary (out-of-`Step` at W>1)
  + coordination, NOT inner-state representation.** Do not re-target TOP-K here.

**This arc is §17 #3: the columnar / arena inner-multiset** — fold the aggregate/join
`IndexedZSet` inner state (and ideally the output build + re-index) into **one reused,
per-column (SoA), arena-backed buffer**, the Feldera `OrdZSet` analogue (§16.4) but
reconciled with our fine-tick reality. It now stands on two proven foundations: §18
showed the inner row can be narrowed; §20 showed cross-tick buffer reuse is safe.

**Reframe — read before proposing anything (the honest opening, do NOT skip):**

1. **Re-justify the target FIRST. §18 may have already captured most of q4's term-2.**
   After narrowing, q4's inner row is *3 columns*, so a columnar/SoA win **on q4
   specifically may be small** (§18.5 flagged this). Before any rewrite, **measure
   where term-2 is still large after narrowing** — i.e. inner multisets that are
   genuinely wide/many-column and *cannot* be narrowed: (a) aggregates referencing
   many columns; (b) **join inner state** (the stored side's wide rows, which
   narrowing does not touch — see q20 1,291 B/ev, wide join output); (c) workloads
   where narrowing's append-only envelope does not hold (retraction-heavy CDC), so
   the wide row is retained. If term-2 is *not* large anywhere after §18, this arc
   should retire before it starts — say so honestly. Extend `reprbench` / `w1profile`
   to locate the residual wide-row hashing post-narrowing.

2. **The spine lesson is the central constraint (do not repeat it).** We built a
   sorted-columnar LSM substrate (`Spine*`, §5–§13) and it **lost** to the flat
   dictionary on fine ticks (§8.3: 1.4–2.5×) because per-tick sorted-batch rebuild
   dominates. The decomposition explains why (§17.3): sorted-merge attacked term-2
   (hash→compare) but **worsened term-1** (per-tick batch build) — it traded the
   smaller term for the bigger one. **Columnar here must attack term-1 AND term-2
   together:** per-column storage (term-2) over a **reused/arena buffer** (term-1,
   §20's proven mechanism), and it must **keep an O(1)-ish hash probe**, NOT
   sorted-merge — the flat-hash *execution* model won on our ticks. The synthesis is
   "columnar *storage* + flat-hash *probe* + buffer reuse," explicitly not
   "sorted-columnar storage + cursor merge" (measured losing).

3. **The hard design question is the incremental probe over columnar storage.** The
   incremental aggregators (typed MIN/MAX, §12/§14) probe `after.WeightOf(deltaRow)`
   per delta row — an O(1) hash today. A columnar SoA store must answer that probe
   without re-hashing whole wide rows *and* without an O(state) scan: likely a hash
   index (open-addressing over the column arrays) mapping a row's hash → its column
   offset, reused across ticks. Design this before claiming a win; it is where the
   spine's point-probe-vs-merge tension reappears.

**Deliverable:** a design note (`docs/design-row-representation.md` §21) + the
**smallest benchmark-gated first increment** — convert *one* hot operator's inner
multiset (the q4 aggregate, or the join stored side if §1 shows it is the wider
term-2 site) to the columnar/arena representation behind a seam (mirroring
`DeltaPoolMode` / `NonLinearNarrowingMode`), gated on **q4 (or the chosen query) W=1
`w1profile` beating BOTH the struct-dict and the pooled-dict controls, AND W=8 step
via a `q4*`-style harness with the per-tick output cross-check**, retiring it if it
loses (as sorted-merge was). **No broad rewrite before the gate.**

1. **MEASURE FIRST** — §1 above: locate residual term-2 post-§18-narrowing; pick the
   target operator by evidence, not assumption. Reuse `reprbench` (the apportionment
   microbench), `w1profile` (`… narrow`/`… pool` flags exist), `profile` (handwired
   ceiling).
2. **Design the columnar inner multiset** — SoA per-column arrays + weight array +
   reused/arena backing + a hash index for the O(1) probe; reconcile with the
   fine-tick lesson (§2/§3). Say honestly when it would and would not beat the flat
   dict. Decompose by what the aggregator/join actually reads (MIN/MAX read one
   column; join cross-products enumerate).
3. **Honest fundamental-vs-fixable** — §17.5's realistic ceiling: a .NET unboxed/
   columnar engine should expect to **narrow** the 2–5× single-core gaps toward
   ~1.3–2×, **not reach parity** (bounds checks, no SIMD-by-default, object headers vs
   monomorphised Rust). State the ceiling; don't assume parity.
4. **Smallest gated increment + retire-if-loses**, behind a seam, on the q4 step.

**Respect / landmines.**
- **Preserve q3 (2.83× single-core win)** — it pays *neither* term (filter sheds 94 %,
  no retained aggregate state, tiny output). The columnar change must be **opt-in /
  seam-gated to the stateful-heavy ops**, never a universal per-tuple tax on the
  filter/project/small-join path that is already at-or-above parity.
- **Honor the typed-compiler reflection gotcha** (`[[typed-compiler-reflection-gotcha]]`):
  q4 runs the typed parallel path; reach the new representation via an ambient
  `[ThreadStatic]` seam (as `SpineStagingConfig` / `DeltaPoolMode` / `NonLinearNarrowingMode`
  did), **not** a builder-signature change.
- **§20's retention rule is proven and reusable** — a columnar buffer reused across
  ticks must obey the same dead-after-tick (no-`z⁻¹`, non-terminal) constraint;
  `DeltaPoolingPbtTests` is the template for proving it. Recursive-CTE (`z⁻¹` on
  deltas) is excluded.
- **Surrogate keys are CLOSED** (§14.9, dominated). **Sorted-merge storage lost on our
  ticks** (§5–§13) — don't re-propose it; "columnar" here = reused buffers + per-column
  work + hash probe. **Coordination is NOT a target** (§16.11, it's a strength;
  q18/q19's residual gap is coordination + out-of-`Step` output, §15/§19).
- **Codegen / monomorphization is dead** (§17.2 — execution is 3–10 % and the
  abstraction is already free). Do not revive it.
- **Note the not-yet-built operators the representation should accommodate:**
  TUMBLE/HOP/SESSION windowing TVFs (more `IndexedZSet`-shaped inner multisets — the
  columnar design should generalise to them) and UDFs (`IScalarFunction` phase 5,
  `[[scalar-function-registry-temporal]]` — scalar-path delegate dispatch, the one
  place a *scalar* codegen seam could later matter, even though operator-loop codegen
  is demoted).

**Read first:** `docs/design-row-representation.md` — esp. **§16** (apportionment
arc), **§16.11** (single-core proof), **§17** (representation × execution design
space + the term-1/term-2/execution split + the spine reconciliation), **§18**
(narrowing — what term-2 it already captured), **§19** (TOP-K dead-end — why not
there), **§20** (pooling — the proven buffer-reuse foundation + retention rule),
**§5–§13** (the spine lesson), **§14** (surrogate, closed). Memories
`[[repr-execution-apportionment]]`, `[[per-row-execution-efficiency]]`,
`[[row-representation-design]]`, `[[surrogate-key-design]]`,
`[[parallel-pipeline-perf]]`, `[[exchange-scaling-decomposition]]`. Code:
`ZSet`/`IndexedZSet`, `ZSetBuilder` (incl. the §20 `Reset`/`BuildShared`),
`IncrementalAggregateOp`/`IncrementalJoinOp` (the §20 pooled-builder wiring shows the
seam pattern), `LazyMergeMultiset`/`IMultiset` (the aggregate consumption interface),
the `Spine*` family (the sorted-columnar substrate that lost — study why),
`TypedRowEmitter`/`StructuralRow`. Tooling already built and reusable:
`reprbench` (apportionment), `w1profile` (`… narrow`/`… pool`), `profile` (ceiling),
`q4narrow`/`q4pool`/`q4flat` + `SpineParallelHarness` (W=8 gates), the
`NonLinearNarrowingMode`/`DeltaPoolMode`/`FlatAggregateMode` seam pattern. Comparison
data: `D:\src\dbsp-bench-2.txt` (single-core) + `D:\src\dbsp-bench.txt` (multi-core).
