# Design note: row & indexed-state representation

**Status:** proposal / decision record. No code yet — this ranks architectural
options for the CPU bottleneck and recommends a benchmark-gated first increment.
**Author context:** follows the q4/q0 parallel-pipeline profiling work
(see `docs/benchmarks-comparison.md` and commits `d93db1d`, `11a7105`,
`6a172a1`, `4b4ca6b`).
**Date:** 2026-06.

---

## TL;DR

The engine is CPU-bound on **whole-row hashing**. Z-sets are
`Dictionary<TRow, TWeight>` and indexed Z-sets are
`Dictionary<TKey, ZSet<TRow, TWeight>>`, so every join probe, aggregate group
merge, exchange partition, and output build hashes and compares full row
structs. Delegate dispatch and `object?[]` boxing are real but *secondary* —
the prior profiling already established the binding cost is hashing, not
barriers, GC, or dispatch.

Two findings reshape the menu the roadmap memory assumed:

1. **Neither Differential Dataflow nor Feldera/DBSP interns rows to integer
   surrogate keys.** Both replace "one hash dictionary per operator keyed by
   whole rows" with **sorted, columnar, immutable batches merged in an LSM
   spine**, and resolve joins/aggregates by **merge/galloping over ordered runs
   keyed on `Ord`, never hashing a whole row**. Integer/dictionary encoding
   appears only as an *order-preserving per-column block encoding* inside their
   storage tier, not as global row interning. The "integer surrogate keys =
   biggest ROI" hypothesis in the roadmap memory is **demoted** here, with
   reasons (§6.1).

2. **Feldera deliberately abandoned monomorphized codegen** (it blew up Rust
   compile times) and moved to *dynamic dispatch* — and is still fast. Their
   speed comes from representation (sorted columnar merge), **not** static
   dispatch. This **demotes whole-query codegen** for us: it attacks per-row
   delegate dispatch, which our own profiling already showed is not the binding
   cost (§6.3).

The pivotal fact about our own codebase: **we already built the
sorted-columnar LSM substrate** — the `Spine*` trace family
(`SpineBatch`, `SpineZSetTrace`, `SpineIndexedZSetTrace`) stores state as tiered
immutable sorted-columnar batches (`TKey[]`, `int[] offsets`, `TValue[]`,
`TWeight[]`) with bloom filters and disk spill. But it is **opt-in**
(`TraceFamily.Flat` is the default), built primarily for **persistence/spill**,
**per-operator with zero sharing**, and its operators still resolve per-tick
work by **point probes** (`GroupFor(key)` per delta key), not batch merges.

So the gap vs. the reference systems is **not the storage format** (we have it)
— it is the **execution model** (merge/batch-at-a-time vs. point-probe) and
**arrangement sharing** (build the index once, read it from many operators).

**Recommendation:** the highest-ROI direction is **batch-at-a-time
sorted-merge execution on the existing spine substrate**, with **arrangement
sharing** as the natural second step. The recommended first increment is a
**merge-based aggregate/join inner loop** for the q4 hot operator, behind the
existing `PureTraceBenchmark` + `nexprofile` harness, gated on beating the flat
dictionary path at realistic trace sizes. There is a real incremental-specific
caveat (tiny per-tick deltas vs. huge state — §5) that the prototype exists to
settle before any broad rollout.

---

## 1. Where the hashing actually happens

All citations are current as of this note.

| Site | Structure | What gets hashed | File |
|---|---|---|---|
| Z-set core | `Dictionary<TKey, TWeight>` | full key (row) per add/probe/merge | `src/DbspNet.Core/Collections/ZSet.cs:14-18` |
| Indexed Z-set outer | `Dictionary<TKey, ZSet<TValue,TWeight>>` | join/group key per probe | `src/DbspNet.Core/Collections/IndexedZSet.cs:24,40` |
| Indexed Z-set **inner** | `ZSet<TValue,TWeight>` per group | **full value row** per group merge | `IndexedZSet.cs:18-24` |
| Join probe | `IndexedZSetTrace.GroupFor(key)` | join key, once per delta key | `src/DbspNet.Core/Operators/Stateful/IncrementalJoinOp.cs:158-225` |
| Aggregate group merge | `beforeGroup + groupDelta` | full value rows in the group | `src/DbspNet.Core/Operators/Stateful/IncrementalAggregateOp.cs:122-133` |
| Exchange partition | `hash(row) % W` | **full row** for routing | `src/DbspNet.Core/Circuit/Operators/ExchangeOp.cs:65` |
| Exchange gather (flat) | `ZSetBuilder.AddRange` | full row **again** | `ExchangeOp.cs:89` |
| Output build | `ZSetBuilder.Add` | full result row | `src/DbspNet.Core/Collections/ZSetBuilder.cs` |

Two structural observations:

- The **inner** Z-set of an indexed trace is keyed by the full value row
  (`IndexedZSet.cs:18`). The aggregate's per-tick `beforeGroup + groupDelta`
  therefore hashes whole value rows — this is the q4 nested MAX/AVG cost, and
  it is *unavoidable in the hash model* because MIN/MAX need the full multiset
  retained (the same reason `NarrowAggregateInput` can't column-prune ahead of
  a non-linear aggregate — see the parallel-pipeline-perf memory).
- The exchange hashes the **whole row** for partition routing. DD/Feldera also
  hash for routing, but they hash the **partition key**, not the full row. For
  q4's wide bid rows with a narrow key this is a large, avoidable difference —
  *if* the partition key can be extracted cheaply (it can; see §6.5/§6.1).

The typed fast path (`TypedPlanCompiler` + `TypedRowEmitter`) emits sealed
structs with IL `GetHashCode`/`Equals`/`CompareTo` over the fields. This kills
`object?[]` boxing and gives one hash per row construction — but the dictionary
still hashes the full struct on every operation. **The typed path narrows the
constant, not the asymptotics.**

---

## 2. What Differential Dataflow does (arrangements)

- State is an **arrangement**: a `Trace` = an LSM **`Spine`** of immutable,
  sorted `Batch`es of `(key, val, time, diff)` tuples, physically a columnar
  ordered trie (`keys: Vec<(K,usize)>, vals: Vec<(V,usize)>,
  times: Vec<(T,isize)>` — McSherry's "Specialize differential dataflow").
  Keys/values require **`Ord`, not `Hash`**.
- Operators resolve work by **merging sorted cursors** with galloping search —
  comparing keys in order, never hashing a whole row to find a bucket. Hashing
  appears only at the **exchange**, to route by key.
- An arrangement is **built once by `arrange_by_key`/`arrange_by_self` and
  shared** across many downstream operators (`join_core`, `reduce_core`,
  `count`, `distinct`, `semijoin`) via a `TraceHandle`/`import`. "Build the
  index once… the cost of each query is determined only by the new work it
  introduces" (Materialize). This is the *Shared Arrangements* result (VLDB
  2020) — order-of-magnitude wins from not maintaining N identical indices.
- Merges are **fueled/amortized** (each insert does a bounded multiple of its
  size of merge work), so no tick eats a giant compaction.
- Columnar region storage (**columnation**, **flatcontainer**) packs nested
  data into few large allocations addressed by **opaque integer offsets**
  (not pointers) — killing per-row allocation and enabling cheap
  serialize/share.
- **Tradeoffs:** requires a total key order; multiversioned history held until
  frontier compaction is permitted by *all* readers (memory pressure); batch /
  frontier latency; the join-vs-physical-compaction coupling is a known sharp
  edge.

## 3. What Feldera/DBSP does

- Same shape: immutable **sorted columnar batches** (`ColumnLayer` = parallel
  key/weight vectors; `OrderedLayer` = keys + per-key value runs via an offsets
  vector) organized into an LSM **`Spine`**. `OrdZSet`, `OrdIndexedZSet`,
  `OrdKeyBatch`, `OrdValBatch` are compositions of these layers. Read API is a
  universal **`Cursor`** over sorted runs.
- **No global row interning.** Integer/dictionary encoding exists only as an
  **order-preserving per-block column encoding** in the *storage* tier
  ("range scans evaluated without dictionary lookup" because codes sort like
  values), plus frame-of-reference `v−min` truncation. The win is columnar
  sorted merge, not interning.
- The SQL compiler generates Rust, but **deliberately uses dynamic dispatch**
  (type-erased values + vtable for compare/clone/serialize) — they
  *removed* monomorphization to cut compile time and it stayed fast. Dispatch
  granularity is per **key comparison**, amortized across a sorted run — far
  cheaper than a structural hash of an entire row per tuple.
- Persistence is a file-backed `Batch` (the LSM maps directly to on-disk sorted
  runs), `rkyv` zero-copy values, custom B-tree format, sequential sorted
  writes. (We have the analogue: `SpineBatch` spill + Arrow snapshot.)

**The actionable cross-system takeaway:** the lever is **representation +
execution model**, not dispatch. Replace dictionary point-probes with sorted
columnar merge; share the index. Static-dispatch codegen is explicitly *not*
where either system's speed comes from.

---

## 4. What DbspNet already has — and the three real gaps

We are further along than the roadmap memory implies. The `Spine*` family
(`src/DbspNet.Core/Operators/Stateful/Spine/`) is a faithful arrangement
substrate:

- `SpineBatch` / `ResidentSpineIndexedBatch`: immutable **sorted columnar**
  batches — `TKey[] _keys`, `int[] _offsets`, `TValue[] _values`,
  `TWeight[] _weights` — with per-batch bloom filters and disk spill
  (`SpineBatch.cs:129-131,450-455`).
- `SpineZSetTrace` / `SpineIndexedZSetTrace`: tiered LSM levels with pluggable
  `ICompactionStrategy` (default `TieredCompactionStrategy`).
- Wired through both compile paths via `CompileOptions.TraceFamily`
  (`StructuralRowComparer` for structural rows; emitted `IComparable` structs
  for the typed path).
- A ready microbenchmark, `PureTraceBenchmark`, already A/B's flat vs. spine
  probe/integrate at N = 1e3…1e6.

**Gap 1 — it's opt-in and persistence-oriented, not the hot path.**
`TraceFamily.Flat` is the default (`CompileOptions.cs:46-50`). The spine was
built for spill/snapshot, and it is *not currently a throughput win* — almost
certainly because its operators still do **point probes** (§Gap 2), so it pays
read amplification across O(log n) batches without the merge amortization that
makes sorted representations win. That it remains opt-in is itself evidence.

**Gap 2 — operators point-probe instead of merge.** `SpineIncremental*Op` and
the flat ops both resolve a tick by iterating delta keys and calling
`GroupFor(key)` once per key. That is the *hash-dictionary access pattern* even
when the backing store is sorted. DD/Feldera instead **merge the whole
(sorted) delta against the whole (sorted) state in one linear cursor pass**,
amortizing key comparison and getting sequential cache behavior. **This is the
single biggest unrealized lever.**

**Gap 3 — zero arrangement sharing.** Every stateful operator owns its own
trace and re-indexes independently. There is no `TraceHandle`/import, and no
plan-level detection of "the same relation arranged by the same key feeds two
operators." DD's headline win is structurally absent.

So the design space is not "adopt arrangements from scratch" — it is "finish
the arrangement model we started": make operators **merge** over the sorted
substrate (Gap 2), then **share** it (Gap 3).

---

## 5. The incremental-specific caveat (read before prototyping)

There is a genuine reason sorted-merge might *not* win, and the prototype exists
to settle it:

DD/Feldera amortize merges over **large batches** because they trade latency for
throughput at frontier boundaries. DbspNet's fine-grained model has **tiny
per-tick deltas against huge state**. A full merge pass of a singleton delta
against a 1M-row sorted state is **O(state)** — catastrophically worse than an
**O(1)** hash probe or an **O(log n)** galloping search.

Two consequences for the design:

- **For tiny deltas, the right primitive is galloping search into the sorted
  state, not a full merge.** That still avoids whole-row hashing — it compares
  *keys* (`Ord`, short-circuiting on the first differing field) instead of
  hashing the *whole row* — and on narrow keys over wide rows that is the
  actual mechanism that beats the dictionary. The win is "compare the key, not
  hash the row," available even at delta size 1.
- **Merge-at-a-time only pays off with larger ticks.** The parallel-pipeline
  memory already found ~18% headroom from larger batches and quantified
  small-batch re-emission amplification. So sorted-merge execution and
  **batch-size tuning are synergistic**: the prototype should sweep delta size
  and report the crossover where merge beats probe.

Net: the prototype must measure **both** regimes (galloping point-lookup at
delta=1, merge at delta=100/1k) — exactly the axes `PureTraceBenchmark` already
sweeps — and the rollout decision is per-regime.

---

## 6. Ranked options

Ranking is by **ROI against the stated bottleneck (whole-row hashing)**,
discounted by effort and risk. Effort/risk/blast are S/M/L/XL.

| # | Option | ROI | Effort | Risk | Blast radius |
|---|---|---|---|---|---|
| 1 | **Merge/columnar execution on the spine** (finish Gap 2) | **Highest** | L | M | Operator inner loops; opt-in path first |
| 2 | **Shared arrangements** (finish Gap 3) | High (query-dependent) | XL | H | Plan optimizer + operator I/O + compaction coordination |
| 3 | **Per-column dictionary encoding of keys** (narrow interning) | Medium | M | M | Exchange + key-extraction; codec |
| 4 | **Typed ingest / kill boundary boxing** | Low (cheap) | S | L | Ingest + structural path edges |
| 5 | **Whole-query codegen** | Low (wrong target) | XL | H | Entire compile backend |
| 6 | **Global integer surrogate row keys** | Low (hidden re-hash) | L–XL | H | Everything keyed on rows |

### 6.1 — Option 1: merge/columnar execution on the spine ⭐ recommended

**Idea.** Stop point-probing. Make `SpineIncremental{Join,Aggregate,Distinct}`
resolve a tick by a **cursor merge** of the sorted delta against the sorted
state (large deltas) and by **galloping search** (tiny deltas), reading the
`TKey[]/TValue[]/TWeight[]` columns directly instead of materializing
dictionary `ZSet`/`IndexedZSet` intermediates. This is "columnar execution" and
"shared-substrate" combined, built on storage we already have. It attacks the
hash sites in §1 directly: key comparison replaces whole-row hashing for
probes; the inner value-group merge becomes a sorted-run merge; output is built
sorted for the downstream consumer instead of re-hashed.

**Why highest ROI.** It is the exact mechanism both reference systems use, it
targets the binding cost (hashing) rather than a secondary one, and the
expensive substrate (sorted columnar batches, spill, comparers, the
microbench) **already exists**. The remaining work is operator inner loops, not
a new storage engine.

**Effort: L.** Rewrite ~3 operator inner loops to consume cursors; add a
galloping-search path to `SortedKeySearch` (binary search exists); a
merge-builder that emits sorted output. No new persisted format.
**Risk: M.** The incremental caveat (§5) is real; merge may lose at delta=1
until batch sizes grow. Mitigated by keeping it behind `TraceFamily.Spine` and
the galloping path. Soundness for MIN/MAX is unchanged (full value multiset
still retained, just sorted not hashed).
**Blast radius:** contained to the spine operator family while opt-in; only
becomes default after the benchmark gate.

**First increment (the prototype):** convert **one** operator — the q4
aggregate (`SpineIncrementalAggregateOp`) or join probe — to a merge/gallop
inner loop over the sorted batch columns, and benchmark via `PureTraceBenchmark`
(delta sweep) + `nexprofile` on the q4 step. **Gate:** ship only if it beats the
flat path on the q4 step at W=14 *and* doesn't regress the delta=1 probe.

### 6.2 — Option 2: shared arrangements

**Idea.** Introduce a `TraceHandle`/import abstraction so one arranged relation
(state keyed + sorted by K) is read by multiple operators, and a plan-optimizer
pass (arrangement CSE) that detects "same relation arranged by same key" and the
"reduce-by-K then join-on-K" adjacency, eliding redundant indexing.

**Why high but query-dependent.** This is DD's order-of-magnitude result, but
the win is *structural to the query*: it pays off on star-schema/self-join/
repeated-CTE shapes and reduce→join-on-same-key adjacency. For q4 specifically
the reuse is limited (join output is re-keyed for the aggregate). So it's a
broad-surface win, not a q4 win — sequence it **after** Option 1 proves the
merge substrate.

**Effort: XL. Risk: H.** Requires lifetime/compaction coordination across
readers — DD's documented sharp edge (a join holding cursors must pin physical
compaction). **Blast radius: L** — plan optimizer, operator input contracts,
trace ownership/GC. **First increment:** a read-only `import` handle + an
optimizer rule for the single clearest case (same table arranged by same key
feeding two joins), measured on a synthetic star-schema bench, before touching
compaction coordination.

### 6.3 — Option 3: whole-query codegen → **demoted**

Generating one IL routine to remove per-row delegate dispatch and intermediate
Z-sets. **Demoted because** (a) Feldera's evidence: they abandoned
monomorphization and stayed fast — dispatch is not the lever; (b) our own
profiling already ruled dispatch a secondary cost behind hashing; (c) we
already do partial codegen (`TypedRowEmitter` IL hash/equals/compare). It would
help the `object?[]` boundary and delegate calls — real, but not the binding
cost — at XL effort and H debugging risk. **Not recommended as the first
increment.** Revisit only after representation is fixed and dispatch becomes the
new top of the profile.

### 6.4 — Option 4: typed ingest / kill boundary boxing

Push typed values end-to-end so the structural-path `object?[]` boundary
(~3500 B/event) disappears. **Low ROI for the stated bottleneck** (it's alloc,
and we're CPU- not alloc-bound) but **S effort, L risk, small blast** — a
worthwhile cheap win and a prerequisite that makes Option 1's typed merge path
cleaner. **First increment:** typed ingest for the Nexmark source rows so the
benchmark path never boxes. Do this *alongside* Option 1, not instead of it.

### 6.5 — Option 6: global integer surrogate row keys → **demoted**

The roadmap memory's "biggest ROI" pick. **Demoted with reasons:**

- **Interning re-introduces the hash you're removing.** row→int needs a hash
  probe into a global intern table on ingest — you pay the whole-row hash once
  at the boundary instead of N times downstream. It only wins when the *same
  row* recurs many times so the intern cost amortizes. That's true for
  **low-cardinality keys** (join/group keys) but **false for wide fact rows**
  (Nexmark bid rows are near-unique) — interning them costs a hash and saves
  almost nothing.
- **It's a parallelism hazard.** Under W>1 the exchange is all-to-all; a shared
  intern table becomes a cross-worker contention point, or you need per-worker
  tables + reconciliation. DD/Feldera avoid this entirely.
- **It breaks hash-partition routing** unless surrogates preserve the key's hash
  distribution.
- **Neither reference system does it.** Their analogue is *order-preserving
  per-column block dictionaries in storage* (Option 3, narrow form), not global
  row interning.

**Salvageable narrow form (Option 3 in the table):** an order-preserving
**per-column dictionary for the partition/group key only**, so the exchange
hashes/sorts a compact code instead of a wide row, and comparisons run on codes.
Medium ROI, M effort. Worth a follow-up after Option 1, *not* a global row
interning scheme.

---

## 7. Recommendation & sequencing

1. **Now — Option 1 prototype (benchmark-gated).** Merge/gallop inner loop for
   the q4 hot operator on the existing spine columns. Settle the §5 crossover.
   This is the "prototype the single highest-ROI option behind a benchmark"
   deliverable. Pair it with **Option 4** (typed ingest) so the measured path
   never boxes.
2. **If it lands** — generalize merge execution across the spine operator
   family and flip `TraceFamily.Spine` toward default for the typed path.
3. **Then — Option 2** (shared arrangements) for the broad query surface, and
   **Option 3** (per-column key dictionary) for the exchange, both now sitting
   on a proven merge substrate.
4. **Defer — Options 5 & 6** until representation is fixed and the profile's top
   line moves from hashing to dispatch/alloc.

The unifying thesis: we already paid for the hard part (a sorted columnar LSM
store). The win is to **execute over it by merging, and share it** — exactly
what DD and Feldera do — and explicitly **not** to chase codegen or global row
interning, which the evidence says are the wrong targets.

---

## 8. Next-increment scoping — wiring the merge probe into the aggregate

> Status: scoping only (no code). Prototype landed in commit `0f31ddf`
> (`GallopIndexOf` + `GroupForManySorted` + `MergeProbeBenchmark`, results in
> [merge-probe-bench.md](merge-probe-bench.md)). This section captures what a
> follow-up session needs so it can start at the real decision, not discovery.
>
> **Update (2026-06): the join went first, not the aggregate — see §8.1.** On
> reading the actual operators, the aggregate is the *harder* wire-up and the
> join is the *cleaner* one, inverting the order this section assumed. The
> aggregate's premise below is partly wrong: the **typed** SQL aggregators
> (`TypedSqlAggregators.cs`, the path q4 uses) are already incremental and
> consume `afterMultiset` by **point-probe** (`after.WeightOf(deltaRow)` per
> delta row) and a `SumWeights()` gate — *not* a full scan. So Option D
> (feed a sorted span to scan) doesn't fit them; capturing the aggregate win
> really needs the larger Option B (`IMultiset` abstraction so `after` answers
> `WeightOf`/`SumWeights`/enumerate off a sorted run without hashing). The
> structural `SumAggregator`/`MinMaxAggregator` below *do* scan, so Option D
> still fits the structural path. Treat §8 as the **aggregate** plan; the join
> (a pure cross-product consumer of the probed group) needed none of this.
>
> **Update 2 (2026-06): the aggregate is now LANDED via Option B — see §8.2.**
> The `IMultiset` abstraction shipped; both compile paths' aggregators consume
> the post-delta multiset through it, so the spine aggregate hands them a
> sorted-run-backed view instead of a rebuilt Z-set. The detailed Option-D
> discussion below is kept as the design record but is **superseded by Option
> B**, which fit the real (point-probe) consumption pattern.

### 8.1 — Join increment LANDED (this session)

The spine **inner join** (`SpineIncrementalJoinOp`) was the first operator
wired, because it consumes the probed group purely by **iteration** into a
cross-product — no `WeightOf`, no aggregator interface, no semantic subtlety —
so `GroupForManySorted` drops straight in. The bench also showed the
join probe-side (absent-key) win is the biggest.

- **Change.** `Step` now sorts each delta side's keys once and calls
  `_rightTrace.GroupForManySorted` / `_leftTrace.GroupForManySorted` instead of
  a per-key `GroupFor` loop; matched sorted runs feed the existing
  cross-product unchanged. Single-key ticks (`D == 1`, the trace-level soft
  spot) keep the point probe. Output is identical — verified by the full spine
  join suite + spine-compile + random-query PBTs (all green).
- **Gate (operator-level A/B, [join-probe-bench.md](join-probe-bench.md)).** A
  benchmark seam (`SpineJoinProbeMode.ForcePointProbe`) drives the whole
  `Step` both ways in one process. Result: **1.2–2.8× faster steps on
  multi-key ticks, no regression at `D == 1`.** Smaller than the trace-level
  2–135× because a whole `Step` also pays the unchanged cross-product / output
  build / left-trace integrate.
- **Deferred.** End-to-end q4 on the spine path at W=14 — the parallel Nexmark
  harness compiles flat-only today, so that measurement belongs with the
  rollout step that flips `TraceFamily.Spine` toward the typed default. The
  spine LEFT/FULL joins were left on the point probe (same drop-in applies when
  needed).

### 8.2 — Aggregate increment LANDED via Option B / `IMultiset` (this session)

The aggregate needed the abstraction §8 anticipated. Why Option D (a sorted span
to scan) was wrong: the **typed** and **structural** SQL aggregators are already
incremental and read the after-group by **point-probe** — `after.WeightOf(row)`
per delta row (MIN/MAX, nullable-SUM) plus the composite's `SumWeights()` gate —
not a full scan. The right abstraction is therefore one that answers
`WeightOf`/`SumWeights`/enumerate off a sorted run *without hashing*.

- **`IMultiset<TKey,TWeight>`** (`src/DbspNet.Core/Collections/IMultiset.cs`):
  `IsEmpty` + `WeightOf` + `SumWeights` + `IEnumerable<KVP>`. `ZSet` implements
  it (every member already existed), so **widening aggregator `Compute`/`Update`
  `after`/`multiset` parameters from `ZSet` to `IMultiset` is caller-transparent**
  — the flat op, window op and `LoadAsync` keep passing Z-sets unchanged. `delta`
  stays a concrete `ZSet` (small, flat, no win). The swap touched the interface,
  the four Core aggregators, every `SqlAggregator`/`TypedSqlAggregator` +
  both composites, and the Hll/DdSketch fold helpers — all mechanical, bodies
  unchanged (they only use interface members). **No builder signature changed**,
  so the reflective typed-compiler call is untouched ([[typed-compiler-reflection-gotcha]]).
- **`SortedRunMultiset<T>`** (Spine folder): the sorted-run-backed implementation
  — `WeightOf` is a binary search (compare the key), `SumWeights`/enumerate are a
  linear pass. Built per changed group from the `GroupForManySorted` before-run
  merged with the tick's delta.
- **`SpineIncrementalAggregateOp.Step`.** Multi-key ticks now: sort the delta
  keys, `GroupForManySorted` the before-runs, merge each with its delta into a
  sorted after-run, wrap in `SortedRunMultiset`, call `Update`. The shared
  `EmitForKey` does the cache/emit (empty-group detection off `IMultiset.IsEmpty`,
  which a fully-cancelled run reports as length 0). `D == 1` and the
  `SpineAggregateProbeMode.ForcePointProbe` seam keep the old `GroupFor` + Z-set
  rebuild path.
- **Verified.** Full suite green incl. the existing flat-vs-spine SUM PBT and a
  new MIN multi-key PBT (the non-linear / point-probe consumer), plus the SQL
  spine-compile + random-query PBTs that exercise the typed `WeightOf`-on-run.
- **Gate ([aggregate-probe-bench.md](aggregate-probe-bench.md), `aggprobe`).**
  **1.3–5.5× faster `Step` on multi-key ticks, ~1.0× at `D == 1`.** Beats the
  join's margin because the point-probe path here hashes the whole group *twice*
  (build the before-Z-set, then the after-Z-set) — both passes the merge removes.

### What the code actually does today (verified)

- **Aggregators recompute over the full group every tick.** `SumAggregator` and
  `MinMaxAggregator` (`src/DbspNet.Core/Operators/Stateful/Aggregators/`)
  implement **only** `Compute(ZSet<TValue,Z64>)` and inherit `IAggregator`'s
  default `Update(...) => Compute(afterMultiset)`. So `afterMultiset` is
  genuinely consumed — fully scanned — and the expensive
  `afterGroup = beforeGroup + groupDelta` (with `beforeGroup = _trace.GroupFor(key)`)
  in `SpineIncrementalAggregateOp.Step` is real, necessary work, not waste.
  (The "MIN/MAX keep a sorted set in state" note in `benchmarks.md` does **not**
  match this `MinMaxAggregator` — it scans.)
- **The merge win is therefore: produce `afterMultiset` as a sorted
  `(value,weight)[]` instead of a rebuilt-and-rehashed `ZSet`, and let the
  aggregator scan that.** SUM is order-independent (sums either way); MIN/MAX get
  *faster* on a sorted run (first/last positive-weight element, no full scan).
- **Both compile paths reach the spine aggregate.** Structural via
  `PlanToCircuit.SpineIncrementalAggregate` with
  `IAggregator<StructuralRow,StructuralRow>` (`CompositeAggregator`); typed via
  the **reflective** `TypedPlanCompiler.InvokeSpineIncrementalAggregate`
  (line ~3112) with `TypedCompositeAggregator<TIn,TAgg>`. Both Composite
  aggregators wrap the component aggregators (Sum/MinMax/Count/Avg/…).

### The one real decision: how the aggregator consumes the sorted run

The aggregators take `ZSet<TValue,Z64>`. Feeding them a sorted run without
rebuilding a `ZSet` is the crux. Options:

- **D (recommended) — additive optional capability, mirroring the existing
  `Update` default.** Add to `IAggregator` an optional
  `Optional<TOut> ComputeSorted(ReadOnlySpan<(TValue Value, Z64 Weight)> sortedRun)`
  with a default that builds a `ZSet` and calls `Compute` (i.e. today's
  behaviour). Override it in `SumAggregator`/`MinMaxAggregator`/`Count`/`Avg`
  and have the two Composite aggregators fan out to component `ComputeSorted`.
  `SpineIncrementalAggregateOp.Step` then: sort the delta keys once →
  `GroupForManySorted` for the before-groups → merge each with its delta run →
  call `ComputeSorted`; fall back to today's `GroupFor` + `Compute` path for any
  aggregator that doesn't override (and for `D==1`, per the §5 soft spot).
  **Effort S–M. Risk LOW — and this is the key safety property: it adds an
  interface method with a default, so no existing signature changes, so the
  reflective builder call `SpineIncrementalAggregate(input, aggregator, codec,
  compaction, keyComparer, valueComparer, …)` is untouched** (see
  [[typed-compiler-reflection-gotcha]] — the gotcha is about *builder* signature
  changes; this is not one).
- **B — replace `ZSet` with an `IMultiset<TValue>` abstraction** that both `ZSet`
  and a sorted-run type implement. Cleanest conceptually, but changes the
  `IAggregator` generic surface and every impl + every caller. Effort L, risk M.
  Not worth it over D for a first increment.
- **C — keep the `ZSet` signature, build the `ZSet` cheaper.** Defeats the
  purpose (still hashes value rows). Rejected.

### Open sub-questions for the follow-up (cheap to answer first)

1. **Delta side.** The per-tick `groupDelta` is a flat `IndexedZSet` group
   (`delta.GroupFor(key)`) — small and unsorted. Either sort it once per key
   (tiny) to merge against the sorted before-run, or extend `GroupForManySorted`
   to also fold in the delta. Decide which; the former keeps the trace method
   pure.
2. **Empty-group detection.** `Step` prunes caches on `afterGroup.IsEmpty`. The
   merged sorted run gives this directly (empty result ⇒ empty group) — keep the
   check, just off the new representation.
3. **`Z64` weight sign for MIN/MAX.** `MinMaxAggregator` skips non-positive
   weights; the merged run already sums weights and drops zeros, so a value
   present with net-positive weight is the gate — preserve that.

### Parallel follow-up (separate change, not this increment)

The **spine join probe** (`SpineIncrementalJoinOp`) also point-probes via
`GroupFor` per delta key, and `merge-probe-bench.md` shows the absent-key
(probe-side miss) case is where the merge wins biggest (up to 135×). But the
join *consumes* the matched group differently (cross-product into output rows,
not an aggregate scan), so it's a distinct wiring job — `GroupForManySorted`
gives the batched probe, then the existing combine/cross-product loop runs over
the sliced runs. Scope it after the aggregate proves out end-to-end.

### End-to-end gate

Wire D, then measure the q4 Nexmark **step** (not the microbench) via the
`comparison`/`nexmark` benchmark and the `nexprofile` split/step/gather timing
at W=14. Ship only if the q4 step improves without regressing the existing
flat-vs-spine benchmark or the `D==1`/small-group cases.

---

## Appendix — sources

DbspNet: `ZSet.cs`, `IndexedZSet.cs`, `IncrementalJoinOp.cs`,
`IncrementalAggregateOp.cs`, `ExchangeOp.cs`/`ExchangeIndexOp.cs`,
`Spine/SpineBatch.cs`, `Spine/SpineZSetTrace.cs`,
`Spine/SpineIndexedZSetTrace.cs`, `CompileOptions.cs`,
`Benchmarks/PureTraceBenchmark.cs`; parallel-pipeline-perf profiling notes.

Differential Dataflow: arrangements mdbook (ch. 5), `trace/mod.rs`,
`spine_fueled.rs`, McSherry "Specialize differential dataflow" (2017) &
"Containers" (2024), `columnation` & `flatcontainer` repos, *Shared
Arrangements* (VLDB 2020, arXiv:1812.02639), Materialize arrangements docs.

Feldera/DBSP: `dbsp` Rustdocs (`Batch`/`BatchReader`/`Builder`/`Cursor`/
`Trace`/`Spine`, `OrdZSet`/`OrdIndexedZSet`, `dynamic` module), feldera/
storage-design + layer file format, "Meet the Feldera storage engine",
"Cutting Down Rust Compile Times… One Thousand Crates", DBSP VLDB 2023
(p1601-budiu.pdf).
