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

### 8.3 — End-to-end gate RAN — spine loses to flat on q4; do NOT flip the default

The gate was built (`dotnet run -- q4spine [events] [W] [runs]`,
`Q4SpineBenchmark`, report [q4-spine-bench.md](q4-spine-bench.md)) and run. It
compiles q4's **whole parallel pipeline** three ways at the host core count and
times the **step** phase apart from split/gather, cross-checking every config's
output against flat:

- **flat** — `TraceFamily.Flat` (today's default).
- **spine·point** — `TraceFamily.Spine` with the merge probe forced off
  (`SpineJoinProbeMode`/`SpineAggregateProbeMode.ForcePointProbe`): the LSM
  substrate *without* the merge win. Isolates the substrate's own cost.
- **spine·merge** — `TraceFamily.Spine` with the merge probe live.

**Result (W=24, 1M events, median-of-3, two runs, stable):**

| Batch | flat step | spine·point | spine·merge | merge↑ vs flat | merge↑ vs point |
|------:|----------:|------------:|------------:|---------------:|----------------:|
| 10k   | ~675 ms   | ~1720 ms    | ~1650 ms    | **0.40–0.42×** | ~1.04×          |
| 100k  | ~675 ms   | ~1070 ms    | ~955 ms     | **0.68–0.73×** | ~1.11×          |

**Verdict: the merge probe is confirmed end-to-end but the spine substrate is
not.** Two findings, cleanly separated by the three-way A/B:

1. **The merge probe carries its operator-level win all the way through** —
   spine·merge beats spine·point at every batch (1.04× at 10k, ~1.11× at 100k),
   reproducing the `joinprobe`/`aggprobe` microbench direction in the full
   parallel pipeline. The wiring is sound and worth keeping.
2. **But the spine *substrate* loses to the flat dictionary on q4** —
   spine·merge is **1.4–2.5× slower than flat**, and the gap is driven entirely
   by spine·point (the substrate), not the probe. The cause is structural and
   matches §5: at W=24 each replica's per-tick delta is `batch/24` rows, so the
   spine rebuilds many *tiny* sorted-columnar batches per tick (build + bloom +
   compaction churn) where the flat dictionary just does in-place hash updates.
   The merge probe makes that overhead cheaper but cannot erase it. The gap
   narrows as the batch widens (0.41× → 0.68×, exactly §5's "merge only wins
   with larger batches"), but never crosses 1.0× at realistic q4 tick sizes.

**Consequence for the roadmap.** Do **not** flip `TraceFamily.Spine` toward the
typed default — the gate's ship condition ("beats flat at W=14") is not met. The
bottleneck is no longer the probe (Option 1 is done and validated); it is the
**per-operator, zero-sharing substrate cost** the design doc flagged as the next
gap. The next increment is therefore **Option 2 — shared arrangements**
(§6.2): build the indexed batch once and share it across operators / amortize it
across ticks, so the substrate's build cost is paid once instead of every tick
per replica. The merge probe stays in place (committed, behind the `D==1` guard)
as the execution model that shared arrangements will read through.

---

## 9. Option 2 increment — cross-operator shared arrangements (flat, LANDED)

> Status: first increment of Option 2 (§6.2). Built and benchmark-gated this
> session. Report: [shared-arrangement-bench.md](shared-arrangement-bench.md).
> Scope deliberately matches §6.2's "first increment": a read-only shared
> arrangement handle + a join that reads it, **before** touching compaction
> coordination, on the **flat** substrate first.

After §8.3 closed Option 1 (the merge probe is done and validated, but the spine
*substrate* loses to flat on q4 because of per-operator, per-tick index rebuild),
the next gap is **arrangement sharing** — build an index once and read it from
many operators, instead of each operator owning and re-maintaining its own.

### 9.1 — What landed

A relation arranged by a key for several consumers is now maintained **once**:

- **`IArrangement<TKey,TValue,TWeight>`** (`Operators/Stateful/Arrangement.cs`):
  a read-only handle exposing `Current` — the running integral **after** this
  tick's delta. Consumers read it within their `Step`, exactly as a join reads
  its own (after-)right trace.
- **`ArrangeOp`** (same file): a stateful op that reads an already-indexed
  delta stream and integrates it into ONE `IndexedZSetTrace`, exposing itself as
  the handle. Registered before its consumers, so they see the post-delta
  integral in the same tick (operators fire in registration / topological
  order — `RootCircuit`).
- **`IncrementalJoinSharedRightOp`** (`IncrementalJoinSharedRightOp.cs`): an
  inner join whose RIGHT side is a shared `IArrangement` instead of a private
  right trace. It owns only the left (delayed `L_{t-1}`) trace. The incremental
  rule is **unchanged** — `out_t = dl ⋈ R_t + L_{t-1} ⋈ dr` — because the shared
  relation is always the `R_t` ("after", integrate-first) side, which is exactly
  what a private right trace would hold; output is therefore byte-identical to a
  plain join. The join kernel was factored into a shared `IncrementalJoinCore.JoinInto`
  so the private-trace and shared-arrangement joins run identical cross-product
  logic.
- **Builder API** (`StatefulOperators.cs`): `Arrange(indexedStream)` →
  `IArrangement`, and `IncrementalInnerJoinSharedRight(left, rightDelta, arrangement, combine)`.
  Internal (experimental), reachable from the Core builder.

**Key feasibility insight:** sharing needs no change to the join math. Two joins
`A ⋈ R` and `B ⋈ R` both want `R_t` (after) and `dR`; the arrangement folds `dR`
into the shared trace once, both read it, and each keeps its own left trace. The
asymmetric two-pass factoring (one side after, one before) is satisfied by always
placing the shared relation on the after side — and inner join is commutative, so
the optimizer can always orient it there.

### 9.2 — Why flat first

The §8.3 motivation is the *spine* per-tick build cost, but the cleanest, lowest-
risk **first** demonstration of cross-operator sharing is family-agnostic and
simplest on the flat trace: no LSM batch/bloom/compaction subtleties, apples-to-
apples with the substrate that wins on q4 today, and a much smaller diff. The
flat `IndexedZSetTrace` is also built per-operator, so sharing it is a legitimate
(if smaller) win. The spine arrangement is the follow-up that captures the bigger
prize.

### 9.3 — Verification & gate

- **Correctness.** `SharedArrangementTests` builds both pipelines (independent
  joins vs. one shared arrangement) over the SAME inputs and asserts byte-
  identical per-tick output across same-tick arrival, late right arrival,
  retraction of the shared row, and 4 seeded random delta sequences (incl.
  retractions). Green; full suite 1648 passing. The `sharedarr` benchmark also
  self-checks equivalence before timing.
- **Gate (`dotnet run -- sharedarr`).** A/B of unshared (`F` private right
  traces) vs. shared (one `Arrange` + `F` shared-right joins) across fan-out
  `F ∈ {2,4,8}` and tick width `D`, with wide `R` rows. **Result: sharing wins
  across the grid (every cell ≥ 1.0×) and the win grows with fan-out — ~1.0–1.3×
  at F=2, ~1.2–1.4× at F=4, ~1.5× at F=8** (clearest at moderate `D≈1024`;
  cells carry ~±0.2× jitter, and `D=4096` is noisiest as per-step allocation
  starts to dominate). The ceiling is modest because the flat integrate it
  deduplicates is an in-place dict merge — already cheap.

### 9.4 — Limitations & the two follow-ups

1. **Spine arrangement (the bigger prize).** On the spine path the duplicated
   per-consumer maintenance is a full sorted-columnar **batch build** (+ bloom +
   compaction) — the exact §8.3 q4 substrate cost — far more expensive than a
   dict merge, so sharing it should pay much more. The variant: a spine `Arrange`
   whose consumers probe via `GroupForManySorted` against the shared trace
   (a different consumption pattern than reading a materialised `Current`), reusing
   this same `IArrangement` abstraction.
2. **Arrangement-CSE optimizer rule.** Today the feature is reachable only via
   the Core builder API; no SQL routes through it because nothing detects the
   reuse. The rule: spot a relation arranged by the same key for ≥2 operators
   (star-schema / self-join / repeated-CTE shapes, and reduce-by-K→join-on-K
   adjacency) and emit a single `Arrange` both consume. q4 itself has no such
   reuse (§6.2), so the gate for the rule is a synthetic star-schema query, not q4.
3. **No GC on the shared trace.** A shared trace can only drop keys when *all*
   consumers permit — DD's compaction-coordination sharp edge — which §6.2 says
   to leave out of the first increment. The arrangement retains full history;
   adding coordinated frontier GC is a later step.

### 9.5 — Spine arrangement increment (LANDED) — and an honest correction to §8.3

Follow-up (1) above — the spine arrangement, expected to be "the bigger prize"
because it would deduplicate the expensive sorted-batch build — was built and
gated. **The expectation was wrong: spine sharing is real and correct, but its
speedup is comparable to (often slightly below) the flat win, not larger.**

**What landed.**
- **`ISpineArrangement<TKey,TValue,TWeight>`** + **`SpineArrangeOp`**
  (`Spine/SpineArrangement.cs`): unlike the flat handle (which exposes a
  materialised `Current`), the spine handle exposes the
  `SpineIndexedZSetTrace` **itself** plus its key comparer, so consumers probe
  it by sorted-merge (`GroupForManySorted`) and never materialise the whole
  integral.
- **`SpineIncrementalJoinSharedRightOp`** (`Spine/SpineIncrementalJoinSharedRightOp.cs`):
  probes the shared trace for `R_t` and owns only the delayed left trace. The
  probe kernel was factored into **`SpineJoinProbe`** (shared by the
  private-trace `SpineIncrementalJoinOp` and this one — no behaviour change,
  `D==1`/`ForcePointProbe` seam preserved). Builder API (internal):
  `SpineArrange` + `SpineIncrementalInnerJoinSharedRight`.
- `SharedArrangementTests` is now parameterised over **both** substrates
  (14 cases, all green; full suite 1655 passing); the `sharedarr` gate verifies
  spine equivalence before timing.

**Gate result (`sharedarr`, both substrate tables in
[shared-arrangement-bench.md](shared-arrangement-bench.md)).** Across repeated
runs, both substrates win and scale with fan-out — ~1.0–1.1× at `F=2`, climbing
to ~1.2–1.4× at `F=8`. **Spine is not bigger than flat.** The absolutes explain
why: at a given `F`/`D`, sharing removes a *similar absolute* per-tick cost on
both substrates, but the spine's total Step is ~1.5–2× the flat Step (its
`GroupForManySorted` probe across batches is heavier than a dict lookup), so the
same saving is a *smaller fraction* of the spine baseline — the ratio is diluted.

**Why the §8.3 hypothesis was wrong.** Sharing removes a relation's per-tick
**maintenance** (the integrate). But that is not the dominant per-tick term: the
per-consumer **probe** of `R_t` (each join probes with its own left delta), plus
the cross-product / output build / left integrate, stay per-consumer and
unshared. So deduplicating the integrate — even the expensive spine batch build —
moves the total only modestly. More fundamentally, §8.3 conflated two levers:
the spine substrate's q4 disadvantage is the per-tick **rebuild paid even with no
reuse** (a *cross-tick / amortisation* problem), whereas arrangement sharing is a
*cross-operator* lever that only fires when a relation is genuinely arranged by
the same key for ≥2 consumers — which **q4 does not have** (§6.2). Cross-operator
sharing is therefore a real, broad-surface win for fan-out / star-schema /
repeated-CTE shapes, but it is **not** the lever that flips the spine substrate
past flat on q4.

**Net for the roadmap.** The shared-arrangement abstraction is complete and
correct on both substrates. Its realistic payoff is a modest fan-out-scaling
win, not a step change. The remaining follow-up that makes it reachable from SQL
is the **arrangement-CSE optimizer rule** (§9.4 item 2). Closing the spine
substrate's q4 gap is a *different* problem (cross-tick rebuild amortisation /
larger ticks per replica), not addressed by sharing — see §5 and §8.3.

### 9.6 — Arrangement-CSE optimizer rule (LANDED)

Follow-up (2) — the rule that makes cross-operator sharing reachable from SQL —
is in. Opt-in via `CompileOptions.ShareArrangements`.

**Why compile-time CSE, not a plan rewrite.** The logical plan is a *tree*;
sharing is a *DAG* (one arrangement, ≥2 consumers). So the rule lives in the
structural compiler as common-subexpression elimination, not as a
`PlanOptimizer` tree rewrite. The "same relation" identity is free: the compiler
already compiles a base-table scan or a repeated CTE reference to a **single
shared `Stream`** (`CompileContext.Scans` / `CteCache`). So a relation that is
the right input of two joins on the same key already shares its delta stream;
the rule adds one shared `Arrange`/`SpineArrange` over it and routes the joins
through the shared-right join from §9.1/§9.5.

**Mechanism.**
- A pre-pass (`CollectShareableArrangements`, mirroring `CollectScans`) counts
  inner-join right inputs by `ArrangementKey(source, rightKeySig)` — `source` is
  the `CteRef` (reference identity) or base-table name (value identity) — and
  marks those used ≥2×. Only bare `ScanPlan` / `CteScanPlan` right inputs (which
  compile to a shared stream) and non-NULL-accepting joins qualify.
- `CompileInnerJoin` consults a per-compile `ArrangementCache`: the first
  qualifying join builds the right index + arrangement; the rest reuse it. The
  shared relation is the `R_t` ("after") side, so the join math is unchanged and
  output is byte-identical.
- **Guards (per join):** sharing engages only with no join-key GC frontier
  (a shared trace carries no coordinated frontier GC yet — §9.4) and no snapshot
  codec (the shared-right ops aren't snapshotable yet). Because CSE is
  structural-only, the flag also **forces the structural path** (skips the typed
  fast path).

**Verification & gate.** `ArrangementSharingTests`: the rule fires (exactly one
`Arrange`/`SpineArrange`, two shared-right joins for a 2-fact star schema), does
not fire without reuse, and produces identical results to the unshared compile
over random insert/delete sequences on both substrates. Full suite 1660 green.
End-to-end gate (`dotnet run -- sharedarrsql`,
[shared-arrangement-sql-bench.md](shared-arrangement-sql-bench.md)): a wide `dim`
joined by `F` facts, structural-shared vs structural-unshared (same codec). The
SQL-level win is **~1–6%, growing with fan-out** — smaller than the operator-
level ~1.5× (§9.5) because at the query level the deduplicated `dim` maintenance
is a small fraction of per-step work (input encoding, per-fact re-index, UNION,
projection, output build are all unchanged and per-branch).

**Deferred.** Typed-path CSE (the flag forces structural today); left-side
sharing (commute inner joins so a shared relation lands on the after side);
general subplan fingerprinting beyond bare scans/CTE refs (e.g. a shared
`Filter(Scan)`); and the coordinated frontier-GC / snapshot support that the
guards currently exclude.

---

## 10. Cross-tick amortisation — the spine memtable (the actual q4 lever, LANDED)

§9.5/§9.6 established that cross-operator *sharing* is not what closes the spine
substrate's q4 gap: that gap (§8.3) is the **per-tick rebuild paid even with no
reuse**. At W=24 each replica's tick delta is `batch/24` rows, so the spine
builds many *tiny* sorted-columnar batches per tick (sort + bloom + compaction)
where the flat dictionary does in-place updates. This is a *cross-tick* problem,
and the classic LSM fix is a **memtable**.

**What landed.** `SpineIndexedZSetTrace` gains an in-memory mutable memtable
(an `IndexedZSet`). When enabled, `Integrate(delta)` folds the delta into the
memtable **in place** (flat-dictionary cost) and flushes it into ONE sorted
batch only when it holds ≥ N distinct keys — amortising the batch build + bloom +
compaction across ~N keys' worth of ticks instead of paying it every tick. Every
read path (`GroupFor`, `GroupForManySorted`, `Materialize`/`Entries`/`GroupCount`,
`IsEmpty`), GC (`DropKeysBelow`), and snapshot (`GetBatches` flushes first) merges
the memtable with the immutable batches, so results are unchanged. Weights sum
across memtable + batches exactly as across batches (Z-set addition is
commutative), so the memtable is just one more additive layer.

**Opt-in, default-off, zero behaviour change.** The threshold is a static seam
`SpineStagingConfig.Capacity` (default 0 = disabled), mirroring
`SpineJoinProbeMode.ForcePointProbe`. At 0 the memtable is never populated and
every read's `!_memtable.IsEmpty` guard short-circuits, so the trace is
byte-identical to before — all ~13 batch/level-structure assertions and the four
flat-vs-spine PBTs pass untouched. A trace reads the seam once at construction,
so it needs no operator/builder signature change (dodging the typed-compiler
reflection gotcha) and no wide plumbing.

**Verification.** New `SpineMemtableTests`: with staging on (capacities 1/4/16/64)
the trace matches a flat oracle on `GroupFor` + `GroupForManySorted` + `Materialize`
read *every tick* (mid-stream, before any flush), plus targeted memtable-only-key,
flush-on-snapshot, GC-from-memtable, and intra-memtable-cancellation cases. Full
suite 1668 green. The q4 gate's end-to-end output cross-check (every config must
reproduce flat) also passed with staging on the real parallel pipeline.

**Gate result (`q4spine`, staged configs added; [q4-spine-bench.md](q4-spine-bench.md)).**
Staging **closes the §8.3 gap**: at the realistic small batch (10k), spine goes
from clearly losing to flat — spine·point 0.47–0.66×, spine·merge 0.53–0.92× —
to **competitive parity, ~0.8–1.2×** (staging roughly *doubles* spine step
throughput); at batch 100k it reaches **parity, ~1.0–1.03×**. Run-to-run noise on
this heavy parallel benchmark straddles 1.0× at 10k (one run 1.10–1.23×, another
0.79–0.84×), so the honest claim is *gap closed / parity*, not a guaranteed win.
Two further findings: (1) **staged·point ≈ staged·merge** — once the memtable
absorbs recent deltas at tiny per-tick D, the merge probe adds little over a dict
point-probe (the §5 `D==1` caveat); the memtable, not the probe, is what matters
here. (2) Staging helps **most at small batches** (tiny ticks, where the per-tick
build dominated) and least at large batches (where the build was already
amortised over a wide tick) — the inverse of the merge probe's batch-size curve.

**Net.** This is the increment that makes the spine substrate *competitive* with
the flat dictionary on q4 — the blocker §8.3/§9.5 identified. It is opt-in and
default-off; flipping `TraceFamily.Spine` + a memtable capacity toward a default
is a follow-up gated on a fuller eval (1M events, more runs, more queries, and
tuning the capacity). The memtable applies to `SpineIndexedZSetTrace` (join +
aggregate + shared arrangements); the `SpineZSetTrace` used by DISTINCT / import
traces is the same idea, deferred.

---

## 11. Fuller evaluation — should the spine + memtable default flip? (DECIDED: no, with a caveat)

§10 showed the memtable closes the q4 gap; this section ran the broader
evaluation that decision needs: the candidate config (**spine·merge·staged** —
`TraceFamily.Spine` + merge probe + memtable) vs the flat default across the
stateful Nexmark queries, plus a memtable-capacity sweep, at **1M events, W=24,
batch 10k, median of 3 runs** (`dotnet run -- spineeval`,
[spine-eval-bench.md](spine-eval-bench.md)). The harness
(`SpineParallelHarness`) was extracted from the q4 gate so both share one
knob-driven parallel runner.

**Result.**

| query | shape | spine·merge·staged vs flat |
|---|---|---|
| q3 | join (filtered) | ~1.48× |
| q9 | join + window top-1 | ~1.03× |
| q4 | join + nested MAX + outer AVG | ~0.8–0.92× |
| q0/q1/q2 | stateless (controls) | ~1.0× ± noise |

q4 capacity sweep (sequential, the cleanest signal): memtable **off 0.39× →
1,024: 0.68× → 4,096: 0.81× → 8,192: 0.92× → 16,384: 0.86× → 65,536: 0.82×**.

**Findings.**
- **The memtable is an unconditional win for spine** and **8,192 keys is the
  knee.** It takes q4's step from 0.39× to 0.92× (a 2.4× speedup) with a clean
  peak; smaller thresholds re-introduce per-tick builds, larger ones grow the
  per-read memtable merge. It also lifts q3 to ~1.48× and q9 to ~1.03×. Whenever
  spine is used, the memtable should be on.
- **The global flat→spine flip is NOT justified.** Even staged, q4 — the
  aggregate-heavy canonical query — is still ~0.9× flat (a small regression), and
  the stateless / join-light queries gain nothing from the spine trace. Flat
  remains the safe universal default. The residual q4 gap is the **aggregate**
  read path (q3/q9, join-only, already win); the spine aggregate's per-group
  multiset reads are heavier than the flat dictionary's — a separate, later
  optimisation.
- **Spine + memtable is now a validated, competitive option** (~0.9–1.5× on the
  stateful queries, up from the pre-memtable 0.39–0.99×). It is worth selecting
  for its spill / snapshot / bounded-memory properties at near-flat throughput,
  rather than the 1.4–2.5× penalty §8.3 measured.
- **Noise caveat.** The stateless controls calibrate the bench: q1/q2 hold no
  trace, so `merge` and `staged` are identical work, yet q1 read 1.61× vs 1.13× —
  a ~±0.5× parallel-pipeline noise floor. The sequential capacity sweep (one
  query, configs back-to-back) is far more trustworthy than individual per-query
  cells.

**Decision.** Keep `TraceFamily.Flat` the default.

**Memtable-on-by-default with spine — DONE.** The one change the data justified
landed: `CompileOptions.SpineStagingCapacity` (default **8,192**, the sweep knee)
is now the public knob, and the compiler realises it whenever
`TraceFamily.Spine` is selected — both compile entry points
(`PlanToCircuit.CompileCore` for the structural / single-typed path and
`TypedPlanCompiler.TryCompileParallel` for the parallel path) set the
`SpineStagingConfig` ambient seam from the option for the duration of the build
and restore it after; each trace reads it once at construction. Threading the
capacity through every spine builder method was avoided (it would have meant
editing the typed compiler's reflective arg-arrays — the
[[typed-compiler-reflection-gotcha]]); the seam is the channel instead. The seam
is **`[ThreadStatic]`** so concurrent compiles (and direct trace construction in
the parallel test suite) cannot observe each other's value — sound because the
whole graph, including a parallel circuit's sequentially-built replicas, is
constructed synchronously on the compiling thread. `SpineStagingDefaultTests`
pins the default and confirms spine-with-memtable equals flat on a join and an
aggregate; the full spine SQL suite now compiles *with* staging and stays green
(1,672 tests).

Closing q4's residual ~8% is the spine **aggregate** read path — a separate item.

---

## 12. Spine aggregate read path — the lazy merge-view (q4's residual gap, CLOSED)

§11 left q4 the one stateful query still behind flat (~0.92× at the capacity
knee), and attributed it to the spine **aggregate** read path. This closes it.

**Where the cost was.** q4's inner `MAX(price)` compiles to
`TypedSqlMinMaxAggregator`, which is *incremental*: per tick it probes only the
few **delta** rows — `after.WeightOf(row)` — and maintains its own `SortedSet`.
But the spine aggregate op materialised the **whole** after-group every tick to
serve those few probes: `GroupForManySorted` gathers the before-run (O(N)) and
then `MergeDeltaIntoRun` merged it with the delta into a fresh sorted array
(another O(N) + an allocation), all so a handful of `WeightOf` calls could binary-
search it. Flat answers the same probes O(1) from its dictionary. For q4's
growing per-auction groups with small per-tick deltas, that redundant O(N) merge
was the spine's drag.

**The fix — `MergeViewMultiset`.** A lazy `IMultiset` view over
`(beforeRun, groupDelta)` that never builds the merged array:
- `WeightOf(v)` = binary-search the before-run + an O(1) delta lookup — so a
  probe-only tick (incremental MIN/MAX) pays no merge at all;
- `IsEmpty` short-circuits in O(1) for a non-empty (growing) group — the first
  before-run value the delta does not touch has net = its before weight ≠ 0;
- `SumWeights` = `sum(beforeRun) + delta.SumWeights()` (no merge);
- enumeration (SUM/COUNT/AVG and the non-incremental MIN/MAX `Compute`) folds the
  two lazily — same work as the old eager merge, minus the throwaway array.

Enumeration order is no longer sorted, which is sound because every `IMultiset`
consumer folds commutatively (MIN/MAX/SUM/COUNT/AVG and the sketches). The op's
merge path now hands the aggregator a `MergeViewMultiset` instead of
`SortedRunMultiset(MergeDeltaIntoRun(...))`; the eager merge is gone. Drop-in,
contained to the spine aggregate op + one new type.

**Verified.** Full suite 1,672 green (the existing flat-vs-spine SUM/MIN PBTs and
the SQL random-query PBTs exercise it). The operator gate (`aggprobe`) keeps the
merge path ahead of point-probe (1.5–4.3×).

**Result (`q4spine`, batch 10k, three median-of-2/3 runs).** q4
spine·merge·staged vs flat now reads **0.99×, 1.11×, 1.20×** — **parity-to-ahead**,
up from §11's 0.92×, and spine·merge·staged now beats spine·point·staged (the
merge-view made merge the winning probe). The exact figure sits inside the
parallel bench's ~±0.1× noise, but the gap is closed: **all three stateful
Nexmark queries (q3, q4, q9) are now at parity-or-better with spine + memtable**.
This removes §11's q4-specific objection; the global default stays `Flat` for the
unchanged reasons (stateless / join-light queries gain nothing, conservative
default).

The remaining spine read-path idea — for probe-only aggregators, skip even the
O(N) `GroupForManySorted` gather and probe individual delta values across batches
(`WeightOfValueInGroup`) — was scoped but not needed to reach parity; it is
blocked on a cheap per-group emptiness signal (today `IsEmpty` needs the
before-run) and left as future work.

---

## 13. The memtable for the non-indexed trace — DISTINCT & import (LANDED)

§10 added the memtable to `SpineIndexedZSetTrace` (join + aggregate state). Its
non-indexed sibling `SpineZSetTrace` backs **DISTINCT** (`SpineDistinctOp`) and
the **recursive-CTE import** state (`SpineImportTrace`), and paid the same
per-tick batch-build cost. This applies the identical memtable there.

**What landed.** `SpineZSetTrace` gains a mutable `ZSet` memtable, mirroring the
indexed trace: `Integrate` folds the delta in place (flat-dictionary cost) and
flushes into one sorted batch only at ≥ capacity keys; every read (`WeightOf`,
`Materialize`/`Entries`/`KeyCount`, `IsEmpty`), GC (`DropKeysBelow`), and snapshot
(`GetBatches` flushes first) merges the memtable with the batches. Same opt-in
`SpineStagingConfig.Capacity` `[ThreadStatic]` seam (default 0 = disabled,
byte-identical), realised from `CompileOptions.SpineStagingCapacity` by the
compiler — so DISTINCT and import traces pick it up **automatically** (their ops
construct the trace without an explicit capacity → read the seam), no operator or
builder change. New `SpineZSetMemtableTests` (caps 1/4/16/64 match a flat oracle
on WeightOf/Materialize/Entries/IsEmpty read every tick, + memtable-only-key /
flush-on-snapshot / GC-from-memtable / cancellation). Full suite 1,680 green.

**Gate (`dotnet run -- distinct`, [distinct-bench.md](distinct-bench.md)).** A
`SELECT DISTINCT` over a churning table, flat vs spine vs spine·staged at
pre-loaded sizes:
- **`staged/spine` = 0.17–0.50×** — the memtable makes spine DISTINCT **2–6×
  faster than un-staged spine** (the §10 lever: integrate becomes a dict merge,
  and the trace holds far fewer batches so probes are cheaper too).
- **`staged/flat` = 1.0–2.2×** — staged reaches **parity with flat at small N**
  but stays ~2× behind at larger N.

**Honest read.** Unlike the join/aggregate, DISTINCT is **probe-bound** — every
step probes the trace, and a sorted-batch probe is O(log) + a bloom check versus
the flat dictionary's O(1) hash. The memtable amortises the *integrate*, not that
*probe*, so it closes most of the gap (and all of it at small N) but does not make
spine DISTINCT beat flat on throughput. Spine's value for DISTINCT/import remains
spill / snapshot / bounded memory, now at a much smaller throughput cost. The
import (recursive-CTE) path is `Materialize`-bound per outer tick (it reads the
whole consolidated set), so it benefits on the integrate exactly as DISTINCT does.

This was the last deferred row-representation item: every spine trace family now
has the cross-tick memtable, on by default when `TraceFamily.Spine` is selected.

---

## 14. Integer surrogate keys / dictionary-encoded rows — revisiting §6.5

> Status: **design proposal, no code yet.** This is the deliverable of a
> design-first session that deliberately re-opens Option 6 (§6.5), which the
> original doc *demoted*. The demotion was correct **relative to the world the
> doc assumed we were heading into** (DD/Feldera sorted-merge). The spine arc
> (§5–§13) since established a different world — **flat-dictionary execution
> beats sorted-merge on our fine-grained ticks** — and in *that* world the
> surrogate lever comes back live. This section re-litigates §6.5 against the
> current code and scopes a benchmark-gated first increment.

### 14.1 — Why the demotion no longer binds: the flat-dict-wins reframe

§6.5 demoted global row interning because the doc's whole thesis (§7) was to
adopt the reference systems' answer: **stop hashing whole rows, compare sorted
keys instead** (merge/gallop on the spine). Against that plan, interning is
redundant — sorted-merge already avoids the whole-row hash.

But the spine arc settled the §5 caveat the other way:

- §8.3: the spine **substrate loses to the flat dictionary on q4** (1.4–2.5×),
  because our fine-grained per-tick deltas make per-tick sorted-batch rebuild
  dominate.
- §10–§13: the memtable claws spine back only to **parity** with flat, never
  past it on the aggregate-heavy query, and only by *re-introducing an in-memory
  mutable dictionary* (the memtable **is** a flat dict) in front of the sorted
  batches.

So the empirical verdict of §5–§13 is: **for DbspNet's tick granularity, the
flat dictionary is the winning execution model.** Sorted-merge — the mechanism
that made interning redundant — is *not* the path. That removes the reason
§6.5 had to demote surrogates. The live question becomes: given the flat dict
wins, what is its *one remaining cost*, and can we remove it?

Its one remaining cost is exactly §1: **the keys it hashes are whole rows.**
The natural synthesis the original doc never considered (because it was
committed to sorted-merge) is:

> **Keep the flat-dictionary execution model that won, but make its keys cheap
> to hash — replace whole-row keys with interned integer surrogates.**

DD/Feldera don't need this because their key-comparison model already sidesteps
whole-row hashing; we measured that model losing, so we need the other half.

### 14.2 — Where whole-row hashing actually remains on the flat path (verified)

The routing hash is already narrow — `ExchangeIndexOp._partition` hashes
`keyOf(row)` (the bigint key), not the row (the §1 "exchange hashes the full
row" row is **stale**; the ExchangeIndex fusion fixed it). What remains, all in
**inner value multisets keyed by the full row**:

| Site | Code | Cost shape |
|---|---|---|
| Aggregate group rebuild | `IncrementalAggregateOp.cs:126` — `afterGroup = beforeGroup + groupDelta` | **Re-hashes the *entire* inner Z-set every tick a group is touched**, unconditionally (needed for `Update` and the `IsEmpty` gate). For a group that grows over K ticks → **O(K²) whole-row hashes.** *Verified:* `ZSet.Plus` → `ZSetBuilder.From` (`ZSetBuilder.cs:91-94`) does `new Dictionary(cap)` then `d[k]=w` per entry — a genuine per-entry re-hash, **not** the bucket-copy fast path, so the existing group is rehashed in full, not copied. |
| Exchange gather | `ExchangeIndexOp.cs:94` — `indexed.Add(keyOf(row), row, w)` | one whole-row hash per gathered row per tick (builds the inner Z-set). |
| Join trace probe/build | `IncrementalJoinOp` cross-product over `GroupFor(key)` inner Z-sets | stored side's rows hashed on integrate; cross-product re-touches them across ticks. |
| Output build | `ZSetBuilder.Add` of result rows | one whole-row hash per output row. |

The **critical correction to §6.5(b).** §6.5 dismissed interning wide fact rows
because "Nexmark bid rows are near-unique → interning costs a hash and saves
almost nothing." That reasoning silently assumed each row is hashed **once**.
The aggregate code disproves it: a value row in a *growing* group (q4: bids
accumulate per auction until it closes) is re-hashed **every tick the group
changes** by the line-126 rebuild. A row interned **once** then re-touched as a
cheap int across those K ticks turns O(K²) struct-hashes into K struct-hashes +
O(K²) int-hashes. **The recurrence §6.5 said didn't exist is created by
incremental maintenance itself**, independent of row uniqueness.

### 14.3 — Honest re-litigation of §6.5's five objections

| §6.5 objection | Verdict under the current design |
|---|---|
| (a) Interning re-introduces the hash you remove | **Partly stands, but amortizes.** You pay one struct-hash to intern, then every *repeated* touch is an int-hash. Net win iff a row is touched > ~1× inside the operator — true for all **stateful** operators (agg rebuilds, join cross-products, retained traces), false for stateless map/filter. ⇒ **apply surrogates to stateful operators only.** |
| (b) Only low-card keys win; wide fact rows hashed once | **Refuted for the hot path** (§14.2): incremental maintenance re-hashes the same wide rows across ticks. |
| (c) W>1 contention on a shared intern table | **Defused by locality.** Surrogates need not be global. Post-exchange, a stateful operator's value rows are already co-located on one worker. Make the intern table **operator-local (per replica)** — no cross-worker sharing, no contention. The price is de-referencing at the operator's output boundary (int→row), a cheap dict lookup. |
| (d) Breaks hash-partition routing | **Refuted.** Routing already uses the narrow extracted key (§14.2) and happens *before* the operator-local surrogate space exists. Surrogates live inside a worker, after routing. |
| (e) Neither DD nor Feldera does it | **True but no longer dispositive.** They use sorted key-compare, which we measured losing (§8.3). "Flat dict + int surrogates" is the synthesis their model doesn't need and ours does. |

The demotion survives **only** as "don't build a *global row-interning* scheme."
The salvageable, now-recommended form is **operator-local, reversible surrogate
encoding of inner value multisets in stateful operators** — which is also what
§6.5's own closing line gestured at ("salvageable narrow form").

### 14.4 — The design

**Core idea.** A stateful operator owns a reversible bijection
`RowDict : TValue ↔ int` (a `Dictionary<TValue,int>` forward + `List<TValue>`
reverse). Its inner value multisets become `ZSet<int, Z64>` instead of
`ZSet<TValue, Z64>`. Per tick:

1. **Intern** each *delta* value row → int (one struct-hash per distinct delta
   row, paid once ever for that row).
2. All trace/group state, the line-126 rebuild, and join cross-products operate
   on `int` keys (int-hash, no struct-hash, no per-field compare).
3. **De-reference** only at the operator's output (`reverse[id]`), and only for
   rows that actually appear in the output delta.

**Reversibility is mandatory** because aggregators need the real value:
`MIN/MAX` compare values (surrogate order ≠ value order, so they deref to
compare), `AVG/SUM` read the numeric column. The deref is an indexed
`List<TValue>` read (`O(1)`, no hash) — strictly cheaper than re-hashing the
struct. Incremental `MIN/MAX`/`SUM` only touch *delta* rows + their own state,
so they barely deref; the win there is the **op's** line-126 rebuild going int.

**Surrogate lifetime / GC.** The reverse list must drop a row when its net
weight across the operator hits zero **and** no retained state references it.
Frontier GC already drops whole groups (`DropKeysBelow`); per-row reclamation
inside a surviving group needs a refcount (sum of `|weight|` occurrences). Open
sub-question — first prototype can **leak** (never reclaim) to isolate the hash
win, then add reclamation once the win is proven (mirrors how the spine
prototypes deferred GC).

**What stays struct-keyed.** Stateless operators (map/filter/the exchange
itself), the **outer** group key (already narrow), and the snapshot codec
boundary (serialize de-referenced rows; surrogates are an in-memory runtime
encoding, never persisted — sidesteps the codec/versioning problem entirely).

### 14.5 — The cheaper alternative the design must be honest about

For the **aggregate specifically**, there is a non-surrogate fix that captures
much of the same win: **port the §12 lazy `MergeViewMultiset` to the flat
path.** Line 126 materializes `afterGroup` unconditionally; §12 showed the spine
op avoid it with a lazy view that answers `WeightOf`/`SumWeights`/`IsEmpty`/
enumerate over `(beforeGroup, groupDelta)` without building the merged dict.
A flat lazy view would kill the O(K²) rebuild **without** any surrogate
machinery — smaller, lower-risk, aggregate-only.

The honest trade:

- **Flat lazy merge-view** — small, low-risk, fixes *only* the aggregate
  rebuild. Doesn't touch join cross-products, exchange gather, or output build,
  and doesn't make the dict's keys cheap (still struct-hashes on probe/intern).
- **Surrogates** — larger, generalizes across join + aggregate + output, makes
  *every* inner-multiset hash an int-hash, but adds the intern/deref/GC
  machinery and the locality argument.

Recommended sequencing: **prototype the flat lazy merge-view first as the
control** (it's the cheap baseline the surrogate must beat), then the surrogate
aggregate, A/B both against the struct-keyed flat aggregate on the q4 step. If
the lazy view alone closes q4, surrogates must justify themselves on the
**join**-heavy queries (q3/q9) where there is no single group rebuild to elide.

### 14.6 — Benchmark-gated first increment

Smallest decisive experiment, mirroring the spine prototypes' discipline:

1. **Control:** flat lazy merge-view in `IncrementalAggregateOp` (no surrogates).
2. **Treatment:** surrogate-encoded inner value multiset in a variant aggregate
   op (operator-local `RowDict`, leaking GC), behind a flag/seam like
   `SpineJoinProbeMode.ForcePointProbe`.
3. **Harness:** reuse `SpineParallelHarness` (the §11 knob-driven parallel
   runner) — add a `RowEncoding ∈ {Struct, LazyView, Surrogate}` knob; A/B on
   **q4** (growing groups, the strongest surrogate case) and **q3/q9**
   (join-heavy, the generalization case) at W=host, 1M events, batch 10k, with
   the existing per-tick output cross-check against the struct path.
4. **Gate:** ship surrogates only if they beat **both** the struct path *and the
   lazy-view control* on q4 step at W=14, **and** win on q3/q9 (where the lazy
   view can't), **without** regressing low-fan-out / tiny-group cases (the
   intern overhead with no re-touch to amortize — the §14.3(a) loss regime).

Microbench precursor (cheaper, run first): extend `PureTraceBenchmark` /
add a `surrogatebench` that A/Bs a `Dictionary<WideStruct,Z64>` vs
`Dictionary<int,Z64>` + intern, sweeping row width and re-touch count R — this
directly measures the §14.3(a) crossover (R where intern+int-hash beats
R struct-hashes) and predicts which queries can win before any operator wiring.

### 14.7 — Risks & open questions

- **The R-crossover is the whole ballgame.** If realistic per-operator re-touch
  counts are low, surrogates lose to their own intern cost. The microbench
  (§14.6) must establish the crossover *before* operator work.
- **Typed-path reach.** q4 runs the **typed** parallel path
  (`TypedPlanCompiler` + emitted structs). The surrogate op must be reachable
  there without changing a builder signature (the
  [[typed-compiler-reflection-gotcha]]); favor a seam/wrapper over a new generic
  parameter, as the memtable did.
- **De-ref locality at output.** Output rows must deref before leaving the
  worker; confirm the downstream consumer (another exchange / the egest) doesn't
  re-intern, which would thrash. Cross-operator surrogate *sharing* (one space
  across an operator chain) is a later optimization, not the first increment.
- **MIN/MAX deref in non-incremental rescan.** The structural (non-incremental)
  MIN/MAX scans the whole group and would deref every element every tick —
  measure whether deref-per-scan still beats struct-hash-per-rebuild, or gate
  surrogates to the incremental aggregators only.
- **GC reclamation** (deferred to a second increment; leak in the prototype).

### 14.8 — Recommendation

Re-opening §6.5 is justified: its demotion was sound for a sorted-merge future,
and the spine arc chose a flat-dict present. The recommended path is **design as
above, then the §14.6 microbench first** — it is cheap and decides whether the
operator wiring is worth it. Treat the **flat lazy merge-view as the mandatory
control**, because for the canonical query (q4) it may capture most of the win
at a fraction of the risk, and surrogates must earn their keep on the
**join-heavy** queries the lazy view cannot help. This keeps the bet
benchmark-gated and avoids re-committing the original doc's error of ranking an
option before measuring it.

### 14.9 — Microbench RAN: the crossover is real, but the lazy view dominates the only high-R site (DECISION: build the lazy view, not surrogates)

The §14.6 microbench is built (`dotnet run -- surrogatebench`,
`SurrogateKeyBenchmark`, report [surrogate-key-bench.md](surrogate-key-bench.md))
and run. It A/Bs a whole-row-keyed `Dictionary` (the emitted typed row's
multi-field hash) against an interned-`int`-keyed one — surrogate totals
**include** the one-time intern — across row width (`W2`/`W4`/`W8` longs, `WStr`
= a Nexmark-bid-like 3 longs + string) and re-touch `R`, faithfully modelling the
verified `ZSetBuilder.From` per-entry re-hash. Two findings:

**(1) The crossover and the width-independence thesis are confirmed.** R\* ≈ 2–4
for any row wider than ~2 longs; the surrogate win then scales with both `R` and
width. The faithful **growing-group** table (one group → K, rebuilt each tick,
the q4 per-auction shape) shows surrogate vs struct **WStr 1.4× at K=16 → 5.3× at
K=1024**, W8 up to 3.7×, even narrow W2 1.05–1.4×. The headline signal: at
K=1024 the **surrogate rebuild is ~2.3–2.5 ms regardless of width** (W2 2.96, W4
2.31, W8 2.49, WStr 2.47 ms) while the struct rebuild scales 3.0→13.1 ms with
width — surrogates convert a width-dependent whole-row hash into a
width-independent int hash, exactly as §14.1 argued.

**(2) The `R=1` column is the decisive caveat — and it reframes everything.** At
`R=1` (a row touched once) surrogates **lose** (W2 0.70×, W4 0.99×, WStr 1.05×):
pure intern overhead with nothing to amortise — the §14.3(a) loss regime,
measured. So the whole question collapses to: **where in the engine is
`R ≫ 1`?** Inventorying the §14.2 hash sites by re-touch count:

| Site | Re-touch R of a given row | Surrogate verdict |
|---|---|---|
| **Aggregate group rebuild** | **O(K)** — re-hashed every tick the group is touched | the *only* `R ≫ 1` site |
| Exchange gather | 1 (gathered once per tick it appears) | loses (R=1) |
| Join trace integrate | ~1 (hashed once on `MergeInPlace`; churn aside) | loses |
| Join cross-product | 0 re-hash (matched rows are *enumerated*, not hashed) | n/a |
| Output build | 1 (each result row built once) | loses |

**The only `R ≫ 1` whole-row hashing in the engine is the aggregate rebuild** —
and the **flat lazy merge-view** (§14.5's mandatory control) *removes* it rather
than cheapening it: probe only the delta rows against the before-`ZSet`, never
materialise `afterGroup`, turning the op's **O(K²)** rebuild into **O(K)**.
Removing the rebuild asymptotically beats making its hashes cheaper (O(K)
struct-probes < O(K²) int-hashes), so for the aggregate the lazy view dominates
the surrogate. And it is **cheap to build**: the §8.2 `IMultiset` widening
already lets `Update` consume a non-`ZSet` after-multiset, and §12's
`MergeViewMultiset` is the spine precedent — a flat analogue over
`(beforeZSet, groupDelta)` needs no surrogate machinery (no intern table, no
reversibility, no per-row GC reclamation, no W>1 locality argument, no typed-path
reach).

**Decision.**
1. **Build + benchmark the flat lazy merge-view** as the real aggregate lever.
   Asymptotic argument predicts it dominates the surrogate on the one site that
   matters; confirm with an `aggprobe`/`q4spine`-style gate.
2. **Do not build operator-local surrogate encoding.** It loses at every `R≈1`
   site (most of them) and is dominated by the lazy view at the one `R≫1` site.
3. **Global / cross-operator interning stays demoted (§6.5), now with measured
   support.** The *only* surrogate form that could win broadly is sharing one
   surrogate space *across* an operator chain (so each per-operator `R=1`
   compounds into pipeline-wide `R = #operators`) — but that is precisely
   §6.5's global-interning form, with its W>1 contention, boundary-hash, and
   routing hazards. The `R=1` per-operator loss reinforces the demotion.

This is the §14.6 gate working as intended: **measuring first retired an XL
option (surrogate encoding + its intern/reversibility/GC/locality machinery)
before any operator was wired**, and redirected the effort to a small, lower-risk
change (the flat lazy view) that the same inventory shows is the actual lever.
The next increment is therefore the **flat lazy merge-view**, benchmarked against
the struct rebuild on q4 — not surrogates.

### 14.10 — Flat lazy merge-view LANDED + gated (the §14.9 lever, built)

The §14.9 lever is implemented and benchmark-gated.

**What landed.** `LazyMergeMultiset<T>`
(`Operators/Stateful/LazyMergeMultiset.cs`) — the flat analogue of §12's spine
`MergeViewMultiset`, and simpler: the flat before-group is a hashed `ZSet` with
O(1) `WeightOf`/`Contains`, so no binary search is needed. `IncrementalAggregateOp`
now hands the aggregator this lazy view over `(beforeGroup, groupDelta)` instead
of materialising `afterGroup = beforeGroup + groupDelta` (the verified
per-entry-re-hash rebuild). The eager rebuild is kept behind a process seam
(`FlatAggregateMode.ForceEagerRebuild`) for the A/B, and the per-key body was
factored into a shared `EmitForKey(…, IMultiset afterGroup, …)`. The lazy view is
the **default**; it only ever *removes* the rebuild + its allocation, so there is
no regression regime (a single-element group builds a view object instead of a
1-entry dict — comparable).

**Correctness.** `LazyMergeMultiset.IsEmpty` matches the eager
`(before + delta).IsEmpty` exactly: `ZSetBuilder` drops zero-net entries, so the
eager after-group's dict-shape emptiness *is* "every value cancels", which the
view reproduces (zero-free before run; scan stops at the first surviving value —
O(1) for a growing group). Full suite green (**1743 passed, 0 failed**),
including the existing flat-vs-spine SUM/MIN PBTs and the SQL random-query PBTs.

**Gate (`dotnet run -- flatagg`, [flat-agg-bench.md](flat-agg-bench.md)).** The
q4 shape — `MAX(price)` over growing per-auction groups (`MAX` blocks
`NarrowAggregateInput`, keeping the inner value rows *wide*) — driven through
compiled SQL, eager rebuild vs lazy view, output cross-checked identical:

| K (final group size) | eager | lazy | Speedup |
|---:|---:|---:|---:|
| 128  | ~204 ms | ~24 ms  | **8.6×** |
| 512  | ~556 ms | ~120 ms | **4.6×** |
| 2048 | ~13.8 s | ~715 ms | **19.3×** |

**Reading it — the win's two regimes (verified).** The structural
`MinMaxAggregator` implements only `Compute(IMultiset)` and inherits
`Update => Compute(after)`, so it **scans the whole group every tick** (confirmed
by reading it; the `benchmarks.md` "MIN/MAX keep a sorted set" note describes the
*typed* `TypedSqlMinMaxAggregator`, not this one). So this gate measures the
**constant-factor** win: the lazy view removes the per-tick dict allocation +
wide-row re-hash, leaving only the aggregator's scan — already 4.6–19×, growing
with K because the eager path allocates and re-hashes a K-entry dict *every*
tick. For the **incremental** aggregators (SUM/COUNT/AVG, and the **typed**
MIN/MAX that **q4 actually runs**), the rebuild was the *only* O(K) per-tick
term, so the lazy view additionally collapses the asymptotics **O(K²)→O(K)** —
the typed q4 path gets that on top of the constant win shown here.

**Operator-level vs query-level.** The table above is an *operator-level* gate on
the aggregate step in isolation. End-to-end q4 is join + exchange + outer AVG +
the inner MAX, so the query-level gain is Amdahl-diluted by the work the lazy
view does not touch.

**End-to-end q4 gate (`dotnet run -- q4flat`,
[q4-flat-bench.md](q4-flat-bench.md)).** The whole q4 `W`-replica parallel
pipeline (`TraceFamily.Flat`, W=24, 1M events) run flat·eager vs flat·lazy
through the `SpineParallelHarness` (a `ForceEagerRebuild` knob added to
`RunConfig`), step phase timed apart from split/gather, lazy output
cross-checked identical to eager:

| Batch | flat·eager step | flat·lazy step | Step↑ |
|---:|---:|---:|---:|
| 10k  | ~620 ms | ~488 ms | **~1.3–1.6×** |
| 100k | ~610 ms | ~413 ms | **~1.5×** |

So the operator-level 4.6–19× dilutes to a **~1.3–1.5× query-level step win** on
q4 — real and worthwhile, since q4 is the worst non-inherent gap vs Feldera
(0.53×) and is step-bound on exactly this aggregate. **Noise caveat:** this is
the noisy parallel bench (the 100k batch is only ~10 Step calls per pass); across
runs the 10k win held 1.28–1.55× while a single 100k pass once read 0.83× before
settling to ~1.5× at runs=5 — trust the small-batch (realistic Nexmark operating
point) number and the multi-run medians. The lazy view is the default and only
ever removes the rebuild, so there is no regression regime; the bench variance is
measurement, not the operator.

**Status: the row-rep flat-path lever is shipped.** The surrogate question is
closed (dominated, §14.9); the flat lazy merge-view is the realized win on the
flat default path — operator-level 4.6–19×, query-level ~1.3–1.5× on q4 — with
full-suite correctness and no regression regime.

---

## 15. Exchange / parallel-scaling — opening up the step (the coordination ceiling)

With the row-rep flat-path arc shipped (§14), the remaining Feldera gaps —
q4 (0.66×), q18 (0.41×), q19 (0.67×), q22 (0.70×) — are all the
*parallel-scaling efficiency* ceiling: every query realises only ~2.4–4.9× on
14 cores. The q18 profile (docs/q18-profile.md) localised the cost to the
**step** (not ingest/egest) and showed it saturates by ~W=12, but the step
itself was a black box. The going-in hypothesis from
[parallel-pipeline-perf] was that the step is **CPU/bandwidth-bound whole-row
movement** through the all-to-all shuffle. This section opens the step up and
**the measurement overturns that hypothesis.**

### 15.1 — Method: the step decomposition profiler

`StepProfiler` (`Core/Circuit/StepProfiler.cs`, a default-off internal seam in
the established `SpineStagingConfig`/`FlatAggregateMode` mould — benchmark-only,
each worker accumulates its own `[worker]` array slot during a Step, the
controller reads after the end-of-tick barrier publishes the writes, zero
production cost when disabled) splits each worker's `Step` into four phases:
**split** (bucket this shard's rows by `hash(key)%W` — instrumented in
`ExchangeOp`/`ExchangeIndexOp`), **wait** (`_coordinator.Wait` — pure idle at the
per-exchange `Barrier`), **gather** (rebuild the post-shuffle indexed Z-set,
re-hashing full rows), and **op** (`step − split − wait − gather`, the residual
join/aggregate/TOP-K compute). It also records each replica's whole-`Step` raw
ticks, so the report can compare the **mean** per-worker step against **Ctrl**
(the controller's real per-step wall clock = `Σ_tick max_worker step` — what
actually bounds throughput). `dotnet run -- stepprofile [events] [q…] [W-sweep]`
writes docs/step-profile.md. A query with several exchanges sums their phases
into the same slot (so the figures are step-total movement/idle, not one
exchange's).

Two derived ratios separate the failure modes:
- **Strag = Ctrl / mean-Step** — the barrier's straggler tax (1.00 = none; the
  per-tick slowest worker dragging the rest shows here).
- **Imbal = max-busy / mean-busy** (busy = step−wait) — *persistent* per-worker
  work skew (one worker always heavier), distinct from per-tick straggling.

### 15.2 — What the measurement found (1M events, batch 10k, i9-12900K)

The W-sweep (docs/step-profile.md) is decisive and consistent across q18 (TOP-1,
1 exchange), q4 (join + 2 aggregates, multi-exchange) and q19 (TOP-10, 1
exchange):

1. **The operators scale ~7–9×; the realised step scales only ~3.5–5×.** At
   W=24: q18 Op↓ 8.2× vs **Ctrl↓ 3.5×**; q4 Op↓ 6.9× vs **Ctrl↓ 3.5×**; q19 Op↓
   9.2× vs **Ctrl↓ 5.1×**. Ctrl flattens by ~W=12–16 and then wobbles or
   regresses (q4 is no better at W=24 than W=16). **The operators are not the
   bottleneck — coordination is.**
2. **Wide-row movement is a *small and shrinking* term, not the ceiling.**
   split+gather is 5–22% of the step and *falls* with W (q4 Move% 31→5% over
   W=4→24; the gather — the one residual whole-row-rehash site, §14.2 — is
   0.2–1.1 ms and shrinks as rows split W ways). **This refutes the
   bandwidth-bound-movement hypothesis:** the all-to-all of wide rows is cheap
   relative to the step, and gets cheaper per worker as W grows.
3. **The dominant non-scaling term is the barrier WAIT, and it rises with W.**
   q4's exchange Wait% climbs 6→14→19→36→36→**40%** over W=4→24 — at W=24 two
   fifths of the step is idle at the exchange barrier. q4 carries the most
   exchanges (its join's two inputs), so it has the most barrier-straggler
   exposure and is the worst-scaling join query — exactly the 0.66× Feldera gap.
4. **Single- vs multi-exchange queries wear the same tax in different places.**
   q4 (multi-exchange) shows it as high exchange **Wait%** (fast workers idle at
   the exchange barrier) with low Strag; q18/q19 (one exchange) push more of the
   imbalance *past* the exchange into the TOP-K, so it surfaces as **Strag**
   rising to ~1.4 (the controller's done-barrier waiting on the slowest worker's
   post-exchange work). Same root cause — a global rendezvous paying for the
   slowest worker each tick — located at whichever barrier the imbalance lands
   before.
5. **Persistent skew is modest (Imbal ≤ 1.4).** It is *not* one hot worker; it is
   *per-tick rotating* straggling (the unlucky worker varies tick to tick) plus,
   at W>16, core heterogeneity (below). So rebalancing the hash would not help —
   the partition is already even on average.

### 15.3 — Hybrid-core note (both boxes are hybrid — not a confound)

The step-decomposition host is an i9-12900K: a **hybrid** 8 P-core (16 threads) +
8 E-core (8 threads) part, 24 logical. Past W≈16 some workers land on the slower
E-cores and become *permanent* stragglers the barrier waits on every tick — so
the W>16 rows mix that heterogeneity with the structural barrier tax, and explain
why q4 is no faster (sometimes slower) at W=24 than W=16. **The Feldera
comparison box ([[nexmark-feldera-w14-snapshot]]) is an Apple M4 Pro — also
hybrid (the 14-core part is ~10 P + 4 E), confirmed by the user 2026-06-10.** So
the E-core straggler tax is present in the *actual comparison numbers too*, not
just this local profile — which makes the decomposition *more* representative of
the comparison environment, not less. Two consequences:

- **It sharpens lever 1 (§15.5).** The comparison runs DbspNet at W=14 on a 10P+4E
  box, i.e. 4 workers pinned to permanent-straggler E-cores. The decomposition
  predicts DbspNet step throughput should **peak around W≈10 (P-cores only) and go
  flat-or-worse at W=14** — so *capping W at the P-core count is a concrete,
  testable throughput claw-back on the very box the comparison uses*, not a
  homogeneous-box non-event as first written.
- **It does not explain the gap away.** Feldera runs on the *same* hybrid box and
  still wins q4/q18/q22, so the gap is not a measurement artifact: either its
  per-row work is cheaper (same straggler tax = smaller fraction of the step) or
  its scheduling tolerates core heterogeneity better than our static equal-shard
  BSP. This profile (our side only) cannot separate those — but on a hybrid box,
  static equal sharding is *especially* penalised, which is itself an argument for
  the adaptive-scheduling direction (levers 3–4).

The **portable** finding is unchanged and machine-independent: operators scale
~7–9× while the step scales ~3.5–5×, the gap being barrier wait that grows with W.
(If anything the M4 Pro's high unified-memory bandwidth makes the *movement* term
even smaller relative to coordination — reinforcing the conclusion.)

### 15.4 — Why this is substantially fundamental

The engine is lockstep **BSP / SPMD-replica** (§ParallelCircuit): W replicas
advance in lockstep, every `exchange` is a hard `Barrier` over the W worker
threads, and the controller barriers the whole tick. The barrier always pays for
the slowest worker. As W grows, each worker's per-tick slice (≈ batch/W = 10k/W
rows) shrinks, so (a) the *relative* variance in per-tick row counts grows, (b)
the fixed per-barrier latency amortises over less work, and (c) on a hybrid CPU
the slow-core workers stop hiding. All three make the idle fraction *grow* with
W — precisely what the data shows.

The obvious escape — let a fast worker steal a slow worker's rows — is **blocked
by the co-location invariant**: a stateful operator (aggregate, TOP-K, join)
keeps a key's running state on one fixed worker, so its rows cannot move to
another worker mid-stream without moving the state. Dynamic rebalancing is
incompatible with the replica model for exactly the stateful operators that
matter. This is why Differential/Timely and Feldera/DBSP use **asynchronous
frontier-based progress** (data flows through channels; workers synchronise on
logical-time frontiers, not a hard per-operator thread barrier) rather than
lockstep BSP. **Correction (see §15.8): this applies to Differential/Timely, NOT
to Feldera/DBSP.** DBSP is a *synchronous, clocked* circuit engine — per-tick
exchange + barrier, like ours — confirmed by the §15.8 experiment showing Feldera
is *uniformly* straggler-sensitive (the fingerprint of BSP, not async frontier).
So "matching Feldera" is **not** an async-rewrite goal — both engines are BSP; the
async-frontier model is the Timely/DD escape, a bigger rewrite than parity with
Feldera. Even then the *slowest-worker-per-tick* lower bound on a synchronous
incremental result is partly intrinsic.

### 15.5 — Ranked levers

1. **Right-size W to the fast-core count; do not oversubscribe.** *Cheap config —
   and, given §15.3, a directly testable win on the comparison box.* The data
   shows the efficiency knee at ~W=12–16 and W>16 flat-to-negative on the i9;
   defaulting W to the P-core count (not logical cores) avoids the E-core
   straggler tax. Because the comparison M4 Pro is *also* hybrid (10P+4E) and the
   comparison runs DbspNet at W=14 (4 workers on E-cores), the prediction is that
   **DbspNet at W≈10 may beat its own W=14** on q4/q18/q22 — a concrete experiment
   for the next comparison run, not a homogeneous-box non-event.
2. **Coarser ticks (larger batch) where the latency budget allows.** *Config /
   tradeoff.* A bigger per-worker slice lowers relative per-tick variance and
   amortises barrier latency over more work (consistent with the prior finding
   that large batches scale better). Pure latency↔throughput knob, not free, but
   the cheapest real throughput lever for batch-shaped workloads.
3. **Coalesce co-partitioned exchange barriers (multi-exchange queries).**
   *Bounded engine change — the one gated prototype candidate.* q4's two
   join-input exchanges are two separate `Barrier` rendezvous per step and own
   its 40%-wait term; publishing both grids and rendezvousing **once** (one
   barrier, then gather both indexed inputs) halves q4's straggler exposure
   without touching the join math. Gate: the `stepprofile` q4 Wait% / Ctrl before
   vs after. This is the highest-value *clean* lever the measurement points to.
4. **Asynchronous / overlapped exchange** (replace the hard `Barrier` with
   per-cell ready-flags so a worker's gather consumes each source bucket as it is
   published, overlapping the slow source's publish with fast sources' gather).
   *The Timely model — a real execution-model change.* The slowest-source bound
   remains, so the upside is barrier-latency removal + compute/movement overlap,
   not unbounded. High complexity/risk; **investigate, do not commit** without a
   prototype showing it beats the coalesced-barrier (#3) lever.
5. **Narrow the gather** (append to per-key lists instead of hashing full rows
   into an `IndexedZSetBuilder` when the exchange input is distinct and no
   column-dropping map sits upstream — the gather analogue of the split
   bucket-list opt). *Mechanical, low-risk, small.* Measured upside is the
   5–22%-and-shrinking gather term, so minor; column-*pruning* the shuffle stays
   blocked by non-linear MIN/MAX (q4, [parallel-pipeline-perf]) and the data says
   it would not help much anyway (movement is not the ceiling). Do opportunistically.
6. **Heterogeneity-aware weighted shards / thread affinity.** *Keep demoted, but
   note it is now relevant to the comparison box.* Both the i9 and the comparison
   M4 Pro are hybrid, so weighting E-core workers' shards smaller (or pinning
   workers to P-cores) would, in principle, help on the actual benchmark hardware
   — but it needs affinity pinning the OS scheduler fights and is brittle across
   machines. Lever 1 (just cap W at the P-core count) captures most of the same
   benefit far more cheaply, so this stays below it.

### 15.6 — Recommendation

> **Superseded by §15.7–§15.8.** This recommendation predates the two
> measure-first follow-ups: the barrier-coalescing prototype was built and **held**
> (negative, §15.7), and the W-sizing lever was run on the comparison box and
> **falsified as a competitive move** (§15.8) — DbspNet is W-insensitive while
> Feldera gains uniformly from shedding E-cores, so capping W does not close the
> gap. The current bottom line is §15.8's reframe: the residual q4/q18/q19 gaps are
> **per-row execution efficiency**, looping back to the row-rep/columnar arc, not
> the exchange layer. The paragraph below is kept as the pre-experiment reasoning.

Measure-first overturned the going-in hypothesis: the exchange/scaling ceiling is
**coordination — the per-step/per-exchange barrier straggler tax at fine-grained
ticks — not wide-row movement bandwidth.** Operators already scale ~7–9×; the
realised ~3.5–5× is barrier-bound, and the one residual whole-row-hashing site
(the gather) is a small, shrinking term. There is **no clean single big lever**:
the realistic improvements are right-sizing W (1), coarser ticks (2), and — the
one worth a gated prototype — coalescing co-partitioned exchange barriers for
multi-exchange queries like q4 (3). The big structural option (async non-BSP
exchange, 4) is a research-grade rewrite whose payoff is bounded by the
slowest-worker-per-tick floor that is partly intrinsic to synchronous fine-grained
incrementality. Net: ship the profiler (durable measurement infrastructure),
adopt the W-sizing default, and treat much of the residual q4/q18/q22 Feldera gap
as a property of the execution model rather than a bug awaiting one fix. Both
boxes are hybrid (§15.3), so the E-core straggler tax is part of the *real*
comparison, not a local artifact — which makes lever 1 (cap W at the P-core
count, ≈10 on the M4 Pro) the first concrete experiment to run, since the
comparison currently puts 4 of DbspNet's 14 workers on permanent-straggler
E-cores.

### 15.7 — Prototype RAN: join exchange-barrier fusion — gated and HELD (negative result)

Built the lever-3 prototype: `ExchangeIndexJoinOp` (`Circuit/Operators/`) fuses a
join's two key exchanges into **one** shared barrier — split both inputs, publish
the pair of buckets per destination, rendezvous *once*, gather both indexed sides.
Reached from SQL via `CompileOptions.CoalesceJoinExchange` (off by default; typed
parallel path only; W=1 unchanged) through a new `CircuitBuilder.ExchangeIndexJoin`
+ reflective `InvokeExchangeIndexJoin`. Correctness proven byte-identical to the
unfused (two-barrier) form and to the single circuit across inserts / group-growth
/ both-side retractions at W=1/2/4/8 (`JoinExchangeFusion_MatchesUnfusedAndSingle`).

Gate (`dotnet run -- exchangefuse`, docs/exchange-fuse-bench.md — median step A/B
+ the exchange Wait% it targets + output cross-check; q4, 1M events):

| W | unfused step | fused step | Step↑ | Wait% (unfused→fused) |
|--:|---:|---:|---:|---:|
| 24 | 679 ms | 576 ms | **1.18×** | 61→44% |
| 16 | 550 ms | 600 ms | **0.92×** | 33→24% |
| 12 | 521 ms | 603 ms | **0.86×** | 25→30% |

**The mechanism works but the lever does not.** Fusing reliably *cuts the exchange
Wait%* (across the full q4/q9/q20/q3 set: q20 66→24%, q4 52→37%, etc.) — yet the
throughput-bounding step only improves in the **W=24 oversubscribed regime** (the
one §15.5 lever 1 says *not* to run, where 8 E-cores are active and wait is
highest) and **regresses 0.86–0.92× at the sensible operating point** (W≈12–16, the
P-core count, where q4 is actually fastest: 521 ms unfused at W=12 beats every
fused cell at every W). So unfused-at-the-right-W dominates fused-at-any-W.

**Why — it confirms §15.4.** A 15–40 pp Wait% drop producing ~0 % (or negative)
step change is direct evidence that the ceiling is the straggler *bound* — the
slowest worker's *actual work* each tick — not the barrier *count*. Fusing removes
a barrier's fixed latency but not a row of work, so the bound is unmoved; the
Wait% "drop" is largely re-accounting (one rendezvous's idle counted instead of
two). And fusing can *hurt*: it concentrates both sides' split into one pre-barrier
phase, so the single barrier waits on each worker's *combined* left+right skew,
whereas the two separate barriers let independent per-side skews partially cancel
and **resync** between them — two barriers are sometimes better than one.

**Decision: HOLD — do not adopt.** `CoalesceJoinExchange` stays off by default,
kept behind the flag as a documented, reproducible negative result + regression
guard (matching the repo's `ForceEagerRebuild`/`ForcePointProbe`/`ShareArrangements`
gated-seam discipline). The measure-first gate did its job: it stopped a
plausible-but-counterproductive "optimization" and tightened §15's conclusion:
reducing barrier *count* is not the lever (the straggler *bound* is). At the time
this left lever 1 (right-size W) as the apparent best lever — but §15.8 below then
falsified *that* too as a competitive move. The remaining levers (coarser ticks;
async non-BSP exchange) are untouched, but the bar they must clear is now sharper:
beat *unfused at W≈P-cores*, not unfused at W=host.

### 15.8 — Experiment RAN: W=10 vs W=14 on the comparison box — lever 1 FALSIFIED, and a reframe

The user ran lever 1 on the comparison M4 Pro (10 P + 4 E): DbspNet and Feldera
each at W=10 (P-cores only) vs W=14 (all cores), full Nexmark set (single run;
the cross-engine pattern below is too uniform to be noise). Head-to-head at 10
cores: q3 1.87×, q16 3.25×, q17 2.47×, q9 1.48×, q15 1.17×, q2 1.05× (wins/ties);
q20 0.89×, q1 0.87×, q0 0.86×, q22 0.68×, q19 0.55×, q18 0.46×, **q4 0.49×**
(gaps). Same-engine 14→10:

| | DbspNet 14→10 | Feldera 14→10 |
|---|---|---|
| pattern | **mixed ±7–22%** (q0/q3/q17/q22 gain, q1/q2/q9/q16 lose) | **uniform +2–38%, every query** |

Three findings, the first two unexpected:

1. **Feldera is uniformly faster at 10 than 14** — every query, +2–38%. Textbook
   BSP straggler: its per-tick barrier waits on the slowest worker, so pinning 4
   workers to E-cores taxes every step; shedding them is pure win.
2. **DbspNet is W-insensitive (10 ≈ 14, mixed ±)** — it neither suffers much from
   nor benefits much from the E-cores.
3. **So the head-to-head ratio moves TOWARD Feldera at W=10** (q4 0.66×→**0.49×**,
   q0 0.91→0.86, q22 0.70→0.68) — *not* because DbspNet got slower but because
   Feldera gained more from shedding E-cores. **Lever 1's competitive prediction is
   falsified:** capping W at the P-core count is a per-machine *absolute*-throughput
   nicety for DbspNet, NOT a way to close the Feldera gap; the 10-core comparison
   is if anything a slightly *tougher* test.

This forces two corrections on §15.4:

- **Feldera/DBSP is synchronous/clocked BSP, not async-frontier** (fixed in §15.4).
  Its *uniform* straggler-sensitivity is the BSP fingerprint; it is *more* tightly
  barrier-coupled than DbspNet, not less. The async-frontier escape is Timely/DD,
  not what we are chasing for Feldera parity.
- **DbspNet's lower straggler-sensitivity cuts both ways.** It is genuine
  resilience on heterogeneous hardware (a real point in our favour) — but partly a
  *symptom of slower per-row work*: the more efficient your per-tuple work, the
  less work sits between barriers, so the larger the straggler/coordination
  fraction. Feldera is faster per-row → more straggler-bound → hurt more by
  E-cores; we are slower per-row → less straggler-bound → tolerate them.

**The reframe (where the gap actually is).** Putting §15.1–15.8 together: our
*scaling* is coordination-bound (§15.2, confirmed) and W-sizing does not close the
competitive gap (§15.8, shown). So the residual q4/q18/q19 gaps (0.46–0.55× at
10c) are substantially **per-row execution efficiency** — Feldera processes the
same tuples cheaper (columnar/vectorised batch operators), winning *even while
paying a heavier straggler tax*. That points the lever back at **per-row/columnar
execution** (the row-representation arc, §1–§14) for those specific queries, NOT
the exchange/scaling layer. **The exchange/scaling investigation concludes here:**
movement is not the ceiling (§15.2), barrier count is not the lever (§15.7), and
W-sizing is not competitive (§15.8); the durable deliverables are the measurement
infrastructure (`stepprofile`/`exchangefuse`) and a correct, evidence-backed map
of where the gap is *not*.

---

## 16. Per-row / columnar execution efficiency — the §15.8 lever (MEASURED)

§15.8 concluded the exchange/scaling arc with a reframe: the residual competitive
gaps — **q4 (0.49×), q18 (0.46×), q19 (0.55×) at 10 cores** — are not scaling
gaps (movement isn't the ceiling §15.2, barrier count isn't the lever §15.7,
W-sizing isn't competitive §15.8). They are **per-tuple execution cost**: Feldera
processes the same tuples cheaper, winning *while paying a heavier straggler tax*.
This section measures that per-tuple cost directly — at **W=1**, because per-row
efficiency is a single-thread property that must be isolated from parallelism —
and ranks the levers. Same discipline as the prior arcs: measure first, gate, and
be honest about fundamental-vs-fixable.

### 16.1 — Method

Two W=1 measurements, both durable and benchmark-only:

- **`w1profile`** (`Benchmarks/W1ProfileBenchmark.cs`, `dotnet run -- w1profile
  [events] [batch] [runs]`, report `docs/w1-profile.md`). Runs each Nexmark query
  through a **single (non-parallel) circuit** and reports, per stream event:
  wall-clock **ns/event** (median of N runs after one warmup), managed
  **bytes/event** (`GC.GetAllocatedBytesForCurrentThread` — accurate at W=1
  because a single circuit's `Step()` runs synchronously on the calling thread, no
  worker thread allocates), and **GC collections**. The queries form a
  differential ladder (q0 = ingest/egest boundary only; q1 = +1 map; q2 = +filter;
  q20/q4/q9 = +join/+aggregate; q18/q19 = +partitioned TOP-K) so per-event deltas
  attribute to operator classes without a sampling profiler.
- **`profile {handwired,typed,structural}`** (`Benchmarks/ProfileHotPath.cs`, now
  also reporting **alloc/step**) on a steady-state single-row-insert join+GROUP BY.
  `handwired` builds the circuit directly in Core with typed `ZSet`s — **no SQL,
  no `object?[]`, no `StructuralRow` boundary** — establishing the ceiling. The
  `typed`/`structural` deltas over it isolate the SQL-engine + boundary tax.

### 16.2 — What the measurement found

**`w1profile` (1M events, batch 10k, median-of-2, Server GC, i9-12900K):**

| Query | Shape | ns/event | **B/event** | out rows |
|:--|:--|--:|--:|--:|
| q0 | passthrough (ingest+egest) | 829 | **1,263** | 9,200 |
| q1 | + 1 projection (price map) | 916 | 1,633 | 9,200 |
| q2 | + filter (selective, 74 out) | 472 | 818 | 74 |
| q22 | + 3 string SPLIT_INDEX | 1,206 | 1,751 | 9,200 |
| q3 | join a⋈p (reads ~6% of stream) | 121 | 111 | 22 |
| q20 | join b⋈a (wide output) | 1,349 | 1,725 | 1,890 |
| **q4** | join + nested MAX + outer AVG | **2,430** | **2,812** | 10 |
| q9 | join + partitioned TOP-1 | 1,704 | 2,883 | 1,430 |
| **q18** | partitioned TOP-1 dedup | **2,620** | **3,530** | 9,200 |
| **q19** | partitioned TOP-10 | **3,592** | **5,130** | 8,706 |

**`profile` steady-state (1 row/step, join+GROUP BY):**

| Mode | µs/step | **B/step** | vs handwired |
|:--|--:|--:|--:|
| handwired (pure Core typed ZSet) | 0.92 | **2,843** | 1.0× |
| typed (SQL typed path) | 3.14 | 4,949 | 3.4× time, +2,106 B |
| structural (SQL `object?[]` path) | 4.12 | 5,467 | 4.5× time, +2,624 B |

Three facts, all decisive:

1. **Per-tuple time tracks allocation almost exactly.** Across the ladder, ns/event
   and B/event move together (q19 worst on both, q3 cheapest on both). The engine
   is **allocation-bound** per tuple, not dispatch-bound or compute-bound — which
   is why §6.3's codegen demotion still holds (codegen attacks dispatch, not the
   binding cost).
2. **The three competitive-gap queries are exactly the three heaviest allocators.**
   q4/q18/q19 — the 0.46–0.55× Feldera gaps — allocate **2.8 / 3.5 / 5.1 KB per
   event** and are the slowest. The correlation between the §15.8 gap list and the
   allocation ranking is not a coincidence: **allocation is the per-tuple gap.**
3. **There is a large generic-engine floor present in *everything*.** Even
   passthrough (q0) allocates **1,263 B/event**, and even the hand-wired Core
   ceiling allocates **2,843 B/step**. This floor is structural, not query-specific.

### 16.3 — Attribution: where the per-tuple allocation goes (code-grounded)

The two measurements bracket the cost into two layers:

- **Layer A — the Core dictionary-Z-set churn (~2,843 B/step, the floor).** Every
  stateful operator's `Step()` allocates a **fresh `ZSetBuilder` (a new
  `Dictionary`)**, fills it, and `Build()`s it into the output Z-set — verified
  universal: the `new ZSetBuilder … _output.SetCurrent(builder.Build())` pattern
  appears across **all 25** stateful-operator files
  (`IncrementalAggregateOp.cs:121,147`, `TopKOp.cs:101,119`, join ops, etc.), plus
  per-key `GroupFor` materialisations inside the loop. This is the analogue of
  Feldera's per-operator batch — except Feldera builds **one reused sorted columnar
  buffer** and we build **a fresh hash `Dictionary` every tick**. Present even in
  the hand-wired path, so it is **architectural**, not a SQL-compiler artifact.
  Critically, **`Build()` transfers the dictionary's ownership into the output
  `ZSet`** (`ZSetBuilder.cs:72-82` nulls `_entries`), which the output stream holds
  until the next tick and which `z⁻¹`/trace consumers may retain — so the dict
  cannot be trivially pooled (see §16.5 lever 1).
- **Layer B — the SQL boundary tax (+2,106 B/step typed, +2,624 structural).** The
  `object?[]`→typed-struct **input lift** at the scan and the typed→`StructuralRow`
  **output materialisation** at the sink, plus per-projection delegate result rows.
  q4/q18/q19 run the **typed** path at W=1, so they pay the +2,106 (not the
  structural +2,624). The `w1profile` ladder localises this: q2 (74 output rows)
  allocates 818 B/event while q0 (9,200 output rows) allocates 1,263 — the ~445 B
  delta on near-identical input work is **output materialisation** (building a
  `StructuralRow` per result row). q18/q19, with ~9,200 wide output rows per batch
  *plus* retained wide rows in the TOP-K `SortedDictionary`, are the worst hit.

So roughly **~55–60% of per-tuple allocation is the architectural Layer A floor**
(dictionary-Z-set churn, hits *every* operator including q4's internal pipeline),
and **~40–45% is the Layer B boundary** (`object?[]` in / `StructuralRow` out,
concentrated in output-heavy q18/q19, near-zero for tiny-output q4).

### 16.4 — Fundamental vs fixable (honest)

- **Layer B is bounded and partly fixable.** Typed ingest (source emits typed rows,
  no `object?[]` at the scan) and deferring `StructuralRow` materialisation to the
  true sink would remove much of the +2,106. **But** it helps **q18/q19**
  (output-heavy) far more than **q4** (10 output rows, ~entirely Layer-A-bound), and
  it cannot touch the floor. A real but partial, query-skewed win.
- **Layer A is substantially architectural.** Matching Feldera's near-zero
  steady-state per-tuple allocation means not building a fresh `Dictionary` per
  operator per tick. Two ways: **(1) reuse the delta buffers** (bounded, but fights
  the `Build()` ownership transfer and `z⁻¹`/trace retention — correctness-risky),
  or **(2) columnar/vectorised batch operators** (an `OrdZSet`-style reused sorted
  columnar buffer + cursor-merge operators — the Feldera model, a near-rewrite).
  This is the honest core of the §15.8 framing: **the gap is in large part the cost
  of a generic `object?[]`/hash-`Dictionary` managed engine versus Feldera's
  monomorphised columnar Rust.** The spine arc (§5–§13) already tried sorted-merge
  on our tick granularity and it lost to the flat dict — so "columnar" here means
  *buffer reuse + vectorised per-column work*, not *sorted-merge execution*, which
  we already measured is the wrong model for fine-grained ticks.

### 16.5 — Ranked levers

ROI against the measured bottleneck (per-tuple allocation), discounted by
effort/risk.

| # | Lever | Hits | Effort | Risk | Notes |
|--|--|--|--|--|--|
| 1 | **Delta Z-set buffer/builder pooling** | Layer A (all ops incl. q4) | M | **H** | The only bounded lever that touches q4's internal-bound cost. Fights `Build()` ownership + `z⁻¹`/trace retention — needs a microbench to bound the reclaimable fraction *and* prove the retention constraint before any wiring. |
| 2 | **Typed ingest + deferred output materialisation** | Layer B | S–M | L | Removes much of +2,106 B; helps q18/q19 (output-heavy) and every structural-fallback query; ~no help for q4. Low-risk partial win. |
| 3 | **Columnar/vectorised batch operators** (`OrdZSet` analogue, reused buffers) | Layer A floor | XL | H | The real Feldera-parity move and the end-state if 1/2 fall short. Near-rewrite; defer until the bounded levers are measured. |
| 4 | **Whole-query codegen / delegate fusion** | dispatch | XL | H | **Demoted again** (§6.3): time tracks *allocation*, not dispatch, so codegen without allocation reduction won't move the needle. |

### 16.6 — Recommendation & first increment

The measurement confirms the going-in framing and sharpens it: the per-tuple gap
is **allocation**, split between a fixable boundary (Layer B) and an architectural
floor (Layer A), and **q4 lives almost entirely in the floor** while q18/q19 are
split. There is no single bounded lever that closes all three.

Sequenced, measure-first:

1. **Microbench-precursor for lever 1 (next).** Before wiring any operator, build a
   focused Core microbench that (a) quantifies how much of the ~2,843 B/step floor
   is the *reclaimable* delta-`Dictionary` backing vs genuinely-live retained state,
   and (b) tests whether the `Build()`-ownership + `z⁻¹`/trace-retention constraint
   actually permits reuse. This mirrors how `surrogatebench` (§14.9) retired an XL
   option *before* operator work. If the reclaimable fraction is small or retention
   blocks reuse, lever 1 is dead and the answer is lever 3.
2. **Lever 2 in parallel (low-risk).** Typed ingest + deferred output for the
   output-heavy queries (q18/q19), gated on `w1profile` ns/event + B/event with the
   existing per-tick output cross-check. Independent of lever 1.
3. **Lever 3 (columnar) stays the explicit end-state**, deferred until 1/2 are
   measured — not assumed, because the spine arc already showed one "obvious"
   representation change (sorted-merge) lose on our tick granularity.

Durable deliverables this session: the `w1profile` harness, the `profile`
allocation ceiling, and an evidence-backed map showing the per-tuple gap **is**
allocation (Layer A floor + Layer B boundary), with q4 floor-bound and q18/q19
split — so the next experiment is the lever-1 reclaimability microbench, not a
speculative columnar rewrite.

### 16.7 — Lever-1 reclaimability microbench RAN (prize real, but it's Layer-A-only)

The §16.6 gate — bound the pooling prize and test the retention constraint before
wiring — is built (`Benchmarks/PoolBenchmark.cs`, `dotnet run -- poolbench`,
report [pool-bench.md](pool-bench.md)) and run. It A/Bs the delta dictionary's
lifecycle — **fresh** (`new Dictionary()`, today's bare `ZSetBuilder()`),
**presized** (`new Dictionary(D)`, what `ZSetBuilder.From`'s capacity hint buys),
**pooled** (reuse via `Clear()`) — across delta sizes `D` spanning q4/q18/q19's
operating points, measuring managed bytes per built dictionary.

**Result (bytes/dict/tick, median):**

| D | fresh (long→long) | presized | pooled | fresh (pair→long) |
|--:|--:|--:|--:|--:|
| 1 | 216 | 216 | **0** | 240 |
| 256 | 22,312 | 8,336 | **0** | 28,560 |
| 1,024 | 102,216 | 31,016 | **0** | 131,264 |
| 4,096 | 451,344 | 136,240 | **0** | 580,136 |
| 9,216 | 941,928 | 283,016 | **0** | 1,210,872 |

Three findings:

1. **Pooling reclaims 100% of the dict backing.** `Clear()` keeps the bucket/entry
   arrays, so a stable-size delta re-fills with **zero** allocation at every `D`.
   The prize is real and scales with the delta: ~283 B/entry-cleared… no — ~31 B
   per entry of *steady* backing (presized), but the **fresh** path pays **~3.3×
   that** because growing a dictionary from capacity 0 reallocates and copies its
   backing ~11 times over 9,216 inserts (resize churn).
2. **A strictly-safe sub-lever falls out: pre-sizing.** `fresh − presized` is ~70%
   of `fresh` at large `D` — pure resize churn — and pre-sizing the builder
   reclaims it with **zero retention risk** (no cross-tick reuse; the dict is still
   handed off and dropped). The bare `ZSetBuilder()` ctor (used by the stateful
   ops, `IncrementalAggregateOp.cs:121`, `TopKOp.cs:101`) takes no capacity hint;
   `ZSetBuilder.From` already does. Sizing the builder to the input-delta count
   (an exact upper bound for linear ops, a good estimate for stateful ops) is a
   safe, mechanical partial win for the large-batch operators.
3. **The retention constraint is satisfiable per-edge** (verified from the Core
   lifecycle, recorded in the report): `ZSet` takes dict ownership; trace
   `Integrate` folds the delta into its *own* dict (`MergeInPlace`) and does not
   retain it; a `Stream.Current` holds an output only until the next `SetCurrent`;
   the **only** cross-tick aliasing of a delta `ZSet` is `DelayOp`
   (`_nextOutput = _input.Current`, `DelayOp.cs:34`). q4/q18/q19's flat pipelines
   put no `z⁻¹` on a delta edge (their delays are trace-internal — the join's
   `L_{t-1}` is `_leftTrace.Current`, a trace-owned dict), so their delta edges are
   dead-after-tick and poolable. Recursive/nested circuits have explicit `z⁻¹` on
   deltas and must be excluded — a per-edge analysis the compiler can do.

**The honest limit — pooling is Layer-A-only.** poolbench measures the dict
**backing arrays**. The per-row **objects** the dict references — the
`StructuralRow` + `object?[]` + boxed scalars at the output boundary (Layer B) —
are *separate* heap allocations that pooling the dictionary does **not** reclaim.
Cross-referencing §16.2: the `handwired` ceiling (typed value-type rows stored
*inline* in the dict's `Entry[]`, no boundary) is ~all reclaimable backing, but the
SQL path's +2,106 B (typed) / +2,624 B (structural) is boundary objects pooling
cannot touch. So:

- **q4** (10 output rows, Layer-A-dominated, value-type internal rows): the **best
  case for lever 1** — most of its per-tuple allocation is poolable dict backing.
- **q18/q19** (≈9,200 wide output rows/tick): **pooling helps the internal/TOP-K
  state dicts but not the output boundary objects**, which dominate their per-event
  number — they need **lever 2** (deferred `StructuralRow` materialisation).

**Decision.** Lever 1 is **alive** (prize real, constraint satisfiable) but
**Layer-A-scoped** — it is the right lever for q4 and a partial one for q18/q19,
and it pairs with lever 2 (boundary objects) rather than replacing it. The
microbench did its gating job: it confirmed the prize without speculative wiring
and surfaced the **pre-sizing sub-lever** as the zero-risk first step. Recommended
sequencing for the wiring increment (next): (a) pre-size the bare `ZSetBuilder()`
to the input-delta count where estimable — safe, mechanical, captures the ~70%
resize-churn term immediately, gate on `w1profile` B/event; then (b) full
cross-tick delta pooling behind a per-edge "no-z⁻¹" guard for the dead-after-tick
edges, gate on q4 W=1 ns/event + B/event with the per-tick output cross-check;
(c) lever 2 (deferred output) in parallel for q18/q19. Columnar (lever 3) stays
the deferred end-state.

### 16.8 — Adaptive delta-builder pre-sizing LANDED (lever-1 step (a), gated)

The §16.7 (a) sub-lever — pre-size the delta builder, zero retention risk — is
wired and gated. The refinement over §16.7's wording: size to the **previous
tick's output count**, not the input count. Last-output sizing self-tunes to the
true output (a 1:1 projection sizes to its input; a selective filter or
few-groups aggregate to its small result), so it kills the resize churn **without**
the over-allocation that input-count sizing would pay on selective operators.

**What landed.** A `ZSetBuilder(int capacity)` ctor (`ZSetBuilder.cs`) and
last-output sizing at the hot delta-builder sites: the fused linear
`MapFilterRows`/`FlatMapRows` (closure-captured `lastSize`,
`LinearOperators.cs`), `ZSet.MapKeys` (sized to the input count — a tight upper
bound for a projection), and the three stateful operators that dominate the gap
queries — `IncrementalJoinOp`, `IncrementalAggregateOp`, `PartitionedTopKOp`
(instance `_lastOutputSize` field). **Correctness-neutral by construction**: a
`Dictionary`'s initial capacity is a pure allocation hint, so the built Z-set is
byte-identical. Fresh allocation each tick (no cross-tick reuse), so there is no
`z⁻¹`/retention hazard — this is the strictly-safe half of lever 1; true pooling
(reclaiming the *steady* backing, not just the churn) remains the gated follow-up.

**Verification.** Full suite **1,747 passed**, 0 failed (the capacity hint changes
no behaviour).

**Gate (`w1profile`, 1M events, batch 10k, before = commit `d7c38af`).** Managed
allocation falls 16–35% across every query, and per-event time falls on the gap
queries:

| Query | B/event before→after | ns/event before→after |
|:--|--:|--:|
| q4 | 2,812 → **2,376** (−16%) | 2,430 → **2,197** (−10%) |
| q9 | 2,883 → **2,382** (−17%) | 1,704 → **1,398** (−18%) |
| q18 | 3,530 → **2,417** (−32%) | 2,620 → **2,249** (−14%) |
| q19 | 5,130 → **4,059** (−21%) | 3,592 → **2,945** (−18%) |
| q0 | 1,263 → 962 (−24%) | 829 → 803 |
| q1 | 1,633 → 1,078 (−34%) | 916 → 810 |

The win is **larger than §16.7 predicted** (it framed pre-sizing as a ~70%-of-
backing churn reclaim, expecting a single-digit query-level effect). At the batch-
10k operating point the resize churn is a bigger share than expected — especially
q18/q19, whose wide bid rows are value-type structs stored *inline* in the
dictionary `Entry[]`, so the churned backing is large. The allocation drop tracks
into a real per-event time drop (−10% to −18%) on all four competitive-gap
queries, confirming §16.2's allocation-bound finding *and* that q4's earlier
single noisy `ns` reading (an apparent regression) was measurement noise — the
clean median-of-4 shows q4 improved.

**Status.** Lever-1 step (a) shipped: a safe, correctness-neutral, 16–32%
per-event allocation reduction on the gap queries with a 10–18% time win. Next
increments (unchanged from §16.7): (b) true cross-tick delta pooling behind a
per-edge "no-`z⁻¹`" guard to reclaim the *steady* backing the fresh-alloc path
still pays; (c) lever 2 (deferred `StructuralRow` output) for the boundary objects
that dominate q18/q19; (d) columnar (lever 3) deferred end-state.

### 16.9 — Lazy-boxing output boundary LANDED (lever 2, gated)

After (a) shipped, the post-(a) re-attribution (from the `w1profile` differential)
showed the largest *universal* remaining term is the **typed→`StructuralRow`
output boundary**: `AdaptTypedToStructural` mapped each output row to
`new object?[] { (object)r.F0, … }` → `StructuralRow` **every tick** — an array +
N field-boxes + a wrapper per output row. This wires the lazy-boxing fix.

**What landed.** `TypedStructuralRow<TRow>` + `StructuralRowShape<TRow>`
(`Core/Collections/TypedStructuralRow.cs`): a `StructuralRow` subclass that holds
the emitted typed row struct **inline** and boxes columns **lazily** (only when the
indexer is read). The typed output boundary
(`TypedPlanCompiler.BuildTypedToStructuralDelegate`) now, on the default codec,
constructs one of these per output row instead of the eager `object?[]`. The shared
`StructuralRowShape` carries a **typed hash** delegate that reproduces
`StructuralRow.ComputeHash` field-by-field with **no boxing** (valid because
`HashCode.Add(typedField)` and `HashCode.Add((object)boxed)` feed the identical
per-element hash, null → 0), plus a per-column boxing accessor. Per output row this
allocates one wrapper (struct inline) instead of array + N boxes + wrapper, and
defers all boxing to actual column reads.

**Correctness-equivalent by construction** — the wrapped row is indistinguishable
from a backing-array `StructuralRow` (same `Count`, indexer values, hash, and
inherited `Equals`), so output Z-set dedup and cross-type lookups are unchanged.
The eager `object?[]` path is kept as the fallback for any non-default codec (which
gates the typed path off anyway). **Full suite 1,747 passed** — the gate, since the
SQL output-correctness tests compare materialised `ZSet<StructuralRow>` (a hash
mismatch would fail them).

**Gate (`w1profile`, 1M events, batch 10k, before = commit `ed21c68` = post-(a)):**

| Query | B/event (a)→(2) | ns/event (a)→(2) |
|:--|--:|--:|
| q0 | 962 → **719** (−25%) | 803 → **~490** (−39%) |
| q1 | 1,078 → 835 (−23%) | 810 → 563 (−30%) |
| q22 | 1,139 → 948 (−17%) | 1,144 → 789 (−31%) |
| q18 | 2,417 → **2,173** (−10%) | 2,249 → **~1,810** (−20%) |
| q19 | 4,059 → 3,831 (−6%) | 2,945 → ~2,700 (−8%) |
| q4 | 2,376 → 2,376 (**0%**) | 2,197 → 2,133 (~noise) |

Exactly the predicted shape: large wins on output-heavy queries (q0/q1/q22 and the
gap query **q18 −10% alloc / −20% time**), modest on q19 (its cost is more TOP-10
retained state than output boundary), and **nothing for q4** — confirming q4 is
boundary-light (10 output rows) and floor-bound, needing lever (b)/columnar, not the
boundary. **Honest caveat:** `w1profile`'s consumer (`CountOutput`) does not read
output columns, so the *time* win is partly deferred boxing work that a
column-reading consumer would still pay lazily on read; the **allocation** reduction
(the eliminated array + eager-box construction) is real for every consumer, and a
consumer that reads only some columns saves the boxing of the rest outright.

**Status.** Lever 2 shipped: a correctness-equivalent output-boundary allocation
cut (6–25%) and time cut (8–39%), concentrated on output-heavy queries including
the gap query q18. Combined with (a), the per-tuple allocation on q18 is down ~38%
from the start of this arc (3,530 → 2,173 B/event). q4 remains the holdout — its
remainder is join/aggregate internals (the IndexedZSet trace structures), addressed
only by lever (b)'s cross-tick pooling or lever 3 (columnar). Remaining sequencing:
(b) is thin-prize/high-risk (§16.7 re-attribution); the larger structural step for
q4 is the columnar end-state (lever 3), which deserves its own arc.

### 16.10 — Same-box A/B regression check + the W=1-vs-W>1 architecture correction

The M4 Pro comparison run (W=1/10/14, both engines) after (a)+lever 2 showed the
**W=1** per-row wins land on the real box (10c W=1: q0 +47%, q18 +46%, q19 +14%,
q4 +3% vs the pre-arc snapshot) but the **competitive W>1 ratios** barely moved
(q4 0.49→0.55, q19 0.55→0.62, q18 0.46→0.39 at 10c — the q18 dip looked like a
regression). A clean same-box A/B (HEAD `140dff9` vs pre-arc `215fac0`, this
i9-12900K, W=8, 1M events, 3 runs) settles both questions:

| | W=1 pre→HEAD | W=8 pre→HEAD | speedup pre→HEAD |
|--|--|--|--|
| q0 | 1.45M→2.05M (+42%) | 4.02M→5.13M (+28%) | 2.78→2.50 |
| q4 | 446k→463k (+4%) | 1.10M→1.25M (**+14%**) | 2.47→2.70 |
| q18 | 391k→549k (+40%) | 1.01M→1.07M (**+6%**) | 2.58→1.96 |
| q19 | 303k→375k (+24%) | 1.08M→1.05M (−2%) | 3.56→2.80 |
| q20 | 883k→1.01M (+15%) | 2.13M→2.16M (+1%) | 2.41→2.13 |

**(1) No parallel regression.** Every query is ≥ pre-arc at W=8 (q18 *+6%*,
q19 −2% within noise). The M4 Pro 10c q18 dip was single-run/cross-session noise,
not our changes.

**(2) Lever 2 helps only the single-circuit path, not W>1 throughput — a
load-bearing architecture fact.** The typed→`StructuralRow` output conversion is
an **in-circuit operator** on the single circuit (`AdaptTypedToStructural`'s
`MapRows`), so it runs **every `Step()`** and is in the W=1 timed loop — lever 2
cut it (q18 W=1 +40%). But the **parallel** path decodes output **lazily on
`q.Current` read**, which the throughput benchmark does **after** `sw.Stop()`
(`MaterializeParallel`), so the parallel hot loop never eagerly materialises output
— there is **nothing for lever 2 to remove at W>1**, and the comparison's W>1
numbers don't even time output decode. *Conclusion: do not "extend lever 2 to the
parallel output path" — it is moot.* Lever 2 is a real single-thread/latency win
(and the "DbspNet W=1" column), not a W>1 throughput lever.

**(3) The W>1 competitive lever is in-`Step` work only.** W>1 `Step` = exchange
coordination + per-replica operator compute. The only per-row levers that move it
are inside `Step`: (a)'s builder pre-sizing did (q4 W=8 +14%, an in-`Step` join/agg
win), whereas q18's headline gain was the output boundary, out-of-`Step` at W>1
(hence only +6% W=8 despite +40% W=1).

**(4) Amdahl dilution, measured.** Per-row wins shrink the parallel speedup
(q18 2.58→1.96×, q19 3.56→2.80×): cheaper per-tuple work makes the fixed
coordination a bigger fraction (the §15.8 effect, directly observed). So per-row
efficiency improves absolute single-thread throughput cleanly but is **partially
eaten at W>1** by coordination — and q18/q19 are partly coordination-bound (§15),
so their competitive ceiling is not purely a per-row-cost problem.

**Net for the roadmap.** (a)+lever 2 are the bounded per-row wins; they lift W=1
strongly and W>1 modestly (q4 +14% at W=8 is the best W>1 gain, an in-`Step`
result). Closing the *competitive* W>1 gaps further means in-`Step` per-row cost —
lever (b) pooling the operator internals (thin ~5% prize, high risk, §16.7) — or
the **columnar end-state (lever 3)**, the real structural step, plus the
substantially-fundamental coordination ceiling (§15). There is no remaining cheap,
safe, high-W>1-ROI lever; the honest next move is either the columnar arc or to
consolidate the bounded wins here.

### 16.11 — Single-core comparison resolves the confound: per-row is the whole gap, coordination is a strength

A 1-thread-vs-1-thread run of both engines (M4 Pro) settles the §16.10 confound
decisively. **DbspNet trails Feldera single-threaded on 11 of 13 queries, often
2–5×:** q4 **0.21×**, q15 0.32×, q18 0.33×, q19 0.35×, q22 0.41×, q0 0.49×,
q2 0.59×, q20 0.67×, q1 0.75×, q17 0.84×, q16 0.86×. The only single-core wins are
**q3 (2.83×)** — a real, persistent algorithmic/data-structure edge — and q9
(~parity, 1.07×).

So the multi-core competitiveness is **not per-core speed — it is scaling.**
DbspNet scales positively on 12/13 queries; **Feldera goes *negative* on q4/q15/
q16/q17** (slower at 14c than 1c — on q15 it loses 4.5×), because its exchange/merge
overhead swamps the tiny per-row work on low-cardinality aggregations. That, not raw
speed, is why DbspNet "wins" q15/q16/q17 at 14c (it loses all three single-threaded).
Even the ingest/egest-bound passthrough q0 — which §16.10 floated as a possible
serial-boundary weakness — scales **better** for us (1c→14c 2.35×) than for Feldera
(**1.19×**). So the §16.10 serial-boundary worry is also dispelled: that boundary is
shared and we handle it at least as well.

**Conclusion — the coordination question is answered, and inverted:** our
coordination/scaling is a **strength**, not a leak. We out-scale Feldera and never
de-parallelize. The entire competitive gap on the laggards (q4/q18/q19/q22/q0/q2) is
**per-row** — the managed-runtime + `object?[]` boundary + per-tick allocation tax,
worst on aggregate/join/string-heavy queries. This retires coordination (§15) as a
*competitive* target (it is the shared BSP ceiling and we sit below Feldera on it)
and vindicates the §16 per-row direction unambiguously.

**Refinement of the §16.10 dilution caveat (it strengthens the columnar case).** The
q18 W=1→W=8 collapse (+40%→+6%) was specifically **lever 2 being out-of-`Step` at
W>1**, not a general law that per-row wins vanish in parallel. **In-`Step` per-row
wins translate to W>1 and can *amplify*:** (a)'s in-`Step` join/agg pre-size went
+4% at W=1 → **+14% at W=8** on q4 (its parallel speedup *rose*, 2.47→2.70×). The
columnar rewrite attacks in-`Step` operator internals, so it would lift **both**
single-core (huge, 2–5× headroom) **and** multi-core (well, amplified on the
join/agg-heavy laggards). **Highest-value columnar target: the aggregate/join inner
representation** (the `IndexedZSet` trace), where q4 (0.21×) and q15–q19 bleed.
Worth studying our own q3 win (2.83×) to learn what to *preserve* in that design.

---

## 17. Data representation & per-tuple execution — apportioning the single-core gap (MEASURED, design-only)

> Status: **design note, no engine code.** This is the deliverable of a
> measure-first design session opening the next arc the §16.11 single-core proof
> pointed at. §16 established (by *correlation* — ns/event tracks B/event across
> the query ladder) that the per-tuple gap is allocation-bound. The §16.11
> single-core run then established *that the per-tuple gap is the whole competitive
> gap* (we trail Feldera 1-thread on 11/13, 2–5×; our multi-core wins are scaling,
> not speed). This section does the next thing the prompt demands: **apportion that
> per-tuple cost between the representation axis and the execution axis** with a
> direct decomposition, confront the spine lesson, rank the representation ×
> execution design space, and scope the smallest benchmark-gated first increment —
> *without* assuming the answer is "all heap" or "go columnar."

### 17.1 — Method: a third probe that apportions, not just correlates

The two existing probes bracket the cost but neither *apportions within the floor*:

- **`w1profile`** (refreshed this session, 1M events / batch 10k / median-3, this
  i9-12900K, post-§16.9 HEAD) reconfirms the current per-event state: q0 **520 ns /
  719 B**, q4 **2,142 ns / 2,376 B**, q18 **1,966 ns / 2,175 B**, q19 **2,754 ns /
  3,831 B**, and the cheap end q3 **81 ns / 89 B**. ns and bytes still move together.
- **`profile {handwired,typed,structural}`** (refreshed) reconfirms the boundary
  split: handwired **0.86 µs / 2,782 B** (Layer-A floor, no SQL boundary), typed
  **2.98 µs / 4,688 B** (+2.12 µs / +1,906 B of Layer-B boundary), structural
  **3.93 µs / 5,420 B**. So the *boundary* (Layer B) is ~71 % of typed time — but it
  is `object?[]`-in / `StructuralRow`-out, already partly addressed by levers (a)+2.

Neither tells us, **inside the Layer-A floor that q4 lives in**, how per-tuple time
splits between *representation* (allocating a fresh dictionary + hashing a whole wide
row) and *execution* (delegate dispatch + the generic `ZSet`/`IZRing` abstraction).
That is the open question §1 of this arc poses, and codegen vs representation hinges
on it. So this session built a third probe.

**`reprbench`** (`Benchmarks/ReprDecompBenchmark.cs`, `dotnet run -- reprbench
[ticks] [delta] [runs]`, report [repr-decomp-bench.md](repr-decomp-bench.md)) times
the **universal per-tick Layer-A hot loop** — build a delta dictionary of `D` rows
keyed by a wide value-type row, fold it into retained state, enumerate the result —
**four ways**, at three key widths (2 longs / 8 longs / Nexmark-bid-like 3 longs + a
string, the rows pre-generated outside the timed loop so the floor is a clean
hash+probe, not a row-materialisation artefact):

1. `gen·fresh` — today's model: a fresh `ZSetBuilder` (`new Dictionary`) per tick,
   generic `ZSet`/`Z64` ops, a `Func<>` transform per row.
2. `gen·pooled` — same generic zero-suppressing `Z64` add, dictionary **pooled**
   (`Clear()`+reuse). `fresh − pooled` = **the allocation tax**.
3. `mono·deleg` — raw `Dictionary<Row,long>`, pooled, no `Z64`/`ZSet` wrapper, but
   the transform still a `Func<>`. `pooled − mono·deleg` = **the abstraction tax**.
4. `mono·inline` — raw pooled dict, transform inlined, no delegate. `mono·deleg −
   mono·inline` = **the dispatch tax**; `mono·inline` itself = **the irreducible
   compute floor** (wide value-type key hash + probe) a monomorphised engine still pays.

### 17.2 — What the measurement found: the apportionment

**`reprbench` (D=256, the q4/q18/q19 mid operating point, median-of-11, ns/row):**

| Width | gen·fresh | gen·pooled | mono·inline (floor) | alloc tax | exec tax (pooled−floor) |
|:--|--:|--:|--:|--:|--:|
| W2 (2 long) | 50.5 | 19.7 | 16.9 | **30.8 (61 %)** | ~2.8 (6 %) |
| W8 (8 long, bid-like) | 116.0 | 57.5 | 47.0 | **58.5 (50 %)** | ~10.5 (9 %) |
| WStr (3 long + string) | 168.3 | 84.5 | 80.1 | **83.7 (50 %)** | ~4.4 (3 %) |

(Swept also at D=16 and D=1024; the D=1024/W8 `fresh` blows up to 345 ns — almost all
allocation, 268 ns — because growing a per-tick dict from empty reallocates its
backing ~11× over the delta, the §16.7 resize-churn term. The qualitative ordering is
identical at every D.)

Three findings, all decisive and all *new* (apportionment, not correlation):

1. **Allocation is the single largest controllable term — ~50–61 % of per-tuple
   floor time, and the part that explodes with width and large deltas.** This is the
   fresh-`Dictionary`-per-tick cost (§16.3 Layer A). Pooling (`Clear()`+reuse) removes
   **100 %** of it (0 B/row, confirming `poolbench` §16.7). The allocation tax is the
   prize lever-1 targets.
2. **The irreducible whole-row-hash floor is the second structural term — ~33 % (W2)
   → 41 % (W8) → 48 % (WStr) — and it scales with row width.** This is `mono·inline`:
   a raw value-type-keyed dict with the wide row stored **inline** in the `Entry[]`
   (exactly what the typed path's emitted-struct dict does), pooled, no dispatch. It
   is what remains after you remove allocation *and* dispatch. **Nothing but a
   representation change touches it** — you cannot make hashing 8 longs + a string
   cheaper without either hashing *fewer* bytes (a narrow/extracted key) or hashing
   *per column* (columnar/SoA). For an aggregate inner multiset keyed by the whole row
   (q4's MIN/MAX, which needs the full row retained), the key *is* the wide row, so
   this floor is **largely irreducible short of columnar** — the sobering core fact.
3. **The execution axis is a distant third, and half of it is free.** The combined
   dispatch + generic-abstraction tax is only **~3–10 %** of the floor. Within it, the
   **abstraction tax is consistently ~0** (−3 to −14 ns, i.e. noise around zero):
   **.NET monomorphises value-type generics, so the `ZSet`/`Z64`/`IZRing` wrapper is
   already free** — "monomorphising the Z-set abstraction" buys *nothing*. Only the
   per-row `Func<>` **delegate dispatch** is real (~10–18 ns on numeric rows), and it
   shrinks relative to the floor as rows widen.

**The apportionment answer to §1 is therefore: it is NOT all the heap, but the two
real levers are both on the representation axis.** Of the addressable per-tuple floor,
roughly **half is fresh-dict allocation** (fixable by pooling/arena, Layer-A,
dead-after-tick edges only) and **~40–48 % is whole-row hashing** (fixable only by
changing the representation — narrower keys or columnar per-column work), while
**execution/dispatch is ~3–10 % and codegen of the abstraction is worthless because
the abstraction is already monomorphic.** This **re-confirms and sharpens the §6.3 /
§16.5 codegen demotion with a direct measurement**, not just the "time tracks
allocation" correlation: whole-query codegen / monomorphization attacks the one axis
that is already cheap. Re-litigated honestly as §1 asked — and it loses again, now on
apportioned evidence.

### 17.3 — Confronting the spine lesson head-on (the central question)

§2 of the prompt is the pivotal one: we *built* a sorted-columnar LSM substrate
(`Spine*`, §5–§13) and it **lost** to the flat dictionary on fine ticks (§8.3:
1.4–2.5×), yet Feldera uses sorted-columnar `OrdZSet` and wins single-core. The
apportionment resolves the apparent paradox cleanly:

- **Sorted-merge attacked term 2 (the whole-row hash) by replacing it with key
  *compare*** — galloping/merge over `Ord` keys, never hashing the whole row. That is
  a real attack on the ~40 % floor.
- **But it *worsened* term 1 (allocation):** on our fine-grained ticks it rebuilds
  many tiny **sorted-columnar batches per tick** (sort + bloom + compaction), *more*
  per-tick allocation/work than the flat dict's in-place update. The §10 memtable only
  clawed it back to parity by **re-introducing an in-memory mutable dictionary** in
  front of the batches. So sorted-merge **traded a smaller term-2 for a larger term-1**
  — and term-1 is the *bigger* half (50–61 %). That is exactly why it lost, and the
  decomposition now *predicts* it would.

**The lesson for any columnar direction: the win must attack term 1 AND term 2
together, never trade one for the other.** Feldera does this because its columnar
buffers are **reused/arena-backed** (term 1 ≈ 0 steady-state) *and* **per-column /
sorted** (term 2 cheap) — both at once. Sorted *storage* on its own (what `Spine*`
is) only gets term 2 and pays more term 1 at our granularity. So:

> **The lever is not columnar *storage* (tried, lost) and not sorted-merge
> *execution* (tried, lost on fine ticks). It is (i) getting the flat-hash model off
> the per-tick managed heap — pooled/arena, value-type, inline — to kill term 1
> without touching the execution model that *won*, and, where the key genuinely is a
> wide row that must be retained (the aggregate inner multiset), (ii) a *columnar
> per-column* representation of that inner state to attack term 2 — with reused
> buffers so it does not re-incur term 1 the way the spine did.**

This makes the prompt's "unboxed pooled flat-hash" a first-class candidate (it is the
clean term-1 win on the model that already beat sorted-merge), and scopes columnar
narrowly to the *one* place term 2 is both large and irreducible: the
`IndexedZSet` **inner value multiset** of the aggregate/join, where q4 (0.21×) and
q15–q19 bleed — not as a whole-engine storage rewrite.

### 17.4 — The representation × execution design space, ranked

ROI against the apportioned bottleneck (term 1 allocation ≈ 50–60 %, term 2 whole-row
hash ≈ 40–48 %, execution ≈ 3–10 %), discounted by effort/risk. Effort S/M/L/XL.

| # | Candidate | Axis / term | ROI | Effort | Risk | Verdict |
|---|---|---|---|---|---|---|
| 1 | **Cross-tick delta/builder pooling** (reuse the `Dictionary` backing on dead-after-tick edges) | repr / term 1 | **High** (the 50–60 % term) | M | **H** (`Build()` ownership + `z⁻¹`/trace retention) | **Lead bounded lever.** §16.7 proved the prize real & the per-edge "no-`z⁻¹`" retention constraint satisfiable; (a) presizing already shipped the *churn* half. This is the *steady-backing* half. |
| 2 | **Unboxed pooled flat-hash trace** (open-addressing over pooled `T[]`, value-type keys/weights inline, reused across ticks) | repr / terms 1+2 | **High** | L | M | The prompt's leading candidate. Generalises pooling into the *retained* state, keeps the flat-hash execution that beat sorted-merge, kills per-entry object indirection. The end-state for the *non-aggregate* stateful ops. |
| 3 | **Columnar per-column inner multiset** for the aggregate/join `IndexedZSet` (SoA `Span<T>`-per-column, reused buffers, per-column hash/scan) | repr / term 2 | **High but narrow** (q4/q15–q19 only) | XL | H | The *only* attack on the irreducible term-2 floor, scoped to where it is large. Must reuse buffers (or it repeats the spine's term-1 mistake). The real Feldera-parity move for aggregates. |
| 4 | **Typed ingest + deferred output materialisation** | repr / Layer B | Medium (q18/q19, every structural query) | S–M | L | §16.9 lever 2 did the output half; typed *ingest* (source emits typed rows, no `object?[]` at the scan) is the remaining half. Cheap, low-risk, q4-irrelevant. Do opportunistically. |
| 5 | **Per-column dictionary / narrow extracted keys** (hash a compact code, not the wide row, where the key is *not* the whole row) | repr / term 2 | Medium | M | M | Attacks term 2 for *join/group* keys (already narrow at the exchange, §14.2) and any operator whose key ⊊ row. **Does not help the aggregate inner multiset** (key = whole row). The salvageable narrow form of the closed surrogate idea (§14.9). |
| 6 | **Whole-query codegen / delegate fusion / monomorphization** | execution | **Low (measured wrong target)** | XL | H | **Demoted a third time, now on apportioned evidence (§17.2):** execution is 3–10 % of the floor and the generic abstraction is *already free*. Codegen cannot move a needle that lives in allocation + whole-row hashing. |
| 7 | **Off-heap / `NativeMemory` / ref-struct buffers** | repr / term 1 | Low–Medium | XL | **H** (safety/lifetime, GC interop, `Span` plumbing) | Literally off the GC heap, but pooling (#1/#2) gets ~all of term 1 *inside* safe managed code. Off-heap buys spill/bounded-memory (the spine's job already), not throughput. **Not worth the unsafety for the per-tuple goal.** |
| 8 | **Global integer surrogate row keys** | repr / term 2 | Low | L–XL | H | **Stays closed (§14.9):** loses at every `R≈1` site, dominated by the lazy view at the one `R≫1` site; the broad form is global interning with its W>1 contention/routing hazards. |

The shape of the table: **everything with real ROI is on the representation axis**;
the execution axis (6) is measured-dead; off-heap/unsafe (7) and global surrogates (8)
are dominated by safe managed pooling. The design collapses to a **two-front
representation program**: kill term-1 allocation with pooling/unboxed-pooled-flat-hash
(#1→#2, broad, all stateful ops), and attack the irreducible term-2 floor with a
columnar per-column inner multiset (#3, narrow, aggregate/join only).

### 17.5 — Fundamental vs fixable (honest ceiling)

- **Term 1 (allocation, ~50–60 %) is fixable in safe managed code** — pooling reclaims
  100 % of the dict backing (`poolbench`), and the retention constraint is satisfiable
  per-edge (§16.7). The cap is the residual GC/bump-allocate of the genuinely-live
  retained state, which Feldera also pays. **Reachable: most of term 1.**
- **Term 2 (whole-row hash, ~40–48 %) is partly fundamental.** Where the key ⊊ row
  (join/group keys) it is fixable by narrowing (#5). Where the key *is* the wide
  retained row (the aggregate inner multiset, MIN/MAX), the only escape is columnar
  per-column work (#3) — and even then a managed engine pays bounds checks, no
  SIMD-by-default, object-header/`Span` overhead that monomorphised Rust does not.
  **Realistic: shrink it, do not erase it.** A .NET unboxed/columnar engine should
  expect to *narrow* the 2–5× single-core gap on q4/q18/q19 toward ~1.3–2×, not reach
  parity. The §16.11 q3 **2.83× win** proves the ceiling is well above parity when the
  query avoids both terms (filter sheds 94 % of the stream, no retained aggregate
  state, tiny output) — so the engine is not categorically slow; it is slow *exactly
  where it allocates and whole-row-hashes retained state*.
- **Execution (~3–10 %) is fixable but not worth fixing** (§17.2): the abstraction is
  free, dispatch is small.

The honest bottom line: **the reachable prize is term 1 in full and term 2 partially;
parity with monomorphised columnar Rust is not reachable, but closing most of the
2–5× on the three laggards is.**

### 17.6 — Recommendation & smallest benchmark-gated first increment

Sequenced, measure-first, each gated before the next — mirroring how `surrogatebench`
/ `poolbench` retired options before any operator wiring:

1. **First increment (next): cross-tick delta pooling on the q4 hot operator** (lever
   #1's steady-backing half, behind a per-edge "no-`z⁻¹`" guard). It is the smallest
   change that validates the term-1 thesis on the floor-bound query: pool the delta
   `Dictionary` backing in `IncrementalAggregateOp` (and the join), reused across ticks
   on edges the compiler proves dead-after-tick (§16.7 item 3). **Gate:** q4 W=1
   `w1profile` ns/event + B/event must drop with the per-tick output cross-check green,
   *and* in-`Step` W=8 must improve (the §16.11 in-`Step` amplification — (a) went
   +4 % W=1 → +14 % W=8, so an in-`Step` term-1 win should translate to W>1, unlike the
   out-of-`Step` lever 2). If pooling the retained state proves to fight `Build()`
   ownership too hard, fall back to #2 (a purpose-built unboxed pooled flat-hash trace
   that owns its buffers and never hands them off).
2. **If it lands: generalise to the unboxed pooled flat-hash trace (#2)** across the
   stateful ops, and **start the columnar inner-multiset prototype (#3)** as its own
   arc — convert *one* operator (the q4 aggregate inner multiset) to SoA per-column
   reused buffers behind a seam, gated on q4 W=1 beating both the struct-keyed dict and
   the pooled-dict control. This is the term-2 attack and the real structural step; it
   deserves the same staged, benchmark-gated discipline the spine arc had (and the same
   willingness to retire it if the fine-tick floor defeats it, as sorted-merge was).
3. **In parallel (low-risk, independent): typed ingest (#4)** for q18/q19's input
   boundary, gated on `w1profile` B/event.
4. **Do not** build whole-query codegen (#6), off-heap buffers (#7), or surrogate keys
   (#8) — all measured-dominated.

**Microbench precursor already delivered this session:** `reprbench` is the
apportionment gate, and it *predicts the per-operator outcome before wiring* — it says
pooling captures the ~50–60 % term-1 prize and a columnar inner multiset is required
for the residual ~40–48 % term-2 floor on the aggregate. That is the §1 deliverable:
the gap is apportioned, the execution axis is retired with evidence, and the two
representation fronts are scoped with the smallest validating increment named.

### 17.7 — Landmines & what the representation must preserve

- **Preserve q3 (2.83× win).** `w1profile`: q3 is **81 ns / 89 B per event** — it wins
  because the `category = 10` filter sheds ~94 % of the stream *before* the join, the
  join output is tiny (22 rows) and flat, and there is **no retained aggregate state
  rebuilt per tick** — i.e. it pays *neither* term. The representation change must stay
  **opt-in / seam-gated to the stateful-heavy operators** (aggregate/join/TOP-K
  inner state), never a universal per-tuple tax on the filter/project/small-join path
  that is already at-or-above parity (q3 2.83×, q9 ~1.07×, q16/q17 win at W>1 by
  scaling). A representation that taxes the cheap path to help the expensive one is a
  net regression.
- **Honor the typed-compiler reflection gotcha** ([[typed-compiler-reflection-gotcha]]):
  q4 runs the **typed** parallel path; the new representation must be reachable without
  changing a builder signature — use an ambient seam (`[ThreadStatic]`, as the memtable
  / `SpineStagingConfig` did), not a new generic parameter or reflective arg-array edit.
- **Surrogate keys are CLOSED** (§14.9, dominated), **sorted storage lost on our ticks**
  (§5–§13 — don't re-propose it without confronting term 1), **coordination is a
  strength not a target** (§16.11).
- **Accommodate the not-yet-built operators.** The representation should not be designed
  blind to functionality the engine still owes: **windowing TVFs (TUMBLE/HOP/SESSION)**
  will add per-window grouped state (another `IndexedZSet`-shaped inner multiset — the
  columnar #3 target should generalise to them), and **UDFs** (`IScalarFunction` phase 5,
  [[scalar-function-registry-temporal]]) will reintroduce per-row delegate dispatch on
  the *scalar* path — the one place the execution axis (#6) could matter later, so the
  inner-loop representation should keep a clean delegate-or-codegen seam for scalars even
  though codegen is demoted for the *operator* loops today.

**Durable deliverables this session:** the `reprbench` decomposition harness +
[repr-decomp-bench.md](repr-decomp-bench.md), refreshed `w1profile` / `profile`
baselines, and an **apportioned** map of the per-tuple gap (term-1 allocation ~50–60 %
→ pooling; term-2 whole-row hash ~40–48 % → columnar inner multiset; execution ~3–10 %
→ retired) that scopes the next two representation fronts and the smallest gated first
increment — *not* a speculative columnar rewrite.

---

## 18. Term-2 attack, first increment — non-linear aggregate input narrowing (LANDED, gated, opt-in)

> Status: **prototype landed behind a default-off seam; benchmark-gated.** This is
> the first increment of §17's *term-2* front (the whole-row-hash floor). It began as
> "scope the columnar inner-multiset bet" and the investigation **corrected a
> load-bearing premise of §17** before any columnar code was written — exactly the
> measure/verify-first discipline paying off.

### 18.1 — The premise §17 got wrong: term-2 is *not* irreducible for q4

§17 asserted q4's term-2 (whole-row hash) is "largely irreducible short of columnar,
because the aggregate inner multiset is keyed by the whole wide row." Reading the
actual code disproves the "whole *wide* row" part:

- q4's inner aggregate is `MAX(b.price) GROUP BY a.id, a.category` — it references
  exactly **3 columns**.
- But its inner multiset stores the **full ~17-column join row**, because the
  `NarrowAggregateInput` optimizer rule (`PlanOptimizer.cs:354`) — which projects an
  aggregate's input down to `{group keys, aggregate-argument columns}` — **bails
  entirely if any aggregate is MIN/MAX/APPROX_COUNT_DISTINCT/COUNT(DISTINCT)**
  (`PlanOptimizer.cs:361-368`).

So term-2 for q4 is hashing/storing **17-column rows when 3 suffice** — not an
irreducible floor, a **conservative guard**. The §17 floor measurement (`reprbench`
WStr/W8 ≈ 47–80 ns/row) is the cost of the *wide* row; narrowing moves q4 toward the
W2 regime (~17 ns/row) at S effort, *before* any columnar rewrite.

### 18.2 — Why the guard is conservative, and the exact soundness envelope

The guard (`PlanOptimizer.cs:343-352`) protects against narrowing collapsing two rows
that share the kept columns but cancel in weight, hiding a value MIN/MAX would have
seen. Worked against the real `SqlMinMaxAggregator` (`SqlAggregators.cs:340-465`),
which keys its own incremental state (`Counts: Dictionary<value,count>`,
`Active: SortedSet<value>`) on the **aggregated value** and probes `after.WeightOf(row)`
per delta row to detect each row's positive/non-positive transitions, the precise
verdict is:

> **Narrowing to `{group keys, aggregate-argument columns}` is sound for MIN/MAX/
> DISTINCT whenever the per-group consolidated multiset is non-negative — i.e. for any
> *well-formed* insert/delete stream (a row is only deleted if previously inserted).**

The narrowing keeps the aggregate's *argument* columns, so collapsing rows that share
them is invariant for the aggregate (it reads only those columns); and over a
non-negative integral, the narrowed entry weight `Σ` over same-kept-columns rows is
`> 0` iff some positive-weight row exists — exactly the value-presence the aggregate
reads. The failure case the guard protects requires a genuinely **negative** weight in
the *consolidated* state (e.g. `(price=100, A):+1` and `(price=100, B):−1` coexisting),
which only a **malformed** stream (delete-without-insert / net-negative bag) produces.
The guard is correct for *arbitrary signed Z-sets* — which the engine does support and
test — and is needlessly conservative for the streams real workloads produce.

**This is a real, documented restriction, not a free win:** narrowing **cannot be a
default** because the engine's contract includes signed-Z-set correctness (the random-
query PBT deliberately generates `±1` weights including deletes of never-inserted rows,
`RandomQuery.cs:312`). It is a sound **opt-in for append-only / non-negative inputs**
(Nexmark, CDC-free event streams, most analytics ingest) — a large and common class.

### 18.3 — What landed

- **`NonLinearNarrowingMode`** (`Sql/Optimizer/NonLinearNarrowingMode.cs`): a
  default-off `[ThreadStatic]` seam, mirroring `SpineStagingConfig`/`FlatAggregateMode`.
  When enabled, `NarrowAggregateInput` narrows non-linear aggregates too. Read at
  `Optimize` time; no plan/compiler signature change (dodges
  [[typed-compiler-reflection-gotcha]]). Default-off ⇒ byte-identical to before.
- **Correctness gate.** `NonLinearNarrowingTests`: (1) the seam genuinely narrows the
  aggregate input arity 4→2 for MIN/MAX (non-vacuous); (2) over **300 seeded
  well-formed insert/delete streams** that deliberately create `{k,val}`-colliding rows
  differing in dropped columns, the narrowed circuit is **byte-identical** to the
  full-row circuit. The complementary regression guard is the full suite green with the
  seam off (**1,749 passed**, +2 new). The PBT is deliberately *not* reused (its
  generator emits out-of-envelope net-negative streams).
- **Gates.** `w1profile … narrow` (W=1 A/B) and `q4narrow` (W>1 in-`Step` A/B, the
  parallel typed path, output cross-checked identical — which also re-proves
  in-envelope correctness end-to-end on the insert-only Nexmark stream).

### 18.4 — Gate results

**W=1 (`w1profile`, 1M events, batch 10k, median-3):**

| Query | B/event full→narrow | ns/event full→narrow |
|:--|--:|--:|
| **q4** | 2,376 → **1,841 (−23 %)** | 2,256 → **1,456 (−35 %)** |
| q3 | 89 → 89 | ~unchanged (no MIN/MAX) |
| q9 | 2,345 → 2,345 | ~unchanged (TOP-1, not an aggregate) |

**W=8 in-`Step` (`q4narrow`, 1M events, median-3, this i9-12900K):**

| Batch | flat·full step | flat·narrow step | Step↑ |
|--:|--:|--:|--:|
| 10k | 579 ms | **424 ms** | **1.37×** |
| 100k | 565 ms | **462 ms** | **1.22×** |

The W=1 −35 % time translates to a **1.22–1.37× W=8 step** win — confirming §16.11's
claim that an **in-`Step`** per-row win translates/amplifies at W>1 (narrowing shrinks
the rows the inner aggregate hashes/stores *inside* `Step`), unlike the out-of-`Step`
output-boundary lever 2 that was Amdahl-eaten. This is the **largest single W>1 q4 step
win in the arc** — bigger than (a)'s +14 % (§16.11) and the §14.10 lazy view's
1.3–1.5×, because it removes ~14 columns of per-row hash/store/copy from the hottest
operator.

### 18.5 — Decision & what it does to §17's two-front plan

1. **Narrowing is a real, cheap, large chunk of q4's term-2** — captured at S effort
   without any columnar machinery, vindicating the verify-first detour. Productize it
   as an **opt-in `CompileOptions`/optimizer flag for non-negative inputs** (the clean
   public surface is a parameter to `PlanOptimizer.Optimize`, since narrowing runs at
   optimize time, separate from `Compile`; the seam is the prototype channel). Document
   the append-only envelope prominently.
2. **The columnar SoA inner-multiset bet (§17 #3) is reframed, not retired.** After
   narrowing, q4's inner multiset is a **3-column** row, so columnar now attacks a much
   smaller term-2 floor (W2 ≈ 17 ns/row, not WStr ≈ 80) — the prize shrank, so columnar
   drops in priority for q4 specifically. It remains the attack for aggregates whose
   arguments are genuinely wide or many, and for the TOP-K queries (q18/q19) where wide
   rows are retained for *output* (narrowing can't help — the columns are needed).
3. **Term-1 (allocation pooling, §17 #1/#2) is unchanged and still the other front** —
   narrowing reduces but does not remove the fresh-dict-per-tick allocation (the
   narrowed dict is smaller). Cross-tick pooling stays the next term-1 increment.

**Net:** the term-2 front's first increment shipped a **1.22–1.37× W=8 / 1.55× W=1 q4
win** behind a sound, default-off, append-only-gated seam — and the investigation
**corrected §17's "irreducible" premise** before the expensive columnar arc, so that
arc is now correctly scoped to the *residual narrow-row* floor and the output-retaining
TOP-K queries, not q4's (now-cheap) aggregate.

**Durable deliverables:** `NonLinearNarrowingMode` seam + rule change,
`NonLinearNarrowingTests` (well-formed-stream equivalence), the `w1profile narrow` and
`q4narrow` gates ([q4-narrow-bench.md](q4-narrow-bench.md)), and the corrected term-2
map. The honest caveat travels with it: **sound only for non-negative / append-only
inputs**, so it is opt-in, never a default.

> **DEFERRED — revisit to productize.** The prototype is proven and gated but lives
> behind a benchmark/test seam. Productization (a clean public opt-in — a parameter on
> `PlanOptimizer.Optimize`, with the append-only envelope documented, and ideally a
> planner check that engages it only when the aggregate's input lineage is provably
> insert-only) is a deliberate follow-up, not done here. The win (q4 W=1 −35 %,
> W=8 1.22–1.37×) justifies coming back to it once the other representation fronts are
> explored.

---

## 19. Term-2 attack on q18/q19 (TOP-K) — window-representation prototype RAN, mixed, REVERTED

> Status: **prototype built, measured, and reverted.** A documented dead-end, in the
> spirit of §8.3 (spine substrate), §9.5 (spine sharing), and §15.7 (barrier fusion):
> the value is the measured conclusion, not shipped code. q18/q19 are the next-worst
> single-core gaps after q4 (0.33×/0.35×) and, unlike q4, are **narrowing-immune** —
> partitioned TOP-K (`PartitionedTopKOp`) retains full rows for *output*, so columns
> cannot be dropped. This section looked for the analogous in-`Step` representation
> lever and **did not find a clean one.**

### 19.1 — The hypothesis and the change

The operator stores each partition's emitted window as a `Dictionary<TRow,long>`
(`PartitionedTopKOp.cs:75`), even though a window holds at most `limit` rows (q18: 1,
q19: 10). The hypothesis: for ≤10 entries a compact **`(TRow,long)[]`** beats the dict
on both per-tuple axes — one allocation per recompute instead of three, and
linear-scan equality over a tiny window instead of **hashing whole wide rows** in
`ComputeWindow`/`EmitDiff`. The change is **correctness-neutral** (same data, different
container; no soundness envelope, unlike §18) and needs no snapshot-format change
(`SaveAsync` serialises only `_accum`; `_window` is derived on load). A second
increment skipped the array realloc + diff when the recomputed window equals the
retained one.

### 19.2 — Why it was reverted (the measurement)

Clean same-build A/B (`w1profile`, 1M events, batch 10k, stateless controls q0/q2
byte-identical so the allocation measurement is exact):

| Query | shape | B/event base→proto | result |
|:--|:--|--:|:--|
| q18 | TOP-1, `PARTITION BY (bidder,auction)` | 2,160 → **1,837 (−15 %)** | win |
| q19 | TOP-10, `PARTITION BY auction` | 3,835 → **3,572 (−7 %)** | win |
| q9 | TOP-1, `PARTITION BY auction` (+ join) | 2,347 → **2,685 (+14 %)** | **regression** |

Three queries on the *same* operator moved in **two directions**, and the q9
allocation regression is **deterministic, reproducible, and unexplained by the diff**
— the skip-unchanged increment did not move it (2,687 → 2,683), so it is not the
per-recompute array realloc the hypothesis targeted. The structural difference is that
q9 partitions by `auction` (few partitions, many bids each, top-1 churns) while q18
partitions by `(bidder,auction)` (many partitions, touched ~once) — but that did not
yield an account of why the array container *raises* q9's allocation while *lowering*
q18's. Time at W=1 was a wash on q9 (noisy 1,294–1,512 ns) and a modest win on q18/q19
(−9 % / −12 %).

### 19.3 — Decision and conclusion

**Reverted.** A correctness-neutral representation change that **regresses a structurally
identical query by 14 % for reasons I could not explain** is not shippable, and the
wins it does deliver (q18/q19, −7…−15 % allocation at W=1) are both **modest** and
**likely diluted at W>1**: §16.10 already showed q18's competitive (W>1) cost is the
*output boundary* (decoded out-of-`Step` at W>1) plus coordination, not in-`Step`
window state — so an in-`Step` window-container win is exactly the kind that Amdahl-
shrinks where it would need to count.

**The portable conclusion:** **q18/q19's TOP-K has no cheap in-`Step` representation
lever analogous to q4's narrowing.** Its retained window is already tiny (≤`limit`),
so changing its container is marginal; its competitive gap lives in the wide-row
**output boundary** (Layer B, out-of-`Step` at W>1 — needs the parallel output path to
eagerly decode, a different lever than §16.9's single-circuit one) and coordination
(§15, the shared BSP ceiling), not the window dictionary. This **redirects q18/q19
effort away from `PartitionedTopKOp`'s state** and back to (i) the term-1 allocation
front (cross-tick delta pooling, §17 #1, which hits the per-touched-partition builder
churn broadly) and (ii) the parallel-path output-materialisation boundary. The columnar
SoA bet (§17 #3) remains scoped to genuinely-wide aggregate inner state, not TOP-K.

**Durable value:** a measured dead-end that saves the next session from re-trying the
"narrow the TOP-K state" idea, plus the localisation of q18/q19's real gap (output
boundary + coordination, not window representation).

---

## 20. Term-1 attack — cross-tick delta-builder pooling (LANDED behind a seam; thin throughput, but the architectural question is answered YES)

> Status: **mechanism built, gated, kept default-off behind a seam.** This is §17's
> *term-1* front (the ~50–60 % per-tick **allocation** half of the per-tuple floor)
> and the lever §17 ranked #1. Its headline result is **not** a throughput win — that
> is thin, exactly as §16.10 predicted — but the **load-bearing architectural
> question it was chosen to answer**: *can a managed operator reuse its delta buffer
> across ticks despite `Build()`'s ownership transfer?* The answer is **yes, safely**,
> which is the prerequisite the columnar end-state (§17 #3) needed proven.

### 20.1 — The mechanism

The per-tick allocation §16.3 measured is the fresh `ZSetBuilder` (a new
`Dictionary`) each stateful op builds, fills, and `Build()`s every `Step`. §16.8
pre-sizing removed the *resize churn* half; this removes the *steady backing* half by
**reusing one builder across ticks**:

- **`ZSetBuilder.Reset()`** (clear, keep the grown dictionary) + **`BuildShared()`**
  (wrap the dictionary in a Z-set **without** nulling it, so the builder keeps it) —
  `ZSetBuilder.cs`. This deliberately breaks the `ZSet` ctor's "callers must not
  retain the dict" invariant, which is sound **only on a dead-after-tick edge**.
- **`DeltaPoolMode`** (`Operators/Stateful/DeltaPoolMode.cs`): a default-off
  `[ThreadStatic]` seam read at operator construction (mirroring
  `SpineStagingConfig`/`FlatAggregateMode`; no builder signature change). When on,
  `IncrementalAggregateOp` and `IncrementalJoinOp` hold one pooled builder and
  `Reset` + refill + `BuildShared` each `Step` instead of allocating fresh.

### 20.2 — The retention constraint, and that it is satisfiable (the real result)

A pooled `BuildShared` output **shares** the builder's dictionary, which the next
tick's `Reset` clears — so it is correct only if **nothing retains the output across
ticks**. §16.7 item 3 established the only cross-tick aliaser of a delta is `DelayOp`
(`z⁻¹`), and flat (non-recursive) pipelines put no `z⁻¹` on an operator-output delta.
This section **proves that constraint holds empirically**:

- **`DeltaPoolingPbtTests`** runs the full random-query oracle (joins / aggregates /
  filters / TOP-K — all flat) with pooling **on**, over **3000 iterations including
  retractions** (`±1` weights), and the pooled circuit's accumulated output **equals
  the batch re-computation** every time. Pooling is pure memory reuse, so a mismatch
  would expose an aliasing bug — none appeared.
- **Full suite green with the seam off** (1,750 passed, +1 the pooled PBT) — the
  byte-identical-when-disabled regression guard.
- **Parallel output cross-check** (`q4pool`) confirms pooled = unpooled output on the
  W-replica pipeline, re-proving the constraint end-to-end on the parallel path.

So the `Build()`-ownership invariant **can be broken safely** with the
`Reset`/`BuildShared` pattern under a no-`z⁻¹`/non-terminal edge guard. Recursive-CTE
circuits (explicit `z⁻¹` on deltas) and a circuit's externally-read terminal output
(unless the consumer copies each tick) are the excluded cases — the per-edge analysis
a productionised version would need.

### 20.3 — The throughput prize is thin (as §16.10 predicted)

**W=1 (`w1profile … pool`, 1M events, batch 10k, median-4):**

| Query | B/event off→on | note |
|:--|--:|:--|
| q4 | 2,376 → **2,198 (−7.5 %)** | join + 2 aggregates pooled |
| q20 | 1,291 → 1,237 (−4 %) | wide join |
| q9 | 2,349 → 2,324 (−1 %) | join + TOP-1 (TOP-K not pooled) |
| q18 | 2,175 → 2,175 (**0 %**) | control — TOP-K only, nothing pooled |

Time was flat within noise. **W=8 (`q4pool`):** the step ratio ranged **0.86×–1.29×**
across runs — inside the parallel bench's ±0.5× noise floor (§11), a slight positive
lean (median ~1.05–1.14× at the realistic batch 10k), **not a reliable win.**

Why thin: (1) §16.8 pre-sizing already removed the larger *churn* term, leaving only
the steady backing; and (2) the pooled output builders are only **part** of q4's
per-tick allocation — the `GroupProject` re-index `IndexedZSet` builds, the trace
`MergeInPlace` growth, and the `object?[]` input boundary are all unpooled. Reclaiming
~7 % of q4's allocation moves throughput by less than the bench's noise.

### 20.4 — Decision: keep as a seam; the value is the de-risked columnar foundation

- **Do not productionise pooling as a throughput optimisation.** A per-edge
  "no-`z⁻¹`, non-terminal" compiler analysis to reclaim ~7 % of q4's allocation (and a
  noise-level step) is not worth the complexity/risk. `DeltaPoolMode` stays
  **default-off behind the seam**, joining `ForceEagerRebuild` / `ForcePointProbe` /
  `CoalesceJoinExchange` / `NonLinearNarrowingMode` as a proven, reproducible, gated
  mechanism + regression guard.
- **The architectural result is the deliverable.** §17 ranked term-1 #1 *specifically
  because it answers whether buffer reuse is feasible despite `Build()` ownership* —
  the question the entire columnar/arena end-state (§17 #3) hinges on. It is now
  answered **yes, safely**, with a working `Reset`/`BuildShared` primitive and an
  empirically-validated dead-after-tick edge constraint. A columnar inner-multiset
  that reuses arena buffers across ticks (the Feldera model, §16.4) can build on this
  with the retention rule already proven — it does **not** have to re-litigate buffer
  reuse from scratch, and it must own its buffers exactly the way this seam does.
- **Term-1 is therefore substantially closed as a *standalone* lever** (pre-sizing
  shipped §16.8; steady-backing pooling proven-but-thin here). The remaining per-tuple
  headroom on the gap queries is the **other unpooled allocation sites** (re-index /
  trace / boundary) and the **columnar restructuring** that would fold the output
  build, the re-index, and the inner multiset into one reused columnar buffer — the
  §17 #3 end-state, now standing on a proven buffer-reuse foundation.

**Durable deliverables:** the `ZSetBuilder.Reset`/`BuildShared` pooled-builder
primitive, the `DeltaPoolMode` seam, `DeltaPoolingPbtTests` (the retention-constraint
proof), and the `w1profile … pool` / `q4pool` ([q4-pool-bench.md](q4-pool-bench.md))
gates — plus the **answered architectural question** that unblocks the columnar arc.

---

## 21. The columnar arc, re-justified — and the cheaper term-2 lever it surfaced: projection pushdown through joins (LANDED, gated, unconditionally sound)

> Status: **measure-first design session that pivoted the increment, gated it, and
> shipped it default-ON (unconditionally sound).** This opened as the columnar / arena
> inner-multiset arc (§17 #3) with the prompt's own honest instruction: *re-justify the
> target first — §18 narrowing may already have captured most of q4's term-2, so measure
> where whole-row hashing is still large before any rewrite.* The measurement did exactly
> that and **corrected a load-bearing premise of §17/§18** — the same way §18 corrected
> §17 — surfacing a cheaper, **unconditionally sound** lever the optimizer was missing, and
> retiring (for now) the expensive columnar rewrite for the query that motivated it.

### 21.1 — The measure-first question

§17 scoped columnar to "the one place term 2 is both large and irreducible: the
`IndexedZSet` inner value multiset." §18 then proved term 2 is **not** irreducible for
q4's *aggregate* — narrowing the MAX input to `{keys, args}` (3 cols) gave −35 % W=1 /
1.22–1.37× W=8. So before writing any columnar code, §1 of this arc demands: **after §18,
where is whole-row hashing still large, and does it actually need columnar?** The
candidates §1 named: (a) aggregates referencing many columns; (b) **join inner state** —
the stored side's wide rows, which §18's *aggregate-input* narrowing sits above and never
touches; (c) retraction-heavy streams outside §18's append-only envelope.

### 21.2 — The measurement: term-2 at the join is large, and §18-immune

Two verified facts localise it to the **join trace**:

- **The optimizer has no column-liveness rule across joins.** `PlanOptimizer` documents
  it in so many words (`PlanOptimizer.cs:38`: *"Not yet applied: general top-down column
  liveness across joins"*). The only narrowing that exists is §18's, *above* the join.
- **The join stores and hashes the full source row.** `IncrementalJoinOp`'s trace is an
  `IndexedZSet<joinKey, storedRow>` whose inner `ZSet` is keyed by the **whole stored
  row**; every `MergeInPlace` integrate re-hashes it and the cross-product probe re-touches
  it (§14.2). So for q4's `auction ⋈ bid`, the traces store all ~10 auction and ~7 bid
  columns even though only `{id,category,date_time,expires}` (auction) and
  `{auction,price,date_time}` (bid) are live — read by the aggregate, the residual
  (`b.date_time BETWEEN a.date_time AND a.expires`), and the equi-key.

`reprbench` was extended with an **`idx` mode** ([repr-decomp-bench.md](repr-decomp-bench.md))
that models exactly this — one join trace's per-tick `IndexedZSet` integrate + cross-product
probe — and A/Bs **wide-stored vs narrow-stored** inner rows (the only difference being the
stored row width):

| Stored inner row | wide ns/row | narrow (W2) ns/row | **term-2 prize** | alloc saved |
|:--|--:|--:|--:|--:|
| W8 (8 long) → W2 | 134.0 | 80.9 | **53.1 ns (40 %)** | 402 B (49 %) |
| WStr (3 long+str) → W2 | 178.3 | 75.7 | **102.6 ns (58 %)** | 134 B |

**Term 2 at the join is large (40–58 % of the join-trace per-row cost) and §18 cannot reach
it.** So the arc does *not* retire — there is real term-2 left after §18. But the measurement
says something more specific about *which lever* captures it.

### 21.3 — The reframe: the prize is a cheap optimizer rule, not columnar SoA

The decisive detail in the table: the prize is captured by the **`narrow` column — which is
still an ordinary flat `Dictionary`.** Narrowing the stored row from wide → few-columns
recovers the entire 40–58 %; a columnar SoA inner multiset could only chase the *residual
below* the narrow-dict line. And narrowing the join's stored rows is exactly **projection
pushdown through the join** — the column-liveness rule `PlanOptimizer.cs:38` says is "not
yet applied." That is an **S–M optimizer rule, no new data structure**, the join analogue
of §18 — and it is the right first increment, not the XL columnar rewrite. The same shape
as §18: *a cheap narrowing corrects the "needs columnar" premise before the expensive arc.*

**Critically, join column-pruning is *unconditionally* sound — strictly better than §18.**
§18 narrows the aggregate's *own* multiset, which a *non-linear* aggregate (MIN/MAX) can
distinguish, so it needs the non-negative/append-only envelope and stays opt-in. Join
pruning drops only columns **no consumer reads** (not the parent, not the join's
combine/residual/equi-key). Two stored rows identical on the kept columns but differing on a
dropped one produce **identical** join output rows — the dropped column is in no output — so
they consolidate to the same weight whether collapsed before the join (pruned) or after. The
join is bilinear and no aggregator reads a dropped column *through* the join (the aggregate's
argument columns are kept, being referenced above). This holds for **arbitrary signed
Z-sets** — it is ordinary relational projection pushdown, valid by construction, so the
random-query PBT can run with it **on** (it could not for §18).

### 21.4 — What landed

- **`JoinColumnPruningMode`** (`Sql/Optimizer/JoinColumnPruningMode.cs`): a default-off
  `[ThreadStatic]` seam, mirroring `NonLinearNarrowingMode`/`DeltaPoolMode`. Read at
  `Optimize` time; no plan/compiler signature change (dodges [[typed-compiler-reflection-gotcha]]).
- **`PruneJoinInputs`** (`PlanOptimizer.cs`): when a `Project` or `Aggregate` parent sits
  over an **INNER** `JoinPlan`, compute the live output columns (parent refs ∪ equi-key ∪
  residual), split per side, and insert a narrowing `ProjectPlan` on each side that has a
  strict non-empty live subset — remapping the equi-keys (native indices), residual (concat
  space), output schema, and the parent's references. INNER-scoped in v1; a side never
  narrows to zero columns (keeps the multiplicity-only cross-product case safe). Idempotent
  (re-firing bails), so it converges under the optimizer's fixpoint loop.
- **Correctness gate.** `JoinColumnPruningTests`: (1) the **random-query oracle with pruning
  ON over the full ±1 surface, 3000 iters** including deletes of never-inserted rows
  (outside §18's envelope) — pruned circuit ≡ batch oracle every time; (2) a direct
  seam-on-vs-off circuit equivalence over arbitrary signed streams; (3) a non-vacuous check
  that the seam genuinely narrows **both** stored join inputs 3→2. Full suite **1,753
  passed**, seam off ⇒ byte-identical.

### 21.5 — Gate results (the increment is a large win)

**W=1 (`w1profile … prune`, 1M events, batch 10k, median-3):**

| Query | ns/event base→prune | B/event base→prune | note |
|:--|--:|--:|:--|
| **q4** | 1,987 → **988 (−50 %)** | 2,376 → **1,592 (−33 %)** | join `auction ⋈ bid`, traces 10→4 / 7→3 cols |
| q20 | 1,125 → **1,010 (−10 %)** | 1,291 → 1,291 | wide-output join: trace narrows, output stays wide (correct) |
| q3 | 68.8 → 68.8 (**unchanged**) | 89 → 75 (−16 %) | the 2.83× win **preserved**; GC 1/1/0 → 0/0/0 |
| q9 | ~noise | 2,349 → 2,307 | auction side narrows; right side (TOP-1) already narrow |
| q0/q1/q2/q18/q19 | noise | identical | no join — plan unchanged (clean control) |

**W=8 in-`Step` (`q4prune`, 1M events, median-3, this i9-12900K; output cross-checked
identical):**

| Batch | flat·full step | flat·prune step | **Step↑** |
|--:|--:|--:|--:|
| 10k | 594.8 ms | **203.1 ms** | **2.93×** |
| 100k | 620.5 ms | **148.0 ms** | **4.19×** |

The W=1 −50 % **amplifies** to a **2.93–4.19× W=8 step** — confirming §16.11's in-`Step`
thesis emphatically (the join integrate runs inside `Step`), and growing with batch because
larger batches accumulate more bids per auction, so the wide-row hashing pruning attacks is
the §14.2 O(K²)-in-group-growth term. This is **the largest single q4 step win in the entire
arc** — bigger than §18's 1.22–1.37×, §14.10's 1.3–1.5×, and (a)'s +14 % — and it is on the
**worst single-core gap in the whole comparison** (q4 0.21× vs Feldera, `dbsp-bench-2.txt`).

### 21.6 — Decision

- **Flipped default-ON (this session).** Unlike every prior seam in this family,
  `JoinColumnPruningMode` is **unconditionally sound** (proven by the full-±1 PBT) and delivers
  a **−50 % W=1 / 2.93–4.19× W=8** win on the worst query with **no regression on the cheap
  path** (q3 preserved, the §17.7 landmine respected) — so it ships on, not as an opt-in. The
  rule now runs for every caller that optimizes (`PlanToCircuit.Compile(PlanOptimizer.Optimize(plan))`,
  the documented idiom); the **full SQL e2e suite is green with it on (1,753 passed)**, every
  join query pruned by default with output unchanged — the production-scale corroboration of
  the PBT. Implementation note: the seam stores the **inverse** of an opt-out flag, because a
  `[ThreadStatic] bool` cannot default to `true` (a field initializer never runs on threads
  that did not first touch the type) — so "on" is the zero/default state and benchmarks set it
  `false` to A/B the full-row baseline. The next generalisation (deferred) is the proper
  top-down column-liveness pass `PlanOptimizer.cs:38` anticipates — pruning Filter/TOP-K/window
  parents and chained joins, not just the local `Project/Aggregate(Join)` patterns.
- **The columnar SoA inner multiset (§17 #3) is reframed, not built — and its prize shrank
  again.** For q4 it is now firmly behind two cheaper levers (§18 on the aggregate, §21 on
  the join), both of which leave the inner state a *narrow* flat dict where columnar's
  residual win is small (the reprbench `narrow` floor, ~17 ns/row, is mostly alloc +
  narrow-key hash, not wide-row hash). Columnar is correctly scoped to the genuine residual:
  **(i) genuinely-wide-and-needed stored rows** — q20-style wide *output* joins, where
  pruning can't shrink the rows because the output needs them (and where §16.9's lazy-boxing
  output boundary already attacks the same cost); **(ii) wide aggregates** with many genuine
  argument columns; **(iii) retraction-heavy / CDC** streams where neither §18 nor the
  append-only assumptions hold and wide rows are retained. On the Nexmark suite, after §18 +
  §21, term-2 on the wide-row-hash floor is **largely captured by the two cheap narrowing
  rules** — so the XL columnar rewrite should **wait for a workload that exhibits the
  residual**, not be built speculatively against q4 (whose term-2 is now narrow).

### 21.7 — Honest ceiling & landmines preserved

- **Ceiling (§17.5 restated).** Two cheap narrowing rules + pooling close most of term-1 and
  the *reducible* part of term-2. The *irreducible* part — hashing the genuinely-live narrow
  key, plus bounds-checks / no-SIMD-by-default / object-header overhead a managed engine
  pays — remains. The §16.11 q4 0.21× gap had room for a ~2× W=1 narrowing (now measured),
  pointing q4 from "heavy loss" toward "competitive," **not** to parity with monomorphised
  Rust. The honest target stands: narrow the 2–5× laggards toward ~1.3–2×.
- **q3 (2.83×) preserved** — measured unchanged at W=1, with a small alloc/GC improvement;
  the rule only fires on stateful join inputs and only drops dead columns, never taxing the
  filter/project/cheap path (§17.7).
- **Typed reflection gotcha honored** — the rule is a logical-plan rewrite at `Optimize`
  time; q4's typed parallel path consumes the already-pruned plan with no builder-signature
  change (the W=8 gate's identical cross-checked output re-proves this on the typed path).
- **Surrogates closed, sorted-merge lost, coordination not a target, codegen dead** — all
  unchanged; this increment touched none of them.

**Durable deliverables:** the `reprbench` `idx` join-trace measurement
([repr-decomp-bench.md](repr-decomp-bench.md)); the `JoinColumnPruningMode` seam +
`PruneJoinInputs` rule; `JoinColumnPruningTests` (full-±1 PBT — the unconditional-soundness
proof — + the non-vacuous narrowing check); the `w1profile … prune` and `q4prune`
([q4-prune-bench.md](q4-prune-bench.md)) gates; and the reframed columnar map — q4's term-2
is now captured by **two cheap narrowing rules**, and columnar SoA is correctly deferred to
the genuinely-wide residual, not built speculatively.

---

## 22. Partitioned TOP-K (q18/q19) — decomposing the last competitive gaps, and pricing the row-narrowing lever (MEASURED, design-only)

> Status: **measure-first design session, no shipped engine code.** q18/q19 are the
> last clear Nexmark losses (q18 0.46× @10c / 0.34× @1c; q19 0.52× @10c / 0.32× @1c,
> Run D). This arc opened with the prompt's own honest instruction: *decompose the gap
> first; §19 already found "no cheap in-`Step` lever" for the TOP-K window, and the
> queries are `SELECT *` so §21's column-pruning does not transfer — there is a real
> chance the conclusion is "narrow modestly or accept the BSP floor."* The measurement
> did three things: (1) **completed** the 4-way decomposition (single-core **and** W=10/14),
> (2) **priced** the one non-forbidden lever — true row/key narrowing — and found it
> **large** (50 % q18 / 82 % q19 of the in-state op cost), and (3) **corrected** §19's
> blanket pessimism: §19's reverted *container* swap (dict→array, wide rows kept) was
> marginal; a *key-representation* change (store `{order, rowRef}`, recover wide rows for
> survivors) is 2–4× bigger and sits **outside** the container-change zone §19 closed.
> The honest catch: capturing it is a real operator rewrite (an order-key extractor +
> a wide-row recovery store), **not** the ThreadStatic seam §18/§20/§21 were — so it is
> **justified but not cheap**, and scoped here as the next increment rather than shipped
> speculatively under the measure-first mandate.

### 22.1 — The measure-first question, and the two honest constraints

q18 dedups the latest bid per `(bidder, auction)` (TOP-1, `ORDER BY date_time DESC`,
many tiny partitions); q19 keeps the ten highest bids per `auction` (TOP-10,
`ORDER BY price DESC`, fewer partitions accumulating). Both run on `PartitionedTopKOp`,
which keeps **per partition** a `SortedDictionary<wideRow,long>` (`_accum`, ordered by
the ORDER BY key with a full-row tiebreak) plus a `Dictionary<wideRow,long>` window —
**both keyed by the whole 7-column bid row** (incl. the `url`/`extra` strings), though
ranking needs only `{partition, order}`. Two constraints bound the search before any code:

- **Not column-prunable.** Both queries `SELECT` all 7 bid columns — effectively `SELECT *`
  — so §21's projection-pushdown-through-join lever (drop columns no consumer reads) has
  **nothing to drop**. This is exactly the "genuinely-wide residual" §21.6 named.
- **§19 closed the container door.** §19 swapped the window `Dictionary` for a compact
  array (still storing wide rows) and **reverted** it: −7…−15 % at W=1 but a deterministic,
  unexplained **+14 % q9 allocation regression**. So a TOP-K *container* change is off the
  table. The only remaining lever is changing **what the state stores**, not its container.

### 22.2 — The 4-way decomposition (single-core AND W=10/14)

Combining `stepprofile` (in-`Step` phase split, docs/step-profile.md) with the egest
profile (`q18profile q18,q19`, docs/q18-profile.md, which confirms out-of-`Step` output)
attributes the gap into the prompt's four phases. **(d) out-of-`Step` output is ~0** for
both (egest `Gather%` = 0–1 % at every W — the parallel path decodes output lazily after
`sw.Stop()`, §16.10), so the gap is entirely in-`Step`, split three ways:

| | (a) in-`Step` TOP-K op | (b) in-`Step` exchange (split+gather) | (c) coordination wait | (d) egest |
|:--|--:|--:|--:|--:|
| **q18 @ W=1** | **100 %** (1.30 µs/ev) | — (no shuffle) | — | ~0 % |
| **q18 @ W=10** | 35 % | **43 %** | 22 % | ~0 % |
| **q19 @ W=1** | **100 %** (2.02 µs/ev) | — | — | ~0 % |
| **q19 @ W=10** | **83 %** | 11 % | 6 % | ~0 % |

Three load-bearing reads:

1. **Single-core, the whole cost is the in-`Step` TOP-K op (a).** This is the §16.11
   single-core gap (q18 0.34×, q19 0.32×) localised precisely: per-partition `_accum`/
   `_window` over wide rows. The op **scales well** (8.2× q18 / 5.5× q19 to W=10) — the
   single-core per-row cost is the problem, exactly the §16.11 thesis.
2. **q18 and q19 wear the competitive (W=10) gap in *different* phases.** q18 is
   **exchange-movement-bound** (43 % moving wide rows across the shuffle) + op (35 %) +
   wait (22 %). q19 is **overwhelmingly op-bound** (83 %); its exchange is cheap (11 %)
   because few auctions ⇒ few distinct shuffle keys. So a single lever that narrows **both
   what the op stores and what the exchange moves** hits q18 on both (a)+(b) and q19 on (a).
3. **(c) coordination is not a target** (§15/§16.11 — it is the shared BSP ceiling and our
   strength). It is 22 % (q18) / 6 % (q19) and excluded from the lever's reachable surface.

### 22.3 — Pricing the lever: `reprbench topk` (the decide-or-retire number)

`reprbench` was extended with a `topk` mode (docs/repr-decomp-bench.md) modelling the
operator's per-tick hot path — partition + sorted-insert each delta row, recompute the
top-k window, diff, emit — over the full 7-column bid row, and A/Bing three stored
representations (same access pattern; only the stored representation differs):

- **`wide`** — today: `SortedDictionary`/`Dictionary` keyed by the whole row (whole-row
  hash + the wide value-type node copy).
- **`narrow+fetch`** — the candidate: store `{order, rowRef}`; **recover the wide row from
  a pool only for the ≤k survivors** the output needs. Output is wide in **both** (the
  fetch-back recovers it), so the charge is apples-to-apples and the prize is purely the
  cheaper in-state compare+hash — nothing the lever cannot reach.
- **`wide·sorted`** — a comparison-based `SortedDictionary` window (no whole-row hash, wide
  rows kept) — used only to **attribute** the prize, *not* a shippable option (it is the
  §19-forbidden container change).

| Shape | wide ns | narrow+fetch ns / B | **total prize** | kill-hash | shrink-key | fetch-back cost |
|:--|--:|--:|--:|--:|--:|--:|
| **q18** (TOP-1, tiny partitions) | 702 | 348 / 678 | **50 %** | 25 % | 25 % | cheap (raw 231 ≈ n+f 348) |
| **q19** (TOP-10, accumulating) | 5080 | 923 / 1458 | **82 %** | 40 % | 42 % | cheap (raw 857 ≈ n+f 923) |

(Numbers from the instrumented run that apportions the prize, docs/repr-decomp-bench.md;
a plain concrete-`Dictionary` baseline — without the `IDictionary` dispatch the attribution
adds to *both* wide variants — measured the total prize slightly lower at 36 %/75 %, the
same order. The control join-trace `idx` rows reproduce §21's 31–58 % reference values,
validating the harness. The prize apportions ~evenly between **kill-hash** (whole-row hash
→ cheap key hash) and **shrink-key** (wide node → narrow node), and **`narrow+fetch`
captures both** — a `{order,ref}` key hashes cheaply *and* stores small — so the
row-narrowing rewrite earns the **full** 50 %/82 %, whereas the §19-forbidden `wide·sorted`
container would earn only the kill-hash half.)

**Three findings settle the lever:**

1. **The prize is large and real** — 50 % (q18) / **82 %** (q19) of the in-state op cost
   (36 %/75 % on the plain-`Dictionary` baseline), well above §21's join-trace `WStr`
   (58 %) that justified shipping projection pushdown. This **refutes §19's blanket "no
   cheap in-`Step` lever"**: §19 measured a *container* swap (marginal); row/key narrowing
   is a different, far larger axis.
2. **The fetch-back — the prompt's flagged "hard part" — is cheap.** `narrow-raw` (no
   recovery) ≈ `narrow+fetch` (231 vs 348 ns q18; 857 vs 923 ns q19): recovering wide
   survivors via an O(1) pool reference costs ~15 % of the narrow path, not the prize.
   Because q18 keeps 1 survivor/partition and q19 ≤10, the recovery volume is tiny.
3. **The model is faithful to the competitive path.** The bench uses a **value-type** wide
   row, matching the **typed parallel** path (the path the W>1 comparison runs), where the
   node copy *and* the hash both apply. On the StructuralRow (W=1 single-circuit) path the
   wide row is a reference, so the copy term shrinks and the prize tilts toward the
   kill-hash half — still real, slightly smaller. Either way the prize survives.

### 22.4 — Why this is a real operator change, not a ThreadStatic seam

The prompt's template was "mirror `JoinColumnPruningMode` — a ThreadStatic seam, no
builder-signature change." That worked for §21 because pruning only **drops** columns the
optimizer already knows are dead — no new data crosses into the operator. Row-narrowing is
different: the operator must **build a narrow key** and **recover the wide row for
survivors**, and neither is derivable from what `PartitionedTopKOp` holds today (it has
only `IComparer<TRow>` order/sort-key-only comparers — these enable narrow *comparison* but
**not** narrow *hashing*, and the measurement says the hash is half the prize). So the
faithful version needs two things plumbed in:

- **An order-key extractor** (`TRow → narrowKey`). Good news: it already exists at **both**
  compile sites — `TypedPlanCompiler.BuildPartitionedTopK` builds the per-column sort
  extractors (`keys[i]`), and `PlanToCircuit.CompilePartitionedTopK` has the ORDER BY
  column indices. It would be threaded as an **optional** `PartitionedTopK` builder/ctor
  parameter (defaulting null). This is **reflection-safe**: the typed path reaches the
  builder through `BuildPartitionedTopK` (found by *name*, then called as ordinary compiled
  code), so an added optional parameter — updated at that call site and in the `Invoke`
  args array together — does **not** trip [[typed-compiler-reflection-gotcha]] (which bites
  only *silently-reflected* signatures). No `Optimize`-time-only constraint is violated.
- **A wide-row recovery store.** The narrow key carries a reference/index to the wide row;
  for the typed value-struct path that means a backing buffer + index maintained under
  retraction (append on insert, release when a row's accumulated weight hits 0). Equal-order
  **ties** fall into small value-equality buckets (q18 unique `date_time` ⇒ size 1; q19
  occasional price ties ⇒ ~1), so the only residual whole-row work is within a tie group.

That is the **§17 #3 columnar/arena-class structural change, scoped to one operator** —
plus snapshot-format compatibility, a random-query equivalence **PBT with the seam on**,
and a **q9 non-regression** gate (q9 shares this operator and is where §19 regressed). It is
*buildable and gated*, but it is **not** a same-day seam, and shipping a half-verified
incremental operator under retraction would be the wrong risk for a measure-first session.

### 22.5 — Decision, scoping, and honest ceiling

- **Justified — build it, as a focused gated increment (next).** Unlike §19 (reverted) and
  §8.3/§9.5 (dead-ends), the measurement here is **positive and large** and the lever is
  **non-forbidden** (key-representation, not container). The disposition is *build*, not
  *retire* — but as the scoped operator rewrite above, **default-off behind a seam**
  (mirroring `NonLinearNarrowingMode`/`DeltaPoolMode`, since TOP-K narrowing has the
  retraction/tie correctness surface that join-pruning's unconditional soundness lacked),
  gated on the q18 single-core `w1profile` + a W=8 step harness with the per-tick output
  cross-check, and the q9 non-regression check. **Start with q18 (TOP-1)** — one survivor
  per partition, no tie buckets — then generalise to q19's TOP-10.
- **Expected reach (from the decomposition × the prize).** q19 is 83 % op at W=10 and the
  op is mostly in-state, so removing ~82 % of the in-state cost could move q19's competitive
  step materially (the cleanest case — a pure op-bound query meeting a large op-state
  prize). q18 gets the op share (35 % × 50 %) **plus** the exchange share **if** the narrow
  `{order,ref}` also flows through the shuffle (attacking the 43 % Move term) — a
  pipeline-level extension beyond the operator, and the larger build.
- **Honest ceiling (§17.5 restated).** This narrows the in-state floor; it does not touch
  coordination (c, not a target) and cannot make a `SortedDictionary` of order keys free.
  The target stays "narrow the 2–5× laggards toward ~1.3–2×," not parity — q18/q19 from
  ~0.32–0.34× single-core toward "competitive," consistent with §21's q4 result on the
  worst gap.
- **Landmines preserved.** q3 (3.19×) and the W>1 wins are untouched (seam-gated to the
  TOP-K path); coordination is not a target (§16.11); the typed reflection gotcha is honored
  (optional param via the compiled `BuildPartitionedTopK` call site, not a reflected
  signature). Retired-by-measurement and **not** revived here: §19's container swap, typed
  ingest (`ingestpath`), surrogate keys (§14.9), whole-query codegen (§17.2), sorted-merge
  on fine ticks (§8.3).

**Durable deliverables:** the 4-way decomposition (single-core + W=10/14, docs/step-profile.md
+ docs/q18-profile.md, the latter extended to q19 and reframed as the egest/phase-(d)
probe); the `reprbench topk` measurement (docs/repr-decomp-bench.md) that **prices the
row-narrowing lever at 50 %/82 %**, proves the fetch-back cheap, and apportions kill-hash vs
shrink-key; and the **decision** — a positive, non-forbidden, **justified** lever, scoped as
the next focused gated operator increment (q18 first), correcting §19's pessimism with
evidence rather than re-trying its dead-end.

### 22.6 — Built and gated: q19 lands, q18 doesn't — the prize tracks partition state, not partition count (SHIPPED, default-off seam)

The §22.5 increment was built: a narrow-key partitioned-TOP-K operator
(`PartitionedTopKNarrowOp`) keying its per-partition `_accum`/`_window` by a
`readonly struct OrderRowKey {order value, wide row}` — hashing on the order value
alone (the kill-hash half) and comparing the wide row only within an equal-order tie
group, with the wide row **carried in the key** (the recovery store, no separate
buffer/index). It sits behind the default-off `PartitionedTopKNarrowingMode` seam
(mirroring `DeltaPoolMode`), selected by `CircuitBuilder.PartitionedTopK` only when a
**single-column** ORDER BY extractor was plumbed through — which both compile sites
(`PlanToCircuit.CompilePartitionedTopK`, `TypedPlanCompiler.BuildPartitionedTopK`) now
pass as `keys[0]/descending[0]/nullsFirst[0]` when `SortKeys.Count == 1`. The typed
reflection gotcha is dodged exactly as §22.4 predicted: `BuildPartitionedTopK` already
*had* `keys` as a local, so only its own in-method call site changed — no
reflected-signature change. Snapshot format is byte-compatible (same flattened
`(wideRow, weight)` Z-set).

**Correctness held.** A 5000-iteration equivalence PBT
(`PartitionedTopKNarrowingPbtTests`) over the whole TOP-K surface
(ROW_NUMBER/RANK/DENSE_RANK, k ∈ {1,2,3}, ASC/DESC, ±1 ticks **incl. retractions** over
tie-forcing domains) proved seam-on accumulated output ≡ an independent batch TOP-K
oracle **and** ≡ the seam-off whole-row operator. The full suite is green with the seam
off (1754 tests, byte-identical), and every W=8 gate cross-checks the narrow output
identical to whole-row (re-proving correctness on the typed parallel path).

**The throughput result inverts the prompt's expected ordering, and it is the
load-bearing finding.** The lever was scoped "q18 first (simplest), then q19" — but q18
is exactly where it **doesn't** pay, and q19 (the harder TOP-10) is where it lands large:

| Query | shape | W=1 time (off→on) | W=1 alloc (off→on, deterministic) | W=8 step ↑ |
|:--|:--|--:|--:|--:|
| **q9** (shared op, TOP-1) | join + dedup | 1300 → 1335 ns (noise) | 2345 → 2349 B (**+0.2 %, flat**) | — (control) |
| **q18** (TOP-1, ~1 row/partition) | dedup | 1826 → 1840 ns (flat) | 2173 → 2224 B (**+2.3 %**) | 0.89–1.08× (**parity/noise**) |
| **q19** (TOP-10, accumulating) | top-10 | 2542 → **1862 ns (−27 %)** | 3831 → **1734 B (−55 %)** | **1.30× / 1.29×** |

Three reads settle it:

1. **q19 is a clean, robust win** — deterministic −55 % W=1 allocation, −27 % W=1 time,
   and a **1.30× W=8 step** that holds across both batch sizes (cross-checked identical
   output). This is precisely the §22.2-identified **83 %-op-bound** query meeting the
   §22.3 **82 %** in-state prize, and the §16.11 in-`Step` amplification carrying it
   through Amdahl to W=8. The lever works.
2. **q18 does not pay — and *why* is the correction to §22.3.** q18 keeps TOP-1 over
   **size-1 partitions**, so its window/accum dicts hold ~1 entry. There the whole-row
   hash is both **cached** (`StructuralRow` precomputes it upstream) **and** trivially
   cheap (a 1-entry probe), so the kill-hash prize ≈ 0, while the narrow machinery (the
   16-byte struct key vs an 8-byte ref, slightly larger dict backing) adds a
   **deterministic +2.3 % allocation**. The `reprbench topk` 50 % q18 figure was modelled
   on a **value-type** wide row (faithful to the typed W>1 path, where the hash is *not*
   cached and the node copy is real) — landmine 1(a)/1(b) materialised exactly:
   on the single-circuit `StructuralRow` path the prize erodes to nothing for q18, and
   even the typed W=8 path can't recover it because the **win tracks per-partition state
   size, not partition count** — q18 has no state to narrow.
3. **q9 is unharmed** (alloc +0.2 %, the §19 failure mode — a deterministic q9 allocation
   regression — does **not** recur), so enabling the seam is safe for the shared operator.

**Disposition — ship the seam, default-off (like §18/§20).** Correctness holds, the gate
is a clean robust positive **on q19**, and q9 is unharmed — so the operator and seam land
on `main`. But the honest scope is narrower than §22.5 hoped: the lever is a **TOP-K-state
narrowing**, and it pays **only when a partition holds enough state to amortise the
narrow-key overhead** (q19's accumulating TOP-10), not on dedup-shaped TOP-1 with size-1
partitions (q18/q9). The seam is process-wide opt-in; a productionising follow-on would
gate it per-operator (e.g. `limit > 1`, or a partition-fanout heuristic) so q18/q9 keep
the whole-row op while q19-shaped windows take the narrow path. **The §22 arc thus
delivers a real, gated competitive lever for q19 (~0.32× → narrowed) and a measured
"no" for q18** — correcting not §19's pessimism this time but §22.3's own q18 optimism,
with the same measure-first honesty: the microbench's value-type model over-priced the
single-circuit/cached-hash reality for the size-1-partition case.

**Durable deliverables (22.6):** `PartitionedTopKNarrowOp` + `OrderRowKey`/
`OrderRowComparer`/`OrderRowEquality` + the `PartitionedTopKNarrowingMode` seam; the
single-column extractor plumbed through both compile sites + `CircuitBuilder.PartitionedTopK`;
`PartitionedTopKNarrowingPbtTests` (batch-oracle + whole-row equivalence, retractions);
the `w1profile … narrowtopk` and `q18narrow [q18|q19]` gates
(`Q18NarrowBenchmark` + `SpineParallelHarness.FlatNarrowTopK`); and the **decision** — a
gated, correct, default-off lever that **lands on q19 (1.30× W=8) and is measured flat on
q18**, with the prize attributed to per-partition state size rather than the partition
count the q18-first framing assumed.

### 22.7 — Default-ON via a per-operator limit gate; the crossover sweep proves `limit` is a proxy for accumulation, not the predicate (SHIPPED, default-on)

§22.6 shipped the narrow operator behind a process-wide, default-off seam because it
does not always *pay* (it is always *correct*). This increment turns the q19 win on
**automatically**: the seam's *selection* role is replaced by a **per-operator compile
decision**, and the ThreadStatic seam is demoted to a force-on/off override for the §22
A/B gates. `PartitionedTopKNarrowingMode.Enabled` (bool) becomes
`PartitionedTopKNarrowingMode.Override` (`Auto` / `ForceNarrow` / `ForceWholeRow`); the
selection moves into `ShouldNarrow(limit)`, which under the default `Auto` takes the
narrow path iff a single-column extractor is present **and** `limit > AutoLimitThreshold`.
The predicate is a pure plan-read at both compile sites — `plan.Limit` was already a local
in `CompilePartitionedTopK` / `BuildPartitionedTopK`, so no analysis and no
reflected-signature change (the typed-compiler reflection gotcha stays honoured, exactly
as §22.4/§22.6).

**The crossover sweep (`topkcrossover`, docs/topk-crossover-bench.md) measured the
threshold before trusting it — and the finding is the load-bearing one.** §22.6 had data
only at the endpoints (limit∈{1,10}); this A/Bs whole-row vs narrow on the **real
single-circuit W=1 path** (cached `StructuralRow` hash and all — the reality the
value-typed `reprbench topk` over-priced) across `limit ∈ {1,2,3,5,10}` on **both**
partition shapes:

| limit | q18 shape (tiny partitions) | q19 shape (accumulating) |
|------:|:--|:--|
| 1 | narrow **+2.2 % alloc** (2224 vs 2176 B), time ≈ noise | narrow **1.78× time / 2.91× alloc** |
| 2 | narrow +2.2 % alloc, time ≈ noise | narrow 1.67× / 2.83× |
| 3 | narrow +2.2 % alloc, time ≈ noise | narrow 1.59× / 2.77× |
| 5 | narrow +2.2 % alloc, time ≈ noise | narrow 1.51× / 2.49× |
| 10 | narrow +2.2 % alloc, time ≈ noise | narrow 1.48× / 2.21× |

**`limit` is orthogonal to the prize.** On the **accumulating** shape narrow wins at
*every* limit — including limit=1 (1.78×/2.91×), the win actually *largest* at the small
limit and compressing as the limit grows (more TOP-K window to narrow-key adds overhead).
On the **tiny-partition** shape narrow costs a flat, deterministic **+2.2 % allocation**
at *every* limit — including limit=10 — with time within noise (the 16-byte struct key vs
an 8-byte ref in the dict backing; no accumulated `_accum` to shrink, no kill-hash prize
on a 1-entry window). The prize tracks **per-partition accumulation**, and `limit` is not
that — it only *correlated* with the win across the two real Nexmark queries by accident:
q18 happens to be tiny-partition **and** limit=1, q19 accumulating **and** limit=10.

**Decision — flip default-on with `limit > 1` anyway, because it is the right cut for the
queries that exist, and the only cheap static one.** Accumulation is not knowable at
compile time; `limit` is the sole cheap static signal. `limit > 1`:
- **provably leaves every TOP-1 (limit=1) on the whole-row op** — `ShouldNarrow(1) = 1 > 1
  = false` — so q18 and q9 (the named protected shapes, both limit=1) are byte-identical
  to the pre-§22.7 default. The +2.3 % q18 regression that §22.6 kept the seam off to
  avoid **cannot recur** — q18 never reaches the narrow path.
- **routes q19 (limit=10) to narrow**, capturing the win automatically.

This satisfies the §21 default-ON precedent's bar (a gated lever goes default-on once the
gate is justified and the suite is green) for the *named* shapes. The honest caveat is the
off-diagonal: `limit > 1` mis-serves the two shapes the two real queries don't occupy —
**tiny-partition k≥2** pays the +2.2 % alloc with no win (a small, deterministic
regression, but no such query exists in the Nexmark suite), and **accumulating k=1** (e.g.
q9's *own* TOP-K op, which wins ~2.9× *in isolation* but is join-diluted to +0.2 % at the
query level, §22.6) is left on whole-row — money on the table, not a regression. The true
predicate is **runtime per-partition state size**; a runtime accumulation-adaptive gate
(switch keying once a partition's `_accum` crosses a size) is the named follow-on — out of
scope for a static plan-read, and a bigger operator change.

**Validation.**
- *Selection* — `PartitionedTopKSelectionTests` asserts the constructed operator type on
  **both** compile paths (flat `PlanToCircuit` and typed `TryCompile`): narrow for
  limit∈{2,3,10}, whole-row for limit=1 and for any multi-column ORDER BY, and the force
  overrides pin either arm regardless of limit.
- *Non-regression* — `w1profile` under `Auto` (the production default) vs the
  `wholerowtopk` baseline: **q9 2345 B = 2345 B** and **q18 2173 ≈ 2175 B** (same operator;
  the 2 B is GC-timing noise in the per-event divisor), **byte-identical**; **q19 1734 B /
  1742 ns vs 3831 B / 2709 ns** — the −55 % alloc / −36 % time win is now picked up with no
  flag. W=8 `q18narrow q19` holds **1.49× / 1.69×** step (≥ §22.6's 1.30×; output
  cross-checked identical). The full suite is **1762 green** with `Auto` default-on (narrow
  runs on every k≥2 windowed-TOP-K test, re-confirming the equivalence the PBT proves).

**Durable deliverables (22.7):** the `Auto`/`ForceNarrow`/`ForceWholeRow` override +
`ShouldNarrow(limit)` per-operator gate (`AutoLimitThreshold = 1`); the gate threaded
through `StatefulOperators.PartitionedTopK` and the §22 A/B harnesses (now force-arms);
`PartitionedTopKSelectionTests` (both compile paths); the `topkcrossover` sweep
(`TopKCrossoverBenchmark`, docs/topk-crossover-bench.md); and the **decision** — q19's
narrow win is now **default-on automatically**, with the measured-honest finding that
`limit` is a proxy for accumulation (perfect on the real queries, fuzzy off-diagonal) and
the runtime-adaptive gate named as the follow-on.

### 22.8 — q18 shuffle-narrowing measured: the two halves never coexist → NO-BUILD (measurement only)

With q19 landed default-on (§22.7), the prompt's gated follow-on — flowing the narrow
`{order, ref}` through the **shuffle** to attack q18's exchange-movement share (§22.2/§22.5
"Expected reach") — was opened as a **measure-first** thread. The operator-only narrow
(§22.6) does not reach q18's exchange; the question is whether a *pipeline-level*
shuffle-narrowing build would. Two measurements priced it.

**(1) Re-decompose q18's step (`stepprofile q18`, design §15 tooling).** The parallel step
splits into `split` (bucket each row by `hash(partitionKey)%W`), `wait` (barrier), `gather`
(rebuild the post-shuffle indexed Z-set, re-hashing rows), and `op = step − the rest` (the
TOP-K). Across W=4–16 (single-pass, noisy): **op dominates (≈50–66 % of step) and scales**
(14 ms W=1 → 2.5–4 ms), `split+gather` ("move") is ≈25–37 %, `wait` is small (<10 % off the
W=12 straggler outlier). This re-confirms §15's conclusion (movement is not the ceiling; the
op — the per-row floor — is) and §16.11.

**(2) Price the movement narrowing (`gatherbench`, `ExchangeGatherBenchmark`,
docs/exchange-gather-bench.md).** A faithful A/B of the per-worker `split+gather` round-trip
(bucket into W=8 destinations + rebuild the index), wide 7-col bid vs a narrow
`{bidder, auction, order, ref}` payload, on the **value-type uncached-hash** model faithful
to the typed W>1 path q18 actually runs:

| Shape | gather (hash only) | split+gather (full move) |
|:--|--:|--:|
| q18 (tiny partitions) | wide→narrow **−58 %** | wide→narrow **−73 %** |
| q19 (accumulating) | −51 % | −69 % |

So narrowing recovers ~**70 % of the movement** — and because the typed exchange copies a
*value struct* by value into the bucket list, `split` is width-attributable too, not just
`gather` (correcting the first-pass assumption that `split` only moves an 8-byte reference).
Naively: 70 % × ~30 % move ≈ a ~20 % step reach — within the §17.5 ceiling, q19-ballpark.

**Why it is still NO-BUILD — the two halves never coexist for q18.** The microbench's narrow
arm recovers the wide row from a shared `pool[idx]` **pre-populated outside the timed loop** —
i.e. it assumes wide-row recovery is *free*. That is the load-bearing cheat, and unwinding it
kills the lever for q18 specifically:
- q18 is `SELECT *` — all seven columns are in the output, which is produced **after** the
  shuffle on the destination worker. So the wide row must reach that worker regardless.
- On the **typed parallel path** (the one with the real movement cost — value structs copied)
  a value struct has **no heap identity**, so a narrow `{order, ref}` cannot carry a free
  8-byte object reference to recover it; recovery needs **boxing** (a per-row heap allocation —
  the very cost being removed) or a **shared id-keyed wide-row pool** whose maintenance under
  an append-heavy, retracting bid stream reintroduces the wide-row storage + hash. The
  free-reference recovery `OrderRowKey` uses (§22.6) exists only on the **structural W=1
  path** — and there q18 is already flat (size-1 partitions, cached hash, §22.6).
- Even granting the full ~20 % movement reach, **`op` (50–66 %, the per-row TOP-K floor) is
  untouched** — and §22.6 measured that floor flat to narrowing on q18's size-1 partitions.
  A ~20 % step gain moves q18 from ~0.44× toward ~0.53× vs Feldera — not parity.

So q18's two narrowable halves are disjoint: the path with real movement to save (typed,
value structs) has no cheap recovery for `SELECT *`, and the path with cheap recovery (free
reference) has no movement worth saving (cached hash, size-1). The lever priced at ~70 % in
isolation collapses once the recovery store it hides is charged. **Decision: do not build the
q18 shuffle-narrowing thread.** q18's residual gap is the per-row `op` floor (§16.11/§17.5)
plus the `SELECT *` wide-row materialisation (§21.6's "genuinely-wide residual") — neither a
cheap lever. This closes the §22 arc: q19 shipped default-on (§22.7), q18 measured to a
documented NO-BUILD, consistent with §15 (movement is not the ceiling) and the honest ceiling
(narrow the gaps to ~1.3–2×, not parity).

**Durable deliverables (22.8):** the `stepprofile` q18 re-decomposition (op-dominated, move
~30 %); the `gatherbench` / `ExchangeGatherBenchmark` movement A/B (docs/exchange-gather-bench.md)
pricing the isolated narrowing at ~70 % of `split+gather`; and the **decision** — the
isolated prize is real but uncashable for q18 because the wide-row recovery it hides is free
only on the path that has no movement to save (the two-halves-never-coexist finding), so the
pipeline-level build is not worth it.

---

## Appendix — sources

DbspNet: `ZSet.cs`, `IndexedZSet.cs`, `IncrementalJoinOp.cs`,
`IncrementalAggregateOp.cs`,
`ExchangeOp.cs`/`ExchangeIndexOp.cs`/`ExchangeIndexJoinOp.cs`,
`Circuit/ParallelCircuit.cs`/`ExchangeCoordinator.cs`/`StepProfiler.cs`,
`Spine/SpineBatch.cs`, `Spine/SpineZSetTrace.cs`,
`Spine/SpineIndexedZSetTrace.cs`, `CompileOptions.cs`,
`Benchmarks/PureTraceBenchmark.cs`/`StepProfileBenchmark.cs`/`ExchangeFuseBenchmark.cs`
(`stepprofile`/`exchangefuse`, docs/step-profile.md + exchange-fuse-bench.md);
`Benchmarks/W1ProfileBenchmark.cs`/`ProfileHotPath.cs`/`ReprDecompBenchmark.cs`
(`w1profile`/`profile`/`reprbench`, docs/w1-profile.md + repr-decomp-bench.md — §16/§17);
`Sql/Optimizer/NonLinearNarrowingMode.cs` + `Benchmarks/Q4NarrowBenchmark.cs`
(`q4narrow`, docs/q4-narrow-bench.md — §18);
`Operators/Stateful/DeltaPoolMode.cs` + `ZSetBuilder.Reset/BuildShared` +
`Benchmarks/Q4PoolBenchmark.cs` (`q4pool`, docs/q4-pool-bench.md — §20);
`Sql/Optimizer/JoinColumnPruningMode.cs` + `PlanOptimizer.PruneJoinInputs` +
`Benchmarks/Q4PruneBenchmark.cs` + `ReprDecompBenchmark` idx mode
(`q4prune`/`w1profile … prune`, docs/q4-prune-bench.md + repr-decomp-bench.md — §21);
`Operators/Stateful/PartitionedTopKOp.cs` + `ReprDecompBenchmark` topk mode +
`Q18ProfileBenchmark` (q18+q19 egest) + `StepProfileBenchmark`
(`reprbench`/`q18profile`/`stepprofile`, docs/repr-decomp-bench.md + q18-profile.md +
step-profile.md — §22);
`Operators/Stateful/PartitionedTopKNarrowOp.cs` (+ `OrderRowKey`/`OrderRowComparer`/
`OrderRowEquality`) + `PartitionedTopKNarrowingMode.cs` +
`PartitionedTopKNarrowingPbtTests` + `Benchmarks/Q18NarrowBenchmark.cs` +
`SpineParallelHarness.FlatNarrowTopK` (`q18narrow [q18|q19]`,
`w1profile … narrowtopk`, docs/q19-narrow-bench.md — §22.6, shipped default-off);
the `PartitionedTopKNarrowing` override + `ShouldNarrow` per-operator gate +
`Benchmarks/TopKCrossoverBenchmark.cs` + `PartitionedTopKSelectionTests`
(`topkcrossover`, `w1profile … wholerowtopk`, docs/topk-crossover-bench.md — §22.7,
default-on); `Benchmarks/ExchangeGatherBenchmark.cs` + `StepProfileBenchmark` q18
re-decomposition (`gatherbench`, `stepprofile q18`, docs/exchange-gather-bench.md +
step-profile.md — §22.8, NO-BUILD);
parallel-pipeline-perf profiling notes.

Differential Dataflow: arrangements mdbook (ch. 5), `trace/mod.rs`,
`spine_fueled.rs`, McSherry "Specialize differential dataflow" (2017) &
"Containers" (2024), `columnation` & `flatcontainer` repos, *Shared
Arrangements* (VLDB 2020, arXiv:1812.02639), Materialize arrangements docs.

Feldera/DBSP: `dbsp` Rustdocs (`Batch`/`BatchReader`/`Builder`/`Cursor`/
`Trace`/`Spine`, `OrdZSet`/`OrdIndexedZSet`, `dynamic` module), feldera/
storage-design + layer file format, "Meet the Feldera storage engine",
"Cutting Down Rust Compile Times… One Thousand Crates", DBSP VLDB 2023
(p1601-budiu.pdf).
