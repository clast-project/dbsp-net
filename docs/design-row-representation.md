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

**Decision & next step.** Keep `TraceFamily.Flat` the default. The one change the
data justifies is to **make the memtable on-by-default whenever `TraceFamily.Spine`
is selected, at capacity 8,192** — a strict improvement on every stateful query.
That needs the capacity threaded through `CompileOptions` → the spine operators →
the trace ctor (the plumbing the `SpineStagingConfig` static seam currently
sidesteps), so it is a scoped follow-up. Closing q4's residual ~8% is the spine
**aggregate** read path, a separate item.

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
