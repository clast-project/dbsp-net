# Design: state representation — the throughput-decay arc

Status: **Rounds 1–3 complete (2026-10-29).** Shipped: A, B, G (+ NULLable SUM), B2 (typed and
structural), A's column dedup. Not shipped: C, D, E, H. Round-3 standing is in §9.

Goal: stop DbspNet's throughput from decaying as operator state grows. The M4
Nexmark comparison (`docs/benchmarks-comparison.md`, 2026-08-31) showed our ev/s
falling 1M→10M events while Feldera's holds flat. This arc asks whether a
different in-memory representation of the state kept between ticks (the
integral / trace behind join, aggregate, distinct and TOP-K) fixes that.

## 0. Instruments

`dotnet run --project src/DbspNet.Benchmarks -c Release -- decay [events] [batch] [segments] [stream|pregen] [q..]`
(`src/DbspNet.Benchmarks/DecayBenchmark.cs`). One long W=1 run per query, split
into segments; per segment: engine ev/s (push+step only), GC pause time that
landed inside the timed engine work, GC counts, heap size, B/ev, retained state
rows. At the end: **live state bytes after a forced full GC** (outside the timer).

Two of these are deterministic and therefore immune to machine contention — the
property that lets several agents measure in parallel:

| metric | determinism (two processes, same commit) |
|---|---|
| **live state bytes** | 1M events: q4 490,388,328 vs 490,384,512; q18 1,966,893,528 vs 1,966,830,184 — **≤0.004%** |
| **B/ev** | byte-exact (established in [design-row-representation §25]) |

ev/s is **not** — single segments swing 2× with GC timing. Wall-clock claims in
this arc come only from serialized runs on an idle machine.

## 1. Phase 0 findings (10M events, W=1, DATAS off, `stream` mode)

### 1.1 The harness confound is not the cause

`pregen` (whole stream materialized, ~3 GB of live event objects) vs `stream`
(generated per batch): q0 and q22 are flat in **both** modes at W=1 (q0 ≈ 4.3M,
q22 ≈ 1.9M ev/s from segment 1 to segment 10). The q22 "decay" in the M4
Nexmark record (8.95M→6.37M at W=14, −29%) is inside that run's documented ±40%
noise band and is discarded as evidence. Stateless queries do not decay.

### 1.2 Stateful queries decay in two distinct ways

| query | ev/s first → last fifth | in-step GC pause, worst fifth | live state @10M | stored rows | **bytes / stored row** |
|---|---|---|---|---|---|
| q3 (filtered join) | flat ~35–40M | ≤13% | 100 MB | ~135k | ~740 |
| q4 (join + agg) | 1.43M → 1.55M (flat) | 13% | 4,501 MB | ~9.2M bids | ~490 |
| q9 (join + TOP-1) | 1.83M → 1.74M (flat) | 9% | 3,315 MB | 10.4M | ~320 |
| q15 (COUNT DISTINCT / day) | 1.28M → 0.82M (**−36%**) | 4% | 762 MB | (distinct sets, uncounted) | — |
| q16 (… / channel, day) | 0.97M → 0.66M (**−32%**) | 8% | 2,159 MB | (uncounted) | — |
| q18 (TOP-1 dedup) | 1.02M → 0.35–0.87M | **28%** (1.6 s) | **20,537 MB** | 9.2M | **~2,230** |
| q19 (TOP-10 / auction) | 1.26M → 0.73–1.12M | **41%** (1.1 s) | 8,191 MB | 9.2M | ~890 |
| q20 (bid ⋈ auction) | 2.04M → 1.11–2.37M | 23% | 5,192 MB | ~9.2M bids | ~560 |

1. **GC-tracing decay (q18, q19, q20).** Throughput becomes bimodal as the heap
   grows: segments with no GC run at roughly the start rate; segments containing
   a gen2 (or a gen0 that must scan dirty cards in a huge gen2) lose 20–40% of
   their time to pause alone. The pauses scale with the **number of live objects**
   — and these queries keep 5–20 GB of them. A Rust engine pays none of this.
2. **Locality decay without GC (q15, q16).** Ex-pause throughput falls ~35%
   while pause stays ≤8%: each group's eight COUNT(DISTINCT) sub-states are
   `Dictionary<object,long>` keyed by a **boxed** argument (§2), growing to
   millions of entries; every probe chases entry → box out of cache. A layout
   cost, not a GC cost.
3. **Joins/aggregates whose working set is append-mostly hold up (q4, q9).**
   Their state is large too (320–490 B/row) but their throughput is flat — they
   carry the memory cost without (yet) the time cost.

### 1.3 Bytes per stored row is the headline

A Nexmark bid is 7 columns (~3 longs, a timestamp, 3 short strings). Packed, with
the strings interned or arena-allocated, that is ~60–100 B. We store
**320–2,230 B per row** — 4–30× bloat. q18 is the outlier: ~9.2M partitions of
one row each, so per-partition container overhead dominates (see §2).

This is also the most tractable target: it is deterministic, it predicts both
decay mechanisms (fewer objects → shorter GC marks; denser layout → fewer cache
misses), and it bounds how far state can grow before memory runs out.

## 2. Where the bytes are (state-structure map)

All seven queries compile on the **typed** path (`TypedPlanCompiler`), so stored
trace rows are emitted structs (fields inline, VARCHAR = `Utf8String` over one
`byte[]` per string), not `StructuralRow`. The bloat is in the **containers**
and in the places the typed path still boxes.

| state | type today | heap objects | queries |
|---|---|---|---|
| join / aggregate trace | `IndexedZSetTrace` → `Dictionary<K, ZSet<V>>`, each `ZSet` its own `Dictionary` (`IndexedZSet.cs:24`) | **4 per key** (ZSet, Dictionary, buckets, entries) + 1 `byte[]` per string cell | q4, q9, q20 |
| COUNT(DISTINCT) sub-state | `Dictionary<object,long>` keyed by the **boxed** argument (`TypedSqlAggregators.cs:911`), ×8 per group in q15/q16 | 1 box per distinct value + dictionary | q15, q16 |
| MIN/MAX sub-state | `Dictionary<T,long>` + `SortedSet<T>` per group — duplicates the trace | ~2 per value | q4 |
| partitioned TOP-K | `Dictionary<TKey, SortedDictionary<TRow,long>>` accum + `Dictionary<TKey, Dictionary<TRow,long>>` last window (`PartitionedTopKOp.cs:72,75`); `TKey` is a `StructuralRow` of **boxed** values | **~15–20 per partition** (sorted dict ×3, tree node, window dict ×3 rebuilt per touch, up to 2 key rows each with array + boxes, strings) | q9, q18, q19 |

q18 is the degenerate case: 9.2M partitions × 1 row each → the per-partition
fixed cost *is* the per-row cost (~2.2 KB).

**Seams.** There is no trace interface. Operators construct the concrete
`IndexedZSetTrace` / `ZSetTrace`, and `IndexedZSet` is simultaneously the delta
type, the trace's `.Current`, and the snapshot-codec type. The spine precedent
was "copy the operator family + compile-time switch" (`CompileOptions.TraceFamily`),
not an interface. The only true shared seam today is `IMultiset<T,Z64>` (how
aggregators read a group; spine already implements it). TOP-K and window state
are inline in their operators — swapping them is operator-local.

## 3. Phase 1 plan

Ranked by measured prize, cheapest-to-swap first. Each is a branch
(`exp/state-*`) in its own worktree, judged on the two deterministic instruments
(live bytes/row, B/ev) plus the incremental≡batch property suites; wall-clock
for survivors only, serialized, on an idle machine.

| # | target | variant(s) | seam | prize signal |
|---|---|---|---|---|
| A | partitioned TOP-K state | one flat table keyed by an **unboxed** typed partition key → small inline sorted array per partition; last window kept as a count/threshold, not a rebuilt dictionary | operator-local | q18 2.2 KB/row, q19 890; q18/q19 GC decay |
| B | COUNT(DISTINCT) (and MIN/MAX) sub-state | typed `Dictionary<T,long>` → open-addressing table over unboxed keys; MIN/MAX read the trace instead of duplicating it | aggregator-local | q15/q16 −35% ex-pause decay |
| C | join/agg trace — hash family | extract `IIndexedTrace<K,V>` seam; implement a **single flat open-addressing table** over (K,V) with a key→run index (1 object per *table*, not 4 per key) | new interface (prerequisite, serial) | q4/q9/q20 320–560 B/row |
| D | join/agg trace — sorted family | same seam; Feldera-shaped **sorted columnar batches + LSM merge**, no hash map on the row path | same | same, plus locality; tests whether Feldera's structure is the reason it holds flat |
| E | strings (cross-cutting) | one `byte[]` per string cell → shared arena / interning | `Utf8String` construction at ingest | every query storing VARCHAR |

Sequencing: **A, B, E** are independent and operator-local — run them in
parallel now. **C/D** need the seam first; I extract it serially (a pure
refactor, gated by the existing suites and byte-identical B/ev), then C and D
fan out on it.

## 4A. Variant A — compact partitioned TOP-K state (branch `exp/state-A-topk`)

**Built.** `PartitionedTopKCompactOp<TRow,TKey>` replaces both `PartitionedTopKOp` and
the §22 `PartitionedTopKNarrowOp`. It sits behind `PartitionedTopKStateMode`, which is
default-on. The `DBSPNET_TOPK_STATE=compact|compactboxedkey|legacy` env var, or the
thread-static `Override`, selects the arm.
- **One flat table.** `Dictionary<TKey, Partition>`, where `Partition` is a struct. A
  partition with one row stores that row **inline**, with no heap object of its own.
  2..32 rows live in one sorted `Entry[]`. Past 32 rows a partition moves to a
  `SortedDictionary`, so a hot partition's insert stays O(log n).
- **No stored window.** The window is a pure function of the state. On a partition's
  first touch in a tick, the operator computes the old window from the pre-delta state
  into a reused scratch buffer. After the delta it computes the new window and
  merge-diffs the two, both sorted by `order`. Scratch buffers are reused, so the only
  per-tick allocation is the output delta.
- **Unboxed partition key (typed path).** The key is an emitted key struct built by
  `BuildExprKeyExtractorDelegate`, replacing the `StructuralRow` of boxes.
  `BuildPartitionedTopK` is now `<TRow,TKey>`, and its reflected invoke was updated with
  it. The structural path keeps the `StructuralRow` key but gets the compact container.
- The snapshot payload is unchanged (one flattened Z-set), and load just replays it.

**Results** (1M events, batch 10k, W=1). Live bytes come from `decay … stream`; B/ev
comes from `w1profile`. The legacy arm reproduces the branch-point baseline: B/ev is
byte-identical, and live bytes are within 0.001% for q18/q19 and 0.3% for q9.

| query | live bytes legacy | compact, boxed key | **compact** | ratio | B/ev legacy → boxed key → **compact** |
|---|---|---|---|---|---|
| q18 | 1,968,248,608 (2,139 B/row) | 681,988,568 | **473,135,792 (514 B/row)** | **4.16×** | 2106 → 1518 → **1429** (−32%) |
| q19 | 874,274,896 (950 B/row) | 549,917,720 | **538,989,264 (586 B/row)** | **1.62×** | 1668 → 1351 → **1270** (−24%) |
| q9 (incl. join state) | 361,170,928 | 286,055,240 | **275,194,880** | 1.31× | 1116 → 1111 → **1030** (−8%) |

On q18 the container lever and the key lever multiply. The container alone gives
2.89×, and unboxing the key takes that to 4.16×.

**What is left in q18's 514 B/row** (a probe on a synthetic q18): the typed row is a
104 B struct. Three of its ten fields (bidder, auction, date_time) are duplicated by
the plan's window-input projection, which is a planner issue, not this operator's.
Each row also has three string `byte[]`s, which are variant E's territory. The rest is
`Dictionary` growth slack: capacity runs up to about 1.7× count, and the entry is
152 B, holding the 16 B key and a 24 B partition trailer. A custom row-keyed
open-addressing table could save that ~40 B/entry plus some slack, an estimated
~15% more. It was not built.

**Correctness.** The new `PartitionedTopKCompactPbtTests` check two things:
- compact ≡ batch oracle, on the §22 tie domain (3000 cases);
- compact ≡ legacy **every tick**, across structural, typed W=1, typed W=2 and typed
  boxed-key. This runs on a domain that crosses the inline↔array↔tree boundaries in
  both directions, with NULLable and VARCHAR multi-column partition keys and NULLable
  and multi-column ORDER BY (250 cases).

Both of these mutations were caught:
- dropping the row on array→tree promotion;
- corrupting the array→inline demotion.

A snapshot restore-then-step test covers all three storage shapes on each arm. The
full suite passes: 2380 passed, 1 skipped.

**Risk: snapshot compatibility.** The payload is unchanged, but the manifest's plan
fingerprint hashes operator type names. A snapshot written by a pre-arc binary
therefore fails the fingerprint check under the new default. Today the only way to
restore it is `DBSPNET_TOPK_STATE=legacy`, or a fingerprint alias, which has not been
built. Every operator-type change has this property, the §22 narrow operator included.

**Recommendation: ship.** It is a large deterministic win on all three rows, with
lower B/ev everywhere. If it ships, `PartitionedTopKNarrowOp` and the §22 selection
seam become dead code apart from the legacy arm, and can be retired together with the
legacy arm once restore compatibility has been dealt with.

### 4A.1 No duplicated partition/order columns (`exp/state-r3-looseends`)

**Built.** The resolver (`TryResolvePartitionedTopK`) used to append every PARTITION
BY / ORDER BY expression as a hidden trailing column of the window input, even when
the expression was a bare column already in the select list. That is how q18's TOP-K
row carried bidder, auction and date_time twice. `ReuseVisibleColumnsForHidden` now
points such a key at the existing column and drops the copy. It also folds a key
repeated across PARTITION BY and ORDER BY onto one hidden column. Computed keys
(`v + 1`) and keys over non-selected columns stay hidden as before. The visible
prefix, the strip projection above the TOP-K and every result are unchanged. The
resolver is shared, so the structural and typed paths both get the narrower row.

A second effect is larger than the bytes saved in the row. With no hidden columns
left, q18/q19's window-input projection and strip projection become identities. The
optimizer then removes them: the structural q18-shape circuit goes from three
operators to one, and the typed path loses the same two per-row projections.

**Results** (1M events, batch 10k, W=1, `NEXMARK_GEN=feldera`; both arms built clean
from the same tree, differing only in the resolver; output rows identical):

| query | live bytes before → after | Δ | B/ev w1profile before → after | B/ev decay (mean of 5) |
|---|---|---|---|---|
| q9 | 284,998,848 → **231,742,040** | −18.7% | 1022 → **949** (−7.1%) | 994 → 921 |
| q18 | 641,558,456 → **594,180,832** | −7.4% | 1070 → **860** (−19.6%) | 1070 → 860 |
| q19 | 656,960,736 → **619,202,400** | −5.7% | 1081 → **879** (−18.7%) | 1083 → 879 |

The other w1profile rungs are byte-identical.

**Tests.** `PartitionedTopKHiddenColumnTests` has three parts:
- plan-shape tests: bare selected keys (one of them aliased) are reused, while
  computed and unselected keys stay hidden and a repeated key shares one column;
- a differential PBT (600 cases, structural and typed): a query with bare keys must
  emit, tick for tick, exactly what the same query emits with every key spelled as
  a computed identity (`c + 0`, `LOWER(h)`). The computed form keeps the old
  hidden-column shape, which is asserted. The domain covers aliased, reordered and
  partially-selected select lists, NULLable and VARCHAR keys, multi-key PARTITION
  BY / ORDER BY, and ROW_NUMBER / RANK / DENSE_RANK.

Three mutations were caught, each after a clean rebuild:
- reusing the wrong visible column (24 TOP-K tests fail);
- a stale index for kept hidden columns after a removal (caught only by the new
  tests);
- reusing any column regardless of identity (26 fail).

**Snapshot compatibility: old snapshots do not restore, and no alias can fix that.**
Tested cross-process, writing with the old binary and reading with the new one, on a
q18-shaped query:
- **Structural path:** the plan fingerprint changes. The two identity projections
  are gone, so the operator list and every operator's position differ, and the
  TOP-K's persisted row is 4 columns instead of 6. `IFingerprintAlias` substitutes
  one operator's type in place. It cannot express removed operators, shifted
  indices or a different payload shape. Restoring an old snapshot would need a
  compile-time compatibility switch that keeps the duplicate columns. That switch
  has not been built. Old→old and new→new restore fine.
- **Typed path: pre-existing, and broader than this change.** A typed-path snapshot
  never restores in another process, *even with the same binary*. The plan
  fingerprint hashes `Type.FullName` of each operator, and the typed row and key
  structs are emitted as `TypedRow_<random GUID>` (`TypedRowEmitter.BuildTypeFor`).
  Each process therefore computes a different fingerprint. In-process round trips,
  which is what every snapshot test does, pass because the emitter caches the
  types. So §4A's "pre-arc snapshots fail" risk, and §4G's legacy→linear alias,
  only ever held in-process on the typed path. Making typed snapshots portable needs
  a deterministic emitted name (e.g. a hash of the field-type fingerprint) or a
  fingerprint that hashes schemas instead of CLR names.

  **Fixed (2026-10-29):** `TypedRowEmitter` now names each struct
  `TypedRow_<XxHash3 of its column types>`, so the same schema gets the same name
  in every process (definition is serialised under a lock, because a fixed name
  can't be defined twice). `TypedSnapshotCrossContextTests` writes a snapshot from
  a second `AssemblyLoadContext`, which has its own emitter statics, and restores it
  in the default one. With the old GUID names restored, it fails with exactly
  "plan fingerprint mismatch". (The context must not be collectible: a collectible
  copy of the engine slowed the whole suite from ~17 s to ~13 min.) Typed snapshots
  written before the fix carry GUID-based fingerprints and stay unrestorable, as
  they always were outside their writer process.

**Recommendation: ship.** It is a deterministic memory and allocation win on all
three queries with identical output. Its compatibility cost is confined to
structural-path snapshots of windowed TOP-K queries, which (like every
operator-shape change) need a re-snapshot. **Shipped**, with that break accepted.

## 4B. Variant B — COUNT(DISTINCT) and MIN/MAX sub-state (`exp/state-B-distinct`)

**Built** (`TypedCompactAggregatorState.cs`, default-on via `CompactAggregatorStateMode`,
old aggregators kept as the fallback):

- **Shared unboxed COUNT(DISTINCT).** At compile time, COUNT(DISTINCT) calls whose
  argument is a column `x` or its FILTER lowering `CASE WHEN p THEN x END` are grouped
  by `x`; each group becomes one `TypedSharedCountDistinctAggregator<TIn,T>` whose
  per-group state is one open-addressing table (`DistinctValueTable<T>`: linear
  probing, backward-shift delete, Fibonacci-mixed hash) from the **unboxed** value to
  k `int` row counters. q15/q16 go from 8 `Dictionary<object,long>` per group to 2
  tables (bidder ×4, auction ×4). `x` is extracted once per row, and the post-delta
  weight is probed once per row per table (8 → 2 trace probes), only when some member's
  predicate holds. The semantics are unchanged: it still counts positive-weight row
  transitions, so incremental ≡ batch stays exact. The composite gained multi-result
  units (`ResultCount` / `UpdateInto` + a slot map).
- **MIN/MAX as (best, count).** The state is the extreme value plus the number of
  positive rows that hold it. If a tick retracts the last holder (or is inconsistent),
  the aggregator rescans the post-delta multiset once at the end of the tick, which
  reads the trace the operator already keeps.

**Results** (1M events, batch 10k, W=1, clean rebuild; baseline = `910fa89`):

| query | live bytes base → variant | ratio | B/ev base → variant |
|---|---|---|---|
| q15 | 117,953,664 → 102,895,400 | 1.15× | 1015 → 918 (decay) |
| q16 | 248,180,312 → 199,725,952 | 1.24× | 1155 → 1043 (decay) |
| q4 | 491,567,576 → 331,349,544 | **1.48×** | 2222 → 2116 (w1profile); 1396 → 1291 (decay) |

The other w1profile rungs are byte-identical (q0/q1/q2/q22/q3/q20/q9/q18/q19).

**Where q15/q16's bytes actually are.** To isolate the distinct sub-state, I stubbed
it out (COUNT(DISTINCT) returns 0, no state; measurement only). Without it, q15 is
98.3 MB and q16 is 181.7 MB. So at baseline the distinct state is only **19.6 MB
(17%) of q15** and **66.5 MB (27%) of q16**. The variant cuts it to ~4.6 MB and
~18 MB (4.3× and 3.7×). The remaining ~95% of q15 is the aggregate operator's
**trace**: every bid row of the day, kept so `after.WeightOf(row)` can answer
positivity transitions. Further aggregator-local work cannot produce a large
multiple on q15/q16. That needs a **trace-free** aggregate for composites made only
of COUNT(*) and COUNT(DISTINCT). Such an aggregate counts *weights* per (value,
member), not positive rows. That is equivalent whenever the integrated group has no
negative weights (true for every well-formed SQL table), but it is not identical on
transient negative-weight Z-sets. It would also need persisted aggregator state,
because there is no trace to fold on restore. Projected size: ~5 MB for q15 (~20×).
That is an operator-level change, so it is listed as follow-up, not built.

**Risks.** The MIN/MAX cost model changes. A workload that keeps retracting the
current extreme (sliding windows) now pays O(group) per such tick instead of
O(log n) per row; `CompactMinMax = false` restores the sorted set. Values that
compare equal but are not `Equals` (Decimal128 1.0 vs 1.00) count as one holder.

**Tests.** `CompactAggregatorStateTests` runs a 3-way differential: compact typed,
fallback typed and structural must integrate to the same view after every tick of
random insert/delete streams. The streams include duplicated values, NULLs, a
VARCHAR argument, a non-column argument, FILTERed MIN/MAX and biased extreme
deletes. The test ends with a batch recompute. It also runs a 200k-op churn test of
`DistinctValueTable` against a `Dictionary` oracle. Full suite: 2379 passed, 1
skipped, 0 failed.

### 4B.1 Ship decision — compact MIN/MAX is OFF by default (superseded by §4B.2: the hybrid ships on)

`dotnet run -- minmaxslide [groups] [window] [perTick] [ticks] [reps]` keeps a
source-maintained sliding window over `SELECT g, MIN(v), MAX(v) … GROUP BY g`:
each tick inserts `perTick` rows per group and deletes the oldest, retracting
the current MIN on every tick. Serialized, idle machine, steady-state reps:

| groups × window, change/tick | legacy | compact | |
|---|---|---|---|
| 100 × 10,000, 10 | 1.46–1.53 ms/tick | 16.1 ms/tick | **11× slower** |
| 10 × 100,000, 10 | 0.62 ms | 15.7 ms | **25× slower** |
| 1,000 × 1,000, 1 | 2.7–3.0 ms | 14.2 ms | **5× slower** |
| 100 × 10,000, 1,000 | 62.5–63.3 ms | 52.8–53.1 ms | 1.2× faster |

Rescan-on-loss only pays when a tick replaces ~10% of a group. The COUNT(DISTINCT)
compaction (the q15/q16 +35–38% of §6) ships **on**; `CompactMinMax` ships
**off**. Follow-up: a hybrid that starts at (best, count) and promotes a group to
a sorted structure the first time its extreme is retracted. That keeps q4's
append-only −33% and costs a sliding window one rescan per group.

### 4B.2 Hybrid MIN/MAX — promote on first retraction (`exp/state-B2-minmax`)

**Built.** The MIN/MAX state is now a three-way switch,
`CompactAggregatorStateMode.MinMax` (`MinMaxStateKind`): `Legacy` (Dictionary +
SortedSet), `RescanOnly` (§4B.1) and `Hybrid` (**the default**). A hybrid group
starts as (best, count). The first tick that retracts its best (or finds it
inconsistent) does one rescan of the post-delta group into a
`SortedValueCounts<T>`, and the group keeps that state from then on.

`SortedValueCounts<T>` is a chunked sorted array: a short array of chunks, each
holding up to 128 inline `(T value, long rows)` entries. An insert or remove is
a binary search plus a memmove inside one chunk, and MIN/MAX is read off the
ends. A full chunk splits in half, except that appending past the top or
prepending below the bottom starts a new chunk. Without that rule, a sliding
window leaves every chunk half full: the in-situ cost was 41 B/value, and it is
21 B/value with the rule. A chunk under 32 entries merges with a neighbour when
the two fit in half a chunk. The bulk build sorts into chunks filled to 3/4
(with a reused per-thread scratch buffer). Values are keyed by `CompareTo`, so
the Decimal "equal but not Equals" behaviour is the same as (best, count).

**Demotion:** only when the group empties. Losing the extreme once is the
evidence that it will be lost again, and a wrong demotion costs another
O(group) rescan. Restore folds a fresh state from the trace, so a restored group
starts compact and re-promotes on its next loss. Nothing new is persisted.

**`minmaxslide`** (ms/tick; serialized, other agents' builds paused; one process
runs all three arms back to back; reps 0/1/2, where rep 0 is legacy's JIT
warm-up). Live heap is after a compacting GC; the trace is the same in every
arm.

| shape (groups × window, change/tick) | legacy | rescan-only | **hybrid** | hybrid/legacy (reps 1–2) | live MB L / R / H |
|---|---|---|---|---|---|
| 100 × 10k, 10 | 2.18 / 1.49 / 1.54 | 16.7 / 16.2 / 16.2 | **1.44 / 1.25 / 1.22** | 0.80–0.84× | 231 / 37 / 58 |
| 10 × 100k, 10 | 0.89 / 0.62 / 0.63 | 16.0 / 15.7 / 15.8 | **0.76 / 0.68 / 0.67** | 1.06–1.10× | 223 / 39 / 61 |
| 1000 × 1k, 1 | 3.67 / 3.17 / 3.22 | 14.8 / 14.1 / 14.1 | **2.08 / 1.68 / 1.66** | 0.52–0.53× | 246 / 42 / 63 |
| 100 × 10k, 1000 | 68.6 / 63.6 / 64.4 | 55.9 / 53.9 / 53.5 | **50.9 / 49.2 / 49.3** | 0.77× | 284 / 90 / 107 |
| append 100 × 10k, 10 | 1.66 / 1.07 / 1.09 | 0.89 / 0.85 / 0.86 | **0.87 / 0.85 / 0.85** | = rescan-only | 299 / 76 / 76 |
| append 1000 × 1k, 1 | 3.00 / 2.26 / 2.27 | 1.36 / 1.37 / 1.35 | **1.35 / 1.35 / 1.37** | = rescan-only | 319 / 86 / 86 |

Two earlier serialized passes agree to within ~10%. Per promoted value, the
hybrid costs **~21 B** (for example (57.7 − 37.2) MB / 1M values), against
**~97 B** per value per aggregator for legacy. The one-time promotion shows as
allocation (10 × 100k: 2310 B/row-change against 924 B for legacy, amortised
over 300 ticks).

**q4** (1M events, batch 10k; hybrid ≈ rescan-only, so q4 groups essentially
never promote):

| generator | live bytes legacy → rescan-only → hybrid | B/ev (w1profile) L → R → H |
|---|---|---|
| default | 490,412,232 → 331,255,440 → **331,280,896** (−32.4%) | 2222 → 2116 → **2116** |
| `NEXMARK_GEN=feldera` | 422,394,528 → 282,789,352 → **282,861,560** (−33.0%) | 1978 → 1888 → **1888** |

**Tests.** `CompactAggregatorStateTests` now has these parts:

- a 4-way differential: hybrid, rescan-only, legacy and structural;
- sliding-window streams that promote groups mid-run (FILTERed and computed
  MIN/MAX, weight-2 rows, NULLs, emptied groups that demote);
- a snapshot/restore test taken after promotions, which must continue
  tick-for-tick like an uninterrupted legacy run;
- a direct check that append-only never promotes and the first loss does;
- a churn test of `SortedValueCounts` against a `SortedDictionary` oracle
  (random, ascending slide and descending slide).

Full suite: 2394 passed, 1 skipped. Mutations caught: promotion disabled,
bulk-build duplicate counts dropped, wrong end emitted, and a split off-by-one.
The defensive "remove missed → rebuild" path is unreachable from well-formed
transition folds, so that mutant is equivalent.

**Decision: ship the hybrid as the default.** It meets the gate on every
sliding shape (≤ 1.10×, and faster than legacy on three of four) and matches
rescan-only on append-only. It also keeps q4's −32/−33%. Legacy and
rescan-only stay selectable.

### 4B.3 Round 3 — the structural path (`exp/state-r3-structural`)

ivm-bench runs the **structural** program path (`SqlProgram.Compile`, default options,
W=1; `TypeEligibleProgramViews` is off). Census of its 18-output program
(`dotnet run -- statecensus program.json`):

| variant | structural path today | ivm-bench hits |
|---|---|---|
| A compact TOP-K | selected (`StatefulOperators.PartitionedTopK`, `StructuralRow` key) | **0** — every RANK/ROW_NUMBER is rank-in-output (`PartitionedRankOp`, 13 in 6 views) |
| B compact COUNT(DISTINCT) | typed only (`TypedPlanCompiler.cs:2383`); structural keeps `Dictionary<object,long>` | 5 aggregates in 5 gold views |
| B2 hybrid MIN/MAX | typed only (`TypedPlanCompiler.cs:2061`) → **ported here** | 5 aggregates (watches, customer_concentration, daily_market_pulse, market_volatility, trade_volume_stats) |
| G trace-free linear | typed only (`TypedPlanCompiler.cs:1889`) | **0** under the typed rules (every candidate has a NULLable SUM); 3 if NULLable SUM were admitted (fact_cash_balances, broker_performance, customer_concentration) |

**Built:** `SqlMinMaxHybridAggregator` (`StructuralCompactAggregatorState.cs`): (best, count)
per group; the first loss of the best rescans the post-delta group once into the legacy
`Dictionary`+`SortedSet` state; demotes when the group empties. Default-on;
`StructuralCompactAggregatorStateMode.MinMax` / `DBSPNET_STRUCT_MINMAX=legacy` select
the old state. The batch evaluator keeps the legacy aggregator (it is the oracle).

`dotnet run -- structstate` (deterministic; live bytes after full GC, allocated bytes):

| shape | legacy | hybrid |
|---|---|---|
| watches-like (100k groups, 130k rows, MIN+MAX of CASE-filtered TIMESTAMP) | 320,988,672 live / 235,974,624 alloc | **218,082,904 (−32%) / 186,448,528 (−21%)** |

Tests: `StructuralMinMaxHybridTests` (hybrid ≡ legacy every tick incl. negative-weight
inputs, batch oracle, promotion/demotion unit test). Mutations caught: loss never
detected, wrong end, no demotion, duplicate holders not counted; "promotion keeps
non-positive rows" is equivalent (the fold skips them anyway).

**Wall-clock gate** (`minmaxslide … structural`, serialized, idle, `caffeinate`; reps 1–2
of 3, ms/tick; the shapes of §4B.2):

| shape (groups × window, change/tick, ticks) | legacy | hybrid | hybrid/legacy | live MB L → H |
|---|---|---|---|---|
| 100 × 10k, 10, 300 | 2.29 / 2.32 | 2.41 / 2.38 | 1.03–1.05× | 343 → 246 |
| 10 × 100k, 10, 300 | 1.37 / 1.38 | 1.71 / 1.70 | 1.24× | 334 → 242 |
| 10 × 100k, 10, **1500** | 1.36 / 1.37 | 1.42 / 1.42 | **1.04–1.05×** | 334 → 242 |
| 1000 × 1k, 1, 300 | 4.36 / 4.24 | 3.51 / 3.41 | 0.80× | 357 → 255 |
| 100 × 10k, 1000, 50 | 78.0 / 79.1 | 64.4 / 65.2 | 0.82× | 372 → 275 |
| append 100 × 10k, 10, 300 | 1.94 / 1.94 | 1.69 / 1.72 | 0.88× | 438 → 215 |
| append 1000 × 1k, 1, 300 | 2.73 / 2.71 | 2.12 / 2.12 | 0.78× | 456 → 223 |

The 1.24× on few large groups is the one-time promotion rescan (100k values per group),
amortised over only 300 ticks; at 1,500 ticks it is 1.04–1.05×. Every shape meets the
≤1.10× gate at steady state, and live state falls 28–51%. **Shipped default-on.**

## 4G. Variant G — trace-free linear aggregate (branch `exp/state-G-lineragg`)

**Built.** `LinearAggregateOp<TKey,TValue,TOut>` (Core) over a new
`ILinearAggregator<TValue,TOut>` (fold a delta / result / is-zero / write / read).
Per group it keeps only `{summed row weight, linear state, last emitted value}` in
one `Dictionary<TKey, struct>`. It has **no input trace**. The typed compiler
(`TypedPlanCompiler.LinearAggregate.cs`) selects it when **every** aggregate in the
GROUP BY is one of:

- `COUNT(*)`, `COUNT(x)`, or a FILTERed count (`COUNT(CASE WHEN p THEN 1 END)`);
- `SUM(x)` over any argument, NULLable or not (BIGINT, DOUBLE or DECIMAL accumulator;
  NULLable since §4G.1);
- `AVG(x)` over any argument, NULLable or not, DOUBLE or DECIMAL;
- `COUNT(DISTINCT x)`, FILTERed or not, sharing per-base tables like variant B. The
  base value must be blittable or `Utf8String`.

Anything else keeps `IncrementalAggregateOp`: MIN/MAX, sketches, STDDEV, the spine
trace family, or a snapshot codec without keyed
blobs. `LinearAggregateMode` is default-on, uses the inverse thread-static flag, and
`DBSPNET_LINEAR_AGG=off` turns it off. The structural path is unchanged, so the
program path and LATENESS still compile to the trace-backed operator.

Nexmark coverage: q15 and q16 compile completely to the new path. In q4, only the
outer `AVG(final) GROUP BY category` does. Its inner `MAX(price)` keeps the trace.

**Semantics.**

- A group is present while its summed row weight is non-zero. That is the
  composite's `SumWeights() != 0` gate, so retract/insert pairs are emitted exactly
  as today, including when a group appears or disappears.
- State is dropped only when the weight is 0 **and** every integer accumulator has
  cancelled. A weight-0 group whose sums have not cancelled therefore survives, as
  it does in the trace.
- COUNT(x) counts non-NULL weight. AVG returns NULL when the summed non-NULL weight
  is 0; this was already the rule in the trace-backed aggregators.
- FILTER is evaluated per row.
- BIGINT SUM uses the same `checked(sum + v*w)` per delta row. DECIMAL sums keep
  the same Int256 accumulator and narrow at output.
- **SUM over a NULLable argument** was excluded at first, because the trace-backed
  rule is "no non-NULL row has non-zero weight", a per-row fact: rows `{+1 x=4,
  −1 x=2}` sum to weight 0 but give 2. The arbitrary-Z-set PBTs (`RandomQuery*`)
  caught this. §4G.1 makes it linear under summed-weight presence.
- **COUNT(DISTINCT) corner, documented and tested.** G counts values whose
  *summed* weight is positive. The trace-backed operator counts values held by
  *some positive-weight row*. The two agree whenever no integrated row weight is
  negative, which holds for every well-formed table. They differ otherwise:
  `{+1 (id1,v=5), −1 (id2,v=5), +1 v=7}` gives G 1 and trace 2.
  `Corner_CountDistinctUnderNegativeRowWeights_*` pins this.
- **Pre-existing bug found.** The trace-backed `TypedAvgDecimalAggregator`
  (AVG of a DECIMAL NOT NULL argument) divides by its count on every Update, so
  emptying a group throws `DivideByZeroException`. G only asks for a result while
  the group is present. This is pinned by a test, and the old aggregator is left
  unfixed: it lives in a file another branch is editing.

**Persistence.** Each group's (weight, state) is written verbatim, double bits
included, as keyed blobs in `lagg.arrows`. `TypedIndexedZSetTraceCodecAdapter` now
forwards keyed blobs; it did not before, so the typed path never persisted
aggregator state. G implements `IFingerprintAlias` → `IncrementalAggregateOp`. A
legacy snapshot restores by folding its `trace.arrows`, which is exact for integer
and DECIMAL sums. For DOUBLE sums it carries the same §7.2 re-fold drift that the
trace-backed typed op already had on its own restores.

**Frontier GC.** The op drops groups whose monotone key is below the frontier, as
the current operator does. The typed path passes no frontier today, because
LATENESS forces the structural compile.

**Results** (1M events, batch 10k, W=1). The baseline is `c53e782`, built clean
from the same tree. The `=off` arm reproduces it: B/ev is identical and live bytes
are within 0.15%. B/ev comes from the decay report, and q4 also from `w1profile`.
The other `w1profile` rungs are byte-identical.

| gen | query | live bytes base → G | ratio | B/ev base → G (decay) | q4 w1profile |
|---|---|---|---|---|---|
| feldera | q15 | 78,938,344 → **19,066,752** | **4.14×** | 933 → 819 (−12%) | |
| feldera | q16 | 181,330,824 → **52,219,648** | **3.47×** | 1060 → 929 (−12%) | |
| feldera | q4 | 422,340,008 → 419,943,632 | 1.006× | 1317 → 1311 | 1978 → 1973 |
| legacy | q15 | 102,883,376 → **18,375,616** | **5.60×** | 918 → 756 (−18%) | |
| legacy | q16 | 199,963,704 → **40,426,576** | **4.95×** | 1043 → 850 (−19%) | |
| legacy | q4 | 490,380,720 → 487,350,848 | 1.006× | 1396 → 1392 | 2222 → 2217 |

The stateless q0/q2 retain 11–13 MB in the same harness. So q15's aggregate
state is now about 6–8 MB, close to B's ~5 MB projection; the old figure was about
66–90 MB. q4 barely moves, because its outer AVG trace holds only one MAX row per
auction. q4's state is the join plus the inner MAX trace.

**Tests.** `LinearAggregateTests` has 14 cases:

- `Linear_EqualsTraceBacked_EveryTick` checks output deltas tick by tick on 4 seeds:
  18 aggregates, NULLs, FILTERs, VARCHAR/DECIMAL/expression DISTINCT, duplicate
  rows, and groups emptied and refilled. Doubles are compared by bit pattern.
- `NonDistinctMembers_*` checks arbitrary ±1 streams with negative weights.
- `RestoreMidStream_*` covers linear→linear restores, with floats, and
  legacy→linear restores through the alias.
- A selection/fallback test, the two corner tests, and a churn and round-trip
  test of `DistinctWeightTable`.

Six deliberate mutations were all caught:

- the DISTINCT sign test;
- no retraction when a group empties;
- persisted double drift;
- ignoring a FILTER;
- dropping state at weight 0, caught only by the negative-weight PBT;
- the legacy restore not folding.

Full suite: 2399 passed, 1 skipped.

**Risks.**

- The COUNT(DISTINCT) negative-weight corner above.
- ~~A NULLable SUM keeps the trace~~ — resolved by §4G.1.
- Per-group state is an `object[]` of cells, which is fine for few, large groups.
  Many-group linear aggregates (q12-like) have not been measured.
- Wall-clock has not been measured under contention. The decay ev/s moved up
  (q15 2.1M → 2.9M, q16 1.6M → 2.2M), but that is noise-grade evidence.

**Recommendation: ship.** It is default-on, with the trace-backed fallback kept.
Before quoting a throughput number, run a serialized 10M decay on q15/q16 to check
whether the decay curve flattens. Follow-ups:

1. ~~Make nullable SUM linear~~ — done, §4G.1.
2. Give the structural path the same operator.
3. Fix the `TypedAvgDecimalAggregator` divide-by-zero.

### 4G.1 SUM over a NULLable argument (summed-weight presence, `exp/state-r3-looseends`)

**Rule (user-approved).** SUM over a NULLable argument is NULL iff the summed weight
of the group's non-NULL-argument rows is 0. That is AVG's rule, and every linear SUM
unit already tracked that weight (`N`) and persisted it in its keyed blob, so the
change is only the removal of the compile-time exclusion. Legacy→linear restore
through the alias folds `trace.arrows`, which rebuilds `N` exactly.

**Divergence.** The rule equals the trace-backed one whenever no integrated row
weight is negative. Otherwise it differs: `{+1 (id1, v=4), −1 (id2, v=2), +1 (id3,
v NULL)}` gives the trace 2 and G NULL, for all three accumulators. This is the same
accepted class of corner as G's COUNT(DISTINCT).
`Corner_SumNullableUnderNegativeRowWeights_FollowsSummedNonNullWeight` pins it.

**Tests.**
- The `RandomQuery*` PBTs (incremental, optimized, pooled, pruned) now skip only
  the cases where the query SUMs over the NULLable table `n` **and** the integrated
  `n` holds a negative row weight (`RandomQuery.OutsideNullableSumDomain`).
- `LinearAggregateTests` covers BIGINT, DOUBLE and DECIMAL nullable SUM in the
  every-tick differential and in both restore directions.
- A scripted all-NULL → value → NULL group test runs with and without a
  snapshot/restore before every tick.

Mutations, all caught after a clean rebuild:
- BIGINT SUM never NULL (10 tests fail, including 4 `RandomQuery*` PBTs);
- NULL rows counted into `N` (10 fail);
- DOUBLE SUM never NULL (5 fail);
- DECIMAL SUM never NULL (6 fail);
- `N` not persisted (3 restore tests fail).

**Nexmark: nothing newly eligible.** Every Nexmark column is NOT NULL, and the
compiled aggregate operator types for all queries are identical before and after.
The only SUM is q17's, and that group also has MIN/MAX.

**Synthetic** (1M rows, batch 10k, 20k groups, ~20% NULL per argument, ~10% deletes;
`SELECT g, COUNT(*), SUM(v BIGINT), SUM(x DOUBLE), SUM(d DECIMAL) GROUP BY g`; B/ev
covers push + step):

| arm | operator | live bytes | B/ev |
|---|---|---|---|
| off (= behaviour before §4G.1) | IncrementalAggregateOp | 193,686,448 | 2157 |
| on | LinearAggregateOp | **25,084,480 (7.7×)** | **1817 (−16%)** |

## 5. Cross-cutting findings

### 5.1 Our Nexmark generator under-represents strings (found by variant E)

`NexmarkGenerator` emits constant `extra` values (`"b"`/`"a"`/`"p"`) and 4 fixed
bid URLs. Feldera's generator (`../feldera/crates/nexmark/src/generator/strings.rs`
`next_extra`, `bids.rs`) pads `extra` with random text toward
`avg_bid_byte_size` and builds URLs with per-channel ids. Consequences:

- **String-interning wins measured on our data are inflated.** Variant E
  (`exp/state-E-strings`, `e60bf95`) cut q20 live bytes −44% / objects −88% and
  q19 −28% / −53%, almost all from columns that are high-cardinality in Feldera's
  data. Interning stays sound and bounded (it switches itself off on
  high-cardinality columns), so it is still worth keeping for genuinely
  low-cardinality columns. But it can't be credited against Feldera until it is
  re-measured on faithful data.
- **Every prior DbspNet-vs-Feldera Nexmark ratio ran on lighter rows on our
  side.** That is not a fair comparison for string-heavy queries (q0, q18–q22).
- **Done (`4ed465e`, merged to main):** `NEXMARK_GEN=feldera` selects a
  Feldera-faithful port (hot auction/bidder/seller ratios, 10k channels, padded
  `extra`, Feldera's price and expiry rules). It matches Feldera's own generator
  to sampling noise at 1M events (avg string bytes/row 192.0/452.0/119.5 vs
  192.1/452.2/119.5; distinct (bidder,auction) 291,513 vs 291,295). Legacy stays
  the default and is byte-identical. `genstats [events] [legacy|feldera]` prints
  the statistics. On faithful data q18 retains **44% less** (hot keys collapse
  partitions 917k → 291k), q19/q20 retain more (longer strings), and q16's
  output grows 15×. **All Phase 0 baselines must be re-taken on this profile.**

### 5.2 The aggregate trace dominates aggregate state (found by variant B)

Variant B (`exp/state-B-distinct`, `7db2dec`/`8ba8d46`) compacted the
COUNT(DISTINCT) and MIN/MAX sub-states: q4 live state −33% (1.48×), q15 1.15×,
q16 1.24×. q15/q16 moved little because their distinct sub-state was only
17% / 27% of live bytes. **The rest is `IncrementalAggregateOp`'s input trace**:
every input row is kept so aggregators can read post-delta weights.

The trace is only *needed* by non-linear aggregates (MIN/MAX, and
COUNT(DISTINCT) through its value counts). **Linear aggregates (COUNT, SUM, AVG)
are exact from running weight sums with no trace at all** — DBSP's linear
aggregation, which Feldera implements as a separate operator. Candidate **G**:
elide the trace when every aggregate in a group is linear, or keep per-value
weight counts in place of the trace for COUNT(DISTINCT). B projects q15 → ~5 MB
(~20×). Costs: restore needs persisted aggregator state, and frontier GC needs a
substitute for the trace. It is operator-level work, separate from C/D's
representation of the trace.

B's open risk: MIN/MAX now rescans the group when the current extreme is
retracted, instead of O(log n) per row. Validate on a retraction-heavy
(sliding-window) workload before shipping.

## 6. Phase 1 wall-clock (serialized, idle M4 Pro, 10M events, W=1, legacy generator, 2 reps)

Mean ev/s is the harmonic mean over five 2M-event segments; reps agreed within ~5%.

| variant | query | baseline ev/s | variant ev/s | live state @10M |
|---|---|---|---|---|
| **A** compact TOP-K (`exp/state-A-topk` f5592aa) | q18 | 561k / 650k | **1,234k / 1,197k (~2.0×)** | 18,512 → 4,089 MB |
| | q19 | 919k / 909k | **1,434k / 1,523k (~1.6×)** | 8,189 → 4,993 MB |
| **B** compact agg state (`exp/state-B-distinct` 7db2dec) | q15 | 940k / 946k | **1,315k / 1,294k (+38%)** | 762 → 582 MB |
| | q16 | 705k / 721k | **955k / 986k (+35%)** | 2,160 → 1,586 MB |
| **C** FlatHash (`exp/state-C-flathash` 682ce09) | q4 | 1,592k / 1,614k | 1,353k / 1,386k (**−15%**) | 4,501 → 2,782 MB |
| | q20 | 2,049k / 1,970k | 1,773k / 1,739k (−13%) | 5,192 → 3,414 MB |
| | q9 | 1,841k / 1,803k | 1,881k / 1,806k (≈) | 3,315 → 2,691 MB |
| | q3 | 27.4M / 26.7M | 31.3M / 33.0M (+20%) | 100 → 30 MB |
| **D** Sorted F=16 (`exp/state-D-sorted` f788fe0) | q4 | as above | 1,454k / 1,461k (−9%) | 4,501 → 2,434 MB |
| | q20 | | 1,817k / 1,709k (−12%) | 5,192 → 3,164 MB |
| | q9 | | 1,680k / 1,692k (−7%) | 3,315 → 2,632 MB |
| | q3 | | 20.7M / 22.3M (−21%) | 100 → 25 MB |

(D at F=8 is within noise of F=16 on q4/q9 and worse on q20.)

**Reading.**

1. **A and B are real wins, in both time and memory.** They remove *objects*
   (A: ~15–20 per partition → ~0 plus strings; B: boxed distinct keys), and
   throughput follows.
2. **C and D shrink join/aggregate state 35–48% at 10M, but neither is faster.**
   Both lose 9–15% on q4/q20. FlatHash also decays *more* within the run (q4
   1.52M → 1.20M), presumably from chasing in-group links across pages. D is
   the flattest curve on q4 (1.42–1.51M across all segments) but starts below
   Flat. The hash-of-hashes container is not what costs time on joins.
3. **q20's in-step GC pause is unchanged (18–34%) under C and D.** D measured
   q20's live object count dropping only 9%: what remains is one `byte[]` per
   string cell. GC pause follows object count, not bytes, and strings now
   dominate it. **String storage in state (an arena, not interning — see §5.1)
   is the remaining GC lever for joins.**
4. q15/q16 keep their decay *shape* under B (the curve is lifted, not
   flattened). The remaining lever is candidate G (§5.2): a trace-free linear
   aggregate.

**Decisions proposed:** ship A (after a snapshot-fingerprint alias) and B
(after a retraction-heavy MIN/MAX check). Keep C/D opt-in only, as a memory
option, not a speed one. Next builds: a VARCHAR arena in state, then G.
All of the above to be re-confirmed on `NEXMARK_GEN=feldera`.

## 7. Round 2 wall-clock (serialized, idle, `caffeinate -i`, 10M events, W=1, `NEXMARK_GEN=feldera`, 2 reps)

Each pair is one build with its variant switch off/on. Mean is the harmonic mean
of five 2M-event segments.

| variant | query | off: segments (k ev/s) → mean | on: segments → mean | live state |
|---|---|---|---|---|
| **G** linear agg | q15 | 2001 1368 979 795 816 → **1,056–1,119k** | 2979 3064 3067 3081 3087 → **2,931–3,055k (~2.8×, flat)** | 846 → 75 MB |
| | q16 | 902 402 257 183 219 → **285–286k** | 2306 2300 2318 2437 2290 → **2,295–2,329k (~8×, flat)** | 1,809 → 321 MB |
| **B2** hybrid MIN/MAX | q4 | 2111 2012 2073 1906 1537 → **1,885–1,902k** | 2320 2106 2109 2252 2221 → **2,141–2,198k (+14–16%, flat)** | 3,893 → 2,556 MB |
| **H** ingest arena | q18 | 1,271–1,371k | 1,347–1,393k | 6,438 → 5,021 MB |
| | q19 | 1,359–1,488k | 1,397–1,453k | 6,140 → 4,736 MB |
| | q20 | 1,974–2,104k | 2,123–2,124k | 5,706 → 4,686 MB |

**Reading.**

1. **G removes the decay on q15/q16 outright.** The old operator loses 60–80%
   of its throughput across the run on Feldera-faithful data; the trace-free
   operator is flat from the first segment to the last. Shipped `a55b0e4`.
2. **B2 removes q4's late-run sag** (last segment 1.54–1.57M → 2.08–2.22M) and
   cuts its state 34%. Shipped with this section.
3. **H does not pay at W=1.** It removes 82–88% of live objects on q18–q20, and
   worst-segment pause drops (q19 22% → 11%, q20 18% → 10%), but mean throughput
   moves −2% to +8%, inside run-to-run spread. With its retention risk (§4H: 1%
   survival pins 20× the strings), **H is not shipped.** Revisit if a W>1 or
   memory-bound workload makes object count the limit. At W=1, GC pause is no
   longer the dominant cost for these queries after A.
4. **Lesson, superseding §6's "throughput follows object count":** throughput
   follows *work per row*. A, G and B2 removed per-row work (per-partition
   containers, the aggregate trace, the duplicated MIN/MAX structure) along with
   the objects; C, D and H moved bytes or objects without removing work.

**Measurement note.** A first run of this round (without `caffeinate`) showed
15-minute "stalls" and isolated ~0.54M ev/s segments. A watchdog proved they were
macOS idle sleep at ~2–3 am: its 5 s `sleep` loop counted 15 s across 14.5
wall-minutes. The table above is the re-run with sleep prevented. Run every
unattended timing benchmark under `caffeinate -i`.

## 8. Round 3 — are A/B/G/B2 selected at W>1? (`exp/state-r3-parallel`)

**Yes, all four, in every replica — no fix was needed.** `TypedPlanCompiler.TryCompileParallel`
runs the same `TryCompileNode` per replica inside `ParallelCircuit.Build`, which builds
replicas sequentially **on the calling thread**, and every switch (`PartitionedTopKStateMode`,
`LinearAggregateMode`, `CompactAggregatorStateMode.CompactDistinct/MinMax`) is read at
compile time, with no `Workers` condition. So the thread-static opt-outs reach the replicas
too. Evidence: `r3par` (below) prints each replica's state operators. `ParallelStateSelectionTests`
asserts selection and each opt-out per replica at W=1/2/4, and checks parallel-on ≡ single-off
on every tick plus ≡ a one-tick batch for q4/q16/q18/q19 shapes. Mutating the G gate to
`ctx.Workers == 1` fails 4 cases. Caveat: the *structural* parallel path
(`PlanToCircuit.TryCompileParallel`) and the program path get only A's container (no unboxed
key, no B/G/B2), by design.

**Instrument.** `dotnet run -c Release -- r3par [events] [batch] [W,W,..] [q..]` compiles
through `TryCompileParallel` for every W (W=1 included). It reports live bytes after a forced
full GC and B/ev from `GC.GetTotalAllocatedBytes(precise)` around push+step only, so
worker-thread allocation counts. It also gives an order-independent output digest, checked
equal across W. Env A/B: `DBSPNET_TOPK_STATE=legacy`, `DBSPNET_LINEAR_AGG=off`,
`DBSPNET_MINMAX_STATE=legacy`, `DBSPNET_COMPACT_DISTINCT=off` (new). Determinism across
two processes: B/ev agrees to ≤0.2 B/ev at every W (≤1.6 at q9 W=1). It is not byte-exact
like `w1profile`, but it is far inside every effect below. Live bytes agree to ≤0.5%, except
for one q18 W=1 pair (3.4%).

## 9. Round 3 — where we stand (serialized, idle, `caffeinate`, `NEXMARK_GEN=feldera`, main @ `610f184`)

### 9.1 Head-to-head against Feldera (`scripts/compare-nexmark.sh`, now on the faithful generator)

DbspNet W=N ÷ Feldera at N cores; > 1.0 = DbspNet faster. 10M events unless marked. Only
gaps beyond ~1.5× are readable (§ "How much of this to believe" in `benchmarks-comparison.md`).

| query | 1M, 14c | 10M, 14c | 10M, 8c | 10M, 4c | 2026-08-31 10M, 14c (legacy gen) |
|---|---|---|---|---|---|
| q0 | 1.14× | 0.83× | 0.85× | 0.73× | 1.00× |
| q1 | 1.02× | 0.96× | 0.70× | 0.68× | 0.97× |
| q2 | 1.22× | 0.82× | 0.73× | 0.76× | 1.06× |
| q3 | 1.35× | 2.10× | 2.24× | 2.87× | 2.79× |
| q4 | 1.15× | 1.25× | 1.17× | 0.96× | 1.19× |
| q9 | 1.76× | 2.22× | 1.96× | 2.30× | 2.36× |
| q15 | 10.71× | **11.25×** | 7.25× | 4.58× | 2.87× |
| q16 | 18.92× | **14.96×** | 12.78× | 8.49× | 7.43× |
| q17 | 10.33× | 8.66× | 6.05× | 4.04× | 6.42× |
| q18 | 0.89× | **0.86×** | 0.72× | 0.80× | **0.46×** |
| q19 | 1.00× | 1.34× | 0.99× | 0.83× | 0.95× |
| q20 | 0.83× | 1.16× | 1.12× | 1.06× | 1.52× |
| q22 | 0.67× | **0.64×** | 0.63× | 0.63× | 0.79× |

**Decay, 1M → 10M at 14 cores (absolute ev/s):**

| query | DbspNet W=14 | Feldera 14c |
|---|---|---|
| q4 | 3.70M → 3.95M | 3.22M → 3.15M |
| q9 | 3.51M → 3.62M | 2.00M → 1.63M |
| q15 | 4.26M → 4.08M | 398k → 363k |
| q16 | 5.38M → 3.88M | 285k → 259k |
| q18 | 3.32M → 2.92M (−12%) | 3.74M → 3.39M (−10%) |
| q19 | 3.37M → 3.45M | 3.36M → 2.57M |
| q20 | 3.07M → 3.43M | 3.68M → 2.96M |
| q22 | 5.32M → 5.10M | 7.94M → 7.99M |

Reading:

1. **The arc's headline goal is met at W=14.** On 2026-08-31 our stateful queries lost
   throughput 1M → 10M while Feldera's held. Now no query decays more than Feldera's does;
   q18 loses ~10–12% on both sides. (q16's 5.38 → 3.88M is the W=14 noise band; W=1 is flat,
   §9.2.)
2. **q18 went from a clear loss (0.46×) to parity (0.72–0.89×)**, and q15/q16 went from wins
   to 5–19× wins. Feldera's q15/q16 *slow down* as cores are added (799k at 4c → 363k at 14c);
   ours scale up.
3. **What still loses is stateless or near-stateless: q0/q1/q2/q22 at 0.63–0.85×**, at every
   core count. That is per-row and exchange cost, not state. It matches §8's finding that W>1
   adds ~300–550 B/ev of exchange/gather allocation that no state variant touches. **That is
   the next lever, and it belongs to the row-representation / per-row arc, not to this one.**

### 9.2 Phase 0 decay table re-taken (W=1, 10M, five 2M segments, `decay … stream`)

| query | ev/s first → last fifth | ex-pause first → last | worst pause | live state @10M | §1.2 (legacy gen, pre-arc) |
|---|---|---|---|---|---|
| q0 | 3.84M → 3.91M | flat | 8% | 13 MB | flat |
| q3 | 27.1M → 27.4M | flat | 4% | 123 MB | flat |
| q4 | 2.16M → 2.21M | flat ~2.35M | 8% | 2,510 MB | 1.43 → 1.55M, 4,501 MB |
| q9 | 1.80M → 1.87M | flat ~1.9M | 9% | 2,580 MB | 1.83 → 1.74M |
| q15 | 2.85M → 3.04M | flat | 5% | 75 MB | 1.28 → 0.82M (−36%) |
| q16 | 2.19M → 2.36M | flat | 4% | 322 MB | 0.97 → 0.66M (−32%) |
| q17 | 2.69M → 2.38M (−11%) | flat ~2.8M | 15% | 1,582 MB | (not measured) |
| **q18** | **1.54M → 1.01M (−35%)** | **1.54 → 1.13M (−27%)** | 11% | 6,433 MB | 1.02 → 0.35–0.87M, 20,537 MB |
| **q19** | 1.47–1.60M ↔ 1.01–1.12M (bimodal) | 1.59 ↔ 1.12–1.27M | 12% | 6,146 MB | 1.26 → 0.73–1.12M |
| q20 | 2.17M → 1.93M | flat ~2.3–2.4M | **22%** | 5,701 MB | 2.04 → 1.11–2.37M |
| q22 | 1.99M → 1.96M | flat | 9% | 16 MB | flat |

**The remaining decayers, ranked:**

1. **q18: the only true decayer left.** It loses 27% *ex-pause*, so this is per-row
   work or locality growing with state, not GC. 9.2M stored rows at ~700 B/row. This is
   the next target for this arc: A's per-partition `Dictionary` (§4A notes ~40 B/entry +
   1.7× slack) and three `byte[]` strings per row.
2. **q19: GC-bimodal.** Segments with a gen2 lose ~30%; segments without one run at the
   start rate. Object count, not work: strings (H/E territory) or fewer, larger objects.
3. **q20 and q17: a flat GC tax** of 15–22% pause late in the run, with ex-pause throughput flat.
   Same lever as q19.
4. Everything else is flat.

At W=1 GC pause is still a 10–22% tax on q17–q20. H (the ingest arena) removed 82–88% of
those objects but did not move W=1 mean throughput in round 2 (§7). Re-test it on q19/q20
at W=14, where Feldera's numbers sit, before reopening it.

### 9.3 Round 3 changes shipped

- **G admits SUM over NULLable arguments** (`610f184`, §4G.1): summed non-NULL weight
  decides NULL. That changes nothing on Nexmark (every column is NOT NULL), but it makes 3
  ivm-bench aggregates G-eligible once G reaches the structural path.
- **A's duplicated partition/order columns removed** (`af774fd`, §4A.1): q9 −19% live
  state, q18/q19 −19% B/ev. It breaks old structural snapshots of windowed TOP-K queries,
  which is accepted.
- **Typed snapshots restore across processes** (`c505acd`, §4A.1): emitted row types are
  named deterministically.
- **Structural hybrid MIN/MAX** (`af47f4f`, §4B.3): passes its sliding-window gate; ivm-bench's
  `watches` + four gold views.
- **W>1 selection asserted** (`58ce2be`, §8): all four variants are selected per replica.

### 9.4 Next

1. ~~**Structural G.**~~ *Demoted by §9.5: all aggregates are 6–7% of ivm-bench batch-1 step.* It was billed as the biggest ivm-bench prize: fact_cash_balances shape 1,972 →
   748 B/row. It now applies to 3 aggregates under the NULLable-SUM rule. It needs a structural
   operator plus persisted aggregator state.
2. **q18 ex-pause decay** (§9.2 item 1).
3. **W>1 per-row/exchange cost** (q0/q1/q2/q22), handed back to the row-representation arc.
4. **C/D retired** as memory-only options. Both are 9–15% slower, and no workload is
   memory-bound yet. The branches are recorded in §6 and deleted.

### 9.5 ivm-bench SF=3 batch 1 on main (`0b847c4`) — the priority changes

Local `IvmBatchProfile` (structural program path, ServerGC, DATAS off, `caffeinate`), 18-view compile
set (16 outputs + the 2 `+stored` views). Two runs per arm, alternating:

| arm | engine step | batch wall | allocated |
|---|---|---|---|
| structural MIN/MAX **hybrid** (default) | 63.4 / 63.6 s | 70.2 / 67.6 s | **84.53 GiB** |
| `DBSPNET_STRUCT_MINMAX=legacy` | 63.6 / 64.2 s | 67.9 / 69.3 s | 85.64 GiB |

- **The hybrid is time-neutral on the real workload and saves 1.1 GiB (−1.3%) of allocation.** The
  per-operator profile charged `watches`' aggregate +1.5 s under the hybrid. That is GC-pause
  attribution: the whole step is equal, and a counter probe found 549,669 promotions that rescanned
  **0 rows** each (groups emptying), 68 ms in total, with no group staying promoted. Per-operator
  ms under ServerGC is not an A/B instrument; whole-step and per-operator alloc are.
- **One view is half the batch.** `fact_market_history`'s left join (18.7 s, 25.8 GiB), the Apply
  after it (6.8 s, 12.4 GiB) and its Apply2 (2.7 s) are **~28 of 63 s of step and ~39 of 84.5 GiB**.
  Its `LEFT JOIN wrk_company_financials USING (sk_company_id)` has no temporal predicate, so each
  `daily_market` row fans out to every financials row of its company (~40M output rows,
  [[ivm-bench-stored-view-asymmetry]]). That is ~1 KB allocated per output row: wide joined
  `StructuralRow` built, then projected by the next Apply. Feldera computes the same view.
- **All 13 aggregates together are 3.9–4.5 s (6–7%).** Structural G (§9.4 item 1) is worth a few
  percent here, not the ~2.6× its per-row state shrink suggested. **Demoted.**

**Next for ivm-bench:** *(resolved by §9.6: the cause was a missing plan rewrite, not the join's per-row cost.)* The per-output-row cost of a fan-out join on the structural path. Build the
projected row directly in the join's combine (fuse the following Apply into it, as join column
pruning does for inputs), and check whether the joined row can stay unboxed. Measure with whole-step
and allocation; this is a row-construction lever, so it belongs with per-row-execution-efficiency,
not with state representation.

### 9.6 Outer-join column pruning: −37% ivm-bench batch-1 step

**Diagnosis.** `fact_market_history`'s 40M join rows were neither churn nor real output. A
one-tick-per-round experiment (all 20 sources pushed, one `Step`) left the join's output at
40,269,098 rows in that single tick, while the view integrates to 664,780 distinct rows.
The view selects no column of `wrk_company_financials`, so `LEFT JOIN … USING
(sk_company_id)` only multiplies each row's weight by its company's financials count (~60).
`PruneJoinInputs` would have narrowed the right side to its key and collapsed those rows
into one weighted row, but it handled INNER joins only.

**Built.** `PruneJoinInputs` now also prunes LEFT/RIGHT/FULL joins without a residual. An
outer join with a residual lowers through an anti-join on the whole preserved row, so it is
left alone. The narrowed join takes its schema from the old join's schema, so the padded
side stays nullable.

**Semantics.** Narrowing a preserved side is exact. Narrowing a padded side merges rows that
differ only in dead columns, and their weights sum. A key whose integrated rows have
cancelling signs then looks unmatched and gets padded. On arbitrary ±1 streams a probe found
pruned ≠ unpruned in 8–47% of cases, per query shape. It is exact whenever no integrated row
weight is negative, which holds for every well-formed table. Curt accepted this on
2026-10-29, the same envelope as §4G.1. `Corner_OuterJoinUnderNegativeRowWeights_*` pins it.

**Feldera does the same** (checked at `78afc9077`). Its circuit pass `RemoveUnusedFields`
narrows the inputs of `DBSPLeftJoinOperator` / `LeftJoinIndex` / `LeftJoinFilterMap`, as it
does for inner joins, so a right side whose columns are unused is projected to its key before
the join and its weights sum. Its generic outer-join path is stricter still: unmatched rows
come from `dyn_antijoin`, which projects the right side to keys and applies `distinct`, so
presence is *always* the summed key weight. On the pinned corner case Feldera pads the left
row, as our pruned join does; our old unpruned join was the outlier. Its dedicated LEFT JOIN
(`saturate`) uses per-row presence, but only over the columns that survive trimming. Feldera
disables Calcite's `PROJECT_JOIN_TRANSPOSE` as "unsound, replaced with UnusedFields done
later", and the replacement still trims outer-join inputs.

**Results** (SF=3 batch 1, two runs, same harness as §9.5; all 16 outputs identical to main,
row for row with multiplicities):

| | main | outer-join pruning |
|---|---|---|
| engine step | 63.4–64.2 s | **40.0–40.4 s (−37%)** |
| allocated | 84.5 GiB | **46.7 GiB (−45%)** |
| batch wall | 67.6–70.2 s | 44.4–44.8 s |
| `fact_market_history` left join | 20.4 s / 25.8 GiB | 2.05 s / 0.3 GiB |
| its projection | 6.7 s / 12.4 GiB | 0.45 s / 0.2 GiB |

**Tests** (`JoinColumnPruningTests`, +13 cases): plan shape (fires, padded side stays
nullable, fan-out narrows to the key, a residual blocks it); pruned ≡ unpruned ≡ batch
oracle on well-formed streams with NULL keys and an aggregate parent, on both the typed and
structural paths; the corner. Mutations caught: inner-join schema for outer joins (7
failures) and dropping the residual guard (2). Full suite: 2457 passed, 1 skipped.

**Lesson.** The view was 45% of the batch, and the profile said "join". The cause was a
plan rule that stopped at INNER. Check the logical plan for a missing rewrite before
optimising the operator.

### 9.7 Audit after §9.6: no second plan blunder; the rest is per-row work

Same harness, rebased ivm-bench SQL (`2c10de5`), `e19cb37` per-view rollup. Engine step
~39.6 s, 46.7 GiB. **No view is above 12% of the step**; the top five are `fact_holdings`,
`watches`, `trades_history`, `daily_market` and `fact_trade` (~55% together). ApplyOp is still
about half of step time and allocation across 373 instances. It is spread thin: projections
between joins, each building new rows and a fresh hashed Z-set.

What the audit found, by allocation (per-operator ms is not trusted under ServerGC, §9.5):

- **Fan-out written into the benchmark SQL, not ours to remove.** `fact_holdings` joins
  `dim_trade` (×2) and `dim_security` on keys with no temporal predicate, although both are
  SCD-2 tables. Its 912,231 output rows come from 362,342 holding rows. Feldera runs the
  same SQL and pays the same multiplication. This is a candidate upstream report, not an
  engine lever.
- **SCD temporal joins** (`fact_trade`, `fact_holdings` ⋈ `dim_account`): equi-key plus
  `BETWEEN` residual, already pushed into the join (no materialised product). Inherent
  per-candidate work.
- **`watches`**: a MIN/MAX GROUP BY over CASE projections. Aggregate-input narrowing
  (`NonLinearNarrowingMode`) does not fire, because ivm-bench's tables are not declared
  `append_only`, so non-negativity is not provable. Under "Always" it would. That is the
  same well-formed-input assumption as §4G.1 and §9.6, so it is a **policy decision**, not a
  bug.
- Parsing-shaped Applies (`finwire_financial` 1.5 GiB, `dim_trade` 1.5 GiB) are real work that
  Feldera also does.

**Conclusion.** The remaining batch-1 cost is genuine per-row work: row construction plus a
consolidating hash per operator per tick. That is the row-representation arc's "Layer A + B",
now on a clean profile. Next: price a non-consolidating delta path for linear operators
(map/filter chains between stateful ops) against the `reprbench`/`w1profile` instruments
before any representation rewrite.

### 9.8 MIN/MAX input narrowing by default: measured, not shipped

Policy (Curt, 2026-10-29): an assumption Feldera makes that *improves performance* should be
on by default. Feldera narrows aggregate inputs unconditionally (Calcite projects an
aggregate's input to its keys and arguments), so `NonLinearNarrowingMode` was flipped to
`Always` and measured. The suite passed once the signed-stream PBTs pinned `Auto`, and a
corner test pinned the divergence.

SF=3 batch 1 (outputs identical): **`Always` allocated 47.74 GiB vs 46.71 GiB under `Auto`
(+1 GiB)**, and the step was no faster (39.1 vs 38.5 s). On `watches` the narrowing added a
*separate* projection ApplyOp (502 MiB) directly after the view's own CASE projection,
without shrinking the aggregate (1,205.6 MiB, 885,922 state rows, both arms). Nexmark already
declares every table `append_only`, so `Auto` already narrows there and `Always` changes
nothing. **Reverted.** It fails the policy's own condition.

**Lead for the per-row arc.** Optimizer-inserted narrowing projections (this rule, and
join-input pruning) can land as their own ApplyOp next to an existing projection instead of
being fused into it. Count adjacent Project→Project ApplyOps in the compiled ivm-bench
circuit before designing any representation change. If the pattern is common, fusing them is
a cheap, deterministic allocation win.

### 9.9 Unfused pointwise chains: ~10 GiB of throwaway Z-sets (the §9.8 lead, measured)

Instrument: `tests/DbspNet.Tests/Scratch/IvmApplyChainCensus.cs` (same env vars as
`IvmBatchProfile`). It runs SF=3 batch 1 and rebuilds the operator graph by reflection (each
operator's `Stream<>` fields). It labels every ApplyOp with the builder method and the
`PlanToCircuit` method that made it, and reports **single-consumer ApplyOp → ApplyOp edges**:
a producer whose whole hashed output Z-set exists only to feed one pointwise consumer.

**Result: 373 ApplyOps (21.2 GiB). 187 single-consumer Apply→Apply edges, whose producers
allocate 10.2 GiB, ~22% of the batch's 46.7 GiB.** By pattern:

| edge (producer → consumer) | edges | producer alloc |
|---|---|---|
| input projection → join's null-key `Filter` | 40 | 3,088 MiB |
| join's null-key `Filter` → join's key-index `GroupProject` | 64 | 1,496 MiB |
| linear chain → linear chain (view / CTE boundary) | 17 | 3,753 MiB |
| projection → aggregate's key-index `GroupProject` | 10 | 1,145 MiB |
| aggregate output map → linear chain | 4 | 460 MiB |
| the rest (semi/outer-join plumbing) | 52 | ~270 MiB |

Causes:

1. **Join and aggregate lowering emits its own pointwise operators.** `CompileInnerJoin`
   builds `Filter(HasNoNullKey)` then `GroupProject(key, row ⇒ row)` on each side, after the
   input's own projection. That is three passes and two intermediate Z-sets per join input.
   Operator fusion (`CompileLinearChain`) only merges adjacent *plan* nodes, and these are
   created inside the join's lowering. The null-key filter is a pure no-op when the key
   columns are NOT NULL.
2. **View and CTE references stop the chain.** A referenced view or CTE is compiled once to
   its own stream, so a projection over it starts a new chain even when the census shows
   exactly one consumer (`finwire_financial → financials`, `watches_history → watches →
   fact_watches`, `holdings_history → fact_holdings`).

**Levers, cheapest first** (all compile-time lowering; each is exact):

1. Fuse the null-key filter into the key-index step (one `GroupProject` that drops NULL-keyed
   rows), and skip the check entirely when every key column is NOT NULL.
2. Lower the input's linear chain *into* the join's or aggregate's indexing pass: compile the
   chain to a row function instead of a stream when its only consumer indexes it.
3. Inline a single-consumer view/CTE reference through `CompileLinearChain` instead of
   materialising it as a separate stream.

The ceiling is ~10 GiB of allocation, plus one full hash-and-insert pass per removed edge.
Feldera does the equivalent (checked at `78afc9077`): its circuit pass
`visitors/outer/OptimizeProjections.java` composes `Map → MapIndex` and `MapIndex → MapIndex`
into one operator when the producer's fan-out is 1, and folds a `Map` after a join into the
join (`JoinFilterMap`). Measure each lever with whole-step + allocation on SF=3, and w1profile B/ev on
Nexmark (the typed path has its own join lowering: check it for the same pattern).

## 10. Appendix — unshipped variants (write-ups archived from their branches)

These sections lived only on their experiment branches. They are copied here verbatim
(headings renumbered) so the record survives deleting the branches. Their wall-clock
verdicts are in §6 (C, D) and §7 (H). The `IIndexedTrace` seam C and D were built on is
`exp/state-trace-seam`. Cited as §4C/§4D/§4E/§4H elsewhere in this doc.

### 10.C Flat hash trace (`exp/state-C-flathash` @ `682ce09`, `DBSP_TRACE_REPR=FlatHash`)

**Design** (`FlatHashIndexedTrace.cs`). One trace = two struct arrays + two
`int[]` bucket arrays. `KeySlot {key, hash, head, tail, count, next}` in a chained
hash table over keys; `Entry {value, weight, valueHash, slot, nextInBucket,
nextInGroup, prevInGroup}` in a second chained table keyed on `(slot, value)`
(O(1) `WeightOf`/integrate), with each key's values on a doubly-linked run in
insertion order (group enumeration, join probe). Both hashes stored, so a rehash
never re-hashes a row; free lists recycle retracted slots. The struct arrays are
`PagedArray<T>` (first page doubles to 4,096 elements, then grows a page at a
time) — the first cut used plain doubling arrays and *raised* B/ev (q4 2,351 vs
2,222) from the 2× cumulative copy; paging turned that into a −14% win and also
cut live bytes further (q4 333 → 308 MB). Container objects per trace:
4 + ⌈entries/4096⌉ + ⌈keys/4096⌉, versus 4 per key for `Flat`.

The aggregate gets a lazy after-group view through a new
`IIndexedTrace.AfterGroupView(key, delta)` (default `null` → the eager rebuild);
FlatHash returns `MergedGroupMultiset`, the generalization of `LazyMergeMultiset`
(or the delta itself when the key is absent). `Flat`'s concrete fast path is
untouched: Flat B/ev byte-identical to the seam commit.

**Measurements** (1M events, batch 10k, W=1, same build):

| query | live bytes Flat | live bytes FlatHash | Δ | B/ev Flat | B/ev FlatHash | Δ |
|---|---|---|---|---|---|---|
| q4  | 490,422,568 | 307,757,688 | **−37.2%** | 2,222 | 1,913 | −13.9% |
| q9  | 361,307,768 | 295,496,152 | −18.2% | 1,116 | 1,077 | −3.5% |
| q20 | 557,884,520 | 368,664,208 | **−33.9%** | 1,190 | 1,079 | −9.3% |
| q3  | 12,138,144 | 5,088,096 | −58.1% | 83 | 80 | −3.6% |

q9's smaller gain is because its TOP-1 state (row A) is untouched. **Tests:**
2,379 pass / 1 skip under both `Flat` (default) and `FlatHash` (2,375 existing +
4 new differential tests vs. `IndexedZSetTrace`: random cancelling deltas,
colliding hashes, multi-page growth, frontier GC, join both directions, lazy view).

**Loose ends.** Nothing shrinks (a trace that GCs most of its keys keeps its
peak arrays); per-key iteration is a linked-list walk (pointer-chasing inside one
array rather than across objects, but not sequential); remaining live objects
are dominated by `Utf8String` `byte[]`s (row E), which this variant does not touch.

### 10.D Sorted runs + LSM (`exp/state-D-sorted` @ `f788fe0`, `DBSP_TRACE_REPR=Sorted`)

**Design** (`src/DbspNet.Core/Operators/Stateful/SortedIndexedTrace.cs`). The trace is
a short list of immutable runs. Each run is seven flat arrays — key hash, keys, offsets,
value hash, values, weights (typed struct rows stored inline) — with no per-key object.
`Integrate` sorts the delta once into a new run, then does a size-tiered compaction:
when the newest tier holds `F` runs (default 8, `DBSP_SORTED_FANOUT`; tier = ⌊log_F(values/1024)⌋)
they are k-way merged into one run, consolidating equal (k, v) and dropping zero nets.
A join sorts the delta's keys once and gallop-walks every run, with no per-key hash
probes. The aggregate gets `PrepareTouchedGroups`: one walk of each run locates every
touched key's segments, and `SortedGroupView` is a lazy before+delta view over them,
the sorted analogue of `LazyMergeMultiset`, so the eager `MergeView` fallback is never
used. `Flat` keeps its concrete fast path. Its B/ev is byte-identical: 2222 / 1116 / 1190 / 83.

**How this differs from the old spine, and whether it mattered.** The old spine also
used inline typed struct arrays on the typed path (keys/offsets/values/weights), so
"objects vs structs" is not the difference. What differs:
1. **Order by stored 32-bit equality hash**, not `Comparer<T>.Default`. Ties are resolved
   by `EqualityComparer<T>.Default`. Every merge or probe comparison is an `int` compare
   against an array, never a field-by-field `IComparable` walk. Equality semantics are
   exactly flat's, and any row type works (records, tuples, `StructuralRow`) with no
   comparer plumbing. The cost is 4 B per key and per value, and no range locality for
   monotone keys (GC scans, as flat does).
2. **No per-batch bloom filter**: the walk is a merge, so it needs no point-probe filter.
3. **Larger fan-out (8 vs 4) and no memtable**: a run per tick, merged geometrically.
4. **Aggregate view over segments** instead of the spine's own aggregate operator.
Only wall-clock can show whether (1)–(3) fix the spine's 20% build cost. On the
deterministic instruments, the representation wins on bytes and objects without
losing allocation.

**Results** (1M events, batch 10k, W=1, same build; live bytes reproduce within 0.1%):

| query | live bytes Flat | live bytes Sorted | Δ | B/ev Flat | B/ev Sorted | live objects Flat → Sorted |
|---|---:|---:|---:|---:|---:|---:|
| q4  | 490,419,552 | 264,285,232 | **−46%** | 2222 | 2259 (+1.7%) | 1,981,674 → 1,262,488 (−36%) |
| q9  | 362,684,992 | 293,702,776 | −19% | 1116 | 1101 (−1.4%) | 2,126,209 → 1,646,571 (−23%) |
| q20 | 556,794,736 | 345,083,688 | **−38%** | 1190 | 1198 (+0.7%) | 3,182,775 → 2,895,628 (−9%) |
| q3  | 12,139,352 | 4,428,928 | −64% | 83 | 91 (+10%) | 84,819 → 30,987 (−63%) |

At 2M events: q4 963.8 MB → 505.4 MB (−48%), q20 1,099.8 MB → 673.6 MB (−39%). The
ratio holds as state grows. The objects left behind are VARCHAR `byte[]`s (row E) and
the aggregate/TOP-K side state (rows A/B). q9's state is mostly its TOP-K. Live objects
were counted by a reflection walk from the compiled query (`DBSP_COUNT_OBJECTS=1`, decay bench).

**Merge work** (F=8, 1M events): q4 52 merges, 2.89 rows moved/ev, max 14 runs/trace;
q9 26 merges, 0.26/ev, max 13; q20 27 merges, 1.53/ev, max 14; q3 28 merges, 0.10/ev,
max 8. At 2M events, q4 moves 3.57/ev with max 16 runs (logarithmic growth). Fan-out
trades merge allocation against probe fan-in:

| F | q4 B/ev | q20 B/ev | q4 rows moved/ev | q4 max runs |
|---|---:|---:|---:|---:|
| 4  | 2524 | 1287 | 4.77 | 9 |
| 8  | 2259 | 1198 | 2.89 | 14 |
| 16 | 2115 | 1140 | 1.85 | 20 |

**Wall-clock phase should look at** (1) the per-run walk cost with 14–20 runs, versus
the merge allocation saved at higher F; (2) merge latency spikes, since compaction runs
on the step thread and the largest merge rewrites the whole trace; (3) gen2/LOH churn
from the discarded run arrays (B/ev counts it, but LOH sweeps are not counted).

**Correctness.** `DBSP_TRACE_REPR=Sorted`: 2380 pass / 1 skip, which is the full
suite plus 5 new differential tests (`SortedIndexedTraceTests`: random insert/retract
streams against flat with a colliding hash, covering join both ways, the lazy views,
GC and full retraction). Snapshot save/restore round-trips through `Materialize`/`Integrate`.
The default and `Reference` suites are also 2380 / 1.

### 10.E Strings — ingest interning (`exp/state-E-strings` @ `e60bf95`)

**Built:** a bounded per-column intern table at `TableInput` ingest
(`IngestStringInterner`, `CompileOptions.InternIngestStrings`, **default-on**,
`false` = old path). Strings ≤64 chars in a VARCHAR column map to one shared
*boxed* `Utf8String`, so equal values share the box **and** the `byte[]` in the
input delta and in every trace that keeps the row. ≤1,024 entries per column;
once full, a column whose next 4,096 lookups are mostly misses releases its
table and stops probing (ids, card numbers, free text). Semantics-neutral by
construction: same bytes, content-based hash/equality/order — no hash path
touched. Not built: an ingest arena (§ below).

**Results** (1M events, batch 10k, W=1, `stream`; objects from `dotnet-gcdump`
via the new `DECAY_GCDUMP` hook — gcdump caps at 10M objects, so q18 objects are
taken at 400k events). `DECAY_INTERN=0` gives the A/B arm in one binary.

| query | live bytes base → E | Δ | objects base → E | Δ | B/ev base → E |
|---|---|---|---|---|---|
| q4 | 490,409,992 → 486,110,832 | −0.9% | 1,987,995 → 1,931,626 | −2.8% | 2222 → 2010 |
| q9 | 361,854,256 → 357,258,384 | −1.3% | 2,137,047 → 2,080,667 | −2.6% | 1116 → 905 |
| q18 | 1,967,932,856 → 1,724,811,664 | **−12.4%** | (400k ev) 5,218,629 → 4,087,068 | **−21.7%** | 2106 → 1898 |
| q19 | 874,508,824 → 631,444,872 | **−27.8%** | 5,242,493 → 2,455,012 | **−53.2%** | 1668 → 1461 |
| q20 | 558,312,416 → 314,791,120 | **−43.6%** | 3,194,963 → 394,509 | **−87.7%** | 1190 → 979 |

B/ev falls by ~208 B on every query that ingests bids (q0 653→445, q1 769→561,
q2 515→307, q22 882→674, q3 83→73). q4/q9 barely move in live state because
column pruning already drops the bid strings before their traces; q19/q20 store
the strings and lose most of their objects (q20: 2.80M `byte[]` → 25k).

**Caveat — the prize is workload-shaped.** Our generator's bid strings are
low-cardinality (4 channels, 4 URLs, constant extras). Feldera's Nexmark
generator pads `extra` with random text and randomises some URLs, so on that
data only `channel` (and part of `url`) would intern; high-cardinality columns
fall back to today's encoding at the cost of one bounded probe window. An
ingest **arena** for those columns was not built: on these five queries the
remaining string objects after interning are ≤61k per query (nothing to win),
and arena retention (one surviving row pins its batch's chunk) is a real risk
for q18-style replace-heavy state. It needs a high-cardinality generator to be
measured honestly.

**Recommendation: ship** (default-on, cheap, bounded, semantics-neutral; full
suite 2,383 pass / 1 skip). Arena = follow-on, only with high-cardinality data.

### 10.H VARCHAR ingest arena (`exp/state-H-arena` @ `9aa41aa`)

**Built.** `TableInput.Push` encodes each batch's VARCHAR cells into a few shared
`byte[]` chunks and hands out `Utf8String` slices of them
(`src/DbspNet.Sql/Compiler/Utf8IngestArena.cs`). Before this, every cell got its
own array. There are two passes. The first sums each column's exact UTF-8 length
and plans the chunks. The second encodes into those chunks, so every chunk is
filled exactly and none of the retained bytes are slack.

- **Chunks are per column, not per batch.** Columns that are pruned before
  state therefore free their chunks after the tick.
- **Chunk size is ≤ 64 KiB.** That is under the 85,000 B LOH threshold, so
  chunks are ordinary gen0 small-object-heap arrays that can be compacted and
  add no LOH budget pressure toward gen2. A single string bigger than the cap
  gets its own exact chunk.
- **The switch is on by default.** `Utf8ArenaMode.Enabled` is thread-static and
  read per `TableInput` at compile time. `DBSP_UTF8_ARENA=0` turns it off, and
  `DBSP_UTF8_ARENA_CHUNK` overrides the chunk size.
- `Utf8String` is untouched, so hash, equality and ordering are the same bytes
  as before. `Insert`/`Delete` of a single row keep the per-cell path.

**Measured** at 1M events, batch 10k, W=1. The baseline is main at `c53e782`;
q3's baseline is the same binary with the arena off, whose numbers match main's
(B/ev byte-identical, bytes within 0.4%). Live objects come from
`DBSP_COUNT_OBJECTS=1` (`ObjectGraphCounter`, cherry-picked from D).

| gen | query | live objects | live bytes | B/ev |
|---|---|---|---|---|
| feldera | q3 | 94,245 → 62,093 (−34%) | 14,962,912 → 13,532,424 | 116 → 108 |
| | q4 | 1,848,444 → 1,819,070 (−2%) | 422,360,568 → 420,977,904 | 1978 → 1900 |
| | q9 | 1,189,514 → 1,100,839 (−7%) | 285,525,688 → 281,190,136 | 1022 → 943 |
| | q18 | **3,271,164 → 512,965 (−84%)** | 641,515,320 → 492,989,440 (−23%) | 1070 → 996 |
| | q19 | **3,372,472 → 614,273 (−82%)** | 655,819,576 → 507,710,848 (−23%) | 1081 → 1008 |
| | q20 | **3,180,837 → 385,565 (−88%)** | 611,280,320 → 502,693,560 (−18%) | 1170 → 1091 |
| legacy | q3 | 84,819 → 64,394 | 12,140,392 → 11,506,984 | 83 → 75 |
| | q4 | 1,981,678 → 1,952,291 | 490,393,640 → 488,859,800 | 2222 → 2140 |
| | q9 | 734,007 → 645,319 | 274,942,592 → 270,347,888 | 1030 → 948 |
| | q18 | **2,854,816 → 95,617 (−97%)** | 472,689,584 → 319,950,400 (−32%) | 1429 → 1353 |
| | q19 | **2,918,923 → 159,724 (−95%)** | 538,058,640 → 384,911,528 (−28%) | 1270 → 1193 |
| | q20 | **3,182,775 → 386,842 (−88%)** | 556,291,864 → 405,613,968 (−27%) | 1190 → 1109 |

On q18, q19 and q20, per-cell `byte[]` were 84–88% of all live objects (2.76M
for 920k bids). They are now about 1,800 chunks. Every query's B/ev drops by
about 75–80, because array headers disappear.

**Retention: chunks do pin garbage.** The counter now reports referenced slice
bytes against the bytes of the arrays that hold them.

- **Nexmark is mostly benign.** q18 and q19 are 100% utilised, because the
  TOP-K multiset keeps every bid. q20 is 84.3% utilised (feldera): the
  `category = 10` filter keeps ~21% of auctions, which pins ~21 MB of unused
  auction strings, but the query still nets −108 MB. q3 is 71% (feldera) and
  35.5% (legacy), but it still nets smaller.
- **The worst case is severe.** `arenaretain` keeps 1 row in 100 of
  ~100-byte unique strings, either through a filter or through retractions.

  | chunk | arena off | arena on |
  |---|---|---|
  | 64 KiB | 9.5 MB | 204 MB (2% utilised) |
  | 4 KiB | 9.5 MB | 86 MB |
  | 1 KiB | 9.5 MB | 27 MB |

  At 20% survival, 64 KiB chunks give 219 MB against 72 MB with the arena off.
  Smaller chunks only help at very low survival. Under uniform sparse survival,
  every chunk is pinned. The bound is: each survivor pins at most one chunk per
  string column. That makes retention up to ~1/selectivity × string bytes,
  capped by the bytes ingested.

**Correctness.**

- The full suite passes: 2413, plus 1 skipped.
- `Utf8IngestArenaTests` adds 28 tests:
  - slice bytes, hash, `StructuralRowHash`, equality and order match
    `Utf8String.Of`, for multibyte strings, emoji, empty strings, nulls and a
    70 KB string;
  - exact chunk fill;
  - an on/off differential, where 8 queries (string-keyed join, TOP-K partitioned
    by string, COUNT DISTINCT, MIN/MAX(s), UPPER/LIKE/concat, DISTINCT) × 3
    paths (typed, structural, spine) get 40 ticks of random inserts and deletes
    and must give identical deltas on every tick.
- Persistence round-trips run with the arena on, since it is the default.

**Verdict: iterate. Do not ship default-on as is.** The object kill is the
largest seen in this arc and matches §6's diagnosis. But a selective filter, or
CDC retractions, can multiply string retention by 1/selectivity. Next steps:

1. A compile-time gate: no arena for a table that has a filter between its scan
   and its first stateful consumer. This fixes q20's auction side and q3.
2. A state-side copy that covers retraction-heavy inputs, with this arena kept
   as the ingest half. The design is to rebase a row's `Utf8String`s into a
   paged arena owned by the trace, at integrate time.

Wall-clock was not measured; the machine was shared.

