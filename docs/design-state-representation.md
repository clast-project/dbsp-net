# Design: state representation — the throughput-decay arc

Status: **Phase 0 (measure + map) complete; Phase 1 (variants) not started.**

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

## 4. Variant A — compact partitioned TOP-K state (branch `exp/state-A-topk`)

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
