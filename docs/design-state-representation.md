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

## 4. Variant B — COUNT(DISTINCT) and MIN/MAX sub-state (`exp/state-B-distinct`)

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
