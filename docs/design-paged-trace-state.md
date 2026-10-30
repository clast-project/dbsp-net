# Paged trace state — trace rows as values in reference-free pages (DESIGN, proposed)

Branch `arc/row-repr`. Phase 1 of the row-representation arc
(docs/next-arc-row-representation-prompt.md). The measurements this rests on are
design-row-representation §27 — read §27.3, §27.6 and §27.7 first.

## 1. The problem, restated by the measurements

ivm-bench batch 1 at W=8 spends ~6 s (~40% of the step) in young-GC pauses. §27 established:

- **It is not copying.** Padding every row by 64 B (+8.7 GB copied) moved the W=8 pause by 0.00 s
  (§27.3). Actual copying is 5.8% of in-GC GC-thread time (§27.6).
- **It is old→young references.** 48% of in-GC time is marking and relocating through the cards of
  large-object-heap arrays — the trace hash tables — that point at newly inserted rows (§27.6).
- **Moving the reference does not help; removing the object does.** In `TableCardProbe` (§27.7) a
  reference-free hash table plus a paged array of row references is −5–10%; copying the row's
  *values* into reference-free pages, so that no row object is kept, is **−48–49% young pause at W=1
  and W=8**, gen1 promotion 2.7 GB → 0.02 GB, build −38% at W=8.

The cost unit is **one young GC object referenced from an old container, per row inserted into
state**. Batch 1 inserts ~45M such rows (census §27.1: 32.5M in traces, ~13M in non-trace operator
state). The lever is therefore: *operator state stores row values, not row objects*.

What is **not** proposed, and why: an arena-backed `StructuralRow` that keeps a per-row handle
(prompt candidate 1) keeps exactly the object that costs; columnar inter-operator batches (candidate
2) attack deltas, which are short-lived and cheap for a young GC (the `cells` probe's residual
0.9 s is deltas *plus* transient rows — a later lever); typed structs (§23.8) are unboxed but still
hold `Utf8String` → `byte[]` references and copy per trace.

## 2. Representation

A **row store** per trace side: rows of one schema as fixed-width values in pages that contain no
object references, so the GC never marks, scans or relocates them.

```
RowStore (per trace, per side: key store / value store)
  layout      : RowLayout                 — per column: kind, width, offset; null-bitmap bytes
  fixed pages : byte[] × N                — 1 MiB each (LOH; reference-free); row = [nulls | cells]
  string pages: byte[] × M                — 1 MiB each, append-only UTF-8 bytes
  hash[]      : int (paged, parallel to rows) — the row's StructuralRow hash, stored, never recomputed
  free list   : int slots freed by retraction (fixed part reused; string bytes reclaimed by compaction)
```

Cell encodings (column kind from the compile-time `SqlType`, §5):

| SqlType / CLR cell | stored as | width |
|---|---|---|
| BIGINT `long`, `Timestamp`, `Time64` | int64 | 8 |
| INTEGER `int`, `Date32` | int32 | 4 |
| DOUBLE `double`, REAL `float` | IEEE bits | 8 / 4 |
| BOOLEAN `bool` | byte | 1 |
| DECIMAL `Decimal128` | its 128-bit value + scale (whatever reproduces `Equals` and `GetHashCode` exactly) | 17–24 |
| INTERVAL `Interval` | int32 months + int64 micros | 12 |
| VARCHAR `Utf8String` | (page, offset, length) into string pages | 12 |
| NULL | bit in the row's null bitmap | — |

Census-shaped rows (~8.6 cells, ~2.75 of them strings) come to roughly 70–90 B fixed plus string
bytes. Today they are 32 B `StructuralRow` + ~93 B `object[]` + shared boxes. Live bytes fall, but
that is a side effect, not the goal.

**Escape column.** A cell whose CLR type does not match its column's kind (numeric-string coercion,
`SqlNullType` columns typed `object`, anything the compiler did not foresee) sends the **whole row**
to a per-store overflow `List<StructuralRow>`, addressed by a tagged slot. That costs one reference
per such row, it is correct by construction, and the overflow count is reported (it should be ~0 on
ivm-bench; a non-zero count is a layout bug to fix, not a correctness risk).

**Pages are managed `byte[]` on the LOH**, not native memory. They are reference-free, so a young
GC never touches them and a background gen2 does not mark through them, and their lifetime is the
trace's, so no disposal protocol is needed. Watch-out from the probes: LOH allocation counts
against the gen2 budget (reason-4 gen2s), so pages are large (1 MiB) and grow one page at a time,
never by doubling.

**Rows are not shared across traces** — each trace copies values in at `Integrate`. The census
measured the sharing this gives up: 30.9M of 32.5M trace rows sit in exactly one trace (W=8: 28.9M
of 31.5M). Broadcast-join right sides are copied into every worker's store; those are dimensions,
small.

## 3. Index over the store

The FlatHash structure from state-representation §10.C (built once, on `exp/state-C-flathash`
682ce09), with slots in place of row references:

- **Flat trace** (`ZSet<StructuralRow,Z64>` state): open chained hash
  `int[] buckets`, `Entry { int hash; int next; int slot; long weight }` in pages. Reference-free.
- **Indexed trace** (`IndexedZSet<K,V,Z64>` state): a key table
  `KeySlot { int hash; int next; int keySlot; int head; int count }` and a value table
  `Entry { int valueHash; int nextInBucket; int nextInGroup; int group; int valueSlot; long weight }`
  keyed on (group, value) for O(1) `WeightOf` / integrate, with each group's values on a linked run
  for enumeration. All reference-free.

Lookups compare the stored hash first, then the cells (§4). Retraction to weight 0 unlinks the
entry and frees its slot.

## 4. Hashing and equality — no fourth hash implementation

`StructuralRowHash` has three implementations that must agree (boxed, Expression, emitted IL — see
row-hash-owned). **This design adds none.** Every row enters a store through `Integrate(delta)`,
where it is already a `StructuralRow` carrying its cached `_hash`, computed by whichever path built
it. The store keeps that int. Restore builds boxed `StructuralRow`s through the existing codec
(§7) and inserts them, so restored hashes come from the boxed path, as today.

**Equality** (store row vs a probing `StructuralRow`) compares stored cells against the probe's
cells **with exactly `object.Equals` semantics per kind**, without boxing: `long`/`int`/`Date32`/
`Timestamp` by value; `double` through `double.Equals` (NaN equals NaN, and +0.0 equals −0.0, which
a bitwise compare would get wrong); `Decimal128` through its `Equals` after an in-register decode;
`Utf8String` by bytes; NULL vs NULL is equal. A probe cell of an unexpected CLR type falls back to
`Equals(decoded-boxed, probe)` (allocates; rare). There is a differential test for each kind, plus
NaN, ±0.0, decimal scales, empty strings and null patterns.

## 5. Where the layout comes from

The compiler knows every column's `SqlType` (`PlanToCircuit`, both trace sides). It passes a
`RowLayout` to the trace factory when the paged store is selected. It does not infer types from the
first row, because a NULL-led column would leave the kind unknown and inference makes behaviour
depend on data order. Operators built outside the SQL compiler (Core tests) get no layout and stay
on today's representation.

## 6. The read path — the crux

Operators read trace state as `StructuralRow` today, through concrete types (`GroupFor` returns a
live `ZSet` alias; `JoinInto` enumerates `IndexedZSet`; there is **no trace seam** — the round-3
`IIndexedTrace` seam was never merged). There are three kinds of reader:

1. **Compare-only** — `DistinctOp` (`WeightOf`), the restore folds, `ContainsKey`. The store answers
   them directly (hash + cell compare); nothing is materialised.
2. **Read-cells-then-discard** — join combine (`MergeRows` copies cell references into a new output
   row), aggregates (SUM/COUNT/AVG read a cell; MIN/MAX extract one), left/full join null-padding.
   These get a **flyweight cursor row**: one reusable `StoreRow : StructuralRow` per probe loop,
   repositioned per row, whose indexer decodes a cell. Decoding a value cell *boxes* it, and a join
   output row built by `MergeRows` then holds those boxes. Today it shares the delta's or trace's
   boxes, so this is new allocation on the probe side — short-lived, and dead or re-stored into the
   next store by the next tick. The B/ev instrument will price it (§9).
3. **Escaping** — the row object itself leaves: `IntegrateOp.View` (read by the output
   connectors), the fixpoint imports (whole trace fed to the nested circuit), the anti-join's
   semi-join (`(_, sideRow, _) => sideRow`, PlanToCircuit.cs:3651), and TOP-K / rank / offset /
   window / temporal-filter state, which emit stored rows. These **materialise** a real
   `StructuralRow` (`object[]` + boxes). A flyweight must never escape: a stored slot can be freed
   and reused by a later tick.

**The seam** that makes this possible is an internal interface over trace state, introduced
operator by operator, with today's types as the default implementation. It has the same shape the
round-3 prompt sketched (`GroupFor` returning an `IMultiset`, a probe-with-callback for joins,
`WeightOf`, `Integrate`, `DropKeysBelow`, snapshot enumeration). Each operator converted to it
keeps its flat fast path byte-identical (the `Flat`/`FlatAggregateMode` precedent).

## 7. Snapshot / restore

- **Save** enumerates the store as boxed rows into the existing Arrow codecs (`ArrowZSetTraceCodec`
  / `ArrowIndexedZSetTraceCodec`). This keeps files byte-identical to today, so a snapshot written
  by either representation restores into either. A direct page → Arrow column writer is a later
  optimisation; the store is already columnar-ish.
- **Load** decodes through the codec as today, then `Integrate` copies into pages. Restored rows are
  short-lived instead of becoming permanent state. That should help restore (lazy-restore found it
  boxing-bound, §11), and the Arena VARCHAR mode stops mattering for paged traces, because the
  string bytes are copied into string pages.
- **Parallel**: per-replica stores, `worker-{w}/` layout unchanged, same-W restore unchanged.

## 8. Parallel path

Exchange keeps moving delta row *references* between workers (`ExchangeOp` et al.). Deltas are
unchanged. Each replica's paged store copies values at `Integrate`. Partitioning uses
`StablePartitionHash` over key cells, independent of all this. Nothing else changes.

## 9. Increments, instruments, and what each should show

Every increment sits behind `DBSPNET_TRACE_STORE=paged` (default off). Default-off must be
**byte-identical**: w1profile B/ev identical on all Nexmark queries, all 16 ivm-bench view digests
identical. Default-on: same digests at W=1 and W=8.

| # | scope | rows moved off-object (census) | readers | expected W=8 young pause |
|---|---|---|---|---|
| **1** | flat `ZSetTrace` in **`IntegrateOp`** (in-circuit integrated output views) | ~4.3M (36 flat traces, incl. `DistinctOp`'s) | escaping, but **cold**: read only at output / `/wait` | −0.2–0.4 s |
| 2 | `DistinctOp` | (in #1's 4.3M) | compare-only | small |
| **3** | `IndexedZSetTrace` in joins (inner / left / full / semi / anti) and the shared arrangement | the bulk of 24.9M values + 5.4M keys | cursor in `JoinInto`; anti-join semi materialises | −1.5–2.5 s |
| 4 | `IncrementalAggregateOp` traces | rest of the indexed traces | cursor via `IMultiset` (`LazyMergeMultiset` over a store group) | — |
| 5 | non-trace state: TOP-K / rank / window / temporal filter (`SortedDictionary` nodes 6.2M, etc.) | ~13M | escaping (materialise on emit) | — |

Pause estimates assume pause ∝ rows referenced from old containers, as in the probe. They are not
measurements.

**Increment 1 is the smallest change the instruments can see**, and it de-risks the store itself
(layout, strings, decimals, NULLs, escape column, free list, snapshot round trip) behind a read
path that is cold: `IntegrateOp` only integrates per tick. The instruments it should move:

- **promoted gen1 bytes** and **young pause** from `IVM_GC_EVENTS=1` (pause needs ≥5 alternating
  runs per arm at W=8; promotion is near-deterministic);
- **`StructuralRow` objects reachable** after batch 1 (`IVM_HEAP_CENSUS=1`) — should fall by ~4.3M;
- **LOH card time** share (`sample` + `scripts/gc-phases.py`);
- digests identical; default-off w1profile identical.

**Increment 3 is the prize and the risk.** It needs the seam in `IncrementalJoinCore.JoinInto`,
and it prices the new cell boxing on the probe side (w1profile B/ev on q4/q9/q20, with the store on,
for Nexmark joins too). It is kept only if ivm-bench W=8 batch 1 improves by ≥1.5 s of pause, or
step time beyond noise, *and* the Nexmark B/ev cost is acceptable to Curt.

**Stop rule.** If increment 1's promoted-gen1 and census deltas do not track its row count (i.e. the
real engine does not behave like `TableCardProbe`), stop before increment 3 and write up why.

## 10. Risks and open questions

1. **Probe-side boxing** (§6, kind 2) adds allocation: every value cell a join copies out of a
   store is a fresh box. That allocation is short-lived and is not referenced from old containers,
   which is the cheap case for a young GC. But it is real work, and on fine-tick Nexmark joins it
   is pure cost. There is no cheap mitigation inside this design: the output row needs boxed cells
   as long as deltas are `object[]` rows. The structural fix is downstream operators consuming
   cells without boxes, which is the columnar arc. Increment 3 measures it before anything is
   committed to.
2. **Probe CPU.** Cell compare on hash collision instead of reference/`_hash` compare; decoding on
   group enumeration. Expected to be small next to dictionary probes; `TableCardProbe` built 38%
   *faster* (fewer GC pauses), but its rows are all longs.
3. **String churn.** Retracted strings leak bytes until compaction; batch 1 is insert-only, but
   steady-state streaming (Nexmark) retracts. Compaction policy: rebuild a store's string pages when
   dead bytes exceed live bytes, off the hot path at a tick boundary.
4. **Non-trace state is ~13M rows** (increment 5). Each operator is its own conversion, and TOP-K's
   sorted structures need an order comparer over stored cells (`StructuralRowComparer` semantics,
   NULL first).
5. **Deltas on the LOH** (§27.7 point 2) remain. They are a separate lever (chunked delta storage)
   and are not designed here.

**Decisions for Curt:**
(a) Layout from compile-time `SqlType` (proposed) or inferred from data?
(b) Managed LOH pages (proposed) or native memory?
(c) Start with increment 1 as the de-risking step (proposed), or go straight to increment 3 (joins),
    where the prize is?
(d) Is the probe-side boxing cost on Nexmark joins (increment 3) acceptable if ivm-bench wins, and
    what B/ev regression would you accept?

Decided 2026-10-30: (a) compile-time `SqlType`, (b) managed LOH pages, (c) increment 1 first.
(d) open — needed before increment 3 is kept, not before it is built.

## 11. Increment 1 — built and measured (2026-10-30)

Commits `574fbaf`, `aa45e16`. `PagedZSetStore` (src/DbspNet.Sql/Compiler/PagedTraceStore.cs)
holds each `IntegrateOp`'s integral behind `DBSPNET_TRACE_STORE=paged`; `IntegrateOp` takes an
`IIntegratedViewStore<TRow>` (Core) and forms `View` on demand, once per step. View counts and
weights (`IntegratedViewHandle.Count` / `WeightOf`, `ProgramOutput.RowCount`) no longer form it.

Built as §2–§4 say, with two details settled in code:
- **Every non-string kind is one codec**: `Decimal128`, the temporal structs and the primitives
  are unmanaged and `IEquatable<T>`, so a cell is its raw bytes (decodes bit-for-bit) and compares
  with `IEquatable<T>.Equals` — what `T.Equals(object)` does for a same-typed probe.
- **Slots are allocated like `Dictionary` entries** (LIFO free list, else append; `Clear` frees in
  enumeration order like `MergeInPlace(Negate())`), so the formed view enumerates in the same order
  and snapshot files are byte-identical.
- The bucket index must mix the hash first (Fibonacci). Masking the low bits made `fact_holdings`
  take 13.5 s to integrate at W=8 (Dictionary: 0.26 s); the cause in those rows' hashes is not
  pinned down.

**Correctness.** Differential tests against `ZSet` (random signed deltas over every kind, NaN
payloads, ±0.0, NULL patterns, off-kind and wrong-arity rows that escape, string compaction,
the load reset); snapshot bytes identical across representations, and cross-restore both ways;
full suite 2628/2628 with the store off and with `DBSPNET_TRACE_STORE=paged`; ivm-bench SF=3 all
16 view digests identical at W=8 in every paired run. Default off: w1profile B/ev identical on all
ten Nexmark queries against `9935319`.

**ivm-bench SF=3 batch 1, W=8, Server GC.** Census at drain end (`IVM_MEASURE_AT=drain` — after
the output write the formed views would be counted too):

| | boxed | paged | Δ |
|---|---|---|---|
| `StructuralRow` objects | 44.59M | 41.01M | **−3.59M** (= the view rows) |
| all objects | 160.1M | 149.8M | −10.3M |
| LOH arrays holding references | 221 MB (133) | 102 MB (68) | −119 MB |
| live bytes (`PromotedBytes`) | 10.16 GB | 10.27 GB | +0.11 GB (pages 0.79 GB; shared strings now copied) |

Drain-only GC (`GCPAUSE-DRAIN`), 5 alternating runs per arm:

| | boxed | paged | Δ |
|---|---|---|---|
| young pause | 6.50 s (sd 0.36) | 6.26 s (sd 0.36) | −0.24 s, SE ≈ 0.23 — not resolved |
| promoted out of gen0 / gen1 | 15.77 / 13.84 GB | 15.53 / 13.50 GB | −0.24 / −0.34 GB |
| drain | 18.4 s (sd 1.0) | 18.1 s (sd 0.9) | noise |
| `IntegratedView` CPU (summed over workers, 1 run) | 0.45 s | 1.17 s | +0.7 s (≈ +90 ms per worker) |

**Reading.** The census moved exactly as designed. The pause moved by the amount §9 predicted
(−0.2–0.4 s for 8% of the state rows), but that is at the noise floor of 5 runs, and promotion
moved less than in proportion (−2.5% gen1 for −8% of rows). So the stop rule is neither clearly
met nor clearly failed: increment 1 is too small to resolve against this instrument. Increment 3
moves ~8× more rows; its keep-gate (≥ 1.5 s pause) is ~6 SE, which this instrument can resolve.

**Open costs found.** (1) Forming the view is now O(|view|) of new objects per read; at W>1 each
shard's formed view is cached in its `IntegrateOp` *and* summed into `ParallelProgramOutput`'s
cache, so formed copies are held twice until the next step. A streaming enumeration for the
output writer would remove both. (2) Integrate CPU is ~2.6× the Dictionary's (per-cell virtual
codec calls, boxed `row[i]` reads, first-page growth by copy) — small next to the step, not yet
optimised.

## 12. Increment 3 — built, measured, gate FAILED; the LOH card cost is deltas, not traces (2026-10-30)

Commit `2d36bf6`. `PagedIndexedStore` holds both traces of `IncrementalJoinOp` at the two join
sites whose combine copies cells (main inner join, residual join): key and value records in
`PagedRows` (the flat store now shares it), `int[]` key→group and (group, value)→slot indexes, and
per-group lists that mirror the inner Z-sets' `Dictionary` order (append; a retraction leaves a
tombstone that the group's next insert reuses, LIFO). Combine gets `PagedRowView`s (a handle;
cells boxed on read); string cells decode zero-copy from the append-only pages. Semi-joins return
their argument, so they stay on the in-memory trace. Ownership census first (`TraceAccessProfile`
now records the constructing operator): inner joins own 21.6M of 31.5M trace entries (3.3 GB),
aggregates 5.0M, integrated views 3.6M, left joins 0.7M, distinct 0.6M.

**Correctness.** Differential test against `IndexedZSet` (groups, values, weights, enumeration
order, drop-below, the formed snapshot); a join snapshot byte-identical across representations
(the test forces the structural compile — a single query otherwise takes the typed path and never
reaches the store, which a probe caught); suite 2632/2632 off and on; ivm-bench all 16 digests
identical at W=8 in every paired run.

**Census at drain end, W=8** (increments 1 + 3 on):

| | boxed | paged |
|---|---|---|
| objects | 160.1M | **109.7M** |
| `StructuralRow` | 44.6M | **20.2M** |
| live LOH arrays holding references | ~538 MB | ~140 MB |
| live bytes | 10.16 GB | 10.47 GB (pages 4.19 GB) |
| `Utf8String` / `Double` boxes | 6.8M / 10.5M | 14.6M / 16.3M (join output rows now carry fresh boxes, kept downstream) |

**GC, drain only, 5 alternating runs:** young pause 6.21 s (sd 0.37) → **6.12 s** (sd 0.27);
promoted gen0 15.92 → 17.30 GB, gen1 13.50 → 12.06 GB; drain 18.9 → 18.8 s. Inner-join allocation
9.6 → 15.9 GiB, join CPU +1.5 s summed (one profiled run). **The keep-gate (≥ 1.5 s of pause) fails
by more than an order of magnitude.** Per §9's stop rule, nothing further is built on this design.

**Why: the native trace and an LOH allocation tally.** `sample` + gc-phases.py on the paged arm:
mark through LOH cards 24.7% of in-GC time (boxed 25.3%), relocate through LOH cards 17.2%
(22.5%), survivors 23.2% (21.6%). Halving the live trace containers barely touched LOH card work.
`IVM_LOH_ALLOC=1` (runtime AllocationTick, LOH only) shows what does the LOH allocating in batch 1:

| LOH bytes allocated, batch 1, W=8 | boxed | paged |
|---|---|---|
| `Entry<StructuralRow,Z64>[]` — flat Z-set dictionaries | **6.00 GB** | **5.78 GB** |
| `KeyValuePair<StructuralRow,Z64>[]` | 1.28 GB | 1.28 GB |
| `object[]` | 0.75 GB | 0.75 GB |
| `Entry<StructuralRow,ZSet<…>>[]` — indexed groups | 0.51 GB | 0.25 GB |
| `Entry<StructuralRow,ZSetBuilder<…>>[]` | 0.41 GB | 0.41 GB |
| `byte[]` | 1.48 GB | 6.36 GB (pages) |

Only ~0.2 GB of the flat dictionaries is alive at drain end, so ~96% of that 6 GB is per-tick
**delta** Z-sets: each large enough for the LOH, each holding references to young rows, each
card-scanned by every young GC until a background gen2 frees it. That is the §27.7 side
observation ("per-tick delta Dictionaries are LOH arrays too") — and it, not trace state, is
where the 48% of young-GC time goes. `TableCardProbe`'s `cells` arm halved the pause because,
with `DELTA_CHUNK` set, its deltas were already off the LOH; the real engine's are not.

**What this leaves.** The paged store is correct and stays behind `DBSPNET_TRACE_STORE=paged`
(default off, byte-identical), but it is not the lever, and its probe-side boxing is a pure cost.
The lever the evidence now points at is **delta containers off the LOH** — §10 risk 5, never
designed here: per-tick Z-sets/builders whose backing arrays stay under 85,000 B (chunked, or
pooled across ticks so they age into gen2 once instead of being re-allocated on the LOH each
tick). Before designing it: the instrument is `IVM_LOH_ALLOC=1` plus gc-phases.py; a cheap test is
an arm that caps the delta builders' pre-size so no delta dictionary reaches the LOH, measured on
LOH-card share and drain-only young pause.

## 13. The delta test — LOH residency is the lever (2026-10-30)

Two arms against the baseline, ivm-bench SF=3 batch 1, W=8, Server GC, all digests identical.

- **Capping the delta builders' pre-size** (3000 entries; temporary seam, reverted) does nothing:
  drain 18.0 s, young pause 6.40 s (baseline 18.2 / 6.37, one run each), and LOH allocation of
  flat Z-set dictionaries *rises* 6.0 → 7.4 GB — a big delta still outgrows 85,000 B, and its
  resize steps leave more dead LOH arrays. Pre-size is not the lever; delta size is.
- **`DOTNET_GCLOHThreshold=0x8000000` (128 MB)** — no code change; arrays below it are allocated
  young and die young instead of sitting on the LOH, card-scanned, until a background gen2.
  5 alternating runs per arm:

| | baseline | LOH threshold 128 MB | Δ |
|---|---|---|---|
| drain | 18.56 s (sd 1.42) | **15.82 s** (sd 0.29) | **−2.74 s** (SE 0.65) |
| young pause | 6.53 s (sd 0.61) | **5.41 s** (sd 0.12) | **−1.12 s** (SE 0.28) |
| background gen2s | 6–9 | 0 | |
| promoted gen0 / gen1 | 15.8 / 13.9 GB | 20.0 / 14.9 GB | more copied, shorter pauses |

The drain gain exceeds the pause gain (no background gen2s competing for cores, and run-to-run
variance collapses 5×). Combined with the paged store (one run) it is worse — 17.3 s / 5.67 s —
because 1 MiB pages are then young objects that get copied; the store's design assumed the LOH.

This also moves trace containers off the LOH, but §12 showed those are a small share; the effect
is overwhelmingly the per-tick deltas. Next decisions: whether a process-wide runtime setting is
acceptable to ship (runtimeconfig `System.GC.LOHThreshold`; needs a threshold sweep — the 128 MB
point is arbitrary — and Nexmark/other benchmarks under it), or whether to engineer delta
containers that stay under 85,000 B per array (chunked Z-sets), which the setting now prices.

### 13.1 Threshold sweep and Nexmark (2026-10-30)

**Sweep**, ivm-bench SF=3 batch 1, W=8, 5 alternating runs per arm, digests identical in all:

| `GCLOHThreshold` | drain | young pause | background gen2s | peak heap after a GC |
|---|---|---|---|---|
| default (85,000 B) | 18.37 s (sd 0.78) | 6.44 s | 7–10 | 19.5 GB |
| 1 MB | 17.94 s | 5.71 s | 5–6 | 18.3 GB |
| 4 MB | 20.13 s (sd 1.25) | 5.49 s | 1–2 | 15.0 GB |
| **16 MB** | **15.73 s** (sd 0.49) | **5.26 s** | **0** | **14.9 GB** |
| 128 MB | 15.73 s | 5.41 s | 0 | 15.0 GB |

16 MB takes the whole gain; 128 MB adds nothing. Peak heap *falls* ~4.6 GB (dead LOH arrays no
longer pile up waiting for a background gen2). 4 MB is the odd point — better pause, worse drain;
one or two background gen2s still run there, timing unexplained — a reason not to sit at the edge.

**Nexmark** (`nexmark 1000000 10000 3 8`, the benchmark's own Server GC config, default 85,000 B vs
16 MB, 3 alternating processes per arm, median of the three per-process medians, events/s ratio):

| | q0 | q1 | q2 | q3 | q4 | q9 | q5 | q7 | q8 | q12 | q15 | q16 | q17 | q18 | q19 | q20 | q22 | geomean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| W=1 | 1.08 | 1.19 | 1.09 | 0.99 | 1.07 | 1.09 | 1.18 | 1.15 | 1.01 | 1.07 | 1.10 | 1.12 | 1.11 | 1.11 | 1.13 | 1.20 | 1.06 | **1.10** |
| W=8 | 1.21 | 1.13 | 1.02 | 0.99 | 1.06 | 1.21 | 2.58 | — | 0.90 | 1.07 | 1.18 | 1.20 | 1.50 | 1.37 | 1.09 | 1.08 | 1.25 | **1.20** |

Every query but q3 (flat) and q8 at W=8 (0.90, inside its 1.31× spread) moves the same way. A
10,000-event batch makes ~240 KB delta dictionaries, so Nexmark's deltas were on the LOH too. Single
queries at W=8 swing a lot on this box (spreads up to 2.7×), so read the per-query W=8 cells as
direction; the W=1 row and both geomeans are the result.
