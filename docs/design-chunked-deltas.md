# Chunked Z-set maps — delta containers that stay off the large-object heap (DESIGN, proposed)

Branch `arc/row-repr`. Follows docs/design-paged-trace-state.md §12–§13: the batch-1 young-GC
cost that §27.6 traced to large-object-heap (LOH) cards is the **per-tick delta Z-sets**, not
trace state. Raising `GCLOHThreshold` to 16 MB — so those arrays are born young and die young — cut
ivm-bench SF=3 batch 1 at W=8 from 18.37 s to 15.73 s of drain (young pause 6.44 → 5.26 s, zero
background gen2s, peak heap −4.6 GB) and lifted Nexmark by a geomean +10% at W=1 and +20% at W=8.
That setting is process-wide; DbspNet is a library. This design gets the same effect in the data
structure.

## 1. What the deltas look like (measured)

`DBSPNET_DELTA_SIZE_PROFILE=1` (with `DBSPNET_PROFILE=1` for attribution) records every
dictionary-backed Z-set and indexed Z-set as it is formed — rows, and entry/bucket bytes from the
dictionary's capacity — by the operator stepping. ivm-bench SF=3 batch 1, W=8:

| entry-array size | Z-sets | rows | entry bytes |
|---|---|---|---|
| < 85 KB | 11,748,137 | 48.2M | 1.74 GB |
| 85 KB – 1 MB | 1,386 | 13.6M | 0.40 GB |
| 1 – 4 MB | 778 | 64.6M | 1.94 GB |
| 4 – 16 MB | 176 | 34.9M | 1.19 GB |
| > 16 MB | 7 | 6.3M | 0.22 GB (largest 33.5 MB, 982,180 rows) |

- **A few big deltas, not many medium ones.** 2,347 of 11.75M Z-sets cross the threshold, but they
  carry 71% of the rows.
- **Growth copies are a third of the LOH traffic.** Their final arrays total 3.75 GB; the runtime's
  AllocationTick tally (§12) saw 6.0 GB of `Entry<StructuralRow,Z64>[]` on the LOH — the rest is,
  most likely, the intermediate arrays a dictionary leaves behind each time it doubles (not
  recorded by this instrument, which sees only the final dictionary).
- **Producers** (LOH entry bytes): `ApplyOp` (fused filter/project) 1.84 GB; inner join output
  0.63; `ExchangeIndexOp` 0.29 and `ExchangeOp` 0.20; outside any operator step (input push,
  gather) 0.35; window/aggregate outputs ~0.35. All are built through `ZSetBuilder` /
  `IndexedZSetBuilder` or a `ZSet` operation — the structure lives in Collections, not in operators.
- The 11.7M small ones are mostly the inner per-group Z-sets of indexed deltas (exchange-index
  1.78M–6.09M per operator kind). They are on the small-object heap already; the design must not
  make them slower or bigger.

## 2. The structure: `ChunkedMap<TKey, TValue>`

The `Dictionary<TKey,TValue>` algorithm — separate chaining through an entry array, a bucket
index, a LIFO free list — with its two arrays split into chunks that never reach 85,000 bytes.

```
ChunkedMap<TKey, TValue>
  entries : Entry[][]   Entry { int hash; int next; TKey key; TValue value }
                        chunk 0 starts small and doubles (copying) up to EntryChunk entries;
                        after that, fixed chunks of EntryChunk, appended — never copied again
  buckets : int[][]     power-of-two total, chunks of BucketChunk ints; slot = Fibonacci(hash)
  count, freeList, freeCount                      — exactly Dictionary's bookkeeping
EntryChunk = 2048 for 24-byte entries (48 KB) — computed per instantiation as
             largest power of two with EntryChunk × sizeof(Entry) < 85,000
BucketChunk = 16,384 ints (64 KB)
```

- **Order is Dictionary's, by construction.** A new entry goes to the free-list head if there is
  one, else to position `count`; `Remove` pushes onto the free list; resizing never moves an entry.
  So any sequence of operations enumerates in the order a `Dictionary` would — the property the
  paged store kept (§11 there), which keeps join/aggregate emission order, downstream floating-point
  sums, view digests and snapshot files byte-identical.
- **Small maps are a Dictionary in all but name.** Below `EntryChunk` there is one entry chunk and
  one bucket chunk, grown by doubling — the same allocation pattern as today (power-of-two instead
  of prime capacities, so sizes differ slightly).
- **Large maps never copy entries.** Growth past `EntryChunk` appends a chunk; only the bucket
  index is rebuilt on resize (reading the stored hash, as `Dictionary` does). The growth copies that
  are a third of today's LOH traffic disappear, as well as the LOH residency.
- **Every array is small, so every array is born young.** A delta dies within its tick, so a young
  GC that sees it either finds it dead (free) or copies a few 48 KB chunks once. No chunk is on the
  LOH, none is card-scanned while dead, and none counts toward the gen2 budget that drives the
  background gen2s.
- **Hashing and equality** go through `EqualityComparer<TKey>.Default` as `Dictionary` does; for
  `StructuralRow` that is the cached hash and the existing `Equals` — no new hash implementation.
- **Indexing cost.** One shift and mask per access to pick the chunk. Expected to be within noise
  of `Dictionary` (which pays a `FastMod`); the micro-benchmark gate in §5 checks it.

### Why not the alternatives

- **Pool deltas across ticks** (`DeltaPoolMode`, §20): removes allocation, not the cost — a pooled
  map lives in gen2 and is refilled with references to young rows each tick, the old→young pattern
  that made traces expensive.
- **Sorted batches, Feldera-style**: still large arrays unless chunked, and sorting `StructuralRow`s
  costs comparisons the hash path does not pay. A different redesign.
- **Large-only switch-over** (keep `Dictionary` below the threshold, migrate above): exact order
  after migration needs `Dictionary`'s free-list positions, which it does not expose. One structure
  everywhere is simpler and is exact.

## 3. Where it goes

`ZSet<TKey,TWeight>._store` is already polymorphic (a `Dictionary` or an `InlineEntry`); it becomes
a `ChunkedMap` or an `InlineEntry`. Work, by surface:

| surface | change |
|---|---|
| `ZSet`, `ZSetBuilder`, `IndexedZSet`, `IndexedZSetBuilder` (~22 `Dictionary` references) | swap the backing map; reproduce each `Dictionary` operation exactly — including the copy constructor's *compacting* copy used by `Clone`, and `new Dictionary(capacity)` |
| `ZSet.Enumerator` | wraps the map's struct enumerator instead of `Dictionary`'s |
| `IndexedZSet.GetEnumerator` | returns `Dictionary<…>.Enumerator` publicly today → its own struct enumerator; every `foreach` keeps compiling |
| internal `ZSet(Dictionary)` / `IndexedZSet(Dictionary)` constructors | used by Trace.cs, the two spine traces, the paged store and the census/profile hooks; they take the map instead |
| `TraceAccessProfile`, `HeapCensus`, `DeltaSizeProfile` | read the map's sizes |

Trace state uses the same types, so it is chunked too. For long-lived state that is neutral: an
old→young reference costs a card whether its array is on the LOH or in gen2 small-object pages, and
§12 measured trace containers as a small share.

Behind `DBSPNET_ZSET_MAP=chunked` while it is measured; default `Dictionary`. The seam is a
static readonly flag read at map creation, as `DeltaPoolMode` does.

**Not covered here, seen in §12's LOH tally:** `KeyValuePair<StructuralRow,Z64>[]` (1.28 GB) and
`object[]` (0.75 GB) on the LOH — other producers (exchange sharding buffers and similar, to be
located with the AllocationTick stack or the size profile extended to them). A follow-up once the
map lands, judged by what is left on `IVM_LOH_ALLOC`.

## 4. Increments

1. **`ChunkedMap` alone**, with a differential test against `Dictionary`: random operation
   sequences (add, set, remove, re-add, clear-by-removal, capacity hints, copy) checking contents
   *and enumeration order* after every step, across the chunk boundary; plus a micro-benchmark
   (insert/lookup/enumerate at 10, 1K, 100K, 1M entries) against `Dictionary`.
2. **Flat Z-sets** (`ZSet`, `ZSetBuilder`) behind the seam — the bulk of the LOH bytes.
3. **Indexed Z-sets** (`IndexedZSet`, `IndexedZSetBuilder`, outer map and enumerator).
4. Whatever `IVM_LOH_ALLOC` still shows (the `KeyValuePair[]` / `object[]` producers).

## 5. Gates and instruments

- **Correctness:** suite green with the seam off and on; ivm-bench SF=3 view digests identical at
  W=1 and W=8; snapshot files byte-identical (same order by construction).
- **It did what it says:** `IVM_LOH_ALLOC=1` shows no `Entry<…>[]` on the LOH;
  `DBSPNET_DELTA_SIZE_PROFILE` largest chunk < 85 KB.
- **Batch 1 (the prize):** drain-only young pause and drain time, ≥ 5 alternating runs per arm at
  W=8. Target: the 16 MB-threshold result (−2.6 s drain). **Keep if it reaches at least half of
  that without the runtime setting**; then also measure it combined with the setting (anything left
  for the knob is the follow-up's size).
- **Nexmark:** throughput at W=1 and W=8, alternating processes; geomean not worse, and compared
  with the +10% / +20% the setting gave.
- **Fine-tick cost:** w1profile B/ev will *change* (power-of-two capacities, different growth), so
  it is reported, not required identical; the micro-benchmark gate is ≤ 5% slower than `Dictionary`
  at every size.

## 6. Decisions for Curt

(a) One structure everywhere (proposed) or large-only with a migration step?
(b) If it lands, also ship `System.GC.LOHThreshold` as belt and braces for the hosts we own
    (server, benchmarks), or rely on the structure alone?
(c) Start with increment 1 + 2 (flat Z-sets, ~80% of the LOH entry bytes) as one measured step?

## 7. Increments 1 + 2 — built and measured (2026-10-30)

Commits `c981a0f`, `dc7d5b3`. `ChunkedMap<TKey,TValue>` (src/DbspNet.Core/Collections/ChunkedMap.cs);
`ZSet`, `ZSetBuilder` and the flat `ZSetTrace` take it when `DBSPNET_ZSET_MAP=chunked`. Indexed
Z-sets' outer maps are still `Dictionary` (increment 3); their inner per-group Z-sets are chunked.

**Correctness.** Differential test against `Dictionary` — random add/set/remove/clear sequences
up to ~25K live keys and a 1M-key fill, contents and enumeration order after every block, and the
compacting copy — passes; every array of a 1M-entry map is under 85,000 B. Suite 2640/2640 with the
seam off and on. ivm-bench SF=3: all 16 digests identical at W=8 in every run.

**Default off is byte-identical** on w1profile B/ev (all ten queries, against `9935319`) — after a
fix: a second field on `ZSetBuilder` had cost q20/q4/q9 +1–3 B/ev; the builder now keeps one
`object` store field, as `ZSet` does. With the seam **on**, B/ev rises 0.7–6.5% (q3 62 → 66, q9
+4.7%): power-of-two capacities and the 0.5 load factor below.

**Micro-benchmark** (StructuralRow keys, chunked ÷ Dictionary time, median of 9):

| entries | add | lookup | enumerate |
|---|---|---|---|
| 1K | 0.59 | 0.76 | 1.00 |
| 100K | 0.74 | **1.30** | 0.96 |
| 1M | 0.71 | **1.16** | 1.01 |

The §5 gate (≤ 5% slower everywhere) is **missed for lookup in large maps**. Unchecked two-level
access, a chunk-at-a-time enumerator and a 0.5 load factor brought it down from 1.4–1.8×;
single-array buckets made no difference, so the bucket index is not the cause. The rest is not
explained. Small maps — the fine-tick case — are faster than `Dictionary` on every operation.

**ivm-bench SF=3 batch 1, W=8**, 5 alternating runs, drain only:

| | Dictionary | ChunkedMap | Δ |
|---|---|---|---|
| young pause | 6.57 s (sd 0.14) | **5.69 s** (sd 0.19) | **−0.87 s** (SE 0.11) |
| drain, mean | 19.03 s (sd 0.40) | 17.60 s (sd 1.71) | −1.44 s (SE 0.78) |
| drain, median | 19.15 s | 16.49 s | −2.66 s (four runs at 16.5 s, one at 20.3 s) |
| background gen2s | 7–9 | 5 | |

`Entry<StructuralRow,Z64>[]` (6.0 GB) is gone from the LOH. The keep-gate (≥ half of the 16 MB
setting's −2.6 s drain) is met on the mean, noisily. What stays on the LOH (one `IVM_LOH_ALLOC` run):
`KeyValuePair<StructuralRow,Z64>[]` 1.28 GB, `object[]` 0.75 GB, indexed outer maps
`Entry<StructuralRow,ZSet<…>>[]` 0.51 GB, `Entry<StructuralRow,ZSetBuilder<…>>[]` 0.41 GB — which is
why 5 background gen2s remain where the 16 MB setting had none.

**Nexmark** (`nexmark 1000000 10000 3 8`, 3 alternating processes per arm, median ratio, events/s):

| | q0 | q1 | q2 | q3 | q4 | q9 | q5 | q7 | q8 | q12 | q15 | q16 | q17 | q18 | q19 | q20 | q22 | geomean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| W=1 | 1.09 | 1.37 | 1.11 | 1.03 | 1.22 | 1.18 | 1.33 | 1.17 | 1.07 | 1.07 | 1.21 | 1.18 | 1.22 | 1.20 | 1.19 | 1.27 | 1.17 | **1.18** |
| W=8 | 1.28 | 1.33 | 1.12 | 1.03 | 1.19 | 1.13 | 3.00 | — | 1.00 | 1.28 | 1.09 | 1.10 | 1.00 | 1.09 | 1.02 | 0.91 | 1.09 | **1.18** |

Better than the 16 MB setting at W=1 (+10%), close to it at W=8 (+20%). Only q20 at W=8 is below
1.0 (0.91, inside its 1.27× spread). Nexmark's 10,000-event batches make ~240 KB maps, so it gains
both from staying off the LOH and from add being cheaper than `Dictionary`'s at those sizes.

**Next:** increment 3 (indexed Z-sets' outer map + `IndexedZSetBuilder`) and increment 4 (the
`KeyValuePair[]` / `object[]` producers — not yet located), each measured the same way; then the
default flip.

## 8. Increments 3 + 4 — built and measured (2026-10-30)

Commits `8de9260` (indexed Z-sets: `IndexedZSet` / `IndexedZSetBuilder` keep one `object` store
field read through `MapView`, a struct over either map; `IndexedZSet.GetEnumerator` returns its own
struct enumerator) and `145d7cf` (exchange buckets: the five exchange operators publish
`IReadOnlyList` buckets — `List<T>` with the seam off, unchanged; `ChunkedList<T>` with it on — and
gather through `ExchangeBuckets.Items`, an unboxed struct enumeration of either).

**Correctness.** Suite 2640/2640 off and on; ivm-bench all 16 digests identical in every run below.
Default off is still byte-identical on w1profile B/ev (all ten queries vs `9935319`); on, B/ev is as
after increment 2 (+0.9–6.5%; W=1 creates no exchange operators).

**LOH after all four increments** (`IVM_LOH_ALLOC=1`, one run, W=8): gone — `Entry<Row,Z64>[]`
6.0 GB, `KeyValuePair<Row,Z64>[]` 1.28 GB, indexed outer maps 0.51 GB, builder maps 0.41 GB. Left:
`byte[]` 1.48 GB (reference-free: never card-scanned), `object[]` 0.75 GB (**producer not
located** — no allocation site in the engine, Arrow or connector code matches by inspection, and
in-process AllocationTick events carry no stacks), `int[]` 0.31 GB (reference-free), the Delta output
connector's `Entry<object, List<RowAddress>>[]` 0.20 GB, and small non-Z-set operator-state maps.

**ivm-bench SF=3 batch 1, W=8**, 5 alternating runs, drain only:

| | Dictionary | ChunkedMap (1–4) | Δ |
|---|---|---|---|
| drain | 19.97 s (median 19.40, sd 1.20) | **16.89 s** (median 16.29, sd 1.14) | **−3.08 s** (SE 0.74) |
| young pause | 6.55 s (sd 0.42) | **5.53 s** (sd 0.17) | **−1.02 s** (SE 0.20) |
| background gen2s | 7–9 | 2–5 | |

The keep-gate (≥ 1.3 s drain without the runtime setting) is met with margin. (This session's
baseline ran slower than earlier ones — 19.97 s vs ~18.4–19.0 s — so compare within the table, not
against §13.1 of the paged-store doc.)

**Nexmark** (3 alternating processes per arm, median ratio, events/s), W=1 / W=8:

q0 1.08/1.05 · q1 1.35/1.38 · q2 1.12/1.10 · q3 1.02/1.06 · q4 1.24/0.96 · q9 1.18/1.03 ·
q5 1.42/2.42 · q7 1.14/— · q8 0.99/1.03 · q12 1.11/1.11 · q15 1.21/1.32 · q16 1.19/1.06 ·
q17 1.22/0.99 · q18 1.27/1.28 · q19 1.20/0.90 · q20 1.21/0.99 · q22 1.11/1.12 —
**geomean 1.18 (W=1), 1.14 (W=8)**.

W=8 is slightly below increment 2's 1.18 (q19 0.90, q4 0.96). Single W=8 cells on this box spread
up to ~2.7× across processes, so this is not attributed to increment 4 without more runs; a W=8
A/B of increment 4 alone (exchange buckets on vs off) would settle it.

**The `object[]` producer, located** (`dotnet-trace` with GC verbose events attached to the test
host, the process held after batch 1 by `IVM_PAUSE_DIR` so the trace ends cleanly; AllocationTick
stacks totalled by a throwaway TraceEvent analyzer). Of the 0.75 GB of LOH `object[]`:
~66% is `ArrowExtensions.ToArrowChanges` — the incremental **output** writer
(`ProgramRunner.WriteIncrementalAsync`), which copies every cell into a `List<object?>` per column
and `ToArray()`s it again before building Arrow arrays; ~25% is `ArrowColumns.Extract*` in
`PushArrow` — the **input** decode, a boxed `object?[n]` per Arrow column before rows are assembled.
Both are boxed column staging buffers at the Arrow boundary, not engine state. The remaining LOH
`byte[]`/`int[]` is also output-side and reference-free (Arrow buffer builders growing, MemoryStream,
the Parquet encoder). So inside the drain the only reference-holding LOH producer left is the input
decode (~0.2 GB); the output writer runs after the drain, outside every drain-only number above.

**Open:** (1) the Arrow-boundary staging buffers — input decode row-major (no per-column boxed
arrays; inside the drain) and the output writer appending rows straight into Arrow builders (after
the drain, but batch 1 spends ~3.7 s in output); (2) large-map lookup still 1.16–1.30× Dictionary (§7); (3) the
default flip — the seam is still off; (4) the unexplained short test totals (2561, 2599 of 2640,
both "Passed!", not reproduced in seven reruns — suspected test-host abort hidden by the summary
grep; abort lines are now checked).

## 9. The Arrow boundary — input decode in blocks, output built from rows (2026-10-30)

Commit `d71c6eb`. **Input:** `ArrowColumns.Extract` gains a row offset; the parallel ingest
(`ShardedTableInput.PushColumnBlocks`) has each worker row range decode its rows in 4,096-row
blocks, and the serial `DecodeCore` does the same, so no whole-batch boxed `object?[n]` column is
allocated (batches with zero-weight rows keep the whole-column decode — rare, and their surviving
rows are not contiguous). **Output:** `ToArrowChanges` / `ToArrowDelta` / `ToArrowView` build each
Arrow column straight from the row list through a struct cell source (the per-column
`List<object?>` + `ToArray()` staging is gone) and reserve builder capacity up front.

**Correctness.** A new ingest test checks the decoded table against values read directly from the
batch's Arrow accessors (a serial-vs-parallel comparison cannot catch an offset bug both paths
share); an injected offset bug makes it — and the existing ingest tests — fail. Suite 2642/2642
off and on. ivm-bench: all 16 digests identical; the written Parquet is identical per view in
schema and row multiset (pyarrow), with only the `created_by` build stamp differing.

**ivm-bench SF=3 batch 1, W=8, chunked on**, before (`f0f6be7`, built in a worktree) vs after,
5 alternating runs:

| | before | after | Δ |
|---|---|---|---|
| output (materialise + Delta write) | 5.66 s | **3.58 s** | **−2.09 s** (SE 0.78) |
| drain | 17.07 s | 17.28 s | +0.21 s (SE 0.66) — noise |
| background gen2s | 3–5 | 1–2 | |

A first run of the same A/B showed drain **+1.76 s** (SE 0.22); it did not reproduce. A same-build
A/B of the input decode alone (old whole-column decode behind a temporary switch, since removed)
gave drain +0.20 s (SE 0.56) and young pause +0.15 s (SE 0.08) — the block decode is neutral on
the drain. The input side's value is the LOH residency it removes, not step time.

## 10. Default on (2026-10-30)

`ZSetMapMode.Chunked` is now true unless `DBSPNET_ZSET_MAP=dictionary`; a test pins the default.
Suite 2643/2643 with the default and with the opt-out. With no environment set: ivm-bench SF=3
digests identical at W=8 and W=1, and `Entry<StructuralRow,Z64>[]` on the LOH in batch 1 is
0.02 GB (W=8) / 0.03 GB (W=1), down from 6.0 GB — the remainder is code that still builds a
`Dictionary` directly (the spine traces, the paged store's snapshot form).

**New fine-tick baseline** (w1profile B/ev, default config): q0 672, q1 789, q2 526, q22 899, q3 66,
q20 1080, q4 1991, q9 939, q18 1132, q19 1007 — +0.9% to +6.5% over the Dictionary numbers, as
measured in §7. `docs/w1-profile.md` still records the Dictionary-era table; future B/ev A/Bs
compare against these.

What the default buys, from §7–§9: ivm-bench batch 1 at W=8 drain −3.1 s and young pause −1.0 s,
output write −2.1 s; Nexmark +18% (W=1) / +14% (W=8) geomean.
