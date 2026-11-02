# Flat trace state: trace state as reference-free blocks

Status: **phase 1 (census) done, phase 2 (this design) for review.** Branch `arc/flat-trace-state`.
Related: design-durable-state.md §1f (runs, lazy restore, "Giant groups decoded without sharing"),
design-flat-state-probe.md (q20), design-row-representation.md §17/§20/§23/§27,
comparison-feldera-decisions.md.

## 0. Thesis and scope

The throughput thread and the checkpoint thread both ended at "fewer managed objects", but about two
populations:

- **step throughput** — short-lived churn (a fresh Z-set per operator per tick, projected rows);
- **restore, checkpoint, first batch after restore** — long-lived state, decoded into objects,
  promoted, then re-marked by every GC.

This arc is about the second: **hold trace state as reference-free blocks in the run format**
(v3, with `__hash`), an in-memory LSM of immutable blocks plus a small managed hot part, merged off
the step thread the way `RunCompactor` already merges on disk.

There is also a step-throughput reason, from §27.6: about **48% of the batch-1 young-GC pause is
scanning the dirty cards of large trace hash tables on the LOH, twice per GC**. A trace whose bulk
holds no references is never card-scanned.

Out of scope: per-tick churn (Layer A), GC tuning, pooling state objects, typed struct state
(§23: +26% live).

## 1. Phase 1 — census (2026-11-01)

Instrument: `IVM_STATE_CENSUS=1` (`tests/.../Scratch/StateCensus.cs`, commit 75ada67). It is a
reflection walk of the live graph. Every object is charged to the operator field, three levels
deep, that first reaches it, with replicas and operators walked in registration order. It reports
objects, bytes, reference slots (the edges a full mark walks) and ref-free bytes. In a resumed run
it also reports the objects absent from a walk taken right after restore, with their generation.

Setup: ivm-bench SF=3, W=12, runs layout, M4 Pro. One census run per point (the counts are
deterministic) and one plain GC-events run.

### (a) After batch 1: 168M objects, 10.8 GB, 592M references (14% of bytes ref-free)

| structure | operator kind | objects | bytes | references (share) |
|---|---|---|---|---|
| left trace (`IndexedZSetTrace`) | IncrementalInnerJoin | 53.6M | 3.49 GB | 200M (**33.9%**) |
| `_window`: per-partition dict of widened output rows | WindowAggregate | 10.7M | 0.97 GB | 67M (11.4%) |
| `_trace` | IncrementalAggregate | 21.3M | 1.15 GB | 55M (9.2%) |
| `_accum`: per-partition `SortedDictionary` | WindowAggregate | 17.9M | 0.96 GB | 52M (8.7%) |
| `_window` | WindowOffset | 6.0M | 0.64 GB | 50M (8.4%) |
| `_view` (`ZSetTrace`) | IntegratedView | 12.2M | 0.76 GB | 43M (7.2%) |
| right trace | IncrementalInnerJoin | 20.5M | 1.03 GB | 36M (6.1%) |
| `_accum` | WindowOffset | 7.0M | 0.51 GB | 35M (5.9%) |
| output connectors' position maps (runner, not operators) | — | 5.9M | 0.43 GB | 15M (2.5%) |
| `_stateCache` + `_aggCache` | IncrementalAggregate | 6.1M | 0.41 GB | 14M (2.3%) |
| left trace | IncrementalLeftJoin | 2.5M | 0.17 GB | 10M (1.7%) |
| `_trace` | Distinct | 2.5M | 0.18 GB | 10M (1.7%) |

By kind: **join traces 43% of references** (4.7 GB), **partitioned window state 36%** (3.1 GB),
aggregates 12%, views and DISTINCT 9%.

Join-trace rows cost 3.3 objects, 12 references and 216 B each. Boxes are heavily shared: 3.4M
`Utf8String` boxes carry 49.7M references.

### (b) Fresh restart (`IVM_STOP_AFTER=1`, then `IVM_RESUME=1`)

- **After restore:** 2.9M objects, 0.19 GB.
- **After the first batch's drain:** 53M objects (1.88 GB) created and kept.
  - **97.7% of it is in three inner-join left traces.** These are the giant groups (more than
    4,096 entries), decoded in the background at restore and taken by the first probes.
  - They were decoded with `StringDecoding.Copy` and one box per cell: 28 objects and 980 B per
    row. This is now fixed separately (design-durable-state.md, "Giant groups decoded without
    sharing", branch `arc/giant-group-arena`): arena strings bring it to 1.55 GB / 41M objects.
- **After the output write:** another 51M objects (2.2 GB), all outside operator state.
  - 1.87 GB is in the output handles materialising views; 0.33 GB is the Delta position map.
  - Not this arc.
- **GC pauses** (plain run):
  - drain: gen0 38 ms (promoted 1.26 GB), gen1 91 ms (1.74 GB, plus 1.27 GB into gen2);
  - output write: gen0 64 ms, plus a 131 ms suspension;
  - restore: four blocking gen2 GCs, 98 ms;
  - 429 ms over the whole batch.

### What the census decides

1. **The first operator is the join trace.** It holds 43% of references in steady state and,
   after a restart, essentially all the promoted state.
2. **Window state is second (36%), but its biggest structure is a cache.** `_window` holds a
   widened output row per input row. The question there is whether to store outputs at all
   (cf. arc/window-outputs), not their layout. Not in this design.
3. The output write's 2.2 GB is connector-side. Not this arc.

## 2. Design

### 2.1 What Feldera does, and where we differ

Feldera's spine is an LSM of immutable sorted batches with background merging. Readers use cursors
(gallop / seek), with no hash map on the row path.

**In memory its batches are not in the on-disk format.** They are tries of native typed arrays
whose string cells are refcounted `ArcStr` pointers. Spilling re-encodes through a builder, and file
reads decode each probed key (rkyv `deserialize_with`). Their reference-heavy in-memory layout costs
them nothing, because Rust has no tracing GC.

In .NET the same layout (`StructuralRow[]` columns) would keep every reference. So we diverge
deliberately: **in-memory blocks use the on-disk byte layout, and strings live in an offset heap.**
A side benefit is that we can compare an encoded key against a probe without decoding it, which
Feldera cannot.

### 2.2 Block layout: the v3 run block, held as a managed `byte[]`

A **block** is exactly one v3 run block: the Arrow IPC RecordBatch message for up to 1,024 entries
(`RunFile.DefaultBlockEntries`), cut only where the hash changes.

- **Indexed trace columns:** key columns, value columns, `__weight` (Int64), `__hash` (Int32,
  the key's `StructuralRowHash`). Sorted by unsigned `__hash`.
- **Locality** (the q20 probe's lesson):
  - a key's entries are contiguous and never straddle blocks;
  - a giant group is one oversized block;
  - the block's (first, last) hash is its index entry.
- **In memory:** the block's IPC body sits in **one managed `byte[]`**. Columns are
  (offset, length) views into it:
  - fixed-width columns are read with `MemoryMarshal.Cast`/`Read`;
  - VARCHAR is int32 offsets plus a UTF-8 heap;
  - validity is a bitmap.
- **References per block:** the `byte[]` plus a small descriptor (column offsets, entry count, hash
  range). About 3 objects per 1,024 entries, against today's ~3.3 objects per entry.

We do not use Arrow's own array objects in memory. IPC reads back native buffers, and aliasing them
measured +152% on the hash pass (design-incremental-persistence §11.4). Over a managed `byte[]`,
`Span` access is a plain array slice. **Phase-3 check:** see whether `ArrowStreamReader` over a
`ReadOnlyMemory<byte>` aliases array-backed memory without copying. If it does, we use it to parse
block metadata. If not, we parse the RecordBatch flatbuffer (buffer offsets and lengths) ourselves.

### 2.3 The trace: levels

`BlockedIndexedTrace` (structural rows only: `StructuralRow` key and value, `Z64`) is an ordered
list of **levels**, newest first. A level is one of:

- **managed** — an `IndexedZSet` exactly as a tick produced it. **Integrate pushes the tick's delta
  as a new managed level; nothing is copied.** The trace takes ownership, so a pooled delta
  (`DeltaPoolMode`) must be cloned first, or pooling disabled for these edges.
- **run** — immutable sorted blocks, with an in-memory hash index per run (first hash per block)
  and a Bloom filter (10 bits per key, the v3 footer's own filter).

Each level is immutable once pushed. Only the background merger replaces levels, and the step
thread installs its results between ticks (at `Step` entry), never during one.

**Background merger** (per worker, bounded: a shared pool of `min(2, cores − W)` merge threads, like
`RunCompactor`'s `SemaphoreSlim(2)`):

- **Freeze:** sort a managed level by key hash, then by value hash, and encode it into blocks.
- **Merge:** k-way merge of adjacent runs by `__hash`. Within equal hashes, entries are matched by
  key and value equality, weights summed, zeros dropped. This is RunCompactor's merge, over memory
  instead of files.
- **Policy:** geometric, merging when a level reaches about 4× the size of the next newer one,
  aiming for at most about 6 levels. This is a starting point to measure, not a decision.
- **Backpressure:** if more than K managed levels are waiting to freeze (start with K = 8), the step
  thread freezes the oldest itself, so memory and probe depth stay bounded.

### 2.4 Serving probes and group reads

A join probe needs, for a delta key, every (value, weight) of that key in the trace. **Because the
join is bilinear, the probe does not need a consolidated group:** a value at +1 in one level and −1
in another produces output rows that cancel in the output `ZSetBuilder`. So a probe just visits the
key in every level. Feldera's `CursorList` does the same. Merging consolidates over time.

Per level:

- **managed:** `GroupFor(key)`, as today.
- **run:**
  1. range check and Bloom check;
  2. binary search of the block index by hash;
  3. binary search of the block's `__hash` column for the equal-hash range;
  4. for each entry in that range, **compare the encoded key with the probe key column by column,
     without materialising it**;
  5. matching entries form a contiguous range: the key's group.

  Collisions (32-bit hash) are handled by step 4.
- **Batched probes:** a large delta (batch 1, catch-up) sorts its keys by hash once and walks each
  run in order, galloping, Feldera-style. This replaces one binary search per key with a sequential
  pass.

**Encoded-key equality must agree exactly with `StructuralRow.Equals`** (e.g. how doubles and NULLs
compare). It is a fourth implementation next to the three row-hash implementations (memory
row-hash-owned). It needs its own property test against `StructuralRow.Equals` over every engine
type, including NULL, ±0.0, NaN, and empty and non-ASCII strings.

**The row boundary.** The join's `combine(key, left, right)` takes `StructuralRow`s, so the values
read from a run are materialised: a `StructuralRow`, an `object[]`, and boxes for the cells. Strings
are boxes over slices of the block's heap: an alias into a managed array, with no copy. This is gen0
churn, which the brief accepts, but it is a real per-row cost the steady-state trace does not pay
today (its rows are shared, never rebuilt). It is the arc's main step-time risk; §3 gates it.
A later lever, not in the first cut: a combine that reads cells from the run straight into the
output row's `object[]`, skipping the intermediate `StructuralRow`.

`JoinInto` today enumerates the *smaller* side. A run-backed trace is always probed, never
enumerated, on the step path. Enumeration (snapshot fallback, `GroupCount`, metrics) walks the
levels.

### 2.5 GC / LATENESS

`DropKeysBelow(threshold, monotoneKey)` records the frontier and does not scan anything.

- Probes return nothing for a key below it.
- Freeze and merge drop entries below it (Feldera applies its filters only in merges, the same way).
- Retained-row metrics count the entries not yet dropped. They overstate until a merge, as
  Feldera's do.

### 2.6 Restore adopts blocks without decoding

A restored chain (`runs.txt`) becomes run levels directly:

1. the footer is read as today (`RunReader`: block index, Bloom, schema);
2. each block's bytes are read into a `byte[]` **on first probe** (lazy, as Feldera's buffer cache
   faults blocks in), or by a low-priority background prefetch;
3. **no block is ever decoded into rows on the restore path.**

Giant groups need no special case and no background decode: a giant group is a block whose bytes
are read when first probed.

### 2.7 Checkpoint writes blocks raw

- **Every run level has a file.** A freeze or merge output is written by the merger (background)
  as a v3 run file, then kept in memory as that file's bytes. Memory and disk hold the same bytes.
- **A checkpoint is then:**
  - freeze any managed levels, which are the changes since the last freeze;
  - write them as run files (small);
  - write the `runs.txt` manifest listing every level's file.
- This is Feldera's checkpoint (a manifest of already-durable batch files). It replaces
  `CheckpointTracking.Pending` for converted traces: the managed levels are the pending changes.
  Pending is itself a retained copy today (census: `ZSetChangeTracker` Pending).
- The raw-copy writer already exists (`RunStreamWriter.AppendRawAsync`, used by RunCompactor).

### 2.8 Interaction with the existing lazy-restore machinery

| piece | for a converted trace |
|---|---|
| `IColdGroups` / `RunColdGroups` | **Not used.** A cold block is a run level whose bytes are not loaded yet. Unconverted traces keep using it. |
| `ColdPromotion` (the delta ≥ 1/64 of cold rows rule) | **Not needed.** Reading a run whole is a sequential byte read with no decode, so it reduces to "prefetch every block of this run". |
| giant-group background decode | **Gone** (§2.6). |
| `RunPins` | **Still needed.** Run levels reference files, and RunPins keeps the cleanup sweep from deleting a file a live level still uses. Merges release their inputs' pins when installed. |
| `RunCompactor` | Merging moves into the trace's own merger (§2.3), and the merger writes the files. On-disk compaction stays for unconverted chains. |
| re-shard at restore (inc. 3 / 7a) | **Not supported in the first cut.** Re-shard decodes and re-partitions entries; a converted trace refuses re-shard, as spine does today, until it is taught to split runs by `hash % W`. |
| eager restore (`DBSPNET_RESTORE=eager`) | Reads every block's bytes up front; still no decode. |

### 2.9 Expected effect, to be measured

**After batch 1:**

- Join traces go from 247M references and 77M objects to about 3 objects per 1,024 entries.
- The join traces' `Entry[]` tables on the LOH (§27.6's card-scan cost) disappear.
- Bytes are estimated at 120–160 B per entry against 216 B today. Rows no longer share boxes, but
  short strings cost their length plus 4 B instead of an 8 B reference plus a share of a box. This
  is an estimate; the census measures it.

**First batch after restore:** no decoded join state is retained. The trace keeps its restored
blocks and adds the batch's delta as a managed level. The 1.55 GB that the drain now retains should
go to roughly the size of the delta.

**Step:**

- Probes cost a few binary searches per level instead of one hash lookup.
- Every trace row read is materialised.
- Freeze and merge use background CPU, which at W=12 on 14 cores is contended.

These costs are why §3 gates step time before anything widens.

## 3. Phase 3 — first operator: the inner join's traces, behind a flag

**Flag:** `DBSPNET_TRACE_BLOCKS=join`, with a `.Use()` seam for tests.

- It covers `IncrementalJoinOp` traces with structural keys and values (both sides, since one class
  serves both).
- It does not cover typed traces, left/full outer joins (they read old groups for null padding, the
  next step), aggregates, `ArrangeOp` shared traces, or spine.
- It is refused under re-shard, as spine is.

**Build order:**

1. The block reader (column views over a `byte[]`, encoded-key equality) and its property tests:
   encoded equality agrees with `StructuralRow.Equals` and hash agrees with `StructuralRowHash`, over
   every engine type and NULLs.
2. `BlockedIndexedTrace`: levels, probe, freeze, merge (synchronous first), frontier.
   Property test: equivalent to `IndexedZSet` under random interleavings of integrate, probe,
   frontier drops and enumeration, including forced hash collisions (a seam that maps the hash to
   a few buckets).
3. The background merger and install-at-step.
4. Checkpoint (run files plus manifest) and restore adoption.
5. Wire it into `IncrementalJoinOp`, and run the full suite with the flag on, under runs layout
   off, on, and verify (`DBSPNET_STATE_LAYOUT=runs`, `DBSPNET_RUNS_VERIFY=1`). Mutation-check the
   new tests.

**Measurements** (5 alternating runs per arm, flag off vs on, clean builds, nothing else running):

| measurement | instrument | gate |
|---|---|---|
| per-row work | `w1profile` B/ev, all 10 queries | reported per query; join queries (q3, q4, q8, q9, q20) are expected to rise from materialisation, and the size is the information |
| batch-1 step | ivm-bench SF=3 W=12 batch-1 drain | **median no worse than +3%** (the widening gate) |
| batch-1 GC | `IVM_GC_EVENTS` young-pause total, batch 1 | expected down (§27.6); reported |
| retained state | `IVM_STATE_CENSUS` after batch 1 | join-trace references −90% or better |
| restart | fresh restart (`resume.sh`): restore, first drain, output write | restore ≤ main; first drain ≤ main; drain GC pauses down; output write unchanged |
| checkpoint | batch-2/3 checkpoint ms, MiB written | no worse than main |

**Widen only if the batch-1 drain gate holds.** If it fails, the first question is which part
costs: probe depth (levels), materialisation, or merger contention. Each has its own instrument (the
level-count histogram, `DBSPNET_PROFILE` per-operator ms, merge-thread CPU).

**The order for widening after a pass:** outer joins, then aggregate traces (these need a
consolidated group, so their reads merge levels by value hash, which is why freeze sorts values by
hash within a key), then flat `ZSetTrace` views and DISTINCT.

## 4. Open questions for review

1. **Freeze on the step thread vs background.** The design defers it (managed levels pile up and
   the merger freezes them). The simpler first cut freezes synchronously at checkpoint and when hot
   exceeds a threshold. Is a synchronous first cut acceptable as phase 3's step 2, with the merger
   in step 3?
2. **Memory vs residency.** Everything stays resident as bytes. Evicting cold blocks (bounded
   memory, Feldera's buffer cache) is a natural follow-on but out of scope.
3. **Pooled deltas.** Do we take ownership and disable `DeltaPoolMode` reuse on trace-feeding
   edges, or clone? The census shows deltas are not retained today, so cloning would add a copy per
   tick.
