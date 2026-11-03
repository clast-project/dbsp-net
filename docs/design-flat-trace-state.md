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

### 3.0 De-risk first: the row boundary, measured (2026-11-01)

Before building the trace, a microbenchmark priced §2.4's main risk (`FLAT_PROBE_BENCH=1`,
`tests/.../Scratch/FlatTraceProbeBench.cs`). It runs one join probe pass, delta ⋈ trace, through
the structural `MergeRows` combine and a `ZSetBuilder`, over the same trace held two ways. The
trace has 400k BIGINT keys, 1.6M entries, and 8 value columns (BIGINT, INT, 3 VARCHAR, DOUBLE,
DATE, DECIMAL). The block arms use v3 blocks encoded by the production codec, held in managed
`byte[]`s: 1,561 blocks, 170 MB. All three join arms produce identical Z-sets. Setup: 5 processes,
7 alternating rounds each, `DOTNET_gcServer=1`. The ns figures are the median of the per-process
medians.

| arm | small delta (2k keys) ns / B per output row | large delta (200k keys) ns / B |
|---|---|---|
| A: today (`IndexedZSet`, shared rows) | 233 / 204 | 237 / 199 |
| B: blocks, value rebuilt as a `StructuralRow`, then combined | 346 (+48%) / 545 | 333 (+41%) / 474 |
| B2: blocks, cells boxed straight into the output row | 302 (+30%) / 426 | 297 (+25%) / 421 |
| probe only, hash map (no rows built) | 60 / 0 | 48 / 0 |
| probe only, blocks (block index → `__hash` → key column) | 55 / 0 | 45 / 0 |

What this decides:

- **Lookup is not the risk.** Two binary searches over hash-ordered blocks match the hash map, and
  are slightly faster.
- **The cost is the boxes.** B2 adds about 220 B (five 24 B boxes and three 32 B `Utf8String`
  boxes) and about 65 ns per output row. B also pays a whole-row hash for the intermediate value
  row: `StructuralRow`'s constructor hashes eagerly.
- **B2 becomes the first cut,** not a later lever. The structural inner join's combine is a pure
  concatenation (`MergeRows`; pruning happens on the inputs), so the blocked join writes output
  rows itself.
- **Scale in batch 1.** A per-operator profile (`DBSPNET_PROFILE=1`, one run, W=12) shows inner
  joins at 21.1 s of about 112 s non-exchange operator CPU, allocating 8.9 GiB. That is about 45M
  output rows at today's ~200 B each. At +65 ns each this is about +2.9 s CPU, about +0.25 s wall
  across 12 workers, or +1.7% of the 14.8 s step, before any GC saving. Batch 1's GC pause in the
  same run was 6.1 s.
- **One case is worse: residual joins.** A pushed-down residual (`broker_performance`'s
  `IncrementalInnerJoin`: 5.1 s CPU, only 53 MiB allocated) evaluates the residual on every
  candidate pair. Under B2 each candidate would be boxed before the residual rejects it. Either
  evaluate the residual on cells read from the block, or leave residual joins on the managed
  trace in the first cut.

### 3.1 First cut built and measured (2026-11-02)

**What was built** (commits 9b2b809..eb783e9 on `arc/flat-trace-state`):

- **Block reader:** `TraceBlock`, with encoded-key equality held against the run decoder.
- **Trace:** `BlockedIndexedTrace`, with managed levels, runs, geometric merge and GC floors.
- **Operator:** `BlockedJoinOp`, which builds output rows straight from block cells, and its
  compiler wiring.
- **Encoder:** `BlockWriter`, a hand-written Arrow IPC block encoder.
- **Checkpoint and restore:** blocked checkpoints as v3 run chains written from blocks; restore
  adopts blocks lazily; snapshots are interchangeable both ways with the managed join
  (`IFingerprintIdentity`).

Freeze and merge are **synchronous** and there is no background merger. The full suite passes
three ways with the flag on.

**What the first measurement taught:**

- **Freezing was allocation-bound.** Through Arrow's builders it cost ~6.8 µs per entry in place,
  against 0.4 µs alone.
  - A 12-thread microbenchmark and section timers traced it to the large-object heap.
  - ivm-bench's giant groups made one-hash blocks huge: 1.27 GB in 7,785 blocks, most over 85 KB.
  - Fix: a hand-written encoder (one exact `byte[]` per block); blocks cut at ~60 KB; a giant
    group's hash may span blocks in memory.
  - On disk the v3 invariant still holds: such a span is re-encoded as one block when written.
- **Re-shard works unchanged.** The existing rewrite reads blocked chains as v3 runs, so the first
  cut does not need to refuse re-shard.

**Gates** (ivm-bench SF=3 W=12, 5 alternating runs per arm, clean build, all view digests
identical across arms):

| measurement | managed | blocked | gate |
|---|---|---|---|
| batch-1 drain, median (range) | 15,945 ms (15,583–16,751) | 15,976 ms (15,886–16,239) | ≤ +3%: **met (+0.2%)** |
| batch-1 drain GC pause, median | 5,118 ms | 5,081 ms | reported (unchanged) |
| batch-1 output write, median | 4,437 ms | 4,219 ms | reported |
| batch-1 checkpoint (first, whole) | 4.7–5.7 s | 8.4–15.5 s | **regressed** (see below) |
| restart: restore, median | 360 ms | 244 ms | ≤ managed: **met** |
| restart: first drain, median | 817 ms | 359 ms (−56%) | ≤ managed: **met** |
| restart: drain GC pause, median | 168 ms | 37 ms | down: **met** |
| restart: output write, median | 3,404 ms | 3,315 ms | unchanged: **met** |
| batch-2 checkpoint, median / written | 292 ms / 51.2 MiB | 298 ms / 51.1 MiB | no worse: **met** |
| census after the batch-1 checkpoint: inner-join references | 237.1M | 87.5M (−63%) | −90%: **met for converted joins, not overall** |

Notes on the misses:

- **Census.** The converted traces hold 180k objects and 270k references (1.55 GB, all
  reference-free). The remaining 86.7M references are joins that stayed managed: the residual
  joins the first cut leaves out (§3.0). Converting them is the next lever, by evaluating the
  residual on block cells.
- **First checkpoint.** It freezes the ~2/3 of join state still in managed levels at the end of
  batch 1 (traces under the 64k freeze threshold per worker). It does this under the checkpoint's
  `GCLatencyMode.Batch`, so the managed rows die in blocking gen2 GCs (gc_pause +3–10 s).
  Later checkpoints write only new runs (batch 2: 298 ms). Remedies to try: freeze those levels
  at the end of the drain or in idle time instead, or outside the Batch latency window.
- **w1profile B/ev** was not measured. w1profile compiles single queries, which take the typed path
  when they can, so the blocked join probably never engages there. That is unverified.

### 3.2 Residual joins and the first checkpoint (2026-11-02)

**Residual joins.** These now use `BlockedJoinOp`. The compiled residual reads cells through
`IReadOnlyList<object?>`'s indexer, so it runs on one reusable view of each candidate pair. That
view boxes only the cells the residual reads, from a block, and a rejected pair builds no row.

**First checkpoint.** Two causes, found with section timers:

1. **Levels still managed at the end of the batch froze inside the checkpoint's batch GC latency
   mode.** A join now freezes its managed levels on an idle tick, when both input deltas are empty.
   19.4M entries freeze during batch 1, against 9.2M before.
2. **Writing giant groups was expensive.** A giant group whose hash spans blocks in memory was
   decoded and re-encoded to keep v3's one-hash-per-block rule on disk: 36 s summed, because 16M
   entries sit in giant groups. `BlockWriter.Concat` now joins the blocks at the byte level: 3.3 s.

**Gates, re-measured** (5 alternating runs per arm, clean build, view digests identical):

| measurement | managed | blocked |
|---|---|---|
| batch-1 drain, median (range) | 16,167 ms (15,768–16,214) | 16,269 ms (16,087–16,844): **+0.6%, met** |
| batch-1 drain GC pause, median | 5,205 ms | 5,345 ms |
| batch-1 output write, median | 5,075 ms | 3,663 ms (−28%) |
| batch-1 checkpoint (first, whole), median | 4,678 ms | 3,583 ms (−23%): **met** |
| restart: restore, median | 352 ms | 252 ms |
| restart: first drain, median | 746 ms | 363 ms (−51%) |
| restart: drain GC pause, median | 156 ms | 39 ms |
| restart: output write / batch-2 checkpoint | 3,391 / 277 ms | 3,387 / 286 ms |
| census after the batch-1 checkpoint: inner-join references | 237.1M | 6.47M (**−97.3%, met**) |
| census: whole heap, references / objects / bytes | 592M / 168M / 10.8 GB | 385M (−35%) / 151M (−10%) / 10.8 GB |

The heap-wide object and byte counts barely move. Rows the join traces held are mostly still
reachable from other operators (views, aggregates, window state), so freeing them needs those
converted too: the widening order of §3. The remaining 5.5M join references are in joins that
stayed managed (a column type without a block encoding, or a typed or spine path); not yet
attributed.

### 3.3 Outer joins (2026-11-02)

**Design.** The left and full outer join operators reach their traces through `IJoinTrace`:

- `ManagedJoinTrace` is the `IndexedZSetTrace`, as before.
- `BlockedJoinTrace` is used under the flag. An outer join's match test needs a key's consolidated
  group, so its `GroupFor` probes every level and sums the entries by value. These groups are
  decoded, unlike the inner join's cells-into-output path; outer-join probes touch only the keys a
  tick touches.

The operator types are unchanged, so snapshots, fingerprints and the typed compiler's reflected
builders are untouched. All five outer-join compile sites are covered: LEFT, RIGHT, FULL, the
scalar subquery, and the correlated scalar join.

**Gates** (5 alternating runs per arm, clean build, view digests identical):

| measurement | managed | blocked |
|---|---|---|
| batch-1 drain, median (range) | 15,945 ms (15,716–16,467) | 16,236 ms (15,944–16,408): **+1.8%, met** |
| batch-1 output write / first checkpoint, median | 4,058 / 5,479 ms | 3,689 / 3,436 ms |
| restart: restore / first drain / drain GC pause, median | 334 / 987 / 203 ms | 256 / 446 / 48 ms |
| census after the checkpoint: left-join references | 10.45M | 0.16M (−98.4%) |
| census: inner-join references | 237.1M | 6.47M |
| census: whole heap, references / objects / bytes | 592M / 168M / 10.8 GB | 374M (−37%) / 150M / 10.8 GB |

Drain went from +0.6% (§3.2) to +1.8%. Both runs are within each other's ranges, but the trend
is the decoded group read; it is the thing to watch as more operators convert.

### 3.4 Aggregate traces (2026-11-02, branch `arc/flat-trace-agg`, not merged)

**Design.** `IncrementalAggregateOp` reaches its input trace through `IGroupTrace`; this is the
outer joins' interface, renamed. The flag is now a list: `DBSPNET_TRACE_BLOCKS=join|agg|all`.

- **Consolidated read:** the before-group is the decoded, consolidated group.
- **Restore:** a blocked trace always restores lazily, so the per-group caches are rebuilt on
  first touch.
- **Frontier GC:** a blocked trace drops lazily and doesn't list the dropped keys, so the
  operator prunes its caches by scanning them.

**Measured** with three arms in one session (5 runs each, rotated order, view digests identical):

| median | off | join | all (join + agg) |
|---|---|---|---|
| batch-1 drain | 15,871 ms | 16,466 ms (+3.7%) | 16,809 ms (**+5.9%**; +2.1% over join) |
| batch-1 output write | 4,091 ms | 3,445 ms | 3,219 ms |
| batch-1 first checkpoint | 5,527 ms | 3,498 ms | 3,287 ms |
| batch-1 drain + write + checkpoint | 25.5 s | 23.4 s | 23.3 s |
| restart: restore | 338 ms | 238 ms | 250 ms |
| restart: first drain | 841 ms | 436 ms | 513 ms (+18% over join) |
| census: aggregate trace references | — | 57.9M | 0.12M |
| census: aggregate references (trace + caches) | — | 71.6M | 22.5M (−69%) |
| census: whole heap, references / bytes | 592M / 10.8 GB | 374M / 10.79 GB | 333M / 10.57 GB |

**The drain gate is missed.** The join-only arm measured +3.7% in this session, against +1.8%
and +0.6% in earlier ones. Between-session variation is visible, but within this session
aggregates add about 2% on top of joins.

- **Likely cause:** the aggregate decodes each touched key's whole group on every tick it is
  touched. Most aggregators (SUM, COUNT, AVG) only ask the after-group whether it is empty and
  what weight it gives the delta's values.
- **Next lever:** a lazy blocked group view (`IMultiset`) that answers `IsEmpty` and `WeightOf`
  from encoded value equality, and decodes only when an aggregator walks the group (MIN/MAX
  recompute).
- **Restart:** the +18% first drain over join is the same decode, in the first-touch cache rebuild.

**The remaining aggregate references are the caches** (`_aggCache`, `_stateCache`). They are
charged the key rows the trace used to reach first, so they are now the structure to convert.

### 3.5 The aggregate group view, and where the aggregate cost really is (2026-11-02)

**Built.** `BlockedGroupView` gives an aggregate its before-group without decoding:

- `WeightOf` matches by value hash, then compares the encoded value. Hashes are stored per
  block: written at freeze, computed once for restored blocks.
- `SumWeights` reads the stored weights.
- `IsEmpty` stops at the first non-zero net.
- Enumeration decodes the group once.

It is exact, including where equal cells hash differently (NaN payloads). Property tests and
mutations pass.

**It did not move the drain** (three arms, 5 runs each):

| median | off | join | all |
|---|---|---|---|
| batch-1 drain | 16,060 ms | 16,183 ms (+0.8%) | 16,595 ms (+3.3%; **+2.5% over join**) |
| batch-1 output write / first checkpoint | 5,066 / 4,698 ms | 3,450 / 3,489 ms | 3,492 / 3,180 ms |
| restart: first drain | 897 ms | 441 ms | 519 ms |
| census: aggregate references | — | 71.6M | 22.6M |

**Why: the §3.4 diagnosis was wrong.** SQL's SUM, COUNT, AVG and MIN/MAX are all incremental: they
call `WeightOf` on the delta's rows and never walk the group, so decoding was not the cost. A
per-operator profile (one run per arm) plus block counters shows aggregates at 9.4 → 14.7 s CPU
and 1.75 → 5.17 GiB allocated.

- **The cost is converting the aggregate traces into blocks:**
  - +5.5M entries frozen;
  - 2.26M entries rewritten by merges (+2.2 s CPU);
  - +1.2 s of encoding;
  - +1.1 GB of block bytes.
- **Why there is so much merging:** an aggregate gets a small delta every tick, so idle-tick
  freezes cut many small runs, which merge repeatedly.
- **The remainder** is the view's scans (my estimate: ~1.8 s CPU): a key-equality check per
  entry for `SumWeights`, and hash scans for `WeightOf`.

**Levers, not yet tried:**

1. Freeze aggregate traces less eagerly (a larger threshold, or only at checkpoint or on longer
   idleness), trading memory held in managed form during a batch for less encoding and merging.
2. A tiered merge policy with a larger fan-in (fewer rewrites).
3. A per-block "no hash collisions" flag, so a probed range needs one key comparison, not one
   per entry.

### 3.6 Lever 1, freezing aggregates less eagerly: tried, no gain (2026-11-02)

Separate aggregate settings were built behind env knobs: the freeze threshold, the managed-level
cap, and an opt-out for the idle-tick freeze. All were reverted.

**The threshold and level cap alone change nothing.** With the threshold at 64K, 1M or 10^9, the
profile is identical: 1,019 freezes, 60 merges, 2,264,138 merged entries. The checkpoint freezes
nothing, so the aggregate traces are frozen during the drain by idle ticks. A busy tick's delta
exceeds 64K, so with idle freezing off but the default threshold, the counts barely move (954
freezes, the same merges).

**Idle freeze off and no effective threshold** (4M entries, 64 levels) makes the drain profile
exactly the join arm's: 906 freezes, 48 merges, 47K merged entries. The aggregate freeze moves to
the checkpoint as one freeze per trace with no merges: 4.36M entries, 13.5 s CPU summed over
workers.

**Wall clock** (SF=3, W=12, batch 1, 5 alternating runs per arm, outputs identical, medians):

| median | join | all (current) | all + lazy aggregate freeze |
|---|---|---|---|
| drain | 16,338 ms | 16,475 ms (+0.8%) | 16,544 ms (+1.3%) |
| checkpoint | 3,511 ms | 3,129 ms | 3,721 ms |
| GC pause | 6,793 ms | 6,305 ms | 6,949 ms |

Spreads per arm are about 600–900 ms. The lazy freeze is no better on the drain and worse on the
checkpoint, so it was dropped. In this round `all` cost only +0.8% over `join`, against +2.5% in
§3.5. The aggregate drain cost is within run-to-run noise at five runs, and the 5.3 s of extra
aggregate CPU (≈0.4 s per worker) is at most a few percent of wall clock.

**Widen only if the batch-1 drain gate holds.** If it fails, the first question is which part
costs: probe depth (levels), materialisation, or merger contention. Each has its own instrument (the
level-count histogram, `DBSPNET_PROFILE` per-operator ms, merge-thread CPU).

**The order for widening after a pass:** outer joins, then aggregate traces (these need a
consolidated group, so their reads merge levels by value hash, which is why freeze sorts values by
hash within a key), then flat `ZSetTrace` views and DISTINCT.

### 3.7 Views and DISTINCT: plan (2026-11-02, branch `arc/flat-trace-zset`)

**Census (SF=3, W=12, after the checkpoint, joins and aggregates on blocks):** the stored output
views' `ZSetTrace`s hold 48.4M references (14.5% of the heap's), and DISTINCT's hold 10.2M (3.1%).
The window operators hold 68%, but they are a different structure (per-partition sorted state) and
out of scope here. Some of a view's rows may also be held by the incremental output connector
(`ProgramRunner`, 4.4%, which keys on the written rows). The census after the change shows how
much of the 14.5% is really freed.

**Shape.** Both operators keep a consolidated Z-set (row → weight). DISTINCT asks
`WeightOf(row)` once per delta row; a view integrates each delta and is read whole (truncate sinks,
tests, the parallel `CurrentView`), counted (`Count`), and asked `WeightOf` (the parallel
`RowCount`).

**Design.** A `BlockedZSetTrace` over the existing `BlockedIndexedTrace`, with the **row as the
key and an empty value**:
- **Managed levels:** a level may be the tick's `ZSet` as it arrived (no indexed copy). Probes look
  the row up, and freeze sorts its rows by hash.
- **Runs:** unchanged blocks with zero value columns. Their column layout is the Z-set run's (row
  columns, `__weight`, `__hash`), so a checkpoint writes an ordinary **Z-set chain**. A managed
  trace restores it, and a blocked trace adopts a managed Z-set chain's blocks lazily, as joins do.
- **`WeightOf`:** a probe summed over levels.
- **`Count`:** computed on first read by materialising, then kept up to date by probing each delta
  row while anyone reads it (as the parallel `RowCount` does).
- **`Current` and enumeration:** materialised and cached until the next change.
- **GC:** level floors, as before.
- **Freeze:** at the threshold, and on an idle tick (an empty delta).

**Gate:** `DBSPNET_TRACE_BLOCKS=view,distinct` (and `all`); flat trace family and
block-encodable columns only.

**Measure:** the batch-1 drain and the first checkpoint (5 runs per arm against `join,agg`), the
output-write time (the view is read for the incremental connector's deltas only, but truncate
sinks materialise it), restart, and the census.

### 3.8 Views and DISTINCT: built and measured (2026-11-02, branch `arc/flat-trace-zset`)

**Built as planned (§3.7):** b72af15, c45d502, 7bd7117. Tests: a random-operation oracle against
`ZSetTrace` (drops, idle freezes, NaN payloads; enumeration must yield each row once with a
non-zero weight), SQL DISTINCT and UNION twins every tick, stored-view twins (contents, counts,
weights), and checkpoints in both directions with and without the runs layout. The random-query
oracle compiles a blocked DISTINCT in 326 of 1,500 queries. The suite passes 3,134 tests in each
of the four arms, and 11 mutations were tried: 10 caught, and 1 changed no behaviour.

**One regression found and fixed on the way.** After a restart, the first incremental write
enumerates each view whole, and it was 26% slower (3.45 → 4.36 s). It was not the decode count but
cell locality. The Arrow builder reads column-wise; the managed cold reader decodes a column at a
time into an arena, while blocks decoded a row at a time with each string copied. Over the same
rows with no GC, the Arrow conversion took twice as long. The full-view stream now merges runs by
hash and decodes each block whole on entry, column-major, with strings aliasing the immutable
block bytes. The parallel output decodes such shards in parallel (`EnumerationDecodes`).

**Wall clock** (SF=3, W=12, 5 alternating runs per arm against `join,agg`, outputs identical,
medians, ms):

| | join,agg | all |
|---|---|---|
| batch-1 drain | 16,631 | 16,595 |
| batch-1 output write | 3,505 | 2,744 (spread 2,283–4,824) |
| batch-1 checkpoint | 3,254 | 2,740 |
| batch-1 GC pause | 6,641 | 6,153 |
| restart: restore | 252 | 267 |
| restart: first drain | 459 | 520 |
| restart: output write | 3,515 | 3,595 |
| restart: checkpoint | 403 | 264 |

**Census after the checkpoint (one run each):**

| | join,agg | all |
|---|---|---|
| total references | 333.5M | 320.4M (−4%) |
| total bytes | 10.67 GB | 11.09 GB (+0.42 GB) |
| IntegratedView references | 48.5M | 0.4M |
| Distinct references | 10.4M | 0.47M |
| output connector (`ProgramRunner`) references | 14.7M | 57.5M |

**DISTINCT is a clean win; views are not, on ivm-bench.** The incremental Delta connector keeps
every row it has written as a key (its row-address index) and shares the row objects with the
view. A blocked view frees its dictionary, but the rows stay alive through the connector, and are
also held as block bytes: references move rather than disappear, and bytes grow by 0.42 GB.
Without such a connector (truncate sinks, embedded programs), views would save as much as
DISTINCT does. The view half needs the connector's index made reference-free, a separate piece of
work. Until then, decided: `all` means joins, aggregates and DISTINCT, and views are opt-in
(`DBSPNET_TRACE_BLOCKS=all,view`).

### 3.9 The output connector's row index, reference-free: plan (2026-11-02)

**What holds the rows.** `DeltaOutputConnector` in `OutputMode.Incremental` keeps
`Dictionary<object, List<RowAddress>>` (`DeltaOutputConnector.cs:41`), keyed by each written view
row (the `StructuralRow` the runner passes as `insertKeys`). A `RowAddress` is a file path and a
position, and a retraction becomes a deletion vector at that position. Per distinct row the
dictionary holds the row itself, a `List`, its backing array, and a path reference. With a managed
view the row object is shared with the view, so the index costs only the list. With a blocked view
the index is the row's only owner, which is why the census put 57.5M references under `(runner)`
(§3.8). The runner's own structures (`Pending`, the `ArrowChanges` key list) live for one write and
are not retained.

**Feldera keeps no such index.** Its Delta output truncates on start and then appends each
batch's records; it writes no deletion vectors. Our Incremental mode keeps the table equal to the
view, which needs a row → position map, so this is a cost Feldera does not have. The design keeps
that semantics and changes only how the map is held.

**Alternatives considered:**
- **Key by a 64-bit row hash only** (struct arrays, no row bytes). This is the smallest option, but
  two live rows with equal hashes cannot be told apart at delete time. Making it exact needs the row
  somewhere (in the view, or read back from Parquet). Rejected: every other blocked structure is
  exact, including NaN payloads (§3.5).
- **Store addresses in the view's trace** (row → its positions as the value). This saves the second
  copy of the row bytes, but it ties engine state to one sink. A view can have several sinks, and
  the parallel view is sharded. Rejected.
- **Shrink the managed index** (an int file id instead of a path, a single address inline instead
  of a `List`). This removes two to three references per row, but the row objects stay alive. Worth
  doing for the managed arm anyway (below), but it is not the lever.

**Design: the index is a `BlockedIndexedTrace`**, with the **row as the key** and one **packed
`long` address as the value** (file id << 40 | position, so up to 2^24 files of up to 2^40 rows,
checked). Each live copy of a row is one entry with weight +1, and deleting a copy integrates the
same entry with weight −1. This reuses the tested machinery as it stands: exact encoded-key
equality, hash-ordered blocks below the LOH threshold, freezing and merging, and giant groups
spanning blocks. The only new code is the address bookkeeping.
- **Where it lives:** a public `OutputRowIndex` in DbspNet.Persistence, which already sees Core's
  internals and owns the block format (`BlockSchema`). Connectors.Abstractions references
  Persistence, so `IOutputConnector` does not change. The connector creates the index in
  `BindSchemaAsync` from the view schema. The index is **managed** (today's dictionary, with the
  int-file-id shrink) unless the view is blocked: `TraceBlocksMode.View` is set and every column is
  block-encodable, the same condition `PlanToCircuit` uses (`PlanToCircuit.cs:1432`). A blocked index
  beside a managed view would only add bytes. `OutputRowIndex.Use(...)` overrides the choice in tests.
- **Apply:** one call per `ApplyChangesAsync`. For each delete `(row, count)`, read the row's group
  as a consolidated view (addresses whose net weight is +1; a −1 may sit in a later level than its
  +1), take `count` of them, and stage them as −1 entries. Then write the inserted files, stage a
  +1 entry per inserted copy, and integrate the whole batch as one delta. The runner's Z-set is
  consolidated, so a row is either deleted or inserted in one call, never both, and the lookups see
  the state before the batch, as today.
- **Freeze:** after each apply (once per batch, off the step path), and at the usual threshold
  within a large apply.
- **The first write** carries the whole view (tens of millions of rows on ivm-bench). Integrating it
  as a single managed delta would recreate exactly the transient object peak this arc removes. It is
  integrated in chunks of `FreezeEntries`, freezing each chunk, so at most one chunk is managed at a
  time, and merges compact the resulting runs.
- **Not persisted.** The index is rebuilt by the first write after a restart, which truncates the
  table and writes the whole view, as today. Persisting the index together with the table version
  (to make restart writes incremental) is a separate decision.

**Expected effect, to be measured.** The `(runner)` references (57.5M under `all,view`) go to near
zero. Bytes trade the row object graphs (the 0.42 GB §3.8 added, plus the managed rows themselves)
for a second block copy of each row (about the view's block size) plus 8 bytes per entry. That
should be net negative, but it has not been measured. Apply time gains a hash-and-encode per inserted
row and a decode per deleted row's group. That cost is small next to Parquet encoding, but the first
write encodes every row, so watch it.

**Gate:** `DBSPNET_TRACE_BLOCKS=view` (with `all,view` in the A/B). There is no separate flag: the
blocked index exists only to make the blocked view pay.

**Tests:** a random-operation oracle against the managed index (insert copies, delete counts,
re-insert, freeze at tiny thresholds so runs and merges are exercised, giant groups, NaN payloads):
after each apply, the set of live addresses per row must match. Run the existing incremental-connector
tests (read the table back, compare to the view) in both arms, plus a mutation pass.

**Measure:** the census after the checkpoint (`(runner)` references, total bytes), the batch-1 and
restart output write (the `addresses` phase of `DBSPNET_OUTPUT_TRACE`, plus totals), and the drain
as a guard. Use 5 alternating runs of `all` against `all,view`, and compare outputs. **Decision rule:**
if `all,view` has fewer references and no more bytes than `all`, and the output write is no slower
beyond noise, views join `all`.

### 3.10 The output row index: built and measured; views join `all` (2026-11-02, branch `arc/flat-trace-view-index`)

**Built as planned (§3.9):** `OutputRowIndex` (DbspNet.Persistence) holds a row → packed address map,
blocked or managed, and `DeltaOutputConnector` uses it with int file ids. Tests:
- a model oracle over both representations (tiny runs, large groups, NaN payloads, clears);
- the incremental-connector tests with views and the index on blocks;
- `IntegrateEntries` inside the random trace oracle, and a 150k-entry batch with and without forced
  collisions.

The suite passes 3,150 tests with blocks off and with `all`. 6 mutations were tried: 4 were caught,
and 2 changed no behaviour (clearing an index that is always empty at truncate, and a zero-weight
check that decoding already makes).

**Two things the first A/B found:**
1. **Chunked freezes merged each entry about 6.6 times.** Indexing a first write of 4M rows took
   5.3 s: 3.9 s of merges and 0.9 s of freezes. The index now gives a batch to the trace whole, as
   one run, through `BlockedIndexedTrace.IntegrateEntries`. The entries are sorted by hash once and
   encoded once, with no managed level. The encoding also replaces §3.9's chunked first write: the
   flat lists hold only the keys the runner already holds. A large batch is cut into hash ranges
   (the top bits of the hash) that are sorted and encoded in parallel. A run is the ranges'
   blocks concatenated, and building it is cache-miss bound: rows are visited in hash order. The
   output write is off the step path, when cores are idle. A 4M-row first write now takes 0.34 s
   (managed: 0.39 s).
2. **The parallel view kept a decoded copy of every row.** `ParallelProgramOutput.CurrentView`
   caches the formed view (the sum of the shard integrals) until the next read. The runner's first
   incremental write and the harness's first `RowCount` both went through it, and nothing released
   it before the checkpoint. With managed views the cache shares row objects with the traces, so
   it was nearly invisible (2.9M references). With blocked views it was the whole view decoded:
   32.8M objects and 45.7M references, which §3.8 had blamed on the connector. Now a full write
   (`EnumerateView`) and a first count form the view for that pass only, and a step drops the
   cache.

**Census after the checkpoint** (SF=3, W=12, one run each, both arms on the fixed code):

| | all (join, agg, DISTINCT) | all + views + row index |
|---|---|---|
| objects | 133.1M | 98.9M (−26%) |
| references | 319.6M | 263.0M (−18%) |
| bytes | 10.59 GB | 10.08 GB (−0.51 GB) |
| IntegratedView references | 48.5M | 0.4M |
| `(runner)` references | 8.8M | 0.09M (the index: 0.54 GB of blocks, 77k refs) |

**Wall clock** (5 alternating runs per arm, outputs identical, medians, ms):

| | all | all + views + row index |
|---|---|---|
| batch-1 drain | 16,814 | 16,794 |
| batch-1 output write | 3,352 | 2,647 (−21%) |
| (of which index upkeep) | 189 | 564 |
| batch-1 checkpoint | 3,091 | 2,575 (−17%) |
| batch-1 GC pause | 6,331 | 6,172 |
| restart: restore | 256 | 250 |
| restart: first drain | 508 | 510 |
| restart: output write | 3,471 | 3,513 (+1%, noise) |
| restart: checkpoint | 284 | 299 |

**Decided (by §3.9's rule):** views and the connector's row index join `all`
(`DBSPNET_TRACE_BLOCKS=all` is now joins, aggregates, DISTINCT and views), and `TraceBlocksMode.Use`
defaults `view` to `join`. The index upkeep costs about 0.4 s more per first write, but it is
outweighed by the cheaper Arrow conversion and GC: the output write gets faster overall.
**Lesson:** a census that attributes references to an owner names only the first path found. Check
which field actually holds the rows (`STATE … path`) before designing the fix.

### 3.11 Window operators: plan — stop storing the emitted output (2026-11-02, branch `arc/flat-trace-window`)

**Census (after §3.10, `all`):** the window operators hold 228M of the heap's 263M references (87%).
WindowAggregate holds 120M and WindowOffset 108M. Each has two per-partition structures:
- `_accum`: the integrated input, as a `SortedDictionary` per partition (52M and 58M references).
- `_window`: the last emitted output, as one widened row per base row (base cells plus aggregate
  or offset cells: 67M and 50M references).

**`_window` is redundant.** An emitted row is a pure function of its partition's contents:
- Running frames fold the partition from its start.
- Bounded and whole-partition frames call `Compute` over a frame slice.
- Offsets index positional slots.

Lazy restore already relies on this: `EnsureWindow` and `InitWindow` rebuild `_window` from `_accum`.
So the previous output of any row can be recomputed from the partition as it stood before the
delta: the current rows with the delta subtracted. GC drops a row only once no row whose frame
holds it can be recomputed again (the existing argument), so recomputing never meets a frame that
lost a row.

**Design.**
- **Union list.** Each touched partition gets the union of its rows before and after the delta,
  each with its old and new weight, ordered as today (order value, then the total order).
- **Window aggregate.** A running frame folds two aggregates side by side in one pass. Bounded
  and whole-partition frames compute old and new over the same candidates (rows in the affected
  value range with a non-zero weight on either side). A row is emitted only where its widened
  row or its weight changed, as `EmitRowDiff` does now.
- **Offsets.** The positional slots are expanded for the old and the new partition, and the two
  widened multisets are diffed.
- **Removed:** `_window`, the faulted-partition window hulls (`_partial`, `EnsureWindow`), and
  `InitWindow` at restore. A restored partition needs nothing beyond its rows.
- **Unchanged:** the snapshot format (only `_accum` is persisted). `_accum` itself moves to blocks
  in a second step, if this one holds.

**Cost:** about twice the aggregator work and widening per touched partition. In batch 1 most
partitions are new, so the old side is empty. Later batches pay it, and they are small.

**Gate:** batch-1 drain ≤ +3% against main (5 alternating runs, two builds), plus restart first
drain, census and identical outputs. There is no flag: the change is a simplification, kept only
if it holds.

### 3.12 Window operators without stored output: measured (2026-11-02)

**Built as planned (§3.11):** 6228aaa. Both operators walk each touched partition's union of
rows (old and new weights), with the removed rows merged in by the total order. The window
aggregate folds two running aggregates side by side, or computes old and new frames. The offset
operator expands both slot lists and diffs them.
- **Restore no longer rebuilds windows:** `_partial`, `EnsureWindow` and `InitWindow` are gone, and
  the eager load no longer recomputes every partition.
- **Tests:** the suite passes 3,150 tests in both arms. 6 mutations were tried, and 5 were caught by
  the existing tests. The sixth (not sorting the removed rows) changes only the order in which
  peers are folded, so only floating-point sums can see it. A new test catches it: running,
  bounded, whole-partition, descending and LAG windows over peers summing 1e16 ± 1, with several
  rows deleted per tick, checked every tick against a fresh run.

**Census after the checkpoint** (SF=3, W=12, `all`, one run each):

| | main | no stored window output |
|---|---|---|
| objects | 98.9M | 82.6M (−16%) |
| references | 263.0M | 155.4M (−41%) |
| bytes | 10.08 GB | 8.57 GB (−1.5 GB) |
| WindowAggregate references | 119.9M | 60.9M |
| WindowOffset references | 108.3M | 58.4M |

**Wall clock** (5 alternating runs per arm, two builds, outputs identical, medians, ms):

| | main | no stored window output |
|---|---|---|
| batch-1 drain | 16,521 | 16,727 (+1.2%, gate +3%) |
| batch-1 output write | 2,973 | 2,483 (−16%) |
| batch-1 checkpoint | 2,625 | 2,462 (−6%) |
| batch-1 GC pause | 6,068 | 5,743 (−5%) |
| restart: restore | 260 | 256 |
| restart: first drain (batch 2) | 509 | 467 (−8%) |
| batch 3 drain (steady state) | 301 | 322 (+7%) |

**Kept and merged.** The steady-state batch pays the doubled recompute: about +21 ms on a 0.3 s
batch. If that matters, the lever is to stop refolding unchanged prefixes. A running frame's old
and new folds agree up to the first delta row, so one fold could serve both until there.

**What is left:** the remaining references are the two operators' `_accum`, a `SortedDictionary`
of input rows per partition: about 119M of 155M. Holding it as blocks is the next step (§3.13).

### 3.13 The window aggregate's input as blocks: plan (2026-11-02, branch `arc/flat-trace-window-accum`)

**Profile first** (batch 1, SF=3, W=12, CPU summed over workers): the window aggregates spend
21 s in four places:
- **Integrating 4M delta rows into per-partition `SortedDictionary`s: 9.1 s.** Every insert makes
  about log n full structural comparisons through the ORDER BY comparer.
- **The running fold: about 8.7 s.** It builds a one-row Z-set per row.
- **The union: 2.1 s.**
- **Widening and diffing: 1.5 s.**

Each partition is touched about once per batch (4M union rows for 4M delta rows). So `_accum`
costs CPU on insert as well as references (the two window operators' `_accum` hold about 119M of
the remaining 155M).

**Design.**
- **One store, two representations.** The operator reads its per-partition input through a store
  (`IPartitionRows`): managed (today's sorted dictionaries) or blocked.
- **The blocked store is a `BlockedIndexedTrace`**, keyed by the partition key and holding the
  whole input row as its value. A tick's delta is integrated as one level, or as one run through
  `IntegrateEntries` when it is large. A touched partition is read as its decoded group.
- **The union is formed by one sort,** by (order value, then the total order), over the
  partition's rows and the delta's. Equal rows end up adjacent and are coalesced into old and new
  weights. That is the order the operator already folds in, and the order value is a precomputed
  `long`, so most comparisons never reach the structural comparer. The managed store keeps
  today's merge, since its rows are already in the total order.
- **Checkpoints.** The blocked store attaches an empty in-memory snapshot plus a `fullState`
  hook, so an incremental checkpoint writes only the pending changes. Only a full write decodes
  the whole state.
- **Restore** stays lazy: a faulted partition's rows enter as a level.
- **Not blocked:** frontier GC (bounded ascending frames with LATENESS: dropping rows inside a
  partition is not a key drop), and non-structural rows.

**Gate:** `DBSPNET_TRACE_BLOCKS=window`, not in `all` until measured. Measure the batch-1 drain
(≤ +3%), batch 3 (steady state: every partition touched by one new day), restart, and the census,
over 5 alternating runs. The offset operator follows if this holds. It has no numeric order value,
so it needs a numeric prefix of its first ORDER BY key to keep its sort cheap.

### 3.14 The window aggregate's input as blocks: built and measured (2026-11-02)

**Built as planned (§3.13):** 3e86c63, c2e4020. Tests:
- the bit-exact window test (§3.12) with a blocked arm, which also checks blocked against managed;
- the lazy partition restore tests with a blocked arm;
- the persistence conformance suite's blocks family, now with blocked windows.

The suite passes 3,162 tests with blocks off, with `all`, with `all,window`, and with tiny runs.
5 mutations were tried: 4 were caught, and the fifth (no idle freeze) only affects performance.
The lazy-restore mutation survived until the lazy tests gained their blocked arm.

**Found on the way: a bug on main, fixed separately.** `LAG(x, 1, 0.0)` over a DOUBLE emitted its
default as a DECIMAL cell, because the constant was never cast to the value's type. Managed views
carried it silently. The blocked view's encoder rejected it.

**Three rounds of measurement:**
1. **The first A/B grew the state** (objects 23M → 68M). The first checkpoint's full write called
   `Materialize()`, which caches a decoded copy of the whole trace. Full writes now use
   `MaterializeUncached`.
2. **Drain was +4.1%.** The profile (CPU summed over workers) showed the union at 15.6 s against
   2.1 s managed, while the integrate fell from 9.2 to 6.5 s. Most of the union was the sort. In
   batch 1 every row sat next to its own delta copy, and each such tie cost a full structural
   comparison. Consolidating first (decode the group, subtract the delta by lookup, then sort only
   distinct rows) cut the sort from 10.1 s to 2.7 s.
3. **What remains is decoding.** Each touched partition is decoded whole every tick, because a
   running frame folds from the partition's start: about 10 s CPU in batch 1.

**Census after the checkpoint** (SF=3, W=12, one run each):

| | all | all,window |
|---|---|---|
| objects | 82.6M | 70.6M (−15%) |
| references | 155.4M | 96.7M (−38%) |
| bytes | 8.57 GB | 8.25 GB (−0.32 GB) |
| WindowAggregate references | 60.9M | 1.4M |

**Wall clock** (5 alternating runs per arm, one binary, outputs identical, medians, ms):

| | all | all,window |
|---|---|---|
| batch-1 drain | 16,752 | 17,079 (+2.0%) |
| batch-1 output write | 3,033 | 2,227 |
| batch-1 GC pause | 5,899 | 6,292 (+6.7%) |
| batch-1 checkpoint | 2,537 | 2,457 |
| restart: restore | 222 | 264 (+19%) |
| restart: first drain | 469 | 531 (+13%) |
| batch 3 drain | 295 | 287 |

**Not decided: left opt-in (`DBSPNET_TRACE_BLOCKS=window`).** The memory gain is large and the
batch-1 gate holds. The restart costs about 100 ms more (restore plus first drain), because
faulted partitions are decoded whole on first touch. The structural fix is algorithmic: a touch
should read only the frame it changes, not the whole partition. That needs partial aggregates
stored at block boundaries (a running frame's prefix, Feldera's tree of partial aggregates),
after which decoding scales with the delta, not the partition. The offset operator waits on the
same question.

### 3.15 Reading only what a change reaches: design (2026-11-02)

**Problem.** A touched partition costs O(partition): the blocked store (§3.13) decodes and sorts
it whole, and the managed store walks it whole. A running frame refolds from the partition's start
because the operator has no prefix state. In ivm-bench the cost lands where it hurts most, on the
first drain after a restart: about 100 ms (§3.14).

**Two constraints found while designing:**
- **Float exactness ties partial aggregates to stored outputs.** §3.11 recomputes the old side by
  refolding, which reproduces a floating-point SUM bit for bit only in the same fold order.
  Combining partial aggregates over a tree changes the association, so a recomputed old value can
  differ in the last bits from the one emitted, and the retraction would not cancel. Feldera
  stores its emitted outputs (an integrated output trace) and reads the old side from them; that
  is what lets it use a tree of partial aggregates.
- **The fold state is heavy and opaque.** `IAggregator` has no way to clone or combine state, and
  SQL MIN/MAX keep a `SortedSet` and `Dictionary` of every distinct value in the prefix. A running
  fold therefore cannot resume from a checkpoint without new aggregator support.

**What ivm-bench runs:** every window aggregate is MIN, MAX or COUNT (52-week highs and lows,
flag dates, dim_customer counts). All three combine exactly under any association.

**Options:**

| | how | what a touch reads | risk |
|---|---|---|---|
| A. Feldera's | ordered input chunks + partial-aggregate tree + emitted outputs stored as blocks (old side read, not recomputed) | O(log p + affected rows) for every aggregate | largest: undoes §3.11's storage saving (outputs back, as blocks) and adds an output trace with its own checkpoint |
| B. Ranges only | ordered input chunks, read by order-value range | bounded frames and offsets: the frame's range. Running frames: still the whole prefix | small, but does not help ivm-bench's running frames |
| **C. Exact partials** | ordered input chunks, each with a partial aggregate when every aggregate in the window combines exactly (COUNT, MIN, MAX, integer and DECIMAL SUM/AVG). Otherwise the full fold, from already-ordered chunks with no sort | running: the chunks from the change on, plus O(chunks) partials. Bounded: the frame's range | moderate: §3.11's recompute stays exact because the combination is exact |

**Recommended: C.**
1. **Ordered chunk store.** Per partition, a list of chunks sorted by (order value, total order).
   Each chunk is one reference-free block with its first and last order value, row count, and
   (when exact) its partial aggregate. An insert or retract rewrites the one chunk its value
   lands in, splitting at about 1,000 rows. Appends, the time-series case, touch only the last
   chunk. This replaces the LSM for the window input: no hash order, no merges, no per-touch sort.
2. **A partial interface for exact SQL aggregates.** An accumulator from rows, a combine and a
   result, implemented by `CompositeAggregator` only when every slot is exact. Floats (DOUBLE
   SUM/AVG, STDDEV) and sketches keep the full fold.
3. **Running frames** combine the partials of the chunks before the change, fold the boundary
   chunk's rows below it, then fold the suffix on both sides, decoding only from the boundary
   chunk on. **Bounded frames** decode only [dmin − preceding, dmax + preceding].
   **Whole-partition frames:** if the partition's aggregate is unchanged, only the delta's own rows
   change output.
4. **The offset operator next, on the same store.** Chunk row counts give positions, so
   LAG/LEAD read only the neighbourhood of the change.

**Checkpoint and restore** keep §3.13's shape: incremental writes carry the pending changes, a
full write decodes the chunks, and a faulted partition's rows are sorted once into chunks.
**Gate:** batch-1 drain no worse than `all` (+3%), restart first drain and restore no worse than
`all`, batch 3 no worse. Then `window` joins `all`.

### 3.16 Ordered chunks with exact partials: built and measured (2026-11-02, branch `arc/flat-trace-window-chunks`)

**Built as designed (§3.15, design C):** 43472a6, 68d9650, f47bab2.
- `OrderedPartitionStore`: small partitions in a shared trace; a partition past 512 rows moves to
  ordered 512-row chunks, each with its order values and, when exact, its partial aggregate.
- `IPartialAggregator`, implemented by `CompositeAggregator` when every slot is COUNT, COUNT(*),
  MIN or MAX.
- A running ascending frame reads from the chunk its change lands in, with the chunks below as
  one partial. A bounded frame reads its frames' span.

**Tests:** a store oracle against a model (random batches, tiny promote and chunk sizes; reads
must be ordered, consolidated, cover their range, and carry the exact prefix partial), and the
bit-exact window test with exact running, descending and nullable-COUNT windows on tiny chunks.
The suite passes 3,181 tests in four arms. 11 mutations were tried: 8 were caught, and 3 change
no behaviour (MIN/MAX tie-breaking between values that compare equal but are not `Equals`, and
the weight check on rows that are always positive). Two shapes survived until the test gained
them (an exact DESC window, a nullable COUNT).

**Found while profiling:**
- **The default MIN/MAX is the hybrid aggregator** (`SqlMinMaxHybridAggregator`), which had no
  partials, so every MIN/MAX window silently took the full fold. Fixed by delegating to the plain
  MIN/MAX.
- **A bulk-loaded partition crossed the shared trace twice:** it was written there, read back,
  promoted, and retracted (13.8 s CPU in batch 1). A partition whose share of a change alone passes
  the threshold now goes straight to chunks.
- **The union hashed each row twice;** it now does one lookup. Small reads consolidate by sort,
  not by hashing.

**Wall clock against `all`** (5 alternating runs, one binary, outputs identical, medians):

| | §3.14 (decode whole) | §3.15 (ordered chunks) |
|---|---|---|
| batch-1 drain | +2.0% | +4.1% (17,002 → 17,699 ms) |
| batch-1 output write | −27% | −15% |
| restart: restore | +19% | +3% |
| restart: first drain | +13% | +4% |
| batch 3 drain | −3% | −8% |
| references (census) | −38% | −37% (155.4M → 97.3M) |

**Reading.** The design does what it was for: restart and steady state are at parity or better
(the first drain after a restart no longer decodes partitions whole). Batch 1 pays instead,
+4.1% against the +3% gate, because a bulk load encodes 4M rows into chunks and reads about a
million one-row partitions (`trades`, partitioned by trade id) from the shared trace. Window
operator CPU in batch 1 (summed over workers) is 28.6 s against 18.8 s managed: integrate 11.8 s
(chunk encoding), union 10.6 s (5.2 s of it the tiny-partition probes), fold 6.2 s (the partial
fold is cheaper than the incremental one, 7.8 s).

**Left opt-in, pending a decision.** The remaining batch-1 levers: one-row partitions need no
window state at all (a row's frame is itself), and the bulk encode could move off the step thread.

### 3.17 Batch 1's remaining window cost (2026-11-02, branch `arc/flat-trace-window-fresh`)

**Built:**
- **A key filter** (a scalable Bloom filter over partition keys, plain bit arrays). A partition
  the filter has never seen is read from the tick's own rows instead of probed for in every run.
  Small-partition reads went from 5.2 to 3.1 s CPU in batch 1. The last tick's rows are dropped
  when the step ends. Holding them until the next change had cost 22M references at the
  checkpoint, caught by the census.
- **One-row fast paths** in the union and the read. The union fell from 9.5 to 5.9 s.
- **Mutable slot holders** for the exact partials: folding a row allocates nothing.
- **`RetainedRows` reads the entry count.** It had decoded the whole state, every tick, in
  profiling runs.

**Mutations:** a key wrongly treated as new is caught. Three others change no result: forgetting
keys after a lazy restore (cold partitions are faulted in before the delta, so the filter is
never wrong), not recording a promoted key, and stale keys after a clear (false positives only
cost a probe).

**Wall clock against `all`** (5 alternating runs, outputs identical): batch-1 drain +3.2% and
+4.7% in two A/Bs (16.6 → 17.4 s in the second), restart first drain +3–5%, restore +3–5%,
batch 3 −16% and +3% (noisy). References are −37% (155.4M → 97.3M).

**Where the remaining batch-1 cost is:** small partitions. The full operator profile
(`DBSPNET_PROFILE_TOP`) shows every small window operator slower on blocks (dim_customer's
22 operators: about 110 → 200 ms and 40 → 140 MiB each; accounts' and securities' alike). These
are mostly whole-partition MAX windows over wide rows (about 35 columns), touched in many ticks.
Each touch decodes the partition's rows from blocks, while the managed store already holds them
as objects. That is the price of holding small partitions as blocks, and no constant-factor fix
removes it. The big operators (daily_market) are faster on chunks than managed.

**Options (decision pending):**
1. **Small partitions managed, large ones as chunks.** The decode churn goes, and so do the
   references the blocked small partitions save (about 1.1M of the 3.7M window rows: trades' one-row
   partitions and the customer windows), so references would be about −26% instead of −37%.
   Batch 1 should come in at or under `all`, because the large windows are faster on chunks.
2. **Keep everything blocked:** −37% references, batch 1 +3–5%.
3. **Decode cheaper:** strings aliasing the block bytes. But output rows carry the cells
   downstream, so aliased strings would pin blocks. Not recommended.

### 3.18 Small partitions managed, large ones chunked: `window` joins `all` (2026-11-02)

**Decided (§3.17 option 1, agreed with Curt):** a partition's representation follows its size.
- **Small partitions are managed:** a sorted dictionary per partition, ordered by (order value,
  total order), so a read is already ordered and consolidated and decodes nothing.
- **Large partitions:** one that grows past 512 rows (at a read, or at integrate time when one
  change alone takes it past) becomes ordered chunks with exact partials (§3.15). A chunked
  partition that shrinks keeps its (smaller) chunks and leaves the store when empty. If it
  returns, it starts small again.
- **The key filter (§3.17) is gone:** a managed lookup is already cheap.

c91b00f; tests unchanged, plus a check that a read never holds a zero weight (a mutation that
kept zeros was invisible to results but leaked). Four mutations of the small side: all caught.

**Wall clock against `all` before the change** (5 alternating runs, outputs identical, medians, ms):

| | all | all + window |
|---|---|---|
| batch-1 drain | 16,760 | 16,741 (−0.1%) |
| batch-1 GC pause | 5,938 | 5,641 (−5%) |
| batch-1 output write | 2,398 | 2,178 (−9%) |
| batch-1 checkpoint | 2,529 | 2,355 (−7%) |
| restart: restore | 244 | 261 (ranges overlap; a lazy restore never touches the store) |
| restart: first drain | 521 | 532 (+2%) |
| batch 3 drain | 293 | 270 (−8%) |

A profiled run shows why batch 1 is now neutral. The large windows (daily_market) are faster on
chunks than managed, window-aggregate CPU fell from 32.1 to 22.4 s, and the engine step went
from 15.7 to 14.4 s.

**Census after the checkpoint:** references 155.4M → 130.6M (−16%), bytes 8.57 → 8.42 GB,
WindowAggregate references 60.9M → 35.4M. That is less than the −26% estimated: the managed
small partitions hold more of the window rows than the row counts suggested.

**Decided by §3.15's rule:** `window` joins `all`. `DBSPNET_TRACE_BLOCKS=all` is now joins,
aggregates, DISTINCT, views and the window aggregate. The offset operator (LAG/LEAD) and the
remaining managed window state are next.

### 3.19 LAG/LEAD on the ordered store (2026-11-02, branch `arc/flat-trace-offset`)

**Design and build (3626ab2).** The offset operator uses the same store as the window aggregate:
small partitions managed, large ones as ordered chunks.
- **Routing.** The offset operator has no numeric order value, so the store routes by a prefix of
  the first ORDER BY key that agrees with the total order: the key's monotone value, bit-flipped
  (`~x`, no overflow) for DESC. NULLs go to `long.MinValue` or `long.MaxValue` as NULLS FIRST/LAST
  says (the comparer places NULLs absolutely). Ties fall to the total order. This applies only to
  integer and temporal first keys. Other keys keep the managed path.
- **A touch.** It reads the chunks overlapping the change's prefix range, widened by whole chunks
  until 2 × the largest offset + 1 rows lie on each side (`ReadAround`), with flags for whether
  the read reaches the partition's start and end. It builds the old and new slot sequences
  (a weight-w row fills w slots), marks the rows within reach of a changed row on either side, and
  emits their old outputs retracted and new ones inserted. FIRST_VALUE/LAST_VALUE read the whole
  partition.

**Tests:** the bit-exact window test gained LEAD by 2, DESC, FIRST/LAST_VALUE, a nullable first
key under NULLS FIRST and NULLS LAST, and duplicate rows (weights above one). The suite passes
3,193 tests in three arms. 8 mutations were tried:
- **Caught (5):** no margin, the prefix ignoring DESC, NULLs always first, the old side ignoring
  the delta, and both neighbour-marking loops cut short together.
- **Not caught alone (2):** cutting either marking loop short on its own. The two loops overlap,
  each covers the other.
- **No change (1):** a read always claiming to start the partition. With a correct margin, the
  start flag only guards the "reached past the rows read" exception.

**Wall clock against main** (5 alternating runs, two binaries, outputs identical, medians, ms):

| | main | + offset store |
|---|---|---|
| batch-1 drain | 16,527 | 16,411 (−0.7%) |
| batch-1 checkpoint | 2,475 | 2,184 (−12%) |
| batch-1 output write | 2,195 | 2,332 (ranges overlap) |
| restart: restore | 247 | 268 |
| restart: first drain | 474 | 515 (ranges overlap: 446–525 vs 458–538) |
| batch 3 drain | 271 | 258 (−5%) |

**Census:** references 130.6M → 113.3M (−13%), bytes 8.42 → 8.14 GB, WindowOffset references
58.9M → 41.1M. What remains is in small (managed) partitions.

**Merged; restart flagged.** The restart medians are about 40 ms higher, but within the overlap
of the two arms' runs. One cost is real: after a restart, each touched large partition is faulted
in and cut into chunks (a sort and an encode), where the managed store only inserts. If a later
A/B confirms it, the lever is to adopt the faulted partition's cold rows as chunks directly,
since they arrive already in partition order.

### 3.20 Aggregate caches: plan — small exact groups keep none (2026-11-02, branch `arc/flat-trace-agg-caches`)

**Census after §3.19:** IncrementalAggregate holds 23.5M references. Its input traces are blocks
(0.1M); the rest are two per-group caches:
- `_aggCache`, the last emitted aggregate per group (13.5M);
- `_stateCache`, the aggregator's incremental state per group (9.8M), mostly hybrid MIN/MAX
  `Compact` states and their boxed best values.

One operator, `watches` (a GROUP BY over six columns with MIN/MAX of timestamps, about half a
million groups of around two rows each), holds 14M of them.

**The caches are derivable.** A lazy restore already rebuilds a group's caches by folding its
rows through the aggregator (`EnsureCaches`). For an exact aggregator, folding the group gives the
same value and state as the incremental update that produced the emitted output. "Exact" here means
`CanPersistState` is false: no floating-point accumulator.

**Design.** On a blocked trace, an exact aggregator keeps no caches for a small group. A group is
small when its entries across levels (counted from the group view's ranges, an upper bound,
without decoding) are at most `SmallGroup` = 8.
- **A touch of an uncached group:** decode the before-group (a few rows), fold it to recover the
  old value and state, apply the delta, and emit the change.
- **Caching after the change:** the group is cached only if it has grown past the threshold.
- **A cached group that shrinks** to the threshold drops its caches.
- **Unchanged:** float aggregates, large groups, and managed traces.
- **The lazy-restore rebuild** stores nothing for small groups either.

**Cost:** a small decode per touched small group. In batch 1 the groups are new (nothing before).

**Gate:** batch-1 drain no worse than main (+3%), restart and batch 3 no worse, identical
outputs, census. 5 alternating runs against a main build.

### 3.21 Aggregate caches: built and measured (2026-11-02)

**Built as planned (§3.20), with one correction (7286ff6).** Any exact group without caches is
refolded, whatever its size. Size only decides whether the caches are kept after the change. A
group's entry bound can rise without the group being touched (another key sharing its hash adds
entries to the same block range), so routing small groups by size alone could have taken the
cached path with no cache. Two exclusions:
- **Frontier GC (LATENESS)** keeps the caches: that state is already bounded, and a blocked trace
  learns which groups GC dropped from them (an operator metric test caught this).
- **Float aggregates** (`CanPersistState`) keep their caches.

**Tests:** a new test drives groups past the threshold and back, with repeated rows and several
deletes per tick, comparing with a fresh managed run every tick. A DOUBLE SUM case checks only
that each group keeps exactly one row, since its incremental fold need not match a fresh one bit
for bit. The suite passes in three arms. Mutations:
- **Caught:** letting float aggregates refold, and a refold that ignores the group.
- **Equivalence check:** no caching at all for exact aggregators still passes every aggregate,
  persistence and random-query test. Refolding is exact for all of them, COUNT(DISTINCT) included.

**Wall clock against main** (5 alternating runs, two binaries, outputs identical, medians, ms):

| | main | small groups uncached |
|---|---|---|
| batch-1 drain | 16,510 | 16,217 (−1.8%) |
| batch-1 GC pause | 6,149 | 5,671 (−8%) |
| batch-1 checkpoint | 2,408 | 2,123 (−12%) |
| restart: restore | 252 | 251 |
| restart: first drain | 515 | 457 (−11%) |
| batch 3 drain | 245 | 251 (noise) |

**Census:** references 113.3M → 99.3M (−12%), bytes 8.14 → 7.74 GB, objects 67.1M → 57.8M.
IncrementalAggregate references 23.5M → 9.5M. Merged.

### 3.22 Small window partitions: blocks as the record, a per-batch read cache (2026-11-02, branch `arc/flat-trace-window-cold`)

**Census after §3.21:** most of what is left is the window operators' small managed partitions:
the offset operator holds about 41M references, and `trades`' window (MIN/MAX over
`PARTITION BY trade_id`, a few rows per trade) about 23M.

**Why §3.17 failed and what to keep from it.** On blocks, every touch decoded the partition.
dim_customer's wide partitions were touched in many ticks of a batch and paid the decode each
time. Holding small partitions as blocks was never the cost; the repeated decode was.

**Design.**
- **The record:** a shared `BlockedIndexedTrace` (partition key → row), as §3.17. It receives every
  tick's change to small partitions, so it is always current. Its managed levels freeze at the
  usual threshold.
- **A read cache:** the managed sorted map of §3.18, holding only partitions touched since the
  last idle tick. A miss reads the partition once. A partition the key filter has never seen (new
  this tick) is read from the tick's own rows; any other is probed and consolidated by sort. A
  cached partition is kept current by applying each tick's change to it, so later touches in the
  batch decode nothing.
- **Clearing:** on an idle tick and before a checkpoint, the cache is dropped and the trace
  freezes. What a checkpoint (and the heap between batches) holds is then blocks.
- **Large partitions** (over 512 rows) are chunks, as before. Promotion retracts the partition from
  the trace. A bulk change past the threshold goes straight to chunks.
- **Fallback:** a partition key with no block encoding keeps today's managed small side.

**Cost:** each tick's small changes are integrated twice (trace and cache), and the trace freezes
(encodes) them once. A partition is decoded at most once per batch.

**Gate:** batch-1 drain, restart and batch 3 no worse than main (+3%), identical outputs, census.
5 alternating runs against a main build.

### 3.23 Small window partitions on blocks: built and measured (2026-11-02, branch `arc/flat-trace-window-cold`)

**What was built (aec12a4, 0c0256a), after two failed variants found by profiling:**
1. **Cache dropped on idle ticks.** dim_customer and trades got slower: an operator's idle tick is
   not the end of a batch, so partitions were decoded again between data-carrying ticks.
2. **Cache kept, record written every tick.** Still slower: each tick's small changes became a
   level of the record, and its merges decoded and re-encoded the growing state of wide,
   often-touched partitions again and again.
3. **Built: a write-back cache.** A touched partition is loaded once into the managed cache (from
   the record plus its queued changes, or empty for a key the filter has never seen) and changed
   there. Its changes queue per partition and reach the record in one sorted run
   (`IntegrateEntries`) when the cache is dropped: before a checkpoint, or on an idle tick once it
   holds more than 2^18 partitions. A partition's content is always the record plus its queued
   changes. A cached partition that empties leaves the cache with its changes still queued, which
   was the one bug the store oracle found.

**Tests:** the store oracle gained a cold arm (cache drops and freezes at random). The suite passes
3,204 tests in three arms. Mutations: a cached partition not kept current, every uncached key
treated as new, both promotions leaving rows in the record, a reload ignoring queued changes, a
whole-state read missing them, and promotion keeping them: all caught. Two change nothing
visible: an idle tick keeping the cache, and a drop that does not flush (the changes stay queued
and are read). Both only cost memory.

**Census after the checkpoint** (SF=3, W=12, one run each):

| | main | write-back cache |
|---|---|---|
| objects | 57.8M | 15.7M (−73%) |
| references | 99.3M | 25.2M (−75%) |
| bytes | 7.74 GB | 6.58 GB (−1.16 GB) |
| WindowOffset references | 41.1M | 0.3M |
| WindowAggregate references | 35.4M | 2.0M |

**Wall clock against main** (5 alternating runs, two binaries, outputs identical, medians, ms):

| | main | write-back cache |
|---|---|---|
| batch-1 drain | 16,048 | 16,618 (+3.6%; ranges overlap, one 18.3 s outlier) |
| batch-1 checkpoint | 1,989 | 2,668 (+34%: the flush) |
| batch-1 output write | 3,208 | 2,362 |
| batch-1 GC pause | 5,667 | 6,136 (+8%) |
| batch 1 total (drain + output + checkpoint) | 21,245 | 21,648 (+1.9%) |
| restart: restore | 253 | 263 |
| restart: first drain | 523 | 470 (−10%) |
| batch 3 drain | 240 | 256 (noise) |

**Merged (Curt, 2026-11-02).** Three quarters of the remaining references go, and 1.16 GB with
them. The cost is about 0.4 s on batch 1, mostly the flush in the checkpoint (one encode of every
small partition touched since the last one). Follow-up: flush in the background between batches.

### 3.24 Flushing the window record in the background: measured, no net gain (2026-11-02, branch `arc/flat-trace-bg-flush`)

**Built (3b81bac).** When a drain ends, the runner tells the circuit it is idle (`IIdleWork`). The
window operators then start writing their queued small-partition changes into the record on a
background task, while outputs are written. Every store access waits for it first, and the
checkpoint only drops the cache. The suite passes in both arms; the store oracle and the window
test start flushes at random.

**Wall clock against main** (5 alternating runs, two binaries, outputs identical, medians, ms):

| | main | background flush |
|---|---|---|
| batch-1 checkpoint | 2,738 | 2,465 (−273) |
| batch-1 output write | 2,340 | 2,609 (+269) |
| batch-1 drain | 16,248 | 16,394 (noise) |
| batch 1 total | 21,326 | 21,468 (+0.7%, noise) |

Census unchanged (25.2M references).

**Reading.** The flush is CPU work: an encode of every small partition touched since the last
checkpoint. On a machine whose cores the output write already keeps busy, overlapping it moves
the cost from the checkpoint into the output write and saves nothing. It would pay only where the
engine has idle time between batches, which ivm-bench does not have (the next batch follows the
checkpoint at once). **Not merged** (kept on its branch). The flush already writes only the queued
changes; in batch 1 those are the whole state, so its encode is the cost of holding that state as
blocks at all, the same cost any blocked trace pays when it freezes.

### 3.25 Blocks on by default (2026-11-02)

`DBSPNET_TRACE_BLOCKS` unset now means `all`: joins, aggregates, DISTINCT, stored views (with the
output connector's row index), and the window operators' state. `off` (or `none`) turns blocks
off. An explicit list of kinds still selects just those.

**Evidence:**
- **ivm-bench** (SF=3, W=12, against blocks off at the start of the arc): the checkpoint heap
  held 333.5M references and 10.7 GB, and now holds 25.2M and 6.6 GB. Batch-1 drain is level, and
  restart's first drain and checkpoints are faster (§3.10–§3.23).
- **Nexmark** (10M events, W=14, docs/benchmarks-comparison.md, 2026-11-02): neutral or better
  with blocks on. q15 is +24%. q9's apparent −20% was one slow run; re-measured, it is at parity.

**Unchanged:** the spine trace family, typed (non-structural) operators, and columns with no block
encoding keep their managed state. The suite passes with blocks unset, `off` and `all`.

### 3.26 Semi-joins and residual outer joins on blocks (2026-11-03, branch `arc/semi-join-blocks`)

**Finding.** After §3.25, inner joins still held 6.7M managed references at the SF=3 checkpoint.
None were typed or unencodable. All 32 ordinary inner joins in ivm-bench were blocked. The
managed ones came from three compiler sites that never asked for blocks:

| Site | Census ops | Managed refs | What |
|:--|:--|--:|:--|
| `UnmatchedPreservedRows` | #520, #501 | 4.4M | the anti-join half of an outer join with a residual (temporal SCD `LEFT JOIN … AND ts BETWEEN lo AND hi`) |
| `CompileOuterJoinWithResidual` | #491, #510 | 0.7M | the matched half of the same joins |
| `CompileSemiJoin` | #418 | 0.6M | IN / EXISTS subqueries |

The program has 13 such joins in all (4 + 4 + 5); the census lists the five largest.

The anti-join half is wasteful by construction: `unmatched = side − semi(side, distinct(π_side(matched)))`,
with the semi-join keyed on the whole preserved row *and* valued with it again. Each preserved row
was stored twice in that one trace (key and value), and each matched row twice in its right trace.

**Change.**
- `BlockedJoinOp` gains an output mode: `Concat` (the inner join, unchanged), `Left` (the left
  value alone) or `Key` (the key alone). In the non-`Concat` modes with no residual, a left block
  entry is boxed once for the whole right delta group, with the group's weights summed.
- The residual outer join's matched half uses the ordinary blocked join with its residual on the
  candidate view (§3.2).
- Both semi-joins index the side that only witnesses a match with an **empty value**: the row is
  the key. The whole-row semi-join empties both sides and emits the key. The IN-subquery semi-join
  empties the subquery side and emits the left row. This applies with blocks off too, and the
  snapshot schemas of those traces change accordingly (a recompile starts fresh state, §2).

**Tests.** `BlockedJoinTests.SemiJoinsAndResidualOuterJoins_BlockedEqualsBatch` runs IN, NOT IN,
correlated IN, EXISTS, NOT EXISTS and residual LEFT/RIGHT/FULL joins under tiny freeze and block
sizes, both row codecs, against the batch evaluator, and asserts a blocked join was compiled.
Three mutations (the summed-weight fast path, the residual, the `Key` output) each fail it. The
suite passes with blocks on and off.

**Measured.** SF=3 W=12, five alternating runs against main (medians; views byte-identical in
every run, batch 1 and restart):

| | main | semi-join blocks |
|:--|--:|--:|
| References at the checkpoint | 25.2M | **18.8M** (−26%) |
| of which inner joins | 7.9M | 1.4M |
| Heap bytes at the checkpoint | 6.58 GB | 6.51 GB |
| Snapshot | 4887 MiB | 4840 MiB |
| Batch-1 drain | 16.66 s | 16.20 s |
| Batch-1 GC pause | 6.22 s | 6.10 s |
| Batch-1 checkpoint | 2.72 s | 2.55 s |
| Batch-1 output | 2.37 s | 2.42 s |
| Restore | 244 ms | 266 ms |
| Restart first drain | 526 ms | 470 ms |
| Restart first output | 3.50 s | 3.50 s |

All five managed inner joins are gone; the largest remaining inner-join holder is 74K references.
The drain and checkpoint differences are within noise (±2.5 s on drain, §9 of the state arc).
Two of the new arm's batch-1 outputs took 3.4–3.5 s. That tail appears about one run in five
across both arms in earlier runs (the Delta write; the output phase touches no join), so it is
noise, not this change.

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
