# Bounded memory: keeping cold blocks on disk

Status: **phase 1 (census) done 2026-11-03. Design agreed: fixed per-worker budget, persisted blocks only; batch-1 spilling deferred (§1.4). Increment 1 BUILT 2026-11-03, opt-in (`DBSPNET_BLOCK_CACHE_MIB`), §3.5; miss cost apportioned and cut ~8× (§3.6). Increment 2 BUILT: window state backed and evictable (§3.7). The output row index is spilled too (§3.9): 0.51 GB live at 128 MiB. Mid-batch spilling: design proposed 2026-11-04 (§5); decisions §5.4, knobs §5.5; extended TPC-DS census §5.6 reorders it: R0 tick-0 release, S2 window write-back by bytes, S1, S3. R0 BUILT (§5.7), S2 BUILT (§5.8, default 1 GiB), S1a BUILT opt-in (§5.9, scratch spill), inputs released (§5.10); TOP-K state blocked (§5.12); next is tick 18's in-step peak on extended TPC-DS; S1b, S3 not built.** Follows
[design-flat-trace-state.md](design-flat-trace-state.md) §3.34. That arc made trace state
reference-free blocks: at the SF=3 W=12 checkpoint the heap is ~5.8 GB of bytes, with 1.01M
references. This arc asks how much of those bytes needs to be in memory at all.

## 1. Census (2026-11-03)

### 1.1 Instrument

- `DBSPNET_BLOCK_ACCESS=1` turns on read stamps (`BlockAccess`, `BlockStamp`). Every
  `TraceBlock` read (hashes, weights, cell reads and compares, value hashes) stamps the block
  with the current epoch and phase: drain, write, checkpoint or restore. A read made while a
  run merge is in progress records `Maintenance` instead. Each block also records the epoch it
  was built in. When the variable is unset, all of this is a static readonly flag the JIT folds
  away.
- `IVM_BLOCK_RESIDENCY=1` turns on the walker in `IvmMultiBatch` (`Scratch/BlockResidency.cs`).
  It reports after each batch's drain and after its checkpoint. It finds every
  `BlockedIndexedTrace` and `OrderedPartitionStore` reachable from an operator and classifies
  each block:
  - **persisted**: the block's run file was written by a committed checkpoint, so there is a
    durable copy.
  - **unpersisted**: no run file exists yet.
  - **chunk**: a large window partition's chunk.
  - **lazy_cold**: a restored block that has never been read.
- One walk takes 20–50 ms.
- Raw output is in that session's scratchpad (`run1/{warm,stop,resume}.txt`). The run script was
  `resid.sh`, with DOTNET_gcServer=1, `DBSPNET_STATE_LAYOUT=runs`, incremental output, and
  checkpoints after every batch.

### 1.2 Warm process (batches 1→3 in one process)

| after | block bytes resident | of which persisted | unpersisted | chunks | read this batch |
|---|---|---|---|---|---|
| batch 1 drain | 4,331 MB | 0 | 4,019 | 312 | 1,098 MB (drain) |
| batch 1 ckpt | 4,873 MB | **4,019 (82%)** | 543 | 312 | +485 write, **+2,024 ckpt** |
| batch 2 | 4,874 MB | 4,019 | 543 | 312 | **42.6 MB (0.9%)**, 1,003 of 83,900 blocks |
| batch 3 | 4,874 MB | 4,019 | 543 | 312 | **89.7 MB (1.8%)**, 1,873 of 84,033 blocks |

Blocks are ~84% of the ~5.8 GB heap measured at the checkpoint (§3.34 there). Breakdown by
operator kind after batch 3:

| kind | resident | persisted | batch-3 read |
|---|---|---|---|
| IncrementalInnerJoin | 2,524 MB | 100% | 5.8 MB |
| IncrementalAggregate | 691 MB | 100% | 8.3 MB |
| IntegratedView | 583 MB | 100% | 53.6 MB |
| WindowAggregate | 476 MB | 0 (282 record + 194 chunks) | 4.4 MB |
| WindowOffset | 363 MB | 0 (245 record + 118 chunks) | 2.7 MB |
| IncrementalLeftJoin | 158 MB | 100% | 13.8 MB |
| Distinct | 63 MB | 100% | 1.0 MB |
| PartitionedRank | 16 MB | 0 | 0 |

What the census shows:

1. **Steady-state batches read 1–2% of block bytes.** Everything else stays resident only
   because nothing drops it.
2. **82% of resident block bytes already have a durable copy** once the first checkpoint
   commits. That covers every join, aggregate, view and DISTINCT trace. Dropping those bytes
   costs no extra writes, only a re-read on a miss.
3. **Window state is the one big holder with no block-level durable copy** (839 MB). The
   small-partition record (`_cold`, a `BlockedIndexedTrace`) and the large-partition chunks are
   checkpointed through the window operator's own snapshot, not as run files.
4. **Batch 1's checkpoint reads 2.0 GB of blocks.** Batch 2's and 3's checkpoints read 0.6–0.8 MB.
   This is explained in §1.4. It is not a problem for a cache.

### 1.4 Who reads at batch 1's write and checkpoint (2026-11-03)

`DBSPNET_BLOCK_ACCESS_STACKS=6` records the caller of each block's first read in the write and
checkpoint phases, grouped by the innermost DbspNet frames (raw output: `run2/stacks.txt`).

**(a) Writing the runs.** About 3.97 GB, matching the 4.02 GB persisted afterwards:

| holder | read |
|---|---|
| joins | 2,487 MB |
| aggregates | 684 + 155 MB |
| views | 583 MB |
| DISTINCT | 63 MB |

- `BlockedTraceCheckpoint.WriteRunAsync` reads `block.Hashes` to build each run's index. The
  bytes are resident because they are unpersisted, and the checkpoint has to read them to
  persist them. No cache problem.
- The §1.2 per-kind figure was smaller for joins (295 MB) because most join blocks belong to
  **giant hashes that span blocks in memory**. The writer `Concat`s each span into one temporary
  block raw, so the read lands on that temporary block, not on the trace's blocks.
- Two consequences:
  - The first checkpoint copies ~2.5 GB of join bytes into temporary large-object-heap blocks.
    These are 7,229 written blocks averaging 344 KB, against 43,221 in memory.
  - On disk, a join run's block is often far larger than its in-memory blocks. Increment 1's
    fault-in unit is the file block, so see §3.3.

**(b) Window state snapshotted whole: 854 MB.** These are `PartitionedWindowAggregateOp`,
`PartitionedOffsetOp` and `PartitionedRankOp` `.SnapshotNow`. Each one materialises its record
(`MaterializeUncached`) and its chunks into a Z-set, then the codec re-encodes it. This is
increment 2's target. Later checkpoints write only changes, which is why batches 2 and 3 read
under 1 MB.

**(c) Output write: 485 MB in the write phase.** `ParallelProgramOutput.FormView` streams the
whole view for batch 1's full write. The resumed batch 2's 474 MB is the same path, rebuilding
after a restart.

**Batch 1 conclusion.** Batch 1's peak is the 4.3 GB of blocks built during the drain, and none of
it is persisted until the checkpoint. The checkpoint's reads are of bytes that are resident
anyway. Bounding batch 1 therefore needs spilling (runs written mid-drain, Feldera's
moderate-pressure mode), not a fix to the checkpoint. **Deferred**, as agreed. Increment 1 bounds
the state from the first checkpoint on.

### 1.3 Fresh process (restore at batch 1, then batches 2–3)

| after | resident | never read (on disk only) | read this batch |
|---|---|---|---|
| restore (152 ms) | 0 MB | 3,858 MB | 0 |
| batch 2 drain | 230 MB | 3,632 | 229 (drain) |
| batch 2 (after write+ckpt) | 704 MB | 3,159 | +474 (output write) |
| batch 3 | 943 MB | 2,923 | 372 (drain) |

**The lazy-restore machinery already behaves like a buffer cache with no eviction.** A
restored process runs batches 2–3 correctly with under 1 GB of block bytes resident, against
4.9 GB warm.

A resumed batch 3 reads 4× what a warm one does (372 vs 90 MB). The likely cause is
ColdPromotion: a delta of at least 1/64 of the cold rows triggers one whole-pass read. The
resumed batch 2's 474 MB output-write read is the output connector rebuilding its index after
a restart.

## 2. What Feldera does

Paths are under `../feldera/crates/`.

- **The buffer cache holds decoded blocks.** Data blocks, index blocks and file trailers are
  keyed by `(file_id, offset)` (`dbsp/src/storage/buffer_cache/cache.rs:40-55`). Entries are
  weighted by their size in bytes.
- **Eviction is S3-FIFO by default**, with LRU as an option (`dev_tweaks.buffer_cache_strategy`).
  One cache is shared by each worker's foreground and background threads.
- **The default capacity is 256 MiB per thread**, so 512 MiB per worker. `StorageOptions.cache_mib`
  overrides it (`dbsp/src/circuit/runtime.rs:451-462`).
- **The writer inserts the blocks it writes into the cache.**
- **Index blocks are evictable too. Per-batch filters stay resident:** a key-range filter plus a
  Bloom filter or roaring bitmap. Probes are filtered through them before any block fetch.
- **Spilling is driven by memory pressure** (RSS / `max_rss`: 0.85, 0.90, 0.95):
  - At low pressure, only merge outputs whose inputs total at least `min_storage_bytes`
    (10 MiB) go to storage.
  - At moderate pressure, every merge output spills.
  - At high pressure, inserts spill too.
  - Fallback batches are `Vec` or `File`, and a builder can switch from one to the other partway
    through (`trace/ord/fallback/utils.rs:41-139`).
- **Merges stream through the cache with readahead** (`storage/file/reader/bulk_rows.rs`) and
  run in fuel-bounded steps off the step thread.

How this differs from us: Feldera spills between checkpoints by writing new files. We already
have durable run files for 82% of the bytes, written by the checkpoint anyway.

## 3. Proposed design

### 3.1 Increment 1: evict persisted run blocks (no new writes)

- **Rebind after a checkpoint.** When a checkpoint commits (`session.OnCommit` already sets
  `Run.Persisted`), rebind each written run's in-memory blocks to their file blocks.
  - Restore already does this with the file's block index: `LazyTraceBlock` over
    `(store, path, RunBlock)`.
  - The rebound block keeps its current bytes as its loaded body, so nothing is re-read.
  - A giant hash that spans several blocks in memory is one block on disk. The rebind therefore
    replaces the whole run's block array from the run file's index, as `AdoptRun` does. It must
    not map block to block.
- **A loaded body becomes droppable.** `LazyTraceBlock._loaded` gets a byte-budgeted cache.
  - Proposed policy: per worker, a CLOCK or S3-FIFO cache over loaded bodies, with the budget in
    bytes. `DBSPNET_BLOCK_CACHE_MIB` sets it; unset means unbounded, which is today's behaviour.
  - Eviction just clears `_loaded`. The file stays pinned by `RunPins` while the run is alive.
- **What stays resident per block:** first and last hash, count, file offset and length. That
  is the run's `_first`/`_last` arrays, which already exist. Feldera's Bloom filter is a possible
  later lever if misses on absent keys show up. Joins and aggregates probe by hash range, and a
  range hit already means a block read.
- **Expected effect** at a budget of about 2× the working set (~200 MB, set by the measurement):
  - Block residency: 4.9 GB → ~1.1 GB, which is window state (839 MB, until increment 2) plus the
    cache.
  - Step: unchanged on hits. A miss costs a synchronous file read, which restore already pays
    today (resumed batch 2: 478 ms drain for 230 MB faulted).
  - This is the same code path a resumed process runs in already, so correctness risk is low.
    `RunPins` and run lifetimes are already exercised.

### 3.2 Increment 2: window state as run files

Give the window record (`OrderedPartitionStore._cold`) and the chunks a run-file checkpoint, so
they become persisted and evictable like increment 1. The record is already a
`BlockedIndexedTrace`, so it can reuse `BlockedTraceCheckpoint`. The chunks would need a run per
store, keyed by partition hash. Estimated gain: 0.84 GB.

### 3.3 Things the cache must survive

- **Scans: the full view write (`FormView`), the window snapshot, and verify.** These should read
  through the cache without promoting what they read into it.
- **Giant file blocks.** Restore already faults in file blocks whole. A join's giant hash is one
  file block, averaging 344 KB, with some much larger. An evicted giant block faults back as a
  large-object-heap allocation. Increment 1 accepts this as restore does today, and charges the
  cache the block's real size. If it shows up in GC pauses, a later run-file version can let a
  hash span file blocks, as memory already does.
- **Merges.** A merge reads both input runs. Its inputs should be read as a scan that does not
  pollute the cache. Its output is unpersisted and stays resident until the next checkpoint.
  With geometric merging, the base run is rewritten rarely. Measure how often it happens in
  batches 2–3.
- **Between checkpoints, memory is not bounded.** Unpersisted runs (new deltas, merge outputs)
  grow until the next checkpoint persists them. With the default 60 s snapshot interval, that is
  the bound. Spilling unpersisted runs early, as Feldera does under pressure, is a later
  increment that would write files outside checkpoints.
- **Concurrency.** Workers own their traces. Eviction runs on the owning worker's thread, at tick
  boundaries or when the budget is crossed, so a block is never dropped mid-probe. The only
  shared mutation is `_loaded`, which already tolerates racing loads.

### 3.5 Increment 1 built and measured (2026-11-03)

**What was built.**
- **Rebind on commit.** Each run a checkpoint wrote is swapped (`BlockedIndexedTrace.Rebind`)
  for one whose blocks are `LazyTraceBlock`s over the run file. A block that is byte for byte a
  file block keeps its body. The run file is pinned (`RunPins`).
- **`BlockCache`.** A single process-wide cache with a byte budget (`DBSPNET_BLOCK_CACHE_MIB`;
  unset means off) and CLOCK eviction. A merge's reads are admitted unreferenced.
- **`ITraceBlock.Body`.** Probes, scans and merge cursors hold a block's body while they read
  it, so an eviction can't force one file read per entry.

**Two findings fixed along the way.**
1. **Re-read blocks went through `ArrayPool.Shared`, and the pool kept them.** Live heap grew
   about 200 MB per batch under a budget, outside any operator: the pool held a large buffer per
   size per core. Lazy blocks now read straight into their own array
   (`LocalRandomAccessFile.ReadExactly`), with no pool and no copy. This fixes today's lazy
   restore too.
2. **Giant hashes shared blocks with their neighbours.** Blocks are cut at the byte budget
   regardless of hash. A giant hash's first and last blocks therefore held neighbours, and on
   disk the whole span became one multi-MB block. A probe for a neighbour faulted in the giant:
   joins read 59 MB per batch instead of 5.8.
   - First fix: re-encode the edges at write time. That cost +470 ms (+17.5%) on batch 1's
     checkpoint.
   - Final fix: the block builder cuts the neighbours off. When the byte cut falls inside a hash,
     the entries before it are flushed as their own block, and a span's last block closes when
     the hash changes. Spans are pure in memory, and the writer only concatenates.
     `BlockedTraceCheckpoint.Pieces` still slices runs from older layouts.
   - Tests: the bounded-cache test (64-entry blocks) checks purity in memory and in the file.
     Each check was verified by its mutant.

**Memory, SF=3 W=12.** Live bytes after a full compacting GC, after each batch's checkpoint:

| | unbounded | 512 MiB | 128 MiB |
|---|---|---|---|
| after batch 1, warm | 5,775 MB | 2,277 | 1,889 |
| after batch 3, warm | 5,778 | 2,274 (flat) | 1,886 (flat) |
| after batch 3, restarted process | 2,024 | 1,236 | 846 |

What remains at 128 MiB (1.89 GB) is window state with no run file (0.84 GB, increment 2), the
cache, and managed state.

**Time, 5 runs per arm, rotated** (run8). Medians against main (60aa369), with views identical in
all 15 runs:

| | main | cache off | 512 MiB |
|---|---|---|---|
| b1 drain | 17,023 ms | +3.1% (ranges overlap) | +6.3% (ranges overlap) |
| b1 checkpoint | 2,580 ms | +6.6% (ranges overlap) | +6.8% |
| b1 output | 2,294 ms | −5% | −8% |
| b2 drain | 93 ms | +5% | **+13% (+12 ms)** |
| b3 drain | 113 ms | −2% | **+21% (+24 ms)** |
| b2/b3 checkpoint | 212/258 ms | level | level |

In the earlier run6 (128 MiB), the restarted process's batch 2 was level and its batch 3 was
+26 ms.

**The remaining cost is misses after eviction.**
- Rebind admits ~4 GB unreferenced, and the hand keeps whatever it reaches last. Batches 2 and 3
  then fault in their working set (~40–90 MB).
- A fault costs a read (page cache), the `Adopt` copy, and, for traces compared by value
  (aggregates, views, DISTINCT), recomputing the block's value hashes by decoding every value.
  Value hashes are not stored in the run file.
- None of this is apportioned yet. Measure it before choosing a lever:
  - store value hashes in the block (run file v4),
  - keep value hashes resident,
  - prefer recently read blocks at rebind,
  - read ahead.

### 3.6 What a miss costs, and the fixes (2026-11-03)

**Profile.** `DBSPNET_BLOCK_CACHE_PROFILE=1` prints a `FAULTS` line per batch phase. With
`DBSPNET_BLOCK_ACCESS=1 DBSPNET_BLOCK_ACCESS_STACKS=-1` it also records the caller of each
fault.

**Batch 2 at 512 MiB, summed over the 12 workers:** 7,450 faults, 426 MB.

| part | time |
|---|---|
| file open | 319–855 ms (a handle opened per fault) |
| read | 39 ms (page cache) |
| adopt | 290 ms, of which 278 was the Arrow reader parsing the schema and building arrays, and 12 the extra copy |
| value-hash recompute | 0 (never needed) |
| merges | 13 faults |

**Where the faults come from.** About 6,500 of batch 2's faults are a full view scan:
`ParallelProgramOutput.FormView`, reached through `ViewRowCounts` (the server's `/wait` row
count), right after the rebind. That is outside `drain_ms`. The drain's own faults are about 65 MB:
the output view's `WeightOf` on the driver thread, left joins, aggregates and joins.

**Tried and reverted: admitting scans cold.** Batch 3's faults went up 615 → 823, because the
scanned view blocks serve the output view's `WeightOf` probes. A scan here is not junk.

**Fixes.**
1. **The block header read directly** (`TraceBlock.TryParseHeader`). The record-batch flatbuffer
   gives the field nodes (null counts) and buffer offsets. A VARCHAR column has 3 buffers, every
   other kind 2. Anything unusual (compression, another header type) falls back to the Arrow
   reader, and `DBSPNET_BLOCK_PARSE=arrow` forces that reader. `TraceBlockTests` holds the two
   equal over 40 random schemas and both encoders; the test is mutation-checked.
2. **Kept-open read handles** (`LocalReadHandles`). An LRU of 512 (`DBSPNET_READ_HANDLES`), with
   leases so an evicted handle closes only after its read. Every create, rename, write and delete
   through `LocalTableFileSystem` invalidates its path, which the test's mutant confirms. If
   descriptors run out ("too many open files"), the idle handles close and the open is retried.

**After:** batch 2's faults cost open 41 + read 38 + adopt 19 ms summed (13 µs per fault, from
~84).

**The default path.** The span alignment of §3.5 makes ~10% more blocks (92,254 vs 83,675), and
cost the first checkpoint +8.7%. Slicing at write time instead cost +35%. Both pay only when
blocks can be evicted. `BlockAccess.AlignSpans` therefore follows the cache: on with
`DBSPNET_BLOCK_CACHE_MIB`, overridden by `DBSPNET_SPAN_ALIGN=on|off`. Without the cache, the writer
concatenates spans whole, as before.

**Final A/B** (run13, 5 runs per arm, rotated, views identical in all 15 runs). Medians against
main:

| | main | cache off | 512 MiB |
|---|---|---|---|
| b1 drain | 17,108 ms | +0.8% | +3.8% (ranges overlap) |
| b1 checkpoint | 2,647 ms | −4.6% | +4.3% (ranges overlap) |
| b1 output | 2,389 ms | +0.5% | −13% |
| b2 drain | 112 ms | −5% | −10% |
| b3 drain | 117 ms | +2.6% | **+7.7% (+9 ms)** |
| b2/b3 checkpoint | 208/255 ms | level | +5% / +2% |

The cache-off default is level with main. With the cache on, the remaining cost is batch 3's ~600
genuine misses. Some of them are serial, on the driver thread's output-view probes.

### 3.7 Increment 2: window state backed by files (2026-11-03)

Window state was the one large holder with no run file (§1.2): the small-partition record and the
large partitions' chunks, 839 MB. Each window operator's snapshot is its input rows as a Z-set
change chain, which restore reads. The store's blocks are a second, in-memory copy that nothing
on disk mirrors.

**Design.** With an evicting cache only (`BlockAccess.Evicting`, which follows the budget), each
checkpoint also writes **backing files** that only the cache reads back. Restore ignores them;
it keeps reading `trace.arrows`.
- **The record** (`record.backing`). The store's `BlockedIndexedTrace` is written through the
  ordinary blocked-run path (`IBlockedTraceCodec.SaveBackingAsync`). It is incremental: unchanged
  runs are named again. On commit it rebinds exactly like a join trace.
- **The chunks** (`runs/<snapshot>/<op>/chunks.backing`). The chunks not yet backed go into one
  block file per checkpoint (`SaveBackingBlocksAsync`). The file index lists each block under its
  ordinal, since chunks have no key hash and nothing looks them up by hash. On commit each chunk's
  block becomes a `LazyTraceBlock` that keeps its body, pinned per block (`RunPins`). The file
  isn't in the run index, so the pin is what keeps it from the sweep. A test mutant without the
  pin fails with `FileNotFoundException` after the sweep.
- **Chunk reads** take `Block.Body` once per chunk.
- **Re-shard** ignores `*.backing` files: they are never state, and the next checkpoint writes
  them again.
- The tests that set the process-wide budget now run in a collection with parallelism disabled.
  Flipping the budget beside another test made `IncrementalCheckpointTests` flaky.

**Tests.** `WindowBackingTests` runs SUM OVER, LAG and RANK by partition, with both small and large
partitions, at budgets of 1 byte and 4 KiB, checkpointing every 3 ticks with retain 1 and the
sweep awaited. It checks that the record runs and the chunks are file-backed after every
checkpoint, that outputs equal an uncheckpointed twin every tick, and that the checkpoint
restores exactly. Mutants (no backing, no chunk rebind, no pin) all fail it.

**Memory, SF=3 W=12.** Live heap after a full GC, after each batch's checkpoint, flat over batches
1–3. Views identical in every run.

| budget | inc 1 | + record | + chunks |
|---|---|---|---|
| 512 MiB | 2.27 GB | – | **1.42 GB** |
| 128 MiB | 1.89 GB | 1.34 GB | **1.04 GB** |

At 128 MiB the resident block bytes equal the budget (118 MB), and every block is persisted. The
remaining ~0.9 GB is managed state outside blocks. A restarted process is unchanged (0.85 GB at
128 MiB), because its window state is faulted from the restore chain, not rebuilt.

**Time.**

Run16, 5 runs per arm, medians against main:

| | main | 512 MiB |
|---|---|---|
| b1 checkpoint | 2,499 ms | +13.4% (it now also writes ~0.8 GB of window backing) |
| b1 drain | 16,450 ms | +2.6% |
| b2 drain | 91 ms | +8% (overlapping) |
| b3 drain | 116 ms | +15% (+17 ms; window misses add to the join and view misses) |

Run17, cache off against main, 6 runs per arm:

| | main | cache off |
|---|---|---|
| b1 checkpoint | 2,699 ms | −0.1% |
| b1 drain | 17,564 ms | +3.8% (ranges overlap: 16.6–18.6 vs 16.9–18.8 s) |
| b2/b3 drain | 98/119 ms | +5% / −7% |

The cache-off path is level.

### 3.8 What a 128 MiB budget leaves in memory (2026-11-03)

Object census (`IVM_STATE_CENSUS=1` with `IVM_LIVE_EACH_BATCH=1`), warm, after batch 3's checkpoint
at 128 MiB. Live heap is 846 MB, flat over batches 1–3; the census reaches 794 MB of it.

| holder | bytes | what it is |
|---|---|---|
| runner `_outputs` | **546 MB** | the incremental Delta connector's `OutputRowIndex` (§3.9–§3.10 of the flat-trace doc): a `BlockedIndexedTrace` of view rows → file positions |
| views' `_runs` | 95 MB | view blocks resident in the cache (within the budget), serving the output view's `WeightOf` probes |
| window `_large` | 34 MB | chunk order values (`long[]`, 8 B per row) and partials, resident by design |
| codecs, schemas, other per-op metadata | ~60 MB | thousands of small objects over ops × 12 workers |
| joins, aggregates, DISTINCT, rank | ~30 MB | run headers, caches |

**The output row index is two-thirds of what is left.** It has no file behind it, so the cache
cannot drop it. A restart rebuilds it, which is the 474 MB read in a resumed batch 2's output
write (§1.3). Feldera keeps no such index: its output is append-only, with no deletion vectors.

The options:
- **(a)** A backing file written at each checkpoint, as §3.7 does for window state. This makes
  it evictable without changing any semantics.
- **(b)** Persisting it as state. It would then also save the rebuild after a restart, but it
  must stay aligned with the Delta table's contents at the checkpoint.
- **(c)** An append-only output mode, as Feldera has. That is a connector-semantics change.

**RSS is not the live heap.** RSS stays at 7–8.7 GB at a 128 MiB budget against ~1 GB live.
Server GC keeps the heap it committed, and batch 1's peak sets it (§1.4: batch 1 is not bounded).
Bounding the process needs that peak bounded (spilling) and the GC told
(`GCHeapHardLimit`/`GCConserveMemory`). Neither is measured yet.

### 3.9 The output row index, spilled (2026-11-03)

Option (a) of §3.8. The incremental Delta connector's `OutputRowIndex` is part of no checkpoint,
so it gets its own **spill** (`BlockSpill`).
- **Where:** a private scratch directory, `DBSPNET_BLOCK_SPILL_DIR` (default the temp directory),
  one subdirectory per index, removed at process exit.
- **When:** after each batch's `Commit`, synchronously, every run without a file is written
  there (`BlockedTraceCheckpoint.SpillRun`) and rebound at once. The files are a cache, not
  state, so there is no commit point to wait for.
- **Cleanup:** files whose runs merges have retired are deleted, and `Clear` deletes them all.
- **Gating:** only with an evicting cache. With the cache off, no spill is created.

**Tests.** `OutputRowIndexSpillTests` runs the model test at a 1-byte budget, with every block
evicted, and checks that runs are file-backed with one file per live run. A mutant that never
deletes stale files fails it.

**Memory, 128 MiB**, views identical:

| | before | with spill |
|---|---|---|
| warm, after batches 1–3 (flat) | 1.04 GB | **0.51 GB** |
| restarted process | 0.85 GB | **0.33 GB** |

From the start of the arc, a 128 MiB budget takes the warm live heap 5.78 GB → 0.51 GB.

**Time.**

Run20, 5 runs per arm, medians against main:

| | main | 512 MiB |
|---|---|---|
| b1 output | 1,946 ms | **+36% (≈+700 ms)**: the index's ~0.5 GB written synchronously in the output phase |
| b1 checkpoint | 2,552 ms | +4% |
| b2 drain | 93 ms | +12% (+11 ms) |
| b3 drain | 118 ms | +5% |
| b3 checkpoint | 247 ms | +13% (overlapping) |

Run21, cache off against main, 6 runs per arm (run20's cache-off +22% on b1 output was drift; it
did not reproduce):

| | main | cache off |
|---|---|---|
| b1 output | 2,126 ms | +3% (overlapping) |
| b1 checkpoint | 2,666 ms | −4% |
| drains | | within noise |

**Not built:** moving the spill write off the output path, into the background, rebinding at the
next commit. §3.24 of the flat-trace doc found that a background write between batches only
moves the cost when there is no idle time, and ivm-bench has none.

### 3.4 Gates

These are the usual ones:
- ivm-bench SF=3 W=12, 5 runs per arm, budgets unbounded / 512 MiB / 128 MiB.
- Batch-1 drain within +3%.
- Batches 2 and 3, restart, and checkpoint times.
- Views byte-identical.
- Peak RSS and the census.
- Nexmark W=14: no regression with no budget.

## 3a. Batch 1: where its peak is (2026-11-03)

**Instrument.** `IVM_TICK_LIVE=1` (census only) samples after every batch-1 tick, through
`ProgramRunner.AfterTick`. It forces a full compacting GC, then reports the heap before, the live
bytes after, block bytes, unfrozen managed entries and window write-back rows.
`IVM_TICK_CENSUS=<tick>` adds an object census at that tick. `IVM_TICK_DROPSTREAMS=<tick>` clears
every operator stream's current value there and measures live again (an experiment, not a fix).

**Batch 1 is one huge tick, not a climb.** Ticks 0–17 stay at 1.2–5.2 GB live. Tick 18
(`trade_type`: a 4.8 s step, 18.6 GB allocated in that tick alone) peaks at **14.1 GB live and
16.6 GB RSS**. Tick 19 (`audit`, small) falls back to 7.1 GB, of which 4.3 GB is blocks. RSS then
stays at the high-water mark: server GC keeps what it committed.

**What is live after tick 18** (14.1 GB):

| part | bytes | lever |
|---|---|---|
| the tick's deltas, still referenced by operator streams (`Stream.Current` until the next tick overwrites it) | **6.3 GB** (clearing every stream value: 14.06 → 7.73 GB) | release a value once its consumers have read it. Not spilling. |
| block state | 3.8 GB | spill runs mid-batch (Feldera's moderate-pressure mode) |
| window write-back caches and chunks | ~2.4 GB (2.65 GB, 47M objects, at the drain's end) | flush by a byte cap as well as at checkpoint |
| aggregate state caches, unfrozen levels, other | ~1.5 GB | — |

**A false start: the exchange mailbox.** The tick-18 object census put 4.2 GB in the exchange
coordinators' mailboxes. Buckets stayed in the W×W grid until the next tick overwrote them.
Readers now `Take` their cell, clearing it as they gather. That is correct, and the suite passes,
but live did not move (14.32 → 14.06 GB). The same rows stay reachable through the operators'
output streams, which the census charges elsewhere or not at all. Clearing a pooled-array
suspect (freeze's `ArrayPool` scratch arrays) did not move it either.

**Order proposed.**
1. **Release consumed stream values.** It targets 6.3 GB, and helps every batch's transient
   peak, not only batch 1's. Two cuts:
   - (a) clear every stream at the end of each tick, after the runner has read the outputs. This
     is simple, and bounds what one tick keeps into the next.
   - (b) release each value after its last consumer within the tick. This also lowers the in-tick
     peak. It needs consumer counts and care with the output handles and exchanges that read
     streams after `Step`.
   - Check how Feldera's scheduler drops consumed values before choosing.
2. **Window write-back caches:** flush to the record once they pass a byte budget.
3. **Spill block runs mid-batch.** Write runs to the spill directory during the drain and rebind
   them (§3.9's mechanism). A checkpoint can then link or copy them instead of re-encoding. This
   is the Feldera-style lever, and it is the last 3.8 GB.

Process RSS also needs the GC told: `GCHeapHardLimit` or `GCConserveMemory`, once the peak is
bounded.

## 3b. Releasing consumed stream values (2026-11-03)

**Feldera** (`crates/dbsp/src/circuit/circuit_builder.rs`, `StreamValue`) counts each stream's
consumers when the circuit is built. Every consumer gives back a token after its evaluation, and
the last consumer takes or drops the value, so a delta lives only until its last reader has run.

**Ours** (`StreamRelease`; opt-in when built, on by default since §3e, `DBSPNET_STREAM_RELEASE=0` turns it off). Operators declare no inputs, so:
- **Readers:** a conservative reflection scan of each operator's fields, through wrappers,
  closures and small arrays, but never into another operator or a collection's contents. A
  superset of readers only releases later.
- **Producers:** observed on the first tick (`Stream.SetCurrent` records the stepping operator).
- **Plan:** built after the first tick. After operator *i* steps, the values of the streams whose
  last reader is *i* are set to null, so a missed reader fails loudly.
- **Only forward edges are released** (producer before every reader). Not released: feedback
  through delays, input-handle streams (written outside the step), streams no other operator
  reads, and **pinned** streams. Output handles pin their stream, and an integrated view's handle
  pins the integrate operator's input, because `LastDelta` reads it after the step. That last one
  was the only post-step reader the suite found: 14 failures, all with a loud
  `NullReferenceException`.
- Introspection's `LastOutputRows` reads are null-safe.

**Tests.** The full suite passes with release on (3,279).

**ivm-bench SF=3 W=12, batch 1.** Live after each tick, from the `IVM_TICK_LIVE` census runs:

| tick | off | release on |
|---|---|---|
| 1 | 4.33 GB | 1.56 GB |
| 14 | 5.18 GB | 2.80 GB |
| **18 (the peak)** | **14.06 GB** | **8.47 GB** |
| 19 | 7.04 GB | 7.06 GB |

**A/B**, 5 runs per arm, views identical in all of them.

Default GC (run24):
- b1 drain −13%, GC pause −25%, checkpoint −9%; batches 2 and 3 level.
- b1 output +28%, bimodal: over run23 and run24, 6 of 10 release runs sit at ~2.9 s against 1 of
  10 without.

With `DOTNET_gcConcurrent=0` (run25):
- b1 output −1%: the bimodality is background-GC timing, as in §3.31 of the flat-trace doc.
- b1 drain −2%, GC pause −18%.

**Peak RSS** (a testhost `ps` sampler, run25): 19.2 → 17.3 GB, only −10%. RSS is driven by
garbage, not live data. Tick 18 allocates 18.6 GB, and server GC's per-heap gen0 budgets let it
pile up between collections. Bounding the process needs the GC told: `GCHeapHardLimit`,
`GCConserveMemory`, or a DATAS/heap-count setting. With live data at the peak now 8.5 GB, that
is the next experiment.

## 3c. Telling the GC: limits on peak RSS (2026-11-03)

All arms below have stream release on, `DOTNET_gcServer=1` and `DOTNET_gcConcurrent=0`, 3 runs
each, with peak RSS from the testhost `ps` sampler. Views are identical in every completed run
(run26, run27).

| arm | peak RSS | b1 drain | b1 GC pause | b1 checkpoint | outcome |
|---|---|---|---|---|---|
| default | 16.8–18.0 GB | 15.0–16.2 s | 4.6–5.1 s | 2.2–2.8 s | |
| `GCConserveMemory=7` | **14.7–15.8 GB** | 15.3–16.0 s | 4.6–5.3 s | 2.2–2.9 s | level on time |
| `GCgen0size=64 MB` | 17.1–17.8 GB | level | level | level | no effect |
| `GCDynamicAdaptationMode=0` (DATAS off) | 17.7–18.4 GB | level | −6% | −8% | slightly more RSS |
| `GCHeapHardLimit=12 GiB` | 12.9 GB | 15.6 s | 7.3 s | 4.2 s | **OOM in 5 of 6 runs** |
| `GCHeapHardLimit=10 GiB` | 10.8 GB | — | — | — | **OOM in 3 of 3** |

**The drain fits.** Under either hard limit batch 1's drain completes; every OOM is in batch
1's *checkpoint*, in `BlockWriter.Concat`. Without the block cache, a hash spanning blocks is
written as one file block, concatenated in memory first (§3.6). On ivm-bench that is 2,497
concatenations per checkpoint, **2.7 GB in total**, the largest 175 MB (930k entries over 2,914
blocks, a join trace). All of it is transient: the giant's body is not kept.

**A copy removed.** The writer copied every block's message (`ReadOnlyMemory.ToArray()`) before
writing it: the whole state, once more, at every checkpoint. `AppendRawAsync` now takes the
memory. Checkpoint −7% (2.55 → 2.38 s, within noise); peak RSS unchanged, since the peak is the
drain. It moved the 12 GiB arm from 0 of 3 completions to 1 of 3.

**Reading.** Conserve-memory buys ~2.3 GB (−13%) for free. A hard limit is the real bound, and
the obstacle to it is now the checkpoint's giant concatenations, not batch 1's data. Writing a
giant straight to the file from its parts removes the 2.7 GB: §3d.

## 3d. Giant groups streamed to the file (2026-11-03)

A hash spanning blocks is one file block (§3.2). It was built in memory by `BlockWriter.Concat`
and then written: 2.7 GB of transient copies per batch-1 checkpoint on ivm-bench (§3c), and the
cause of every OOM under a hard heap limit. Now `BlockWriter.ConcatToAsync` writes the same message
(header from the layout `Concat` computes, which is now shared) straight to the run file:
- fixed widths and string heaps from the parts' own bytes;
- validity and BOOLEAN bitmaps, and rebased string offsets, through one scratch buffer of at most
  `max(n/8, 4 × largest part)` bytes.

`RunStreamWriter.AppendConcatAsync` takes the parts; `Pieces` (with the cache) and the whole-span
path (without it) hand giants over as parts. Their bodies were never kept, so nothing else changes.
A test checks the streamed bytes equal `Concat`'s for every cell kind with nulls, over 1–4 parts.
Mutating the offset rebase, the validity rule or the padding fails it.

**A/B** (run28; 3 runs per arm; release on, `gcServer=1`, `gcConcurrent=0`; views identical in all
completed runs):

| arm | peak RSS | b1 drain | b1 output | b1 GC pause | b1 checkpoint | outcome |
|---|---|---|---|---|---|---|
| previous (5e9970a) | 17.4–18.7 GB (avg 18.0) | 15.4 s | 2.29 s | 4.76 s | 2.33 s | |
| streamed | 15.7–16.7 GB (avg 16.1) | 15.2 s | 2.35 s | 4.67 s | 2.26 s | **−10% RSS, level time** |
| streamed + `GCConserveMemory=7` | 14.9–15.5 GB | 15.5 s | 2.21 s | 5.10 s | 2.42 s | |
| streamed + `GCHeapHardLimit=12 GiB` | **12.9 GB** | 15.4 s | 3.13 s | 6.30 s | 2.75 s | **3 of 3 complete** (was 1 of 3) |
| streamed + `GCHeapHardLimit=10 GiB` | 10.8 GB | — | — | — | — | OOM 3 of 3 |

So the peak RSS that §3b and §3c measured was partly the checkpoint's concatenations, not only the
drain's garbage. A 12 GiB limit now holds: drain level, GC pause +32%, output +37%, checkpoint
+18%. At 10 GiB the failures are spread out (the output's Arrow build, a window store materialised
as a Z-set for its checkpoint, a run file's Z-set write): the live floor after the drain is near.

## 3e. Defaults: stream release on; GC settings are the host's (2026-11-03)

**Stream release is on by default** (`DBSPNET_STREAM_RELEASE=0` turns it off). Before that, the
reader scan had to be made safe for operators it did not write:
- The §3b scan stopped at depth 8, at collections other than small lists, and at System types. A
  reader behind any of those would see its input released early. A first fix marked such an
  operator *opaque* (read every stream), but 386 operators per worker on ivm-bench came out opaque
  (deep SQL plan objects past depth 8) and peak RSS went back to 21–22 GB.
- **Now the scan walks the real object graph**, bounded by a visited set, and enters only objects
  whose type can hold a stream. Types that cannot (cached per type): primitives, strings, Z-set
  rows (`DbspNet.Core.Collections`), non-generic System types, System generics whose arguments
  cannot, and types from assemblies that do not reference DbspNet.Core (Arrow), except through
  their type arguments. Object, interfaces, unsealed classes and delegates may hold anything.
- Collections are walked element by element. A lazy sequence is not enumerated: its fields are
  walked, since what it yields comes from them. It stops at root operators and the circuit.
- **Opaque** is left for a collection of more than 65,536 elements or nesting past depth 200. It
  extends every later stream's life to that operator and keeps any stream produced after it.
- On ivm-bench: 0 opaque operators, 549 streams released per worker, plan built once in ~214 ms.
- Tests: release after the last reader, and readers through a dictionary, a lazy sequence and a
  70,000-element list (opaque). Removing the opaque rule or the release fails them. Suite: 3,283.

**A/B** (run30; 3 runs per arm; `gcServer=1`, `gcConcurrent=0`; §3d's streaming in all arms; views
identical in all runs):

| arm | peak RSS | b1 drain | b1 GC pause | b1 output | b1 checkpoint |
|---|---|---|---|---|---|
| release off | 19.5 GB | 16.4 s | 5.8 s | 2.32 s | 2.16 s |
| §3b scan (opt-in) | 18.0 GB | 15.9 s | 4.9 s | 2.37 s | 2.43 s |
| **default (new scan)** | **17.1 GB** | **15.2 s** | **4.8 s** | 2.35 s | 2.50 s |

Default against off: peak RSS −12%, drain −7%, GC pause −18%; batches 2–3 level.

**GC settings cannot be library defaults.** The runtime reads them once at process start, from the
host's `runtimeconfig.json` (`System.GC.ConserveMemory`, `System.GC.HeapHardLimit`) or `DOTNET_`
variables. Recommended for a host bounding memory:
- `GCConserveMemory=7`: about −1 GB peak RSS at level time (§3c, §3d).
- `GCHeapHardLimit` at 12 GiB holds on ivm-bench SF=3 W=12 (§3d), at the cost of GC pause +32% and
  output +37%. The floor is set by the places that build a whole result before writing it (§3d).

## 3f. Output and checkpoint: garbage, not transients (2026-11-03)

§3d's OOMs at 10 GiB (the output's Arrow build, a window store materialised for its checkpoint, a
Z-set run write) looked like three large transients. Measured, batch 1 (`IVM_PHASE_LIVE=1`: full
GC at each phase boundary; `IVM_ALLOC_TYPES=1`: allocation by type per phase; release on, no cache):

| boundary | live | allocated in the phase |
|---|---|---|
| end of drain | 7.06 GB (peak in tick 18: 8.5) | 54.8 GB |
| end of output | 7.60 GB | **4.7 GB** for 3.6M rows, 175 MB of Parquet |
| end of checkpoint | 5.78 GB | **8.3 GB** |

None of the three sites is a big transient by itself. The output and checkpoint phases churn
13 GB of garbage, and under a hard limit that is where the GC time goes (§3d: output +37%,
checkpoint +18%). By type:
- **Output (4.7 GB).** `byte[]` 1.5 GB (Arrow buffers, Parquet), `string` 0.9 GB, then rows:
  `object[]`, `Utf8String`, `StructuralRow`, boxed `double`/`long`/`Timestamp` (~1.3 GB) from
  decoding the blocked view into rows to build Arrow. The `string`s are engineered-wood's Delta stats
  (`StatsCollector.CollectStringMinMax`): every VARCHAR cell becomes a .NET string to find the
  column's min and max. Comparing UTF-8 bytes (code-point order, which the code already specifies)
  and decoding only the final bounds would remove them (filed as engineered-wood#661).
- **Checkpoint (8.3 GB).** `byte[]` 2.8 GB, then rows again: `object[]` 1.1 GB, `Utf8String` 0.8,
  boxed `double` 0.6, `StructuralRow` 0.4, `(row, row, long)[]` 0.2. That is state decoded into
  rows and re-encoded (window stores via `SnapshotNow` → Z-set → run file; managed levels frozen
  to blocks).

So the lever is not streaming but **not decoding**: block → Arrow for the output, and window chunks
written as blocks for the checkpoint. Both cut time as well as memory.

## 3g. Window stores checkpoint as their own files (2026-11-03)

§3f's per-operator profile (`IVM_CHECKPOINT_PROFILE=alloc`, one worker, exact): the window
operators were 6.6 of the batch-1 checkpoint's 6.9 GB allocated and 5.0 of its 6.3 s (aggregates
3.7 GB, LAG/LEAD 2.9). They kept their state as blocks and wrote it as rows: `SnapshotNow` decoded
every row into a Z-set, which `RunFile.WriteZSetAsync` sorted by partition hash and encoded again.

**Now an `OrderedPartitionStore` checkpoints as its own files**, nothing decoded:
- **Small partitions:** the shared record (a `BlockedIndexedTrace`, partition key → row) as the run
  chain a blocked trace writes (`store.record`): runs an earlier checkpoint wrote are named again.
- **Large partitions:** their chunks as a **block set** (`store.chunks`). Blocks an earlier
  checkpoint wrote (durable `LazyTraceBlock`s in the same store) are named, the rest copied raw to
  one new file. Past 16 files the set is rewritten into one, so a set cannot pin every file ever
  written.
- **Manifest:** the store's metadata (each large partition's key hash and chunk sizes, and the
  record's key filter), then each block's file and index entry. It is a file under `runs/`, first
  in the chain the snapshot's run index records, so nothing is written in the operator's directory.
  A store whose manifest is unchanged since its last commit, or since the restore that read it,
  re-records that chain and writes nothing.
- **Restore** adopts all of it lazily: the record's blocks are read on probe, and a large
  partition (known by key hash) is decoded on first touch, which recomputes its chunks' order
  values and partials. The key filter is restored with it.
- **Re-shard** (`IStoreSnapshot`) reads a worker's store files into a scratch store, takes its rows,
  and writes them in the old `trace.arrows` form at the new W. Restore reads either form, so
  snapshots from before this change still restore.
- **Fallback:** without the runs layout, or without block formats, the operator writes its rows as
  before.

**Tests.** `WindowStoreCheckpointTests`:
- a lazy restore (nothing read, large partitions untouched) that carries on exactly;
- an untouched restore re-checkpoints with no writes;
- unchanged chunks named again;
- the file cap;
- a checkpoint into another store.

`ParallelReshardTests` re-shards from store files (3→2, 2→4). The lazy-partition test now checks
the store's laziness, and three chain tests that used window operators to exercise Z-set chains run
with window stores off. Mutations (no fault-in, filter lost, never reuse, no cap, re-shard ignores
the store, no unchanged skip, no restore seed) each fail a test. Suite: 3,289.

**A/B** (run33; 3 runs per arm; release on, `gcServer=1`, `gcConcurrent=0`; views identical in
every run; previous = dcd9c29):

| | previous | store files |
|---|---|---|
| b1 checkpoint | 2.24 s | **1.14 s (−49%)** |
| b1 checkpoint allocated | 8.0 GB | **1.7 GB (−79%)** |
| b1 GC pause | 4.8 s | 4.1 s (−16%) |
| peak RSS | 16.1 GB | 14.9 GB (−8%) |
| b2 / b3 checkpoint | 0.22 / 0.25 s | 0.23 / 0.27 s (+6%) |
| fresh-process restore | 162 ms | 180 ms |
| b2 / b3 checkpoint after restore | 0.24 / 0.25 s | 0.27 / 0.28 s (+10%) |

Two earlier versions measured worse on the later batches:
- **First version:** a manifest file per store in the operator's directory on every checkpoint
  (468 per checkpoint) made b2/b3 +43%.
- **Second version:** after a restore, stores did not know their manifests were already on disk.

The remaining +15–25 ms on later checkpoints is manifests of stores that did change, which are
rewritten whole.

## 3h. Full output writes from the view's blocks (2026-11-03)

Batch 1's output allocated 4.7 GB to write 3.6M rows (`DBSPNET_OUTPUT_TRACE` now records bytes per
stage):

| stage | allocated |
|---|---|
| form the view: decode every block row, sum the W shards into one Z-set | 1.39 GB |
| Arrow from those rows | 0.85 GB |
| engineered-wood's Parquet write (0.9 GB of it per-cell strings for stats) | 1.46 GB |
| the connector's row index: every row added as a key, encoded again | 0.99 GB |

**engineered-wood** fixed the stats (#661, 9e4ce64: compare UTF-8 bytes, decode only the bounds);
the submodule is at that commit. Parquet write: 1.46 → 0.64 GB, 941 → 616 ms.

**The rest is built from the blocks now.** Stored blocks already hold every column in its Arrow
layout (INT, BIGINT, REAL, DOUBLE, BOOLEAN, VARCHAR, DATE, TIME, TIMESTAMP(us), DECIMAL128).
- `BlockedIndexedTrace.StreamEntries` yields each consolidated entry as (block, index, weight) in
  hash order:
  - an entry whose hash only one run holds passes through;
  - entries of a hash several runs hold are decoded only to sum equal rows;
  - rows not yet frozen are encoded into a scratch run, and the trace is left unchanged;
  - null (fall back to rows) when a run or level carries a GC floor.
- `ViewArrow.TryBuild` reads every shard's entries (a row with count n is written n times) and
  copies each column from the blocks, a run of consecutive entries at a time. It returns null
  when a count is negative on some shard (then the shards must be summed) or a type does not match.
  The shards' integrals are not summed: with no negative counts, writing each shard's rows is the
  same multiset.
- The row index takes the same entries (`OutputRowIndex.AddEntries`):
  - The blocked index sorts them by the hash their blocks carry.
  - `BlockWriter.Gather` copies each entry's key columns and writes the address as the value
    column, one run per write (`BlockedIndexedTrace.IntegrateGathered`).
  - A managed index decodes.
- The connector side is internal: `IBlockOutputConnector.ApplyViewAsync(batches, entries)`, which
  `DeltaOutputConnector` implements. `IOutputConnector` is unchanged, and other connectors keep the
  row path. Only a connector's first (full) write in a process takes it; incremental batches carry
  deltas as before. `DBSPNET_OUTPUT_BLOCKS=0` turns it off.
- **Batch size.** Batches are up to 1M rows (`DBSPNET_OUTPUT_BATCH_ROWS`). engineered-wood
  allocated 1.47 GB writing the same rows in 64K-row batches against 0.64 GB in one batch per view:
  its rolling file writer concatenates (copies) a row group's batches before encoding it
  (engineered-wood#667).

**Tests** (`OutputFromBlocksTests`): the same input written by two runners, block writes on and
off, must leave identical Delta tables after every batch:
- every column kind, nulls, multiplicities above one, tiny blocks (several runs and an unfrozen
  level at write time), W = 1 and 3;
- four incremental batches after the full write, their deletes found through the gathered index.

Mutations (string offsets, validity, multiplicity, gathered addresses, unfrozen rows ignored,
multi-run sums, boolean bits) each fail them. Suite: 3,293.

**A/B** (run34; 5 runs per arm, same binary, `DBSPNET_OUTPUT_BLOCKS=0` vs default; views identical
in all 10):

| | row path | blocks |
|---|---|---|
| b1 output | 2.18 s | **1.78 s (−18%)** |
| b1 GC pause | 4.09 s | 3.98 s |
| peak RSS | 15.9 GB | **14.3 GB (−10%)** |
| b1 drain, checkpoint | level | level |
| b2 / b3 output | 19 / 16 ms | 23 / 14 ms |

The b2 output is +4 ms. Gathered index blocks are now cut by bytes as well as entries, as a
freeze cuts rows (`ITraceBlockFormat.GatheredEntryBytes`, `GatheredRunTests`), but that did not
close the gap (run35, 5 runs against the count-cut build: b2 output 22.0 → 22.4 ms, b3 13.8 →
16.2). Block size was not the cause.

**The +4 ms is JIT** (run36, `DBSPNET_OUTPUT_TRACE`, 3 runs per arm):
- All of the difference is the runner's `mat` stage, the delta's rows to Arrow: 6.0 ms with
  blocks against 2.3 ms without. Index lookups, Parquet and commit are equal.
- It falls on the first views written (`dim_account`, `dim_customer`, ...).
- Batch 2's JIT time is higher by the same order: median 54.5 against 40.5 ms.
- By batch 3 `mat` is 3.0 against 2.0 ms.

The block write never runs the row-to-Arrow builders (`ToArrowChanges`, `ArrowColumns`) in batch
1, so batch 2 compiles them. It is a one-time cost per process, not a steady-state regression.

Per stage on the probe (one batch per view): forming 1.39 GB → 0, index 0.99 → 0.66 GB. The Arrow
build's 0.8 GB is the output's own bytes.

## 3i. The GC heap hard limit, again (2026-11-04)

With release on by default, window store files (§3g) and output from blocks (§3h), on
ivm-bench SF=3 W=12 (`gcServer=1`, `gcConcurrent=0`, 3 runs per arm, peak RSS from the testhost
sampler, views identical in every completed run):

**At first it got worse** (run37):
- 12 GiB failed 1 of 3 (§3d: 3 of 3 passed), and 10 and 9 GiB failed every run.
- Nearly every failure was in engineered-wood's Parquet row-group encoding during the output.
- run38 isolated the cause: the row output path passed 12 GiB 3 of 3, and the block path failed 2
  of 3 with either engineered-wood. The block path built its Arrow buffers as managed `byte[]`,
  so under the limit the heap had to hold the output. Apache.Arrow's builders, which the row path
  uses, allocate native memory, outside the limit.

**Now `ViewArrow` allocates from Arrow's native allocator when a heap hard limit is set**
(`DOTNET_GCHeapHardLimit[Percent]` or runtimeconfig; `DBSPNET_OUTPUT_NATIVE=on|off` overrides).
The buffers are freed as soon as the sink has written the view. Without a limit they stay managed,
since managed garbage reuses heap the GC already holds. The harness prints `native_output`.

| batch 1 (run39, run40, run42) | default | 10 GiB | 10 GiB + conserve | 9 GiB | 8 GiB |
|---|---|---|---|---|---|
| outcome | pass | **pass 9/9** | pass | OOM 3/3 | OOM 3/3 |
| peak RSS | 14–15.5 GB | **10.76 GB** | 10.8 GB | | |
| drain | 15.1 s | 16.0 s (+6%) | 15.7 s | | |
| output | 1.57 s | 2.2 s (+40%) | 3.0 s | | |
| GC pause | 3.9 s | 5.3 s (+36%) | 6.1 s | | |

At 10 GiB the block path beats the row path (run39): output 2.36 against 3.47 s, GC pause 5.1
against 6.5 s, and 3/3 passes against 2/3. 9 and 8 GiB fail in the drain: its live peak
(~8.5 GB in tick 18, §3a) is the floor until state spills mid-batch.

**Output batch size** (run41, engineered-wood 02c97cb, which joins strings into exactly-sized
buffers, #668): 64K-row batches against 1M did not lower peak RSS, were slower (output 1.79
against 1.62 s), and failed once in 3 at 10 GiB. Batches stay at up to 1M rows.

**Peak RSS without a limit is noisy, and unexplained** (run43, one session, managed buffers
unless noted):

| | peak RSS | b1 output |
|---|---|---|
| engineered-wood 9e4ce64 | 13.5 / 14.1 / 13.9 GB | 1.64 s |
| engineered-wood 02c97cb | 15.8 / 15.6 / 13.2 GB | 1.52 s |
| 02c97cb, native buffers | 15.4 / 15.5 / 15.8 GB | 1.61 s |

The newer engineered-wood's runs are bimodal, and its change only touches concatenation, which
1M-row batches rarely reach. With 3 runs per arm this does not separate engineered-wood from GC
timing. Native buffers cost ~0.6 GB at most here, so they stay conditional.

## 4. Open questions

1. **Budget policy.** A fixed byte budget per worker, as Feldera does by default, or one driven
   by memory pressure (RSS against a limit)? Proposal: fixed first; pressure as a follow-on.
2. **Scope of increment 1.** Evict only persisted blocks, so nothing new is written, and accept
   that memory between checkpoints is bounded only by the checkpoint interval?
3. **Batch 1.** Its working set is the whole state (the bulk load), and nothing is persisted
   until its checkpoint. Bounding batch 1 itself needs spilling (Feldera's moderate-pressure
   mode). Is that in scope for this arc?

Q3 is taken up in §5.

## 5. Mid-batch spilling: design (2026-11-04)

**Why now.** Two workloads need state to leave memory *during* a batch, before any checkpoint:
- **TPC-DI SF=3 W=12.** A 9 GiB heap limit OOMs in batch 1's drain (§3i). Its floor is tick 18.
- **Extended TPC-DS SF1 (issue #2).** 88M input rows, and the 96 models total 304M output rows.
  It OOMs at W=1 under a 48 GiB cap after ~3 min, in `BlockedJoinOp` → `Freeze` →
  `BlockWriter.Write`, at RSS ~43 GB. ivm-bench sets no `DBSPNET_SNAPSHOT_DIR`, so today nothing
  in it can be evicted at all.

### 5.1 What Feldera does (re-read at origin/main 43f0d6c, November 2026; updates §2)

Paths are under `../feldera/crates/dbsp/src/`.

- **Spilling needs storage.** Every threshold is `None` without a storage backend, and then
  nothing spills. 70f60b489 (October) also stopped the merger relieving pressure without storage:
  it had merged its own in-memory output forever.
- **The limit** is `min(max_rss, memory available)` minus a reservation (capped at half). The
  available memory is the host's or the cgroup's, and is rechecked every second (b03ea8036). A
  pipeline with no `max_rss` still has a limit. ivm-bench sets `max_rss_mb: 0`.
- **Pressure** comes from a 1 s RSS monitor (`circuit/runtime.rs`): Low below 85% of the limit,
  then Moderate, High (90%) and Critical (95%). High and Critical have hysteresis: they hold until
  RSS falls two levels.
- **What spills at each level.** "Build on storage" means the batch builder writes the file
  directly. `fallback` batches are `Vec` or `File`, and a `Threshold` builder switches partway
  through (`trace/ord/fallback/utils.rs`).

| level | merge outputs | batches inserted into a spine | a step's transient batches |
|---|---|---|---|
| Low | ≥ `min_spill_size` (10 MiB) | memory | memory (`min_step_spill_size` unset) |
| Moderate | all | ≥ 10 MiB | memory |
| High | all; the merger also force-merges every in-memory batch, to move it to storage | all | memory |
| Critical | all | all | all non-empty |

- **Feldera spills even at Low.** With storage on, any merge output of 10 MiB or more goes
  straight to a file behind the buffer cache. ivm-bench's Feldera runs with storage, and its
  client notes "batches do spill to storage here". So Feldera's large state lives on disk by
  default, not only under pressure.
- **A spilled batch is a checkpoint file.** `Spine::save` names each batch's file, writing it
  first only if it is still in memory. Since 29880eec9 (September), checkpoints are written in
  the background, and the in-memory batches they write stay resident. That is our increment 1
  with the cache off.
- **Backpressure counts batches, not bytes.** The step waits when merging falls behind.

### 5.2 Where tick 18's bytes are now (run45, main f02030d)

`IVM_TICK_LIVE=1 IVM_TICK_CENSUS=18`, W=12, `gcServer=1`, `gcConcurrent=0`, stream release on
(the default since §3e). Views were identical, and all three batches passed.

- **Before stream release (§3a):** 14.1 GB live at the end of tick 18.
- **Now:** 8.47 GB live, 11.4 GB heap before the GC, 16.2 GB RSS.
- **The object census** accounts for 7.85 GB of it:

| part | bytes | what it is | lever |
|---|---|---|---|
| run blocks | **3.76 GB** (join `_left` runs alone 2.0 GB) | frozen, reference-free runs, mostly built in this batch | **S1**: spill runs as they are built |
| window write-back caches | **~2.2 GB** (`WindowOffset._small` 1.20, `WindowAggregate._small` 0.50 + `_pending` 0.27, rank 0.10) | managed partitions waiting for the cold record. The cap is a partition **count** (`CacheCap` = 2^18), not bytes | **S2**: flush by bytes into the record, which then spills by S1 |
| unfrozen managed levels | **~1.45 GB** (join left 0.81, right 0.24, aggregate 0.20, distinct 0.15) | 3.5M entries, each trace below its 64K freeze threshold, held as objects at ~230 B/entry | **S3**: freeze at the tick boundary under pressure (Feldera's High: "inserts spill") |
| aggregate `_stateCache` and the rest | ~0.5 GB | | none yet |

**This is the end-of-tick floor, not the in-tick peak.** The 9 GiB OOM is inside tick 18's step,
where the tick's deltas are also live until their last reader (§3b). Spilling cuts the state
under that peak, which the deltas then sit on top of. S1 alone reaches 3.76 of the 8.47 GB. S1–S3
together leave about 1 GB plus the cache.

**Extended TPC-DS has no census yet.** Its split (join traces, integrated views over 304M rows, the
output row index) is unknown, and its 304M view rows may make views dominate there, unlike
TPC-DI. Step 0 below measures it.

### 5.3 Proposal

**Step 0: census on extended TPC-DS.** Take the same per-tick live/blocks/managed lines from the
server path (the instrument is in the test harness today), at W=1 and W=12, under a cap that
completes or near the OOM. This decides whether views or join traces lead there.

**S1: spill on build.** When a freeze or a merge produces a run of at least `SpillMin` bytes, the
`RunBuilder` streams each block to a run file as it flushes it. This is Feldera's Low
mode. The finished run is `LazyTraceBlock`s over the file (`SpillRun`'s rebind, but done during
the build, not after it), and the blocks just written enter the cache unreferenced, so they go
first.
- **Where.** With a snapshot directory, in its store's `runs/`, pinned (`RunPins`) from creation.
  The next checkpoint then names the file instead of rewriting it (`Persisted.Store ==
  ctx.FileSystem` already skips it), which is Feldera's model, and batch 1's checkpoint shrinks
  by what was spilled. Without one, in a scratch `BlockSpill` directory, deleted when the merges
  retire it.
- **Durability.** A spilled file is not state until a checkpoint names it, so the commit must
  fsync whatever it names (check what the run writer does today), and restore must ignore
  unnamed spill files. The sweep must not race a spill: the pin goes on before the file
  appears.
- **Merges** read their inputs through the cache without promoting them, as today. Their outputs
  of `SpillMin` or more are built on storage. Batch 1 rarely merges (one freeze per trace is
  typical), so write amplification should stay small. Count it.
- **Needs an evicting cache.** Spilling without eviction only adds writes, so S1 is gated on
  `BlockCache.Enabled`.

**S2: window write-back by bytes.** Flush `_small` and `_pending` into the cold record when their
estimated bytes pass a cap, not only at `CacheCap` partitions or a checkpoint. The record is a
`BlockedIndexedTrace`, so S1 then spills it. The risk is the window path's lazy-recompute
assumptions about which partitions are hot. Measure batch-1 drain time.

**S3: pressure.** A monitor in Feldera's shape: Low / Moderate / High / Critical against a limit.
In .NET the limit is `GCMemoryInfo.TotalAvailableMemoryBytes`, which honours
`GCHeapHardLimit` and the cgroup. The measure is either the heap size or RSS, to be decided.
- **Moderate:** `SpillMin` → 0.
- **High:** freeze every managed level at the tick boundary, and spill (S3 proper), and shrink the
  cache budget.
- **Critical:** a step's transients. **Not proposed**: our deltas are managed objects, not
  blocks, so spilling them means encoding them first.
- **Cadence.** Feldera samples every second, but tick 18 allocates ~19 GB in 4.8 s. Our check is
  better taken at each freeze and merge decision, where it is cheap (`GC.GetGCMemoryInfo` is
  updated per GC).

**Order:** step 0, then S1 behind a flag with the usual 5-run A/B (TPC-DI SF=3 W=12 at no limit,
10 GiB and 9 GiB, and Nexmark for the step cost), then S2 and S3. Success: 9 GiB, then 8 GiB,
completes TPC-DI batch 1, and extended TPC-DS SF1 completes in CI's container.

### 5.4 Open questions

1. **Trigger.** A fixed `SpillMin` always, as Feldera's Low mode does, with pressure added later
   (S3)? Or pressure-only, so nothing is written unless memory is short?
2. **Default.** Feldera spills by default whenever storage exists, and our policy is to adopt
   Feldera's assumptions by default. Our "storage" would be a snapshot directory, or a default
   scratch directory under temp. ivm-bench has no snapshot directory, so only the second would
   reach it. Should S1 be default-on with an opt-out once measured, or stay with
   `DBSPNET_BLOCK_CACHE_MIB`?
3. **Spill location with a snapshot directory.** Into its `runs/`, so checkpoints reuse the files,
   or always into scratch, keeping state and spill strictly apart, at the cost of rewriting them
   at the checkpoint?

**Decided (Curt, 2026-11-04):**
- A fixed 10 MiB threshold.
- Default-on, once measured, with an opt-out.
- Spill into `runs/` when there is a snapshot directory.
- Start with step 0.
- Every policy number must be a well-defined knob, for later tuning.

### 5.5 Knobs

One settings type (`SpillSettings`) holds the knobs, with an environment variable for each
(the hosts' path) and a programmatic override (tests, embedders), the same pattern as
`TraceBlocksMode`. A host reads it once at start and logs the effective values. Feldera's
equivalent is in brackets.

| knob | default | what it sets |
|---|---|---|
| `DBSPNET_SPILL` = `on`/`off` | on (S1 lands opt-in, flips once measured) | spill on build at all |
| `DBSPNET_SPILL_MIN_MIB` | 10 [`min_spill_size`] | the smallest freeze or merge output built on storage; 0 = all |
| `DBSPNET_BLOCK_SPILL_DIR` (exists, §3.9) | temp | scratch directory when there is no snapshot directory |
| `DBSPNET_BLOCK_CACHE_MIB` (exists) | off today; S1 needs a default [`cache_mib`, 256 MiB per thread] | the evicting cache that makes a spill free memory. Its default is measured in S1 |
| `DBSPNET_WINDOW_WRITEBACK_MIB` (S2) | to be measured | the byte cap on a window store's write-back partitions |
| `DBSPNET_MEMORY_LIMIT_MIB` (S3) | GC's `TotalAvailableMemoryBytes` [`max_rss`, capped by available memory] | the limit pressure is measured against |
| `DBSPNET_PRESSURE_LEVELS` (S3) | `85,90,95` [Feldera's constants] | Moderate, High and Critical, as % of the limit |
| `DBSPNET_PRESSURE` = `on`/`off` (S3) | on with S3 | pressure tiers at all. Off leaves the fixed threshold only |

### 5.6 Step 0: extended TPC-DS SF1 at W=1 (2026-11-04)

**Instrument.** `Scratch/XtServerCensus` hosts the server's endpoints in-process around an engine
the test owns, and runs ivm-bench's own `run_batch('dbspnet', …)` driver against it. The census
goes to a file, flushed per line, so an OOM loses nothing. Batch 1 never finishes a tick, so a
per-tick census sees nothing. A new diagnostic hook, `RootCircuit.AfterOperatorStep` (one null
check per operator), samples **inside** the step:
- `OPLIVE`: a forced compacting GC each time the heap grows another 4 GiB.
- A full object census each time live crosses another 8 GiB.
- `OPSLOW`: any operator over 2 s.

The run used `gcServer=1`, `gcConcurrent=0`, a 48 GiB `GCHeapHardLimit` and W=1, as CI does.

**Batch 1 OOMs inside tick 0**, at operator 172 of 1,868, after ~600 s with the instrument (~186 s
without). Peak RSS was 42–43 GB.

| after op | live | run blocks | window rows | what stepped |
|---|---|---|---|---|
| 0 (`ApplyOp`, 29 s) | 10.3 GB | 0 | 0 | the input, as the first operator's output |
| 29 (`PartitionedRankOp`, 25 s) | 17.9 GB | 0 | 14.8M | one rank over the biggest input |
| 143 | 27.8 GB | 2.7 GB | 15.0M | |
| 144 (`BlockedJoinOp`) | 30.4 GB | 5.3 GB | | |
| 172 (`IncrementalAggregateOp`) | 35.7 GB | 6.7 GB | | the OOM followed |

**The object census after op 143** (27.8 GB live) found 17.4 GB of operator state:

| holder | bytes | |
|---|---|---|
| **one `PartitionedRank` store's write-back** (`_small` 12.9 GB + `_pending` 1.5 GB) | **14.55 GB** | 14.8M rows as 307M objects: ~980 B and ~21 objects per row |
| integrated views (run blocks) | 2.83 GB | already reference-free |
| everything else | < 0.1 GB | |
| **not operator state: stream values** | **~10.4 GB** (live minus state) | the tick's deltas in flight. After op 0 alone, live was 10.3 GB with 4 MB of state |

**Three findings change the order of §5.3.**
1. **The window write-back cap only applies between ticks.** `OrderedPartitionStore.Freeze`
   drops the cache past `CacheCap` partitions on an *idle* tick, and a checkpoint does too. One
   step can therefore put a whole input into `_small` as managed objects. Here that is 14.8M rows
   and 14.5 GB, 52% of live. **S2 (flush by bytes, checked during the step) is the biggest lever
   on this workload**, and it moves ahead of S1. The rows flushed into the cold record become
   blocks, roughly 3–5× smaller (§5.2's managed-to-block ratio), and S1 then spills them.
2. **Stream release does nothing in the first tick.** The plan is built *after* tick 0, from the
   producers that tick observed (§3b), and here tick 0 is the whole batch. TPC-DI's big tick is
   18, so §3b's measurement never saw this. A forward edge's producer always writes before its
   readers run, so tick 0 can release a stream at its last reader once its producer has been
   seen in this tick. Readers come from the reflection scan, which can run before tick 0.
   **New lever R0**, about 10 GB in flight here, cheap, and no spilling.
3. **Run blocks are the smallest of the three here.** S1 matters for the join traces that the
   OOM was in. It does not get this workload under a cap alone.

**Revised order:** R0 (release in tick 0), then S2 (window write-back by bytes, during the step),
then S1 (spill on build), then S3 (pressure). Re-run this census after each one. W=12 has not
been measured. Its state is the same in total, but it is split 12 ways, and in-flight deltas are
per worker.

### 5.7 R0: stream release in the first tick (2026-11-04, c778499)

**What changed.**
- **Readers are scanned before tick 0**, not after it, as before.
- **Each stream's release point is decided at its first write.** That is when its producer
  becomes known. `StreamRelease.Readers.ReleasePoint` is the rule `Plan` already used, now
  shared by both. A forward edge's producer writes before any of its readers runs, so the point
  is always still ahead.
- **The plan for later ticks reuses the first tick's scan**, so it is still one scan per circuit.
- `DBSPNET_STREAM_RELEASE_FIRST_TICK=0` opts out.

**A bug it exposed.** Every operator's `LastOutputRows` read `_output.Current?.Count` after the
step. Release had already nulled that, so introspection reported 0 from the second tick on. Three
single-step tests now failed under R0. `Stream` now records the released value's `Count`
(`Stream.LastCount`, a compiled per-type accessor), and the 25 operators read that.

**Tests.** First-tick release, plus feedback in every tick. The feedback test needs a reader on
*each* side of the producer: a reader only before it is safe to release after, which is why the
first version of the test missed a mutant. A third test checks the count after release. Five
mutants (skip the first-tick releases; drop the feedback rule; drop the opaque extension; drop
the count; an empty first-tick scan) each fail a test. Suite: 3,449 pass.

**Extended TPC-DS SF1, W=1, 48 GiB** (xt47 against xt46):

| after op | before | R0 |
|---|---|---|
| 144 | 30.4 GB live | **23.3 GB** |
| 172/173 | 35.7 GB | **26.1 GB** (state 21.6 GB, so ~4.5 GB in flight, down from ~10.4) |
| OOM at | op 172 | **op 389** of 1,868 (live 34.2 GB: run blocks 14.9 GB, rank write-back ~14.5 GB) |

It still OOMs, and what remains is S2's target (the rank write-back) and S1's (blocks).

**TPC-DI SF=3 W=12.** Three sessions (run48–50, 14 runs per arm pooled); views identical in
every run.

| | main | R0 |
|---|---|---|
| batch 1 drain, pooled median | 15.38 s | 15.72 s (+2.2%) |
| session medians, main / R0 | 15.10 / 15.48, 15.43 / 16.16, 15.54 / 15.26 | |
| tick 0 step (run50) | ~365 ms | ~465 ms |
| batch 1 checkpoint | 1.35 s | 1.13 s (run48) |
| peak RSS | level | |

- **Drain:** the session-to-session spread (±0.7 s) is larger than the difference, and run50 had
  R0 faster. R0 with the opt-out set measured 15.13 s.
- **Tick 0 is the one consistent cost: +~100 ms.** The scan itself is unchanged (~205 ms in every
  arm). It used to overlap the wait for the slowest worker at the end of tick 0, and now runs
  before the tick. A one-off per process. TPC-DI's tick 0 is small (267 MB live), so R0 has
  nothing to release there.

### 5.8 S2: window write-back by bytes (2026-11-04, 9c51c44)

**What changed.**
- **Each cold-backed `OrderedPartitionStore` estimates its write-back cache's managed bytes:**
  `_small`'s cached partitions plus `_pending`'s queue.
  - Per row: the mean of `ManagedSize.Row` over a 32-row sample of each change, plus node and
    queue overhead.
  - Per partition: dictionary and collection overhead.
  - Values: `Utf8String` reports its bytes through a new `IManagedSize`. Strings and byte arrays
    are estimated by length, and any other boxed value as 24 bytes.
- **At `EndTick`**, after the operator's last read of the step, a store drops its cache into its
  blocked record (`DropCache`: flush the queue, clear the cache, freeze the record). It does so
  when its own estimate, or the total over all live stores, is over
  `DBSPNET_WINDOW_WRITEBACK_MIB` (default 1024; `off` = unbounded, as before). Partitions touched
  later reload from the record.
- **The total sums a weak registry of live stores.** Stores are never disposed, so a running
  counter would keep a dropped program's bytes, and every later program in the process would run
  over budget.
- **`SpillSettings`** holds the knob, the first of §5.5's.

**Tests.** `WindowWriteBackBudgetTests` covers LAG, RANK and a framed SUM OVER, with small and
large partitions:
- A zero budget gives every tick's outputs equal to an unbounded twin's, with an idle tick in
  between. Every store ends each step with nothing cached, while the twin keeps its caches.
- A budget above the caches keeps them.
- Four mutants each fail a test: never drop; drop without flushing; no estimate; ignore the
  budget.
- The whole suite also passes with `DBSPNET_WINDOW_WRITEBACK_MIB=0`, where every window store
  drops after every step (3,452 pass).

**Extended TPC-DS SF1, W=1, 48 GiB** (xt48 against xt47):
- **The rank's cache drops at the end of op 29.** Rank state goes from 14.5 GB of objects to
  3.9 GB of blocks.
- **Tick 0 completes for the first time:** 180 s step, 40.7 GB live at its end, 25.0 GB of it
  blocks. Ticks 1–7 complete too.
- **The OOM is now in tick 8**, in `PartitionedTopKCompactOp`'s own dictionary (another managed
  state, a new holder).
- **State at the end of tick 0** is 27.1 GB, almost all reference-free blocks:

| holder | bytes |
|---|---|
| inner joins | 10.0 GB |
| left joins | 7.4 GB |
| rank (blocks now) | 4.9 GB |
| views | 3.3 GB |
| aggregates | 1.5 GB |

  Blocks reach 32 GB by tick 7. **S1's target now dominates.**

**TPC-DI SF=3 W=12.** run52, against R0, 5 runs per arm; views identical in every run.

| | R0 | S2 |
|---|---|---|
| batch 1 drain | 15.51 s (15.30–16.16) | 15.87 s (14.84–16.17), ranges overlap fully |
| batch 1 GC pause | 3.86 s | 3.72 s |
| batch 1 checkpoint | 1.26 s | 1.16 s |
| batch 2 / 3 drain | 108 / 123 ms | 97 / 139 ms |
| **peak RSS** | **14.69 GB** | **12.77 GB (−13%)** |

The tick-18 census (run51, budget off against on):

| | off | on |
|---|---|---|
| live after tick 18 | 8.47 GB | **7.47 GB** |
| RSS after tick 18 | 15.5 GB | **12.9 GB** |
| write-back actually live | 2,061 MiB, 2.35M rows | 415 MiB, 308K rows |
| drops in batch 1 | | 166 |

**Under a heap hard limit** (run53, release + R0 + S2):
- **9 GiB passes 1 run of 3** (drain 16.4 s, views identical). It failed 3 of 3 in §3i.
- 8 GiB fails 3 of 3. All failures are in the drain: the blocks and the tick's deltas, now S1's
  part.

**The estimate is rough, in both directions.**
- **On TPC-DI it is 2.4× high:** 1,005 MiB estimated against 415 MiB measured. Rows of partitions
  that leave the cache stay counted until the next drop: promotions to chunks, emptied
  partitions, retractions. That only makes drops earlier, which is safe.
- **On TPC-DS it is ~0.64× low:** 630 B/row estimated against ~980 B/row from the census.
- So the knob holds to within a factor of about 2.5. Making the estimate decrease on removal is
  a possible refinement, if the budget's meaning needs to be tighter.

### 5.9 S1a: spill on build, to scratch (2026-11-04, 9f91ebd)

**What changed.**
- **Every run change hands the trace to a spiller.** `BlockedIndexedTrace.Compact` (every freeze
  and merge ends there) calls `BlockAccess.Spiller`.
- **`RunSpiller`** writes each run with no file and at least `DBSPNET_SPILL_MIN_MIB` (default 10,
  Feldera's `min_spill_size`) of in-memory blocks to a scratch run file, then rebinds it there
  (`SpillRun`). The scratch directory is `DBSPNET_BLOCK_SPILL_DIR`, default temp, under `runs/`.
  The blocks become `LazyTraceBlock`s, admitted to the cache unreferenced, so they leave first.
- **Files are deleted once their runs are collected**: merged away, or replaced by a checkpoint's
  rebind. They are tracked by weak reference and swept every 64 spills.
- **Opt-in** (`DBSPNET_SPILL=on`), and only when the block cache can evict. Without eviction a
  spill only adds a write.

**Not S1b yet.** A checkpoint still writes its own copy of a spilled run, so it faults each block
back from the scratch file. Naming the spilled file instead needs two things. One is the
checkpoint's exact store instance: the reuse test compares by reference, and some paths build a
new `LocalTableFileSystem` per call. The other is the run layout: Z-set traces (views, DISTINCT)
checkpoint as `Kind.ZSet`, and indexed ones as `Kind.Indexed`.

**Tests.**
- The suite passes with every run spilled and a 1 MiB cache (3,452).
- Three assertions about in-memory block shape are skipped under a spiller: a giant spanning
  blocks in memory, an index-spill file count, a residency bound. Their output and restore checks
  still run.
- `RunSpillerTests`, on a join with a giant group: outputs equal a twin's every tick; runs are
  file-backed; a checkpoint restores exactly and goes on matching; files go with their runs and
  none remain after the program; runs below the threshold stay in memory.
- Four mutants each fail a test: no hook; never delete; ignore the threshold; delete live files.

**Extended TPC-DS SF1, W=1, 48 GiB, 1 GiB cache** (xt49):
- **The run gets much further**: all of ticks 0–17 complete. Before, it died in tick 8.
- **Block residency stays at the cache, ~1 GB**, against 25–32 GB before. 123 GB was spilled in
  448 runs.
- **The OOM is now in tick 18, inside a join** (`DeltaOnLeft` decoding cells for its output delta):
  the in-step peak.

**TPC-DI SF=3 W=12** (run54, 4 runs per arm, views identical in every run; "current" is S1a plus
§5.10's input release):

| | S2 | current, spill off | current, spill on, 512 MiB cache |
|---|---|---|---|
| b1 drain | 15.50 s | 15.40 s | 16.18 s (+4.4%) |
| b1 output | 1.56 s | 1.48 s | 1.72 s |
| b1 checkpoint | 1.12 s | 1.26 s | 1.05 s |
| b2 drain | 103 ms | 100 ms | 138 ms (cache misses) |
| peak RSS | 13.01 GB | 14.05 GB (overlaps; bimodal) | **11.87 GB** |

**Under a heap hard limit, spill on** (run55):
- **9 GiB: the drain completes 3 of 3**; S2 alone completed 1 of 3. But batch 1's checkpoint then
  fails 3 of 3, in `LazyTraceBlock.Load` → `BlockSchema.Adopt` under `WriteRunAsync`. That is
  S1a's copy, faulting spilled runs back in, which S1b removes. ivm-bench takes no checkpoints, so
  its batch 1 at 9 GiB is now the drain alone.
- **8 GiB: 2 failures are the same checkpoint reads, and 1 is in the drain.**

### 5.10 Inputs released after their last reader (2026-11-04, be88d2d)

**What was holding memory.**
- **The census showed something tick-scoped.** After S1a, extended TPC-DS ended tick 0 with
  16.7 GB live but only 1 GB of blocks. The heap split showed 13.1 GB of small objects (gen2), and
  0 evicted bodies alive. One 38 ms tick later, live was 2.4 GB.
- **The view-delta pin was ruled out by experiment** (xt50: releasing all 96 views' pinned
  `IntegrateOp` inputs freed nothing).
- **`dotnet-gcdump` could not show it.** On .NET 10 it falls back to lossy buffering and stopped at
  10M objects.
- **The culprit: input-handle streams were never released** (written outside the step), so a tick's
  whole input lived until the next commit. In-step sampling had shown live at 10.3 GB right after
  op 0, with no state.

**The fix.**
- **An `InputHandle` commits its stream before every step**, with its zero when nothing was pushed.
  Such a stream is marked `CommittedEveryTick` and released after its last reader, in the first
  tick too (`FirstTick.QueueInputs`).
- **Other streams written outside the step are kept.** Example: a value set once and read every
  tick.

**Tests.** An input is released, and a quiet tick reads its zero, not null. A stream written outside
the step only once is kept. Four mutants each fail a test; the last of them (releasing any
outside-written stream) needed that second test.

**Extended TPC-DS** (xt53 against xt49): live at the end of tick 0 went 16.7 → 11.2 GB, and at ticks
6–7 9.4 → 5.4 GB. It still fails in tick 18, inside the join.

### 5.11 What extended TPC-DS holds now: TOP-K (2026-11-04)

From tick 8 on, ticks end with 10–17 GB in gen2 plus 4–5 GB of large arrays, and it persists
across tiny ticks. The census at the end of tick 12 (xt54: 15.6 GB live, 0.75 GB of blocks) found:

| holder | bytes | |
|---|---|---|
| `PartitionedTopK._parts` | **9.82 GB** | 201M objects. TOP-K state is entirely managed: it was never moved to blocks |
| `PartitionedTopK._touched` | 0.92 GB | per-tick scratch that survives the tick |
| `PartitionedRank` (store 1.76, `_accum` 0.60, `_window` 0.33) | 2.69 GB | |
| window aggregates | 0.41 GB | |
| every join, aggregate and view trace | < 1.1 GB | spilled |

**Next, for extended TPC-DS:**
1. TOP-K state as blocks. The same shape as §3.15's window store: ordered per-partition state with a
   cold record, and S2's byte budget.
2. Clear `_touched` at the end of the tick.
3. Then the in-step peak of tick 18's join.

**Next, for TPC-DI under a limit:** S1b, so the checkpoint names spilled files instead of reading
them back.

### 5.12 Partitioned TOP-K state as blocks (2026-11-04, 5155382)

**What changed.**
- **State.** `PartitionedTopKCompactOp` kept every partition's whole integrated multiset as managed
  objects. With block formats it now keeps an `OrderedPartitionStore` instead:
  - partitions ordered by the total order and read whole (`valueOf: null`, so no chunks);
  - a blocked cold record, spilled by S1;
  - the write-back cache bounded by S2's budget.
- **When.** On the rank operator's terms: a window kind, the flat family, a PARTITION BY.
- **Step.** Each touched partition's window is read from the store before the delta and after it,
  then diffed as before.
- **Checkpoints.** In the runs layout the store writes its own files; otherwise the whole state goes
  to `trace.arrows`. Re-shard reads the store through `IStoreSnapshot`.
- **Scratch.** The per-tick touched set and window buffers are replaced after a bulk tick instead
  of cleared, since `Clear` keeps capacity. That was §5.11's 0.92 GB, in both modes.

**A test trap found on the way.** `PlanToCircuit.Compile` on a single query takes the typed fast path
whenever it can, so the existing compact TOP-K PBTs' "structural compact" arm was typed all along.
Multi-view programs, ivm-bench's path, compile structurally by default. The new PBT pins the
structural compile (`ShareArrangements = true`) and asserts the operator is blocked.

**Tests.**
- **Blocked against legacy, every tick, 250 cases.** Wide domain: NULL and VARCHAR partition
  columns, growth past `SmallMax`, ROW_NUMBER, RANK and DENSE_RANK. Tiny freezes and blocks, and a
  zero write-back budget.
- **Checkpoint and restore on the program path in both layouts**, then continued ticks, equal to a
  twin.
- **Four mutants each fail a test:** no old window; ignore the store files on load; an empty whole
  write; no rank cut.
- **The suite passes** (3,459), and again with everything spilled and a zero budget.

**Extended TPC-DS SF1, W=1, 48 GiB, spill on, 1 GiB cache** (xt55 against xt53). Live at the end of
each tick:

| tick | before | blocked TOP-K |
|---|---|---|
| 8 | 17.8 GB | 13.8 GB |
| 12 | 15.6 GB | **7.3 GB** |
| 13 | 22.7 GB | 14.7 GB |
| 17 | 19.3 GB | **10.0 GB** |

It still OOMs in tick 18, now in `IncrementalLeftJoinOp` allocating a `HashSet` sized to its delta.
That is the in-step peak: tick 18's step needs more than ~38 GB above a ~10 GB baseline. Next: an
in-step census of tick 18.

**TPC-DI SF=3 W=12** (run56, 5 runs per arm, spill off, views identical):

| | before | blocked TOP-K |
|---|---|---|
| b1 drain | 15.47 s (15.12–15.79) | 15.67 s (15.01–16.00), ranges overlap fully |
| b2 / b3 drain | 106 / 126 ms | 102 / 118 ms |
| peak RSS | 13.5 GB | 14.2 GB (bimodal) |

### 5.13 Tick 18's in-step peak (2026-11-04)

**Instrument.** `XT_OP_TICK=18` samples inside that tick only: forced-GC live every 4 GiB of heap
growth, and `OPROWS` for every operator whose output delta has at least 1M rows. The output size comes
from introspection, or else from the operator's `_output` stream's `LastCount`.

The run (xt56: spill on, 1 GiB cache, blocked TOP-K, 48 GiB) ended in an OOM inside the instrument
itself. The full object census started walking the heap at ~37 GB live. The samples before that point
stand:

| ops | live | what flows |
|---|---|---|
| 0–1280 | 10–12 GB | ~2.0M-row deltas through chains of `IncrementalLeftJoinOp` → `Apply2Op` → `ApplyOp` (18 left joins); also a rank, an aggregate and a DISTINCT |
| 1281 (`IntegrateOp`) | 23 GB | |
| 1377 (`PartitionedTopKCompactOp`, blocked) | 25 GB | the step took **45.6 s**: blocked TOP-K is slow on a delta this size |
| 1380 (`BlockedJoinOp`) | **34 GB** | **21.7M-row output** from a ~2M-row input |
| 1381–1391 (`ApplyOp`, `IncrementalLeftJoinOp`, `Apply2Op`, `ApplyOp`) | 37 GB | each another 21.7M-row delta. Blocks are 1.4 GB of it |

**So the peak is not state.** One join fans a delta out about 10×, and the operators after it each
hold a delta that size at the same time, as managed rows (~400 B per row). Stream release frees each
after its last reader, but a chain of one-reader operators still has an input and an output alive
together, plus whatever readers further down still need. This is Feldera's Critical case (§5.1: a
step's transient batches).

**Options.**
1. **Bounded ticks.** The runner splits a big input version into ticks of at most N rows. An
   incremental circuit accepts its input in any number of ticks, and views are read at the end of a
   batch, so outputs are unchanged. Per-tick deltas then shrink in proportion, here about 2M → 2.2M
   per join output for ten chunks.
   - Cost: more ticks (per-tick overhead and more merges).
   - Care: the tick journal (§2 of the durable-state design) records a tick per input version and
     offset, and a split version must journal and replay correctly.
   - Feldera feeds steps in input chunks too.
2. **Spill in-step deltas.** Encode a delta past a threshold as blocks (Feldera's `min_step_spill_size`).
   This needs a block-backed delta representation that every consumer can read: a large change.
3. **Make the deltas smaller.** Narrower rows from projection pushdown, or the join's output kept as
   blocks. These help, but don't bound anything.

Option 1 is the only one that bounds the peak by a knob, and it is the cheapest. A knob
`DBSPNET_MAX_TICK_ROWS` (unset: today's one tick per version) would join §5.5.
Separately, the 45.6 s blocked TOP-K step on a ~2M-row delta needs a look. The managed operator's
time on the same delta is unmeasured.

### 5.14 Spilling in-step deltas: the pieces (2026-11-04, estimate)

Curt prefers §5.13's option 2, spilling in-step deltas, as the long-term fix. This section breaks it
into increments and sizes each one.

**What exists.**
- **Deltas are `ZSet<StructuralRow, Z64>`** (816 lines; three backings behind `object _store`:
  dictionary, `ChunkedMap`, inline) **or `IndexedZSet`** (578 lines; a dictionary of key → group
  `ZSet`). Join and aggregate inputs are `IndexedZSet`: 16 operator files take one, 8 of them spine
  variants.
- **Consumers mostly enumerate.** 15 operators load `_input.Current` into a local and loop over it.
  `WeightOf`/`Contains` are used almost only on state.
- **Producers consolidate in a hash map** (`ZSetBuilder`): 52 construction sites in 32 Core operator
  files, and 28 in the SQL layer's compiled row functions (`MapFilterRows` and friends).
- **What a step allocates is O(|delta|), beyond the delta itself.** Tick 18's OOM site was
  `IncrementalLeftJoinOp`'s `HashSet` of touched keys, sized to its inputs' group counts. Rank, TOP-K
  and window operators keep touched maps and old windows the same way.
- **Most of the machinery is already built.** `BlockedIndexedTrace` turns managed levels into
  consolidated, hash-ordered block runs and merges them; S1 spills them; the cache evicts them. A
  delta that outgrows memory needs exactly that, just for one step.

**Increments.** Production code only; tests add roughly as much again. Each increment is about one
session of work plus a measured A/B, and each lands behind a threshold knob.

| # | piece | where | size | alone it buys |
|---|---|---|---|---|
| D1 | **Block-backed `ZSet`:** a fourth backing, hash-ordered runs of `ITraceBlock`s (lazy or spilled). Enumeration decodes block by block; `Count`/`IsEmpty` are O(1); `WeightOf`/`Contains` probe by hash; set algebra materializes, as a fallback | `ZSet.cs`, the enumerator struct | ~350 lines | nothing yet: nobody builds one |
| D2 | **Spilling builder:** `ZSetBuilder` consolidates in memory up to a threshold, then freezes a sorted run to blocks (reusing `BlockedIndexedTrace`'s freeze and `RunBuilder`). `Build` returns a D1 set over the runs, merged lazily, and spills them through S1 above `min_step_spill` | `ZSetBuilder.cs`, the blocks code | ~300 lines | a producer's peak is bounded by the threshold, not by its output |
| D3 | **Format plumbing:** the compiler passes each structural stream's block format to the operators that can emit big deltas: compiled row functions (Apply), joins, union/difference (Apply2), aggregate, rank/TOP-K/window outputs, exchange gather. Typed path excluded | `PlanToCircuit`, ~10–12 operators, of the 32 + 28 builder sites | ~300 lines, mechanical | **D1–D3 bound every `ZSet` edge.** In tick 18 the 21.7M-row join output and the Apply chain after it become blocks (~4× smaller) and spillable |
| D4 | **Block-backed `IndexedZSet` deltas:** entries in key-hash order (the trace run layout), groups found by hash probe (as `BlockedGroupView`), produced by index and exchange-index operators | `IndexedZSet.cs`, index ops, ~8 flat consumers | ~500 lines | join and aggregate inputs bounded too |
| D5 | **Consumers in key-hash slices:** an operator whose own per-step structures are O(\|delta\|) processes a sorted block delta one hash range at a time, feeding D2's builder, so its working set is one slice. It is bounded ticks *inside* the operator: joins are bilinear, and aggregate, rank, TOP-K, window and DISTINCT update per key | left, full and inner joins; aggregate; DISTINCT; rank/TOP-K/window | ~6 operators × 80–150 ≈ 700 lines | **the step's peak is bounded by knobs.** Tick 18's left-join `HashSet` is a slice's size |
| D6 | **Cross-cutting:** exchange (W>1) splitting a block delta by key hash and gathering; view handles' `LastDelta`; `LastCount` and introspection; stream release over block bodies | exchange ops, handles | ~250 lines | the parallel path |
| D7 | **Verification:** a forced mode (threshold 0: every delta blocked and spilled) run over the whole suite, as for S1. PBTs against the managed path. Nexmark and TPC-DI A/B for the hot enumerator: below the threshold nothing changes, but the enumerator's switch grows | tests, bench | ~600 lines | the default-on decision |

**Total:** about 2,400 lines of production code plus about 1,500 of tests, in 6–7 measured increments.
For scale: S1a was ~250 lines, blocked TOP-K ~250, and the flat-trace-state arc several times this.

**Order and risk.**
- **Order:** D1 → D2 → D3 ship together as the first measurable step, since they bound `ZSet` edges.
  D4 and D5 are the larger half, and D5 is what bounds a step by knobs. D6 then follows the parallel
  path.
- **Biggest risk: D1's enumerator.** It is the hottest loop in the engine. The de-boxed enumerators
  (b5f90aa) were worth −13.5% on many-ticks workloads, so a fourth arm must cost nothing below the
  threshold. Nexmark is the gate.
- **Second risk: D5's per-operator slicing.** Each operator's retraction and old-window logic has to
  be right across slice boundaries. The twin PBTs cover it.

**How Feldera does it.** Its batches are always sorted and key-indexed; there is no separate Z-set and
indexed Z-set. Builders fall back from `Vec` to `File` partway through, and consumers walk cursors.
D1–D5 adopt that shape only for deltas past the threshold, and leave the small-delta path as it is.

**Bounded ticks (§5.13 option 1) still has a role.** At ~150 lines plus journal care, it is a cheap
interim. D5 makes it redundant for memory, though it may still matter for latency.

### 5.15 D1–D3 built: block-backed deltas behind a threshold (2026-11-04)

**What changed** (all behind `DBSPNET_STEP_SPILL_ROWS`, off by default; structural path only).
- **D1, a fourth `ZSet` backing.** `ZSetExternalStore` is the abstraction; `BlockedRows` is the one
  implementation: a consolidated, hash-ordered run of blocks. `Count` is exact (one run), `WeightOf`
  probes by hash, enumeration decodes a block at a time with strings copied, so a row a consumer keeps
  does not pin a block. Set algebra on a block-backed set builds through a builder of the same kind;
  a sum with one merges the runs instead of re-hashing; in-place mutation (state, never a builder's
  output in practice) moves it into a managed map first.
  - **The enumerator's hot arms are unchanged.** The external store rides the inline arm: its one
    `object` field holds either the inline set or the external store's enumerator, told apart by a
    bool. The dictionary and chunked arms test exactly what they did.
- **D2, a spilling builder.** `ZSetBuilder` takes a `SpillingRows` store in its one field. It
  consolidates in a managed `ZSetBuilder` up to the threshold, then freezes that chunk into a run of a
  private Z-set `BlockedIndexedTrace` (its ordinary freeze and geometric merge, so S1's spiller writes
  runs past `DBSPNET_SPILL_MIN_MIB` to files and the cache can evict them). `Build` merges every run into
  one (`MergeAllRuns`), so the set's count is exact. Below the threshold it builds the managed set it
  always did. A capacity hint is capped at the threshold, or pre-sizing would allocate the map this
  bounds.
- **D3, formats on streams.** `Stream.DeltaFormat` carries the stream's Z-set block format. The compiler
  sets it in `CompilePlan` (every structural plan node's stream, from `plan.Schema`) and on the outer
  joins' internal join streams, only while the knob is on. Operators build their output through
  `DeltaSpill.Rows/For(_output.DeltaFormat, …)`: map / filter / map-filter / flat-map, inner, shared-right,
  left and full joins (managed and blocked), DISTINCT, rank, TOP-K (both), window aggregate, offset.
  Union and difference need nothing: `+` and `-` follow a block-backed operand.
- **The knob.** `DBSPNET_STEP_SPILL_ROWS=N` freezes a builder's map every N rows; `0` is the forced mode
  (every delta with a format built as blocks, chunks of `DBSPNET_STEP_SPILL_FORCED_CHUNK`, default 4096,
  a test knob). `SpillSettings.UseStepSpill` overrides it in an async context.

**Tests.**
- The suite passes knob off (3,459), forced (546,857 deltas built as blocks, 3.6M rows; a stats dump,
  `DBSPNET_STEP_SPILL_STATS`, proves it is not vacuous), forced at chunk 2, and forced at chunk 2 with S1
  spilling every run into a 1 MiB cache (3,465 each).
- **That last run found a real bug.** `BlockedRows.Plus` adopts the source runs' blocks into a new trace,
  as new `Run` objects. A spilled file lives as long as its *original* run (the spiller tracks it
  weakly), and after the adopt loop neither source was reachable (the JIT ends their lifetime, and stream
  release had dropped the inputs). A sweep then deleted a file mid-merge. Fixed with `GC.KeepAlive` on
  both sources. No unit test reaches it: it needs a GC inside the merge. The forced+S1 suite run is its
  gate.
- `BlockedDeltaTests`: a property test that a block-backed set reads and derives as the managed one
  (400 cases: chunk 1–9, forced and thresholded, every operation above both ways); count and
  cancellation across chunks; knob off is the plain builder; a 600-row fan-out join emits blocks past a
  100-row threshold and a managed delta below it (blocked and managed traces); 600 random-query twins,
  forced at chunk 1–3, equal every tick.
- Four mutants each fail a test: no spilling builder; the enumerator skipping an entry; no final merge;
  the compiler attaching no format.

**Nexmark (`w1profile`, B/ev).** Byte-identical on all ten queries, main against the branch with the
knob off and with it on at 1M rows.

**TPC-DI SF=3 W=12** (di1/di2; gcServer, gcConcurrent=0; views identical in every run, bc345419):

| arm | runs | b1 drain | b1 GC pause | b2 / b3 drain | peak RSS | deltas blocked (rows) |
|---|---|---|---|---|---|---|
| main | 5 | 15.54 s | 3.38 s | 103 / 122 ms | 13.57 GB | — |
| branch, knob off | 5 | 15.48 s | 3.55 s | 90 / 121 ms | 13.30 GB | 0 |
| S1 (spill on, 512 MiB cache) | 5+3 | 16.38 s | 3.9–4.0 s | 132–135 ms | 11.8–12.0 GB | 0 |
| S1 + 1M rows | 5 | 16.75 s | 4.00 s | 147 / 130 ms | 11.44 GB | **0** |
| S1 + 64K rows | 3 | **35.23 s** | 8.38 s | 112 / 132 ms | **14.02 GB** | 606 (61.6M) |
| S1 + forced | 3 | 55.24 s | 11.70 s | 116 / 136 ms | 15.32 GB | 3,974 (82.8M) |

- **Knob off is level with main.**
- **At 1M rows nothing engages at W=12:** no worker's delta reaches 1M rows (the 1M arm is the S1 arm).
- **Where it engages, D1–D3 alone cost time AND memory on TPC-DI:** 64K doubles batch 1's drain and
  raises peak RSS 18%. Not measured yet, so candidates only: every consumer decodes the blocks into
  fresh rows (a chain of N readers decodes N times, and that garbage is what server GC's peak tracks);
  `MergeAllRuns` rewrites every multi-chunk delta once more; and the index operators that consume these
  deltas (D4, not built) still build managed `IndexedZSet`s of the same rows, so the managed copy is
  made anyway, one hop later.

**Extended TPC-DS SF1, W=1, 48 GiB heap cap, S1 on, 1 GiB cache, 1M-row threshold** (xt57; xt56 is the
same run before D1–D3):

| | xt56 (before) | xt57 (D1–D3) |
|---|---|---|
| ticks 0–17 | complete, ~2,235 s | complete, 3,308 s |
| deltas built as blocks by tick 17 | — | 437 (1.25 billion rows) |
| live at tick 18, op 1377 (blocked TOP-K) | 25 GB | **11.2 GB** |
| live after the 21.7M-row fan-out (ops 1380–1391) | 34–37 GB, then the census walk OOMs | not sampled (no 4 GiB step crossed live) |
| furthest op | 1391 | past 1404 |
| peak RSS | 40 GB | 37 GB |
| how it ended | OOM | `IncrementalLeftJoin: oversized per-key cross product 7,222,266 × 1` |

- **Tick 18's fan-out chain now fits.** The 21.7M-row deltas after `BlockedJoinOp` are block runs, and live
  memory before the chain is less than half what it was.
- **It ends on a guard, not memory.** `IncrementalLeftJoinOp.CrossProductGuardRows` (5M) refuses any one
  key's cross product past 5M rows, because materializing it used to OOM. With a spilling output builder
  that reason no longer holds. Whether to lift the guard when the output spills (Feldera has none) is
  open, and so is whether this tick finishes once it is lifted.
- **The chain is much slower:** the left join at op 1397 took 141.8 s (xt56's equivalent took 26.8 s) and
  the Apply ops after it 70–95 s (xt56: 6.6–7.8 s). Ticks 0–17 took 48% longer. The cost is the same as
  TPC-DI's 64K arm (decode per consumer, final merge, managed index copies) and is the next thing to
  apportion.
- **Disk:** S1 wrote 626 GB of spill files by tick 17, with ~92 GB live at once. Spill directories of
  killed processes are not cleaned up: 364 GB of them had piled up in temp from earlier runs.

**Next.** (1) Apportion the in-step cost (decode, `MergeAllRuns`, index copies); the obvious levers are a
k-way-merge enumeration without the final rewrite, and D4 so index ops stop rebuilding managed copies.
(2) Decide on the cross-product guard under spilling. (3) Sweep stale `dbspnet-spill-<pid>` directories
at start.

### 5.16 Where in-step spilling's cost goes, and the cross-product guard (2026-11-04)

**Instrument.** `DBSPNET_STEP_SPILL_PROFILE=1` (`StepSpillProfile`) charges each phase its wall time and the
bytes its thread allocated, exclusive of nested phases; `IvmMultiBatch` prints a `STEPPROF` line per batch.
Times below are summed over the 12 worker threads.

**TPC-DI SF=3 W=12, batch 1, 64K rows** (di3; views identical, bc345419). Knob off allocates 54.9 GiB in the
drain; 64K allocates 174.8 GiB without S1 and 188.9 with it:

| phase | thread-time | allocated | share of the extra 120 GiB |
|---|---|---|---|
| **run merges** (the builder trace's geometric merges + the final merge into one run; less 2.2 GiB of ordinary state merges) | 48 s | **76.6 GiB** | **64%** |
| enumeration (decoding rows for readers) | 26.7 s | 35.3 GiB | 29% |
| chunk freeze (sort + encode) | 25.7 s | 8.4 GiB | 7% |
| chunk adds (the managed map) | 10.2 s | 2.4 GiB | 2% |
| S1 spill writes | 11.4 s | 2.1 GiB | — |

- **Merging is most of it.** `MergeCore` decodes every entry to a `StructuralRow` (key and value) and encodes
  it again. A builder's chunks are equal-sized, so the trace's quarter rule merges them like a binary
  counter: about log2(chunks) passes per row, then one more to make a single run.
- **Re-reading is not the cost:** 68.1M rows decoded for 61.6M built (1.1×). Decoding costs ~540 B/row
  (fresh rows, cells, copied strings).
- **Disk is not the cost:** without S1 the drain is 34.4 s, against 35.2 s with it.

**Lever, in order.**
1. **No merges.** Keep a built delta as its k chunk runs: no geometric compaction in a builder's trace
   and no final merge. Enumeration k-way merges by hash and sums equal rows only on a hash shared by two
   runs, compared with `KeyEquals` (no decode). The count is one pass over the hashes alone. A probe
   checks every run. That removes ~64% of the extra allocation and ~48 s of thread time.
2. **Cheaper decode:** column-wise `KeysAliased` instead of a row-wise copy. It is cheaper, but a row a
   consumer keeps then pins its 60 KB block. Measure the retention first.
3. D4: the index operators downstream.

**The cross-product guard.** `IncrementalLeftJoinOp` threw when one key's left × right exceeded 5M rows
(7739eb8, July: a diagnostic for a TPC-DI OOM, with no test). TPC-DS tick 18 tripped it at **7,222,266 × 1**:
a dimension lookup, whose output is no larger than its input (already in memory), not the coarse-key
cross product the guard was written for. Feldera has no such guard. **Decided:** it trips only on a
*blowup*, a product past the limit and more than 4× the two groups together, and never when the output
builder spills (its memory is bounded by the threshold). The limit is now an internal test seam. A test
covers each case (a 300 × 1 lookup passes, 20 × 20 throws, 20 × 20 into a spilling builder emits 400 block
rows), and dropping either new condition fails it.

### 5.17 Merge-free deltas (2026-11-04, a2e2926)

**What changed.** A spilling builder's trace no longer compacts, and `Build` no longer merges. A built
delta keeps its chunk runs, each with its sorted key hashes beside it (4 B a row, taken before the freeze,
since a spill inside the freeze makes the blocks file-backed).
- **Count:** exact, from one k-way walk over the hash arrays. Blocks are read only for a hash two runs
  share.
- **Readers:** the same walk, decoding entries as they go; a shared hash's entries are decoded and summed.
- **A sum:** concatenates the two sets' parts.
- **Ownership:** runs are read through their owning traces (`BlockedRows.Part`), never adopted into a new
  trace. A spilled file lives as long as its original run object, and holding the owner keeps that run
  reachable and follows a spill's rebind. This removes §5.15's `KeepAlive` hazard by construction.
- `MergeAllRuns` is gone.

**Tests.** The suite passes knob off, and forced at chunk 2 with S1 spilling every run into a 1 MiB cache
(3,466 each). That forced run's exit-time stats did not get written; the SQL and end-to-end subsets in the
same mode built 365K and 104K blocked deltas. The property test now also requires multi-run sets (its
48-row domain makes shared hashes common). Mutants on the tie count and the tie's zero-weight filter
each fail it.

**TPC-DI SF=3 W=12, batch 1, 64K rows** (di4; views identical, bc345419):

| | merged (§5.16) | merge-free | S1 alone |
|---|---|---|---|
| drain | 35.2 s | **24.1 s** | 16.4 s |
| drain allocation | 188.9 GiB | **103.2 GiB** | 55.3 GiB |
| peak RSS | 14.0 GB | 13.4 GB | 11.8–12.0 GB |

The profile has the builder's merges gone (the 2.2 GiB left is ordinary state merging) and the count pass
at 0.07 GiB. **Decoding for readers is now most of what is left:** 39 GiB, ~600 B a row. Chunk sort and
encode is 8.6 GiB.

**Extended TPC-DS SF1, W=1, 48 GiB, 1M rows** (xt58):

| | xt56 (before D1) | xt57 (merged) | xt58 (merge-free) |
|---|---|---|---|
| ticks 0–17 | ~2,235 s | 3,308 s | **2,737 s** |
| allocation by tick 17 | — | 5,746 GiB | 3,589 GiB |
| live at op 1377 / after the fan-out (op 1388) | 25 / 34–37 GB | 11.2 / — GB | **11.6 / 11.4 GB** |
| left join op 1397 / Apply op 1400 | — | 141.8 / 95.4 s | 44.1 / 25.8 s |
| furthest op | 1391 (OOM) | ~1404 (guard) | **1433** |
| peak RSS | 40 GB | 37 GB | 37 GB |

- **The guard (§5.16) passed the 7.2M × 1 lookup**, and live memory stayed at ~11.5 GB through the fan-out
  chain.
- **It ends on a run-file limit:** `OverflowException` in `RunStreamWriter.AppendConcatAsync`, as S1 spilled
  the left join's left trace. Key `PREMIUM` is a giant group of 7.2M rows. A giant group goes to a run file
  as ONE block (§3d), and `RunBlock.Length` is an `int`; this group encodes past 2 GiB. The limit is old:
  a checkpoint of this state hits it too (same `WriteRunAsync`), and a loaded block is one `byte[]`, so a
  single block cannot pass ~2 GiB anyway.
- **Options.** (a) Let a hash span file blocks, as it already does in memory: the writer cuts a giant
  every ~1 GiB, and readers (block index, `Find`, Bloom filter, compaction by `__hash`, RunPins) accept
  adjacent blocks sharing a first/last hash. This is a format change, but it fixes checkpoints too.
  (b) Best effort: the spiller leaves a run with such a span in memory. That unblocks the measurement but
  not checkpoints.

### 5.18 A giant group spans file blocks (2026-11-04, fe18267)

**Why.** TPC-DS tick 18 ended (§5.17) when S1 spilled the left join's left trace: key `PREMIUM` is a giant
group of 7.2M rows, and a giant went to a run file as ONE block, with an `int` length and loaded into one
`byte[]`. In memory a giant's hash already continues from block to block; the file format now does too.

**What changed** (run file format version 4; readers open 3 and 4, and a v3 file never spans).
- **Writers.** A blocked trace's concatenated giant is cut every `RunFile.MaxSpanBlockBytes` (256 MiB), at
  part boundaries, so each file block holds only the giant's hash. Compaction cuts a hash run every
  `MaxSpanBlockEntries` (2^18). `RunStreamWriter` accepts a block that begins with the previous block's
  last hash; the run's group count counts that group once, and each block keeps its own count.
- **Readers.** `RunReader`'s point lookups (weight of a row, the entries of a hash, the group of a key)
  read the whole block range of a hash. `RunColdGroups` sums a key's parts across blocks, preloaded giant
  blocks included. The in-memory `Run.Find` already continued across blocks.
- **Compaction.** A delta entry of a spanning hash that matches nothing in the first base block is carried
  to the next ones and emitted where the span ends. Compacted runs therefore stay consolidated, a
  blocked reader's invariant, while only one base block is decoded at a time.
- **Not changed:** the managed run writer (`RunFile.Cut`) still writes a group as one block, so a managed
  trace's group past ~2 GiB would still overflow. Blocked traces are the default for that state.

**Tests.**
- The suite passes at the default caps, at 8 KB / 16-entry caps (`DBSPNET_RUN_SPAN_BLOCK_BYTES` /
  `_ENTRIES`, so every giant splits), and at those caps with forced in-step spilling and S1 (3,468 each).
  The tiny caps found a bug of mine: `Cuts` dropped the entries before a giant when the cap was below a
  block's entries.
- A new test checks that a giant past a 16 KB cap is several consecutive file blocks holding only its
  hash, that a point lookup returns the whole range, and that it restores exactly into the blocked and
  the managed join. Eight incremental checkpoints then delete giant rows and probe the whole giant; each
  restores exactly into both, and the blocked one's runs are checked consolidated.
- Three mutants each fail it: no split, no carry, a one-block lookup.

**Extended TPC-DS SF1, W=1, 48 GiB, 1M rows** (xt59):

| | xt58 | xt59 |
|---|---|---|
| ticks 0–17 | 2,737 s | 2,744 s |
| live at op 1406 / 1671 | — | 11.5 / 17.1 GB |
| furthest op in tick 18 | 1433 (run-file overflow) | **1794 of 1868** |
| peak RSS | 37 GB | 41 GB |
| how it ended | `OverflowException`, giant block | `OutOfMemoryException`, view freeze |

- **The giant spilled** and the run went 361 operators further, through all 18 left joins of the chain,
  each a 21.7M-row block-backed delta.
- **Where it ends:** the last operators sum three 21.7M-row streams into a **65M-row** delta (op 1794,
  block backed). A view's `IntegrateOp` pushes it as a managed level, and `BlockedIndexedTrace.FreezeRows`
  enumerates it into pooled arrays to sort and encode: all 65M rows decoded and live at once.
- **The lever:** the delta already IS sorted, consolidated Z-set block runs in the trace's own layout.
  Integrating a block-backed delta should adopt its runs, with no decode and no re-encode, given a format
  check (the delta's format and the trace's must encode the same schema). The same applies wherever a
  trace integrates a delta: DISTINCT, and the index operators' inputs (D4).
- Disk: this run left a 208 GB spill directory behind (removed after the run).

### 5.19 A blocked Z-set trace adopts a block-backed delta's runs (2026-11-04, 7a96ef2 + 58874ea)

**Why.** xt59 ended in a view's `IntegrateOp`. The view's blocked Z-set trace pushed a 65M-row block-backed
delta as a managed level, and its freeze decoded every row to sort and re-encode them.

**What changed.**
- **`BlockedZSetTrace.Integrate`** (views and DISTINCT) hands a block-backed delta's runs to the trace
  (`BlockedIndexedTrace.TryAdoptRuns`): no row decoded or encoded.
- **New run objects over the same blocks.** Each keeps its original reachable (`Run.AdoptedFrom`, the
  owner a spilled file lives with), so a GC floor set on the view's run never touches the delta's.
- **Layout check.** The formats must read alike (`ITraceBlockFormat.Adopts`: the same cell kinds and Arrow
  types, column names aside, and no nullable column taken into a NOT NULL one); otherwise the old level
  path.
- **One merge, not a cascade.** More than two adopted runs merge in ONE k-way pass (`MergeMany`, MergeCore's
  grouping for k runs), and the trace then compacts as usual.
  - The first version (xt60) let the trace's pairwise compaction merge a delta's ~65 chunk runs, about
    log2(k) passes, each output spilled.
  - Dead intermediate files are deleted only after a GC collects their run and a sweep runs, so they piled
    up: 518 GB of live spill files filled the disk.
  - A merge cascade inside `Compact` now also spills each output as it is made.

**Tests.**
- Property test (200 cases): a blocked Z-set trace adopting block-backed deltas reads exactly like one
  integrating the same deltas managed (`Current`, `Count`, `WeightOf`), and its runs stay consolidated.
- Layout test: refused into NOT NULL, accepted across renamed columns, and a refused delta still
  integrates correctly.
- The random-query twin must now see adoptions (its blocked DISTINCTs).
- Mutants fail it: adopting all runs but one, and a k-way merge that skips consolidation.
- Suite green knob off, and forced + S1 + 8 KB span caps (3,470 each).

**Extended TPC-DS SF1, W=1, 48 GiB, 1M rows:**

| | xt59 | xt60 (adopt, pairwise merges) | xt61 (adopt, one k-way merge) |
|---|---|---|---|
| ticks 0–17 | 2,744 s | 2,774 s | 2,684 s |
| deltas adopted by tick 17 | — | 37 (152M rows) | 37 (152M rows) |
| the 65M-row view integrate (op 1795) | OOM decoding it | disk full (518 GB of spill files) | **36.9 s** |
| furthest op in tick 18 | 1794 | 1795 | **~1808 of 1868** |
| peak RSS | 41 GB | 41 GB | 42 GB |
| how it ended | OOM, view freeze | `IOException`, no space left on device | OOM in `GroupProject` |

- **The view took the 65M-row delta without decoding it.** Live memory through the fan-out chain stayed at
  ~11.5 GB.
- **It ends in an index operator** (`StatefulOperators.GroupProject`): an `Apply` that turns a Z-set delta
  into a managed `IndexedZSet` for a join or aggregate, here over one of the large deltas after the view.
  Its builder's map pre-sizing exhausted the heap. That is **D4** (§5.14: block-backed `IndexedZSet` deltas,
  produced by the index and exchange-index operators), the next planned increment, and not a regression.
- Disk: 217 GB of spill files were left when this run died (removed). Files of merged-away runs wait for a
  GC; a deterministic delete (reference counting, as Feldera does) is worth doing alongside the startup sweep.

### 5.20 Spilled run files deleted when their last holder lets go (2026-11-05, 0461c08)

**Why.** Before this, a spilled run file was deleted only after a GC found its run object dead and a sweep
ran (every 64 spills). Large, long-lived run objects die in gen2, so on a big heap the files of merged-away
runs piled up (217 GB left by xt61, 518 GB in xt60).

**What changed.**
- **A lease per spilled file** (`RunFileLease`). A trace that lists a run over the file holds it, and
  releases it the moment the run leaves its levels: merged away (`Compact`, `PrepareCheckpoint`), rebound to a
  checkpoint's copy (`Rebind`), or the trace cleared. The file is deleted at once if no other holder is alive.
- **Readers hold it until collected.** A lazy reader that captured runs (`StreamRows`/`StreamEntries` snapshot
  arrays, the checkpoint's list) and a block-backed delta's own trace hold the lease without releasing it.
  Holds are weak references, so a trace dropped whole also frees its files at the next sweep.
- **Adopted runs hold their delta's lease** (§5.19's `Run.AdoptedFrom` is gone).
- **A tripwire.** A hold on a deleted file throws, so a missed hold fails loudly, not as a missing-file read.
- **Startup sweep.** The first spill in a process deletes the `dbspnet-spill-<pid>` roots of processes that are
  not running. It leaves its own root, roots whose pid is running, and names that don't parse.
- **StreamRelease's scan skips leases.** A lease's delete closure reaches the process-wide spiller; walking into
  it raced with other threads' prunes (18 failures in the forced suite).

**Tests.** No GC between ticks, yet on disk are exactly the files the circuit's traces list. A reader taken
before the merges still reads the merged-away files with every body evicted, and they go once it is collected.
An adopted delta's files outlive the delta. Plus lease and sweep units. Mutants fail them: no hold on adopt, no
hold for readers, no release on merge. Suite green knob off and in forced + S1 + 8 KB span caps (3,475 each).

**Extended TPC-DS (xt62, same settings as xt61):**

| | xt61 | xt62 |
|---|---|---|
| ticks 0–17 | 2,684 s | 2,721 s |
| the 65M-row view integrate (op 1795) | 36.9 s | 37.0 s |
| how it ended | OOM in `GroupProject`, ~op 1808 | the same (`BlockedRows.Merged` into a managed `IndexedZSet`) |
| spill dir left at death | 217 GB | 214 GB |
| lowest free disk | — | 351 GB of 600 (~250 GB of spill at peak) |
| files deleted on release / by sweep | — | 313 / 1,928 (by op 1444) |

- **No time cost, and no disk saving.** About 86% of the deletes still came from sweeps. Most spilled bytes in
  this workload are not runs merged away by a state trace. They are in-step delta chunks, which only a GC can
  free, because nothing yet says when a delta is dead (a sum with another set can keep it).
- **What is unknown.** How much of the ~214–250 GB is live state, live deltas, or dead deltas waiting for a GC.
  The census needs that split before choosing a fix: releasing a delta's files when its stream value is
  released, or nudging a GC under disk pressure.
- **The exit cleanup did not run** when the process died this way; the startup sweep now covers it.
- The run still ends in D4 (§5.19).

### 5.21 What the spill disk holds: a census by holder (2026-11-05, 4eed547, xt63)

**Method.** Every spilled file is classed by its strongest live holder: a state trace that lists the run, else
a block-backed delta's own trace, else a lazy reader. A file with no live holder is *dead*: a sweep deletes it.
Readings come right after a forced full GC (the census's tick-end and per-op points), so *dead* is exactly what
only a GC frees. A sweep follows each reading. xt63 used xt62's settings with a per-op point every 2 GiB; it was
stopped at op 1671 of tick 18 (~4,700 s).

| point | files | state | delta | dead (swept) |
|---|---|---|---|---|
| end of tick 0 | 84 | 24.6 GB | 3.1 GB | 6.0 GB |
| end of tick 8 | 176 | 68.0 GB | 4.9 GB | 5.1 GB |
| end of tick 11 | 237 | 89.4 GB | 2.3 GB | 2.3 GB |
| end of tick 17 | 284 | 92.8 GB | 0.9 GB | 1.6 GB |
| tick 18, op 1269 | 280 | 92.7 GB | 1.2 GB | 0.6 GB |
| tick 18, op 1388 | 396 | 106.2 GB | 8.9 GB | 14.6 GB |
| tick 18, op 1425 | 400 | 128.0 GB | 10.4 GB | 15.0 GB |
| tick 18, op 1671 | 421 | **186.6 GB** | 10.3 GB | 9.0 GB |

- **The disk is state.** Live operator traces list 87–95% of the spilled bytes at every point. Tick 18 adds
  ~94 GB of state between op 1269 and op 1671, the fan-out's joins, aggregates and views integrating 21–65M-row deltas.
- **What a GC alone frees is small.** Dead bytes peak at ~15 GB, and in-step deltas at ~10 GB, in the fan-out.
  Between ticks, a delta's files die by the next tick. Deleting delta files deterministically, or nudging a GC
  under disk pressure, would trim at most ~25 GB from a ~200 GB peak. Not worth the risk of freeing a stashed delta.
- **Next disk lever: compression.** Our run files are uncompressed. Feldera compresses storage batches by default
  (`StorageCompression::Default`; Snappy is the named option). The state's size per row is the other half of the
  question, but this census does not measure it.
- Memory held: live heap ~11–17 GB through tick 18. The run would still have ended in D4 (§5.19).

### 5.22 D4: block-backed indexed deltas (2026-11-05, f351503, xt64)

**What.** `IndexBy`, `GroupProject`, `MapFilterGroupProject` and `StepGroupProject` build their indexed output
through a spilling builder whenever the stream carries a block format. The compiler sets that format for join and
aggregate inputs. Past the step-spill row count (`DBSPNET_STEP_SPILL_ROWS`), the entries freeze into hash-ordered
`compact:false` runs (`BlockedGroups`, the indexed twin of D1's `BlockedRows`):
- The exact group count is a k-way walk over each run's in-memory key-hash array. It reads blocks only for hashes
  that several runs share.
- Readers merge the runs by hash with a priority queue, again over the hash arrays, and decode only the runs that
  hold the current hash. A `GroupFor` probe checks every run.
- A blocked indexed trace that integrates the delta adopts its runs without decoding them (`TryAdoptRuns`, rows:
  false). It takes one or two runs as they are and merges more than two (`MergeMany`).

D4 is off by default and uses the same knob as D1–D3. Nexmark w1profile B/ev is identical with D4 on and off. The
full suite passes with D4 off, and with spilling forced on (chunk 2, spill everything).

**A landmine found by the forced suite.** The first enumerator scanned every run's cursor for each hash and read
the hash from the block body. With many runs and a small block cache, that reloaded spilled blocks O(hashes × runs)
times, and a lazy-restore test ran for two hours. The fix: the in-memory hash arrays with a priority queue (as
D1 already did). Rule: **a multi-run reader must order the runs by metadata, never by block bodies.**

**xt64 (xt63's settings plus D4).**
- **It gets past op 1808.** The `GroupProject` that ran out of memory in xt61–xt62 now finishes.
- **It ends at the next aggregate.** `IncrementalAggregateOp` integrates the D4 delta, `AdoptInto` gets more than
  two runs, and `MergeMany` → `RunBuilder.Flush` runs out of memory (peak RSS 43 GB, heap limit 48 GiB). Blocks
  are bounded by bytes, so no single block is too big. `RunBuilder` holds the whole output run's blocks in memory,
  and the spiller writes them only after the merge finishes (`AfterCompact`). This applies to **every** merge,
  ordinary compaction included. The adopted delta was simply the first merge whose output was bigger than the
  free heap.
- **Cost.** Through tick 17, allocation was +45% (5,226 GiB vs 3,610) and step time +12% (2,995 s vs 2,666 s). The
  reason: indexed deltas above 1M entries now freeze and spill on ticks that fit before. In tick 18 the heap is
  lower at the same ops (25 GB vs 42 GB at op 1500).
- Spill disk peaked at ~157 GB at op 1433, a mix close to xt63's.

**Next.** The output of a merge has to stream to disk: a run builder that writes each flushed block to a spill
file once the run outgrows a budget, keeping only lazy block handles. Merge output is then bounded by a block
rather than a run, for compaction and adoption alike. Without it, any run bigger than the heap ends the batch,
whatever the deltas do. Compression (§5.21) comes after, because it shrinks bytes on disk but not resident merge
output.

### 5.23 Merge output streams to its spill file (2026-11-05, a04fed2, xt65)

**What.** A `RunBuilder` whose run goes into a trace's levels (both freezes, `MergeCore`, `MergeMany`) hands its
blocks to a spill file (`IRunSpiller.BeginRun` → `StreamedRunFile`) once they pass the spill threshold
(`DBSPNET_SPILL_MIN_MIB`, default 10, Feldera's `min_spill_size`). It hands over the blocks made so far, then each
block as it is flushed. The run comes back file-backed, with no rewrite by `AfterCompact`. A chain of blocks that
a spanning hash continues is held until the hash ends, and written as one file block, as `SpillRun` writes it. The
building trace holds the lease *before* the file is registered: otherwise a sweep, even one on another worker
thread, can delete it in between.

**xt65.** Same pace as xt64, and a lower heap through tick 18. It still ran out of memory in the op-1809
aggregate's `MergeMany`, now while decoding one hash's entries into a list. That led to §5.24.

### 5.24 Giant groups, a leak in §5.23, and tick 18 completes (2026-11-05, 08f2c7e, 786304f, 6f32916, xt66–xt70)

**Value-hash order (08f2c7e).** Runs now keep each key's entries in canonical order: together, by ascending value
hash. Freezes sort each group; a merge's small path sorts its consolidated output. `MergeCore` and `MergeMany`
share one per-hash step:
- A hash with at least `GiantGroupEntries` (64K) entries across the tied runs is counted from the hash arrays
  alone (past one block only when the hash spans blocks).
- Before merging, a check on cursor copies confirms that each run holds the hash as one key, the same in every
  run, with decoded value hashes never decreasing.
- If so, the runs are merged k-way by value hash, holding only one value hash's entries at a time. If not, the
  merge falls back to the in-memory path, and a counter records it.

The sink writes a giant chain in pieces of at most 64 MiB. Nexmark w1profile B/ev is identical on all 10 queries.
xt66 did 364 giant merges with 0 fallbacks, but still ran out of memory in op 1809, now on the *small* path.

**Finding what fills the heap (xt67, xt68).**
- `dotnet-gcdump` cannot snapshot this heap on .NET 10: it fails, or stops at a 10M-object cap (322 MB of tens of
  GB). The tool is the in-process census: `XT_CENSUS_AT_OPS=1801,1808` forces a live reading and a full
  `StateCensus` after those ops (about 40 s each).
- The census's per-op reading re-armed at `max(heap, live) + step`. The heap figure includes garbage, so after a
  70 GB reading at op 1433 no reading was taken for the rest of the tick. It now re-arms at `live + step`, and
  unforced readings come at least 30 s apart.
- xt68: live is 11–15 GB through tick 18, and 15.3 GB going into op 1809. Operator state is 10.9 GB after ops 1801
  and 1808: partitioned rank 3.6 GB, aggregates 1.4 GB, then joins and views. Op 1809 alone fills the 48 GiB heap.

**The leak (786304f).** `StreamedRunFile` kept every written block's body until `Finish`, to wrap them as cached
lazy blocks at the end. So a streamed merge still held its whole output in memory. Each written block now becomes
a `LazyTraceBlock` at once, its body admitted to the block cache, which evicts past its budget. Only the pending
chain waits. A test drives the sink with a one-byte cache: every appended block but the pending one is collectable
before `Finish`.

*Wrong turn, for the record:* I suspected that values with `string` cells hash differently from their decoded
`Utf8String` form, which would break the value-hash order. They can't: `BlockWriter` casts every VARCHAR cell to
`Utf8String`, so an encoded value never holds a `string`.

**xt69: tick 18 completes.**
- All 1,868 ops ran. The tick-18 step took 4,624 s, 616 s of it in GC pauses. Peak RSS was 42 GB, and spill disk
  went to about 330 GB (230 GB free at the low point).
- Op 1809 took 261 s, and live went from 15 to 36 GB: the aggregate's 28.6M-row output delta is a managed Z-set of
  about 21 GB. Op 1815 is the same, at 35 GB live. **That is the next pressure point:** aggregate output should go
  through a block-backed builder, as join and index outputs already do.
- The run then ran out of memory in the server's `/wait` reply. `OutputStats` asks each view for its row count,
  and `BlockedZSetTrace.Count` materialized the whole view to count it. **Fixed (6f32916):** `CountRows` streams
  the runs (consolidated by hash, floors applied) and folds in the managed levels, which a freeze bounds.

**xt70: batch 1 completes and validates.** These are xt69's settings plus the driver's validation: a fresh DuckDB
replay of the sources and a full recomputation of every model, compared by `EXCEPT ALL` with floats rounded to 10
decimals. The driver reported `status: completed` (`run_batch` sets `failed` unless validation passes) after
7,783 s: 19 ticks and 96 output views, tick 18 in 4,492 s, peak RSS 42 GB, at least 172 GB of disk free. **The
extended TPC-DS batch that motivated this arc (issue #2) now runs within a 48 GiB heap, and its result is correct.**
Next: block-backed aggregate output deltas (ops 1809/1815 hold about 21 GB live each), then run-file compression
(spill disk peaked at about 330 GB).

### 5.25 A blocked aggregate's output rows build as blocks (2026-11-06, fa7274f, xt71)

**What.** The structural aggregate emitted a managed `ZSet<(key, aggregate)>`, which a `MapRows` then flattened
into one row of group key plus every aggregate. The map's output already went through a block-backed builder
(D3), but the tuple delta in between did not. In xt69 that delta was op 1809's 28.6M rows, about 21 GB live.
With in-step spilling on and the aggregate's trace as blocks, the flatten now runs inside
`IncrementalAggregateOp`, and its rows go through the output stream's `DeltaSpill` builder.
- **The rows' format is their runtime layout** (group key, then every aggregate the plan collects), not
  `plan.Schema`, which the compiler's comment says can be narrower. A format narrower than the row fails the
  new test (mutant); on the queries tried, `plan.Schema` happened to be the full width.
- **The circuit keeps its shape.** Removing the map op shifted operator indexes and changed the snapshot's plan
  fingerprint, so a checkpoint written by a blocked aggregate no longer restored into a managed one (the forced
  suite caught it in `Checkpoint_RestoresLazily_AndContinuesExactly`). A `FusedSlotOp` now sits in the map's
  slot. It passes the rows through and fingerprints as the `ApplyOp` it replaces (`IFingerprintIdentity`).
- Knob off, nothing changes: Nexmark w1profile B/ev is identical on all 10 queries. Suites pass normal (3,484)
  and forced (3,484). Mutants caught: never fusing, a plain builder in the fused op, a narrower format, no slot.

**xt71 (xt70's settings plus fa7274f): batch 1 completes and validates**. The driver reports `status: completed`
after 7,720 s (xt70: 7,783 s).

| | xt69/xt70 | xt71 |
|---|---|---|
| live after op 1809 (aggregate) | 36.0 GB | **15.3 GB** |
| live after op 1815 (aggregate) | 35.3 GB | **15.5 GB** |
| peak live in tick 18 | 36.0 GB (op 1809) | **15.6 GB** (op 1812), the level going in |
| heap after op 1809 / 1811 (incl. garbage) | 69.6 / 78.4 GB | 33.4 / 35.8 GB |
| op 1809 / op 1810 | 261 s / 18 s (the map) | 264 s / below the log threshold (the slot) |
| ticks 0–17, tick 18 done | 3,018 s, 7,511 s | 3,020 s, 7,452 s |
| peak RSS, spill disk peak, min free | 42 GB, ~330 GB, 172 GB | 45 GB, 312 GB, 178 GB |

- **Tick 18 no longer grows live memory.** Every op after 1808 runs at the 15–15.6 GB that the state takes going
  in. The two aggregates' deltas were the last managed copies of tick 18's big outputs.
- **Peak RSS did not fall** (45 vs 42 GB). It is garbage: the highest heap readings are at ops 1433 (an `Apply`,
  70.6 GB) and 1823 (a left join, 67.8 GB), and live never passes 15.6 GB. Under server GC with a 48 GiB hard limit,
  RSS follows how much garbage the GC lets build up, not the live set. Bounding it is a GC-settings question
  (§3c, §3i), separate from what the engine holds.
- Time is unchanged. The fused aggregate saves the map's 18 s and a second pass, which is within run-to-run noise
  at this length.

**Next.** Spill disk (312 GB peak) is now the largest resource the batch uses: run-file compression (§5.21).
