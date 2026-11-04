# Bounded memory: keeping cold blocks on disk

Status: **phase 1 (census) done 2026-11-03. Design agreed: fixed per-worker budget, persisted blocks only; batch-1 spilling deferred (§1.4). Increment 1 BUILT 2026-11-03, opt-in (`DBSPNET_BLOCK_CACHE_MIB`), §3.5; miss cost apportioned and cut ~8× (§3.6). Increment 2 BUILT: window state backed and evictable (§3.7). The output row index is spilled too (§3.9): 0.51 GB live at 128 MiB.** Follows
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
  allocated 1.47 GB writing the same rows in 64K-row batches against 0.64 GB in one batch per view.

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

The b2 output's +4 ms is probes into the gathered index run. Its blocks are cut by entry count,
not bytes, so for wide rows they are larger than the row path's. Cutting by bytes would close it.

Per stage on the probe (one batch per view): forming 1.39 GB → 0, index 0.99 → 0.66 GB. The Arrow
build's 0.8 GB is the output's own bytes.

## 4. Open questions

1. **Budget policy.** A fixed byte budget per worker, as Feldera does by default, or one driven
   by memory pressure (RSS against a limit)? Proposal: fixed first; pressure as a follow-on.
2. **Scope of increment 1.** Evict only persisted blocks, so nothing new is written, and accept
   that memory between checkpoints is bounded only by the checkpoint interval?
3. **Batch 1.** Its working set is the whole state (the bulk load), and nothing is persisted
   until its checkpoint. Bounding batch 1 itself needs spilling (Feldera's moderate-pressure
   mode). Is that in scope for this arc?
