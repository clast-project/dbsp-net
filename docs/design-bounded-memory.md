# Bounded memory: keeping cold blocks on disk

Status: **phase 1 (census) done 2026-11-03. Design agreed: fixed per-worker budget, persisted blocks only; batch-1 spilling deferred (§1.4). Increment 1 BUILT 2026-11-03, opt-in (`DBSPNET_BLOCK_CACHE_MIB`), §3.5; miss cost apportioned and cut ~8× (§3.6). Increment 2 BUILT: window state backed and evictable (§3.7).** Follows
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

### 3.4 Gates

These are the usual ones:
- ivm-bench SF=3 W=12, 5 runs per arm, budgets unbounded / 512 MiB / 128 MiB.
- Batch-1 drain within +3%.
- Batches 2 and 3, restart, and checkpoint times.
- Views byte-identical.
- Peak RSS and the census.
- Nexmark W=14: no regression with no budget.

## 4. Open questions

1. **Budget policy.** A fixed byte budget per worker, as Feldera does by default, or one driven
   by memory pressure (RSS against a limit)? Proposal: fixed first; pressure as a follow-on.
2. **Scope of increment 1.** Evict only persisted blocks, so nothing new is written, and accept
   that memory between checkpoints is bounded only by the checkpoint interval?
3. **Batch 1.** Its working set is the whole state (the bulk load), and nothing is persisted
   until its checkpoint. Bounding batch 1 itself needs spilling (Feldera's moderate-pressure
   mode). Is that in scope for this arc?
