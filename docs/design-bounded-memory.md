# Bounded memory: keeping cold blocks on disk

Status: **§5.39: the +17% at 262,144-row chunks is mid-sized deltas going to blocks (every reader decodes them, +468 GiB; merges, encodes, probes the rest) and that setting is 22% FASTER (2,536 vs 3,268 s restored); decode for readers is 33–40% of tick 18 in both. §5.38: a giant group's file blocks are capped at 16 MiB (b50feb2); xt134–xt137 full batch 1 at 262,144-row chunks validate 4 of 4 (~3,480 s), against 5 of 7 before. §5.38: step chunks capped by bytes (`DBSPNET_STEP_SPILL_MIB`, off by default) match 262,144-row chunks (2 of 3 full runs validate; tick 18 +17% allocation either way, the cost of block-backed deltas); the one failure, now named, is a 59 MiB block load failing with 17.5 GB live in 48.9 GB committed. §5.37: a block-backed delta hands a giant group out streamed, so the giant joins (ops 1683–1703) no longer each materialize a 7.2M-row left group; xt118 restored batch 1 completes, tick 18 allocation 6,712 → 6,087 GiB. §5.36: ops 2153–2177 (a rolling eligibility view, per-row cost, no skew): merges copy entries, aggregates and joins read each spilled group once, surrogate keys (`MD5(CONCAT_WS(...))`) format and hash in one buffer; −29% allocation in a local stand-in. The restored batch 1 at W=12 completes (xt102, xt109, §5.35): ops 1680–1705, where one join key per worker carried 7.2M rows, no longer decode and re-encode in giant merges and exchange gathers (−18% allocation there, −6% for tick 18). Before that, tick 18 completed restored once (xt99, §5.34) and failed in its ops 1689–1700 in other runs; the end-of-batch view count no longer forms the views (§5.34). §5.33: stored views kept each tick's delta managed (~10 GB). S3 (pressure thresholds) BUILT 2026-11-06, default on (§5.28); xt82 reaches tick 18 op 1733 and still OOMs (§5.29). A restart point after tick 17 (§5.30) halves an iteration; it fixed a checkpoint that pinned every spilled body until commit. A census of what a restore leaves behind (§5.31) found ~4–5 GB of empty `Dictionary` capacity in the window stores; releasing it takes the live run past op 1733 to op 1994 (`ExchangeIndexOp`, live 48 GB), the same wall as the restored run, so the restart point is now a faithful stand-in for it. Phase 1 (census) done 2026-11-03. Design agreed: fixed per-worker budget, persisted blocks only; batch-1 spilling deferred (§1.4). Increment 1 BUILT 2026-11-03, opt-in (`DBSPNET_BLOCK_CACHE_MIB`), §3.5; miss cost apportioned and cut ~8× (§3.6). Increment 2 BUILT: window state backed and evictable (§3.7). The output row index is spilled too (§3.9): 0.51 GB live at 128 MiB. Mid-batch spilling: design proposed 2026-11-04 (§5); decisions §5.4, knobs §5.5; extended TPC-DS census §5.6 reorders it: R0 tick-0 release, S2 window write-back by bytes, S1, S3. R0 BUILT (§5.7), S2 BUILT (§5.8, default 1 GiB), S1a BUILT opt-in (§5.9, scratch spill), inputs released (§5.10); TOP-K state blocked (§5.12); next is tick 18's in-step peak on extended TPC-DS; S1b, S3 not built.** Follows
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
- **Writers.** A blocked trace's concatenated giant is cut every `RunFile.MaxSpanBlockBytes` (256 MiB; 16 MiB since §5.38), at
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

### 5.26 Run files compress each block whole, with zstd level 1 (2026-11-06, bff1bfd, xt72–xt73)

**What we measured.** xt72 reran batch 1 with a sampler that hard-linked random spill files as they appeared:
120 files, 40.7 GiB of blocks, every layout the batch spills. Each block's record-batch message was compressed
whole, as Feldera compresses a block. Ratios are identical in both runtimes; speeds are .NET 10 on the M4, one
thread, with tiering off so the JIT does not muddy them (`DOTNET_TieredCompilation=0`).

| codec | ratio | compress MB/s | decompress MB/s | the 312 GB peak becomes |
|---|---|---|---|---|
| Snappy (Feldera's default) | 3.01 | 2,990 | 5,801 | ~104 GB |
| LZ4 | 3.25 | 1,610 | 6,813 | ~96 GB |
| zstd −1 | 3.68 | 1,364 | 4,053 | ~85 GB |
| **zstd 1** | **4.97** | 1,050 | 2,207 | **~63 GB** |
| zstd 3 | 5.06 | 827 | 2,306 | ~62 GB |
| Brotli q0 / q1 | 4.26 / 4.62 | 1,151 / 676 | 873 / 833 | |
| Deflate (through `DeflateStream`) | 3.45 | 822 | 1,196 | |
| Arrow IPC per-buffer LZ4 / zstd | 2.40 / 4.33 | | | |

- **zstd 1 is the pick.** It keeps about 40% more off the disk than Snappy, and the disk is what we are saving.
  Level 3 costs 25% more CPU for under 2% more ratio. Brotli and Deflate lose on ratio and on decompression.
  The widest layout (a fact row, 16% of all bytes) compresses 11.6×; timestamp-heavy rows compress least (1.6×).
- **Whole blocks, not Arrow's per-buffer compression.** Per-buffer zstd compresses less (4.33×), and a compressed
  Arrow message no longer takes `TraceBlock.TryParseHeader`'s fast path.
- **zstd ships in .NET 11** (`ZstandardEncoder`/`ZstandardDecoder`, span to span). On .NET 10 we use
  ZstdSharp.Port, behind the same span-shaped calls, so the switch changes no byte on disk.

**The cost is CPU on the step thread.** The batch writes 1.15 TB of spill over its life (`spilled_mib`), almost
4× the disk peak. Compressed inline at 1,050 MB/s, that is about 1,100 s on a 7,785 s batch. Spill writes are
synchronous on the step thread (`RunSpiller.AfterCompact`, the streamed run sink), but the batch runs one worker
on a 14-core machine and blocks are independent.

**What changed.**

- **Run format 5.** Each index entry gains the block's message length and codec (36 bytes, from 28), so a block
  is stored raw or compressed block by block. A reader still opens versions 3 and 4 (every block raw). Block-set
  manifests (§3g) are version 2 for the same two fields, and still read version 1.
- **`BlockCompression`.** zstd at `DBSPNET_RUN_ZSTD_LEVEL` (default 1) with per-thread contexts;
  `DBSPNET_RUN_COMPRESSION=none` turns it off. A block under 256 bytes, or one that does not shrink, is stored raw.
- **`RunStreamWriter` compresses in parallel and writes in order.** A block appended as its message is
  compressed on the thread pool. Up to `MaxInFlight` blocks (cores − 2) and 256 MB are in flight before an append
  waits for the oldest. Its index entry reaches the caller through a callback once it is written, so the callers
  that read `Index[^1]` after each append (the spill writer, the streamed run file, backing blocks, block sets)
  now take the entry from the callback.
- **The writer waits for a compression synchronously; it never awaits one.** Awaited, the rest of the write moved
  to a pool thread. That thread's last work item, the write's state machine, then stayed reachable while the
  thread sat idle, and it pinned the blocks the callbacks had built, and through them their files. The spiller
  test caught it as two spill files that outlived their program.
- **A giant block streams through a zstd frame** (`AppendConcatAsync`). It is never built in memory, raw or
  compressed.
- **Reads decompress straight into the block's array:** the lazy block fault, both on local files and other
  stores, and the reader's block decode. Compaction copies a stored block as stored, with no decode and no
  recompression. Blocks written from a record batch (`RunFile.WriteAsync`, `AppendAsync`: the managed traces'
  checkpoints and compaction's merged blocks) stay raw. A run with compressed blocks is no longer a plain Arrow
  IPC stream, as Feldera's files are not.

**Tests.** Seven in `RunCompressionTests`:
- a streamed run is compressed and reads back byte for byte, on local and in-memory stores;
- with compression off, blocks are stored raw;
- a giant block streams through the compressor;
- a run can mix codecs block by block, and compaction's copy keeps each block as stored;
- blocks that finish compressing out of order are still written in append order;
- a version 4 run still reads.

The streamed-run memory test now allows the compression window.

Suites pass with compression on, off (`DBSPNET_RUN_COMPRESSION=none`), and in the forced spill mode with each
(3,491 every time). Mutants caught: a decoder that ignores the codec (64 failures) and a block-set manifest that
drops the codec (8, the window stores). Nexmark does not spill, so its hot path is untouched.

**xt73 (xt71's settings plus bff1bfd): batch 1 completes and validates, with a quarter of the spill disk.**
The driver reports `status: completed`, which `run_batch` only does when validation passes.

| | xt72 (raw) | xt73 (zstd 1) | |
|---|---|---|---|
| spill disk peak (`spill_disk_peak_mib`) | 311,711 MiB | 73,588 MiB | 4.24× smaller |
| bytes written to spill (`spilled_mib`) | 1,151,926 MiB | 298,855 MiB | 3.85× smaller |
| batch duration (`duration_s`) | 7,785 s | 8,800 s | +1,015 s, +13% |
| tick 17 / tick 18 done at | 3,008 / 7,517 s | 3,299 / 8,468 s | |
| tick 18 live heap | 15,538 MiB | 15,565 MiB | unchanged |
| peak RSS | 44.8 GiB | 40.6 GiB | |

- **The disk saving is what the samples predicted.** The peak shrank 4.24×, against 4.97× on the sampled
  blocks. Blocks written from record batches (compaction's merged blocks; see What changed) stay raw, and so do blocks
  under 256 bytes. That is why the bytes written shrank less (3.85×) than the peak did.
- **The time cost is about the size the inline estimate gave (~1,100 s), even though compression now runs in
  parallel.** This is one run of each, and it is not apportioned. Candidates are decompression on block faults
  (every faulted spilled block now decodes at ~2.2 GB/s) and spill writes that do not go through
  `RunStreamWriter`'s parallel window. If the cost matters, LZ4 is a one-line change (6.8 GB/s decode) and
  would keep about 96 GB of the 312.
- xt72's minimum free disk (178 GiB) included 41 GB of sample hard links, so the two runs' free-disk minima
  (xt73: 445 GiB) are not comparable. As in xt72, the run's spill directory outlived the test host and was
  deleted by hand (80 GB).

**xt74 (xt73 with `DBSPNET_RUN_COMPRESSION=lz4`): LZ4 trades disk for time, as the samples said.** LZ4 is a
third codec: the LZ4 block format for a block, an LZ4 frame for a giant block streamed from its parts (codec 3).
Batch 1 completes and validates.

| | raw (xt72) | LZ4 (xt74) | zstd 1 (xt73) |
|---|---|---|---|
| spill disk peak | 311,711 MiB | 111,980 MiB (2.78×) | 73,588 MiB (4.24×) |
| bytes written to spill | 1,151,926 MiB | 442,828 MiB (2.60×) | 298,855 MiB (3.85×) |
| batch duration | 7,785 s | 8,155 s (+370 s, +4.8%) | 8,800 s (+1,015 s, +13%) |
| tick 17 / 18 done at | 3,008 / 7,517 s | 3,111 / 7,862 s | 3,299 / 8,468 s |
| tick 18 live heap | 15,538 MiB | 15,575 MiB | 15,565 MiB |

- Against the samples, LZ4 shrank the peak a little less than predicted (2.78× against 3.25×), as zstd did
  (4.24× against 4.97×): the raw-written blocks dilute both.
- **Per GB of disk saved, zstd's time is not much dearer.** LZ4 saves 200 GB of peak for 370 s, and zstd saves
  238 GB for 1,015 s, so zstd's last 38 GB cost about 645 s. Each arm is one run.
- **The default stays zstd 1.** Disk is the constraint this arc exists for. LZ4 is there for when time matters
  more (`DBSPNET_RUN_COMPRESSION=lz4`). Tests: the round-trip and giant-block tests now run for LZ4 too (10 in
  `RunCompressionTests`), and the suite passes with LZ4 as the default and in the forced spill mode (3,494).


### 5.27 Twelve workers: what grows with W, and three fixes (2026-11-06, xt75–xt81)

**The question.** ivm-bench runs DbspNet at `DBSPNET_WORKERS=12`. Every run so far (xt70–xt74) was W=1. xt75
reran batch 1 at W=12 with xt73's settings (48 GiB heap cap, spill on, step threshold 1M rows, zstd).

**W=12 is 3.2× faster through tick 17 (1,039 s against 3,299 s) and runs out of memory in tick 18.** Every W=12
run so far fails there (cap 48 GiB; CI's container gives .NET about 90 GB, which this 64 GB machine cannot test):

| run | change | ticks 0–17 | tick 18 |
|---|---|---|---|
| xt75 | W=12, xt73's settings | 1,039 s | OOM at 1,448 s, op 1689 |
| xt76 | step threshold ÷ 12 (83k rows) | 1,251 s | OOM at 1,536 s, same op and stack |
| xt79 | every run spills, freeze ÷ 12 | 1,062 s | OOM at 1,480 s, same op |
| xt80 | spill-aware exchange (fix 1, 2) | 1,186 s | OOM at 1,799 s, op 1691 |
| xt81 | + streamed group count (fix 3) | 1,166 s | OOM at 1,480 s, in a block load |

**What grows with W, at the end of tick 17** (state census, xt77 against xt78):

| | W=1 | W=12 |
|---|---|---|
| live state | 7.4 GB | 13.7 GB |
| blocked traces (left/inner join, view, aggregate, distinct) | ~1.1 GB | ~6.2 GB |
| ordered partition stores (TOP-K, rank) | 4.0 GB | 5.5 GB |
| resident blocks / managed entries | 0.5 GB / 0 | 4.0–4.5 GB / 2.5M |

The two thresholds that let a trace's state leave memory are per trace: a run under `DBSPNET_SPILL_MIN_MIB`
(10 MiB) never spills, and a trace keeps up to `DBSPNET_TRACE_BLOCKS_FREEZE` (65,536) managed rows. W=12 has
twelve times the traces at a twelfth of the size each, so far more stay under both. xt79 forced both down and
the baseline went back to W=1's (blocks 515 MB, managed 0), but tick 18 still failed: the baseline is real but
not what kills tick 18. Feldera's thresholds are the same per-batch kind, but it switches them by memory
pressure (RSS against `max_rss`: 0.85 moderate spills every merge, 0.90 high every insert, 0.95 critical every
step batch); that is the fix for the baseline, and is not built.

**What kills tick 18 is skew through an exchange.** An exchange (op 1687) repartitions 21.6M rows on a key with
about three values: three workers receive about 7.2M rows each. At W=1 there is no exchange and those rows are
built as blocks past the threshold.

1. **The exchange was not part of in-step spilling** (`ExchangeOp`). It listed every input row (materializing a
   block-backed input) and gathered into a plain builder. Now the output inherits the input's block format; a
   block-backed input splits into one spilling builder per destination, and the gather spills past the
   threshold. Spill off: the path is unchanged. Test: two block-backed shards all routed to worker 0 (W=4, 12)
   stay blocks and equal the union; the plain gather fails it.
2. **A block-backed delta's enumerator did not hold its files** (`BlockedRows.GetEnumerator`). Every consumer
   so far also held the set through its stream; fix 1's gather enumerates a bucket nothing else references, and
   a sweep deleted its files mid-read (the forced suite caught it: 7 parallel tests, "Could not find file").
   The enumerator now holds its runs' leases, as a trace's lazy readers do. Test: only an enumerator left, the
   set collected and swept, it still reads every row, and the files go once it is collected; without the hold
   both cases fail.
3. **Counting a group-by delta's groups built every group a key hash spans runs for** (`BlockedGroups`). The
   skewed key spans every 1M-row run, so the count materialized three 7.2M-value groups at once, with every
   block body pinned (`At` returned a list). `At` now streams, and the count reads a key whose entries all have
   one sign as non-empty without building it; only a key with both signs is summed. Test: a key cancelled
   across chunks, a one-signed giant key and a mixed key that survives; counting mixed keys as non-empty fails
   it (the randomized test did not: random adds almost never cancel a whole group across chunks).

Suites pass default and forced (3,499).

**Still open.** xt81 fails while loading a block (an allocation at the limit, not one structure): worker 0's
live heap through tick 18 is 25–30 GB against W=1's 15.5 GB, and managed entries reach 9M. Candidates: the
per-trace baseline (memory pressure, above), the downstream consumer of the skewed group-by (each worker
enumerating a 7.2M-value group), and server GC's twelve heaps under a hard cap. `BroadcastExchangeOp` and
`ExchangeIndexOp` still gather managed.

### 5.28 S3: spill thresholds that follow memory pressure (2026-11-06, design)

**Why.** §5.27 left two things at W=12: twelve times the traces, each under the per-trace thresholds
(`SPILL_MIN` 10 MiB, `FREEZE` 65,536 entries), keep ~5 GB more of baseline in memory than W=1. Tick 18 then
runs out of room on top of it. xt79 showed that forcing the thresholds down for the whole run brings the
baseline back to W=1's, but that costs every run. Feldera keeps its thresholds where they are until memory is
short, then moves them (§5.1). This section builds that S3 (§5.3), with the knobs of §5.5.

**The signal: the GC heap in use, not RSS.** Feldera measures RSS against `max_rss`. Under a .NET hard limit,
RSS and the committed heap both climb toward the cap between full collections: that headroom is what server
GC is for. Measured that way, a healthy run would read Critical half the time. The measure here is the heap
still in use after the most recent collection of any kind: `GCMemoryInfo.HeapSizeBytes - FragmentedBytes`.
After a gen0 or gen1 collection it still counts gen2 garbage, so it overstates and never understates. That
errs the same way RSS does, only less. The limit is `DBSPNET_MEMORY_LIMIT_MIB`, or else
`TotalAvailableMemoryBytes`, which is the hard limit when one is set and otherwise physical memory or the
cgroup's.

**Cadence: every collection, not a 1 s timer.** Tick 18 allocates gigabytes a second, so a 1 s sample is
stale before anyone reads it. A finalizable sentinel object re-registers itself on every collection (the
`Gen2GcCallback` pattern, minus the gen2). Its finalizer recomputes the level once per GC, on the finalizer
thread. Hot paths read one static `int`, so a per-row check costs one load.

**Levels and hysteresis are Feldera's.** Low below 85% of the limit, Moderate below 90, High below 95, then
Critical (`DBSPNET_PRESSURE_LEVELS`, default `85,90,95`). Low and Moderate follow the raw level. High holds
until the raw level falls to Low. Critical holds until it falls to Moderate or below. Each entry into High or
Critical from below bumps a **pressure epoch**.

**What each level changes.** Each row is Feldera's table (§5.1) mapped onto our three knobs:

| level | runs written to files (`RunSpiller`, merge and freeze outputs, §5.9/§5.23) | managed entries before a trace freezes (`BlockedIndexedTrace.Push`) | in-step delta chunks (`SpillingRows`/`SpillingGroups`, §5.15/§5.22) | window write-back (§5.8) |
|---|---|---|---|---|
| Low | ≥ `SPILL_MIN` (10 MiB) | `FREEZE` (65,536) | `STEP_SPILL_ROWS` | ≤ `WINDOW_WRITEBACK` (1 GiB) |
| Moderate | **all** | `FREEZE` | `STEP_SPILL_ROWS` | as Low |
| High | all | **every insert** (`DBSPNET_PRESSURE_HIGH_FREEZE`, default 0), and **every managed level pushed** once per epoch | `STEP_SPILL_ROWS` | **dropped at every step end** |
| Critical | all | as High | **`DBSPNET_PRESSURE_CRITICAL_STEP_ROWS`** (default 65,536) when that is lower | as High |

- **Moderate is Feldera's "merge outputs all", widened to freezes.** We have one spill hook for both
  (`AfterCompact`), and a freeze's run is what Feldera calls an inserted batch.
- **High's push is Feldera's `memory_pressure_epoch`.** Feldera wakes its merger threads to write every
  in-memory batch. Our traces belong to their worker thread, so nobody else may freeze them. Instead, each
  `RootCircuit` keeps weak references to the state traces (`Compacts`) that took a managed level since its last
  push. At its next operator boundary, if it last pushed in an older epoch, it freezes them all on its own
  thread. Each worker has a dedicated thread (`ParallelCircuit`) and one replica per circuit, so the owner is
  the one stepping. A delta's private trace is never registered, so a dead delta is not kept alive.
- **Waiting workers relieve too** (added after xt82's first half, below). A worker parked at an exchange
  barrier, or at the end-of-step rendezvous behind a skewed peer, waits in 100 ms slices
  (`PressureRelief.WaitRelieving`) and runs its boundary check between them. A timed-out `SignalAndWait`
  withdraws its signal, so the retry is the same rendezvous. It's the worker's own thread with none of its
  operators running, so it may freeze its traces.
- **Critical has a floor, not Feldera's "all non-empty".** Feldera's unit is a whole batch. Ours is a
  builder's managed chunk, so freezing every non-empty chunk would mean a run per row. The floor is the trace
  freeze size, as a knob. It acts only where in-step spilling is on (`STEP_SPILL_ROWS` set, a format
  attached).
- **Not changed by pressure:** the block cache budget (Feldera doesn't change its cache either), the
  window promotion sizes, and backpressure (Feldera's is counted in batches, §5.1).

**Knobs** (all read once at start, overridable in an async context for tests):

| knob | default | |
|---|---|---|
| `DBSPNET_PRESSURE` | `on` | `off`: the fixed thresholds only. `low`/`moderate`/`high`/`critical`: pin the level (a forced suite run uses `high` and `critical`) |
| `DBSPNET_MEMORY_LIMIT_MIB` | `TotalAvailableMemoryBytes` | the limit |
| `DBSPNET_PRESSURE_LEVELS` | `85,90,95` | Moderate, High and Critical, in % of the limit |
| `DBSPNET_PRESSURE_HIGH_FREEZE` | 0 | managed entries a trace keeps at High or above (0: freeze every insert, as Feldera) |
| `DBSPNET_PRESSURE_CRITICAL_STEP_ROWS` | 65,536 | the in-step chunk at Critical |

**Default on.** This follows Feldera's default (policy: adopt Feldera's assumptions by default). Below 85% of
the limit nothing changes, so runs that never come near the limit (Nexmark, TPC-DI with no cap) see one int
load per insert and per delta row. The host logs the limit, the levels, and every level change (with the
used bytes), so a run's log shows when pressure acted.

**Tests.** The hysteresis table, and the raw level from used bytes and limit, as pure functions. Pinned
levels change behaviour: Moderate spills a run under `SPILL_MIN`; High freezes on one insert; an epoch bump
pushes every listed trace's managed level at the next operator boundary, once, and only at High or above
(an idle join already freezes on its empty tick, §3.2), and leaves a delta's trace alone; Critical
freezes a delta chunk at the floor. Each test fails with its threshold left at Low. A forced suite run with
`DBSPNET_PRESSURE=high`, and one with `critical`, must give the same results as default.

**Measure.** xt82 = xt81's setup (W=12, 48 GiB cap, step spill 1M, spill on, cache 1 GiB) with pressure on.
Question: does tick 18 fit? Log the level changes and the census next to xt81's.

### 5.29 xt82: pressure acts, and tick 18 still does not fit (2026-11-06)

xt82 = xt81's setup (W=12, 48 GiB `GCHeapHardLimit`, server GC, step spill 1M rows, spill on, cache 1 GiB),
with §5.28 at its defaults (98c3a7d).

| | xt81 | xt82 |
|---|---|---|
| ticks 0–17 | 1,166 s | ~1,170 s (tick 18 starts at 1,172 s) |
| furthest op in tick 18 | 1671 | **1733** |
| out of memory at | 1,480 s | **2,471 s** |
| managed entries in tick 18 | 9.06M throughout | 9.06M → **0** (op 1681 on) |
| live at the last census | 25.0 GB | 26.6 GB |
| peak RSS | 44 GiB | 44 GiB |

- **The level moved 74 times**, all within tick 18 (ticks 0–17 stayed Low). It was Moderate from about 42 GB in
  use, High from about 45 GB, and Critical four times, at 46.9–48.7 GB. Every High ended at Low within seconds:
  a collection freed garbage. The hysteresis was never what held a level.
- **The push did the work, but late.** 17 pushes froze all 9,058,761 managed entries. Until op ~1680 there were
  none, though the level reached High several times. The busy worker sat inside one long operator, and the
  other eleven were parked at an exchange barrier, so none of them reached a boundary while High lasted. Fix,
  built after xt82 and not yet measured at scale: a parked worker waits in 100 ms slices and checks between them
  (`PressureRelief.WaitRelieving`, §5.28). It would have pushed at the first High, around op 1657.
- **What still fails is not state the thresholds move.** At the last census live is 26.6 GB, nothing is managed,
  resident blocks are 3.6 GB, and the cache is 1 GiB. The last level reading is Low (29.4 GB in use). The run
  died in an allocation burst inside ApplyOps that emit ~2M-row outputs over and over. The heap swung between
  38 and 69 GB between collections (`GC.GetTotalMemory`), and an OOM came while live was half the cap. So the
  remaining gap is allocation rate against what server GC's twelve heaps can reclaim under a hard limit, not
  live bytes.

**Next candidates, not tried:** the GC configuration under the cap (`GCHeapCount` below 12, `GCConserveMemory`,
regions; CI's container gives ~90 GB, not 48); what op 1731/1733 builds per output row (a managed builder per
2M-row chunk); and whether step spill at 1M rows is too coarse at W=12, where a chunk is per worker. The barrier
fix is unlikely to move xt82's failure, since everything was pushed by op 1681.

### 5.30 A restart point before tick 18, and what a restore leaves behind (2026-11-07)

Ticks 0–17 take ~1,170 s of each ~2,470 s W=12 run. To iterate on tick 18 alone, the census harness can
checkpoint after a chosen tick and exit (`XT_CHECKPOINT_AT_TICK`, with `DBSPNET_SNAPSHOT_DIR` and
`DBSPNET_STATE_LAYOUT=runs`); a later run deploys against an APFS clone (`cp -c`) of that directory and restores
before tick 18. `ProgramRunner.AfterTick` now fires after the tick's source cursor advances, so a checkpoint
taken there resumes at the next tick; the harness offsets its tick numbers by the restored tick.

- **Smoke (checkpoint after tick 2):** 8.7 s to save 6.6 GB. The restored run resumes at `ext_account_profile`
  (xt82's tick 3), and step times match (tick 3 109 s vs 103 s, tick 6 3.5 s vs 3.1 s).
- **A bug the save found.** The first save after tick 17 ran out of memory. A checkpoint rewrites a spilled run
  into its own store by reading each block from the spill file, and `AppendChainAsync` handed each body it read
  back to be bound to the checkpoint's copy at commit. Until commit, every spilled body of the circuit was held:
  ~67 GB uncompressed at tick 17. Now only a body that was already in memory is handed back; one read from a
  file is left to the cache (`RunSpillerTests.A_checkpoint_of_spilled_runs_holds_none_of_their_bodies`, which
  fails without the fix). The fixed save takes 22.5 s and writes 19 GB. Production checkpoints with spill on
  hit the same path.
- **The control (xt83-ctl: today's code, restored after tick 17) is not faithful.** It ran out of memory at
  tick 18 op 1994/2222 (`ExchangeIndexOp`, 15.7 s, live 46.7 GB: live-bound), not at xt82's op 1733 (an
  allocation-rate failure at 26.6 GB live). At every matching op it is 10–14 GB leaner:

  | op in tick 18 | xt82 live | xt83-ctl live |
  |---|---|---|
  | start | 17.0 GB | 0.9 GB (op 436) |
  | 1528 | 20.8 GB | 10.4 GB |
  | 1669 | 27.5 GB | 19.4 GB |
  | ~1733 | 26.6 GB | 12.9 GB (op 1735) |

  Its pushes came at about the same point (managed 8.1M → 4 by op 1725; xt82: op 1681), so the barrier-wait
  relief (§5.28) does not explain the difference. Time per op is about the same (op 1733 at 1,136 s after
  restore; xt82 1,292 s after tick 17).

**What this means.** A restore keeps the state and drops ~16 GB that xt82 carries into tick 18: resident
blocks (4.5 GB), managed entries (2.5M), and ~10 GB more. (§5.31: most of that ~10 GB is state a restore
leaves lazy, not state the run does not need; ~4–5 GB of it was empty dictionary capacity, now released.)
That ~10 GB is itself the cheapest lead: whatever the restore does not rebuild, the live run could drop. Until it is identified,
the restart point answers "does this change help tick 18 after op 1733" (the control's op 1994 failure,
live-bound) but not "does tick 18 fit" in a run that did not restore. Next: a state census at the end of tick
17 (`XT_STATE_CENSUS_TICK=17`) in a save run, against the same census right after a restore.

### 5.31 What a restore leaves behind: empty window-store capacity (2026-11-07)

**Census.** `XT_STATE_CENSUS_TICK=17` (a forced compacting GC, then `StateCensus` and `BlockResidency`) at
the end of tick 17 in a save run (xt84-save), against the same census right after a restore
(`XT_CENSUS_AFTER_DEPLOY=1`, xt84-restored):

| | end of tick 17 | right after restore |
|---|---|---|
| live | 16,958 MB | 597 MB |
| reached by the walk | 13,066 MB | 322 MB |
| block cache | 1,024 MB | — |

Charged by the walk at the end of tick 17:

- `PartitionedRank` 4.2 GB: `_accum` (managed rank state, 1.2 GB in 32.8M objects; a restore leaves it lazy),
  `_window` 0.5 GB, `_store` 2.5 GB.
- `PartitionedTopK` `_store` 3.0 GB.
- Blocked traces' resident in-memory runs ~4.9 GB, managed levels ~1 GB, window aggregates' `_small` 0.3 GB.
- ~2.9 GB beyond the cache that the walk does not reach (statics), not yet identified.

So most of the gap is residency, state the live run holds in memory and a restore reads back on demand, not
garbage. The exception was in the two stores: `Entry<StructuralRow, SortedDictionary>[]` and
`Entry<StructuralRow, List>[]` arrays of 1,092 MB each under TOP-K, 901 MB each under rank, and
`Int32[]` buckets of 369 and 306 MB. Each had zero entries. `OrderedPartitionStore` keeps a write-back
cache (`_small`) and a queue (`_pending`) of partitions, bounded by `WindowWriteBackBytes` (§5.8). It
empties both with `Dictionary.Clear()`, which keeps the capacity, so one huge tick leaves arrays sized for
it in every replica's store for the rest of the run. The byte estimate the budget reads counts entries, not
capacity, so it saw none of the ~4–5 GB.

**Fix.** Emptying either dictionary now trims it when its capacity is above 1,024
(`OrderedPartitionStore.Release`). Test: `WindowWriteBackBudgetTests.A_dropped_cache_releases_a_huge_ticks_capacity`
(20,000 partitions in one tick, budget 0: both capacities must be ≤ 1,024 afterwards and the unbounded twin's
≥ 20,000, then five more ticks with matching digests); without the trim it fails with capacity 36,353.
Suites pass default and forced.

**xt85 (a full W=12 batch 1, xt82's setup, with the fix).**

- At the end of tick 17, live is 11,989 MB, against 16,958 MB: −5.0 GB. Walked is 8,081 MB, against 13,066 MB.
- In tick 18 it passes op 1733, where xt82 failed (live there 20.8 GB, against 26.6 GB), and runs out of
  memory after op 1994/2222 (`ExchangeIndexOp`, 40.5 s, live 48.0 GB, pressure Critical). That is where the
  restored control xt83-ctl failed (46.7 GB). Wall 3,350 s, peak RSS 43 GiB, spill on disk 35 GB.

**What this means.** The live run and the restored run now hit the same wall, so the tick-17 restart point
stands in for tick 18 from here (~27 min to op 1994 instead of ~54). Op 1994 is live-bound: ~27 GB more live
appears during one exchange. Next: a census right after ops 1984, 1993 and 1994 in a restored run
(`XT_CENSUS_AT_OPS`) to see what holds it.

### 5.32 The fused exchange-and-index gathered managed (2026-11-07)

**Why.** Op 1994 is an `ExchangeIndexOp`: a W>1 shuffle fused with the re-index a join or aggregate reads.
At W=1 the same plan is a `GroupProject`, whose index is D4's spilling builder (`DeltaSpillGroups`, §5.22).
The compiler attaches the consumer's block format to the indexed stream either way, but `ExchangeIndexOp`
ignored it: it split a block-backed input by listing every row into managed buckets, then gathered its
column into a plain `IndexedZSetBuilder`. A skewed key puts most of a large delta on one worker, all managed.
§5.27 made the plain `ExchangeOp` spill-aware; its fused siblings were left (§5.27 "still open").

**Fix.**
- `ExchangeIndexOp` splits a block-backed input into one spilling builder per destination
  (`ExchangeBuckets.SplitSpilling`, now shared with `ExchangeOp`; each builder comes from the delta's own
  store, `NewBuilder`, so it needs no stream format), and gathers through `DeltaSpillGroups.For(output format)`.
  A spilled bucket (`ExchangeBuckets.SetBucket`) is enumerated as a set; `Items` reads only list buckets.
- `ExchangeIndexStepOp` (the W>1 `StepGroupProject`) gathers through the same builder. Its split stays
  managed: mapped rows have no block format.
- Not changed: `ExchangeIndexJoinOp` (its indexes are internal to the join) and `BroadcastExchangeOp`.

Test: `BlockedDeltaTests.An_exchange_index_keeps_a_block_backed_delta_as_blocks` (W=4 and 12, with and
without a pre-step): two block-backed shards all routed to worker 0, past a 100-row threshold, must come out
as a `BlockedGroups` delta equal to the managed index of their union. With the plain builder back, the
`ExchangeIndexOp` cases fail, and so do the `ExchangeIndexStepOp` cases.
Suites pass default, forced, and forced at Critical (3,535).

**The census that pointed here (xt86, restored, `XT_CENSUS_AT_OPS=1984,1993,1994`).** Right after op 1984
(also an `ExchangeIndexOp`, feeding an aggregate) live is 27.2 GB, but the state walk reaches 4.4 GB: ~22 GB
is in flight (deltas and exchange buckets), not operator state. (That run's forced census fired once per
replica, twelve walks under the hook's lock, and stalled op 1993 for ~20 min; the hook now fires once per op.)


**xt87 (restored after tick 17, with the fix).** Op 1994 takes 22.2 s (40.5 s in xt85) and no longer holds
the heap; tick 18 runs on to op 2064/2222, the next `ExchangeIndexOp` (63.5 s, live 26.4 GB), then out of
memory during op 2068 (an `ApplyOp` emitting ~1.5M-row chunks, heap 60–76 GB between collections, RSS at the
48 GiB cap). Wall 1,836 s; peak RSS 47 GiB. The harness logged only the outer stack; it now logs each distinct
inner stack (`INNER xN`).

**xt88 (the same restored run, inner stacks and a census after ops 2064 and 2067).** It gets further than
xt87: past op 2068 to op 2152 (`IntegrateOp`, live 43.3 GB), then out of memory in a later `ExchangeIndexOp`
while its split's spilling builder freezes a chunk (`SharedArrayPool.Rent` in `FreezeRows`, three workers),
the allocation that happened to fail, not the holder. The census is the finding: after op 2064, live is
28.8 GB and the walk reaches 5.9 GB; after op 2067, 27.8 GB against 6.1 GB. The walk covers operators and the runner
but, it turned out, not the streams' current deltas: its streams pass counted each `Stream` object and never
entered it. Nor closures (delegates) or statics. So ~22 GB is in-flight deltas, closures or statics.

- **xt89** (a whole-heap `dotnet-gcdump` after op 2064, `XT_GCDUMP_AT_OPS`, `XT_GCDUMP_EXIT=1`) failed in op
  2064 before the dump, again in the split's spilling builders. That was a flaw of this section's split: one
  builder per destination, each holding up to a whole step chunk (1M rows) managed before it freezes, so up to
  W chunks per worker where D2 promises one. Each split builder now freezes at its share
  (`ZSetExternalStore.NewBuilder(capacity, parts)`, `DeltaSpill.Rows(…, parts)`): test
  `BlockedDeltaTests.An_exchange_split_shares_one_step_chunk_across_its_destinations` (400 rows over four
  destinations under a 120-row threshold: every 100-row bucket must be block backed; without the share all four
  stay managed).
- **xt90** dumped after op 2016, but on .NET 10 `dotnet-gcdump` falls back to lossy buffering and stopped at
  10M objects (270 MB of a ~25 GB heap): no use at this size.
- The census then entered each stream's current delta (xt91, xt92): with hundreds of millions of rows in
  flight, its visited set ran out of memory itself, at 48 and at 56 GiB. It now walks closures (delegates'
  targets, `(closures)`) and the static fields of the engine's assemblies (`(statics)`), and *estimates*
  the streams instead (`StateCensus.Streams`): each distinct stream's current value, its entry count, whether
  it is block backed, and a deep size sampled over 256 entries and extrapolated.

### 5.33 Stored views kept each tick's delta managed (2026-11-07)

**xt93 (restored, census after ops 2016 and 2063).** Closures hold 7 MB and statics 1.1 GB (the block
cache's ring, which is accounted). The streams hold the rest: 26,892 streams, 162M entries after op 2063,
only 53 block backed. Every large one is a stored view's delta, in pairs: the `ApplyOp` that produces a
view's rows and the view's `IntegrateOp` (`IntegratedView`), with equal counts. The integrate op passes the
delta through, so each pair is one set counted twice; halved, the estimate is ~20 GB, the gap the census
could not account for. The largest are views finished long before op 2063 (op 1863 5.8M rows ~2.6 GB, op
2015 6.2M ~2.1 GB, op 1552 5.8M ~1.0 GB, then ops 1018, 732, 2038, 845…): each lives to the end of the tick
because the view handle pins its integrate op's input (§3b) for the sink, which reads `LastDelta` after the
step. The pin keeps a *managed* set: a view's producer is a pointwise `ApplyOp`, which never builds through
D2, so a view's whole tick of output stays in memory. xt93 itself went on to op 2174 of 2222 before running out
of memory (wall 3,688 s; at 56 GiB it had headroom the 48 GiB runs do not).

**Fix.** The integrate op keeps the delta itself instead of pinning the stream (the input's managed value is
released like any other). Past a threshold (below), with a blocked view, it re-stores the delta as block runs
(`DeltaSpill.Rows` over the view's format) before integrating; the view adopts the runs (§5.19), so the rows
are encoded once either way, and the kept delta is spillable. Below the threshold, and with the knob off, the
delta is the managed set it was. Test `BlockedDeltaTests.A_stored_views_kept_delta_is_block_backed_past_the_threshold`
(300 rows under a 100-row threshold must be kept block backed and read as the delta, the view as the sum; 50
rows and the knob off stay managed); without the re-store it fails.

**xt94 (restored, the fix at the step chunk's threshold).** Live after op 2064 fell only from 28.8 GB (xt88)
to 24.9 GB, and the run ran out of memory in op 2082 (`IncrementalLeftJoinOp`, a `HashSet` sized for its
delta). The threshold was wrong: the census sums twelve replicas, so op 1863's 5.8M rows are ~480k a worker,
under the 1M-row step chunk, and most views never re-stored. The step chunk bounds one builder's managed
peak; kept deltas add up over the tick, so their floor is now the Critical chunk (65,536 rows). The runner
logs `kept_spilled` (deltas re-stored, and their rows) on each `OPLIVE` line.

**xt96, xt97 (the 65,536-row floor).** 167 deltas (57.7M rows) re-stored and xt96 reached op 2146, but live
*rose* (op 1994: 25.3 GB against ~19–21 GB). xt97's census after op 1994 says why: every view stream is still
managed (`external=0/12`), and the integrate ops' own state is 13.6 GB. The re-store built through the
ordinary spilling builder, which freezes only a full step chunk, so a delta under 1M rows came back as a
managed *copy*: each kept delta was held twice until the original was released. The re-store now builds always
blocked (the remainder freezes on build). Test
`BlockedDeltaTests.A_kept_delta_under_one_step_chunk_is_still_block_backed` (65,546 rows under a 1M-row step
chunk must be kept block backed); with `alwaysBlocked: false` it fails, and the first test did not see it
because its rows were past its own chunk.

**xt98 (restored, kept deltas block backed).** 173 deltas (59.0M rows) re-stored. Live after op 1994 is 14.3 GB
(19–25 GB before), after op 2064 16.5 GB (25–29 GB). Tick 18 runs to op 2171 of 2222, the furthest under the
48 GiB cap, and runs out of memory there: six workers in `IncrementalLeftJoinOp.Step`, allocating the set of
touched keys (`HashSet(dl.GroupCount + dr.GroupCount)`). Live there is 24.3 GB but the heap before the
collection is 82.6 GB at Low pressure: what is left is allocation between collections, not held state.
Wall 2,781 s.

### 5.34 Outer joins' touched-key set, and a parallel view's first count (2026-11-07)

**The outer joins' touched keys.** `IncrementalLeftJoinOp` and `IncrementalFullJoinOp` collected every key
of both deltas into a `HashSet` sized for both before visiting them: xt98's six failing workers were
allocating it. They now visit the left delta's keys, then the right's not on the left
(`TouchedKeys.Of`): each key once, no set. Without the test (a key on both sides visited twice), 65 join
tests fail. The first version tested membership with `ContainsKey` and then probed the left group again:
on a block-backed left delta that doubled the probes, and op 1693 (a left join) took 81 s and 68 s in xt99
and xt100 against 34.6 s in xt98. `TouchedKeys.Of` now yields each key with its left group: a left key's
group as the enumeration gives it (no probe), a right key's one probe as both the test and the group.

**xt99 (restored, the above).** Tick 18 completes: op 2222 at 3,087 s, live 9.6 GB at the end of the tick,
peak RSS 47 GiB. Live after the busiest ops stays 16–27 GB; the heap between collections reaches 62–84 GB.
Then, still in batch 1, the run runs out of memory *after* the step: the engine's statistics
(`DbspNetEngine.OutputStats`) read each view's `RowCount`, and a parallel view's first count formed the whole
view (every shard's rows decoded into lists, then summed into one set) to count it. A count is kept current
from each tick's delta only once it has been read, and batch 1's ticks run under one wait, so a full run
reads it for the first time here too: this is the end of batch 1 at W > 1, not a restore artifact (W = 1
counts its one shard's trace in place, §5.24).

**Fix.** `BlockedIndexedTrace.CountRowsAcross(traces)` counts the rows whose weight summed over the shards
is non-zero, by one hash-ordered merge of every shard's runs (the shards are replicas of one operator, so
their runs share one layout and order), plus a dictionary of rows not yet frozen (which a freeze bounds).
A row split across workers counts once, a row that cancels across them not at all. `RowCount` uses it
whenever every shard is held as blocks. Test
`BlockedDeltaTests.A_count_across_shards_sums_each_rows_weight_without_forming_the_view` (three shards with
overlapping, cancelling and pending rows, against the sum's count); it fails if the pending rows are dropped,
and if each shard is streamed by itself (a split row counted twice).

**xt100 (restored, the count fixed, the doubled probe still in).** Out of memory at 635 s, just past op 1693,
in `ZSet.Plus` (a key's left group plus its delta, `IncrementalLeftJoinOp`), with 7.7 GB live but RSS 45 GiB:
the heap between collections swings up to ~44 GB at ops 1689–1696, and whether a run survives this stretch
varies (xt98 and xt99 did).

**xt101 (restored, one probe per key).** Op 1693 takes 43.9 s (xt98 34.6 s). Out of memory at 665 s, again just
past op 1693, in that left join's trace adopting its delta's runs (`MergeMany` loading lazily restored blocks).
Through ops 1650–1700 live stays 8–10 GB in all four runs; what differs is RSS at op 1693: 35–39 GiB in xt98
and xt99, which went on, and 42–45 GiB in xt100 and xt101, which did not. The failure here is the heap's swing
between collections, not held state, and it is not deterministic from run to run.

### 5.35 Ops 1680–1705: one join key per worker, decoded and encoded again at every step (2026-11-07)

**Lazy left groups (xt102).** The left join read each touched key's old left group from its trace and
formed the new one (`oldL + dlK`) before choosing a branch. On a coarse key that is millions of rows
decoded from blocks and copied again, and the common branch (still matched, no right-side change) uses
neither. Each branch now reads `oldL` only where it needs it (matched with a right delta, gained a match,
lost one). `IncrementalFullJoinOp` is unchanged: its case analysis needs the emptiness of both left
groups. Test `IncrementalLeftJoinTests.A_matched_key_with_no_right_change_does_not_copy_its_left_group`
(a 100k-row group, one new row: the step allocates under 8 B a row of the group; the eager version
allocated 3.46 MB and fails). Fourteen join tests fail if `oldL ⋈ drK` is skipped on the wrong condition.
xt102 (restored) **completed tick 18 and batch 1**: 3,246 s, live 9.2 GB at the end of the tick,
peak RSS 47 GiB. Op 1693 took 47 s.

**Per-op allocation.** The census harness now logs `OPALLOC` (each worker's allocated bytes per
operator, ≥ 512 MiB) and, with `XT_DESCRIBE_OPS=from-to`, `OPDESC` (each operator's streams and
delegates). Tick 18 allocates 7,138 GiB. Ops 1680–1700 come to ~560 GiB summed over workers. Their shape
repeats every ten ops: an exchange (1687, 1697) hashes on the next join's key, which has three values, so
three workers take 7.2M rows each and the other nine none. Two filters (1688, 1689) read the exchange's
output, a `GroupProject` (1691) indexes it, and a left join (1693, 1703) finds one key with 7.2M left rows
and one right row. The left delta is block backed (`BlockedGroups`). A temporary probe in the join gave
each phase's allocation on a hot worker:

| op 1693, one hot worker | xt103 | xt107 | xt108 |
|---|---|---|---|
| per-key loop (combine, output builder) | 19.5–20.7 GiB | same | same |
| output `Build` | 0.6 GiB | same | same |
| left trace `Integrate(dl)` | 16.6–17.8 GiB | 11.7–12.5 GiB | 5.2–5.5 GiB |
| whole op | 37.5–39.9 GiB | 31.8–33.8 GiB | 25.3–26.7 GiB |

**The integrate is a giant merge.** The trace adopts the delta's runs (§5.19), and with more than two
runs it merges them in one k-way pass (`MergeMany`), which for one key is `TryMergeGiant`. That pass
decoded every value twice: once to check that each run is in value-hash order, and once to merge by
value hash. Then it encoded every value again. Three changes:

1. The order check reads each entry's stored value hash (`ITraceBlock.ValueHash`), not its decoded row.
2. The merge copies entries whole. A new `ITraceBlockFormat.Copy` (`BlockWriter.Copy`, the key-only
   `Gather` of §3h generalised to every column) builds a block from (source block, index) pairs without
   decoding, carrying the value hashes over. `RunBuilder` gains a copy mode for one hash, so that hash's
   blocks hold only it, as before. A value is decoded only where entries from two runs share its value
   hash, to sum equal rows; the survivor is copied from one of its entries with the summed weight.
3. A delta's spilled blocks keep their value hashes. The bodies of a 7.2M-row delta are written to files
   and evicted long before the merge reads them, and a body reloaded from a file had no value hashes, so
   the first `ValueHash` decoded the whole block to compute them. `LazyTraceBlock.KeepValueHashes` holds
   them (4 B an entry) when the cache drops the body, for a block-backed delta's own runs only (a trace
   that does not compact; a delta dies with its tick). A giant group's chain, written to the file from
   its parts and never concatenated in memory, is seeded with the parts' hashes.

Change 1 alone moved nothing on xt105 (17.4 GiB): change 3 was the reason. Tests:

- `BlockedDeltaTests.A_giant_groups_merge_checks_value_order_without_decoding_values`: merging one key's
  60k values across six runs allocates 0.44× one decode of every value. It was 1.24× with only change 1,
  and 2.08× before; the test requires under 1×.
- `A_giant_group_merges_streaming_and_runs_stay_in_canonical_order` (the property test of §5.24) now runs
  the copying merge. It fails if tied entries are copied without summing, or if a sum's weight is dropped.
- `RunSpillerTests.A_spilled_deltas_blocks_keep_their_value_hashes_when_evicted`: every body is evicted
  as it is admitted; adopting the delta recomputes no value hash. With the kept hashes ignored, it
  recomputes 14.

The giant joins earlier in the tick gain too: a 2M-row giant integrate at t ≈ 51 s went from 8.9 GiB to
5.9 GiB. Those merge the adopted run into the trace's older run pairwise, and the trace's own runs
(which compact) keep no hashes, so their reloaded blocks are still hashed by decoding.

**The skewed exchange's gather.** Each source splits its block-backed share into one spilling builder
per destination (§5.32), so a hot worker receives twelve block-backed buckets. It then decoded every row
into one more builder and encoded it again: 13 GiB on each hot worker at op 1687. `ExchangeOp` now sums
block-backed buckets of its format as the union of their runs (`ExchangeBuckets.GatherBlocked`,
`BlockedRows.Union`), merged only when read, and adds any managed buckets through a builder. Op 1687 on a
hot worker went from 13 GiB to 3 GiB (most of that is now its own split). Test
`BlockedDeltaTests.An_exchange_gathers_spilled_buckets_without_decoding_them` (two overlapping block-backed
shards, with cancelling rows, and a managed one, at W = 4 and 12); it fails with the union disabled.

**What is left.** The join's per-key loop is ~2.8 KB a row: each left row is decoded, combined with the
right row into a new structural row, hashed into the output builder and encoded at its freeze. The
filters and projections in this stretch (1688, 1689, 1691, 1696) cost 10–13 GiB each on a hot worker for
the same reason. A filter over a block-backed delta could copy its surviving entries instead of building
them again (its input is consolidated, so its output needs no dictionary). That and the loop are general
structural-row costs, not specific to this stretch.

**xt109: the whole restored batch 1 on 8b2470c.** It completes again with all 21 views reported (wall
3,278 s against xt102's 3,246 s, inside noise; peak RSS 47 GiB both). Tick 18 allocates 6,712 GiB against
xt102's 7,138 GiB (−6%), and ops 1680–1705 together 581 GiB against 706 GiB (−18%). Op 1693 takes 38.8 s
against 47.2 s. The rest of the tick is unchanged, and the largest allocators are now at its end (ops
2153–2177: aggregates, a left join and an exchange-index of 80–385 GiB each), which this section did not
touch.

### 5.36 Ops 2153–2177: a rolling eligibility view, decoded and built again row by row (2026-11-08)

After §5.35, the largest allocators of the restored tick 18 (xt109) are at its end: ops 2153–2175 are
one view, `fact_engagement_status_channel_eligibility_28d_rolling_hist_12m` (ops 2176 on are its 7-day
twin). It reads `int_hist_unpivoted_28d`, a three-way `UNION ALL` of ~65M rows in tick 18. It aggregates
them twice (`COUNT(DISTINCT)` and two `SUM`s by eleven columns; `SUM`s over a `SELECT DISTINCT`), left-joins
the two on eleven columns (three by `COALESCE`, a residual), and keys each row by
`MD5(CONCAT_WS('-', COALESCE(TRY_CAST(c AS TEXT), ''), ...))` over eleven columns. There is no skew:
each worker holds ~2.4M rows. The cost is per row, ~36 KB per unpivoted row across the stretch, with
2154 (aggregate, 385 GiB), 2171 (left join, 359 GiB), 2153 (exchange-index, 252 GiB) and 2174 (the MD5
projection, 157 GiB) the largest.

**The instrument.** The census hook now prints each op's view (`OPDESC ... view=`; operators keep the
view they were built for) at the first step, so a restored run names a stretch in seconds.
`Scratch/RollingEligibilityRepro` compiles the two views as a W = 12 program over synthetic rows of the
same columns, with the xt run's spill settings (`XT_MICRO=1`, step spill 100k). It allocates ~39 KB per
unpivoted row, and its ops rank as production's; an allocation trace (EventPipe `AllocationTick` with
stacks) totalled by leaf frame apportions it. About 40% was `TraceBlock.Cell`: decoding cells into boxed
values and `Utf8String`s. By mechanism: trace merges decoding and encoding rows (~19% of the step),
operators decoding a spilled delta row by row (~19%), a spilled delta's groups resolved to name their
keys (~13%), probes of old groups (~8%).

**Each group read once.** The aggregate walked `delta.Keys`, which for a block-backed delta resolves
every group (values and all) to name its key, then probed `delta.GroupFor(key)`, decoding the group
again. It now enumerates (key, group) pairs. `TouchedKeys` (the left and full joins, §5.34) also
hands back each key's right group: probed for a left key, as enumerated for a right-only one (the join
probed the right delta again for it). Tests `An_aggregate_reads_a_block_backed_deltas_groups_once`,
`A_left_join_reads_a_block_backed_right_deltas_groups_once` (no probe of the delta;
`BlockedGroups.ThreadProbes`).

**Merges copy entries.** §5.35's copying merge was for a giant group only; every other hash was decoded,
consolidated and encoded again. `MergeHash` now copies (`BlockedIndexedTrace.CopyMerges`, on): a hash one
run holds is consolidated and canonical there, so its entries are copied as they are, no row read unless
a floor needs the key; entries of a hash several runs hold are decoded to sum equal (key, value) entries,
and each survivor is copied from its first entry with the summed weight, in canonical order. `RunBuilder`
cuts copied entries into blocks by the same rules as decoded ones (block entries at a hash change, byte
budget, spanning hashes), and keeps its mode across a flush. `BlockWriter.Copy` carries value hashes only
when every source holds them: a trace block reloaded from a file has none, and computing them would decode
the block. Tests `A_merge_copies_entries_rather_than_decoding_and_encoding_them` (30k keys over six runs:
0.28× the decoding merge's allocation), `A_copying_merge_sums_entries_several_runs_hold` (property test,
cancelling entries, canonical order); the floored path is covered by `BlockedTrace_BehavesLikeTheManagedTrace`.

**Surrogate keys in one buffer.** Each `CAST(x AS TEXT)` built a .NET string, its UTF-8 copy and a
`Utf8String`; `CONCAT_WS` took them in an `object[]` and copied them into its result, which `MD5` hashed.
A `CONCAT_WS` with a literal separator now compiles each argument to write its text into a per-thread
`Utf8TextBuffer` (`BuiltinScalarFunctions.FusedConcatWs`, on): a cast to text of an integer, float, date,
boolean or string formats into it as the cast formats (invariant culture; `yyyy-MM-dd`; `True`/`False`),
a failing `TRY_CAST` rolls back to NULL, a `COALESCE` writes its first non-NULL argument, anything else
is evaluated and copied. `MD5` of such a `CONCAT_WS` hashes the buffer, allocating only the digest.
Decimals and timestamps keep their cast and are copied. Tests `FusedConcatWsTests`: the same rows as the
runtime form over every argument shape and edge values (NaN, infinities, −0.0, extreme dates and
integers, empty and non-ASCII strings, nested `CONCAT_WS`, a column separator), a failing `TRY_CAST`,
and the eleven-column key at 0.08× the runtime form's allocation.

**In the stand-in** (W = 12, 2.85M unpivoted rows; per-op allocation):

| op | before | after |
|---|---|---|
| first aggregate (2154) | 17.6 GiB | 12.0 GiB |
| second aggregate (2162) | 8.8 GiB | 5.2 GiB |
| left join (2171) | 21.0 GiB | 16.1 GiB |
| MD5 projection (2174) | 10.2 GiB | 5.1 GiB |
| the two views' integrals | 5.7 GiB | 1.7 GiB |
| step (less the harness's own profile reads, ~20 GiB) | ~83 GiB | ~59 GiB (−29%) |

Each change was mutation-checked: its test fails with it undone.

**What is left** is the general structural-row cost: each operator decodes its spilled input row by row
into boxed cells and builds its output rows again (the exchanges' splits, the projections and filters, the
join's per-group lists and builders for groups of one or two rows), and a probe of an old group decodes it.

**The restored run on 9c98731 (xt111, xt112) did not complete.** Up to where it failed, the changes held:
op 2140 81.0 → 24.3 GiB, 2154 385 → 288 GiB, 2162 162 → 81 GiB, 2159 113 → 100 GiB (2153, 2157, 2158 are
the operators' own row-by-row decoding, unchanged). xt111 then ran out of memory at op 2170 (an exchange
index freezing a chunk), xt112 at op 1693 (the giant left join's integrate loading a spilled block). A
census hook now measures live bytes after chosen operators, once every worker has stepped them
(`XT_LIVE_AT_OPS`, a forced compacting GC; `XT_LIVE_EXIT`), and `DBSPNET_COPY_MERGES=off` turns the
copying merge off. With it off and on (xt114), and on the pre-§5.36 engine (2d50551, xt115), the restored
run holds 20–23 GB live after op 1683 and runs out of memory in op 1693 alike. So §5.36 did not cause
this: the giant joins at ops 1683–1703 are fragile at 48 GiB on the old engine too, and xt102 and xt109
completing was luck. (xt109's 8.9 GB at op 1693 was sampled when the first worker finished it, not the
last; the samples are not comparable to the fixed-point measure.) What holds the 20 GB at op 1683 is
the next question.

### 5.37 The giant joins' peak: a skewed key's left group, streamed (2026-11-08)

§5.36's restored runs fail at the giant left joins (ops 1683–1703) on the old engine as well. The
in-process census after op 1683 (§5.36 probes) settles where the memory is: operator state 7.1 GB and
stream deltas 3.1 GB, ~10 GB as before; but live once all twelve workers have stepped the next operator
is 18–23 GB. The difference is the working memory of operators still running on other workers, chiefly
the three hot workers' joins running at once. (A full `dotnet-dump` after op 1683, 42 GB, cannot be read
on macOS: ClrMD's Mach-O reader fails on it; nor can ClrMD snapshot its own process there.)

`Scratch/GiantJoinPeakProbe` measures one hot join: a W = 2 program, 2M left rows of one key and one
right row, with the xt run's spill settings (set through the environment: a test's `AsyncLocal`
overrides do not reach worker threads started before them). Sampling live with forced GCs every ~100 ms,
the hot worker's join rose ~2.7 GB above where it started; the left delta's group, resolved into a
managed Z-set as `TouchedKeys` enumerates it, is 2.1 GB of that (1,130 B a row, measured alone). At 7.2M
rows a hot worker held ~8 GB of it, three workers at once.

**The change.** Enumerating a block-backed delta (`BlockedGroups`) hands out a giant group streamed
(`StreamedGroup`; `DBSPNET_STREAM_GIANT_GROUPS=off` to A/B): a hash whose entries, counted from block
metadata alone (the blocks it fills whole), reach `GiantGroupEntries`. Every run holds the hash as one
key, its values in ascending stored value hash (as freezes and merges write them), so a k-way walk by
value hash yields the group's rows in order, decoding only the entries it yields and summing entries of
equal value hash (decoded) where several share one. The exact count is one such walk at creation that
decodes only those ties; the walk also checks the key and the order, and a group that fails is resolved
as before. A point lookup (the residual join's `WeightOf`) materializes the group once. The join's
common branches only enumerate it.

In the probe, the hot join's peak live went from 3.91 to 1.93 GiB (its rise from ~2.7 to ~0.7 GB).

Tests: `A_streamed_giant_group_reads_as_the_managed_one` (property test over skewed adds in 4-entry
blocks: rows, count, lookups, sums, negation), `A_streamed_giant_group_decodes_none_of_its_rows_until_read` (60k rows, this thread's allocation: 2 KB against 15.7 MB; was
8.9 KB held against 30.8 MB materialized by GC.GetTotalMemory, which other tests disturb), `A_left_join_over_a_streamed_giant_group_matches_its_twin`
(W = 2, plain and residual: unmatched, gaining a match, cancellations, losing it, against an unspilled
twin), `A_giant_group_out_of_value_order_is_resolved_not_streamed` (an adopted run in descending value
hash). Mutants caught: ties not summed, lookups wrong, count off by one, streaming off, order check off.

**xt118 (296f18a, restored from `ckpt-xt83-save`, W = 12, 48 GiB heap): batch 1 completes.** Tick 18
allocates 6,087 GiB against xt109's 6,712 (−9.3%), most of it §5.36's cuts now measured whole: ops
2135–2190 2,559 → 2,111 GiB (op 2171, the eligibility left join, 359 → 288; op 2174 157 → 80; op 2177
96 → 68; op 2162 162 → 98). Wall 3,328 s against 3,279 (noise). Ops 1680–1705 allocate as before (577
against 581 GiB): streaming the giant group saves its ~1.1 KB/row materialization in flight, not much
allocation. Live sampled *between* ops is about unchanged (21.6, 19.1, 19.0 GB after ops 1684, 1694,
1704, against 18–23 GB in xt114/xt115), as expected: the group is transient within the join's step, so
the probe cannot see the peak it removes. What changed is that the three hot workers' joins no longer
each hold a materialized 7.2M-row group at once (~8 GB each), and the run passed op 1693, where
xt114/xt115 ran out of memory. One completion is not proof against a nondeterministic failure (xt99 and
xt109 completed before it too), but the margin it removes is the one measured. End-of-batch live 11.7 GB
(xt109 10.5).

**xt119 (296f18a, full batch 1 from scratch, W = 12, 48 GiB heap): out of memory late in tick 18.**
It passed everything that had stopped full runs before: the giant joins (ops 1683–1703), op 1994 (xt85's
wall) and ops 2153–2170, and it allocated like xt118 up to there (ops 1680–1705 577 GiB, op 2171 261 GiB
against xt109's 359). It failed at t = 3,732 s in op 2171, the eligibility left join
(`ResidualOuterJoin.Pairs` → `ZSetBuilder.Add` → `SpillingRows.FreezeChunk` → `BlockedIndexedTrace.FreezeRows`),
on the one worker of twelve that had not finished it. That op is not skewed: every worker emits ~2.39M rows
and allocates ~24 GiB in it. The full run carries more between ops than the restored one (live 29.8–33.4 GB
after ops 2168–2170, against 26.2–29.9 GB in xt118; cf. §5.30), so twelve workers' concurrent working memory
in op 2171, including each worker's managed output chunk (up to `DBSPNET_STEP_SPILL_ROWS` = 1M rows,
held while it freezes), no longer fits. Wall to the failure 3,733 s, peak RSS 46 GiB, spill 65 GB.

**The step-spill chunk, A/B (xt120, xt121; restored, same binaries, `XT_GC_TIMELINE=30000`).** A new
census hook, `XT_GC_TIMELINE=MIB`, logs every collection that leaves at least MIB in use (heap less
fragmentation, the pressure monitor's measure), and every gen-2 one, as a `GCUSED` line.

| | xt120: 1M rows (default) | xt121: 262,144 rows |
|---|---|---|
| Batch 1 | out of memory, op 2170, t ≈ 2,170 s | completes, 2,276 s (xt118: 3,326 s) |
| Most in use after a full GC | 34.3 GB | 30.7 GB |
| Most in use after any GC | 48.1 GB (t = 1,741 s, Critical) | 45.4 GB (t = 225 s, High) |
| Collections leaving ≥ 30 GB | 660, 190 of them gen 1 | 406, 30 of them gen 1 |
| Tick 18 allocation | — | 7,139 GiB (xt118: 6,087) |
| Peak RSS | 47 GiB | 44 GiB |

xt120 failed where xt118 (same settings) passed, in an exchange-index's chunk freeze
(`SpillingGroups.FreezeChunk` → `BlockedIndexedTrace.Freeze` → `ArrayPool.Rent`). Live after full GCs
was 24–26 GB just before it, and the pressure monitor read Low: between full collections each gen-1 GC
promoted 4–6 GB of chunk-lived data into gen 2, and the heap filled in seconds, faster than a level
measured once per collection moves. A quarter-size chunk makes far fewer such collections, and the run
completed 31% faster than xt118 (the gen-2 work saved), but it allocates 17% more: more, smaller runs per
delta, so readers decode and sum more entries that several runs share (ops reading those deltas, e.g. the
left joins 2012, 2082 and 2194, 10–14 → 52–76 GiB). One run each, under a failure that is nondeterministic.

**xt122 (full batch 1 from scratch, `DBSPNET_STEP_SPILL_ROWS=262144`): out of memory at t = 1,814 s, op
~1724 of tick 18,** with little live. The throw came from `LazyTraceBlock.Load`, reading a block for a
streamed giant group's walk (`StreamedGroup.TryCreate` → `TouchedKeys.Of` → `IncrementalLeftJoinOp.Step`;
ops 1719–1721 carry 21.7M rows a worker). For its last ~10 s the GC ran a full collection about every
second, each leaving ~12.5 GB in use (the most after any full collection in the run was 30.5 GB, at t =
1,462 s). A failure with three quarters of the 48 GiB limit not holding live data is not working memory:
it is committed memory the heap cannot use (fragmentation, free regions kept committed, pinned blocks).
The census line did not record which; `GCUSED` now adds committed bytes, fragmentation by generation
(gen 2, LOH, POH), LOH and POH sizes, the pinned-object count and whether the collection compacted.

**xt123 (xt122 again, with the wider `GCUSED` line): out of memory at t = 1,624 s, op ~1696, the same
throw** (`LazyTraceBlock.Load` ← `StreamedGroup.TryCreate`'s counting walk ← `TouchedKeys.Of` ← a left join).
The last collections before it: 14–18 GB in use, 39–40 GB committed (limit 48 GiB), fragmentation ~0, LOH
under 1 GB, nothing pinned, every collection compacting. So it is not fragmentation or a full heap. The
failing allocation is the block's message buffer (`new byte[MessageLength]`, inlined into `Load`); an
out-of-memory there now throws `InsufficientMemoryException` naming the block's size, entries, groups,
codec and file, and the heap's state, so the next failure says whether one block is outsized.

**xt124 (xt123's settings and binaries plus that message): batch 1 completes and validates.** The driver
reports `status: completed` (DuckDB replay and full recomputation, `EXCEPT ALL`) after 3,492 s (xt70, W = 1:
7,783 s), every model `success`. Tick 18 (t ≈ 1,300–2,900 s) allocates 7,125 GiB (restored xt121: 7,139);
live after it 17.1 GB; spill disk peak 72.7 GB. The most in use after any collection was 44.9 GB, and
committed peaked at 48,871 MiB, 281 MiB under the limit; the most after a full collection 31.9 GB (with
7.8 GB fragmentation, 4.2 GB of it gen 2). It passed ops 1696–1724, where xt122 and xt123 failed, so that
failure is nondeterministic too: **one full run of three at 262,144 rows validated, and the margin is a
few hundred MiB.** Open: what `TryCreate`'s block read failed on with ~9 GB of commit headroom.

**xt125, xt126 (xt124's settings, run until one fails).** xt125 completes and validates (3,521 s, no
out-of-memory). xt126 passed ops 1696–1724 and reached op 2180 at t = 2,922 s with no out-of-memory, when
the host stopped it for low system memory (not a failure of the run). Full runs at 262,144 rows so far: two
validate (xt124, xt125), two fail in `StreamedGroup.TryCreate`'s block read (xt122, xt123), one stopped
past that point (xt126). The named-block message has not fired yet.

**xt127–xt129 (xt124's settings, run outside the Claude Code host so its memory reaper cannot stop them):
all three complete and validate** (3,560, 3,469 and 3,496 s; peak RSS 44–47 GiB). Tally of full batch-1 runs
at 262,144-row step chunks: **five validate (xt124, xt125, xt127, xt128, xt129), two fail in
`StreamedGroup.TryCreate`'s block read (xt122, xt123), one was stopped past that point by the host (xt126).**
Committed memory peaks at exactly 48,871 MiB in every one of them, so that figure is where the GC holds
the heap at its hard limit, not a margin; xt124's "281 MiB under the limit" above measured nothing. The
two failures were the two runs before `LazyTraceBlock.Load`'s out-of-memory handler went in (it moves the
load body into `LoadCore`, behind a try/catch); every run since has passed. Five straight passes at a 5-in-7
rate has a probability of about 0.19, so that is not evidence the change matters, and the named-block
message has still never fired. Open: the cause of those two failures.

### 5.38 The step chunk capped by bytes (2026-11-09)

At 262,144 rows a step chunk keeps wide rows' joins inside the heap (§5.37: five of seven full runs
validate, against none of the 1M-row full runs that reached tick 18's late joins), but every delta pays for
it, narrow ones included: more, smaller runs, more entries that several runs share to decode and sum,
+17% allocation in the restored tick 18. What matters is the chunk's bytes, not its rows.

`DBSPNET_STEP_SPILL_MIB` (off by default until measured) caps the in-step chunk of both delta builders
(`SpillingRows`, `SpillingGroups`) by estimated managed bytes, beside `DBSPNET_STEP_SPILL_ROWS`, whichever
comes first. `ChunkBudget` sizes every 256th entry added (`ManagedSize.Row` of the row, and of the group key
for an indexed builder, plus 48 B of map entry) and sets the chunk's entry limit to the cap over the running
mean, never above the row cap. A builder filled as one of several side by side gets its share of the cap, as
of the rows. The forced test mode (rows 0) ignores it. The census tick line counts chunks the byte cap froze
(`byte_freezes`, `byte_freeze_mrows`).

Tests: wide rows freeze under the byte cap where narrow ones stay managed (and both read as their managed
twins), the row cap still bounds a chunk under a huge byte cap, an indexed builder freezes wide groups by
bytes, and the setting parses. Mutants caught: the limit never lowered, no row ceiling, groups ignoring the
budget. Suites: default, forced and forced+critical, 3,569 each.

**Measured (xt130–xt133, `DBSPNET_STEP_SPILL_ROWS=1000000`, `DBSPNET_STEP_SPILL_MIB=256`, 48 GiB heap).**
The restored run xt130 completed batch 1 in 2,173 s. Tick 18 allocated 7,131.7 GiB, the same as xt121 at
262,144 rows (7,139) and 17% above xt118 at 1M rows (6,087). So the cap did not give narrow deltas their
large chunks back for free: the byte cap froze 34,466 chunks (2,815M rows, ~82k rows a chunk), so 256 MiB is
tighter than 262,144 rows for most deltas, not looser. The +17% is not narrow deltas paying for wide ones'
chunk count; it is the cost of a delta being block-backed at all, its readers decoding runs instead of
walking a managed map. The GC timeline (xt130, gen-1 collections at ≥30,000 MiB): used peaked at 45.9 GB,
at most 29.2 GB after a full collection.

Full runs: xt131 and xt132 completed and validated against DuckDB (3,415 s and 3,395 s; cumulative
allocation 11,006 and 10,991 GiB; 43,624 byte freezes each). xt133 failed at 1,622 s, in tick 18 past
op 1696 (the giant joins, ops 1683–1703), the same place and stack as xt122/xt123:
`IncrementalLeftJoinOp` → `TouchedKeys.Of` → `StreamedGroup.TryCreate` → `LazyTraceBlock.Load`. The
named-block message fired for the first time: *loading a 61,617,408-byte block (18,161,356 stored,
239,460 entries, 1 groups, codec Zstd); heap 17,517 MiB in use, 48,850 MiB committed, limit 49,152 MiB*.
The last census line before it read live 14.8 GB, heap before the collection 43.7 GB, pressure Low.
So the block is ordinary (one 59 MiB group), and the heap at the last collection held 17.5 GB of live data
inside 48.9 GB committed: the 59 MiB large-object allocation failed with ~30 GB of the committed heap
collectable garbage. The failure is the GC, at its hard limit, not making room for a large object while
twelve workers allocate, not live state.

Tally: 2 of 3 full runs validate at 1M + 256 MiB, 5 of 7 at 262,144 rows; every failure is this one.
The byte cap matches 262,144 rows on speed and reliability but does not recover the allocation; it stays
off by default. Next: (1) on an out-of-memory in a block load, collect (blocking, compacting, the large
object heap included) and retry once, which both tests the diagnosis and would turn these failures into a
pause; (2) cut the cost of reading block-backed deltas, which is where the 17% went.

**Correction, and what the GC timeline shows.** The runtime already makes a full compacting collection
(large object heap included) before it throws `OutOfMemoryException`, so collect-and-retry is dropped as a
fix. Native memory is not the cause either: `GCHeapHardLimit` counts only the managed heap's commit, native
allocations do not stop compaction (only pinning does, and the timeline shows ≤1 pinned object), and the
process allocates almost nothing natively (ZstdSharp is a managed port). xt133's last collections before the
failure at ~1,621 s: gen 0/1 every ~0.8 s promoting 0.7–3.5 GB; between gen 2s the large object heap grows to
6.4 GB, almost all garbage (gc 2222, gen 2 at 1,616 s, takes it to 0.67 GB); committed sits at 47.9–48.2 GB
with 21–24 GB in use before that gen 2, and even right after it holds 42.0 GB committed for 14.8 GB in use.
The last logged collection (gc 2228, gen 1, 1,620.3 s) left 18.3 GB in use and 41.3 GB committed; the
exception reports 17.5 GB in use and 48.9 GB committed, so committed rose 7.5 GB within about a second while
in use fell. The GC keeps ~25 GB of committed-but-free space (gen 0 budgets across 14 server heaps, free
regions not yet decommitted), and a block message of 59 MiB is larger than a large-object region (32 MiB),
so it needs a fresh region of its own and fresh commit at the limit. That, not live state, is the leading
explanation; it is unverified. A standalone repro (server GC, hard limit 8 and 24 GiB, 12 threads, 59 MiB
arrays into a 1 GiB queue, 2–6 GB live, chunk-lived garbage) did not fail in any shape tried: its live set
was a large share of the limit, so every collection was gen 2. The block-load message now records the last
collection's index, generation, compaction and pinning and the last full blocking collection's (90530f1),
so the next failure says whether the numbers follow a full compacting collection. Candidate fix, needing no
GC setting: keep every block-read allocation under the large-object region size (read and decode a big
block in pieces, or bound a block's message size when it is written, giant groups included).

**The fix: file blocks of at most 16 MiB.** The failing block was a giant group's in-memory blocks
(60,000 bytes each) concatenated into one file block, which `RunFile.MaxSpanBlockBytes` allowed up to
256 MiB. A block is adopted zero-copy over one `byte[]`, so reading it in pieces would change `TraceBlock`;
cutting it smaller when written needs nothing new, since a giant's hash already continues from file block to
file block (§5.18) and every reader follows it (the suite has passed with the cap at 8 KB). The default is
now 16 MiB, half a large-object region: the 59 MiB block becomes four. This also fixes the problem if the
diagnosis is only partly right, since no block load asks for more than 16 MiB at once. Not covered: the
checkpoint compactor (`RunCompactor`, managed traces' checkpoints, not the in-step path) still cuts a giant
by entries (2^18) only. The block-load message stays, so a failure that recurs says which collection
preceded it.
Suites: default and forced+critical pass (3,569); forced had one failure, the timing-bound
`Read_RunsTheWorkersConcurrently_OverASynchronousFileSystem` rendezvous, which passed 5 of 5 alone.

**Measured with the cap (xt134–xt137, full batch 1, 262,144-row chunks, 48 GiB heap).** All four completed
and validated against DuckDB, with no out-of-memory error:

| runs | validated | wall (s) | cumulative alloc (GiB) | spill disk peak (GB) |
|---|---|---|---|---|
| xt124–xt129, 256 MiB cap | 5 of 7 (xt122/xt123 failed) | 3,469–3,560, mean 3,508 | 11,867–11,953 | 72.4–73.0 |
| xt134–xt137, 16 MiB cap | 4 of 4 | 3,430–3,554, mean 3,480 | 11,894–11,943 | 72.2–72.9 |

The cap costs nothing measurable. The GC behaves as before: committed still peaks at 48,871 MiB in every
run, in use at 46.0–46.8 GB, the large object heap at 10.6–11.3 GB between gen 2s, ~400 gen 2 collections a
run. So the fix removed the allocation that failed, not the pressure. Four clean runs are encouraging, not
proof: before the cap, 3 of 10 full runs at these two settings failed (xt122, xt123, xt133), and at that
rate four in a row come out clean about a quarter of the time by chance. The new message has not fired, so
the diagnosis (a fresh large region needing commit at the limit) is still unconfirmed, but no block load
now asks for more than 16 MiB.

### 5.39 Where block-backed deltas' extra allocation goes (2026-11-09)

**The question.** At 262,144-row step chunks the restored tick 18 allocates 7,139 GiB (xt121), against
6,087 GiB at 1M rows (xt118): +17%. The 256 MiB byte cap gives the same (xt130, §5.38), so the cost is a
delta being block-backed at all, not how many chunks it has.

**What the census already shows.** Per operator (OPALLOC, tick 18, summed over workers) the extra is spread
over every operator type that reads deltas, 15–30% each, not concentrated in a few:

| type | ops | xt118 GiB | xt121 GiB | Δ |
|---|---|---|---|---|
| `ApplyOp` (map/filter) | 165 | 2,229 | 2,621 | +393 |
| `IncrementalLeftJoinOp` | 39 | 1,194 | 1,539 | +344 |
| `ExchangeIndexOp` | 18 | 685 | 867 | +182 |
| `IncrementalAggregateOp` | 13 | 711 | 822 | +111 |
| `ExchangeOp` | 43 | 563 | 637 | +74 |

The census counters: xt121 built 7,705 block-backed deltas (3,050M rows) against xt118's 3,630 (2,399M),
so ~4,075 more deltas and ~650M more rows went to blocks. 1,052 GiB over 650M rows is ~1,700 bytes per
extra block-backed row, several times one row's decode. A block-backed delta decodes every row into a new
`StructuralRow` (strings copied, so a kept row does not pin a block) on every enumeration, where a managed
delta hands out the rows it holds; each one also pays for encoding when its chunk freezes. Chains of maps
pay both per operator. Which of these dominates is not known yet.

**Instrument.** `DBSPNET_STEP_SPILL_PROFILE=1` (§5.16's `StepSpillProfile`) now also charges each phase to
the operator running it (the circuit sets `StepSpillProfile.CurrentOp` while profiling), and the xt census
prints a `STEPPROF` line per tick and a `STEPOP` line per operator whose phases allocated ≥1 GiB: GiB and
calls for chunk add, freeze, count, enumerate, probe, plus, copy, spill and run merge, and rows decoded.
Next: two restored runs with the profile, at 262,144 and 1M rows (`~/xtpcds/scripts/profile.sh`).

**Measured (xt138 at 262,144 rows, xt139 at 1M; restored tick 18, profile on).** Profiling costs no
allocation: tick 18 allocates 7,133.5 GiB in xt138 (xt121: 7,139) and 6,076.4 in xt139 (xt118: 6,087).
The gap, 1,057 GiB, by phase (GiB, thread allocation, exclusive of nested phases):

| phase | xt139 (1M) | xt138 (262,144) | Δ |
|---|---|---|---|
| enumerate (decode rows for readers) | 2,376 | 2,844 | **+468** |
| run merge | 721 | 857 | +135 |
| chunk freeze (sort, encode) | 609 | 741 | +131 |
| probe (weight lookups across runs) | 186 | 284 | +97 |
| build count (hash walk, tied hashes decoded) | 275 | 339 | +64 |
| chunk add | 93 | 101 | +8 |
| all phases | 4,262 | 5,167 | +905 |

The other ~150 GiB lies outside the phases (consumers handling the decoded rows). Rows decoded:
2,748M → 3,554M (+806M); enumerations 2,288 → 5,294. By operator type the gap is `ApplyOp` +320 GiB
(+210 of it decode), `IncrementalLeftJoinOp` +213 (probes across 4x the runs +98, giant merges +70),
`ExchangeIndexOp` +143 (+100 decode: it reads its input twice, decoding 2x the rows it adds),
`ExchangeOp` +74, `IncrementalAggregateOp` +58 (merges +52; also reads its input twice).

So the +17% is mid-sized deltas (262,144 to 1M rows) going to blocks: each reader then decodes every row,
~600–1,000 B a row for TPC-DS's wide rows (an `object[]`, every numeric cell boxed, every string a new
`Utf8String` over a copied array), and each pays an encode, more runs to probe and merge. Not a defect
in one place.

**And it is the faster setting.** xt138 took 2,536 s, xt139 3,268 s: the 1M-row chunks spend 10,673 s of
thread time freezing against 3,394 s at 262,144 (bigger sorts, large-object builder arrays), and their
wall is worse though they allocate less. Allocation here does not track time, so the +17% alone is not
worth a fix that keeps mid-sized deltas managed.

**Where the prize is.** Decoding for readers is the largest single cost in both arms: 2,376–2,844 GiB,
33–40% of tick 18's allocation, `ApplyOp` 1,765 GiB of it. Candidates, cheapest first:
1. Read once: `ExchangeIndexOp` and `IncrementalAggregateOp` enumerate a block-backed input twice. One
   pass, or decode once into the first pass's output, saves ~455 + ~180 GiB of decode at 262,144 rows.
2. Decode only the columns a projection reads. Plan-level column liveness exists
   (`PlanColumnLiveness`); a map over a block-backed delta could ask the reader for those columns. Worth it
   only where projections narrow wide rows; a filter keeping the row needs every column. Measure the share
   first.
3. Cheaper strings: a block's string heap copied once per block and the rows' strings aliasing that copy,
   not one array per string. A kept row then pins its block's string bytes (tens of KB), not just its own.
