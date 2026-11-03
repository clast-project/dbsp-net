# Bounded memory: keeping cold blocks on disk

Status: **phase 1 (census) done 2026-11-03; design proposed, not built.** Follows
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
4. **Batch 1's checkpoint reads 2.0 GB of blocks** (views 583, window 838, joins 295, aggregates
   220, DISTINCT 63). Batch 2's and 3's checkpoints read 0.6–0.8 MB. The likely sources are
   one-time work in the first checkpoint: the window write-back flush, dropping aggregate caches
   and refolding them, and the view or output-index setup. This is not explained yet, and a
   bounded cache must not let it refault everything (§3.3).

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

- **Batch 1's checkpoint read sweep (2 GB).** Explain it first (§1.2 item 4). Anything that has
  to read everything should read through the cache without promoting into it, as a scan, or
  should not happen at all.
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
