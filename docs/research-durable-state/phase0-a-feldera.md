# Phase 0-A: how Feldera does checkpoints and recovery (extends docs/research-feldera/persistence.md)

Checkout: `~/src/feldera` @ `78afc9077`. That is the same commit the existing notes cite, so their line
numbers still apply. All paths below are relative to `~/src/feldera`. Nothing was built or run.
Tags: **[verified]** means I read the code; **[inference]** means I reasoned from code I read.

This brief does not repeat what `persistence.md` already gets right. Where that file is **wrong or
incomplete**, the point is marked **CORRECTION** or **EXTENSION**.

---

## 1. On-disk batch format, when a batch becomes a file, where its bytes live

### 1.1 What a layer file holds [verified]

- **Logical shape.** A file has n columns, each holding (key, aux) items. Column i's rows point to
  row groups in column i+1. So a 1-column file is like `BTreeSet<K>` (a Z-set: key plus weight as
  aux), and a 2-column file is like `BTreeMap<K, BTreeSet<V>>` (an indexed Z-set). Files are
  written once and are immutable. Serialization uses rkyv (`crates/dbsp/src/storage/file.rs:1-68`).
- **Physical layout.** The file is a sequence of blocks. Each block is a multiple of 512 bytes and
  starts with an 8-byte header: a crc32c checksum plus a magic number. The last block is a
  **512-byte `FileTrailer`** (`storage/file/format.rs:1-77`, magics at `:106-118`).
- **The B-tree.** Each column has one B-tree. Leaves are **data blocks** (`LFDB`): rkyv items, a
  value map of offsets, and row-group pointers into the next column. Interior nodes are **index
  blocks** (`LFIB`): each child's (min, max) bounds, cumulative row totals, child offsets and child
  sizes in 512-byte units (`format.rs:15-61`).
- **Compression.** Each non-trailer block may be compressed (`format.rs:63-77`). The default is
  **Snappy**. Data and index blocks are at least 8 KiB, with a minimum branching factor of 32
  (`storage/file/writer.rs:168-179`).
- **The trailer** holds: version (currently 5, minimum readable 5), compression, the per-column root
  `(node_offset, node_size, node_type, n_rows)`, the filter block location (32- and 64-bit forms),
  compatible and incompatible feature bitmaps, and batch metadata such as stats and the negative
  weight count (`format.rs:86-103, 202-272, 336-353`). Format compatibility is checked with golden
  files in crate `storage-test-compat` (`format.rs:97-100`).
- **Membership filter.** There is one filter block per batch. The default is a Bloom filter
  (`LFFB`) at FPR 1e-4, which costs **19.2 bits/key**. An exact roaring bitmap (`LFFR`) is used when
  the keys' span fits in u32 (`file.rs:38-42, 119-124`; cost table at
  `crates/feldera-types/src/config.rs:370-384`).
- **File naming.** Files are named `w<worker>-<unique>.feldera` and live at the **storage root**,
  not inside a checkpoint directory (`writer.rs:1160-1166`). A file is written as `*.mut`, then
  `fsync`ed, then **renamed** to drop the suffix (`storage/backend/posixio_impl.rs:272-301`).

### 1.2 When a batch becomes a file [verified, extends notes]

The thresholds table in the existing notes is correct (`circuit/runtime.rs:1155-1245`;
`trace/ord/fallback/utils.rs:41-113`). In summary:

| batch | low memory pressure | moderate | high | critical |
|---|---|---|---|---|
| **merge output** | file if ≥ `min_storage_bytes` (10 MiB) | always file | always file | always file |
| **inserted batch** | stays in RAM | file if ≥ 10 MiB | always file | always file |
| **transient step batch** | stays in RAM | stays in RAM | stays in RAM | always file |

**EXTENSION.** A merge's output location is chosen from the summed approximate size of its
*inputs* (`utils.rs:86-113`). So when two small *file-backed* batches merge into something under
10 MiB, the result goes back to **RAM**. The next checkpoint then has to persist it again. See §2.3.

**EXTENSION, a cost the notes miss: every layer file is fsynced, including spill temporaries.**

- `PosixWriter::complete` always calls `sync_all()` and then renames the file
  (`posixio_impl.rs:272-301`).
- `BlockWriter::complete` then calls `reader.commit()`, which is a **second** `sync_all()`
  (`writer.rs:1048-1052`; `posixio_impl.rs:125-131`).

So each batch file costs two fsyncs when it is created. That includes memory-pressure spills that
will never be part of any checkpoint.

### 1.3 Where a file's bytes live afterwards [verified]: two cache tiers

1. **OS page cache, the default primary tier.** `StorageCacheConfig::PageCache` is the default
   because it "currently performs better". `FelderaCache` is still "under development" and is the
   only mode that opens files with `O_DIRECT` (Linux only) (`feldera-types/src/config.rs:292-320`;
   `storage/backend.rs:45-57`). Reads are plain `pread` into a fresh `FBuf` (`posixio_impl.rs:137-190`).
2. **Feldera's in-process buffer cache.** It holds **parsed** `DataBlock`, `IndexBlock` and
   `FileTrailer` objects as `Arc<dyn CacheEntry>`, keyed by `(FileId, offset)`
   (`storage/buffer_cache/cache.rs:25-55`; inserts at `storage/file/reader.rs:557, 1040, 1354`).
   - **Write-through.** The writer inserts each block into this cache as it writes it, already
     parsed (`writer.rs:325-380` via `from_raw_with_cache`).
   - **Eviction.** A file's entries are dropped when its last `ImmutableFileRef` drops, but only if
     the dev-tweak `eager_evict` is set (`reader.rs:1422-1430`; `cache.rs:95-110`). Otherwise they
     age out.
   - **Sizing.** `cache_mib` is total MiB spread over all threads. When unset, the default is
     **256 MiB × workers × 2 thread types (foreground and background)**
     (`circuit/runtime.rs:451-462`; doc at `config.rs:360-366`).
   - **Policy.** The default is a sharded **S3-FIFO** cache (`quick_cache`); a weighted LRU is the
     alternative. By default one cache is shared by each foreground/background worker pair
     (`config/dev_tweaks.rs:353-378`; builder in `crates/buffer-cache/`).

   So with default settings, bytes are cached twice: raw and compressed in the page cache, and
   decoded in the buffer cache.
3. **Always-resident per-file state on the heap.** Each file batch keeps three things in RAM for
   its lifetime: the deserialized **filter** (Bloom or roaring), the **key bounds** (min and max
   key), and the trailer metadata (`trace/ord/file/wset_batch.rs:486-499` → `BatchFilters::from_file`).
   This is the only per-key RAM cost of a file batch.

---

## 2. How a checkpoint is taken

### 2.1 Is the circuit paused? Yes, at a step boundary, and only for the "prepare" phase [verified]

- **Workers act between steps.** `CheckpointBuilder::prepare` broadcasts `Command::Checkpoint(dir)`
  to every worker thread and waits for all of them to reply (`circuit/dbsp_handle.rs:2630-2676`;
  `broadcast_command`, `:1510-1560`). Each worker handles the command in the same loop that handles
  `Step` (`dbsp_handle.rs:861-868`), so it runs between steps.
- **What each worker does.** It walks every node, recursively, and calls `node.checkpoint(base, files)`
  (`circuit/circuit_builder.rs:7819-7864`). Workers do this in parallel, each on its own thread.
- **The controller side.** `RunningCheckpoint::start` captures connector configs and the input
  `seek` metadata, calls `prepare()`, and records a span named *"Time during which checkpointing
  blocked pipeline execution"*. It then hands a `CheckpointCommitter` to a `feldera-checkpoint`
  thread (`crates/adapters/src/controller.rs:9443-9660`).
- **Preconditions** (`controller.rs:8782-8828`). A checkpoint is refused or delayed unless all of
  these hold:
  - storage is configured;
  - every input endpoint supports fault tolerance;
  - no replay or bootstrap is running;
  - **no transaction is open**;
  - **no input endpoint is at a `Resume::Barrier`**;
  - no multihost coordination hold is active.

### 2.2 What is written at checkpoint time [verified, refines notes]

The checkpoint is a new directory named `<uuid-v7>/` (`dbsp_handle.rs:2652`). Each worker writes
into that one shared directory, using file names that include its persistent id, which carries the
worker index.

- **Each `Spine` (`trace/spine_async.rs:2199-2266`).**
  - `pause_new_merges()` splits the batches into `not_merging` and `merging` (`spine_async.rs:932-944`).
  - Every in-RAM batch in either set is copied to a new file via `Batch::persisted()`.
  - `not_merging` is resumed into the merger with the file-backed copies.
  - `merging` is **not** touched: the merge already running keeps going on the in-RAM originals.
  - It then writes `pspine-<pid>.dat`, an rkyv `CommittedSpine`, and `pspine-batches-<pid>.dat`,
    a JSON list of paths.
- **CORRECTION: `CommittedSpine` holds only `{batches, merged (always empty), effort (always 0),
  dirty}`** (`trace.rs:133-141`; `spine_async.rs:137-146`). It does **not** hold the key or value
  filters. `restore` explicitly clears both filters, in the spine and in the merger
  (`spine_async.rs:2292-2302`). The GC filters (LATENESS `retain_keys` / `retain_values`) are
  re-derived on the first steps after restore from the restored waterline, which is a `Z1` (§4.3).
  Our notes said the file held "path list + filters/dirty flag".
- **Small operator state files.** Each is written with `backend.write`, which creates, writes,
  `complete()`s (that is, fsyncs) and calls `mark_for_checkpoint` (`crates/storage/src/lib.rs:127-142`).
  The full list is in §4.3.
- **Forced flush.** Yes: every in-RAM spine batch is flushed. This is the only call site of
  `persisted()`. Transient streams have nothing to flush, because the checkpoint runs between steps.

**EXTENSION: the "background commit" saves less than it appears.**

- `complete()` already fsyncs each file on the worker thread during prepare.
- `CheckpointCommitter::commit` (`dbsp_handle.rs:2694-2707`) then calls `commit()` on each
  `FileCommitter`, which is a **redundant** fsync, followed by `Checkpointer::commit`.
- So the blocking window covers writing and fsyncing all the RAM-resident batches. The background
  thread does the re-fsyncs, the manifest, and the wait for outputs (§2.5).

**Ordering of the commit (`circuit/checkpointer.rs:348-424`), then `controller.rs:9725-9806`:**

1. The `CHECKPOINT` marker, committed.
2. `dependencies.json`, holding `{batches: union over the checkpoint's pspine-batches,
   state_files: a listing of the directory}`, committed.
3. `fsync_dir(cp_dir)`.
4. The `checkpoints.feldera` catalog at the root, updated in memory and then committed.
   `fsync_dir(root)` follows, and the in-memory list is rolled back if it fails.
5. On the controller's background thread: **wait until every output connector has transmitted
   everything derived from records processed before the checkpoint**.
6. Write `<uuid>/state.json`, then the **root `state.json`**.
7. Back on the circuit thread (`RunningCheckpoint::finish`, `controller.rs:9662-9680`):
   `gc_checkpoint(except = checkpoints with a pending S3 sync)`, then truncate the journal
   (`ft.checkpointed(step)`).

**The root `state.json` is the resume pointer, not the catalog.** `with_latest_checkpoint` reads the
root `state.json`, which embeds the circuit checkpoint uuid (`controller.rs:5700-5720`, then
`with_init_checkpoint` at `:5745`). A crash after step 4 but before step 6 leaves a cataloged
checkpoint that nothing points to. Recovery then uses the previous checkpoint, which is still on
disk, because GC (step 7) only runs after the pointer has moved.

### 2.3 In-progress background merges [verified + inference]

- Merges are never cancelled or awaited. `pause_new_merges` only stops new merges from starting.
  The checkpoint records the merge **inputs** (`spine_async.rs:2219-2226`, doc at `:932-937`).
- **Cost.** An in-RAM merge input is written as a **side copy**. The live merge never reads that
  copy; only the checkpoint holds it. If the merge is still running at the next checkpoint, the
  input is copied again.
- **After restore**, all the batches are inserted as loose batches (`spine_async.rs:2303-2311`) and
  the merger simply merges them again.
- **CORRECTION / refinement of "O(delta) incremental checkpoint".** Persisted not-merging batches
  rejoin the merger as file batches. But a merge whose output is under 10 MiB writes that output
  **back to RAM** (§1.2). So each checkpoint pays roughly the **sub-threshold RAM residue** at that
  moment: everything below 10 MiB per merge, across every spine and every worker. That residue can
  include old data recently merged back into memory. The cost is bounded and small per spine, but
  it is O(RAM residue), not strictly O(new data). The notes' wording, "only the batches produced
  since the previous one", is slightly too strong.

### 2.4 Cross-worker consistency [verified]

There is no distributed snapshot protocol within a host. Consistency follows from three facts:

1. Steps are lockstep across workers, so all workers are at the same step boundary.
2. Exchange channels are empty between steps.
3. The handle broadcasts the command and waits for every worker. The checkpoint is cut between two
   global steps.

Other consequences:

- **Restore is verified across workers.** `collect_restore_info` requires every worker to compute
  an **identical `BootstrapInfo`** (the replay plan) and kills the pipeline if they differ
  (`dbsp_handle.rs:2000-2048`).
- **Worker and host counts are pinned to the checkpoint.** *"Can't change number of workers or hosts
  yet"*: on resume, `workers` and `hosts` come from the checkpointed config (`controller.rs:5845-5847`).
  Persistent ids embed `worker_index` (`circuit_builder.rs:992-1004`), so rescaling would make every
  operator look new.
- **Multihost** adds a two-phase hold: `prepare_checkpoint` sets `coordination_prepare_checkpoint`,
  which delays the checkpoint, and `release_checkpoint` clears it. An external coordinator drives
  this (`controller.rs:1131-1222`, `:8820`).

### 2.5 Input connector offsets [verified]

- **Per-step resume tokens.** Each input adapter reports a token per step through
  `InputConsumer::extended` (`crates/adapterlib/src/transport.rs:812-880`):
  - `Resume::Barrier`: cannot checkpoint after this step;
  - `Resume::Seek{seek: JSON}`: can restart just after this step (at-least-once);
  - `Resume::Replay{seek, replay, ...}`: can also replay this exact step (exactly-once).
- **What the checkpoint stores.** The current `seek` of every endpoint goes into
  `Checkpoint.input_metadata` (`controller.rs:9535-9544`), along with `step`, `processed_records`,
  `transaction_number`, the full `PipelineConfig`, and input and output statistics. All of it is
  written as `state.json`.
- **Getting endpoints off barriers.** When a checkpoint is requested, the controller tells
  endpoints to reach a checkpointable point (`reader.queue(checkpoint_requested && no transaction)`,
  `controller.rs:4386-4396`). It polls only barrier endpoints until they move off the barrier
  (`:4300-4331`).
- **Outputs are drained before completion.** The checkpoint completes only after outputs have
  drained up to `processed_records` (`controller.rs:9733-9783`). That is what lets exactly-once
  dedup outputs on replay.

---

## 3. GC of files [verified]

The existing notes have this right: delete-on-drop refcounting for live files, plus mark-and-sweep
over `dependencies.json` manifests for checkpointed files, plus a sweep at startup. Extensions:

- **Ownership moves one way.** A file starts as *live-owned*: `DeleteOnDrop{keep:false}`, deleted
  when its last `Arc` drops (`posixio_impl.rs:193-247`). `mark_for_checkpoint()` makes it
  *checkpoint-owned* (`keep=true`). After that, only checkpoint GC can delete it. The **only** path
  that marks a batch file is `Batch::file_reader()`, called from `Spine::save`
  (`wset_batch.rs:481-484`). **Files opened at restore are created with `keep=true`**
  (`posixio_impl.rs:87-111`, `DeleteOnDrop::new(file_name, true, …)`). So when the live spine merges
  away a restored or checkpointed file, the file stays on disk until every checkpoint that references
  it is evicted.
- **Retention.** `MIN_CHECKPOINT_THRESHOLD = 2` (`checkpointer.rs:48-49`). `gc_checkpoint` runs
  after every successful checkpoint and evicts everything older than the newest two, except
  checkpoints pinned by a pending S3 sync (`checkpointer.rs:522-594`). GC is refused during
  concurrent bootstrap (`dbsp_handle.rs:2267-2280`).
- **Steady-state disk use** = live files ∪ the files of the two retained checkpoints.
- **Subtle: the comment and the code disagree, but the code is right.** The comment says *"keep
  batches of the newest retained checkpoint … its merger may still depend on them"*. The code,
  `checkpoint_list.iter().find(|c| !except.contains(..))`, actually takes the **oldest** retained
  checkpoint, because the list is oldest-first (`checkpointer.rs:562-572`). This is still correct.
  An immutable batch that is live at checkpoints k and k+2 must also be live at k+1, so
  `removed ∩ any_retained ⊆ removed ∩ oldest_retained`. **That liveness-monotonicity argument is the
  invariant a copy must preserve** [inference]. It breaks if a restore from an *older* checkpoint is
  followed by checkpoints alongside newer checkpoints from the abandoned timeline. Feldera avoids
  this because the resume pointer is always the newest checkpoint.
- **Safety rules.** An unreadable or corrupt `dependencies.json` is an **error**, never an empty
  set (`storage/src/lib.rs:217-267`). A missing catalog alongside UUID directories **refuses to
  start**, rather than letting the startup sweep wipe them (`checkpointer.rs:435-472`).
  `gc_startup` deletes unreferenced `.feldera`/`.mut` files and keeps unrecognised ones
  (`checkpointer.rs:111-278`).
- **Leak window** [inference]. `mark_for_checkpoint` runs during prepare. If prepare or commit then
  fails, the marked files are referenced by no catalog entry and are never deleted on drop. They
  stay until the next process start's `gc_startup`.

---

## 4. Recovery

### 4.1 Exact steps [verified]

1. **Controller** (`controller.rs:5700-5860`):
   - read the root `state.json`;
   - merge configs, pinning `workers`, `hosts` and the FT model, and keeping `clock_timezone_offset`;
   - `compute_pipeline_diff(checkpoint program, new program)`;
   - carry connector paused state forward for unaffected connectors;
   - pass `init_checkpoint = uuid` to the runtime, which checks `<uuid>/CHECKPOINT` exists
     (`circuit/runtime.rs:432-439`).
2. **`Checkpointer::new`** reads the catalog, then runs **`gc_startup`**: it lists the whole storage
   root and reads `dependencies.json` for every cataloged checkpoint (`checkpointer.rs:53-87, 111-278`).
   The fingerprint is checked only in Ephemeral mode (`dbsp_handle.rs:1442-1463`).
3. **Each worker runs `analyze_checkpoint`** (`circuit_builder.rs:7866-7990`):
   - **(a)** In Persistent mode, `verify_checkpoint_intact`: every file listed in `state_files` must
     exist (`checkpointer.rs:288-330`). It does **not** check root batch files. A missing batch file
     instead fails at `B::from_path` as `InvalidData`, which is a hard error
     (`spine_async.rs:2303-2310`).
   - **(b)** Call `node.restore(base)` on every node. A `NotFound` adds the node to `need_backfill`
     in Persistent mode; any error is fatal in Ephemeral mode.
   - **(c)** Add nodes whose state the balancer invalidated, then map each to its top-level ancestor.
   - **(d)** Walk backward to replay sources, and treat a nested circuit on the path as a unit.
4. **`prepare_replay`** (`circuit_builder.rs:8045-8140`):
   - replay sources `start_replay()`, so a `Z1Trace` streams its stored trace into a replay stream
     (`operator/dynamic/trace.rs:1130-1148`);
   - nodes being backfilled are `clear_state()`d;
   - the scheduler is restricted to the nodes taking part.
   The controller then steps until replay completes, and then `CompleteBootstrap`.
5. **Exactly-once only:** replay the journal steps after `checkpoint.step`, then resume connectors
   from their `seek`.

### 4.2 Eager vs lazy, and the cost model

**CORRECTION.** The notes say restore is "O(number of batch files) small reads … reads exactly the
512-byte trailer … locates (and optionally loads) the Bloom filter". In fact, for every batch file,
`from_path` → `Reader::open_with_filter` **always** does all of the following eagerly
(`wset_batch.rs:486-499`; `reader.rs:1663-1768, 1847-1877`):

- `get_size` and the 512-byte trailer;
- the **column-0 root block**, which is one index or data block of at least 8 KiB, used to compute
  the key range;
- the **entire filter block**, which is deserialized into the heap.

So the eager cost is **O(#files) + O(#keys × filter bits)**. At the default Bloom FPR that is about
**2.4 bytes per key**, read and kept resident. Example: 1e9 keys → about 2.4 GB of filter read at
restore. Roaring filters are smaller when they apply.

Everything else is lazy, through the page cache and buffer cache: data blocks and non-root index
blocks.

- **No index is rebuilt.** The B-tree *is* the index. The filters are deserialized, not recomputed.
- **Rescanned at startup:** the storage-root listing (`gc_startup`) and every checkpoint's
  `dependencies.json`.
- **After restore, merges restart.** Every restored batch sits as a loose batch, and the background
  merger re-merges them.
- **Backfill cost** is O(state of each replay source's trace), streamed. It applies only to
  operators that are new or changed.

### 4.3 Non-trace operator state ("bespoke") [verified]

| operator | saved | cite |
|---|---|---|
| `Z1<T>` | rkyv of the delayed value (`z1-<pid>.dat`). If T is a batch, `FallbackWSet::checkpoint` serializes the **whole batch** and restore makes it `Inner::Vec` | `operator/z1.rs:255-335`; `trace/ord/fallback/wset.rs:724-736` |
| `TransactionZ1` (delay across transactions, used by `differentiate`) | `{old_value, new_value}` bytes | `operator/transaction_z1.rs:25-34, 54-60, 131-160` |
| waterlines (LATENESS) | a `Z1` in a persistent feedback, id `"{name}.delayed_waterline"` | `operator/dynamic/input_upsert.rs:410-416` |
| `Window` | serialized `(lower, upper)` bounds | `operator/dynamic/time_series/window.rs:95-230` |
| balancer, adaptive-join exchange sender | JSON `{current_policy, key_distribution}` | `operator/dynamic/balance/accumulate_trace_balanced.rs:1198-1223` |
| nested circuit | its clock time. A missing clock means the whole scope is backfilled | `circuit_builder.rs:7480-7530` |
| `Output` | empty `()` marker (it marks "this view existed") | `operator/output.rs:530-555` |
| multijoin `Match` | nothing (transient per-transaction state) | `operator/dynamic/multijoin/match_keys.rs:856-872` |
| every trace (`Z1Trace`, `AccumulateZ1Trace`, aggregates, joins, distinct, top-k) | `Spine::save` | notes Q1/Q5 |

The `Accumulator` spine (transaction buffering) has no checkpoint, because checkpoints never happen
inside a transaction. A stateful operator with no persistent id **errors** at checkpoint time
(`require_persistent_id`, `crates/dbsp/src/operator.rs:101-106`).

---

## 5. Operator identity across restarts and recompiles

The notes are correct (Q3): Merkle SHA-256 over the optimized plan subtree, Persistent mode
hard-coded for SQL, the fingerprint check disabled, `NotFound` meaning backfill, and `state_files`
keeping real loss loud. Additions:

- **The id is per worker:** `"{worker_index}-{label}"` (`circuit_builder.rs:992-1004`). That is the
  reason rescaling is blocked (§2.4).
- **Sub-operators get derived ids** by suffixing the parent's label, for example
  `"{pid}.transaction_delay"` (`transaction_z1.rs:31`) and `"{name}.delayed_waterline"`. An internal
  operator without a label fails checkpointing in Persistent mode rather than being skipped.
- **Ephemeral mode** uses a positional `global_id().path_as_string()`. Every restore error,
  including `NotFound`, is fatal, and the fingerprint is verified against *all* cataloged
  checkpoints (`checkpointer.rs:66-76`). There is a TODO to verify only the latest
  (`dbsp_handle.rs:1449-1454`).
- **A program change** in Persistent mode is diffed and gated by `bootstrap_policy` (the docs cited
  in the notes). Backfill reaches back through ancestors to the nearest checkpointed stream or
  input. LATENESS pipelines cannot bootstrap (docs limitation, in the notes).
- **The on-disk format version** is a separate axis. `VERSION_NUMBER=5` and `MIN_SUPPORTED_VERSION=5`
  are in the trailer. An unknown *incompatible* feature bit means the file is refused
  (`reader.rs:1681-1699`).

---

## 6. Things a copy would likely get wrong

1. **Filters are not checkpointed.** Restore must clear the trace GC filters and let the restored
   waterline `Z1` re-establish them (`spine_async.rs:2292-2302`). Persisting them would be a
   redundant second source of truth that can disagree with the waterline.
2. **The merging set must be captured as merge inputs, without stalling the merge**, and the
   persisted copies must not be swapped into the live merge (`spine_async.rs:2219-2226`). Waiting
   for merges (`pause()`, `:921-930`) is what `complete_merges` does. It is not what checkpoint does.
3. **The resume pointer is written last, and GC runs after the pointer is durable.** The order is
   catalog → outputs drained → `state.json` → GC (§2.2). A checkpoint is "real" only once the
   pointer is durable.
4. **Liveness monotonicity is what makes "keep the oldest retained checkpoint's batches" sufficient**
   (§3). It also depends on never branching timelines.
5. **Restored files must be checkpoint-owned (`keep=true`).** Otherwise the first merge after restore
   deletes files that the retained checkpoints still reference (`posixio_impl.rs:87-111`).
6. **Distinguish "new operator" from "lost state" with a committed `state_files` manifest.** Batch
   files are checked separately, by a hard failure on open.
7. **Workers must agree on the replay plan** (`dbsp_handle.rs:2000-2048`). The worker count is
   pinned.
8. **Restore is not free per key:** the filter block per file is read eagerly (§4.2). A design that
   promises "O(#files) restore" while keeping a per-file Bloom filter is wrong by about 2.4 B/key.
9. **Each batch file costs two fsyncs, including spill temporaries** (§1.2). This is a likely
   unexamined cost in Feldera and not something to copy.
10. **Known issues and TODOs in their tree:**
    - checkpoints can be **delayed indefinitely** under end-to-end transactions
      (`controller.rs:4327-4331` FIXME);
    - the fingerprint is not validated in Persistent mode, so a crash-recovery mismatch is not
      detected (`dbsp_handle.rs:1449-1454`);
    - the legacy `pspine-batches` scan fallback remains (`storage/src/lib.rs:244-247` TODO);
    - no checkpoint is allowed during concurrent bootstrap or after an aborted bootstrap
      (`dbsp_handle.rs:2632-2648`);
    - workers and hosts cannot change across a checkpoint (`controller.rs:5845`).
11. **First checkpoint vs steady state** (§2.3). The steady-state cost is the sub-10 MiB RAM residue
    of every spine × worker, written and fsynced on the step thread. It is not literally zero, and
    small file batches merged back into RAM get rewritten.

## Where `docs/research-feldera/persistence.md` was wrong or incomplete

- *"`pspine-<pid>.dat` … path list + filters/dirty flag"*: **filters are NOT saved.** `CommittedSpine`
  is `{batches, merged=[], effort=0, dirty}`, and restore clears the filters.
- *"restore is O(number of batch files) … reads exactly the 512-byte trailer … optionally loads the
  Bloom filter"*: it **always** loads the full filter block, plus the column-0 root block. The cost
  is O(#files + #keys × ~2.4 B).
- *"steady-state incremental checkpoint cost ≈ batches created since the last checkpoint"*: more
  precisely, it is the **sub-threshold RAM residue**. Small file batches can merge back into RAM and
  be rewritten. In-RAM merge inputs are persisted as side copies on every checkpoint while their
  merge runs.
- *"fsync/commit phase runs on a separate thread"*: true, but `complete()` already fsyncs every file
  during the blocking prepare. The background commit is mostly redundant fsyncs, the manifest, the
  output drain, and `state.json`.
- **Missing from the notes:**
  - the two cache tiers, with the OS page cache as the default and the decoded buffer cache at
    256 MiB per thread using S3-FIFO;
  - double fsync per layer file, temporaries included;
  - root `state.json` (not the catalog) as the resume pointer, and the commit ordering;
  - the output-drain wait inside the checkpoint;
  - `keep=true` on files opened at restore;
  - workers pinned and the cross-worker replay-plan check;
  - the oldest-vs-newest retained-checkpoint subtlety in GC;
  - the inventory of bespoke state (TransactionZ1, Window, balancer, nested clock).
