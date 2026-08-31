# Feldera findings — §1: trace family, batch structure, compaction, memory/spill

Checkout: `d:\src\feldera` @ `78afc907773567588e9981be7179f398c0cbd473` (2026-08-29).
All paths below are relative to that root. Everything is **source-verified by reading** unless
marked *(inferred)* or *(prose doc)*. **Nothing was built or run** — no WSL/cargo invocation was
needed.

---

## Q1. One trace family or several? What is the `Spine`/batch-trait structure? Any non-LSM trace?

### The trait hierarchy (verified)

`crates/dbsp/src/trace.rs` defines three layers:

- `BatchReader` (`trace.rs:413`) — read-only, cursor-based, sorted by key then value. Not object
  safe; `Cursor` is the object-safe substitute.
- `Batch: BatchReader + Clone + Send + Sync` (`trace.rs:788`) — adds `Builder` (ordered input) and
  `Batcher` (unordered input) and merging.
- `Trace: BatchReader` (`trace.rs:225`) — adds `insert`, `set_frontier`, `retain_keys`,
  `retain_values`, `save`/`restore`, `fork`, `exert`, `initiate_compaction`.

### There is exactly ONE `Trace` implementation in production code (verified)

```
$ grep -rn 'impl.*> Trace for' crates/
crates/dbsp/src/trace/spine_async.rs:2037   impl<B> Trace for Spine<B>
crates/dbsp/src/trace/test/test_batch.rs:1316  (test-only TestBatch)
crates/dbsp/src/typed_batch.rs:165          (blanket typed wrapper over the above)
```

**`Spine<B>` is the only trace family.** There is no dictionary/hash-backed alternative anywhere
— no analogue of DbspNet's "flat" trace exists in the codebase. All key lookup in Feldera is
*ordered* (`seek_key` = advance-to-value-or-larger / binary search + gallop), never hashed:
`crates/dbsp/src/trace/ord/vec/indexed_wset_batch.rs:644` (`seek_key_exact` = `seek_key` then
compare), `crates/dbsp/src/trace/ord/file/indexed_wset_batch.rs:810` (same, guarded by a
Bloom/roaring membership filter first).

### Concrete batch implementations (verified)

Three families under `crates/dbsp/src/trace/ord/`, each in four shapes (`WSet`, `IndexedWSet`,
`KeyBatch`, `ValBatch`):

| Family | Where | Storage |
|---|---|---|
| `Vec*` (`VecValBatch`, `VecIndexedWSet`, …) | `trace/ord/vec/` | RAM, trie-of-`LeanVec` |
| `File*` (`FileValBatch`, `FileIndexedWSet`, …) | `trace/ord/file/` | on-disk layer file |
| `Fallback*` | `trace/ord/fallback/` | `enum Inner { Vec(..), File(..) }` |

**The `Ord*` names everything actually uses are aliases for the `Fallback*` hybrid**, not for the
in-memory type — `crates/dbsp/src/trace/ord.rs:5-24`:

```rust
FallbackIndexedWSet as OrdIndexedWSet, FallbackValBatch as OrdValBatch,
FallbackKeyBatch as OrdKeyBatch, FallbackWSet as OrdWSet
```

and `crates/dbsp/src/algebra/zset.rs:46,56` alias `OrdZSet`/`OrdIndexedZSet` onto those. So **the
default batch type in every operator is the memory-or-file hybrid**, and a single spine can hold a
mix of in-RAM and on-disk batches simultaneously (`Inner` is per batch —
`trace/ord/fallback/val_batch.rs:138-146`).

`FallbackValBatch::cursor()` returns a `DelegatingCursor` = `Box<dyn ClonableCursor>`
(`trace/cursor.rs:583`, `trace/ord/fallback/val_batch.rs:229`), i.e. **a virtual call per cursor
operation on the read path** — the price of the hybrid.

### What decides which one an operator gets at runtime (verified)

Nothing is decided at *compile* time; it is decided per batch, per event, from three runtime
thresholds in `crates/dbsp/src/circuit/runtime.rs`, each modulated by a live memory-pressure signal:

| Decision point | Function | Default (no pressure) | Under pressure |
|---|---|---|---|
| Build a *transient* in-step batch | `min_step_storage_bytes()` `runtime.rs:1232` | `usize::MAX` → RAM | `Critical` → 0 → all to disk |
| Insert a batch into a spine | `min_insert_storage_bytes()` `runtime.rs:1196` | `usize::MAX` → RAM | `Moderate` → 10 MiB; `High` → 0 |
| Write the *result of a merge* | `min_merge_storage_bytes()` `runtime.rs:1167` | 10 MiB | `Moderate`+ → 0 → all to disk |

Consumed by `pick_merge_destination` / `pick_insert_destination` / `BuildTo::for_capacity` in
`trace/ord/fallback/utils.rs:41-136`. `BuildTo::Threshold(n)` even lets a *single builder* start in
RAM and spill mid-build once `n` bytes are used (`utils.rs:36-39,68-72`).

Memory pressure is a process-RSS poll every 1 s against `max_rss_mb`
(`runtime.rs:735-802`), bucketed at 85 % / 90 % / 95 %
(`crates/feldera-types/src/memory_pressure.rs:6-8,27-37`) with hysteresis (a High/Critical level
persists until RSS drops two buckets, `runtime.rs:763-776`). Crossing into High also *wakes every
merger task* to flush in-memory batches (`runtime.rs:791-799`, consumed by
`Slot::must_relieve_memory_pressure` `spine_async.rs:317-328`).

**Consequence:** with storage configured (the default per the docs), the *steady state* is
in-memory batches; disk is where big merge outputs and everything-under-pressure lands. The
memory/disk decision is a continuous control loop, not a mode.

---

## Q2. Does *all* stateful operator state go through the trace abstraction? Is there any state that cannot spill?

**Essentially yes — everything is a `Spine` of `Fallback*` batches.** I checked each of the four
DbspNet operators named in the brief:

| DbspNet operator | Feldera equivalent | State |
|---|---|---|
| `IntegrateOp` (materialised view) | `integrate_trace` / `accumulate_integrate_trace` | `Stream<C, Spine<B>>` — `operator/dynamic/trace.rs:516`; adapters wire materialised views through `accumulate_integrate_trace()` at `crates/adapters/src/static_compile/catalog.rs:145,640` |
| `PartitionedWindowAggregateOp` (OVER) | `partitioned_rolling_aggregate` → radix tree | radix tree is **encoded as an indexed Z-set** (prefix = key, node = value) and held in `Spine<O>` — `operator/dynamic/time_series/radix_tree.rs:63-66`, `.../radix_tree/partitioned_tree_aggregate.rs:286` |
| `PartitionedOffsetOp` (LAG/LEAD) | `operator/dynamic/group/lag.rs` | `GroupTransform` over `Spine` in/out — `operator/dynamic/group.rs:341-345` |
| `PartitionedRankOp` / TOP-K | `group/rank.rs`, `group/topk.rs`, `group/row_number.rs` | same `GroupTransform` machinery, same `Spine<OB>` feedback |

Also checked and trace-backed: `distinct` (`operator/dynamic/distinct.rs:590,1069`), `join`
(`operator/dynamic/join.rs`), `upsert`/`input_upsert` (`operator/dynamic/input_upsert.rs:308,423` —
`Spine<B>`), the per-transaction `Accumulator` (`operator/dynamic/accumulator.rs:174` — `state:
Spine<B>`), and output buffering (prose doc `docs.feldera.com/docs/operations/memory.md:67-69`:
"Output records can be in memory or on storage").

### The exceptions I found (verified, but small)

1. **`keys_of_interest`** — `RefCell<BTreeMap<Time, Box<DynSet<Key>>>>` in `AggregateIncremental`
   (`operator/dynamic/aggregate.rs:822`) and in `DistinctIncremental`
   (`operator/dynamic/distinct.rs:706,770`). A set of keys that must be revisited at a *future*
   logical time. Pure RAM, no spill path. *(Inferred)* in a root circuit with `Time = ()` this
   stays empty; it only fills inside nested/recursive circuits.
2. **Bloom filters for on-disk batches** stay resident in RAM by design — ~19.2 bits/key at the
   default 1e-4 FP rate (prose doc `docs.feldera.com/docs/operations/memory.md:107-125`; code
   `crates/dbsp/src/storage/file.rs:122-124`). At large state this is the one component that scales
   with state size and *cannot* be paged out. The doc explicitly calls this out as "a large cost
   when many records are in storage".
3. Input/output connector queues and the buffer cache are RAM, bounded by their own knobs, not by
   the spill machinery (`memory.md:150-155`).

**Answer to the brief's sharpest form of the question:** there is no operator in a Feldera pipeline
whose *tuple* state is pinned in RAM. Unlike DbspNet, Feldera has no "29.4 % of state has no
spillable sibling" problem, because there is only one state representation to begin with.

---

## Q3. Compaction strategy and merge scheduling — fuel/budget or bulk-on-threshold?

**Neither, in the way DbspNet frames it. Merging is not on the step's critical path at all.**

### It runs on separate threads (verified)

`Spine::exert()` — the DD-style "spend fuel from the circuit" hook — **is an empty function**:
`crates/dbsp/src/trace/spine_async.rs:2108`:

```rust
fn exert(&mut self, _effort: &mut isize) {}
```

Merging happens in a **dedicated tokio multi-thread runtime**, created once per DBSP runtime with
`num_merger_threads` worker threads (`crates/dbsp/src/circuit/runtime.rs:708-736`). The default is
`num_workers * 1` — i.e. **W merger threads on top of W circuit workers**
(`crates/dbsp/src/circuit/dbsp_handle.rs:54,637-643`). CPU pinning reserves separate cores for
foreground and background (`runtime.rs:362-412`).

One tokio task is spawned **per spine per level** (`MergeWorkers::start/run`, `spine_async.rs:1287-1352`).

### The tiering rule (verified)

- 9 levels (`spine_async.rs:97`), sized by **base-10 log of record count**, not powers of 2
  (`Spine::size_to_level`, `spine_async.rs:2331-2369`): level 0 ≤ 14 999 records, level 1 ≤ 99 999,
  level 2 ≤ 999 999, … level 8 ≥ 100 bn.
- Level 0's ceiling is a named constant `MAX_LEVEL0_BATCH_SIZE_RECORDS = 14_999`
  (`spine_async.rs:113`) chosen so that *typical operator output and connector input batches land in
  level 0* (comment at `spine_async.rs:103-111`).
- A merge starts when a level has enough loose batches — `MERGE_COUNTS`, `spine_async.rs:274-284`:
  level 0 needs **8..=128** batches, level 1 **8..=64**, levels 2–5 **3..=64**, levels 6–8
  **2..=64**. The comment says "the minimum number of batches to merge is key to performance. The
  maximum number seems much less important."
- Merges are **k-way, not pairwise**: up to 64 (128 at level 0) batches merged in one pass through a
  binary-heap `CursorList` (`trace/spine_async/list_merger.rs`, `trace/cursor/cursor_list.rs:12-29`).
- Retraction bias: negative-weight records are counted N+1 times when computing the level, pushing
  retraction-heavy batches up the tree faster so they cancel sooner
  (`spine_async.rs:2338-2356`, `negative_weight_multiplier`, default 0).

### Where fuel *does* appear (verified)

Fuel is used **inside** the background task purely as a yield quantum, not as a circuit budget
(`MergeWorkers::merge_step`, `spine_async.rs:1411-1421`):

- level 0 merges run to completion (`fuel = isize::MAX`);
- levels 1–8 get "the average fuel a level-0 merge consumed", tracked as an EWMA
  (`WorkerState::report_slot0_merge`, `spine_async.rs:1466-1479`);
- after spending it the task calls `yield_now().await` (`spine_async.rs:1345`), so tokio's run queue
  round-robins levels and each level gets roughly equal CPU.
- `Merge::merge` decrements fuel one tuple at a time (`list_merger.rs:429`).

### How a step is prevented from stalling (verified)

Not by bounding merge work — by **backpressure on the producer**. `Trace::insert` is `async`
(`trace.rs:294`); `Spine::insert` awaits `backpressure_wait()` once the spine holds ≥ 128 *loose*
(not-currently-merging) batches (`LooseBatchCount::HIGH_THRESHOLD = 128`, `spine_async.rs:653`;
`Spine::insert`, `spine_async.rs:2128-2152`). The wait is instrumented as
`merge_backpressure_wait_time_seconds`. So a step *can* block, but only when merging is 128 batches
behind, and it blocks the operator's async task rather than the worker thread.

### Full compaction is a manual, out-of-band operation (verified)

`Trace::initiate_compaction` / `is_compaction_complete` drive a level-by-level sweep that collapses
a spine to ≤ 1 batch (state machine documented at `spine_async.rs:143-183`; `try_start_merge`
honours `CompactionStatus::Requested` by merging *all* batches at a level, `spine_async.rs:295-300`).
It is triggered only by an explicit REST call `POST /start_compaction`
(`crates/adapters/src/server.rs:3148`, `crates/pipeline-manager/src/api/endpoints/pipeline_interaction.rs:1145`),
never automatically per step. `DbspHandle::wait_for_compaction` polls with exponential backoff
(`dbsp_handle.rs:2544-2569`).

**Delta vs DbspNet:** DbspNet's compaction is bulk-on-threshold *on the step thread*. Feldera's is
continuous, k-way, size-tiered, and **on other cores**. The step never pays merge CPU; it pays only
(a) cursor-list depth while batches are unmerged and (b) rare backpressure. This is the single
biggest structural reason an LSM does not cost them the +14 % per-step that DbspNet measured for
spine-vs-flat.

---

## Q4. How is the small-batch case handled (LSM per-batch overhead on ~200-row deltas)?

Four mechanisms, all verified:

1. **Transactions absorb steps.** Feldera's clock unit is a *transaction*, not a step. A transaction
   is "a sequence of steps that evaluate a set of inputs for a single logical clock tick"; the
   logical clock advances only between transactions (`crates/dbsp/src/circuit/dbsp_handle.rs:1671-1690`).
   `start_transaction` → many `step()` → `start_commit_transaction` → `step()` until complete
   (`dbsp_handle.rs:1692-1760`).

2. **The `Accumulator` operator is a staging spine.** `Accumulator<B>` holds a private
   `state: Spine<B>` (`operator/dynamic/accumulator.rs:174`), inserts every per-step batch into it
   (`accumulator.rs:307`), and only on `flush` (end of transaction) swaps it out and emits it
   downstream as a whole (`accumulator.rs:315-325`). So a 200-row delta never individually reaches
   the integral spine unless the transaction really is 200 rows. This is a direct analogue of
   DbspNet's `SpineStagingConfig`, but it is **default and unconditional**, and it doubles as the
   transaction boundary.
   It also has an `EnableCount` so an output view with no attached connector accumulates *nothing*
   (`accumulator.rs:88-120`) — an optimisation DbspNet has no equivalent of.

3. **Level 0 is deliberately sized so ordinary deltas never leave it.** Operator output chunks
   default to 10 000 records (`splitter_chunk_size_records`, default 10 000 —
   `crates/feldera-types/src/config/dev_tweaks.rs:297-299`, read via
   `splitter_output_chunk_size()` `dbsp_handle.rs:382`) and level 0's ceiling is 14 999
   (`spine_async.rs:113`). The comment at `spine_async.rs:103-111` states this is intentional: level-0
   arrival rate is used as the estimate of the spine's total arrival rate.

4. **Level 0 refuses to merge fewer than 8 batches.** `MIN_LEVEL0_MERGE_BATCHES = 8`
   (`spine_async.rs:120`) with the doc comment: "A spine holding fewer level-0 batches than this
   merges nothing until something else asks it to." So a trickle of tiny batches costs *nothing* in
   merge work; it costs cursor-list depth until 8 accumulate, then one 8–128-way merge.

5. Builders are always pre-sized exactly: `Builder::for_merge` sums `key_count()` and `len()` over
   the inputs and passes them as capacity (`trace.rs:1110-1122`) — Feldera's structural equivalent of
   DbspNet's "adaptive delta-builder pre-sizing".

**Empty batches are dropped before they ever reach a slot** (`SharedState::add_batches`
`spine_async.rs:394-403`, `Spine::insert` `spine_async.rs:2131`).

---

## Q5. Is there anything corresponding to our lazy merge view (`LazyMergeMultiset`)?

**Yes — but as the *only* mode, not an optimisation.** Feldera never materialises "trace + delta"
at all. There is no code path that produces a merged in-memory image of a trace for reading.

- A `Spine` is read through `SpineCursor`, which is a `CursorList` over `Vec<Arc<B>>` — an on-the-fly
  k-way merge driven by two binary heaps (one for keys, one for values)
  (`spine_async.rs:1870-1907`, `trace/cursor/cursor_list.rs:12-29`). Nothing is copied.
- `SpineSnapshot<B>` is a `Vec<Arc<B>>` plus factories (`trace/spine_async/snapshot.rs:56-62`).
  Taking a read-only snapshot of a whole trace is **N `Arc` clones and nothing else**
  (`SharedState::get_snapshot` `spine_async.rs:440-444`). Every operator that needs "the trace as of
  last tick" does exactly this (`WithSnapshot::ro_snapshot`, `snapshot.rs:23-33`).
- Delta-against-trace is `CursorPair` (`trace/cursor/cursor_pair.rs:16-29`), a two-way lazy merge of
  two cursors — used e.g. by every `GroupTransform` (`operator/dynamic/group.rs:192,231`).
- `Trace::fork()` (`trace.rs:268`) makes a whole logical copy of a trace by sharing the immutable
  batch `Arc`s — "forking copies no data: its cost is proportional to the number of batches".
- The *only* eager materialisations are `Trace::consolidate` (`spine_async.rs:2110`) and
  `Spine::complete_merges` (`spine_async.rs:2390`), neither of which is on the per-step path.

So DbspNet's 4.6–19× "lazy merge view" win on aggregate-heavy shapes corresponds to something
Feldera gets for free from its structure: the merge view *is* the representation, and background
compaction is what keeps the cursor list shallow enough to be cheap.

---

## Q6 (asked as a rider). Physical batch layout: columnar/trie-layered or row-major?

**Trie-layered, à la differential-dataflow — verified.**

In memory (`crates/dbsp/src/trace/layers/`):

```rust
pub struct Layer<K, L, O = usize> {          // layers/layer.rs:59
    keys: Box<DynVec<K>>,                    //   sorted key array
    offs: Vec<O>,                            //   offsets: keys[i] → vals[offs[i]..offs[i+1]]
    vals: L,                                 //   the next layer down
}
pub struct Leaf<K, R> {                      // layers/leaf.rs:70
    keys: Box<DynVec<K>>,
    diffs: Box<DynVec<R>>,                   //   parallel arrays
}
```

- `VecWSet` = `Leaf<K, R>` (`ord/vec/wset_batch.rs:117-127`) — two parallel arrays.
- `VecIndexedWSet` = `Layer<K, Leaf<V, R>>` (`ord/vec/indexed_wset_batch.rs:161-174`).
- `VecValBatch` = `Layer<K, Layer<V, Leaf<Time, R>>>` (`ord/vec/val_batch.rs:40,186-205`) — three
  levels, so the time/diff history per (key, val) is its own run.
- Builders write straight into these arrays (`VecIndexedWSetBuilder` holds `keys/offs/vals/diffs`,
  `ord/vec/indexed_wset_batch.rs:711-725`).

On disk, the "layer file" format is the same shape persisted: *n* columns, one B-tree-ish block tree
per column, index blocks as interior nodes, data blocks as leaves; a 2-column file is "analogous to
`BTreeMap<K[0], BTreeSet<K[1]>>`"; written once and immutable
(`crates/dbsp/src/storage/file.rs:1-68`). Serialisation is `rkyv`; a per-batch Bloom (or exact
roaring) membership filter is stored alongside (`file.rs:38-42`).

### Important qualification: columnar *by trie level*, row-major *within a value*

`DynVec<K>` is a **type-erased vector whose concrete type is `LeanVec<T>`** — a real contiguous
`Vec<T>` with 32-bit len/cap (`crates/dbsp/src/dynamic/vec.rs:22,303-334`, `dynamic/lean_vec.rs`).
`T` is the whole key struct or the whole value struct that the SQL compiler generated. So:

- keys, values, times and weights each live in their own contiguous array (this is the
  "column-layer" part), **but**
- a SQL row's columns are *not* split apart — the key struct is stored inline, row-major, inside the
  key array.

And the whole DBSP core is **deliberately dynamically dispatched**, not statically monomorphised:
`crates/dbsp/src/dynamic.rs:1-38` states that an earlier statically typed version "led to very long
compilation times", so operations (compare, clone, serialise) are exposed as object-safe traits with
factory objects, and the type check is elided in release builds. `crates/dbsp/src/mono.rs:1-24`
exists solely to force *one* monomorphisation of each operator inside the `dbsp` crate rather than
per client crate. Crucially the vtable is **per type, at the container/operation level** — values are
stored unboxed and contiguous — so this is not per-value boxing. *(This bears on axis 2; flagged
here because it is decided in the trace/`dynamic` code.)*

---

## Where this contradicts or challenges a DbspNet decision

### 1. `docs/decision-trace-family.md` — "STOP GROWING SPINE; keep flat as default"

**The evidence does not refute the decision, but it refutes the premise that produced the numbers.**

- Feldera has **no flat trace at all** and no dictionary-backed indexed Z-set anywhere. There is one
  trace family, LSM, everywhere, for every stateful operator.
- DbspNet's measured spine penalty (+14 % on the bulk step, 1.4–2.5× worse as a W=24 substrate) is
  measured against a spine whose **merging runs on the step thread**. Feldera's `Spine::exert` is
  literally `{}` (`spine_async.rs:2108`); merges are on a separate tokio runtime with **W dedicated
  merger threads** (`dbsp_handle.rs:54,637-643`, `runtime.rs:708-736`). The comparison DbspNet ran is
  "LSM with in-step compaction vs. dictionary", not "LSM vs. dictionary". If spine's cost is merge
  CPU stolen from the step, the correct experiment before closing the arc is
  *background-thread merging*, which DbspNet does not appear to have tried.
- Corollary for the single-threaded gap: Feldera at 1 worker also gets **1 merger thread**, so a
  "1-core" Feldera number is actually using ~2 cores' worth of wall-clock work. That is worth
  checking before attributing the whole 11-of-13 Nexmark single-thread deficit to per-row cost.
  (Note the Nexmark bench itself defaults to **storage disabled** — `min_storage_bytes = usize::MAX`,
  `crates/nexmark/src/config.rs:57,153` — so those runs are in-memory `Vec*` batches, but the merger
  threads still exist.)

### 2. "Spine cannot bound memory, because 29.4 % of SF=3 state has no spine sibling
(`IntegrateOp`, `PartitionedWindowAggregateOp`, `PartitionedOffsetOp`, `PartitionedRankOp`)"

**Feldera has spillable equivalents of all four**, because they are not bespoke operators — they are
`GroupTransform`s and integrals over the same `Spine`:
`operator/dynamic/group.rs:341-345` (lag/rank/topk/row_number),
`operator/dynamic/time_series/radix_tree/partitioned_tree_aggregate.rs:286` (windowed aggregate, with
the radix tree *encoded as an indexed Z-set*), `operator/dynamic/trace.rs:516` (integral).
The radix-tree encoding is the interesting trick: rather than a bespoke in-RAM tree, they made the
tree *be* a Z-set so it inherits spill, checkpoint, and GC for free. That is a design pattern
DbspNet's four unspillable operators could adopt regardless of the trace-family decision.

The only genuinely unspillable tuple state I found is `keys_of_interest`
(`operator/dynamic/aggregate.rs:822`, `distinct.rs:706`), which is empty in root circuits, plus
Bloom filters, which scale with on-disk key count by design.

### 3. Track B (reference-manifest snapshots) was retired as "killed by Track A"

**Feldera implements exactly Track B, and it is their only checkpoint mechanism.**
`Spine::save` (`spine_async.rs:2199-2266`): persist any still-in-RAM batches to files
(`batch.persisted()`), **put the persisted batches back into the merger so the next checkpoint does
not rewrite them** (explicit comment at `spine_async.rs:2221-2225`), then write a `CommittedSpine`
manifest (`trace.rs:131-137`) that is just `Vec<String>` of batch file paths, plus a JSON
`PSpineBatches` batch list so the checkpointer can parse it without knowing the spine's type. No
batch data is copied. `restore` (`spine_async.rs:2268-2311`) re-opens each file by path and
`insert_without_blocking`s it.

DbspNet's Track A/B dilemma is, as the brief's §3 Q5 suspected, largely an artefact of keeping state
in RAM: when batches are already immutable files, a checkpoint is a manifest write. Feldera's design
also answers "no free lunch" honestly in its own comment — persisted batches must be read back from
disk to be used again.

### 4. "`SpineStagingConfig` for staging small batches" as an optional knob

Feldera's equivalent (`Accumulator`, `operator/dynamic/accumulator.rs`) is **not optional and not a
tuning knob** — it is the transaction boundary. Every operator's input is accumulated into a private
spine across the steps of a transaction and released as one unit at commit. This makes the "LSM
per-batch overhead on 200-row deltas" question largely moot for them: the LSM's unit of insertion is
a transaction, not a step, and level 0 is explicitly sized (14 999 records) to be larger than a
typical transaction's output chunk (10 000 records).

### 5. `LazyMergeMultiset` framed as an optimisation over an eager merge

For Feldera there is no eager merge to optimise away — `SpineCursor` (`CursorList` heap merge) and
`CursorPair` are the only way anything reads a trace, and `SpineSnapshot` is `Arc` clones.
DbspNet's 4.6–19× win therefore looks like recovering a property Feldera has structurally, which is
mild evidence that the flat/dictionary substrate keeps costing DbspNet things the LSM substrate would
have given for free.

---

## Things I could not determine

- Whether DbspNet's spine penalty would actually disappear with background merging — that is a
  DbspNet experiment, not a Feldera fact. I only established that Feldera's merging is off the step
  thread.
- The real-world *distribution* of memory vs. storage batches in a running ivm-bench-shaped pipeline.
  The thresholds are clear; where a given workload lands is empirical and I did not run anything.
- Whether the `fetch()` parallel-prefetch path (`trace.rs:672-694`, `Spine::fetch`
  `spine_async.rs:1830`) is used in practice: the join call site is gated on the dev tweak
  `fetch_join == Some(true)` (`operator/dynamic/join.rs:1605`), which defaults to **false**
  (`dev_tweaks.rs:285-287`). So storage-latency hiding for joins
  appears to be off by default today.
