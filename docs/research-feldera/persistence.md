# Feldera findings — §3: persistence, checkpointing, durable identity, recovery

Checkout: `d:\src\feldera` @ `78afc9077` (2026-08-29). All paths below are relative to that root.
Nothing was built or run — this is entirely source + their own prose docs. Claims are tagged
**[verified]** (read the code), **[docs]** (read their prose documentation), or **[inference]**.

---

## Headline

Feldera's traces are **file-backed by construction whenever storage is enabled (which is the
default)**, and a checkpoint is **a manifest that names already-durable, immutable batch files plus
a set of tiny per-operator state files**. It writes only the batches that were still in RAM at
checkpoint time, and it puts the newly-persisted batches back into the live spine so the *next*
checkpoint does not rewrite them. That is precisely DbspNet's rejected **Track B**, built and
shipped. On top of it they *also* have Track A (periodic checkpoints every 60 s + an input journal
between them). They did not choose between the tracks; the file-backed substrate made Track B free,
and Track A is layered on it.

Two premises in the DbspNet brief do not survive contact with the source:

1. `transaction_mode: always` is a **Delta-connector batching option**, not a durability setting;
   and **checkpointing is explicitly *disabled* during a Feldera transaction**.
2. In the ivm-bench configuration (`fault_tolerance` unset), Feldera's `FtConfig::checkpoint_interval()`
   returns `None` — **Feldera writes no checkpoints at all during that benchmark run.**

---

## Q1 — Is the checkpoint incremental? Does it copy batch data or write a manifest?

**It writes a manifest.** [verified]

The `Checkpoint` trait's own doc comment states the model outright:

> `crates/dbsp/src/circuit/checkpointer.rs:611-614`
> ```
> /// Trait for types that can be check-pointed and restored.
> ///
> /// This is to be used for any additional state within circuit operators
> /// that's not stored within a batch (which are already stored in files).
> ```

The spine's checkpoint (`Spine::save`, `crates/dbsp/src/trace/spine_async.rs:2199-2266`) does:

1. `pause_new_merges()` → split batches into not-merging / merging (`spine_async.rs:2219`).
2. `persist_batches()` maps each batch through `Batch::persisted()`
   (`spine_async.rs:2205-2217`): if the batch is already file-backed this returns `None` and the
   batch is left alone; if it is an in-memory `Vec` batch it is copied into a new file batch
   (`crates/dbsp/src/trace/ord/fallback/wset.rs:356-366`, and the three sibling
   `fallback/{indexed_wset,key_batch,val_batch}.rs` impls).
3. The newly persisted not-merging batches are **put back into the live merger**
   (`spine_async.rs:2222`), with the comment:
   > `spine_async.rs:2220-2225` — *"Putting the persisted batches into the merger means that we don't
   > have to persist them again for the next checkpoint, saving time then. On the other hand, we do
   > have to read them back from disk to use them: no free lunch."*
4. It then collects only **file paths** (`batch.file_reader()…path()`, `spine_async.rs:2228-2239`)
   and writes two small files into the checkpoint directory: `pspine-<pid>.dat` (rkyv-serialized
   `CommittedSpine`, essentially the path list + filters/dirty flag) and `pspine-batches-<pid>.dat`
   (JSON `PSpineBatches { files: Vec<String> }`), `spine_async.rs:2252-2264`.

The batch files themselves live at the **storage root**, not in the checkpoint directory, and are
named `w<worker>-<unique>.feldera` (`crates/dbsp/src/storage/file/writer.rs:1160-1166`,
`crates/feldera-types/src/constants.rs:33-36`). Multiple checkpoints therefore *share* the same
physical batch files. `dependencies.json` in each checkpoint dir records the batch list it
references and the state files it owns
(`crates/feldera-types/src/checkpoint.rs:205-247`, written at
`crates/dbsp/src/circuit/checkpointer.rs:372-400`).

Every other stateful operator writes a small file, not O(state):

| operator | what it writes | cite |
|---|---|---|
| `Spine` (all traces: joins, aggregates, integrals, top-k, distinct) | path manifest only | `spine_async.rs:2199` |
| `Z1Trace` / `AccumulateZ1Trace` (the delayed traces behind `integrate_trace`) | delegates to `Spine::save` | `crates/dbsp/src/operator/dynamic/trace.rs:1102-1112`; `crates/dbsp/src/operator/dynamic/accumulate_trace.rs:1123-1133` |
| `Z1` (scalar/one-batch delay) | one rkyv value, `z1-<pid>.dat` | `crates/dbsp/src/operator/z1.rs:272-330` |
| nested subcircuit | its clock timestamp | `crates/dbsp/src/circuit/circuit_builder.rs:7483-7507` |
| `Output` | an empty `()` marker | `crates/dbsp/src/operator/output.rs:530-546` |

**There is no whole-operator state rewrite anywhere.** The `Checkpoint` trait exists specifically
for "state not stored within a batch", and in practice that state is scalars, clocks and filters.

Corollary [inference, but strongly grounded]: after the *first* checkpoint the entire spine is
file-backed, so an incremental checkpoint writes only the batches produced since the previous one —
O(delta), not O(state). This is the exact ceiling DbspNet measured for Track B (18.7 s → ~6.3 s) and
then declined to build.

---

## Q2 — Lifetime / GC of a batch file referenced by a retained checkpoint but dropped by compaction

**Two-level: Rust `Arc` refcount + delete-on-drop for live files; manifest-based mark-and-sweep for
checkpointed files.** [verified]

*Level 1 — refcount.* A file created during the run is deleted when the last `Arc<dyn FileReader>`
to it drops, **unless** `mark_for_checkpoint()` was called:

- `crates/storage/src/lib.rs:340-344` — *"The file will be deleted if the reader is dropped without
  calling `FileReader::mark_for_checkpoint`."*
- `crates/storage/src/lib.rs:366-371` — `mark_for_checkpoint` "prevent[s] the file from being deleted
  when it is dropped".
- Implementation: `DeleteOnDrop` with a `keep: AtomicBool`,
  `crates/dbsp/src/storage/backend/posixio_impl.rs:198-217`; `mark_for_checkpoint` sets `keep`
  (`posixio_impl.rs:134-136`).

So compaction dropping a batch that is *not* in any checkpoint deletes the file immediately.

*Level 2 — mark-and-sweep over manifests.* Once a batch is in a checkpoint, the checkpointer owns it.
`Checkpointer::gc_checkpoint` (`crates/dbsp/src/circuit/checkpointer.rs:522-594`):

- builds `batch_files_to_keep` from the `except` set (checkpoints pinned by an in-flight S3 sync),
- **plus the batches of the newest retained checkpoint**, with the comment *"its merger may still
  depend on them"* (`checkpointer.rs:562-572`) — this is the explicit handling of the
  compaction-vs-retention race,
- then for each evicted checkpoint deletes `gather_batches_for_checkpoint_uuid(cp) \ keep`
  (`checkpointer.rs:574-583`) and removes the checkpoint dir,
- `MIN_CHECKPOINT_THRESHOLD = 2` checkpoints are always retained (`checkpointer.rs:48-49`).

*Level 3 — startup sweep.* `gc_startup` (`checkpointer.rs:106-278`) walks the whole storage root,
computes the union of all batch files referenced by all checkpoints in `checkpoints.feldera`, and
deletes any file with a Feldera extension (`.feldera` / `.mut`) that nothing references. It
deliberately keeps files it does not recognise (`Disposition::KeepUnexpected`, `checkpointer.rs:200-204`).
It also logs (does not silently tolerate) references to files that have gone missing
(`checkpointer.rs:245-275`).

There is a deliberate safety property in `gather_batches_for_checkpoint_uuid`
(`crates/storage/src/lib.rs:217-267`): if `dependencies.json` is unreadable it **errors** rather than
returning an empty set — errors propagate so GC can never delete a batch it could not prove unused
(`checkpointer.rs:530-537`, `560-572`). There is a regression test named
`missing_pspine_batches_preserves_batches` (`checkpointer.rs:892-955`).

Summary: **refcount for the transient case, mark-and-sweep over reference manifests for the durable
case.** DbspNet's retired durable-identity stages 2–3 describe the same two mechanisms.

---

## Q3 — Durable operator identity across restart and across a program edit

This is where Feldera is most different from DbspNet, and the difference is structural.

**Two modes** (`crates/dbsp/src/circuit/dbsp_handle.rs:289-307`) [verified]:

- `Mode::Ephemeral` (the dbsp-crate default): persistent id = `"{worker_index}-{global_node_id_path}"`
  — i.e. **positional**, exactly DbspNet's `op-{i}`.
- `Mode::Persistent`: persistent id = `"{worker_index}-{compiler-assigned id}"`, read from a node
  label (`LABEL_PERSISTENT_OPERATOR_ID`).

`Node::persistent_id()` is at `crates/dbsp/src/circuit/circuit_builder.rs:980-1004`. Its doc:

> *"In persistent mode, this id is derived from the operator's persistent Id assigned to it by the
> compiler during circuit construction. This Id will remain stable across circuit restarts even if
> the circuit changes, as long as all ancestors of the node remain the same."*

**Every SQL pipeline runs in Persistent mode** — the adapter controller hard-codes
`.with_mode(Mode::Persistent)` (`crates/adapters/src/controller.rs:6067`). Ephemeral is for
hand-written dbsp circuits.

**The compiler-assigned id is a Merkle hash of the operator's whole upstream subtree.** [verified]

- `MerkleOuter` (`sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/backend/MerkleOuter.java`)
  visits every operator, serialises it to a canonical JSON that includes its class, its *depth*, its
  inner code (each inner IR node itself replaced by a hash of its generated Rust,
  `MerkleInner.java:32-38`), its metadata/column names for I/O nodes, and — when
  `includeInputs` is true — the **hashes of its input operators** (`MerkleOuter.java:170-232`).
- The hash function is **SHA-256** (`MerkleInner.java:21-30`; `org/dbsp/util/HashString.java:1-6`).
- Only the *global* (input-including) hash becomes the persistent id (`MerkleOuter.java:67-71`).
- The code generator emits `let hash = Some("<sha256>"); <handle>.set_persistent_id(hash);`
  (`ToRustVisitor.java:117-127, 500, 621, 863, 1020`).
- Streams leaving a recursive sub-circuit get a **state-format version string mixed into the hash**
  (`RECURSIVE_STATE_VERSION = "recursive-state-v2"`, `MerkleOuter.java:36-45`) so that a runtime
  change to the on-disk recursive-state format automatically changes the id, causing the state to be
  recomputed rather than misinterpreted. That is an explicit, deliberate answer to "a colliding
  stable id fails silently".

**Is the checkpoint portable across a program edit? Yes — that is the headline feature.** [verified + docs]

- The fingerprint check is **skipped in persistent mode** — `DBSPHandle::new`,
  `crates/dbsp/src/circuit/dbsp_handle.rs:1447-1463`:
  > *"TODO: We allow the circuit to change between suspend and resume in Persistent mode; we
  > therefore only validate the fingerprint in ephemeral mode"*

  (`Checkpointer::verify_fingerprint`, `checkpointer.rs:66-75`, is only called when
  `mode == Ephemeral`. The fingerprint itself is an FNV-1a hash over node type names,
  `crates/dbsp/src/circuit/fingerprinter.rs`, `circuit_builder.rs:8788-8795` — much weaker than the
  Merkle ids, and only used as the ephemeral-mode guard.)
- Restore is **partial by design**. `CircuitHandle::analyze_checkpoint`
  (`crates/dbsp/src/circuit/circuit_builder.rs:7866-7960`): every node's `restore()` is attempted;
  a `NotFound` is **not an error** — the node is added to a `need_backfill` set
  (`circuit_builder.rs:7917-7928`). The analysis then walks backward from the `need_backfill` nodes
  to find operators that must replay, stopping at a stream that *can* be replayed from a node that
  does have a checkpoint, or at an input node. New/changed operators are recomputed from the
  retained state of their unchanged ancestors.
- Guardrail against confusing "new operator" with "lost file": in persistent mode
  `verify_checkpoint_intact` (`checkpointer.rs:288-327`, called at `circuit_builder.rs:7902-7910`)
  cross-checks the checkpoint dir against the `state_files` list in `dependencies.json` and hard-fails
  on a *missing* file that was committed. So "file legitimately absent (new operator)" → backfill;
  "file committed but vanished" → loud error. This is exactly the loud/silent distinction DbspNet
  worried about, resolved by recording the expected file set.
- User-facing semantics: `docs.feldera.com/docs/pipelines/modifying.md` — the pipeline diffs the
  checkpointed program against the new one, reports added/removed/modified tables, views and
  connectors, and either awaits approval (`bootstrap_policy=await_approval`, the default), proceeds
  (`allow`), or refuses (`reject`) (`modifying.md:57-84`). Bootstrapping then discards invalidated
  state and recomputes new/modified views from existing state (`modifying.md:86-120`). There is a
  `/diff` endpoint that previews this without restarting (`modifying.md:325-360`).

Known costs they accept, stated in their own docs (`modifying.md:194-268`):
- A **runtime upgrade can change the plan and therefore the hashes**, triggering bootstrap even with
  unchanged SQL (Caveat 1).
- **Cross-view optimizations mean an unmodified view can still get a new hash** when another view
  changes (Caveat 3) — the Merkle id is over the *optimized* plan, not the SQL text.
- Bootstrapping requires all tables to be `materialized` (Limitation 1).
- **Bootstrapping does not work with `LATENESS`** at all — such pipelines must be rebuilt from
  scratch after any change (Limitation 2).
- Table column add/remove/rename clears the table (Limitation 3).
- UDF body changes are not detected (Limitation 4).

Delta vs DbspNet: DbspNet's positional `op-{i}` + hard-fail fingerprint is the same thing as
Feldera's **Ephemeral** mode. Feldera's Persistent mode is a strictly larger design — content-addressed
ids over the optimized plan, a fingerprint check deliberately turned *off*, missing state treated as
"backfill this operator", and an explicit state-file manifest so genuine loss stays loud.

---

## Q4 — Is there a WAL/journal? What does `transaction_mode: always` cost and guarantee?

### Journal: yes, and it is Track A.

`crates/adapters/src/controller/journal.rs` — a `Journal` is a directory of `<step>.bin`
msgpack records (`journal.rs:26-75`). One record per step: `StepMetadata { step, transaction_id,
remove_inputs, add_inputs, changed_inputs, input_logs, changed_outputs }` (`journal.rs:149-198`).

Crucially, **for most connectors the journal stores offsets, not data**:

> `journal.rs:200-219` — `InputLog.data` is *"filled in by input adapters that log actual data
> records (e.g. the HTTP and ad hoc query input adapters). For the other adapters, which only log
> metadata (such as record offsets), this field is `RmpValue::Nil`."*

Plus `InputChecksums { num_records, hash }` per endpoint so replay can verify it re-ingested the same
data (`journal.rs:243-252`). Feldera's WAL therefore relies on the *source* being replayable
(Kafka/Delta/S3 offsets) rather than durably storing the input rows itself.

Durability: `Journal::write` → `StorageBackend::write` → `create_named` + `write_block` +
`complete()` (`crates/storage/src/lib.rs:132-142`), and the POSIX `complete()` does
`file.sync_all()` then renames off the `.mut` suffix
(`crates/dbsp/src/storage/backend/posixio_impl.rs:272-301`). So **one fsync + rename per journaled
step**, of a small metadata record. The *parent directory* is not fsynced per step (only at
checkpoint commit, `checkpointer.rs:394`, `checkpointer.rs:414`), so on some filesystems the tail
directory entry is theoretically at risk. [verified]

Three FT levels [docs, `docs.feldera.com/docs/pipelines/fault-tolerance-overview.md:17-30` and
`fault-tolerance.md:200-217`]:
- *checkpoint and resume* — checkpoint on graceful stop only,
- *at-least-once* — periodic checkpoint (default 60 s), no journal,
- *exactly-once* — periodic checkpoint **plus** the journal, replay + output dedup on restart.

Defaults [verified]: `RuntimeConfig::default()` has `storage: Some(StorageOptions::default())` and
`fault_tolerance: FtConfig::default()` (`crates/feldera-types/src/config.rs:1177-1186`), and
`FtConfig::default()` is `{ model: None, checkpoint_interval_secs: Some(60) }`
(`config.rs:1280-1287`). `FtConfig::checkpoint_interval()` returns `None` unless `model.is_some()`
(`config.rs:1540-1555`). `RuntimeConfig` carries container-level `#[serde(default)]`
(`config.rs:849-851`), so a config that omits these fields gets exactly those values.
**So out of the box: storage on, checkpoints off.** FT is Enterprise-only.

### `transaction_mode: always` is not a durability setting.

`DeltaTableTransactionMode` is a **Delta Lake input connector** option
(`crates/feldera-types/src/transport/delta_table.rs:261-296`):

> *"If `transaction_mode` is set to `always`, the connector ingests the transaction log in a series
> of transactions, generating exactly one Feldera transaction for each entry in the table's
> transaction log."*

A *Feldera transaction* is an atomicity/batching boundary, not a durability boundary
(`docs.feldera.com/docs/pipelines/transactions.md:1-45`): during a transaction the pipeline ingests
without producing output; on commit it computes all view deltas at once. And:

> `transactions.md:218` — **"Checkpointing is disabled during a transaction. A checkpoint initiated
> during a transaction gets delayed until the transaction has committed."**

Verified in code: the controller does not even *request* a checkpointable state from connectors
while a transaction is active (`crates/adapters/src/controller.rs:4386-4396`), and
`CheckpointActivity::Delayed` lists "transaction in progress" as a reason
(`crates/feldera-types/src/checkpoint.rs:31-40`).

### Bearing on the head-to-head

I checked the ivm-bench harness in `d:\src\ivm-bench-bak` (the `d:\src\ivm-bench` checkout named in
memory is not present on this machine). The Feldera profile is
`src/containers/dbt-server/dbt-projects/feldera/profiles.yml`: `workers: 12`,
`compilation_profile: optimized`, `max_rss_mb`, `dev_tweaks.adaptive_joins` — **no
`fault_tolerance`, no `checkpoint_interval`**. `transaction_mode: always` appears only inside
`delta_table_input` connector configs in `dbt_project.yml`. [verified]

Therefore, in that benchmark: `FtConfig.model == None` → `checkpoint_interval() == None` → **no
automatic checkpoints are written during the run**, and any checkpoint requested would be delayed
until each transaction committed anyway. Feldera is paying for **storage spilling** (writing merged
batches ≥10 MiB), not for checkpointing. Forcing DbspNet's per-batch full-state checkpoint on for
"honesty" is not apples-to-apples — it is DbspNet paying a cost Feldera is not paying.

---

## Q5 (the big one) — Is checkpoint nearly free because traces are already file-backed?

**Confirmed, with one important qualification: it is nearly free *incrementally*, not absolutely,
and only because storage spilling is on by default.** [verified]

Mechanism. Batches are `FallbackWSet`/`FallbackIndexedWSet`/… = `enum Inner { Vec(..), File(..) }`
(`crates/dbsp/src/trace/ord/fallback/wset.rs` and siblings). Where a batch lands is decided by three
thresholds read from the runtime (`crates/dbsp/src/circuit/runtime.rs:1157-1245`):

| decision point | knob | default at low memory pressure | at higher pressure |
|---|---|---|---|
| output of a background **merge** | `min_merge_storage_bytes` ← `min_storage_bytes` | **10 MiB** | `0` (everything to storage) at Moderate+ |
| batch **inserted** into a spine by a foreground worker | `min_insert_storage_bytes` | `usize::MAX` (stay in RAM) | `min_storage_bytes` at Moderate, `0` at High+ |
| **transient** batch flowing between operators in a step | `min_step_storage_bytes` | `usize::MAX` (RAM) | `0` at Critical |

(`runtime.rs:1167-1183`, `1196-1218`, `1232-1243`; defaults documented at
`crates/feldera-types/src/config.rs:330-352`; selection logic at
`crates/dbsp/src/trace/ord/fallback/utils.rs:41-113`.)

So with default settings and no memory pressure, **every merged batch of ≥10 MiB is already a file
before any checkpoint happens**. In an LSM spine, that is nearly all of the *bytes* — only the small
low-level batches are in RAM. [inference from the two mechanisms above; I did not measure a real
workload.]

What a checkpoint actually writes, per worker:
1. every batch still in `Inner::Vec` → copied to a new `w*.feldera` file (`spine_async.rs:2205-2217`),
2. `pspine-<pid>.dat` + `pspine-batches-<pid>.dat`: **just the path lists** (`spine_async.rs:2252-2264`),
3. one small `z1-*.dat` per `Z1`, one clock file per nested circuit, an empty marker per output,
4. `dependencies.json` and a `CHECKPOINT` marker (`checkpointer.rs:346-400`),
5. an updated `checkpoints.feldera` catalog at the root (`checkpointer.rs:481-487`).

And because the freshly-persisted not-merging batches are handed back to the merger
(`spine_async.rs:2222`), the *next* checkpoint does not rewrite them. Steady-state incremental
checkpoint cost ≈ the batches created since the last checkpoint.

The blocking window is even narrower than that. `CheckpointBuilder::prepare()` (the operator-side
write) runs on the circuit thread and is explicitly instrumented as *"Time during which checkpointing
blocked pipeline execution"* (`crates/adapters/src/controller.rs:9612-9640`). The fsync/commit phase
runs on a **separate `feldera-checkpoint` thread** (`controller.rs:9641-9659`), and
`CheckpointCommitter::commit` is documented:

> `crates/dbsp/src/circuit/dbsp_handle.rs:2690-2693` — *"Committing a checkpoint ensures that its
> data is on stable storage. It can run in the background while the circuit processes more steps."*

Their own docs say a checkpoint is *"ordinarily a fast operation that takes several seconds"*
(`docs.feldera.com/docs/pipelines/fault-tolerance.md:145-152`), with the caveat that a Delta input
connector may force extra steps to reach a checkpointable position.

**So: yes — DbspNet's Track A/Track B dilemma is largely an artifact of pinning flat state in RAM.**
If state is already on disk in immutable files, "checkpoint" degenerates to fsync + write a path
list, and the question "how often do we checkpoint?" stops being a throughput question. Feldera did
not have to trade the two off; they got Track B from the substrate and then added Track A on top.

Also relevant to DbspNet's "spine cannot bound memory because 29.4% of state has no spine sibling":
in Feldera **every** row-carrying operator state goes through `Spine`. Aggregates keep an input trace
and an output trace (`crates/dbsp/src/operator/dynamic/aggregate.rs:1040-1060`); TOP-K / rank / lag /
row_number all use `add_accumulate_integrate_trace_feedback::<Spine<OB>>`
(`crates/dbsp/src/operator/dynamic/group.rs:342`); materialized views go through
`Z1Trace`/`AccumulateZ1Trace`, which delegate to `Spine::save`
(`operator/dynamic/trace.rs:1102`, `operator/dynamic/accumulate_trace.rs:1123`). I found **no
row-carrying operator state that cannot spill.** [verified]

---

## Q6 — Recovery cost; is state lazily paged back in?

**Restore is lazy. It opens files; it does not read them.** [verified]

`Spine::restore` (`crates/dbsp/src/trace/spine_async.rs:2268-2312`) reads the small
`pspine-<pid>.dat`, then for each named path calls `B::from_path(...)` and inserts the resulting
batch into the merger. `from_path` for a file batch is
`crates/dbsp/src/trace/ord/file/wset_batch.rs:486-499` → `Reader::open_with_filter`
(`crates/dbsp/src/storage/file/reader.rs:1855-1864`) → `Reader::new_with_filter`
(`reader.rs:1664-1768`), which:

- `get_size()`,
- reads exactly the **512-byte file trailer** (`reader.rs:1671-1679`),
- builds per-column descriptors from the trailer,
- locates (and optionally loads) the Bloom filter block.

Data blocks are then served on demand through the buffer cache
(`ImmutableFileRef` / `read_block`, `reader.rs:1752-1758`,
`crates/dbsp/src/storage/backend/posixio_impl.rs:137-155`). So restore is **O(number of batch
files)** small reads, not O(state bytes). Compare DbspNet's ~35 s full rebuild.

Then `analyze_checkpoint` computes which operators lack state and need a **targeted replay/backfill**
from upstream checkpointed traces (`circuit_builder.rs:7866-7960`, described in Q3). Cost is
proportional to the changed part of the plan, not the whole pipeline.

Two operational refinements [docs]:
- *Standby pipelines* continuously pull the latest checkpoint from S3 and "activate and resume
  processing within seconds" (`fault-tolerance-overview.md:36-50`).
- *Concurrent bootstrapping* (experimental) runs a second copy of the circuit that backfills new
  views in the background while the live circuit keeps serving, then atomically cuts over
  (`modifying.md:151-190`; machinery at `dbsp_handle.rs:944-1100`, `circuit_builder.rs:8218`
  `restore_concurrent`, `swap_state_with` at `circuit_builder.rs:1180-1184`).

---

## Q7 — Aggregator accumulator state: persisted or re-folded? Float-order sensitivity?

**Re-folded from the trace. Nothing to persist. And they deliberately avoid the linear
running-accumulator path for floating point, so DbspNet's bug class is designed out at the
compiler.** [verified]

*Runtime.* Non-linear aggregation keeps an input trace and recomputes each affected group in full.
`AggregateIncremental::eval_key` (`crates/dbsp/src/operator/dynamic/aggregate.rs:904-935`) seeks the
key in the input trace and calls `aggregator.aggregate_and_finalize(&mut CursorGroup::new(...))`;
`Fold::aggregate` starts from `self.init.clone()` and folds the whole group
(`crates/dbsp/src/operator/dynamic/aggregate/fold.rs:76-102`). The fold order is the trace's sorted
value order, which is deterministic and identical before and after a restore. There is no
accumulator field to serialize, and none of the operator `checkpoint()` impls write one.

*Compiler.* SQL aggregates have two lowerings: a `LinearAggregate` (a running weighted sum, cheap,
subtract-on-retract) and a `NonLinearAggregate` (refold). The linear form is **guarded by
`!this.fp()`** — i.e. it is never used when the result type is floating point:

- `fp()` = `resultType.is(DBSPTypeFP.class)` —
  `sql-to-dbsp-compiler/.../frontend/aggregates/AggregateCompiler.java:289-291`
- `SUM` — `AggregateCompiler.java:685`
- `SUM0` (`SUM` returning 0 for empty) — `AggregateCompiler.java:734`
- `AVG` — `AggregateCompiler.java:779`
- `STDDEV_POP/SAMP`, `VAR_POP/SAMP` (`doVariance`) — `AggregateCompiler.java:908`
- `COVAR_*`, `REGR_SXX/SYY` (`processCovar`) — `AggregateCompiler.java:1076`

That is precisely DbspNet's list (`SUM`/`AVG` over `DOUBLE`, `STDDEV`/`VAR`). Integer/DECIMAL
aggregates get the fast linear path; floats get the refold.

So: no persisted float accumulator ⇒ no silent wrong values after restore, and additionally no
retraction drift during normal operation. The cost is that a float `SUM` over a large group is
recomputed whenever the group changes — Feldera pays throughput to buy the correctness DbspNet had
to patch by persisting the accumulator.

I did **not** find a DDSketch/HLL-style sketch aggregate with persisted internal state in this tree,
so I cannot say how they would handle that case.

---

## Where this contradicts or challenges a DbspNet decision

### 1. "Track A over Track B" — Feldera built Track B, and it is the load-bearing part

`docs/design-incremental-persistence.md` framed Track B (durable batch identity + reference-manifest
snapshots + mark-and-sweep GC) as an alternative to Track A (WAL every batch, snapshot every N), and
Track A "killed" Track B.

Feldera has **both**, and they are not alternatives:
- Track B is the substrate: batch files at the storage root, checkpoints are manifests
  (`checkpointer.rs:372-400`, `spine_async.rs:2199-2266`), mark-and-sweep GC over those manifests
  (`checkpointer.rs:522-594`, `106-278`).
- Track A sits on top: `checkpoint_interval_secs` default 60 s plus a per-step input journal for
  exactly-once (`journal.rs`, `fault-tolerance.md:200-217`).

The DbspNet reasoning was "once snapshots are periodic, the flat-vs-spine save difference amortises
away, so Track B has no payoff." That reasoning is sound *given flat traces in RAM*. Feldera's
evidence is that the causality runs the other way: build the file-backed substrate and periodic
snapshotting stops being a compromise — you get a genuinely incremental checkpoint you can afford to
take often, *and* recovery becomes lazy (Q6) rather than a 35 s rebuild. DbspNet's measured Track B
ceiling (18.7 s → 6.3 s) is only the *save* half of the prize; the restore half (35 s full rebuild →
open N files and read their 512-byte trailers) was never in the Track A ledger.

### 2. "Stop growing spine" — the trace family and the persistence story are the same decision

DbspNet keeps flat as default partly because spine's LSM byte-reuse (70.6% spine-backed, 91.3% of
those unchanged) only pays off for a per-batch checkpoint, which Track A deleted. Feldera's source
says that reuse is not a checkpointing optimization — it is the *reason the state can leave RAM at
all*. Their spine is the only trace family (`crates/dbsp/src/trace/spine_async.rs`), and every
stateful operator goes through it, including the four DbspNet listed as having no spine sibling
(`IntegrateOp` → `Z1Trace`/`AccumulateZ1Trace` over `Spine`; window/offset/rank → `group.rs:342`).
The 29.4%-of-state-cannot-spill problem is a consequence of the incomplete spine port, not evidence
against spine.

The measured "+14% bulk step, spine loses to flat 1.4–2.5× as a substrate at W=24" stands as a real
cost. But Feldera's structure suggests the axis is not "flat vs spine for throughput" — it is
"flat means state is pinned in RAM, which forces O(state) checkpoints and O(state) restores." Note
also that Feldera's LSM is *async*: merges run on background threads (`AsyncMerger`,
`spine_async.rs`), with backpressure rather than bulk-on-threshold compaction
(`insert_without_blocking` / `backpressure_wait`, `spine_async.rs:2154-2172`), and merges are paused
only at checkpoint (`pause_new_merges`, `spine_async.rs:2219`). DbspNet's bulk-on-threshold
compaction may account for part of the +14%.

### 3. "Deferred operator identity: a colliding stable id fails silently where positions fail loudly"

Feldera solves the silence directly rather than avoiding stable ids:
- Ids are **SHA-256 Merkle hashes over the optimized plan subtree**, not names — so a "collision"
  means the two operators genuinely compute the same thing from the same inputs, which is exactly
  when sharing state is correct (`MerkleOuter.java`, `MerkleInner.java:21-30`).
- A **missing** state file is not silence and not failure: it means "this operator is new", and the
  restore analysis backfills it from upstream retained state (`circuit_builder.rs:7917-7960`).
- **Silent loss** is caught by an explicit expected-file manifest: `dependencies.json.state_files`
  lists every file the checkpoint owned at commit time, and `verify_checkpoint_intact` hard-fails in
  persistent mode if any is gone (`checkpointer.rs:288-327`, `circuit_builder.rs:7902-7910`). The
  comment there says exactly this: distinguish "operator is new since the checkpoint" from "state
  was committed but the file vanished."
- Format-version changes that would make old state *uninterpretable* are folded into the hash
  (`RECURSIVE_STATE_VERSION`, `MerkleOuter.java:36-45`), so stale state can never be mis-restored —
  it just becomes a backfill.
- Because ids are content-addressed, the **plan fingerprint check is switched off** in persistent
  mode (`dbsp_handle.rs:1447-1463`) — a program edit is an ordinary, supported, user-visible event
  (`/diff`, `AwaitingApproval`, `bootstrap_policy`), not a hard failure.

DbspNet's positional-with-hard-fail scheme *is* Feldera's Ephemeral mode. It is the correct
conservative choice for a system without the backfill machinery. But the "colliding stable id fails
silently" objection is answerable, and Feldera shows how: content-address the id, record the expected
file set so real loss is loud, and make "no state for this operator" a first-class recoverable state.
The cheapest borrowable piece is the **state-file manifest** (`dependencies.json.state_files`):
it converts "silent" into "loud" without needing stable ids at all.

### 4. Benchmark methodology — the immediately actionable one

The brief states that ivm-bench runs Feldera with `transaction_mode: always`, i.e. "persistence
*inside* the batch window — so an honest comparison forces our checkpoint on too."

Source says otherwise on both halves:
- `transaction_mode: always` is a Delta-connector option controlling Feldera-transaction boundaries
  (`transport/delta_table.rs:261-276`); it says nothing about durability.
- **Checkpointing is disabled during a transaction** (`transactions.md:218`;
  `controller.rs:4386-4396`).
- The ivm-bench Feldera profile sets no `fault_tolerance`, so `checkpoint_interval()` is `None`
  (`config.rs:1540-1555`) and **no checkpoint is written during the run** at all.

What Feldera *is* paying in that benchmark is storage spilling: writing merged batches ≥10 MiB to
`w*.feldera` files during background merges (`runtime.rs:1167-1183`). That is a much smaller and
qualitatively different cost than DbspNet's O(state) `Snapshot.WriteAsync` per batch. If the goal is
apples-to-apples, the honest configurations are either (a) DbspNet with checkpointing off, versus
Feldera as configured; or (b) both with periodic checkpoints enabled — Feldera with
`fault_tolerance: at_least_once` and a matching `checkpoint_interval_secs`.

---

## Things I could not determine

- **Actual bytes/seconds a Feldera checkpoint costs on the ivm-bench SF=3 shape.** The mechanism
  bounds it to "batches created since the last checkpoint + a few KB of manifests", but I did not run
  anything (Feldera does not build on Windows and the brief says not to build). The `size` field on
  `CheckpointMetadata` (`checkpoint.rs:160-165`) and the `feldera_checkpoint_delay_seconds` metric
  (`controller.rs:1648-1661`) would answer it directly from a real run.
- Whether the enterprise `checkpoint-sync` uploader is content-diffing (only new `w*.feldera` files).
  The implementation is not in this OSS tree (`controller/sync.rs:65` notes enterprise builds supply
  it); immutable root-level batch files would make a diffing upload natural, but I could not verify it.
- How a persisted **sketch-style** aggregate (HLL / DDSketch equivalents) would be checkpointed —
  I found no such aggregate in this tree.
- The exact in-RAM residue of a spine at steady state (i.e. how many bytes a first checkpoint would
  actually have to write). It follows from the 10 MiB merge threshold and the level structure
  (`size_to_level`, `spine_async.rs:2320-2360`), but I inferred it rather than measured it.

---

# Appendix — Resolution: what DBSP transaction commit actually does

Raised by the coordinator against my "Feldera writes zero checkpoints during the ivm-bench run"
claim: `ivm-bench/src/containers/dbt-server/services/feldera_client.py:185-189` asserts that at
commit "the transaction must walk every operator (~47k for our SQL) and **persist its state to
storage**", backed by an observation of `transaction_status: CommitInProgress` for ~80 minutes after
quiescence at SF=100 PARALLEL=1 batch 2.

**Verdict: the observation is real and the "walk every operator" half of the comment is exactly
right. The "persist its state to storage" half is where it over-reads. My original claim survives,
but it needs one narrowing (§A4) — "no checkpoint" is not the same as "no disk I/O".**

## A1 — What transaction commit does

Commit is the **computation** phase of the transaction, not a persistence phase. The scheduler's own
trait doc is unambiguous:

> `crates/dbsp/src/circuit/schedule.rs:219-226`
> *"During the in-progress phase, each operator gets to decide how much of the input to process.
> Some operators may accumulate inputs to process them later.*
> *During the committing phase, the scheduler forces operators to process their inputs to completion
> by invoking `flush` on each operator. Once all predecessors of an operator have finished processing
> inputs for the current transaction, the scheduler invokes `flush` of the operator. It tracks the
> frontier of flushed operators and reports `is_commit_complete()` as true once all operators have
> been flushed."*

Mechanically [verified]:

- `start_commit_transaction` sets `TransactionPhase::Committing(self.tasks.len())` — the count is
  the **number of circuit nodes** (`crates/dbsp/src/circuit/schedule/dynamic_scheduler.rs:467-473`).
- Each `Task` carries a `FlushState`: `UnflushedDependencies(n)` → `Started(Option<Position>)` →
  `Completed(Option<Position>)` (`dynamic_scheduler.rs:71-80`).
- In `spawn_task`, when committing and a node's predecessors have all flushed, the scheduler calls
  `circuit.flush_node(node_id)` and moves it to `Started`, then evaluates the node normally
  (`dynamic_scheduler.rs:424-434`). After each `eval`, `circuit.is_flush_complete(node_id)` is polled
  and, when true, the node moves to `Completed` and its successors become flushable
  (`dynamic_scheduler.rs:653-678`).
- `Operator::flush` is documented as *"Notifies the operator that all of its predecessors have
  produced all outputs for the current transaction. Operators that wait for all inputs to arrive
  before producing outputs (e.g., join, aggregate, etc.) can use this notification to start
  processing inputs the next time `eval` is invoked."*
  (`crates/dbsp/src/circuit/operator_traits.rs:349-364`).
- The controller drives it as a plain loop of `circuit.step()` until `Response::CommitComplete(true)`
  (`crates/dbsp/src/circuit/dbsp_handle.rs:1706-1748`), polling `commit_progress()` only to update
  the status line (`crates/adapters/src/controller.rs:3846-3876`).

So the ~47k-operator walk is real, and it is the walk in which **the views are actually computed**.
Under `transaction_mode: always`, ingestion during the transaction does almost nothing — the docs
say *"During a transaction, the pipeline ingests incoming data without producing output, performing
only minimal processing such as resolving primary keys and indexing inputs"*
(`docs.feldera.com/docs/pipelines/transactions.md:88`). All the join/aggregate/distinct work is
deferred to commit. An 80-minute `CommitInProgress` at SF=100 is the incremental evaluation of the
50-view DAG over the whole accumulated batch. **That is the same work DbspNet does in its "step".**

This also explains why the ivm-bench quiescence detector originally fired early: input records had
been ingested (`buffered_input_records == 0`) and no output connector was moving, because under the
transaction model nothing has flowed through the circuit yet — it is all parked in accumulator
spines.

## A2 — Is it the `Checkpoint`/`Spine::save` machinery? No. Is the `Accumulator` involved? Yes.

The trace agent's `Accumulator` lead is correct, and it is the mechanism that makes commit
operator-shaped [verified].

`Accumulator<B>` (`crates/dbsp/src/operator/dynamic/accumulator.rs:164-190`) holds a private
`state: Spine<B>`. Its `eval_owned` (`accumulator.rs:293-330`):

- while the transaction is in progress, `self.state.insert(batch).await` — accumulate, emit `None`;
- on flush, it allocates a fresh empty `Spine`, `std::mem::swap`s it with `self.state`, and emits
  the accumulated spine downstream as `Some(spine)` (`accumulator.rs:317-325`).

That release is an **ownership transfer of an in-memory/on-disk spine handle** — a `mem::swap`, no
serialization, no `save()`. The expensive part is not the accumulator's flush; it is the downstream
joins/aggregates/distincts consuming that whole accumulated spine for the first time.

The stream-level doc says exactly what the accumulator is for, including its storage relationship:

> `crates/dbsp/src/operator/accumulator.rs:26-32`
> *"This operator is a key part of efficient processing of long transactions. It is used in
> conjunction with stateful operators like join, aggregate, distinct, etc., to supply all inputs
> comprising a transaction at once, avoiding computing mutually canceling changes.*
> *Using `Spine` to accumulate changes ensures that during a long transaction changes are pushed to
> storage and get compacted by background workers."*

That last sentence is the grain of truth in the ivm-bench comment: **data does reach storage during a
long transaction** — but through ordinary spine spilling for memory management, not through the
checkpoint path.

**The checkpoint path is provably not involved** [verified]:

- `Batch::persisted()` — the only function that force-writes an in-memory batch to a file — has
  **exactly one call site in the entire tree**: `Spine::save`
  (`crates/dbsp/src/trace/spine_async.rs:2212`).
- `Trace::save` has **exactly two call sites**, both inside `Operator::checkpoint`
  (`crates/dbsp/src/operator/dynamic/trace.rs:1111`,
  `crates/dbsp/src/operator/dynamic/accumulate_trace.rs:1132`).
- `Operator::flush` / `flush_node` never touch the storage backend.

And a checkpoint **cannot even start** during a transaction, commit included:
`RunningCheckpoint::start` calls `Controller::can_suspend()` first
(`crates/adapters/src/controller.rs:9480-9484`), and `can_suspend()` pushes
`TemporarySuspendError::TransactionInProgress` whenever
`get_transaction_state() != TransactionState::None` (`controller.rs:8810-8812`), which the caller
treats as "defer, retry later" (`controller.rs:4067-4103`). Feldera flags this as a known problem:

> `crates/adapters/src/controller.rs:4327-4331` — *"FIXME: the last point means that checkpoints can
> get delayed indefinitely if the user runs end-to-end transactions."*

That FIXME describes the ivm-bench shape precisely.

## A3 — What `commit_progress` counts

**Operators, plus a records-scanned position for the ones currently in flight. Not bytes, not
batches, not files.** [verified]

`CommitProgress` is three collections keyed by `NodeId`
(`crates/dbsp/src/circuit/schedule.rs:103-112`), populated straight from each task's `FlushState`
(`dynamic_scheduler.rs:480-501`):

| field | source `FlushState` | transitions when |
|---|---|---|
| `remaining` | `UnflushedDependencies(_)` | initial state for every node at `start_commit_transaction` |
| `in_progress` | `Started(pos)` | scheduler calls `flush_node` on a node whose predecessors have all flushed |
| `completed` | `Completed(pos)` | `circuit.is_flush_complete(node_id)` returns true after an `eval` |

`CommitProgressSummary` (`crates/feldera-types/src/transaction.rs:22-37`) is
`{completed, in_progress, remaining}` as **operator counts**, plus
`in_progress_processed_records` / `in_progress_total_records`, computed as the sum of
`Position.offset` and `Position.total` over the in-flight nodes only (`schedule.rs:152-175`).
`Position { total: u64, offset: u64 }` is a **cursor position over the records the operator is
scanning** (`crates/dbsp/src/trace/cursor.rs:37-40`; `Cursor::position()` at `cursor.rs:343`),
surfaced by operators through `Operator::flush_progress` (`operator_traits.rs:366-375`; e.g.
`crates/dbsp/src/operator/async_stream_operators.rs:178, 360, 547, 732, 926`).

`WorkersCommitProgress::summary` **sums across workers**
(`crates/dbsp/src/circuit/dbsp_handle.rs:1422-1438`), so the "~47k" figure is
`operators-per-worker × 12 workers`, not 47k distinct SQL operators.

The progress bar is literally "how far has the evaluation frontier advanced through the operator
DAG" — a compute progress meter, not a bytes-written meter.

## A4 — What is and is not durable at the end of an ivm-bench batch

The distinction the coordinator asked for — "state reached storage" vs "a recoverable checkpoint
exists" — turns out to be exactly the right cut, and Feldera separates them with a single bit.

**State that reached storage** [verified]. During ingest and commit, batches spill to
`w<worker>-<id>.feldera` files at the storage root by two routes:
- background merges whose output is ≥ `min_storage_bytes` (default **10 MiB**) —
  `pick_merge_destination` (`crates/dbsp/src/trace/ord/fallback/utils.rs:86-113`), consulted by the
  fallback batch merge builders (`fallback/{wset,indexed_wset,key_batch,val_batch}.rs`);
- "eager spill" on insert when memory pressure raises the threshold — `Spine::maybe_flush_batch`
  (`crates/dbsp/src/trace/spine_async.rs:2400-2445`), via `pick_insert_destination`.

So at the end of an ivm-bench batch a large fraction of the ~GB-scale state genuinely is bytes on
disk. Feldera is doing real write I/O. My original phrasing ("Feldera is paying for storage
spilling, not for checkpointing") stands, but I want to be explicit that this is not free.

**Why that is nevertheless not a checkpoint** [verified]. Those files are *temporaries*. A file
returned by `FileWriter::complete()` carries `DeleteOnDrop { keep: false }` and is `unlink`ed when
its last `Arc<dyn FileReader>` drops (`crates/dbsp/src/storage/backend/posixio_impl.rs:198-217`;
contract at `crates/storage/src/lib.rs:340-344`). The **only** thing that flips `keep` on a batch
file is `Batch::file_reader()` —

```
// crates/dbsp/src/trace/ord/file/wset_batch.rs:481-484
fn file_reader(&self) -> Option<Arc<dyn FileReader>> {
    self.file.mark_for_checkpoint();
    Some(self.file.file_handle().clone())
}
```

(and the identical impls in `file/{indexed_wset_batch,key_batch,val_batch}.rs:492,363,384`) — and
`file_reader()` has exactly one caller: the path-collection loop inside `Spine::save`
(`spine_async.rs:2228-2239`). **No checkpoint ⇒ no `mark_for_checkpoint` ⇒ every spilled batch file
is still marked for deletion.**

Consequently, at the end of an ivm-bench batch:

| | present? |
|---|---|
| batch bytes physically on disk | **yes** (merge/eager spill, ≥10 MiB batches) |
| those files marked to survive their `Arc` (`keep=true`) | **no** |
| `pspine-*.dat` / `z1-*.dat` per-operator state | **no** |
| `dependencies.json`, `CHECKPOINT` marker, checkpoint dir | **no** |
| entry in `checkpoints.feldera` catalog | **no** |
| journal (`<step>.bin`) | **no** (exactly-once FT not enabled) |
| **anything a restart could resume from** | **no** |

On a clean exit the spines drop and the files are unlinked; on a crash they are left behind and
`gc_startup` deletes them on the next start as unreferenced `.feldera` files
(`crates/dbsp/src/circuit/checkpointer.rs:198-227` — `is_feldera_filename` ⇒ `Disposition::Remove`).
Recoverable state after an ivm-bench batch: **zero**.

**Therefore the honest DbspNet-side equivalent:**

1. Feldera's `CommitInProgress` maps to **DbspNet's step / batch computation**, not to DbspNet's
   checkpoint. The 80 minutes is the workload, not overhead. Comparing it against
   DbspNet's step time is apples-to-apples; comparing it against DbspNet's step *plus* an O(state)
   `Snapshot.WriteAsync` is not.
2. The write I/O Feldera does perform is **memory-management spilling**, whose DbspNet analogue is
   `SpineSpillConfig` — not `Snapshot.WriteAsync`. Flat traces do no such spilling, which is another
   way of saying DbspNet buys lower I/O by requiring the state to fit in RAM.
3. If you want a durability-inclusive comparison, both sides must opt in: DbspNet with its snapshot
   policy, Feldera with `fault_tolerance: at_least_once` (or `exactly_once`) and a stated
   `checkpoint_interval_secs`. Note that with `transaction_mode: always` Feldera's checkpoints would
   land only *between* transactions (`controller.rs:8810`, `9480`), i.e. once per ivm-bench batch —
   which, if you want a per-batch durability comparison, is actually the right cadence, and is a
   configuration ivm-bench could adopt.
4. Whatever the configuration, "recoverable" is the criterion that matters and it is binary here.
   Feldera as ivm-bench runs it ends each batch with **no recovery point**. If DbspNet is
   checkpointing per batch, it is ending each batch with a recovery point Feldera does not have, and
   paying ~18.7 s for it.

## A5 — What I am retracting / narrowing

- **Retracted:** nothing. "No checkpoint is written during the ivm-bench run" holds, and is now
  established by three independent mechanisms (no FT model ⇒ `checkpoint_interval() == None`;
  `can_suspend()` blocks any checkpoint start while a transaction is open; and no batch file ever
  gets `mark_for_checkpoint`).
- **Narrowed:** my summary line "Feldera is paying for storage spilling, not for checkpointing"
  understated the spilling. It is substantial write I/O on a multi-GB state, not a footnote. The
  correct statement is that Feldera's disk I/O during an ivm-bench batch is **spill I/O with no
  recovery value**, whereas DbspNet's per-batch snapshot I/O produces a recovery point. Same
  I/O ledger heading; completely different goods purchased.
- **Corrected in the ivm-bench comment:** "the transaction must walk every operator (~47k for our
  SQL) **and persist its state to storage**". The walk is real and is the dominant cost; the
  persistence attribution is wrong. Suggested rewording: *"…the transaction must walk every operator
  (~47k across all workers) and evaluate it against the accumulated input; this is where all view
  computation happens under `transaction_mode: always`. Batches spill to storage as a side effect of
  memory management, but no checkpoint is written."*
- **Also worth recording:** `~47k` is `CommitProgressSummary` summed over 12 workers
  (`dbsp_handle.rs:1422-1438`), so it is roughly 4k circuit nodes per worker, not 47k SQL operators.

## A6 — Not determined

- Whether the ~80 minutes was dominated by compute or by spill I/O. Both happen in that window and
  the source cannot apportion them. `feldera_operator_commit_latency_microseconds`
  (`circuit_builder.rs:7855-7862`, recorded per node) and the `WRITE_BLOCKS_BYTES` counter
  (`controller.rs:9611`, `9733`) would separate them from a real run's support bundle.
- Whether Feldera's transaction-commit evaluation is *faster* than the equivalent continuous-mode
  evaluation for this workload. The docs claim it is (avoiding mutually-cancelling intermediate
  updates, `transactions.md:24-33`; `accumulator.rs:26-30`), and DbspNet has no equivalent
  accumulate-then-evaluate mode, but I did not measure it. If true, this is a *separate* axis worth
  its own investigation: it would mean part of the batch-1 gap is Feldera evaluating a coalesced
  delta where DbspNet evaluates a sequence of deltas.
