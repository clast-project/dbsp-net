# Persistence and recovery — design sketch

Approaches (A), (B), and (C) below are now implemented in
`DbspNet.Persistence`. (D) — log-structured trace — remains a future
direction. The historical design discussion follows; see "Current
state" at the bottom for what shipped.

The natural checkpoint point is a **step boundary**: between ticks every
operator is in a consistent "end-of-tick T" state.

## Four approaches, ordered by engineering effort

### A. Input replay only — no state serialization

Persist every input delta to a durable log keyed by tick number + table.
On restart, rebuild the circuit from scratch and replay the whole log.

- **Pros:** simplest possible; no per-operator serialization story needed;
  bootstraps the other approaches (WAL is needed anyway for a hybrid).
- **Cons:** recovery time = O(total history). Degrades badly for
  long-running circuits; fine for bounded workloads.

### B. End-of-tick state snapshot

Walk every stateful operator and serialize its state to disk between
ticks. Restart rebuilds the same circuit from the plan, then loads each
operator's state.

- **What needs to be serialized today:** the two trace types, the two
  aggregator caches (`_aggCache`, `_stateCache`) inside
  `IncrementalAggregateOp`, the per-aggregator `object?` state blobs
  produced by `SqlAggregator.Update` (SUM/COUNT/AVG state classes),
  `RecursiveCteOp._r` and `_previousResult`, and any pending input
  (`InputHandle._pending`).
- **What we'd need to add:**
  1. **Stable operator identity** across restarts. Today operators are
     fresh objects from `PlanToCircuit.Compile` with no IDs — a snapshot
     chunk can't find its owner on restart. Fix: give every operator a
     deterministic path-in-the-plan identifier.
  2. **Row codec for `StructuralRow`**. Rows are `object?[]` today; a
     snapshot needs per-schema encoding. The generic version boxes /
     dispatches on runtime types; Feldera's answer is to generate a
     per-schema columnar layout at compile time.
  3. **Atomic write.** Tmp-file + rename, or a manifest pointer that
     swaps to the new snapshot in one operation.
- **Recovery:** O(snapshot size), independent of history length.

### C. Snapshot + WAL hybrid

(A) and (B) combined: periodic full snapshots, plus a WAL of deltas since
the last snapshot. Classical database approach. Bounds both recovery time
(proportional to snapshot size + unsaved deltas) and snapshot cost
(amortized across N ticks).

This is roughly what Feldera does, tied to input-source offsets (Kafka,
etc.) so "resume from offset X" and "load snapshot at tick T" stay
consistent.

### D. Log-structured trace ("spine")

The big architectural move: stop using mutable dicts for `Trace`, instead
keep a sequence of immutable *batches* (LSM-style), with periodic
compaction. The name "spine" comes from Differential Dataflow, DBSP's
research ancestor — the type there is literally `Spine<B>`, a stacked
sequence of immutable batches in order of age/size.

- Checkpointing becomes almost free: each batch is already a file; a
  snapshot is a manifest of batch-file paths.
- Unlocks out-of-memory state (spill batches to disk), cheap compaction,
  and time travel ("what did tick T look like?").
- Not a bolt-on — a meaningful re-architecture of the Core trace
  abstraction. Every stateful operator (`IncrementalJoinOp`,
  `IncrementalAggregateOp`, `IncrementalLeftJoinOp`, `DistinctOp`,
  `RecursiveCteOp`) reads traces, so all of them would move onto the
  new abstraction.
- `docs/skipped.md` already flags "[P2] Spine-backed Trace" and
  "[P2] Persistent storage backends for Trace" as gestures in this
  direction.

## Could we build the spine on a generic LSM (RocksDB, etc.)?

Considered. Mismatch is specifically in **merge semantics**. A spine
isn't storing (key → latest-value); it's storing
(key, value, time) → **weight in an abelian group**, and merging two
batches for the same (key, value, time) means *adding the weights* and
dropping entries whose weight becomes zero — commutative-additive merge
over a ring. Generic LSMs are built around "last write wins" with
user-defined merge operators as an extension point. You *can* encode
additive weights through RocksDB's `MergeOperator`, but several things
don't fit:

- **Time-frontier consolidation has no LSM analogue.** Once you advance
  the `since` frontier past time t, spine updates at the same (key, value)
  below t can collapse into one entry whose weight is their sum —
  reducing record count, not just bytes. RocksDB's compactor doesn't
  know about frontiers and won't schedule consolidation that way.
- **Batch-at-a-time cursors.** DBSP operators (especially bilinear joins)
  iterate whole batches in order, not single keys. RocksDB's iterator
  API gives you a single merged view; per-SST cursors exist but are
  below the supported abstraction line.
- **Row layout.** Spine batches want typed, ideally columnar layouts so
  iteration is cheap. RocksDB stores opaque bytes — serialize /
  deserialize every read.
- **Compaction policy coupling.** LSM compactors are tuned for read
  amplification over key-value data; spine compactors are tuned for
  consolidation-on-frontier-advance, driven by circuit progress.

Technically buildable on RocksDB, but you'd reimplement the interesting
half (frontier, consolidation, typed layouts) and pay for the half you
don't use. The more natural generic substrate is one layer lower: an
append-log / blob store (local files, or S3-shaped) as the "persist a
batch" primitive, with spine structure + merge policy + row codec built
on top. Feldera ran their own storage layer rather than embedding a KV
engine — probably a signal about where the abstraction line actually
cuts.

## Cross-cutting concerns (regardless of approach)

- **Circuit determinism.** Restart must build an identical topology.
  Plan fingerprinting protects against optimizer drift — include a hash
  of the compiled plan in the checkpoint and refuse to load into a
  different one.
- **Input-source offsets.** Checkpoint has to pair operator state with
  "last input offset seen from each source," otherwise you replay
  already-folded deltas on recovery.
- **Output idempotency.** On restart you may re-emit output. Downstream
  either needs to tolerate it, or the system tracks last-emitted tick
  per sink and suppresses duplicates.

## Recommended staging

1. **(A) input replay.** Honest, mechanical proof of concept. Persist
   every input delta with tick number; implement replay. Validates the
   determinism / offset / plan-fingerprint plumbing.
2. **(B) end-of-tick state snapshots.** Adds per-operator
   serialization. Solves recovery time.
3. **(D) spine as its own project.** Rearchitects the Core trace
   abstraction; enables out-of-memory state and gives (C) almost for
   free.

(C) — snapshot + WAL hybrid — is what production looks like, but falls
out of (A) + (B) naturally.

## Current state

| Approach | Status | Entry points |
|---|---|---|
| (A) input replay | Shipped | `WalRecorder(query, walStore)` |
| (B) end-of-tick snapshot | Shipped | `Snapshot.Write` / `Snapshot.Read` |
| (C) snapshot + WAL hybrid | Shipped | `WalRecorder(query, walStore, snapshotStore)` + `WalRecorder.WriteSnapshot()` |
| (D) log-structured trace | Future | — |

### Storage abstraction

The persistence layer is built on `IBlobStore` — a thin abstraction
modeled on cloud object stores (S3 / GCS / Azure Blob):

```csharp
interface IBlobStore {
    Stream OpenWrite(string key);   // atomic on Dispose
    Stream OpenRead(string key);
    bool Exists(string key);
    void Delete(string key);
    IEnumerable<string> ListKeys(string prefix);
}
```

Keys are slash-delimited (`"snap-5/op-3/trace.arrows"`). The single
durability primitive is **atomic single-blob write**: until
`Dispose`, the blob doesn't exist (or has its old value); on
`Dispose`, the new value appears atomically. This contract is true
of S3/GCS/Azure PUTs and is implemented locally by
`LocalFileBlobStore` via tmp+rename.

There are no directory operations, no atomic rename, no batched
multi-blob commits. To replace state across many blobs, callers
write all the new blobs first under their final keys, then commit by
writing a small pointer blob (`current.txt`) last — the partial
intermediate state is invisible until the pointer commits.

**Convenience overloads.** Every public entry point has both an
`IBlobStore` form and a `string`-path form; the string form opens a
`LocalFileBlobStore` rooted at the given directory. New callers
should prefer the explicit `IBlobStore` form so a future cloud impl
swaps in without a code change.

**Built-in impls.** `LocalFileBlobStore` (filesystem) and
`InMemoryBlobStore` (process-lifetime; useful as a test double for
cloud-shaped backends and where filesystem I/O would slow tests
down). Both pass the same `BlobStoreContractTests` conformance suite
— including the cloud-relevant cases like "reads during in-flight
write see the prior value."

**Cloud impls.** Out-of-the-box this ships only the local + in-memory
impls; cloud-flavored stores (S3, GCS, Azure) are expected to live in
separate projects that depend on `DbspNet.Persistence` and implement
`IBlobStore`. The `OpenWrite` stream is the natural place to back a
multipart upload — each `Write` queues a part, `Dispose` finalizes
and the blob becomes visible. Any cloud impl that passes
`BlobStoreContractTests` will work with the rest of the persistence
layer end-to-end.

### What ships in (B)

Five stateful operators implement `ISnapshotable`: `DistinctOp`,
`IncrementalAggregateOp`, `IncrementalJoinOp`,
`IncrementalLeftJoinOp`, `RecursiveCteOp`. Each owns Z-set / indexed-
Z-set traces (and, for the aggregate, per-group caches that bootstrap
from the trace on `Load`). Codecs are wired via
`PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance)`; on-disk
format is Arrow IPC under per-operator `op-{i}/` subdirectories with a
top-level `manifest.json` recording schema version, plan fingerprint
(operator type sequence), and tick.

The `_aggCache` / `_stateCache` problem from the original B sketch is
solved by *not* serialising them: on `Load` the operator walks the
restored trace and calls `aggregator.Update(ref state, None, group,
group)` per group — which is the existing increment path with a fresh
state — so SUM/COUNT/AVG/MIN/MAX all converge to the right
steady-state without per-state-class codecs.

**Layout and retention.** Each snapshot lives under its own
`snap-{tick}/` key prefix in the store, with the per-op `op-{i}/`
contents and the snapshot's `manifest.json`. A top-level
`{snapshotDir}/current.txt` names the latest snap-T and is the source
of truth — `Snapshot.Read` reads it to find the loadable snapshot.
`Snapshot.Write(circuit, dir, retainCount: N)` keeps the N
most-recent snapshots; older ones are pruned after the new snapshot
commits. Default `retainCount` is 1 — only the just-written snapshot
survives, matching the prior overwrite semantics.
`Snapshot.ListSnapshots(dir)` returns retained ticks in ascending
order; useful for debugging and for time-travel queries (load an
older `snap-T` directly by its manifest).

**Cloud-native commit.** Per-op blobs and the `manifest.json` for a
new snap-T are written directly to their final keys. The commit
happens when `current.txt` is atomically updated last — before that,
the new snapshot is invisible to readers (they're still resolving
through the prior `current.txt`). This sequencing works on cloud
object stores (where directory rename doesn't exist) and on the
local store (which simulates atomic single-blob writes via
tmp+rename internally). Pruning of older retained snap-T blobs is
best-effort and runs after the `current.txt` commit. Crash points:

- Mid-Save (operator throws): partial blobs may exist under
  `snap-T/`, but `current.txt` is never updated, so `Read` and
  `Exists` see the prior snapshot (or no snapshot). Orphans get
  pruned eventually as ticks advance and retention catches up.
- Between blob writes and `current.txt` commit: same — orphan
  blobs invisible to readers.
- During `current.txt` rename: atomic — either old or new pointer is
  visible.
- During pruning: best-effort. Failures leave orphan blobs but don't
  break correctness; `Read` still uses `current.txt`.

`Snapshot.Exists(store)` returns true iff `current.txt` exists and
names a snap-T whose manifest exists.

**Schema drift detection.** The manifest carries two fingerprints:

- `PlanFingerprint` — operator types in positional order, including
  generic args. Catches op add/remove/reorder and type-arg drift.
- `SchemaFingerprint` — every snapshottable operator's
  `ISnapshotable.SchemaFingerprint`, derived from its codec(s)' column
  names + `SqlType.Display`. Catches drift the operator-type
  fingerprint can't see: VARCHAR length changes (Arrow's `StringType`
  carries no length), DECIMAL precision/scale changes, NULL/NOT NULL
  flips, intermediate column rename/reorder. Manifest schema bumped to
  v2 with the new field; v1 manifests are rejected on read.

### What ships in (C)

`WalRecorder` accepts an optional `snapshotDir`. On reopen, the
recorder loads the snapshot first (via `Snapshot.Exists` so the
`.old`-recovery case is handled), then replays only WAL ticks past
the snapshot's tick. `WalSegment` carries `StartTick` (manifest schema
v2, with v1 → v2 cumulative-reconstruction on read) so whole segments
below the snapshot tick are skipped without opening their files;
straddling segments read-and-discard ticks below.

`WalRecorder.WriteSnapshot(snapshotRetainCount: N)` takes an
end-of-tick snapshot, optionally retaining the N most-recent ones,
and prunes WAL segments fully covered by the latest snapshot.
The WAL is always pruned against the *latest* snapshot tick — older
retained snapshots are point-in-time-only and can't be rolled forward
with the current WAL. Operations are ordered for crash safety:

1. `Snapshot.Write` (atomic via the snap-T rename + `current.txt`
   commit).
2. WAL manifest is rewritten with the pruned segments removed
   (tmp+rename atomic at the manifest level).
3. Best-effort delete of the now-orphaned `.arrows` files.

A crash during step 1 leaves the old snapshot intact; during step 2,
the snapshot is committed but the WAL manifest still references all
segments, and replay just skips already-applied ticks; during step 3,
the manifest no longer references the orphan files so replay never
opens them, and the next `WriteSnapshot` cleans the leftovers.

`Snapshot.Read` also restores `RootCircuit.TickCount` to the snapshot's
tick so absolute tick numbers stay consistent across the
snapshot/WAL boundary. The recorder validates the pairing on open:
if the snapshot's tick exceeds the WAL's coverage, it throws
`InvalidDataException` rather than silently producing wrong state.

### Test coverage

The persistence test suite (under `tests/DbspNet.Tests/Persistence/`)
covers each stateful operator's snapshot round-trip in isolation, the
snapshot-foundation contracts (manifest, plan fingerprint, schema
version), the WAL recorder's input-replay path, the snapshot+WAL
hybrid (snapshot-only, WAL-only, both, mid-session checkpoints, prune
behavior, error paths), atomic-write semantics (mid-Save crash, stale
`.tmp` cleanup, `.old` recovery), and multi-stateful-op compositions
(JOIN+GROUP BY, three-way JOIN, UNION+GROUP BY, LEFT JOIN+GROUP BY,
recursive CTE+GROUP BY, plus a manifest assertion that
`SnapshottedIndices` records every stateful op in plan order).
