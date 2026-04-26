# Persistence and recovery — design sketch

Unimplemented. DbspNet today holds all circuit state in memory: the
per-stream `Current` buffers, the `ZSetTrace` / `IndexedZSetTrace` dicts
behind joins / aggregates / distinct, the `_aggCache` / `_stateCache` dicts
inside `IncrementalAggregateOp`, `RecursiveCteOp._r`, and the
`InputHandle._pending` buffers. A crash or process restart loses all of it.
This note sketches how to make state survive a restart — either by
snapshotting the state or by replaying inputs. No code yet; this is a
design discussion.

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
