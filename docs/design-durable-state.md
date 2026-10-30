# Design: durable state for long-running circuits

Kickoff: `docs/next-arc-durable-state-prompt.md`. Branch `arc/durable-state`.

Goal: checkpoints whose cost tracks what changed and which run often enough to bound recovery, and
restarts whose time does not grow linearly with state. Every recovery must be digest-verified
against an uninterrupted run.

## 0. Phase 0 — research (2026-10-30)

Three read-only briefs, kept in `docs/research-durable-state/`:

- `phase0-a-feldera.md` — Feldera's storage, checkpoint and recovery path, with file:line citations.
- `phase0-b-other-systems.md` — Flink (RocksDB incremental, changelog/FLIP-158, heap/FLIP-151,
  ForSt), Materialize persist, RisingWave Hummock, Kafka Streams, with URL citations.
- `phase0-c-inventory.md` — every `ISnapshotable`, operator identity, the parallel path, the
  checkpoint flow, spine, and recovery test coverage.

The conclusions that set the plan:

1. **The serial checkpoint was not crash-safe** (brief c, confirmed in code). It had three crash
   windows: a same-tick rewrite into the committed directory; no fsync anywhere; and the pointer
   deleted before the new one was renamed in. Prune could also delete the current snapshot.
   → Increment 0, §1.
2. **The typed path repeated §7.2** (brief c flagged it unverified; the new test confirmed it): no
   `Typed*` aggregator persisted float state. → fixed in Increment 0 (f9362d7).
3. **`ParallelSnapshot` is not atomic across workers.** Each worker rotates its own pointer and
   prunes independently, and restore never cross-checks ticks, so a crash mid-write silently
   restores a mix of ticks. No exchange state is in flight at a tick boundary, so the fix is a
   commit protocol, not a snapshot algorithm.
4. **Feldera gets cross-worker consistency from lockstep steps, with no snapshot protocol.** Each
   worker writes its part between steps, there is one top-level pointer, and the worker count is
   pinned by the checkpoint. The pointer that carries input offsets is written only after outputs
   drain, and file GC runs only after the pointer is durable. Their spine checkpoint writes every
   in-RAM batch to a file, so "incremental" means *what is still in RAM*, not *what changed*.
   Restore rebuilds no index but reads every file's filter block (about 2.4 B/key). Our
   `docs/research-feldera/persistence.md` is wrong in four places; brief (a) lists them.
5. **Other systems:** with a mutable hash map, checkpoints can cost O(delta) through a
   consolidated per-operator delta log plus a periodic base rewrite in the background
   (FLIP-151/158), and Z-set consolidation avoids Flink's changelog bloat. Restart then stays an
   eager O(state) rebuild. Lazy restart needs the durable format to be the serving format (ForSt,
   Hummock). Shared-file GC is where everyone's bugs are; prefer single-owner, logical-then-physical
   deletion keyed to the oldest retained checkpoint (Materialize persist).

## 1. Increment 0 — crash-safe commit (DONE, 2026-10-30)

Correctness only; no step-path change. Protocol is described in `docs/persistence.md`
("Commit protocol").

| defect | fix |
|---|---|
| same-tick checkpoint rewrote the committed `snap-T/` in place | a write always goes to a fresh directory (`snap-T.{gen}`); `ProgramRunner` also skips the checkpoint when the tick has not moved since the last one written or restored |
| no fsync | `IDurableTableFileSystem` (Core.IO), implemented by `LocalTableFileSystem` via libc `fsync` (files and every directory up to the root) plus one macOS `F_FULLFSYNC`; op files are durable before the manifest, and the manifest before the pointer |
| pointer delete-then-rename window | one atomic, durable replace (`WriteAllBytesDurableAsync`); the local `WriteAllBytesAsync` is now write-temp-then-rename as well |
| prune kept the highest-numbered directories, so an orphan could evict the current snapshot | prune keeps the current snapshot plus the newest complete (manifest-present) ones before it |
| damaged pointer read as "never checkpointed", so the runner restarted fresh | `SnapshotCheckpointStore` treats only a *missing* pointer as fresh; otherwise restore throws |
| `retainCount` 1 on the connector path | `SnapshotCheckpointStore` defaults to 2 (Feldera keeps 2) |
| typed float aggregates re-folded on restore (§7.2 again) | the four typed `double` SUM/AVG accumulators persist per slot, with the same framing as the structural composite |
| `IvmRecoveryProbe` digest used `GetHashCode` | it now uses `IvmRestoreProfile.StableCell` |

**Verification.** `CrashSafeCheckpointTests` covers flat and spine, a later-tick and a same-tick
second checkpoint, and every mutation of the commit as a crash point, each as a process crash and
as a power loss. Restore must find a pointer, land exactly on the old or the new commit (the new
one once the write returned), continue identically to an uninterrupted run, and leave the store
prunable. A mutation run reverted each fix in turn, and every revert fails the matrix:

| reverted fix | failing cases |
|---|--:|
| no fsync | 4 of 6 |
| in-place same-tick rewrite | 3 of 6 |
| delete-then-write pointer | 4 of 6 |
| old prune | 1 of 6 |

`TypedFloatAggregateRestoreTests` failed before the typed fix (AVG+MIN, SUM+MIN, bit-exact delta
comparison) and passes after it. `ProgramRunnerCheckpointTests` gained idle-batch and
damaged-pointer cases. Full suite: 2644 pass.

**Cost.** fsync on the M4's APFS volume, measured with a 4 GiB, 168-file write (the SF=3 checkpoint
shape): per-file `fsync` about 0 s, one `F_FULLFSYNC` about 0.07 s, three per checkpoint, against
11.5–17 s of serialisation. Not yet measured end to end on SF=3. On Linux the dirty-page flush will
be real I/O time, but the data has to reach the disk anyway.

**Not covered:** `ParallelSnapshot` inherits the per-worker durability from `Snapshot` but is still
not atomic across workers (Increment 1). `WalRecorder` segments are still not fsynced; that becomes
relevant with A2 (Increment 2).

## 2. Plan from here

1. **Increment 1 — parallel checkpoint.** Each worker writes its part and one top-level manifest
   commits the tick, W and the offsets; prune only after that commit; restore verifies worker
   ticks. Wire it into the parallel `ProgramRunner.CreateAsync` and drop `DbspNetEngine`'s serial
   fallback. W is pinned by the checkpoint, as in Feldera.
2. **Increment 2 — A2 checkpoint policy.** A durable input WAL every batch, with offsets in each
   record, and a snapshot every N batches or T seconds.
3. **Phases 1–3 of the kickoff:** scenario and targets, the deciding measurements (spine vs flat at
   W=8/12 with chunked maps, delta-run bytes per tick, the restore fraction a run-manifest layout
   avoids), then the choice among options 1–3.
