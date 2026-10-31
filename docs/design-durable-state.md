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

## 1b. Increment 1 — parallel checkpoint (DONE, 2026-10-30)

A persistent deploy no longer falls back to serial: `DBSPNET_WORKERS=12` with a snapshot directory
runs the parallel program and checkpoints all 12 workers as one commit. Protocol in
`docs/persistence.md` ("Parallel programs").

| before | after |
|---|---|
| a `current.txt` and a prune per worker; `parallel.json` a root marker no one cross-checked | one `current.txt` naming `snap-T[.G]/`, which holds `worker-{w}/` subtrees and `parallel.json`; the pointer write is the single commit |
| restore never compared worker ticks | every worker manifest's tick is checked against `parallel.json` before any state loads |
| no metadata, so no offsets | `parallel.json` carries metadata; `SnapshotCheckpointStore` implements `IParallelCheckpointStore` |
| parallel `ProgramRunner.CreateAsync` took no store | it takes an `IParallelCheckpointStore`; `RunnerProgram` dispatches save/restore by program kind |
| `DbspNetEngine` ran serial whenever persistence was on | runs parallel; **W is pinned by the checkpoint** (`RecordedWorkersAsync`), overriding `DBSPNET_WORKERS` with a log line, as Feldera does |
| per-worker fsync would have cost 2 device flushes × W | one sync over the whole directory, then `parallel.json`, then the pointer: three device flushes per checkpoint whatever W is |

`Snapshot` was split into internal steps (`WriteDirAsync`, `ReadManifestAsync`, `LoadDirAsync`,
`NextSnapDirAsync`, `PruneAsync` with a completeness marker) that both paths share. The serial
commit's behaviour is unchanged.

**Verification.** `ParallelCrashSafeCheckpointTests` runs the increment-0 matrix (every mutation,
process crash and power loss, flat and spine, later tick and same tick) over W=3 concurrent
workers, and requires all workers to restore at one commit point. Two reverts each fail all four
matrix cases: dropping the directory sync, and moving the pointer write before `parallel.json`.
Also new: a worker at the wrong tick is refused before anything loads; serial and parallel
snapshots refuse each other, naming W; metadata commits with the workers.
`WideProgram_RestoreThenContinue_MatchesUninterrupted` (W=2/4, flat and spine) closes the
coverage gap from brief (c): partitioned top-K, RANK, window SUM, LAG, a join re-keyed by an
aggregate, and a LATENESS aggregate, each restored and then continued for 4 ticks against an
uninterrupted run. `LatenessOperator`'s per-worker `maxSeen` is persisted per replica, so restore
reproduces it. Whether a per-shard frontier is the right semantics is a separate question from
persistence. Runner: `ParallelRunner_RestoreAfterBatch_…` (W=2/4) matches an uninterrupted serial
run and writes nothing on an idle batch. Engine: a W=4 persistent deploy runs parallel, and a
redeploy asking for W=2 resumes at 4. A serial checkpoint redeployed with W=4 stays serial. Both
match a from-scratch run. Full suite: 2658 pass.

**Measured (2026-10-30, 6963d1e, M4 Mac mini, SF=3, `scripts/measure-checkpoint.sh`, 5 alternating
runs per arm, `DOTNET_gcServer=1`, fsync on).** Medians, with the range across the 5 runs:

| | W=1 | W=12 | W=12 vs W=1 |
|---|--:|--:|--:|
| batch 1 step (drain) | 34.2 s (32.6–34.6) | 17.5 s (15.3–17.8) | 1.96× faster |
| checkpoint after batch 1 | 13.2 s (13.0–15.8) | 11.5 s (11.3–11.6) | 1.15× faster |
| checkpoint after batch 2 | 12.9 s (10.1–13.3) | 5.4 s (2.6–5.4) | 2.4× faster |
| checkpoint after batch 3 | 10.2 s (10.1–13.0) | 5.3 s (4.5–5.4) | 1.9× faster |
| snapshot size | 4.58 GiB | 4.59 GiB | same |
| restore | 8.7 s (7.7–11.0) | 19.0 s (18.7–19.4) | **2.2× slower** |

All 10 restores matched the uninterrupted view row counts. The checkpointing compile disables shared
arrangements, so the step times compare only with other checkpointing runs. The snapshot is the
same size at both W, so the earlier 4.05 GiB reference came from a different configuration.

Two findings: **parallel restore was 2.2× slower than serial**, with a very tight range, so the
effect was real. And the W=12 checkpoint after batch 1 is twice as slow as the later ones, which
write the same amount of state; that one is still unexplained.

**Parallel restore fixed: the workers were restoring one after another.**
`LocalTableFileSystem`'s reads complete synchronously, so `ParallelSnapshot.ReadAsync` calling each
worker's `LoadDirAsync` in a plain loop ran worker 0's whole restore, then worker 1's, and so on.
The serial path did not have the problem because `Parallel.ForEachAsync` puts work on the pool,
and neither did small W: a worker whose share of the default degree (7 here) is 2 or more also
takes that branch. Only a share of 1, i.e. W above half the degree, walked sequentially, which is
why the W=2..4 tests never saw it. `ParallelSnapshotConcurrencyTests` now pins it without timing:
each worker's first operator-file access over a synchronous file system blocks until every worker
has arrived (read at W=9, write at W=3), and both tests fail with the fix reverted.
The tell was CPU, not work: the W=12 restore did the same work as serial (54.7 s CPU against 57.1 s,
fewer bytes allocated) but kept 3.2 cores busy against 6.5. `IvmParallelRestoreProfile` (scratch)
restores an existing checkpoint under variants; every truly concurrent variant restored in 5–6 s
against 15–17 s for the shipped loop. The fix puts each worker on `Task.Run`, in the restore and,
for the same reason, in the write. Re-measured with the script (5 alternating runs per arm, medians):

| | W=1 | W=12 before | W=12 after |
|---|--:|--:|--:|
| restore | 8.1 s (7.8–8.8) | 19.0 s | **7.2 s** (7.0–7.3) |
| checkpoint after batch 1 / 2 / 3 | 16.1 / 13.0 / 10.3 s | 11.5 / 5.4 / 5.3 s | 11.6 / 5.5 / 5.4 s |

Restore at W=12 is 2.6× faster than before and now slightly faster than serial; all 10 restores
matched. The checkpoint did not move, so the write was not serialised in the same way; the
`Task.Run` there is kept only so that concurrency does not depend on the file system. The
restore-only harness's 5–6 s against 7.2 s end to end is probably the producer's heap still
being torn down in the end-to-end run; unmeasured.
Every checkpoint still rewrites all ~4.6 GiB, even after a 9-tick batch that changed almost
nothing.

## 2. Plan from here

1. ~~Increment 1 — parallel checkpoint.~~ Done, §1b.
2. **Increment 2 — A2 checkpoint policy.** A durable input WAL every batch, with offsets in each
   record, and a snapshot every N batches or T seconds.
3. **Increment 3 — re-shard at restore (change W between parallel worker counts).** Today W is
   pinned by the checkpoint, as in Feldera (`controller.rs`: "Can't change number of workers or
   hosts yet"; their runtime rebalancer in `operator/dynamic/balance/` already moves integrated
   state between workers). Plan: when the requested W differs from the recorded one, each new worker
   reads every old worker's slice and keeps the entries with `hash(key) % W_new` = its index,
   rebuilding its maps from them. The partition function, the checkpoint format and same-W restore
   are unchanged, so normal running and checkpoints cost nothing extra; only a W-changing restore
   pays one rehash per entry plus the cross-worker read (unmeasured). Fixed virtual partitions
   (Flink key groups) were considered and not chosen: they only pay off if changing W must be
   cheap and frequent, and they cap W. Work:
   - audit every operator for per-worker (not per-key) state and give each a merge rule;
     `LatenessOperator.maxSeen` takes the minimum (conservative: delays GC, never wrong);
   - verification: checkpoint at W_old, restore at W_new, continue, compare against an
     uninterrupted run, over the `WideProgram` operator set, both directions (e.g. 4→12, 12→4,
     3→2), flat and spine; plus a crash during a re-sharding restore leaves the old checkpoint
     restorable;
   - `DbspNetEngine` adopts the configured W instead of the recorded one, with a log line.

   Out of scope: serial ↔ parallel. The two programs have different operators and restore matches
   operators by position, so that waits on the operator-identity decision.
4. **Phases 1–3 of the kickoff:** scenario and targets, the deciding measurements (spine vs flat at
   W=8/12 with chunked maps, delta-run bytes per tick, the restore fraction a run-manifest layout
   avoids), then the choice among options 1–3.
