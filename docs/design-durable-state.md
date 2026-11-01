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

**The slow first checkpoint is background-GC waiting (2026-10-30, single runs, diagnosis only).**
`IVM_CHECKPOINT_PROFILE=1` in `IvmMultiBatch` times every operator save and the GC around each
checkpoint; `IVM_CHECKPOINT_REPEAT=1` writes a second checkpoint straight after batch 1's.

- The W=12 first checkpoint is mostly waiting: 31 s CPU over 11.6 s wall (2.7 cores) with every
  worker slow, against 20 s over 2.7–5.4 s for the repeat.
- Memory pressure is ruled out: free RAM stayed at 20–27 GiB of 64 and the OS logged no
  compressions, pageouts or swapouts during it.
- A ~3 s stall lands once per gen-2 GC. At W=12 six concurrent saves of different operator types
  all stalled 2.93–2.97 s together; at W=1 one operator per checkpoint took ~3 s against its usual
  0.07–0.45 s. The runtime's GC pause total stayed at 7–117 ms, because these are background GCs:
  threads that allocate on the large-object heap wait for a background GC, and the Arrow
  serialisation allocates there (11.3 GiB per 4.6 GiB checkpoint).
- The first checkpoint runs three gen-2 GCs (batch 1 leaves the budget spent); later ones one.
- With `DOTNET_gcConcurrent=0` the stalls are gone and the gen-2s show up as ~1 s blocking pauses
  instead (`gc_pause_ms` 650–2200):

| one run each | background GC (default) | `gcConcurrent=0` |
|---|--:|--:|
| W=12 checkpoint batch 1 / again / 2 / 3 | 11.6 / 5.4 / 5.5 / 3.2 s | 4.6 / 3.3 / 4.0 / 3.7 s |
| W=1 checkpoint batch 1 / again / 2 / 3 | 16.0 / 12.9 / 10.3 / 13.1 s | 10.1 / 8.3 / 8.9 / 8.6 s |
| W=12 / W=1 restore | 7.5 / 11.4 s | 5.5 / 7.5 s |

**Fixed (2026-10-31): no background GC during a checkpoint or restore.** `CheckpointGcMode` sets
`GCLatencyMode.Batch` for the duration of `Snapshot`/`ParallelSnapshot` `WriteAsync`/`ReadAsync`
(ref-counted, restoring the caller's mode), default on; `DBSPNET_CHECKPOINT_GC=default` opts out.
No step runs inside either (both runners await them between ticks); anything else running
concurrently in the process sees blocking gen-2s for that time. Measured with
`ARMS="1 1:gcbatch 12 12:gcbatch" scripts/measure-checkpoint.sh` — 5 alternating runs per arm,
medians [ranges], all 20 restores matched (`docs/checkpoint-gcbatch-results-20261031.txt`):

| | W=1 | W=1, Batch | W=12 | W=12, Batch |
|---|--:|--:|--:|--:|
| checkpoint after batch 1 | 15.5 s [13.1–18.2] | **9.4 s** [9.2–10.3] | 11.5 s [11.5–11.6] | **5.1 s** [4.9–5.4] |
| checkpoint after batch 2 | 13.1 s [12.5–13.3] | **8.5 s** [7.9–8.7] | 2.7 s [2.7–5.7] | 3.0 s [2.8–3.2] |
| checkpoint after batch 3 | 10.4 s [10.1–13.0] | **8.6 s** [8.0–9.0] | 4.4 s [4.3–5.7] | 4.0 s [3.5–4.3] |
| restore | 10.9 s [10.8–12.5] | **7.1 s** [6.9–7.2] | 7.2 s [7.2–7.4] | **5.5 s** [5.3–5.8] |
| batch-1 drain (control) | 34.4 s | 34.1 s | 17.1 s | 17.3 s |

The first W=12 checkpoint is 2.3× faster and every W=1 checkpoint 1.2–1.6×; restore is 24–35%
faster at both. Later W=12 checkpoints were already short and move within noise (batch 2's median
is 0.3 s higher but its range is tighter, 2.8–3.2 against 2.7–5.7). The drain, which runs with the
mode off, is unchanged. The deeper fix — pooling the save path's large buffers, which would also
cut the 11.3 GiB allocated per checkpoint — is still open, and is likely shaped by Increment 2's
format changes.

## 1c. Increment 2 — A2 checkpoint policy (design 2026-10-31)

**What changes.** Today `ProgramRunner` snapshots the whole state at the end of every batch: an
O(state) rewrite (4.6 GiB on ivm-bench SF=3) per batch, however little the batch changed. A2
splits the per-batch commit from the snapshot:

- **Every batch: a durable tick journal.** One small file per batch, `journal/{first:D20}-{last:D20}.json`,
  written atomically and durably (`IDurableTableFileSystem.WriteAllBytesDurableAsync`: a reader or a
  restart sees the whole file or none). It records, per engine tick, the source the tick consumed
  and that source's cursor after it — Feldera's per-step resume tokens (`Resume::Seek`, phase0-a
  §2.5). Offsets only, no row data: every connector we have can re-read a version (Delta CDF by
  version, Parquet bounded), the replayable case of design-connectors.md. A source that cannot
  re-read would need its data journaled (the existing `WalRecorder`, A1) — out of scope.
- **Every N batches or T seconds: a snapshot**, exactly as today (all W workers, offsets in the
  manifest, one pointer). After its pointer is durable, journal files wholly at or below its tick
  are deleted (Feldera truncates its journal after the pointer moves, phase0-a §2.2 step 7).
- **Default policy: journal every batch, snapshot every 60 s** — Feldera's
  `checkpoint_interval_secs` default (`feldera-types/src/config.rs`), clamped to 1–3600 s as
  Feldera does. `DBSPNET_CHECKPOINT_INTERVAL_SECS` overrides it in `DbspNetEngine`; `batch` restores
  the old snapshot-every-batch behaviour. A runner created without a policy keeps the old
  behaviour, so existing callers are unchanged.

**Recovery.** Restore the snapshot the pointer names (tick T₀, cursors C₀), or start from tick 0
and the sources' initial offsets when there is none — the empty circuit is a valid base, so no
initial snapshot is needed. Then replay the journal's ticks after T₀ **exactly**: for each, ask
that source for its next version from the current cursor, check that the version it returns ends
at the journaled offset (a mismatch fails the restore loudly: the source is not replaying
deterministically), push, step, and check the tick number. Then resume normally.

Why replay the journal rather than just let the normal drain re-read from C₀? The drain interleaves
sources by whatever versions are available *now*, so after a crash it would group and order ticks
differently from the original run. The final integrated state would agree, but time-dependent
operators (temporal filters, lateness GC) and the per-tick output deltas need the same tick
sequence. Replaying the journal reproduces it; the drain then continues from the journal's last
cursors.

**Outputs.** Unchanged: outputs are written before the batch commits (journal or snapshot), so a
crash re-processes at most the uncommitted batch — at-least-once. After a restart the first write
of each incremental output carries the whole view onto a truncated sink, as today; replayed ticks
emit nothing.

**The trade.** A batch's commit drops from a full snapshot (5–16 s on SF=3) to one small fsynced
file; recovery grows by the replay of everything since the last snapshot (re-read + re-step). On
ivm-bench, whose batches arrive well under 60 s apart, a run writes journal files only — as
Feldera writes no checkpoints there — and a restart replays from tick 0. `CheckpointAsync` still
forces a snapshot (e.g. before a planned shutdown).

**Built (c5bc996) and measured (2026-10-31).** `TickJournal` (Persistence), `CheckpointPolicy` and
`ProgramRunner.CommitBatchAsync`/journal replay (Connectors.Abstractions), the engine default and
`DBSPNET_CHECKPOINT_INTERVAL_SECS`. `CheckpointJournalTests` covers recovery from journal alone and
from snapshot + journal (serial, W=2, W=3), truncation, stale files, a source that lost a journaled
version, and a journal left by a run under the other policy; with replay disabled, the five
recovery tests fail. A snapshot pins W; a journal-only state replays at any W, serial or parallel
(`DbspNetEngineTests`, 1→4, 4→2, 4→1) — the journal names sources and offsets, never operators.

ivm-bench SF=3, W=12, `ARMS="12 12:journal" scripts/measure-checkpoint.sh`, 5 alternating runs
per arm, medians [ranges], all 10 restores matched (`docs/checkpoint-journal-results-20261031.txt`):

| | snapshot every batch | journal every batch, no snapshot |
|---|--:|--:|
| commit after batch 1 | 4.99 s [4.82–5.16] | **11 ms** [10–11] |
| commit after batch 2 | 3.24 s [2.98–3.38] | **5 ms** [5–6] |
| commit after batch 3 | 4.14 s [4.00–4.46] | **6 ms** [5–6] |
| restore (all 38 ticks) | **5.66 s** [5.34–5.83] | 12.09 s [11.92–16.86] |
| batch-1 drain (control) | 17.12 s | 16.25 s (one run 25.3 s) |

A batch's commit drops by ~500–1000×: three batches cost 12.4 s of snapshots or 22 ms of journal.
Recovery from the journal alone replays everything since tick 0 — 12.1 s here, faster than the
original batch-1 drain because replay writes no outputs — against 5.7 s to load a snapshot. Under
the 60 s default the replay is bounded by roughly one interval of input; a run that wants a short
restart sets a shorter interval and pays a snapshot that often. (A first run of this measurement,
kept out of the doc, ran the snapshot arm with the pre-fix checkpoint GC because the script still
forced `DBSPNET_CHECKPOINT_GC=default`; fixed in 3d2d399.)

**Not in this increment:** a clean-shutdown hook in the server (none exists to call), data journals
for non-replayable sources, exactly-once output (needs the journal plus idempotent sinks keyed by
tick), and buffer pooling in the save path.

## 1d. Increment 3 — re-shard a snapshot to a different W (BUILT 2026-10-31)

**Scope.** Restore a parallel snapshot written at W_old ≥ 2 into a program at W_new ≥ 2. Serial ↔
parallel stays out (different operator graphs; positional identity). A journal-only state already
replays at any W (§1c), so this is only about snapshots.

### The audit (structural parallel path, `PlanToCircuit.TryCompileProgramParallel`)

Inputs are sharded by `OfWholeRow(row) % W`; every exchange places by its own partition function
(`((h % W) + W) % W`). For each stateful operator the parallel compile can build:

| Operator (site) | Persists | Worker of an entry, as a function of the persisted entry |
|---|---|---|
| `IntegrateOp`, each output view | view rows | **none needed — linear**: a view row is legitimately split across replicas today (anti-join, residual outer join, projections, UNION ALL), and every consumer reads the replica **sum** |
| `DistinctOp` (DISTINCT) | rows | `OWR(row, d.Schema)` |
| `IncrementalAggregateOp` (GROUP BY), exchange kept | key = group key row, value = input row; `aggstate` blobs by key | `OWR(key, groupKeySchema)` |
| ″, exchange **elided** (input already partitioned on a subset of the group columns) | same | the **upstream** hash over the subset columns of the stored **value** row, in upstream order/schema; worker 0 if upstream was gathered |
| `IncrementalJoinOp` / `LeftJoin` / `FullJoin` (hash) | left + right traces keyed by the equi-key | `OWR(key, keySchema)` — proven equal to the exchange's `OfRow(row, indices, keySchema)`; left and right agree |
| `IncrementalJoinOp`, **broadcast** join | left trace, right trace | right: **replicated** (identical copy on every worker); left: **linear** given a full right everywhere |
| residual outer join: 2× `DistinctOp` over matched rows | joined rows | `OfRow(row, leftIndices, leftKeySchema)` on the stored joined row (not `OWR(row)`) |
| residual outer join: `DistinctOp` over side rows; semi `IncrementalJoinOp` on row identity | rows / keys = side rows | `OWR(row, sideSchema)` |
| SemiJoin (EXISTS/IN/anti): `DistinctOp` over key tuples; `IncrementalJoinOp` | key rows | `OWR(key, semiKeySchema)` |
| `PartitionedTopK*`, `PartitionedRank`, `PartitionedWindowAggregate`, `PartitionedOffset` | input rows | `OWR(PartitionOf(row), $pk schema)`; **worker 0** when unpartitioned (gathered) |
| `TopKOp` (global ORDER BY … LIMIT) | all input rows | **worker 0** |
| global aggregate / hash cross join (empty key) | — | `OWR(empty)` = a constant worker (not 0), which the `OWR(key)` rule already reproduces |

Unreachable on this path (refused or never built): `LatenessOperator`, `TemporalFilterOp`, the
fixpoints, shared arrangements, `LinearAggregateOp`, `DelayOp`. No per-worker scalar state is
reachable; tick and logical time are equal on every worker. Spine variants persist per-batch files
with the same keys (`TraceFamily = Spine`, not the default).

### Design: rewrite the snapshot, then restore normally

1. **The compiler states each operator's placement.** At each build site above, the parallel compile
   records a `ReshardSpec` for the operator (by its position in the replica circuit): which of its
   files are traces / keyed blobs, their codecs' schemas, and one of
   `Hash(entry → int)` (the function from the table), `Worker0`, `Replicated`, or `Any` (linear).
   An operator without a spec makes re-sharding refuse, loudly — a new stateful operator cannot be
   resharded by accident.
2. **`ParallelReshard.RewriteAsync(fs, program, W_new)`** decodes each old worker's file of each
   operator **once** through the spec's codec, sends every entry to its new worker, and writes a new
   snapshot generation at the same tick with W_new workers, the same offsets metadata and manifests,
   then commits it with the usual durable pointer swap. Then the ordinary `ParallelSnapshot.ReadAsync`
   restores it. Operators' own load code is untouched. Cost ≈ one restore + one checkpoint of
   decode/encode (~10 s on ivm-bench SF=3), paid only when W changes.
3. **Self-check on the way.** For `Hash` specs, every entry read from old worker w must satisfy
   `hash % W_old == w` (`Worker0`: only on worker 0; `Replicated`: identical row counts on every
   worker). A mismatch means the spec is wrong for that operator, and the rewrite aborts before
   anything is committed. This turns a silent misplacement — wrong results ticks later — into an
   error on the old data, where the true placement is known.
4. **Crash safety for free:** the old snapshot stays current until the new pointer commits.
5. **`DbspNetEngine`** compiles at the configured W; when the snapshot's W differs (both ≥ 2) it
   rewrites, logs one line, and restores. Today it adopts the snapshot's W instead.

**Placement of linear state (`Any`).** Entries keep a deterministic spread (`OWR(row) % W_new`);
any placement sums to the same view. **`Replicated`** writes worker 0's copy to every new worker.

### Verification
- For every operator kind in the table, a program that builds it: checkpoint at W_old, reshard to
  W_new, run more batches, and compare against an uninterrupted serial run — 4→12, 12→4, 3→2, 2→3.
  Includes the special cases: elided-exchange aggregate after a join, broadcast cross join with a
  one-row side, residual outer join, anti-join, global TOP-K, global aggregate.
- The self-check rejects a deliberately wrong spec.
- A crash at any point of the rewrite (`CrashSimulatingFileSystem`) leaves the old snapshot
  restorable at W_old.
- ivm-bench SF=3: 12 → 8 and 8 → 12 rewrite time and restored view row counts.

### Built (cdf88de, c9b4240) and measured

Curt approved all three decisions (file-level rewrite, spine refused in v1, the engine re-shards
automatically). Built as designed — `ReshardSpec`/`ReshardRecorder` (Sql), `ParallelReshard`
(Persistence), `SnapshotCheckpointStore.ReshardAsync`, `DbspNetEngine` — with two additions the
audit forced:
- `PartitionInfo` now records the **key schema** an exchange hashed with and whether an empty
  partition key means **gathered** (worker 0) or hashed (the empty key's constant worker); the two
  were indistinguishable, and the elided aggregate's placement needs both.
- The "elided" path is only reachable aggregate-over-aggregate on the program path: join-then-
  aggregate always has a projection between them, so its exchange stays (the existing
  `JoinThenGroupByJoinKey_ElidesExchange` test asserts elision only in a comment). Counting
  exchange operators confirmed which shape elides; `ParallelReshardTests` uses that one.

**Tests** (`ParallelReshardTests`): every stateful operator of a program with every operator kind
has a placement; 2→3, 3→2, 4→12, 12→4, 5→2 match an uninterrupted serial run and keep matching
for 5 more ticks; a wrong placement is rejected with nothing committed; spine is refused; a crash
at every mutation of the rewrite (process crash and power loss) leaves a snapshot that restores
at the old W before the commit and the new W after it. Mutating the elided-aggregate or
residual-join placement makes the self-check fail at that operator. (With few keys and small W a
wrong hash can coincide with the right one, so the self-check is probabilistic on tiny data.)

**ivm-bench SF=3** (`ARMS="12:to=8 8:to=12" scripts/measure-checkpoint.sh`, 5 runs each,
medians [ranges], all 10 restores matched; `docs/reshard-results-20261031.txt`):

| | re-shard (rewrite) | restore after it |
|---|--:|--:|
| 12 → 8 | 6.74 s [6.63–7.22] | 5.26 s [5.20–5.37] |
| 8 → 12 | 6.99 s [6.48–7.42] | 5.37 s [5.09–5.38] |

The first version took 144 s (12→8) and 72 s (8→12): a directory listing per operator per old
worker walked the whole 4.6 GiB snapshot tree each time (90% of samples in `getdirentries`). One
listing grouped by (worker, op) took it to 16.5 s; rewriting operators concurrently at the
restore's degree to ~7 s. Changing W therefore costs about one extra restore's worth of time.

### Decisions for Curt
- **File-level rewrite** (above) rather than a new load path in every operator (which would also
  decode the data W_new times).
- **Spine:** refuse in v1 with a clear error (not the default family); add later if needed.
- **The engine reshards automatically** when the configured W differs, instead of pinning W.

## 1e. Phase 2 — the deciding numbers (MEASURED 2026-10-31)

Branch `arc/durable-phase2`. All on ivm-bench SF=3 (multi-batch staging: batch 1 = 20 ticks of bulk
load, batches 2 and 3 = 9 small ticks each), `DOTNET_gcServer=1`, after Increments 0–3 and the
checkpoint GC fix.

### (a) Spine vs flat at W=8 (`ARMS="8 8:spine" scripts/measure-checkpoint.sh`, 5 alternating runs, medians [ranges])

| | flat | spine |
|---|--:|--:|
| batch-1 drain | 16.70 s [15.64–16.97] | 18.64 s [17.29–19.52] (+11.6%, ranges disjoint) |
| batch-1 drain allocation | 42.69 GiB | 44.56 GiB (+4.4%) |
| batches 2 / 3 drain | 0.03 / 0.04 s | 0.04 / 0.04 s |
| checkpoint after batch 1 / 2 / 3 | 4.91 / 3.15 / 4.31 s | 5.88 / 3.99 / 3.71 s |
| snapshot size | 4.70 GB | 4.86 GB |
| restore | 6.04 s [5.62–6.23] | 6.45 s [6.12–6.74] |

All 10 restores matched. **Spine costs ~12% of batch-1 drain on ivm-bench at W=8**, far from the
1.4–2.5× measured on Nexmark at W=24 before the chunked map. Its checkpoint is no cheaper today:
the spine snapshot still re-serialises every batch (`GetBatches` copies), so the immutable-batch
structure buys nothing yet. Raw: `docs/spine-vs-flat-w8-20261031.txt`.

### (b) How much does a batch change the state? (`CheckpointDeltaProbe`, `IVM_CHECKPOINT_DELTA=1`, W=8, flat)

For each pair of consecutive snapshots, per worker and per operator file: decode both with the
operator's codec, take the Z-set difference, encode it with the same codec. This is what a
batch-granular manifest-of-delta-runs checkpoint would write (per-tick runs cannot cancel within a
batch, so they would write at least this much).

| | full snapshot | changed | ratio | entries changed / total |
|---|--:|--:|--:|--:|
| batch 1 → 2 | 4,698 MiB | **6.73 MiB** | 0.143% | 13,520 / 35.7 M |
| batch 2 → 3 | 4,699 MiB | **7.03 MiB** | 0.150% | 15,802 / 35.7 M |

Much of the 7 MiB is per-file overhead (an Arrow schema and footer per operator file per worker):
the partitioned window aggregate writes 1.84 MiB for 73 changed entries. The largest single file,
a join's left trace (2.1 GB), changes by 0.04%. Raw: `docs/checkpoint-delta-20261031.txt`.

### (c) What restore spends its time on, and how much of the restored state is used

`IvmRestoreProfile`, serial, sequential restore with the stage profile (2 runs; the stages are
exact, the totals noisy: 26.1 s and 17.1 s, against ~6–8 s for the parallel restore):

| stage | share of restore |
|---|--:|
| read (file I/O) | 3.4–5.5% |
| decode (Arrow IPC) | ~1% |
| extract (box columns) | 13–19% |
| materialize (row objects) | 30–49% |
| index (hash into Z-sets) | ~10% |
| integrate (into traces) | 4–5% |
| rebuild (operator state) | 19–28% |

Replay of batches 2–3 after restoring batch 1 (`IVM_RESTORE_MODE=replay`, verified):
**1,208 distinct keys probed of 9,671,750 restored (0.0125%)**; full scans touched 2 keys. The
earlier arc measured 0.0135%. Raw: `docs/restore-apportionment-20261031.txt`.

### What the numbers say about options 1–3

- **Checkpoint cost is ~700× larger than the change it records.** A checkpoint rewrites 4.7 GB
  to record ~7 MiB of change. Any option that writes deltas (all three, once built) turns a
  3–5 s snapshot into milliseconds plus fsync. With the A2 journal the per-batch commit is already
  5–11 ms; what incremental snapshots remove is the 3–5 s paid every snapshot interval, which is
  what bounds recovery replay.
- **Restore is ~95% reconstruction, and the resumed run uses 0.01% of it.** Option 1 (delta runs on
  disk, eager restore) avoids none of that — it would even add a merge of runs at load. Only a
  lazy layout (options 2 and 3) removes it; with a 0.0125% touch rate the reachable floor is
  near O(#files).
- **Spine is not ruled out by step cost** on this workload (+12% at W=8), but it is not a shortcut
  either: its snapshot and restore are not yet incremental or lazy, so option 3 still needs the
  run-manifest and lazy-load work options 1 and 2 need.

Not yet measured: the Nexmark stream scenario with checkpointing on (Phase 1), and how the numbers
scale with state size (only SF=3 exists locally; SF<3 cannot be generated).

## 1f. Phase 3 — design: option 2, flat hot tier + lazy cold runs (APPROVED 2026-10-31; increments 1–6 built)

**Decisions (Curt, 2026-10-31):** option 2; a recompiled program may start from fresh state (no
operator identity across recompiles — the plan/schema fingerprints keep refusing a mismatched
snapshot, as today).

### Constraints from the measurements and the access-pattern inventory

- A batch changes ~0.15% of state; a resumed run reads ~0.0125% of it; restore is ~95% rebuilding
  objects and maps (§1e). So: write deltas, and fault state in only where it is read.
- **Reads are delta-key-driven** for joins, aggregates, distinct and integrate: `GroupFor`,
  `ContainsKey`, `WeightOf`, and an existence check in every `MergeInPlace`. The partitioned
  TOP-K / rank / window / LAG ops read **whole partitions**, keyed by the partitions a delta touches.
  Global `TopKOp` walks an ordered prefix of all its state every tick.
- **Full-state reads to remove or bound:** the first GC pass after restore (`_lastGcFrontier` is not
  persisted); the join's "iterate the smaller side" rule, which may enumerate a whole trace; the
  first output write after restore (both output modes enumerate every view); `ViewRowCounts` needs
  O(1) counts.
- **Steady-state step time must not move.** The hot path stays in-memory hash maps; nothing cold
  is consulted for a key once it is resident.
- GC: cold bytes live in files; what stays resident (Bloom filters, block indexes) is reference-free
  arrays. Blocks are read into pooled buffers and decoded only for the keys asked for.

### The layout

**Cold runs.** An immutable run file per trace per checkpoint, holding an **additive** Z-set delta
(base run = the state at the first checkpoint; each later run = the change since the previous one).
Entries are ordered by the key's stable hash (`StructuralRowHash`, deterministic across processes)
and grouped into blocks (~128 KiB; an indexed trace never splits a key's group across blocks). Each
block is an Arrow record batch in the operator's existing codec schema. A footer holds the block
index (first hash per block), a Bloom filter over keys, entry/group counts, and min/max monotone
key per block (for lateness GC). Point probe = Bloom → binary search the index → read and decode
one block. Hash order needs no comparer and serves every probe these operators make; nothing on this
path needs key-ordered traversal of a trace.

**Hot tier and fault-in, inside the collections.** `ZSet` / `IndexedZSet` get an optional cold
backing: a miss in the hot map consults the cold runs once — summing the key's entries across runs —
and inserts the result into the hot map; a *resolved-keys* set (present or tombstoned) keeps a key
from being read from cold twice. Every operator that probes through `GroupFor` / `ContainsKey` /
`WeightOf` / `MergeInPlace` therefore becomes lazy without changing its code. Enumeration of a
collection that still has cold state either streams the merge of hot + cold without promoting
(for output and counting) or, where an operator truly needs all of it (global `TopKOp`), promotes
everything — eager, as today. Counts (`Count`, `GroupCount`) are persisted per run and maintained
exactly through integration (which resolves each delta key, so a key's crossing of zero is known).

**Operators that keep state outside a trace.**
- Partitioned TOP-K / rank / window / LAG: persist by partition (key = partition row, values = rows)
  instead of one flat row Z-set, and fault a whole partition into its `SortedDictionary` (and, for
  rank / LAG / window, recompute that partition's window) on first touch.
- Aggregates: `_stateCache` / `_aggCache` fault in per group from the `aggstate` keyed blobs, which
  move into the same run format.
- Global `TopKOp` (one gathered partition, read in order every tick): stays eager.

**Checkpoint.** Each trace keeps a pending delta (the sum of what it integrated since the last
checkpoint — ~0.15% of state per batch on ivm-bench). A checkpoint writes one delta run per trace,
fsyncs it, and writes a manifest naming every run the state is the sum of; the pointer swap commits
it as today. Run files are shared by consecutive checkpoints; a mark-and-sweep over the retained
manifests deletes the rest after the pointer moves. **Compaction** merges a trace's runs (streaming,
hash-ordered, summing weights, dropping zeros) when they pile up, off the step thread — the files are
immutable — and the next checkpoint references the merged run.

**Restore.** Read the manifest, open each run's footer (index + Bloom + counts): O(#runs) small
reads, no decoding. Persist `_lastGcFrontier` so the first tick does no full GC pass; GC drops
whole blocks/runs below the frontier by their min/max monotone key and resolves the rest lazily.
The join's smaller-side rule treats a trace with cold state as large (iterate the delta, probe the
trace). The first output write after restore streams each view from hot + cold without promoting;
removing it entirely needs sink alignment (the sink's committed tick == the restored tick), a later
step.

**Parallel and re-shard.** Runs are per worker. `ParallelReshard` learns to read runs and write base
runs at the new W; the placement specs (§1d) are unchanged.

### Increments (each default-off behind `DBSPNET_STATE_LAYOUT=runs`, byte-identical when off)

1. **Run file format** — writer and reader: hash-ordered blocks, block index, Bloom, counts,
   monotone ranges; point probe and sequential scan. Property tests against `ZSet` / `IndexedZSet`
   (random contents, collisions, empty, huge groups); microbenchmark: probe latency, scan
   throughput vs today's decode.
2. **Snapshots in run format, restore still eager** — swap the on-disk format only. Byte-identical
   outputs; write and restore times vs today on SF=3.
3. **Incremental checkpoints** — pending deltas, delta runs, manifest of runs, mark-and-sweep GC,
   compaction policy; the crash matrix (`CrashSimulatingFileSystem`) over checkpoint, compaction and
   GC. Measure checkpoint cost on SF=3 against the delta size.
4. **Lazy restore for traces** — cold backing + fault-in in `ZSet` / `IndexedZSet`, persisted counts
   and GC frontier, the join side rule, per-group aggregate state. Property tests: random
   interleavings of integrate / probe / checkpoint / restore against an eager oracle; digest-verified
   ivm-bench recovery.
5. **Lazy partitioned operators** — persist by partition, fault in partitions.
6. **Outputs after restore** — streaming first write; then sink alignment to skip it.
7. **Re-shard over runs; compaction off the step thread** if increment 3 left it inline.

### Increment 1 — built (2026-10-31)

`DbspNet.Persistence.Runs`: `RunFile.WriteZSetAsync` / `WriteIndexedAsync` and `RunReader`
(open = trailer + footer only; `WeightOfAsync` / `GroupForAsync` decode one block; `ScanZSetAsync` /
`ScanIndexedAsync`). Blocks are the operator's own codec output; the order is the unsigned stable
hash; a block never splits a hash value; the footer holds the block index, counts and a Bloom
filter (10 bits/key, 7 probes). `RunFileTests` (12): Z-set and indexed runs from empty to 5,000
entries at block sizes 1–1,024, with the stable hash and with a 5-value colliding hash, read back
exactly by point lookup and scan (absent keys absent; an indexed group of 3,000 values never split);
truncated and corrupted files refused; Bloom false positives under 3%. Letting a block cut between
equal hashes fails all three collision cases.

Microbenchmark (`Scratch/RunFileBench`, `RUNFILE_BENCH=1`; 1 M ten-column rows, single run, warm
page cache). Today's monolithic file: 96.7 MiB, write 725 ms, full load 275 ms.

| block | size vs today | write | open | probe, present | probe, absent | full scan |
|---|--:|--:|--:|--:|--:|--:|
| 256 | +7.8% | 1,272 ms | 1.74 ms | 98 µs | 1.6 µs | 374 ms |
| **1,024 (default)** | +2.8% | 764 ms | 0.49 ms | 134 µs | 1.8 µs | 128 ms |
| 4,096 | +1.6% | 597 ms | 0.42 ms | 590 µs | 7.8 µs | 135 ms |

Read against the targets: opening a run is ~0.5 ms, so restoring ivm-bench's ~200 operator files is
~0.1 s of footers — inside the 1 s target with room. A present-key probe decodes a whole block,
~134 µs; ~85 µs of it is per-block fixed cost (the 256-entry block is not 4× cheaper), mostly the
per-block Arrow IPC schema message and a scratch copy — worth trimming before increment 4, since the
resumed ivm-bench batches touch ~1,200 keys (~0.16 s at this rate). Absent keys cost ~2 µs (Bloom).

### Increment 2 — built and measured (2026-10-31)

`StateLayout.Runs` (`DBSPNET_STATE_LAYOUT=runs`, or `StateLayout.Use` in tests) makes the Arrow
Z-set and indexed trace codecs write every trace file as a run; a load checks for the run trailer,
so either layout restores whatever the setting is now. Keyed aggregate-state blobs are unchanged
until increment 4. `StateLayoutTests` (4): serial snapshots written in one layout and read in the
other (and in runs both ways) restore and continue byte-identically; a parallel run-format
snapshot restores and re-shards 3→2. Full suite passes with runs on (2,876) and off (2,880).

ivm-bench SF=3, W=12, 5 alternating runs per arm (`ARMS="12 12:runs" scripts/measure-checkpoint.sh`;
raw: `docs/runs-layout-w12-20261031.txt`). Median [min–max]:

| | today | runs | |
|---|--:|--:|--:|
| batch-1 drain | 15.89 s [15.12–21.16] | 15.65 s [15.48–16.22] | noise |
| checkpoint after batch 1 | 5.18 s [4.85–6.72] | 12.95 s [11.92–13.02] | **2.5×** |
| checkpoint after batch 2 | 3.26 s [2.97–3.95] | 9.58 s [8.98–9.90] | **2.9×** |
| checkpoint after batch 3 | 4.23 s [3.43–5.09] | 10.02 s [9.89–10.45] | **2.4×** |
| snapshot size | 4,701 MiB | 4,765 MiB | +1.4% |
| restore | 5.66 s [5.01–6.45] | 6.66 s [6.62–7.10] | +18% |

All 10 restores matched the uninterrupted view row counts. Steady-state step is untouched (the
layout only changes what a checkpoint writes).

The write cost is the finding. The 1 M-row microbenchmark predicted +5% (764 vs 725 ms); at SF=3 a
full run-format snapshot costs 2.5–2.9×. The writer as built does far more than the format needs:
it collects (hash, entry) for the whole trace, sorts it, then for **each block rebuilds a
`ZSetBuilder` / `IndexedZSetBuilder`** (whole-row re-hashing into a fresh dictionary, every value
copied into new inner Z-sets) only so the codec can encode it, into a scratch in-memory file
system, then copies the bytes out. That is a second materialisation of the whole state per
checkpoint. Restore's +1 s is the read-side twin (per-block scratch copy and Arrow schema message,
the ~85 µs fixed cost from increment 1), and increment 4 replaces eager restore anyway.

This matters beyond increment 2: incremental checkpoints (increment 3) write small delta runs, but
the first checkpoint and every compaction still write whole runs. So before increment 3, encode a
block straight from a slice of the sorted entries (no intermediate Z-set, no scratch file system),
and write one Arrow schema per run rather than per block. Target: a full run-format snapshot within
~10% of today's write time.

### Increment 2 — write fix (2026-10-31)

Phase timers (`DBSPNET_RUN_PROFILE=<file>`, CPU summed over all writer threads, 3 checkpoints of
106 M entries) on the writer as first built: `collect` (stable hash per row) 40 s, `sort` 11 s,
**`encode` 236 s** (the per-block Z-set rebuild + codec + scratch file system), `write` 14 s —
303 s against 95 s for today's monolithic writes. The rewrite:

- **Blocks encode straight from the sorted entries.** The codecs implement `IZSetRunCodec` /
  `IIndexedRunCodec` (`EncodeBlock` from row slices via `ArrowColumns.Build(type, rows, column)`;
  `DecodeBlock` into a caller's builder). No intermediate Z-set, no scratch file system.
- **One Arrow stream per run.** The schema message once, a record batch per block, the
  end-of-stream marker; the footer records the schema message's length and the reader caches it
  at open. A run's prefix is now a plain Arrow stream any reader can open (`RunFileTests`).
- **Sort `(hash, position)` with `Array.Sort(uint[], int[])`** instead of a list of tuples with a
  comparison delegate.
- **Restore decodes every block into one builder** (it used to build a Z-set per block and re-add it).

Measured on SF=3, W=12, 5 alternating runs per arm (raw + profiles:
`docs/runs-writefix-w12-20261031.txt`):

| | today | runs as first built | runs, write fix |
|---|--:|--:|--:|
| checkpoint after batch 1 / 2 / 3 | 5.07 / 3.09 / 4.31 s | 12.95 / 9.58 / 10.02 s | 5.91 / 5.78 / 5.87 s |
| restore | 5.61 s | 6.66 s | 6.35 s |

All restores matched. `encode` is now ~95 s — the same as today's whole write — and what is left
on top is the per-row stable hash (`collect`, ~18 s; `StablePartitionHash.OfWholeRow` allocates an
`int[]` per row) and the sort (~4 s).

**Then the hash itself:** the run order is now the row's own cached hash (`StructuralRowHash`,
already deterministic across processes — strings are XxHash3 seed 0, everything else a value
type), so a run write hashes nothing and a probe hashes nothing. To keep that safe against a
future change to the hash function, the footer records the hash of a fixed row holding one cell of
every engine type, and a reader whose hash disagrees refuses the run
(`ARunOrderedByAnotherHash_IsRefused`). The 1 M-row microbenchmark now writes a run faster than
today's file (386 vs 686 ms; absent-key probe 1.0 µs); the SF=3 effect of the hash change is
measured with increment 3, whose base checkpoints are exactly these writes.

### Increment 3 — built and measured (2026-10-31)

**Change tracking, in Core.** `ZSetTrace` / `IndexedZSetTrace` carry an optional
`CheckpointTracking`: the chain persistence last wrote (opaque to Core) and the sum of every change
since (`Pending`). `Integrate` merges each delta into `Pending` too; `DropKeysBelow` (LATENESS GC)
records the dropped entries negated; `Clear` (the restore reset in `IntegrateOp`, the only mutation
that bypassed `Integrate`) forgets the chain. Operators whose state lives outside a trace but whose
snapshot is the integral of their input — partitioned TOP-K, rank, offset (LAG/LEAD), window
aggregate — hold a `ZSetChangeTracker` that records each step's input delta and the window
aggregate's GC drops, and tag the snapshot they build. Nothing tracks until the first checkpoint
commits (no batch-1 memory doubling), and a pending change past half the state stops recording
and frees itself (the next checkpoint rewrites the state whole). Persistence finds a state's
tracker through a side table (`ConditionalWeakTable`), not a field: a field on every Z-set costs
8 bytes, which w1profile's B/ev would see. Steady state pays one null check per integrate while
runs are off.

**Chains, in Persistence.** Inside a session-aware checkpoint (`Snapshot` / `ParallelSnapshot`
with `StateLayout.Runs`), a tracked state with a pending change writes it as one delta run;
anything else writes a base run. Runs live under `runs/{snapshot dir}/…`, named after the snapshot
that first wrote them; the snapshot's trace file becomes a **run list** naming the chain (base
first). A chain is rewritten whole once it has `RunChain.MaxRuns` (8) runs or its deltas pass half
its base. Restore sums the chain into one builder and marks the result, and the trace (or tracker)
that integrates it into empty state **adopts** the chain — so the first checkpoint after a restore
is a delta, not a rewrite.

**Commit and crash safety.** Runs are durable before the manifest (the `runs/{dir}` tree is synced
with the snapshot's). Each snapshot directory gets a `runs.txt` naming every run its run lists
use. Tracking restarts on the new chains only **after** the pointer commits (`CheckpointSession`),
so a checkpoint that fails part-way leaves every pending change in place and the next checkpoint
still carries it (its unsynced runs are never referenced). After each prune, a sweep deletes every
run no retained snapshot's `runs.txt` names. Re-shard writes self-contained runs (no session), so
it needed no change.

**Verification.** `DBSPNET_RUNS_VERIFY=1` reads every chain back as it is written and throws if its
sum differs from the state — a mutation the tracking misses fails at that checkpoint. The full
suite passes three ways (runs off, runs on, runs on + verify: 2,893 each), and one SF=3 run with
verify on passed every checkpoint. `IncrementalCheckpointTests` (10): chains grow by deltas from an
unchanged base and every checkpoint restores and carries on exactly; a restored chain is extended,
not rewritten; long chains are rewritten and the sweep leaves exactly the referenced runs (retain
1 and 3); a checkpoint failed at each of its mutations in turn loses nothing, including after power
loss; LATENESS GC drops reach the deltas (join, DISTINCT, window aggregate); parallel chains restore
and re-shard 3→2 and checkpoint on; a delta checkpoint crashed before each of its mutations
(process crash and power loss) recovers the old or the new checkpoint, and the recovered program
checkpoints on. Mutation-tested: each of these, removed, fails at least one test — pending not
recorded (trace, LAG step), GC drops not recorded (indexed trace, Z-set trace, window aggregate),
runs tree not synced, tracking restarted before the commit, restored chain not adopted, sweep that
deletes nothing.

**Measured** on SF=3, W=12, 5 alternating runs per arm (`ARMS="12 12:runs" scripts/measure-checkpoint.sh`;
raw: `docs/runs-incremental-w12-20261031.txt`). Median [min–max]; *state* is everything a restore
reads (the snapshot directory plus the runs it names), *written* is what that checkpoint wrote:

| | today | runs, incremental |
|---|--:|--:|
| batch-1 drain | 15.96 s [15.71–16.11] | 15.95 s [15.41–17.74] |
| checkpoint after batch 1 (base) | 4.96 s [4.78–5.18] | 5.96 s [5.90–6.10] |
| checkpoint after batch 2 (delta) | 3.04 s [3.00–3.34] | **0.92 s** [0.92–0.93] |
| checkpoint after batch 3 (delta) | 4.31 s [4.13–4.50] | **1.48 s** [1.45–1.48] |
| written per delta checkpoint | 4,701 MiB | **51 MiB** (3 MiB runs + 48 MiB aggregate-state blobs) |
| state | 4,701 MiB | 4,750 MiB |
| restore | 5.54 s [5.37–5.85] | 6.65 s [6.48–7.28] |

All 10 restores matched. Delta checkpoints are 3.3× / 2.9× faster and write 1% of the bytes; the
base checkpoint is +1.0 s (+20%) and eager restore +1.1 s (+20%) — increment 4 replaces eager
restore.

**Against the 0.5 s target — not met yet, and the operators are no longer why.** Phase timing of a
delta checkpoint (one run, ms; batch 2 / batch 3): listing snapshot directories 60 / 102; operator
saves 141 / 137 (slowest worker); fsync 262 / 336; manifest + pointer 9 / 8; **prune + sweep
393 / 756**. What is left is per-file bookkeeping: deleting the previous snapshot's ~2,400 files,
creating and syncing one small run list per trace per worker, and listing every `snap-` file to
find directory names. Levers, for a decision:
1. **Defer prune + sweep past the commit** (background, awaited before the next checkpoint
   starts): −0.4 to −0.75 s off checkpoint latency. The deletion still happens, overlapped with the
   next batch; tests that count snapshots right after `WriteAsync` would await it.
2. **One chain file per worker instead of a run list per trace** (`runs.txt` becomes the map from
   `op-i/file` to its chain): ~2,400 fewer files to create, sync and later delete per checkpoint —
   most of the fsync and prune time. Needs the snapshot reader to resolve trace files through the
   map (operators check `ExistsAsync` before loading).
3. Aggregate-state keyed blobs (48 MiB per checkpoint, written whole) move into runs with
   increment 4, as planned.

**Not measured:** the steady-state cost of tracking. Once a checkpoint has committed, every
integrate also merges its delta into the pending change — O(|delta|) per tick, map entries only
(rows are shared with the state). It is off until the first commit (batch-1 drain unchanged) and
the ivm-bench batch-2/3 drains (~0.08 s) are too small to show it; a throughput run with periodic
checkpoints would.

### Increment 3 — the two bookkeeping levers (2026-10-31; Curt: "build both")

**One run index per directory.** A session-aware checkpoint no longer writes a run-list file per
trace: each snapshot (worker) directory gets one `runs.txt` (`RunIndex`), a line per trace file —
`op-{i}/{file}`, base entries, delta entries, its runs. The snapshot reader resolves a trace file
through its directory's index (`TableFileSystemSnapshotContext.ChainAsync`, read once per context),
and `ExistsAsync` answers through it too, so re-shard (which asks before loading) and operators are
unchanged. An operator whose state is all traces now has no `op-{i}` directory at all. The sweep
reads the same indexes.

**Deferred cleanup.** Prune + sweep start in the background once the pointer commits
(`CheckpointCleanup`, default on; `DBSPNET_CHECKPOINT_CLEANUP=inline` restores the old order).
Every later snapshot operation on the same store — write, read, re-shard, listing — waits for it
first; stores are keyed by identity (a local store's root path), so a second store object over the
same directory waits too. The string-path convenience overloads and `WalRecorder` stay inline (the
caller gets a final directory back). Cleanup only deletes what no retained snapshot needs, so
doing it late changes only when the space returns; a failure in it is ignored and retried by the
next one, as an inline prune's was. Tests that count mutations run inline;
`ACrashDuringDeferredCleanup_StillRecoversTheNewCheckpoint` crashes inside the deferred cleanup
(process crash and power loss) and requires the new checkpoint to restore and checkpoint on.
Mutation-tested: an `ExistsAsync` that ignores the index, and a sweep that ignores it, each fail.
Full suite passes four ways: runs off, runs on, runs on + verify, runs on + inline cleanup.

**Measured** (SF=3, W=12, 5 alternating runs per arm; raw: `docs/runs-levers-w12-20261031.txt`).
`12:inline` is today's layout as it was; `12` is today's layout with deferred cleanup (now the
default for it too):

| | today, inline cleanup | today, deferred | runs, incremental + both levers |
|---|--:|--:|--:|
| checkpoint after batch 1 (base) | 4.86 s [4.13–4.92] | 4.92 s [4.21–5.22] | 5.55 s [4.38–5.68] |
| checkpoint after batch 2 (delta) | 3.92 s [3.31–3.97] | 3.07 s [2.90–3.22] | **0.27 s** [0.24–0.28] |
| checkpoint after batch 3 (delta) | 3.47 s [3.42–3.90] | 3.94 s [3.18–4.16] | **0.30 s** [0.29–0.30] |
| written per delta checkpoint | 4,701 MiB | 4,701 MiB | 51 MiB |
| restore | 5.43 s | 5.58 s | 6.60 s |

All 15 restores matched. **The checkpoint target (under 0.5 s for a batch-2/3-sized change) is
met**: 0.92 / 1.48 s → 0.27 / 0.30 s, 11–13× faster than today. The batch-3 figure includes waiting
for batch 2's deferred cleanup (the batch between them drains in 0.1 s), so it is not flattered by
the deferral. For today's layout deferral is within noise — it deletes a few large files, not
thousands of small ones. Still open against the targets: restore (6.6 s, eager — increment 4) and
the base checkpoint (+0.6 s over today).

### Increment 4 — lazy restore for indexed traces and aggregates (built 2026-11-01)

**Where restore's bytes are** (SF=3, W=12, 4,751 MiB): join traces 57%, aggregate traces 14%,
output views (`IntegrateOp`) 12%, partitioned window/offset/rank 16% (increment 5), DISTINCT 3%,
aggregate-state blobs 1%. Indexed traces — joins and aggregates — are ~70%, so they go first.

**Cold backing in `IndexedZSet`.** A restored indexed trace starts with an empty map and a cold
source (`IColdGroups`: exact group count, read one key, read all). A key is read the first time it
is asked for — `GroupFor`, `ContainsKey`, or `MergeInPlace` touching it — and then lives in the map;
a *resolved* set remembers every key read, so a key absent from cold, or emptied since, is never
read twice. `GroupCount` stays exact without reading: the source's persisted count, minus resolved
keys it held, plus the map. Anything that needs every group (enumeration, `Keys`, equality,
`Flatten`, GC's `RemoveKeysBelow`) **promotes** the rest first, so every caller stays correct and
only loses laziness. The cold state lives in a side table (`ConditionalWeakTable`), not a field —
an indexed Z-set is no bigger, and until a lazy restore has happened in the process a map miss
costs one static read. The codec returns a cold-backed collection; the trace that integrates it
into empty state **adopts** the source (and any groups already read) instead of merging it.

**The run side.** Each run-index line now records the state's exact size at the checkpoint (group
count, or row count). `RunColdGroups` opens a chain's runs *detached* — footer, block index, Bloom
filter and schema message in memory, the file opened only to read a block — so thousands of runs
hold no file handles. A key's group is the sum over the runs whose Bloom filter and index admit it.
`DBSPNET_RESTORE=eager` reads chains whole at restore instead.

**Joins are lazy as they stand.** `JoinInto` iterates the side with fewer groups and probes the
other, so with an exact count a restored trace (large) is probed per delta key. Left joins probe
only touched keys.

**Aggregates rebuild per group.** The eager load folds the whole trace to rebuild the per-group
caches, then overlays the persisted states — a full scan. Lazily restored, it skips both, holds the
persisted states (`aggstate`, 48 MiB) in a dictionary, and rebuilds a group's caches the first time a
delta touches it: exactly the eager fold for that one group, then its persisted state. A checkpoint
writes untouched groups' states back as they were.

**Giant groups are read at restore.** The first measurement had the first resumed batch at 3.6 s
(vs 0.1 s): 592 key reads decoded 2.16 M entries. A block never splits a group, so a group larger
than a block fills a block, and reading its key decodes all of it. On SF=3, 14 such blocks hold 8.4%
of the state — the low-cardinality join keys every batch touches. The writer now starts a new block
before any group that fills one by itself (so a giant group always has a block to itself), and the
restore resolves every key whose block is one group of more than 4,096 entries. Adoption carries
those already-read groups over (it first refused a source with any read groups, and the trace was
promoted whole — caught by `AGiantGroup_IsReadAtRestore_TheRestOnDemand`).

**Tests.** `LazyRestoreTests` (9): a cold-backed `IndexedZSet` against an eager oracle under random
interleavings of probes, merges, counts, enumeration and GC drops (5 seeds × 40 trials); a probe
reads a key once; a restored join stays cold, reads only touched keys, matches its twin through
ticks, checkpoint and a second restore; a restored aggregate rebuilds only touched groups and keeps
untouched groups' persisted states across a checkpoint (the `FloatAggregateRestoreTests` 1e16
construction, where a re-fold disagrees with the live sum); a giant group is read at restore. Full
suite passes four ways (runs off; runs on; runs on + verify; runs on + eager restore: 2,904 each).
Mutation-tested: `GroupCount` ignoring cold, `MergeInPlace` not resolving, no adoption, untouched
blobs not saved, blob not applied on touch — each fails.

**Measured** — `IVM_RESTART_AFTER=1`: after batch 1's checkpoint the program is dropped and a fresh
one restored, then batches 2–3 run on it (SF=3, W=12, 5 alternating runs per arm;
`ARMS="12:restart 12:runs:eager:restart 12:runs:restart"`; raw: `docs/runs-lazy-restore-w12-20261101.txt`).
Median [min–max]; every arm's view row counts after batches 2 and 3 matched the uninterrupted run,
and every final restore matched:

| | today | runs, eager restore | runs, **lazy** restore |
|---|--:|--:|--:|
| restore after batch 1 | 5.48 s [5.23–5.58] | 6.71 s [6.27–6.85] | **3.63 s** [3.54–3.70] |
| batch 2 drain (first after restore) | 0.09 s | 0.09 s | **0.11 s** [0.10–0.12] |
| batch 3 drain | 0.08 s | 0.09 s | 0.13 s [0.12–0.15] |
| batch 2 output write | 1.25 s [1.14–1.60] | 1.18 s [1.15–1.26] | 3.73 s [1.58–3.87] |
| final restore (after batch 3) | 7.12 s | 11.74 s | **3.42 s** |

Against the targets: **first batch after restore within 2× — met** (0.11 vs 0.09 s drain).
**Restore under 1 s — not yet**: 3.6 s, and the restore profile (`IVM_CHECKPOINT_PROFILE=1`, CPU
summed over workers) says why — partitioned window aggregate 19.8 s and offset 10.8 s of ~36 s are
still eager (**increment 5**), joins 3.0 s (the giant-group preload and ~3,000 run footers),
`IntegrateOp` views 1.7 s (increment 6), DISTINCT 0.5 s; aggregates are down to 0.1 s.

**Open — the first output write after a lazy restore is bimodal**: ~1.6 s in two runs, ~3.8 s in
three, against 1.2 s eager; the drain is unaffected and the time is in the Delta write of the full
views, not in reading engine state (with `DBSPNET_PROFILE` the same write took 1.16 s; two further
runs gave 1.63 s and 1.69 s, cleanup deferred or inline). A similar stall shows in batch 1 (one
view's Delta write took 2.1 s once). To be run down with increment 6, which removes the full-view
first write.

### Increment 5 — lazy partitioned operators (built 2026-11-01)

After increment 4 the restore profile was 30 of ~36 s of CPU in the partitioned window aggregate and
offset operators: their load folds every saved row back into a partition-keyed `SortedDictionary`
and recomputes every partition's window.

**Saved in partition order.** Their snapshot is a flat row Z-set, so the run is now ordered by the
**partition key's** hash instead of the row's (`ZSetChangeTracker.Attach(..., orderHash)`; only where
that hash is stable across processes — a `StructuralRow` key's cached hash; typed keys stay eager).
A block never splits a hash, so one partition is one block per run. The run index records the
order (`p`/`r`); a chain is never extended across orders; the footer still pins the row hash.

**Restored cold, faulted per partition.** `IColdRowLoader.LoadColdAsync` hands a partition-ordered
chain back as `IColdRows` (`RunColdRows`, runs open detached). The operator keeps a
`LazyPartitions`: the first time a delta touches a partition, its rows are read (one block per
run, filtered to the key) into the bucket and its window rebuilt into a discarded sink — exactly the
eager load for that one partition — before the delta lands. GC promotes every cold partition first
(it scans them all); so does a write that needs the whole state.

**Writing while partly cold.** A delta checkpoint needs only the pending change. A write that needs
everything — a new chain, compaction, another store, the monolithic layout, verify — calls the
operator's `FullState`, which promotes the rest; the partial in-memory snapshot is never written as
the whole.

Covered: `PartitionedWindowAggregateOp`, `PartitionedOffsetOp` (LAG/LEAD), `PartitionedRankOp`,
`PartitionedTopKOp`. **Not covered:** `PartitionedTopKNarrowOp` and `PartitionedTopKCompactOp` (the
§22 row-narrowing TOP-K and the compact one) — they also have no increment-3 change tracking, so
they are written whole every checkpoint and restored eagerly. ivm-bench uses neither.

**Tests.** `LazyPartitionTests` (3): restored LAG / RANK / framed SUM OVER operators stay lazy, read only
touched partitions and match an uninterrupted twin through ticks, a delta checkpoint and a second
restore; a whole-state write while partitions are cold (into another store) writes every row; LATENESS
GC after a lazy restore collects cold partitions too (the row count a whole-state write records equals
a never-restored twin's — skipping the promotion is invisible in outputs, so this is the check). Full
suite passes four ways (2,907 each). Mutation-tested: a faulted partition without its window rebuilt,
no full-state callback, GC without promotion, rows saved in row order — each fails.

**Measured** — restart after batch 1 (`IVM_RESTART_AFTER=1`), SF=3, W=12, 5 alternating runs per arm
(`ARMS="12:restart 12:runs:restart"`; raw: `docs/runs-lazy-partitions-w12-20261101.txt`). All view row
counts after batches 2–3 and every final restore matched.

| | today | runs, lazy (increments 4 + 5) |
|---|--:|--:|
| restore after batch 1 | 6.26 s [6.09–6.56] | **2.19 s** [1.03–2.39] |
| restore after batch 1 + first output write | 7.33–7.85 s | **3.33–4.07 s** |
| batch 2 drain (first after restore) | 0.08 s | 0.13 s [0.12–0.76] |
| final restore (after batch 3) | 6.94 s [6.67–15.16] | **1.20 s** [1.10–1.37] |
| checkpoint after batch 2 / 3 | 1.79 / 3.22 s | 0.24 / 0.27 s |

The restore after batch 1 is bimodal — 1.03 and 1.12 s in two runs, 2.19–2.39 s in three — and the
~1.2 s moves to the first output write in exactly the fast-restore runs (output 3.04 / 2.34 s with
GC pauses of 1.1 / 0.7 s, against 1.1–1.2 s otherwise): one blocking collection landing in one phase
or the other. Their sum is steady and less than half of today's. The restore profile of a fast run
(`IVM_CHECKPOINT_PROFILE=1`, CPU summed over workers): `IntegrateOp` views 2.9 s, joins 1.9 s,
DISTINCT 0.4 s; the partitioned operators went from ~30 s to 0.2 s.

Against the targets: **restore under 1 s — close but not met** (1.0–1.2 s without a GC in it). What
remains is the output views (`IntegrateOp`, eager — increment 6, which also removes the full-view
first write) and the join traces' giant-group preload and run footers. First batch after restore within
2× — met in 4 of 5 runs (0.12–0.30 s vs 0.08 s; one run 0.76 s).

### Increment 6 — outputs after restore (built 2026-11-01)

**Sink alignment: not built, deliberately.** Skipping the full first write after restore needs the
sink to already hold the restored view *and* the connector to know where each row sits: the Delta
connector deletes by file position (deletion vectors), tracked in memory for rows it wrote itself.
A restarted process knows no positions, so its first write truncates and rewrites the view — and
that is exactly what Feldera does (truncate-on-start). Recovering every row's position means reading
the whole table back, about the cost of rewriting it. Per the adopt-Feldera's-assumptions policy the
rewrite stays; recorded here as the alternative.

**What changed instead: the rewrite streams, and views and DISTINCT restore lazily.**
- `ZSetTrace` gets a cold backing (`IColdZSet`: exact count, one row's weight, all rows). `Current`
  promotes first, so every existing caller is unchanged; `Count` is exact without reading; `WeightOf`
  and `Integrate` read only the rows they touch; GC promotes; `EnumerateWhole` yields every row
  without reading cold rows into memory; while cold, a whole-state write uses `FullState` (built from
  the enumeration, not promoted). `IntegrateOp` (output views) and `DistinctOp` adopt a row-ordered
  chain at restore (`IColdZSetLoader` / `RunColdZSet`); the chain's count is the trace's exact count
  (`ExactCount`), so a view's row count survives lazy checkpoint after lazy checkpoint.
- The first incremental write after restore enumerates the view (`RunnerOutput.EnumerateView` →
  `EnumerateWhole`) straight into the Arrow changes — no hashed copy of the view is formed, and none
  is kept. Parallel outputs sum the workers' enumerations (also without promoting); the row count a
  `/wait` reads is formed the same way, once.

**Two things the first SF=3 run exposed.**
1. *A sweep could delete runs a live cold state still reads.* A checkpoint that rewrites a cold
   state whole (a new base) stops naming the runs it was restored from; the sweep deleted them; the
   next cold read failed (`PipelineRunnerTests` under runs, deterministic). The hole was there for
   every lazy source since increment 4. Fix: `RunPins` — each cold source pins its chain's runs while
   it is reachable (a weak table), and the sweep never deletes a pinned run.
   `ALazyState_RewrittenWholeAndSwept_StillReadsItsRuns` reproduces it (it needed a state above the
   4,096-entry floor below which chains always extend).
2. *A row probe decoded a whole block.* Views and DISTINCT read the old weight of every row a delta
   changes; each read decoded a 1,024-row block, so the first batch after restore drained in 0.62 s
   (vs 0.12 s) allocating 1.4 GiB. Every run block now carries its entries' order hashes (`__hash`,
   ascending; run format version 3), and a point probe binary-searches them and decodes only the
   matching entries (`WeightOfRowAsync`, `GroupOfKeyAsync` — the indexed traces' per-key reads use it
   too). First batch: 0.16 s, 0.42 GiB; cold read time 451 → 97 ms.

**Tests.** `LazyViewTests` (2): a restored view stays cold, counts exactly and streams its full write
equal to the uninterrupted view, then carries its chain through a checkpoint and a second restore;
a lazy state rewritten whole and swept still reads its runs. `RunFileTests` check the hash-range
probes against the sets, colliding hashes included. Full suite passes four ways (2,909 each).
Mutation-tested: a sweep ignoring pins and `Integrate` not resolving the rows it changes each fail.

**Measured** — restart after batch 1, SF=3, W=12, 5 alternating runs per arm (raw:
`docs/runs-lazy-views-w12-20261101.txt`). All view row counts after batches 2–3 and every final
restore matched.

| | today | runs, lazy (increments 4–6) |
|---|--:|--:|
| restore after batch 1 | 6.27 s [5.99–6.41] | **0.76 s** [0.73–0.87] |
| batch 2 drain (first after restore) | 0.09 s [0.07–0.09] | 0.17 s [0.16–0.18] |
| batch 2 output write (first after restore) | 1.32 s [1.11–1.54] | 3.22 s [3.05–3.65] |
| restore + first batch + first write | 7.68 s | **4.15 s** |
| final restore (after batch 3) | 7.13 s [6.55–14.88] | **0.93 s** [0.75–0.96] |
| checkpoint after batch 2 / 3 | 2.25 / 2.76 s | 0.22 / 0.24 s |

The restore is now steady (the GC bimodality of increment 5 is gone: nothing large is built at
restore). Against the approved targets: **restore under 1 s — met** (0.73–0.96 s, both restores);
**checkpoint under 0.5 s — met** (0.21–0.26 s); **first batch within 2× — met** (0.17 vs 0.09 s, 1.9×);
**no steady-state regression** — batch-1 drain 16.6 vs 15.9 s here (within the ±2.5 s noise of
earlier measurements; not separately established).

**The cost that moved:** the first output write after restore is 1.9 s slower than today's (3.2 vs
1.3 s): it now decodes the view from runs rather than writing from a view already in memory. Restore
plus first write is still 46% faster than today, and nothing view-sized is kept in memory. The next
lever is to skip the Arrow → rows → Arrow round trip in that write (the run blocks already are the
view's Arrow batches; only the connector's row-address map needs row keys).

### Targets (approved 2026-10-31)

On ivm-bench SF=3, W=8 and W=12, 5 alternating runs per arm:
- **Checkpoint:** proportional to the change — under 0.5 s for a batch-2/3-sized change (today
  3–5 s).
- **Restore to ready:** under 1 s, independent of state size (today ~6 s), digest-verified.
- **First batch after restore:** within 2× of the same batch without a restart.
- **Steady-state step:** no regression beyond noise (batch-1 drain, 5 runs per arm, layout on vs off).
- **Memory:** resident state after restore ≈ only what was touched; Bloom + index overhead stated.

## 2. Plan from here

1. ~~Increment 1 — parallel checkpoint.~~ Done, §1b.
2. ~~Increment 2 — A2 checkpoint policy.~~ Done, §1c.
3. ~~Increment 3 — re-shard at restore~~ Done, §1d. (Original plan follows.) **Increment 3 — re-shard at restore (change W between parallel worker counts).** Today W is
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
