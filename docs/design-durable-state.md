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

## 1f. Phase 3 — design: option 2, flat hot tier + lazy cold runs (APPROVED 2026-10-31; increment 1 built)

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
