# Starting prompt — next arc: durable state for long-running circuits

> Kickoff for the checkpoint/recovery arc, written 2026-10-30 at the close of the row-representation
> arc. Priority call (Curt): for a long-running circuit, cheap checkpoints and fast recovery are table
> stakes, so this arc comes **before** any further in-process step performance work. Paste the block
> below into a new session.

---

**Durable state for long-running circuits — research, design, then build (multi-agent allowed)**

**Goal.** A DbspNet pipeline that runs indefinitely must be able to (1) take checkpoints whose cost
tracks *what changed*, not total state, and runs often enough to bound recovery, and (2) restart from
the latest checkpoint quickly, in time that does not grow linearly with state size. Correctness is
non-negotiable: every recovery is digest-verified against the never-crashed run.

**This is not starting from zero — read what exists first.** We already ship full snapshot,
snapshot + input WAL, and verified restore. The weakness is *shape and cost*, not absence:

- `docs/persistence.md` — approaches A–D and what ships (`Snapshot`, `WalRecorder`, `IBlobStore`,
  `LocalFileBlobStore`, the `current.txt` pointer-commit protocol).
- `docs/design-incremental-persistence.md` — the arc so far. §0–§4: a checkpoint is a **full O(state)
  rewrite** (`Snapshot.WriteAsync` re-serialises every `ISnapshotable`); Phase 0 reuse measurements;
  Track A (periodic snapshot + input WAL) vs Track B (batch identity + reference-manifest snapshots +
  refcount/GC). §6 A1 built (`ICompiledCircuit`, `ProgramWalRecorderTests`). **A2 — a checkpoint
  policy on `ProgramRunner` (WAL every batch, snapshot every N) — was never built**; verify.
  §7.2/§8: restore once silently produced wrong state — the reason every restore is digest-verified.
  §9–§11: restore measured and apportioned. Today ~9.4 s for 4.05 GiB (parallel restore 5cd1c7a +
  VARCHAR arena b83c593); snapshot write 11.5–17.3 s; restore is boxing/rebuild-bound, read I/O is
  2.5%; resumed ticks touch **0.0135%** of restored keys; flat state cannot be lazy because each
  operator's snapshot is one unsorted, unindexed Arrow batch.
- `docs/design-durable-identity.md` — operator identity vs batch identity, and the dangerous part
  (shared-file lifetime under compaction).
- `docs/comparison-feldera-decisions.md` and `docs/research-feldera/persistence.md`, `trace.md` —
  Feldera: one sorted, immutable, file-backed LSM trace; checkpoint = manifest of already-durable
  batch files; lazy O(#files) recovery; per-column B-tree files with Bloom filters, rkyv zero-copy;
  merging on background threads. Their pipeline also compiles SQL→Rust for ~268 s before it can run
  at all (CI 2026-10-30), which matters for "restart" latency on *their* side.
- `docs/next-arc-lazy-restore-prompt.md` — the previous kickoff for the restore half; superseded by
  this one, but its tables are still the baseline.
- Memory: `sorted-on-disk-state-future`, `lazy-restore-next-arc`, `radix-tree-ceiling`,
  `ivm-bench-checkpoint-premise-wrong`, `spine-compaction-experiment`, `feldera-source-comparison`,
  `row-repr-arc-kickoff`.

**Code entry points.** `src/DbspNet.Persistence/` (`Snapshot.cs`, `ParallelSnapshot.cs`,
`WalRecorder.cs`, `ArrowZSetTraceCodec.cs`, `ArrowIndexedZSetTraceCodec.cs`, `SnapshotManifest.cs`,
`WalManifest.cs`), `src/DbspNet.Connectors.Abstractions/ProgramRunner.cs` (unconditional snapshot at
the end of `RunBatchAsync` when a snapshot dir is set), `src/DbspNet.Server/ProgramSpec.cs` /
`DbspNetEngine.cs` (`SnapshotDir` / `DBSPNET_SNAPSHOT_DIR`; ivm-bench deliberately leaves it unset),
`src/DbspNet.Connectors.Abstractions/ICheckpointStore.cs`, the spine family under
`DbspNet.Core.Operators.Stateful.Spine.*` (sorted immutable batches, spill via `SpineSpillConfig` /
`SpilledSpineBatch`). Probes: `tests/DbspNet.Tests/Scratch/IvmRecoveryProbe.cs`,
`IvmRestoreProfile.cs` (record / profile / replay, digest-verified), `IvmCheckpointReuse.cs`.

**The central design question.** Incremental checkpoints and lazy recovery both want state that is
already organised as **immutable, sorted, identifiable runs** — a checkpoint then names runs that are
already durable and writes only new ones, and recovery maps runs instead of rebuilding hash maps.
Our default in-memory state is the opposite: mutable hash-indexed Z-sets (flat family, now backed by
`ChunkedMap`). So the arc must decide, by measurement:

1. **Keep flat in memory; make the on-disk side incremental.** Write each tick's (or each batch's)
   deltas as sorted runs, with periodic compaction on disk; checkpoint = manifest of runs. Restore
   stays eager (load runs → rebuild maps). Cheapest to build; fixes checkpoint cost, not restore.
2. **Hybrid: flat hot tier + sorted cold tier per trace** — the recent/mutable part stays a map, the
   bulk lives in immutable sorted runs (in memory or on disk) reached through the same probe API.
   Enables lazy recovery. The Feldera-shaped answer without adopting spine wholesale.
3. **Spine as the default family.** Already sorted and batch-structured; lost on step time
   (+22% sorted-batch construction; 1.4–2.5× slower than flat on Nexmark at W=24) — but those numbers
   predate `ChunkedMap` and were never taken on ivm-bench at W=8/12. Re-measure before ruling in or out.

Plus the orthogonal pieces each option needs: durable batch identity, refcount/mark-sweep GC of run
files across retained checkpoints, operator identity stable across recompiles (or an explicit
"recompile = fresh state" contract), the input-offset/output-idempotency alignment (offsets already
commit atomically with state in the manifest), parallel (W>1) checkpoints of per-worker state, and
`IBlobStore` (cloud-shaped: atomic single-blob writes only, no rename).

**Lessons from this project that apply here.**
- The GC is a first-class design constraint. The row-repr arc found that large mutable arrays on the
  LOH, card-scanned until a background GC, cost more than their own work; a GC-heap paged store was
  measured and **removed** (`docs/design-paged-trace-state.md` §11–§16), and native pages recovered
  1.5 s but still lost. Any on-disk/mmap'd or cold-tier design must say where its bytes live
  relative to the GC and be measured with `DOTNET_gcServer=1`.
- Measure the ceiling before building (radix-tree arc: framing dissolved in ten minutes).
- Restore has been silently wrong before (§7.2). Digests must use `StableHash`, never
  `object.GetHashCode()` (process-randomised for `string`).
- Don't benchmark-flatter: Feldera writes **zero** checkpoints in ivm-bench, so ivm-bench batch
  timings are not a durability comparison. This arc needs its own scenario.

**Proposed shape (propose this, adjust, then wait for Curt's go-ahead before building).**

- **Phase 0 — research, in parallel (agents welcome).** Independent, read-only briefs:
  (a) Feldera's storage/checkpoint/recovery path in `../feldera` — on-disk batch format, how a
  checkpoint is taken without stopping the circuit, GC of files, recovery steps and their cost; start
  from `docs/research-feldera/persistence.md` and extend, don't redo.
  (b) Other systems' incremental-checkpoint designs for comparison — Flink incremental RocksDB
  checkpoints, Materialize persist / Differential arrangements, RisingWave Hummock — what the unit of
  durability is and what recovery costs.
  (c) An inventory of our code: every `ISnapshotable`, what it saves, how it restores, which state is
  a trace vs a bespoke runtime structure, and what the parallel path does.
  Give each agent the file pointers above and a narrow scope; keep the conclusions, not the dumps.
- **Phase 1 — define the scenario and targets.** A long-running workload (ivm-bench SF=3 multi-batch
  staging, `IVM_STAGING_ROOT`, plus a Nexmark stream) with checkpoints enabled; metrics = checkpoint
  wall + bytes per checkpoint vs delta size, step-time overhead with checkpointing on, recovery time
  to first output, recovery vs state size. Set numeric targets with Curt.
- **Phase 2 — measure the deciding numbers** (serialised runs, ≥5 alternating per arm):
  spine vs flat on ivm-bench at W=8 with chunked deltas; delta-run bytes per tick; what fraction of
  restore a manifest-of-runs layout would avoid.
- **Phase 3 — design doc** (`docs/design-durable-state.md`) choosing among options 1–3 with numbers;
  staged increments, each landing default-off behind an env/option seam with byte-identical default
  output, and a crash-injection / digest-verified recovery test matrix.
- **Phase 4 — build increments**, smallest useful first (likely A2's checkpoint policy + sorted delta
  runs), each measured against the Phase 1 targets.

**Working rules (unchanged).** Design-first; propose and wait for Curt; branch for the arc and merge
only with his go-ahead. `git ls-files | grep ' [0-9]\.'` before trusting a build; clean-rebuild after
a Sql.dll change. Always `DOTNET_gcServer=1`. Serialise measurements — never two at once; wall-clock
needs ≥5 alternating runs per arm. Docker is Curt's to run — hand him the commands.
