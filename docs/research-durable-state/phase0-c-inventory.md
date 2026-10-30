# Phase 0-C: state and persistence inventory (dbsp-net @ e427d99, 2026-10-30)

Read-only. Paths are relative to `/Users/curthagenlocher/src/dbsp-net`.

---

## 0. The findings that matter most

1. **Two crash-safety holes in the serial checkpoint that exists today.**
   - **Same-tick overwrite.** `Snapshot.WriteAsync` writes per-op files straight into `snap-{tick}/op-{i}/…` (Snapshot.cs:81-101). `ProgramRunner.RunBatchAsync` checkpoints unconditionally, even when `DrainAsync` processed 0 ticks (ProgramRunner.cs:~393-405). So an idle batch rewrites the **committed** `snap-T` in place: `LocalSequentialFile` opens with `FileMode.Create`, which truncates. A crash mid-rewrite corrupts the only snapshot `current.txt` names. With `retainCount=1` (the default in `SnapshotCheckpointStore` and `DbspNetEngine`) there is nothing to fall back to. `AtomicSnapshotTests.Write_OverwritingAtSameTick_PointsToLatest` covers what an overwrite produces, not a crash during one. It also means an idle batch pays the full ~4 GiB save.
   - **No fsync anywhere** (`LocalSequentialFile.Flush` calls `_stream.Flush()`, not `Flush(true)`; `WriteAllBytesAsync` is `File.WriteAllBytes`). The "write files, then rename the pointer" ordering does not hold across an OS crash or power loss. Separately, `WriteCurrentPointerAsync` deletes `current.txt` before renaming the tmp file into place (Snapshot.cs:556-557). A crash between those two calls leaves no pointer, `TryRestoreAsync` returns `null`, and the runner silently **starts fresh** from the connectors' initial offsets.
2. **A parallel snapshot is not atomic across workers, and nothing checks it** (details in §3). Each worker rotates its own `current.txt` and **prunes its previous snapshot** on its own. `parallel.json` is a marker whose presence is all that gets checked; it is not a pointer. `ParallelSnapshot.ReadAsync` never compares worker ticks. A crash mid-write therefore restores a mix of worker-0@T+1 and worker-1@T without any error, and the previous `parallel.json` (tick T) is still there to vouch for it.
3. **Possible recurrence of the §7.2 bug on the typed path (UNVERIFIED).** No `Typed*` aggregator implements `CanPersistState`. `TypedCompositeAggregator` (TypedSqlAggregators.cs:1095) therefore restores by **folding the trace**. That is exactly the order-dependent float reconstruction §7.2 fixed on the structural path. The typed path uses `IncrementalAggregateOp` and this aggregator whenever the aggregate set is not all-linear (MIN/MAX/approx mixed with SUM/AVG(DOUBLE)), under spine, or with `DBSPNET_LINEAR_AGG=off` (TypedPlanCompiler.cs:2030-2039). `PersistenceConformanceTests` only compiles through `SqlProgram` (structural), so nothing covers this. The program/server path is structural, so production exposure today is nil.
4. **Snapshot SAVE is sequential.** The loop at Snapshot.cs:91-101 awaits each operator in turn. Only restore is parallel. Every checkpoint is a full re-serialisation of every operator (nothing is incremental), and that includes the in-circuit output-view `IntegrateOp`s, which duplicate state already held upstream.
5. **A2 (WAL every batch + snapshot every N) is NOT BUILT.** `WalRecorder` has no non-test caller in `src/`. Its open segment only becomes durable on Dispose or `WriteSnapshotAsync`, so a crash loses every tick since the last snapshot. It carries no connector offsets, and `WriteSnapshotAsync` calls `Snapshot.WriteAsync` without metadata (§4).
6. **No ISnapshotable state is silently dropped on the SQL path.** Every snapshotable op throws `NotSupportedException` if it has no codec. The stateful ops that are *not* ISnapshotable (`ArrangeOp`, `SpineArrangeOp`, the two `*SharedRight` joins, `DelayOp`) cannot be reached when persistence is on, or are not used by SQL at all (§1.2).

---

## 1. ISnapshotable implementations (all 23; all in `src/DbspNet.Core`)

Interface: `src/DbspNet.Core/Circuit/ISnapshotable.cs:21`, with `SaveAsync`, `LoadAsync` and `SchemaFingerprint`. Codecs: `ArrowZSetTraceCodec` (ZSet; a single-batch Arrow IPC stream plus a `__weight` column) and `ArrowIndexedZSetTraceCodec` (key cols `__k*`, value cols `__v*`, `__weight`; also the only codec with `SupportsKeyedBlobs`). Both live in `src/DbspNet.Persistence` and are created by `ArrowSqlSnapshotCodecs` (SnapshotCodecs.cs:20). The typed path wraps them in `TypedZSetTraceCodecAdapter` / `TypedIndexedZSetTraceCodecAdapter` (src/DbspNet.Sql/Compiler/TypedTraceCodecAdapters.cs). In the table, "Z" means the ZSet codec and "IZ" the indexed one.

| # | Class | file:line | State held | Save | Restore (cost shape) | Kind |
|---|---|---|---|---|---|---|
| 1 | `IntegrateOp<TRow>` | Operators/Stateful/IntegrateOp.cs:36 | `ZSetTrace _view`: the output-view integral, in-circuit per replica (PlanToCircuit.cs:308/522/722) | Z `view.arrows` | deserialize + `Integrate`, O(n) | flat trace (Z-set) |
| 2 | `DistinctOp` | Stateful/DistinctOp.cs:17 | `ZSetTrace` (post-GC) | Z `trace.arrows` | deserialize + `Integrate` | flat trace |
| 3 | `IncrementalAggregateOp` | Stateful/IncrementalAggregateOp.cs:25 | `IndexedZSetTrace` + `_aggCache` + `_stateCache` | IZ `trace.arrows`, plus keyed-blob `aggstate.arrows` when `CanPersistState` (manifest v4) | deserialize + integrate + **fold every group** (`Update(ref s, None, g, g)`) + overlay of persisted blobs + empty-delta re-derive | flat indexed trace plus a bespoke cache |
| 4 | `LinearAggregateOp` | Stateful/LinearAggregateOp.cs:30 | `Dictionary<TKey,Group>` (weight + state + last), **no trace** | keyed blobs `lagg.arrows` (int64 weight + state) | O(groups) blob decode; falls back to folding a legacy `trace.arrows` through `IFingerprintAlias` | bespoke (trace-free); typed path only |
| 5 | `IncrementalJoinOp` | Stateful/IncrementalJoinOp.cs:27 | left + right `IndexedZSetTrace` | IZ `left.arrows`, `right.arrows` | deserialize + integrate | flat indexed traces |
| 6 | `IncrementalLeftJoinOp` | Stateful/IncrementalLeftJoinOp.cs:45 | same | same | same | flat indexed |
| 7 | `IncrementalFullJoinOp` | Stateful/IncrementalFullJoinOp.cs:49 | same | same | same | flat indexed |
| 8 | `TopKOp` | Stateful/TopKOp.cs:30 | `SortedDictionary _accum` (whole input) + `_window` | Z `trace.arrows` (all of accum, not just K) | re-sort O(n log n) + recompute window, no emit | bespoke (sorted) |
| 9 | `PartitionedTopKOp` (Legacy arm) | Stateful/PartitionedTopKOp.cs:58 | `Dict<part, SortedDictionary>` + window | Z `trace.arrows` | re-partition + re-sort + recompute windows | bespoke |
| 10 | `PartitionedTopKCompactOp` (default) | Stateful/PartitionedTopKCompactOp.cs:34 | `Dict<part, Partition>` (compact) | Z `trace.arrows` | re-partition via `Apply`; window recomputed lazily on next touch | bespoke |
| 11 | `PartitionedTopKNarrowOp` | Stateful/PartitionedTopKNarrowOp.cs:31 | `Dict<part, SortedDictionary<OrderRowKey>>` + window | Z `trace.arrows` | re-partition, re-sort, recompute | bespoke |
| 12 | `PartitionedRankOp` | Stateful/PartitionedRankOp.cs:40 | accum + widened window | Z `trace.arrows` | same, and the window is re-widened | bespoke |
| 13 | `PartitionedOffsetOp` (LAG/LEAD) | Stateful/PartitionedOffsetOp.cs:62 | accum + widened window | Z `trace.arrows` | same | bespoke |
| 14 | `PartitionedWindowAggregateOp` | Stateful/PartitionedWindowAggregateOp.cs:47 | accum + `(widened, weight)` window | Z `trace.arrows` | re-partition + `RecomputePartition(full range)` per partition, no emit. Deterministic (same sorted fold order as live) | bespoke |
| 15 | `TemporalFilterOp` | Stateful/TemporalFilterOp.cs:34 | `Dictionary _accum` + `_emitted` | Z `trace.arrows` | rebuild accum, then `_emitted = ComputeValid(clock)`. **Relies on `RestoreLogicalTime` running before the op loads** (Snapshot.cs:357-362) | bespoke (temporal) |
| 16 | `LatenessOperator` | Stateful/LatenessOperator.cs:37 | `_maxSeen`, `_hasMax` (drives a `MutableFrontier`) | bespoke 9-byte `frontier.bin` (no codec) | `frontier.AdvanceTo(maxSeen - lateness)` | clock/frontier |
| 17 | `SpineDistinctOp` | Stateful/Spine/SpineDistinctOp.cs:39 | `SpineZSetTrace` | `SpineSnapshot`: `trace.manifest` + `trace.batch_{i}.arrows` (Z) | each batch goes back through `Integrate` (re-sort; levels and ids are not preserved) | spine trace |
| 18 | `SpineIncrementalAggregateOp` | Spine/SpineIncrementalAggregateOp.cs:35 | `SpineIndexedZSetTrace` + caches | per-batch IZ + `aggstate.arrows` | per-batch integrate + fold + overlay | spine + bespoke cache |
| 19 | `SpineIncrementalJoinOp` | Spine/SpineIncrementalJoinOp.cs:40 | 2× `SpineIndexedZSetTrace` | `left.*` / `right.*` per batch | per-batch integrate | spine |
| 20 | `SpineIncrementalLeftJoinOp` | Spine/SpineIncrementalLeftJoinOp.cs:21 | same | same | same | spine |
| 21 | `SpineIncrementalFullJoinOp` | Spine/SpineIncrementalFullJoinOp.cs:19 | same | same | same | spine |
| 22 | `FixpointOperator` (naive) | Operators/Nested/FixpointOperator.cs:42 | import traces + `_previousResult` | Z `import_{name}.arrows` + `prev.arrows` | integrate imports; the fixpoint itself is recomputed on the next tick | fixpoint |
| 23 | `SemiNaiveFixpointOperator` | Nested/SemiNaiveFixpointOperator.cs:45 | import traces (`IImportTrace` Save/Load, prefix per import) + materialised `R` | import traces + `r.arrows` | integrate imports + load R | fixpoint |

Notes:
- **Sketch aggregates** (HLL `APPROX_COUNT_DISTINCT`, DDSketch / ordered quantiles, exact `COUNT(DISTINCT)`) have no state of their own on disk. They sit inside #3/#18 with `CanPersistState=false` and are rebuilt by folding the trace. That fold is exact (integer and register state), and AggregatorEmptyDeltaTests pins the empty-delta invariant.
- **Structural aggregators that persist blobs:** SUM (DOUBLE, SqlAggregators.cs:369 area), AVG (non-DECIMAL, :633), STDDEV/moments (:755), and `CompositeAggregator` (:1156), which persists only the slots that need it. **The typed path has none** (finding 3).
- **Fixpoint body ops** are stateless `ApplyOp`/`Apply2Op` (NestedScopeBuilder.cs:79-135). They hold no hidden state.
- **State that is not saved:** `_lastGcFrontier` and `_gcDropped` on GC-capable ops. Benign: the snapshot is post-GC and GC is idempotent.
- **`GetBatches()` has a side effect:** it calls `FlushMemtable()` (SpineZSetTrace.cs:404-408, SpineIndexedZSetTrace.cs:671-675), so taking a snapshot changes the live spine's structure. The memtable is off by default (`SpineStagingConfig.Capacity = 0`).

### 1.2 Stateful operators that are NOT ISnapshotable

| Op | file:line | State | Why it is safe today |
|---|---|---|---|
| `ArrangeOp` | Stateful/Arrangement.cs:57 | shared `IndexedZSetTrace` | Only built when `ShareArrangements && SnapshotCodecs is null` (PlanToCircuit.cs:3147-3150). `ShareArrangements` defaults to false. The builder is `internal`. |
| `SpineArrangeOp` | Spine/SpineArrangement.cs:61 | shared spine trace | same gate |
| `IncrementalJoinSharedRightOp` | Stateful/IncrementalJoinSharedRightOp.cs:34 | private **left** trace | same gate. **If the gate were ever loosened, `Snapshot.WriteAsync` would silently skip this op (non-ISnapshotable ops are just `continue`d at Snapshot.cs:93) and restore would yield an empty left trace.** |
| `SpineIncrementalJoinSharedRightOp` | Spine/SpineIncrementalJoinSharedRightOp.cs:29 | private left spine trace | same |
| `DelayOp<T>` (z^-1) | Circuit/Operators/DelayOp.cs:16 | `_nextOutput` | Not used by SQL. `CircuitBuilder.Delay` is **public** (CircuitBuilder.cs:177), so a hand-built circuit that uses it **silently loses that state on restore**. |
| Exchanges (`ExchangeOp`, `ExchangeIndexOp`, `ExchangeIndexJoinOp`, `ExchangeIndexStepOp`, `BroadcastExchangeOp`) | Circuit/Operators/*.cs | none across ticks. `ExchangeCoordinator._mailbox` is a W×W grid overwritten every tick | Synchronous BSP: nothing is in flight at a tick boundary. |

`PersistenceConformanceTests.CoverageGuard` reflects over ISnapshotable types only, so **it cannot catch a new stateful op that forgets to implement ISnapshotable.**

---

## 2. Operator identity

- **Positional.** State is keyed `snap-{tick}/op-{i}/` with `i` = index in `RootCircuit.Operators` (Snapshot.cs:98). The manifest records `operator_count` and `snapshotted_indices`.
- **Guards** (all throw `InvalidDataException`): plan fingerprint = XxHash3 over `"{i}:{Type.FullName};"` for every operator, generic args included (SnapshotManifest.cs:107-121). `IFingerprintAlias` provides an alternate hash so an old snapshot can load into a replacement op (`LinearAggregateOp`, `PartitionedTopKCompactOp`). Also checked: schema fingerprint = hash of each op's codec `SchemaFingerprint` (catches VARCHAR length, DECIMAL p/s, column reorder), operator count, and `schema_version` (currently 4).
- **Stable across a recompile of the same SQL:** yes. Compilation is deterministic, and `OperatorIdentityProbeTests.Checkpoint_SurvivesRecompileOfTheSameProgram` pins it. **Not stable across program edits:** adding or removing an *output* view shifts indices and the load is rejected (fail-safe, `…IsRejected_WhenAnUnrelatedViewIsAdded/Removed`). Non-output views are pruned and do not count. Stable naming (`{view}/{kind}#{ordinal}` from `RootCircuit.CurrentBuildLabel`) was deliberately deferred (design-durable-identity.md §1). The typed compiler does not set build labels.
- Spine batches have process-global monotone ids (`SpineBatchId`, SpineBatch.cs). Nothing persists them yet: seeding on restore is deferred, and Track B stages 2/3 are RETIRED.

---

## 3. Parallel path

**`ParallelSnapshot`** (src/DbspNet.Persistence/ParallelSnapshot.cs):
- `WriteAsync` (:44-76) runs one `Snapshot.WriteAsync(replica[w], PrefixedFS("worker-{w}/"), retainCount)` per replica **concurrently**, then writes `parallel.json` = `{schema_version:1, workers:W, tick}` through `WriteAllBytesAsync` (not tmp+rename). It accepts **no metadata** parameter, so there is no way to commit offsets with it.
- `ReadAsync` (:111-167) checks only that `parallel.json` exists, its schema version, and `workers == W`. Each replica then restores through `Snapshot.ReadAsync(..., perReplica = max(1, DefaultRestoreParallelism / W))`. **It never compares `manifest.Tick` with the workers' ticks, or the workers' ticks with each other.**
- Partition stability: inputs shard by whole-row `StableHash`, so the same W and the same plan put every key in the same place. A different W is refused.

**Commit 5cd1c7a ("parallel restore")** is *intra-circuit* concurrency, not a W-replica feature. `Snapshot.ReadAsync(circuit, fs, parallelism)` runs `Parallel.ForEachAsync` over `manifest.SnapshottedIndices` (Snapshot.cs:419-450). The default degree is `clamp(ProcessorCount/2, 1, 8)`; `DBSPNET_RESTORE_PARALLEL` overrides it, and degree 14 measured slower than 8. Failures are rethrown unwrapped. `ProfileLoad` forces the sequential walk. Safety rests on disjoint per-op state, which the CSE gate guarantees (shared arrangements are off when codecs are set). Under `ParallelSnapshot` the degree is divided by W.

**Why parallel programs are never checkpointed:**
- `ICheckpointStore` is typed on `RootCircuit` (ICheckpointStore.cs:26,30), and `SnapshotCheckpointStore` calls the single-circuit `Snapshot.WriteAsync/ReadAsync` (:67, :79).
- The parallel `ProgramRunner.CreateAsync(ParallelCompiledProgram, …)` has no store parameter and passes `checkpoint: null` (ProgramRunner.cs:142-152; doc comments at :28-31 and :138-140). `RestoreAsync` and `CheckpointAsync` use `_program.SerialCircuit!` (:207, :240), which is `null` for the parallel `RunnerProgram` (RunnerProgram.cs:90).
- `DbspNetEngine.DeployAsync`: at :108-110, `if (_workers > 1 && persistent) fallback = "checkpointing is configured, and only the serial program checkpoints";`. It then compiles serially with codecs (:131-134) and builds a `SnapshotCheckpointStore` (:144). The doc comment at :78-82 says "the per-worker parallel snapshot has no offset-atomic store yet". Note that `SqlProgram.TryCompileParallel` *does* accept `snapshotCodecs` (SqlProgram.cs:48-60); the engine never reaches that call when persistent.

**What a consistent parallel checkpoint would need:**
1. **An atomic cross-worker commit.** Today each worker's `current.txt` rotation and prune are independent (each one runs Snapshot.cs:116-118). That needs to become a single top-level pointer naming the tick, written after every worker's files. Per-worker snapshot dirs must not be pruned until that pointer commits. Restore must verify that all W worker manifests carry the pointer's tick.
2. **An offset-atomic store:** `ParallelSnapshot.WriteAsync` with metadata (connector offsets) in the top-level manifest, plus an `ICheckpointStore` overload (or abstraction) over `ParallelCircuit`, and `ProgramRunner` wiring for the parallel overload.
3. **Exchange buffers: nothing needed.** Synchronous BSP; `ExchangeCoordinator._mailbox` holds no cross-tick data, and a snapshot between `Step()`s, with the workers parked, sees no data in flight. Writes must not overlap a Step, which today's runner guarantees.
4. **Per-worker clocks:** `LogicalTime` and `TickCount` are per replica and restored from each worker's manifest. `ParallelCircuit.TickCount` reads replica 0. They are consistent only if all workers snapshot at one tick, which #1 enforces. `AdvanceTime` fans out uniformly.
5. **Driver-side state:**
   - `ParallelProgramOutput` caches (`_cached*`, `_rowCount`) are derived and reset by a tick mismatch.
   - `ShardedInputHandle` queues are empty after a Step.
   - `ProgramRunner._incremental` (`Pending`, `Written`) is not checkpointed on **either** path. That is by design: after a restart, the first write carries the full view and truncates the sink.
   - Output views are already in-circuit `IntegrateOp`s inside the worker subtrees (§10.4).
6. Possible semantic issue (not persistence): `LatenessOperator` tracks `maxSeen` per replica, so each worker's frontier depends on its own shard. Check this before trusting parallel GC plus restore.

---

## 4. Checkpoint flow (ProgramRunner / ICheckpointStore)

- **When:** at the end of every `RunBatchAsync`, after `DrainAsync` (one engine tick per source version, round-robin) and `WriteOutputsAsync`, runs `CheckpointAsync` (ProgramRunner.cs:376-405). This is unconditional and happens even with 0 ticks (finding 1). It is also callable directly for a forced checkpoint.
- **What:** `SnapshotCheckpointStore.SaveAsync` serialises `[SourceCheckpoint(name, cursor.Serialize())]` to JSON under manifest metadata key `connector.offsets` and calls `Snapshot.WriteAsync(circuit, fs, metadata, retainCount=1)`.
- **Commit order:** (1) per-op files written directly to final keys `snap-T/op-i/*`, sequentially; (2) `snap-T/manifest.json` carrying plan/schema fingerprints, tick, op count, indices, `logical_time` and metadata (offsets); (3) `current.txt` replaced by delete followed by rename of `current.txt.tmp`; (4) older `snap-*` pruned, best effort. Offsets and state are atomic **relative to the pointer** (finding 1 lists the holes).
- **Restore:** `RestoreAsync` → `TryRestoreAsync` (returns `null` if no pointer, which means a fresh start) → `Snapshot.ReadAsync` validates and restores tick and logical time, then loads ops in parallel → `ReadMetadataAsync` reads the pointer **again** (fine under a single writer) → each `b.Cursor = ParseOffset(...)`.
- **Ordering:** outputs are written before state commits, so a crash between the two re-processes the batch. That gives at-least-once; exactly-once needs idempotent sinks.
- **Checkpoint policy (A2: WAL each batch, snapshot every N): NOT BUILT.**
  - No interval or policy knob exists anywhere (searched for `SnapshotEvery`, `CheckpointInterval` and similar).
  - `WalRecorder` (src/DbspNet.Persistence/WalRecorder.cs) works over `ICompiledCircuit` (A1, done) but has no non-test caller.
  - A WAL segment becomes durable only on `DisposeAsync` or `WriteSnapshotAsync`, which write the IPC trailer and the manifest. A mid-run crash loses the open segment.
  - Its snapshots pass no metadata, so the WAL path carries no connector offsets.
  - The WAL fingerprint covers only the input schemas.

---

## 5. Spine family

- **Immutable and sorted:** `ResidentSpineBatch` has `readonly TKey[] _keys` and `TWeight[] _weights` (sorted by the comparer), a bloom filter, min/max monotone keys and an `Id` (SpineBatch.cs:~169-180). Batches are sealed. Compaction (`SpineZSetTrace.Apply`, ~:490-520) merges the oldest `BatchCount` batches at a level into a **new** batch with a new id. Spilling and materialising are relocations and keep the id.
- **Spill** (`SpineSpillConfig` / `SpineIndexedSpillConfig`, SpineSpillConfig.cs; `MaybeSpill`, SpineZSetTrace.cs:525-559):
  - When a compaction output lands at level ≥ `MinSpillLevel` (default 2), it is materialised into a dict ZSet and saved synchronously through the codec to `{Prefix}/batch_{n}.arrows`. Here `n` is the per-trace `_spillCounter`: process-local, it resets on restart, so names are reused.
  - A `SpilledSpineBatch` keeps the bloom and count in RAM and reloads transiently when a probe passes the bloom.
  - **Freed** by `SyncDelete` when compaction consumes the batch (:512-516) or when lateness GC drops it (:182-203).
  - Not freed on trace disposal or process exit, so files leak.
  - **No production code configures spill:** the typed compiler passes `null` (TypedPlanCompiler.cs:4387-4504), PlanToCircuit never sets it, and only tests use it.
- **Snapshot = re-serialisation, not references.** `GetBatches()` flushes the memtable, then copies every non-empty batch through a fresh `ZSetBuilder` or `IndexedZSetBuilder`; spilled batches are reloaded from disk. `SpineSnapshot.SaveAsync` writes `{prefix}.manifest` (an ASCII count) plus positional `{prefix}.batch_{i}.arrows`. On load, each batch is `Integrate`d, which rebuilds a sorted batch at L0 and compacts. Level structure and ids are lost. Snapshot files and spill files are completely separate.
- Spine is not the default family (Flat is). Measured context: the bulk batch yields about 1.03 batches per trace, so a reference-manifest scheme would not help step cost there.

---

## 6. Recovery test coverage

**Unit and conformance (in-process, value-exact):**
- `PersistenceConformanceTests.RestoreThenStep_EqualsUninterrupted` covers 14 SQL shapes × {flat, spine}: distinct, exact/minmax/float/distinct aggregates, inner/left/full join, window aggregate, LAG/LEAD, partitioned and global rank, global and partitioned top-K. It snapshots at tick 6, restores into a fresh compile, steps through tick 9, and compares against an uninterrupted run with doubles rendered via `"R"`.
  - The schedule is load-bearing: repeated rows give weight above 1, a 1e16 magnitude makes fold-order drift visible, and retractions happen around the boundary.
  - **Serial `SqlProgram` (structural) only.**
- `CoverageGuard` fails if an ISnapshotable has no case and no exemption. Exempt: Lateness → LatenessSnapshotTests; TemporalFilter → LogicalClockSnapshotTests; both fixpoints → RecursiveCteSnapshotTests (restore-then-step, both families); `PartitionedTopKNarrowOp` (not selectable from SQL); `LinearAggregateOp` → LinearAggregateTests; `PartitionedTopKOp` legacy arm → PartitionedTopKSnapshotTests.
- Also present: FloatAggregateRestoreTests (the §7.2 regression), AggregatorEmptyDeltaTests, Join/Aggregate/Distinct/Integrate/TopK/PartitionedRank/WindowAggregate/Spine snapshot tests, SchemaDriftTests (8), AtomicSnapshotTests (8: throw mid-save, orphan dir, stale pointer, same-tick overwrite), SnapshotRetentionTests (9), OperatorIdentityProbeTests (3), TypedSnapshotCrossContextTests (1), WalRecorder/HybridSnapshotWal/ProgramWalRecorder tests, ParallelRestoreTests (5: degree invariance, unwrapped failure).
- Connectors: ProgramRunnerCheckpointTests (3: resume without replay or skip, every batch checkpoints), CheckpointStoreTests (2), ProgramCheckpointCseTests (2).
- Parallel: ParallelProgramSnapshotTests (W = 1/2/4/8; GROUP BY, DISTINCT, projection; restore, then step once and compare to the reference; W mismatch refused) and ParallelSnapshotTests (typed GROUP BY at several W; layout and marker-last).

**Digest-verified (real ivm-bench SF=3, env-gated scratch probes, which do not run in CI):**
- `tests/DbspNet.Tests/Scratch/IvmRestoreProfile.cs` is **cross-process** (record mode, then profile/replay mode) and its digest uses **`StableHash`** per cell (:460-478). It verifies the restore digest and the restore+replay digest.
- `tests/DbspNet.Tests/Scratch/IvmRecoveryProbe.cs` digests with `object.GetHashCode()` (:490-515). That is valid only **in-process**: record and recover in one run (snapshot-only, snapshot+WAL, restore+connector replay, WAL-only).
- Both are gated on `IVM_DATA_ROOT`, `IVM_SPEC`, `IVM_SNAPSHOT_DIR` and related variables.

**Gaps:**
1. Typed-path conformance, the float aggregates in particular (finding 3).
2. Parallel restore-then-step for joins, top-K, windows, temporal filters and lateness. Parallel tests cover aggregate, distinct and projection only.
3. Any crash-injection test: mid-overwrite at the same tick, between the delete and the rename of `current.txt`, or partway through a parallel write leaving mixed worker ticks.
4. A cross-process restore check in CI. Only the gated probe does this; all unit tests restore into a fresh compile inside the same process.
5. `SpineIncrementalFullJoinOp` has no dedicated test; it is reached only through conformance `join_full` + spine.
6. Spill and snapshot interplay (snapshotting a trace whose batches are spilled).
7. `DelayOp` and other non-ISnapshotable state: the guard is blind to it.
8. An idle-batch checkpoint, i.e. 0 ticks followed by a checkpoint.
