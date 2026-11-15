# Design: a LATENESS frontier shared by all workers (issue #3)

**Status: BUILT, 2026-11-15** (§5 has the results). Kickoff: `docs/next-arc-parallel-frontier-prompt.md`. Companions:
`docs/design-structural-parallel.md` (the exchange-inserting parallel compile),
`docs/design-durable-state.md` §1d (reshard), `docs/now-and-temporal-filters.md`.

## 0. The problem

Today both parallel compiles refuse `LATENESS`. `CollectParallelBlockers` returns "LATENESS on <t> (late
rows and GC are not coordinated across workers yet)", and `TryCompileParallel` refuses as well. A
pipeline that declares `LATENESS` therefore runs on one core.

The serial `LatenessOperator` drops a row when `key < maxSeen − lateness`, using `maxSeen` from the
start of the tick, and then advances `maxSeen` with the rows it admitted. A parallel circuit shards a
table by whole-row hash, so each replica sees only its own shard. A frontier built from one shard
drops different rows than the serial path does. A min across the shards would be sound for GC, but it
too changes which rows are dropped. Only the **global** `maxSeen`, agreed at a step boundary,
reproduces serial results.

## 1. What Feldera does (read 2026-11-15, `../feldera` @ 78afc9077)

- `dyn_waterline_monotonic` (`operator/dynamic/time_series/waterline.rs`):
  1. Each worker computes `stream_fold(max(old, largest key in batch))`.
  2. `new_exchange_operators` sends that scalar to every worker.
  3. The receiver folds the incoming values with `max`.
  4. This runs as `ExchangeActivity::AllSteps`, so it is one exchange round per step whether or not
     there is data.
  The SQL path uses the general `dyn_waterline` variant, which has the same exchange.
- `InsertLimiters.processLateness` builds `waterline → DBSPDelayOperator → ControlledKeyFilter` over
  the source. The filter reads the **delayed** waterline: the global value as of the end of the
  previous step, which is the start of this tick. **That is our serial rule exactly.** Retain-keys GC
  (`integrate_trace_retain_keys`) is also fed through `createDelay`.
- Their waterline folds the pre-filter input, including late rows. That makes no difference to the
  max: a late row is below `max − lateness ≤ max`.
- Known difference, out of scope: Feldera computes one waterline tuple over all of a table's LATENESS
  columns and drops a row if any column is late. We chain one operator per column, so column B's max
  only counts rows that column A admitted. This is a pre-existing serial difference and stays as it
  is in this arc.

## 2. Answers

### Q1 Semantics: drop against the global frontier as it stood at the start of the tick

A row on worker w is dropped iff `key < G(t−1) − lateness`, where `G(t−1)` is the max over all
workers of the admitted keys in ticks `< t`. This is the serial rule, and Feldera's (delayed
waterline). Per-row decisions then depend only on the row and on `G`, which every worker agrees on.
The admitted set is therefore identical to the serial one for any sharding, and any W gives serial
results.

The **GC frontier** inside tick t on worker w is `max(G(t−1), local_w(t)) − lateness`: the existing
operator code advances its frontier with this tick's local max, as it does today. That value is at
most the serial in-tick frontier `G(t) − lateness`, so it is sound by the same argument as serial:
- every stateful operator GCs after integrating the tick's delta;
- every future row is at or above `G(t) − lateness`.

GC only reclaims state; no operator's output reads a frontier (checked: every `IFrontier` consumer
uses it only in `CollectGarbage` or in metrics). So a per-worker GC frontier that lags slightly cannot
change results. It only bounds state a little later than serial does, by at most one tick.

### Q2 Mechanism: a driver-side reduction between "done" and the next "go", with no new barrier

`ParallelCircuit.Step` is already two rendezvous on `_barrier`:
1. "go" releases the parked workers;
2. "done" waits until every replica has finished its tick.

Between "done" and the next "go", every worker is parked and the controller thread is free. So the
controller can read each replica's local `maxSeen`, take the max, and write it back into every
replica, with no exchange and no extra phase. `Barrier.SignalAndWait` is a full fence on both sides,
so the workers see the write when they wake.

- **Cost.** For each LATENESS column, W reads plus W writes of a `long`, serially on the controller,
  per step. That is tens of nanoseconds at W=12, against a barrier rendezvous measured in
  microseconds (§15). It is strictly cheaper than Feldera's all-to-all round. Q2's measurement (§5)
  prices it directly.
- **Core primitive.** It is generic and small:
  - `IStepBoundaryReduction` (`void Reduce()`): run by the driver after every parallel step, with
    all workers parked.
  - `GlobalMaxReduction`: groups the same-position participant from each replica, using the
    positional registry pattern `NextCoordinator` already uses for exchanges. The i-th registration
    in every replica's identical build maps to `reductions[i]`.
  - `IGlobalMaxParticipant` (`TryGetLocalMax`, `AdoptGlobalMax`): implemented by
    `LatenessOperator`.
  - `CircuitBuilder.JoinGlobalMax(participant)`, a no-op on a serial build and at W=1. With nothing
    registered, `Step` loops over an empty array, so programs without LATENESS build exactly the
    same circuit as today.
- **Alternatives rejected.**
  - An exchange of a 1-row Z-set per step: it adds a full exchange rendezvous, which §15 says is the
    cost that matters at fine ticks.
  - Folding the scalar into the `WorkerPhaseSync` barrier: that barrier belongs to
    `RunDataParallel`, not to `Step`.

### Q3 Where frontiers live: one `MutableFrontier` per replica

Each replica keeps its own `LatenessOperator`, `maxSeen` and `MutableFrontier`, as on the serial path.
`AdoptGlobalMax(g)` sets `maxSeen = max(maxSeen, g)` and advances that replica's frontier to
`g − lateness`.

- Workers only ever read their own replica's frontier.
- The controller writes only while every worker is parked.
- So there is no shared mutable object and no new synchronisation.

The GC sites (`Resolve*Frontier`, `MinFrontier`, `TransformedFrontier`) are unchanged. Each replica's
build closure creates its own `frontiers` dictionary, which is what `CompileProgram` already does
inside its closure. The parallel paths stop passing `emptyFrontiers`.

### Q4 Checkpoint and reshard

- **What is persisted.** Snapshots are taken between steps, after the reduction. So every replica
  persists the same, global `maxSeen`, and the file format (`frontier.bin`) is unchanged.
- **Same-W restore.** Each replica loads its own file. There is deliberately **no** reduction on
  restore, because the persisted values are already global. A replica that failed to restore would
  therefore admit late rows on its shard in the first tick, and a test can see that. This is the
  required "maxSeen not restored on one replica" mutant.
- **Reshard (W changes).** A new `ReshardFile` kind, `IdenticalBlobReshardFile`:
  - Self-check: the file must be byte-identical on every old worker, and the rewrite refuses if it is
    not.
  - The file is copied to every new worker.
  - This replaces the old plan in `design-durable-state.md` §1d ("maxSeen takes the minimum"). That
    plan is wrong under drop semantics, because a smaller frontier admits rows the serial path drops.
  - `LatenessOperator` gets a `ReshardSpec` at its program-parallel build site.
- **Crash matrix.** Extend `ParallelCrashSafeCheckpointTests` with a LATENESS view. Every crash point
  must restore to a state that continues identically to an uninterrupted serial run.

### Q5 Scope and order

1. **Core**: the reduction primitive, and `LatenessOperator` as a participant (unit-tested on a raw
   `ParallelCircuit`).
2. **Single-query parallel** (`TryCompileParallel`): enforce per table and pass per-replica frontiers.
3. **Program parallel** (`TryCompileProgramParallel`):
   - enforce on the shared table stream;
   - seed program-wide frontiers and the `viewMonotonicity` analyzer, as `CompileProgram` does (the
     parallel program has no single-use inlining, so `inlineViews` is empty);
   - record the reshard spec;
   - remove the blocker.
4. **Temporal filters (`NOW()`)** on the single-query parallel path. The clock is already global
   (`ParallelCircuit.AdvanceTime` fans it out), and `TemporalFilterOp` is a per-row filter, so this
   only needs:
   - the frontier registration;
   - partition passthrough;
   - removal of the single-query refusal.

   The program-parallel path keeps a temporal-filter blocker for now. `TemporalFilterOp` state would
   need a reshard placement equal to its input's partition, and an elided downstream exchange makes a
   wrong placement silently wrong. That is a follow-on, recorded rather than guessed.
5. The typed parallel compile (`TypedPlanCompiler.TryCompileParallel`) is out of scope. It used to
   compile a LATENESS scan and silently skip the drop. It now refuses one in standalone mode, so
   callers compile structurally. In boundary mode (typed program views) the structural table stream
   is already enforced.

## 3. Acceptance

- **Serial vs parallel PBT.** Parallel arms at W ∈ {2, 3, 4} in `LatenessPbtTests` and
  `ProgramLatenessPbtTests`, flat and spine. Each arm compares against the **serial compile**, not
  only against the batch oracle (memory `parallel-view-in-circuit`).
- **GC fires on every worker.** Per-replica retained state stays bounded under an advancing stream.
- **Mutants that must fail a test:**
  - skipping the reduction, so each replica uses its local max;
  - an end-of-tick threshold;
  - `maxSeen` not restored on one replica.
- **Restore** at the same W and at a different W, plus the crash matrix.
- **No regression without LATENESS.** The circuit is unchanged, so Nexmark B/ev should be identical.

## 4. Docs to update when built

- `README.md` (Not supported)
- `ARCHITECTURE.md` (LATENESS)
- `design-durable-state.md` (§1d merge rule, and the "unreachable" list)
- issue #3
- memory `program-lateness`

## 5. Results (2026-11-15, M4 Pro)

**Built.**
- Core:
  - `IStepBoundaryReduction` and `GlobalMaxReduction` (`Core/Circuit/StepBoundaryReduction.cs`);
  - `CircuitBuilder.JoinGlobalMax`;
  - `ParallelCircuit.Step` runs the reductions after "done".
- `LatenessOperator` is a participant.
- Both structural parallel compiles enforce LATENESS through the same `EnforceDeclaredLateness`
  helper the serial paths now use, with per-replica frontiers.
- The program path seeds `viewMonotonicity`, as `CompileProgram` does.
- The LATENESS blocker is gone.
- The single-query parallel path also takes temporal filters (`RegisterTemporalFrontiers`). The
  program path still refuses them.
- Reshard: `IdenticalBlobReshardFile`.

**A bug found on the way: the stream-release scan crossed replicas.** `StreamRelease.ScanReaders`
walks operator fields to find each stream's readers. A closure that had captured the
`CircuitBuilder` led it to `ParallelBuildContext`, then to the reduction, then to *every* replica's
`LatenessOperator` and its streams. Replica 0's release plan then released replica 1's streams from
replica 0's thread. The parallel PBTs caught it as an NRE in `Stream.Release` (a check-then-read
race on `Current`), seen only under load.
- Fix: the scanner treats `ParallelBuildContext` and `IStepBoundaryReduction` as stream-free.
- Regression test: `StreamReleaseScan_StaysInsideItsOwnReplica`, which fails with the exclusion
  removed.
- The exchange coordinators were already reachable the same way, but they hold no streams.

**Fine-tick cost** (`parlat`, W=12, ServerGC, DATAS off, 6 runs alternated, medians):

| ticks × rows/tick | arm | µs/tick | B/ev | retained rows |
|---|---|--:|--:|--:|
| 20,000 × 100 | no LATENESS | 271.6 | 5165.3 | 400,138 |
| 20,000 × 100 | LATENESS 50 | 255.0 | 5139.6 | 200,074 |
| 50,000 × 1 | no LATENESS | 17.2 | 11966.3 | 96,248 |
| 50,000 × 1 | LATENESS 50 | 17.4 | 12174.1 | — |

- **The reduction itself** is 14–15 ns per LATENESS column per step at W=12, with 0 B allocated. It
  was timed in isolation over the W real operators. That is 0.08% of a 1-row tick (about 18 µs,
  barrier-bound) and 0.006% of a 100-row tick. **No extra rendezvous was needed**, so the §15
  barrier cost is untouched.
- **1 row/tick.** The remaining +208 B per event for LATENESS comes from two costs that exist on
  the serial path too:
  - the ~2% late ticks copying the delta;
  - the aggregate's GC path.
- **100 rows/tick.** LATENESS is 6% faster and allocates less, because GC halves retained state.
- **Allocation fix to the operator.** Built along the way: the late filter used to copy every
  non-empty delta. It now passes the delta through untouched when no row is late, which took about
  290 B per event off the 1-row/tick LATENESS arm (12463.9 → 12174.1).

**No regression without LATENESS.**
- `w1profile 1000000 10000 3`: B/ev is byte-identical to HEAD (5c669e0) on all ten queries (391,
  507, 244, 618, 37, 778, 1690, 638, 850, 725).
- At W>1 without LATENESS, the circuit is identical: no reduction is registered, and `Step` loops
  over an empty array.
- Nexmark W=14 wall time was not re-run, because nothing on that path changed.

**Correctness.**
- **PBT arms.** `LatenessPbtTests` / `ProgramLatenessPbtTests` / `TemporalFilterPbtTests` now have
  parallel-vs-**serial** arms at W ∈ {2, 3, 4}. The LATENESS arms cover flat and spine.
- **GC.** `ParallelProgram_GcFiresOnEveryWorker` asserts, on every replica, that the join and
  aggregate frontiers reach 195 and that state is bounded.
- **Checkpoint.** `ParallelLatenessCheckpointTests` covers:
  - same-W restore, flat and spine;
  - reshard 2↔3, 4→2, 2→4;
  - the identical-copy self-check refusing a tampered `frontier.bin` and committing nothing;
  - placements.
- **Crash matrix.** `ParallelCrashSafeCheckpointTests` now carries a LATENESS view, with late rows
  in the continuation. The wide program's `e` declares LATENESS again.
- **Full suite** after a clean rebuild: 3844 pass, 1 skipped.
- **Test renamed.** `WindowAggregateMonomorphizeTests.MonoIncrementalEqualsBatch_UnderLatenessGc`
  is now `…_MonotoneInsertOnly`, without LATENESS. The typed path never wired LATENESS GC, so the
  old name promised GC that test did not exercise. The typed standalone compile now refuses the
  table.

**Mutation checks** (each was reverted after the run):

| mutant | fails |
|---|---|
| reduction skipped (per-worker local max) | 5/5 Core parallel tests; all 4 parallel PBT arms; the 3 `GcFiresOnEveryWorker` cases; `DropsLateRowsAgainstTheGlobalMax` |
| end-of-tick threshold (this tick's max counts) | Core `MatchesSerial` ×3; all parallel PBT arms; the serial oracles too |
| `maxSeen` not restored on one replica | 9/11 `ParallelLatenessCheckpointTests` (rows diverge at tick 6 even with the frontier asserts removed); 4/4 crash-matrix cases; 2/3 wide-program cases |
| stream-release scan not excluding the registries | `StreamReleaseScan_StaysInsideItsOwnReplica` |

**Left open.**
- Temporal filters on the program-parallel path. This needs a `TemporalFilterOp` reshard placement
  equal to its input's partition.
- The typed paths' LATENESS (refused or GC-less, as before).
- Feldera's one-waterline-per-table drop rule versus our per-column chain (§1). This is a serial
  difference that the parallel arc does not change.
