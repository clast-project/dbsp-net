# Starting prompt — next arc: a global LATENESS frontier for the parallel paths

> Kickoff for issue #3's last large item, written 2026-11-15 after the serial program path landed
> (02a3402: late-row drop plus cross-view frontiers; checkpoint coverage in
> `ProgramLatenessCheckpointTests`). Paste the block below into a new session.

---

**Parallel LATENESS: a frontier shared by all workers. Design first, then build.**

**Goal.** A program or query that declares `LATENESS` should run data-parallel (W > 1), with
results identical to the serial path, and with state bounded by the same frontiers. Today both
parallel compiles refuse LATENESS (a `ParallelBlockers` entry: "LATENESS on <table> …"), so
`DbspNetEngine` logs the blocker and runs the program **serially**. A long-running append-mostly
pipeline, which is exactly the workload LATENESS exists for, therefore gets one core.

**Why it is not just wiring.** The serial `LatenessOperator` drops a row when its key is below
`maxSeen − lateness`, with `maxSeen` as it stood at the start of the tick. In a parallel circuit,
tables are sharded by whole-row hash, so each worker sees only its own shard's max. Per-worker
frontiers would therefore drop different rows than the serial path, and the results would diverge.
Combining the workers' frontiers with a min would be sound for GC, but it would still change which
rows are dropped. What matches the serial path is the **global max** of `maxSeen` across workers,
agreed at a step boundary. Core has no primitive for that today: no scalar all-reduce or broadcast
across replicas.

**Feldera does exactly this — read it first.**
`../feldera/crates/dbsp/src/operator/dynamic/time_series/waterline.rs`,
`dyn_waterline_monotonic`:
- Each worker runs a local `stream_fold`: the max of the old waterline and the largest key in the
  batch, which is cheap because the batch is sorted.
- `new_exchange_operators` then sends every worker's value to every worker, and the receiver
  combines them with `max`.
- This runs on every step (`ExchangeActivity::AllSteps`).

So their global waterline is an all-reduce over the existing exchange machinery, one scalar per
worker per step. Also check:
- how their SQL compiler wires LATENESS through `controlled_filter.rs`;
- which step's waterline a row is filtered against (ours uses start-of-tick);
- how `integrate_trace_retain_keys` consumes the waterline (`operator/dynamic/trace.rs`).

Policy (memory `adopt-feldera-assumptions-by-default`): match Feldera's semantics unless there is a
measured reason not to.

**Read what exists here.**
- `src/DbspNet.Core/Operators/Stateful/LatenessOperator.cs`: start-of-tick drop rule, `maxSeen`
  persistence (`LoadAsync` re-advances the frontier), `IIntrospectable`. `Frontier.cs` has
  `MutableFrontier`, `MinFrontier` and `TransformedFrontier`.
- `src/DbspNet.Core/Circuit/ParallelCircuit.cs`: two barriers (driver "go"/"done" plus
  `_workerBarrier` / `WorkerPhaseSync`) and `IExchangeCoordinator`. Exchange cost at fine ticks is
  barrier-bound (memory `exchange-scaling-decomposition`, design §15), so **an extra exchange round
  per step is the risk to price**. Look for a way to piggyback the scalar on an existing rendezvous
  rather than adding a phase.
- `src/DbspNet.Sql/Compiler/PlanToCircuit.cs`:
  - `CompileCore` (single-query serial wiring) and `CompileProgram` (program serial wiring: one
    `EnforceLateness` per column on the shared table stream, and a program-wide `frontiers` map).
  - `TryCompileParallel` and `TryCompileProgramParallel`, which pass `emptyFrontiers` today.
  - `CollectParallelBlockers`: the `ScanPlan s when s.ColumnLateness …` case is what to remove.
  - The `Resolve*Frontier` GC sites.
- `src/DbspNet.Sql/Plan/MonotonicityAnalyzer.cs`. Its program seeds (`viewColumns`,
  `inlineViews`, `RootColumns`) should carry over to the parallel program unchanged, because they
  are plan-level.
- Temporal filters (`NOW()`) are refused on the single-query parallel path for the same reason.
  The clock is already global (`LogicalClock`, the same tick on every worker), so they may only
  need their frontiers registered. Check whether they can ride along in this arc.
- Persistence:
  - `docs/design-durable-state.md` says `LatenessOperator` is unreachable on the parallel path, and
    that reshard (increment 3: restore with a different W) needs a merge rule for per-worker scalar
    state. With an all-reduced frontier, every worker holds the same value, so the merge rule might
    be trivial. Verify that against what is actually persisted: is it the local or the global
    `maxSeen`?
  - `ParallelSnapshot`, `ParallelCrashSafeCheckpointTests`.

**Questions the design must answer before code.**
1. **Semantics.** Does a row on worker w get dropped against the global frontier as of the start of
   the tick? That matches serial exactly. Or against something else, and if so, what does Feldera
   do?
2. **Mechanism.** A new scalar all-reduce primitive in Core, an exchange of a 1-row Z-set, or a
   value folded into an existing barrier phase? Measure the per-step cost at W=12 on a fine-tick
   workload (Nexmark at small batches) against the current no-LATENESS baseline, using `w1profile`
   / B/ev for allocation and wall time over ≥5 runs.
3. **Where frontiers live.** One `MutableFrontier` per worker, all advanced to the same global
   value, or one object shared across replicas? GC sites read frontiers on worker threads. Check
   thread-safety and the ordering against the barrier.
4. **Checkpoint and reshard.** What each replica persists, what restore with a different W
   rebuilds, and the crash matrix (extend `ParallelCrashSafeCheckpointTests`).
5. **Scope.** Single-query parallel, program parallel, temporal filters: all three, or which first?

**Acceptance.**
- A parallel run is identical to a serial run, for the single-query and program paths, flat and
  spine. Extend `LatenessPbtTests` and `ProgramLatenessPbtTests` with parallel arms at W ∈ {2, 3, 4}
  that compare against the serial compile, not only against the batch oracle. A parallel-vs-parallel
  oracle can pass while both runs are wrong (memory `parallel-view-in-circuit`).
- GC fires on every worker: retained state stays bounded, as in the serial unit tests.
- Mutation checks, each of which must fail a test:
  - per-worker local max instead of the global one;
  - an end-of-tick frontier instead of start-of-tick;
  - `maxSeen` not restored on one replica.
- Restore at the same W and at a different W, including the crash matrix.
- No regression on programs without LATENESS. Nexmark W=14 and B/ev should be unchanged, since the
  new machinery should not exist in the circuit at all when nothing declares LATENESS.
- Remove the LATENESS blocker. Update `README.md` (Not supported), `ARCHITECTURE.md` (LATENESS
  section), the issue #3 comment thread, and the memory `program-lateness`.

**Working style.** Design doc first (a section in `docs/design-structural-parallel.md` or a new
`docs/design-parallel-frontier.md`), then build in small commits on main. Run the full suite after
a clean rebuild before each commit (`Sql.dll` incremental builds are flaky; memory
`residual-pushdown-next`). Hand Curt any Docker commands rather than running them.
