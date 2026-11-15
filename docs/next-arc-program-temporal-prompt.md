# Starting prompt — next arc: temporal filters (`NOW()`) on the program path

> Kickoff for issue #3's last item, written 2026-11-15 after parallel LATENESS landed
> (6f1fcec, 5aa9a35, 28bed4a; `docs/design-parallel-frontier.md`). Paste the block below into a new
> session.

---

**Temporal filters on the program path: a clock that moves, GC that fires, and then parallel.
Design first, then build.**

**Goal.** A program (`SqlProgram` / `CompileProgram`, and the server that runs it) with a view like
`SELECT … FROM e WHERE ts > NOW() - INTERVAL '1' HOUR` must do three things:
- emit the retractions as time passes;
- keep bounded state downstream, exactly as the single-query path does;
- run data-parallel, with results identical to serial.

**What is known (verified 2026-11-15, not yet acted on).**
1. **Nothing advances a program's clock.** `CompiledQuery` and `ParallelStructuralCompiledQuery`
   have `AdvanceClock`; `CompiledProgram` and `ParallelCompiledProgram` do not. A grep of
   `DbspNet.Sql`, `DbspNet.Server` and `DbspNet.Connectors.Abstractions` finds no caller of
   `AdvanceTime` on the program or server path. So a `NOW()` view in a deployed program is likely
   evaluated against a clock that never moves.

   **Confirm this first with a failing test.** If it is true, it is the real bug, and bigger than
   missing GC.
2. **No GC frontier on the serial program path.** `CompileProgram` never registers the clock-driven
   frontier, so downstream GROUP BY / join / DISTINCT on the time key does not GC. The single-query
   path does this with `RegisterTemporalFrontiers` (`PlanToCircuit.cs`), using the per-view
   `CollectScans(...).TemporalFrontierSpecs`. Two cases need checking:
   - the spec's `LatenessSource` points at a base-table column;
   - for a view-scan, it may need the same seeding as `MonotonicityAnalyzer`'s `viewColumns`.
3. **The program-parallel path refuses temporal filters.** `CollectParallelBlockers` admits
   `TemporalFilterPlan` only with `allowTemporalFilter: true`, and only the single-query compile
   passes that. The reason: `TemporalFilterOp` state (`_accum`, the integrated input rows) has no
   `ReshardSpec`. Its rows must be placed where the operator's *input* stream is partitioned (look
   at `ctx.GetPartition(input)` / `PartitionInfo`).
   - The danger: a downstream operator whose exchange the compiler skipped, because it trusted the
     partition. A wrong placement there is silently wrong.
   - If the input partition is not a reproducible hash (shard-disjoint after a projection, for
     example), record an `Unsupported` spec. The reshard then refuses rather than guesses.

**Read first.**
- Feldera:
  - how its SQL compiler and pipeline runtime drive `NOW()`: a `now` input stream fed by the
    controller every step (look for `now` in `crates/adapters` and in the SQL compiler's
    `DBSPNowOperator`);
  - whether that clock is wall time, and how it behaves on replay and checkpoint.

  Policy (memory `adopt-feldera-assumptions-by-default`): match it unless there is a measured
  reason not to.
- Ours: `RootCircuit.AdvanceTime` / `LogicalClock`, `ParallelCircuit.AdvanceTime`,
  `TemporalFilterOp`, `docs/now-and-temporal-filters.md`, `LogicalClockSnapshotTests` (is the
  clock persisted, and on the program path?), `ProgramRunner` and the parallel runner, and
  `DbspNetEngine`.

**Questions the design must answer before code.**
1. **Who advances the program clock, and to what?**
   - Options: wall time sampled by the runner once per step; a value from the connector or batch;
     an explicit API.
   - How tests stay deterministic: an injectable clock.
   - What replay after restore does: is the clock persisted, and can it move backward?
2. **The serial GC frontier.** Register clock frontiers per view, and seed them across view
   boundaries the way LATENESS sources are.
3. **The parallel reshard placement** for `TemporalFilterOp`. Which `PartitionInfo` cases map to
   `ReshardPlacement.Hash`, which to `WholeRow`, and which must be `Unsupported`.
4. **Scope.** Fixing the clock, serial GC and parallel are separable. Commit them in that order.

**Acceptance.**
- A program `NOW()` view emits retractions as the clock advances. A test fails before the fix.
- A serial program matches the same query compiled with `PlanToCircuit.Compile` driven by the same
  clock, and matches the batch oracle at the final clock (extend `TemporalFilterPbtTests` with a
  program arm).
- Downstream GC fires with bounded state (like `ProgramLatenessTests`).
- Parallel program equals serial at W ∈ {2, 3, 4}, flat and spine, including a temporal filter
  over an aggregate's output (the elided-exchange case).
- Restore at the same W and reshard to a different W, continued and compared against an
  uninterrupted serial run.
- A mutant with a wrong placement fails a reshard test.
- Remove the program-path blocker, and update:
  - `README.md` (Not supported);
  - `ARCHITECTURE.md` (Temporal filters);
  - `docs/design-parallel-frontier.md` §5 "Left open";
  - issue #3;
  - memory `program-lateness`.

**Landmines from the last arc.**
- `StreamRelease.ScanReaders` walks operator fields. Any new object shared across replicas that a
  closure can reach must be marked stream-free in `StreamRelease.Compute`, or one replica releases
  another's streams. This showed up only under concurrent tests.
- `Sql.dll` incremental builds are flaky. Do a clean rebuild (`--no-incremental`) before the full
  suite and before mutation checks.
- A mutant restored from a backup keeps an old mtime, so `touch` the file.

**Working style.** Design section first, as a new `docs/design-program-temporal.md` or a section in
`now-and-temporal-filters.md`. Then build in small commits on main. Run the full suite after a clean
rebuild before each commit. .NET 10 is in `~/.dotnet`. Hand Curt any Docker commands.
