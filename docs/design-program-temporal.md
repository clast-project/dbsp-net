# Design: temporal filters (`NOW()`) on the program path

**Status: BUILT, 2026-11-15** (§5 has the results). Kickoff: `docs/next-arc-program-temporal-prompt.md`. Companions:
`docs/now-and-temporal-filters.md` (the single-query feature), `docs/design-parallel-frontier.md`
(LATENESS on the parallel paths, §5 "Left open"), `docs/design-durable-state.md` §1c (journal) and §1d
(reshard).

## 0. What is wrong today (verified 2026-11-15)

1. **Nothing advances a program's clock.** `CompiledProgram` and `ParallelCompiledProgram` have no
   clock API, and neither `ProgramRunner` nor `DbspNetEngine` calls `AdvanceTime`. So in a deployed
   program `NOW()` stays at logical −∞, and `ts <= NOW()` admits nothing, ever.
   `ProgramRunnerClockTests.DefaultRunner_NowView_SeesTheWallClock` fails on `main` (expected 1 row,
   got 0).
   - The runner also steps only when a source has a new version. So even with a clock, a view could
     not retract on time alone.
2. **No GC frontier on the serial program path.** `CompileProgram` never calls
   `RegisterTemporalFrontiers`. A `GROUP BY ts` above `ts > NOW() - INTERVAL …` keeps every group it
   has ever seen. The result is correct, but state is unbounded.
3. **A pre-existing soundness bug in the frontier's identity (single-query path too).**
   `RegisterTemporalFrontiers` keys the clock frontier by the base column `(table, col)`, and the
   first filter registered wins.
   - Example: `ts > NOW() - 1s` and `ts > NOW() - 5s` in two branches of a UNION ALL, then a
     GROUP BY. The aggregate GCs against `clock − 1s`. When the 5 s branch later retracts, the
     retraction meets an evicted group.
   - `TemporalFrontierCollisionProbe.SingleQuery_UnionOfTwoWindows_GroupBy` gets two phantom rows
     where it expects none.
   - On the program path this is the common case: two views over one table with different windows.
     Item 2 cannot be fixed with the existing key without hitting it.
4. **The program-parallel path refuses temporal filters.** `TemporalFilterOp`'s state has no
   `ReshardSpec`.

Persistence is already right on every path:
- the snapshot manifest carries `logical_time` (schema v3);
- every parallel worker writes its own manifest, and `LoadDirAsync` restores the clock before the
  operators load;
- `ParallelReshard` copies worker 0's manifest to every new worker.

The gap is the driver and the tick journal: journaled ticks replay without their clock.

## 1. What Feldera does (read 2026-11-15, `../feldera`)

- **The clock source.**
  - `NOW()` is a system table `NOW(now TIMESTAMP)`, fed by a built-in input connector
    (`crates/adapters/src/transport/clock.rs`) that the controller attaches on host 0.
  - Every step inserts exactly one record: `SystemTime::now()` rounded **down** to
    `clock_resolution_usecs` (default **1 s**, `feldera-types/src/config.rs:58`).
- **Monotone by MAX, not by check.**
  - The connector never checks for backward wall time.
  - The compiler defines `NOW()` as `chain_aggregate(MAX)` over the append-only table, and temporal
    filters read a waterline (a LUB fold).
  - So a backward wall-clock step *holds* `NOW()` at the previous maximum. It does not error.
- **Clock-only steps.**
  - The connector requests a step every resolution period when nothing else has (`clock.rs:8-13`,
    `:277-281`).
  - So a `NOW()` view retracts without any input.
- **Replay.**
  - Each emitted value is written to the replay journal (`Resume::Replay { replay: now }`), and
    replay feeds the logged value back exactly.
  - After replay, wall-clock mode jumps to the current wall time.
  - The MAX/waterline state is checkpointed, so `NOW()` cannot go backward across a restart.
- **Deterministic mode.** `dev_tweaks.now_http_driven` holds the clock and advances it only on
  `POST /clock/advance`. This is the injectable clock.
- **Workers.**
  - The record enters once.
  - The waterline exchanges each worker's fold to all workers (`ExchangeActivity::AllSteps`), so
    every worker sees the same scalar.

## 2. Answers

### Q1 Who advances the program clock, and to what: the runner, from wall time, once per step

Adopt Feldera's model (policy `adopt-feldera-assumptions-by-default`).

- **Library API.**
  - `CompiledProgram` and `ParallelCompiledProgram` gain:
    - `AdvanceClock(long µs)`, which is the existing `RootCircuit`/`ParallelCircuit.AdvanceTime`;
      it stays strict and throws on a backward move;
    - `LogicalTime`;
    - `UsesClock`, true when the compiled circuit contains a `TemporalFilterOp`.
  - A library caller drives the clock explicitly, exactly as with `CompiledQuery.AdvanceClock`.
- **Runner.** `ProgramRunner` takes an optional `ProgramClock`:
  - a `TimeProvider`, defaulting to `TimeProvider.System`;
  - a resolution, defaulting to 1 s as in Feldera.

  When the program `UsesClock`, before every step the runner samples
  `floor(now / resolution) × resolution`. It then advances the clock to
  `max(LogicalTime, sample)`. Holding rather than throwing is Feldera's MAX semantics, applied on the
  driver.

  A program with no `NOW()` is never touched: same ticks, same journal, byte-identical circuit.
- **Clock-only tick.** The runner is batch-shaped (drain, then write). After the drain loop it
  samples again. If the sample is past `LogicalTime`, it takes **one clock-only tick**, with no
  source read. So every batch's output reflects `NOW()` as of the end of that batch, retractions
  included. This replaces Feldera's timer: our runner only runs inside a batch, and one tick per
  batch is the batch analogue of one tick per resolution period.
- **Journal.** `JournalTick` gains an optional `Clock` (`long?`, JSON `clock`) and a nullable
  `Source` (null for a clock-only tick).
  - Replay advances the circuit to the journaled clock before stepping, and a clock-only tick reads
    no source.
  - Replay is therefore exact. A journal written before this change has no `clock`, and replay then
    leaves the clock alone, which is what the original run did.
- **Restore.**
  - The snapshot restores the clock, and the journal replays its own values.
  - After that, the next sample is held at `max`, so wall time going backward across a restart
    holds the clock rather than throwing or rewinding. The clock cannot move backward.
- **Deterministic tests.** A test passes a `TimeProvider` it controls. `TimeProvider` is part of the
  BCL, so no new dependency is needed.
- **Server.**
  - `DbspNetEngine` uses the default clock.
  - `DBSPNET_CLOCK_RESOLUTION_USECS` sets the resolution, mirroring Feldera's
    `clock_resolution_usecs`.

**Rejected alternatives.**
- A clock taken from the connector or batch: there is no event-time column a runner could trust,
  and Feldera does not do it either.
- An explicit-only API with no runner default: the server could then never move time, which is the
  bug.

### Q2 The serial GC frontier: register per view, with the window as the frontier's identity

The clock frontier's *value* is `transform(clock) − offset`. It depends only on:
- the clock unit (`Timestamp` or `Date`);
- the offset;
- whether the key was `CAST(ts AS DATE)`.

It does not depend on the table. So its identity in the `frontiers` dictionary becomes exactly that
triple: a synthetic `LatenessSource` (`MonotonicityAnalyzer.TemporalSource`). Two filters with the
same window share one frontier, and two with different windows get two.

The analyzer marks the filter's output column with that source:
- A UNION ALL of the two windows unions the sources.
- `Resolve*Frontier` takes the `MinFrontier`, which is sound.
- This fixes §0.3 on the single-query path too.
- A synthetic source can never collide with a declared LATENESS column.

Then the program path:
- **Registration.** `CompileProgram` registers each reachable view's temporal frontiers into the
  program-wide dictionary before compiling it. That includes single-use views that are inlined,
  because their bodies compile under the consumer. The key is semantic, so registration order does
  not matter.
- **View boundaries.** Nothing new is needed. A downstream view's `MonotonicityAnalyzer` is seeded
  with the upstream view's `RootColumns`, which already carry the synthetic source, exactly as
  LATENESS sources cross today. A temporal filter over a *view* scan (`tf.Input` is a `ScanPlan` of
  a view) is handled by the same arm, because the frontier does not care what is underneath.
- **The single-query path's LATENESS-collision rule goes away.** It used to skip registration when
  the column already had LATENESS. Now the filter's output column carries only the clock source,
  which is sound on its own: every row the filter will ever emit has a key ≥ `clock − offset`.

### Q3 The parallel reshard placement: the input's partition

`TemporalFilterOp` integrates its input into `_accum`. Its output on worker w is a subset of the rows
that arrived on w.

- `ComputeValid` skips rows with weight ≤ 0. So a row's +1 and its later −1 must meet on one worker.
  `Anywhere` (linear) placement is therefore **not** sound, even though the filter is linear.
- The persisted `_accum` (`trace.arrows`) must go where the input stream will deliver that row's
  future deltas.

The filter's output also inherits its input's partition. This is new: today it records none, which is
safe, but it forces a re-exchange downstream. A downstream aggregate on the same key can now elide
its exchange. Once it does, a wrong placement is silently wrong, so the placement mapping is the
load-bearing part.

| input partition | placement | why |
|---|---|---|
| `Gathered` | `OnWorker0` | everything is on worker 0 |
| `PartitionKey` + `KeySchema` (an exchange, a join, or an aggregate's output, including an elided one) | `Hash(OfRow(row, PartitionKey, KeySchema))` | the same function the upstream exchange used, recomputed from the row, which carries the key columns at the same positions |
| a base-table input stream, or a filters-only chain over one | `WholeRow(tableSchema)` | `ShardedInput` places by `OfWholeRow(row, tableSchema)`, and filters keep rows identical |
| anything else (after a projection, a union, or a broadcast-join output) | **`Unsupported`** | the placement is not a reproducible hash, so the reshard refuses rather than guesses |

Same-W restore works in every case. Only a reshard needs the placement.

- The base-table case needs to know the stream is whole-row-hashed. `PartitionInfo` cannot say so
  (`ShardDisjoint: true, PartitionKey: null` is also what a projection of a scan can carry). So the
  compile context keeps a reference-keyed `WholeRowStreams` map (stream → schema), populated for
  the table inputs of both parallel compiles and propagated through filters-only linear chains.
- A new field in `PartitionInfo` would have been copied by the `with { … }` sites (the broadcast-left
  join output) onto rows that are not table rows.
- An alternative to `Unsupported` was rejected for now: insert a whole-row exchange in front of the
  filter whenever the input partition is not reproducible. That makes every shape reshardable, at the
  cost of an exchange the common case (a filter directly over a table) never needs.

**Landmine check (`StreamRelease`).** The new objects are the per-replica frontiers and the
reshard placement closures. The placement closures are recorded on replica 0 only, held by the
program and not reachable from an operator. No object crosses replicas, so nothing new needs marking
stream-free.

### Q4 Scope and order (separate commits)

1. **The clock.** `AdvanceClock`/`LogicalTime`/`UsesClock` on both program kinds; `ProgramClock`
   in the runner (per-step sample, hold, clock-only tick, journal `clock`); the server's resolution
   knob.
   Test: the runner test that fails on `main`; retraction on a clock-only tick; journal replay that
   reproduces the run; restore with wall time behind the snapshot.
2. **Serial GC and the frontier identity.** The synthetic source, and registration in
   `CompileProgram`.
   Tests: the collision probes become tests; the program GC is bounded; the program arm of
   `TemporalFilterPbtTests`, comparing serial program, single query and batch.
3. **Parallel.** Partition passthrough, the placement table, removal of the program blocker.
   Tests:
   - parallel equals serial at W ∈ {2, 3, 4}, flat and spine, including a filter over an
     aggregate view;
   - same-W restore and reshard, continued and compared with an uninterrupted serial run;
   - a placement mutant must fail.

## 3. Acceptance

See the kickoff prompt; it is reproduced in §2 Q4's tests.

## 4. Known, out of scope

- ~~`TemporalFilterOp` drops a row from `_accum` only once its weight is positive and expired. A
  delete that arrives after the row expired leaves a −1 entry that is never GC'd.~~ Fixed
  2026-11-15: expiry, and a NULL time key (never valid, also kept forever before), are checked
  before the weight.
- The per-tick recompute is O(|integral|) (`now-and-temporal-filters.md`, deferred).
- Typed paths: a temporal filter forces structural, as before.

## 5. Results (2026-11-15)

**Built**, in three commits on `main`:

| commit | what |
|---|---|
| `2964c84` | §2 Q1: the clock |
| `b04bd68` | §2 Q2: the window identity and serial GC |
| `af748f5` | §2 Q3: parallel |

**Correctness.**
- **The clock.** `ProgramRunnerClockTests` covers:
  - the default-runner test that failed on `main`;
  - a clock-only tick retracting aged-out rows, and no tick when time did not move;
  - the resolution rounding down;
  - a program without `NOW()` never clocked;
  - backward time held;
  - the parallel runner clocking every replica;
  - journal replay at a much later wall time landing on the journaled clock, tick and view, both
    journal-only and snapshot + journal;
  - a restore on a host whose clock is behind the snapshot, which holds the clock.
- **GC.** `ProgramTemporalFilterTests` covers:
  - the collision fix, on the single-query path and across two program views;
  - bounded GC in the filtering view, across a view boundary (output and inlined), and above a
    filter over an aggregate view;
  - serial program equals single query, tick by tick.
- **PBT.** `TemporalFilterPbtTests` has two new arms:
  - program vs single query, tick by tick, and vs the batch oracle at the final clock, over seven
    shapes. These include the two-window union, a filter over an aggregate view, DISTINCT, a self
    join, an appear-only filter, and the one-view nested-aggregate shape. Intermediate views are
    randomly outputs or inlined.
  - parallel program vs **serial** program at W ∈ {2, 3, 4}, flat and spine, with deletes.
- **Checkpoint.** `ParallelTemporalCheckpointTests` covers:
  - same-W restore (flat and spine, W = 2, 3, 4) and reshard 2→3, 3→2, 4→2, 2→4, each continued
    for 8 ticks with deletes against an uninterrupted serial run;
  - the stated placements;
  - exchange elision above a filter over an aggregate;
  - `Unsupported` after a projection: the reshard refuses, while same-W restore works.
- **Full suite** after a clean rebuild: 3875 pass, 1 skipped, run twice.

**Mutation checks** (each reverted after the run):

| mutant | fails |
|---|---|
| window identity without the offset (all windows on a column share one) | the PBT program arm; both collision tests |
| keyed placement → `Anywhere` | all 4 reshard runs (rows diverge after the reshard); the placement test |
| no partition passthrough | the elision test |

**A correction to §2 Q3, found while building.** The program-parallel compile builds a fresh
`CompileContext` per view, so partition info does **not** cross a view boundary: a view-scan arrives
unpartitioned, which is conservative. The table in Q3 therefore applies within a view. A filter
over a *view* scan records `Unsupported`:
- the filter itself is correct;
- same-W restore works;
- a reshard refuses.

The elided-exchange case only arises inside one view (the nested shape). Carrying partitions across
views would change existing parallel circuits, ivm-bench's included, so it is left open.

**No regression without `NOW()`.**
- A program without a temporal filter builds the same circuit as before: no frontier is
  registered and no partition is recorded on a filter.
- The runner checks a cached `UsesClock` and never samples the clock.
- The journal format is unchanged, because `clock` is omitted when null.
- No benchmark was re-run, because nothing on those paths changed.

**Left open.**
- Cross-view partition passthrough on the parallel program (above).
- Alternatively, a whole-row exchange in front of a filter whose input is not reproducibly placed,
  which would make every shape reshardable (§2 Q3).
- ~~The crash matrix does not carry a `NOW()` view~~ and ~~the `_accum` leak (§4)~~: both done
  2026-11-15. `ParallelCrashSafeCheckpointTests` has a `NOW()` window whose phase C retracts rows
  valid at either commit point. A mutant whose `LoadAsync` drops the restored rows fails all four
  crash cases.
