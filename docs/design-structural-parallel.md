# Design: structural-parallel exchange insertion for `PlanToCircuit` (batch-1 competitiveness)

**Status: BUILT + MEASURED. 2026-07-21.** (§0–§8 are the original scoping/plan; §9 records what was
built and what the measurements said — read it for the outcome.)
Companion to `docs/design-columnar-batch1.md` §9 (the reframe) and `docs/design-row-representation.md`
(the exchange/parallel arc, §15). Same discipline: design-first, measure-first, benchmark-gated,
retire-if-it-loses.

## 0. Why this, why now

The Feldera SF=3 batch-1 scaling curve was measured 2026-07-21 (faithful OAT harness, this i9-12900K
box, 8P+8E):

| Feldera workers | duration_s | vs 1-core | Eff |
|--:|--:|--:|--:|
| 1 | 56.07 | 1.00× | 100% |
| 2 | 34.36 | 1.63× | 82% |
| 4 | 23.93 | 2.34× | 59% |
| **8** | **18.56** | **3.02× (peak)** | 38% |
| 12 | 20.47 | 2.74× | 23% (regresses) |

Two facts reframe the whole batch-1 arc:
1. **Feldera single-core (56.07s) ≈ dbsp-net serial (~59.5s local ServerGC).** Batch-1 per-row
   representation edge is only ~1.06× — the algorithmic wins already closed the per-row gap the
   columnar arc was chasing. **Columnar buys ~nothing on batch-1; serial is already ~parity.**
2. **Feldera's whole batch-1 advantage over us (20.5s vs ~60s, ~2.9:1) is parallelism**, and it
   saturates at ~3× / knee W=8 (negative at W=12 — the synchronous-BSP straggler wall, §15). We are
   W-insensitive (§15.8), so we would not take that oversubscription hit.

**Therefore batch-1 competitiveness is a parallelism-implementation problem.** `PlanToCircuit` (the
structural program compiler the ivm-bench server uses) inserts **zero exchanges** today — it builds one
single-threaded circuit. Porting the typed compiler's exchange-insertion strategy to the structural
path, at a realized 2.5–3×, takes ~59.5s → **~20–24s = 77–92% of Feldera's peak, up to parity vs its
configured run** — inside the 50–80%+ "competitive" goal, plausibly parity, with **no rep rewrite**.

## 1. Premise confirmed by code (2026-07-21 map)

- **The runtime substrate is already generic over `StructuralRow` — NO new operators.**
  `ExchangeOp<TKey,TWeight>` (`Core/Circuit/Operators/ExchangeOp.cs:21`), `ExchangeIndexOp<TKey,TRow,TWeight>`
  (`ExchangeIndexOp.cs:25`), `ExchangeIndexJoinOp` (`ExchangeIndexJoinOp.cs:34`), and the
  `CircuitBuilder.Exchange`/`ExchangeIndex`/`ExchangeIndexJoin` wrappers
  (`Core/Circuit/CircuitBuilder.cs:184/221/260`) are all `<TKey,TRow>`-generic and directly
  `<StructuralRow>`-instantiable. `ParallelCircuit.Build`/`ShardedInput`/`ShardedOutput`
  (`Core/Circuit/ParallelCircuit.cs:114/356/375`) drive the whole thing.
- **Key elision is free on the serial path.** `CircuitBuilder.ExchangeIndex` at `Workers<=1` degrades to
  exactly `GroupProject(input, keyOf, row=>row)` (`CircuitBuilder.cs:233-236`) — **byte-identical to what
  structural `CompileInnerJoin`/`CompileAggregate` already emit**. So the parallel path is a superset;
  W=1 must reproduce today's circuit bit-for-bit (a built-in regression guard).
- **The typed path already does all of this** (`TypedPlanCompiler.TryCompileParallel`,
  `TypedPlanCompiler.cs:107`) — but only for typed struct rows, paying the §23 typing penalty. The
  structural port shuffles `StructuralRow` *refs* → avoids that penalty entirely.

## 2. The algorithm to port (from `TypedPlanCompiler`)

Thread a partition-key annotation through the compile and insert an exchange only when the data is not
already co-partitioned on the operator's key.

- **Partition state.** The typed compiler threads `TypedNode(… bool ShardDisjoint, int[]? PartitionKey)`
  (`TypedPlanCompiler.cs:216`). `PartitionKey` = the column indices the stream is currently hash-partitioned
  by. The structural compiler passes a bare `Stream<ZSet<StructuralRow,Z64>>` with **no partition
  metadata** → the port must add the equivalent (a small struct or a side-table keyed on the compiled
  stream carrying `int[]? PartitionKey` + `bool ShardDisjoint`).
- **Elision test.** `IsKeySubset(dataKey, opKey)` (`:223`): if the current partition key ⊆ the operator's
  key, no shuffle. Reuse verbatim.
- **Per-node decisions (typed → structural):**
  - **Scan:** sharded by whole-row hash, `ShardDisjoint=true`, `PartitionKey=null` (`:527-540`).
  - **Join:** shuffle both sides by equi-key indices, fused via `ExchangeIndexJoin` (single barrier) or two
    `ExchangeIndex`; output `PartitionKey = leftIndices` (`:827/834/930`).
  - **Aggregate:** `needsExchange = Workers>1 && !(inner.PartitionKey ⊆ groupIndices)`; else plain
    `GroupProject`; output `PartitionKey = iota(keyCount)` (`:1735-1797`).
  - **Distinct:** shuffle by whole row when `Workers>1` (`:1227-1231`).
  - **Partitioned window / TopK:** shuffle by the PARTITION BY column list (`:1296/1373`).
  - Also: SemiJoin, scalar-subquery joins (extra `GroupProject` sites).

## 3. Concrete insertion points in `PlanToCircuit.cs`

All line numbers per the 2026-07-21 map (`src/DbspNet.Sql/Compiler/PlanToCircuit.cs`).

| Site | Today | Parallel change |
|---|---|---|
| Single-query build | `RootCircuit.Build(…)` **:105** | branch to `ParallelCircuit.Build(workers,…)` |
| Program build | `RootCircuit.Build(…)` **:286** | same; sharded inputs replace **:298**, sharded/gathered output replaces `Integrate` **:370** |
| Table input | `builder.ZSetInput<StructuralRow,Z64>()` **:112** | `ShardedInput` when parallel |
| Inner join (both sides) | `builder.GroupProject(…)` **:1820 / :1870** | `builder.ExchangeIndexJoin` (or 2× `ExchangeIndex`) keyed on `ExtractEquiKeyIndices` (**:2905**) |
| Aggregate | `builder.GroupProject(…)` **:3017** | `builder.ExchangeIndex` on group indices, with `IsKeySubset` elision |
| Distinct | before `EmitDistinct` (helper **:548**) | `builder.Exchange` by whole-row hash |
| Partitioned window/TopK | dispatched **:1210-1223** | `builder.Exchange` by PARTITION BY indices |

Key columns are **already available** at each site as `int[]` indices: joins via
`ExtractEquiKeyIndices`/`ExtractKey` (**:2905/:2942**), aggregates via `AggregatePlan.GroupKeys` (bare
`ResolvedColumn.Index`), distinct = whole row. So building the partition delegate needs no new plan
analysis — only the hash (below).

## 4. The one genuinely new piece: a `StructuralRow`-slot partition hash

`StablePartitionHash` (`src/DbspNet.Sql/Compiler/StablePartitionHash.cs`) has one overload per **typed**
CLR column type. `StructuralRow[i]` returns a **boxed `object?`** (`StructuralRow.cs:64`). Need a
structural variant:

```
int PartitionOf(StructuralRow row, int[] keyIndices, SchemaColumn[] keyCols)
  // fold StableHash.Combine over StablePartitionHash.OfBoxed(row[keyIndices[k]], keyCols[k].Type)
```

`OfBoxed(object?, SqlType)` dispatches on the column's `SqlType` to the **same underlying `StableHash`
reductions** the typed overloads use — so a value hashes to the same worker whether it arrives typed or
structural (required for snapshot/restore co-location and for typed↔structural A/B parity). Null → the
existing `NullHash` sentinel. This is the only new code; everything else is reuse.

## 5. Build increments (gate-first, retire-if-loses)

1. **Increment 0 — partition-hash + threading, W=1 byte-identical.** Add `OfBoxed`, the node partition-key
   threading, and the `ParallelCircuit.Build` branch, but keep `workers=1`. **Gate: `IvmBatchProfile` 16
   outputs byte-identical + full suite green + circuit structurally identical to today (W≤1 exchange
   degradation proves this by construction).** No perf claim — this is the safe scaffold.
2. **Increment 1 — one hot join+agg subgraph parallel.** Enable exchanges on the highest-volume fact
   path (e.g. `fact_holdings`/`fact_watches` joins + `watches` aggregate). **Gate: byte-identical outputs
   at W=2/4/8 (correctness) + that subgraph's wall scales positively (measure via the operator profiler,
   `DBSPNET_PROFILE=1`).** Retire if it doesn't scale.
3. **Increment 2 — whole batch-1 program parallel, W-swept.** Turn on program-wide exchange insertion;
   sweep W=1/2/4/8/12 on `IvmBatchProfile` (ServerGC). **Gate: byte-identical + realized scaling ≥ ~2×
   (→ ≤30s → ≥62% of Feldera).** This is where the **open number** lands (see §6).

## 6. Risks & open numbers (the honest part)

- **Real-DAG scaling factor is THE unknown.** The 2.5–3× projection is from `ParallelScalingProbe` — a
  single best-case disjoint-shard join+proj (3.07× @ W=8). The real batch-1 DAG has: SCD-2 temporal
  joins (skew on hot keys), wide-row window aggregates (partition-key shuffle of fat rows), an all-view
  program with many exchanges (barrier coordination tax, §15's 40% wait), and gather-side re-materialize.
  Realized could be 2–2.5× → ~24–30s → 62–77%. Increment 2 measures it before any parity claim.
- **Coordination tax is real and rises with W (§15).** Our own arc found ops scale 7–9× but realized step
  only 3.5–5×, barrier WAIT → 40% at high W. Batch-1's coarse ticks (bulk load, big batches) should HELP
  (lower relative variance than Nexmark's fine ticks) — but this is a hypothesis to test, not a given.
- **Skew on SCD-2 keys.** Hash-partitioning by join/group key can land a hot symbol/account on one worker.
  The co-location invariant blocks work-stealing (§15). If a single fact key dominates, scaling caps below
  the probe. Measure Imbal via the step profiler on the real DAG.
- **W-sizing.** Feldera peaks at W=8 and regresses at W=12 on this box; we are W-insensitive but should
  still default W to the P-core count, not oversubscribe. `ParallelCircuit` already spawns no thread at
  W=1.
- **Output gather / re-materialize.** ivm-bench truncate-output re-materializes full state per batch;
  `ShardedOutput` sums Z-sets across workers. Confirm the gather cost doesn't eat the parallel win on the
  output-heavy views (it's ~output I/O, which Feldera pays too).

## 7. Correctness strategy

- **Oracle:** mirror `tests/DbspNet.Tests/Sql/ParallelTypedCompilerTests.cs` (+ `TumbleParallelTests`,
  `WindowAggregateParallelTests`) for the structural path: structural-serial ≡ structural-parallel(W) ≡
  batch, across inserts/deletes/group-growth/retractions, W=1/2/4/8.
- **End-to-end gate:** `IvmBatchProfile` 16 output row-counts + value-diff byte-identical serial vs parallel.
- **W=1 identity:** the `Workers<=1` exchange→`GroupProject` degradation makes W=1 structurally identical to
  today's circuit — a free regression guard.
- **Assembly patterns to copy:** `tests/DbspNet.Tests/Circuit/{ExchangeOpTests,ParallelCircuitTests,
  ShardedIoTests}.cs`; the structural-parallel thesis probe `Scratch/ParallelScalingProbe.cs`.

## 8. Bottom line

The port is compiler-only and additive: (a) thread `PartitionKey int[]?`/`ShardDisjoint` through the
structural node compile; (b) branch the two `RootCircuit.Build` sites to `ParallelCircuit.Build` with
`ShardedInput`/`ShardedOutput`; (c) swap the four `GroupProject` shuffle points for
`Exchange`/`ExchangeIndex`/`ExchangeIndexJoin` keyed on already-available column indices, with `IsKeySubset`
elision; (d) write the one new `StructuralRow`-slot stable partition hash. No new runtime operators. The
measured Feldera curve says this is the lever that makes batch-1 competitive; the open number Increment 2
must land is the **real-DAG realized scaling factor**.

## 9. Built + measured (session outcome, 2026-07-21)

All of the below is **additive and opt-in** (off by default), **byte-identical at W=1** by the exchange
degradation, and lands with the full suite green. Correctness oracle: `ParallelStructuralCompilerTests`
(structural-serial ≡ structural-parallel at W=1/2/4/8 across join / aggregate / distinct / partitioned
window / top-K / union, plus fused and broadcast variants) + `CardinalityEstimatorTests`.

### 9.1 Increment 0 — shipped, gated
Built exactly as §8: `StablePartitionHash.OfBoxed/OfRow/OfWholeRow` (the one new structural row-slot
hash), partition-state threading + `IsKeySubset` elision + a shardability guard (`CanCompileParallel`) in
`PlanToCircuit`, the `GroupProject → Exchange*` swaps, `PlanToCircuit.TryCompileParallel` +
`ParallelStructuralCompiledQuery`, and the program analogue `TryCompileProgramParallel` +
`ParallelCompiledProgram` (driver-side integrate-per-view gather). The swaps degrade to the identical
`GroupProject` at W≤1, so the serial path is unchanged by construction.

### 9.2 Increment 2 — whole-program parallel is BLOCKED (the honest finding)
The batch-1 program is one circuit at one worker count, so a single un-shardable view forces the whole
program serial. Probing the real SF=3 deploy spec: **11 of 50 views block it** — 6 LEFT joins (pure
equi, tractable to add), 4 semi-joins (tractable), and **5 global-RANK leaderboards**
(`RANK() OVER (ORDER BY … DESC)`, no PARTITION BY — inherently sequential, the hard wall). So whole-program
parallel is not reachable on this substrate without mixed serial/parallel regions, independent of join
coverage. (`IvmBatchParallelProbe` reports the live blocker list.)

### 9.3 Increment 1 — the open number, measured (real SF=3, ServerGC, i9-12900K)
Per-worker `StepProfiler` decomposition (split=movement, wait=coordination, gather=movement, residual=op)
of two shardable hot fact subgraphs (`trades_history`, `holdings_history`) resolved the scaling wall:

- **The wall is barrier WAIT (46–56% at W=8) driven by load IMBALANCE (1.7–2.1×), NOT data movement
  (7–20%).** This **corrects §6 / the prior arc's "memory-bandwidth bound" conclusion** — that came from
  `ParallelScalingProbe`'s disjoint-shard best case (no skew, no barriers). The real DAG is
  coordination/skew-bound, which is *software-addressable*, not the hard bandwidth wall.
- Skew source: low-cardinality dimension joins (a 5–14-value reference key hash-sharded across 8 workers
  is inherently lopsided).

### 9.4 The levers, and the result
- **Join-exchange fusion** (`ExchangeIndexJoin`, single barrier vs two; `CompileOptions.CoalesceJoinExchange`).
  Correct, modest: W=8 wait 56→51%, ~1.58→1.75× on `trades_history`. Barriers matter, skew dominates.
- **Broadcast join for small dimensions** — the decisive lever. New all-gather `BroadcastExchangeOp` +
  `CircuitBuilder.BroadcastExchange`; a join whose right side is a small dimension keeps the fact on its
  balanced partition and replicates the dimension to every worker (each worker joins locally). Eliminates
  the wall: imbalance → 1.0×, wait → ~2%, op ~90%.
- **Production-ready, size-gated broadcast**: `CompileOptions.BroadcastMaxRows` + `RelationRowCounts`, with
  a `CardinalityEstimator` deriving each dimension view's size from deployment-supplied base-table counts
  (unknown size ⇒ hash join, never a blind broadcast). Connector helper `DeltaRowCounts` reads the counts
  from Delta transaction-log `numRecords` (no scan; returns null on incomplete stats). **The size gate is
  both safer and faster than broadcast-all** (it broadcasts only the tiny refs, hash-joins the large facts).

Realized scaling with the production size gate (byte-identical at every W, balanced, wait-free):

| subgraph | W=1 | W=2 | W=4 | W=8 |
|---|--:|--:|--:|--:|
| `trades_history`   | 1.00× | 1.50× | 1.97× | **2.49×** |
| `holdings_history` | 1.00× | 1.20× | 1.88× | **2.13×** |

**Both hot fact paths clear the ≥2× Increment-2 gate.** The residual ceiling past W≈4–8 (op ~90%, balanced)
is genuinely compute / memory-bandwidth — the representation arc, the hard floor.

### 9.5 What remains to make batch-1 benefit end-to-end
Deploy is compile-once (`DbspNet.Server/DbspNetEngine.DeployAsync`, the `SqlProgram.Compile` site);
batch-1/2/3 are `Resume`+`Wait` cycles on the same compiled program (parallelism is a build-time property,
not per-batch — and it pays on batch-1's bulk load, not the tiny 2/3 increments). Wiring parallel there is
a small change, but it falls back to serial for batch-1 today because (a) whole-program parallel is blocked
(§9.2) and (b) `ProgramRunner` drives only the serial `CompiledProgram`. To land it: a parallel
`ProgramRunner`, LEFT/semi-join coverage, and a story for the 5 global-rank views (gather-on-one-worker /
mixed regions) or accept they cap the single-circuit approach.

## 10. Per-batch state persistence (2026-07-21)

§9.5 left the program path **stateless across batches**: `DeployAsync` compiled with
`snapshotCodecs: null` and `ProgramRunner` had no checkpoint hook at all, so a batch's engine state
lived only in memory. Two consequences, one honest-measurement and one durability:

- **The batch number was not the whole batch.** Feldera's per-batch cost includes making state
  durable; ours measured step + output write only. Comparing them is apples-to-oranges unless we can
  at least *price* the checkpoint.
- **Nothing survived a restart.** Batch 2/3 could only follow batch 1 inside one process lifetime.

### 10.1 What was built

**Serial program path (what the ivm-bench server drives).**
- `ProgramSpec.SnapshotDir` (falling back to `DBSPNET_SNAPSHOT_DIR`) turns persistence on.
  `DbspNetEngine.DeployAsync` then compiles with `ArrowSqlSnapshotCodecs.Instance` and wires a
  `SnapshotCheckpointStore` over a `LocalTableFileSystem`. Unset ⇒ codec-free compile ⇒ the engine is
  bit-for-bit what it was before the option existed. Off by default, deliberately: the head-to-head
  against Feldera should only pay for checkpointing if Feldera's configured run does too.
- `ProgramRunner` gained the hook it was missing, mirroring `PipelineRunner`: an optional
  `ICheckpointStore`, `RestoreAsync`, `CheckpointAsync`, and an automatic checkpoint **at the end of
  each `RunBatchAsync`** — the natural commit point for a runner whose contract is "drain everything,
  then write every output once". Offsets ride in the snapshot manifest, so engine tick T and the
  source cursors commit atomically (the exactly-once alignment invariant).
- The checkpoint is **inside** the batch's measured span on purpose. A batch is done when its state
  is durable; the honest per-batch number is step + output write + checkpoint. `DBSPNET_PROFILE=1`
  now reports a `checkpoint save` phase alongside the other four.
- Restore runs in `DeployAsync` — outside the measured batch, like the compile. `DeployResult` reports
  `Persistent` and `RestoredTick`.

**Parallel program path.** `PlanToCircuit.TryCompileProgramParallel` hardcoded `snapshotCodecs: null`
(the §9.1 shortcut); it and `SqlProgram.TryCompileParallel` now thread an `ISqlSnapshotCodecs`
through to the per-view `CompileContext`. The build closure runs once per replica, so each worker's
operators register their own codecs and the program checkpoints through `ParallelSnapshot.WriteAsync`
as W disjoint `worker-{w}/` shards, written concurrently. A dimension the broadcast size gate
replicates is held — and therefore persisted — once per worker; the gate only broadcasts relations
under `BroadcastMaxRows`, so the W× duplication is bounded and small.

**Coverage limit, stated plainly.** A parallel program integrates each output view on the *driver*
(`ParallelProgramOutput`), not in-circuit, so the materialised views are outside the per-worker
snapshot. The parallel checkpoint restores **operator state exactly** — `ParallelProgramSnapshotTests`
asserts the post-restore tick's gathered delta equals an uninterrupted run's — but the integrated
views restart empty. That is enough to price the checkpoint, not yet enough for a parallel recovery.
The serial path has no such gap (its outputs are in-circuit `Integrate` operators, snapshotted like
any other stateful operator). Closing it needs either in-circuit integration per replica (which would
add per-tick work, so: no) or a driver-side region in the snapshot tree — a follow-on, and moot until
there is a parallel `ProgramRunner` at all (§9.2/§9.5).

CI: `ProgramRunnerCheckpointTests` (restore resumes without replaying or skipping; every batch
checkpoints; codecs do not change results) and `ParallelProgramSnapshotTests` (per-worker round-trip
at W=1/2/4/8; W-mismatch refused; codecs do not change results).

### 10.2 Measured (real SF=3, ServerGC, i9-12900K, `IVM_BCAST_MAXROWS=1000`)

`IvmSubgraphScaling` now also times the checkpoint, so the same run reports the step sweep, the
save phase, and the honest total. Output byte-identical to serial at every W, as before.

**(1) The codecs do not touch the step.** The persistent W-sweep reproduces the codec-free sweep's
shape — which is the point: a registered codec is read only by Save/Load.

| subgraph | step scaling W1→W8, codec-free | step scaling W1→W8, with codecs |
|---|--:|--:|
| `trades_history`   | 2.76× | **2.87×** |
| `holdings_history` | 2.17× | **2.17×** |

(The serial codec-vs-codec-free ratio came out 0.87× / 0.94× — i.e. the persistent compile timed
*faster*. That is drift between two separately-timed serial measurements in one process, not a
speedup; the direction that would matter, a systematic penalty, is absent in both.)

**(2) The checkpoint is not cheap, and it only partly parallelizes.**

| subgraph | W | step ms | save ms | total ms | step vs W1 | total vs W1 | MiB |
|---|--:|--:|--:|--:|--:|--:|--:|
| `trades_history`   | 1 | 5852 | 3059 |  8911 | 1.00× | 1.00× | 797.1 |
|                    | 2 | 4571 | 2617 |  7188 | 1.28× | 1.24× | 797.1 |
|                    | 4 | 3272 | 2031 |  5303 | 1.79× | 1.68× | 797.1 |
|                    | 8 | 2036 | 1631 |  3667 | **2.87×** | **2.43×** | 797.2 |
| `holdings_history` | 1 | 8274 | 3941 | 12215 | 1.00× | 1.00× | 1059.9 |
|                    | 2 | 6395 | 3492 |  9887 | 1.29× | 1.24× | 1059.9 |
|                    | 4 | 5729 | 2690 |  8420 | 1.44× | 1.45× | 1059.9 |
|                    | 8 | 3816 | 3152 |  6968 | **2.17×** | **1.75×** | 1060.1 |

Serial reference: `trades_history` step 5717 ms / save 3409 ms / 955.1 MiB; `holdings_history` step
8694 ms / save 3908 ms / 1095.5 MiB.

The save is **34–48% of the durable batch** — a ~1 GiB write per hot subgraph. Per-worker shards are
written concurrently, so it does scale (`trades_history` 3059→1631 ms, 1.88×), but weakly and not
monotonically (`holdings_history` 2690 ms at W=4 → 3152 ms at W=8): past W≈4 the save is disk-bandwidth
bound, not CPU bound. Net effect on end-to-end scaling: `trades_history` 2.87× → **2.43×** (still clears
the ≥2× gate), `holdings_history` 2.17× → **1.75×** (does not, once the checkpoint is counted).

**So: persistence does not erode the parallel step at all — it dilutes the *batch* by adding a
weakly-scaling serial-ish tail.** That is the honest number, and the reason the option is off by
default: turn it on for a durability comparison, leave it off for a step-for-step head-to-head unless
Feldera's configured run also checkpoints per batch.

**(3) Snapshot size is flat in W — and the broadcast W× duplication is measurable and negligible.**
797.1 MiB at W=1/2/4 and 797.2 MiB at W=8 (`trades_history`); 1059.9 → 1060.1 MiB (`holdings_history`).
The per-worker shards *partition* state rather than replicate it; the only replicated state is the
broadcast dimensions, costing **+0.1 / +0.2 MiB at W=8** — exactly what a ≤1000-row size gate predicts.

**(4) The §10.1 driver-view gap, quantified.** The serial snapshot holds one more operator than a
parallel worker's (6 vs 5 for `trades_history`, 9 vs 8 for `holdings_history`) and 158 MiB / 36 MiB
more bytes — that operator is the in-circuit `Integrate` for the output view, and those bytes are the
materialised view itself (982k / 362k rows). That difference *is* the parallel path's missing
coverage, now priced: closing it would add ~4–20% to the parallel save, and it is what a real parallel
recovery would need.
