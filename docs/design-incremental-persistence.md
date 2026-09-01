# Design: incremental (O(delta)) state persistence

**Status: PHASE 0 MEASURED; A1 + the §7.2 restore fix BUILT. 2026-07-22.**
Follows `docs/design-structural-parallel.md` §10 (per-batch persistence, landed 2026-07-21) and
`docs/persistence.md` (approaches A–D). Same discipline as the arcs before it: measure the headroom
first, let the number pick the track, retire what loses.

## 0. The problem

§10 gave the program path a checkpoint. It is a **full state rewrite every batch**:
`Snapshot.WriteAsync` walks every `ISnapshotable` and re-serialises its whole trace — O(state),
independent of how much the batch changed.

Feldera's transaction commit is incremental, and ivm-bench measures Feldera **with persistence inside
the batch window** (`transaction_mode: always` on every model; `feldera_client.py` blocks batch
completion on `transaction_status` / `commit_progress`). So the comparison is only apples-to-apples if
our batch also ends durable — and the moment we turn ours on, batches 2 and 3 rewrite gigabytes for a
delta of a few hundred rows.

§10.2 priced the checkpoint on batch 1 (34–48% of a durable batch, per hot subgraph). Batch 1 is the
*flattering* case: it is the batch where a full rewrite is also the only correct thing to do. The
question this document answers is what the checkpoint costs on **batches 2 and 3**, where the delta is
tiny and the state is not.

## 1. Phase 0 — the gate: how much of a checkpoint is reusable?

Everything downstream depends on one number: of the bytes we rewrite each batch, how many are
byte-for-byte identical to what we wrote last batch? That is the ceiling on what any
"skip-the-unchanged-parts" design (Track B) can save, and it is measurable **without building
anything**.

### 1.1 Method

`tests/DbspNet.Tests/Scratch/IvmCheckpointReuse.cs` (gated scratch probe, no-op unless driven) runs
the real ivm-bench SF=3 program — all 50 views, 452/468 operators, the same spec the server deploys —
for batches 1..3 with persistence on and `retainCount` high enough to keep every checkpoint. It then
content-hashes (SHA-256) every file of every `snap-T` and diffs consecutive snapshots as a multiset.

Two granularities fall out of the same measurement, one per trace family:

- **Flat** (the shipping family) writes one file per operator, so a hash match means *this operator's
  entire state was untouched by the batch*.
- **Spine** (`CompileOptions.TraceFamily = TraceFamily.Spine`) writes one file per immutable LSM
  batch, so a match means *this spine batch survived without being compacted* — which is exactly what
  a reference-manifest snapshot would skip.

Content hashing rather than object identity is deliberate: it is precisely what a content-addressed
batch store could skip, it is immune to the positional `batch_i` renaming that compaction causes, and
it needs no production-code change to measure. It slightly **overstates** reuse where two distinct
batches happen to serialise identically (rare, and small); it would understate reuse if serialisation
were nondeterministic for equal state, which it is not (sorted columnar arrays through a fixed Arrow
codec).

**Driving batches 2 and 3 locally.** ivm-bench applies a batch with a Spark job
(`BatchLoader.appendBatch`) that appends the `batch{N}/<table>` Delta table into `staging/<table>`. The
probe reproduces exactly that append without Spark: a copy of `staging` gets the batch2/batch3 parquet
files plus a prepared Delta commit each, held in a `_pending/` sidecar and promoted into `_delta_log/`
one per batch — a commit appearing between two `RunBatchAsync` calls, which is the real-world shape.
The batch2/batch3 schemas were verified identical to staging's, field for field. `BatchLoader` also
applies UPDATE/DELETE mutations, gated on `BATCH_{N}_UPDATE_PCT` / `BATCH_{N}_DELETE_PCT`; both
default to `0` in every ivm-bench compose file, so the configured run is append-only and that is what
this reproduces.

### 1.2 Results (real SF=3, ServerGC, i9-12900K, whole 50-view program)

Batch 2 and batch 3 each deliver **203 input rows** against ~3.3M rows of staging state.

**Checkpoint size and reuse:**

| family | batch | snapshot MiB | files | unchanged MiB | **% unchanged** | changed MiB |
|---|--:|--:|--:|--:|--:|--:|
| flat  | 1 | 4002.7 | 168 | (baseline) | — | 4002.7 |
| flat  | 2 | 4004.7 | 168 | 631.2 | **15.8%** | 3373.5 |
| flat  | 3 | 4004.8 | 168 | 500.7 | **12.5%** | 3504.0 |
| spine | 1 | 4163.5 | 271 | (baseline) | — | 4163.5 |
| spine | 2 | 4163.7 | 312 | 3289.8 | **79.0%** | 874.0 |
| spine | 3 | 4003.1 | 352 | 2847.5 | **71.1%** | 1155.6 |

**Where the durable batch's wall-clock goes:**

| family | batch | step ms | outputs ms | save ms | **save % of batch** |
|---|--:|--:|--:|--:|--:|
| flat  | 1 | 58002 | 2256 | 19298 | 24.3% |
| flat  | 2 | **66** | 2027 | **19052** | **90.1%** |
| flat  | 3 | **59** | 1784 | **19077** | **91.2%** |
| spine | 1 | 67131 | 2441 | 30682 | 30.6% |
| spine | 2 | **65** | 1773 | **30421** | **94.3%** |
| spine | 3 | **61** | 2016 | **22705** | **91.6%** |

**This is the finding.** On batch 2/3 the engine step is **60 ms** and the checkpoint is **19–30
seconds**: the durable batch is ~90% checkpoint, a ~300× dilution of the work actually done. §10.2's
"34–48% of a durable batch" was the batch-1 number; on the incremental batches the checkpoint isn't
part of the batch, it *is* the batch.

### 1.3 The spine-backed vs flat split

Snapshot bytes by operator kind, spine mode, snap-38 (batch 3), with how much of each survived
unchanged:

| operator kind | ops | files | MiB | % of snap | unchanged | spine-backed |
|---|--:|--:|--:|--:|--:|:--|
| `SpineIncrementalJoinOp` | 37 | 209 | 2204.0 | 55.1% | **100.0%** | yes |
| `SpineIncrementalAggregateOp` | 11 | 27 | 538.3 | 13.4% | 60.3% | yes |
| `IntegrateOp` | 16 | 16 | 455.2 | 11.4% | 53.8% | **no** |
| `PartitionedWindowAggregateOp` | 30 | 30 | 421.4 | 10.5% | 0.3% | **no** |
| `PartitionedOffsetOp` | 6 | 6 | 312.1 | 7.8% | 0.5% | **no** |
| `SpineDistinctOp` | 11 | 26 | 57.4 | 1.4% | 100.0% | yes |
| `SpineIncrementalLeftJoinOp` | 5 | 28 | 13.9 | 0.3% | 99.5% | yes |
| `PartitionedRankOp` | 10 | 10 | 0.8 | 0.0% | 39.9% | **no** |

Rolled up:

| | MiB | % of snapshot | unchanged | **changed** | % of all changed bytes |
|---|--:|--:|--:|--:|--:|
| spine-backed | 2813.6 | 70.3% | 92.4% | 214.1 | 18.5% |
| flat residue | 1189.5 | 29.7% | 20.8% | 941.6 | **81.5%** |

**The four operator kinds with no spine sibling are 30% of the snapshot but 81% of what a
reference-manifest commit would still have to write.** The spine-backed 70% is already almost
perfectly reusable (92.4%); its residual 214 MiB is a single large `SpineIncrementalAggregateOp` (op
302, 213.9 MiB, 0% unchanged) that the tiered strategy fully compacted that batch — compaction churn,
inherent to LSM, not a defect.

The missing siblings, in size order: `IntegrateOp` (the materialised output views, 11.4%),
`PartitionedWindowAggregateOp` (10.5%), `PartitionedOffsetOp` (7.8%), `PartitionedRankOp` (~0%).
`RecursiveCteOp` also has none but does not appear in this program.

### 1.4 What the numbers say

1. **Reuse headroom exists, but only in spine mode.** Flat's 12.5% is not a design to build on — it
   comes almost entirely from a few operators whose state the batch never touched at all (e.g. op 397,
   an `IntegrateOp`, 205.3 MiB, 100% unchanged), and there is no mechanism to make it better without
   sub-operator granularity, which is what the spine already is.
2. **Spine mode is not free, and today it is a net loss.** Its step costs +16% on batch 1 (67.1s vs
   58.0s), its snapshot is slightly larger (un-consolidated duplication), and its save is *worse* than
   flat's (+60% on batch 2, +19% on batch 3) because per-batch files mean many more, smaller writes.
   The 70–79% reuse is potential energy: it pays nothing until something is built to skip those bytes.
3. **Even a perfect skip-the-unchanged design does not reach O(delta).** Projecting the save
   bytes-proportionally (an upper bound — a reference-manifest commit still pays per-batch
   bookkeeping): spine batch 3 save 22.7s → 6.6s, so the durable batch goes 24.8s → 8.6s (2.9×). Good,
   but the checkpoint is still **76% of the batch**, for 203 rows. The floor is the *changed* bytes —
   1155.6 MiB — and a 203-row input cannot justify 1.1 GiB of writes under any accounting.
4. **Nothing that rewrites state per batch can close this gap.** 60 ms of step against 19 s of save
   says the checkpoint has to stop being per-batch work at all.

### 1.5 Limits of this measurement — stated plainly

- **The delta is small: 203 rows.** ivm-bench's configured default is `BATCH_2_INSERT_PCT=1` (1%), but
  the copied-out SF=3 `batch2`/`batch3` directories hold only ~203 rows each, so this is the
  small-delta regime. That is the regime the argument lives in — the smaller the delta, the worse a
  full rewrite looks — but the reuse fractions are correspondingly optimistic. A larger delta produces
  bigger level-0 batches and triggers compaction more often, so spine reuse falls; flat reuse, already
  12.5%, falls too.
- **Append-only.** `BATCH_{N}_UPDATE_PCT` / `DELETE_PCT` default to 0, so no retractions were applied.
  Structurally a retraction is just another delta landing in a new level-0 batch, so the *shape* of
  the result should hold, but the volume would differ.
- **Serial path only.** The parallel program path's driver-side view gap (§10.1) is untouched here.
- Both runs were repeated end to end and reproduced their reuse percentages exactly (flat
  15.8%/12.5%; spine 79.0%/71.1%).

## 2. Track A — generalise `WalRecorder` off `CompiledQuery`

Approach (C) from `docs/persistence.md` — periodic full snapshot plus a write-ahead log of input
deltas since it — is already **built, tested, and shipping**. `WalRecorder` does input-delta logging,
snapshot pairing, segment pruning against the snapshot tick, and crash-safe operation ordering. It is
simply **not reachable from the program path**: it is hard-coupled to `CompiledQuery` (field, ctor,
both `CreateAsync` factories, `StepAsync`).

**The coupling is shallower than it looks.** The entire surface `WalRecorder` uses from
`CompiledQuery` is three members — `Circuit`, `Inputs`, and `Step()` — and `CompiledProgram` exposes
all three with identical signatures:

| member | `CompiledQuery` | `CompiledProgram` |
|---|---|---|
| `RootCircuit Circuit { get; }` | yes | yes |
| `IReadOnlyDictionary<string, TableInput> Inputs { get; }` | yes | yes |
| `void Step()` | yes | yes |

So Track A is an interface extraction (`ISteppableCircuit`, or equivalent) implemented by both, with
`WalRecorder` retyped onto it — a refactor with no behavioural change to the existing query path, and
CI already covering the semantics via `WalRecorderTests` / `HybridSnapshotWalTests`.

On top of that, `ProgramRunner` needs a checkpoint policy rather than the unconditional
"snapshot at the end of every `RunBatchAsync`" it has today: append the batch's input deltas to the
WAL, and take a full snapshot only every N batches (or every N bytes/rows of WAL). Recovery = load the
last snapshot, replay the WAL past it — already implemented, including the `StartTick`-based segment
skip.

**What Phase 0 projects for Track A on batch 2/3:** step 60 ms + outputs ~1.9 s + a WAL append of 203
rows (milliseconds) ≈ **~2 s durable, against 21 s today** — ~10×, and the amortised snapshot cost is a
policy knob rather than a per-batch tax. This is the only option measured here that makes the
checkpoint stop dominating.

**Costs and honest caveats.**
- Recovery becomes O(snapshot + WAL since it), not O(snapshot). Bounded by the snapshot interval,
  which is the classic (C) trade.
- It is *input replay*, not incremental *state* persistence. Feldera commits state incrementally; we
  would commit input incrementally and reconstruct state. Equivalent for durability and for the
  benchmark's batch window; not equivalent for recovery latency at a long snapshot interval.
- Output idempotency (`docs/persistence.md` §cross-cutting) matters more once a batch can be replayed:
  the runner's truncate-write outputs are naturally idempotent, so this is a non-issue for ivm-bench,
  but it should be stated in the contract.
- Offsets already ride in the snapshot manifest and commit atomically with state (§10.1), so the
  alignment invariant carries over unchanged.

## 3. Track B — batch identity + reference-manifest snapshots + refcount/GC

Make the snapshot a *manifest of batch references* instead of a copy of every batch: a checkpoint
names the batch files that constitute the state, and only batches created since the last checkpoint
are written.

**What has to be built (each verified against the tree, 2026-07-22):**

1. **Durable batch identity.** `SpineBatch<TKey,TWeight>` is `internal abstract` and has **no id**.
   Snapshot file names are **positional** (`SpineSnapshot.BatchFileName(prefix, i)`, written by a
   `for (i…) saveOne(...)` loop over the level-flattened list), and compaction reorders and renumbers,
   so position cannot serve as identity. Every batch needs an id assigned at construction and carried
   through merge/spill.
2. **A shared batch store.** The closest existing thing is disk spill: `SpineSpillConfig` already
   writes batches to `{Prefix}/batch_{n}.arrows` via `_spillCounter` and reads them back lazily
   (`SpilledSpineBatch.FilePath` / `DeleteAsync`). Today the snapshot path *re-materialises* a spilled
   batch and rewrites it into the snapshot — double I/O that this track would delete outright.
3. **Refcounting / GC.** Compaction **deletes its input spill files** (`SpineZSetTrace.Apply` →
   `SyncDelete`). The moment a retained snapshot references a batch file, that unconditional delete
   becomes a correctness bug. Shared files across retained snapshots need refcounts (or
   mark-and-sweep against the retained manifests) before this is safe.
4. **Spine siblings for the flat residue** — otherwise the ceiling is the §1.3 number: 30% of bytes,
   81% of the changed bytes, untouched. In size order: `IntegrateOp`, `PartitionedWindowAggregateOp`,
   `PartitionedOffsetOp`, `PartitionedRankOp` (plus `RecursiveCteOp`, unused here). `IntegrateOp` is
   the materialised output view and is the one with a real design question attached — §10.1 already
   flags that the parallel path integrates on the driver, outside the snapshot.
5. **Spine mode has to stop costing more than it saves** (§1.4 point 2).

**What it would buy, from Phase 0:** with the flat residue as-is, spine batch-3 save 22.7s → 6.6s
(2.9× on the save, 2.9× on the durable batch). With spine siblings for all four kinds, the changed
bytes fall from 1155.6 MiB to ~214 MiB, i.e. ~94.7% unchanged — a save of roughly 1.2 s. That is a
real number, and it is where Track B becomes genuinely attractive.

**But** it is five workstreams — one of which (refcount/GC over shared mutable-lifetime files) is the
kind of thing that produces subtle, data-losing bugs — to reach a batch that is still doing
snapshot-shaped work every batch.

## 4. Recommendation

**Track A first. Track B second, and only as an amplifier.**

The reasoning is entirely from §1:

- The problem is not that the checkpoint writes too many bytes; it is that it runs **every batch**.
  60 ms of step against 19 s of save cannot be fixed by making the 19 s into 6 s. Track A removes the
  per-batch checkpoint; Track B shrinks it.
- Track A is a refactor of shipping, tested machinery across a three-member surface that both types
  already expose, plus a policy knob on `ProgramRunner`. Track B is five workstreams including batch
  identity, a shared store, GC over shared files, and four new spine operator variants.
- Track A is trace-family agnostic. Track B requires spine mode, which today costs +16% step and a
  worse save — so Track B must first pay back a regression it introduces.
- They compose rather than compete: once checkpoints are periodic instead of per-batch, Track B makes
  each one ~3× cheaper (or ~19× with the missing siblings), which is exactly what shortens the
  snapshot interval and therefore bounds Track A's recovery time. That is the right order to buy them
  in.

**Proposed staging.**

1. **A1 — extract the interface**, retype `WalRecorder`, prove `CompiledProgram` works through the
   existing WAL tests. No behaviour change on the query path. **DONE 2026-07-22** — see §6.
2. **A2 — checkpoint policy on `ProgramRunner`**: WAL-per-batch, snapshot every N. Re-run
   `IvmCheckpointReuse` (it already reports step/outputs/save separately) to confirm the durable batch
   for batches 2/3 lands near the projected ~2 s.
3. **Re-measure, then decide.** If the amortised snapshot still dominates at an acceptable recovery
   bound, take Track B — starting with **B4 (spine siblings for the flat residue)**, since §1.3 says
   that is where 81% of the changed bytes are, and it is the one piece of Track B with value
   independent of the manifest work.
4. Only then batch identity + reference manifests + refcount/GC.

## 5. What Phase 0 does not answer

- **Behaviour at a realistic delta size.** Everything here is the 203-row regime (§1.5). Before
  committing to Track B's ceiling, re-run the probe against a synthesised 1% delta (the ivm-bench
  configured default) to get the reuse-vs-delta-size curve. Track A's case does not depend on this;
  Track B's does.
- **Recovery time.** Nothing here measures restore. Track A explicitly trades recovery latency for
  batch latency, so the snapshot-interval knob needs a restore measurement to be set honestly.
- **The parallel path.** Serial only; the driver-side view gap (§10.1) is unaddressed and remains
  moot until a parallel `ProgramRunner` exists.
- **Whether Feldera's commit is actually cheaper on these batches.** We have priced ours. The
  head-to-head still needs Feldera's own per-batch commit cost broken out of its batch window.

## 6. A1 — built (2026-07-22)

`ICompiledCircuit` (`src/DbspNet.Sql/Compiler/ICompiledCircuit.cs`) declares the three members
`WalRecorder` actually needs — `Circuit`, `Inputs`, `Step()`. `CompiledQuery` and `CompiledProgram`
each gained the interface in their declaration and **nothing else**: both already exposed all three
publicly with matching signatures, so no member was added, moved, or reimplemented.

`WalRecorder` and `WalManifest.ComputePlanFingerprint` are retyped from `CompiledQuery` onto
`ICompiledCircuit`. Source-compatible for existing callers, since `CompiledQuery` implements it.

Two facts made this smaller than §2 assumed:

- **`WalRecorder` has no non-test call sites in `src/`.** The only production references were doc
  comments; every real caller is a test.
- **The per-tick capture hook was never query-specific.** `TableInput.OnPushed` — the event the
  recorder subscribes to — lives on `TableInput`, which both compiled shapes hand out through the
  same `IReadOnlyDictionary<string, TableInput> Inputs`. Only the top-level container type was
  coupled.

**Verification.** The 122 existing persistence tests (including `WalRecorderTests`,
`HybridSnapshotWalTests`, `LifecycleTests`, `SnapshotRetentionTests`) pass **unmodified** — that is
the behaviour-preservation proof. `ProgramWalRecorderTests` adds seven tests driving a two-source,
three-view `CompiledProgram` through the same machinery: per-table segments (views are never logged),
replay restoring every output view, snapshot+WAL and snapshot-only recovery, input-schema drift and
table-set mismatch refused, and a view-body refactor still replaying. Full suite green (2242 passed).

**Not yet done:** `ProgramRunner` still checkpoints unconditionally at the end of every
`RunBatchAsync`. Nothing about the measured batch cost has changed yet — A1 makes the WAL *reachable*
from the program path; A2 is what makes it *used*.

## 7. Recovery measured — a correctness bug (real) and a replay-cost scare (retracted) (2026-07-22)

§5 flagged that nothing measured restore, and that A2's snapshot-interval knob cannot be set honestly
without it. `tests/DbspNet.Tests/Scratch/IvmRecoveryProbe.cs` measures it on real SF=3 state by
recording batches 1–3 through the WAL, snapshotting at a configurable batch, then recovering three
ways — every recovery **verified** against output-view digests captured during recording, not just
timed.

### 7.1 The numbers (flat family, snapshot after batch 2, ~4.0 GiB state)

As first recorded — **these timings are wrong; the correctness column is right**:

| leg | what | wall (as measured then) | correct? |
|---|---|--:|:--|
| (a) | snapshot restore only | 34.5 s | yes |
| (b) | snapshot + WAL replay of 1 batch | 86.7 s (replay leg 52.2 s) | **NO** |
| (d) | snapshot restore + the same batch driven through the connectors, no WAL | 35.3 s (step leg 0.74 s) | **NO** |

The **timings** were contaminated by the measurement defect retracted in §7.3 — corrected numbers
there. The **correctness** column stood up and is the substance of §7.2: leg (d) never touches the
WAL and was wrong the same way as (b), which is what localised the fault to snapshot restore. Both
read `verified` once §7.2 was fixed (§8).

Recording, for comparison: a full snapshot costs ~18.6 s, and a WAL append for an incremental batch
costs ~0.25 s against 0.2 MiB of log.

Two findings fell out — one real (§7.2), one an artifact of my own measurement (§7.3).

### 7.2 Snapshot restore silently produces wrong state

Leg (d) is the isolation: it never touches the WAL, and it is **also wrong**, in exactly the same way
as (b) — so the fault is in **snapshot restore**, not in anything A1 added. One output view of 16,
`market_volatility`, comes back at **exactly 2×** its correct row count (1796 → 3592). The pure
restore at the snapshot tick (leg a) is correct; the divergence appears only on the *next step after*
a restore.

**Mechanism.** `IncrementalAggregateOp.LoadAsync` deliberately does not serialise `_aggCache` /
`_stateCache`; it rebuilds them by calling `_aggregator.Update(ref state, None, group, group)` once
per group over the restored trace — a **bulk fold**, in trace-enumeration order. The live run built
the same cache by **incremental folds**, in tick-arrival order. `docs/persistence.md` argues this
converges, and for SUM / COUNT / MIN / MAX it does — those are exact. But `SqlStddevAggregator`
accumulates `double Sum` / `double SumSq` with repeated `+=`, and `SqlAvgAggregator` likewise:
floating-point addition is not associative, so the two orders can differ in the last bits.

The aggregate then emits a retraction of the value it holds in `_aggCache`, while the downstream
`IntegrateOp` holds the value that was actually materialised before the snapshot. The two differ, the
retraction does not cancel, and the view accumulates both the old row and the new one — the observed
factor of exactly 2.

Consistent with this: `market_volatility` is one of four output views using `STDDEV`/`AVG` over
`DOUBLE` feeding a global `RANK`, and only it diverges — the fault fires only when the reconstructed
bits actually differ *and* the row is re-emitted, which is data-dependent. That data-dependence is
also why snapshotting after batch 1 happened to come back clean while snapshotting after batch 2 does
not: it is luck, not correctness.

**Severity.** This is **not** a bug A1 introduced — it is in the (B) snapshot path that
`design-structural-parallel.md` §10 shipped, and it is silent. §10.3 argues restore is "fail-safe"
because a shape mismatch hard-fails on the plan fingerprint; that reasoning covers *shape* drift and
says nothing about *value* drift in a rebuilt cache. Practical exposure today is limited — persistence
is off by default — but any use of the checkpoint with a float aggregate can silently corrupt a view.

**It blocks A2.** A2's whole premise is "snapshot every N batches and replay the rest," which makes
restore a routine operation rather than a rare one. Fixing this comes first.

**FIXED 2026-07-22 — see §8.**

### 7.3 ~~WAL replay is ~70× slower than the equivalent live ingest~~ — RETRACTED (2026-07-22)

**This finding was wrong — an artifact of how the number was computed, not a property of the replay
path.** The original claim was that legs (b) and (d) applied the same 9 ticks at 52.2 s vs 0.74 s.

**How it was wrong.** The probe computed `replay = (b) − (a)`, subtracting leg (a)'s snapshot restore
from leg (b)'s total. But leg (b) restores its *own* ~4 GiB circuit, and it ran while leg (a)'s
circuit was **still reachable** — two 4 GiB circuits live in one process. The second restore paid far
more GC pressure than the first, and the subtraction charged all of that excess to "replay".
Compounding it, leg (d)'s reported total reused leg (a)'s restore time instead of timing its own, so
the (b)-vs-(d) comparison behind the "70×" was never like-for-like.

**What broke the story.** `tests/DbspNet.Tests/Scratch/WalReplayProfile.cs` reproduces the *ratio*
away from SF=3 but not the *magnitude*: net per-tick replay is sub-millisecond to ~1.5 ms and does
not scale with table or tick count, so 9 ticks is ~10 ms. Most of what a naive measurement calls
"replay" is fixed `CreateAsync` setup — manifest read/rewrite, input subscription, and opening a
fresh segment file per table. Ten milliseconds cannot be 52 seconds, which sent the investigation
back to the measurement rather than the code.

**Corrected numbers** — same configuration, but each leg scoped so the prior circuit is unreachable,
a collection forced between legs, and leg (d)'s own restore timed:

| leg | what | wall | correct? |
|---|---|--:|:--|
| (a) | snapshot restore only | 35055 ms | yes |
| (b) | snapshot + WAL replay of 1 batch | 34779 ms | yes |
| | **replay leg = (b) − (a)** | **−276 ms** | — |
| (d) | restore + same batch via connectors | 35524 ms (own restore 35464 + step 60) | yes |

The (a)-vs-(d) restore cross-check agrees within ~1%, so the subtraction is now trustworthy. **The
replay leg is −276 ms: zero within noise.** Replaying a small batch from the WAL costs nothing
measurable.

The probe now refuses to extrapolate a coefficient inside the noise floor of differencing two ~35 s
restores — the old code happily projected *negative* recovery time at N = 1000 — and its isolation
verdict no longer prints a fault diagnosis when both legs are correct.

**Incidental confirmation:** all three legs now read `verified`, where (b) and (d) were WRONG before.
That is §7.2's fix independently re-confirmed on real SF=3 state.

### 7.4 What this does to the A2 knob

With §7.3 retracted, recovery is **dominated by the snapshot restore** — ~35 s for ~4 GiB of state —
and replaying a small batch on top is free. The knob is far less constrained than §7.3 implied: there
is no per-batch replay tax pushing toward a short interval.

The real bound is different, and worth stating precisely because the "N batches" framing invited the
wrong intuition. **Replay cost tracks the work in the replayed ticks, not the number of batches.**
These incremental batches are ~200 rows, so replaying them is free; replaying a *bulk* batch costs
roughly what that batch's step cost originally — batch 1 here was ~60 s. The interval is bounded by
the largest batch still in the log, not by how many batches are.

For the ivm-bench shape that is an easy trade: snapshot once after the bulk load, then WAL the
incremental batches essentially indefinitely. Recovery stays ~35 s plus trivial replay, and the
per-batch checkpoint tax §1.2 measured at ~90% of a durable batch disappears.

## 8. §7.2 fixed (2026-07-22)

Scoped by measurement, per §7.2's own warning that the perf argument had to be checked rather than
assumed. Temporary instrumentation on a real SF=3 restore said the aggregate cache rebuild is only
**4.5% of restore** (1899 ms of 42072 ms; 11 aggregate ops, 1.19M groups over 3.08M rows), and
persisting state for every aggregator would add ~27 MiB to a 4005 MiB snapshot (0.7%). So **neither
speed nor size argues for persisting state uniformly** — the decision rests on correctness and code
surface alone, and the scope is therefore narrow.

**What persists.** Only the order-sensitive accumulators: `SUM`/`AVG` over `DOUBLE`, and
`STDDEV`/`VAR`. Everything exact — integer `SUM`, `COUNT`, `MIN`/`MAX`, the `Decimal128` forms —
still reconstructs by folding the restored group, because folding lands on the same state either way.

**Per-slot, not per-composite.** `CompositeAggregator` holds one state slot per sub-aggregate, so an
all-or-nothing rule would write no blob for `SELECT AVG(x), MIN(y) … GROUP BY k` and let the `AVG`
drift exactly as before. The blob is framed per slot (`[present][length][bytes]`); on load the
operator folds first (correct everywhere a blob is absent), then overlays only the persisted slots.

**Recovering the emitted value.** Rather than persist the value too, the operator re-derives it with
`Update(ref state, None, delta: empty, after: group)`. Every aggregator answers an empty delta from
its current state without mutating it — true of all ten, including `SqlSumAggregator`, whose
`DistinctNonNullRows` transition tracking looked like the likely exception but sits inside the delta
loop. That was an unenforced assumption spread across ten implementations, so it is now pinned per
aggregate kind by `AggregatorEmptyDeltaTests` (13 cases × value / no-mutation / idempotence /
agreement with `Compute`). `SqlApproxCountDistinct` is the one documented exception — with a *null*
state it rebuilds its sketch from `after` — pinned separately.

**Where the blob lives.** Per-key state cannot be persisted by the operator alone: it is generic in
`TKey` and only the codec knows how to encode one. `IIndexedZSetTraceCodec` therefore gained an
optional keyed-blob capability (default members, so only the Arrow codec implements it; the typed
adapter and test doubles are untouched). A missing blob file loads as "no blobs", so the fold path
stays reachable.

**Manifest v3 → v4.** Deliberate despite the format being additive: a v3 snapshot has no state file,
so a v4 reader would silently fall back to the reconstruction that was wrong. Rejecting it makes the
engine rebuild from scratch rather than resume from state it cannot vouch for.

**Verification.** `FloatAggregateRestoreTests` — the deterministic repro landed before the fix —
passes for both `AVG` and `STDDEV`, including its strict assertion that restore-then-continue equals
an uninterrupted run **bit for bit**.

**Follow-up: the spine sibling had the same defect.** The fix above covered the flat
`IncrementalAggregateOp` only. `SpineIncrementalAggregateOp` rebuilt its caches the same lossy way
(its `LoadAsync` even says "same logic as `IncrementalAggregateOp.LoadAsync`"), and nothing caught it
until the conformance harness of `docs/design-layering-review.md` §8.1 ran the same shape against
both trace families and failed on `agg_float/spine`. Same fix applied. That is the duplication axis
of the layering review biting exactly as predicted: one bug, two implementations, one of them fixed.

**Still open, unchanged by this:** nothing in the recovery path. §7.3's replay-cost scare was
retracted after profiling — a measurement artifact, not a defect — so the revised order is now
**layering review → A2/Track B**.

## 9. Recovery re-measured on the M4 Pro, real SF=3 (2026-08-31)

First run since the data was regenerated locally (`docs/ivm-bench-gap-analysis.md`). Flat family,
`IVM_BATCHES=1`, snapshot after batch 1. **Both recovery legs verified** against the output-view
digests captured during recording — this is a timing of a *correct* answer, which §7.2 taught us not
to assume.

| leg | wall |
|:--|--:|
| record batch 1 (ingest + WAL + step, 20 ticks) | 40,982 ms |
| snapshot write at tick 20 | **11,501 ms** |
| (a) snapshot restore → tick 20, verified | **21,701 ms** |
| (b) snapshot + WAL replay → tick 20, verified | 20,045 ms |

Snapshot on disk **4050.7 MiB**, WAL 0.0 MiB.

**The restore number is the one that matters for pause/resume: ~21.7 s to bring back 4.05 GiB of
state.** Note it is nearly **2× the cost of writing it** (11.5 s) — reading is the expensive
direction, which is the opposite of the intuition the per-batch-checkpoint framing encourages, and it
is exactly why §4's "Track A first" ordering does not serve a pause/resume goal (§4 was chosen when
the *write* ran every batch; a deliberate pause pays the write once and the read every time).

### What this run cannot measure — and the printed extrapolation is meaningless

Leg (b) came out **1,656 ms faster than leg (a)**, i.e. a *negative* replay coefficient, and the
probe's own linear projection dutifully printed `N = 100 -> -143.9 s`. That table is nonsense and
should be ignored.

The cause is configuration, not the engine: with `IVM_BATCHES=1` and the snapshot taken *after* batch
1, **there are no ticks past the snapshot to replay**. The WAL is 0.0 MiB on disk (the log is dropped
once a snapshot supersedes it), so leg (b) is leg (a) plus nothing, and the −1.6 s is run-to-run
variance between two ~21 s measurements. This is the same trap §7.3 already had to retract once:
**differencing two large, noisy legs to extract a small coefficient.**

A real replay coefficient needs ≥2 batches, which needs `IVM_STAGING_ROOT` — a multi-version staging
copy, built by running the batch loader's `append 2` / `append 3` and snapshotting `staging/` between
each. Not built here.

### Not comparable to the i9

§7.1 recorded restore at ~34.5 s and the snapshot write at ~18.6 s. **Do not read 21.7 s vs 34.5 s as
an improvement** — different machine, and wall time does not transfer (§25.1 established that
*allocation* is host-independent; wall emphatically is not). What *is* comparable is the state size:
4050.7 MiB here against the recorded ~4.0 GiB, confirming the regenerated dataset puts the same state
in memory.

### 9.1 Multi-batch staging built; replay measured directly (2026-08-31)

§9 could not price replay because a one-batch run leaves nothing past the snapshot. Multi-version
staging now exists, so batches 2 and 3 replay for real.

**How the staging is built** (the mechanism `IVM_STAGING_ROOT` expects, undocumented until now).
The probe drives Delta versions by hand: `RewindPendingCommits` removes any `_pending/*.json` that
also sits in `_delta_log/`, and `PromotePendingCommits(root, b-1)` copies `_pending/<version>.json`
into `_delta_log/` before batch `b`. Only the **commit JSON** is held back — the parquet data files
are already on disk from the append, so promoting the commit is what makes a batch visible. So:

```bash
cd ~/src/ivm-bench/docker                       # after datagen + batch-loader `init`
cp -R …/delta/staging …/delta/staging-batch1-pristine     # keep the batch-1-only copy
… docker compose -f docker-compose.batch-loader.yml run --rm spark-batch-loader append 2
… docker compose -f docker-compose.batch-loader.yml run --rm spark-batch-loader append 3
cp -R …/delta/staging …/delta/staging-multi     # now at versions 0,1,2
# in staging-multi, per table: move _delta_log/…0001.json and …0002.json into _pending/,
# and delete that version's orphaned .crc / .<name>.crc sidecars
rm -rf …/delta/staging && mv …/delta/staging-batch1-pristine …/delta/staging
```

The last line matters: `staging/` must stay batch-1-only or `IvmBatchProfile`'s batch-1 run silently
ingests all three batches. `staging-multi/` is the probe's copy; the two coexist.

**Measured** (`IVM_BATCHES=3 IVM_SNAPSHOT_AFTER=1`, flat, ServerGC):

| leg | wall | reaches |
|:--|--:|:--|
| record batch 1 (20 ticks) | 46,033 ms | |
| snapshot write | 17,253 ms | 4050.7 MiB |
| record batch 2 (9 ticks) | **36 ms** | |
| record batch 3 (9 ticks) | **52 ms** | |
| (a) snapshot restore | 22,640 ms | tick 20, verified |
| (b) snapshot + WAL replay | 21,189 ms | **tick 38 (end of batch 3)**, verified |
| (d) restore + connectors | 19,674 ms | tick 38, verified — restore 19,611 ms + **step leg 62 ms** |

**Recovery is restore-dominated; replaying small incremental batches is free.** The direct number is
leg (d)'s step leg: **62 ms to apply batches 2 and 3**, against a ~20 s restore. That is measured, not
subtracted.

**The subtraction is still unusable, and the probe now says so itself.** `(b) - (a)` came out
**−1,451 ms** — negative again — and the probe reports it as below its own ±1,132 ms noise floor for
differencing two ~23 s restores. Leg (d) exists precisely to avoid that subtraction, and its
cross-check (a) 22,640 ms vs (d) 19,611 ms is the honest picture: **two restores of the same snapshot,
in the same run, differ by 15%.** Treat any restore figure as ±2–3 s. The snapshot *write* is noisier
still — 11,501 ms in the §9 run vs 17,253 ms here for the identical 4050.7 MiB.

**The caveat that actually bounds a snapshot interval** (the probe prints it, and it is the important
part): replay cost tracks the **work in the replayed ticks, not the batch count**. These batches are
~200 rows. Replaying a *bulk* batch costs what that batch's step originally cost — ~46 s for batch 1
here — so the interval is bounded by the largest batch still in the log, not by how many batches are.

**For pause/resume:** resume latency ≈ restore ≈ **20–23 s for 4 GiB**, essentially independent of how
many small batches follow the snapshot. The lever is the restore path, not replay — which is what the
lazy/one-serializer direction attacks, and which §4's "Track A first" ordering does not.

## 10. Radix-tree re-expression — ceiling measured before building (branch `radix-tree-state`)

The premise for re-expressing operator state as an indexed Z-set (comparison §4: Feldera "did not
write a spillable window aggregate: they expressed the window aggregate's state *as* an indexed
Z-set, so it inherits spill, checkpoint and GC for free"). Measured against the real SF=3 restore
before writing the rewrite.

### 10.1 The premise needed correcting first

**We already persist as a Z-set.** `PartitionedWindowAggregateOp.SaveAsync` flattens its
`Dictionary<TKey, SortedDictionary<TInRow,long>>` into a flat `ZSet` and hands it to the **generic**
codec — there is no bespoke serializer. The "one serializer per operator" framing was wrong.

What is bespoke is the **restore rebuild**. Across the stateful operators, restore falls into two
shapes:

| operator | work beyond deserialize |
|:--|:--|
| `DistinctOp`, `IncrementalJoinOp`, `IncrementalLeftJoinOp` | **none** — `_trace.Integrate(loaded)` |
| `IncrementalAggregateOp` | rebuild per-group aggregator caches (one pass) |
| `PartitionedTopKOp` | re-partition every row, insert into a comparer-ordered `SortedDictionary`, recompute windows |
| `PartitionedWindowAggregateOp` | the same, plus `RecomputePartition` over every partition |

Operators whose state *is* a trace restore in deserialize time. Operators keeping a **parallel
runtime structure** must rebuild it from the flat Z-set. **That** is what the radix-tree trick
removes — not a codec.

### 10.2 Apportioned (real SF=3, flat, ServerGC, 4.05 GiB snapshot, ~20 s restore)

`Snapshot.ProfileLoad` (new, opt-in, free when off) times each operator's `LoadAsync`;
`PartitionedWindowAggregateLoadProfile` splits that operator's own restore into its two legs.

| kind | ms | share | count | rebuild? |
|:--|--:|--:|--:|:--|
| `IncrementalJoinOp` | 7432 | **37.6%** | 37 | **none** |
| `PartitionedWindowAggregateOp` | 5867 | 29.7% | 30 | yes |
| `IncrementalAggregateOp` | 2614 | 13.2% | 11 | partial |
| `PartitionedOffsetOp` | 2549 | 12.9% | 6 | yes |
| `IntegrateOp` | 1088 | 5.5% | 16 | none |
| `DistinctOp` / `IncrementalLeftJoinOp` / `PartitionedRankOp` | 199 | 1.0% | 26 | none |

And the split inside the target operator (30 ops, 5759 ms/restore — cross-checks against the 5867 ms
row above to ±2%):

- deserialize **1893 ms — 9.6% of restore** (irreducible; every trace-shaped operator pays it)
- **rebuild 3866 ms — 19.6% of restore** ← the prize

### 10.3 The ceiling, and what it means

**Re-expressing `PartitionedWindowAggregateOp`'s state as an indexed Z-set can remove ~20% of
restore** — ~3.9 s of ~20 s. Adding `PartitionedOffsetOp` (same shape, split unmeasured; at the same
2:1 ratio ≈ 8.6%) takes the ceiling to **~28%**.

**It cannot touch the majority.** `IncrementalJoinOp` alone is **37.6% with zero rebuild** — pure
deserialize and `Integrate` — and deserialize is another ~10% inside the target operator itself. So
**most of restore is reading bytes back, not rebuilding structure.**

That reframes the arc, and it is the more useful finding: the radix-tree re-expression is the
**smaller half** of Feldera's advantage. Their restore is `O(#files)` because batches stay on disk and
pages come through a buffer cache (comparison §5) — i.e. they mostly **do not deserialize**. Against
that, re-expressing state buys ~20–28%; lazy, file-backed restore attacks the ~70% that deserialize
and trace-integrate account for.

**Recommendation: do not start with the rewrite.** A 20% cut on a 20 s restore is ~4 s, for an
invasive change to the operator's runtime representation, and the same effort spent on lazy restore
targets three times the term. If the window operator's rebuild is worth removing on its own merits
(it also costs on every recovery, and `PartitionedTopKOp` shares the shape), it should be sequenced
*after* the restore path stops being deserialize-bound — otherwise it is optimising 20% of the wrong
thing.

**Measurement caveats.** Restore wall varies ~15% between legs in one run (§9.1), so the shares are
±2 points. The `PartitionedOffsetOp` split is inferred, not measured. All of this is one workload —
ivm-bench's 50-view program — and the mix of operator kinds is what sets these shares.

## 11. Lazy / file-backed restore — the deserialize term apportioned (2026-08-31)

The arc kicked off by `docs/next-arc-lazy-restore-prompt.md`. §10 left restore **deserialize-bound**
("most of restore is reading bytes back, not rebuilding structure") and pointed at Feldera's lazy,
file-backed recovery — `O(#files)`, not `O(state)`. That is a slogan until the deserialize term is
itself apportioned, so this section answers the arc's four questions in order, on the real SF=3
program, flat family, ServerGC, M4 Pro.

**Every restore timed below was verified** against recorded output-view digests. §7.2 was a restore
that silently produced wrong state; a timing of a wrong answer is worthless.

### 11.1 The instrument (and one trap it had to remove first)

`tests/DbspNet.Tests/Scratch/IvmRestoreProfile.cs` splits the run so the expensive half is paid once:
`record` ingests batch 1, snapshots, keeps ingesting batches 2–3 and writes `restore-probe.json`
(digest at the snapshot, digest at the end, connector cursors) beside the snapshot; `profile` and
`replay` then restore that snapshot repeatedly — ~25 s a run instead of ~80 s — and check the recorded
digests. Two opt-in instruments, both free when off:

- **`SnapshotRestoreProfile`** (`DbspNet.Core`, written by the Arrow codecs and by each operator's
  `LoadAsync`) splits restore into **read** (file → memory), **decode** (Arrow IPC framing),
  **extract** (Arrow columns → `object?[]`, i.e. the boxing pass, timed separately for VARCHAR),
  **materialize** (one row object per row), **index** (hashing rows into the loaded Z-set),
  **integrate** (folding that Z-set into the operator's trace) and **rebuild** (re-partition /
  re-sort / recompute for operators whose runtime state is not a Z-set). Enabled for the duration of
  a restore when `Snapshot.ProfileLoad` is set.
- **`TraceAccessProfile`** (`DbspNet.Core.Collections`) counts key probes, distinct keys probed and
  whole-collection scans against **trace-state** collections only. Its gate is a `static readonly`
  environment read (`DBSPNET_TRACE_ACCESS_PROFILE`), so with the variable unset the JIT folds every
  call site out of the join probe path — the shipping code is untouched, not merely un-taken.

The access counters live in a `ConditionalWeakTable` keyed by the collection, not in a field on it.
The field version was written first, and `w1profile` caught it: **+2 to +4 B/event on q20/q4/q9** —
8 bytes on every Z-set, including the per-key inner group of every indexed one, on exactly the join
queries. With the side table, `w1profile` B/event is **byte-identical to the committed baseline on all
ten queries**. That instrument earns its keep again (`docs/w1-profile.md`, §25.1).

**The trap, and what it exposed.** The probe's digest hashed cells with `object.GetHashCode()`. That
is invisible in `IvmRecoveryProbe`, which records and verifies inside one process, but a digest
written by the `record` run and checked by a later `profile` run reported **3 of 16 views as differing
with identical row counts**. It looked exactly like §7.2. It was the hash, and the mechanism is worth
recording because it is not the obvious one:

| hash | process-stable? |
|:--|:--|
| `Utf8String.GetHashCode` (XxHash3 over the bytes) | **yes** |
| `long` / `double` / `decimal` / `DateTime` / the record-struct temporal types | yes |
| `string.GetHashCode` | no (randomized by design) |
| `System.HashCode` (`Combine` / `Add`) | **no — seeded per process** |
| `Clast.DatabaseDecimal.Decimal128.GetHashCode` | **no** |

The three views that mismatched are **exactly the three whose output rows carry `Decimal128`**
(`IVM_DUMP_CELL_TYPES=1` prints the CLR types per view: no raw `string` appears anywhere — VARCHAR
arrives as `Utf8String`, which is stable). Cells are now hashed with `StableHash`, and all 16 views
match on every run below.

The larger fact this turned up: **`StructuralRow.ComputeHash` is built on `System.HashCode`, so every
row hash in the engine is process-randomized** — even a row of nothing but `long`s
(`HashDeterminismProbe` prints it; two runs disagree). The engine already owns a deterministic,
value-based hash **twice** — `StablePartitionHash`/`StableHash` (32-bit, for shard assignment, because
worker placement must survive a restore) and `HllHashing` (64-bit, for `APPROX_COUNT_DISTINCT`) — and
the row-equality path is the one that still delegates. Consequences worth knowing before any
file-backed work: a persisted hash, a persisted Bloom block, or any cross-process digest is
unavailable while row hashing is seeded, and `SpineBatch` rebuilds its Bloom filter from keys on every
load for exactly that reason.

### 11.2 Q1 — restore is not I/O-bound. It is boxing-bound

Four fresh-process restores of the same 4050.7 MiB snapshot (168 files, 30,464,849 rows,
3,539 columns of which 1,730 are VARCHAR), profiling on:

| stage | A | B | C | D | mean | share |
|:--|--:|--:|--:|--:|--:|--:|
| read (file I/O) | 631 | 847 | 695 | 513 | 671 ms | **2.5%** |
| decode (Arrow IPC framing) | 332 | 246 | 270 | 252 | 275 ms | 1.0% |
| **extract (Arrow → boxed `object?[]`)** | 9962 | 11899 | 12851 | 11413 | **11531 ms** | **43.0%** |
| materialize (row objects) | 3955 | 3807 | 4599 | 4235 | 4149 ms | 15.5% |
| index (hash rows into the Z-set) | 2023 | 2358 | 1663 | 1560 | 1901 ms | 7.1% |
| integrate (fold into the trace) | 975 | 866 | 2697 | 1605 | 1536 ms | 5.7% |
| rebuild (operator-specific state) | 7870 | 7419 | 5246 | 5547 | 6521 ms | 24.3% |
| unattributed | 236 | 237 | 230 | 217 | 230 ms | 0.9% |
| **total restore** | **25985** | **27679** | **28250** | **25342** | **26814 ms** | |

VARCHAR columns were timed separately in the two runs that carried that instrument: **6444 ms
(22.8%)** and **7159 ms (28.3%)** — over half the extract term, a quarter of the whole restore.

**Reading the bytes back costs 2.5%.** The premise that restore is dominated by "reading bytes" is
wrong in the way that matters: the bytes arrive in 0.7 s and then cost 25 s to become engine state.
`ArrowColumns.Extract` boxes every cell into an `object`, and for VARCHAR it runs
`Utf8String.Of(a.GetString(i))` — decode the Arrow UTF-8 into a UTF-16 `string`, then re-encode that
back to UTF-8 — two allocations and two transcodes per string cell. Restoring a 4.05 GiB snapshot
**allocates 42.3 GiB** (44.85 GiB with profiling on), and that figure reproduces to within 4 MiB across
runs, exactly as §25.1 of `design-row-representation.md` says allocation should.

This also settles something that looked odd before: arity-preserving dead-column elimination removed
~1 GiB of snapshot and was **wall-neutral**. Of course it was — it was cutting into the 2.5%.

The profiled path reads each file fully into memory before decoding (that is what makes the read leg
separable) and materializes rows into a temporary array; it costs ~20–25% more wall than the shipping
path (26.8 s profiled vs **21.9 s / 21.6 s** unprofiled on the same build, matching §9.1's 20–23 s).
Read the *shares*, not the totals.

### 11.3 Q2 — a resumed pipeline touches 0.0135% of what it restored

`replay` mode restores the snapshot (verified, 23.0 s), then drives batches 2 and 3 through the real
connectors with the access counters armed (128 ms, end state verified):

| | |
|:--|--:|
| restored keys across 118 non-empty trace collections | 8,338,691 |
| **distinct keys probed by the resumed ticks** | **1,124 — 0.0135%** |
| probes including repeats | 1,481 |
| full scans of a trace | 2, over one 1-key collection |
| keys in any scanned collection | 1 |

The largest collections — 982k, 912k, 886k, and seven of ~391k keys — were probed 0 to 75 times each.
**Nothing large was scanned.** So for the pause/resume goal this arc exists to serve, laziness would
not merely *defer* the deserialize work; on this workload it would **never do it**. That is the
strongest possible answer to the question the kickoff flagged as the one that could sink the arc.

Two honest limits on that number:

- It covers **trace-shaped** state only. `PartitionedWindowAggregateOp` / `PartitionedOffsetOp` keep a
  partition-keyed `SortedDictionary`, not a Z-set, and are not counted — they are the rebuild term,
  which is eager by construction (§11.6).
- The probe drives ingest, not output. `ProgramRunner.WriteOutputsAsync` writes **every output view's
  full contents** every batch, so a real ivm-bench resume scans all 16 output views on its first
  batch — that is exactly the `IntegrateOp` state (4.4–5.5% of restore). Laziness cannot skip those,
  and a deployment that emits full views on every tick has no lazy state at all downstream of the
  integral.

### 11.4 Two cheaper levers, measured before the architectural one

**(a) Restore is single-threaded, and the operators are independent.** `Snapshot.ReadAsync` walks
`SnapshottedIndices` in a `foreach`. There is no data dependency between operators, so
`DBSPNET_RESTORE_PARALLEL=<n>` (gated, measurement scaffolding) loads *n* at a time. Same snapshot,
same build, every leg digest-verified:

| degree | restore | vs sequential |
|--:|--:|--:|
| 1 (shipping) | 21.9 s / 21.6 s | — |
| 4 | 12.6 s | 1.73× |
| 6 | 15.1 s | 1.44× |
| 8 | 9.6 s / 14.7 s / 15.4 s (mean 13.2 s) | **1.64×** |
| 10 | 10.1 s | 2.15× |
| 14 | 17.1 s | 1.27× |

The spread at a fixed degree (9.6, 14.7, 15.4 s at 8) is wider than the difference between degrees,
and 14 threads is *worse* than 8 — restore allocates 42 GiB, so what is being scheduled is as much GC
as CPU, and the M4 Pro's 10 P + 4 E cores are not interchangeable. Treat this as **"roughly 1.4–2.3×,
best around 8, for a change that fits on one screen"**, not as a scaling curve. It is the only lever
measured here that needs no representation, format or substrate decision.

**(b) The VARCHAR round-trip is removable without laziness.** `ArrowColumns` already has a zero-copy
path (`ExtractStringAlias`) that aliases the Arrow buffer instead of transcoding; the snapshot codecs
do not use it. Wiring it into the profiled loader (retaining the `RecordBatch` so the buffers outlive
the state) and re-running, **digest-verified — aliased `Utf8String` values compare byte-identical to
transcoded ones**:

| stage | baseline (mean of 4) | zero-copy VARCHAR (mean of 3) | |
|:--|--:|--:|--:|
| extract | 11531 ms | **4864 ms** | −58% |
| of which VARCHAR | 6802 ms | 1717 ms | −75% |
| materialize | 4149 ms | 4477 ms | +8% |
| **index (hash into the Z-set)** | 1901 ms | **4795 ms** | **+152%** |
| integrate | 1536 ms | 1515 ms | — |
| rebuild | 6521 ms | 5622 ms | −14% |
| **total restore** | **26.8 s** (25.3–28.3) | **22.9 s** (21.6–24.2) | **−14.6%** |
| allocated | 44.85 GiB | **33.66 GiB** | **−25%** |

Two results, one of them a warning. The lever works: 11 GiB of allocation and 58% of the extract term
disappear, and the two sample ranges do not overlap, so the −14.6% wall is real rather than noise.
But **hashing got 2.5× more expensive**: an aliased `Utf8String` points into one large retained
buffer, so every hash and every comparison during the index pass is a scattered read, where a
freshly-transcoded value sits in newly-allocated, adjacent memory. Roughly 40% of the extract saving
is handed straight back at the index step. That is the same pattern §16.9 and the dead-column work
hit from the other side, and it is the single most useful caution this arc produces for any future
"keep it columnar / keep it in the file" design: **a representation that is cheaper to produce can be
more expensive to probe, and the probe side is where the engine already lives.**

### 11.5 Q3 — the flat family cannot be made lazy. Laziness is a storage-format change

The kickoff asked whether the flat family is compatible with file-backing. It is not, and the reason
is in the codec rather than in the trace:

- A flat operator's state is written as **one Arrow IPC batch per operator**, in **dictionary
  iteration order** (`ArrowZSetTraceCodec.SaveAsync` iterates the trace). It is unsorted, has no key
  index, and no per-key locator.
- A lazy probe therefore cannot find a key without scanning the whole file — the exact opposite of
  Feldera's 512-byte trailer plus Bloom block, which works because their batches are **sorted,
  immutable runs with an index**.
- Their laziness is not a property of their restore code. It is a property of their **on-disk batch
  format** plus a buffer cache, a shared batch store, and two-level GC (comparison §5).

So "lazy restore" on the flat family means: define a sorted, indexed, immutable on-disk batch format;
teach the trace to serve probes from it; add a page cache and a file-lifetime/GC discipline. That is
the LSM programme — i.e. the spine substrate, which `decision-trace-family.md` §4 decided to stop
growing, and which §9 row 3 of `comparison-feldera-decisions.md` says was measured unfairly (our
compaction ran on the step thread; theirs does not). **This arc does not settle that**; it does
establish that the lazy-restore question and the spine question are the *same* question, and that
whoever reopens it should run §9 row 3's experiment (compaction off the step thread) first, because a
file-backed flat family would be a second LSM implementation.

Note also what laziness costs on the other side: every steady-state probe that today hits a RAM
dictionary becomes a potential page fault plus a decode, on a workload whose per-row cost is already
the standing bottleneck (`repr-execution-apportionment.md`).

### 11.6 Q4 — the ceiling, and what to do

Take the shipping sequential restore, **21.9 s**, and apply the shares from §11.2:

| what a design can remove | share | 21.9 s becomes |
|:--|--:|--:|
| perfect laziness over trace-shaped state (read+decode+extract+materialize+index+integrate, minus the ~0.01% actually touched) | ~75% | **~5.5 s** |
| …plus §10's radix-tree re-expression, which removes the rebuild term too | ~99% | ~0.2 s |
| parallel restore alone, **measured**, degree 8 | — | **13.2 s** (9.6–15.4) |
| zero-copy VARCHAR alone, **measured** | — | **18.7 s** (−14.6%, allocation −25%) |
| both of the above together (not measured; they attack different terms) | — | ~11 s |
| eliminating file I/O entirely — the intuition this arc started from | 2.5% | 21.3 s |

Three conclusions, in the order they matter:

1. **The rebuild term is the part laziness cannot touch, and §10's rewrite is the part laziness needs.**
   §10 recommended deferring the radix-tree work until restore stopped being deserialize-bound. That
   was right, and it also means the two are **complements, not alternatives**: laziness caps out at
   ~75% precisely because `PartitionedWindowAggregateOp` + `PartitionedOffsetOp` + the aggregate cache
   rebuild is 25% of restore and is eager by construction. A lazy design that does not also
   re-express those operators' state buys 21.9 s → 5.5 s, not → 0.
2. **Parallel restore is the largest prize available today.** ~1.6× (1.4–2.3×) for a `Parallel.ForEachAsync`,
   no format change, no representation change, no new substrate, digest-verified at every degree
   tried. It should ship (as an opt-in `Snapshot.ReadAsync` degree, defaulting to something modest —
   the 14-thread regression says the degree needs to be chosen, not maximised). With zero-copy
   VARCHAR alongside it, resume of 4 GiB plausibly lands near ~11 s, and the remaining architectural
   prize is ~11 s → ~3 s.
3. **Do not build lazy/file-backed restore now.** Not because the ceiling is small — it is the largest
   one on the table — but because §11.5 shows the cost is a second LSM storage layer, and the arc that
   would have to authorise it (`decision-trace-family.md`) is decided the other way on evidence that
   §9 row 3 of the Feldera comparison already flags as needing a re-run. The honest sequence is:
   ship parallel restore → run the compaction-off-the-step-thread experiment → if spine wins that,
   lazy restore comes with it, and the radix-tree work becomes the completing half.

**What this arc retires.** "Restore is expensive because reading 4 GiB back is expensive" — no: 2.5%.
"Lazy restore is a restore-path change" — no: it is a storage-format change. "Restore is
deserialize-bound, so make deserialize cheaper" — half right: the cheap version of that (zero-copy
strings) removes a quarter of the allocation and a seventh of the wall — and gives 40% of it back at
the hash; the expensive version is the columnar row representation
— a different arc, with a different constituency (`design-row-representation.md`), which can now
claim restore as a second beneficiary, since extract + materialize is **58%** of one.

**Measurement caveats.** One workload; the operator mix sets every share. Restore wall varies ~15%
between legs and the parallel legs vary more than that. The profiled path is ~20–25% slower than the
shipping one and materializes an extra array per file — read the *shares*, not the totals. The
allocation figures reproduce to within 4 MiB across runs and are the more trustworthy instrument. The
zero-copy VARCHAR leg is a measurement path (it retains every `RecordBatch` deliberately), not a
shipping design; the buffers must outlive the state before it can become one.

### 11.7 The row hash is no longer seeded (built and measured, 2026-08-31)

§11.1's by-product — *every* row hash is process-randomized, because `StructuralRow.ComputeHash`
was built on `System.HashCode` — is fixed. `StructuralRowHash` (Core) is a deterministic 64-bit mix
over per-cell value seeds (four independent lanes, one rotate+multiply a cell, a SplitMix64
finalizer), folded to 32 for the `GetHashCode` contract. Only two cell types were ever the problem —
`string`, which Core hashes itself, and `Decimal128`, whose third-party `GetHashCode` is seeded but
which exposes a content-based `StableHash64()`. Every other type the SQL runtime puts in a row
already hashes by content, so it is left to answer for itself; `SqlCellHash` (Sql) exists to give the
typed compile path a non-boxing seed per column type that agrees with the boxed path by
construction.

**Three implementations had to move together**, and this is the part worth remembering: the boxed
walk in `StructuralRow.ComputeHash`, the Expression-tree delegate behind `StructuralRowShape`
(what a `TypedStructuralRow` carries), and the IL the emitted row struct carries. They must agree or
a typed key and a backing-array key stop finding each other in one dictionary — a silent miss, no
exception. **Nothing tested that**; `TypedRowHashAgreementTests` now does, across all three, and the
emitted struct — which previously omitted the arity and so never matched — agrees for the first time.

**Measured.** Two instruments. `w1profile` prices the change in situ (its B/event is the
deterministic one — §25.1 of `design-row-representation.md`); the new `rowhash` bench isolates the
term, invoking both arms through a `Func<>` so neither inlines.

| | HashCode (was) | first cut | **shipped** |
|:--|--:|--:|--:|
| `w1profile` B/event, all 10 queries | — | byte-identical | **byte-identical** |
| `w1profile` ns/event, median of 3×5-run samples per arm | — | 9/10 faster | **8/10 faster, −5.7% to −16.5%** (q2, q3 neutral) |
| `rowhash` wide row (7 cells incl. Utf8String, Date32, Decimal128) | 20.5 ns | 22.6 ns (+16%) | **19.7 ns (−3.9%)** |
| `rowhash` narrow row (2 longs) | 8.7 ns | 7.1 ns (−4%) | **5.0 ns (−42%)** |
| `rowhash` type-test chain, per SQL-typed cell | — | ~3.1 ns | **~1.4 ns** |

**Getting from the first cut to the shipped version is the instructive part**, because two of the
three things that looked expensive were not:

- **The registration hook was not the cost.** The first cut routed every type Core cannot name
  through a delegate the SQL layer installed, and doubled the dispatch cost. Deleting the delegate
  (Core takes the `Clast.DatabaseDecimal` package reference and names `Decimal128` directly, exactly
  as it already names `Clast.BloomFilter`) fixed *narrow* rows — dispatch went from +110% to −45% —
  and left wide rows **unchanged**. Only one cell type ever needed fixing; every other SQL type's own
  `GetHashCode` is already content-based, so it can answer for itself.
- **The cost was the `isinst` chain.** A boxed `Utf8String` or `Date32` fell through *thirteen*
  failed type tests before reaching its hash: measured at **12.5 ns per four-cell row, ~3.1 ns a
  cell**, against a virtual `GetHashCode` that costs ~2.4. Cutting the chain to the types that
  genuinely must be named — `long`, `double` (full 64-bit seeds for the common key types), `string`
  and `Decimal128` (the two that are seeded) — took it to ~1.4 ns. Everything else, `int`/`bool`/
  `float` included, now agrees with the fallback by definition, so it costs nothing to leave out.
- **A per-cell static write** (`_externalUsed = true`, guarding hook replacement) was on the hot
  path in the first cut. It is a shared cache line dirtied by every worker thread. Set-once
  semantics on the property give the same guarantee with no per-call write.

One bug worth naming: the typed overloads first forwarded to `StructuralRowHash.Opaque(object)`,
which **boxes** — on the one path that exists to avoid boxing. They now call `GetHashCode` on the
statically-typed value.

**What this unlocks** (none of it built): a persisted row hash — restore currently recomputes 30.5M
of them inside the 15.5% materialize term — a persisted Bloom block, which `SpineBatch` rebuilds from
keys on every load precisely because it could not trust a seeded hash, and cross-process digests that
do not need a separate `StableHash` path. **What it gives up**: `HashCode`'s per-process seed was our
only HashDoS protection, and the BCL's collision fallback covers `string`-keyed dictionaries only,
not custom key types like ours. Nothing in this engine's threat model needs it today; if it ever
does, the answer is a keyed hash with the seed persisted in the snapshot manifest, not a return to a
per-process one.

### 11.8 Parallel restore shipped (2026-08-31)

§11.6's conclusion 2 — "parallel restore is the largest prize available today" — is now the shipping
path, not an env-var experiment.

**API.** `Snapshot.ReadAsync(circuit, fs, parallelism, ct)` (and the local-directory overload) load
that many operators at once; the existing overloads use `Snapshot.DefaultRestoreParallelism`, which
defaults to **half the machine's processors, capped at 8**. `DBSPNET_RESTORE_PARALLEL` still
overrides it, and setting the property to 1 restores the sequential walk.

**Measured on the real SF=3 snapshot** (4050.7 MiB, same build, every leg digest-verified):

| degree | restore | allocated |
|--:|--:|--:|
| 1 (was) | 20.7 s / 20.6 s | 42.28 GiB |
| **7 (the default here, 14 cores)** | **10.1 s / 10.7 s** | 45.5 GiB |

**~2.0×, for a change that fits on one screen.** Allocation rises ~8% — concurrency is not free —
and peak memory rises with the degree, since that many operator files decode at once. That, plus the
measured degree-14 regression (§11.4), is why the default is capped rather than set to the processor
count.

**Why it is safe, which is the part that needed checking rather than asserting:**

- **Snapshotted operators own disjoint state.** The one place the compiler shares state between
  operators is arrangement CSE, and its guard already includes `ctx.SnapshotCodecs is null` — shared
  arrangements are disabled whenever snapshotting is on. (`IncrementalJoinSharedRightOp` is not
  `ISnapshotable` at all.)
- **The degree cannot change the result.** `ParallelRestoreTests` restores the same snapshot at
  degrees 1, 2, 4 and 16 and drives each restored circuit through the same continuation, asserting
  identical output — state is only observable through what it makes the circuit emit.
- **Failures surface unwrapped.** `Parallel.ForEachAsync` reports body exceptions in an
  `AggregateException`; callers catch `InvalidDataException` / `NotSupportedException` from
  `ReadAsync`. The concurrent path rethrows the first inner exception through
  `ExceptionDispatchInfo`, and a test corrupts an operator's trace file to prove the exception that
  comes out of a concurrent restore is not a wrapper.
- **`ProfileLoad` forces the sequential walk**, because per-operator timings taken from overlapping
  loads would not mean anything — and the §11.2 stage counters are not thread-safe. A test pins that.
- **Parallel circuits divide, not multiply.** `ParallelSnapshot.ReadAsync` already restores its `W`
  replicas concurrently, so each replica now gets `DefaultRestoreParallelism / W` rather than all of
  it: the product is what bounds how many operator files decode at once.

**Where this leaves resume latency.** 4 GiB of state comes back in ~10 s instead of ~21 s. The
remaining architectural prize (§11.6) is that ~10 s → ~2.5 s, and it still costs a storage-format
change plus the radix-tree rewrite. Nothing about that ordering changes; the cheap half is simply
taken now.
