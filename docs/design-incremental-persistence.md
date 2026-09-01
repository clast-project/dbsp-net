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

