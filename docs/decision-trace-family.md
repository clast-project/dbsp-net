# Decision: the flat / spine trace-family axis

**Status: DECIDED — stop growing spine. 2026-07-22. RE-MEASURED and upheld 2026-09-01 (§6), which
also closes the one experiment that could have overturned it on step cost.** Item 4 of `docs/design-layering-review.md` §8,
which required this be settled by measurement rather than architecture argument.

**Decision: keep spine as it is, do not complete the 8 missing operator siblings, and do not build
Track B — unless bounded memory becomes a stated requirement.** Flat remains the default. This also
retires stages 2 and 3 of `docs/design-durable-identity.md`, which existed to serve Track B.

## 1. The measurement

Real SF=3, ServerGC, i9-12900K, whole 50-view program, batches 1–3 (~200-row incremental deltas).
Re-run to confirm the arc's original single measurements; both reproduced.

| | flat | spine |
|---|--:|--:|
| bulk batch step | 58.3 s | 66.6 s (**+14%**) |
| incremental batch step | 232 / 192 ms | 247 / 269 ms (within run-to-run noise) |
| bulk save | 20.3 s | 30.5 s |
| incremental save | 18.9 / 18.7 s | 23.1 / 21.3 s |
| snapshot reuse | 12.7% | 70.6% of bytes spine-backed, **91.3%** of those unchanged |
| **projected save if unchanged bytes were skipped** | **15.7 / 16.3 s** | **4.8 / 6.3 s** |

**Correction to the layering review**, which cited "+16% step" without qualification: the step penalty
is **bulk-batch only**. On incremental batches the two families are indistinguishable. The review
overstated the ongoing tax.

So the case for spine is the last row: with reference-manifest snapshots (Track B), spine's
incremental save would be **~3× cheaper than flat's** (6.3 s vs 16.3 s), because flat can only skip
whole untouched operators (12.7%) while spine skips unchanged batches (91.3% of the spine-backed
70.6%).

## 2. Why that case does not survive Track A

Track B optimises the **per-batch** checkpoint. Track A — recommended by
`docs/design-incremental-persistence.md` §4, with A1 already landed — removes the per-batch
checkpoint entirely: WAL every batch, snapshot every N. §7.4 concluded that for this workload the
right setting is "snapshot once after the bulk load, then WAL the incremental batches indefinitely",
because replaying a small batch costs nothing measurable and recovery is dominated by the ~35 s
restore.

Amortising the save across the snapshot interval (output write, ~1.7 s/batch, for scale):

| snapshot every | flat save/batch | spine+TrackB save/batch | difference |
|--:|--:|--:|--:|
| 1 batch | 18.70 s | 6.30 s | 12.40 s |
| 5 batches | 3.74 s | 1.26 s | 2.48 s |
| 10 batches | 1.87 s | 0.63 s | **1.24 s** |
| 50 batches | 0.37 s | 0.13 s | 0.25 s |

**At any interval Track A would actually choose, the flat-vs-spine difference is at or below the cost
of writing the outputs.** Spine's checkpoint advantage is real and reproducible, and immaterial.

That is the whole argument. Track B was designed against a per-batch checkpoint that Track A deletes.

## 3. The other argument for spine — and why it is not available

Spine's genuinely unique capability is **out-of-memory state**: `SpineSpillConfig` can push deep
levels to disk. Flat traces have no spill path at all, so flat state is pinned in RAM. That is a
capability argument, not a performance one, and it is the only one Track A does not answer.

**But partial spine coverage cannot bound memory.** The operators with no spine sibling are 29.4% of
SF=3 state, and they include `IntegrateOp` — the materialised output views:

| flat-only operator | % of SF=3 state |
|---|--:|
| `IntegrateOp` | 11.2% |
| `PartitionedWindowAggregateOp` | 10.4% |
| `PartitionedOffsetOp` | 7.7% |
| `PartitionedRankOp` | ~0% |
| **total** | **29.4%** |

So with spill fully enabled today, roughly a third of state still cannot leave RAM, and total memory
is unbounded regardless. "Use spine for out-of-memory state" is unavailable at any effort short of
completing all 8 siblings — which is exactly the expensive work in question.

**Is bounded memory a live requirement?** Not demonstrably. SF=3 holds 4.0 GiB of state; ivm-bench CI
defaults to **SF=10**, so roughly 13 GiB as a first approximation — large, but within a benchmark
machine. This is a scope question rather than a measurement, and the decision below is conditional on
it.

## 4. What this decides

- **Do not complete the 8 missing spine siblings.** The checkpoint payoff is amortised away by Track
  A, and the memory payoff needs all 8 before it delivers anything.
- **Do not build Track B.** Its premise is gone. `docs/design-durable-identity.md` stages 2
  (stop-delete + mark-and-sweep) and 3 (reference manifests) are retired with it. Stage 1 (batch ids)
  is already built, is small, and is harmless to keep — it clarifies the model and costs nothing.
- **Do not retire flat either.** It is the default, it is faster on the bulk batch, and 8 operators
  have no alternative. Retiring it was never viable.
- **Keep spine as-is.** It works, it is tested, it is opt-in behind `CompileOptions.TraceFamily`, and
  it is the substrate if the memory requirement ever arrives.
- **Pursue Track A instead** — A2 (checkpoint policy on `ProgramRunner`) is the remaining work with a
  measured payoff: batches 2/3 go from ~90% checkpoint to a WAL append.

**Revisit if:** state stops fitting in RAM at the target scale factor (the one condition that flips
§3), or a deployment requires per-batch durability that a WAL cannot satisfy (which would restore
Track B's premise).

## 5. What this decision does not claim

- Not that spine is a bad design. Its reuse numbers are excellent — 91.3% of spine-backed bytes
  unchanged across a batch — and they are exactly what its LSM structure promises.
- Not that the duplication axis (§3 of the layering review) is resolved. Two trace families still
  exist, still cost ~10.8k LOC, and still diverge in capability. This decision stops that axis
  *growing*; it does not collapse it. Collapsing it would mean retiring one family, and neither is
  retirable today.
- Not a judgement on the typed/structural axis, which is item 5 and separately measurement-gated.

## 6. Re-measured (2026-09-01): the challenge to §1 does not survive its own experiment

`comparison-feldera-decisions.md` §9 row 3 called this decision "most exposed", on the grounds that
§1's "+14% bulk step" compared **LSM-with-in-step-compaction** against a dictionary while Feldera
merges on background threads, and asked for one experiment: *move our compaction off the step thread
and re-run the flat-vs-spine bulk step.*

That experiment cannot be run, because **there is no in-step compaction to move.**

### 6.1 What was measured

`tests/DbspNet.Tests/Scratch/SpineStepProbe.cs` runs the real ivm-bench SF=3 batch-1 program (the
same 50 views §1 used), timing **step separately from ingest** — ingest is identical work in both
families, so folding them together dilutes the ratio. `SpineCompactionProfile` (opt-in, gated on a
`static readonly` env read, so the merge path keeps its shipping shape when off) counts what
compaction costs on the step thread. M4 Pro, ServerGC, 20 ticks.

| config | step (median) | vs flat | batch builds | **merges** |
|:--|--:|--:|--:|--:|
| flat | ~40.5 s | — | — | — |
| spine, staging=0 | **~49.4 s** | **+22%** | 109 (9.7 s, **20.3% of step**) | **0** |
| spine, staging=100k | 43.2 / 48.7 / 53.3 s | inconclusive | 14 (2.4–3.6 s) | 0 |
| spine, staging=1M | 39.5 s | — | **0** | 0 |

**Every configuration produced identical output** — 3,002,826 rows across 16 views, digest
`-5565782370961525103` — so these are timings of the same answer, not of four different ones.

### 6.2 Why there are no merges

`TieredCompactionStrategy` merges when a level holds 4 batches. A spine trace only builds a batch on
a tick where its input delta is non-empty, and batch 1 delivers each source table **once** across its
20 ticks. The counters show **109 batch builds against ~106 spine-backed traces — 1.03 per trace.**
No level ever reaches 2 batches, let alone 4.

So the §1 measurement was never "LSM-with-in-step-compaction vs a dictionary". It was **sorted-batch
construction vs a dictionary**, and a background merger cannot touch it: a perfect one removes
**0.0%** of the spine step. The same reasoning applies to §1's incremental batches, which are smaller
still (9 ticks, ~200 rows) — inference, not measurement, but the mechanism is the same.

### 6.3 What the penalty actually is

Of the ~9 s gap at staging=0, **9.7 s is batch construction** — sorting each delta and building its
Bloom filter — at 20.3% of the step. The remainder is on the probe side (merge-view reads across
batches), which is what `design-row-representation.md` §8.3 found from the other direction.

**The memtable seam does not rescue it.** `SpineStagingConfig` exists precisely to amortise per-delta
batch building, and it does: staging=100k cuts builds 109 → 14 and build time to 2.4–3.6 s. But the
step time does not follow — its three samples straddle the staging=0 range. (An early single sample
at 43.2 s looked like a win; more samples killed it. Run-to-run variance on a ~45 s step is ±10%, and
that is the floor for any claim here.) At staging=1M the trace builds **no batches at all** — the
memtable never flushes, so the spine is not behaving as an LSM; its 39.5 s is a dictionary's number
wearing a spine's clothes, not a spine result.

### 6.4 What this changes

- **Nothing about the decision.** §4 stands, now on a re-measurement that reproduces §1 on a second
  machine (+22% here against +14% on the i9) and identifies the mechanism.
- **It closes the cheap path to reopening it.** The one experiment that could have overturned this
  on step-cost grounds has run and come back negative. A future case for spine has to rest on
  something else — bounded memory (§3), or the resume-latency argument in
  `design-incremental-persistence.md` §11.6, where file-backed batches are worth ~10 s → ~2.5 s.
- **It re-scopes §9 row 4** ("is our spine slow as a sorted-batch design or as our implementation of
  one?"). The answer for the bulk batch is neither: it is slow because a sorted batch per delta is
  *inherently* more work than a dictionary insert when each delta is written once and read a few
  times. That is a design property of LSM at this tick granularity, not an implementation defect.
