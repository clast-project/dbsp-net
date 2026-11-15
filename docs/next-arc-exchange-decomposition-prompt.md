# Starting prompt — next arc: what ivm-bench's exchanges do with their time (measure first)

> Kickoff written 2026-11-15, after the window-exchange arc closed at step 1
> (`docs/xview-census/README.md`: 0.6% of the step). This arc reuses its profiles. Paste the block
> below into a new session.

---

**Decompose where the TPC-DI program's exchanges spend their own work in ivm-bench batch 1 at
W=12, then decide whether one piece is worth building against. Measurement first; design only if
one piece earns it.**

**What is known (6 profiled runs, 2026-11-15, `99ecf1c`; batch 1, W=12, SF=3).**
- **Setup.** `~/tpcdi-ab/prof1.sh ARM NAME` (copy in `docs/xview-census/`) runs IvmMultiBatch the
  way the ivm-bench server does: incremental output, broadcast ≤ 1,000, size input order,
  `DOTNET_gcServer=1`, `DOTNET_gcConcurrent=0`, `DOTNET_GCDynamicAdaptationMode=0`. It also sets
  `DBSPNET_PROFILE=1 DBSPNET_PROFILE_TOP=100000`; the exchanges-by-wait table now lists every
  exchange. Profiles are in `~/tpcdi-ab/runs/p*.prof` (run 1 split out as `p0-b1.prof`).
- **The step.** About 13.0 s wall (median), 62.7 GiB allocated, GC pause about 6.5 s.
- **The exchanges' totals.** 138 exchanges = 39.2 s of worker CPU (summed over 12 workers),
  about 3.3 s of each worker's 13.1 s. Barrier wait is 16.7 s of it. The remaining ~22.5 s is the
  exchanges' own work. By kind (run 1, CPU ms summed over workers):

  | kind | n | CPU | wait | own work | alloc MiB |
  |---|--:|--:|--:|--:|--:|
  | `ExchangeIndexStepOp` | 62 | 24,066 | 6,792 | **17,274** | **7,339** |
  | `ExchangeIndexOp` | 11 | 4,013 | 1,701 | 2,312 | 1,592 |
  | `ExchangeStepOp` | 1 | 1,496 | 70 | 1,426 | 430 |
  | `ExchangeOp` | 50 | 8,488 | 7,049 | 1,439 | 736 |
  | `BroadcastExchangeOp` | 14 | 1,136 | 1,070 | 66 | 4 |

- **`ExchangeIndexStepOp` dominates.** 17.3 s of own work is about 1.4 s per worker, roughly 11%
  of the step. Its allocation is 17.8% of the batch's.
- **Concentrated sites** (run 1; own work = CPU − wait; rows = rows gathered):

  | op | view | own work ms | alloc MiB | rows |
  |--:|---|--:|--:|--:|
  | 334 | `fact_holdings` | 4,132 | 1,062 | 2,479,768 |
  | 230 | `watches_history` | 2,273 | 209 | 900,042 |
  | 329 | `fact_holdings` | 1,573 | 350 | 982,180 |
  | 169 (`ExchangeStepOp`) | `trades` | 1,426 | 430 | 982,180 |
  | 235 (`ExchangeIndexOp`) | `watches` | 1,002 | 778 | 1,106,447 |
  | 288 (`ExchangeIndexOp`) | `daily_market_pulse` | 821 | 360 | 1,282,768 |

  Op 334 alone runs about 1.7 µs of CPU per row, and op 230 about 2.5 µs. That is several times
  the ~350 ns/row the §22.8 gather bench measured for split + gather (`docs/exchange-gather-bench.md`).
  Find out what the extra is.
- **The exchange's "own work" is not just moving rows.**
  - `ExchangeIndexStepOp` **fuses the upstream map/filter** (`step`, design-state-representation
    §9.13). So its time includes the row rewrite that used to be an `ApplyOp`.
  - It also includes the partition hash, the split into W buckets, and the gather's
    `IndexedZSetBuilder` rebuild, which hashes the whole row.
  - Under Server GC, per-operator CPU also includes **GC pauses** that land inside the operator
    (memory `state-representation-arc` gotcha). Some of the 17 s is GC, not exchange work.
- **Already settled; don't redo.**
  - Barrier coalescing (`ExchangeIndexJoinOp`) is HELD off. It regresses at W ≈ P-cores
    (`docs/exchange-fuse-bench.md`, design-row-representation §15.7).
  - Wait is the straggler bound, not barrier count (memory `exchange-scaling-decomposition`).
  - Window-site exchange skipping is closed at 0.6% (`docs/xview-census/README.md`).
  - Cross-view partition passthrough is dead (0 of 138).

**Step 1: decompose (this is the whole arc unless one piece is big).**
- **Per-site phase split.**
  - `StepProfiler` (`IVM_STEP_PROFILE=1`) splits each worker's step into split / wait / gather,
    but it does not split per operator. Only wait is recorded per op (`WaitTicks`).
  - Add per-op **step (fused map) / partition+split / gather (index build)** ticks to the
    `ExchangeIndexStepOp` path. Keep the overhead zero when the profiler is off.
  - Report the split for the top ~10 sites and for the kind total.
- **Take GC out of the per-op numbers.** Pair the runs with `IVM_GC_EVENTS=1`, or check against
  allocation, which is deterministic and host-independent (memory `w1profile-alloc-instrument`).
  Rank by **allocation** and by ex-GC time, never by raw per-op CPU.
- **Explain ops 334 and 230.**
  - Find what each fuses: the view SQL and the plan node it lowers from.
  - Find the row width and the key.
  - Find whether the gather builds many tiny per-key Z-sets (memory `ivm-bench-multicore`: 62% of
    groups hold one value).
  - `IVM_ALLOC_TYPES=1` gives allocation by type.
- **What Feldera does instead.** Its exchange moves batches into one LSM trace with no hash map on
  the row path (memory `feldera-source-comparison`). Check its shard and gather code in
  `../feldera` before proposing anything.
- **Runs.** ≥ 5 runs (SF=3 step noise is about ±2.5 s, memory `state-representation-arc`). Start
  long loops from Terminal via `nohup` + `bash -c` (memory `bounded-memory-arc`). If a lone run is
  slow, check `pmset -g` (memory `mac-sleep-breaks-benchmarks`).

**Decision rule** (on the ex-GC numbers, against the batch-1 step wall):
- **No single piece (fused map, split, gather, or one site) ≳ 5% of the step, or ≳ 10% of
  allocation:** stop. Write the decomposition into `docs/ivm-bench-gap-analysis.md` and close.
- **One piece ≳ 10%:** go to step 2 on that piece only.
- **In between:** ask Curt.

**Step 2 (only on a go): design first, as a section of the relevant design doc.**
- **Possible shapes, depending on what step 1 finds. Not a plan.**
  - The gather appends to a sorted batch instead of rebuilding a hash index (Feldera's shape).
  - Ship the partition hash with the row so the receiver doesn't rehash it.
  - Narrow rows before they cross: the §22.8 width saving, on the structural path.
  - Fix a bad fused step at a specific site.
- **The bar.** Any change to what crosses an exchange must keep outputs identical, parallel ≡
  serial.
- **Snapshots.** If the change alters state layout, reshard must be correct and the plan
  fingerprint must change.
- **Acceptance.**
  - Parallel-vs-**serial** PBT arms (memory `parallel-view-in-circuit`: parallel-vs-parallel
    oracles pass while wrong).
  - An interleaved batch-1 W=12 A/B against the current pin, ≥ 5 runs per arm, rotating arm
    order, with both arms built in the same worktree path (`~/tpcdi-ab/wt`).
  - TPC-DI views identical (rows + digest).
  - Nexmark W=14 doesn't regress (memory `nexmark-regression-check-2026-11-13`).

**Landmines.**
- `Sql.dll` incremental builds are flaky. Clean rebuild (`--no-incremental`) before the full suite
  and before mutation checks. Check the dll mtime after building into `~/tpcdi-ab/bin-<sha>`.
- A mutant restored from a backup keeps an old mtime, so `touch` the file.
- The build path moves wall time 2–6%. Build A/B arms in the same worktree path.
- In profile mode IvmMultiBatch uses `RunBatchAsync`, so `drain_ms` there includes output writes.
  Compare against the profile's "engine step", not against `drain_ms` of a non-profile run.
- Label any heap or worker advice as sized for the 64 GiB Mac (memory
  `settings-sized-for-our-hardware`). Hand Curt any Docker commands.

**Working style.** Small commits on main. Run the full suite after a clean rebuild before each
commit. .NET 10 is in `~/.dotnet`.
