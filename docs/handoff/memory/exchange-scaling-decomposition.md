---
name: exchange-scaling-decomposition
description: "DONE (measure-first) — the parallel-scaling ceiling is barrier COORDINATION at fine ticks, not wide-row movement; step profiler shipped + ranked levers"
metadata: 
  node_type: memory
  type: project
  originSessionId: bb90cc4e-5814-4cad-944a-51eef664e07f
---

The exchange / parallel-scaling design session (the #1 lever after the row-rep
flat-path arc, see [[nexmark-feldera-w14-snapshot]] / [[surrogate-key-design]]).
**Design-first, measure-first, benchmark-gated** — same discipline as the row-rep
arc. Shipped to main 2026-06-10 (commit fbb4a7c, design doc §15).

## What shipped
- **`StepProfiler`** (`Core/Circuit/StepProfiler.cs`) — default-off internal seam
  (same mould as `SpineStagingConfig`/`FlatAggregateMode`; each worker writes its
  own `[worker]` slot during Step, controller reads after the end-of-tick barrier
  → no ThreadStatic needed, byte-identical when disabled). Instruments
  `ExchangeOp`/`ExchangeIndexOp` (split/wait/gather + row counts) and the
  `ParallelCircuit` step job (per-worker whole-Step raw ticks via new
  `RootCircuit.LastStepRawTicks`).
- **`stepprofile` benchmark** (`StepProfileBenchmark.cs`, `dotnet run -- stepprofile
  [events] [q18,q4,…] [W-sweep]`, report `docs/step-profile.md`). Decomposes the
  Step per-worker into **split / wait / gather / op**, plus **Ctrl** (controller's
  real per-step wall clock = Σ_tick max-worker step = what bounds throughput),
  **Strag** = Ctrl/mean-Step (barrier straggler tax), **Imbal** = max/mean busy
  (persistent skew). Reusable for any Nexmark query.

## THE finding — overturns the prior hypothesis
The going-in framing ([[parallel-pipeline-perf]]) was "CPU/bandwidth-bound
whole-row movement through the all-to-all." **Measurement (1M events, q18/q4/q19
W-sweep) says NO — the ceiling is COORDINATION, not movement:**
1. **Operators scale ~7–9× (Op↓) but the realised step only ~3.5–5× (Ctrl↓)**,
   saturating ~W=12–16. Operators are NOT the bottleneck.
2. **split+gather (incl. the one residual whole-row-rehash site, the gather) is
   only 5–22% of the step and SHRINKS with W** (q4 Move% 31→5% over W=4→24). Wide-
   row movement is cheap and gets cheaper per worker — the bandwidth hypothesis is
   refuted.
3. **The dominant non-scaling term is the barrier WAIT, rising with W** (q4
   exchange Wait% 6→14→19→36→36→**40%**). Coordination is the ceiling.
4. Multi-exchange q4 wears it as high exchange **Wait%**; single-exchange q18/q19
   wear it as **Strag→1.4** (controller done-barrier waiting on the slowest
   post-exchange worker). Same tax, different barrier.
5. **Persistent skew is modest (Imbal ≤1.4)** → per-tick *rotating* straggling +
   (W>16) E-core heterogeneity, NOT a hot worker → rehashing the partition won't help.

## Honest fundamental-vs-fixable
Lockstep **BSP/SPMD-replica** with a hard `Barrier` per exchange: barrier always
pays for the slowest worker, and as W grows each worker's 10k/W slice shrinks →
relative per-tick variance + barrier-latency fraction grow → idle grows with W
(exactly the data). **Work-stealing is blocked by the co-location invariant** (a
stateful op keeps a key's state on one fixed worker; rows can't migrate without
state). This is why DD/Timely/Feldera use async frontier progress, not lockstep
barriers — matching it is an execution-model rewrite, and even then the
slowest-worker-per-tick floor is partly intrinsic to synchronous incrementality.
**Hybrid-core note (BOTH boxes are hybrid — corrected 2026-06-10 by the user):**
the step-decomposition host is an i9-12900K hybrid (8 P/16-thread + 8 E); W>16
lands workers on slow E-cores = permanent stragglers, so W>16 cells mix
heterogeneity with the structural tax. **The Feldera comparison box is an Apple
M4 Pro — ALSO hybrid (14-core = ~10 P + 4 E).** So the E-core tax is in the real
comparison numbers too, NOT a local-only artifact → the profile is *more*
representative, not less. Two consequences: (1) it **sharpens lever 1** — the
comparison runs DbspNet at W=14 on 10P+4E (4 workers on E-cores), so the
prediction is **DbspNet at W≈10 may beat its own W=14** on q4/q18/q22 = a
concrete claw-back to test, not a homogeneous-box non-event; (2) it does NOT
explain the gap away — Feldera on the *same* hybrid box still wins, so the gap is
real (cheaper per-row work, or scheduling that tolerates heterogeneity better
than our static equal-shard BSP). The portable finding (ops 7–9× vs step 3.5–5×,
movement small) is machine-independent; M4 Pro's high memory bandwidth if
anything shrinks the movement term further.

## Ranked levers (design §15.5) — NO clean single big win
1. **Right-size W to fast-core count, don't oversubscribe** (cheap; knee ~W=12–16).
   Now a *testable comparison-box win*: M4 Pro is 10P+4E, comparison runs W=14 → 4
   E-core stragglers → test **DbspNet W≈10 vs W=14** on q4/q18/q22.
2. **Coarser ticks / larger batch** where latency allows (lowers relative variance).
3. **Coalesce co-partitioned exchange barriers** (q4's 2 join-input exchanges → 1
   rendezvous; targets the 40% wait directly) — **THE one gated prototype
   candidate / recommended next step.** Not built this session.
4. **Async/overlapped exchange** (drop hard Barrier for ready-flags = Timely model)
   — research-grade rewrite, bounded payoff. Investigate, don't commit.
5. **Narrow the gather** (append-list when rows distinct + no column-drop) — minor,
   measured small; column-pruning still blocked by MIN/MAX (q4) and wouldn't help.
6. Heterogeneity-aware shards / affinity — demote (machine-specific).

**Recommendation:** ship the profiler (durable), adopt the W-sizing default, treat
much of the residual q4/q18/q22 Feldera gap as a property of synchronous
fine-grained incrementality rather than a bug awaiting one fix.

## Lever #3 PROTOTYPED + GATED → HELD (negative result, design §15.7, commit 1818389)
Built `ExchangeIndexJoinOp` (fuse a join's two key exchanges into ONE barrier) +
`CompileOptions.CoalesceJoinExchange` (off by default) + `CircuitBuilder.ExchangeIndexJoin`
+ `exchangefuse` gate + correctness test (fused≡unfused≡single, W=1/2/4/8, inserts/
group-growth/retractions). **Gate (q4 1M runs=9): W=24 1.18×, W=16 0.92×, W=12 0.86×.**
The mechanism works (Wait% drops 15–40pp) but the step only improves in the
oversubscribed W=host regime (the one lever 1 says NOT to run) and **REGRESSES at
W≈P-core count** (q4 best = 521ms unfused @W=12, beats every fused cell). **Confirms
§15.4: the ceiling is the straggler BOUND (slowest worker's actual work), not
barrier COUNT** — fusing removes no work and concentrates both sides' skew into one
rendezvous, losing the two barriers' resync/skew-cancellation (two barriers can beat
one). **DECISION: HOLD, off by default, kept as documented negative result +
regression guard.** Net: **lever 1 (right-size W to fast cores) is the real win;
reducing barrier count is not.** Untouched: coarser ticks (lever 2), async non-BSP
exchange (lever 4) — but their bar is now "beat unfused at W≈P-cores," not at W=host.
Suite 1747 green.

## Lever #1 EXPERIMENT RAN → FALSIFIED competitively (design §15.8, commit 215fac0)
User ran W=10 vs W=14 on the comparison M4 Pro (10P+4E), both engines, full Nexmark.
**Result corrects my prediction AND a §15.4 error:**
- **Feldera UNIFORMLY faster at 10c than 14c (every query +2–38%)** = textbook
  synchronous-BSP straggler sensitivity; **DbspNet is W-INSENSITIVE (10≈14, mixed
  ±7–22%)**. So the head-to-head ratio moves **TOWARD Feldera at W=10** (q4
  0.66→0.49×, q0 0.86×, q22 0.68×) — Feldera gains more from shedding E-cores than
  we do. **Lever 1 is NOT a competitive lever** — only a per-machine
  absolute-throughput nicety for DbspNet; my "W≈10 claws back the gap" prediction
  is WRONG (10c is if anything a tougher test).
- **§15.4 ERROR FIXED: Feldera/DBSP is synchronous/clocked BSP, NOT async-frontier**
  (that's Timely/DD). Feldera's uniform straggler-sensitivity is the BSP
  fingerprint — it is MORE barrier-coupled than us, not less. So "match Feldera" is
  not an async-rewrite goal.
- **DbspNet's lower straggler-sensitivity cuts both ways:** real resilience on
  heterogeneous HW, but partly a SYMPTOM of slower per-row work (faster per-tuple →
  less work between barriers → bigger straggler fraction → Feldera hurt more by E-cores).

**THE REFRAME / where this arc ends:** scaling is coordination-bound (confirmed)
but W-sizing won't close the gap → the residual q4/q18/q19 gaps (0.46–0.55× at 10c)
are substantially **PER-ROW execution efficiency** (Feldera processes tuples
cheaper via columnar/vectorized batch ops, winning while paying a HEAVIER straggler
tax) → loops back to the **row-rep/columnar arc (§1–§14), NOT the exchange layer**.
**Exchange/scaling investigation CONCLUDED:** movement isn't the ceiling (§15.2),
barrier count isn't the lever (§15.7), W-sizing isn't competitive (§15.8). Durable
deliverables = the measurement infra (`stepprofile`/`exchangefuse`) + a correct map
of where the gap is NOT. Next competitive lever for q4/q18/q19 = per-row/columnar
execution efficiency (a different, partly-explored arc).
