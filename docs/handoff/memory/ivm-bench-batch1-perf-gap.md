---
name: ivm-bench-batch1-perf-gap
description: "ivm-bench SF=3 batch-1 (full historical load): Feldera ~7x faster than dbspnet on PROCESSING (feldera 20.957s vs dbspnet 147.6s); to investigate in a dedicated perf session"
metadata: 
  node_type: memory
  type: project
  originSessionId: 25fe3083-66ab-4999-996e-242a97a89703
  modified: 2026-07-20T22:47:12.095Z
---

**OBSERVATION (2026-07-20, from the SF=3 dbspnet-vs-feldera comparison run):** on **batch 1** —
the full TPC-DI historical load (batch_1_pct=100) — Feldera's PROCESSING time is **20.957s vs
dbspnet's 147.6s ≈ 7:1** (compile excluded on both; feldera's ~295s Rust compile is a separate
one-time cost). This surfaced only after the [[ivm-bench-validation-findings]] correctness work
closed out; the benchmark's headline had dbspnet "faster" on batch 1 only because the harness was
folding feldera's compile into its batch duration — FIXED separately (ivm-bench branch
`fix-feldera-batch-timing-compile`, feeds the measured resume→drain duration into batch.duration_s).
With that fix, feldera is correctly ~7x faster on the bulk load.

**STRONG PRIOR (not yet profiled): this is the SAME gap [[per-row-execution-efficiency]] already
characterized, on a new workload — NOT a novel bug.** Batch-1 bulk load = one big pass through
dbspnet's incremental circuit = the single-core per-tuple regime where dbspnet trails Feldera
(measured 0.2–0.35× on Nexmark q4/q18/q19), engine is ALLOCATION-bound (~2–5 KB/event, fresh
dict-backed Z-sets per op per tick), and dbspnet's multi-core wins come from OUT-SCALING not per-row
speed. TPC-DI batch-1 is the worst case: wide 17-col SCD-2 rows, temporal joins, less parallel
scaling to hide the per-row cost. Plus the raw .NET-vs-Rust runtime floor.

**SESSION 2 (2026-07-20) — INSTRUMENTATION BUILT + PUSHED (dbsp-net `76b9ed2`, on main), awaiting
Curt's profiled Docker run.** No local TPC-DI data on the Windows side (lives only in WSL mount/),
and the Nexmark `stepprofile`/`w1profile` tools don't touch the TPC-DI DAG — so localization has to
happen on the real Docker batch-1. Added **env-gated batch profiling** (`DBSPNET_PROFILE=1`), zero-cost
+ byte-identical path when off:
- `RootCircuit.ProfileOperators`/`CollectOperatorProfile()`/`ResetOperatorProfile()` — per-operator
  cumulative Step time + state/output rows (new `OperatorProfile` record).
- `ProgramRunner.RunBatchAsync` — splits the batch into **read+decode / push(obj[]) / engine-step /
  output-materialize / output-write** (per source & per output) + allocated GiB + GC gen0/1/2 +
  ServerGC/procs + top-30 operators by cumulative step time. Prints to stderr AND (teardown-proof)
  appends to `DBSPNET_PROFILE_FILE`. ivm-bench compose edited (dbspnet-server `environment:` block,
  default off) — NOT committed on his branch.
- **Two STRUCTURAL facts confirmed from code (matter regardless of the profile):** (1) the server's
  program `Step()` is a plain single-threaded `foreach` over operators (structural `CompileProgram`,
  no Exchange) → **`DBSPNET_CPUS` does NOT parallelize engine compute**; and .NET clamps
  `ProcessorCount` to the ~24 physical logical CPUs anyway, so `cpus:32` vs 24 barely changes ServerGC
  heap count (Curt's "32 counterproductive" instinct is right in spirit but the effect is small —
  it's a cheap A/B, not the lever). (2) `RunBatchAsync` uses the **serial `DrainAsync`** (NOT
  `DrainPipelinedAsync`) → read→decode→push→step→write are fully sequential, **no core overlap** →
  extra CPUs mostly just feed background GC. **If the profile shows read+decode or output-write is a
  big share, switching the batch path to the existing `DrainPipelinedAsync` (overlaps decode+compute+
  write-behind) is a real multi-core lever that the serial path leaves on the table.**

**PROFILE RAN (2026-07-20) — ROOT CAUSE FOUND, prior was INCOMPLETE.** Batch-1 profile (SF=3,
`dbspnet-only.json`, commit 76b9ed2):
- **batch-1 = 139s wall, 94.3% ENGINE STEP** (I/O negligible: input decode 0.6%, push 3.1%, output
  mat+write 2.1%) → the gap is NOT I/O; the [[per-row-execution-efficiency]] engine-floor framing was
  directionally right but the SPECIFIC cause is sharper than "generic per-row alloc." **176.9 GiB
  allocated** in the batch.
- **~40% of the whole batch = TWO `WindowAggregate` ops (318=29.6s/22.5%, 320=22.3s/17%) + a
  `WindowOffset`, all state=1,282,768** (= staging_daily_market rowcount). These are the **cumulative
  MIN/MAX running-frame windows in `silver/daily_market`**: `min(dm_low)/max(dm_high) OVER (PARTITION
  BY dm_s_symb ORDER BY dm_date)` (UNBOUNDED PRECEDING→CURRENT ROW), plus market_volatility's LAG/LEAD.
- **ROOT CAUSE = O(partition²) running-frame recompute in `PartitionedWindowAggregateOp.RecomputePartition`**
  (`src/DbspNet.Core/Operators/Stateful/PartitionedWindowAggregateOp.cs`, non-whole branch line ~267):
  on a bulk insert the affected `candidates` = the WHOLE partition (N rows), and for EACH row it calls
  `_aggregator.Compute(FrameFor(rows, v))` where `FrameFor` (line ~290) rebuilds a fresh List+ZSet of
  the entire prefix (all rows ≤ v). So Σ O(i) = **O(N²) time AND O(N²) allocation per partition**,
  re-aggregating every row's whole prefix from scratch instead of one linear pass with a running
  accumulator. Explains all 4 profile facts (slow batch-1 / 176 GiB / batch-2&3 engine ~2% because tiny
  deltas = tiny suffix / Feldera 7× because it does the linear pass).
- **BATCH 2/3 SEPARATE FINDING (not the gap, but recorded):** engine step only ~2%; **~97% is
  full-state output re-materialize (`ToArrowView`) + Delta write** (fact_holdings 912K + dim_trade 982K
  rows dominate). That's ivm-bench's mandated truncate-output work (Feldera pays it too) — NOT an engine
  problem. So the incremental-batch wins are real and the residual cost there is output I/O, not compute.

**FIX SHIPPED (2026-07-20, dbsp-net `62d06c0`, on main; ivm-bench default commit bumped to it).**
Rewrote the running-frame branch of `PartitionedWindowAggregateOp` → new `RecomputeRunningRange`: one
ordered pass (ascending ASC / descending DESC) folding each row into a running aggregate via the
already-incremental `IAggregator.Update` (threading per-partition state), with a dict-backed
`GrowingMultiset` giving O(1) Add/SumWeights/WeightOf (the fields `CompositeAggregator.Update` +
`SqlMinMaxAggregator.Update` actually read). Peer groups (equal order value) folded before emit →
RANGE semantics preserved. **No aggregator changes — MIN/MAX `Update` was already incremental; the op
just never used it (it always did `Compute(FrameFor(...))` = rebuild+rescan each prefix).** Whole /
bounded frames untouched. **RE-PROFILE CONFIRMED THE WIN (2026-07-20, at 62d06c0):** batch wall **139.4s→98.0s (−30%,
−41s)**; allocated **176.9→68.1 GiB (−61%)**; GC gen0 **2570→96**; **op 318 29.6s→4.0s (−86%), op 320
22.3s→2.6s (−88%)** — the two window ops collapsed exactly as predicted (52s→6.6s combined). Ratio vs
Feldera's ~21s: 7:1 → **~4.7:1**. (Curt's "not much difference" = he's measuring vs Feldera's 21s, not
vs the prior dbspnet run.) **NEW top of engine step (now 91%): the IncrementalInnerJoin chain — op 235
13.0s/14.6% (state=7272!), 277 6.5s, 282 5.6s, 352 3.2s = ~28s across 4 joins — plus IncrementalAggregate
op 270 3.1s (state=885922).** op 235 having tiny state (7272) but 13s = high row VOLUME / low-cardinality
fan-out, not state size; worth checking for another algorithmic issue vs inherent fact-join volume. Added
a **self-stamping commit line** to the profile header (reads `DBSPNET_COMMIT`, baked via Dockerfile ARG→ENV;
dbsp-net `ebd2dba`, ivm-bench default bumped) so "which build?" is never ambiguous again — the op-timing
fingerprint was the only proof this round.
Also verified: local single-partition scaling ~3.8µs/row FLAT across N (was
O(n²)); full suite 2018 green; extended the window differential PBT (incremental≡batch, random
inserts+deletes, ts-ties) with running MIN/MAX ASC+DESC + MIN&MAX-together (the daily_market shape +
retraction-rescan path the prior cases missed). **AWAITING Curt's re-profile at 62d06c0** — expect ops
318/320 to collapse from ~52s→~single-digit s and batch-1 wall + the 176 GiB alloc to drop sharply
(ratio 7:1 → maybe ~4-5:1). Re-run: same `dbspnet-only.json`, `DBSPNET_PROFILE` still hardcoded on;
`rm mount/results/3/dbspnet/batch-profile.txt` first. **Next tier after this (from the same profile):
the `IncrementalInnerJoin` chain (235=11.7s/8.9%, 277=6.4s, 282=4.8s...) + the residual ~176 GiB
baseline alloc across the DAG.** [[per-row-execution-efficiency]] framing (allocation-bound) holds but
this was an ALGORITHMIC O(n²), not the generic per-row floor — measure-first paid off big.

**LOCAL DOCKER-FREE LOOP + LABELED PROFILE (2026-07-20, dbsp-net `3e04fb8`).** Curt regenerated the
SF=3 data standalone (datagen + batch-loader init, NOT the OAT runner which wipes raw) and copied it to
`D:\ivm-data\raw\3\delta`. New `tests/.../Scratch/IvmBatchProfile.cs` (env-gated: IVM_DATA_ROOT/IVM_SPEC)
runs the EXACT batch-1 program locally from those tables — **faithful (68.13 vs Docker 68.14 GiB, identical
output rowcounts), ~90s, rebuild-in-seconds.** Spec generated once via `python dbt_to_program.py <dbspnet
project>` → scratchpad/ivm_spec.json. Plus **per-operator VIEW LABELS** (RootCircuit.CurrentBuildLabel /
CircuitBuilder.BuildLabel, tagged in AddOperator; CompileProgram sets it per view) → the profile now names
each op's view. **This is the inner loop for all further batch-1 work — use it, not Docker.**

**LABELED batch-1 ranking (local, 83s) — the join tier named:**
- **#1 `broker_performance` op 235 IncrementalInnerJoin 9.9s (13.3%) + op 236 Apply 1.8s ≈ 15% of the
  batch, for a 23-ROW output view.** ALGORITHMIC BLOW-UP (next clean-win candidate): its correlated
  `NOT EXISTS (SELECT 1 FROM fact_trade ft JOIN fact_cash_transactions fct ON ft.sk_account_id=
  fct.sk_account_id WHERE ft.sk_broker_id=bt.sk_broker_id)` compiles the inner join as a FULL
  many-to-many product per account (~36 trades × ~64 cash-txns × ~3600 accts ≈ 8M intermediate rows) just
  to answer a boolean. A SEMI-JOIN ("does the account have any cash txn") avoids the product. **Fix lives
  in the SQL compiler (EXISTS/NOT EXISTS body → semi/anti-join) — more delicate than the window op fix;
  needs care re correctness.** NOT yet investigated at the plan level.
- Rest looks like INHERENT fact volume, not bugs: `fact_watches` joins 282+277 ≈ 8.7s (456K output),
  `fact_holdings` joins 352+346 (912K output), `watches` IncrementalAggregate 270 3.0s (state=885922),
  `daily_market` windows 318/320 (residual after the O(n) fix, now ~5.5s), `trades`/`market_volatility`
  windows. Long tail of per-view ApplyOp (map/filter/project) ~1s each.
- **Honest ceiling:** even a perfect broker_performance fix (~−10s → ~73s) leaves ~3.4:1 vs Feldera's
  21s; the residual is fact-table join/agg volume + the 68 GiB managed-allocation floor
  ([[per-row-execution-efficiency]], [[row-representation-design]]) = the hard columnar/row-rep territory,
  ceiling ~1.3–2× not parity. broker_performance is likely the LAST clean algorithmic win.

**BIG WIN — PROGRAM PATH WAS UN-OPTIMIZED (2026-07-20, dbsp-net `6a8a9e3`, ivm-bench pin bumped).**
Chasing broker_performance's op 235 uncovered that **`CompileProgram` never called `PlanOptimizer.Optimize`
— the ENTIRE 50-view program compiled un-optimized** (no column pruning, no filter pushdown, nothing),
AND even after enabling per-view optimize, CTE bodies (`CteScanPlan` leaves) were still skipped — so the
bulk of each WITH-heavy view's logic (incl. broker_performance's NOT EXISTS) never saw the optimizer.
Fix = optimize each view's plan AND each CTE body at compile (Optimize is a pass-through on recursive
plans → recursive CTE bodies intact). PLUS new **`NarrowSemiJoinSubquery`** rule: a semi/anti-join reads
its Subquery as a SET of equi-key cols, so `Project`-over-inner-`Join` where kept projections are all
LEFT-only → inner join becomes a semi-join (kills the product). **Result (local harness, vs the
window-fix baseline): 83.1s→60.1s (−28%), 68.1→44.7 GiB alloc (−34%); broker_performance's 9.9s join
GONE; fact_watches joins also collapsed (broad pruning); ALL 16 output row counts identical to baseline.**
Full suite 2023 green; `SemiJoinNarrowingTests` (rule fires + optimized≡unoptimized≡batch PBT).
Docker-equiv est ~98s→~71s → ratio ~4.7:1 → **~3.4:1** vs Feldera 21s. **Awaiting Curt's Docker
head-to-head re-run at 6a8a9e3.** Correctness: row-COUNTS match across all 16 real outputs +
full suite + PBT. A full VALUE-diff vs Feldera (`PRESERVE_RESULTS=1` + `src/.scripts/compare_outputs.py`)
is available and NOT blocked — the old "engineered-wood writer blocks external reads" note was STALE
(the `OmitPathInSchema=false` fix in DeltaOutputConnector.cs:75-87 resolved it; the SF=3 comparison
already RAN, per [[ivm-bench-validation-findings]]). Only residual is a narrower engineered-wood snappy
READ bug on some table(s), not a general block. So a value-diff is a reasonable belt-and-suspenders
next check.

**NEW TOP after this (local, 60s) — mostly INHERENT volume now:** daily_market windows (348/350 ~5.8s,
residual after the O(n) fix), watches IncrementalAggregate 292 (state 885922), fact_holdings joins
(388/381), market_volatility WindowOffset 398, per-view ApplyOp tail. These look like real fact/window
volume, not bugs. **Calcite follow-up (Curt's Q):** this class (missing semi-join/decorrelation rules)
is exactly what Feldera gets free from its Calcite SQL frontend; recommend mining Calcite's RULE CATALOG
as a checklist (SubQueryRemove, SemiJoin family, project/agg/filter transpose, column pruning) but NOT
adopting the framework (doesn't model DBSP incrementalizability/GC/retraction). Also worth: audit which
other optimizer rules only ran on single-query and now newly apply program-wide.

--- superseded plan (kept for context) ---
**THE LEVER (next session): rewrite the running-frame branch of `PartitionedWindowAggregateOp` to a
single O(N) pass per affected partition** — scan the already-sorted `rows` once maintaining a running
aggregate accumulator, emit each row's value as it goes, instead of per-row `FrameFor`+`Compute`.
Needs the aggregator to fold one row at a time (MIN/MAX/SUM/COUNT/AVG all support it). DESC running =
suffix scan. Whole-partition + bounded frames unaffected (bounded already value-range-limited; its
per-row `FrameFor` is a separate, smaller concern). Guard: window-aggregate PBT/oracle exists
([[window-aggregates]]); prove incremental≡batch across running/bounded/whole. Expected: turn ~52s of
O(N²) window work into ~linear → should close a large fraction of the 7:1. Structural path only is
enough for the benchmark (server uses CompileProgram); typed `CompileWindowAggregate` shares the op so
it benefits too. **Instrumentation shipped (76b9ed2): keep it — re-run the same profile after the fix to
confirm ops 318/320 collapse.**

**ORIGINAL RECOMMENDED (do in order):**
1. LOCALIZE before optimizing — profile dbspnet batch-1 with existing tooling (`stepprofile`/
   `w1profile`/`profile` alloc ceiling): allocation vs a specific op (SCD-2 temporal joins + wide-row
   aggregates are prime suspects) vs no-parallelism-on-this-shape vs runtime floor.
2. CHECK STRATEGIC VALUE — for an IVM benchmark the INCREMENTAL batches (2/3) are the point; batch 1
   is one-time (feldera pays ~21s too). With the timing fix, compare batches 2/3 honestly first — if
   dbspnet is competitive there, batch-1 matters less. (Still a real weakness worth understanding.)
3. HONEST TARGET — known levers (pooling, input/row narrowing, the shipped flat lazy merge-view) narrow
   to ~1.3–2×, not parity; full closure needs the deferred columnar/off-heap rearchitecture
   ([[row-representation-design]]). Realistic aim: 7:1 → ~2–3:1; columnar = the bigger swing if judged
   worth it. Related: [[ivm-bench-arc]], [[exchange-scaling-decomposition]], [[repr-execution-apportionment]].
