---
name: join-column-pruning
description: "§21 — projection pushdown through INNER joins (column-liveness); the cheap, unconditionally-sound term-2 lever that beat columnar; DEFAULT-ON; q4 −50% W=1 / 2.93–4.19× W=8"
metadata: 
  node_type: memory
  type: project
  originSessionId: 8d5a25a7-7b63-456d-b1a0-0aa28e92e2c8
---

DONE (LANDED + flipped **DEFAULT-ON**, gated, suite 1753 green with it on): **projection pushdown
through INNER joins** = the columnar arc (§17 #3) re-justified and pivoted, design-row-representation.md §21.

**Measure-first finding (reprbench `idx` mode, new):** after §18 narrowed q4's *aggregate*
term-2, the residual whole-row hashing is at the **join trace** — `IncrementalJoinOp` stores
full source rows in `IndexedZSet<joinKey, storedRow>` and hashes the whole stored row on
every `MergeInPlace` integrate (§14.2). The optimizer explicitly lacked this rule
(`PlanOptimizer.cs:38` "not yet applied: general top-down column liveness across joins").
The idx microbench (wide-stored vs narrow-stored inner row) priced it at **40–58% of the
join-trace per-row cost**.

**The pivot (an §18-shaped premise correction):** the 40–58% prize is captured by narrowing
the stored row to a still-flat `Dictionary` — i.e. by a **cheap optimizer rule (projection
pushdown through join), NOT columnar SoA**. Columnar could only chase the residual below the
narrow-dict line. So columnar (§17 #3) is **reframed/deferred** to the genuinely-wide residual
(q20-style wide *output* joins where pruning can't shrink rows; wide aggregates; retraction-heavy
CDC) — a much smaller prize than §17 assumed — not built speculatively against q4.

**Why strictly better than [[repr-execution-apportionment]]'s §18 narrowing:** join pruning is
**UNCONDITIONALLY sound** (ordinary relational projection pushdown — drops only columns no
consumer reads; sound for arbitrary signed Z-sets), so it can be default-on and the random-query
PBT runs with it ON (3000 iters, full ±1 surface). §18 was envelope-restricted (MIN/MAX
non-negativity) hence opt-in.

**What landed:** `JoinColumnPruningMode` ThreadStatic seam **DEFAULT-ON** (stores the INVERSE of
an opt-out flag because a `[ThreadStatic] bool` can't default to true — a field initializer
doesn't run per-thread; so "on" = zero/default; benchmarks set Enabled=false to A/B baseline) +
`PlanOptimizer.PruneJoinInputs` (fires at `Project(Join)` / `Aggregate(Join)`, INNER-only v1;
remaps equi-keys/residual/schema/parent-refs; idempotent). Production path = the documented
`PlanToCircuit.Compile(PlanOptimizer.Optimize(plan))` idiom (Compile alone does NOT optimize).
Tests `JoinColumnPruningTests` (full-±1 PBT + non-vacuous narrowing check). Gates:
`w1profile … prune` (W=1) + `q4prune` (W=8, output cross-checked).

**Gate results:** q4 **W=1 −50% time / −33% alloc**, **W=8 step 2.93× (batch10k) / 4.19×
(batch100k)** — the largest q4 win in the arc, on the worst single-core gap (q4 0.21× vs
Feldera). q3 (the 2.83× win) PRESERVED (unchanged time, GC 1/1/0→0/0/0). q20 −10% time (wide
output → alloc unchanged, correct). Non-join queries = clean noise control.

**Why:** the per-tuple gap vs Feldera is allocation + whole-row hashing of retained state; §18
took the aggregate's term-2, §21 takes the join trace's. Two cheap narrowing rules + pooling now
capture most of term-2 on Nexmark, deferring the XL columnar rewrite.

**How to apply:** DONE — flipped default-on. Next generalisation (deferred): the proper top-down
column-liveness pass (`PlanOptimizer.cs:38`) covering Filter/TOP-K/window parents and chained
joins, beyond the local Project/Aggregate(Join) patterns. Honest ceiling unchanged
([[per-row-execution-efficiency]]): narrow the 2–5× laggards toward ~1.3–2×, not parity. Honors
[[typed-compiler-reflection-gotcha]] (Optimize-time rewrite, no builder-signature change).
