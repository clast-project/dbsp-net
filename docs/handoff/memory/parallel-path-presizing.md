---
name: parallel-path-presizing
description: "§16.8 pre-sizing left ~50 bare-ctor builder sites; the parallel ones (exchange gather, shard split, ingest) had EXACT sizes available and halved W>1 allocation"
metadata:
  type: project
---

**SHIPPED 2026-08-31, `ef82b33`.** §16.8 pre-sized 9 delta-builder sites and left ~50 on the bare
`new ZSetBuilder<..>()`. The valuable remainder was on the **parallel path**, where — unlike the
last-tick heuristic §16.8 used — the size is *known before the first insert*:

- `ExchangeOp` / `BroadcastExchangeOp` gather = sum of bucket counts. `ExchangeCoordinator.Read` is a
  plain array load, so a counting pass costs W loads.
- `ShardedOutputHandle.Current` — shards already materialised, total exact.
- `ShardedInputHandle.Push`, `ParallelIngestor` serial + parallel — ~delta/W per bucket.

**Deterministic allocation, two-exchange probe: −42% to −50%.** Nexmark W=14 over 7 alternating A/B
pairs (medians): **q4 +23.7%** (patched min > base median — and q4 is the arc's worst gap query),
q17 +13.7% (high variance), q22 −4.1% (within noise; patched holds the best single sample).

**Why this beat the earlier attempts:** §20 cross-tick pooling was thin (~7%) and needed a retention
guard; this needs none — capacity is a pure hint, fresh allocation each tick, no z⁻¹ hazard, and
W=1 is untouched because the sharded push short-circuits at one worker.

**How to apply:**
- When adding any per-tick builder, ask whether the count is knowable up front. On gather/split paths
  it usually is, and an exact hint beats §16.8's last-tick heuristic.
- The probe is exchange-heavy, so ~45% is this lever's **ceiling**, not its typical; an
  operator-heavy query sees less.
- **Measurement discipline on this Mac:** Nexmark W=14 swings ±40% between identical runs. Never
  claim a Nexmark delta from fewer than ~5 alternating pairs, and prefer the deterministic
  allocated-bytes probe. See [[range-shaped-dispatch-repriced]] for the same lesson.
- ~45 bare-ctor sites remain (mostly `BatchPlanEvaluator`, spine ops, nested/fixpoint) — unaudited,
  and most are not per-tick hot.

Related: [[repr-execution-apportionment]] (§16.8/§20 history), [[per-row-execution-efficiency]],
[[machine-migration-to-mac]].
