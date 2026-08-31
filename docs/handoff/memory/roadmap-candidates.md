---
name: roadmap-candidates
description: "Identified follow-on feature gaps for DbspNet (windowing, approx/sketch aggregates, observability, operator fusion) with insertion points"
metadata: 
  node_type: memory
  type: project
  originSessionId: a6f13fdb-c199-418e-a78d-5b234a649cef
---

Candidate next features for DbspNet, surfaced while reviewing it against other
DBSP/streaming implementations and Feldera's surface. Recorded as roadmap, not
commitments — none started. The recursion gap from the same review is already
closed (see [[nested-circuit-fixpoint]]). Ranked by fit to the incremental-SQL
mission:

1. **First-class windowing + window aggregates.** ~~Highest-value next step.~~
   **Largely DONE** — see [[window-aggregates]]. `SUM/COUNT/AVG/MIN/MAX OVER
   (PARTITION BY … [ORDER BY … RANGE …])` ship for whole-partition / running /
   bounded RANGE frames (the bounded frame GCs against the existing
   clock/LATENESS watermark), plus `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`
   (positional). Still open: `ROWS`/`GROUPS` frames, the general rank-on-every-row
   form, and the typed/spine variants.

2. **Approximate / sketch aggregates.** `APPROX_COUNT_DISTINCT` (HyperLogLog) is
   **DONE** — see [[approx-count-distinct]]. Approximate quantiles
   (`APPROX_PERCENTILE`/`MEDIAN`/`PERCENTILE_CONT`/`PERCENTILE_DISC`, DDSketch)
   are **DONE** — see [[approx-quantiles]]. Still open: heavy-hitters (Count-Min)
   and the array-returning `APPROX_QUANTILES(x, n)`. All mergeable +
   bounded-state, so they drop into the existing `IAggregator<TValue,TOut>` seam
   (ARCHITECTURE "extension points") and compose with LATENESS GC. A real SQL
   surface Feldera supports.

3. ~~**Runtime observability.**~~ **DONE** — see [[runtime-observability]].
   `RootCircuit.CollectStats()` / `CompiledQuery.CollectStats()` report
   per-operator state size, last-tick output size, GC frontier and cumulative GC
   drops; `LastStepDuration` for throughput. The opt-in metrics hook over the
   `Operators` list, as envisioned here.

4. ~~**Circuit-level operator fusion.**~~ **DONE** — see [[operator-fusion]].
   A maximal run of consecutive `FilterPlan`/`ProjectPlan` nodes now lowers to a
   single fused `Apply` pass (new `LinearOperators.MapFilterRows`) on BOTH the
   structural and typed compile paths, eliminating the intermediate Z-set
   between adjacent pointwise stages. Done as compile-time lowering (not a new
   plan node), so no optimizer/walker churn.

Out of current scope (noted, not recommended now): key-sharded / parallel
circuit execution (DbspNet is deliberately single-circuit, single-node), and a
pluggable UDAF/UDF surface (scalar-function registry phase 5 is already deferred,
see [[scalar-function-registry-temporal]]).
