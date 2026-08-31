---
name: temporal-quantiles
description: "DONE: temporal-typed quantiles (DATE/TIMESTAMP exact + INTERVAL DDSketch), shipped on main"
metadata: 
  node_type: memory
  type: project
  originSessionId: 9e5e65f7-9ce5-4d62-885c-413caa8a003e
---

DONE (on main): the quantile family ([[approx-quantiles]] —
APPROX_PERCENTILE/MEDIAN/PERCENTILE_CONT/DISC) now also accepts **DATE,
TIMESTAMP, INTERVAL** args and returns that same type. **Hybrid** strategy
(user-chosen; TIME deliberately excluded):

- **DATE / TIMESTAMP → exact** via a new Core `OrderedQuantileSketch`
  (`DbspNet.Core/.../Aggregators/OrderedQuantileSketch.cs`, sibling of DdSketch):
  signed `SortedDictionary<long,long>` count per distinct day-/microsecond key,
  drop-at-zero ⇒ fully invertible, pure function of the multiset ⇒ exact
  incremental≡batch. `EstimateQuantile(q, discrete)`: discrete = ceil(q·N)-th
  member (true PERCENTILE_DISC); continuous = interpolate between floor/ceil rank
  neighbours, `Math.Round(.., AwayFromZero)` to a key. Reason for exact (not
  DDSketch): DDSketch's relative-error bound is on |value|, and an absolute
  timestamp/date is a huge epoch offset → ~200-day error at α=1%.
- **INTERVAL → DDSketch** (relative error is the right model for durations;
  bounded state). Folds the class component — year-month→`Months`,
  day-time→`Micros` (only the matching one is non-zero); reconstructs by class.

**Wiring (same switch sites as [[approx-quantiles]]):** `DdSketchSupport` is now
the strategy hub — `IsExactQuantileType`, `ExactToKey/ExactFromKey`,
`Interval{To,From}Double`, `NumericToDouble`, and the shared
`BuildStructuralPercentile(call)` used by BOTH `PlanToCircuit.BuildSqlAggregator`
(flat+spine) and `BatchPlanEvaluator` (they must agree for the PBT). Generalized
the DDSketch aggregator pair to capture `toDouble`/`fromDouble`/`ResultClrType`
(covers numeric→DOUBLE + INTERVAL→INTERVAL); added the exact aggregator pair
`Sql/TypedExactQuantileAggregator`. Resolver: `ComputeAggregateResultType`
accepts numeric→DOUBLE, DATE/TS/INTERVAL→arg type (nullable), else throws
"...numeric, DATE, TIMESTAMP, or INTERVAL"; added `bool Discrete` to
`AggregateCall` + the private `AggregateKey` (set from `percentile_disc`),
threaded via `ResolveAggregateArgs` returning a triple. `TypedAggregateResultType`
needed no change (uses `call.ResultType.WithNullable(true)`).

**Discrete honored only on the exact (DATE/TS) path** — numeric+INTERVAL DDSketch
is approximate either way, so DISC≡CONT there (unchanged from before). **INTERVAL
results run structural**: an INTERVAL agg slot makes TypedRowEmitter return null
→ whole typed compile falls back to structural (same as INTERVAL arithmetic).
DATE/TIMESTAMP run on typed+structural+spine.

Tests: `tests/.../Operators/Stateful/OrderedQuantileSketchTests.cs` (exact
disc/cont, even/odd, negatives, multiplicity, invertibility, determinism,
merge) + `tests/.../Sql/TemporalPercentileTests.cs` (DATE/TS exact incl.
cont-vs-disc-differ, midpoint interpolation, group by, nulls, NULL-result,
delete-shifts, DESC, exact incremental≡batch PBT on TIMESTAMP; INTERVAL day-time
latency + year-month within α; resolver result-type + TIME/string/bool reject).
Full suite 1503 pass / 1 skip. Deferred still: APPROX_QUANTILES(x,n) array form,
Arrow-persisted INTERVAL output columns, heavy-hitters (Count-Min, P2).
