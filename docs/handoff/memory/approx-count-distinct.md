---
name: approx-count-distinct
description: "DONE: APPROX_COUNT_DISTINCT (HyperLogLog) shipped on main — first approximate/sketch aggregate, IAggregator seam"
metadata: 
  node_type: memory
  type: project
  originSessionId: c49d92fd-29ce-4814-ac34-288cb474f918
---

DONE (shipped to main): `APPROX_COUNT_DISTINCT(expr)` — the first approximate /
sketch aggregate, the #2 item from [[roadmap-candidates]]. Picked over windowing
because it's self-contained and incrementally cheap.

Design:
- `HyperLogLog` math lives in Core (`DbspNet.Core/Operators/Stateful/Aggregators/HyperLogLog.cs`):
  pure ulong-hash sketch, precision 12 (m=4096, ~4 KiB/group, ~1.6% std error;
  exact via linear counting below ~2.5·m). `AddHash`/`Clear`/`EstimateCardinality`.
- SQL value→ulong hashing + a generic `FoldPositive` helper in
  `DbspNet.Sql/Compiler/HllSupport.cs` (`HllHashing.Hash(object)` handles every
  runtime scalar type explicitly — VARCHAR via Utf8String bytes, never relies on
  randomized `string.GetHashCode`; SplitMix64 finalizer).
- Two SQL aggregators mirror the SUM/MIN-MAX pattern:
  `SqlApproxCountDistinctAggregator` (structural, in SqlAggregators.cs) and
  `TypedApproxCountDistinctAggregator<TIn>` (typed, in TypedSqlAggregators.cs).
  Typed path uses a **boxing extractor** `Func<TIn,object?>` (Convert→object; a
  no-value Nullable<T> boxes to null = SQL NULL) so one non-generic class covers
  every arg type. Both flow through Composite/TypedComposite → IAggregator, so
  the **spine path works for free**.

Incrementality: non-invertible (a register holds a max, can't un-set). Update is
incremental on **insert-only ticks** (merge delta into the running sketch);
**any retraction tick rebuilds** from the post-delta `afterMultiset` (always
provided by IncrementalAggregateOp). Because the sketch is a deterministic
function of the present value set, incremental ≡ batch **exactly** — so PBT-style
tests assert exact equality, not just tolerance.

Wiring checklist (the full set of AggregateKind switch sites — see
[[roadmap-candidates]] for why MIN/MAX-shaped): enum in LogicalPlan.cs; Resolver
(IsAggregateName / ToAggregateKind / ComputeAggregateResultType → BIGINT
non-null); PlanToCircuit.BuildSqlAggregator; BatchPlanEvaluator (its mirror
switch); TypedPlanCompiler (TypedAggregateResultType + BuildTypedAggregator);
**PlanOptimizer.NarrowAggregateInput must bail** (non-linear, like MIN/MAX —
narrowing can collapse cancelling rows and drop a visible distinct value).

Tests: `tests/.../Sql/ApproxCountDistinctTests.cs` (typed/structural/spine via
EmittedEqualityCodec to force structural; deletes, nulls, GROUP BY, VARCHAR,
large-cardinality bound, incremental≡batch over random ins/del) +
`tests/.../Operators/Stateful/HyperLogLogTests.cs`. Deferred: other sketch
aggregates (APPROX_QUANTILES/PERCENTILE, heavy-hitters) still P2.
