---
name: runtime-observability
description: DONE — opt-in per-operator metrics (RootCircuit.CollectStats / CompiledQuery.CollectStats); OperatorStat + IIntrospectable
metadata: 
  node_type: memory
  type: project
  originSessionId: 39de43e2-49df-41ae-88e9-744de38480f4
---

Runtime observability shipped on `main` — roadmap-candidates #3 (see
[[roadmap-candidates]]). Opt-in, on-demand per-operator metrics so you can watch
trace state stay bounded as a LATENESS/clock watermark advances.

**API (all public, in `DbspNet.Core/Circuit/OperatorStat.cs`):**
- `readonly record struct OperatorStat(int Index, string Name, long RetainedRows,
  long LastOutputRows, long? GcFrontier, long GcDroppedTotal)` — `Index` is the
  operator's registration position (same stable id the persistence layer uses).
- `RootCircuit.CollectStats() : IReadOnlyList<OperatorStat>` walks `Operators`;
  `CompiledQuery.CollectStats()` delegates.
- `RootCircuit.LastStepDuration : TimeSpan` — last tick's operator-loop wall-clock
  (one `Stopwatch.GetTimestamp` pair per `Step`, near-zero overhead, always on).

**Mechanism:** internal `IIntrospectable` interface (MetricName / RetainedRows /
LastOutputRows / GcFrontier / GcDroppedTotal) implemented by each stateful op;
stateless linear ops don't implement it and are omitted. `CollectStats` is
on-demand (never on the `Step` hot path): O(1) per flat-trace op, O(state) for a
spine op (its KeyCount/GroupCount materialises). Helper `Metric.Frontier(IFrontier?)`
maps a frontier to nullable (long.MinValue→null). GC ops bump a `_gcDropped`
counter where they `DropKeysBelow` (ZSetTrace.DropKeysBelow returns int;
IndexedZSetTrace returns IReadOnlyList<TKey> → `.Count` or count-in-foreach).

**Instrumented ops (MetricName):** IncrementalAggregate, Distinct,
IncrementalInnerJoin / LeftJoin / FullJoin, TopK, PartitionedTopK,
WindowAggregate, WindowOffset, Lateness (RetainedRows=0; GcFrontier=the watermark
it advertises; GcDroppedTotal=late-row drops), TemporalFilter (GcFrontier=clock),
RecursiveCte (RetainedRows=materialised R, unbounded), and the 5 Spine* siblings
(Spine-prefixed names). The TYPED aggregate/join/distinct path reuses the same
generic ops closed over emitted structs, so typed queries report stats too.

Tests: `tests/DbspNet.Tests/Observability/OperatorMetricsTests.cs` — bounded
state under LATENESS GC (state=11, frontier=190, gcDropped=190 for COUNT(*) GROUP
BY ts over 0..200 with LATENESS 10), typed GROUP BY reporting, window-agg
appearing, index ordering + ToString. Full suite green (1410).

This is the [[window-aggregates]] redirect: a spine/typed variant for the
recompute-and-diff window ops was investigated and declined (spine N/A — no
Trace to swap; typed = large rewrite or regression-prone hybrid). Observability
was chosen instead.
