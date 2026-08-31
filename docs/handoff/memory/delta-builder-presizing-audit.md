---
name: delta-builder-presizing-audit
description: "Full audit of remaining bare ZSetBuilder ctors — what got sized, and the reasoned list of what was deliberately left alone"
metadata:
  type: project
---

**DONE 2026-08-31, `b3d5351`** (follows [[parallel-path-presizing]]). §16.8 sized 9 sites; this
audited every remaining bare `new ZSetBuilder<..>()` in `src/`.

**Sized — exact count already in hand:** `ArrowZSetTraceCodec` (rowCount), `TypedTraceCodecAdapters`
(both directions), `CompiledQuery.Push` / `ParallelStructuralCompiledQuery.Push`+`PushSingle` and the
three `ZSet.FromEntries`/`FromKeys` factories (`TryGetNonEnumeratedCount`, else grow as before),
`IndexedZSet.Flatten` (sum of group counts), `TopKOp` / `TemporalFilterOp` (bounded by the two windows
being diffed).

**Sized — §16.8 last-output pattern:** `IncrementalLeftJoinOp`, `IncrementalFullJoinOp`,
`IncrementalJoinSharedRightOp` (**§16.8 gave `IncrementalJoinOp` a `_lastOutputSize` and missed all
three siblings**), `DistinctOp`, `PartitionedWindowAggregateOp`, `PartitionedOffsetOp`,
`LatenessOperator`. The outer joins also pre-size their per-tick `touched` HashSet.

**Deliberately left, with reasons — don't re-audit these:**
- `BatchPlanEvaluator` (14) — batch/oracle path, not per-tick.
- Spine ops (10) — not the default trace family; `docs/decision-trace-family.md` says stop growing spine.
- `SemiNaiveFixpointOperator` (4), `NestedScopeBuilder` (2) — recursion-only, per-iteration; **not yet
  examined for a size hint**, the one real remaining gap.
- `IndexedZSetBuilder`'s inner per-group builder — **genuinely unsizable** (groups grow one entry at a
  time). This is precisely the inner multiset the columnar end-state targets.
- The `DeltaPoolMode` builders — allocated once and reused, not per-tick.
- `PartitionedWindowAggregateOp` restore path — one-shot.

**Honest gate:** individually below this harness's resolution; shipped because the pattern is proven
and capacity is a pure hint (built Z-set unchanged, 2317 green). Measured: no regression, q2 W=1 up in
both verified pairs. Two apparent q19 regressions dissolved at six pairs — **q19's plan contains none
of the changed operators**, which is the check to run first when a query looks off: `dotnet run --
nexmarkplan q<N>` dumps its operator list.
