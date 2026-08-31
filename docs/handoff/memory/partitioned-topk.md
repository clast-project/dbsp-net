---
name: partitioned-topk
description: "DONE — windowed ROW_NUMBER/RANK/DENSE_RANK as partitioned incremental TOP-K, both compiler paths"
metadata: 
  node_type: memory
  type: project
  originSessionId: 00b5528e-e711-47c8-b2e6-d7b98b9759e0
---

Partitioned/windowed `ROW_NUMBER` / `RANK` / `DENSE_RANK` shipped as incremental
partitioned TOP-K (the [[orderby-limit-topk]] follow-on). Implemented on `main`;
full suite green (1166 pass). Docs updated (`docs/skipped.md`, `README.md`,
`ARCHITECTURE.md`).

**Surface (the only supported shape):** standard derived-table + outer filter —
`SELECT … FROM (SELECT …, {ROW_NUMBER|RANK|DENSE_RANK}() OVER (PARTITION BY p
ORDER BY o) AS rn FROM …) s WHERE rn <= k` (also `< k` ⇒ limit k-1, and reversed
`k >= rn`). Derived-table alias is required (parser rule). Chose this because
standard SQL evaluates window functions *after* WHERE, so the alias can't be
referenced in the same query — the subquery spelling is the portable one.

**Shape of the implementation:**
- Parser: new `WindowFunctionExpression(name, WindowSpec(partitionBy, orderBy))`
  AST node; `OVER` / `PARTITION` are CONTEXTUAL keywords (matched by text via
  `IsContextualKeyword`, not reserved — avoids corpus collisions). Parsed in
  `ParsePrimary` after a function call's `)`.
- Plan: `PartitionedTopKPlan(Input, PartitionKeys, SortKeys, RankFunction, Limit)`,
  schema = Input.Schema (rows filtered, never widened). `RankFunction` enum lives
  in **Core** (`DbspNet.Core.Operators.Stateful`) and is reused by the SQL plan
  node — one enum, no duplication.
- Resolver: `TryResolvePartitionedTopK` pre-pass at the TOP of `ResolveSelect`
  (before `ResolveFrom`). Recognises the shape structurally (there is NO general
  window plan node), strips the window item, lifts PARTITION BY + ORDER BY exprs
  as hidden columns (reuses `ResolveWithHiddenOrderColumns`), builds the plan +
  strip projection, then RE-ENTERS `ResolveSelect` for the outer query's
  remaining clauses via a `PreResolvedRelation(LogicalPlan) : FromClause`
  internal node (new `ResolveFrom` arm). This reuses all existing
  projection/WHERE/GROUP BY/DISTINCT machinery for the outer query.
- Operator: `PartitionedTopKOp<TRow,TKey>` (`Core/Operators/Stateful/`) — per-
  partition `SortedDictionary` + last window, recomputes only touched partitions.
  ROW_NUMBER = positional cut (multiplicity-counted); RANK/DENSE_RANK keep whole
  tie groups via a keys-only comparer (`SortKeyComparer` with new
  `ConstantZeroComparer` tiebreak). TKey = `StructuralRow` on BOTH compiler paths
  (partition values boxed into a StructuralRow). Snapshotable (flatten partitions
  to one Z-set, re-bucket on load by re-extracting the partition key).
- Both compiler paths: `CompilePartitionedTopK` in `PlanToCircuit` (structural)
  and `TypedPlanCompiler` (boxed extractors via `BuildBoxedExtractor`, reflected
  `BuildPartitionedTopK<TRow>`, structural fallback). Builder
  `StatefulOperators.PartitionedTopK`.

**Same 4-walker gotcha as TopK:** added arms to `PlanToCircuit.CollectScans` and
`CompilePlan`, and `PlanOptimizer` (pushdown barrier). `MonotonicityAnalyzer`
safe conservative default — no change.

**Deferred (in skipped.md):** selecting the rank value into output (schema would
change — rejected w/ explicit error); `QUALIFY` sugar; window over
grouped/aggregated/DISTINCT inner or >1 window per query (rejected); the general
windowed-column form (rank on every row — unbounded incremental churn, the reason
it's intentionally unsupported); window aggregates / LAG / LEAD.
