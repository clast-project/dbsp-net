# Design: general rank-in-output (RANK / DENSE_RANK / ROW_NUMBER as a column)

Plan for the last ivm-bench gap (gap 1): a ranking window function emitted as an
**output column** on every row, not the existing `WHERE rn <= k` TopK-filter pattern.
Scoped 2026-07-16; not yet built. This is the sole remaining blocker — with it, all 50
feldera models compile (standup was 42/50, all 8 remaining failures are rank-in-output).

## Target shapes (from the analytics models)

- `RANK() OVER (ORDER BY total_notional DESC) AS r` — **unpartitioned** (whole relation =
  one partition), plain output column. `broker_performance`, `customer_concentration`,
  `daily_market_pulse`, `market_volatility` (with a tiebreaker), `trade_volume_stats`.
- `DENSE_RANK() OVER (ORDER BY x DESC)` — same, several models.
- `CASE WHEN ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY … DESC …) = 1 THEN true
  ELSE false END` — `financials`' `is_current`, **partitioned** on small SCD2 groups
  (cheap), rank nested in a CASE.

Cost split: the partitioned `financials` case is cheap (small partitions); the unpartitioned
analytics ranks are the expensive ones (partition = whole relation). Same operator, cost
differs by partition size.

## Reuse assessment — build a new `PartitionedRankOp`, fork + borrow

Assemble from parts that already exist; do **not** extend either op in place.

- **Output/widen/diff side** ← `PartitionedWindowAggregateOp`
  (`src/DbspNet.Core/Operators/Stateful/PartitionedWindowAggregateOp.cs`), almost verbatim:
  - `_accum : Dictionary<TKey, SortedDictionary<TInRow,long>>` (sorted per-partition trace),
    and `_window : Dictionary<TKey, Dictionary<TInRow, (TOutRow Widened, long Weight)>>` keyed
    by base row, valued by the **widened** output row + weight (`:70-73`) — exactly the shape
    rank needs.
  - `EmitRowDiff` (`:321-349`): row changed → retract old widened + insert new widened; only
    weight changed → delta; row vanished → retract. This is the "insert shifts later rows →
    retract-old/insert-new" churn rank produces.
  - The `widen` delegate `Func<TInRow, Optional<TAgg>, TOutRow>` (`:61`) is the only
    row-shape-specific seam; for rank it appends the rank integer.
  - Snapshot Save/Load (`:426-475`) copies over.
- **Rank-assignment side** ← `PartitionedTopKOp.ComputeWindow`
  (`src/DbspNet.Core/Operators/Stateful/PartitionedTopKOp.cs:232-302`), with the `_limit` cut
  removed:
  - `_sortKeyOnly` comparer (keys-only, `ConstantZeroComparer` tiebreak, `:66`; built in
    `PlanToCircuit.CompilePartitionedTopK:1103-1104`) does tie-group detection.
  - ROW_NUMBER = multiplicity-counted position; RANK = `1 + rows strictly before` (the
    `rowsBefore` counter); DENSE_RANK = `1 + distinct ORDER-BY values before` (the `denseRank`
    counter, +1 per `_sortKeyOnly`-distinct group). Write `result[row] = (widen(row, rank),
    weight)` instead of `result[row] = weight`, and drop the early `break` at the cut.
  - `RankFunction` enum already exists and is shared (`PartitionedTopKOp.cs:15-31`).
- **Why fork, not extend:** `PartitionedTopKOp` *filters, never widens* (doc `:46-51`; its
  `_window` value is a bare `long`). `PartitionedWindowAggregateOp`'s recompute range is
  **value/RANGE-arithmetic-driven** (`:218-236`), meaningless for positional rank.

**The one genuinely new piece:** rank is *positional*, not value-defined — an insert shifts
the rank of every row after it in sort order, with no value-arithmetic bound. So the op
recomputes the **whole touched partition** (TopK `Step`-style, `:132-194`) rather than a
value range. This sidesteps suffix arithmetic at an O(P) cost that is the accepted cost anyway.

## Resolver path — the nested case is already free

- `TryResolveWindowAggregate` (`Resolver.cs`, dispatched at `:235`) already collects **all**
  window nodes via `CollectWindowFunctions` (`:1773-1819`), which recurses into CASE/unary/
  binary/cast/function/in-list — so `financials`' `ROW_NUMBER() OVER(...) = 1` in a CASE **is
  already collected**. It currently hits the throw at `:1324-1331` ("ranking functions …
  supported only in the TOP-K filter pattern").
- Change: in the per-node loop (`:1322-1334`), add a **rank family** alongside aggregate/
  offset instead of throwing; group rank nodes by OVER spec (`:1367-1386`, a rank discriminant
  next to `isOffset`); in `BuildWindowGroup` (`:1419`) emit a new `PartitionedRankPlan` arm
  (mirroring the `WindowOffsetPlan`/`WindowAggregatePlan` arms). The `ResolveProjections` /
  `preBound` tail (`:1399-1405`) already maps the hidden rank column into the select list,
  including inside the CASE — the window-in-expression lift machinery is family-agnostic.
- `TryResolvePartitionedTopK` (`:936-1083`, the `<= k` filter form) is untouched — a plain
  output-column rank returns null there and falls to `TryResolveWindowAggregate`.
- **Unpartitioned works structurally**: empty PARTITION BY → `PartitionOf` builds a
  `StructuralRow` from zero extractors = one global partition. No special casing.

## Plan node + compile sites

- **`PartitionedRankPlan`** (`LogicalPlan.cs` near `:193`) = `PartitionedTopKPlan` minus
  `Limit`, plus an explicit widened `Schema` (input cols + one rank INT/BIGINT column):
  `Input, IReadOnlyList<ResolvedExpression> PartitionKeys, IReadOnlyList<SortKey> SortKeys,
  RankFunction Function, Schema Schema`.
- **`PlanToCircuit.cs`**: `CompilePartitionedRank` modeled on `CompilePartitionedTopK`
  (`:1080-1143`, already builds `order` + `sortKeyOnly` + `PartitionOf`) plus a `widen`
  appending the rank column (borrow from `CompileWindowAggregate` `:1147-1226`); wire the 3
  dispatch switches (`:720`, `:874`, `:992`). Add `CircuitBuilder.PartitionedRank` in
  `StatefulOperators.cs` (near `:127`/`:238`).
- **`TypedPlanCompiler.cs`**: `return null` → structural fallback (like `CompileWindowAggregate`
  for no-PARTITION-BY, `:1307-1308`). Since the marquee case is unpartitioned, the typed path
  need not implement rank at all.
- **`BatchPlanEvaluator.cs`**: add `BatchPartitionedRank` (near `:574`, mirror
  `BatchWindowAggregate`) **if** targeting the shared-oracle PBT. Note: TopK is NOT in the
  batch oracle today (no `PartitionedTopKPlan` arm) — it uses a hand-rolled batch instead.

## Incremental cost (inherent, not a bug)

- **O(partition-size) churn per insert** — ROW_NUMBER: every row after the insert re-emits;
  RANK/DENSE_RANK: depends on ties, worst case O(P). Documented at `skipped.md:462-478`.
- **No GC / unbounded state.** `PartitionedTopKOp.GcFrontier => null`. A future row with a
  smaller ORDER BY value re-ranks everyone, so no row is ever finalizable — the op retains the
  whole integrated input per partition. The window-aggregate GC (`:357-424`) only fires for a
  bounded ascending RANGE frame; rank has no lower frame bound.
- **Unpartitioned analytics (P = relation):** O(relation) per tick → O(n²) for a full load.
  This is exactly where the benchmark's own config says Feldera wedged at SF=100. The expensive
  tier is itself a benchmark result, not just a cost.

## Testing

- **Differential PBT** (incremental ≡ batch under random inserts/deletes) — the load-bearing
  test. Two templates: (a) hand-rolled batch like `PartitionedTopKNarrowingPbtTests.cs:117-123`
  (`BatchTopK`), or (b) add `BatchPartitionedRank` to `BatchPlanEvaluator` and use the shared
  oracle like `WindowAggregateTests.cs:647`. Prefer (b) for consistency; it also lets the
  `RandomQuery` generator exercise rank if extended.
- **Behavioural**: RANK vs DENSE_RANK vs ROW_NUMBER tie semantics (1,1,3 vs 1,1,2 vs 1,2,3);
  the `financials` `ROW_NUMBER()=1`-in-CASE shape; unpartitioned; DESC + tiebreaker
  (`market_volatility`). Mutation-test the tie/retraction paths (this arc's discipline).
- **Snapshot round-trip** (mirror `PartitionedTopKSnapshotTests.cs`) — a new op needs one.
- Files to mirror: `PartitionedTopKTests.cs`, `PartitionedTopK*PbtTests.cs`,
  `PartitionedTopKSnapshotTests.cs`, `WindowAggregateTests.cs`.

## Effort / risk

- **Effort: ~2-4 focused days.** Heavy reuse — mostly assembly + the one new recompute mode.
- **Risk: moderate, concentrated in recompute-correctness under ties AND retraction** — the
  candidate set (current ∪ previously-emitted rows, as `RecomputePartition:245-261` does) and
  the retract-vanished-rows path. Whole-partition recompute sidesteps suffix arithmetic. Diff
  churn volume is large per tick (O(P)) but that's the inherent cost, not a correctness risk.
  Everything else (resolver lift, snapshot, typed punt) is low-risk.

## Change-site summary

| Site | File | ~LOC |
|---|---|---|
| New `PartitionedRankOp` | new file in `Core/Operators/Stateful/` | 250–320 |
| `CircuitBuilder.PartitionedRank` | `StatefulOperators.cs` | 30 |
| `PartitionedRankPlan` | `LogicalPlan.cs` | 10 |
| Resolver rank branch | `Resolver.cs` (`:1322`, `:1367`, `:1419`) | 60–90 |
| `CompilePartitionedRank` + 3 switches | `PlanToCircuit.cs` | 70 |
| Typed arm (fallback) | `TypedPlanCompiler.cs` | 5 |
| `BatchPartitionedRank` (for PBT) | `BatchPlanEvaluator.cs` | 40 |
| Tests | mirror TopK / WindowAggregate | 200–400 |
| Docs | `skipped.md` (move from [P2]), gap analysis | small |
