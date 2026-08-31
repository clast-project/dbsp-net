---
name: window-aggregates
description: DONE — SUM/COUNT/AVG/MIN/MAX OVER (RANGE frames) + LAG/LEAD/FIRST_VALUE/LAST_VALUE as new output columns; WindowAggregatePlan/PartitionedWindowAggregateOp + WindowOffsetPlan/PartitionedOffsetOp. PARTITION BY aggregates AND offsets (LAG/LEAD/FIRST/LAST) now have a TYPED/PARALLEL path (2026-06-11) — unlocks the fraud feature view going data-parallel; all window constructs are typed/parallel now (spine still N/A)
metadata: 
  node_type: memory
  type: project
  originSessionId: 39de43e2-49df-41ae-88e9-744de38480f4
---

Window aggregates `SUM/COUNT/AVG/MIN/MAX(x) OVER (PARTITION BY p [ORDER BY o
RANGE …])` shipped on `main` — the roadmap-candidates #1 "first-class windowing"
item (see [[roadmap-candidates]]). Full suite green (1386). This is distinct from
the earlier ranking-only [[partitioned-topk]] (which *filters* rows); window
aggregates emit a value as a **new column on every row** (widen the schema).

**Scope (Feldera-faithful, RANGE-only):** three frame shapes — whole-partition
(no ORDER BY), running (`RANGE UNBOUNDED PRECEDING AND CURRENT ROW`, the default),
bounded (`RANGE BETWEEN <const|day-time INTERVAL> PRECEDING AND CURRENT ROW`).
Structural compile only; typed/spine fall back. **Deferred:** LAG/LEAD,
FIRST_VALUE/LAST_VALUE; ROWS/GROUPS frames; FOLLOWING bounds; window nested in an
expression or over GROUP BY/DISTINCT; the general rank-on-every-row form.

**Multiple distinct OVER specs per query (DONE, later):** `TryResolveWindowAggregate`
now groups the query's window items by `(family, OVER spec)` (family = aggregate vs
offset; spec compared by `SameWindowSpec`), preserving first-occurrence order, and
chains ONE operator per group via a new `BuildWindowGroup` helper — each node widens
the rows the previous produced. So a query may carry several different OVER specs AND
freely mix aggregates with LAG/LEAD (the old "share one OVER spec" / "can't mix
aggregates and LAG/LEAD" resolver errors are gone). **Why chaining just works with
no operator/walker changes:** every layer (both ops' `Widen`, the two batch arms)
appends result cols to the FULL incoming row, not to a `base.Schema`-width row, so
base columns stay a fixed prefix at stable indices; group expressions all resolve
against `baseSchema`, and `preBound` records each item's ABSOLUTE result index
(`inputSchema.Count + k`). A global `synth` counter keeps synthetic col names unique
across groups. Purely a `Resolver.cs` change. Coverage: multi-spec + mixed-family
InlineData added to BOTH randomized incremental≡batch theories; resolver chain-shape
tests replaced the two old rejection tests.

**Shape of the implementation:**
- Parser: `WindowSpec` gained an optional `WindowFrame` (`Parser/Ast/Expressions.cs`);
  `ParseWindowFrameOrNull`/`ParseFrameBound` in `Parser.cs` parse `RANGE/ROWS/GROUPS
  BETWEEN … AND …` (RANGE/PRECEDING/FOLLOWING/UNBOUNDED/CURRENT are contextual
  keywords; ROWS/ROW/BETWEEN/AND are real tokens). `WindowFunctionExpression` now
  carries `Arguments`/`IsStar` (and `COUNT(*) OVER` is handled in the star branch).
- Resolver: `TryResolveWindowAggregate` pre-pass in `ResolveSelect` (after the
  TopK check). KEY TRICK: it resolves the pre-window relation as a synthesised
  `SELECT * FROM <from> WHERE <where>` (reuses all FROM/WHERE machinery incl.
  IN/EXISTS lifts) so partition/order/arg expressions resolve against the FULL
  source schema; then maps the user's select list with a `preBound` dict
  (window-node → result `ResolvedColumn`) via `ResolveProjections`. No
  `PreResolvedRelation` re-entry needed (unlike TopK). Reuses `ToAggregateKind` /
  `ComputeAggregateResultType`. Frame offset → native long via the same
  `Interval.Parse`/`MicrosPerDay` logic as the temporal-filter `IntervalOffset`.
  Ordered frames REQUIRE an INT/BIGINT/temporal ORDER BY key (so frame math is
  uniform on `long`).
- Plan: `WindowAggregatePlan(Input, PartitionKeys, OrderKey: SortKey?, Frame:
  WindowFrameBounds?, Aggregates, Schema)`; `WindowFrameBounds(long? Preceding)`
  (null = UNBOUNDED). Schema = Input cols ++ one result col per aggregate.
- Operator: `PartitionedWindowAggregateOp<TKey>` (`Core/Operators/Stateful/`,
  StructuralRow rows). **CRITICAL DESIGN:** recompute only the output rows whose
  frame a tick's delta could change (bounded value range for bounded; suffix from
  earliest delta value for running; whole partition otherwise), diffed per
  base-row against last-emitted widened rows. Whole-partition recompute would make
  GC unsound (a retained boundary row still needs GC'd rows in its backward
  frame); affected-range recompute is what makes GC correct AND avoids quadratic
  cost. Frame multiset → the existing `CompositeAggregator`. GC (bounded ascending
  only): drop rows with order value < `frontier − preceding` from both `_accum`
  and `_window` silently. `_window` is keyed by **base row** so GC can drop a
  finalized row without emitting a retraction.
- Builder `StatefulOperators.PartitionedWindowAggregate<TKey>` (aggregator passed
  as boxed `IAggregator<StructuralRow,StructuralRow>` — no typed reflection).
- Compile: `CompileWindowAggregate` (`PlanToCircuit`); GC frontier via
  `ResolveWindowFrontier` (mirrors `ResolveGroupKeyFrontier`, only for a bare
  monotone column ORDER BY key). Same 4-walker checklist as TopK — arms in
  `CompilePlan`, `CollectScans`, `PlanOptimizer` (pushdown barrier),
  `TypedPlanCompiler` (`=> null`, structural fallback). MonotonicityAnalyzer DOES
  need an arm (`ResizeTo(Visit(wa.Input), …)`) — without it the input subtree
  isn't analysed so GC can't find the order-key frontier; it also passes through
  base-column monotonicity for downstream GC.
- Oracle: `BatchWindowAggregate` arm in `BatchPlanEvaluator`; correctness held by
  a randomized incremental≡batch test (random inserts+deletes, 8 query shapes ×
  12 seeds) PLUS a LATENESS-GC monotonic variant (would have caught the original
  unsound GC), plus a snapshot round-trip test.

**LAG/LEAD + FIRST_VALUE/LAST_VALUE follow-on (also DONE, same commit family):**
`LAG/LEAD(expr [, offset [, default]])` and `FIRST_VALUE/LAST_VALUE(expr)`
`OVER (PARTITION BY p ORDER BY o)` as a new output column. FIRST/LAST piggyback on
the same positional operator via an `OffsetKind` enum {Lag, Lead, FirstValue,
LastValue} — the source slot is `j−offset` / `j+offset` (LAG/LEAD) or `0` /
`count−1` (FIRST/LAST, UNLIMITED RANGE = whole partition). FIRST/LAST require
ORDER BY, reject a frame, take exactly one arg.
*Positional* (by row, not value) — distinct from the value-based RANGE
aggregates. No parser change (the parser already carries the call args). The
resolver branches inside the same `TryResolveWindowAggregate` recognition:
classify window items as aggregate vs offset (lag/lead), reject mixing; offset
requires ORDER BY, rejects a frame, allows ANY comparable ORDER BY key (positional
uses the comparer, not `long` values). offset = non-negative int constant
(default 1); default = constant (folds `-1` = `Negate(literal)` via
`TryFoldConstant`). New `OffsetFunctionCall` + `WindowOffsetPlan`; new
`PartitionedOffsetOp<TKey>` (`Core`, `OffsetSpec[]`): expand each partition's
sorted rows into weight-aware positional slots, read each spec's value from slot
j±offset, widened-row-keyed recompute-and-diff (like `PartitionedTopKOp`, NOT the
base-row keying — no GC so not needed). Whole-partition recompute per touched
partition (affected-range + GC deferred). 4 walkers + MonotonicityAnalyzer
pass-through arm + `BatchWindowOffset` oracle arm + randomized incremental≡batch
(inserts+deletes, 7 shapes × 16 seeds incl. FIRST/LAST).

**Typed/PARALLEL path for PARTITION BY window AGGREGATES (DONE 2026-06-11, three
commits on main):** the "typed fast path declined" verdict below is REVERSED for
the aggregate case — taking the "full typed-generic operator" route, which turned
out small because the op was already 95% row-opaque. (1) `PartitionedWindowAggregateOp<TKey>`
→ `<TInRow,TAgg,TOutRow,TKey>`: the ONLY StructuralRow coupling was `Widen`
(row.Count/row[i]/new StructuralRow), extracted to an injected
`Func<TInRow,Optional<TAgg>,TOutRow>` widener; all else (affected-range recompute,
frame slicing, diff, GC, snapshot) was already delegate-driven. Structural builder
keeps its signature and delegates to a new generic `PartitionedWindowAggregate<TInRow,
TAgg,TOutRow,TKey>` overload with the default append-cols widener. (2)
`TypedPlanCompiler.CompileWindowAggregate` (flip the `=> null` at the
WindowAggregatePlan switch arm): mirrors `CompilePartitionedTopK` (boxed partition
extractors + parallel Exchange on the PARTITION BY key, co-locating each partition
on one worker; TKey=StructuralRow over boxed partition cols) and `CompileAggregate`
(typed composite aggregator — factored into a shared `BuildTypedComposite` helper).
The struct-fusing widener REUSES `BuildAggregateFlattenDelegate` with the full input
row in the "key" slot → `[input…, agg…]` = plan.Schema. Generic worker
`BuildPartitionedWindowAggregate<TInRow,TAgg,TOutRow>` (invoked via MakeGenericMethod)
builds the typed comparer + `orderValueOf` (MonotoneKey.Extract) + widener. **Unlocks
the [[feldera-comparison-benchmarks]] fraud feature view going data-parallel** (full
view W=1→W=4 ≈2.33×, parallel output cross-checked ≡ W=1; the benchmark's parallel
section now measures the WHOLE view, not just the join slice).

GATES (sound): PARTITION BY required (no-partition global window stays structural);
SUM/COUNT/AVG/**MIN/MAX** all take the typed/parallel path (the typed aggregators
DO cover MIN/MAX — `BuildMinMaxAggregator`; the old "MIN/MAX falls back" note +
the commit message saying so were stale). Only an un-lowerable/unsupported-type
aggregate arg falls back via `BuildTypedComposite => null`; **GC frontier is always null on the typed path and NOTHING is
lost** — PlanToCircuit (`PlanToCircuit.cs:174-177`) gates the typed path off whenever
LATENESS / a temporal filter is present, and those are the ONLY sources
`ResolveWindowFrontier` derives from, so every GC-eligible window query keeps the
structural path. Landmine fixed: WindowAggregateTests
built `ts` via a ternary unifying both arms to long (boxed an INT col as long) —
harmless structurally, but the typed scan lift enforces the schema; box each arm at
its true type. Suite 1785 green.

**OFFSET op (LAG/LEAD/FIRST_VALUE/LAST_VALUE) typed/PARALLEL path also DONE
(2026-06-11, same arc, commits 127f0be + a9bc37d):** identical playbook —
`OffsetSpec` → `OffsetSpec<TRow>` (its Value extractor was the one
`Func<StructuralRow>` coupling), `PartitionedOffsetOp<TKey>` →
`<TInRow,TOutRow,TKey>` with `ComputeWindow` building just the per-spec offset-value
array and handing it to an injected `Func<TInRow,object?[],TOutRow>` widener.
`TypedPlanCompiler.CompileWindowOffset` mirrors `CompileWindowAggregate` (boxed
partition extractors + Exchange, boxed order extractor + **full-row tiebreak
`Comparer<TInRow>.Default` which matches the structural `StructuralRowComparer`
positional order** — LagLead incremental≡batch with ties confirms parity, same
precedent as TopK). The one new piece: `BuildOffsetWidenDelegate` builds a HYBRID
widener (typed base-field reads ++ unbox/cast each boxed offset value onto the
appended nullable cols). Gate: PARTITION BY required; no-partition stays structural.
Caveat to watch: a LAG/LEAD `default` literal whose type ≠ the value column's CLR
type would `InvalidCastException` at the widener's unbox (structural tolerated it);
not hit by tests (defaults match the value type). Suite 1824 green.

**Original typed/spine decision (now superseded for BOTH aggregate AND offset
typed paths; still holds for spine):** a **spine** variant is N/A by design for ALL
these window ops (and TopK) — recompute-and-diff over plain per-partition
`SortedDictionary` state, not a `Trace`. The earlier "typed declined" reasoning
(hybrid-lift boundary cost) was outweighed once the operators proved row-opaque.
Still genuinely deferred: ROWS/GROUPS frames, FOLLOWING bounds, per-row/non-constant
LAG/LEAD default, IGNORE NULLS, offset-op GC, the no-PARTITION-BY typed path, and
the spine variant.
