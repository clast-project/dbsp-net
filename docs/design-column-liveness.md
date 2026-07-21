# Design: program-level column liveness (dead-column elimination across the view DAG)

Status: **analysis landed + validated; rewrite designed, not built.**
Owner arc: ivm-bench batch-1 allocation (see `docs/design-row-representation.md` §24).

## 1. Motivation

Re-profiling ivm-bench SF=3 batch-1 (HEAD `3f2c1a4`, ServerGC wall ~59.5s, 44.74 GiB
allocated) with per-operator allocation instrumentation (commit `16e7d76`) localized the
allocation:

| Operator kind | Count | Alloc | % of alloc |
|---|---|---|---|
| ApplyOp (map/filter/project) | 310 | 17.7 GiB | 47.3% |
| IncrementalInnerJoin | 37 | 10.3 GiB | 27.4% |
| WindowAggregate | 30 | 4.9 GiB | 13.1% |
| IncrementalAggregate | 11 | 2.8 GiB | 7.5% |
| WindowOffset | 6 | 1.36 GiB | 3.6% |

A per-row apportionment of one representative projection ApplyOp (`ApplyOpAllocSplit`
microbench, 14→12 col + 1 computed) splits its allocation:

- **(a) output container + dict entries — 15.2%** (columnar *storage* captures this)
- **(b) per-row `object[]` + `StructuralRow` — 73.2%** (captured only by vectorized column *write*)
- **(c) compute / boxing — 11.6%** (captured only by vectorized expression *eval*)

The lesson: a columnar *storage* inner-rep captures only ~15% of the ApplyOp tail (~7% of
total alloc); the 85% that matters lives behind vectorized execution — a large, separate
project. But a cheaper, orthogonal lever exists that needs **neither columnar storage nor
vectorization**: stop computing columns nothing reads.

## 2. The gap in the current compiler

Two pruning mechanisms exist, neither doing cross-view column liveness:

1. `PlanOptimizer` is a **per-view tree rewrite** — only the local `Project(Join)` /
   `Aggregate(Join)` narrowing rules (`PruneJoinInputs`, `NarrowAggregateInput`). Window /
   offset / TOP-K are hard pushdown barriers. The general top-down column-liveness pass is
   the explicit TODO at `PlanOptimizer.cs:38`.
2. `CompileProgram` prunes dead **views** (reachability from outputs) but **view-granular** —
   all-or-nothing per view.

The gap: a view can be *live* (some output reads it) yet produce individual *columns* no
live consumer reads. `CompileProgram`'s reachability can't see inside a live view, and
`PlanOptimizer` can't see across views.

## 3. The concrete finding (confirmed against the live compiler)

`daily_market` (a non-output view, kept because `market_volatility` reads it) declares 10
columns. Its two most expensive operators — a running `MIN/MAX ... OVER (PARTITION BY
dm_s_symb ORDER BY dm_date)` and a `MAX(flag) OVER (...)` — produce exactly the four
`fifty_two_week_low/high` and `fifty_two_week_low_date/high_date` columns. Those four
columns are read **only** by `fact_market_history`, which is a dead-pruned leaf (no
consumer, not an output). `market_volatility`, the sole live consumer, reads only
`dm_close/date/high/low/s_symb/vol` and never mentions `fifty_two_week`.

The analysis-only diagnostic (`ColumnLivenessProbe`, resolving the real deploy program)
reports:

```
daily_market: 4/10 dead output cols -> [fifty_two_week_low, fifty_two_week_high,
                                        fifty_two_week_low_date, fifty_two_week_high_date]
dead VIEWS (unreached): [daily_market_pulse, finwire_financial, financials,
                         wrk_company_financials, fact_market_history]
views with dead COLUMNS: 13   total dead cols: 64
```

Because those four dead columns are the *produced values* of the two window ops (not
passthrough/key columns), eliminating them eliminates the ops entirely:

| Op | | Time | Alloc |
|---|---|---|---|
| 348 WindowAggregate | 52wk low/high running MIN/MAX | 4.6s | 1620 MiB |
| 350 WindowAggregate | 52wk low/high dates (max-flag) | 2.3s | 1382 MiB |
| 349 ApplyOp | flag-CASE feeding op 350 | 1.0s | 390 MiB |

**≈ 7.9s and ~3.4 GiB — 7.6% of the whole batch, and 61% of all WindowAggregate
allocation.** Pure dead-code elimination; correctness-preserving; config-driven exactly
like the existing dead-view prune.

## 4. Two payoff classes

The diagnostic surfaces 64 dead columns across 13 views. They split into two classes with
very different payoff:

- **Producer-dead → operator elimination.** The dead column is a *value produced* by a
  stateful op (window aggregate/offset, and in principle a projection's computed column)
  whose input columns are needed only to compute it. Dropping the column drops the op.
  `daily_market`'s four window columns are this class — the ~3.4 GiB win.
- **Passthrough/key-dead → row narrowing only.** The dead column is passed through or is a
  GROUP BY / DISTINCT key surfaced to output. Example: `watches: 5/9 dead ->
  [company_id, company_name, exchange_id, security_status, watch_status]` — four of those
  are group keys of `watches`'s aggregate. They are dead *for output* (so the final
  projection can stop surfacing them, narrowing the inter-view row and `fact_watches`'s
  read) but **semantically live inside the aggregate** (dropping a group key would merge
  groups and change the MIN/MAX result). This class narrows row width (the (b) term) but
  does **not** eliminate the op or shrink its state.

The pass captures both; the soundness rules (§6) keep the second class correct.

## 5. Design

### 5.1 Layer 1 — intra-plan column liveness (`PlanColumnLiveness`, analysis: landed)

`LiveScanColumns(plan, liveOut) → Dictionary<scanName, liveColumnSet>`: a backward visitor
mirroring `CompileProgram.CollectScans`' switch, carrying the set of live output-column
indices down and returning, at each `ScanPlan` leaf, the input columns actually demanded.
Recurses through `CteScanPlan` bodies (a CTE referenced N times accumulates the union of its
references' demands). Per-node mapping:

| Node | live-out → live-in |
|---|---|
| `ScanPlan` | record `liveOut` (clamped) as demand on the scanned name — leaf |
| `CteScanPlan` | recurse into the CTE body with `liveOut` (cycle-guarded) |
| `ProjectPlan` | `∪ CollectColumnIndices(Projections[i])` over live `i` only |
| `FilterPlan` | `liveOut ∪ predicate columns` (schema preserved) |
| `JoinPlan` | split at `leftCount`; **force equi-keys + residual columns live** |
| `AggregatePlan` | **all group keys + all aggregate args** (conservative-sound) |
| `WindowAggregatePlan` | passthrough live cols; if any produced col live → `+ partition + order + live-agg args`; **else contribute nothing (op is dead)** |
| `WindowOffsetPlan` | same shape with offset functions |
| `UnionAllPlan` | same `liveOut` to every (position-aligned) branch |
| `DistinctPlan` / `DifferencePlan` | **all input columns live** (set semantics) |
| others (TemporalFilter, Semi/Scalar/Correlated subquery, TopK, PartitionedTopK/Rank, RecursiveCte) | **conservative: all input columns live** |

**Conservative fallback = soundness.** Every rule *over-approximates* liveness. A node type
not modelled precisely marks all its inputs live, which can only prune fewer columns, never
an unsound one. So Layer 1 ships modelling the hot-path kinds precisely and generalises
node-by-node without ever risking correctness. The `WindowAggregatePlan`/`WindowOffsetPlan`
"else contribute nothing" branch is what lets a fully-dead window op drop its own
partition/order/arg columns — the producer-dead elimination signal.

### 5.2 Layer 2 — program glue (`ComputeProgramLiveColumns`, landed)

Backward over the dependency-ordered `views` (mirroring the dead-view reachability pass):
seed each output view fully live; for each reached view `V`, run `LiveScanColumns(V.Query,
live[V])` and union the returned per-scan demand into `live[name]`. A name never reached is
a dead view (already handled by `CompileProgram`). Result: `live[V]` = the live output
columns of every view.

### 5.3 The rewrite (designed, not built)

In `CompileProgram`, after computing `live`, before each view's `PlanOptimizer.Optimize`,
prune the view's plan to `live[V]`:

- **Producer-dead**: a `WindowAggregate`/`WindowOffset` whose produced columns are all dead
  → replace the node with its input (remapping passthrough indices). A `Project` item that's
  dead → drop it.
- **Passthrough/key-dead**: narrow the view's terminal projection to the live columns; the
  per-node live-*input* sets (which keep group/distinct keys) drive how far the narrowing
  pushes down.

Order per view: **prune-to-live → `Optimize` → compile.** Narrowing outputs first lets the
existing `PruneJoinInputs`/`NarrowAggregateInput` rules push further (synergy). Gated behind
a `CompileOptions` flag (default off), same playbook as `JoinColumnPruningMode`.

## 6. Soundness invariants

A column dead *for output* must still be treated live where it affects row multiplicity or a
computed value:

- **GROUP BY / DISTINCT keys** — dropping one merges rows (the `watches` case). The
  `AggregatePlan`/`DistinctPlan` rules keep them live in the demand, so a rewrite built on
  the per-node live-input sets keeps them by construction.
- **Join equi-keys + residual** — decide matching; forced live.
- **Window/offset partition + order + frame + arg columns** — live *only* to serve the
  window's own produced columns; forced live iff a produced column is live (else the op is
  dead and they are not).
- **Filter predicates, TOP-K/rank order+partition** — forced live (conservative for the
  barrier kinds).
- **Output views** — never pruned (the connector writes the full declared schema). The
  diagnostic asserts every output view is fully live as a seeding check.

## 7. Validation

- **Analysis (done):** `ColumnLivenessProbe` reproduces the `daily_market` prediction
  against the live compiler and asserts all 16 output views fully live.
- **Rewrite (planned):** a program-differential test — compile the ivm-bench program (and a
  synthetic multi-view DAG) with and without the pass, drive identical input deltas, assert
  byte-identical output on all 16 views. This is the program-level analog of the existing
  single-plan random-query PBT (which already checks optimized ≡ unoptimized batch eval).
- **Targeted unit tests:** producer-dead window → op eliminated (`daily_market` shape);
  key-dead → a dead-for-output group key is not dropped from the aggregate; CTE referenced
  twice with disjoint demands unions correctly.

## 8. Incremental delivery

1. **Analysis-only diagnostic — DONE.** `PlanColumnLiveness` + `ColumnLivenessProbe`, no plan
   mutation. Confirmed the numbers (§3).
2. **Gated rewrite, producer-dead first** — window/offset elimination captures the
   `daily_market` ~3.4 GiB win with the smallest surface. Validate via the differential.
3. **Passthrough/key-dead narrowing** — the broader 64-dead-col surface (`accounts 23/32`,
   `syndicated_prospect 14/22`, `trades 4/15`, …); (b)-term row narrowing.
4. Generalize the conservative node kinds (semi-join passthrough, TOP-K) as measured value
   justifies.

## 9. Effort / payoff

- Layer 2 + analysis + diagnostic: **done** (small).
- Producer-dead rewrite on the hot path: **medium**, captures ~3.4 GiB / ~7.9s (7.6% of the
  batch) — the single largest lever the re-profile surfaced, and an algorithmic-class dead-
  code elimination, not a per-row micro-optimization.
- Full generalization: **medium-large**, incremental, never-unsound by construction.

Needs neither the columnar storage rewrite nor expression vectorization — it is orthogonal
to and composes with both.
