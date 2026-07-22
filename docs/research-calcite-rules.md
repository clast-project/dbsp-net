# Research: mining Calcite's rule set for IVM-worthy rewrites

Status: **active research project. Phase 1 (instrument + first census) landed —
`dotnet run --project src/DbspNet.Benchmarks -c Release -- rulecensus`, results in
[`calcite-rule-census.md`](calcite-rule-census.md). First shortlist item **built**
(§5.1, the MIN/MAX narrowing unblock — q4 −37 % ns/event at W=1). The rest are gated on
the census, per-candidate, and on the batch-1 corpus arriving (§6).**

Follow-on to the structural CSE pass (`src/DbspNet.Sql/Optimizer/PlanCse.cs`,
`c91fc9b`), which was 5.2× on Nexmark q5 and which Calcite would have done for us —
its `CommonRelSubExprRegisterRule` / digest-based interning is exactly that rewrite.
Feldera plans through Calcite, so every rule Calcite applies before DBSP lowering is
one it gets for free and we must hand-write. That asymmetry is the reason to mine the
catalog. This doc is the mining.

## 0. TL;DR

- **The catalog is ~170 `CoreRules` constants across ~140 rule classes** (§3, fetched
  from the Calcite javadoc). Most are inapplicable to us for structural reasons — we
  have no `Calc`, `Values`, `Sample`, `Match`, `StarTable`, `Correlate` or standalone
  `Sort` node — which cuts the real surface to roughly **35 rules in 9 families**.
- **Batch rule priorities do not transfer to IVM.** A batch rule is worth its saved
  row-passes; an IVM rule is worth its saved *retained state*, because an arrangement
  or aggregate multiset is paid every tick, forever, not once (§2). This reorders the
  catalog substantially, and makes a handful of Calcite's rules actively **negative**
  for us.
- **Measured, on the corpus we have** (18 Nexmark + fraud plans, post-`Optimize`):
  `REDUCE_EXPRESSIONS` 35 sites, `AGGREGATE_CASE_TO_FILTER` 9, `PROJECT_REMOVE` 6,
  `AGGREGATE_NARROWING_BLOCKED_BY_MINMAX` 3 (now **0** — §5.1 shipped),
  `JOIN_TO_SEMI_JOIN` 1, `JOIN_DERIVE_IS_NOT_NULL_FILTER` 1, and **zero** for
  filter/aggregate transpose, transitive predicates, dead aggregate calls, duplicate
  group keys and union flattening (§4).
- **The honest read of that census: the hand-written corpora are already tight.**
  Nexmark SQL is written by humans to be minimal, and our existing pushdown/fusion
  passes cover the families that would fire on it. The rules Calcite earns its
  reputation with fire on *machine-generated* SQL — which in this repo means the
  batch-1 dbt program (50 views), and that corpus is **not on this host** (§6). Treat
  every zero above as "zero on hand-written SQL", not "zero".
- **Where the real upside is**: not in copying Calcite's rules, but in the two IVM
  rules Calcite does not have — state-shrinking rewrites (§5.1, §5.2) and a
  state-weighted (not cardinality-weighted) cost model for the reordering family
  (§5.6). Calcite's value here is mostly as a *checklist of soundness conditions*.

## 1. What we already have, in Calcite's vocabulary

`PlanOptimizer.Optimize` (fixed-point, ≤50 iterations, then `PlanCse`) implements,
under other names:

| our rule (`PlanOptimizer.cs`) | Calcite equivalent |
|:--|:--|
| `SimplifyFilter` — adjacent filter merge | `FILTER_MERGE` |
| `SimplifyFilter` — push past `ProjectPlan` by substitution | `FILTER_PROJECT_TRANSPOSE` (whole-expression variant) |
| `PushFilterPastJoin` — per-conjunct classification, outer-join-safe, residual fold | `FILTER_INTO_JOIN` + `JOIN_CONDITION_PUSH` |
| `SimplifyFilter` — push into `UnionAll` branches / both sides of `Difference` | `FILTER_SET_OP_TRANSPOSE` |
| `SimplifyFilter` — push past `Distinct` | `FILTER_AGGREGATE_TRANSPOSE` (the distinct special case) |
| `SimplifyProject` — fuse adjacent projections | `PROJECT_MERGE` |
| `NarrowAggregateInput` — insert a narrowing project under an aggregate | `AggregateExtractProjectRule` |
| `PruneJoinInputs` + `PlanColumnLiveness` | `ProjectJoinTransposeRule` + `PushProjector` field-pruning |
| `NarrowSemiJoinSubquery` | `SemiJoinProjectTransposeRule` (narrowing half) |
| `PlanCse` — hash-consing to a DAG | `CommonRelSubExprRegisterRule` |
| resolver-level IN/EXISTS/scalar-subquery lifting | `SubQueryRemoveRule` (partial — see §5.7) |

So the pushdown, fusion and pruning families are largely covered. What follows is the
complement.

## 2. The value model: why Calcite's priorities don't transfer

Calcite's rules are ranked, implicitly, by rows-not-touched in a single pass. A DBSP
circuit is ranked by two different quantities:

1. **Per-tick work** — proportional to the *delta*, not the relation. A rule that
   removes work per row is worth (delta size × ticks), which is usually small.
2. **Retained state** — the traces/arrangements behind `JoinPlan`, `AggregatePlan`,
   `DistinctPlan`, `TopKPlan`, the window operators. This is paid in memory forever
   and in merge cost on every tick that touches the key. A rule that keeps one row
   out of one trace is worth far more than a rule that saves one row-pass.

That gives a three-way classification we apply throughout §3:

- **State-shrinking** — fewer rows, fewer columns, or fewer distinct keys entering a
  stateful operator; or one fewer stateful operator. *These are the ones worth
  building*, and they include rules Calcite considers minor.
- **Transient-only** — saves per-row compute in a stateless `ApplyOp`/`MapRows`.
  Worth roughly its constant factor. Cheap to build, cheap to win, easy to overrate.
- **State-inflating** — Calcite likes it, we should not. Anything that duplicates a
  subplan (OR-expansion, distinct-aggregate-to-join, union-of-joins) buys a batch
  cardinality win by paying a *permanent second arrangement*. Under the DBSP cost
  model these can be net-negative even when they are unambiguously good in batch.

A fourth category matters for us and not for Calcite: **linearity-unblocking**. Our
own rewrites gate on aggregates being linear in the Z-set weights (`NarrowAggregateInput`
bails on MIN/MAX; see `CompositeAggregator.SumWeights`). A rule that rewrites a
non-linear aggregate into linear parts does not just save work — it *unblocks other
passes*. Calcite has no analogue for this because batch aggregation has no such gate.

## 3. Catalog triage

Source: `org.apache.calcite.rel.rules.CoreRules` field list and package class list
(Calcite javadoc, fetched 2026-07-22). Grouped by family, classified per §2.

### 3.1 Structurally inapplicable — no such node in our algebra

`CALC_*` (6 rules), `PROJECT_TO_CALC`, `FILTER_TO_CALC`, `*_VALUES_MERGE` /
`AGGREGATE_VALUES` / `UNION_TO_VALUES` / `ValuesReduceRule`, `SAMPLE_TO_FILTER` /
`FILTER_SAMPLE_TRANSPOSE`, `MATCH`, `AGGREGATE_STAR_TABLE` / `AGGREGATE_PROJECT_STAR_TABLE`
/ `MaterializedViewFilterScanRule`, `SpatialRules`, `MeasureRules`, `*_TABLE_SCAN` /
`*_INTERPRETER_*` (Bindable/Enumerable conventions), `UNNEST_*`, `JOIN_TO_CORRELATE` /
`FILTER_CORRELATE` / `PROJECT_CORRELATE_TRANSPOSE` (we decorrelate in the resolver and
never build a `Correlate`), `SORT_*` except as noted in §3.4,
`COERCE_INPUTS`, `CALC_REDUCE_DECIMALS`. **~60 rules, no action.**

### 3.2 Already implemented — see §1. **~11 rules.**

### 3.3 Candidates, state-shrinking (the priority list)

| rule | what it buys under IVM | census |
|:--|:--|--:|
| `FILTER_AGGREGATE_TRANSPOSE` | a group-key predicate below the aggregate keeps whole *groups* out of the per-group multiset — removes state, not just work | 0 |
| `JOIN_PUSH_TRANSITIVE_PREDICATES` | `a.k = b.k AND a.k > 5` ⇒ `b.k > 5`: rows filtered *before* they enter the other side's arrangement. Batch calls this a scan win; for us it is a trace-size win on both sides | 0 |
| `JOIN_DERIVE_IS_NOT_NULL_FILTER` | our equi-join drops NULL keys anyway (`AllowNullKeys=false`) but only *after* they have been arranged. An `IS NOT NULL` pushed below keeps them out of the trace entirely | 1 (q7) |
| `PROJECT_AGGREGATE_MERGE` | a dead aggregate call is a live incremental aggregator with per-group state serving nobody | 0 |
| `AGGREGATE_JOIN_TRANSPOSE[_EXTENDED]` | pre-aggregating below a join can shrink the join's arrangement by orders of magnitude. Sound only for decomposable aggregates (SUM/COUNT with a re-aggregation above) — which is also exactly the DBSP-linear set | not censused (§7) |
| `AGGREGATE_REMOVE` / `AGGREGATE_MERGE` | an aggregate with no calls is a `Distinct`; stacked aggregates whose upper key is a subset collapse — one fewer stateful operator | 0 / not censused |
| `JOIN_TO_SEMI_JOIN`, `PROJECT_TO_SEMI_JOIN`, `AGGREGATE_TO_SEMI_JOIN` | a join used only as a filter keeps the right-hand payload out of the output *and* out of the arrangement. We already have `SemiJoinPlan`; this is a plan-level recognizer, not new runtime | 1 (q8) |
| `AGGREGATE_REMOVE_DUPLICATE_KEYS`, `AGGREGATE_PROJECT_PULL_UP_CONSTANTS` | narrower group key ⇒ smaller key in every trace entry and every probe | 0 |
| `PruneEmptyRules` | a statically-empty branch can be compiled away — under IVM that deletes an operator *and* its state, not just a scan | 0 |
| `EXCHANGE_REMOVE_CONSTANT_KEYS` | directly relevant to the structural-parallel work (`docs/design-structural-parallel.md`): a constant in the exchange key skews every shard to one worker | not censused (§7) |

### 3.4 Candidates, transient-only

| rule | note | census |
|:--|:--|--:|
| `*_REDUCE_EXPRESSIONS` (filter/project/join/window) + `RexSimplify` | constant folding and predicate simplification. Highest raw site count, lowest per-site value — and path-dependent: the structural compiler emits literals as `Expression.Constant(value, typeof(object))` and casts as a runtime conversion on the boxed value (`ExpressionCompiler.BuildLiteral`/`BuildCast`), so a literal `CAST` really is a per-row boxed call there; the typed fast path emits a typed constant the JIT folds. **Measure before claiming a win** | 35 |
| `AGGREGATE_CASE_TO_FILTER` | note our FILTER-clause sugar lowers the *other* way (`agg(x) FILTER (WHERE p)` → `agg(CASE WHEN p THEN x END)`, `NexmarkQueries.cs:228`). Adopting this rule means adding a native filtered-aggregate path so a non-matching row skips the aggregator instead of contributing a NULL | 9 |
| `PROJECT_REMOVE` | an identity projection still compiles to a per-row `MapRows` | 6 |
| `UNION_MERGE` | flattens nested `UnionAll` — one fewer stream hop per level; the HOP lowering builds 5-branch unions | 0 |
| `AGGREGATE_REDUCE_FUNCTIONS` | AVG → SUM/COUNT etc. Transient by itself, but see §5.3: it is the lever that unblocks `NarrowAggregateInput` on MIN/MAX plans | 3 blocked sites |
| `DateRangeRules` | `EXTRACT`/`FLOOR`/`CEIL` → range predicates. Ties into `docs/now-and-temporal-filters.md`: a range predicate on a timestamp column is what the lateness/retain-keys machinery can act on, so this one may be state-shrinking in disguise | not censused (§7) |

### 3.5 Anti-rules — sound, but net-negative under IVM

Recording these is half the value of the exercise: they are the rules a future
"just port Calcite's `RelBuilder` defaults" instinct would adopt by accident.

- `JOIN_EXPAND_OR_TO_UNION_RULE` — turns one join into a union of joins. In batch,
  fewer rows enumerated; for us, **N copies of the join's arrangement**, permanently.
- `AGGREGATE_EXPAND_DISTINCT_AGGREGATES[_TO_JOIN]` — we have a native `CountDistinct`
  aggregate kind; expanding to a self-join adds a whole stateful operator to avoid
  work we do not do.
- `INTERSECT_TO_DISTINCT`, `MINUS_TO_DISTINCT` — add a `Distinct` (state) where our
  resolver's set-op lowering already handles the shape.
- `JOIN_COMMUTE` / `JOIN_ASSOCIATE` / `MULTI_JOIN_OPTIMIZE[_BUSHY]` / `HYPER_GRAPH_OPTIMIZE`
  — not wrong, but their cost model is cardinality-based. See §5.6.
- `FILTER_TO_CALC`-style normalizations — pure overhead without a `Calc` node.
- `AGGREGATE_MIN_MAX_TO_LIMIT` — converts MIN/MAX to ORDER BY + LIMIT. For us that is
  a `TopKPlan`, i.e. trading a cheap incremental aggregate for a *harder* incremental
  operator. Backwards.

## 4. Phase-1 census (measured)

Instrument: `src/DbspNet.Benchmarks/CalciteRuleCensus.cs`, run with
`-- rulecensus`, output [`calcite-rule-census.md`](calcite-rule-census.md). It counts
sites where a candidate rule matches the plan **after** `PlanOptimizer.Optimize` — so
every site is redundancy the current pass set genuinely leaves behind — over 18 plans
(Nexmark q0–q22 minus the ones that don't compile, plus the fraud feature view),
visiting reference-shared subtrees once (post-CSE the plan is a DAG). Each detector
documents its own over/under-counting in the generated report.

Findings worth reading twice:

1. **`REDUCE_EXPRESSIONS` dominates by count (35) and is mostly one shape**: literal
   casts introduced by the resolver's type promotion — `WHERE auction % 123 = 0`
   compiles a `CAST(123 AS BIGINT)` and a `CAST(0 AS BIGINT)` that are re-evaluated
   per row per tick on the structural path. q15/q16 contribute 12 each (the
   `COUNT(*) FILTER (WHERE price < 10000)` family). Cheapest possible rewrite; the
   open question is whether the typed path already makes it free (§3.4).
2. **Every "classic" pushdown candidate censused zero.** No filter-over-aggregate, no
   transitive predicates, no dead aggregate calls, no duplicate group keys, no nested
   unions. This is a real result about the corpus, not a bug in the detectors: the
   detectors run post-`Optimize`, and the shapes they look for are ones hand-written
   Nexmark SQL doesn't contain in the first place.
3. **The two state-shrinking hits are single-site but well-targeted**: q7's nullable
   equi-key (`JOIN_DERIVE_IS_NOT_NULL_FILTER`) and q8's join whose right side is never
   read above it (`JOIN_TO_SEMI_JOIN`) — the latter pending a right-side distinctness
   proof we currently have no statistics for.
4. **3 sites where our own `NarrowAggregateInput` bails on MIN/MAX** while the input
   is 2–3× wider than the aggregate reads (q4, q7, q17). That is the single most
   concrete state win the census found, and §5.1 collected it — a re-run after that
   work reports **0** for this detector, the census doubling as a regression check on
   its own finding.

## 5. Shortlist, with the gate each has to pass

Ordered by (expected state saved) × (confidence), not by site count.

### 5.1 MIN/MAX narrowing, unblocked — **BUILT** (`docs/design-row-representation.md` §18.6)

q4/q7/q17 each carry a MIN/MAX aggregate reading 2–3 of 7 input columns, and our
linearity gate blocked narrowing. The unblock taken was neither of the two routes
sketched here: rather than rewrite the aggregate, **prove the input can't cancel**.
Non-negativity of the aggregate's input is exactly the soundness condition, and it is
now derived per aggregate by `PlanWeightPositivity` — from a declared
`WITH ('append_only' = 'true')` table property (Feldera's spelling), or derived free
from a sign-laundering `DISTINCT`/aggregate in the lineage. The rule is **on by
default** where it is provable; the old global seam became a tri-state
(`Auto`/`Always`/`Never`) whose non-default arms exist only for the A/B.

**Measured:** census blocked-sites 3 → 0; q4 at W=1 **−37 % ns/event, −23 % B/event**
(2,183 → 1,376 ns, 2,376 → 1,841 B), control query unchanged. The W=8 harness was too
noisy on this host to report — see §18.6.

The `AGGREGATE_REDUCE_FUNCTIONS` route stays in the catalog for the case this does not
cover: a MIN/MAX over a genuinely signed input (a `DifferencePlan` below), where the
only way to narrow is to change the aggregate rather than to prove something about
its input. No sites for that on the current corpus.

### 5.2 `JOIN_DERIVE_IS_NOT_NULL_FILTER` — cheapest state-shrinker

One site here, but the rule is ~30 lines and needs no statistics: for an inner join,
emit `IS NOT NULL` on each nullable equi-key column below each side. **Gate:** none
beyond the PBT — it strictly removes rows the join would drop anyway. **Expected win
on this corpus:** near zero (Nexmark columns are `NOT NULL`); the reason to build it
is the batch-1 corpus, where nullable join keys are the norm.

### 5.3 `FILTER_AGGREGATE_TRANSPOSE` + `PROJECT_AGGREGATE_MERGE` — build on data

Both are textbook, both censused zero here, both are near-certain on generated SQL
(a dbt model that selects 3 of 12 aggregates from a shared upstream model is the
canonical case). **Gate:** ≥1 site on the batch-1 program (§6). Do not build on
speculation.

### 5.4 `REDUCE_EXPRESSIONS` — measure, then decide

35 sites is the loudest number in the census and probably the smallest win.
**Gate:** a microbench of a predicate with a literal cast vs. the folded form, on
*both* compile paths. If the typed path folds it in the JIT and the structural path
is the only loser, the rewrite is worth it only where the structural fallback fires —
which is a different (and already-tracked) problem.

### 5.5 `JOIN_TO_SEMI_JOIN` — blocked on statistics, not on effort

`SemiJoinPlan` already exists; the recognizer is easy; the *distinctness proof* for
the right side is the blocker. Cheapest sound version: recognize only when the right
side is syntactically distinct-on-key (a `DistinctPlan`, or an `AggregatePlan` whose
group keys are the join keys). Worth doing in that restricted form.

### 5.6 Join reordering with a state-weighted cost model — the research item

Calcite's `MULTI_JOIN_OPTIMIZE` / `LoptOptimizeJoinRule` / dphyp machinery is the
best-developed part of the catalog and the part that transfers *least* directly: its
cost model counts rows produced, ours should count rows *retained*. In a DBSP circuit
the join order fixes which intermediate arrangements exist for the lifetime of the
pipeline, and shared arrangements (`options.ShareArrangements`) change the calculus
again — two orders with identical batch cost can differ by a whole materialized
intermediate. **This is a design note of its own, not a rule port.** It is also the
one place where reading Calcite's implementation (the conflict-detection / hypergraph
code) is worth more than porting it.

### 5.7 Decorrelation completeness — already P1 in `skipped.md`

`SubQueryRemoveRule` covers IN / EXISTS / scalar / ALL / SOME / NOT IN uniformly; our
resolver-level lifting covers IN (correlated + uncorrelated) and scalar subqueries,
and rejects the negated forms. Calcite's rule is the reference for the shapes we
reject — worth reading before extending, not worth porting.

## 6. What this census cannot see, and why that matters most

The corpus is hand-written SQL. Calcite's rule set exists because most SQL reaching a
production planner is *generated* — by BI tools, ORMs, and dbt — and generated SQL is
full of exactly the redundancy §3.3/§3.4 target: `WHERE 1=1`, restated CTEs, selected
subsets of wide aggregate models, nullable keys everywhere, boilerplate casts.

In this repo that corpus is the **batch-1 dbt program (50 views)**, which lives
behind `IVM_SPEC` + the SF=3 data on the Windows box (see the
`columnar-batch1-tooling-portability` note and `docs/design-cross-view-cse.md` §7,
which is gated on the same thing). `rulecensus` was written to be pointed at it:
teach it a program-level corpus loader and re-run. **Until then, no rule in §5.3
should be built.**

## 7. Next increments

1. **Extend the census** with detectors for the three §3.3/§3.4 rules currently
   marked "not censused" — `AGGREGATE_JOIN_TRANSPOSE` (decomposable-aggregate-over-join
   sites), `EXCHANGE_REMOVE_CONSTANT_KEYS` (constant in an exchange key, over the
   structural-parallel plans), `DateRangeRules` (`EXTRACT`/`FLOOR` on a timestamp
   column in a predicate). Cheap; each is one detector.
2. **Point the census at the batch-1 program** when the spec lands (§6) — this is the
   measurement that decides §5.3, and it is now on the batch-1 arrival checklist.
3. ~~**Build §5.1**, gated on its A/B.~~ **Done** — see §5.1 and
   `docs/design-row-representation.md` §18.6.
4. **Build §5.2** — small, unconditionally sound, and pre-pays for the batch-1 corpus.
   It is now the head of the queue.
5. **Write the state-weighted join-order design note** (§5.6) separately. Do not start
   it before the shared-arrangement work settles; the two share a cost model.

## 8. Honest bottom line

Mining Calcite gave us three things, in descending order of value: a **catalog of
soundness conditions** we would otherwise rediscover by writing bugs; a short list of
**genuinely missing state-shrinking rewrites** (§5.1, §5.2, §5.3, §5.5); and a list of
**rules to deliberately not adopt** (§3.5), which is the part a port-it-all approach
would get wrong. What it did *not* give us is a large pile of free wins on the current
corpus — the census says the hand-written benchmarks are close to rewritten-out
already, and the interesting sites are 3 blocked MIN/MAX narrowings and a handful of
one-off shapes. The catalog's leverage is real but it is **contingent on the batch-1
generated-SQL corpus**, and the single highest-value item on the list (§5.6, join
order under a state cost model) is one Calcite cannot give us at all.
