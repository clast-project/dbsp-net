# ivm-bench gap analysis

Audit of what DbspNet would need to run [ivm-bench](https://github.com/mdrakiburrahman/ivm-bench),
a TPC-DI-based Incremental View Maintenance benchmark that compares engines through a
shared dbt model DAG. Feldera participates, which makes it the reference implementation
for our engine class (DBSP, view-definition-only SQL, mutation through connectors).

Audited against commit `main` as of 2026-07-15. Cross-referenced against `skipped.md`
and the resolver.

## Scope: what the benchmark actually asks for

Each engine gets a dbt project of 50 models (bronze → silver → gold), expressing the
TPC-DI data model *without* dbt's `incremental` materialization — the README's stated
reason is that "watermark columns are unreliable". Queries are aligned to the "lowest
common denominator" across engines so that systems with different incrementalization
ability stay comparable.

The Feldera project (`src/containers/dbt-server/dbt-projects/feldera`) carries 70 models:
the same 50 plus 20 in `models/sources/` that declare input tables. Sibling projects
declare inputs via dbt `source()`; Feldera materializes each as a model so the adapter
can emit `CREATE TABLE` with an input connector attached. Every downstream
`{{ source('tpcdi', X) }}` becomes `{{ ref('X') }}`.

**Everything is a view.** No `INSERT`/`UPDATE`/`MERGE` appears anywhere, so DbspNet's
by-design absence of statement-level DML (`skipped.md:34-37`) does not bite. This is the
single biggest reason the benchmark is a plausible target despite TPC-DI being nominally
an ETL workload.

## The output contract: full state, not deltas

**Engines are measured on materializing and writing full current state.** This was not
obvious and is worth stating plainly, because the natural DBSP instinct — emit deltas —
would produce a fast but non-comparable number.

Evidence, all from `dbt-projects/feldera/dbt_project.yml` and the harness:

- All 16 gold output connectors are `delta_table_output` with **`mode: truncate`** —
  full snapshot replace per batch. Zero `append`-mode outputs in the repo.
- Gold models are `+materialized: view` **plus `+stored: true`**, which in dbt-feldera
  maps to `CREATE MATERIALIZED VIEW` — full view contents retained, not just the
  minimal state needed for future changes.
- No `__feldera_op` / `__feldera_ts` change-tagging columns anywhere. Output schema is
  the plain model SELECT.
- Write-out is **inside** the measured window. The timer starts at pipeline `resume`
  (`benchmark-server/services/engine_runner.py:786-789`) and stops only when
  `poll_output_completion()` (`dbt-server/services/feldera_client.py:93`) observes every
  output connector drained *and* the DBSP transaction commit finished — the latter
  explicitly including state persistence to storage.
- Rust compile time is measured separately and excluded (`engine_runner.py:813`).

Two gold views have had their output connector **deliberately removed** because
truncate-mode could not keep up. Quoting `dbt_project.yml` on `fact_market_history`:

> At SF=100, the truncate-mode `delta_table_output` for this 54M+ row materialized view
> never drains — every input batch rewrites the full snapshot, the connector queue grows
> unboundedly (1100+ batches with 0 transmitted observed), and `pipeline_complete` never
> trips. Drop the output connector so Feldera computes the view in-memory but skips the
> Delta write that wedges the pipeline. The benchmark still measures the compute cost;
> no Delta is persisted for this table.

Same for `daily_market_pulse` (`dbt_project.yml:426-434`). So Feldera is charged for
compute + full snapshot rewrite on 16 of 18 gold views, and compute-only on the other
two.

### Correctness is not enforced on this path

The `EXCEPT ALL` validation is **engine-internal self-consistency**, not a cross-engine
diff: it builds a temp table from the compiled SQL inside the same DuckDB and diffs it
against that engine's own IVM-maintained relation
(`dbt-server/services/openivm_validation.py:116-125`). It exists only for
`duckdb-openivm` and `spark-openivm`. Grepping the validation services for `feldera`
returns nothing — Feldera's Delta output is never read back.

**Implication.** Nothing in the harness would catch an engine that emits deltas instead
of materializing. The contract that forces materialization is the *timer* (sinks must
drain), not the correctness gate. Emitting deltas would post a fast number while doing
strictly less work than Feldera is charged for. If we participate, we should match the
measured work: full state + full rewrite per batch.

### Batch model

The pipeline is long-lived and holds state across all three batches. Batches 2/3 are
pause → append input → resume → poll (`engine_runner.py:2428-2450`); data loading happens
while paused, so it sits outside the window. Batch 1 compiles the pipeline paused before
measurement starts.

Streaming engines have no clean batch-done signal here: `pipeline_complete` never trips
for `snapshot_and_follow` inputs, so the harness falls back through three signals ending
in a **120-second quiescence timer** (`FELDERA_QUIESCENCE_S`), which appears to be
included in the reported duration (`feldera_client.py:273`). Batches 2/3 are 1–2%
appends and are the most likely to terminate that way. Any comparison against Feldera's
published batch-2/3 numbers should account for that constant.

## Gaps, ranked

### 1. Ranking window functions projected into the output — BLOCKING, architectural

Affects all 5 analytics models (`models/gold/analytics/*.sql`): 12 call sites.

```sql
DENSE_RANK() OVER (ORDER BY bw.total_notional DESC) AS rank_by_notional   -- broker_performance.sql:77
RANK()       OVER (ORDER BY wl.total_volume DESC)   AS rank_by_volume     -- daily_market_pulse.sql:76
RANK()       OVER (ORDER BY sv.return_volatility DESC, sv.dm_s_symb) AS … -- market_volatility.sql:79
```

DbspNet rejects this at `Resolver.cs:988` — "the window rank column may only be used in
the TOP-K filter; selecting or otherwise [using it]" — and `skipped.md:411-416` marks the
general windowed-column form **[P2]**, on the grounds that one insert shifts the rank of
every later row in the partition, producing O(partition-size) retractions.

**The cost analysis is correct. The Feldera-parity justification is stale.**
`skipped.md:415-416` currently reads "Feldera restricts the rank functions to TopK
patterns for the same reason; DbspNet does too". Feldera's current documentation says
otherwise — the ranking functions are supported generally, with a cost warning rather
than a restriction:

> These functions can have a reasonable cost in three circumstances: each modified group
> (created by `PARTITION BY`) is relatively small in size; new insertions and deletions
> feature rows that appear towards the end of the order produced by the `ORDER BY`
> clause; they are used in a TopK pattern with a small limit.
> — <https://docs.feldera.com/sql/aggregates/>

Older Feldera docs did carry the TopK-only restriction, which is presumably where our
claim came from. It has since been lifted.

**Worst case, and it is observed.** These ranks are *unpartitioned* — `OVER (ORDER BY x
DESC)` with no `PARTITION BY` means the whole relation is one partition, which is exactly
the shape Feldera's cost note warns about. ivm-bench hit that wall in practice
(`dbt_project.yml:427-433`):

> At SF=100 the global-window aggregations (RANK + LAG over the full daily_market space,
> plus CROSS JOIN + NOT EXISTS) keep the DBSP circuit producing a long stream of
> incremental updates that the truncate-mode Delta writer cannot drain.

So: implementing this is required to run the benchmark, Feldera does implement it, and
it is genuinely expensive in precisely the way `skipped.md` predicted. Both things are
true. Feldera pays the cost and takes the loss on those views rather than refusing the
query.

Infrastructure that already exists and should be reusable: `PartitionedTopKOp` (rank
machinery for the filter pattern) and `PartitionedWindowAggregateOp`'s affected-range
recompute (which is how bounded churn is kept sound for window aggregates).

### 2. Nested `ROW` structs — DONE (by flattening)

`models/sources/batch1_customer_mgmt.sql` declares five levels of nested `ROW(...)`,
consumed by `bronze/crm/crm_customer_mgmt.sql` via three-deep dotted paths like
`cm.Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE`.

The decisive scoping finding: **every** use of the structs bottoms out at a scalar leaf —
no query selects, constructs, compares, or passes a whole `ROW`. So the runtime never needs
a struct value. Shipped by **flattening** at CREATE TABLE — a ROW column expands into
dotted-name scalar leaf columns, dotted access resolves to those leaves, and the runtime
(`StructuralRow`, expression compiler, typed codec, Arrow) is entirely untouched. Work
confined to the parser (ROW-spec + multi-dot access + keyword-as-field-name) and the
resolver (`ExpandRowColumn`).

Chosen over a first-class `SqlRowType` because it's far smaller and this is the only
nested-data model in the benchmark. The limitation — no whole-struct selection / `SELECT *`
of structs — is unexercised here. The first-class alternative (and why **arrays**, unlike
structs, cannot be flattened and would force it) is captured in
`docs/design-nested-types.md` for when a workload needs a composite *value*.

Note gap 2 unblocks the *struct* part of `crm_customer_mgmt`; that model also needs two
gap-6 tail items to run fully — `CONCAT_WS` and `CAST(bigint AS timestamp)`.

### 3. Multi-key window `ORDER BY` — DONE

Shipped. Scope turned out to be a third of what this entry first implied, for two
reasons found by reading the code rather than the docs:

- **The rank family was already multi-key.** `PartitionedTopKPlan.SortKeys` is a list,
  the resolver populates every `OVER` term with no arity check, and `PartitionedTopKOp`
  takes an opaque `IComparer<TRow>`. `financials`' seven-key `ROW_NUMBER` already
  resolved. Multi-key only opts out of the §22 narrowing optimization (perf, not
  correctness).
- **The benchmark needs no multi-key window *aggregates*.** Its aggregate windows are
  whole-partition (`MAX(ts) OVER (PARTITION BY k)`) or running on one key. Bounded
  RANGE is inherently single-scalar-key anyway — `FrameFor` does `v - _preceding.Value`
  on a `long` — so that restriction stays, now with an accurate error message.

So the work was the **offset family only**: `WindowOffsetPlan.OrderKey` → `OrderKeys`,
and the three consumers (structural, typed, batch oracle) that fed 1-element arrays to
an already-N-key `SortKeyComparer`. The single rejection had been sitting in
`BuildWindowGroup`'s prologue above the `if (isOffset)` branch, catching a family that
didn't need catching.

Also fixed here: `TryMatchRankFilter` now accepts `rank = 1` (`financials`' spelling).
Only `= 1` — ranks start at 1 so `= 1` ≡ `<= 1`, but `= k` for k > 1 selects a single
rank, which TOP-K cannot express, and must not be read as `<= k`.

**Testing note worth keeping.** `PlanToCircuit.Compile` tries typed first, so any
window test carrying a `PARTITION BY` exercises the *typed* path only; the structural
path is reached only with no `PARTITION BY`. Mutation testing caught this — a
first-key-only mutation of the structural path passed the entire suite until a
no-`PARTITION BY` case was added. Likewise, a multi-key test whose tie-break runs ASC
can pass even when the extra keys are dropped, because the full-row tiebreak
(`StructuralRowComparer`) reproduces ASC order by coincidence; the DESC tie-break case
is what actually pins the behaviour. Both traps are live for anyone extending this.

---

*Original entry, for context:*

`skipped.md` marked this **[P3]**, but it sits on the workload's critical path.

The SCD Type 2 end-dating idiom appears in 6 silver models (`accounts:42`,
`customers:42`, `companies:22`, `securities:22`, `financials:34`, `trades_history:25`):

```sql
LAG(effective_timestamp) OVER (PARTITION BY key ORDER BY effective_timestamp DESC, <tiebreakers>)
  - INTERVAL '0.001' SECOND
```

`financials` is the extreme case — `ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY
effective_timestamp DESC, year DESC, quarter DESC, posting_date DESC, company_name,
revenue, earnings) = 1`: seven sort keys, mixed ASC/DESC.

These six SCD2 models produce the validity intervals consumed by the 17 `BETWEEN` range
joins across 11 models. This idiom is the structural spine of the workload.

Note `financials` uses `= 1` rather than `<= 1`; `Resolver.cs:1060` matches `< k` / `<= k`
(and reversed). Trivially adjacent, but check it.

### 4. OUTER JOIN, two separate gaps — 4a DONE, 4b BLOCKING

The original entry here ("outer join with a non-equi conjunct, bounded") was wrong on
both diagnosis and size. Reading the actual models rather than the summary:

```sql
-- silver/securities.sql and silver/financials.sql, both branches
left join {{ ref('companies') }} c1
    on cast(s.cik as varchar) = cast(c1.company_id as varchar)
    and pts between c1.effective_timestamp and c1.end_timestamp
left join {{ ref('companies') }} c2
    on s.company_name = c2.name
    and pts between c2.effective_timestamp and c2.end_timestamp
```

The `c1` branch's join key is `cast(…) = cast(…)`. `TryExtractEquiKey` required **bare
`ResolvedColumn` on both sides**, so that equality was classified as a *residual*,
leaving `c1` with **zero** equi-keys — meaning it failed a different check first
("`LEFT JOIN requires at least one equi-key (v1)`"), not the non-equi-conjunct one. The
`c2` branch has a real equi-key and fails only on the residual. Two gaps, not one.

Also checked and ruled out: the cheap fix. If the residual were right-side-only it would
be a filter pushdown into the right input and nothing else. It isn't — `pts` comes from
the left, so `pts BETWEEN c1.effective_timestamp AND c1.end_timestamp` is cross-side and
the per-left-row match problem is real.

#### 4a. Computed equi-keys — DONE

Lifted the bare-column restriction as a resolver lowering: side-pure key expressions are
hoisted into synthetic columns projected onto each input, the join keys on those, and a
projection above strips them. Nothing downstream sees an expression key.

Worth having independently of this benchmark: *any* `JOIN ON UPPER(a.x) = b.y` previously
compiled to a keyless unit-key cross product plus a filter — correct, quadratic, and
silent. This is the same generalization `GROUP BY` got when it lifted its own
bare-column-only rule.

#### 4b. Outer join with a cross-side residual — DONE

Match-presence was a per-**key** emptiness test — `IncrementalLeftJoinOp.cs:153-154` is
`var oldMatched = !oldR.IsEmpty; var newMatched = !newR.IsEmpty;`, asking "does *any*
right row exist under this key". A residual can reference left columns, so two left rows
under one key can disagree about whether that key is matched: the property becomes
per-(key, left row), the O(1) check becomes O(|R_key|) per left row, and the
stayed-matched fast path (which never enumerates `oldL × oldR`) collapses.

Shipped as a **plan rewrite** (`PlanToCircuit.CompileOuterJoinWithResidual`), not
operator surgery.

```
matched   = σ_residual( L ⋈_key R )        -- INNER already fuses residuals
unmatched = L − antisemi( L, π_L(matched) )
result    = matched ∪ NULLPAD(unmatched)
```

Rationale: it reuses tested machinery, inherits GC/snapshot/spine for free, and matches
the codebase's lowering-over-new-ops grain (TUMBLE, operator fusion). Gap 4b unblocks two
models — thin justification for surgery on the operator that FULL OUTER, LATENESS GC, and
the spine family all depend on. If it shows up hot, operator surgery becomes a *measured*
optimization with the rewrite as its correctness oracle.

The rewrite, for each preserved side `S`:

```
matched   = σ_residual( L ⋈_key R )
unmatched = S − antisemi( S, π_S(matched) )
result    = matched ∪ NULLPAD(unmatched…)
```

FULL applies it to both sides independently. Three subtleties, all found by building the
PBT first (it failed until each was handled):

- **The keyless-outer restriction dissolved for free** — the rewrite never uses the keyed
  operator, so a keyless outer join lowers as a unit-key cross product (correct,
  quadratic). Both resolver rejections (`equi.Count == 0` and non-equi conjunct) were
  removed together.
- **NULL-safe anti-join.** The anti-join keys on the **whole** preserved row and must NOT
  drop NULL-bearing rows — unlike `CompileSemiJoin`, whose keys are SQL probe values
  where `NULL = x` is never TRUE. Here the key is row identity, so `NULL = NULL`. Dropping
  NULL rows would let a matched row survive the subtraction and emit a spurious NULL-pad
  beside its join output. Reached only when the residual does *not* reference the nullable
  column (otherwise NULL there makes the residual non-TRUE and the row can't match) — so
  the PBT needed a dedicated shape (`n LEFT JOIN t ON a.k=b.k AND b.v > p`) to exercise
  it; mutation testing confirmed both the targeted test and that shape catch it.
- **Presence, not positivity.** The match set is collapsed by `Distinct(x) + Distinct(−x)`
  (weight ≠ 0), not `Distinct` alone (weight > 0, via `TWeight.IsPositive`). The join
  operators define match as `!IsEmpty` (≠ 0), and the PBT feeds arbitrary ±1 streams, so
  accumulated weights genuinely go negative and the two notions diverge. Projecting before
  set-ifying would also sum a preserved row's matches and could cancel to zero — set-ify
  the pairs first, so the projection can only sum upward.

`matched` is consumed once per preserved side plus once for output; at the circuit level
the stream is a value, so reusing the variable shares the operator — no CSE needed.

The independent oracle: `BatchPlanEvaluator` now reads `plan.Residual` in all three outer
branches (it ignored it before, deciding on `matches.Count > 0`). `BatchFullOuterJoin`
needed its right-pad pass changed from a per-**key** `matchedRightKeys` set to a
per-**row** `matchedRightRows` set — the same per-key-is-wrong lesson as the operator.
Because the node keeps its natural shape, the 3000-iteration PBT compares two genuinely
independent implementations.

Most of the 17 `BETWEEN` joins are INNER and were always fine — non-equi INNER via the
unit-key nested-loop path. This closes the LEFT-joined ones.

### 5. `STDDEV` — DONE

11 call sites across all 5 analytics models, always unqualified `STDDEV`.

Confirmed bare `STDDEV`/`VARIANCE` = the **sample** forms (÷ n−1) in PostgreSQL, Spark,
and DuckDB — all three ivm-bench participants — so `STDDEV` maps to `STDDEV_SAMP`. Getting
this wrong would make the numbers silently incomparable rather than error.

Shipped the whole family (`STDDEV`/`STDDEV_SAMP`/`STDDEV_POP`, `VARIANCE`/`VAR_SAMP`/
`VAR_POP`) as one invertible `SqlStddevAggregator` holding the three moments `n`,
`Σ(value·weight)`, `Σ(value²·weight)`. The moment form is the only one that inverts under
arbitrary signed retraction — Welford's online update can't remove an arbitrary element —
at the cost of floating-point cancellation, guarded by clamping negative variance to zero
before the square root. One class serves the circuit and the batch oracle.

Two test layers, and mutation testing proved they cover **distinct** failure classes: the
known-dataset test (hand-computed pop/sample values on {2,4,4,4,5,5,7,9}) catches formula
errors; the differential PBT catches invertibility errors (`Update` fold ≠ `Compute`
rescan). A formula mutation passed the PBT — because both circuit and oracle share
`Finish` — but failed the known-dataset test; an `Update`-only mutation passed the
known-dataset test but failed the PBT on a retraction-heavy counterexample. Both are
needed. This also gave AVG its first PBT coverage (the generator had none).

### 6. Small, well-scoped additions — DONE except named `WINDOW`

| Gap | Sites | Status |
|---|---|---|
| `MD5` | 6 dim models | **DONE**. Transitively required — `dbt_utils.generate_surrogate_key` expands to `md5(cast(coalesce(...) \|\| '-' \|\| ... as varchar))`. 32-char lowercase hex over UTF-8 bytes (PG/Spark-compatible), structural-only. |
| `CONCAT_WS` | 6 | **DONE**. `crm_customer_mgmt`. Separator-join skipping NULL values; NULL sep → NULL. Structural-only (variadic). |
| `RLIKE` | 2 | **DONE**. `finwire_financial:22`, `finwire_security:17`. Infix contextual-keyword arm desugaring to `REGEXP_LIKE`. |
| `TINYINT` / `SMALLINT` | 3 columns | **DONE**. Parsed, widened to `INTEGER` (lossy, like `CHAR`→`VARCHAR`). |
| `CAST(bigint AS TIMESTAMP)` | 2 | **DONE**. `cdc_dsn`. Value = MICROSECONDS since epoch (DuckDB convention; Spark=seconds is a one-line swap). Both compile paths. |
| Typed temporal literals | 6 | **DONE**. `TIMESTAMP '…'` / `DATE '…'` / `TIME '…'` parse to a constant `CAST(string AS type)`, mirroring `INTERVAL '…'`. |
| Named `WINDOW` clause | 2 models | **Deferred** — see below. `silver/daily_market.sql`, `gold/dim_customer.sql`; inlining the spec is equivalent, so a forward-reference resolver substitution, structurally different from the rest of the tail. |

### 7. Verified non-issues

Worth recording, because several looked like blockers on first pass.

- **Window frames.** The project contains **zero** explicit frame clauses — no
  `ROWS`/`RANGE`/`GROUPS`, no `PRECEDING`/`FOLLOWING`/`UNBOUNDED` anywhere. Everything
  relies on the SQL default `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which is
  exactly DbspNet's supported shape (`Resolver.cs:1499-1520`: RANGE-only, end bound
  `CURRENT ROW`, start `UNBOUNDED PRECEDING`). Windows without `ORDER BY` get the
  whole-partition shape, also supported. Our `ROWS`/`GROUPS` and `FOLLOWING` gaps do not
  bite.
- **Cross-temporal CAST** (`CAST(ts AS DATE)`, ~10 sites) is **shipped** —
  `ExpressionCompiler.cs:610,618` implement both directions. See doc corrections below.
- **Correlated `NOT EXISTS`** (4 models) is shipped. See doc corrections below.
- **Typeless NULL** (`skipped.md:744-747`) does not bite: the 3-way `UNION ALL` in
  `crm_customer_mgmt:119,121` pads branches with typed `CAST(null AS <type>)`.
- **Identifier quoting**: the project sets `quoting: identifier: true`, so dbt emits
  `"double quotes"` — the only form DbspNet parses (`skipped.md:681-683`). No `::` casts
  either; the Feldera project uses `CAST(...)` throughout (the duckdb project uses `::`).
- **`SUBSTRING(s, pos, len)`** — comma form only, which is what we parse. The
  `SUBSTRING(s FROM a FOR b)` keyword spelling we don't parse never appears.
- **No `ORDER BY` / `LIMIT`** anywhere in the project, so the TOP-K path is not exercised
  at query level.
- **`COUNT(DISTINCT CAST(ts AS DATE))`** — `COUNT(DISTINCT <expr>)` is supported.
- Supported and used throughout: `UNION ALL` (2 sites), CTEs (pervasive), `CROSS JOIN`
  (5, each against a 1-row global-aggregate CTE), `JOIN … USING` (5), `HAVING` (4),
  self-joins (4 models), `CASE` simple + searched, `COALESCE`, `NULLIF`, `ROUND`,
  `TRIM`, `POSITION`, `DECIMAL(38,4)`/`(38,6)`, `INTERVAL '0.001' SECOND`.

## Precedent: engine-specific rewrites are legitimate

The Feldera project already carries documented workarounds for its own engine's gaps:

- `LAST_VALUE(x IGNORE NULLS)` → a `COUNT(col) OVER w` cumulative-group-id +
  `MAX(col) OVER (PARTITION BY key, grp)` forward-fill (`gold/dim_customer.sql`, 22
  columns each way). The duckdb project uses `IGNORE NULLS` directly.
- `strptime` / `::DATE` → `SUBSTRING` + `||` + `CAST` (the three finwire models).
- `regexp_matches` → `RLIKE`.
- `source()` → `ref()`.

So a `dbspnet` dbt project may legitimately carry its own rewrites for the §6 tail. The
line worth holding: rewrites that preserve the query's algorithmic shape are fair;
rewrites that change it are not. Dodging §1 by restructuring the analytics models into
TopK filters would mean measuring a different query — that one has to be built or
declared out of scope.

## Corrections to `skipped.md` found during this audit — FIXED

All three were stale in a way that made this benchmark look harder than it is. Fixed as
a standalone docs commit before implementation started; recorded here because the
*reason* each was wrong is worth keeping.

1. **CAST matrix** claimed cross-temporal casts (`date ↔ timestamp`) were missing. They
   are implemented — `ExpressionCompiler.cs:610` (`CAST(timestamp AS date)`, floors to
   the day) and `:618` (`CAST(date AS timestamp)`, midnight). TPC-DI leans on this
   heavily (~10 sites). The entry now scopes the remaining gap to numeric→temporal,
   which is real (§6).
2. **`NOT IN (subquery)`** was listed **[P1]** deferred under the uncorrelated-subquery
   bullet, contradicted by the very next bullet describing it as implemented with full
   3VL. The doc-comment at `Resolver.cs` likewise claimed correlated `NOT EXISTS`
   "rejects with a deferred message — the anti-semi-join primitive isn't in v1", three
   lines above its own `IsAnti: isNegated`. Implemented (`Resolver.cs:2977`, `:3055`) and
   tested (`AntiSemiJoinTests.cs`, `NullableNotInTests.cs`). The benchmark uses
   correlated `NOT EXISTS` in 4 models.
3. **Feldera rank parity** — see §1. The entry now records that Feldera supports the
   general form, keeps the (still-correct) cost analysis, and cites this document.

## Standup results (2026-07-16) — measured, corrects the static gaps

Ran every feldera model through resolve+compile (minimal Jinja expansion: `{{ ref }}` →
name, `generate_surrogate_key` → its `md5(coalesce/cast/||)` form; sources wrapped as
`CREATE TABLE`, views registered in dependency order). Outcome:

- **Sources: 20/20 register** — including `batch1_customer_mgmt`'s 5-level nested `ROW`.
- **Views: 17/50 compile** today. The other 33 reduce to **three root causes**; ~21 of
  them are pure cascades (`unknown table X` because X failed upstream).

The three roots, ranked by cascade impact:

1. **Window function nested in an expression** — the SCD2 `is_current` marker:
   `CASE WHEN action_ts = MAX(action_ts) OVER (PARTITION BY key) THEN true ELSE false END`.
   The resolver requires a window function to be a top-level SELECT item. Roots in 5 silver
   models (`accounts`, `companies`, `customers`, `securities`, `trades_history`) and
   cascades to nearly every downstream dim/fact. **The #1 blocker, and it was missed** — the
   static analysis logged `MAX(ts) OVER (PARTITION BY key)` as a supported whole-partition
   shape but didn't notice the enclosing `CASE`. Fix shape: lift the window aggregate to a
   hidden column and rewrite the expression to reference it.

2. **Typeless NULL** — bare `null` resolves to `INTEGER NULL`, which then won't unify. Bites
   two ways: a `CASE` branch of bare `null` against a typed other branch
   (`finwire_company` null-vs-DATE, `finwire_financial`/`finwire_security`/`watches_history`
   null-vs-VARCHAR), and `CAST(null AS DATE)` (`crm_customer_mgmt`, the one **compile**
   failure — `INTEGER→DATE` isn't in the cast matrix, but the real cause is the bare null).
   This is `skipped.md`'s **[P1]** typeless-NULL gap. **The static analysis explicitly called
   it a non-issue** (the 3-way UNION pads with typed `CAST(null AS …)`) — true for the UNION,
   but it missed the `CASE` branches and `CAST(null AS DATE)`. Fix shape: make a bare-null
   literal adopt its context type (CASE-branch unification, CAST target, comparison peer).

3. **Named `WINDOW` clause** — `daily_market`, `dim_customer` (+ cascade to
   `market_volatility`, `fact_market_history`, `daily_market_pulse`). The gap-6 item already
   deferred.

Gap 1 (rank-in-output) did **not** surface — the analytics models that use it all failed as
`unknown table 'fact_trade'` (cascade behind root #1). It's still there, masked; it reappears
once the cascade clears.

**Net: the real remaining critical path is #1 (window-in-expression) + #2 (typeless NULL),
neither of which was on the ranked list below.** Both are resolver work of similar weight to
the completed gaps. A throwaway harness (`tests/DbspNet.Tests/Scratch/IvmBenchStandup.cs`,
uncommitted) reproduces these numbers for measuring progress.

## Summary

| # | Gap | Models hit | Class |
|---|---|---|---|
| 1 | Rank projected into output | 5 (all analytics) | Architectural — design decision |
| 2 | Nested `ROW` structs | 2 | **DONE** (flattened) |
| 3 | Multi-key window `ORDER BY` | 7 | **DONE** |
| 4a | Computed equi-keys | 2 (+ latent perf bug) | **DONE** |
| 4b | OUTER JOIN + cross-side residual | 2 | **DONE** |
| 5 | `STDDEV` | 5 | **DONE** |
| 6 | Scalar/type tail | ~10 | Trivial, partly dodgeable |

Items 1–5 are the critical path. Item 1 gates 5 of 50 models and is a design decision
rather than a coding task; the rest are ordinary implementation work. Agreed order:
3 → 4 → 5 → 2, with 1 to be decided. **3, 4a, 4b, 5 and 2 are done — the whole "bounded"
critical path plus the struct gap.** Remaining: the gap-1 decision (rank-in-output), and
the gap-6 scalar/type tail (`CONCAT_WS`, `MD5`, `RLIKE`, `TINYINT`, `CAST(bigint AS
timestamp)`, typed temporal literals, named `WINDOW`).

Input/output adapter work is **not** on the critical path and should follow, not lead —
the SQL surface is what determines whether the benchmark can run at all. See the
connector notes in this repo's memory and `08-feldera-internals.md` §6 in the external
research journal (transport vs. integrated connectors; Delta Lake is *integrated*,
decoding Parquet to Arrow `RecordBatch`es and pushing structured records straight into
`InputHandle` with no generic parser).
