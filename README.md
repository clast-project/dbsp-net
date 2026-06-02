# DbspNet

A research-grade C# / .NET 10 reimplementation of the DBSP (Database Stream
Processor) incremental-computation model, as described in Budiu et al.,
*DBSP: Automatic Incremental View Maintenance for Rich Query Languages*
(VLDB 2023), and as exhibited in [Feldera](https://github.com/feldera/feldera)'s
Rust runtime. The code is written from the paper, not translated from Rust.

DBSP turns SQL queries into circuits that process input *changes* and emit
output *changes* — so a view can be kept up-to-date at a cost proportional
to the size of the change, not the size of the data.

This repository is a **prototype**: it targets a small, honest slice of SQL
end-to-end (CREATE TABLE / CREATE VIEW, SELECT with projection, filter, inner
join, group-by aggregates). It is not a production system and makes no
attempt at feature parity with Feldera. Features deferred from v1 are tracked
in [`docs/skipped.md`](docs/skipped.md) against Feldera's real surface.

For a map of the codebase — how a SQL query flows through the system,
what each operator does, where to plug in — see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Layout

```
src/
  DbspNet.Core         algebra, Z-sets, circuit runtime, operators
                       (both flat and spine/LSM trace families)
  DbspNet.Sql          SQL parser, logical plan, plan→circuit compiler
                       (structural + typed-row fast path),
                       expression compiler, plan optimizer
  DbspNet.Arrow        Arrow IPC boundary (RecordBatch ↔ Z-set delta)
  DbspNet.Persistence  WAL, end-of-tick snapshots, snapshot+WAL hybrid
                       over an IBlobStore abstraction
  DbspNet.Demo         runnable end-to-end scenarios
  DbspNet.Benchmarks   regenerates docs/benchmarks.md
tests/
  DbspNet.Tests        unit + property-based tests
ARCHITECTURE.md        codebase map: pipeline, operators, extension points
docs/
  design-notes.md      DBSP primer and implementation notes
  persistence.md       persistence/recovery design and what shipped
  benchmarks.md        regenerated; cold-batch vs incremental latency
  skipped.md           deferred features tracked against Feldera
```

## Build & test

```
dotnet build
dotnet test
dotnet run --project src/DbspNet.Demo
dotnet run --project src/DbspNet.Benchmarks -c Release
```

Requires .NET 10 SDK. See [`docs/benchmarks.md`](docs/benchmarks.md) for
performance numbers (cold-batch vs. steady-state incremental latency across
four query shapes).

## Walkthrough: one SQL query, running incrementally

The pipeline is `SQL text → Parser → Resolver → LogicalPlan → PlanToCircuit
→ CompiledQuery`. The `CompiledQuery` exposes input handles for each source
table and an output handle exposing the current *delta* Z-set after every
`Step()`. Push `INSERT`/`DELETE` deltas, call `Step()`, read the output —
that's the whole loop.

```csharp
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

var catalog = new Catalog();
var resolver = new Resolver(catalog);

// Register schema (populates the catalog so the query can resolve `orders`).
resolver.Resolve(Parser.ParseStatement(
    "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL)"));

// Parse + resolve the query into a logical plan, then compile to a circuit.
var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
    "SELECT cust, SUM(amount) AS total FROM orders GROUP BY cust"))).Query;
var query = PlanToCircuit.Compile(plan);

// Tick 1: two orders land.
query.Table("orders").Insert(1, 100);
query.Table("orders").Insert(2, 50);
query.Step();
// query.Current is the delta emitted this tick:
//   +1  (1, 100)
//   +1  (2, 50)

// Tick 2: customer 1 places another order. Incremental output retracts the
// previous total for customer 1 and emits the new one — customer 2 is
// untouched.
query.Table("orders").Insert(1, 25);
query.Step();
// query.Current:
//   -1  (1, 100)
//   +1  (1, 125)
```

`query.Current` is a `ZSet<StructuralRow, Z64>` — a weighted multiset of
rows. Positive weights are insertions, negative weights are retractions.
Summing the deltas tick-by-tick reconstructs the full view; the cost of each
`Step()` is proportional to the delta, not the size of `orders`.

The four canonical scenarios in [`src/DbspNet.Demo/Program.cs`](src/DbspNet.Demo/Program.cs)
(filter, inner join, group-by, joined group-by) run this loop with explicit
printouts and an end-of-run assertion that the accumulated output matches a
batch re-computation.

## What works today

- DDL: `CREATE TABLE` (with `NOT NULL`/`NULL`; `PRIMARY KEY` parsed but ignored), `CREATE VIEW`.
- Types: `INTEGER`, `BIGINT`, `REAL`, `DOUBLE PRECISION`, `DECIMAL(p,s)`,
  `VARCHAR`, `BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL`. `DECIMAL` is Arrow-
  aligned `Decimal128` (Int128 mantissa) via `Clast.DatabaseDecimal` with
  native arithmetic and SQL-Server/Substrait result-type promotion;
  `VARCHAR` is `Utf8String` (Arrow-aligned `ReadOnlyMemory<byte>`) with
  native UTF-8 equality, ordering, hashing, code-point `LENGTH`, and
  `Rune`-based invariant `UPPER`/`LOWER`. Temporal types use Arrow's
  `Date32` / `Time64[microsecond]` / `Timestamp[microsecond]` (naive).
  `INTERVAL` (`'…' YEAR`/`MONTH`/`DAY`/`HOUR`/`MINUTE`/`SECOND`, `YEAR TO
  MONTH`, `DAY TO SECOND`) is a `(months, microseconds)` value supporting
  `date`/`time`/`timestamp ± interval` (calendar-aware month add; DATE
  arithmetic is day-granular), `interval ± interval`, `interval ×/÷ numeric`,
  and `date − date` / `ts − ts` → interval.
- Queries: `SELECT` (list or `*`, with optional `DISTINCT`) with aliases and
  scalar expressions, `FROM`
  (single table, derived tables `(SELECT …) AS x`, `INNER JOIN … ON …`, or
  `LEFT [OUTER] JOIN` / `RIGHT [OUTER] JOIN … ON …`, with `ON` or
  `USING (cols)`), `WHERE`, `GROUP BY`,
  `HAVING`, set operations `UNION ALL` / `UNION` / `INTERSECT` / `EXCEPT`
  with per-column type unification (INTERSECT binds tighter; NULL=NULL for
  matching), `WITH … AS (…)` CTEs (a CTE referenced twice compiles
  to one shared subcircuit), and `WITH RECURSIVE … AS (base UNION ALL step)`
  for transitive-closure-style queries (fully incremental — semi-naïve on
  inserts, delete-and-re-derive on deletes — on both flat and spine traces).
  `ORDER BY … LIMIT [OFFSET]` (and `FETCH FIRST n ROWS ONLY`) compile
  to an incremental TOP-K operator; a bare `ORDER BY` (no limit) is a no-op
  since row order is unobservable in the output Z-set. The ranking window
  functions `ROW_NUMBER` / `RANK` / `DENSE_RANK` are supported in the
  partitioned TOP-K filter pattern — `SELECT … FROM (SELECT …, ROW_NUMBER()
  OVER (PARTITION BY p ORDER BY o) AS rn FROM …) WHERE rn <= k` — compiling to
  a per-partition TOP-K operator (`RANK`/`DENSE_RANK` keep whole tie groups; an
  empty `PARTITION BY` is a single global partition).
- Window aggregates `SUM` / `COUNT` / `AVG` / `MIN` / `MAX` `OVER (PARTITION BY p
  [ORDER BY o RANGE …])`, emitted as a new column on every row, for the three
  `RANGE` frame shapes: whole-partition (no `ORDER BY`), running (the default
  `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`), and bounded
  (`RANGE BETWEEN <const | day-time INTERVAL> PRECEDING AND CURRENT ROW`). Lowered
  to a `PartitionedWindowAggregateOp` that recomputes only the rows whose frame a
  tick changed (RANGE peer-group semantics; a bounded ascending frame over a
  `LATENESS` / temporal-filter key GCs old rows). The offset / value functions
  `LAG`/`LEAD(expr [, offset [, default]])` and `FIRST_VALUE`/`LAST_VALUE(expr)`
  `OVER (PARTITION BY p ORDER BY o)` are also supported (positional, via
  `PartitionedOffsetOp`; FIRST/LAST span the whole partition). `ROWS`/`GROUPS`
  frames and `FOLLOWING` bounds are deferred.
- Scalar subqueries (uncorrelated, exactly one column) in `WHERE`, `SELECT`,
  and `HAVING` expressions. Empty subquery → `NULL`; changing subquery
  value correctly retracts and re-emits outer rows.
- Expressions: literals, column refs, arithmetic (`+ - * / %`), comparison
  (`= <> < <= > >= IS [NOT] NULL`), boolean (`AND OR NOT`) with SQL three-valued
  logic, `CAST`, and `CASE WHEN ... THEN ... [ELSE ...] END` (both the
  searched and the simple `CASE x WHEN v ...` form). CASE branches evaluate
  lazily and an arm is taken only on a definite TRUE (NULL/FALSE fall through).
  The conditional functions `IIF(c, a, b)` and `DECODE(...)` desugar to CASE
  (DECODE matches `NULL = NULL`, per Oracle). `[NOT] BETWEEN` desugars to a
  comparison conjunction. The `||` string-concatenation operator propagates
  NULL (unlike PG-style `CONCAT`, which skips NULLs). `IS [NOT] DISTINCT
  FROM` gives NULL-safe (in)equality (always a definite boolean). `IS [NOT]
  TRUE` / `FALSE` / `UNKNOWN` are definite-boolean tests over a boolean
  operand (NULL never leaks through).
- Scalar functions: `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `UPPER`,
  `LOWER`, `LENGTH`, `CONCAT`, `SUBSTRING`/`SUBSTR`, `TRIM`/`LTRIM`/`RTRIM`
  (whitespace or a custom char set), `REPLACE`, `POSITION(x IN y)`/`STRPOS`,
  `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND`, `POWER`, `SQRT`, `SIGN`, `LN`,
  `LOG` (base-10, or `LOG(b, x)`), `EXP`. String functions are native UTF-8
  with code-point semantics. NULL semantics follow PostgreSQL (most propagate;
  `CONCAT`/`GREATEST`/`LEAST` skip NULLs).
- Aggregates: `SUM`, `COUNT(*)`, `COUNT(col)`, `MIN`, `MAX`, `AVG`, and
  `APPROX_COUNT_DISTINCT` (HyperLogLog; bounded state, exact in the
  small-cardinality regime). NULL skipping per SQL semantics; `COUNT(*)`
  counts all.
- `LATENESS` bounded-history GC: a column declared `LATENESS d` in
  `CREATE TABLE` (e.g. `ts TIMESTAMP NOT NULL LATENESS 10000000`) promises no
  future row's value falls more than `d` below the running maximum. The engine
  drops late rows at the input and advertises a monotone frontier
  (`max_seen − d`); a monotonicity analysis (`MonotonicityAnalyzer`) propagates
  it through filters, projections, joins, aggregates, and set ops, and every
  keyed stateful operator — `IncrementalAggregateOp`, inner / `LEFT` / `RIGHT`
  join, `DistinctOp`, and their spine siblings — garbage-collects trace state
  below the frontier. GC reduces *state*, never *output*: the last value of a
  collected group stays downstream. The frontier source (`max_seen`) is
  persisted, so a restored circuit resumes its late-drop consistently with the
  GC'd traces. This is what keeps long-running, append-mostly pipelines
  bounded-memory. On the spine traces, GC is compaction-folded — each batch
  records its min/max monotone-key projection, so `DropKeysBelow` dispatches
  per batch (whole-batch drop / keep-in-place / mask-filter) at O(touched
  batches) and preserves batch / level layout. The bound is written in
  native units (µs for `TIMESTAMP`, days for `DATE`, the integer itself for
  `BIGINT`); duration-literal sugar (`'1' HOUR`) and a first-class
  `INTERVAL` type are deferred.
- Temporal filters (`NOW()` / `CURRENT_TIMESTAMP`): the `mz_now()`-style
  advancing-clock feature. `NOW()` is an injected, monotone, persisted logical
  clock (host-driven via `RootCircuit.AdvanceTime` / `CompiledQuery.AdvanceClock`,
  microseconds — never the wall clock, so replay is deterministic), legal only
  in WHERE predicates of the form `ts {<|<=|>|>=} NOW() [± constant day-time
  INTERVAL]` (e.g. `WHERE ts > NOW() - INTERVAL '1' HOUR`; both operand orders;
  `BETWEEN` folds into one window). These lower to a time-driven
  `TemporalFilterOp` that emits inserts and retractions as the clock advances
  *with no new input* — a row is retracted on the tick the clock crosses its
  upper bound. `NOW()` anywhere else is a resolve error. The clock doubles as a
  watermark: a windowed (disappear-bounded) filter bounds both its own state and
  a downstream `GROUP BY` / join / `DISTINCT` on the time-key, reusing the
  LATENESS frontier machinery. Correctness is the redefined incremental≡batch
  oracle (output accumulated at the final clock equals the batch evaluated with
  `NOW` = that clock). Deferred: `CURRENT_DATE` / `CURRENT_TIME`, a spine
  sibling, and the typed fast path. See `docs/now-and-temporal-filters.md`.
- Plan optimizer (`DbspNet.Sql.Optimizer.PlanOptimizer`, explicit pass):
  predicate pushdown (through Project / Join / UnionAll / Distinct /
  Difference, respecting outer-join restrictions), projection
  composition, and aggregate-input column pruning. Apply with
  `Compile(Optimize(plan))`.
- Typed-row fast path: `PlanToCircuit.Compile` tries `TypedPlanCompiler`
  first — when in scope, every stage's stream carries a per-schema
  emitted struct rather than a boxed `StructuralRow`, with only the
  input/output boundary paying a structural↔typed conversion. Falls back
  to the structural compile transparently on anything out of scope.
- Arrow boundary (`DbspNet.Arrow`): `CompiledQuery.ToArrowDelta()` and
  `TableInput.PushArrow(RecordBatch[, weights])` for column-major,
  type-dispatched I/O; opt-in `PushArrowZeroCopy` aliases Arrow buffers
  without copying. Z-set weights ride on the wire as a trailing
  `__weight : Int64` column. IPC streaming via `WriteArrowStream` /
  `ReadArrowStream` (one-shot snapshots) and `OpenArrowDeltaWriter` /
  `ReadArrowStreamBatches` (multi-batch pipelines).
- Persistence (`DbspNet.Persistence`): WAL of input deltas (approach A),
  end-of-tick snapshots (B), and snapshot+WAL hybrid (C) — all layered
  on an `IBlobStore` abstraction modelled on S3 / GCS / Azure Blob, with
  built-in `LocalFileBlobStore` and `InMemoryBlobStore`. Every stateful
  SQL operator (`DistinctOp`, `IncrementalAggregateOp`,
  `IncrementalJoinOp`, `IncrementalLeftJoinOp`, `SemiNaiveFixpointOperator`)
  round-trips its trace as Arrow IPC; manifests carry plan + schema
  fingerprints so drift is caught on `Load`. See
  [`docs/persistence.md`](docs/persistence.md).
- Spine (`DbspNet.Core.Operators.Stateful.Spine`): LSM-style sibling
  trace family — `SpineZSetTrace` / `SpineIndexedZSetTrace` hold the
  integral as a tiered sequence of immutable sorted-columnar batches
  with configurable compaction, per-batch Arrow IPC snapshot, and
  optional disk spill via `SpineSpillConfig`. Every flat operator has
  a spine-backed counterpart (`SpineDistinctOp`,
  `SpineIncrementalAggregateOp`, `SpineIncrementalJoinOp`,
  `SpineIncrementalLeftJoinOp`). Emitted by the SQL compiler via
  `PlanToCircuit.Compile(plan, snapshotCodecs, new CompileOptions {
  TraceFamily = TraceFamily.Spine })` — on both compiler paths: the
  structural compile supplies `StructuralRowComparer` for the sorted
  batches; the typed-row fast path closes the spine operators over
  emitted struct row types whose `IComparable<TSelf>` is generated by
  `TypedRowEmitter`, so `Comparer<TRow>.Default` drives the sort
  without a per-schema comparer object. Snapshot round-trips and the
  random-query equivalence PBT both run green on the spine path.
  Recursive CTEs honour spine too: the nested fixpoint circuit's
  import-relation integrals use a `SpineZSetTrace` in spine mode (its loop
  body is stateless, so the import integral is the only trace).
- Runtime observability (`RootCircuit.CollectStats()` /
  `CompiledQuery.CollectStats()`): an opt-in, on-demand per-operator metrics
  snapshot over the registered operators — each stateful operator reports its
  retained-state size, last-tick output size, current GC frontier and cumulative
  GC drops (as an `OperatorStat` keyed by the operator's registration index).
  The headline use is watching trace state stay bounded as a `LATENESS` / clock
  watermark advances — you can see groups GC'd and the frontier climb.
  `RootCircuit.LastStepDuration` exposes the most recent tick's wall-clock cost
  for throughput tracking. Stateless operators are omitted; reads never touch the
  hot `Step` path (a spine trace's count materialises only when asked).
- Correctness: 840+ unit tests plus property-based tests (≥3000 CsCheck
  iterations) across 40 query templates, run both with and without the
  optimizer and on both trace families — semantic equivalence is
  continuously verified. A dedicated `LATENESS` property test checks that
  the incremental run with trace GC equals a batch evaluation over the
  non-late input. Persistence has end-to-end snapshot round-trip coverage
  for every stateful operator (in isolation and in compositions, including
  the `LATENESS` frontier), plus crash-point coverage for the atomic-write
  protocol.

## What's deferred

See [`docs/skipped.md`](docs/skipped.md). The short list of v1 restrictions
beyond "Feldera is much bigger":

- `INNER JOIN` supports any `ON` predicate, including non-equi
  (`ON a.x > b.y`) and `CROSS JOIN` — with no equi-key the join runs as a
  unit-key nested-loop cross product with the `ON` predicate applied as a
  residual filter. `LEFT` / `RIGHT` / `FULL [OUTER] JOIN` require at least one
  equi-key in `ON` and reject non-equi residual conjuncts (keyed
  match-presence tracking). Comma-join (`FROM a, b`, implicit cross join) is
  supported; `NATURAL JOIN` is not yet parsed.
- Subqueries cover all the row-filter shapes — scalar (one column),
  `IN (subquery)`, `NOT IN (subquery)` (full SQL 3VL including
  nullable operands), `EXISTS (subquery)`, and `NOT EXISTS (subquery)`
  — both uncorrelated AND **correlated** (single level; the resolver
  decorrelates against an inner-WHERE equi-predicate referencing an
  outer column). Correlated `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS`
  lift to a multi-key `SemiJoinPlan` (with `IsAnti=true` for the
  NOT-forms; compiled as `outer − SemiJoin(outer, sq)` via the
  existing Z-set subtraction). Nullable `NOT IN` adds a hidden
  per-correlation-group null-count column (via
  `CorrelatedScalarSubqueryJoinPlan` or `ScalarSubqueryJoinPlan`)
  filtered on `probe IS NOT NULL AND (null_count IS NULL OR = 0)`
  before the anti-semi-join. Correlated scalar lifts to a
  `CorrelatedScalarSubqueryJoinPlan` whose inner aggregate is
  augmented with a GROUP BY on the correlation columns.
  `IN (literal_list)` / `NOT IN (literal_list)` are flat-AST.
  In SELECT / HAVING / nested-boolean positions, the resolver lifts
  `IN` / `EXISTS` / `NOT IN` / `NOT EXISTS` to a hidden
  per-correlation-group `COUNT(*)` column layered via
  `CorrelatedScalarSubqueryJoinPlan`; the bound AST node rewrites to
  `COALESCE(count, 0) > 0` (or `= 0` when negated). Nullable-operand
  `IN` / `NOT IN` in these positions get the full SQL three-valued
  result: the resolver layers per-group match / total / null counts and
  emits a `CASE` (`match>0 → TRUE`, empty group → `FALSE`,
  NULL probe or a NULL subquery value with no match → `NULL`), with
  `NOT IN` the three-valued negation.
  **Deferred**: nested correlation (subquery-inside-subquery
  referencing grand-outer columns).
- `WITH RECURSIVE` evaluation is fully incremental (semi-naïve on inserts,
  delete-and-re-derive on deletes), but the body may reference only base tables
  and the self-ref — no aggregates, subqueries, outer joins, or nested recursion
  inside the body.
- Set ops: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` all supported;
  `INTERSECT ALL` / `EXCEPT ALL` (bag-semantics variants) are deferred.
- `CROSS JOIN` / non-equi `INNER JOIN` (unit-key nested loop) and
  `FULL OUTER JOIN` (symmetric both-sides match-presence tracking) are
  supported. `ORDER BY` / `LIMIT` / `OFFSET` / `FETCH FIRST` compile to
  incremental TOP-K — including ordering by non-selected columns / expressions
  (carried as hidden columns). Windowed `ROW_NUMBER` / `RANK` / `DENSE_RANK` in
  the partitioned TOP-K filter pattern are supported, as are window aggregates
  (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX` `OVER` with whole-partition / running / bounded
  `RANGE` frames) and the offset / value functions `LAG`/`LEAD`/`FIRST_VALUE`/
  `LAST_VALUE`; the general windowed *rank*-column form (a rank emitted on every
  row) and `ROWS`/`GROUPS` frames are deferred. `LIKE` / `ILIKE` / `SIMILAR TO` (with optional `ESCAPE`, default
  backslash) are supported — pattern matching lowered to a `Regex`, with the
  contextual keywords leaving `like`/`to`/`escape` usable as identifiers.
  `JOIN … USING` is supported (equi-join on the
  named columns + merged-column projection; FULL merges via `COALESCE`); the
  `SUBSTRING(s FROM a FOR b)` and `TRIM(LEADING|TRAILING| BOTH … FROM …)`
  keyword spellings are not (use the comma / char-set forms).
- Scalar function library covers the common arithmetic / string set
  listed above plus temporal functions `EXTRACT(field FROM …)` /
  `DATE_PART`, `DATE_TRUNC`, `DATEADD`, `DATEDIFF` (dispatched through an
  `IScalarFunction` registry; `DATEADD`/`DATEDIFF` take a string-literal unit,
  not SQL Server's bare keyword) and the POSIX-regex functions `REGEXP_LIKE` /
  `REGEXP_REPLACE` / `REGEXP_SUBSTR` (optional `flags`, PostgreSQL replace
  semantics). Missing pieces include other math (`SIN`/`COS`/`TAN`, `MOD`). `NOW`/`CURRENT_TIMESTAMP` is not in the registry —
  being non-deterministic, it ships instead as advancing temporal filters (see
  the streaming-features list above).
- `NULL` literal has a concrete type (`INTEGER NULL`) rather than the
  polymorphic "unknown" of PostgreSQL.
- `INTERVAL` (core) and date/time arithmetic are supported; deferred pieces
  are `INTERVAL` *stored columns* through the Arrow codec (intervals are
  intermediate-only today), `interval × decimal`, and typed-fast-path temporal
  arithmetic (it falls back to the structural compiler, as temporal
  comparisons do).

## License

MIT. See [LICENSE](LICENSE).
