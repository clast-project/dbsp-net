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
  `VARCHAR`, `BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP`. `DECIMAL` is Arrow-
  aligned `Decimal128` (Int128 mantissa) via `Clast.DatabaseDecimal` with
  native arithmetic and SQL-Server/Substrait result-type promotion;
  `VARCHAR` is `Utf8String` (Arrow-aligned `ReadOnlyMemory<byte>`) with
  native UTF-8 equality, ordering, hashing, code-point `LENGTH`, and
  `Rune`-based invariant `UPPER`/`LOWER`. Temporal types use Arrow's
  `Date32` / `Time64[microsecond]` / `Timestamp[microsecond]` (naive).
- Queries: `SELECT` (list or `*`) with aliases and scalar expressions, `FROM`
  (single table, derived tables `(SELECT …) AS x`, `INNER JOIN … ON …`, or
  `LEFT [OUTER] JOIN` / `RIGHT [OUTER] JOIN … ON …`), `WHERE`, `GROUP BY`,
  `HAVING`, set operations `UNION ALL` / `UNION` / `INTERSECT` / `EXCEPT`
  with per-column type unification (INTERSECT binds tighter; NULL=NULL for
  matching), `WITH … AS (…)` CTEs (a CTE referenced twice compiles
  to one shared subcircuit), and `WITH RECURSIVE … AS (base UNION ALL step)`
  for transitive-closure-style queries (semi-naïve incremental evaluation
  on pure-insert ticks; see `docs/skipped.md` for the retraction-fallback
  caveat).
- Scalar subqueries (uncorrelated, exactly one column) in `WHERE`, `SELECT`,
  and `HAVING` expressions. Empty subquery → `NULL`; changing subquery
  value correctly retracts and re-emits outer rows.
- Expressions: literals, column refs, arithmetic (`+ - * / %`), comparison
  (`= <> < <= > >= IS [NOT] NULL`), boolean (`AND OR NOT`) with SQL three-valued
  logic, `CAST`.
- Scalar functions: `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `UPPER`,
  `LOWER`, `LENGTH`, `CONCAT`, `ABS`, `FLOOR`, `CEIL`/`CEILING`, `ROUND`,
  `POWER`, `SQRT`. NULL semantics follow PostgreSQL (most propagate;
  `CONCAT`/`GREATEST`/`LEAST` skip NULLs).
- Aggregates: `SUM`, `COUNT(*)`, `COUNT(col)`, `MIN`, `MAX`, `AVG`. NULL
  skipping per SQL semantics; `COUNT(*)` counts all.
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
  `IncrementalJoinOp`, `IncrementalLeftJoinOp`, `RecursiveCteOp`)
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
  TraceFamily = TraceFamily.Spine })` — the structural compile selects
  the spine family and supplies a `StructuralRowComparer` for the
  sorted batches. Snapshot round-trips and the random-query
  equivalence PBT both run green on the spine path; the typed-row fast
  path still emits the flat family (see [What's next](#whats-next)).
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

- `GROUP BY` takes bare column references only (no expression grouping).
- `INNER` / `LEFT` / `RIGHT [OUTER] JOIN` require at least one equi-key in
  `ON`. Non-equi conjuncts are allowed on `INNER JOIN` (applied as a
  post-filter) but rejected on outer joins.
- Subqueries cover all the row-filter shapes — scalar (one column),
  `IN (subquery)`, `NOT IN (subquery)`, `EXISTS (subquery)`, and
  `NOT EXISTS (subquery)` — both uncorrelated AND **correlated**
  (single level; the resolver decorrelates against an inner-WHERE
  equi-predicate referencing an outer column). Correlated `IN` /
  `NOT IN` / `EXISTS` / `NOT EXISTS` lift to a multi-key
  `SemiJoinPlan` (with `IsAnti=true` for the NOT-forms; compiled as
  `outer − SemiJoin(outer, sq)` via the existing Z-set subtraction);
  correlated scalar lifts to a `CorrelatedScalarSubqueryJoinPlan`
  whose inner aggregate is augmented with a GROUP BY on the
  correlation columns. `IN (literal_list)` / `NOT IN (literal_list)`
  are flat-AST. **Deferred**: `NOT IN (subquery)` with nullable
  operands (needs an extra "any NULL in sq" scalar-subquery layer
  for full 3VL); `IN`/`EXISTS`/`NOT IN`/`NOT EXISTS` in
  SELECT/HAVING/nested-boolean positions; nested correlation
  (subquery-inside-subquery referencing grand-outer columns).
- `WITH RECURSIVE` evaluates semi-naïvely on pure-insert ticks (preserves
  `R` across ticks, propagates only newly-derivable rows), and falls back
  to full recomputation on any tick containing a retraction. Body may
  reference only base tables and the self-ref; no aggregates, subqueries,
  outer joins, or nested recursion inside the body.
- Set ops: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` all supported;
  `INTERSECT ALL` / `EXCEPT ALL` (bag-semantics variants) are deferred.
- `FULL OUTER JOIN`, window functions, `ORDER BY` / `LIMIT`,
  `CASE WHEN`, `LIKE` / `SIMILAR TO`, `||` concatenation, and
  `IS [NOT] DISTINCT FROM` are deferred.
- Scalar function library covers the common arithmetic / string set
  listed above; missing pieces include `SUBSTRING`, `TRIM`, `REPLACE`,
  `POSITION`, `SIGN` / `LN` / `LOG` / `EXP`, and anything involving
  dates/times.
- `NULL` literal has a concrete type (`INTEGER NULL`) rather than the
  polymorphic "unknown" of PostgreSQL.
- `INTERVAL` type and date/time arithmetic are deferred.

## What's next

The biggest tracked-but-not-yet-shipped pieces:

- **Spine on the typed-row fast path.** The spine family is now
  emitted by the structural compile via `CompileOptions { TraceFamily
  = TraceFamily.Spine }`; the typed-row pipeline still emits the flat
  operators, so a spine-mode query compiles structurally. Extending
  the typed compiler to spine-backed operators (generated per-schema
  comparers for the emitted structs) is the remaining integration
  step. `RecursiveCteOp` likewise has no spine sibling and stays flat.
- **DRED-style retraction propagation for recursive CTEs**, so the
  full recomputation fallback on retraction-containing ticks goes
  away.
- **`NOT IN (subquery)` with nullable operands** — full SQL three-valued
  semantics needs an "any NULL in sq" per-correlation-group
  scalar-subquery layer atop the anti-semi-join. The
  `CorrelatedScalarSubqueryJoinPlan` machinery is in place; the
  missing piece is the resolver-side rewrite. NOT NULL operands ship
  today.
- **`IN` / `EXISTS` / `NOT IN` / `NOT EXISTS` in SELECT / HAVING /
  nested-boolean positions** — per-row boolean shape, distinct from
  the WHERE-conjunct semi-join lift.

## License

MIT. See [LICENSE](LICENSE).
