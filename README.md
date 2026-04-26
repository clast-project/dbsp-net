# DbspNet

A research-grade C# / .NET 10 port of [Feldera](https://github.com/feldera/feldera)'s
DBSP (Database Stream Processor) engine.

DBSP is an incremental data-computation model that turns SQL queries into
circuits that process input *changes* and emit output *changes* — so a view
can be kept up-to-date at a cost proportional to the size of the change,
not the size of the data. See Budiu et al., *DBSP: Automatic Incremental View
Maintenance for Rich Query Languages*, VLDB 2023.

This repository is a **prototype**: it targets a small, honest slice of SQL
end-to-end (CREATE TABLE / CREATE VIEW, SELECT with projection, filter, inner
join, group-by aggregates). It is not a production system and makes no
attempt at feature parity with Feldera. Features deferred from v1 are tracked
in [`docs/skipped.md`](docs/skipped.md) against Feldera's real surface.

## Layout

```
src/
  DbspNet.Core     algebra, Z-sets, circuit runtime, operators
  DbspNet.Sql      SQL parser, logical plan, plan→circuit compiler
  DbspNet.Demo     runnable end-to-end scenarios
tests/
  DbspNet.Tests    unit + property-based tests
docs/
  design-notes.md  DBSP primer and implementation notes
  skipped.md       deferred features tracked against Feldera
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
- Types: `INTEGER`, `BIGINT`, `REAL`, `DOUBLE PRECISION`, `DECIMAL(p,s)`, `VARCHAR`, `BOOLEAN`.
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
- Plan optimizer (`DbspNet.Sql.Optimizer.PlanOptimizer`, explicit pass):
  predicate pushdown (through Project / Join / UnionAll / Distinct /
  Difference, respecting outer-join restrictions) and projection
  composition. Apply with `Compile(Optimize(plan))`.
- Correctness: 370+ unit tests plus property-based tests (≥3000 CsCheck
  iterations) across 40 query templates, run both with and without the
  optimizer — semantic equivalence is continuously verified.

## What's deferred

See [`docs/skipped.md`](docs/skipped.md). The short list of v1 restrictions
beyond "Feldera is much bigger":

- `GROUP BY` takes bare column references only (no expression grouping).
- `INNER` / `LEFT` / `RIGHT [OUTER] JOIN` require at least one equi-key in
  `ON`. Non-equi conjuncts are allowed on `INNER JOIN` (applied as a
  post-filter) but rejected on outer joins.
- Subqueries are uncorrelated and scalar only (exactly one column). `IN`,
  `EXISTS`, and correlated subqueries are deferred.
- `WITH RECURSIVE` evaluates semi-naïvely on pure-insert ticks (preserves
  `R` across ticks, propagates only newly-derivable rows), and falls back
  to full recomputation on any tick containing a retraction. Body may
  reference only base tables and the self-ref; no aggregates, subqueries,
  outer joins, or nested recursion inside the body.
- Set ops: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` all supported;
  `INTERSECT ALL` / `EXCEPT ALL` (bag-semantics variants) are deferred.
- `FULL OUTER JOIN` and window functions are deferred.
- Scalar function library is `COALESCE` and `CAST` only.
- `NULL` literal has a concrete type (`INTEGER NULL`) rather than the
  polymorphic "unknown" of PostgreSQL.

## License

MIT. See [LICENSE](LICENSE).
