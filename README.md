# DbspNet

DbspNet is a C# / .NET 10 implementation of DBSP, the incremental view
maintenance model from Budiu et al.,
*DBSP: Automatic Incremental View Maintenance for Rich Query Languages*
(VLDB 2023). [Feldera](https://github.com/feldera/feldera)'s Rust runtime is the
reference implementation. DbspNet was written from the paper, not translated
from Rust.

DBSP compiles SQL views into circuits that consume input *changes* and emit
output *changes*. Each update costs time proportional to the change, not to the
size of the data.

It is a research project. It is not production software, and it does not aim
for full parity with Feldera. Still, it runs a wide slice of SQL end to end on
real benchmarks:

- **Nexmark.** On a 14-core M4 Pro with 10M events, DbspNet at 14 workers beats
  Feldera on all 13 queries that both run, by 1.1× to 20× (2026-11-02). See
  [`docs/benchmarks-comparison.md`](docs/benchmarks-comparison.md).
- **TPC-DI.** This is the ivm-bench benchmark, which keeps 50 views up to date.
  At SF10 with 12 workers, the initial bulk load (batch 1) ties Feldera at 1:31,
  and the two incremental batches take 1 s each against Feldera's 7–8 s
  (2026-11-14). See [`docs/design-tpcdi-batch1.md`](docs/design-tpcdi-batch1.md).

[`ARCHITECTURE.md`](ARCHITECTURE.md) maps the codebase.
[`docs/skipped.md`](docs/skipped.md) lists what is deliberately missing.

## Build and test

You need the .NET 10 SDK. Package versions are pinned in `Directory.Packages.props`,
and everything restores from nuget.org.

```
dotnet build
dotnet test                                  # ~2,300 tests, a few minutes
dotnet run --project src/DbspNet.Demo        # five annotated scenarios
```

## Quick start

A query compiles to a `CompiledQuery`. To use it:
1. Push inserts and deletes into its tables.
2. Call `Step()`.
3. Read `Current`, which holds this tick's output delta.

```csharp
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

var resolver = new Resolver(new Catalog());
resolver.Resolve(Parser.ParseStatement(
    "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL)"));

var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
    "SELECT cust, SUM(amount) AS total FROM orders GROUP BY cust"))).Query;
var query = PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));

query.Table("orders").Insert(1, 100);
query.Table("orders").Insert(2, 50);
query.Step();          // Current: +1 (1, 100), +1 (2, 50)

query.Table("orders").Insert(1, 25);
query.Step();          // Current: -1 (1, 100), +1 (1, 125); customer 2 untouched
```

`Current` is a `ZSet<StructuralRow, Z64>`, a multiset of rows with integer
weights. A positive weight is an insert and a negative weight is a retraction.

For several views over shared tables, compile them together as a **program**.
`SqlProgram.Compile` turns a list of `CREATE TABLE` / `CREATE VIEW` statements
into one circuit. Every view is optimized and built once and shared by the
views that read it. Each output view is materialized in the circuit:

```csharp
var program = SqlProgram.Compile(
    [
        "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL)",
        "CREATE VIEW totals AS SELECT cust, SUM(amount) AS total FROM orders GROUP BY cust",
        "CREATE VIEW big AS SELECT cust FROM totals WHERE total > 100",
    ],
    outputViews: new HashSet<string> { "big" });
program.Table("orders").Insert(1, 125);
program.Step();        // program.Outputs["big"].CurrentView holds (1)
```

To run on W workers, use `SqlProgram.TryCompileParallel(..., workers, out var p)`.
The single-query form is `PlanToCircuit.TryCompileParallel`. Both return `false`
when the plan can't be sharded soundly; compile serially in that case, and use
`SqlProgram.ParallelBlockers` to see why.

## SQL support

The dialect follows PostgreSQL by default. It has switches for Feldera and Spark
behaviour, such as NULL collation and numeric↔string coercion.

**DDL**
- `CREATE TABLE`, with `NOT NULL` and per-column `LATENESS`. `PRIMARY KEY` is
  parsed but ignored.
- `WITH ('append_only' = 'true')` declares that a table only receives inserts,
  which lets MIN, MAX and DISTINCT keep less state.
- `CREATE VIEW`.
- Nested `ROW(...)` column types, flattened to dotted leaf columns (`t.a.b`).

**Types**
- `INTEGER` (and `TINYINT` / `SMALLINT`, which widen to `INTEGER`), `BIGINT`,
  `REAL`, `DOUBLE`.
- `DECIMAL(p,s)`, stored as Arrow-aligned `Decimal128`.
- `VARCHAR` / `CHAR` / `TEXT`, stored as native UTF-8 with code-point semantics.
- `BOOLEAN`.
- `DATE`, `TIME` and `TIMESTAMP`, using Arrow's microsecond layouts.
- `INTERVAL`, holding year-month and day-time values with calendar-aware
  arithmetic.

**Queries**
- `SELECT [DISTINCT]`, `WHERE`, `GROUP BY` (on any expression), `HAVING`, and
  derived tables.
- CTEs: a CTE used twice is built once. `WITH RECURSIVE` is fully incremental:
  semi-naive on inserts, delete-and-re-derive on deletes.
- Joins: `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS` and comma joins, with `ON`
  (equi, computed-key or non-equi) or `USING`.
- Set operations: `UNION [ALL]`, `INTERSECT`, `EXCEPT`.
- Subqueries: scalar, `[NOT] IN`, and `[NOT] EXISTS`, both uncorrelated and
  single-level correlated, with full three-valued `NOT IN`.
- `ORDER BY … LIMIT / OFFSET / FETCH` runs as an incremental TOP-K. `NULLS FIRST`
  and `NULLS LAST` are supported.
- Window functions:
  - `ROW_NUMBER`, `RANK` and `DENSE_RANK`, either as a column or filtered with
    `rn <= k`, which compiles to a per-partition TOP-K.
  - `SUM`, `COUNT`, `AVG`, `MIN` and `MAX` `OVER` whole-partition, running, or
    bounded `RANGE` frames.
  - `LAG`, `LEAD`, `FIRST_VALUE` and `LAST_VALUE` (`IGNORE NULLS` on the last two).
  - Named `WINDOW` clauses.
- Event-time windows: `GROUP BY TUMBLE(...)` with `TUMBLE_START` / `TUMBLE_END`,
  and the `TABLE(TUMBLE(...))` / `TABLE(HOP(...))` table functions.

**Expressions and functions**
- Arithmetic, comparison and three-valued logic.
- `CASE` (searched and simple), `IIF`, `DECODE`, `[NOT] BETWEEN`.
- `IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE/FALSE/UNKNOWN`.
- `CAST`, `TRY_CAST`, and typed literals (`DATE '…'`).
- Pattern matching: `LIKE` / `ILIKE` / `SIMILAR TO` (with `ESCAPE`), `RLIKE`, and
  `REGEXP_LIKE` / `REGEXP_REPLACE` / `REGEXP_SUBSTR`.
- Strings: `||`, `CONCAT`, `CONCAT_WS`, `UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`,
  `TRIM` / `LTRIM` / `RTRIM`, `REPLACE`, `POSITION` / `STRPOS`, `SPLIT_PART`,
  `MD5`, `JSON_EXTRACT_STRING`.
- Numeric: `ABS`, `FLOOR`, `CEIL`, `ROUND`, `POWER`, `SQRT`, `SIGN`, `LN`, `LOG`, `EXP`.
- General: `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`.
- Temporal: `EXTRACT` / `DATE_PART`, `DATE_TRUNC`, `DATEADD`, `DATEDIFF`,
  `[TRY_]STRPTIME`, and date/time ± interval arithmetic.

**Aggregates**
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and exact `COUNT(DISTINCT)`.
- The `STDDEV` and `VARIANCE` families.
- `APPROX_COUNT_DISTINCT` (HyperLogLog).
- `APPROX_PERCENTILE`, `MEDIAN`, and `PERCENTILE_CONT` / `PERCENTILE_DISC …
  WITHIN GROUP` (DDSketch; exact for dates and timestamps).
- Any aggregate accepts `FILTER (WHERE …)`.

**Streaming**
- `LATENESS d` on a column promises that values never fall more than `d` behind
  the maximum seen. Late rows are dropped, and joins, aggregates and DISTINCT
  garbage-collect state below the resulting frontier, which keeps long-running
  pipelines in bounded memory.
- `NOW()`, `CURRENT_TIMESTAMP` and `CURRENT_DATE` read a logical clock that the
  host advances (`AdvanceClock`). It is persisted and deterministic. `WHERE ts >
  NOW() - INTERVAL '1' HOUR` emits retractions as the clock moves, even on ticks
  with no input.

### Not supported (highlights)

The full list is in [`docs/skipped.md`](docs/skipped.md).

- `ROWS` / `GROUPS` frames and `FOLLOWING` bounds.
- SESSION windows. HOP windows work but are not garbage-collected.
- `NATURAL JOIN`, `INTERSECT ALL` and `EXCEPT ALL`.
- Correlation more than one level deep. `WITH RECURSIVE` bodies are limited to
  filters, projections, inner joins and `UNION ALL`.
- Trigonometric functions and `MOD`. Use `%` instead of `MOD`.
- The `SUBSTRING(s FROM a FOR b)` and `TRIM(LEADING … FROM …)` keyword forms.
- `INTERVAL` as a stored (Arrow) column.
- An interval `LATENESS` bound. The bound must be written in native units:
  µs for `TIMESTAMP`, days for `DATE`.
- `CURRENT_TIME`, which is rejected because it wraps at midnight.
- DML, transactions, `ALTER`, `CREATE INDEX` and bind parameters. Data enters
  through `Table(...).Push` or connectors.
- Temporal filters (`NOW()`) on the parallel *program* path. A program that
  uses one compiles serially (a `ParallelBlockers` entry). The single-query
  parallel path does run them, and both parallel paths run `LATENESS`.

## Engine

- **Two compilers.** A typed fast path uses a struct emitted per schema. A
  structural path uses boxed rows; it is the canonical path and handles
  everything. A query falls back to structural without anything breaking.
- **Optimizer.**
  - Predicate pushdown and projection composition.
  - Aggregate-input narrowing and join column pruning.
  - Semi-join narrowing, plan-level CSE, and rank→TOP-K rewriting.
  - Operator fusion and other compile-time rewrites (star joins, hot-key splits).
- **Data-parallel execution.** `ParallelCircuit` hash-shards the input across W
  workers. Exchanges repartition streams, small dimensions are broadcast, and a
  global rank is computed by key range.
- **State.**
  - Trace state is held by default as reference-free byte blocks, so the GC sees
    bytes, not object graphs.
  - An LSM-style spine trace family is available through `TraceFamily.Spine`.
  - Bounded-memory machinery: pressure levels, a block cache, and mid-batch and
    in-step spilling to compressed run files. Most of it is opt-in.
- **Persistence.**
  - Crash-safe end-of-tick snapshots with plan and schema fingerprints.
  - A WAL and snapshot+WAL hybrid.
  - A tick journal of source offsets.
  - One-commit parallel checkpoints.
  - Re-sharding a checkpoint to another worker count.
  - An opt-in run-file layout with incremental checkpoints and lazy restore
    ([#4](https://github.com/clast-project/dbsp-net/issues/4)).
  
  See [`docs/persistence.md`](docs/persistence.md).
- **Arrow.** `TableInput.PushArrow` / `CompiledQuery.ToArrowDelta()` and IPC
  streaming. Weights travel as a trailing `__weight` column.
- **Connectors.** Delta Lake input (change data feed) and output (truncate,
  changelog, or incremental with deletion vectors), and Parquet input. All run
  through `PipelineRunner` / `ProgramRunner` with checkpoint policies.
- **Observability.** `CollectStats()` reports each stateful operator's state
  size, output size and GC frontier. `LastStepDuration`, and
  `DBSPNET_PROFILE=1` for a per-batch phase and operator profile.

## Server

`DbspNet.Server` is a small HTTP control service that deploys a SQL program
between Delta tables. ivm-bench uses it as its engine container. It plays the
role of Feldera's pipeline manager.

```
docker build -f src/DbspNet.Server/Dockerfile -t dbspnet-server .   # listens on :8080
```

| Endpoint | What it does |
|---|---|
| `GET /healthz` | health check |
| `POST /compile` | a dry run |
| `POST /deploy` | takes a `ProgramSpec`: SQL statements, Delta inputs and outputs, optional snapshot directory and stored views |
| `POST /resume` | starts the run |
| `POST /wait` | returns when the inputs are drained and the outputs written |
| `POST /pause` | |
| `GET /stats` | |

Configuration is all environment variables. The main ones:

| Variable | Default | Meaning |
|---|---|---|
| `DBSPNET_WORKERS` | 1 | worker count; falls back to serial, with a logged reason, if a view won't shard |
| `DBSPNET_SNAPSHOT_DIR` | off | enables checkpoints and restore |
| `DBSPNET_CHECKPOINT_INTERVAL_SECS` | 60 | snapshot interval; `batch` = every batch, `none` = journal only |
| `DBSPNET_OUTPUT_MODE` | incremental | `truncate` rewrites each view every batch |
| `DBSPNET_BROADCAST_MAX_ROWS` | 1000 | broadcast dimensions this small in parallel joins; `off` disables |
| `DBSPNET_MEMORY_LIMIT_MIB` | GC limit | memory budget behind the pressure levels |
| `DBSPNET_BLOCK_CACHE_MIB`, `DBSPNET_SPILL`, `DBSPNET_STEP_SPILL_ROWS` | off | bounded-memory eviction and spilling (see [`design-bounded-memory.md`](docs/design-bounded-memory.md)) |
| `DBSPNET_PROFILE` | off | per-batch phase and operator profile |

Many more `DBSPNET_*` switches exist. They are A/B opt-outs and measurement
seams, documented where they are read.

## Benchmarks

```
cd src/DbspNet.Benchmarks
dotnet run -c Release                                   # latency report → docs/benchmarks.md
dotnet run -c Release -- nexmark 10000000 10000 3 14    # events batch runs [workers] → docs/benchmarks-nexmark.md
dotnet run -c Release -- fraud 500000 10000 10000 14    # history customers batch [workers] → docs/benchmarks-fraud.md (not checked in)
```

- **Nexmark.** The supported queries are q0–q5, q7–q9, q12, q15–q20 and q22.
  SESSION windows block q11.
- **Head-to-head.** `scripts/compare-nexmark.sh` runs DbspNet and Feldera on the
  same host and prints a merged events/s table. Feldera needs
  `scripts/feldera-nexmark-pregen.patch`. The checked-in head-to-head results
  are hand-written sections of `docs/benchmarks-comparison.md`. The
  `comparison` subcommand overwrites that file.
- **Instruments.** The project also hosts about 40 measurement subcommands. For
  example, `w1profile` measures bytes allocated per event, which stays stable
  across hosts; see `Program.cs`.

## Repository layout

```
src/
  DbspNet.Core                      Z-sets, circuits, operators, parallel runtime, trace state
  DbspNet.Sql                       parser, resolver, optimizer, compilers, programs
  DbspNet.Arrow                     Arrow boundary
  DbspNet.Persistence               checkpoints, WAL, journal, run files, block cache, spill
  DbspNet.Connectors.Abstractions   connector interfaces, pipeline/program runners
  DbspNet.Connectors.EngineeredWood Delta Lake + Parquet connectors
  DbspNet.Server                    HTTP control service (ivm-bench engine)
  DbspNet.Demo                      walkthrough scenarios
  DbspNet.Benchmarks                benchmarks and measurement instruments
tests/DbspNet.Tests                 unit, property-based (CsCheck) and conformance tests
scripts/                            Nexmark head-to-head, checkpoint/ingest measurement
docs/                               design notes, decisions, benchmark results
```

## Documentation guide

`docs/` holds several kinds of document:
- **Design notes** (`design-*.md`) are long-running journals. Each starts with a
  design and then records measurements section by section. The newest sections
  are the current state.
- **Decisions** (`decision-*.md`, `comparison-feldera-decisions.md`).
- **Research** (`research-*`).
- **Dated benchmark results** (`*-bench.md`, `*.txt`).

Start with:

- [`design-notes.md`](docs/design-notes.md): a DBSP primer and the core implementation notes.
- [`ARCHITECTURE.md`](ARCHITECTURE.md): the codebase map and extension points.
- [`persistence.md`](docs/persistence.md): checkpoints and recovery.
- [`design-flat-trace-state.md`](docs/design-flat-trace-state.md): block-held trace state.
- [`design-bounded-memory.md`](docs/design-bounded-memory.md): residency and spilling.
- [`design-structural-parallel.md`](docs/design-structural-parallel.md): parallel compilation.
- [`ivm-bench-gap-analysis.md`](docs/ivm-bench-gap-analysis.md): TPC-DI coverage.
- [`benchmarks-comparison.md`](docs/benchmarks-comparison.md): dated Nexmark head-to-heads with Feldera.
- [`skipped.md`](docs/skipped.md): deferred features, with their Feldera counterparts.

## License

MIT. See [LICENSE](LICENSE).
