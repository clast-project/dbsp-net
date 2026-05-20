# Skipped / deferred features

This file is a *living punch list* tracking features that exist in Feldera
but are out of scope for the DbspNet v1 prototype. New entries get added as
we hit deferred work during implementation. Each entry should cite the
Feldera counterpart so future work is cheap to pick up.

Legend:
- **P1** — essential next step once v1 lands (NULL-in-schema beyond columns,
  outer joins, subqueries).
- **P2** — valuable but can wait for a v2 rewrite.
- **P3** — parity work, lowest priority.

## SQL surface

### DDL
- **[P1]** `CREATE MATERIALIZED VIEW` — Feldera models this via view
  annotations; in DbspNet v1 every view is materialized.
- **[P2]** `CREATE INDEX` — Feldera's pipeline uses indexed Z-sets internally
  but indexes are not user-declared.
- **[P3]** Constraints beyond NOT NULL (UNIQUE, CHECK, FOREIGN KEY). Feldera
  has partial support.
- **[P3]** `ALTER TABLE` / `DROP TABLE` / `DROP VIEW` / `COMMENT ON`. The
  plan is built once from a complete source string; schema evolution
  isn't part of the model and would need a mutable catalog.

### DML and sessions

DbspNet is push-driven: data enters via `TableInput.Push`, and queries
are compiled once and stepped. The entries below are recognised neither
by the parser nor by the engine model. The ones marked *by design*
reflect that shape, not a backlog.

- *by design* — `INSERT` / `UPDATE` / `DELETE` / `MERGE` / `TRUNCATE`.
  Row mutation goes through `TableInput.Push(rows)` and weight-bearing
  retractions, not statement-level DML. Feldera matches this: SQL is
  view-definition only, with mutation through the connector layer.
- *by design* — `BEGIN` / `COMMIT` / `ROLLBACK` / savepoints / isolation
  levels. Each `Step()` is the atomic unit; there is no client-visible
  transaction surface.
- **[P2]** Bind parameters (`?`, `@name`, `$1`). Literals are baked into
  the plan today; parameterised compilation would need a parameter-slot
  AST node and a re-plan-with-bindings API.
- **[P3]** `EXPLAIN`. The plan is reachable via the in-process API; no
  SQL surface for it.

### Query constructs
- **[P1]** `IN` / `EXISTS` subqueries. DbspNet supports uncorrelated scalar
  subqueries in expression position; `IN` and `EXISTS` are deferred
  (both decompose to a semi-join that needs one more operator-level primitive).
- **[P1]** Correlated subqueries of any kind. Feldera handles these via
  Calcite's decorrelation rewrite; DbspNet's resolver treats subqueries as
  closed over their own scope only.
- **[P2]** DBSP-paper-faithful retraction propagation for recursive CTEs.
  `WITH RECURSIVE` uses semi-naïve incremental evaluation on pure-insert
  ticks: <c>R</c> is preserved across outer ticks, and each tick's external
  delta triggers only the newly-derivable rows to propagate through
  <c>step</c>. For insert-only workloads (e.g. a growing graph), per-tick
  cost is proportional to the newly-reachable output, not to the full
  closure. On any tick containing a retraction (negative-weight external
  delta), the operator falls back to full batch recomputation — correct but
  at the cost of re-running the fixed point from scratch. The proper
  solution is DRED-style delete propagation / DBSP-weight arithmetic
  through the recursion, which needs bag-semantic fixed-point termination
  arguments we haven't worked through here.
  <br/>
  Additional v1 restrictions on the recursive body:
  - body must be a `UNION ALL` with at least one base case and one recursive branch
  - body may reference only base tables and the self-reference (not other CTEs)
  - no aggregates, no subqueries, no outer joins inside the recursive body
  - no nested recursion (recursive CTE inside recursive CTE)
  - set semantics (duplicates collapsed per iteration) — deliberate
    deviation from strict `UNION ALL` bag semantics to guarantee termination
    on finite inputs; a 10,000-iteration safety cap throws if a query
    diverges regardless
- **[P1]** `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`. DbspNet
  supports `INNER`, `LEFT [OUTER] JOIN`, and `RIGHT [OUTER] JOIN`.
  `RIGHT JOIN` is a swap wrapper over `IncrementalLeftJoinOp`.
  `FULL OUTER JOIN` needs symmetric match-presence tracking on both sides.
- **[P1]** `LEFT/RIGHT/FULL JOIN` with a non-equi conjunct in the `ON`
  clause (e.g. `LEFT JOIN b ON a.k = b.k AND b.v > 0`). Semantically,
  failing the residual should drop the match but retain the preserved row
  NULL-padded; that requires residual-aware logic in the operator.
  Resolver rejects with an explicit error.
- **[P2]** `LEFT ASOF JOIN`. Feldera-specific, used for time-series matching.
- **[P2]** `INTERSECT ALL` / `EXCEPT ALL` (bag semantics for intersect/
  except — min/clamped-subtract of weights). DbspNet supports the
  set-semantics variants: `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT`.
  Feldera supports neither `INTERSECT ALL` nor `EXCEPT ALL`.
- **[P2]** `UNNEST`, `LATERAL`. Feldera supports both.
- **[P2]** `GROUPING SETS`, `ROLLUP`, `CUBE`.
- **[P1]** `ORDER BY` / `LIMIT` / `OFFSET` / `FETCH FIRST`. No tokens
  and no `SelectStatement` fields. Incremental TopK is a Feldera-
  supported pattern (RANK / ROW_NUMBER restricted to TopK windows);
  `LIMIT` without `ORDER BY` is also useful for snapshot queries.
- **[P1]** `SELECT DISTINCT` (SELECT-list form). Distinct-inside-
  aggregates is listed separately under Aggregate functions; the
  list form (`SELECT DISTINCT a, b FROM t`) reduces to a `DistinctOp`
  over the projection.
- **[P1]** `CASE WHEN ... THEN ... [ELSE ...] END` and the function-
  form variants (`IIF`, `DECODE`). No keyword, no AST node, no
  expression-compiler support. Fundamental conditional.
- **[P1]** `BETWEEN x AND y` / `NOT BETWEEN`. Desugars at parse time
  to `x >= a AND x <= b`; no runtime work needed.
- **[P1]** `LIKE`, `ILIKE`, `SIMILAR TO`. No keyword. Runtime story
  ties into the Utf8String roadmap noted under Type system.
- **[P1]** `IN (literal_list)` / `NOT IN (literal_list)`. Distinct
  from the IN-subquery form already listed — desugars to a disjunction
  of equalities at parse time, no operator-level support needed.
- **[P1]** `IS [NOT] DISTINCT FROM`. NULL-safe equality. The parser's
  `IS` arm only accepts `NULL` / `NOT NULL` today.
- **[P2]** `IS TRUE` / `IS FALSE` / `IS UNKNOWN`. Boolean tests; same
  parser arm as above.
- **[P1]** `JOIN ... USING (col, ...)`. Parser only accepts `ON`.
  Desugars to `ON a.c = b.c AND ...` plus dedup of the join columns
  in the projection.
- **[P2]** `VALUES (...), (...) AS t(a, b)` as a row source.
  `ParsePrimaryTableRef` accepts only `(SELECT ...)` or a base table;
  would need a literal-table constructor.
- **[P1]** `||` string concatenation operator. Lexer has no `||`;
  users must call `CONCAT(...)`. One lexer entry + one parser arm.
- **[P2]** `ROW(...)` and `ARRAY[...]` constructors. Tied to the
  deferred ROW/STRUCT and ARRAY types under Type system.
- **[P2]** Exponentiation operator `**` / `^`. `POWER(x, y)` exists.
- **[P2]** Bitwise operators (`&`, `|`, `^`, `<<`, `>>`, `~`).
- **[P2]** `OVERLAPS`, `ALL` / `ANY` / `SOME` quantifiers.

### Aggregate functions
- **[P1]** `MIN`, `MAX` are included in v1 via per-group multiset tracking.
- **[P1]** `FILTER (WHERE …)` clause on aggregates.
- **[P1]** `DISTINCT` in aggregates; `WITHIN DISTINCT`.
- **[P2]** `ARG_MIN`, `ARG_MAX`, `ARRAY_AGG` (with `ORDER BY`,
  `RESPECT|IGNORE NULLS`), `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`.
- **[P2]** Bitwise aggregates: `BIT_AND`, `BIT_OR`, `BIT_XOR`.
- **[P2]** Boolean aggregates: `BOOL_AND`/`EVERY`, `BOOL_OR`/`SOME`,
  `LOGICAL_AND`, `LOGICAL_OR`, `COUNTIF`.
- **[P1]** Multi-argument aggregates. v1 requires exactly one argument for
  `SUM`/`COUNT`/`MIN`/`MAX`/`AVG`; `COUNT(*)` is the only zero-arg form.
- **[P1]** Nested aggregates inside scalar expressions inside aggregates
  (e.g. `SUM(CASE WHEN MAX(x) ...)`). Rejected by `Resolver.HasAggregate`.

### Window functions
- **[P2]** All of them. Feldera supports SUM/COUNT/AVG/MIN/MAX over windows,
  plus RANK / DENSE_RANK / ROW_NUMBER (restricted to TopK patterns),
  LAG / LEAD, FIRST_VALUE / LAST_VALUE (UNLIMITED RANGE frames).
- **[P3]** Full SQL frame spec: `ROWS`, `RANGE`, `GROUPS`; `PARTITION BY`;
  `BETWEEN … AND …` with named bounds.

### Type system
- **[P1]** INTERVAL. Feldera has full support.
- **[P2]** TIMESTAMP WITH TIME ZONE, typed temporal literals
  (`DATE 'yyyy-mm-dd'` etc.), and date/time arithmetic
  (`date + interval`, `timestamp - timestamp`). DATE / TIME / TIMESTAMP
  base types now exist with Arrow-aligned representations
  (`Date32`, `Time64[microsecond]`, `Timestamp[microsecond]` naive),
  with `CAST` from string and ordering / equality.
- VARCHAR is stored as `Utf8String` (Arrow-aligned
  `ReadOnlyMemory<byte>`) with native UTF-8 equality, ordering, hashing
  (XxHash3), `LENGTH` (code points), byte-wise `CONCAT`, and `Rune`-based
  invariant `UPPER` / `LOWER`. Substring / LIKE / position-based ops are
  not yet implemented — when added they should also be native UTF-8 with
  code-point semantics.
- Apache Arrow boundary: `DbspNet.Arrow` exposes `CompiledQuery.ToArrowDelta()`
  (returns a `RecordBatch` + parallel weights array — positive for inserts,
  negative for retractions) and `TableInput.PushArrow(RecordBatch[, weights])`
  on the input side. Schema mapping is mechanical (the type system was
  pre-aligned to Arrow). Conversion is column-major — type dispatch is
  hoisted out of the per-row loop into a single typed loop per column.
  String output uses direct UTF-8 byte append (no `string` round-trip).
  Opt-in `TableInput.PushArrowZeroCopy(...)` aliases Arrow `ValueBuffer`
  slices via `Utf8String.FromBytes` for zero-allocation string ingest;
  caller commits to keeping the `RecordBatch` alive for the lifetime of
  engine state. Decimal128 mantissas memcpy directly between Arrow's
  16-byte buffer and `Int128` via `MemoryMarshal` — same little-endian
  layout, no per-cell `BinaryPrimitives` shuffling. IPC streaming is
  available via `CompiledQuery.WriteArrowStream(stream)` /
  `TableInput.ReadArrowStream(stream)` for one-shot snapshots, and
  `OpenArrowDeltaWriter` / `ReadArrowStreamBatches` for multi-batch
  pipelines. Z-set weights travel inline as a trailing
  `__weight : Int64` column on the wire schema; readers that don't
  expect weights see a well-formed Arrow stream and can ignore the
  extra column.
- Persistence (approaches A, B, C — input WAL, end-of-tick snapshot,
  and snapshot+WAL hybrid): all three are shipped under
  `DbspNet.Persistence`, layered on an `IBlobStore` abstraction
  modelled on cloud object stores (S3 / GCS / Azure Blob) with a
  built-in `LocalFileBlobStore` and `InMemoryBlobStore`. Every
  stateful SQL operator — `DistinctOp`, `IncrementalAggregateOp`,
  `IncrementalJoinOp`, `IncrementalLeftJoinOp`, `RecursiveCteOp` —
  implements `ISnapshotable` and round-trips its trace(s) as Arrow IPC
  via the `ArrowSqlSnapshotCodecs` registry. Snapshot manifests carry
  both a plan fingerprint (operator-type sequence with generic args)
  and a schema fingerprint (per-op column names + `SqlType.Display`)
  so VARCHAR-length / DECIMAL-precision / NULLability drift is
  detected on `Load`. See `docs/persistence.md` for the full design.
- DECIMAL is stored as `Decimal128` (Arrow-aligned `Int128` mantissa with
  scale-in-type) via the cross-repo `Clast.DatabaseDecimal` library.
  Native arithmetic (`AddKernel` / `MultiplyKernel` / `DivideKernel`)
  with banker's rounding for division, scale-aware comparison, mixed-
  type promotion (INT + DECIMAL), CAST round-trips through string and
  numeric, plus `ABS` / `FLOOR` / `CEIL` / `ROUND` (banker's rounding)
  on the column scale. Per-operator result-type promotion follows
  SQL Server / Substrait rules via `DecimalTypeRules` (precision grows
  by 1 on add/sub for carry, scales add on multiply, scale extends on
  divide; clamped to 38 with scale-borrowing). SUM / AVG accumulators
  are widened to `Int256` so per-row mantissa × weight cannot silently
  wrap; output is narrowed to `Decimal128` with an explicit overflow
  check. The remaining hard upper bound is ~10^38 at the result column
  itself (Decimal128 capacity); supporting Decimal256-typed result
  columns is a future SQL-frontend extension.
- **[P2]** Unsigned integer variants (`UINT8`, `UINT16`, etc.). Feldera
  supports them.
- **[P2]** ARRAY, MAP, ROW/STRUCT. Feldera supports all three.
- **[P2]** BINARY, VARBINARY, UUID.
- **[P3]** VARIANT (dynamic / JSON). Feldera supports it but restricts it
  in table schemas.
- **[P3]** GEOMETRY. Feldera has rudimentary support.
- **[P3]** User-defined types (aliases + records, non-recursive).
- **[P3]** CHAR(n) with space-padding semantics.

### Streaming extensions
- **[P1]** `LATENESS` bounds — or any equivalent retain-keys story. The
  `LATENESS` syntax is Feldera-specific, but the underlying capability
  (bounded-history trace GC driven by a monotonicity analysis) is the
  standard answer in every long-running IVM engine surveyed: Feldera's
  `MonotoneAnalyzer` + `IntegrateTraceRetainKeysOperator`, Materialize's
  temporal filters, Flink's watermark-driven state expiry. Without some
  form of retain-keys analysis, `_leftTrace` / `_rightTrace` /
  `RecursiveCteOp._r` grow without bound — that caps DbspNet to
  bounded workloads. A minimal user-declared `LATENESS` annotation +
  insert-limiter is small and unlocks long-running pipelines; the
  full Calcite-style monotonicity analyser is heavier.
- **[P3]** `WATERMARK` (experimental in Feldera).
- **[P2]** `append_only` table annotation. Feldera-specific.
- **[P2]** `emit_final` view annotation. Feldera-specific.

### UDFs
- **[P2]** User-defined scalar, table, and aggregate functions.

### Surface syntax

Lexer-level dialect niceties not parsed today. None affect expressive
power — each has an equivalent already supported — but they show up in
copy-pasted queries from other engines.

- **[P3]** Identifier quoting variants. Lexer accepts only double-
  quoted identifiers; backtick (MySQL) and `[bracket]` (T-SQL) are
  not recognised.
- **[P3]** Postgres `expr::type` cast syntax. Only `CAST(expr AS type)`
  is accepted.

### v1 resolver restrictions

Short list of places where DbspNet v1 is deliberately stricter than SQL /
Feldera. Each is enforced by `DbspNet.Sql.Plan.Resolver` with an explicit
`ResolveException`, so loosening is a localized change.

- **[P1]** `GROUP BY` supports only bare column references. Expression
  grouping (`GROUP BY a + b`, `GROUP BY LENGTH(name)`) rejects with
  "GROUP BY supports only bare column references in v1". The plan→circuit
  layer relies on `ResolvedColumn` to rekey; extending means generalising
  `ExtractKey` to run compiled delegates.
- **[P1]** `INNER JOIN` requires at least one equi-key conjunct in the `ON`
  clause. Pure-inequality joins (`ON a.x > b.y`) and cross joins
  (`ON TRUE`) are rejected. v1 never materialises a cross product. The
  v1 framing positioned this as a deliberate scope choice, but
  Calcite-style cross-join-then-filter is standard SQL surface — TPC-H
  Q3 and similar shapes need it — so loosening this is closer to a
  completeness gap than a deferred niceness. Feldera handles it via
  Calcite.
- **[P1]** `SELECT *` is forbidden with `GROUP BY` / aggregates. Feldera
  (via Calcite) rewrites to an explicit column list during resolution.
- **[P1]** Scalar function library is still modest. Currently supported:
  `COALESCE`, `CAST`, `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `ABS`, `FLOOR`,
  `CEIL`/`CEILING`, `ROUND`, `POWER`, `SQRT`, `GREATEST`, `LEAST`, `NULLIF`.
  Missing and commonly needed: `SUBSTRING`, `TRIM`/`LTRIM`/`RTRIM`,
  `REPLACE`, `POSITION`, numeric `SIGN`/`LN`/`LOG`/`EXP`, and anything
  involving dates/times. Adding one is a single entry in
  <c>BuiltinScalarFunctions</c>.
- **[P1]** Typeless `NULL`. A bare `NULL` literal resolves to `INTEGER NULL`
  rather than an unknown-type marker that context narrows (the PostgreSQL /
  Calcite behaviour). In practice this means `NULL = 'x'` fails at resolve
  time; use `CAST(NULL AS VARCHAR)` as a workaround.
- **[P2]** Qualified-star beyond a single table alias. `SELECT t.*` works;
  `SELECT t.*, u.*` is not parsed specially (each star is an independent
  `SelectItem`, which is fine — just a reminder that aliasing collisions
  are the caller's problem).
- **[P2]** `LATERAL` derived tables. `FROM (SELECT …) x` works; Feldera
  also supports `LATERAL` where the derived subquery can reference earlier
  table aliases in the same FROM clause. Requires correlated-subquery
  machinery in the resolver.
- **[P2]** Scalar-subquery CSE across expression boundaries. The resolver
  dedups by structural AST equality, but C# record equality on
  `SelectStatement` (which embeds `IReadOnlyList` fields by reference)
  means two separate parses of the same subquery text do NOT compare
  equal. Workaround: use a CTE for guaranteed sharing. Fix would require
  a custom equality/hash on the parser AST.

## Runtime

- **[P1]** Nested / child circuits. Needed for recursive CTEs and some
  aggregate implementations. Explicit `TODO` in `Circuit/RootCircuit.cs`.
- Spine-backed Trace (layered LSM-style). Implemented in
  `DbspNet.Core.Operators.Stateful.Spine` —
  `SpineZSetTrace<TKey,TWeight>` and
  `SpineIndexedZSetTrace<TKey,TValue,TWeight>` hold the integral as a
  tiered sequence of immutable sorted-columnar batches, with
  configurable compaction (`ICompactionStrategy`,
  `TieredCompactionStrategy`). Every flat-trace operator has a
  spine-backed sibling: `SpineDistinctOp`,
  `SpineIncrementalAggregateOp`, `SpineIncrementalJoinOp`,
  `SpineIncrementalLeftJoinOp`. Per-batch Arrow IPC snapshot via
  `SpineSnapshot`; optional disk spill via `SpineSpillConfig` /
  `SpineIndexedSpillConfig` with per-batch bloom filters so probes
  typically don't touch disk. Exercised by unit tests and the trace
  microbenchmarks (`PureTraceBenchmark`, `DistinctBenchmark`).
  **[P1]** SQL compiler integration. `PlanToCircuit.Compile` still
  emits the flat-trace operator family. The natural next step is a
  compiler-side toggle (or per-operator decision driven by trace-size
  heuristics) that selects the spine family for stateful operators.
  See `docs/persistence.md` "What ships in (D) — spine".
- **[P2]** Trace compaction / waterline (since-frontier
  advancement that lets multiple updates at the same (key, value)
  collapse into one row). The spine has the right shape for this —
  consolidation runs naturally inside compaction — but no frontier is
  advertised today, so traces still retain every update.
- **[P2]** Multi-threaded circuit execution. v1 is single-threaded per
  `step()` call.
- **[P3]** Dynamic / polymorphic operators (Feldera's `dynamic` module).

## Compiler

- **[P1]** Logical-plan optimizer — partial. <c>DbspNet.Sql.Optimizer.PlanOptimizer</c>
  does predicate pushdown (through Project, InnerJoin + outer-join-safe,
  UnionAll, Distinct, Difference; adjacent-filter merging), projection
  composition, and aggregate-input column pruning (inserts a narrowing
  <c>ProjectPlan</c> between <c>AggregatePlan</c> and its input when
  the input carries more columns than the aggregate references). Still
  deferred: <b>general top-down column liveness</b> across joins,
  <b>constant folding</b>, and <b>join reordering</b> (needs
  statistics). The optimizer is not invoked automatically from
  <c>PlanToCircuit.Compile</c> — callers opt in with
  <c>Compile(Optimize(plan))</c>.
- **[P1]** Subquery decorrelation.
- **[P2]** Common subexpression elimination.
- **[P2]** Circuit-level optimizer (Feldera's `CircuitOptimizer`).
- **[P3]** Calcite's ~200 RBO rules. v1 performs none of them.
- Row layout — typed fast path landed. `TypedPlanCompiler` walks a
  `LogicalPlan` and builds a circuit whose streams carry per-schema
  emitted structs from `TypedRowEmitter` instead of `StructuralRow`.
  `PlanToCircuit.Compile` tries the typed path first and falls back to
  the structural compile when the plan or any subexpression is outside
  the typed pipeline's scope; only the input / output boundaries pay a
  structural↔typed conversion. Snapshot codecs flow through via a
  typed adapter so the on-disk format stays compatible across the two
  paths. Remaining work — extending the typed-supported plan / scalar
  surface so the structural fallback fires less often (currently
  supports `ScanPlan`, `FilterPlan`, `ProjectPlan`, INNER `JoinPlan`,
  and via a hybrid lift: aggregates, set ops, CTEs, recursive CTE,
  scalar subqueries).
- **[P2]** Expression-compiler CAST matrix. v1 supports numeric↔numeric,
  numeric↔string, and bool→string. Missing: string→bool (`'t'`/`'f'`),
  decimal-precision-aware numeric narrowing, and anything involving the
  deferred date/time/interval types.

## Type system edge cases

- **[P1]** Implicit-to-explicit type coercion rules matching PostgreSQL.
- **[P2]** `OVERFLOW` semantics on integer arithmetic — v1 uses `checked`.
- **[P2]** Floating-point edge cases: NaN, ±Inf, subnormals.
- **[P2]** Decimal precision/scale overflow.

## Test surface

- **[P2]** SqlLogicTest harness integration. Feldera's SLT suite is the
  gold standard for compatibility testing.
- **[P2]** TPC-H, NEXMark benchmarks.

## Non-goals

Items explicitly *not* on the roadmap, recorded so future contributors
don't ask whether they should be.

- **Higher-order IVM** (DBToaster-style recursive differencing — the
  delta of the delta of the delta…). DBToaster reports 3–6 orders of
  magnitude over re-evaluation by materialising every derivative as
  its own view, but both DBSP/Feldera and Materialize deliberately
  stop at first-order incremental over the IR. The complexity cost is
  large and the win shape doesn't match streaming workloads (it's
  strongest for analytical refresh-on-update on small dimension
  tables). DbspNet follows DBSP/Feldera here — first-order only.
