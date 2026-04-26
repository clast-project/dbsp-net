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

### Aggregate functions
- **[P1]** Incrementalized `MIN` / `MAX`. SUM / COUNT / AVG now fold the
  per-group delta into running state via `SqlAggregator.Update` (O(|delta|)
  per changed group). `MIN` / `MAX` still inherit the default `Update`,
  which rescans the post-delta multiset — retracting the current extremum
  requires knowing the next-best value, so a proper incremental impl needs
  a per-group retraction-aware structure (heap or sorted trace). See
  `docs/benchmarks.md` for the effect on the composite-aggregate curve.
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
- **[P1]** DATE, TIME, TIMESTAMP, INTERVAL. Feldera has full support.
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
- **[P2]** `LATENESS` bounds. Feldera-specific.
- **[P3]** `WATERMARK` (experimental in Feldera).
- **[P2]** `append_only` table annotation. Feldera-specific.
- **[P2]** `emit_final` view annotation. Feldera-specific.

### UDFs
- **[P2]** User-defined scalar, table, and aggregate functions.

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
  (`ON TRUE`) are rejected. Feldera handles these via Calcite-style
  cross-join-then-filter; v1 never materialises a cross product.
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
- **[P2]** Persistent storage backends for Trace. v1 is in-memory only.
- **[P2]** Trace compaction / waterline. v1 keeps every batch until the
  circuit is torn down.
- **[P2]** Multi-threaded circuit execution. v1 is single-threaded per
  `step()` call.
- **[P2]** Spine-backed Trace (layered LSM-style). v1 is flat.
- **[P3]** Dynamic / polymorphic operators (Feldera's `dynamic` module).

## Compiler

- **[P1]** Logical-plan optimizer — partial. <c>DbspNet.Sql.Optimizer.PlanOptimizer</c>
  does predicate pushdown (through Project, InnerJoin + outer-join-safe,
  UnionAll, Distinct, Difference; adjacent-filter merging) and
  projection composition. Still deferred: <b>projection pruning</b>
  (top-down column-liveness — would reduce state in stateful operators),
  <b>constant folding</b>, and <b>join reordering</b> (needs statistics).
  The optimizer is not invoked automatically from <c>PlanToCircuit.Compile</c> —
  callers opt in with <c>Compile(Optimize(plan))</c>.
- **[P1]** Subquery decorrelation.
- **[P2]** Common subexpression elimination.
- **[P2]** Circuit-level optimizer (Feldera's `CircuitOptimizer`).
- **[P3]** Calcite's ~200 RBO rules. v1 performs none of them.
- **[P2]** Row layout. Every row at every stream is a `StructuralRow`
  backed by `object?[]` of boxed values. Feldera generates per-schema
  struct layouts (via the `dynamic` module) that avoid boxing in the hot
  path. A per-schema row codegen pass would be the single biggest
  throughput lever in DbspNet.
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
