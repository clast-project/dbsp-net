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
- `IN (subquery)` / `EXISTS` / `NOT EXISTS` — **implemented** for the
  **uncorrelated** form. `IN (subquery)` lifts to a `SemiJoinPlan` at the
  top-level conjunct of WHERE (compiles to `Distinct(sq) ⋈ outer`).
  `EXISTS (sq)` desugars at parse time to
  `COALESCE((SELECT COUNT(*) FROM (sq)), 0) > 0`, which rides on the
  existing scalar-subquery + COALESCE + comparison machinery; `NOT EXISTS`
  falls out via the unary-NOT arm. Deferred follow-ons:
    - **[P1]** `NOT IN (subquery)` — needs anti-semi-join + three-valued
      NULL handling (`NOT IN` with a NULL in the subquery is NULL, not
      simply `NOT (x IN ...)`).
- Correlated subqueries — **implemented** for the single-level form
  across all four shapes: `IN (subquery)`, `EXISTS (subquery)`,
  scalar subquery, and the anti-semi-join forms
  `NOT IN (subquery)` + correlated `NOT EXISTS`. When the subquery's
  body equi-references an outer column (`outer.col = inner.col`),
  the resolver decorrelates by lifting the correlation predicate out
  of the inner WHERE, projecting the inner correlation column into
  the subquery schema, and emitting one of:
    - `SemiJoinPlan(IsAnti=false)` for `IN` / `EXISTS` (multi-key
      with probe + correlation columns for `IN`; correlation-only
      for `EXISTS`).
    - `SemiJoinPlan(IsAnti=true)` for `NOT IN` / correlated
      `NOT EXISTS`. The compiler emits this as
      `outer − SemiJoin(outer, sq)` via the existing
      `builder.Difference` Z-set subtraction.
    - `CorrelatedScalarSubqueryJoinPlan` for correlated scalar (the
      inner aggregate's GROUP BY is augmented with correlation
      columns; multi-column-key LEFT JOIN appends the scalar value).
  All shapes share the same `outerSchema: Schema?` plumbing,
  `ResolvedCorrelationRef` expression node, and
  `TryMatchEquiCorrelation` / `FindAllCorrelations` helpers. `NOT IN`
  covers the full SQL three-valued logic — nullable probe drops the
  row, NULL in the (per-correlation-group) subquery drops outer rows
  that don't otherwise match. Implementation: a hidden
  per-correlation-group null-count column layered via
  `CorrelatedScalarSubqueryJoinPlan` (or `ScalarSubqueryJoinPlan` when
  uncorrelated), filtered on `probe IS NOT NULL AND (null_count IS
  NULL OR null_count = 0)` before the anti-semi-join.

  Non-WHERE positions (`IN` / `EXISTS` / `NOT IN` / `NOT EXISTS` in
  SELECT / HAVING / nested boolean) ship as a separate per-row
  boolean shape: the resolver runs a pre-pass that lifts each such
  expression into a hidden per-correlation-group `COUNT(*)` column
  layered via `CorrelatedScalarSubqueryJoinPlan` (or
  `ScalarSubqueryJoinPlan` for uncorrelated EXISTS). For `IN`, the
  value column joins as an additional equi-key (so even uncorrelated
  `IN` lifts through the correlated path). Scalar resolution then
  rewrites the bound AST node to `COALESCE(count, 0) > 0` (or `= 0`
  when negated). Threading is via a single
  `IReadOnlyDictionary<Expression, ResolvedExpression>? preBound`
  parameter on `ResolveScalarExpression` /
  `ResolvePostAggregateExpression`.

  Nullable-operand `IN` / `NOT IN` in non-WHERE positions — **implemented**.
  When the probe or subquery column is nullable, the resolver layers two
  more hidden counts (per-group total and per-group NULL-value count, the
  latter via `LayerNullCountColumn`) and `BuildBooleanSubqueryRef` emits
  the full SQL three-valued result as a `CASE`: `match>0 → TRUE`, empty
  group → `FALSE`, NULL probe (vs a non-empty group) or a NULL subquery
  value with no match → `NULL`, else `FALSE`; `NOT IN` is the three-valued
  negation. Deferred:
    - **[P2]** Correlated scalar subquery without an aggregate — would
      need a uniqueness guarantee on the inner per correlation key.
    - **[P2]** Correlation refs inside the aggregate expressions
      themselves (`SELECT MAX(outer.k + y) FROM t WHERE ...`).
      Rejected today as "unknown column" via the existing
      column-resolution path.
    - **[P2]** Nested correlation (subquery inside subquery
      referencing grand-outer columns). v1 supports a single level.
    - **[P2]** Non-equi correlation (`outer.col > inner.col`) and
      correlation in JOIN ON / HAVING / GROUP BY / aggregates.
      Rejected with explicit "only equi-correlation in inner WHERE
      supported" errors.
- ~~**[P2]** DBSP-paper-faithful retraction propagation for recursive CTEs.~~
  **Done.** `WITH RECURSIVE` evaluation is fully incremental on both trace
  families. Insert ticks extend the preserved fixpoint `R` semi-naïvely (per-tick
  cost proportional to the newly-reachable output, not the full closure).
  Retraction ticks use Delete-and-Re-Derive: over-delete the R-tuples whose
  derivation used a deleted input (propagated transitively through the old
  graph), then re-derive those still reachable from the survivors via surviving
  edges — correct under cycles and alternative derivation paths. Recursion
  compiles onto a reusable Core nested-circuit (fixpoint) primitive
  (`SemiNaiveFixpointOperator`); the old `RecursiveCteOp` is gone.
  Correctness is held by a random insert/delete incremental≡batch PBT
  (`RecursiveCtePbtTests`, flat and spine). Multiset input deltas
  (a weight magnitude &gt; 1) fall outside the set model and keep a from-scratch
  recompute.
  <br/>
  Remaining v1 restrictions on the recursive body:
  - body must be a `UNION ALL` with at least one base case and one recursive branch
  - body may reference only base tables and the self-reference (not other CTEs)
  - no aggregates, no subqueries, no outer joins inside the recursive body
  - no nested recursion (recursive CTE inside recursive CTE)
  - set semantics (duplicates collapsed per iteration) — deliberate
    deviation from strict `UNION ALL` bag semantics to guarantee termination
    on finite inputs; a 10,000-iteration safety cap throws if a query
    diverges regardless
- `CROSS JOIN` and non-equi `INNER JOIN` — **implemented**. A join with no
  equi-key conjunct (`ON a.x > b.y`, `ON TRUE`, or the `CROSS JOIN` keyword,
  which desugars to `INNER JOIN ... ON TRUE` at parse time) builds a
  `JoinPlan` with empty `EquiKeys` and the whole `ON` predicate as `Residual`.
  Both compiler paths route the two sides through a single unit (zero-column)
  key, so `IncrementalJoinOp` produces the full bilinear cross product and the
  residual filters it — same machinery, no new operator. The op stays bilinear,
  so retractions are correct.
- `FULL OUTER JOIN` — **implemented**. A new `IncrementalFullJoinOp` (plus its
  spine sibling `SpineIncrementalFullJoinOp`) extends the LEFT-join per-key case
  analysis with a symmetric right-pad pass keyed on left-side presence:
  `inner + leftPad` (the existing LEFT-join behaviour, keyed on right-presence)
  plus `rightPad` (NULL-padded-left rows for right rows whose key has no left
  match). The two decompositions are independent, so both-side match flips
  compose correctly to `F(new) − F(old)` per key. NULL-keyed rows on either side
  bypass the operator to their respective NULL-padded branch. The resolver makes
  both sides nullable; `FULL JOIN ... USING (c)` merges each shared column via
  `COALESCE(left, right)` (no non-null side). Supported on both compiler paths
  (typed bails on nullable equi-keys → structural fallback handles the bypass).
  Deferred:
    - **[P1]** `NATURAL JOIN`.
- Comma-join `FROM a, b` (implicit cross join) — **implemented**. Comma-
  separated table references in `FROM` fold left-deep into `INNER JOIN ... ON
  TRUE` cross joins (the comma binds lower than any explicit `JOIN`, so
  `a, b JOIN c ON p` parses as `a × (b JOIN c)`). Rides the same keyless
  unit-key path as the `CROSS JOIN` keyword; `FROM a, b WHERE a.k = b.k` is the
  classic pre-ANSI inner-join spelling (cross product + residual filter, which
  the optimizer's predicate pushdown can later fold into an equi-join).
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
- `ORDER BY` / `LIMIT` / `OFFSET` / `FETCH FIRST` (incremental TOP-K) —
  **implemented**. The parser wraps any query expression in an
  `OrderLimitQuery` (clauses bind to the whole expression, so they work on a
  set-op result and inside a subquery / derived table); the resolver lowers
  `ORDER BY … LIMIT/OFFSET` to a `TopKPlan` and both compiler paths wire a
  `TopKOp` driven by a total-order `SortKeyComparer` (the `ORDER BY` keys plus
  a full-row tiebreak — `StructuralRowComparer` structurally,
  `Comparer<TRow>.Default` on the emitted typed struct). The operator keeps the
  integrated input sorted, recomputes the `[offset, offset+limit)` window each
  tick, and emits the delta against the previously-emitted window, honouring row
  multiplicity at the window edge. `FETCH FIRST [n] { ROW | ROWS } ONLY` and
  `LIMIT ALL` are accepted. Sort keys resolve against the query's **output**
  first — a select-list column / alias, an ordinal (`ORDER BY 2`), or an
  expression over selected columns. A key that references a **non-selected**
  column (e.g. `SELECT a FROM t ORDER BY b`, or a mixed expression like
  `ORDER BY a + b`) is carried as a hidden column: the inner `SELECT` is
  re-resolved with the ordering expression appended (so it resolves against the
  FROM scope, under the resolver's normal aggregate / non-grouped-column rules),
  TOP-K orders by the hidden column, and a final projection strips it. NULL
  ordering follows the PostgreSQL default (`ASC` ⇒ NULLS LAST, `DESC` ⇒ NULLS
  FIRST), overridable with `NULLS FIRST | LAST`. Notes on semantics and what is
  deferred:
  - A **bare** `ORDER BY` (no `LIMIT`/`OFFSET`) is a validated no-op — row order
    is unobservable in the output Z-set, so the result set equals the inner
    query. `LIMIT` without `ORDER BY` works, using the implicit full-row total
    order so the chosen rows are deterministic.
  - Hidden ORDER BY columns apply only to a single `SELECT`. `SELECT DISTINCT`
    rejects ordering by a non-selected column (ambiguous after dedup), and a set
    operation's `ORDER BY` may reference only its output columns — both raise an
    explicit resolver error (matching standard SQL).
  - Partitioned TOP-K / windowed `ROW_NUMBER` / `RANK` / `DENSE_RANK` —
    **implemented** for the standard filter pattern
    `SELECT … FROM (SELECT …, {ROW_NUMBER|RANK|DENSE_RANK}() OVER (PARTITION BY p
    ORDER BY o) AS rn FROM …) WHERE rn <= k` (also `< k`, and the reversed
    `k >= rn`). Standard SQL evaluates window functions after `WHERE`, so the
    derived-table-plus-outer-filter spelling is the portable one. A new
    `WindowFunctionExpression` AST node carries the `OVER (…)` clause (`OVER` /
    `PARTITION` are contextual, not reserved); the resolver recognises the shape
    structurally (there is no general window plan node) and lowers it to a
    `PartitionedTopKPlan` driven by a new `PartitionedTopKOp<TRow,TKey>` — the
    per-partition generalisation of `TopKOp`, with one sorted window per
    `PARTITION BY` key (an empty `PARTITION BY` ⇒ a single global partition).
    `ROW_NUMBER` cuts at the limit position (multiplicity-counted); `RANK` /
    `DENSE_RANK` keep whole tie groups, detected via a keys-only comparer
    (`ConstantZeroComparer` tiebreak). Supported on both compiler paths and
    snapshotable. PARTITION BY / ORDER BY may reference non-selected columns
    (carried as hidden columns, reusing the ORDER BY machinery). Deferred:
      - **[P2]** Selecting the rank value into the output (`SELECT …, rn …`).
        Today the rank exists only to drive the `<= k` cut, so the output schema
        is unchanged; emitting the rank as a column is a separate feature.
        Rejected with an explicit error.
      - **[P2]** `QUALIFY` (Snowflake/BigQuery/DuckDB sugar for the same pattern
        without a derived table).
      - **[P2]** A window function over a grouped/aggregated/`DISTINCT` inner
        query, or more than one window per query. Rejected with explicit errors.
      - See the general windowed-column form (a rank column on every row) under
        Window functions below — deliberately not supported (unbounded
        incremental churn).
- `SELECT DISTINCT` (SELECT-list form) — **implemented**. A `Distinct`
  flag on `SelectStatement` (parsed after `SELECT`; `ALL` is the
  bag-semantics no-op default); the resolver wraps the projection's
  `ProjectPlan` in a `DistinctPlan`, so the existing `DistinctOp` (and its
  spine sibling) handles the dedup incrementally on both compiler paths.
  Distinct-inside-aggregates (`COUNT(DISTINCT x)`) is still listed
  separately under Aggregate functions.
- `CASE WHEN ... THEN ... [ELSE ...] END` (searched form) and
  `CASE x WHEN v THEN ... END` (simple form) — **implemented**. Modeled
  as a flat `CaseExpression(whens, elseResult)` AST node so the recursive
  walkers stay shallow regardless of arm count; the simple form desugars
  to the searched shape at parse time (each arm becomes `x = v`). Branch
  evaluation is lazy (non-taken THEN/ELSE never evaluated) and an arm is
  taken iff its condition is a definite TRUE — NULL/FALSE fall through
  (SQL three-valued semantics). Result type unifies all branches +ELSE
  via `CommonComparableType`; nullable when ELSE is absent. Supported on
  both the typed fast path and the structural compiler. The function-form
  variants `IIF(c, a, b)` and `DECODE(expr, s1, r1, …, [default])` are
  **implemented** too — desugared to a `CaseExpression` in the parser
  (`DECODE` uses NULL-safe equality per arm, since Oracle DECODE matches
  `NULL = NULL` unlike `=`), so they need no resolver/compiler support.
- `BETWEEN x AND y` / `NOT BETWEEN` — **implemented**. Parse-time desugar
  to `x >= a AND x <= b` (and `x < a OR x > b` for `NOT BETWEEN`, the De
  Morgan dual that agrees under 3VL); no new AST node or runtime work. A
  subquery as the test operand is rejected (it would be reference-
  duplicated across the two bounds).
- `LIKE` / `ILIKE` / `SIMILAR TO` (with optional `ESCAPE`) — **implemented**.
  Parse-time desugar to boolean scalar-function calls (`like` / `ilike` /
  `similar_to`) resolved and lowered through `ScalarFunctionRegistry`; the
  `[NOT]` form wraps in a unary `NOT`, inheriting SQL three-valued NULL
  handling. The keywords are *contextual* (matched by identifier text like
  `OVER`/`PARTITION`), so none of `like`/`ilike`/`similar`/`to`/`escape` is
  reserved. Lowering translates the pattern to a .NET `Regex` — `%`→`.*`,
  `_`→`.`, whole-string anchored, `Singleline` so `_`/`%` span newlines; a
  constant pattern is compiled once at build time and baked in, otherwise it is
  translated + cached per pattern at runtime. `SIMILAR TO` additionally passes
  the SQL-regex metacharacters `| * + ? ( ) { } [ ]` through (with `.` etc.
  treated as literals). The escape character defaults to backslash
  (PostgreSQL-aligned); `ESCAPE ''` disables it. No typed fast path (returns
  null → structural compile), as with the other string predicates. *Deferred:*
  POSIX class names inside bracket expressions (`[[:alpha:]]`).
- `REGEXP_LIKE` / `REGEXP_REPLACE` / `REGEXP_SUBSTR` — **implemented**.
  POSIX-regex functions built on the same `SqlPatternMatch` regex cache as
  LIKE, but **substring** matches (not whole-string anchored) — the pattern is
  handed to .NET's engine directly (POSIX ERE ≈ a subset of .NET syntax for the
  common constructs). Optional trailing `flags` string: `i` (ignore case), `c`
  (case-sensitive, default; clears a prior `i`), `m` (multiline `^`/`$`), `s`
  (dot matches newline), `g` (global — `REGEXP_REPLACE` only). `REGEXP_REPLACE`
  follows PostgreSQL: replaces the **first** match by default, all with `g`;
  the replacement string supports `\1`…`\9` and `\&` backreferences (translated
  to .NET `$1`/`$0`). `REGEXP_SUBSTR` returns the first match or NULL. Regular
  registry entries; ordinary function-call syntax (the `~` / `~*` / `!~` / `!~*`
  match operators are *deferred* — they need new lexer tokens around `!`/`!=`).
  Shares the `[[:alpha:]]` POSIX-class gap noted above.
- `IN (literal_list)` / `NOT IN (literal_list)` — **implemented**.
  Modeled as a flat `InListExpression(probe, values, isNegated)` AST
  node, not a parser-time desugar to a left-leaning OR chain. The flat
  representation keeps every recursive walker (resolver, expression
  compiler, monotonicity analyzer, optimizer passes) at constant depth
  contribution; a desugar would build an O(N)-depth tree and risk a C#
  stack overflow on large lists (.NET's practical recursion limit is
  ~100-200 levels). Compiles to a single iterative call into
  `InListRuntime.Evaluate` that honours SQL three-valued NULL semantics.
- `IS [NOT] DISTINCT FROM` — **implemented**. NULL-safe (in)equality,
  always a definite boolean (two NULLs are not distinct; a one-sided NULL
  is distinct). Parse-time desugar to guarded nodes —
  `a IS NOT DISTINCT FROM b` ≡ `(a IS NULL AND b IS NULL) OR (a IS NOT
  NULL AND b IS NOT NULL AND a = b)` — where the `IS NOT NULL` guards make
  3VL `FALSE AND (a = b)` collapse to FALSE so a one-sided NULL never leaks
  UNKNOWN; `IS DISTINCT FROM` is its negation. No new resolver/compiler
  support.
- `IS TRUE` / `IS FALSE` / `IS UNKNOWN` (and the `IS NOT …` forms) —
  **implemented**. Definite-boolean tests (never NULL) over a boolean
  operand, in the same `ParseIsNull` arm as `IS [NOT] NULL` / `IS [NOT]
  DISTINCT FROM`. Parse-time desugar with no operand duplication:
  `b IS TRUE` ≡ `COALESCE(b, FALSE)`, `b IS FALSE` ≡ `NOT COALESCE(b, TRUE)`
  (the `IS NOT` forms negate); `b IS [NOT] UNKNOWN` ≡ `b IS [NOT] NULL`. The
  `COALESCE(b, <bool>)` also pins the operand to BOOLEAN at resolve time.
- `JOIN ... USING (col, ...)` — **implemented**. The parser carries a
  `UsingColumns` list on `JoinClause`; the resolver builds the equi-join
  directly (one `JoinEquality` per shared column, resolved against each
  side) and wraps the `JoinPlan` in a `ProjectPlan` that merges each shared
  column to a single unqualified copy (taken from the preserved side for
  outer joins). Output order is SQL-standard: merged USING columns, then
  remaining left, then remaining right. Supported for INNER / LEFT / RIGHT.
- **[P2]** `VALUES (...), (...) AS t(a, b)` as a row source.
  `ParsePrimaryTableRef` accepts only `(SELECT ...)` or a base table;
  would need a literal-table constructor.
- `||` string concatenation operator — **implemented**. A run of `||`
  parses to a single flat `FunctionCallExpression("||", …)` (not a binary
  chain), keeping walkers shallow and compiling each operand once. Unlike
  PG-style `CONCAT` (which skips NULLs), `||` PROPAGATES NULL per the SQL
  standard — it's a distinct internal builtin, not a `CONCAT` desugar.
  Supported on both compiler paths.
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
- `ROW_NUMBER` / `RANK` / `DENSE_RANK` **restricted to the TopK filter pattern**
  are **implemented** (see "Partitioned TOP-K" under Query constructs above).
  The rest are deferred:
- **[P2]** The general windowed-column form — a rank/number emitted as an output
  column on every row (`SELECT *, ROW_NUMBER() OVER (…) AS rn FROM t`, used
  outside a `<= k` filter). Incrementally expensive: inserting one row shifts the
  rank of every later row in the partition, so a single insert produces
  O(partition-size) retractions. Feldera restricts the rank functions to TopK
  patterns for the same reason; DbspNet does too.
- **[P2]** Window aggregates (SUM/COUNT/AVG/MIN/MAX OVER), LAG / LEAD,
  FIRST_VALUE / LAST_VALUE. Feldera supports these (UNLIMITED RANGE frames).
- **[P3]** Full SQL frame spec: `ROWS`, `RANGE`, `GROUPS`; `PARTITION BY`;
  `BETWEEN … AND …` with named bounds.

### Type system
- **[DONE]** INTERVAL (core) and date/time arithmetic. `INTERVAL '…' <unit>`
  literals (single-field `YEAR`/`MONTH`/`DAY`/`HOUR`/`MINUTE`/`SECOND`, plus
  `YEAR TO MONTH` and `DAY TO SECOND` compounds) parse and resolve to an
  `Interval` value (`(int Months, long Micros)` — the two SQL interval classes
  carried side by side) typed by an `IntervalQualifier`. Arithmetic: `date`/
  `time`/`timestamp ± interval`, `interval ± interval` (same class),
  `interval × / ÷ numeric`, and `date − date` / `ts − ts` / `time − time`
  → interval. Month addition is calendar-aware; DATE arithmetic is
  day-granular (a day-time interval shifts a DATE by whole days). Both compile
  paths supported: full support on the structural compiler (so the batch
  reference and incremental agree); the typed fast path falls back to
  structural for temporal/interval ops, matching temporal comparisons.
  Deferred follow-ons: `INTERVAL` *stored columns* through the Arrow codec
  (intervals are intermediate-only today — a persisted/snapshotted interval
  output column would need an Arrow `MonthDayNano` mapping); `interval ×
  decimal`; and typed-fast-path temporal arithmetic.
- **[P2]** TIMESTAMP WITH TIME ZONE and typed temporal literals
  (`DATE 'yyyy-mm-dd'` etc.). DATE / TIME / TIMESTAMP base types exist with
  Arrow-aligned representations (`Date32`, `Time64[microsecond]`,
  `Timestamp[microsecond]` naive), with `CAST` from string and ordering /
  equality. (Temporal literals can be written as `CAST('…' AS DATE)` etc.)
- VARCHAR is stored as `Utf8String` (Arrow-aligned
  `ReadOnlyMemory<byte>`) with native UTF-8 equality, ordering, hashing
  (XxHash3), `LENGTH` (code points), byte-wise `CONCAT`, and `Rune`-based
  invariant `UPPER` / `LOWER`. `SUBSTRING`, `TRIM`/`LTRIM`/`RTRIM`,
  `REPLACE`, and `POSITION`/`STRPOS` are implemented with native UTF-8 /
  code-point semantics (substring offsets and POSITION results are in code
  points; REPLACE / POSITION search is byte-wise, which is correct for
  valid UTF-8). `LIKE` / `ILIKE` / `SIMILAR TO` pattern matching is
  implemented by decoding to a .NET string and matching a translated `Regex`
  (see the SQL-features section above).
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
  `IncrementalJoinOp`, `IncrementalLeftJoinOp`, `IncrementalFullJoinOp`,
  `SemiNaiveFixpointOperator` (recursive CTEs) —
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
- `LATENESS` bounds — **implemented** (bounded-history trace GC driven by a
  monotonicity analysis, the standard answer across long-running IVM engines:
  Feldera's `MonotoneAnalyzer` + `IntegrateTraceRetainKeysOperator`,
  Materialize's temporal filters, Flink's watermark-driven state expiry). A
  column declared `LATENESS d` in `CREATE TABLE` is lifted to
  `ScanPlan.ColumnLateness`; `MonotonicityAnalyzer`
  (`DbspNet.Sql/Plan/MonotonicityAnalyzer.cs`) propagates monotonicity through
  filters / projections / equi-joins / group keys / set ops — including monotone
  scalar functions (`date_trunc`, `dateadd`) and forward-shift arithmetic
  (`ts + interval`), carrying a frontier transform so a `date_trunc` group key is
  GC'd against the truncated bound; a per-column
  `LatenessOperator` drops late rows at the input and advertises a
  `max_seen − d` frontier; and the keyed stateful operators
  (`IncrementalAggregateOp`, inner / `LEFT` / `RIGHT` join, `DistinctOp`, and
  their spine siblings) garbage-collect sub-frontier trace state. GC reduces
  state, never output. The frontier source is persisted across snapshot. See
  ARCHITECTURE.md *LATENESS / bounded-history trace GC* and `README.md`.
  Deferred follow-ons: duration-literal sugar (`'1' HOUR`) and a first-class
  `INTERVAL` type (the bound is written today as a plain integer in native
  units); GC on the typed-row fast path (`LATENESS` forces the structural
  compile); and a recursive-fixpoint retain-keys story
  (`SemiNaiveFixpointOperator`'s materialised `_r` still grows unbounded — only
  joins / aggregates / distinct are bounded so far).
- **Temporal filters (`NOW()` / `CURRENT_TIMESTAMP`)** — **implemented** (the
  `mz_now()`-style advancing-clock model; the headline streaming feature). An
  injected, monotone, persisted per-circuit logical clock (`RootCircuit.AdvanceTime`,
  microseconds; persisted in the snapshot manifest as `logical_time`) that
  `NOW()` resolves to, legal only in WHERE predicates of the form
  `key {<|<=|>|>=} NOW() [± constant day-time INTERVAL]` (both operand orders;
  `BETWEEN` folds into one window). The resolver folds these into a
  `TemporalFilterPlan`; `PlanToCircuit` compiles it to a time-driven
  `TemporalFilterOp` that emits inserts/retractions as the clock advances with
  no new input (recompute-and-diff, like TOP-K). `NOW()` outside a sanctioned
  predicate is a resolve error. Correctness is the redefined oracle
  (incremental output at the final clock == batch with `NOW` = that clock); the
  clock doubles as a watermark so a windowed (disappear-bounded) filter GCs both
  its own state and a downstream `GROUP BY`/join/`DISTINCT` on the time-key. See
  [`now-and-temporal-filters.md`](now-and-temporal-filters.md). Deferred:
  `CURRENT_DATE`/`CURRENT_TIME`; spine sibling; a transition-time index (per-tick
  recompute is O(state)); monotone-expression time-keys / non-direct-scan inputs
  for downstream GC; typed fast path; WAL per-tick clock recording.
- **[P3]** `WATERMARK` (experimental in Feldera).
- **[P2]** `append_only` table annotation. Feldera-specific.
- **[P2]** `emit_final` view annotation. Feldera-specific.

### UDFs
- **[P2]** User-defined scalar, table, and aggregate functions. The
  prerequisite refactor — reifying the builtin scalar library behind an
  `IScalarFunction` registry (which also carries the monotone-function
  catalog hook for LATENESS, now landed) — is done; see
  [`scalar-function-registry.md`](scalar-function-registry.md). The remaining
  UDF-specific work is the registration surface and the determinism contract:
  scalar functions must be pure for incremental correctness, and the engine
  cannot verify a UDF's purity.

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

- ~~**[P1]** `GROUP BY` supports only bare column references.~~ **Done.**
  Expression grouping (`GROUP BY a + b`, `GROUP BY LENGTH(name)`,
  `GROUP BY CAST(ts AS DATE)`) is supported: the resolver accepts any scalar
  group-key expression (aggregates rejected), non-aggregated SELECT/HAVING
  sub-trees that equal a key read from its output column (matched by a
  structural `AstEqual`), and `PlanToCircuit` / `BatchPlanEvaluator` rekey by
  running compiled key delegates. A monotone key (e.g. `CAST(ts AS DATE)`) picks
  up a temporal filter's day-space GC frontier directly — no derived-table
  workaround.
- **Outer joins** (`LEFT` / `RIGHT [OUTER] JOIN`) still require at least one
  equi-key conjunct in `ON`; their keyed match-presence tracking has no
  keyless operator. `INNER JOIN` no longer has this restriction — pure-
  inequality joins (`ON a.x > b.y`) and cross joins (`ON TRUE` / `CROSS JOIN`)
  are supported via the unit-key nested-loop path (see the `CROSS JOIN` /
  non-equi `INNER JOIN` entry under SQL surface → Query constructs).
- **[P1]** `SELECT *` is forbidden with `GROUP BY` / aggregates. Feldera
  (via Calcite) rewrites to an explicit column list during resolution.
- **[P1]** Scalar function library. Currently supported:
  `COALESCE`, `CAST`, `UPPER`, `LOWER`, `LENGTH`, `CONCAT`,
  `SUBSTRING`/`SUBSTR`, `TRIM`/`LTRIM`/`RTRIM` (whitespace default or a
  custom char set), `REPLACE`, `POSITION(x IN y)`/`STRPOS`, `ABS`, `FLOOR`,
  `CEIL`/`CEILING`, `ROUND`, `POWER`, `SQRT`, `SIGN`, `LN`, `LOG`
  (base-10, or `LOG(b, x)`), `EXP`, `GREATEST`, `LEAST`, `NULLIF`, and the
  temporal functions `EXTRACT(field FROM …)` / `DATE_PART`, `DATE_TRUNC`,
  `DATEADD`, `DATEDIFF`. String functions are native UTF-8 / code-point.
  `SIGN`/`LN`/`LOG`/`EXP` cast the operand to DOUBLE in the resolver, so the
  multi-arg string functions and the temporal functions fall back from the
  typed-row fast path to the structural compile (matching temporal
  arithmetic / comparison behaviour).
  Missing and commonly needed: other math (`SIN`/`COS`/`TAN`, `MOD`,
  `TRUNC`). `NOW`/`CURRENT_TIMESTAMP` is **not** a scalar function — it is
  non-deterministic (breaks the purity the registry and the incremental≡batch
  oracle rest on) and is instead **implemented as advancing temporal filters**
  (the `mz_now()` model): an injected, monotone, persisted logical clock legal
  only in sanctioned WHERE predicates (`ts {<|<=|>|>=} NOW() [± day-time
  INTERVAL]`), compiled to a time-driven operator that retracts rows as the
  clock advances. See [`now-and-temporal-filters.md`](now-and-temporal-filters.md)
  and the Streaming-extensions entry below. Deferred: `CURRENT_DATE` /
  `CURRENT_TIME`.
  **Dispatch routes through `ScalarFunctionRegistry`** (the `IScalarFunction`
  framework — phases 1–3 landed): every builtin is now a registry entry in
  `ScalarFunctionLibrary.cs`, the four parallel switches are gone, and
  `BuiltinScalarFunctions` / `TypedBuiltinScalarFunctions` remain only as
  implementation-helper libraries the entries delegate to. The
  `Monotonicity()` hook for LATENESS GC (phase 4) is landed —
  `date_trunc` / `dateadd` / `ts + interval` group keys keep bounded-history GC
  via a frontier transform. Still deferred: UDFs (phase 5). See
  [`scalar-function-registry.md`](scalar-function-registry.md). The keyword
  spellings
  `SUBSTRING(s FROM a FOR b)` and `TRIM(LEADING|TRAILING|BOTH … FROM …)`
  are not parsed — use the comma / char-set forms.
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
  SQL compiler integration has landed: `PlanToCircuit.Compile(plan,
  snapshotCodecs, new CompileOptions { TraceFamily = TraceFamily.Spine })`
  routes the structural compile's stateful sites through the spine
  builders (supplying a `StructuralRowComparer` for the sorted
  batches), validated by a spine pass of the random-query PBT and by
  spine snapshot round-trip tests. See `docs/persistence.md` "What
  ships in (D) — spine".
  Spine on the typed-row fast path — **implemented**. `TypedRowEmitter`
  emits `IComparable<TSelf>` + non-generic `IComparable` on every
  generated struct (lexicographic field compare via `Comparer<T>.Default`,
  consistent with the emitted `IEquatable<TSelf>`), so the spine builders'
  default `Comparer<TKey>.Default` fallback works without a per-schema
  comparer. `TypedPlanCompiler` dispatches Distinct / inner / left / right
  joins / aggregate to the spine sibling when `CompileOptions.TraceFamily`
  is `Spine` (see `Emit{Distinct,InnerJoin,LeftJoin,Aggregate}` and the
  `InvokeSpine*` reflection wrappers). `PlanToCircuit` no longer gates
  the typed pipeline on `TraceFamily.Flat`. Verified by the existing
  spine-PBT and SQL compile tests plus dedicated
  `SpineCompileTests.Spine{Distinct,Aggregate,InnerJoin,LeftJoin}_ClosesOverTypedRow`
  that inspect the spine ops' closed generic args. Deferred follow-ons:
    - ~~**[P1]** `RecursiveCteOp` has no spine sibling.~~ **Done.** Recursive
      CTEs honour `TraceFamily.Spine`: the nested fixpoint circuit's import-
      relation integrals use a `SpineZSetTrace` (via the `IImportTrace`
      abstraction, with per-batch snapshot), the same family as every other
      stateful site. The recursive body itself is stateless, so the import
      integral is the only trace. Covered by the spine arm of
      `RecursiveCtePbtTests` and a spine snapshot round-trip.
    - **[P2]** Trace-size-driven automatic flat/spine decision — today
      it's an explicit caller toggle.
- Trace compaction / waterline — **implemented** for the spine traces.
  Each spine batch tracks its `(min, max) monotone-key projection` (set
  at construction from a `Func<TKey, long>` supplied to the trace),
  letting `SpineZSetTrace.DropKeysBelow` and
  `SpineIndexedZSetTrace.DropKeysBelow` dispatch per batch: whole-batch
  drop when the batch's max projection is below the frontier (with
  spill-file delete), keep-in-place when its min projection is at or
  above, and mask-filter when the batch is mixed. Batch ordering and
  level layout are preserved, so the tiered compaction strategy's
  insertion-order assumption stays intact and above-frontier batches
  consolidate on the normal compaction schedule rather than via a
  global rebuild on every frontier advance. Cost is O(touched batches)
  per call, not O(retained state). Deferred follow-ons:
    - **[P2]** Within-group value-monotone consolidation. Today's
      analyzer only flags outer/group keys; an indexed trace whose
      `TValue` carries its own monotone column could consolidate
      per-(key, value) histories more aggressively. Needs
      `MonotonicityAnalyzer` extension.
    - **[P2]** Frontier-aware compaction strategy.
      `ICompactionStrategy` only sees `SpineState`-of-counts; a strategy
      that proactively merges sub-frontier-heavy levels would compact
      hot keys faster than batch-count alone.
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
  numeric↔string, bool→string, string↔temporal (date/time/timestamp), and
  string↔interval. Missing: string→bool (`'t'`/`'f'`),
  decimal-precision-aware numeric narrowing, and cross-temporal casts
  (e.g. `date ↔ timestamp`).

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
