# DbspNet architecture

A map of the codebase: how a SQL query flows through the system, what
each piece does, and where to plug in when you want to add something.
For the *why* behind the choices on this map, see
[`docs/design-notes.md`](docs/design-notes.md); for what's deliberately
left out, [`docs/skipped.md`](docs/skipped.md).

## Project layout

Dependencies point downward — `Core` knows nothing about SQL, `Sql`
knows nothing about Arrow IPC or persistence, etc.

```
                    DbspNet.Core
                   (algebra, Z-sets, circuit runtime,
                    linear + stateful operators, spine)
                        ▲       ▲
                        │       │
                  DbspNet.Sql   │
            (parser, resolver,  │
            logical plan, plan  │
            optimizer, two      │
            plan→circuit paths) │
                        ▲       │
                        │       │
                DbspNet.Arrow   │       DbspNet.Benchmarks
            (RecordBatch ↔      │       (regenerates
             Z-set delta;       │        docs/benchmarks.md)
             IPC streaming)     │
                        ▲       │
                        │       │
              DbspNet.Persistence  ──── DbspNet.Demo
              (WAL, snapshot,           (canonical end-
               snapshot+WAL hybrid       to-end scenarios)
               over IBlobStore)
```

External: `DbspNet.Core` depends on `Clast.BloomFilter` (per-batch
blooms in the spine); `DbspNet.Sql` depends on `Clast.DatabaseDecimal`
(`Decimal128`); Arrow is Apache Arrow's .NET package.

## Compile pipeline

A SQL string becomes a runnable circuit through five stages. Every
stage has an explicit, named output that the next stage consumes —
there's no hidden state passed under the table.

```
SQL text
   │  Parser.ParseStatement
   ▼
SqlStatement AST                   ── DbspNet.Sql/Parser/{Lexer,Parser,Token,Ast/*}
   │  Resolver.Resolve
   ▼
PlanStatement                      ── DbspNet.Sql/Plan/{Resolver,LogicalPlan,
   │  (SelectPlan, CreateTablePlan,    ResolvedExpression,Schema,TypeInference}
   │   CreateViewPlan)
   │
   │  PlanOptimizer.Optimize          ── DbspNet.Sql/Optimizer/PlanOptimizer
   │  (explicit, opt-in)              (predicate pushdown, projection
   │                                   composition, aggregate-input pruning)
   ▼
LogicalPlan
   │
   │  PlanToCircuit.Compile           ── DbspNet.Sql/Compiler/PlanToCircuit
   │     │
   │     ├─ TypedPlanCompiler         ── DbspNet.Sql/Compiler/TypedPlanCompiler
   │     │  .TryCompileWith…             — per-schema emitted struct rows
   │     │                                 (TypedRowEmitter); falls back
   │     │                                 on out-of-scope subtrees
   │     │
   │     └─ structural compile           — ZSet<StructuralRow, Z64> on every
   │        (CompilePlan)                  stream; the canonical path
   ▼
CompiledQuery                       ── DbspNet.Sql/Compiler/{CompiledQuery,
   │  query.Table("…").Insert/Delete   TypedCompiledQuery}
   │  query.Step()
   │  query.Current
   ▼
ZSet<StructuralRow, Z64> delta
```

### Stage details

**Parser** (`DbspNet.Sql/Parser/`). Hand-written recursive-descent over
a hand-written lexer. The lexer emits a `Token` stream; the parser
walks it into a `SqlStatement` AST (records under
`Parser/Ast/SqlStatements.cs` and `Parser/Ast/Expressions.cs`). The AST
mirrors SQL surface 1:1 — joins, projections, expressions are recorded
as written, no semantic flattening.

**Resolver** (`DbspNet.Sql/Plan/Resolver.cs`). Takes a `SqlStatement`
and a `Catalog` of declared schemas and emits a `PlanStatement`. This
is where every name resolves to a column index, every expression gets
a `SqlType`, every `*` expands, aggregates are split into
group-key/agg-call buckets, scalar subqueries are lifted into
`ScalarSubqueryJoinPlan`s, CTEs become `CteRef`s, and resolver
restrictions fire (e.g. equi-key required on joins, `GROUP BY` bare
columns only). Errors throw `ResolveException` with explicit
messages.

**LogicalPlan** (`DbspNet.Sql/Plan/LogicalPlan.cs`). A small algebra of
records: `ScanPlan`, `FilterPlan`, `ProjectPlan`, `JoinPlan`,
`AggregatePlan`, `UnionAllPlan`, `DistinctPlan`, `DifferencePlan`,
`CteScanPlan`, `RecursiveCtePlan`, `ScalarSubqueryJoinPlan`. Every
plan node carries its output `Schema`. Statement-level wrappers
(`CreateTablePlan`, `CreateViewPlan`, `SelectPlan`) sit outside the
relational tree.

**Plan optimizer** (`DbspNet.Sql/Optimizer/PlanOptimizer.cs`). Bottom-
up rewrite to a fixed point. Three rule families today: predicate
pushdown (through Project / Join / UnionAll / Distinct / Difference,
respecting outer-join restrictions; adjacent filters merge),
projection composition (adjacent `ProjectPlan`s fuse via expression
substitution), and aggregate-input column pruning (narrows the input
to just the columns the aggregate references). Opt-in — callers wrap
with `Compile(Optimize(plan))`.

**Plan→circuit** (`DbspNet.Sql/Compiler/PlanToCircuit.cs`). Walks the
logical plan and builds a `RootCircuit`. Two parallel paths:

- *Typed-row fast path* — `TypedPlanCompiler.TryCompileWithStructural-
  Boundary` is tried first. When the plan and every subexpression are
  in scope, internal stages run typed: streams carry per-schema
  emitted structs from `TypedRowEmitter`, expressions compile through
  `TypedExpressionCompiler` / `TypedBuiltinScalarFunctions`,
  aggregates use `TypedSqlAggregators`. Only the input/output
  boundaries pay a `StructuralRow` ↔ typed conversion. Snapshot
  codecs flow through via `TypedTraceCodecAdapters` so on-disk format
  stays identical.
- *Structural compile* — the fallback. Every stream is
  `ZSet<StructuralRow, Z64>`; expressions compile through
  `ExpressionCompiler` / `BuiltinScalarFunctions`; aggregates use
  `SqlAggregators`. This is the canonical path and the only one that
  honours a custom `IRowCodec<StructuralRow>` (e.g.
  `EmittedEqualityCodec`).

Both paths share the same operator catalog below and the same
snapshot/codec interfaces.

## Runtime mechanics

A `RootCircuit` is a directed graph of operators connected by typed
streams. Construction is one-shot via `RootCircuit.Build(builder =>
…)`: the action wires inputs, operators, and outputs through a
`CircuitBuilder`; after `Build` returns the topology is frozen.

**Streams** (`DbspNet.Core/Circuit/Stream.cs`) are edges. Each stream
holds one value — the *current tick's* value. Operators write their
output stream during `Step()`; downstream operators read it in the
same step. There's no buffering across ticks.

**`Step()`** (`RootCircuit.Step`) advances one logical tick:

1. Commit every queued input push (`InputHandle.Push` enqueues; the
   value lands on the input stream when `Commit` runs).
2. Fire every operator in registration order. Registration order is
   topological by construction — `CircuitBuilder` registers an
   operator only after its inputs already exist as streams.
3. Increment `TickCount`.

Each tick is atomic under `SyncRoot`; external threads may
`InputHandle.Push` concurrently, but pushes serialise against the
step. Output is read via `OutputHandle.Current` — the latest delta
emitted on the output stream.

**Persistence interception.** `RootCircuit.Operators` exposes the
registered list in order; `DbspNet.Persistence` uses positional
indices as stable operator IDs (paired with a plan-fingerprint
manifest entry that catches structural drift). `RootCircuit.Restore-
TickCount` lets a snapshot reposition the tick counter to T so
absolute tick numbers stay consistent across snapshot/WAL boundaries.

## Operator catalog

DBSP operators split cleanly into two families.

### Linear operators (stateless)

`DbspNet.Core/Operators/Linear/LinearOperators.cs`. Each satisfies
`Q(a + b) = Q(a) + Q(b)` — incremental form is identical to batch
form. Deltas flow straight through.

| Operator | What it does |
|---|---|
| `MapRows(input, projection)` | Pointwise row transform; same-key rows accumulate weights. |
| `Filter(input, predicate)` | Drops rows that fail the predicate. |
| `Union(left, right)` | Tick-wise Z-set addition. |
| `Difference(left, right)` | Tick-wise Z-set subtraction. |
| `FlatMap(input, expand)` | One row in, zero-or-more out. |
| `IndexBy(input, keyOf)` | Re-shape a flat Z-set into an indexed one. |
| `GroupProject(input, keyOf, valueOf)` | Same, but also drops a value selector for aggregation. |
| `ZSetInput()` | Create an input stream (Z-set, additive merge). |

### Stateful operators (with traces)

`DbspNet.Core/Operators/Stateful/`. Each carries a `Trace` (the
running integral of all deltas seen so far) and incrementalises
against it. Every stateful operator implements `ISnapshotable` for
the persistence layer.

| Operator | File | State | What it does |
|---|---|---|---|
| `DistinctOp` | `DistinctOp.cs` | one `Trace` over input keys | Emits a row at weight +1 the first tick its cumulative weight goes strictly positive, weight −1 when it returns to zero. SQL `DISTINCT` and the set-semantics half of `UNION` / `INTERSECT`. |
| `IncrementalAggregateOp` | `IncrementalAggregateOp.cs` | indexed `Trace` over (group key → value multiset), plus per-key last-emitted aggregate cache and an opaque per-key scratch state | Per touched group: hand the aggregator its prior cache, the per-key delta, and the post-delta multiset. Retracts old aggregate and emits new when the result changed. SUM / COUNT / AVG fold incrementally; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. |
| `IncrementalJoinOp` | `IncrementalJoinOp.cs` | two indexed `Trace`s (one per side) | Bilinear inner-join factoring: `delta = dl ⋈ R + L ⋈ dr + dl ⋈ dr` against the *prior* integrated states, then both traces integrate the new deltas. |
| `IncrementalLeftJoinOp` | `IncrementalLeftJoinOp.cs` | two indexed `Trace`s | LEFT OUTER per-key case analysis on match-presence transitions (stayed-matched / stayed-unmatched / gained-match / lost-match). NULL-padded rows ride the same Z-set. `RIGHT JOIN` is a swap-wrapper at the SQL layer. |
| `RecursiveCteOp` | `DbspNet.Sql/Compiler/RecursiveCteOp.cs` | materialised CTE result `R`, last per-tick base inputs, last full closure | Semi-naïve incremental recursion: preserves `R` across outer ticks; for an insert-only tick, propagates only newly-derivable rows through the recursive body to fixed-point. On any retraction-containing tick, falls back to full batch recomputation. |

### Spine variants

`DbspNet.Core/Operators/Stateful/Spine/`. Sibling family —
`SpineDistinctOp`, `SpineIncrementalAggregateOp`,
`SpineIncrementalJoinOp`, `SpineIncrementalLeftJoinOp` — with the same
external behaviour, but backed by `SpineZSetTrace` /
`SpineIndexedZSetTrace` (tiered immutable sorted-columnar batches with
configurable compaction) instead of a flat dictionary. Per-batch
Arrow IPC snapshot via `SpineSnapshot`; optional disk spill via
`SpineSpillConfig`. Exposed through `SpineStatefulOperators`
extensions, but the SQL compiler doesn't emit them today — see
[`docs/persistence.md`](docs/persistence.md).

### Aggregators

`DbspNet.Core/Operators/Stateful/Aggregators/`. The
`IncrementalAggregateOp` is generic over an `IAggregator<TValue,
TOut>` strategy. Built-ins: `SumAggregator`, `CountAggregator`,
`AvgAggregator`, `MinMaxAggregator`. The SQL layer adds typed
SQL-shaped wrappers in `DbspNet.Sql/Compiler/{SqlAggregators,
TypedSqlAggregators}.cs` that handle NULL skipping, the
linear-emission gate, and per-aggregate result types.

## Extension points

If you want to add…

- **A SQL scalar function (e.g. `SUBSTRING`).** One entry in
  `DbspNet.Sql/Expressions/BuiltinScalarFunctions.cs`
  (`IsKnown`, `Resolve` arm, `Build` arm). The typed pipeline picks it
  up via `TypedBuiltinScalarFunctions` — add the matching entry there.
  Test coverage convention: a fact per arity / per type-coercion edge
  in `tests/DbspNet.Tests/Sql/ScalarFunctionTests.cs`.

- **A SQL aggregate (e.g. `STDDEV`).** A new
  `IAggregator<TValue, TOut>` under
  `DbspNet.Core/Operators/Stateful/Aggregators/`, a typed wrapper in
  `DbspNet.Sql/Compiler/SqlAggregators.cs` and a sibling in
  `TypedSqlAggregators.cs`, plus a new `AggregateKind` and the
  matching arm in `Resolver.cs` and both compilers. If state can be
  recomputed from the trace (the SUM/COUNT/AVG pattern), follow
  `IncrementalAggregateOp.Load`'s replay strategy — no per-state codec
  needed.

- **A SQL type (e.g. `BINARY`).** A new `SqlType` record in
  `DbspNet.Sql/TypeSystem/SqlType.cs`, parser arm for the keyword,
  resolver/inference edges in `TypeInference.cs`, expression-compiler
  support, and Arrow mapping in `DbspNet.Arrow/`. The snapshot layer
  picks it up automatically through the Arrow codecs once the Arrow
  mapping exists.

- **A logical plan node (e.g. `OrderByPlan`).** Record in
  `LogicalPlan.cs`, resolver-side construction, plan-optimizer arm
  (at minimum, the no-op recurse-into-children case in
  `OptimizeNode`), and a compile arm in both `PlanToCircuit.CompilePlan`
  and `TypedPlanCompiler.TryCompileNode`. If the typed compiler can't
  handle it, `return null` there and the structural fallback takes
  over.

- **A new operator.** Implement `IOperator` (and `ISnapshotable` if it
  carries state), wire a `CircuitBuilder` extension in
  `StatefulOperators.cs` or `LinearOperators.cs`. If it holds a trace
  and you want a spine variant, the existing spine operators are the
  template — `SpineZSetTrace` / `SpineIndexedZSetTrace` are drop-in
  replacements for the flat counterparts at the snapshot-codec
  interface.

- **A new storage backend (e.g. S3).** Implement `IBlobStore`
  (`DbspNet.Persistence/IO/`). The contract is *atomic single-blob
  write only* — no directory ops, no multi-blob commits. Validate
  against `BlobStoreContractTests`; if it passes, every persistence
  entry point works.

## Testing

`tests/DbspNet.Tests/` mirrors the source tree:

- `Algebra/`, `Collections/`, `Circuit/` — Core unit tests.
- `Operators/` — per-operator behaviour, including a parallel
  `Stateful/Spine/` subtree for the spine family.
- `Sql/` — parser / resolver / compiler / optimizer, plus per-feature
  suites (CTEs, recursive CTEs, set ops, scalar subqueries, scalar
  functions, temporal, Decimal128, typed pipeline).
- `Arrow/` — round-trip tests for the Arrow IPC boundary.
- `Persistence/` — snapshot round-trip per operator, WAL replay,
  hybrid snapshot+WAL, atomic-write crash points, schema-drift
  detection, retention.
- `EndToEnd/RandomQueryPbtTests.cs` — CsCheck property-based tests:
  generate a random query from one of 40 templates, run it both with
  and without the optimizer, and assert semantic equivalence against a
  batch oracle (`IncrementalOracle`) over the same input deltas. This
  is the test of record for "incremental ≡ batch".
- `Spike/` — feature-flag experiments (emitted equality codec, typed-
  row spike) that have since landed in main code paths.
