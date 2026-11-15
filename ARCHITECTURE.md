# DbspNet architecture

This is a map of the codebase. It covers how SQL becomes a running circuit,
what each subsystem does, and where to plug in new pieces. Rationale lives in
[`docs/design-notes.md`](docs/design-notes.md) and the `docs/design-*.md`
notes. Deliberate omissions are tracked in [`docs/skipped.md`](docs/skipped.md).

## Projects

All projects target `net10.0` (`Directory.Build.props`, warnings as errors).
Package versions are pinned centrally in `Directory.Packages.props`.

```
DbspNet.Core                    algebra, Z-sets, circuits, operators, parallel runtime,
  ▲                             trace families (flat / blocks / spine), Core/IO file abstraction
  │
DbspNet.Sql                     parser, resolver, logical plan, optimizer, plan→circuit
  ▲                             compilers (structural + typed), programs, batch oracle
  │
DbspNet.Arrow                   Arrow RecordBatch ↔ Z-set delta, IPC streaming
  ▲
  │
DbspNet.Persistence             snapshots, WAL, tick journal, parallel checkpoints, re-shard,
  ▲                             run files, block format, block cache, spill
  │
DbspNet.Connectors.Abstractions input/output connector interfaces, PipelineRunner,
  ▲                             ProgramRunner, checkpoint stores and policy
  │
DbspNet.Connectors.EngineeredWood  Delta Lake input/output, Parquet input
  ▲
  │
DbspNet.Server                  ASP.NET control service (deploy/resume/wait/stats); Dockerfile

DbspNet.Demo        → Core, Sql                    end-to-end walkthrough scenarios
DbspNet.Benchmarks  → Core, Sql, Arrow, Persistence  latency, Nexmark, fraud, probes
```

External packages:
- Core: `Clast.BloomFilter` and `Clast.DatabaseDecimal` (`Decimal128`).
- Sql: `System.IO.Hashing`.
- Arrow: `Apache.Arrow`.
- Persistence: `ZstdSharp.Port` and `K4os.Compression.LZ4` (run-block compression).
- Connectors: `EngineeredWood.DeltaLake.Table` and `EngineeredWood.Parquet`, from NuGet.

One edge points the other way at runtime. Persistence installs the trace block
format into Core through a module initializer
(`Persistence/Blocks/BlockFormatRegistration.cs`), and the tests force that load
in `TestAssemblyInit.cs`.

## Compile pipeline

```
SQL text
  │ Parser.ParseStatement                 Sql/Parser/{Lexer,Parser,Ast/*}
  ▼
SqlStatement AST
  │ Resolver.Resolve (against a Catalog)  Sql/Plan/{Resolver*,LogicalPlan,TypeInference}
  ▼
LogicalPlan
  │ PlanOptimizer.Optimize                Sql/Optimizer/
  ▼
  │ PlanToCircuit                         Sql/Compiler/
  │   ├─ TypedPlanCompiler  (per-schema emitted struct rows; null ⇒ fall back)
  │   └─ structural compile (ZSet<StructuralRow, Z64> on every stream)
  ▼
CompiledQuery / CompiledProgram / Parallel*  →  Table(…).Insert/Delete, Step(), Current
```

There are two front doors:

- **Single query.** Use `PlanToCircuit.Compile(plan[, codecs, options])`, which
  returns a `CompiledQuery`. It does *not* optimize, so callers write
  `Compile(PlanOptimizer.Optimize(plan))`.
- **Program.** Use `SqlProgram.Compile(statements, outputViews[, codecs, options])`,
  which returns a `CompiledProgram`. This is a DAG of `CREATE TABLE` / `CREATE VIEW`
  statements compiled into one circuit. Each view is optimized and built once and
  shared by its readers, dead views are skipped, and each output is integrated in
  the circuit (`ProgramOutput.CurrentView`, `RowCount`). The Server and
  `ProgramRunner` use this path. `SqlProgram.TryCompileParallel` is its
  data-parallel form, described below.

**Parser.** A hand-written lexer feeds a recursive-descent parser. The AST
mirrors the SQL surface 1:1. Variadic syntax (IN-lists, CASE, GROUPING-style
lists) uses flat nodes, not nested binary ones, which avoids deep recursion.

**Resolver.** It binds names to column indices and types every expression. It
expands `*` and splits aggregates. It lifts subqueries into semi-join and
scalar-subquery-join plans, including decorrelation, and recognises the
window, rank, TOP-K and temporal-filter shapes. Errors throw `ResolveException`.

**Logical plan.** A record algebra in `LogicalPlan.cs`. It covers Scan, Filter,
Project, Join, Aggregate, UnionAll, Distinct, Difference, SemiJoin,
(Correlated)ScalarSubqueryJoin, CteScan, RecursiveCte, TopK, PartitionedTopK,
PartitionedRank, WindowAggregate, WindowOffset and TemporalFilter. Every node
carries its output `Schema`.

**Optimizer** (`Sql/Optimizer/`). It rewrites bottom-up to a fixed point.
- Predicate pushdown (respecting outer-join rules) and filter merging.
- Projection composition.
- Aggregate-input narrowing, including non-linear aggregates when the input's
  weights are provably positive.
- Join column pruning, through inner joins and residual-free outer joins.
- Semi-join subquery narrowing.
- Rank-CTE inlining, so a filtered rank compiles as a TOP-K.
- A final hash-consing CSE (`PlanCse`).
- Program-level dead-column elimination (`PlanColumnLiveness`), opt-in via
  `CompileOptions.EliminateDeadColumns`.

**Compile-time lowering.** These rewrites happen in the compilers, not in the
plan. Each is a `*Mode.cs` switch, default on, with a `DBSPNET_*=off` opt-out
for A/B work.
- Adjacent Filter/Project steps fuse into one `MapFilterRows` / `ApplyOp` pass.
- A join residual is pushed into the join's combine step.
- A join input's Filter/Project chain runs in its indexing pass (index fusion).
- A view or CTE used once is compiled inline.
- A LEFT join builds its consumer's projection directly.
- A run of LEFT joins against small dimensions becomes one `StarLeftJoinOp`.
- A LEFT join on a hot default key is split (hot-key split).

**Typed vs structural.** The single-query path tries `TypedPlanCompiler` first.
On that path streams carry structs emitted per schema by `TypedRowEmitter`
(with a generated `IComparable<TSelf>`), and only the input and output
boundaries convert to and from `StructuralRow`. Any node without a typed arm
returns null, and the whole query compiles structurally. The current fallbacks
are:
- semi-joins, correlated scalar subqueries, and rank-in-output;
- unpartitioned windows and window MIN/MAX;
- `LATENESS` inputs, temporal filters, and a custom row codec.

The structural path is canonical. Block-held traces are wired only there, and
program views compile structurally (`TypeEligibleProgramViews` was measured
and left off).

**`CompileOptions`** (`Sql/Compiler/CompileOptions.cs`):
- `TraceFamily` (Flat or Spine) and spine compaction settings.
- `StoredOutput`, which integrates the output in the circuit.
- The broadcast-join settings (`BroadcastSmallDimensionJoins`, `BroadcastMaxRows`,
  `RelationRowCounts`).
- `ShareArrangements`, `CoalesceJoinExchange` and `EliminateDeadColumns`.
- Order-key monomorphization switches for windows and TOP-K.

## Runtime

A `RootCircuit` is built once with `RootCircuit.Build(builder => …)` and its
topology is frozen afterwards. Streams hold exactly one value, the current
tick's.

`Step()` runs one tick under the circuit's lock:
1. Commit queued input pushes.
2. Fire operators in registration order, which is topological by construction.
3. Release each stream value after its last reader (`StreamRelease`, default on),
   and run memory-pressure relief at operator boundaries.
4. Increment `TickCount`.

`InputHandle.Push` may be called from other threads; it serialises against the
step.

Other circuit-level features:
- **Clock.** `RootCircuit.AdvanceTime` / `CompiledQuery.AdvanceClock` /
  `CompiledProgram.AdvanceClock` set a monotone logical clock in µs (`Clock`,
  `LogicalTime`). It drives `NOW()`, is persisted, and doubles as an `IFrontier`.
  `ProgramRunner` samples it from a `ProgramClock` before each step of a program
  that `UsesClock`.
- **Observability.** `CollectStats()` returns one `OperatorStat` per stateful
  operator that implements `IIntrospectable` (`Circuit/OperatorStat.cs`): retained
  state, last output size, GC frontier and GC drops. It is read on demand and is
  never on the step path. `LastStepDuration` times the last tick.
  `ProfileOperators` / `CollectOperatorProfile` and `StepProfiler` give
  per-operator timings.
- **Persistence hooks.** `Operators` (positional IDs plus a plan fingerprint)
  and `RestoreTickCount`.

### Data-parallel execution

`ParallelCircuit.Build(workers, …)` (`Core/Circuit/`) replicates one circuit
across W workers.
- Inputs are hash-sharded (`ShardedInputHandle`, `StablePartitionHash`), and
  outputs are gathered.
- Exchange operators (`Circuit/Operators/Exchange*`, `BroadcastExchangeOp`)
  repartition streams at a barrier coordinated by `ExchangeCoordinator`.
- The SQL compilers insert exchanges where a keyed operator needs
  co-partitioned input.
- Small dimensions in INNER and LEFT joins can be broadcast instead of
  exchanged. This is size-gated by `BroadcastMaxRows` and `CardinalityEstimator`.
- An exchange sender runs the pointwise chain that precedes it, including
  DISTINCT and TOP-K chains (exchange fusion).
- A global rank is computed in ranges of its order key across workers
  (`RangeRankOp`).

The entry points are:
- `PlanToCircuit.TryCompileParallel`, which returns a `ParallelStructuralCompiledQuery`.
- `TypedPlanCompiler.TryCompileParallel`, which returns a `ParallelTypedCompiledQuery`.
- `SqlProgram.TryCompileParallel`, which returns a `ParallelCompiledProgram`.

Each one returns `false` when the plan can't shard soundly, and the caller then
compiles serially. `SqlProgram.ParallelBlockers` explains the refusal. Current
blockers include recursive CTEs and some semi-join or difference shapes. `LATENESS` runs on both structural parallel
paths. Design notes:
[`design-structural-parallel.md`](docs/design-structural-parallel.md) and
[`design-tpcdi-batch1.md`](docs/design-tpcdi-batch1.md).

## Operators

### Linear (stateless)

These are in `Core/Operators/Linear/LinearOperators.cs`. Each satisfies
`Q(a+b) = Q(a)+Q(b)`, so deltas flow straight through.

| Operator | Role |
|---|---|
| `MapRows` / `FlatMapRows` | pointwise and one-to-many row transforms |
| `Filter` | drops rows failing a predicate |
| `MapFilterRows` | fused Filter/Project chain (one pass) |
| `Union` / `Difference` / `Negate` | Z-set addition / subtraction / negation |
| `ZSetInput` | an input stream |

`IndexBy`, `GroupProject` and their fused forms (in `StatefulOperators.cs`)
reshape a Z-set into an indexed Z-set. `Apply`, `Delay` and the exchange
operators are `CircuitBuilder` methods.

### Stateful

These are in `Core/Operators/Stateful/`. Each keeps an integral (a trace) and
incrementalises against it. All of them implement `ISnapshotable`.

| Operator | Role |
|---|---|
| `DistinctOp` | set semantics (`DISTINCT`, `UNION`, `INTERSECT`) |
| `IncrementalAggregateOp` | grouped aggregates; an `IAggregator` per call; a lazy before+delta group view (`LazyMergeMultiset`) |
| `LinearAggregateOp` | trace-free path for linear aggregates (SUM/COUNT/AVG) |
| `IncrementalJoinOp` | inner join, `ΔL⋈R + L⋈ΔR + ΔL⋈ΔR`; unit key for CROSS / non-equi |
| `IncrementalLeftJoinOp` / `IncrementalFullJoinOp` | outer joins via match-presence transitions; RIGHT is a swap |
| `ResidualOuterJoin` | outer joins with a non-equi `ON` residual |
| `StarLeftJoinOp` | a run of LEFT joins against small dimensions as one operator |
| `IncrementalJoinSharedRightOp`, `Arrangement` | joins sharing one indexed arrangement |
| `TopKOp` | `ORDER BY … LIMIT/OFFSET` (recompute-and-diff of the window) |
| `PartitionedTopKOp` (+ `…CompactOp`, `…NarrowOp`) | `ROW_NUMBER/RANK/DENSE_RANK … <= k` per partition |
| `PartitionedRankOp` | a rank emitted as a column on every row |
| `RangeRankOp` | a global rank split across workers by key range |
| `PartitionedWindowAggregateOp` | `agg OVER (PARTITION BY … ORDER BY … RANGE …)` |
| `PartitionedOffsetOp` | `LAG` / `LEAD` / `FIRST_VALUE` / `LAST_VALUE` |
| `TemporalFilterOp` | `NOW()` windows; rows age in and out as the clock advances |
| `LatenessOperator` | drops late rows; advances the `LATENESS` frontier |
| `IntegrateOp` | stored output (the full view, not just deltas) |

Recursion uses the nested-circuit fixpoint primitives in `Core/Operators/Nested/`
(`SemiNaiveFixpointOperator`, `FixpointOperator`, `NestedScopeBuilder`).
Inserts are semi-naive and deletes use delete-and-re-derive (DRED). Only
multiset (non-set) input deltas fall back to recompute.

Semi-joins and anti-joins are compiled in SQL from these operators. They have
no dedicated operator.

**Aggregators** (`Stateful/Aggregators/`):
- SUM, COUNT, AVG and MIN/MAX.
- Sketches: `HyperLogLog` behind `APPROX_COUNT_DISTINCT`, and `DdSketch` /
  `OrderedQuantileSketch` behind the approximate quantiles.
- The `ILinearAggregator` / `IPartialAggregator` capabilities.

The SQL layer wraps these for NULL handling and result types in
`SqlAggregators` and `TypedSqlAggregators`, including the exact
`COUNT(DISTINCT)`.

### Trace state

There are three representations behind the same operator contracts.

- **Blocks** (default). Code in `Stateful/Blocks/`: `BlockedIndexedTrace`,
  `BlockedZSetTrace`, `BlockedGroupTrace`, `BlockedJoinOp`, `OrderedPartitionStore`,
  and others. Trace state is held as reference-free byte blocks: sorted,
  hash-ordered runs merged geometrically. The GC sees bytes, not object graphs,
  and a checkpoint writes the blocks directly. `DBSPNET_TRACE_BLOCKS` selects
  which operators use blocks: `join`, `agg`, `distinct`, `view`, `window`;
  unset means all, `off` means none. Structural path only. See
  [`design-flat-trace-state.md`](docs/design-flat-trace-state.md).
- **Flat managed.** Hash-indexed Z-sets on `ChunkedMap` (`Core/Collections/`,
  default; `DBSPNET_ZSET_MAP=dictionary` opts out). This is the typed path's
  state, and the fallback for anything blocks don't cover.
- **Spine.** `Stateful/Spine/` is an LSM-style sibling family (`SpineZSetTrace`,
  `SpineIndexedZSetTrace`, `Spine*Op`) with tiered compaction, per-batch Bloom
  filters and optional spill. It is opt-in through `TraceFamily.Spine` on both
  compiler paths and is not the default; see
  [`decision-trace-family.md`](docs/decision-trace-family.md).

### Bounded memory

All of this lives in `Stateful/Blocks/` and `Persistence/Blocks/`. Only stream
release and pressure levels are on by default; the rest is opt-in.
- `StreamRelease` frees values early.
- `MemoryPressure` / `PressureRelief` implement Feldera-style pressure levels
  (`DBSPNET_PRESSURE`, `DBSPNET_MEMORY_LIMIT_MIB`).
- A CLOCK `BlockCache` evicts persisted blocks (`DBSPNET_BLOCK_CACHE_MIB`).
- Mid-batch run spilling (`DBSPNET_SPILL`) and in-step delta spilling
  (`DBSPNET_STEP_SPILL_ROWS` / `_MIB`) write zstd-compressed run files.

The design and measurement journal is
[`design-bounded-memory.md`](docs/design-bounded-memory.md).

### `LATENESS` (bounded-history GC)

A column declared `LATENESS d` promises that no future value falls more than
`d` below the running max.

- **Input.** A `LatenessOperator` per such column drops late rows and advances a
  `MutableFrontier` to `max_seen − d`. `max_seen` is persisted.
- **Plan.** `MonotonicityAnalyzer` (`Sql/Plan/`) proves which columns stay
  monotone. Monotonicity carries through filters, projections, joins, group keys
  and set ops, and through monotone scalar functions (`date_trunc`, `tumble_start`,
  `dateadd`, via `IScalarFunction.Monotonicity`). Each such function comes with a
  frontier transform. Anything unproven is treated as non-monotone.
- **State.** Keyed stateful operators take an optional `(IFrontier, monotoneKey)`
  and drop trace keys below it. GC reduces *state*, never *output*. Joins GC only
  when the key is monotone on both sides. Spine batches GC per batch, using
  each batch's min/max key range.

**Programs.** On the serial program path (`CompileProgram`), each declared
column gets one `EnforceLateness` on the shared table stream, so every view sees
the same late-row drop, and one frontier shared by the whole program. Each view
exports its output-column verdicts (`MonotonicityInfo.RootColumns`). A view
that scans it is analyzed with those verdicts as the scan's seed, and a
single-use view inlined into its consumer is analyzed through its body, like a
CTE. Monotonicity therefore crosses view boundaries exactly as if the views
were written inline, and the sources stay base-table columns.

**Parallel.** Both structural parallel paths enforce `LATENESS`. Each replica
sees only its shard, so a frontier built from its own max would drop different
rows than the serial path. Instead, each replica's `LatenessOperator` joins a
step-boundary `GlobalMaxReduction`. After every `ParallelCircuit.Step`, the
driver takes the max of the replicas' `max_seen` and hands it back to each.
- The driver does this between the "done" rendezvous and the next "go", so it
  needs no extra barrier or exchange: about 15 ns per column per step at W=12.
- Every replica therefore drops against the global frontier as of the start of
  the tick, which is the serial rule and Feldera's (it filters on the delayed
  waterline).
- In-tick GC uses the replica's own advance, which never exceeds the serial
  value.
- Every replica persists the same `max_seen`. A re-shard checks the copies are
  byte-identical and copies one to every new worker.

See [`design-parallel-frontier.md`](docs/design-parallel-frontier.md).

Limits: `LATENESS` forces the structural compile on the single-query path. A
typed program view (`TypeEligibleProgramViews`, off by default) compiles
without GC, which is sound but unbounded. The typed parallel compile refuses
`LATENESS`.

### Temporal filters (`NOW()`)

`NOW()` / `CURRENT_TIMESTAMP` is the advancing logical clock (the `mz_now()`
model), not a scalar function.

- The resolver accepts `key {<,<=,>,>=} NOW() [± day-time INTERVAL]` in WHERE and
  lowers it to a `TemporalFilterPlan`. A `BETWEEN` folds into one window.
- `TemporalFilterOp` emits inserts and retractions as the clock moves, including
  on ticks with no input. It self-GCs.
- A bounded window advertises `clock − offset` as a frontier, so a downstream
  GROUP BY, join or DISTINCT on the time key GCs through the `LATENESS`
  machinery. The frontier is identified by its window (clock unit, offset, cast),
  not by the base column, so two windows over one column never share one. It
  crosses program view boundaries like a `LATENESS` source.
- **Programs.** `ProgramRunner` advances the clock from wall time once per step,
  rounded down to its resolution and held, not rewound, if the time source steps
  back. It takes a clock-only tick at the end of a batch when time moved, and
  journals each tick's clock for replay.
- **Parallel.** The filter's state stays where its input's partition put each row,
  and the partition passes through. A re-shard places the rows by that partition's
  hash, or refuses when the hash is not reproducible.

The oracle is the batch query with `NOW` set to the final clock. See
[`now-and-temporal-filters.md`](docs/now-and-temporal-filters.md) and
[`design-program-temporal.md`](docs/design-program-temporal.md).

## Persistence

This lives in `DbspNet.Persistence`, over the `ITableFileSystem` abstraction in
`Core/IO`. Built-in stores are `LocalTableFileSystem`, `InMemoryTableFileSystem`
and `PrefixedTableFileSystem`. A store that implements `IDurableTableFileSystem`
gets explicit fsyncs.

The checkpointing pieces:
- `Snapshot.WriteAsync` / `ReadAsync` write end-of-tick snapshots, committed
  crash-safely by a `current.txt` pointer. They carry plan and schema
  fingerprints.
- `WalRecorder` provides input replay and the snapshot+WAL hybrid.
- `TickJournal` records source offsets every batch, with snapshots taken by
  policy.
- `ParallelSnapshot` commits all W workers in one step.
- `ParallelReshard` rewrites a snapshot for another W.
- `Runs/` holds the run-file format (hash-ordered blocks, index, Bloom filter),
  incremental run chains, lazy restore and background compaction. The run
  layout is opt-in (`DBSPNET_STATE_LAYOUT=runs`). Block-held traces always
  checkpoint as run chains from their own blocks.

See [`docs/persistence.md`](docs/persistence.md) and
[`design-durable-state.md`](docs/design-durable-state.md).

## Connectors and server

**`DbspNet.Connectors.Abstractions`:**
- Interfaces: `IInputConnector`, `IOutputConnector`, `ISchemaMapper`, and the
  checkpoint stores (`SnapshotCheckpointStore` keeps two snapshots plus source
  offsets).
- `CheckpointPolicy`: the default journals every batch and snapshots every 60 s.
- `PipelineRunner` drives one `CompiledQuery`, one tick per source version.
- `ProgramRunner` drives a serial or parallel program. Each batch drains every
  available source version and then writes the outputs.

**`DbspNet.Connectors.EngineeredWood`** has three connectors:
- `DeltaInputConnector` reads the change data feed, one Delta version per tick.
- `DeltaOutputConnector` writes in truncate, changelog or incremental mode;
  incremental uses deletion vectors.
- `ParquetInputConnector` reads a Parquet file once.

**`DbspNet.Server`** is the engine container for the ivm-bench TPC-DI
benchmark. It is an ASP.NET minimal API with these endpoints: `GET /healthz`,
`POST /compile`, `POST /deploy` (a `ProgramSpec` of SQL, inputs, outputs and an
optional snapshot directory), `POST /resume`, `POST /wait`, `POST /pause`, and
`GET /stats`. It is configured entirely through `DBSPNET_*` environment
variables. The README lists the user-facing ones.

## Extension points

- **Scalar function.** Add one `IScalarFunction` (`Resolve`, `BuildStructural`,
  `BuildTyped`) registered in `Sql/Expressions/ScalarFunctionRegistry.cs`. Return
  null from `BuildTyped` to fall back to structural. Tests go in
  `tests/DbspNet.Tests/Sql/ScalarFunctionTests.cs`. See
  [`scalar-function-registry.md`](docs/scalar-function-registry.md).
- **Aggregate.** Add an `IAggregator` under `Stateful/Aggregators/`, wrappers in
  `SqlAggregators.cs` / `TypedSqlAggregators.cs`, an `AggregateKind`, and resolver
  plus compiler arms. Persist state only if folding the trace can't reproduce it
  exactly (see `CanPersistState`; `double` sums can't be refolded exactly).
- **SQL type.** Add a `SqlType` record (`Sql/TypeSystem/SqlType.cs`), a parser
  arm, `TypeInference` edges (`Sql/Plan/`), expression-compiler support, and an
  Arrow mapping in `DbspNet.Arrow`.
- **Plan node.** Add the record in `LogicalPlan.cs` and its resolver
  construction. Add arms in the optimizer (including `PlanCse` and
  `PlanColumnLiveness`), in both compilers (`return null` in `TypedPlanCompiler`
  to fall back), in `BatchPlanEvaluator` (the oracle), and in the parallel
  blocker analysis (`PlanToCircuit.ProgramBlockers.cs`).
- **Operator.** Implement `IOperator`, plus `ISnapshotable` if it is stateful and
  `IIntrospectable` for stats. Wire it through a `CircuitBuilder` extension. Add
  a case to `PersistenceConformanceTests`; its coverage guard reflects over every
  `ISnapshotable`.
- **Storage backend.** Implement `ITableFileSystem` (and `IDurableTableFileSystem`
  if it needs explicit syncs). `WriteAllBytesAsync` must be atomic.
- **Connector.** Implement `IInputConnector` / `IOutputConnector`; the
  EngineeredWood project is the template.

## Testing

`tests/DbspNet.Tests/` mirrors the source tree: `Algebra/`, `Collections/`,
`Circuit/`, `Operators/` (including `Nested/` and `Stateful/Spine/`), `Sql/`,
`Arrow/`, `Persistence/`, `Connectors/`, `Observability/` and `EndToEnd/`. It
holds about 2,300 xUnit facts and theories. `Scratch/` holds probes and
measurement harnesses, and `Spike/` holds retired experiments.

The tests of record:
- **`EndToEnd/RandomQueryPbtTests.cs`.** CsCheck property tests over about 70
  generated query templates (`RandomQuery.cs`), 3,000 iterations each. They run
  unoptimized, optimized-vs-original, and on spine, and each run is checked
  against `BatchPlanEvaluator`. This is the "incremental ≡ batch" check.
- **Feature property suites.** LATENESS, temporal filters, TUMBLE, recursive
  CTEs, CSE, TOP-K variants, the four DBSP laws, and parallel global operators.
- **`Persistence/PersistenceConformanceTests`.** Checks that save → restore →
  step equals an uninterrupted run, across the `flat`, `spine` and `blocks`
  families.
- **Crash-safety matrices.** `CrashSafeCheckpointTests` and
  `ParallelCrashSafeCheckpointTests`.
