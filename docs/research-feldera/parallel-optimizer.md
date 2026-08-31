# Feldera findings — §4 parallelism/scheduling/exchange, §5 SQL compilation & optimizer

Checkout: `d:\src\feldera` @ `78afc907773567588e9981be7179f398c0cbd473` (2026-08-29).
All paths below are relative to that root. **Nothing was built or run** — source reading only.
Claims are marked **[V]** verified in source, **[D]** read in their prose docs, **[I]** inferred.

---

# §4 — Parallelism, scheduling, and the exchange model

## 4.0 One-paragraph answer to the question behind the axis

Feldera runs the *same* synchronous BSP family as DbspNet — N OS worker threads, each a full
replica of the circuit, hash-sharded by key, with all-to-all rendezvous per step. They do **not**
structurally avoid barrier coordination; their own source says the shard operator "introduces a
synchronization barrier across all workers" and "limits the scalability"
(`crates/dbsp/src/operator/communication/shard.rs:63-69`) **[V]**. What differs is four things,
all of which reduce the *number of barriers per unit of work* rather than the cost of a barrier:

1. **Per-worker cooperative async scheduling.** Each worker drives its whole circuit on a
   *current-thread* Tokio runtime with a `LocalSet`; every operator is an `async fn eval` spawned
   as a local task. When an operator blocks on an exchange, the worker does **not** stall — the
   scheduler's `select!` loop runs any other operator whose predecessors are done
   (`crates/dbsp/src/circuit/schedule/dynamic_scheduler.rs:588-716`,
   `crates/dbsp/src/circuit/circuit_builder.rs:3229`, `:7808-7817`) **[V]**.
2. **Exchange is split into two nodes** (`ExchangeSender` / `ExchangeReceiver`) precisely so the
   send is non-blocking and the circuit "does not need to block waiting for its peers to finish
   sending and can instead schedule other operators"
   (`crates/dbsp/src/operator/communication/exchange.rs:1616-1618`) **[V]**.
3. **Much coarser default ticks.** Default input batch is `DEFAULT_MAX_WORKER_BATCH_SIZE = 10_000`
   records *per worker* (`crates/feldera-types/src/config.rs:56`, used at
   `crates/adapters/src/controller.rs:7347-7354`) **[V]**. At W=16 a step carries up to 160k rows.
   Barrier cost per row is therefore small by construction.
4. **Transactions collapse many ticks into one computation.** In transaction mode, operators
   *accumulate* input into a spine across many steps and only compute at commit
   (`crates/dbsp/src/operator/dynamic/accumulator.rs:294-330`) **[V]**;
   `docs.feldera.com/docs/pipelines/transactions.md:16-40` **[D]**.

The one place they went further than DbspNet did is a **runtime, statistics-driven repartitioner**
(the "balancer", §4.3) — evidence that their residual scaling problem is *straggler skew*, not
barrier count.

## 4.1 Worker/runtime model; how step completion is detected

**Threads.** `Runtime::run` spawns one OS thread per worker, named `dbsp-worker-{i}`, and pins it
to a CPU: `crates/dbsp/src/circuit/runtime.rs:804-834` (`Builder::new().name(...).spawn(...)`,
`WORKER_INDEX.set(worker_index)`, `runtime.inner().pin_cpu()`) **[V]**. Each worker builds its own
copy of the circuit — `RootCircuit::build(circuit_fn)` runs *inside* the worker closure
(`crates/dbsp/src/circuit/dbsp_handle.rs:733-761`) **[V]**. There is **no work stealing between
workers**: operator tasks are `spawn_local` onto a per-worker current-thread Tokio runtime
(`dynamic_scheduler.rs:406-433`; runtime built at `circuit_builder.rs:3226-3237`;
driven at `circuit_builder.rs:7808-7817` via `block_on` + `LocalSet::run_until`) **[V]**.

Beyond the W foreground threads, there are:

- a **shared multi-threaded Tokio runtime** for IO and background work
  (`crates/storage/src/tokio.rs:9-26`, `Builder::new_multi_thread()`), re-exported as
  `dbsp::circuit::tokio::TOKIO` (`crates/dbsp/src/circuit/tokio.rs:1-4`) **[V]**;
- **background spine mergers**: "Merging work is performed in a separate tokio runtime, where we
  spawn a task" (`crates/dbsp/src/trace/spine_async.rs:719-720`; spawn at `:1300-1313`) **[V]**;
- a memory-pressure monitor thread that wakes mergers when pressure crosses `High`
  (`runtime.rs:740-802`) **[V]**.

So LSM compaction is **off the critical path of a step** and runs on a work-stealing pool, while
the dataflow itself is strictly one-thread-per-worker. That is a materially different bargain from
DbspNet's flat traces with bulk-on-threshold compaction inside the step.

**Scheduling within a worker.** `DynamicScheduler`
(`crates/dbsp/src/circuit/schedule/dynamic_scheduler.rs`) keeps a task per node with
`num_predecessors` / `unsatisfied_dependencies` counters (`:82-113`). `do_step` resets the
counters, spawns every zero-dependency node (`:600-613`), then loops on `select!` over
`JoinSet::join_next()` and a notification channel (`:615-716`) **[V]**. Ownership preferences are
turned into extra scheduling edges so the consumer that wants an owned value runs last
(`circuit/schedule.rs:561-645`) **[V]** — this is how they avoid deep-copying a batch that has
several consumers.

**Step completion.** Purely local and count-based: a step ends when
`completed_tasks == self.tasks.len()` (`dynamic_scheduler.rs:671-676`) **[V]**. There is no
scheduler-level global barrier for a step. The *global* rendezvous per step come from three
separate places:

1. **Driver command broadcast.** `DBSPHandle::step` → `broadcast_command(Command::Step)` sends to
   each worker's channel, unparks it, then waits for all W responses
   (`dbsp_handle.rs:1510-1560`, `:1712-1720`) **[V]**. This *is* a per-step barrier at the
   controller.
2. **Metadata broadcast, every step.** `Inner::step` calls `exchange_metadata` after `do_step`
   (`dynamic_scheduler.rs:504-517`, called at `:551-553`), which does `Broadcast::collect` —
   implemented on top of a real `Exchange` with **serde_json** payloads (`circuit/runtime.rs:1468-1500`) **[V]**. Skipped
   only for non-root scopes. Purpose: give the balancer identical key-distribution snapshots on all
   workers (`circuit_builder.rs:1716-1728`) **[V]**.
3. **Commit consensus**, once per step while in the `Committing` phase
   (`dynamic_scheduler.rs:555-579`) **[V]**.

**Conclusion for DbspNet:** Feldera pays roughly *three* all-worker rendezvous per step, versus
DbspNet's one `ExchangeCoordinator` barrier — they are not cheaper per barrier. Their advantage is
(a) a step covers ~10k rows/worker by default and (b) exchange waits overlap with other operators
on the same worker.

## 4.2 What is physically moved on a shuffle; is there a zero-copy path?

**Local (same-process) exchange is a pointer move, not a copy and not a serialization.** The
mailbox is `Mutex<Option<Mailbox<T>>>` with `Mailbox::{Tx(FBuf), Rx(AlignedVec), Plain(T)}`
(`exchange.rs:1107-1112`) and there are `npeers²` mailboxes, "each accessed by exactly two threads,
so contention is low" (`exchange.rs:1198-1213`) **[V]**. `send_all_with_serializer` picks
`WorkerLocation::Local => Mailbox::Plain(data)` vs `Remote => Mailbox::Tx(serialize(data))`
(`exchange.rs:1430-1442`) **[V]**. So an intra-process shuffle moves an owned batch by value; only
cross-*host* traffic is serialized (rkyv / `FBuf`, framed by `ExchangeHeader`, over TCP:
`exchange.rs:84-170`, `:1470-1505`) **[V]**.

Flow control is a pair of atomic counters plus `Notify` — `sender_counters` (mailboxes free) and
`receiver_counters` (messages arrived) — never a lock-step barrier
(`exchange.rs:1164,1176`, `wait_for_ready_to_send` `:1406-1428`, `receive_all` `:1527-1560`) **[V]**.

**What gets built before the move.** `shard_batch`
(`crates/dbsp/src/operator/dynamic/communication/shard.rs:419-476`) **[V]**:

- one `Builder` per worker, **pre-sized** with `key_count()/shards` and `len()/shards` (`:431-447`)
  — their equivalent of DbspNet's adaptive delta-builder pre-sizing;
- it walks a **`consuming_cursor`** and, when the input batch is owned (`cursor.has_mut()`), uses
  `push_key_mut` / `push_val_mut` / `push_diff_mut` to **move** keys and values out of the source
  batch rather than clone them (`:449-472`);
- the partition function hashes **the key only**, never the whole row:
  `cursor.key().default_hash() as usize % shards + workers.start` (`:452`, `:465`);
- `default_hash` is **xxh3** (`crates/dbsp/src/hash.rs:6-11`) **[V]**.

On the receive side, the receiver collects one batch per peer and a *separate* node
`apply_owned_named("merge shards", merge_batches)` merges them (`shard.rs:131-135`) **[V]** — so
the merge is independently schedulable, not folded into the receive.

There is an explicit un-taken optimization in their source:
`// XXX If shards == 1 and OB and IB are the same, then we could implement this more efficiently,
without copying.` (`shard.rs:428-430`) **[V]**.

## 4.3 Exchange elision; is partitioning a tracked plan property?

**Yes, and more aggressively than DbspNet's.** Sharded-ness is a first-class property of a
*stream*, memoized on the circuit build cache:

- `circuit_cache_key!(ShardId<C, D>((StreamId, Range<usize>) => Stream<C, D>))` and `UnshardId`
  (`shard.rs:32-33`) **[V]**.
- `shard_generic_ref` is `cache_get_or_insert_with(ShardId::new((stream_id, workers)))`
  (`shard.rs:99-106`) — **two operators that shard the same stream to the same worker range share
  one physical exchange** (CSE on the shuffle itself) **[V]**.
- Immediately after building it: `cache_insert(ShardId::new((output.stream_id(), workers)), output)`
  (`shard.rs:140-144`) — **re-sharding an already-sharded stream is a no-op** (elision) **[V]**.
- `mark_sharded` / `mark_sharded_workers` / `mark_sharded_if` / `is_sharded` /
  `has_sharded_version` / `get_sharded_version` / `try_sharded_version` / `try_unsharded_version`
  (`shard.rs:616-688`) **[V]**, used in **81 places** across the operator library
  (`grep -c mark_sharded` over `crates/dbsp/src` = 81) **[V]**: `filter_map.rs:332,393,453,514`
  propagate shardedness through filters/maps; `delta0.rs:27,48` and `differentiate.rs:52,115`
  propagate it into and out of nested circuits; `distinct.rs:300,336,344,449` and
  `aggregate.rs:390,496,498,618` mark keyed outputs sharded; `concat.rs:95,126`,
  `consolidate.rs:34`, `group/rank.rs:185,203`, `accumulate_trace.rs:125,193,450,614,674`.
- Single-worker short-circuit: `if Runtime::num_workers() == 1 { return None }` (`shard.rs:92-94`),
  and `is_sharded()` returns `true` unconditionally at W=1 (`shard.rs:670-673`) **[V]**.

Note the design point: this lives in the **Rust dataflow library**, not the SQL compiler. Each
operator calls `.shard()` on its own inputs and the cache dedups; the compiler is not required to
reason about partitioning at all (see §5). Concretely, `join` shards both of its inputs itself
(`operator/dynamic/join.rs:373-374`, `:393-394`, `:498-499`, `:704-742`), antijoin shards the left
and reuses `other.try_sharded_version()` for the right (`join.rs:424-441`, output
`mark_sharded()` at `:462`), and `aggregate` shards its input (`aggregate.rs:388-390`) **[V]**.

**Measurement hook worth using.** They expose per-operator `exchange_wait_time_seconds`
(`circuit/metadata.rs:74-75`, populated in `exchange.rs:2000`) and per-worker
`circuit_wait_time_seconds` / `circuit_runtime_seconds` / `circuit_cpu_time_seconds` /
`steps_count` (`circuit/metadata.rs:167-176`), where the circuit wait time is measured by park
hooks on the worker's Tokio runtime (`RuntimeIdle`, `circuit_builder.rs:3226-3237`) **[V]**. That
gives a directly comparable number to DbspNet's "barrier WAIT = 40% on q4": run Feldera's profiler
on the same query/W and read `circuit_wait_time_seconds / circuit_runtime_seconds`. **[I]** that
this is the cheapest available experiment to settle whether Feldera's coordination overhead is
actually lower or merely amortised over bigger ticks — I did not run it.

**Beyond elision: a runtime balancer.** `crates/dbsp/src/operator/dynamic/balance/` implements
dynamic re-partitioning. `PartitioningPolicy` is `Shard | Broadcast | Balance` (the last hashes the
whole key-value pair to defeat key skew) (`balancer.rs:190-198`) **[V]**. The balancer:

- builds the **join graph**, takes its strongly connected components as `Cluster`s, and layers each
  cluster so dependent streams are decided in order (`balancer.rs:228-268`) **[V]**;
- encodes join compatibility (`Shard×Shard`, `Shard×Broadcast`, `Broadcast×Balance`, …) as
  **MaxSAT hard constraints** and solves for a policy assignment (`balancer.rs:68-115`;
  solver in `balance/maxsat.rs`) **[V]**;
- feeds on **runtime key-distribution statistics** collected per stream and shared via the per-step
  metadata broadcast (`balancer.rs:305-330`; `KeyDistribution` /
  `RebalancingExchangeSenderExchangeMetadata` in `balance/accumulate_trace_balanced.rs`) **[V]**;
- re-solves only when the distribution has drifted past `KEY_DISTRIBUTION_REFRESH_THRESHOLD = 0.1`
  and the predicted win beats `MIN_RELATIVE_IMPROVEMENT_THRESHOLD = 1.2` /
  `MIN_ABSOLUTE_IMPROVEMENT_THRESHOLD = 10_000`, with a `BALANCE_TAX = 1.1` penalty against
  switching (`balancer.rs:31-34`, `:492-510`, `:727`) **[V]**;
- is driven at transaction/step boundaries from the scheduler: `balancer().prepare(...)`,
  `start_transaction()`, `start_step()`, `update_metadata()`, `transaction_committed()`
  (`dynamic_scheduler.rs:384`, `:463`, `:542-553`, `:576`; balancer side `balancer.rs:1267-1345`) **[V]**;
- accepts SQL-level hints `/*+ shard */`, `/*+ broadcast */`, `/*+ balance */` on joins
  (`sql-to-dbsp-compiler/.../frontend/calciteCompiler/SqlToRelCompiler.java:279-290`;
  `circuit/annotation/JoinStrategy.java:16-23`; emitted at
  `backend/rust/ToRustVisitor.java:1901-1915`) **[V]**, plus programmatic `BalancerHint::{Policy,
  Size, Skew}` (`balancer.rs:277-296`) **[V]**.

That whole subsystem is a direct admission that the real scaling limit of a hash-shard BSP engine
is **skew-induced straggler wait**, which is exactly what a barrier makes visible.

## 4.4 Nested circuits / fixpoint: scheduled differently? do they parallelise?

**Same scheduler, plus one extra global rendezvous per iteration.** A nested circuit is a
`ChildCircuit<P, T>` with its own `Executor`; recursion uses `IterativeExecutor`, which loops
`scheduler.transaction(&circuit)` until a termination check passes
(`circuit/schedule.rs:351-372`, `:402-422`) **[V]**. `Circuit::fixedpoint` supplies that check:

```rust
let consensus = Consensus::new("fixed point");
let termination_check = async move || {
    let local_fixedpoint = child_clone.inner().check_fixedpoint(0);
    consensus.check(local_fixedpoint).await
};
```
(`circuit_builder.rs:4678-4705`) **[V]**. `Consensus` is a `Broadcast<bool>`
(`runtime.rs:1396-1430`) — a real all-worker exchange. Therefore:

- **It parallelises.** Every worker runs the nested circuit over its own shard; shardedness crosses
  the nesting boundary (`delta0.rs:27,48` and `differentiate.rs:52,115` call `mark_sharded_if`) **[V]**.
- **Cost:** one all-worker consensus round **per inner iteration**, on top of whatever exchanges the
  loop body contains. A deep fixpoint multiplies barrier count. Their `Consensus` docs warn that
  every worker must call `check` the same number of times or peers stall
  (`runtime.rs:1405-1418`) **[V]**.
- `check_fixedpoint` accounts for state still buffered *inside* operators (e.g. join results
  precomputed for future nested timestamps), not just an empty output stream
  (`circuit_builder.rs:1967-1980`) **[V]** — stronger than an emptiness test.
- The nested executor runs in a non-root scope, so `exchange_metadata` and the balancer are skipped
  there (guards on `circuit.root_scope() == 0` at `dynamic_scheduler.rs:505-507`, `:541,546,553`) **[V]**.
- The scheduler is pluggable per circuit (`iterate_with_scheduler` / `fixedpoint_with_scheduler`,
  `circuit_builder.rs:4664-4705`), but `DynamicScheduler` is the only implementation shipped
  (`circuit/schedule.rs:20-21`) **[V]**.

## 4.5 Is there asynchrony — pipelining across steps? adaptive batching?

**No pipelining across steps.** The controller's circuit thread is strictly serial: decide an
action, then `self.step()` (`crates/adapters/src/controller.rs:3410-3421`); `step_circuit` calls
`self.circuit.step()`, which broadcasts to all workers and waits for all of them
(`controller.rs:3804-3840`; `dbsp_handle.rs:1712-1720`) **[V]**. Step N+1 cannot begin before every
worker finishes step N. Input is pushed before the step; output is taken after, **per worker**
(`OutputHandle::take_from_worker`, `crates/dbsp/src/operator/output.rs:418`) **[V]** — so there is
**no gather barrier for outputs**; each worker's output shard is read independently by the
controller.

There *is* a knob for accepting input during a multi-step commit
(`CircuitConfig::allow_input_during_commit`, library default `true`:
`dbsp_handle.rs:362,528,550`), but the controller explicitly sets it **false**
(`controller.rs:6069`) **[V]**. So in the shipped product it is off.

**Adaptive batching: yes, at both ends.**

- Lower bound: the controller delays a step until `min_batch_size_records` have accumulated or
  `max_buffering_delay_usecs` elapses (`crates/feldera-types/src/config.rs:944-956`; both default 0)
  **[V]**.
- Upper bound: per-connector `max_batch_size`, else `max_worker_batch_size × workers`, with
  `DEFAULT_MAX_WORKER_BATCH_SIZE = 10_000` (`config.rs:1806-1835`, `:56`;
  `controller.rs:7338-7354`) **[V]**. Their comment: it "automatically adjusts batch size as the
  number of worker threads changes to maintain constant amount of work per worker per batch" **[V]**.
- **Transactions** are the coarse-grained version: an explicit two-level scheduling model of *steps*
  inside *transactions*, documented on the `Scheduler` trait (`circuit/schedule.rs:200-226`) **[V]**.
  During the `Started` phase "each operator gets to decide how much of the input to process. Some
  operators may accumulate inputs to process them later." At `start_commit_transaction` the
  scheduler walks a flush frontier — `flush` is called on a node once all its predecessors have
  finished flushing — and `is_commit_complete()` becomes true when the frontier is exhausted
  (`schedule.rs:218-226`; `dynamic_scheduler.rs:196-224`, `:479-484`, `:650-668`) **[V]**.
  The `Accumulator` operator is the concrete mechanism: it inserts each incoming batch into a
  `Spine` and emits `Some(spine)` **only on flush**, `None` otherwise
  (`operator/dynamic/accumulator.rs:294-330`) **[V]**.

  **This is the most consequential item in §4 for the ivm-bench bulk-load comparison.** With
  `transaction_mode: always`, Feldera ingests the entire historical load across many cheap
  ingest-and-index-only steps and then runs the join/aggregate graph **once** over the accumulated
  spine. It never materialises the intermediate per-chunk deltas that DbspNet computes. Their own
  docs state the motivation plainly: "computing all intermediate updates can be more expensive than
  computing the cumulative update … can be very significant in some cases"
  (`docs.feldera.com/docs/pipelines/transactions.md:25-28`) **[D]**, and "During a transaction, the
  pipeline ingests incoming data without producing output, performing only minimal processing such
  as resolving primary keys and indexing inputs" (`:84`) **[D]**.
- There is also an `EnableCount` on accumulators: when nothing downstream is listening, the
  accumulator **discards** its input instead of accumulating (`accumulator.rs:39-46`, `:306-315`)
  **[V]** — runtime, not compile-time, materialization control.

## 4.6 How a 50-view multi-view program executes

**One circuit, one `RootCircuit`, shared subgraphs, one sink per declared view.** The Rust code
generator emits a single `pub fn circuit0(workers: usize) -> (DBSPHandle, Catalog)` wrapping one
`Runtime::init_circuit(workers, |circuit| { ... })`, in which every table becomes an
`add_input_zset` and every view is registered on one `Catalog`
(`sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/backend/rust/ToRustVisitor.java:115-128`)
**[V]**. `RootCircuit` vs `NestedCircuit` is chosen only by nesting depth
(`backend/rust/BaseRustCodeGenerator.java:52`) **[V]**. There is no per-view circuit, no per-view
driver, and hence no cross-view scheduling problem — which is the direct contrast with DbspNet's
single-threaded `ProgramRunner`.

(Aside on *build* time, not run time: there is an optional **multi-crate** emission mode
(`--crates`, `CompilerOptions.IO.multiCrates()` at
`compiler/CompilerOptions.java:252-254`) that splits the generated program into many Rust crates,
one operator per generated function via `SingleOperatorWriter`, with `CircuitWriter` stitching them
together (`backend/rust/multi/MultiCrates.java`, `multi/CircuitWriter.java:36-44`) **[V]**. This is
purely to parallelise/incrementalise `rustc`; the runtime is still a single circuit.)

**Sharing between views is structural, at two levels:**

1. *Compiler level* — one `DBSPCircuit` DAG for the whole program (see §5.2 for the CSE passes).
2. *Runtime/library level* — arrangement reuse is memoization keyed by `StreamId` on the circuit
   build cache, so any two consumers anywhere in the program (same view or different views) asking
   for the same derived stream get **the same node**:
   - `TraceId(StreamId => Stream)`, `DelayedTraceId`, `BoundsId`
     (`operator/dynamic/trace.rs:42-46`), consumed via `cache_get_or_insert_with(TraceId::new(...))`
     at `trace.rs:342`, `:551`, `:741` **[V]**;
   - plus `IndexId` (`operator/dynamic/index.rs:17`), `ConsolidateId` (`consolidate.rs:15`),
     `DistinctId` / `DistinctIncrementalId` / `PositiveIncrementalId` (`distinct.rs:50-52`),
     `SemijoinId` (`semijoin.rs:21`), `AntijoinId` (`join.rs:52`), `IntegralId` /
     `NestedIntegralId` (`integrate.rs:21-22`), `DifferentiateId` (`differentiate.rs:20-21`),
     `AccumulateTraceId` / `ShardedAccumulateTraceId` / `AccumulateDelayedTraceId`
     (`accumulate_trace.rs:33-41`), `AccumulatorId` (`accumulator.rs:29`),
     `SaturateId` (`saturate.rs:27`), `GatherId` (`gather.rs:30`), `ShardId` (`shard.rs:32`) **[V]**.

   This is differential-dataflow's arrangement reuse, implemented as a build-time memo rather than
   an explicit `arrange()` the query author has to hoist by hand.

**A cost of that sharing** worth knowing before copying it: a shared trace's GC bound is the
**minimum** over all consumers' bounds. `TraceBounds::add_key_bound` pushes each consumer's bound
into a vector and `effective_key_filter` takes `bounds.iter().min()`
(`operator/dynamic/trace.rs:148-215`) **[V]**. One consumer that needs unbounded history pins the
whole shared arrangement — sharing and LATENESS-driven GC are in tension.

---

# §5 — SQL compilation & optimizer

All §5 paths are under
`sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/` unless noted.
Calcite version is **1.43.0** (`sql-to-dbsp-compiler/SQL-compiler/pom.xml:19`) **[V]**.

## 5.0 Shape of the front end

Two distinct optimizers run back to back:

1. **A Calcite `RelNode` optimizer** — `compiler/frontend/calciteCompiler/optimizer/CalciteOptimizer.java`
   — a *linear list of independent Hep programs*, one per named "step".
2. **A DBSP circuit optimizer over their own IR** —
   `compiler/visitors/outer/CircuitOptimizer.java` — ~70 passes over a `DBSPCircuit` (the operator
   DAG for the *whole program*).

The second is where nearly all of the interesting work is. The Calcite layer does relational
normalization; the DBSP layer does incrementalization, field liveness, index sharing, fusion, CSE
and GC insertion.

## 5.1 Which Calcite rules are enabled, and what DBSP-specific rewrites sit on top

### Pure Hep, no Volcano

`CalciteOptimizer.HepOptimizerStep.optimize()` builds a fresh `HepPlanner(program)` per step and
calls `findBestExp()` (`CalciteOptimizer.java:96-107`) **[V]**. The `RelOptCluster`'s own planner is
a deliberately empty Hep planner — the source comment says "This planner does not do anything. We
use a series of planner stages later to perform the real optimizations."
(`frontend/calciteCompiler/SqlToRelCompiler.java:336-338`) **[V]**. There is **no
`VolcanoPlanner`** anywhere in the tree **[V]**. Steps are gated by `--optimizationLevel` (default
2, `compiler/CompilerOptions.java:51-52`), can be skipped by name regex
(`--skip_calcite_optimization`, `CalciteOptimizer.java:176`), and — worth noting — **a step that
throws is silently skipped with a warning** (`CalciteOptimizer.java:191-196`) **[V]**.

### The enabled rule set, in registration order (`CalciteOptimizer.createOptimizer()`, `:221-458`) **[V]**

| # | Step (min level) | Lines | Rules |
|---|---|---|---|
| 1 | Rewrite SESSION (0) | 232-233 | `SessionRewriteRule` (custom) |
| 2 | Constant fold (2) | 234-248 | `COERCE_INPUTS`; `SingleValuesOptimizationRules.{JOIN_LEFT,JOIN_RIGHT,JOIN_LEFT_PROJECT,JOIN_RIGHT_PROJECT}_INSTANCE`; `ReduceExpressionsRule.{FILTER,PROJECT,JOIN,WINDOW,CALC}_REDUCE_EXPRESSIONS` (Feldera fork); `ValuesReduceRule.{FILTER_VALUES_MERGE,PROJECT_FILTER_VALUES_MERGE,PROJECT_VALUES_MERGE}` (fork); `AGGREGATE_VALUES` |
| 3 | Remove empty relations (0) | 249-260 | `PruneEmptyRules.*` (10 instances) |
| 4 | Convert complex aggregates (0) | 261-271 | `MaxCaseToCountRule`, `AGGREGATE_CASE_TO_FILTER`, `AggregateNowFilterRule`, `ModeToArgMaxRule` |
| 5 | Simplify set operations (0) | 272-281 | `UNION_MERGE`, `INTERSECT_MERGE`, `MINUS_MERGE`, `SetopOptimizerRule×2`, `{INTERSECT,MINUS,UNION}_FILTER_TO_FILTER` |
| 6 | Useless sort removal (0) | 282-285 | `SORT_REMOVE`, `SORT_REMOVE_REDUNDANT`, `SORT_REMOVE_CONSTANT_KEYS` |
| 7 | Simplify correlates (0) | 286-288 | `PROJECT_CORRELATE_TRANSPOSE`, `FILTER_CORRELATE` |
| 8 | Merge identical operations (0) | 222-228, added 289 | `PROJECT_MERGE`, `MINUS_MERGE`, `UNION_MERGE`, `AGGREGATE_MERGE`, `INTERSECT_MERGE` |
| 9 | Join order (2) | 291-334 | BOTTOM_UP: `JOIN_PUSH_EXPRESSIONS`, `FilterJoinRule.JoinConditionPushRule` (fork), `FilterJoinRule.FilterIntoJoinRule` (fork), `EXPAND_FILTER_DISJUNCTION_GLOBAL`, `EXPAND_JOIN_DISJUNCTION_GLOBAL`, `JOIN_EXPAND_OR_TO_UNION_RULE`, `FILTER_MERGE`. Conditionally (0 outer joins and ≥3 joins, `:316`): `JOIN_TO_MULTI_JOIN`, `PROJECT_MULTI_JOIN_MERGE`, `MULTI_JOIN_OPTIMIZE_BUSHY`, `MULTI_JOIN_OPTIMIZE` |
| 10-15 | Decorrelation / set-op desugaring (2) | 336-352 | `UNNEST_DECORRELATE`, `UNNEST_PROJECT_DECORRELATE`, `InnerDecorrelator`, `CorrelateUnionSwap`, `InnerDecorrelator` again, `ExceptOptimizerRule`, `ExceptAllFoldRule` |
| 16 | Decorrelate (not Hep) | 353-368 | direct `RelDecorrelator.decorrelateQuery` |
| 17-19 | Windows / DISTINCT (0) | 370-383 | `PROJECT_OVER_SUM_TO_SUM0_RULE`, `PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW`, `RowsToRangeRule`, `AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN`, `AGGREGATE_EXPAND_DISTINCT_AGGREGATES` |
| 20 | Hypergraph (0) | 384-397 | if no correlate / no join hints / >1 join: `JOIN_TO_HYPER_GRAPH`, `HYPER_GRAPH_OPTIMIZE` |
| 21 | Join order again | 398 | same object as #9 |
| 22 | Move projections (0) | 402-411 | `PROJECT_WINDOW_TRANSPOSE`, `PROJECT_SET_OP_TRANSPOSE`, `FILTER_PROJECT_TRANSPOSE`, `FILTER_AGGREGATE_TRANSPOSE` |
| 23-24 | Join conditions / Pushdown (2) | 413-442 | `JOIN_PUSH_EXPRESSIONS`; then the FilterJoinRule forks + disjunction expansion, BOTTOM_UP |
| 25 | Merge identical ops again | 443 | same as #8 |
| 26 | Remove dead code (0) | 444-451 | `AGGREGATE_REMOVE`, `AntiJoinDistinctRemoveRule`, `UNION_REMOVE`, `PROJECT_REMOVE`, `PROJECT_JOIN_JOIN_REMOVE`, `PROJECT_JOIN_REMOVE` |

**Rules they deliberately turned off, with their reasons (verbatim comments)** **[V]**:

- `CoreRules.PROJECT_JOIN_TRANSPOSE` — **"Rule is unsound, replaced with UnusedFields done later."**
  (`CalciteOptimizer.java:409-410`). *This is the direct counterpart of DbspNet's join column
  pruning, and they moved it out of Calcite into their own IR — see §5.2.*
- `JOIN_CONDITION_PUSH` / `FILTER_INTO_JOIN` — replaced by their forked `FilterJoinRule`
  (`:299-300`, `:308-309`, `:433-434`).
- `JOIN_PUSH_TRANSITIVE_PREDICATES` — "I think this is broken" (`:305`).
- `PROJECT_CORRELATE_TRANSPOSE` in "Move projections" — breaks a regression test (`:403-404`).
- `FILTER_MULTI_JOIN_MERGE`, `MULTI_JOIN_{BOTH,LEFT,RIGHT}_PROJECT` (`:325-328`);
  `AGGREGATE_PROJECT_PULL_UP_CONSTANTS`, `AGGREGATE_UNION_AGGREGATE` (`:452-457`).

**Custom `RelOptRule`s they add** (all in `frontend/calciteCompiler/optimizer/`) **[V]**:
`FilterJoinRule` (fork that ignores non-determinism, `:49-53`), `ReduceExpressionsRule` (fork that
does not fold ROW, `:87-105`), `ValuesReduceRule` (fork, `:38-65`), `SessionRewriteRule` (SESSION
TVF lowering, `:25-66`), `RowsToRangeRule` (ROWS frames → RANGE over synthetic `ROW_NUMBER`,
`:39-94`), `MaxCaseToCountRule` (`MAX(CASE…1/0)` → `COUNT(…) FILTER`, `:31-63` — chosen over
`AGGREGATE_CASE_TO_FILTER` explicitly because "a COUNT-based rewrite is **linear**",
`CalciteOptimizer.java:262-264`), `ModeToArgMaxRule` (`:27-59`), `AggregateNowFilterRule` (`:24-63`),
`AntiJoinDistinctRemoveRule` (`:17-46`), `ExceptOptimizerRule` (`:24-45`), `ExceptAllFoldRule`
(`:12-29`), `SetopOptimizerRule` (`:29-51`), `CorrelateUnionSwap` (`:25-53`), `InnerDecorrelator`
(`:10-14`), plus `RexOptimize` (a `RexShuttle` re-implementing part of `RexSimplify` with correct
time semantics, `:16-20`).

### The DBSP IR pass pipeline (`visitors/outer/CircuitOptimizer.java:74-197`) **[V]**

~70 passes in a fixed order. Abbreviated, with the ones that matter to DbspNet flagged:

```
 79 UDFInliner ... 82 DecomposeExpensiveFilters   83 ImplementNow
 89 RecursiveComponents          90 DeadCode      93 PropagateConstants
 96 CreateStarJoins              99 OptimizeProjections(expand=true)
101 FuseExpensiveMaps                              <- fusion
103 UnusedFields                                   <- FIELD LIVENESS / column pruning
104 Intern                       105 CSE           <- operator CSE (whole program)
106 ExpandAggregates             111 OptimizeDistinctVisitor
113 OptimizeIncrementalVisitor
116 IncrementalizeVisitor        [if -i]           <- the D∘Q∘I lifting
118 OptimizeIncrementalVisitor   119 RemoveIAfterD
123 OptimizeProjectionVisitor    124 PullFilterVisitor   125 OptimizeProjections
127 ShareIndexes                                   <- ARRANGEMENT SHARING
131 CSE                          132 ShareWindowIntegrals
134 FilterJoinVisitor                              <- filter pushdown into joins
135 MonotoneAnalyzer                               <- LATENESS/GC insertion
141 LinearPostprocessRetainKeys  142 ExpandIndexedInputs
151 ExpandHop                    154 RemoveIdentityOperators
155 ShareInputIndexes                              <- ARRANGEMENT SHARING (2)
156 ChainVisitor                 157 OptimizeProjections(false)
164 CSE                          169 BalancedJoins        170 ChainVisitor
171 ImplementChains                                <- fusion lowered to FlatMap
175 ImplementJoins               177 PushDifferentialsUp
178 CSE                          179 InnerCSE             <- expression CSE
192 MerkleOuter×2                195 FindUnboundedState   196 CircuitStatistics
```

`OptimizeWithGraph` = rebuild graph + pass + `DeadCode`, repeated to fixpoint
(`visitors/outer/OptimizeWithGraph.java:9-30`); `Repeat` bounds iterations at
`max(circuit.size(), 10)` and errors if it does not converge (`Repeat.java:37-64`) **[V]**.

Incrementalization itself is the textbook `D ∘ Q ∘ I` lifting and is honest about it:
`IncrementalizeVisitor.java:31-34` — *"converts a `DBSPCircuit` into a new circuit which computes
the incremental version of the same query. The generated circuit is not efficient, though, it
should be further optimized."* The efficiency comes from `OptimizeIncrementalVisitor`
(*"optimizes incremental circuits by pushing integral operators forward"*,
`OptimizeIncrementalVisitor.java:40-41`) run both before and after, plus `RemoveIAfterD` and
`PushDifferentialsUp`. `NoIntegralVisitor` then asserts no `integrate` survives in incremental mode
(`NoIntegralVisitor.java:9-13`) **[V]**.

## 5.2 Arrangement/index sharing and cross-view CSE — the headline answer

**Yes to both, at two independent levels, and it is cross-view by construction because the whole
program is one circuit (§4.6).**

### Level 1 — compiler passes that *merge* index/arrangement construction

Three passes, and their strategy is the mirror image of DbspNet's:

- **`ShareIndexes`** (`visitors/outer/indexSharing/ShareIndexes.java:8-27`) **[V]** —
  when the same collection is indexed twice on the *same key* with *different values* (each feeding
  a join), the two `index` nodes collapse into **one index producing the union of the fields**, and
  each join's function is rewritten to read its own subset. Their own diagram:
  `source → index → {join, join}` replacing `source → {index → join, index → join}`.
- **`ShareWindowIntegrals`** (`visitors/outer/windowSharing/ShareWindowIntegrals.java:7-19`) **[V]**
  — same trick for window operators: if several windows use the same timestamp from the same
  source, force them to share one left `MapIndex` "by **widening** the operator to contain the union
  of all fields needed by all inputs, and by inserting corresponding projections after each of the
  widened windows."
- **`ShareInputIndexes`** (`visitors/outer/indexSharing/ShareInputIndexes.java:28-31`) **[V]** —
  a table with a primary key is *already* indexed; a redundant `MapIndex` on the same key feeding a
  join is deleted and the input used directly.

Note the direction: **they widen values to share one arrangement; DbspNet narrows values to shrink
many.** Both reduce work but they trade against each other (§ final section).

### Level 2 — the runtime memo (already covered in §4.6)

`TraceId`, `IndexId`, `IntegralId`, `DistinctId`, `SemijoinId`, `AntijoinId`, `ShardId`, … all
memoize on `StreamId` in the circuit build cache. So even where the compiler emits two `.integrate()`
or `.shard()` calls on the same stream, the library builds one node. Cross-view sharing falls out
for free because there is one `RootCircuit`.

### Cross-view CSE

`CSE` (`visitors/outer/CSE.java:21-42`) is "Common-subexpression elimination at the level of circuit
operators", implemented as `Repeat{Graph, FindCSE, RemoveCSE}` **[V]**. It runs **four times** in the
main pipeline (`CircuitOptimizer.java:105, 131, 164, 178`) plus once inside `UnusedFields`
(`UnusedFields.java:72`). It operates on the whole-program `DBSPCircuit`, so **it is inherently
cross-view** — there is no separate "intra-view" and "cross-view" CSE the way DbspNet has. One
carve-out: it refuses to CSE an operator that has a GC successor (`CSE.java:70-77`, `:121`
"Do not CSE something which is followed by a GC operator") **[V]** — i.e. sharing is suppressed
where it would break per-consumer retention, which is the same tension noted in §4.6.

Expression-level CSE is separate: `InnerCSE` (`visitors/outer/InnerCSE.java:16-30`) drives
`ValueNumbering` + `ExpressionsCSE`, turning repeated subexpressions into
`let t = LazyCell::new(|| …)` bindings (`ExpressionsCSE.java:38-51`) **[V]**.

### Column pruning — the pass that replaced the Calcite rule

`UnusedFields` (`visitors/unusedFields/UnusedFields.java:39`, "Find and remove unused fields")
**[V]** is a whole field-liveness framework, not a single rewrite:

- Inner fixpoint `RepeatRemove.OnePass` (`UnusedFields.java:48-73`):
  `RemoveUnusedFields → DeadCode → OptimizeProjections → DeadCode → FindCommonProjections →
  ReplaceCommonProjections → TrimFilters → TrimWindows → CSE`.
- Analysis core: `FindUsedFields.java:64-76` is an abstract interpretation producing a `FieldUseMap`
  (`FieldUseMap.java:22-24`); `RemoveUnusedFields.java:55-66` decomposes `f = g ∘ h` where `h` is
  the projection.
- `FindUnusedInputFields` (`:77-136`) warns per unused *table column*; `TrimInputs` (`:138-232`)
  actually narrows the source operator's row type — but only under `--trimInputs`, default **false**
  (`UnusedFields.java:245-247`; `CompilerOptions.java:212-213`) **[V]**. Materialized tables are
  exempt (`:131-133`).
- Calcite also trims at conversion time via `.withTrimUnusedFields(true)`
  (`SqlToRelCompiler.java:363`) **[V]**.
- There is additionally a per-table `skip_unused_columns` property that stops the *connector* from
  even parsing unused columns (`frontend/statements/CreateTableStatement.java:43,78`;
  `frontend/CalciteToDBSPCompiler.java:3354`, `:3375-3386`) **[V]**.

### Fusion

`ChainVisitor` "Combine chains of Map/MapIndex/Filter into Chain operators"
(`visitors/outer/ChainVisitor.java:16`), and a chain is lowered to a single
`DBSPFlatMapOperator` by `ImplementChains` (`circuit/operator/DBSPChainOperator.java:40-43`;
`visitors/outer/ImplementChains.java:13`) **[V]** — the same shape as DbspNet's `MapFilterRows`, but
with a cost guard: chaining stops at an "expensive" function (`DBSPChainOperator.java:264-265`).
`FuseExpensiveMaps` additionally fuses *sibling* maps that share expensive subexpressions into one
map plus per-consumer projections (`FuseExpensiveMaps.java:37-40`), and
`CloneOperatorsWithFanout.java:15-19` *duplicates* a cheap fan-out operator so it can then be fused
into each successor — a lever DbspNet does not have.

## 5.3 What gets materialised

**Only what the user declares.** Three orthogonal, entirely user-driven knobs **[V]**:

1. **View kind** — `SqlCreateView.ViewKind ∈ {LOCAL, STANDARD, MATERIALIZED}`
   (`frontend/parser/SqlCreateView.java:25-32`), with the semantics stated right in the enum:
   "For materialized views the DBSP program will keep the full contents" / "Local views are not
   program outputs" / "Standard views only produce deltas".
   - LOCAL gets a `DBSPViewOperator` and **no** `DBSPSinkOperator`
     (`frontend/CalciteToDBSPCompiler.java:3157-3172`); CTEs become LOCAL views
     (`frontend/calciteCompiler/CteToLocalViews.java:226`).
   - `RemoveViewOperators` erases the marker nodes — first with `all=false`, preserving recursive
     views, views with LATENESS and the error view (`RemoveViewOperators.java:11-30`), later with
     `all=true` (`CircuitOptimizer.java:102`, `:176`).
2. **Codegen consequence** — `ToRustVisitor.preorder(DBSPSinkOperator)` selects the catalog call
   (`backend/rust/ToRustVisitor.java:1266-1270`):
   `MATERIALIZED → register_materialized_output_zset_persistent`,
   `STANDARD → register_output_zset_persistent`, `LOCAL → internal error`. A `CREATE INDEX` on a
   view forces the materialized form (`ToRustVisitor.java:1207-1213`, `:1258`).
   On the runtime side this is exactly the integral/no-integral split:
   `register_output_zset_persistent_inner` leaves `integrate_handle: None`
   (`crates/adapters/src/static_compile/catalog.rs:530`), whereas the materialized variant calls
   `stream.accumulate_integrate_trace()` and sets `integrate_handle: Some(..)`
   (`catalog.rs:640`, `:656`; sharded variants at `:124`, `:137`) **[V]**.
3. **Input tables** — `materialized` table property (`CreateTableStatement.java:41,62-64`), and any
   table with a primary key is registered as a materialized map anyway *unless* a PK column has
   LATENESS (`ToRustVisitor.java:903-908`) **[V]**.

**So: intermediate views get no integrated state.** Only `MATERIALIZED` views, indexed views, and
(mostly) keyed input tables are integrated. Materialization also *disables* optimizations —
`UnusedFields` will not trim a materialized table's columns (`UnusedFields.java:131-133`) and
`InsertLimiters` will not GC one (`monotonicity/InsertLimiters.java:1745-1746`) **[V]**.

This is a direct contrast with DbspNet, where every declared output view carries an `IntegrateOp`
and (per the brief) materialised-output integrates are 29.4% of SF=3 state.

## 5.4 LATENESS / watermarks / retention: declared or inferred?

**Declared at the boundary, inferred everywhere inside.** The analysis is real dataflow analysis,
but its *seeds* are exclusively user annotations.

**Declaration sites** **[V]**: column attributes `LATENESS <expr>` and `WATERMARK <expr>` on
`CREATE TABLE` (`src/main/codegen/includes/ddl.ftl:93-99`), and a standalone
`LATENESS <view>.<column> <expr>` statement for views (`ddl.ftl:110-125`;
`frontend/parser/SqlLateness.java:22-23`). Also `append_only` on tables
(`CreateTableStatement.java:42`) and `emit_final` on views (`CreateViewStatement.java:43,82-84`).
There is **no `RETENTION` DDL clause**.

**The inference pass** is `MonotoneAnalyzer`
(`visitors/outer/monotonicity/MonotoneAnalyzer.java:31-33`): *"Implements a dataflow analysis for
detecting values that change monotonically, and inserts nodes that prune the internal circuit state
where possible."* It runs once, at `CircuitOptimizer.java:135`, and sequences
(`MonotoneAnalyzer.java:90-174`) **[V]**:
`EnsuresTree` normalization → `SeparateIntegrators` → `AppendOnly` → `KeyPropagation` →
`DeltaExpandOperators` (builds a *separate expanded circuit* used only for analysis) →
`Monotonicity` (the fixpoint) → `InsertLimiters` (applied to the **original** circuit, `:159-164`)
→ `MergeGC` → `CheckRetain`.

`Monotonicity` (`monotonicity/Monotonicity.java`) has ~30 per-operator transfer functions and a
lattice of `IMaybeMonotoneType` / `MonotoneType` / `NonMonotoneType` / `PartiallyMonotoneTuple` etc.
in `visitors/monotone/` **[V]**. Its **seeds**:
- source columns are `NonMonotoneType` unless `metadata.lateness != null`
  (`Monotonicity.java:247-257`);
- a view with declared LATENESS resets the analysis — "Trust the annotations, and forget what we
  know about the input" (`Monotonicity.java:266-285`), with a TODO citing feldera#1906 about
  blending declared and inferred lateness (`:269-271`);
- the only non-declaration seeds are `HOP` (synthesises a monotone `hop_start_timestamp`,
  `Monotonicity.java:220-245`) and the `now` stream (`temporal/RewriteNow.java:538`).

Their own package doc is explicit: *"Discover monotone expressions **starting from LATENESS
annotations** and optimize the circuit by inserting garbage-collection operators."*
(`visitors/monotone/package-info.java:1-4`) **[V]**. So: no declaration ⇒ nothing monotone ⇒ no GC.

**What gets emitted** (`monotonicity/InsertLimiters.java:80-99`, their own list) **[V]**: bound-
computing `apply` operators, `DBSPControlledKeyFilterOperator` (drops late tuples, routing a
`LATE_ERROR` to the error view, `:1689-1691`, `:1800-1833`), `DBSPWaterlineOperator` near sources
(`:1675-1682`), `DBSPIntegrateTraceRetainKeysOperator` / `…RetainValuesOperator` /
`…RetainNValuesOperator` to prune integrals, `DBSPPartitionedRollingAggregateWithWaterlineOperator`,
and `DBSPWindowOperator` before `emit_final` views. For a keyed table the waterline + delay +
controlled filter collapse into one `DBSPInputMapWithWaterlineOperator` with three outputs
(`:1695-1715`).

Two correctness details worth stealing **[V]**:
- key-GC is only legal for LATENESS columns **inside the primary key** — long comment at
  `InsertLimiters.java:1747-1759`;
- `CheckRetain.java:14-15`: *"The DBSP runtime will incorrectly GC a relation that has multiple
  Retain operators of the same kind. Check that this doesn't happen."* — a compiler-side guard
  against a runtime footgun, plus `StrayGC.java:23` requiring every GC operator to have an obvious
  target, and `MergeGC.java:34-44` de-duplicating equivalent retain chains by taking the min of
  control inputs (the same "min over consumers" rule as the runtime `TraceBounds`, §4.6).

There is also a diagnostic pass `FindUnboundedState` (`visitors/outer/FindUnboundedState.java:46-58`)
that reports operators whose state may grow without bound — and its own comment says **"Currently no
one consumes the output produced by this pass"** (viewable with `-TFindUnboundedState=1`) **[V]**.

Generated-Rust names: `integrate_trace_retain_keys` / `accumulate_integrate_trace_retain_keys`
(`circuit/operator/DBSPIntegrateTraceRetainKeysOperator.java:37`),
`accumulate_integrate_trace_retain_values` (`…RetainValuesOperator.java:33`),
`accumulate_integrate_trace_retain_values_<which>` (`…RetainNValuesOperator.java:53`, used to GC the
right input of ASOF JOIN, Min/Max/ArgMin/ArgMax/TopK), `waterline`, `input_map_with_waterline`,
`partitioned_rolling_aggregate_with_waterline`. Emission at `backend/rust/ToRustVisitor.java:1085-1116`
**[V]**.

## 5.5 Cost model?

**There is none.** No `RelOptCostFactory`, no `RelOptCost`, no `computeSelfCost`, no `VolcanoPlanner`,
and no cardinality propagation over the DBSP IR anywhere in `src/main/java` **[V]**.

The one place a cardinality can exist at all: `CalciteTableDescription.getStatistic().getRowCount()`
returns the value of a **user-declared** `expected_size` table property, or `null`
(`frontend/statements/CalciteTableDescription.java:34-52`; constant at
`CreateTableStatement.java:45`) **[V]**, and `RelMdRowCount.SOURCE` is chained into the cluster's
metadata provider (`SqlToRelCompiler.java:355-357`) **[V]**. **[I]** the only way this can affect a
plan is by feeding Calcite's own `MULTI_JOIN_OPTIMIZE_BUSHY` / `HYPER_GRAPH_OPTIMIZE` inside the
Calcite jar; no Feldera code ever reads a row count. Without `expected_size`, Calcite falls back to
its own defaults.

Everything else is **syntactic heuristics** **[V]**:

| Heuristic | Cite |
|---|---|
| `Expensive`: any external function call is expensive; also any expression with `> SIZE_THRESHOLD = 100` node terms | `visitors/inner/Expensive.java:16-33` |
| Bushy join reordering only when 0 outer joins and ≥3 joins | `CalciteOptimizer.java:313-317` |
| Hypergraph only when no correlate, no join hints, >1 join | `CalciteOptimizer.java:387-389` |
| Adaptive-vs-hash join chosen from **user hints**, and "Operators with GC cannot be adaptive" | `visitors/outer/BalancedJoins.java:22-23,31,43` |
| `MAX(CASE…)`→`COUNT FILTER` preferred because the rewrite is *linear* (an incremental-complexity argument, not a cardinality one) | `CalciteOptimizer.java:262-264` |
| Clone a fan-out operator only if its function is "not very expensive" | `CloneOperatorsWithFanout.java:15-19` |
| Stop fusing a chain at an expensive function | `DBSPChainOperator.java:264-265` |
| Pull a filter above a map only "if it doesn't become too complex" | `PullFilterVisitor.java:34` |

`CircuitStatistics` (`visitors/outer/CircuitStatistics.java:27-39`) counts operators/joins/aggregates
purely for a debug log; nothing consumes it **[V]**.

The runtime **balancer** (§4.3) is therefore the *only* cost-based optimizer in the whole system —
and it is a *runtime* one, driven by measured key distributions and solved with MaxSAT, not a
compile-time one.

---

# Where this contradicts or challenges a DbspNet decision

Ordered by how much I think each should move a standing decision.

## 1. §4 "coordination is our strength / barrier ceiling" — the framing may be measuring the wrong thing

**DbspNet's decision (brief §4):** the parallel-scaling arc *concluded*; the ceiling is barrier
coordination at fine ticks; coalescing barriers and W-sizing were both falsified; residual gaps are
per-row, so the arc closed and work returned to axis 2.

**Feldera evidence:** they pay *more* all-worker rendezvous per step than DbspNet, not fewer —
a driver command broadcast (`dbsp_handle.rs:1510-1560`), a serde_json metadata broadcast every step
(`dynamic_scheduler.rs:504-517`, `runtime.rs:1468-1500`), and a commit consensus
(`dynamic_scheduler.rs:555-579`) — and their own `shard` docstring concedes the barrier "limits the
scalability" (`shard.rs:63-69`). What they do instead is make each barrier cover far more work
(`DEFAULT_MAX_WORKER_BATCH_SIZE = 10_000` *per worker*, `config.rs:56`) and overlap the wait with
other operators (per-worker cooperative async scheduling; split sender/receiver,
`exchange.rs:1616-1618`).

**Implication:** "coalescing barriers" was the wrong lever to falsify the hypothesis with.
The two levers Feldera actually pulls are *tick size* and *wait overlap*, and DbspNet has neither
(a synchronous `ExchangeCoordinator` barrier stalls the whole worker). Before treating the arc as
closed, it is worth measuring DbspNet's Nexmark q4 barrier WAIT at Feldera's tick size, and
comparing against Feldera's own `circuit_wait_time_seconds / circuit_runtime_seconds`
(`circuit/metadata.rs:167-176`) at the same W. If Feldera's wait fraction on q4 is also ~40%, the
DbspNet conclusion is confirmed and the arc genuinely closes. If it is much lower, the lever is
"let a blocked operator yield to a ready one", which is a scheduler change, not a barrier change.

## 2. §4/§1 — Feldera's ivm-bench bulk load may not be measuring the same computation at all

**DbspNet's position (brief §0/§1):** the SF=3 bulk load gap (originally 7×, now ~3.5×) is
allocation-bound per-row work.

**Feldera evidence:** with `transaction_mode: always`, operators **accumulate input into a spine
across many steps and only compute at commit** (`accumulator.rs:294-330`); during the transaction
the pipeline does "only minimal processing such as resolving primary keys and indexing inputs"
(`transactions.md:84`). Their docs state the motive directly: computing all intermediate updates
"can be more expensive than computing the cumulative update … can be very significant in some
cases" (`transactions.md:25-28`). There is also an `EnableCount` that makes an accumulator *discard*
input when nothing downstream listens (`accumulator.rs:39-46`, `:306-315`).

**Implication:** part of the residual 3.5× may be *algorithmic*, not per-row: Feldera runs the
50-view DAG once over the whole accumulated batch; DbspNet runs it once per chunk. This is checkable
cheaply from the DbspNet side (how many circuit passes does batch 1 make?) and, if true, it is a
larger and cheaper win than any row-representation change. It also reframes the "columnar / typed
path" arcs that were repeatedly demoted.

## 3. §5 join column pruning — Feldera solved the *same* problem in the *opposite* direction, and named ours unsound

**DbspNet's decision (brief §2/§5):** join column pruning (projection pushdown through join) is
default-on and was "the single biggest lever we found" (q4 −50% at W=1, 2.93–4.19× at W=8).

**Feldera evidence:** they explicitly **disabled** Calcite's `PROJECT_JOIN_TRANSPOSE` with the
comment *"Rule is unsound, replaced with UnusedFields done later"* (`CalciteOptimizer.java:409-410`)
and replaced it with a whole-circuit **field-liveness fixpoint** (`visitors/unusedFields/`,
`UnusedFields.java:39-73`) that prunes fields anywhere in the DAG, not just across joins, and can
optionally stop the connector from ingesting the columns at all (`--trimInputs`;
`skip_unused_columns`). Simultaneously, `ShareIndexes` / `ShareWindowIntegrals`
(`indexSharing/ShareIndexes.java:8-27`, `windowSharing/ShareWindowIntegrals.java:7-19`) deliberately
**widen** index values to let several joins share one arrangement.

**Implication, two parts:**
(a) DbspNet's `PruneJoinInputs` is claimed "unconditionally sound" and is backed by full-±1 PBT — I
found no evidence in the Feldera tree of *why* they call the Calcite rule unsound, only that they
do. Worth a look at whether their objection applies to DbspNet's formulation (my guess **[I]**: it
concerns outer joins / null-generating sides, which DbspNet's PBT would cover).
(b) The generalisation is the real prize: DbspNet's pruning is join-local, Feldera's is a global
field-liveness analysis that also reaches filters (`TrimFilters`), windows (`TrimWindows`), common
projections (`FindCommonProjections`/`ReplaceCommonProjections`) and the input adapter. Given that
narrowing wins were the only ones that landed for DbspNet, generalising pruning to whole-DAG field
liveness looks like the highest-value §5 item available.
(c) And there is a genuine tension to design around: pruning and arrangement sharing pull opposite
ways. Feldera resolves it by pruning first (pass 103) and *widening to share* later (passes 127/132).

## 4. §5 materialization — DbspNet integrates every output view; Feldera integrates almost nothing

**DbspNet's position (brief §1):** `IntegrateOp` (materialised output views) is one of the operators
with no spine sibling, and those operators are 29.4% of SF=3 state — a stated reason spine cannot
bound memory.

**Feldera evidence:** a `STANDARD` view produces **deltas only** and gets no integral
(`ToRustVisitor.java:1266-1270`; `catalog.rs:530` `integrate_handle: None`). Only
`CREATE MATERIALIZED VIEW`, an indexed view, or a keyed input table gets
`accumulate_integrate_trace()` (`catalog.rs:640`, `:656`). `LOCAL` views (and CTEs) get no sink at
all (`CalciteToDBSPCompiler.java:3157-3172`).

**Implication:** a large slice of DbspNet's "unspillable 29.4%" may be state Feldera simply never
materialises. Adding a view-kind distinction (delta-only vs materialised vs local) would shrink the
state that the trace-family decision has to cover — which is relevant to `docs/decision-trace-family.md`
even though that decision (stop growing spine) itself looks unaffected.

## 5. §4.3 — partitioning as a plan property: DbspNet has elision; Feldera also has *runtime* repartitioning

**DbspNet's position (brief §4):** exchange elision landed; the arc concluded; W-sizing falsified.

**Feldera evidence:** shardedness is a memoized stream property propagated through 81 call sites
(`shard.rs:616-688`), *and* on top of that there is a runtime balancer that re-solves partitioning
policies (`Shard`/`Broadcast`/`Balance`) per join-graph SCC with MaxSAT over measured key
distributions, at transaction boundaries, with hysteresis thresholds
(`balance/balancer.rs:31-34`, `:68-115`, `:190-198`, `:228-268`, `:492-510`).

**Implication:** DbspNet has never tested the *skew* hypothesis, only the *barrier-count* one.
Feldera's investment says the binding constraint at high W is a straggler worker, and a
`Broadcast` policy for a small join input (which removes the shuffle for that input entirely) is a
cheap, well-bounded experiment DbspNet could run without adopting the MaxSAT machinery.

## 6. §5 cost model — this one *confirms* a DbspNet decision

DbspNet is purely rule-driven. So is Feldera: no Volcano, no cost factory, no cardinality
estimation, only a user-declared `expected_size` that can leak into Calcite's own join enumeration
(`CalciteTableDescription.java:34-52`, `SqlToRelCompiler.java:355-357`). Their only cost-based
component is the *runtime* balancer. **No reason to build a compile-time cost model.**

## 7. §4.1 — background merge threads (bears on the trace-family decision)

Not a §4 question, but it fell out of the thread model: Feldera's LSM merges run on a **separate
shared multi-threaded Tokio runtime** (`spine_async.rs:719-720`, `:1300-1313`;
`crates/storage/src/tokio.rs:9-26`), so compaction is off the step's critical path, and a
memory-pressure monitor thread pushes batches to disk under pressure (`runtime.rs:740-802`).
DbspNet's flat traces compact in-step (bulk-on-threshold). The measured "+14% step for spine vs
flat on the bulk batch" in `docs/decision-trace-family.md` was therefore measured against an
in-step merge schedule; Feldera's numbers are not comparable to it. This does not obviously reverse
the decision — but it does mean the spine-vs-flat step-cost comparison is not the same experiment
Feldera would run.

