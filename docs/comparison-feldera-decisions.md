# Comparison: where DbspNet and Feldera decided differently

**Status: RESEARCH SYNTHESIS, 2026-08-30.** Source-read against the Feldera tree at `d:\src\feldera`,
git `78afc9077` (2026-08-29). No Feldera build was run; every claim below is a source citation, and
the four underlying reports are preserved in `docs/research-feldera/` (`00-brief.md` is the briefing
that framed the questions; `trace.md`, `rowrep.md`, `persistence.md`, `parallel-optimizer.md` carry
the `path:line` evidence).

**Confidence discipline.** Nothing here is a DbspNet measurement. It is what Feldera's source says.
Where a finding *challenges* one of our decisions, §7 names the decision and the measurement that
would settle it — this document does not reverse anything on its own.

## 0. The one-sentence result

Feldera made a single structural choice we did not — **the only trace is a sorted, immutable,
file-backed LSM, and merging happens off the step thread** — and most of the differences below,
including the per-row gap we have chased through three arcs, fall out of it.

## 1. The convergent finding: no hash map on the row path

Two agents reached this from opposite ends (`trace.md` §1, `rowrep.md` §1-4).

`Spine<B>` is the only production `impl Trace` in Feldera (`crates/dbsp/src/trace/spine_async.rs:2037`).
There is no dictionary- or hash-backed trace anywhere in their tree. Batches are trie-layered
(`Layer<K, Layer<V, Leaf<T,R>>>`, `trace/ord/vec/val_batch.rs:31`), and the batch types operators
actually name (`OrdValBatch`, `OrdIndexedZSet`) are aliases for a `Fallback*` hybrid
(`trace/ord.rs:5-24`) — a per-batch `enum { Vec, File }`, so one spine holds a mix of RAM and disk
batches.

Consequences, all verified:

- **Joins** are merge/gallop with exponential seek (`join.rs:1095-1099`, `advance_retreat.rs:29-47`).
- **Group-by** walks contiguous runs off a sorted cursor with three reused scratch boxes
  (`aggregate.rs:582-615`).
- **Hashing** exists only to shard across workers: `crates/dbsp/src/hash.rs` is 15 lines, xxh3, applied
  to *the key only, once per distinct key, at an exchange* (`shard.rs:454`).
- **Builders** are sized exactly from `input.len()` / `key_count()` — order-preserving operations on
  sorted input have an exact output bound, so there is nothing to estimate.

DbspNet's measured per-tuple floor is ~50-60% fresh-dictionary allocation plus ~40-48% whole-row
hashing (`design-row-representation.md`, `reprbench`). **Neither is a cost of doing DBSP. Both are
costs of a hash-indexed Z-set.** Feldera pays neither because it never builds one, and our adaptive
delta-builder pre-sizing (§16.8, -16-35% alloc) is us estimating a number their structure knows
exactly.

## 2. Codegen is not the answer — confirmed from the other side

We demoted "generate code for expressions" three times on our own apportionment. Feldera's source
now says the same thing from their side: they **deliberately erased their runtime**.
`crates/dbsp/src/dynamic.rs:11-17` states that static monomorphisation "led to very long compilation
times" and that dynamic dispatch "speeds up compilation without sacrificing much performance."

Two mechanisms keep that cheap, and the second is the transferable idea:

1. `downcast` is a raw pointer cast with the TypeId check compiled out (`downcast.rs:56-66`).
2. **Every hot operation is range-shaped.** `extend_from_range`, `sort_slice`, `advance_to`,
   `consolidate_paired_slices` each pay one virtual call and then run a fully static loop over an
   entire run. One vcall per *run*, not per tuple.

Row types themselves are positional `TupN<T1..TN>`, nullables as `Option<T>`, `VARCHAR` as `ArcStr`
(clone = atomic increment), `DECIMAL(p,s)` as `Fixed<P,S>` over one `i128` with const-generic scale,
`TIMESTAMP` as `{i64}`. No SIMD, no arena allocator, no in-memory batch pooling.

## 3. Compaction runs off the step thread

`Spine::exert` — the differential-dataflow "spend fuel per step" hook — is an empty body
(`spine_async.rs:2108`). Merging runs on a dedicated Tokio runtime with **W merger threads alongside
W circuit workers** by default (`dbsp_handle.rs:54,637-643`; `runtime.rs:708-736`), one task per spine
per level. Fuel survives only as a yield quantum inside that background task. Steps are protected by
async backpressure at 128 loose batches (`spine_async.rs:653,2128`), not by bounding in-step merge
work.

Small batches are handled structurally rather than by a knob: level 0 is sized (≤14,999 records)
above a typical operator output chunk (10,000) and refuses to merge fewer than 8 batches; levels are
base-10 log of record count; k-way merges of 8-128 batches with a retraction-count bias that promotes
cancelling batches.

Our lazy merge view (`LazyMergeMultiset`, a 4.6-19× win on aggregate-heavy shapes) is their **only**
mode: nothing ever materialises trace+delta, reads go through `SpineCursor` / `CursorPair`, snapshots
are `Arc` clones, `fork()` copies no data.

## 4. Nothing escapes the trace abstraction

Every operator we listed in `decision-trace-family.md` §3 as having no spine sibling has a
`Spine`-backed equivalent in Feldera:

| DbspNet "flat-only" operator | % of SF=3 state | Feldera equivalent |
|---|--:|---|
| `IntegrateOp` | 11.2% | integral over a spine (`operator/dynamic/trace.rs:516`) — **and see below** |
| `PartitionedWindowAggregateOp` | 10.4% | radix tree *encoded as an indexed Z-set* (`time_series/radix_tree.rs:63-66`) |
| `PartitionedOffsetOp` | 7.7% | `GroupTransform` (`group.rs:341-345`) |
| `PartitionedRankOp` | ~0% | `GroupTransform` (same) |

The radix-tree trick is the one worth stealing conceptually: they did not write a spillable window
aggregate: they expressed the window aggregate's state *as* an indexed Z-set, so it inherits spill,
checkpoint and GC for free. That is how you avoid solving the problem eight times.

Only `keys_of_interest` (nested circuits) and Bloom filters (~19 bits/key, by design) stay pinned.

**And a chunk of our 11.2% may not need to exist at all:** `STANDARD` views get no integral in
Feldera — only `MATERIALIZED`/indexed views and keyed tables do (`ToRustVisitor.java:1266-1270`;
`catalog.rs:530` vs `:640`). We may be materialising views they simply don't.

Independently verified by a separate Calcite-rules census: there is **no backend divergence** on this.
The multi-crate path delegates per-operator emission, including sink `register_*` selection, to the
same `ToRustVisitor` (`backend/rust/multi/SingleOperatorWriter.java:117-121`,
`NestedOperatorWriter.java:164`, `CircuitWriter.java:174`), and `discoverIndexes`
(`ToRustVisitor.java:2173-2190`) is the identical grouping used on the single-crate path. So
`MATERIALIZED` / `STANDARD` / `LOCAL` → `register_materialized_output_zset_persistent` /
`register_output_zset_persistent` / *error* is a single point of truth for both backends. This claim
is safe to build on.

### 4.1 LATENESS × materialization: the filtered stream is what gets stored

`CircuitPostfix` is a backend-side analysis (not part of `CircuitOptimizer`), applied by both writers
(`backend/rust/multi/MultiCratesWriter.java:149-152`, `RustFileWriter.java:138-141`). Its own doc
(`visitors/outer/CircuitPostfix.java:24-30`): *"Discover input nodes that immediately feed the left
input of a `controlled_key_filter` operator. This pattern is generated by inputs with LATENESS
annotations. If the input table is materialized, the materialized stream has to be the output of the
`controlled_key_filter` operator."* It records the source→filter bijection **only when
`source.metadata.materialized`** (`:50-57`), consumed at `CircuitWriter.java:116-117`.

So a materialized input table carrying LATENESS registers its *filtered* stream as the queryable
state, and late records never enter the stored integral. Worth checking against our own LATENESS
implementation (bounded-history trace GC): we should confirm what our materialised inputs store when
a lateness bound is declared, because "GC the trace" and "never admit the record to the integral in
the first place" are different guarantees.

### 4.2 CHECKED (2026-08-31): we do *not* integrate non-output views — but the two program paths are inconsistent in opposite directions

§4's "we may be materialising views they simply don't" was checked against the compiler. **In its
literal form it is wrong**, and the item should not be built as stated:

- `CompileProgram` inserts the integral **only** `if (v.IsOutput)` (`PlanToCircuit.cs:470-476`).
  A non-output view gets a delta stream and nothing else — the same gate Feldera applies with
  `MATERIALIZED` vs `STANDARD`.
- Views not reachable from any output are pruned outright before compilation
  (`PlanToCircuit.cs:352-360`, `:545`), and `ColumnLivenessProbe` confirmed the live set: 5 whole dead
  views already pruned, all 16 outputs live.
- For ivm-bench specifically the integral is **required, not waste**: the benchmark measures full view
  *state* and diffs view contents across engines, so both sides must materialise those 16.

So there is no pile of needless integrals to delete. Two real findings came out of the check instead,
and both matter for **pause/resume**, which is a different goal from ivm-bench's per-batch durability.

**(1) The serial path has no delta-only output.** `IsOutput` ⇒ integral *structurally*: `ProgramOutput`
is defined as an `IntegratedViewHandle` (`CompiledProgram.cs:58`), so a program cannot declare "emit
this view's deltas to a sink, retain nothing". That is exactly Feldera's `STANDARD` shape. For a
streaming deployment whose consumer takes deltas, the integral is checkpoint bytes written and restore
time paid for state nobody reads — on the §4 table's own numbers, `IntegrateOp` is **11.2% of SF=3
state**. The machinery already exists: the parallel path emits `builder.Output(stream, "view:" + name)`
(`PlanToCircuit.cs:657-661`). This is a missing *option*, not missing capability.

**(2) The parallel path has the opposite defect — its view is outside the snapshot tree.**
`ParallelProgramOutput` holds the sharded delta handle plus a **plain driver-side `ZSet` field**
(`ParallelStructuralCompiledQuery.cs:129-130`), accumulated by the driver rather than by an in-circuit
`IntegrateOp`. It is therefore not an `ISnapshotable` and **not covered by `Snapshot.WriteAsync`**.
`design-structural-parallel.md` §10.1 records this as the "driver-side view gap" and calls it moot
until a parallel `ProgramRunner` exists — **for pause/resume it is not moot, it is the blocker**:
resuming a parallel program would silently come back with empty views, the same *class* of failure as
`design-incremental-persistence.md` §7.2 (restore silently producing wrong state), which is the bug
this codebase has already been bitten by once.

**Net:** serial always integrates and cannot opt out; parallel integrates where it cannot be persisted.
Neither is Feldera's arrangement, and the gap for a pause/resume stream is a *policy* knob plus closing
the parallel gap — not deleting integrals.

**Not measured.** This is a source-level check only; no SF=3 data exists on the current machine, so the
11.2% is quoted from the earlier i9 measurement and nothing here was re-timed.

## 5. Persistence: they built Track B *and* Track A

Our `design-incremental-persistence.md` framed these as alternatives and chose A. Feldera has both,
with B underneath.

- `Batch::persisted()` writes an in-memory batch to disk and returns it; an already-file-backed batch
  returns `None` and is untouched (`trace/ord/fallback/wset.rs:356`).
- `Spine::save` (`spine_async.rs:2199`) persists only the RAM residue, writes a **path list**, and
  hands the newly-persisted batches back into the live merger so the next checkpoint does not rewrite
  them. Their comment: *"we don't have to persist them again for the next checkpoint… we do have to
  read them back from disk to use them: no free lunch."*
- The `Checkpoint` trait exists explicitly for *"state within circuit operators that's not stored
  within a batch (which are already stored in files)"* (`checkpointer.rs:613`).
- Batch files live at the storage root, shared across checkpoints; checkpoint directories hold
  manifests and tiny per-operator files.
- **GC is two-level**: `Arc` refcount with delete-on-drop for un-checkpointed files
  (`posixio_impl.rs:205`), manifest-driven mark-and-sweep for checkpointed ones (`checkpointer.rs:522`,
  `gc_startup:106`), with an explicit "keep the newest retained checkpoint's batches, its merger may
  still depend on them" rule.
- **Recovery is lazy**: restoring a batch reads a 512-byte trailer and a Bloom block; data pages come
  through the buffer cache. O(#files), not O(state) — against our ~35 s full rebuild.

### 5.1 Operator identity: they shipped what we deferred

SQL pipelines run `Mode::Persistent` (hard-coded, `controller.rs:6067`), where an operator's persistent
id is a **SHA-256 Merkle hash of its whole upstream subtree** (`MerkleOuter.java`). The plan
fingerprint check is *deliberately disabled* in that mode: checkpoints are portable across program
edits, and that portability is the headline "bootstrapping" feature. A missing state file means "new
operator, backfill from upstream."

Our stated reason for deferring stable ids was that a colliding stable id fails silently where
positions fail loudly (`design-durable-identity.md` §1). Their answer is content-addressing *plus* a
recorded `state_files` manifest that keeps genuine loss loud (`verify_checkpoint_intact`). **The
manifest half is borrowable on its own**, independent of Merkle ids, and it directly addresses the
objection that blocked us.

### 5.2 Float aggregates: designed out at the compiler

The linear/running-accumulator lowering is refused when the result type is FP — `linearAllowed &&
!this.fp()` guards SUM, SUM0, AVG, STDDEV/VAR, COVAR/REGR (`AggregateCompiler.java:685,734,779,908,1076`).
Floats re-fold the whole group from the trace. They pay throughput; we pay a persisted-accumulator
blob (§7.2/§8). Both are correct, but theirs cannot regress into silent corruption the way ours did.

## 6. The benchmark premise was wrong — and it cost us

This is the most immediately actionable finding, and it corrects **our own** code and design doc.

`design-incremental-persistence.md` §0 says ivm-bench "measures Feldera with persistence inside the
batch window (`transaction_mode: always`)", and concludes the comparison is only apples-to-apples if
our batch also ends durable. That is a misreading, established in three steps:

1. `transaction_mode: always` is a per-source **Delta connector** option. Verified on our side: it
   appears identically in the `dbspnet` and `feldera` dbt projects, and **no `fault_tolerance` key
   exists anywhere in ivm-bench** — so `checkpoint_interval()` is `None`.
2. A checkpoint cannot even start mid-transaction: `RunningCheckpoint::start` → `can_suspend()` →
   `TransactionInProgress` (`controller.rs:9480`, `8810`). Feldera carries a FIXME about exactly this:
   *"checkpoints can get delayed indefinitely if the user runs end-to-end transactions"*
   (`controller.rs:4327`).
3. `Batch::persisted()` has exactly one call-site tree-wide — `Spine::save` (`spine_async.rs:2212`) —
   and `Trace::save` has two, both inside `Operator::checkpoint`. `flush` never touches the backend.

**Recoverable Feldera state at the end of an ivm-bench batch: zero.** Bytes do reach disk (merge spill
≥10 MiB, plus eager spill under pressure), but those files carry `DeleteOnDrop{keep:false}`, and the
only thing that flips `keep` is `Batch::file_reader()`, whose sole caller is `Spine::save`. No
checkpoint ⇒ nothing marked ⇒ every spilled file is a temporary, unlinked on drop or swept at startup.

The precise statement of the difference: **Feldera's batch-window I/O is spill I/O with no recovery
value; DbspNet's is snapshot I/O that buys a recovery point.** Same ledger line, different goods.

So turning our per-batch checkpoint on "for honesty" made the comparison *less* fair, not more, and
charged us ~18.7 s/batch that the other side was never paying.

### 6.1 What commit actually is — our client comment is wrong

`ivm-bench/.../feldera_client.py:185-189` attributes the ~47k-operator commit walk to persisting state.
It is the **computation** phase. The scheduler doc is explicit (`circuit/schedule.rs:219-226`): during
the in-progress phase operators may merely *accumulate*; during committing the scheduler "forces
operators to process their inputs to completion by invoking `flush`", advancing a frontier through the
DAG. `start_commit_transaction` sets `Committing(self.tasks.len())` — node count
(`dynamic_scheduler.rs:467`) — and the controller drives it as a plain `circuit.step()` loop until
`CommitComplete` (`dbsp_handle.rs:1706`). Under `transaction_mode: always`, ingestion does "only
minimal processing such as resolving primary keys and indexing inputs" (`transactions.md:88`).

`commit_progress` counts operators — `{completed, in_progress, remaining}` as `NodeId` set sizes by
`FlushState`, summed across workers (`schedule.rs:152-175`), so ~47k ≈ 4k nodes × 12 workers.

The SF=100 "80 minutes of `CommitInProgress`" is therefore **the 50-view DAG being evaluated**. It maps
to DbspNet's *step*, not to DbspNet's checkpoint. Suggested comment fix is in
`docs/research-feldera/persistence.md` §A5.

### 6.2 A new axis this exposed: accumulate-then-evaluate

`Accumulator<B>` holds a private `state: Spine<B>`; at flush it allocates an empty spine and
`mem::swap`s (`accumulator.rs:317-325`) — ownership transfer, no serialization. The point is
algorithmic: Feldera evaluates **one coalesced delta** where DbspNet evaluates a *sequence* of them,
explicitly "avoiding computing mutually canceling changes" (`accumulator.rs:26-30`), and their docs
note "computing all intermediate updates can be more expensive than computing the cumulative update".

DbspNet has no equivalent. **Part of the residual 3.5× on ivm-bench batch 1 may be algorithmic rather
than per-row** — we are computing intermediate states they skip entirely. This is a separate axis from
everything above and was not on our roadmap.

### 6.3 `+stored: true` — a whole view chain Feldera computes and we skip (2026-08-31)

A second premise problem, found while checking §4.2 and confirmed in both dbt projects.

`ivm-bench`'s `dbt_project.yml` — **identically in the `feldera` and `dbspnet` projects** — declares
one gold view with no output connector at all:

```yaml
fact_market_history:
  # NOTE: At SF=100, the truncate-mode delta_table_output for this
  # 54M+ row materialized view never drains … Drop the output connector so
  # Feldera computes the view in-memory but skips the Delta write …
  # The benchmark still measures the compute cost; no Delta is persisted.
  +stored: true
```

The intent is explicit: **Feldera still computes and materialises the view**; only the Delta write is
dropped.

**We do not compute it at all.** `dbt_to_program.py` derives `outputs` / `output_bindings` from
`+connectors` alone and skips `+stored` as a config key it does not model
(`services/dbt_to_program.py:117` — *"a config key (+materialized, +stored, …), not a model"*). With
no connector there is no output binding, so `fact_market_history` is not an output, and
`CompileProgram`'s dead-view pruning removes it **together with its whole upstream chain** —
`finwire_financial → financials → wrk_company_financials → fact_market_history`, exactly the 4-view
chain `ColumnLivenessProbe` reported as already-pruned (design-row-representation §24).

**This makes the ivm-bench batch-1 comparison asymmetric in our favour.** Feldera computes a view
chain we never build. The asymmetry is at least:

- `fact_market_history` itself (54M+ rows at SF=100; unmeasured at SF=3), plus its three upstream views;
- and the `daily_market` window operators that exist *only* to feed it — op348 + op350 + op349, measured
  at **~3.4 GiB, ≈7.6% of the 44.74 GiB batch-1 allocation** (§24). Our column-liveness work correctly
  called those dead *for our outputs*, but Feldera is not given the same licence by this config.

So the headline **~3.4:1 batch-1 ratio is measured with DbspNet doing strictly less work**, and the
true engine-to-engine gap on the same workload is wider by whatever that chain costs. This does not
touch the Nexmark numbers (§26/§25, different harness entirely), and it does not change any
*allocation* result — but every ivm-bench batch-1 ratio in these docs inherits the caveat.

**Fixing it is a spec-generation change, not an engine change:** teach `dbt_to_program.py` to honour
`+stored: true` as "materialise this view even though nothing reads it" — which needs the delta-only
vs stored distinction §4.2 says we lack, since the honest translation is "stored, no output binding".
Until then, quote batch-1 ratios with this caveat attached.

**Not measured.** No SF=3 data exists on the current machine; the ~3.4 GiB figure is the earlier i9
measurement of the upstream window ops only, and `fact_market_history`'s own cost at SF=3 is unknown.

## 7. Parallelism: they pay *more* coordination, over bigger ticks

Our exchange/scaling arc concluded (design §15) that the ceiling is barrier coordination at fine ticks,
after falsifying both barrier-coalescing and W-sizing. Feldera does **not** avoid that cost: three
all-worker rendezvous per step (driver command broadcast, a `serde_json` metadata broadcast, commit
consensus), and their own `shard` docstring concedes the barrier "limits the scalability"
(`communication/shard.rs:63-69`).

Two things differ, and neither was tested by our arc:

1. **Tick size.** `DEFAULT_MAX_WORKER_BATCH_SIZE = 10_000` records *per worker*
   (`feldera-types/src/config.rs:56`). We tuned W; we never made the tick bigger.
2. **Yield-on-block.** Each worker runs its circuit on a current-thread Tokio `LocalSet`, so an
   operator blocked on an exchange yields to other ready operators instead of stalling the worker —
   exchange is split sender/receiver expressly for that (`exchange.rs:1616-1618`).

Their `circuit_wait_time_seconds / circuit_runtime_seconds` (`circuit/metadata.rs:167-176`) is directly
comparable to our 40% WAIT on q4, so this is measurable rather than arguable.

Beyond that: shardedness is a **tracked stream property** (81 propagation sites) giving exchange
elision, and a runtime **MaxSAT balancer** re-solves Shard/Broadcast/Balance per join-graph SCC from
measured key distributions — evidence their binding constraint is *skew*, not barrier count.

## 8. Optimizer: pruning and sharing pull opposite ways, resolved by ordering

- They **disabled Calcite's `PROJECT_JOIN_TRANSPOSE` as unsound**, "replaced with UnusedFields done
  later" (`CalciteOptimizer.java:409-410`).
- `visitors/unusedFields/` is a whole-circuit fixpoint liveness analysis over **closure parameter
  fields** (not relational projections), reaching joins, aggregates, star-joins, flatmaps, filters and
  windows. It rewrites the *source operator's* row type and marks columns so Delta/Iceberg connectors
  never decode them from Parquet.
- Then `ShareIndexes` / `ShareWindowIntegrals` deliberately **widen** index values so several joins
  share one arrangement — the opposite direction — resolved purely by pass ordering: prune at 103,
  widen-to-share at 127/132.
- **Arrangement reuse and cross-view CSE are free by construction**: one `RootCircuit` for all 50
  views, plus a build-time memo keyed by `StreamId` (`TraceId`, `IndexId`, `ShardId`, `IntegralId`),
  so any two consumers anywhere get the same node.
- `CreateStarJoins` builds n-ary joins with no intermediate materialisation; `ImplementChains`'
  `shrinkMapFilterMap` fixpoint narrows the tuple *between* fused stages.
- **No cost model anywhere** — pure Hep, no Volcano, the only cardinality being a user-declared
  `expected_size`. This validates our call.

Our join column pruning is the narrowing half without the sharing half, and `design-column-liveness.md`
designed a program-level liveness pass we never built — Feldera's is that pass, extended back into the
connector.

## 9. Which DbspNet decisions this puts in question

Ranked by how much the evidence moves them. **None of these reverse on this document alone**; each row
names what would settle it.

| # | Decision | Status after this research | What would settle it |
|--:|---|---|---|
| 1 | **ivm-bench runs with our per-batch checkpoint on, for "honesty"** (`design-incremental-persistence.md` §0) | **Wrong on the facts.** Feldera writes no checkpoint and retains nothing. Verified on both sides. | Nothing — just turn it off and fix the client comment. This is a correction, not an experiment. |
| 2 | **Track A over Track B** (`decision-trace-family.md` §2) | Premise weakened. Track A's urgency came from the per-batch checkpoint tax that item 1 says the benchmark never required. Feldera built B first, then A on top. | Re-price A2 with the benchmark corrected. If the checkpoint isn't in the batch window, what is A2 actually worth? |
| 3 | **Stop growing spine / flat stays default** (`decision-trace-family.md`) | ~~Most exposed.~~ **SETTLED against this row (`decision-trace-family.md` §6, 2026-09-01).** The experiment ran and the premise was wrong: on the ivm-bench bulk batch there is **no in-step compaction at all** — 0 merges over 20 ticks, because each spine trace builds ~1.03 batches and tiered compaction needs 4. A perfect background merger removes **0.0%** of the spine step. The +14% reproduces as +22% on the M4 and is **sorted-batch construction** (20.3% of step) plus probe-side reads. | Nothing on step cost — it has been run. A future case for spine needs the memory argument (§3) or resume latency (`design-incremental-persistence.md` §11.6). |
| 4 | **We are allocation- and hash-bound; columnar deferred three times** (`design-row-representation.md`, `repr-execution-apportionment.md`) | Reframed, not refuted. Both costs are consequences of the hash-indexed Z-set, not of DBSP. "Flat beats spine" and "we are allocation-bound" may be the same observation seen twice. | **Half-answered by item 3's experiment**: on the bulk batch our spine is slow *as a sorted-batch design* — one sorted batch + Bloom per delta is inherently more work than a dictionary insert when each delta is written once and read a few times. That is not an implementation defect, so this row's question narrows to the fine-tick steady state. |
| 5 | **Parallel-scaling arc CONCLUDED** (design §15) | Reopened narrowly. Barrier-coalescing and W-sizing were falsified; **tick size** and **yield-on-block** were never tried. | Raise the per-worker tick toward 10k records and measure WAIT% on q4 against their metric. Cheap. |
| 6 | **Operator identity deferred** (`design-durable-identity.md` §1) | The blocking objection has a published answer. Content-addressed ids + a `state_files` manifest keep loss loud. | Only worth it when checkpoint portability across a program edit is actually wanted — still not today. |
| 7 | **Codegen as a lever** (demoted 3×) | **Confirmed dead**, now from their side too (`dynamic.rs:11-17`). | Nothing. Stop revisiting it. |
| 8 | **No cost model** | Confirmed as the right call. | Nothing. |

## 10. Things worth stealing, independent of any decision above

Ordered by (value / cost), all of them separable from the trace question:

1. **Range-shaped dispatch** — one virtual call per *run*, then a static loop. Portable to our
   structural path as-is, and it attacks per-row cost without changing representation.
2. **Fix the ivm-bench comparison** (§6) — pure win, removes ~18.7 s/batch we were never obliged to pay.
3. **Tick-size and yield-on-block experiments** (§7) — two cheap probes against an arc we closed.
4. **`state_files` manifest** (§5.1) — makes checkpoint loss loud without adopting Merkle ids.
5. **State-as-indexed-Z-set** (§4) — the radix-tree trick; the general answer to "N operators each need
   their own spill/checkpoint path".
6. **Accumulate-then-evaluate** (§6.2) — potentially a real slice of the batch-1 gap, and entirely
   absent from our roadmap.
7. ~~**Don't integrate non-materialised views** (§4)~~ — **CHECKED and refuted in its literal form (§4.2)**: we already gate the integral on `IsOutput`. The live items it turned up are a delta-only output *option* on the serial path, and the parallel path's un-snapshotted driver-side view.

## 11. What this research did not establish

- **No DbspNet measurement was taken.** Every "this challenges X" is a claim about Feldera's source,
  not a demonstration about our code.
- **Nothing was built or run on the Feldera side** — no verification that their code behaves as read.
- **We did not price any of §10.** "Portable" is not "cheap", and the last three items are substantial.
- **We did not examine** their connector/adapter layer, SQL semantics coverage, or anything about
  correctness — only the performance and durability axes in `00-brief.md`.
- **The 3.5× ivm-bench batch-1 gap is still unapportioned.** §6.2 says *part* may be algorithmic. How
  much is unknown, and that apportionment is the obvious next measurement.
- **Calcite's `HepProgramBuilder` semantics are undetermined**, and this is a real gap rather than an
  unattempted one: the checkout vendors no Calcite (stock `calcite-core:1.43.0` from Maven Central,
  `pom.xml:19,329-331`), and the project has never been built on this machine, so no jar or `~/.m2`
  cache exists. Consequently we do not know whether the twice-added `joinOrder` step
  (`CalciteOptimizer.java:334` and `:398`) registers its rules once or twice on the second run, nor
  the default `HepMatchOrder` for steps that don't call `addMatchOrder`. Resolving either needs the
  Calcite 1.43.0 sources.
