# Briefing: DbspNet's decision space, for comparison against Feldera

You are researching **Feldera** (local checkout: `d:\src\feldera`, git `78afc9077`, 2026-08-29) on
behalf of **DbspNet** (`d:\src\dbsp-net`), an independent C#/.NET implementation of the DBSP
incremental-computation model with a SQL front end.

This document states **where DbspNet landed** on each major design axis, and **why**, so you can
answer the only question that matters: **where did Feldera decide differently, and what does their
choice buy or cost that ours doesn't?**

Read this as *our* position — it is sourced from our own design docs and measurements. Your job is
not to audit it. Your job is to find Feldera's corresponding choice in their source and report the
delta, with evidence.

## 0. Context: why we care

We benchmark head-to-head against Feldera on two workloads:

- **Nexmark** (q0–q22) — streaming queries, small rows, high tick rate.
- **ivm-bench** (TPC-DI-derived, 50-view dbt DAG, SF=3/SF=10) — wide SCD rows, bulk historical load
  then small incremental batches. Feldera is the reference implementation for our engine class.

Current standing, roughly:
- Nexmark at W=14 workers: we win or tie most queries; residual gaps (q4/q18/q19/q22) are
  exchange/scaling-bound.
- **Single-threaded, we lose to Feldera on 11 of 13 Nexmark queries.** Per-row cost is essentially
  the *whole* gap. We out-scale them (coordination is our strength), which masks it at high W.
- ivm-bench bulk load (batch 1, SF=3): Feldera ~20.9 s, us originally ~147.6 s, now ~3.5× gap after
  algorithmic wins. The residual is allocation-bound per-row work.

So the recurring question behind every axis below is: **why is Feldera's per-row/per-tuple cost so
much lower, and which of their structural decisions produce that?**

## 1. Axis: trace family — "flat" vs "spine" (LSM)

DbspNet has **two** trace implementations, and this is our biggest duplication cost (~10.8k LOC):

- **Flat** (default, shipping): `src/DbspNet.Core/Operators/Stateful/` — `Trace.cs`,
  `Arrangement.cs`, dictionary-backed indexed Z-sets. State is a live mutable dictionary per
  operator. There is **no spill path**: flat state is pinned in RAM. On checkpoint it writes **one
  file per operator** (whole-operator rewrite).
- **Spine** (opt-in via `CompileOptions.TraceFamily = TraceFamily.Spine`):
  `src/DbspNet.Core/Operators/Stateful/Spine/` — an LSM-style structure of immutable sorted batches
  with tiered compaction (`TieredCompactionStrategy`, `SpineBatch`, `SpineZSetTrace`,
  `SpineIndexedZSetTrace`), optional disk spill (`SpineSpillConfig`), merge/gallop probes.

**Our decision (2026-07-22, `docs/decision-trace-family.md`): STOP GROWING SPINE.** Keep flat as
default; keep spine as-is behind the flag; do NOT build the 8 missing spine operator siblings; do NOT
build reference-manifest snapshots. Measured basis:

| | flat | spine |
|---|--:|--:|
| bulk batch step (SF=3, 50 views) | 58.3 s | 66.6 s (+14%) |
| incremental batch step | 232/192 ms | 247/269 ms (noise) |
| incremental checkpoint save | ~18.8 s | ~22 s |
| snapshot byte reuse across a batch | 12.7% | 70.6% spine-backed, **91.3% of those unchanged** |

Spine's LSM reuse is excellent and reproducible — but the payoff only exists for a *per-batch*
checkpoint, and our persistence decision (§3) deletes the per-batch checkpoint. Also: spine cannot
bound memory, because the operators with no spine sibling are **29.4% of SF=3 state** and include
`IntegrateOp` (materialised output views), `PartitionedWindowAggregateOp`, `PartitionedOffsetOp`,
`PartitionedRankOp`.

We also measured spine as a general **substrate**: it loses to flat by 1.4–2.5× at W=24 on Nexmark.

**Questions for Feldera:**
1. Does Feldera have one trace family or several? What is the `dbsp` crate's `Spine` / batch-trait
   structure, and are there non-LSM trace implementations for any operator?
2. Do *all* stateful operators go through the same trace abstraction, or are there operators with
   bespoke state (their equivalents of our `IntegrateOp`, window aggregate, offset/lag, rank/top-k)?
   Specifically: **is there any state in a Feldera pipeline that cannot spill to storage?**
3. What is their compaction strategy and merge scheduling? Is merging amortised incrementally per
   step (a "fuel"/budget model) or done in bulk? Our compaction is bulk-on-threshold.
4. How do they handle the small-batch case, where an LSM's per-batch overhead dominates? (We
   measured +14% step on the bulk batch for spine vs flat, and we have a `SpineStagingConfig` for
   staging small batches.)
5. Do they have a "lazy merge view" concept? Ours is `LazyMergeMultiset` / `MergeViewMultiset`:
   defer materialising the merge of a trace with its delta until forced. It was a 4.6–19× win on
   aggregate-heavy shapes.

## 2. Axis: row representation & per-row execution cost

This is where we believe the real gap is. Our findings, from a long measured arc
(`docs/design-row-representation.md`, 245 KB of decision record):

- The engine is **allocation-bound**, not compute-bound. An apportionment benchmark (`reprbench`)
  put the per-tuple floor at **~50–60% fresh-dictionary allocation + ~40–48% whole-row hashing**,
  with actual expression *execution* essentially free. This demoted "generate code for expressions"
  as a lever **three separate times**.
- We have **two compile paths**: a **structural** path (rows are `object[]` / `StructuralRow`,
  interpreted expressions, shared by reference between operators) and a **typed** path (monomorphised
  generic operators over CLR types). Typing the ivm-bench views made things **worse** (+82% alloc,
  +42% wall) because the structural path shares `object[]` by reference and typing inserts
  decode/encode at every seam. The seam round-trip was only ~3.5%; the real cost was **boxed key
  extraction inside operators** (72% of the typed penalty). We fixed one instance
  (`MonomorphizeWindowOrderKey` — unboxed monotone long key: −58% alloc, −23% time on typed
  window-agg) and shipped it default-on.
- **Columnar execution** has been designed twice (`docs/design-columnar-batch1.md`,
  `docs/design-column-liveness.md`) and never built — each time the apportionment said the prize was
  smaller than the cost.
- Landed wins were all *narrowing* wins, not representation changes: join column pruning
  (projection pushdown through join: q4 −50% at W=1, 2.93–4.19× at W=8), non-linear aggregate input
  narrowing (q4 W=1 −35% time), partitioned TOP-K row narrowing, operator fusion (Filter/Project →
  one pass), cross-view CSE, adaptive delta-builder pre-sizing (−16–35% alloc), lazy boxing on
  output.
- Delta buffer pooling (`DeltaPoolMode`) is built but thin on throughput.

**Questions for Feldera:**
1. **What is a row, concretely, at runtime?** The SQL compiler (`sql-to-dbsp-compiler`) generates
   Rust — so what does it generate? Named structs per schema? Tuple types (`Tup2`…`TupN`)? How are
   NULLs represented (`Option<T>`? a nullability bit)? What are SQL `DECIMAL`, `VARCHAR`, `TIMESTAMP`
   lowered to?
2. Is the generated code **statically monomorphised end to end** (no dynamic dispatch on the row
   path), or is there a `dyn`/trait-object boundary anywhere per-row?
3. **Where do rows live?** Are batches columnar (per-column arrays / "layers" / trie-structured, à la
   differential-dataflow's `OrdValBatch` column layout), or row-major? Do they use arena/bump
   allocation, `rkyv` archived/zero-copy data, or interning?
4. **Hashing**: do they hash whole rows, or only key columns? Is a hash cached/memoized on the row or
   batch? What hasher?
5. **Do they do projection/column pruning in the compiler**, so wide TPC-DI rows never reach an
   operator that doesn't need them? This is the single biggest lever we found — I want to know if
   they get it structurally instead.
6. Any evidence of **vectorised/batch-at-a-time** inner loops vs tuple-at-a-time?
7. How do they avoid the fresh-allocation-per-tick cost we measured? (Reused buffers? Batch builders
   with capacity hints? An allocator?)

## 3. Axis: persistence, checkpointing, recovery

Our checkpoint is a **full state rewrite every batch** — `Snapshot.WriteAsync` walks every
`ISnapshotable` and re-serialises its whole trace, O(state) not O(delta). On ivm-bench SF=3 (~4.0 GiB
state) an incremental batch of ~200 rows costs ~60 ms of step and **~18.7 s of save**: the checkpoint
is ~90% of a durable batch. This matters competitively because ivm-bench runs Feldera with
`transaction_mode: always` — persistence *inside* the batch window — so an honest comparison forces
our checkpoint on too.

Two tracks were considered (`docs/design-incremental-persistence.md`):

- **Track B — incremental serialization**: durable batch identity + reference-manifest snapshots (a
  snapshot *names* unchanged immutable spine batch files instead of copying them) + mark-and-sweep GC
  over shared files. Measured ceiling: incremental save 18.7 s → ~6.3 s.
- **Track A — don't checkpoint every batch**: WAL every batch, snapshot every N.

**We chose Track A and it killed Track B.** Once snapshots are periodic, the flat-vs-spine save
difference amortises to at or below the cost of writing the outputs (1.24 s/batch at N=10). For the
ivm-bench shape the policy is "snapshot once after the bulk load, then WAL the incremental batches
indefinitely". Recovery is dominated by the ~35 s snapshot **restore**; WAL replay of a small batch
is free (a claimed 70× replay slowdown was measured, then **retracted** as a measurement defect).

Related, now retired: `docs/design-durable-identity.md`. Stage 1 (a durable id on every spine batch)
shipped. Stages 2–3 (stop-delete + mark-and-sweep, reference manifests) retired with Track B.
**Operator identity is positional** (`op-{i}` by build order), guarded by a plan fingerprint + schema
fingerprint + operator count that hard-fails on mismatch — so a checkpoint survives a recompile of
the same program but **not a program edit** (adding/removing an output view shifts indices). We
deliberately deferred stable operator ids: a colliding stable id fails *silently* where positions
fail *loudly*.

One real bug this arc found: order-sensitive float accumulators (`SUM`/`AVG` over DOUBLE,
`STDDEV`/`VAR`) weren't persisted, so restore silently produced wrong values. Everything exact
(integer SUM, COUNT, MIN/MAX, Decimal128) is reconstructed by re-folding the restored group.

**Questions for Feldera:**
1. **Is Feldera's checkpoint incremental?** Concretely: does a checkpoint copy batch data, or does it
   write a manifest referencing already-durable immutable batch files? (I believe this is exactly our
   Track B and I want it confirmed or refuted in source.)
2. If reference-based: how do they handle **lifetime/GC** of a batch file referenced by a retained
   checkpoint but dropped by compaction? Refcount, mark-and-sweep, epoch, or "never delete"?
3. **How do they name operators/state durably** across a restart and across a *program change*? Is
   there a stable operator identity (node id from the compiled plan?), and what happens if the SQL
   changes — is a checkpoint portable across a program edit, or rejected?
4. Is there a **WAL / journal** at all, or is the checkpoint the only durability mechanism? What
   exactly does `transaction_mode: always` cost and guarantee — what has to be fsynced before a batch
   is acknowledged?
5. **How is state that is already on storage treated at checkpoint time?** If their traces are
   file-backed by default, "checkpoint" may be nearly free by construction — which would make our
   whole Track A/B dilemma an artifact of keeping state in RAM. Confirm or refute.
6. What does **recovery** cost them, and is state lazily paged back in rather than fully restored?
   (Our ~35 s restore is a full rebuild.)
7. Do they persist **aggregator accumulator state**, or re-fold groups on restore? Do they have our
   float-order-sensitivity problem, and if so how is it handled?

## 4. Axis: parallelism, scheduling, and the exchange model

Our findings (`docs/design-structural-parallel.md`, `docs/design-row-representation.md` §15):

- We run a **synchronous BSP** model with hash-partitioned `ExchangeOp` / `ExchangeIndexOp` and an
  `ExchangeCoordinator` barrier per step (`src/DbspNet.Core/Circuit/`). Same family as DBSP's own.
- The parallel-scaling arc **concluded**: our ceiling is **barrier coordination at fine ticks**, not
  wide-row movement. Individual operators scale 7–9×; realised step scales 3.5–5×; barrier WAIT
  reaches 40% on q4. Coalescing barriers and W-sizing were both tried and **falsified**.
- Exchange elision, bucket-list partitioning, and `ExchangeIndex`/join fusion all landed.
- The residual gaps after that arc were **per-row**, which sent us back to axis 2.
- Only the **structural** path got parallel exchange insertion; the typed path and `ProgramRunner`
  (the multi-view program driver) are single-threaded — the driver-side view gap is unaddressed.

**Questions for Feldera:**
1. What is the worker/runtime model — OS threads per worker, work stealing, or strict BSP with a
   barrier per step? How is a step's completion detected (our barrier vs their scheduler)?
2. **How do they exchange data between workers?** Hash-partition on the key? What is physically moved
   — whole rows, or batches of a columnar layout? Is there a zero-copy/shared-memory path?
3. Do they have **exchange elision** (skipping a shuffle when the data is already partitioned
   correctly)? Is partitioning tracked as a plan property?
4. Are **nested circuits / fixpoint (recursive views)** scheduled differently, and do they parallelise?
5. Is there any **asynchrony** — pipelining across steps, or must all workers finish step N before
   step N+1? Any adaptive batching of input into ticks?
6. How does their **multi-view program** (a pipeline with 50 views) execute — one circuit with shared
   subgraphs, or independent circuits? Do they do cross-view CSE?

## 5. Axis: SQL compilation & optimizer

Ours: `src/DbspNet.Sql/` — our own parser, resolver, and plan optimizer (no Calcite). Landed
plan-level optimizations: intra-view CSE and cross-view CSE, operator fusion (Filter/Project),
projection pushdown through joins ("join column pruning", default-on), non-linear aggregate input
narrowing, partitioned TOP-K narrowing, join residual pushdown into the flat join combine, lateness /
bounded-history trace GC, temporal filters as `NOW()`-driven advancing predicates. We recently mined
Calcite's rule set for gaps (`docs/calcite-rule-census.md`, `docs/research-calcite-rules.md`).

Feldera's front end is `sql-to-dbsp-compiler` (Java, Calcite-based) emitting Rust.

**Questions for Feldera:**
1. Which Calcite rules do they actually enable, and what **DBSP-specific** rewrites do they add on
   top (incrementalisation, distinct pushdown, lateness/watermark propagation, index sharing)?
2. Do they **share arrangements/indexes** across operators and across views (one arrangement reused
   by several joins on the same key)? This is the DD "arrangement reuse" idea — do they have it, and
   is it cross-view?
3. How do they decide **what to materialise**? Do intermediate views get integrated state, or only
   declared outputs?
4. What is their story for **LATENESS / watermarks / retention** — how much state can be GC'd, and is
   it inferred or declared?
5. Anything on **query planning cost models** — or is it all rule-driven?

## 6. Ground rules for the research

- **Source-reading first.** The checkout is at `d:\src\feldera` (readable directly from Windows).
  Prefer reading code and their own design docs (`d:\src\feldera\docs`,
  `d:\src\feldera\docs.feldera.com`, crate-level `//!` module docs, `CLAUDE.md`) over inference.
- **Do not build or run unless a claim genuinely requires it.** Feldera does **not** build on
  Windows; if you must, use WSL: `wsl bash -lc "cd /mnt/d/src/feldera && cargo ..."`. A Rust build of
  this tree is very expensive — treat it as a last resort and say so if you do it.
- **Cite evidence** as `path:line` for every substantive claim, e.g.
  `crates/dbsp/src/trace/spine_fueled.rs:120`.
- **Distinguish** what you verified in source from what you inferred or read in prose docs. Mark
  inferences explicitly. "I could not determine X" is a valuable answer; a confident guess is not.
- **We are not looking for a verdict on who is better.** We are looking for *decisions that differ*
  and the mechanism behind the difference — especially anything that would change one of our
  standing decisions above, or explain the single-threaded per-row gap.
