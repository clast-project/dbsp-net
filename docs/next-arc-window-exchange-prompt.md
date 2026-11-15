# Starting prompt — next arc: what `dim_customer`'s window exchanges cost (measure first)

> Kickoff written 2026-11-15, after the program-path `NOW()` arc (`docs/design-program-temporal.md`)
> and a compile-only exchange census of the ivm-bench program. Paste the block below into a new
> session.

---

**Measure what window-function exchanges cost in ivm-bench batch 1 at W=12, then decide whether to
let window sites skip exchanges. Measurement first; design only if the number earns it.**

**What is known (census 2026-11-15, compile only, no data run).**
- **Setup.** The ivm-bench TPC-DI program compiled as the server does:
  - `numericStringCoercion`, `NullCollation.Low`, `NumericDialect.Feldera`;
  - `TryCompileParallel(workers: 12)`;
  - broadcast at the 1,000-row default, with SF=3 row counts.
- **Totals.** 138 exchanges among 419 operators in replica 0.

  | operator | count |
  |---|--:|
  | `BroadcastExchange` | 14 |
  | `ExchangeIndex` | 11 |
  | `ExchangeIndexStep` | 62 |
  | `Exchange` | 50 (7 of them gathers) |
  | `ExchangeStep` | 1 |

  The per-view baseline is in `docs/xview-census/baseline-w12-20261115.txt`.
- **Cross-view partition passthrough is dead. Don't revive it.**
  - Sharing the partition map across views removes **0 of 138** exchanges: 49 of 50 views end in a
    projection, which drops partition info.
  - Adding a bare-column projection remap reaches **136**.
  - Join-side elision would reach about **134**.
  - Harness and experiment diff: `docs/xview-census/`.
- **The lever it found is inside one view.**
  - `dim_customer` alone has **25 exchanges** (23 `Exchange`, 2 `ExchangeIndexStep`).
  - It runs one window `PARTITION BY customer_id`, then 22 windows
    `MAX(..) OVER (PARTITION BY customer_id, x_grp)`.
  - Every window site (`CompileWindowAggregate`, `CompileWindowOffset`,
    `CompilePartitionedRank`, `CompilePartitionedTopK` in `PlanToCircuit.cs`) exchanges
    unconditionally. None of them records an output partition (only the global ones mark
    `Gathered`), and none checks its input's.
  - By the subset rule, input already partitioned on `customer_id` co-locates every
    `(customer_id, x_grp)` group, so about 22 of the 23 could be skipped. That is about 16% of
    the program's exchanges.
  - **This was counted from the SQL. It is not built or timed.**

**Step 1: measure (this is the whole arc unless the number is big).**
- **What to run.** A profiled batch-1 run at **W=12** on the local SF=3 data:
  - inputs `~/src/ivm-bench/mount/raw/3/delta`;
  - spec `~/tpcdi-ab/spec-main.json`;
  - the recipes in memory `ivm-bench-local-datagen` and `ivm-bench-multicore`.
- **Harness.** `tests/DbspNet.Tests/Scratch/IvmBatchProfile.cs` runs the serial program only. Give
  it a worker-count knob, or use the multicore recipe.
- **Profile.**
  - `DBSPNET_PROFILE=1` gives per-operator times labelled by view (`BuildLabel`).
  - Run with `DOTNET_gcServer=1`, or scaling looks falsely bad (memory `ivm-bench-multicore`).
  - Turn GC DATAS off as in the other W>1 runs (memory `datas-gc-nexmark-throughput`).
- **The number.** The summed time of `dim_customer`'s exchange operators (gather and scatter
  together), and the window operators' own time, against the batch-1 step. Use **≥ 5 runs**,
  because SF=3 step noise is about ±2.5 s (memory `state-representation-arc`).
- **Also record** allocation per view, if the profile gives it. An exchange copies every row it
  moves.
- **Long runs.** Start them from Terminal via `nohup` + `bash -c` (memory `bounded-memory-arc`),
  and check `pmset -g` if a lone run is slow (memory `mac-sleep-breaks-benchmarks`).

**Decision rule.**
- **Exchanges ≲ 5% of the step:** stop. Write the number into `docs/xview-census/` and the
  ivm-bench gap analysis, and close the idea.
- **≳ 10%:** go to step 2.
- **In between:** ask Curt.

**Step 2 (only on a go): design first, as a section or a new `docs/design-window-partition.md`.**
- **Co-location.** A window site may skip its exchange when its input's `PartitionKey` (bare
  columns) is a subset of its PARTITION BY columns. Its output then records a partition, so the
  next window in the chain can skip too.
- **The hashes differ.** Window sites exchange by
  `StablePartitionHash.OfWholeRow(PartitionOf(row), SyntheticKeySchema(plan.PartitionKeys))`, a
  hash of the partition-key values. Aggregates and joins use `OfRow(row, indices, keySchema)`.
  Co-location only needs the subset rule. The **reshard placement** of a skipped window's state,
  though, must be the *upstream* hash recomputed from the stored row. That is the same discipline
  as `ElidedAggregatePlacement` (`docs/design-bounded-memory.md` §5.57.6) and as temporal filters
  (`docs/design-program-temporal.md` §2 Q3). A wrong placement is silently wrong, so a mutant with
  a wrong placement must fail a reshard test.
- **The chain's root.** The first `PARTITION BY customer_id` window still exchanges, and its output
  must record `customer_id` at its output position. Check what sits between the windows: a
  projection drops partition info today (`CompileLinearChain`). The census found a bare-column
  remap is sound, except through `TryFoldJoinProjection`, which records no partition.
- **Snapshots.** Skipping an exchange changes the operator layout, so the plan fingerprint changes.
  Old checkpoints then refuse loudly; say so in the design.
- **Acceptance.**
  - Parallel-vs-**serial** PBT arms over window chains (memory `parallel-view-in-circuit`).
  - Reshard tests continued against an uninterrupted serial run.
  - The batch-1 W=12 A/B against the current pin, ≥ 5 runs per arm.
  - TPC-DI still validates.

**Landmines.**
- `Sql.dll` incremental builds are flaky. Do a clean rebuild (`--no-incremental`) before the full
  suite and before mutation checks.
- A mutant restored from a backup keeps an old mtime, so `touch` the file.
- Label any heap or worker advice as sized for the 64 GiB Mac (memory
  `settings-sized-for-our-hardware`).
- Hand Curt any Docker commands.

**Working style.** Small commits on main. Run the full suite after a clean rebuild before each
commit. .NET 10 is in `~/.dotnet`.
