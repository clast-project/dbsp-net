# Starting prompt — next arc: the first batch after a lazy restore (issue #8)

> Kickoff written 2026-11-15, after the exchange-work decomposition closed
> (`docs/xchange-phases/README.md`: no piece earns a design). Batch-1 parity with Feldera is
> reached (PR #98), so this goes back to the durable-state targets. Paste the block below into a
> new session.

---

**Get the first batch after a fresh-process restore within 2× of the same batch without a restart
(issue #8), and find out why the first output write after restore takes 2.5–3.0 s against
0.9–1.3 s. Measure first, and attribute before you build.**

**What is known (issue #8 comments; `docs/design-durable-state.md`, "The first batch after restore,
re-measured and apportioned"; raw data in `docs/fresh-restart-w12-20261115.txt`).**
- **Recipe.** IvmMultiBatch (`tests/DbspNet.Tests/Scratch/IvmMultiBatch.cs`), SF=3, W=12,
  `DBSPNET_STATE_LAYOUT=runs`, `DOTNET_gcServer=1`. The "straight" arm runs batches 1–3 in one
  process. The "resume" arm runs `IVM_STOP_AFTER=1`, then `IVM_RESUME=1` in a new process (needs
  `IVM_SNAPSHOT_DIR`). Run 5 alternating rounds. Every `RESTORE` line must say
  `view_rows_match=True`.
- **Where it stands (`d70a912`).**

  | | JIT | ReadyToRun |
  |---|--:|--:|
  | batch-2 drain after restore | 473 ms | **264 ms** |
  | batch-2 drain, no restart | 128 ms | 98 ms |
  | ratio (target 2×) | 3.7× | **2.7×** (~70 ms over) |
  | first output write after restore | 2,992 ms | 3,236 ms |
  | first output write, no restart | ~0.9–1.3 s | ~1.2–1.7 s |

- **The drain's excess, apportioned (JIT build).**
  - Stream-release reader scan: ~170 ms. Mostly fixed in `d70a912`; 33–56 ms remain.
  - Fresh-process JIT / tier-0 code: ~255 ms.
  - Restored state itself: ~75 ms, mostly `dim_customer` left-join probes faulting and
    zstd-decompressing blocks.
- **Under R2R, batch 2 still spends 614–877 ms in JIT** (summed over threads, the `jit_ms` field).
  The guess is Expression-compiled delegates, which R2R can't precompile. **That guess is
  unverified.**
- **`DOTNET_TieredPGO=0` on top of R2R** (3 runs, a hint only): drain unchanged, first write
  ~2.6 s.
- **The first output write is the bigger absolute cost**: ~1.7 s of excess vs ~0.17 s in the
  drain. It was fixed once (2026-11-01, §"The first output write after a lazy restore"): GC debt,
  paid by `CheckpointGcMode.AfterLazyRestore`'s blocking collection, brought it back to 1.33 s.
  Two things have changed since:
  - **That collection no longer fires.** It is skipped below a 1 GiB heap
    (`src/DbspNet.Persistence/CheckpointGcMode.cs:47`). Block-held traces left the restored heap at
    **0.44 GiB** (was ~5.7 GiB), and the RESUME lines show `restore_gc_ms=0`.
  - Batch 2 after a resume runs **6–7 gen2 GCs** (`gen2=` field) against 0–1 without a restart,
    with `gc_pause_ms` 130–210.
  - Second suspect: the Delta output connector keeps every view row as a key in its position map
    (incremental mode, for deletion vectors). After a restore it may rebuild that map for ~43M view
    rows on the first write.
  - Neither suspect is measured. `DBSPNET_OUTPUT_TRACE=<file>` gives per-view `mat` / `apply` /
    `form_mib` / `arrow_mib` / `apply_mib`.
- **Unexplained caveat.** These runs report ~43M view rows against 3.58M in the 11-01 data. Both
  arms run the same workload, so the ratios stand, but say which one you measured.
- **Already settled; don't redo.**
  - Lever 2, the cache of open run files and recent blocks: superseded. Block-held traces read a
    block once and keep it, and `DBSPNET_RESTORE=eager` is a no-op for them.
  - Moving the remaining ~40 ms scan into restore would race restore's writes to operator state.
    Not built.
  - The window and rank operators aren't slow after restore. With JIT-warm code (an in-process
    restart) they match the no-restart run.

**Step 1: attribute (this may be the whole arc).**
- **The first write.**
  - Same recipe with `DBSPNET_OUTPUT_TRACE` and `IVM_GC_EVENTS=1`.
  - Split the 2.5–3.0 s into materialize, Arrow, connector apply and GC pause. Do it per view, for
    the resume and straight arms.
  - Test the GC-debt suspect directly: one arm where `AfterLazyRestore` collects regardless of
    heap size (a temporary env override).
  - Test the position-map suspect: time the connector's position-map setup on its own.
- **The JIT.**
  - Get per-method JIT events for batch 2 under R2R: `dotnet-trace` with the JIT keyword, or a JIT
    `EventListener` in IvmMultiBatch.
  - Split them into `lambda_method` / DynamicMethod (Expression-compiled), generic instantiations
    R2R didn't pregenerate, and the rest. The split decides the lever.
- **Runs.** 5 alternating rounds per arm. Rotate arm order. Build both arms in the same worktree
  path (`~/tpcdi-ab/wt`). For R2R, publish both arms from the same project with
  `-r osx-arm64 -p:PublishReadyToRun=true|false`, as the 11-15 A/B did.

**Decision rule.**
- **Drain:** build only the lever the JIT split points at.
  - Mostly Expression delegates: warm or cache them during restore. Verify first that forcing a
    DynamicMethod's JIT ahead of its first call actually works on CoreCLR. Don't assume
    `RuntimeHelpers.PrepareDelegate` does it.
  - Mostly missed generics: try R2R composite or instantiation hints.
  - Mostly gone with `TieredPGO=0`: ship that as the server default. Measure Nexmark W=14 first;
    PGO may be buying steady-state throughput.
- **First write:** if one cause is ≳ 1 s, fix it (the GC gate's threshold, or the connector map).
  If the excess is diffuse, write it up and ask Curt. It has no approved target yet (the approved
  target is the first batch's drain), so ask Curt whether "restart → first output" should get one.
- **Shipping R2R** needs a RID in both server Dockerfiles, which currently publish with none. That
  is an ivm-bench change: hand Curt the diff and the commands.

**Acceptance for anything built.**
- Batch-2 drain after restore ≤ 2× no-restart. Report the median and range over 5 alternating rounds.
- Every `view_rows_match=True`.
- Batch-1 drain doesn't regress (5 runs per arm).
- Nexmark W=14 doesn't regress (memory `nexmark-regression-check-2026-11-13`).
- The crash matrix (`CrashSafeCheckpointTests`, `ParallelCrashSafeCheckpointTests`) passes.
- A restore-path change keeps restore under 1 s and digest-verified (approved targets,
  `docs/design-durable-state.md` §"Targets").
- Update issue #8 with the numbers. Close it only on the 2× result.

**Landmines.**
- `DBSPNET_PROFILE`'s `StepProfiled` path skips the stream-release plan, so the per-operator
  profile never shows the scan. In a fresh process, per-op times are JIT-inflated, and `RangeRank`
  time is mostly barrier wait. Under Server GC, per-op CPU includes GC pauses. Use the new
  GC-subtracted columns (`b84c7ae`) or allocation.
- `Sql.dll` incremental builds are flaky. Clean rebuild (`--no-incremental`) before the suite, and
  check dll mtimes in a `bin-<sha>`. A mutant restored from backup needs a `touch`.
- IvmMultiBatch doesn't follow the server's broadcast default. Pass `IVM_BCAST_MAXROWS=1000`.
- Lone slow run: check `pmset -g`. Start long loops via `nohup` + `bash -c`.
- Label heap and worker advice as sized for the 64 GiB Mac. Hand Curt any Docker commands.

**Working style.** Small commits on main. Run the full suite after a clean rebuild before each
commit. .NET 10 is in `~/.dotnet`.
