# Handoff: migrating development off the Windows i9 box

**Status: HANDOFF, 2026-08-30.** Written to move active development to a Mac Mini. Records what is
machine-local on the Windows box (`D:\src\dbsp-net`, i9-12900K), what must travel, what cannot
travel, and what to do first on the new machine.

Read §5 before trusting any performance number measured on the new machine.

## 0. The short version

- **Everything that matters is now in git and pushed.** The Windows box holds no unique source.
- **The ivm-bench environment here is already gone** — no `d:\src\ivm-bench`, no `~/ivm-bench` in WSL,
  and Docker is not installed in WSL. Nothing was lost in the move; it was dismantled earlier.
- **The Mac can develop and test, but it cannot rebaseline our benchmarks.** §5.
- **One thing did not travel and is still at risk**: uncommitted engineered-wood work in WSL (§4.3).

## 1. Why pushing was mandatory, not hygiene

`ivm-bench/src/containers/dbspnet/Dockerfile` clones `clast-project/dbsp-net` from GitHub at a pinned
`DBSPNET_COMMIT` build arg. The benchmark never sees a local working tree. Any commit that is not
pushed is invisible to every ivm-bench run, on any machine. The 14 commits that were sitting unpushed
on this box included the whole persistence arc and the trace-family decision.

To point a run at a new engine build, override `DBSPNET_COMMIT` (or `DBSPNET_REPO` for a fork).

## 2. Repository inventory

| Repo | Was at | Branch | State when handed off | Action on new machine |
|---|---|---|---|---|
| **dbsp-net** | `D:\src\dbsp-net` | `main` | 14 commits unpushed + 8 uncommitted paths — **all now committed and pushed** | `git clone https://github.com/clast-project/dbsp-net.git` |
| **ivm-bench** | *gone* (only `D:\src\ivm-bench-bak`) | `dbspnet-engine-experiments` | Clean, 0 unpushed vs its own upstream | `git clone https://github.com/CurtHagenlocher/ivm-bench.git`, checkout `dbspnet-engine-experiments` |
| **feldera** (reference only) | `D:\src\feldera` | — | Clean, 0 unpushed, git `78afc9077` (2026-08-29) | Clone if you want the source; nothing of ours lives there |
| **engineered-wood** | `C:\src\GitHub\engineered-wood` | `main` | Clean, 0 unpushed, **9 behind** origin | Only needed if you build it from source; we consume it from nuget.org |
| **engineered-wood (WSL)** | `~/ew` | `chore/python-313` | **Uncommitted real work — see §4.3** | Decide before wiping the box |

Note on the ivm-bench count: measured against `origin/main` it looks like 34 unpushed commits, but it
lives on a feature branch and is fully pushed against `origin/dbspnet-engine-experiments`. Use
`git rev-list --count '@{u}..HEAD'`, not a comparison against `main`.

## 3. Environment needed on the new machine

- **.NET SDK 10.0.400** (what this box ran). The repo has no `global.json`, so a newer patch is fine.
- **Docker** — required for ivm-bench. On Windows this meant WSL; on macOS, Docker Desktop is enough
  and **WSL is not part of the story any more**. Per `ivm-bench/DBSPNET.md`, Docker is the *only*
  prerequisite: the benchmark container clones and builds the engine itself.
- **NuGet**: `EngineeredWood.DeltaLake.Table` / `EngineeredWood.Parquet` are consumed as
  `PackageReference` at version 0.1.0 (`Directory.Packages.props`). **Verified published on
  nuget.org** (0.1.0, 0.2.0, 0.3.0 available), and no custom feed is configured — so a clean restore
  works with no local package source. This box also had 0.1.0 in `~/.nuget/packages`; that cache is
  not needed.
- **No `CLAUDE.md`** exists in this repo. `.claude/` is gitignored (`.gitignore:19`), so
  `settings.local.json` (permission allowlist, ~24 KB) does not travel and will be rebuilt by use.

## 4. Machine-local state, and what happened to it

### 4.1 Claude Code project memory — snapshotted into the repo

48 memory files lived in `C:\Users\curt\.claude\projects\D--src-dbsp-net\memory\`, entirely outside
git. That directory is the index of every completed arc, landmine, and roadmap note, and a fresh
machine starts with it empty.

Snapshot committed to **`docs/handoff/memory/`**. To restore on the new machine, copy its contents
into that machine's project memory directory (path will differ — it is derived from the project path,
e.g. `~/.claude/projects/<mangled-path>/memory/`).

Treat it as a point-in-time copy: once restored and live, the copy in `docs/handoff/memory/` is stale
and should not be edited. It exists so the context survives the move, not as a second source of truth.
The authoritative record of *decisions* remains `docs/`.

### 4.2 Benchmark data — none of it is here

There is no SF=3 dataset on this machine: nothing under `D:\`, and `~/ivm-bench` in WSL is gone. It
must be regenerated on the new machine through ivm-bench's `spark-batch-loader` datagen (see
`ivm-bench/DBSPNET.md` §Prerequisites/Run). The full 3-batch benchmark needs it; the compile-only
validation path does not.

The scratch probes in `tests/DbspNet.Tests/Scratch/` are all env-var gated and no-op unless driven.
The variables they read:

```
IVM_SPEC IVM_DATA_ROOT IVM_STAGING_ROOT IVM_OUT_ROOT IVM_SNAPSHOT_DIR IVM_WAL_DIR
IVM_BATCHES IVM_TARGET IVM_SOURCES IVM_TRACE_FAMILY IVM_TYPE_VIEWS IVM_SNAPSHOT_AFTER
IVM_WAL_ONLY IVM_WALPROF IVM_MICRO IVM_FUSE IVM_BCAST IVM_BCAST_MAXROWS IVM_DEAD_COLS
IVM_PROFILE_FILE IVM_CENSUS_FILE IVM_SEAM_FILE IVM_DUMP_FILE IVM_DUMP_VIEW
```

Two probes carry stale Windows-specific advice in comments — `IvmCheckpointReuse.cs:30` and
`IvmRecoveryProbe.cs:27` both say to put the snapshot dir "on /mnt/d" because a run writes several GB.
The several-GB part still holds; the path does not.

### 4.3 At risk: uncommitted engineered-wood work in WSL

`~/ew` in the WSL Ubuntu distro is on branch `chore/python-313` with **15 modified files**. Most of
the raw diff is CRLF noise, but ignoring line endings there is genuine work: **311 insertions / 220
deletions across 14 files**, including an `Xunit.SkippableFact` mechanism that lets the interop tiers
decide at runtime whether a JDK or `deltalake` is present.

Its `origin` is a **local path** (`/mnt/c/src/GitHub/engineered-wood`), not GitHub — so even
committing it only moves it to another folder on this machine. **This is the only content on this box
that is not recoverable from a remote.** It belongs to a different project, so it was deliberately
left alone here. Decide before the box is wiped: commit and push it through the C: clone to
`clast-project/engineered-wood`, or discard it consciously.

## 5. What cannot travel: the measurement baseline

Every performance number in `docs/` was measured on **this box**: i9-12900K (8 P-cores + 8 E-cores),
ServerGC, Windows. The Mac Mini is Apple Silicon with fewer cores and no SMT. Consequences:

- **W=24 experiments have no equivalent.** The spine-vs-flat substrate result (spine loses 1.4–2.5× at
  W=24), the `flatagg` and `q4flat` numbers, and the exchange/scaling decomposition were all taken at
  worker counts the Mac cannot reach. These become *unrepeatable*, not merely different.
- **The Nexmark W=14 snapshot** (`nexmark-feldera-w14-snapshot`) and the q4/q18/q19 W=8 results are in
  the same position.
- **Allocation cost is exactly what changes.** Our whole per-row thesis is that the engine is
  allocation-bound (~50–60% fresh-dict allocation, ~40–48% whole-row hashing). Apple Silicon's unified
  memory and different allocator behaviour move precisely that term, so old and new numbers must never
  be compared directly, even at the same W.
- **Docker architecture may invalidate comparisons outright.** `docker-compose.benchmark.feldera.yml`
  pins `images.feldera.com/feldera/pipeline-manager:latest` with no `platform:` key. If that image has
  no arm64 manifest, Docker will emulate amd64 and **every DbspNet-vs-Feldera number from the Mac is
  meaningless**. This could not be checked from Windows (the registry requires auth).

**Recommendation:** treat the Mac as the development and correctness machine, and keep an x86 Linux or
Windows box as the measurement machine. If that is not possible, the honest move is to declare a new
baseline and re-measure the handful of results that decisions actually rest on, rather than compare
across machines.

## 6. First actions on the new machine

1. **Check the Feldera image architecture** — this gates everything comparative:
   `docker manifest inspect images.feldera.com/feldera/pipeline-manager:latest | grep architecture`
   If there is no `arm64`, stop and read §5 before running any benchmark.
2. Clone both repos (§2); check ivm-bench out on `dbspnet-engine-experiments`.
3. Restore the memory snapshot (§4.1).
4. `dotnet build` + `dotnet test` to confirm the toolchain and the nuget.org restore.
5. Regenerate benchmark data only when you actually need a full run (§4.2).
6. Resolve the engineered-wood question (§4.3) **before** this box is wiped.

## 7. Where the work stands

The live document is **`docs/comparison-feldera-decisions.md`** (2026-08-30), a source-level comparison
against Feldera `78afc9077` with the four underlying research reports in `docs/research-feldera/`. Its
§9 ranks which of our decisions the research put in question and names the measurement that would
settle each; §10 lists what is worth stealing, ordered by value over cost.

Two items there need no measurement and are the natural first work:

1. **Fix the ivm-bench comparison** (§6 of that doc). We run our per-batch checkpoint on "for honesty",
   but Feldera writes no checkpoint during an ivm-bench run and retains nothing — verified on both
   sides. It cost us ~18.7 s/batch we were never obliged to pay. Also fix the wrong comment at
   `ivm-bench/src/containers/dbt-server/services/feldera_client.py:185-189`, which attributes the
   47k-operator commit walk to persistence when it is the DAG being evaluated.
2. **Range-shaped dispatch** (§10 item 1) — one virtual call per *run* rather than per tuple. Attacks
   per-row cost without changing representation, and is independent of every open decision.

Prior open work, unchanged: **A2 — checkpoint policy on `ProgramRunner`** (WAL per batch, snapshot
every N), from `docs/design-incremental-persistence.md` §4. Note that item 1 above changes A2's
justification: if the benchmark never required a per-batch checkpoint, A2's payoff needs re-pricing
before it is built.

## 8. What this handoff does not cover

- No decision was reversed, and nothing in `docs/` was rewritten to match the Feldera findings. The
  comparison doc records the challenges; the original decision docs still state what they stated.
- The engineered-wood WSL work (§4.3) was not committed, pushed, or discarded — it needs an owner's
  call.
- No attempt was made to preserve the WSL distro, the NuGet cache, or `.claude/settings.local.json`.
- The Feldera arm64 question (§5, §6.1) is unresolved and blocks trusting any comparative measurement
  taken on the Mac.
