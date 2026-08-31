# Handoff: migrating development off the Windows i9 box

**Status: HANDOFF, 2026-08-30.** Written to move active development to a Mac Mini. Records what is
machine-local on the Windows box (`D:\src\dbsp-net`, i9-12900K), what must travel, what cannot
travel, and what to do first on the new machine.

Read §5 before trusting any performance number measured on the new machine.

## 0. The short version

- **Everything that matters is now in git and pushed.** The Windows box holds no unique source.
- **The ivm-bench environment here was already gone before the move** — no `d:\src\ivm-bench`, no
  `~/ivm-bench` in WSL, and Docker not installed in WSL. That was lost in an earlier reinstall of this
  machine, not dismantled for the migration. Everything is recoverable from GitHub (§2).
- **The Mac can develop and test, but it cannot rebaseline our benchmarks.** §5.
- **The Windows box is not being wiped.** It stays available, so anything left on it (§4.3) is parked
  rather than lost — and it remains the only machine that can reproduce the x86 measurement baseline.

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

### 4.3 Parked on the old box: uncommitted engineered-wood work in WSL

`~/ew` in the WSL Ubuntu distro is on branch `chore/python-313` with **15 modified files**. Most of
the raw diff is CRLF noise, but ignoring line endings there is genuine work: **311 insertions / 220
deletions across 14 files**, including an `Xunit.SkippableFact` mechanism that lets the interop tiers
decide at runtime whether a JDK or `deltalake` is present.

Its `origin` is a **local path** (`/mnt/c/src/GitHub/engineered-wood`), not GitHub — so even
committing it only moves it to another folder on that machine. The commit it sits on (`eacb02e`,
"chore(ci): move to Python 3.13") is likewise not on GitHub; branch `chore/python-313` exists only
locally.

The work is complete and coherent as far as static inspection goes — zero leftover `EnsureAvailable`
call sites, zero plain `[Fact]`/`[Theory]` remaining in the Interop tests, 95 `SkippableFact` /
`SkippableTheory` — but it has never been compiled (no `dotnet` in that WSL distro). Substance is in
three files: `delta_rs_driver.py` (hard `os._exit` past CPython finalization, which gh-87135 turned
from a post-result abort into a permanent hang on 3.13), `InteropDriver.cs` (`EnsureAvailable()`
early-return reported skipped tiers as *passed*; plus a stdout-drain deadlock that defeated the
`WaitForExit` timeout), and the test csproj (`Xunit.SkippableFact`).

**Not urgent:** the Windows box is staying, so this is parked, not at risk. It belongs to a different
project and should be picked up in a fresh session in that repo, where it can actually be built and
the interop tiers run.

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
- **Docker architecture: RESOLVED, the stack is arm64-clean on our path.** Confirmed 2026-08-30.
  Feldera publishes an arm64 image (verified by Curt; the registry needs auth so it was not checkable
  from Windows). Audited the rest of the stack at the same time — only **two** images are pulled
  rather than built:

  | Image | Arch | On the DbspNet-vs-Feldera path? |
  |---|---|---|
  | `images.feldera.com/feldera/pipeline-manager:latest` | arm64 ✔ | yes |
  | `mcr.microsoft.com/mssql/server:2022-latest` | **amd64 only** (single-arch manifest) | **no** — Hive metastore for the `spark` / `spark-openivm` engines only |

  Everything else is built locally, and every base image on our path publishes arm64 (verified by
  registry manifest for the Temurin and sbt images): `mcr.microsoft.com/dotnet/{sdk,aspnet}:10.0`
  (dbspnet), `python:3.11-slim[-bookworm]` (dbt-server, benchmark-server),
  `sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.6_2.12.21` + `eclipse-temurin:17-jre`
  (spark-batch-loader, spark-digen-delta), `eclipse-temurin:8-jre` (tpc-di-gen).

  So SQL Server is the only emulation exposure, and only if the Spark comparison engines are run.

- **EngineeredWood is pure managed** — `lib/{net10.0,net8.0,netstandard2.0}`, no `runtimes/`
  directory and no native payload — so the connector layer carries no architecture risk.

**Recommendation:** treat the Mac as the development and correctness machine, and keep an x86 Linux or
Windows box as the measurement machine. If that is not possible, the honest move is to declare a new
baseline and re-measure the handful of results that decisions actually rest on, rather than compare
across machines.

## 6. First actions on the new machine

1. ~~Check the Feldera image architecture.~~ **Done 2026-08-30 — arm64 exists**, and the rest of the
   stack was audited with it (§5). Nothing on the DbspNet-vs-Feldera path needs emulation.
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

## 8. Starting prompt for the first session on the new machine

Paste this into a fresh Claude Code session in the cloned repo. It deliberately ends without starting
work — the first session should orient and propose, not plunge.

```
This is DbspNet: a C#/.NET implementation of DBSP (incremental view maintenance)
with its own SQL front end. Development has just moved here from a Windows i9
box, and this machine has no history of the project.

Read `docs/handoff-machine-migration.md` first — it is the migration record
(repo inventory, environment, what did and did not travel). Then read
`docs/comparison-feldera-decisions.md`, which is the newest and most important
work: a source-level comparison against Feldera that challenges several of our
standing decisions.

Before anything else, two setup steps:

1. Restore project memory. Copy `docs/handoff/memory/*.md` (all except
   README.md) into this machine's Claude project memory directory. That is 51
   files indexing every completed arc, decision and landmine; without them you
   are missing most of the project's history. Once restored, treat the copy
   under docs/ as stale — do not edit it.
2. Verify the toolchain: `dotnet build`, then `dotnet test`. Report anything
   that fails, especially anything arm64-specific — nothing here has ever been
   built on Apple Silicon.

Three things to hold onto while you read:

- Every performance number in `docs/` was measured on the old i9 (8P+8E, x86,
  ServerGC). Never compare a number measured on this machine against them, and
  never quote them as current. W=24 experiments cannot be reproduced here at
  all — that is core count, not architecture.
- Do not reverse any decision on the strength of the Feldera comparison alone.
  Its §9 names, for each challenged decision, the measurement that would
  actually settle it.
- The docs are the source of truth for project state and roadmap, not memory
  and not git history.

When you have finished reading, tell me what you think the first piece of work
should be and why, with the alternatives you rejected. Do not start it yet.
```

Why it is shaped this way: the memory restore has to happen before the model forms a plan, or it will
re-derive things we already settled; the "do not quote i9 numbers" rule is the single easiest mistake
to make on a new machine; and asking for a proposal rather than an action gives you a checkpoint
before any work starts on hardware whose behaviour we have not characterised.

## 9. What this handoff does not cover

- No decision was reversed, and nothing in `docs/` was rewritten to match the Feldera findings. The
  comparison doc records the challenges; the original decision docs still state what they stated.
- The engineered-wood WSL work (§4.3) was not committed, pushed, or discarded — it needs an owner's
  call.
- No attempt was made to preserve the WSL distro, the NuGet cache, or `.claude/settings.local.json`.
- The Feldera arm64 question (§5, §6.1) is unresolved and blocks trusting any comparative measurement
  taken on the Mac.
