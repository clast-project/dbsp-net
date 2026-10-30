# Starting prompt — next arc: a row representation that is unboxed *and* shared

> Kickoff prompt for the multi-session row-representation arc, written 2026-10-30 at main
> `e6073cf`. Supersedes `docs/next-arc-columnar-prompt.md` (written before the parallel
> program existed and before batch 1's GC pause was decomposed). Paste the block below into
> a new session. Use Opus.

---

**Row-representation arc for ivm-bench batch 1 — design-first, measure-gated, on branch
`arc/row-repr` (multi-session; agents where the work is independent)**

**Where we are.** ivm-bench now runs the parallel structural program (`DBSPNET_WORKERS=12`,
Feldera's worker count). CI (PR #89, run 40980861676, SF=3): Feldera 27.2 / 3.50 / 3.70 s,
DbspNet 44.4 / 0.36 / 0.35 s — batches 2/3 are ~10× faster than Feldera, batch 1 (the bulk
load) is 1.6× slower using 376 vs 284 CPU-s. Read `docs/design-state-representation.md`
§9.15 and `docs/design-row-representation.md` §23.8 first; they hold the measurements below.

**What limits batch 1 (measured, local M4 Pro, Server GC):**
- No serial bottleneck at W=8/12: workers are balanced; worker 0 (gathered global operators)
  is not the straggler.
- **GC pause is the Amdahl term**: 4.5 s serial, 6.3–6.9 s at W=8/12 (~40% of a ~15 s step).
- **The pause is entirely young-generation copying** (runtime GC events): gen0 + gen1
  blocking/foreground collections of 200–450 ms each; gen2 runs in the background at ~0 pause;
  there is no blocking full GC. A bulk load keeps almost everything it allocates, so each
  young GC copies the new state up a generation — pause follows **surviving bytes**, each
  copied about twice.
- Live state after batch 1: **10.6 GB**. Per-group trace containers are 1.19 GB (11%); the
  **rows are ~80%**. 115 indexed traces, 5.4M groups, 24.9M values.

**Already tried — do not redo** (each is written up):
- GC knobs (gen0 budget 1–4 GiB, non-concurrent, GCHeapCount): no effect on pause.
- Shared boxes for decoded small ints / dates / bools: −0.25% allocation, reverted.
- One-entry Z-set held inline: **shipped** (`3be707e`, live −5.5%).
- Flattened trace: measured, not built (§9.15; FlatHash §10.C was built once — joins 9–15%
  slower, pause unchanged). Ceiling ~3% of batch wall.
- Typed program path (emitted structs): **grows** live state +26%, allocation +53% (§23.8).
  Structs are copied by value into every trace; a structural row is one `object[]` shared by
  reference. Whatever we build must keep that sharing.
- Sorted/spine substrate: loses to the flat hash on fine ticks (row-repr §8.3; spine memo).
- `docs/design-columnar-batch1.md` §7–§9 sized a columnar inter-operator interface on
  *serial allocation* and found no gateable single-view increment (fusion invariant). That
  was the wrong metric for today's problem — re-judge it on surviving bytes at W>1.

**The goal.** Cut the bytes (and objects) that survive a bulk load, so young GCs copy less:
a row form that is **unboxed** (no per-cell box objects, no `object[]` header per row) **and
shared** (one copy referenced by every trace, not copied by value). Candidates to design and
size — not a pre-made decision:
1. **Arena-backed `StructuralRow`**: rows keep their identity/handle object but their cells
   live in per-schema column pages large enough for the large-object heap (never copied by
   young GCs). Paging is mandatory: Server GC marks one huge reference array on a single
   thread (`TraceLayoutGcProbe`: 25M refs in one array = 3 s full-GC pause; 16K-entry pages =
   0.25 s).
2. **Columnar batches between operators** (design-columnar-batch1 target B), re-sized on
   surviving bytes.
3. Anything the phase-0 numbers point to instead (e.g. string storage, if `Utf8String`
   `byte[]`s are a large share — Nexmark found strings dominated live objects, row-repr §6).

**Phase 0 — measure before designing (gate).** Establish the survivor composition and the
ceiling:
- An in-process census of what traces hold, by kind: `StructuralRow` objects, `object[]`,
  boxes by type, `Utf8String` + `byte[]`, `Decimal128` boxes — bytes and counts. gcdump on
  .NET 10 is lossy at this heap size (dropped at 10M objects); either walk the traces
  (extend the `DBSPNET_TRACE_ACCESS_PROFILE` census) or try the .NET 11 runtime in
  `~/.dotnet11` for non-lossy gcdump.
- The **sharing factor**: how many traces reference each row (this decides how much a
  copy-by-value design loses).
- A synthetic ceiling in the style of `TraceLayoutGcProbe`, but timing *young-generation
  copying* of a growing heap (the real pause), not forced full GCs of a static one.
- **Gate:** if the best candidate cannot remove ≥ ~30% of surviving bytes (→ roughly ≥ 1.5 s
  of pause at W=8), stop and write up why.

**Phase 1 — design doc** (`docs/design-row-arena.md` or similar): the representation, how
rows stay shared across traces and workers (exchange moves row *references* today), how
hashing/equality work without boxing (`StructuralRowHash` has THREE implementations that must
agree — boxed, Expression, emitted IL; see the row-hash memory), snapshot/restore, and the
smallest increment that the gate instruments can see. Propose, then wait for Curt.

**Phase 2+ — build in measured increments**, each kept only if it pays.

**Instruments (all exist on main):**
- `tests/DbspNet.Tests/Scratch/IvmMultiBatch.cs` — SF=3 batches 1→2→3 locally. Env:
  `IVM_DATA_ROOT=$HOME/src/ivm-bench/mount/raw/3/delta`,
  `IVM_SPEC=$HOME/src/ivm-bench/mount/ivm_spec_sf3.json`,
  `IVM_STAGING_ROOT=$HOME/src/ivm-bench/mount/raw/3/delta/staging-multi`,
  `IVM_OUTPUT_MODE=incremental`, `IVM_WORKERS=N`. Add `IVM_MEASURE_LIVE=1` (live bytes after
  batch 1 = `PromotedBytes` of a forced full GC — never `GC.GetTotalMemory`, which swung
  12.7–21.5 GB on unchanged code), `IVM_GC_EVENTS=1` (pause by generation),
  `DBSPNET_PROFILE=1` + `DBSPNET_PROFILE_FILE` (phases, GC pause, per-worker balance,
  operators), `DBSPNET_TRACE_ACCESS_PROFILE=1` (trace shape census). It prints a
  `VIEW name rows= digest=` line per output — every arm must match main's digests (capture
  them on main first).
- `dotnet run --project src/DbspNet.Benchmarks -c Release -- w1profile 1000000 10000 5` —
  Nexmark B/ev, deterministic: the no-regression check for any core change. It overwrites
  `docs/w1-profile.md`; restore it after.
- `TraceLayoutGcProbe` (`TRACE_LAYOUT_PROBE=rows|indexed|flat|rows-paged|flat-paged`).

**Measurement rules (learned the hard way):** always `DOTNET_gcServer=1` (the server is
`Sdk.Web`; workstation GC made W=8 look 1.3×); system sleep is off (`pmset sleep 0`) — if a run shows a lone slow segment, check `pmset -g`; serialize runs
(never two measurements at once); wall-clock needs ≥5 alternating runs per arm (±15% noise) —
prefer deterministic measures (live bytes, allocation, B/ev); this Mac has 4 efficiency
cores, so W=12 locally is slower than W=8 and says nothing about the CI box.

**Working rules:** work on branch `arc/row-repr` (not main — this arc is long), push the
branch, merge only with Curt's go-ahead. Propose the plan and the agent count before
launching agents; good fits are independent measurements in phase 0 and competing design
prototypes in separate worktrees, each told what is ruled out above. Check
`git ls-files | grep ' [0-9]\.'` before trusting a build (iCloud conflict copies). After a
`Sql.dll` change, clean-rebuild before trusting a mutation test. Keep the default path
byte-identical until a change is proven, behind an env/option seam.
