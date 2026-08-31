---
name: ivm-bench-repo-topology
description: "ivm-bench topology — both old checkouts are GONE; clone fresh, branch dbspnet-engine-experiments, and edits must be COMMITTED+PUSHED to reach any run"
metadata: 
  node_type: memory
  type: project
  modified: 2026-08-31T02:18:47.278Z
  originSessionId: 76b7e6c9-eb62-4996-8dd2-e02c5fd00969
---

**Updated 2026-08-30 for the move off the Windows box** — see
[[machine-migration-to-mac]]. The old two-checkout topology no longer exists.

**Gone from the Windows machine:** `D:\src\ivm-bench` (deleted), `/home/curt/ivm-bench` in WSL
(deleted), and Docker is not installed in WSL any more. Only `D:\src\ivm-bench-bak` remains — clean,
fully pushed, kept as a reference copy.

**Actual shape on the Mac (verified 2026-08-30):** `~/Documents/GitHub/ivm-bench`, remote
`https://github.com/mdrakiburrahman/ivm-bench.git` (upstream directly, *not* the CurtHagenlocher
fork), on branch **`dbsp-engine`** — 8 ahead / 2 behind `origin/dbsp-engine`. The handoff doc's
`CurtHagenlocher` + `dbspnet-engine-experiments` description belongs to the old Windows topology and
does not match this machine; trust the checkout, not the doc. Count unpushed with
`git rev-list --count '@{u}..HEAD'` — comparing against `origin/main` gives a bogus number.

**Why the branch is on someone else's repo:** `dbsp-engine` lives on `mdrakiburrahman/ivm-bench`
because that is the only place benchmark runs can use their credentials. Curt has collaborator push
rights there. Pushed and level as of 2026-08-30.

**The branch was rebuilt, so it diverges:** the July dbspnet integration was pushed, then recreated on
the Mac on top of six newer upstream PR commits (#48/#49/#57/#60/#61/#62). Verified the integration
content is byte-identical to the July original, so force-pushing lost nothing — done 2026-08-30 with
`--force-with-lease=dbsp-engine:0cdff85`. Expect to need `--force-with-lease` again if it re-diverges;
never a plain `--force` on this shared branch. Some Windows-era local WIP was lost before it was ever
pushed, which is why the old checkpoint config never reached this branch
([[ivm-bench-checkpoint-premise-wrong]]).

**The engine pin goes stale silently:** `DBSPNET_COMMIT` is duplicated in TWO places that must move
together — `docker/docker-compose.benchmark.dbspnet.yml` and `src/containers/dbspnet/Dockerfile`. It
sat at `2afa69b` (2026-07-21, 42 commits behind) until bumped to `0ee4b67` on 2026-08-30. Check it
before trusting any run: a stale pin silently benchmarks an old engine.

**The rule that survives the move, and the one that caused a lost round-trip:** an ivm-bench edit
reaches a run only if it is **committed AND pushed**. Nothing reads a local working tree.

dbsp-net is separate and the same rule bites harder: `src/containers/dbspnet/Dockerfile` clones
`clast-project/dbsp-net` from GitHub at a pinned `DBSPNET_COMMIT`, so unpushed dbsp-net commits are
invisible to every benchmark run on every machine. Bump `DBSPNET_COMMIT` (or `DBSPNET_REPO` for a
fork) to move the engine.

**Prerequisites are just Docker** (`ivm-bench/DBSPNET.md`) — the container clones and builds the
engine itself. No SF=3 data exists anywhere any more; regenerate via the `spark-batch-loader` datagen
when a full 3-batch run is actually needed.

Related: [[docker-runs-in-wsl]], [[ivm-bench-arc]], [[ivm-bench-checkpoint-premise-wrong]].
