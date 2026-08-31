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

**Current shape:** clone `https://github.com/CurtHagenlocher/ivm-bench.git`, check out branch
**`dbspnet-engine-experiments`** (NOT `main` — and note that counting unpushed commits against
`origin/main` gives a bogus number; use `git rev-list --count '@{u}..HEAD'`). Upstream is
`mdrakiburrahman/ivm-bench`.

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
