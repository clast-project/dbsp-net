---
name: ivm-bench-repo-topology
description: "Two checkouts + WSL/Windows split for the ivm-bench work — where I edit vs where Curt runs Docker, and how they sync"
metadata: 
  node_type: memory
  type: project
  originSessionId: 59c84cbd-f811-435b-8b7b-3f6e874d8a8b
  modified: 2026-07-20T19:48:44.783Z
---

The ivm-bench work spans TWO checkouts on the SAME physical machine (`columnar`):

- **`D:\src\ivm-bench`** (Windows) — where I (the agent) edit ivm-bench files. My edits
  land in this working tree.
- **`/home/curt/ivm-bench`** (WSL-native ext4) — where Curt runs Docker (`benchmark.sh`,
  compose). Its `mount/` (raw data, results, logs) is **root-owned** (Docker writes as root
  → needs `sudo` to copy/rm). Branch: `dbspnet-engine-experiments`.

**Sync = git.** Curt commits my `D:\src\ivm-bench` working-tree edits, pushes to GitHub, and
`git pull`s into `~/ivm-bench`. So **for an ivm-bench edit to reach Curt's Docker runs it must
be COMMITTED + PUSHED** (origin/dbspnet-engine-experiments), not just left in the D: working
tree. (dbsp-net is different: its Dockerfile clones from GitHub `clast-project/dbsp-net` at a
pinned commit, and I push dbsp-net straight to main — so dbsp-net changes reach Docker via the
DBSPNET_COMMIT bump, independent of the ivm-bench checkout.)

**Landmine that cost a round-trip:** I edited `oat_runner.py` (PRESERVE_RAW patch) in the D:
working tree but the ivm-bench changes were uncommitted → not on GitHub → Curt's `git pull`
couldn't get them. Symptom: "mount/raw is empty" after a PRESERVE_RAW=1 run. **ALWAYS commit +
push ivm-bench edits (to Curt's branch) so the pull works** — don't leave them uncommitted.

**Windows↔WSL data bridge:** `/mnt/d/...` in WSL == `D:\...` in Windows (same disk). So the
local Docker-free harness ([[ivm-bench-batch1-perf-gap]]) reads data at `D:\ivm-data\...` that
Curt copies from `~/ivm-bench/mount/raw/3/delta` via `sudo cp -r ... /mnt/d/ivm-data/...`.
Windows can also reach WSL files via `\\wsl.localhost\<distro>\home\curt\...` if a copy is
undesirable. dbsp-net local harness + `dotnet` run on the Windows side (D:\src\dbsp-net).

Related: [[docker-runs-in-wsl]], [[ivm-bench-arc]], [[ivm-bench-batch1-perf-gap]].
