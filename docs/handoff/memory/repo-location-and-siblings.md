---
name: repo-location-and-siblings
description: "dbsp-net moved out of iCloud-synced ~/Documents/GitHub to ~/src/dbsp-net (2026-08-31); where the sibling repos are and what the move does NOT carry"
metadata:
  type: project
---

**Moved 2026-08-31: `~/Documents/GitHub/dbsp-net` → `~/src/dbsp-net`.** The old path was the GitHub
Desktop default and sat inside iCloud-synced Documents, which actively corrupts builds
([[icloud-conflict-copies-break-builds]]).

**Project memory is keyed by the absolute path** — `~/.claude/projects/<path with / replaced by ->/memory/`.
A move therefore orphans it silently: the new session derives a different directory, finds it empty,
and starts blank. This directory was pre-seeded at `-Users-curthagenlocher-src-dbsp-net` before the
move, and `docs/handoff/memory/` in the repo is the path-independent backup (restore procedure in its
README).

**The siblings did NOT move** (unless separately relocated) and are still under
`~/Documents/GitHub/`: **feldera** (reference only), **ivm-bench** (branch `dbsp-engine`, remote
`mdrakiburrahman/ivm-bench` — see [[ivm-bench-repo-topology]]). So `../feldera` and `../ivm-bench`
relative to dbsp-net are **no longer siblings**. `scripts/compare-nexmark.sh` was fixed at the same
time: it now prefers `$REPO/../feldera`, falls back to `$HOME/Documents/GitHub/feldera`, and still
honours `FELDERA_DIR=` / `--feldera=PATH`. Its old default hard-coded a `/Users/curt` home that never
existed on this machine.

**How to apply:** when a path in an older memory says `~/Documents/GitHub/dbsp-net`, read it as
`~/src/dbsp-net`; sibling repo paths in older memories are still correct. Verify a path before quoting
it. If the repo moves again, pre-seed the new memory directory *before* restarting the session.
