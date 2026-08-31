---
name: repo-location-and-siblings
description: "All three repos moved out of iCloud-synced ~/Documents/GitHub to ~/src (2026-08-31); dbsp-net, feldera and ivm-bench stay siblings"
metadata:
  type: project
---

**Moved 2026-08-31, out of `~/Documents/GitHub/` into `~/src/`:** `dbsp-net`, `feldera` and
`ivm-bench` all together. The old location was the GitHub Desktop default and sat inside iCloud-synced
Documents, which actively corrupts builds ([[icloud-conflict-copies-break-builds]]) — so it was a bad
home for ivm-bench too, which is also a build tree.

**They remain siblings**, so `../feldera` and `../ivm-bench` relative to dbsp-net still resolve.
`scripts/compare-nexmark.sh` was made location-independent at the same time: it prefers
`$REPO/../feldera`, falls back to `$HOME/Documents/GitHub/feldera` for the old layout, and still
honours `FELDERA_DIR=` / `--feldera=PATH`. Its previous default hard-coded a `/Users/curt` home that
never existed on this machine. Neither repo contains any other absolute path to the other.

**Project memory is keyed by the absolute path** — `~/.claude/projects/<path with / replaced by ->/memory/`
— so a move silently orphans it: the new session derives a different directory, finds it empty, and
starts blank. dbsp-net's memory was pre-seeded at `-Users-curthagenlocher-src-dbsp-net` before the
move. ivm-bench's own project memory directory was **empty**, so nothing needed migrating there.
Session transcripts are keyed the same way and were *not* migrated, so `--resume`/`--continue` will
not reach pre-move sessions.

`docs/handoff/memory/` in the repo is the path-independent backup (restore procedure in its README).

**How to apply:** when an older memory says `~/Documents/GitHub/<repo>`, read it as `~/src/<repo>` —
this applies to sibling paths too, unlike the first version of this note. Verify a path before
quoting it. If anything moves again, pre-seed the new memory directory *before* restarting the
session.
