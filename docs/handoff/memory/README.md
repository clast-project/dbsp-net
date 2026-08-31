# Project memory snapshot (point-in-time)

Claude Code keeps project memory **outside git**, in a directory named after the project's absolute
path: `~/.claude/projects/<absolute path with / replaced by ->/memory/`. Because the name is derived
from the path, **moving or renaming the repo silently orphans the memory** — a new session derives a
different directory, finds it empty, and starts with no context.

This directory is the path-independent backup. Refreshed **2026-08-31**, before moving the repo out of
the iCloud-synced `~/Documents/GitHub/` (see `icloud-conflict-copies-break-builds.md` for why that
location is hostile to builds).

**To restore:** copy the `*.md` files — including `MEMORY.md`, the index, but not this README — into
the new machine's or path's memory directory. For `~/src/dbsp-net` that is
`~/.claude/projects/-Users-curthagenlocher-src-dbsp-net/memory/`.

**After restoring, this copy is stale.** Do not edit it and do not treat it as a second source of
truth. The authoritative record of decisions is `docs/`; live memory is whatever the active project
directory holds.
