---
name: machine-migration-to-mac
description: Development moved off the Windows i9 to a Mac Mini (2026-08-30) — and why Apple Silicon cannot rebaseline our benchmarks
metadata: 
  node_type: memory
  type: project
  originSessionId: 76b7e6c9-eb62-4996-8dd2-e02c5fd00969
  modified: 2026-08-31T02:18:58.645Z
---

Development moved from the Windows i9-12900K box (`D:\src\dbsp-net`) to a Mac Mini, 2026-08-30. Full
detail in `docs/handoff-machine-migration.md`; project memory was snapshotted to
`docs/handoff/memory/` so it survives the move.

**The constraint that matters: the Mac cannot reproduce our measurement baseline.**

- Every number in `docs/` came from the i9 (8P+8E, ServerGC, Windows). W=24 experiments have no
  equivalent on Apple Silicon — the spine-substrate 1.4–2.5× loss, `flatagg`/`q4flat`, the
  exchange/scaling decomposition and the Nexmark W=14 snapshot become *unrepeatable*, not merely
  different.
- Unified memory moves **allocation cost specifically** — the exact term our per-row thesis turns on
  ([[per-row-execution-efficiency]]). Never compare old and new numbers directly, even at equal W.
- **RESOLVED 2026-08-30 on the Mac: the Feldera image has a native `linux/arm64` manifest.**
  `docker manifest inspect images.feldera.com/feldera/pipeline-manager:latest` lists both amd64 and
  arm64 (no auth needed). The compose file pins no `platform:`, but Docker will pull arm64 natively —
  so there is no amd64 emulation and comparative runs on the Mac are *architecturally* valid. The
  cross-machine caution above still stands in full: valid against each other, never against the i9.

**How to apply:** treat the Mac as the development/correctness machine and keep an x86 box for
measurement. If that is not possible, declare a new baseline explicitly and re-measure the few results
decisions actually rest on — do not quote i9 numbers alongside Mac numbers.

**Environment:** .NET 10 (the Mac has 10.0.300 at `~/.dotnet/dotnet`; the `dotnet` on PATH is 8.0.405 — see [[nexmark-feldera-benchmark-setup]]); Docker Desktop (no WSL in the story any more —
[[docker-runs-in-wsl]] is obsolete); EngineeredWood packages restore from nuget.org with no custom
feed.

**Left unresolved on the old box:** uncommitted engineered-wood work in WSL `~/ew`
(`chore/python-313`, 311 insertions across 14 files) behind a *local-path* remote — the only content
there not recoverable from any remote.

Related: [[ivm-bench-repo-topology]], [[feldera-source-comparison]].
