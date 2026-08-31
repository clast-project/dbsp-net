---
name: feldera-source-comparison
description: "Source-level comparison vs Feldera (2026-08-30) — one LSM trace, no hash map on the row path, background merging; which of our decisions it challenges"
metadata: 
  node_type: memory
  type: project
  originSessionId: 76b7e6c9-eb62-4996-8dd2-e02c5fd00969
  modified: 2026-08-31T02:18:19.691Z
---

`docs/comparison-feldera-decisions.md` (2026-08-30), from four research agents reading Feldera
`78afc9077`. Raw reports in `docs/research-feldera/`.

**The result in one line:** Feldera's only trace is a sorted, immutable, file-backed LSM and merging
runs off the step thread — and most differences, including the per-row gap, fall out of that.

- **No hash map anywhere on the row path.** Trie-layered batches, merge/gallop joins, sorted-cursor
  group-by. `hash.rs` is 15 lines and exists only to shard at an exchange. So our ~50–60% fresh-dict
  alloc + ~40–48% whole-row hash are costs of a *hash-indexed Z-set*, not of DBSP. See
  [[per-row-execution-efficiency]], [[repr-execution-apportionment]].
- **`Spine::exert` is empty** — W background merger threads. Our "+14% bulk step" for spine measured
  LSM-with-in-step-compaction vs a dictionary, which is not the experiment we thought. Most exposed
  decision: [[row-representation-design]] and the stop-growing-spine call.
- **They built Track B *and* Track A.** Checkpoint = manifest of already-durable batch files; lazy
  O(#files) recovery. Contradicts our "Track A amortises B away". See [[ivm-bench-arc]].
- **Codegen confirmed dead as a lever, from their side**: they erased their runtime deliberately
  (`dynamic.rs:11-17`) and keep it cheap with range-shaped dispatch — one vcall per *run*, not per
  tuple. That trick is portable to us and is the top steal.

**Why:** three arcs treated allocation/hashing as intrinsic per-row cost; they are consequences of one
representation choice we made early.

**How to apply:** don't reverse anything on this doc alone — §9 names the measurement that settles
each challenged decision. First real experiment: move our compaction off the step thread, then re-run
flat-vs-spine.
