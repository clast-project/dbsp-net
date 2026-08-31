---
name: spine-sql-integration
description: Ongoing work — wiring the spine trace family into the SQL compiler; phased plan and current status
metadata: 
  node_type: memory
  type: project
  originSessionId: b8e59bcd-7fbb-4293-8e10-b8e75ac20bc3
---

Picking the project back up (2026-05-24), the agreed highest-priority work item is **wiring the spine trace family into the SQL compiler**. The spine operators (`SpineDistinctOp`/`SpineIncrementalAggregateOp`/`SpineIncrementalJoinOp`/`SpineIncrementalLeftJoinOp`, with per-batch Arrow snapshot + disk spill) exist in `DbspNet.Core` but `PlanToCircuit` still emits the flat family. It's the README "What's next" #1 and unblocks waterline/compaction → LATENESS-driven trace GC (the real memory-bound goal).

Phased plan:
- **Phase 1 (DONE):** `StructuralRowComparer : IComparer<StructuralRow>` in `src/DbspNet.Core/Collections/` + 23 passing unit tests. This is the crux — spine traces sort keys/values but `StructuralRow` has no `IComparable`. Comparer is lexicographic, null-sorts-first, arity tiebreak, element compare via non-generic `IComparable` (mirrors the MIN/MAX `ComparableComparer` at `SqlAggregators.cs:366`). Verified invariant `Compare==0 ⟺ Equals` across long/double/bool/Utf8String/Decimal128.
- **Phase 2 (DONE):** added public `CompileOptions { TraceFamily Flat|Spine, ICompactionStrategy? Compaction }` + `TraceFamily` enum (`src/DbspNet.Sql/Compiler/CompileOptions.cs`); new 3rd optional arg on `PlanToCircuit.Compile`. Threaded through `CompileContext.Options`; the 6 stateful call sites now go through private `EmitDistinct`/`EmitInnerJoin`/`EmitLeftJoin`/`EmitAggregate` helpers that pick flat vs spine and pass `StructuralRowComparer.Instance`. Spine mode skips the typed fast path. `AttachScalarColumn` now takes `ctx`. 4 smoke tests in `tests/.../Sql/SpineCompileTests.cs` (DISTINCT-via-UNION, GROUP BY, INNER/LEFT JOIN) assert correct deltas + spine ops engaged + flat≡spine. Full suite 800 green.
- **Phase 3 (DONE):** `RandomQueryPbtTests.RandomQuery_SpineCircuitEqualsBatch` — 3000-iter PBT sweep, spine circuit vs batch oracle, **passes** (CheckOne now takes optional `CompileOptions`). `tests/.../Persistence/SpineSnapshotTests.cs` — 3 spine snapshot round-trip compositions (JOIN+GROUP BY, UNION+GROUP BY, LEFT JOIN+GROUP BY, covering all 4 spine ops) + 2 flat↔spine cross-load rejection tests (plan fingerprint uses `op.GetType().FullName` at `SnapshotManifest.cs:86`, so Spine* vs flat differ → `InvalidDataException` "plan fingerprint mismatch"). Full suite 806 green.
- **Phase 4 (DONE):** docs synced — README ("What works today" spine bullet + "What's next" now leads with "Spine on the typed-row fast path"), ARCHITECTURE.md spine-variants note, docs/persistence.md (opening, staging, state table, "What ships in (D)"), docs/design-notes.md, docs/skipped.md Runtime item all updated to "emitted via `CompileOptions { TraceFamily = Spine }`". `docs/benchmarks.md` left alone (auto-generated; no stale prose).

**STATUS: spine→SQL integration COMPLETE and pushed to origin/main.** Commits: `6ba2f52` (integration, Phases 1-4, 806 tests green) and `0b01680` (flat-vs-spine SQL benchmark `RunJoinedGroupBySpineBenchmark` in DbspNet.Benchmarks/Program.cs + regenerated docs/benchmarks.md). Benchmark finding: spine is ~1.2-1.6× slower than flat on per-step incremental for the Joined GROUP BY composition at N=100-100k (per-batch bloom+binsearch probe vs flat dict lookup), and 1.2-2.3× slower on cold batch (sort-on-integrate) — the spine's win is checkpointing/spill, not raw speed at these sizes.

Remaining follow-ups (tracked [P1] in docs/skipped.md, NOT this effort): (1) spine on the typed-row fast path — needs generated per-schema comparers for emitted structs; (2) `RecursiveCteOp` has no spine sibling; (3) the actual memory-bound payoff still needs frontier/waterline + LATENESS (the next roadmap item this unblocked).

Key decisions: **v1 = structural path only** (typed+spine deferred); `RecursiveCteOp` stays flat (no spine sibling exists); persistence works unchanged (spine ops implement `ISnapshotable` with the same `IZSetTraceCodec`/`IIndexedZSetTraceCodec`; flat vs spine = distinct plan fingerprint, so cross-load is correctly rejected). See [[dbspnet-overview]].
