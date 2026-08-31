---
name: dbspnet-overview
description: What DbspNet is and where the canonical docs/roadmap live
metadata: 
  node_type: memory
  type: project
  originSessionId: b8e59bcd-7fbb-4293-8e10-b8e75ac20bc3
---

DbspNet (`D:\src\dbsp-net`) is a research-grade C#/.NET 10 reimplementation of the DBSP incremental-computation model (VLDB 2023 paper; Feldera-inspired), written from the paper not ported from Rust. Prototype targeting a slice of SQL end-to-end. Solo project, Curt Hagenlocher; on branch `main`.

The docs are unusually complete and kept in sync — trust them as the source of truth for state and roadmap:
- `README.md` — what works / what's deferred / "What's next".
- `ARCHITECTURE.md` — pipeline, operator catalog, extension points.
- `docs/skipped.md` — deferred features tracked vs Feldera, prioritized P1/P2/P3.
- `docs/persistence.md`, `docs/design-notes.md`, `docs/benchmarks.md`.

Pipeline: SQL text → Parser → Resolver → LogicalPlan → (PlanOptimizer, opt-in) → PlanToCircuit → CompiledQuery. Two compile paths: typed fast path (`TypedPlanCompiler`, tried first) and structural fallback (`ZSet<StructuralRow,Z64>`). 770+ unit tests + CsCheck PBT (the "incremental ≡ batch" test of record in `EndToEnd/RandomQueryPbtTests.cs`).

Current active effort: [[spine-sql-integration]].
