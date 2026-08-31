---
name: nested-circuit-fixpoint
description: Recursion now compiles to a reusable Core nested-circuit (fixpoint) primitive; RecursiveCteOp deleted; stages 2-3 remain
metadata: 
  node_type: memory
  type: project
  originSessionId: a6f13fdb-c199-418e-a78d-5b234a649cef
---

Recursive CTEs (`WITH RECURSIVE`) were rewired off the bespoke `RecursiveCteOp`
(which looped over `BatchPlanEvaluator`) onto a new reusable Core primitive in
`src/DbspNet.Core/Operators/Nested/`:

- `FixpointOperator<TRow>` — one outer operator that integrates imported deltas
  (δ₀), drives a body sub-graph to a least fixpoint on an inner clock, exports
  the per-tick delta; `ISnapshotable` (persists import traces + previous-tick
  fixpoint).
- `NestedScopeBuilder<TRow>` — wires the body from stateless Z-set ops
  (`Import`/`Filter`/`Map`/`Union`/`Distinct`/`Join`).
- `CircuitBuilder.Fixpoint(...)` extension (`NestedOperators.cs`).

SQL wiring: `PlanToCircuit.CompileRecursiveCteFixpoint` + `CompileRecursiveBody`
compile the v1 body subset (Scan→Import, self-ref→feedback, Filter/Project/
Inner-Join/UnionAll, trailing Distinct) into the scope; both the structural and
typed paths share it. `RecursiveCteOp.cs` is **deleted**. `BatchPlanEvaluator`
stays (test oracle + non-recursive lazy CTEs only).

**Stage 1 (done):** generic naive `Fixpoint` primitive — `R₀=∅; Rₙ₊₁=body(Rₙ)`,
recompute-per-tick. Kept for non-linear bodies + the Core transitive-closure
test; no longer the SQL path.

**Stage 2 (done):** `SemiNaiveFixpointOperator` + `CircuitBuilder.SemiNaiveFixpoint`
— for linear recursion `R = distinct(base ∪ step(R))`. Body returns base + step
separately; operator preserves `R` across ticks and extends it semi-naively on
insert-only ticks (δ-pass with imports=ΔI then iterate with imports=I, feeding
only the frontier back through `step`), recompute-fallback on any retraction
tick. This is the old `RecursiveCteOp` algorithm re-expressed on wired operators.
SQL recursion now uses this. Correctness net: `RecursiveCtePbtTests` — random
insert/delete tick sequences vs batch TC after every tick (1000 iters). All
1279 tests pass.

**Stage 3 (done):** DRED retraction in `SemiNaiveFixpointOperator` — tick is
delete-then-insert; deletes do over-delete (R-tuples whose derivation used a
deleted input, propagated through I_old) + re-derive (still reachable from
survivors via surviving edges). Correct under cycles + alternative paths.
Multiset input deltas (|weight|>1) keep a from-scratch recompute. Recursion is
now fully incremental for inserts AND deletes. PBT strengthened (delete-biased,
5-node graph, 2000 iters; stress-passed at 5000).

**Spine sibling (done):** `IImportTrace<TRow>` abstraction (flat + spine impls)
+ public `SpineImportConfig<TRow>`; `SemiNaiveFixpointOperator` is trace-family-
agnostic. In `TraceFamily.Spine` mode the import integrals use a `SpineZSetTrace`
(per-batch snapshot via `SpineSnapshot`); the loop body is stateless so only the
import has a trace. Flat snapshot format unchanged. Recursive PBT runs flat AND
spine; spine snapshot round-trip covered.

**Nothing left** — recursion is fully incremental (inserts + deletes) on both
trace families. Old-snapshot compatibility was explicitly dropped (prototype).

Commits: stages 1+2 → 833913a; stage 3 (DRED) → ecd47b2; spine → 38a1d90. All on
main. (The 833913a/ecd47b2 titles have a stray leading "@ " from a shell-quoting
slip; left as-is since pushed.)
