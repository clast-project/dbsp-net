---
name: residual-pushdown-next
description: "DONE — join residual pushdown shipped on the structural path so temporal SCD joins don't materialise the full cross product (SF=3 OOM fix)"
metadata: 
  node_type: memory
  type: project
  originSessionId: d04e1240-137c-49b7-b437-e6290184b5c5
---

**DONE + pushed (2026-07-17):** dbsp-net main `357dd6f`, ivm-bench pin bumped to it
(`dbspnet-engine` branch commit `709ecab`; both Dockerfile + compose default). Awaiting
Curt's SF=3 re-run to confirm no OOM, then the full 3-batch `benchmark.sh` vs Feldera.

**What the task actually was (corrected the original design):** the operator-level residual
was ALREADY built in commit `d93db1d` (for the TYPED path, Nexmark q4's cross-cutting WHERE) —
`IncrementalJoinOp`/`IncrementalJoinSharedRightOp` carry an optional `Func<TOut,bool>? residual`
threaded into `IncrementalJoinCore.JoinInto` (`if (residual is null || residual(outRow)) add`).
So the fix was NOT the "combine returns null" design in the old note — it was WIRING the
STRUCTURAL path (`PlanToCircuit`, the ivm-bench `CompileProgram` path), which still built the
full equi product and post-filtered. Only `PlanToCircuit.cs` changed (+59/−26).

**As-built:**
- `EmitInnerJoin` / `EmitSharedRightInnerJoin` gained an optional `residual` param: flat path
  passes it to `builder.IncrementalInnerJoin(...)` (pushdown); SPINE has no residual hook so it
  post-filters internally (`builder.Filter` after the spine join) — unchanged behaviour, and the
  benchmark's SCD joins are structural/flat so the OOM path is the fixed one.
- `CompileInnerJoin`: build `residualFn` from `plan.Residual` (`CompilePredicate`, NULL→false),
  pass to both emit calls, DROP the trailing downstream `Filter`.
- `CompileOuterJoinWithResidual` (LEFT/RIGHT/FULL): push residual into the `matched` inner join
  (`CompileScalar` + `is true`, matching the old predicate exactly), `matched = joined`, drop the
  Filter. `matched` is the SAME set (σ_residual(L ⋈ R)), so the `UnmatchedPreservedRows`
  anti-joins and outer-join match-presence semantics are unchanged. GC unaffected — key retention
  doesn't depend on which output rows survive the residual.
- Do NOT touch builder signatures (the residual delegate type is unchanged — a separate
  `Func<TOut,bool>?`, not a combine parameter) → [[typed-compiler-reflection-gotcha]] avoided.

**Tests** (`tests/DbspNet.Tests/Sql/JoinResidualPushdownTests.cs`): behavioural — the join op
emits only the 5 survivors of a 20×5 single-key product, NOT the full 100 (proves no
materialisation), verified via `CollectStats().LastOutputRows` + `plan.Residual != null` assert;
plus incremental≡batch-oracle differential over signed streams (well-formed insert/retract) for
INNER, LEFT and FULL residual joins where the per-key product dwarfs the result. Full suite 2005
(4 new). Mutation-proven at the `JoinInto` chokepoint.

**ENV LANDMINE:** `DbspNet.Sql.dll` incremental build is FLAKY in this WSL/Win checkout — mutation
tests gave self-contradictory results (inner passed / outer failed under the same mutation) until a
full `rm -rf */bin */obj` + rebuild. When mutation-testing, force a clean rebuild or the stale dll
lies. The definitive mutation was at `IncrementalJoinCore.JoinInto` (Core project, forced clean).

Related: [[ivm-bench-arc]], [[typed-compiler-reflection-gotcha]], [[join-completeness-next]]
