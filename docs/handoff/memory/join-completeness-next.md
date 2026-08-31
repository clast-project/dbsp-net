---
name: join-completeness-next
description: "DONE — join completeness (CROSS/non-equi INNER JOIN + FULL OUTER JOIN) shipped to main; deferred follow-ons noted"
metadata: 
  node_type: memory
  type: project
  originSessionId: ce43642b-0378-4fed-8531-a129b64b0fd0
---

**DONE** (shipped to main, two commits, all tests green):

1. **CROSS / non-equi INNER JOIN** (`feat: CROSS JOIN / non-equi INNER JOIN`).
   A zero-equi-key INNER join builds a `JoinPlan` with empty `EquiKeys` + the
   whole `ON` as `Residual`; both compiler paths route the two sides through a
   single unit (`Schema.Empty`) key, so the existing `IncrementalJoinOp` yields
   the full cross product and the residual filters it — **no new operator**.
   Added the `CROSS JOIN` keyword (`Cross` token → `INNER JOIN ... ON TRUE`).
   Typed path reuses the `Schema.Empty` unit-key broadcast pattern already used
   by `CompileScalarSubqueryJoin`. Outer joins still require an equi-key.

2. **FULL OUTER JOIN** (`feat: FULL OUTER JOIN`). New `IncrementalFullJoinOp`
   (+ spine sibling `SpineIncrementalFullJoinOp`) = LEFT-join case analysis
   (inner + left-pad, keyed on right-presence) **plus** a symmetric right-pad
   pass keyed on left-presence. Independent decompositions → both-side match
   flips compose to the exact per-key delta. `Full` token + `JoinType.FullOuter`;
   resolver makes both sides nullable; `FULL ... USING (c)` merges via
   `COALESCE(left, right)`. Structural `CompileFullOuterJoin` (3-way union,
   NULL-key bypass); typed FullOuter arm (nullable keys → structural fallback,
   so typed only handles non-null keys); `MonotonicityAnalyzer` FullOuter case
   emits no output monotonicity (GC still works off input-side sources);
   `BatchPlanEvaluator.BatchFullOuterJoin` oracle for the PBT.

Both have operator unit tests, end-to-end (structural+typed+spine), PBT shapes,
and (FULL) snapshot round-trip. See [[works-directly-on-main]].

**Deferred follow-ons** (in `docs/skipped.md`):
- `NATURAL JOIN` (P1); comma-join `FROM a, b` (P2).
- Outer joins (LEFT/RIGHT/FULL) with a **non-equi residual conjunct** in `ON`
  (still rejected — needs residual-aware operator logic that retains the
  preserved row NULL-padded when the residual fails).
- Typed-path keyless inner falls back to structural only if the residual
  predicate is outside typed scope (otherwise it's typed).

**Next candidate from `docs/skipped.md`** for TPC-H: `ORDER BY` / `LIMIT` /
`OFFSET` (P1, incremental TopK), or `LIKE` (P1). Plan-first in a fresh session.
