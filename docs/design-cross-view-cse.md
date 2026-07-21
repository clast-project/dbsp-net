# Design: cross-view common-subexpression elimination

Status: **design-only; gated on the ivm-bench batch-1 `IVM_SPEC` + SF=3 data (not
present on the current machine — see the memory note
`columnar-batch1-tooling-portability`). Measurement-first; retire-if-absent.**

Follow-on #3 to the structural CSE pass (`src/DbspNet.Sql/Optimizer/PlanCse.cs`,
commits `c91fc9b` / `d20973a` / `35527d6`). Intra-view CSE shares duplicated
sub-expressions *within one view*; this doc scopes sharing them *across views* in a
multi-view program (`PlanToCircuit.CompileProgram`), the shape the 50-view batch-1
dbt program is.

## 0. TL;DR

- **What already works.** A *named* view read by several views computes once: the
  program compiles views in dependency order into a shared `name → stream` dict, and
  a scan of a view name wires to that view's already-built stream
  (`PlanToCircuit.cs:288-291,363-364`). Intra-view CSE (`PlanCse`) collapses
  duplication *inside* a single view before it compiles.
- **The gap.** Two *different* views that each spell the **same sub-expression**
  (not factored into a shared view) compile it twice. Per-view `Optimize`
  (`PlanToCircuit.cs:334`) runs `PlanCse` per view, and each view gets a **fresh
  `CompileContext`** (`PlanToCircuit.cs:336`) — so neither the interner nor the
  per-reference compile memo spans view boundaries.
- **The design.** A program-level pre-pass — `PlanProgramCse` — interns
  structurally-identical sub-expressions across *all* views, and **hoists** each
  sub-expression shared by ≥2 views into a synthesic anonymous view, rewriting its
  consumers to *scan* that view. This reuses the existing named-view sharing
  machinery wholesale: a synthetic view is just another `ProgramView`; the
  `streams` dict makes it compile once and both consumers read its stream. No change
  to `CompileContext`, the compile memo, or the runtime.
- **Why it might matter for batch-1, and why it's unmeasurable here.** dbt programs
  routinely restate the same source CTE/subquery across models; batch-1's 50 views
  are a prime candidate. But *whether* batch-1 actually has heavy cross-view
  duplication — and its allocation weight — cannot be measured without the deploy
  spec. **The first task is measurement, not code** (§7).

## 1. What exists today (baseline, with file refs)

`CompileProgram` (`PlanToCircuit.cs:227`) compiles a list of `ProgramView(ViewName,
Query, IsOutput)` in dependency order:

1. **Dead-view elimination** (`:248-264`): only views reachable from an output are
   built.
2. **Program-level column liveness** (`:269-271`, opt-in): per-view live output
   columns; `PruneDeadColumns` narrows each view's plan to what live consumers read
   (`:331`).
3. **Per-view compile loop** (`:308-372`):
   - `Optimize(viewQuery)` (`:334`) — filter pushdown, projection fusion, and (now)
     `PlanCse` — **per view**.
   - **A fresh `CompileContext ctx`** (`:336`) — so `ctx.PlanCache` (the
     per-reference compile memo) and `ctx.CteCache` are **per view**.
   - Compile to a stream; register `streams[ViewName] = stream` (`:363-364`).
   - A later view that scans `ViewName` resolves to that stream (`:288-291`) — this
     is the *named-view* sharing that already works.

**Related prior art:** arrangement sharing (`options.ShareArrangements`,
`CollectShareableArrangements` at `:1003`, `ArrangementCache` at `:464`) shares join
index arrangements — but it is **single-query only** (the program path passes
`emptyShareable`, `:337`) and intra-plan. It is a precedent for "find shareable
structure with a pre-pass + serve from a cache," not a cross-view mechanism.

## 2. The gap

Consider two batch-1 views that both read the same windowed bid count (the q5 shape,
but split across two dbt models):

```sql
CREATE VIEW hot_items   AS SELECT ... FROM (<HOP(bid) GROUP BY auction,window>) ...;
CREATE VIEW window_stats AS SELECT ... FROM (<HOP(bid) GROUP BY auction,window>) ...;
```

The `<…>` sub-expression is identical text in both, but it is **not a named view**.
Each view is a separate `ProgramView.Query`; per-view `Optimize` + per-view `ctx`
mean the fan-out + aggregate compiles **twice**, once per view. Intra-view CSE never
sees the cross-view duplication because it runs *within* each view.

This is exactly the intra-view q5 problem (commit `c91fc9b`) lifted to the program
level. The fix mechanism is the same idea — share the identical subplan — but the
sharing point is a view boundary, not an operator boundary.

## 3. Design: hoist shared sub-expressions into synthetic views

The key realization: **named-view sharing already does what we want.** So instead of
threading a program-wide interner/memo through the compiler, *rewrite the program*
so the shared sub-expression becomes a named (synthetic) view, and let the existing
`streams`-dict machinery share it.

`PlanProgramCse.Rewrite(views) → views'`:

1. **Intern across views.** Walk every view's `Query` into one shared interner
   (the existing `PlanCse` hash-consing, lifted to run over multiple roots). Two
   structurally-identical sub-expressions in different views become the *same
   instance*. `PlanCse`'s equality already compares `ScanPlan` by table **name**, so
   a `bid` scan in view A and an identical one in view B are genuinely the same
   computation — this is sound across views without change.
2. **Count references.** For each interned instance, count how many **distinct
   views** reference it (a subplan used twice *inside one* view is already handled by
   intra-view CSE; cross-view hoisting targets instances spanning ≥2 views).
3. **Gate** (§6): keep only instances worth hoisting — non-trivial (contains a
   stateful op: join / aggregate / window / top-k), not a bare scan or cheap filter,
   and not inside a recursive CTE.
4. **Hoist.** For each surviving instance `S`:
   - Synthesize `ProgramView("__cse_<n>", S, IsOutput: false)`.
   - Replace every occurrence of `S` (across all views) with
     `ScanPlan("__cse_<n>", S.Schema)` — the same node kind a view reference already
     is. `S.Schema` becomes the synthetic view's output schema; because the scan
     preserves column order, consumers' positional `ResolvedColumn` indices into
     `S`'s output stay valid unchanged.
   - Insert `__cse_<n>` into `views` **before** its first consumer (dependency
     order is already required and already relied upon).
5. Return the augmented `views'` (synthetic views prepended, consumers rewritten).

`CompileProgram` then proceeds unchanged: the synthetic view compiles once into
`streams["__cse_<n>"]`; every consumer's `ScanPlan("__cse_<n>")` wires to that one
stream. **One sub-circuit, N consumers** — identical to a hand-factored CTE/view.

Blast radius: one new pass + one call site (insert `views = PlanProgramCse.Rewrite(
views)` at the top of `CompileProgram`, before dead-view/liveness). No change to
`CompileContext`, `CompilePlan`, `TypedPlanCompiler`, or any operator.

## 4. Ordering vs the existing passes

Run `PlanProgramCse` **before** dead-view elimination and liveness (`:248-271`), on
the **raw** view plans (pre per-view `Optimize`):

- **Before dead-view elim:** hoisting can only *add* reachability edges (a consumer
  now scans `__cse_n`); running dead-view elim afterward correctly keeps synthetic
  views that a live view references and drops any that end up unreferenced.
- **Before liveness:** the synthetic view is then a normal input to
  `ComputeProgramLiveColumns` (`:270`), which will compute its live-out as the
  **union** of what all consumers read — exactly right for a shared view (a
  per-consumer narrowing would be unsound once the stream is shared). This falls out
  for free; no special union logic needed.
- **On raw plans (pre-Optimize):** matching identical *raw* sub-expressions is the
  most permissive point — before per-view pushdown/pruning can transform the same
  source pattern *differently* in two views and destroy the match. The shared
  sub-expression is then optimized **once**, inside its synthetic view's per-view
  `Optimize` (a synthetic view goes through the same `:334` loop). Trade-off: it
  loses per-consumer specialization of the shared part — acceptable and standard
  (a CTE has the same property).

## 5. Correctness constraints

- **Structural equality is already conservative** (`PlanCse`): unknown nodes fall
  back to record equality (never a false positive); names/qualifiers ignored
  (positional downstream); types + lateness compared. Lifting it to multiple roots
  changes nothing about soundness — it still only ever replaces a subplan with a
  structurally-equal one.
- **Recursive CTEs / `z⁻¹`:** exclude sub-expressions inside `RecursiveCtePlan`
  (already pass-through in `PlanCse`) — hoisting across a fixpoint back-edge is
  unsound. A synthetic view must itself be non-recursive.
- **Schema/index alignment:** the synthetic view's output schema is `S.Schema`
  verbatim (same column order), so every consuming parent's `ResolvedColumn.Index`
  into `S` remains valid against the scan. This is the one invariant a test must pin
  hard (a differential test, §8).
- **Column lateness / frontier:** a synthetic view inherits the program's current
  "unbounded but sound" cross-view frontier behavior (`:303-305` notes cross-view
  LATENESS wiring is already a deferred follow-on). No regression; it does not
  *improve* GC either. Out of scope.
- **Output identity:** synthetic views are never outputs; the 16-output
  byte-identical cross-check (the batch-1 gate) is the acceptance test — hoisting
  must not change any observable output.

## 6. Cost model & gating

Hoisting a sub-expression shared by N≥2 views replaces N computations with 1
computation + 1 materialized stream boundary. It is a win when

```
(N − 1) × cost(S)  >  materialize_boundary(S)
```

- **Always ≥ neutral for the stateful term:** computing a join/aggregate/window once
  instead of N times strictly removes (N−1) copies of its retained state + per-tick
  integrate. This is the term batch-1 cares about (allocation-bound).
- **The boundary cost** is the same `ZSet<StructuralRow>` re-materialization the
  columnar doc prices in `design-columnar-batch1.md` §7 #2. For a *stateful* shared
  `S` the boundary is negligible against the saved state; for a *cheap* `S` (bare
  scan, single filter/project) it can dominate — and scans already share via the
  `streams` dict / `CteCache`, so hoisting them is pure loss.
- **Gate:** hoist only when `S` contains ≥1 stateful operator (join, aggregate,
  window, top-k) and is referenced by ≥2 distinct views. Log every hoist and every
  *rejected* candidate (with reason) — no silent truncation, per house style.

## 7. Measurement first (before writing the pass)

The whole arc is unjustified if batch-1 has little cross-view duplication. Once
`IVM_DATA_ROOT` + `IVM_SPEC` are available, run — **as analysis only, no rewrite:**

1. **Cross-view duplication census.** Intern all 50 views' raw plans into one
   interner (a read-only `PlanProgramCse` in count-only mode) and report: how many
   interned sub-expressions are referenced by ≥2 distinct views, their node kinds,
   and their arity/width. Reuse the `nexmarkplan`-style operator/sharing dump.
2. **Allocation weight of the shared set.** Cross-reference the shared
   sub-expressions against the per-operator batch profiler (`DBSPNET_PROFILE`,
   commit `16e7d76`): what fraction of the 44.7 GiB batch-1 allocation flows through
   operators that a hoist would de-duplicate? This is the ceiling, analogous to the
   columnar doc's −38% microbench gate.
3. **Boundary price on a real shared view.** For the single highest-weight shared
   `S`, hand-hoist it (author a `WITH`/view by hand in the spec) and measure the
   before/after batch allocation + wall via the local `IvmBatchProfile` gate (16
   outputs byte-identical). This validates the cost model on real data before
   generalizing — the same retire-if-loses discipline as every prior increment.

If (1) finds few cross-view dups or (2) shows a small ceiling, **retire here** with
the number recorded. The mechanism is cheap to build but worthless without the
duplication to feed it.

## 8. Smallest first increment (once measurement clears the gate)

- Implement `PlanProgramCse.Rewrite` for the **stateful-only, ≥2-view** case, behind
  a `CompileOptions.ShareCrossViewSubplans` flag (default off), inserted at the top
  of `CompileProgram`.
- **Differential test** (the primary gate): compile the batch-1 program with the
  flag off vs on; assert **all 16 outputs byte-identical** on the real workload, and
  every unit-test program's outputs unchanged. Add a random-*program* PBT analog of
  `PlanCsePbtTests`: generate a small multi-view program that restates a subquery in
  two views, assert flag-on ≡ flag-off output, and assert the synthetic view fires
  (a `MemoHits`-style count of hoists > 0).
- Gate on `IvmBatchProfile`: alloc drop toward the §7 ceiling, no wall regression.
- Retire-if-loses.

## 9. Risks / open questions

- **Does batch-1 actually duplicate across views?** The load-bearing unknown.
  Unmeasurable without the spec. Everything else is contingent on §7.1 being nonzero.
- **Raw-plan matching vs optimized-plan matching.** §4 argues for raw (more matches,
  shared part optimized once). If measurement shows the interesting duplication only
  emerges *after* per-view optimize (unlikely for dbt-restated subqueries), the pass
  would need to run post-Optimize on a shared interner + a program-spanning compile
  memo instead of hoisting — a larger change (touches `CompileContext` scoping).
  Decide from the census.
- **Interaction with the typed fast path.** A synthetic view compiles through the
  same per-view typed/structural selection (`:350-363`); its output is a
  `StructuralRow` stream regardless, so consumers are unaffected. Should just work,
  but the differential test covers it.
- **Debuggability/observability.** Synthetic views need stable names + a
  `BuildLabel` (`:340`) so the per-operator profiler attributes their cost sensibly
  ("__cse_0" rather than an opaque blank).

## 10. Honest bottom line

Cross-view CSE is **architecturally clean** — it reduces to synthesizing anonymous
views and reusing the named-view sharing the program compiler already has, with a
one-call-site blast radius and no runtime change. There are **no showstoppers**: the
sharing machinery, conservative structural equality, and dependency-ordered view
compilation all already exist. The single load-bearing unknown is **whether batch-1
carries enough cross-view duplication to be worth it**, which is measurable only with
the deploy spec. So this is a *measurement-gated* arc: run the §7 census + ceiling
first; build §8 only if the number justifies it; retire with the honest figure if
not.
