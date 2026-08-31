---
name: operator-fusion
description: "DONE — circuit-level fusion of consecutive Filter/Project nodes into one Apply pass, both compile paths, on main"
metadata: 
  node_type: memory
  type: project
  originSessionId: 90d57bf2-ca65-4fd1-a3ac-277b917ebcc4
---

DONE (on main): circuit-level operator fusion — the roadmap-candidates #4 item
(see [[roadmap-candidates]]). A maximal run of consecutive `FilterPlan` /
`ProjectPlan` nodes now lowers to a **single** fused `Apply` pass instead of one
operator per node, eliminating the intermediate Z-set (one allocation + one full
iteration) materialized between every adjacent pointwise stage. Full suite 1514
pass / 1 skip.

**Design — compile-time lowering, NOT a new plan node / optimizer rule.** Fusion
is about not materializing intermediate Z-sets between pointwise ops, which is a
lowering concern, so it lives in the compilers, not the optimizer. This avoids
the usual new-node walker churn (TypedPlanCompiler / MonotonicityAnalyzer /
BatchPlanEvaluator / CollectScans / optimizer arms all untouched); the plan tree
stays canonical; and it works whether or not `PlanOptimizer` ran. Implemented on
**both** compile paths.

**Soundness:** map and filter are pure row functions, and accumulating into one
`ZSetBuilder` matches staged `MapKeys`/`Filter` because Z-set addition is
associative/commutative (two inputs the projection collapses to the same output
key add their weights either way; a filter's verdict is identical for both since
it's a pure function of the post-map row). Single-stage chains keep their exact
prior operator (`Filter`/`MapRows`) so common single-op queries are byte-for-byte
unchanged; only genuine multi-stage chains fuse.

**Core operator:** new `LinearOperators.MapFilterRows<TIn,TOut,TWeight>(input,
Func<TIn,(bool Keep,TOut Value)> step)` — one pass, `Keep==false` drops the row.
The tuple drop-flag (not a `null` sentinel) is deliberate: the typed path's rows
are **value-type emitted structs**, which can't carry `null` — so the universal
signature serves both paths.

**Structural path (`PlanToCircuit`):** `CompileLinearChain` replaces the old
`FilterPlan`/`ProjectPlan` switch arms (and the deleted `CompileProjection`).
Collects stages top→down (identity projections — same-arity sequential
`ResolvedColumn`s — drop out as no-ops via `IsIdentityProjection`), compiles the
chain base once, reverses to data-flow order, and folds into one
`MapFilterRows<StructuralRow,StructuralRow,Z64>`. Each map stage reuses the
`ExpressionCompiler.CompileScalar` delegates + `codec.BuildRow`; each filter uses
`CompilePredicate`. Per-stage delegates read by index from the row the previous
stage produced — correct because each plan node's expressions are resolved
against exactly its input's schema.

**Typed path (`TypedPlanCompiler`):** the harder half (rows are reflected,
strongly-typed structs). `CompileLinearChain` collects the same chain, reuses the
EXACT per-stage delegate builders (`BuildTypedPredicateDelegate` /
`BuildTypedProjectionDelegate`), so fusion succeeds iff each un-fused stage would
have — any stage outside typed scope returns null and the whole compile falls
back to structural, same as before (no regression). `BuildFusedTypedDelegate`
emits ONE `Expression` block: filters short-circuit to `(false, default)` via a
`LabelTarget`; maps assign the projected row into a local that the next stage
reads. The pre-compiled per-stage typed delegates are embedded as
`Expression.Constant` and `Expression.Invoke`d, so each stage stays strongly
typed (NO boxing of the row structs inside the fused delegate). New reflected
`InvokeMapFilterRows`. Note typed rows are structs ⇒ the fused output is
`ValueTuple<bool,TOutFinal>`, which is why `MapFilterRows` needed the tuple form.

**Tests:** `tests/.../Operators/LinearOperatorTests.cs` — 3 `MapFilterRows` tests
(map+drop, weight accumulation on colliding outputs, equivalence to a staged
Filter→MapRows chain via PBT). `tests/.../Sql/OperatorFusionTests.cs` — 4 ×
{Typed, Structural} theory cases: a 4-stage `Project(Filter(Project(Filter(Scan))))`
adds exactly ONE ApplyOp over a `SELECT *` baseline (would be four un-fused);
chain length doesn't change ApplyOp count; correctness under insert+delete (delta
semantics); colliding-projection weight accumulation. ApplyOp counts read via
internal `RootCircuit.Operators` (InternalsVisibleTo DbspNet.Tests). Structural
mode forced with `EmittedEqualityCodec.Instance` (non-default codec disables the
typed fast path, same lever as `CompileMode.Structural`). The optimized-vs-batch
PBT exercises both paths' fusion across random query shapes for free.

**Benchmark (commit after the feature):** `src/DbspNet.Benchmarks/FusionBenchmark.cs`
— head-to-head of a `map→filter→map` chain wired as 3 separate operators vs one
fused `MapFilterRows`, over StructuralRows, reporting per-step latency AND
bytes-allocated-per-step (via `GC.GetAllocatedBytesForCurrentThread`). Measured
**~2–4.5× per-step speedup and a steady ~72% allocation drop**, flat across
N=100..100k (the fused pass allocates one output Z-set + a row only for
survivors, vs an intermediate Z-set per stage + a row for every input at the
first map). Wired into `Program.cs`; regenerates `docs/benchmarks.md`.

**Deferred / not done:** operator fusion across NON-pointwise boundaries (none —
joins/aggregates/distinct stay separate by nature); the optimizer still doesn't
do general top-down column liveness / constant folding / join reordering (see
`docs/skipped.md`, unrelated to fusion).
