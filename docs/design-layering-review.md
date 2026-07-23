# Layering review

**Status: REVIEW, 2026-07-22.** Triggered by the incremental-persistence arc
(`docs/design-incremental-persistence.md`), which produced two defects in one session that both lived
in seams rather than in code. Timed deliberately *before* Track B, because Track B would deepen the
structure under review (durable batch identity, refcount/GC over shared files, more spine siblings).

Everything below is measured against the tree at `a0d4d89`. Where I am reporting an impression rather
than a measurement, it says so.

## 0. Scope, and what this review did not look at

Examined structurally: the assembly graph, both SQL compile paths, the two trace families, the
persistence layer, and the connector/server chain.

**Not examined in depth:** the parser and resolver (9.7k LOC), the optimizer's rule set (2.7k), and
the expression compilers' semantics. I looked at their size and position, not their internals. A
claim about them here is a claim about *where they sit*, not about whether they are good.

## 1. The graph, as it actually is

| assembly | LOC | depends on |
|---|--:|---|
| `DbspNet.Core` | 17661 | — |
| `DbspNet.Sql` | **36364** | Core |
| `DbspNet.Arrow` | 1195 | **Sql** |
| `DbspNet.Persistence` | 2789 | Sql, Arrow |
| `DbspNet.Connectors.Abstractions` | 1449 | Arrow, Persistence |
| `DbspNet.Connectors.EngineeredWood` | 543 | Connectors.Abstractions |
| `DbspNet.Server` | 285 | Connectors.* |

It is a clean chain with one inversion (§7). The striking number is that **`DbspNet.Sql` is twice
Core** and holds 50% of the codebase: parser (3.6k), resolver (6.0k in one file), plan (7.5k),
optimizer (2.7k), expressions (6.1k), and *two* compilers (15.4k). "Sql" is not a layer; it is four
layers in a trench coat.

## 2. Core's public/internal boundary is not a boundary

77 public types, 76 internal — and `DbspNet.Core` grants `InternalsVisibleTo` to `DbspNet.Sql`,
`DbspNet.Persistence`, `DbspNet.Benchmarks`, and `DbspNet.Tests`. That is every consumer that exists.

So half of Core's type surface is marked `internal` while being visible to everything that could
possibly want it. The keyword is documenting an intention ("this is not for you") that the build does
not enforce. `DbspNet.Sql` does the same to Persistence, Benchmarks, and Tests.

This is not harmless. It means there is **no mechanical answer to "is this a supported seam?"** — the
answer is social. When `IncrementalAggregateOp` (Core) needed to persist state produced by
`SqlAggregator` (Sql), nothing in the build objected to any of the shapes that would have been wrong;
the constraint that forced the right design was that Sql depends on Core and not the reverse, which
is an *assembly* fact, not a visibility one.

## 3. Two orthogonal duplication axes, and they multiply

**Axis 1 — typed vs structural compile path** (in `DbspNet.Sql`):

| structural | LOC | typed | LOC |
|---|--:|---|--:|
| `PlanToCircuit` | 3951 | `TypedPlanCompiler` | 4162 |
| `SqlAggregators` | 1236 | `TypedSqlAggregators` | 1134 |
| `ExpressionCompiler` | 904 | `TypedExpressionCompiler` | 953 |

~12.3k LOC in three matched pairs — a third of Sql.

**Axis 2 — flat vs spine trace family** (in `DbspNet.Core`): 5661 LOC flat, 5099 LOC spine ≈ 10.8k,
about 60% of Core.

These are not independent. The compiler chooses the family, so the real surface is
**compile-path × trace-family × operator kind**, and coverage is ragged:

- **8 of 14 stateful operators have no spine sibling**: `IntegrateOp`, `PartitionedOffsetOp`,
  `PartitionedRankOp`, `PartitionedTopKOp`, `PartitionedTopKNarrowOp`,
  `PartitionedWindowAggregateOp`, `TemporalFilterOp`, `TopKOp` (plus `RecursiveCteOp`). The
  persistence arc measured what that costs: on the real SF=3 program those gaps are 30% of snapshot
  bytes but **81% of the bytes a reference-manifest commit would still have to write**.
- The families are not merely parallel, they **differ in capability**. The typed compiler fuses a
  join residual on flat but not on spine, because "the spine join variant has no residual hook"
  (`TypedPlanCompiler.cs:886`) — so selecting spine silently drops an optimization.

Every new stateful operator, and every new capability on one, is a decision in a cross product rather
than a single implementation. That is the structural reason work in this area is slow.

## 4. The recurring bug shape: two implementations of "the same thing" that quietly disagree

Both defects this session were instances of one pattern — *a second code path that is supposed to be
equivalent to the first, with nothing enforcing it.*

- **§7.2 (real bug, shipped).** `IncrementalAggregateOp.LoadAsync` rebuilt per-group state by bulk
  folding, where the live path folded incrementally. Exact for associative accumulators, lossy for
  `double`. The operator then retracted a value the view never held. Nothing anywhere asserted
  "reload must be observationally equivalent to the live path".
  - Telling detail: `PartitionedWindowAggregateOp`, `PartitionedRankOp` and `PartitionedOffsetOp`
    are *fine* — because they reload by calling **the same function the live path calls**
    (`RecomputePartition` / `ComputeWindow`) over exactly-restored raw rows. The aggregate was the
    only one that reconstructed by a *different* process. The distinction is invisible in the type
    system and was invisible in review.
- **§7.3 (not a bug — my measurement was wrong).** Worth keeping in the review because the fix was
  the same shape: an invariant nobody had written down (differencing two large measurements requires
  comparable heap conditions).

The generalisable defect is the absence of a **conformance contract**. There is no shared harness
saying: for every (compile path, trace family, operator) triple, these must agree. Where such
harnesses exist they work — the random-query equivalence PBT covers spine against the batch oracle,
and `ParallelStructuralCompilerTests` covers serial ≡ parallel at W=1/2/4/8. The gap is that the
*persistence* dimension has no equivalent, which is exactly where the bug was.

## 5. "Identity" is answered three different ways

- Snapshot state is addressed **positionally**: `op-{i}` by circuit build order.
- Spine batch files are addressed **positionally**: `batch_{i}`, renumbered by compaction.
- `SpineBatch` has **no durable id at all**; disk spill invents one via a `_spillCounter`.

Each subsystem worked around the same missing concept differently, and the workaround is a
fingerprint that hard-fails (§10.3 of the structural-parallel doc). Fail-safe is the right default,
but it means CSE changing the operator count turns a resumable checkpoint into a full rebuild.

Track B needs durable batch identity. That is the same missing concept a third time, and it is the
reason to settle it before building Track B rather than after.

## 6. The typed path buys speed with 104 reflection sites

`TypedPlanCompiler` contains **104** `MakeGenericMethod` / `MakeGenericType` / `GetMethod` calls.
`PlanToCircuit` contains **zero**.

That is inherent to what it does — instantiating generic operators over per-schema struct types
discovered at compile time — but it has two consequences worth stating. Operator wiring on the typed
path is **not statically checked**: each site turns a type mismatch into a runtime failure. And it
makes the typed path markedly harder to refactor, which compounds §3, since it is the half of the
duplication that resists mechanical change.

Whether the typed path still earns this is an open question I cannot answer from here. The
structural-parallel arc closed much of the gap it was built to address (broadcast joins, exchange
fusion, >2× scaling on the hot paths), so the premise deserves re-measurement before more is invested
in either half.

## 7. `DbspNet.Arrow` points the wrong way, for two reasons only

`Arrow` depends on `Sql`, which is why Persistence must depend on both. The entire coupling is:

- `DbspNet.Sql.TypeSystem` — `SqlType`, for column conversion (`ArrowColumns`, `ArrowSchemaBridge`)
- `DbspNet.Sql.Compiler` — `TableInput` (15 uses), `CompiledQuery` (8), `CompileOptions` (1), all in
  the ingest extension methods (`ArrowExtensions`, `ArrowIpc`)

So `Arrow` is two things glued together: a **type-system-aware column codec**, which needs only
`SqlType`, and **ingest extensions for the compiler's handles**, which need `Sql`. Splitting those —
or moving `SqlType` below `Sql` — would let the codec half sit under `Sql` and make the graph a clean
chain. This is the cheapest item in the review and the only one that is nearly mechanical.

## 8. What I would actually do, ranked

1. **A persistence conformance harness** — **BUILT 2026-07-22**, and it immediately found a second
   instance of the §7.2 bug in `SpineIncrementalAggregateOp` that the original fix had missed.
   A mutation test (reverting the §7.2 fix and confirming the harness fails) was necessary to make it
   trustworthy: the first version passed *with the bug reintroduced*, because its tick driver used
   distinct row keys and so never produced the weight-coalesced trace entries the defect needs.
   (cheap, highest value). One parametrised suite asserting,
   for every stateful operator and every trace family: save → restore → step ≡ uninterrupted step,
   including value equality, not just shape. This is the harness whose absence caused §7.2, and it
   generalises the two one-off tests that arc produced (`FloatAggregateRestoreTests`,
   `AggregatorEmptyDeltaTests`). Do this regardless of every other decision here.
2. **Settle identity before Track B** (§5). One durable-id concept for operators and batches, rather
   than a third positional scheme. Track B needs it; the CSE fragility wants it.
3. **Extract `SqlType` (or split `Arrow`)** (§7). Mechanical, removes the one inversion.
4. **Decide the trace-family axis** (§3) — *with a measurement gate, not from this document.* Either
   complete spine (8 missing siblings) and retire flat, or accept flat as the persistence-relevant
   family and stop growing spine. The persistence arc says spine's checkpoint reuse is real (71–79%)
   but that it currently costs +16% step and a worse save, so it is not yet a win end to end. That
   number is the gate.
5. **Re-measure the typed path's premise** (§6) before investing in either half of axis 1.
6. **Decide what `internal` means in Core** (§2) — either narrow the `InternalsVisibleTo` set so the
   keyword carries information, or drop the pretence and make the real seam the assembly boundary.
   Low urgency, but it is why "is this a supported seam?" currently has no mechanical answer.

Items 1–3 are independent and can proceed immediately. Items 4–5 are measurement-gated and should not
be decided from architecture argument alone — the same discipline that made the persistence arc
produce a retraction instead of a bad build.

## 9. What this review is not

It is not a verdict on the parser, resolver, optimizer, or expression semantics (§0). It is not a
rewrite proposal: nothing here argues for collapsing an axis on aesthetic grounds, and the two
duplication axes may both be load-bearing. The claim is narrower — that the *cross product* is
currently unmanaged, that equivalence between parallel implementations is unenforced, and that both
defects this session came from those two facts.
