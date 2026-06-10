# DbspNet — pool reclaimability microbench

## Pool reclaimability microbench — delta Z-set dictionary reuse

Microbench for docs/design-row-representation.md §16.5 lever 1. The W=1 profile found the engine allocation-bound per tuple, with the dominant term being the fresh `Dictionary`-backed Z-set delta every stateful operator builds every tick (`new ZSetBuilder` → `Build()`). Lever 1 pools those dictionaries. This measures the **prize** (bytes/dict/tick reclaimed) across the delta sizes `D` that span q4/q18/q19's operating points.

- **fresh** — `new Dictionary<K,V>()` (bare `ZSetBuilder()`, no capacity), fill D entries, hand off, drop. *Today's path.*
- **presized** — `new Dictionary<K,V>(D)` (what `ZSetBuilder.From`'s capacity hint already buys when the source size is known).
- **pooled** — reuse one dictionary via `Clear()` (keeps backing arrays; a stable-size delta re-fills with ~zero allocation). *The lever's ceiling.*

`B/tick` is managed bytes allocated per built dictionary (`GC.GetAllocatedBytesForCurrentThread`); **reclaim** = fresh − pooled = the per-dict-per-tick prize. Multiply by the number of delta-producing operators in a query (~5–10 for q4) for the per-tick total.

Host: .NET 10.0.9, 24 cores.

### long → long (narrow agg key, e.g. q4 outer group / q18 dedup weight)

| D (entries) | fresh B/tick | presized B/tick | pooled B/tick | reclaim (fresh−pooled) | reclaim % |
|---:|---:|---:|---:|---:|---:|
| 1 | 216 | 216 | 0 | 216 | 100% |
| 16 | 992 | 608 | 0 | 992 | 100% |
| 256 | 22312 | 8336 | 0 | 22312 | 100% |
| 1,024 | 102216 | 31016 | 0 | 102216 | 100% |
| 4,096 | 451344 | 136240 | 0 | 451344 | 100% |
| 9,216 | 941928 | 283016 | 0 | 941928 | 100% |

### (long,long) → long (composite group key, e.g. q4 (id,category))

| D (entries) | fresh B/tick | presized B/tick | pooled B/tick | reclaim (fresh−pooled) | reclaim % |
|---:|---:|---:|---:|---:|---:|
| 1 | 240 | 240 | 0 | 240 | 100% |
| 16 | 1208 | 744 | 0 | 1208 | 100% |
| 256 | 28560 | 10680 | 0 | 28560 | 100% |
| 1,024 | 131264 | 39840 | 0 | 131264 | 100% |
| 4,096 | 580136 | 175128 | 0 | 580136 | 100% |
| 9,216 | 1210872 | 363840 | 0 | 1210872 | 100% |

### The retention constraint (verified from the Core lifecycle)

Pooling reuses one `Dictionary` across ticks, so it is only sound where a tick's delta dictionary is **dead after the tick that produced it**. The verified Core rules:

- **`ZSet` takes ownership of the builder's dict** (`ZSet.cs` ctor: "callers must not retain a reference to the dict"); `Build()` nulls the builder's reference (`ZSetBuilder.cs:75`). So one dict ↔ one `ZSet`.
- **Trace `Integrate(delta)` folds the delta into the trace's *own* dict in place** (`Trace.cs` → `ZSet.MergeInPlace`) — it does **not** retain the delta dict. So after a stateful op integrates, its input delta is free.
- **A `Stream.Current` holds a tick's output only until the next tick's `SetCurrent` overwrites it**, and all same-tick consumers read it before then. So an output delta is dead at the next tick — **unless** a `z⁻¹` captures it.
- **`DelayOp` aliases its input `ZSet` by reference** (`DelayOp.cs:34`: `_nextOutput = _input.Current`) and re-emits it next tick. A delta on an edge feeding a `z⁻¹` is therefore **retained one tick** and is **NOT poolable**.

**Consequence:** pooling is sound *per edge*, gated on "this delta edge feeds no `z⁻¹`/`DelayOp` and is not otherwise captured." In q4/q18/q19's flat pipelines the only delays are **trace-internal** (the join's `L_{t-1}` is `_leftTrace.Current`, a trace-owned dict, not a delta), so their delta edges are dead-after-tick and poolable. Explicit `z⁻¹` on a delta edge appears in nested/recursive circuits (differentiate/integrate, fixpoint) — those edges must be excluded. The compiler knows the graph, so this is a per-edge analysis, not a global switch.

