# Design: stored / integrated output (delta → full materialized view)

The engine-side piece ivm-bench needs beyond the (now complete) SQL surface: a way
to turn the circuit's per-tick **output delta stream** into the **full current view
contents**, retained and snapshot-persisted, so the output adapter can write a full
snapshot per batch (`mode: truncate`). This is the DBSP integration operator `I`
applied at the output boundary — the analogue of Feldera's `+stored: true`
materialized view. Scoped 2026-07-16; **BUILT 2026-07-16** — shipped as `IntegrateOp` +
`CircuitBuilder.Integrate` + `CompileOptions.StoredOutput` + `CompiledQuery.CurrentView`
(see `IntegrateOpTests` / `StoredOutputTests` / `IntegrateSnapshotTests`). As-built matches
the design below.

## Why it's needed (recap from `ivm-bench-gap-analysis.md`)

- ivm-bench measures **full-state materialization**, not delta emission: 16 gold
  connectors are `delta_table_output` `mode: truncate` (full snapshot rewrite per
  batch), views are `+stored: true` (`CREATE MATERIALIZED VIEW`), and write-out +
  DBSP commit + **state persistence** are all inside the measured timer.
- DbspNet today emits **deltas**: `CompiledQuery.Output.Current` is the last tick's
  delta Z-set, "only meaningful after `Step`" (`CompiledQuery.cs:62`). Nothing at the
  output boundary retains the accumulated view. The only accumulation that exists is
  in test/bench helpers (`IncrementalOracle.RunAndAccumulate`,
  `SpineParallelHarness.Materialize`) — not a production circuit primitive.
- A delta-emitting engine would post a fast, non-comparable number (strictly less
  work than Feldera is charged for). To participate we must match the measured work:
  retain full state + hand the writer the full contents each batch.

## The integration primitive already exists

DBSP's `I` operator is a running Z-set sum, and the repo already has its state:

- **`ZSetTrace<TKey,TWeight>`** (`Operators/Stateful/Trace.cs:20`) holds `Current` —
  "all deltas folded in so far" — and `Integrate(delta)` folds one tick's delta into
  it via `ZSet.MergeInPlace` (`ZSet.cs:157`), which preserves the zero-is-absent
  invariant (a row whose accumulated weight returns to 0 is removed). This is exactly
  the integrated view. `O(|delta|)` per tick.
- Every stateful operator's snapshot Save/Load already serialises a per-partition
  `ZSet` through an `IZSetTraceCodec` (e.g. `PartitionedRankOp.cs:275-305`); the same
  codec persists the integrated view.

So the new operator is thin: it owns a `ZSetTrace`, integrates the delta each `Step`,
and exposes `Current`. No new algebra.

## The new operator — `IntegrateOp<TRow>`

New file `Core/Operators/Stateful/IntegrateOp.cs`, structural at the output boundary
(the output handle is already `OutputHandle<ZSet<StructuralRow, Z64>>`, so `TRow =
StructuralRow`; kept generic for testability / reuse).

```
internal sealed class IntegrateOp<TRow> : IOperator, ISnapshotable, IIntrospectable
    where TRow : notnull
{
    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;      // pass-through (delta)
    private readonly ZSetTrace<TRow, Z64> _view = new();   // the integrated state
    private readonly IZSetTraceCodec<TRow, Z64>? _snapshotCodec;

    public void Step()
    {
        var delta = _input.Current;
        _view.Integrate(delta);          // O(|delta|): fold into the running view
        _output.SetCurrent(delta);       // delta still flows through unchanged
    }

    /// The full current view contents — valid until the next Step (same contract
    /// as OutputHandle.Current). The output adapter enumerates this for a truncate
    /// write; the integration cost is O(|delta|), the write cost O(|view|).
    public ZSet<TRow, Z64> View => _view.Current;

    // ISnapshotable: Save/Load `_view.Current` through _snapshotCodec — identical to
    // every other op's per-partition ZSet round-trip. Makes the stored view
    // recoverable (Feldera `+stored: true` parity) and part of the measured commit.
    // IIntrospectable: RetainedRows = View.Count, MetricName = "IntegratedView".
    // GcFrontier => null: the materialized view is inherently unbounded — it *is*
    // the whole output, not a bounded trace. Correct and matching Feldera's MV.
}
```

**Pass-through, not replacement.** The op re-emits the delta on its output stream, so
the existing `OutputHandle` and every delta consumer are unchanged; the integrated
view is an *addition* read through `View`, not a new stream that re-materialises the
whole relation every tick. That keeps the tick cost `O(|delta|)`, not `O(|view|)` —
the `O(|view|)` cost is paid once per batch by the *writer* enumerating `View`, which
is precisely the truncate-mode cost Feldera is charged for.

### Why an accessor, not a full-snapshot stream

Two shapes were considered:

- **(a) emit the full view on the output stream each tick** — simplest to consume
  (`Current` becomes the whole view) but pays `O(|view|)` allocation every tick even
  when nothing reads it, and breaks existing delta consumers.
- **(b) integrate internally, expose `View` on demand** — chosen. Tick cost stays
  `O(|delta|)`; the full-relation cost is incurred only when the adapter actually
  writes (truncate reads the whole thing anyway). Delta consumers keep working.

## Wiring

- **`CircuitBuilder.Integrate`** (`StatefulOperators.cs`, near the other stateful
  builders): register an `IntegrateOp` on the query stream, return a small handle
  exposing both the (pass-through) output stream and the op, so the compiler can
  stash a reference for `CompiledQuery`.
- **`CompileOptions.StoredOutput`** (new `bool`, default **false**). Opt-in: the
  delta path stays zero-cost for consumers that want deltas (Nexmark / fraud
  benchmarks, streaming sinks); ivm-bench turns it on. Default-off means no behaviour
  change and no fingerprint change for existing circuits.
- **`PlanToCircuit.cs:194`**: when `options.StoredOutput`, wrap the final
  `queryStream` in `builder.Integrate(queryStream, codec)` before `builder.Output`,
  and thread the op reference into `CompiledQuery`. The integrate op sits *last*, so
  it integrates exactly the rows the output emits (post-projection / post-DISTINCT).
- **`CompiledQuery.CurrentView`** (new): `ZSet<StructuralRow, Z64>` — the full
  accumulated view after the last `Step`, or throws/returns null when
  `StoredOutput` was not requested. Documented "valid until the next `Step`", the
  same lifetime contract as `Current`. A convenience `EnumerateView()` yields
  `(row, weight)` for the writer.

The snapshot codec is over the **output schema** (`plan.Schema`) — the integrated
rows are output rows. Adding the op changes the plan fingerprint, so old (delta-only)
snapshots correctly refuse to load into a stored-output circuit; that is the intended
positional-identity guard (`ISnapshotable.cs:14-20`).

## Semantics / correctness notes

- **The accumulated view is the true multiset.** After tick T, `View` = Σ of all
  output deltas through T = the correct full view. Bag queries (`UNION ALL`) retain
  multiplicity > 1; set queries carry a `DISTINCT`/aggregation upstream, so the view
  is already a set. The op stores whatever the query emits, faithfully.
- **Transient negatives are a non-issue.** Individual output deltas can carry
  negative weights, but `MergeInPlace` folds them and drops any row that reaches
  weight 0; the *accumulated* weight of a well-formed query's row is its true
  multiplicity (≥ 0). No clamping, no ordering assumption.
- **No GC by design.** The stored view is the materialized output; retaining it in
  full is the point (and the cost). This is distinct from internal trace GC (LATENESS
  / clock frontiers), which bounds *intermediate* state — the output MV has no such
  bound, exactly as Feldera's `+stored` view doesn't.
- **State persistence is inside the commit.** `IntegrateOp` implementing
  `ISnapshotable` is what makes the view part of `Snapshot.WriteAsync` — matching the
  benchmark's timer, which stops only after DBSP commit + state persistence. It also
  gives crash-recovery of the materialized view for free.

## Testing

- **Unit** (`IntegrateOpTests`): push a sequence of ±1 deltas, assert `View` equals
  the running sum; a row driven to weight 0 is absent; multiplicity accumulates.
- **Differential** — the test of record: `CurrentView` after N random ticks ==
  `IncrementalOracle.RunAndAccumulate` over the same ticks (which is itself the
  accumulate-the-deltas reference) == `BatchPlanEvaluator` over the net input. Reuse
  the existing generators; run across the SQL surface (joins, aggregates, DISTINCT,
  the new rank-in-output, UNION ALL for multiplicity).
- **Snapshot round-trip** (`IntegrateSnapshotTests`): build a view, snapshot, restore
  into a fresh circuit, continue feeding deltas, assert the restored run's `View`
  equals an uninterrupted run's — proving the materialized view round-trips and that
  a retraction after restore correctly touches rows the consumer never inserted.
- **Fingerprint guard**: a delta-only snapshot refuses to load into a stored-output
  circuit and vice-versa.

## Cost

| Phase | Cost | Who pays |
|---|---|---|
| Integrate delta per tick | `O(\|delta\|)` | engine (cheap) |
| Retain view | `O(\|view\|)` memory | engine (inherent — it's the MV) |
| Truncate write per batch | `O(\|view\|)` serialize | output adapter |
| Persist view in commit | `O(\|view\|)` per snapshot | persistence (measured) |

This is the same work Feldera is charged for on 16 of 18 gold views (compute + full
snapshot rewrite), and the reason two views (`fact_market_history`,
`daily_market_pulse`) had their truncate connectors *removed* — the full rewrite
couldn't drain. Our numbers on those two should be compute-only likewise; the design
doesn't change that, it just makes the full-state path exist.

## Effort / risk

- **Effort: ~1 day.** The operator is thin (reuses `ZSetTrace`), the wiring is a flag
  + an accessor, and the snapshot path is the established per-op ZSet round-trip.
- **Risk: low.** No new algebra; the integration primitive is already exercised by
  every trace-backed operator. The one contract to hold is the `View`-valid-until-
  next-`Step` lifetime (documented, matching `Current`) and the opt-in default-off so
  nothing else moves.

## Change-site summary

| Site | File | ~LOC |
|---|---|---|
| New `IntegrateOp<TRow>` | new file in `Core/Operators/Stateful/` | 90–120 |
| `CircuitBuilder.Integrate` | `StatefulOperators.cs` | 25 |
| `CompileOptions.StoredOutput` | `CompileOptions.cs` | 3 |
| Wire at output boundary | `PlanToCircuit.cs:194` | 10 |
| `CompiledQuery.CurrentView` / `EnumerateView` | `CompiledQuery.cs` | 15 |
| Tests | new `IntegrateOpTests` / `IntegrateSnapshotTests` | 150–250 |

## What this does *not* cover

- **Input adapter** (Delta/Parquet → Arrow `RecordBatch` → `InputHandle`) — separate
  connector work.
- **Output adapter** (enumerate `View` → Delta truncate write + the drain signal the
  timer waits on) — separate connector work; this design gives it the seam (`View` /
  `EnumerateView`).
- **End-to-end correctness on real TPC-DI data** — the differential tests prove
  incremental ≡ batch on generated inputs; a real-data run is due-diligence beyond
  the harness's own (timer-based, non-cross-validating) contract.
