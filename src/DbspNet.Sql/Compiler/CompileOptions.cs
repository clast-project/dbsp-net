// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Selects which trace implementation the stateful operators use.
/// </summary>
public enum TraceFamily
{
    /// <summary>
    /// The default flat dictionary-backed traces (<c>DistinctOp</c>,
    /// <c>IncrementalAggregateOp</c>, <c>IncrementalJoinOp</c>,
    /// <c>IncrementalLeftJoinOp</c>).
    /// </summary>
    Flat,

    /// <summary>
    /// The LSM-style spine traces (<c>SpineDistinctOp</c>,
    /// <c>SpineIncrementalAggregateOp</c>, <c>SpineIncrementalJoinOp</c>,
    /// <c>SpineIncrementalLeftJoinOp</c>): a tiered sequence of immutable
    /// sorted-columnar batches with per-batch Arrow snapshot and optional
    /// disk spill.
    /// </summary>
    Spine,
}

/// <summary>
/// Knobs for <see cref="PlanToCircuit.Compile(DbspNet.Sql.Plan.LogicalPlan, ISqlSnapshotCodecs, CompileOptions)"/>.
/// </summary>
/// <remarks>
/// Selecting <see cref="TraceFamily.Spine"/> routes every keyed stateful site
/// (DISTINCT, GROUP BY aggregate, INNER / LEFT / RIGHT join) to its spine
/// counterpart — on both the typed-row fast path and the structural fallback.
/// The typed pipeline supplies <c>Comparer&lt;TRow&gt;.Default</c> implicitly
/// (the emitted struct row types implement <c>IComparable&lt;TSelf&gt;</c> via
/// <c>TypedRowEmitter</c>'s <c>EmitTypedCompareTo</c>); the structural pipeline
/// supplies <see cref="DbspNet.Core.Collections.StructuralRowComparer.Instance"/>.
/// Recursive CTEs honour this setting too: in spine mode the nested fixpoint
/// circuit's import-relation integrals use the spine trace family (the inner
/// loop body itself is stateless, so it has no trace either way).
/// </remarks>
public sealed record CompileOptions
{
    /// <summary>The default options: flat traces, no spine.</summary>
    public static CompileOptions Default { get; } = new();

    /// <summary>Which trace implementation stateful operators use.</summary>
    public TraceFamily TraceFamily { get; init; } = TraceFamily.Flat;

    /// <summary>
    /// Compaction policy for the spine trace family. Ignored when
    /// <see cref="TraceFamily"/> is <see cref="TraceFamily.Flat"/>. A null value
    /// selects <see cref="TieredCompactionStrategy.Default"/> at the operator.
    /// </summary>
    public ICompactionStrategy? Compaction { get; init; }

    /// <summary>
    /// Spine indexed-trace memtable flush threshold, in distinct keys
    /// (cross-tick amortisation; docs/design-row-representation.md §10–§11).
    /// Ignored when <see cref="TraceFamily"/> is <see cref="TraceFamily.Flat"/>.
    /// The default — <b>8,192</b>, the §11 capacity-sweep knee — buffers each
    /// tick's delta as an in-place dictionary merge and flushes a sorted batch
    /// only every few ticks, so the spine substrate is competitive with the flat
    /// dictionary on small per-tick deltas (the W&gt;1 replica case) instead of
    /// paying a fresh batch build every tick. Set to 0 to disable the memtable
    /// (one batch per delta, the pre-§10 behaviour).
    /// </summary>
    public int SpineStagingCapacity { get; init; } = 8192;

    /// <summary>
    /// Opt-in arrangement common-subexpression elimination (Option 2 /
    /// cross-operator shared arrangements; see docs/design-row-representation.md
    /// §9.6). When a relation is the right input of ≥2 INNER joins on the same
    /// key, the compiler builds ONE shared arrangement (an <c>Arrange</c> /
    /// <c>SpineArrange</c>) and routes those joins through the shared-right join
    /// instead of each maintaining a private right trace.
    /// </summary>
    /// <remarks>
    /// CSE is implemented on the <b>structural</b> compile path only, so this
    /// flag also forces that path (the typed fast path is skipped). It engages a
    /// join only when there is no join-key GC frontier and no snapshot codec —
    /// the shared arrangement carries neither coordinated frontier GC nor
    /// snapshot in this first increment (a relation joined by the same key in two
    /// places is the "single clearest case" of §6.2). A modest, fan-out-scaling
    /// win (§9.5); off by default.
    /// </remarks>
    public bool ShareArrangements { get; init; }

    /// <summary>
    /// Opt-in fusion of a join's two input exchanges into one shared barrier
    /// (docs/design-row-representation.md §15). A parallel INNER/OUTER join shuffles
    /// both inputs by the join key through two independent
    /// <c>ExchangeIndex</c> rendezvous back to back; each pays the barrier
    /// straggler tax separately (the dominant scaling cost — q4's exchange wait
    /// reaches ~40% of the step at W=24). When set, the typed parallel compiler
    /// emits a single <c>ExchangeIndexJoin</c> that publishes both sides and
    /// rendezvouses once, halving the join's straggler exposure with identical
    /// output. Affects only the typed parallel path (W&gt;1); W=1 is unchanged.
    /// Off by default while it is gated.
    /// </summary>
    public bool CoalesceJoinExchange { get; init; }

    /// <summary>
    /// Opt-in stored / integrated output (docs/design-stored-output.md). When set,
    /// the compiler integrates the final delta stream at the output boundary (the
    /// DBSP <c>I</c> operator, an <c>IntegrateOp</c>) so
    /// <see cref="CompiledQuery.CurrentView"/> exposes the <b>full current view
    /// contents</b> — a materialized view — for a truncate-mode sink to write a full
    /// snapshot per batch (the analogue of Feldera's <c>+stored</c> MV; required for
    /// ivm-bench's full-state contract). The delta output (<see cref="CompiledQuery.Current"/>)
    /// is unaffected — the delta still flows through. Off by default: a delta-only
    /// pipeline pays nothing, and the circuit's operator set (hence its snapshot
    /// fingerprint) is unchanged unless this is requested.
    /// </summary>
    public bool StoredOutput { get; init; }

    /// <summary>
    /// Opt-in (measurement gate): on the <b>program</b> path
    /// (<see cref="PlanToCircuit.CompileProgram"/>), attempt the typed-row fast
    /// path (<c>TypedPlanCompiler.TryCompileWithStructuralBoundary</c>) per view,
    /// falling back to the structural compile for any view the typed compiler
    /// cannot handle. A typed view runs its inner operators on emitted struct
    /// rows and pays a structural↔typed conversion only at its scan (lift) and
    /// output (adapt) boundaries; the shared inter-view streams stay
    /// <c>StructuralRow</c>, so a typed view consuming another typed view's output
    /// still round-trips through the structural boundary (no cross-view fusion).
    /// This is the row-representation measurement lever
    /// (docs/design-row-representation.md; typed rows attack the Layer-B boxing
    /// cost, not the Layer-A per-tick dict floor). Off by default; forced off when
    /// <see cref="ShareArrangements"/> is set (CSE is structural-only).
    /// </summary>
    /// <remarks>
    /// <b>MEASURED DECISIVELY NEGATIVE — do not enable for perf</b> (ivm-bench
    /// SF=3 batch-1, docs/design-row-representation.md §23): typing the 19
    /// type-eligible views drove allocation <b>+82%</b> (44.7→81.3 GiB) and wall
    /// time <b>+42%</b> (identical output row counts). The fully-structural program
    /// shares one <c>object[]</c> by reference across adjacent views, so its
    /// inter-view boundary is near-free; typing inserts a decode(lift)+encode(adapt)
    /// round-trip at each boundary, and a typed→typed adjacency additionally forces
    /// the boxing the lazy <c>TypedStructuralRow</c> deferred (worst hit:
    /// watches/daily_market/trades). Retained solely as a default-off,
    /// re-measurable gate for the arc (flip IVM_TYPE_VIEWS=1 on the local harness).
    /// </remarks>
    public bool TypeEligibleProgramViews { get; init; }

    /// <summary>
    /// On the typed window-aggregate path, key the per-partition ordered state on the
    /// <b>unboxed</b> monotone order value (a <c>long?</c> via
    /// <see cref="DbspNet.Core.Collections.LongKeyComparer{TRow}"/>) instead of the
    /// boxed one-key <c>SortKeyComparer</c>. On a typed struct row the boxed comparer
    /// allocates one heap box per key per comparison (the dominant term in the
    /// typed-vs-structural window-aggregate allocation gap — the sort comparator runs
    /// O(log n) times per insert); the monomorphized comparer removes it. Sound only
    /// when the order key's monotone <c>long</c> extraction preserves the boxed
    /// comparer's order (integer/temporal carriers), and falls back to the boxed
    /// comparer otherwise. The structural path is unaffected.
    /// </summary>
    /// <remarks>
    /// <b>Default-on</b> (design §23.7): a correctness-equivalent, byte-identical win
    /// validated across every window-aggregate shape (running / bounded RANGE /
    /// MIN-MAX / DESC / INTERVAL-over-TIMESTAMP / DATE / nullable / multi-spec) at
    /// W=1..8 and on the spine trace family, incremental≡batch. The competitive A/B on
    /// the fraud rolling-window feature view (TIMESTAMP order key, `windowmono`
    /// benchmark) shows +13–21% step throughput W=1..14 and up to −39% allocation,
    /// output byte-identical. The prize is workload-dependent (near-best-case: a
    /// value-type key over large sorted partitions; string keys don't box, small
    /// partitions do few comparisons) — set this <c>false</c> to force the boxed
    /// comparer. The boxed comparer also stays live for any non-carrier order key.
    /// </remarks>
    public bool MonomorphizeWindowOrderKey { get; init; } = true;

    /// <summary>
    /// Opt-in program-level dead-column elimination (docs/design-column-liveness.md).
    /// On the <see cref="PlanToCircuit.CompileProgram"/> path, computes per-view
    /// output-column liveness across the whole view DAG and prunes each view's plan
    /// to the columns some output or live downstream view actually reads. Most
    /// valuably it eliminates a window / offset operator all of whose produced
    /// columns are read only by a dead-pruned leaf view — e.g. ivm-bench
    /// <c>daily_market</c>'s two <c>fifty_two_week_*</c> window aggregates, read only
    /// by the unreachable <c>fact_market_history</c> (~3.4 GiB / ~7.9s of batch-1).
    /// </summary>
    /// <remarks>
    /// Arity-preserving: a dead column becomes a cheap NULL constant and a fully-dead
    /// producer op becomes a constant-filling projection, so no downstream view's
    /// column indices shift (the view's output schema is unchanged). Correctness-
    /// preserving — a dropped column is read by nothing, and GROUP BY / DISTINCT / join
    /// keys stay live via the liveness rules, so row multiplicity is unchanged. Off by
    /// default while gated; program path only.
    /// </remarks>
    public bool EliminateDeadColumns { get; init; }
}
