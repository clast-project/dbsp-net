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
}
