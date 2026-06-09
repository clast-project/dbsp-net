// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Process-wide opt-in for the spine indexed trace's in-memory <b>memtable</b>
/// (cross-tick amortisation; docs/design-row-representation.md §9.7).
/// </summary>
/// <remarks>
/// <para>Default <see cref="Capacity"/> is 0 — disabled, so
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.Integrate"/> builds one
/// immutable sorted batch per delta exactly as before (every existing spine test
/// that pins batch/level structure stays valid).</para>
/// <para>When set to N &gt; 0, a trace constructed afterwards buffers per-tick
/// deltas in a mutable dictionary memtable and only flushes them into a sorted
/// batch once the memtable holds ≥ N distinct keys — turning the hot per-tick
/// <c>Integrate</c> into an in-place merge (flat-dictionary cost) instead of a
/// fresh batch build + bloom + compaction. The §8.3 q4 gate showed that per-tick
/// build was the spine substrate's loss to the flat dictionary; this amortises
/// it across N keys' worth of ticks.</para>
/// <para>The compiler (<c>PlanToCircuit</c> / <c>TypedPlanCompiler</c>) realises
/// <c>CompileOptions.SpineStagingCapacity</c> through this ambient seam: it sets
/// the value for the duration of the build and restores it after, and each trace
/// reads it once at construction. The seam (rather than threading the capacity
/// through every spine builder method) sidesteps the typed-compiler reflection
/// over builder signatures. <b>The field is <see cref="ThreadStaticAttribute"/></b>
/// so concurrent compiles — or a direct <c>new SpineIndexedZSetTrace(...)</c> on
/// another thread — cannot observe each other's value. This is sound because the
/// whole graph (every trace, including a parallel circuit's sequentially-built
/// replicas) is constructed synchronously on the compiling thread, so a
/// thread-scoped value captures exactly the traces that compile meant to
/// configure.</para>
/// </remarks>
internal static class SpineStagingConfig
{
    /// <summary>
    /// Memtable flush threshold in distinct keys; 0 disables the memtable (the
    /// default, byte-identical to the pre-memtable behaviour). Thread-static:
    /// each thread's traces read that thread's compile-scoped value.
    /// </summary>
    [ThreadStatic]
    internal static int Capacity;
}
