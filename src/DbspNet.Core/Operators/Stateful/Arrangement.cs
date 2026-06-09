// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// A read-only, <b>shared</b> indexed arrangement: the running integral of an
/// indexed Z-set delta stream, keyed and grouped by a shared key, exposed so
/// that MANY downstream operators can probe ONE index instead of each
/// integrating its own private copy. This is Differential Dataflow's
/// shared-arrangement result — "build the index once, read it from many
/// operators" — applied to the cross-operator case (see
/// docs/design-row-representation.md §6.2).
/// </summary>
/// <remarks>
/// <see cref="Current"/> is the integral <b>after</b> the current tick's delta
/// has been folded in. The producing <see cref="ArrangeOp{TKey,TValue,TWeight}"/>
/// is registered before its consumers, so — like the right trace inside a plain
/// <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/> — every
/// consumer that reads it in the same step sees the post-delta ("after") state.
/// Consumers must read it within a single <c>Step</c> and never retain the
/// reference across ticks.
/// </remarks>
internal interface IArrangement<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>The integral including the current tick's delta (the "after" trace).</summary>
    IndexedZSet<TKey, TValue, TWeight> Current { get; }
}

/// <summary>
/// Builds and owns one shared indexed arrangement. Reads an already-indexed
/// (key-extracted) Z-set delta stream and integrates it into a single trace, so
/// that any number of downstream joins read that one integral via
/// <see cref="IArrangement{TKey,TValue,TWeight}.Current"/> instead of each
/// owning and integrating a private right trace.
/// </summary>
/// <remarks>
/// <para>Registered <b>before</b> its consuming joins, so each consumer sees the
/// post-delta integral in the same tick (operators fire in registration /
/// topological order — <see cref="RootCircuit"/>).</para>
/// <para>This is the <b>flat</b> (dictionary-trace) arrangement: consumers read
/// the whole <see cref="IArrangement{TKey,TValue,TWeight}.Current"/> indexed
/// Z-set and probe it by key. A spine variant — where consumers probe via
/// <c>GroupForManySorted</c> against shared sorted batches rather than reading a
/// materialised <c>Current</c> — is a follow-up (docs §6.2 / §8.3).</para>
/// <para>No frontier GC: a shared trace can only drop keys when <i>all</i>
/// consumers permit it (Differential Dataflow's compaction-coordination sharp
/// edge), which this first increment deliberately leaves out (docs §6.2). The
/// arrangement therefore retains full history.</para>
/// </remarks>
internal sealed class ArrangeOp<TKey, TValue, TWeight>
    : IOperator, IArrangement<TKey, TValue, TWeight>, IIntrospectable
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TValue, TWeight>> _in;
    private readonly IndexedZSetTrace<TKey, TValue, TWeight> _trace = new();

    public ArrangeOp(Stream<IndexedZSet<TKey, TValue, TWeight>> input)
    {
        ArgumentNullException.ThrowIfNull(input);
        _in = input;
    }

    public IndexedZSet<TKey, TValue, TWeight> Current => _trace.Current;

    public void Step() => _trace.Integrate(_in.Current);

    public string MetricName => "Arrange";

    public long RetainedRows => _trace.Current.GroupCount;

    // The arrangement has no output stream — it is read by reference, not piped.
    public long LastOutputRows => 0;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;
}
