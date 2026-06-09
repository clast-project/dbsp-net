// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// A read-only, <b>shared</b> spine arrangement: the running integral of an
/// indexed Z-set delta stream held in ONE
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/>, exposed so that
/// many downstream joins probe that single trace via its galloping merge
/// (<see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>)
/// instead of each building and re-maintaining a private right trace. This is
/// the spine counterpart of <see cref="IArrangement{TKey,TValue,TWeight}"/>
/// (docs/design-row-representation.md §6.2 / §9): unlike the flat handle, which
/// hands consumers a materialised <c>Current</c>, the spine handle exposes the
/// trace itself so consumers read it by sorted-merge probe — never materialising
/// the whole integral.
/// </summary>
/// <remarks>
/// The duplicated per-consumer maintenance a shared spine trace removes is a
/// full sorted-columnar <b>batch build</b> (+ bloom + compaction) per tick — the
/// §8.3 q4 substrate cost — which is far more expensive than the flat dictionary
/// merge, so spine sharing should pay more than the flat win. <see cref="Trace"/>
/// is the integral <b>after</b> the current tick's delta (the producing
/// <see cref="SpineArrangeOp{TKey,TValue,TWeight}"/> integrates before its
/// consumers run, in registration / topological order).
/// </remarks>
internal interface ISpineArrangement<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>The shared integrated trace, post the current tick's delta.</summary>
    SpineIndexedZSetTrace<TKey, TValue, TWeight> Trace { get; }

    /// <summary>
    /// The key order the trace's batches use — consumers must sort their probe
    /// keys by this comparer before calling
    /// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>.
    /// </summary>
    IComparer<TKey> KeyComparer { get; }
}

/// <summary>
/// Builds and owns one shared spine arrangement: reads an already-indexed delta
/// stream and integrates it into a single
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/>, paying the
/// sorted-batch build once for all consumers. Registered before its consuming
/// joins, so they probe the post-delta integral in the same tick.
/// </summary>
/// <remarks>
/// No frontier GC: a shared trace can only drop keys when all consumers permit
/// it (the compaction-coordination sharp edge §6.2 defers); the arrangement
/// retains full history. No snapshot for the same reason this first increment
/// keeps the shared join out of the snapshot path.
/// </remarks>
internal sealed class SpineArrangeOp<TKey, TValue, TWeight>
    : IOperator, ISpineArrangement<TKey, TValue, TWeight>, IIntrospectable
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<IndexedZSet<TKey, TValue, TWeight>> _in;
    private readonly SpineIndexedZSetTrace<TKey, TValue, TWeight> _trace;
    private readonly IComparer<TKey> _keyComparer;

    public SpineArrangeOp(
        Stream<IndexedZSet<TKey, TValue, TWeight>> input,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TValue>? valueComparer = null,
        SpineIndexedSpillConfig<TKey, TValue, TWeight>? spillConfig = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        _in = input;
        _keyComparer = keyComparer ?? Comparer<TKey>.Default;
        _trace = new SpineIndexedZSetTrace<TKey, TValue, TWeight>(
            compactionStrategy ?? TieredCompactionStrategy.Default, keyComparer, valueComparer, spillConfig);
    }

    public SpineIndexedZSetTrace<TKey, TValue, TWeight> Trace => _trace;

    public IComparer<TKey> KeyComparer => _keyComparer;

    public void Step() => _trace.Integrate(_in.Current);

    public string MetricName => "SpineArrange";

    public long RetainedRows => _trace.GroupCount;

    public long LastOutputRows => 0;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;
}
