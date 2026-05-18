using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// <see cref="CircuitBuilder"/> extension methods for the spine-backed
/// stateful operators. Each method is the spine counterpart of an
/// existing flat-trace builder in
/// <see cref="StatefulOperators"/>; both can coexist in the same
/// circuit, which keeps phase-2 A/B benchmarks honest.
/// </summary>
/// <remarks>
/// <b>Total-order requirement.</b> Spine batches store keys (and inner
/// values, for indexed traces) in sorted order. Each
/// <typeparamref name="TKey"/> / <typeparamref name="TValue"/> must
/// either implement <see cref="IComparable{T}"/> so that
/// <see cref="Comparer{T}.Default"/> can be used, or the caller must
/// pass an explicit <see cref="IComparer{T}"/>. Record / class types
/// without <c>IComparable&lt;T&gt;</c> will fail at runtime during
/// compaction with "At least one object must implement IComparable".
/// </remarks>
public static class SpineStatefulOperators
{
    /// <summary>
    /// Incremental <c>distinct</c> backed by an LSM-style
    /// <see cref="SpineZSetTrace{TKey,TWeight}"/>. Observable behaviour
    /// matches <see cref="StatefulOperators.Distinct"/>; the difference
    /// is purely in how the running trace is stored.
    /// </summary>
    /// <param name="compactionStrategy">
    /// Optional compaction policy; defaults to
    /// <see cref="TieredCompactionStrategy.Default"/> (4 batches per
    /// level).
    /// </param>
    public static Stream<ZSet<TKey, TWeight>> SpineDistinct<TKey, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TKey, TWeight>> input,
        ICompactionStrategy? compactionStrategy = null,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null,
        IComparer<TKey>? keyComparer = null)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);

        var output = new Stream<ZSet<TKey, TWeight>>(ZSet<TKey, TWeight>.Empty);
        builder.AddRawOperator(
            new SpineDistinctOp<TKey, TWeight>(input, output, compactionStrategy, snapshotCodec, keyComparer));
        return output;
    }

    /// <summary>
    /// Incremental per-group aggregate backed by a
    /// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/>. Observable
    /// behaviour matches <see cref="StatefulOperators.IncrementalAggregate"/>.
    /// </summary>
    public static Stream<ZSet<(TKey Key, TOut Value), Z64>> SpineIncrementalAggregate<TKey, TValue, TOut>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        IAggregator<TValue, TOut> aggregator,
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TValue>? valueComparer = null)
        where TKey : notnull
        where TValue : notnull
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(aggregator);

        var output = new Stream<ZSet<(TKey, TOut), Z64>>(ZSet<(TKey, TOut), Z64>.Empty);
        builder.AddRawOperator(
            new SpineIncrementalAggregateOp<TKey, TValue, TOut>(
                input, output, aggregator, snapshotCodec, compactionStrategy, keyComparer, valueComparer));
        return output;
    }

    /// <summary>
    /// Incremental inner equi-join with both traces backed by the spine.
    /// Observable behaviour matches <see cref="StatefulOperators.IncrementalInnerJoin"/>.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> SpineIncrementalInnerJoin<TKey, TLeft, TRight, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TLeft, TWeight>> left,
        Stream<IndexedZSet<TKey, TRight, TWeight>> right,
        Func<TKey, TLeft, TRight, TOut> combine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TLeft>? leftValueComparer = null,
        IComparer<TRight>? rightValueComparer = null)
        where TKey : notnull
        where TLeft : notnull
        where TRight : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        ArgumentNullException.ThrowIfNull(combine);

        var output = new Stream<ZSet<TOut, TWeight>>(ZSet<TOut, TWeight>.Empty);
        builder.AddRawOperator(
            new SpineIncrementalJoinOp<TKey, TLeft, TRight, TOut, TWeight>(
                left, right, output, combine,
                leftSnapshotCodec, rightSnapshotCodec,
                compactionStrategy, keyComparer, leftValueComparer, rightValueComparer));
        return output;
    }

    /// <summary>
    /// Incremental LEFT OUTER equi-join with both traces backed by the spine.
    /// Observable behaviour matches <see cref="StatefulOperators.IncrementalLeftJoin"/>.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> SpineIncrementalLeftJoin<TKey, TLeft, TRight, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TLeft, TWeight>> left,
        Stream<IndexedZSet<TKey, TRight, TWeight>> right,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        ICompactionStrategy? compactionStrategy = null,
        IComparer<TKey>? keyComparer = null,
        IComparer<TLeft>? leftValueComparer = null,
        IComparer<TRight>? rightValueComparer = null)
        where TKey : notnull
        where TLeft : notnull
        where TRight : notnull
        where TOut : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        ArgumentNullException.ThrowIfNull(joinCombine);
        ArgumentNullException.ThrowIfNull(nullPadCombine);

        var output = new Stream<ZSet<TOut, TWeight>>(ZSet<TOut, TWeight>.Empty);
        builder.AddRawOperator(
            new SpineIncrementalLeftJoinOp<TKey, TLeft, TRight, TOut, TWeight>(
                left, right, output, joinCombine, nullPadCombine,
                leftSnapshotCodec, rightSnapshotCodec,
                compactionStrategy, keyComparer, leftValueComparer, rightValueComparer));
        return output;
    }
}
