using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Extension methods on <see cref="CircuitBuilder"/> for stateful Z-set
/// operators: distinct, incremental join, incremental aggregate.
/// </summary>
public static class StatefulOperators
{
    public static Stream<ZSet<TKey, TWeight>> Distinct<TKey, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TKey, TWeight>> input,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);

        var output = new Stream<ZSet<TKey, TWeight>>(ZSet<TKey, TWeight>.Empty);
        builder.AddRawOperator(new DistinctOp<TKey, TWeight>(input, output, snapshotCodec));
        return output;
    }

    /// <summary>
    /// Index a flat Z-set stream by a key-extraction function. The key
    /// extractor is applied tick-by-tick and results in an indexed Z-set
    /// stream with the same weights.
    /// </summary>
    public static Stream<IndexedZSet<TKey, TRow, TWeight>> IndexBy<TKey, TRow, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, TWeight>> input,
        Func<TRow, TKey> keyOf)
        where TKey : notnull
        where TRow : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(keyOf);

        return builder.Apply(input, z => IndexedZSet.IndexBy(z, keyOf));
    }

    /// <summary>
    /// Rekey a flat Z-set by extracting a group key and a per-row value,
    /// producing an indexed Z-set ready for aggregation. Weight is preserved.
    /// </summary>
    public static Stream<IndexedZSet<TKey, TValue, TWeight>> GroupProject<TKey, TRow, TValue, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, TWeight>> input,
        Func<TRow, TKey> keyOf,
        Func<TRow, TValue> valueOf)
        where TKey : notnull
        where TRow : notnull
        where TValue : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(keyOf);
        ArgumentNullException.ThrowIfNull(valueOf);

        return builder.Apply(input, z =>
        {
            var b = new IndexedZSetBuilder<TKey, TValue, TWeight>();
            foreach (var (row, w) in z)
            {
                b.Add(keyOf(row), valueOf(row), w);
            }

            return b.Build();
        });
    }

    /// <summary>
    /// Incremental inner equi-join. Both inputs must already be indexed on
    /// the shared key. <paramref name="combine"/> produces the output row
    /// from the join key, the left row and the right row.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> IncrementalInnerJoin<TKey, TLeft, TRight, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TLeft, TWeight>> left,
        Stream<IndexedZSet<TKey, TRight, TWeight>> right,
        Func<TKey, TLeft, TRight, TOut> combine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null)
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
            new IncrementalJoinOp<TKey, TLeft, TRight, TOut, TWeight>(
                left, right, output, combine, leftSnapshotCodec, rightSnapshotCodec));
        return output;
    }

    /// <summary>
    /// Incremental LEFT OUTER equi-join. Behaves like
    /// <see cref="IncrementalInnerJoin{TKey,TLeft,TRight,TOut,TWeight}"/> on
    /// keys with a right-side match, and emits a NULL-padded row (produced by
    /// <paramref name="nullPadCombine"/>) for every left row whose key has
    /// no right-side match. Match-status flips correctly retract / emit
    /// NULL-padded rows under retractions.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> IncrementalLeftJoin<TKey, TLeft, TRight, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TLeft, TWeight>> left,
        Stream<IndexedZSet<TKey, TRight, TWeight>> right,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null)
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
            new IncrementalLeftJoinOp<TKey, TLeft, TRight, TOut, TWeight>(
                left, right, output, joinCombine, nullPadCombine,
                leftSnapshotCodec, rightSnapshotCodec));
        return output;
    }

    /// <summary>
    /// Incremental per-group aggregate. Input is an indexed Z-set (keyed by
    /// the GROUP BY columns); output is a flat Z-set of (key, aggregateValue)
    /// rows with correct retraction semantics.
    /// </summary>
    public static Stream<ZSet<(TKey Key, TOut Value), Z64>> IncrementalAggregate<TKey, TValue, TOut>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        IAggregator<TValue, TOut> aggregator,
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null)
        where TKey : notnull
        where TValue : notnull
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(aggregator);

        var output = new Stream<ZSet<(TKey, TOut), Z64>>(ZSet<(TKey, TOut), Z64>.Empty);
        builder.AddRawOperator(
            new IncrementalAggregateOp<TKey, TValue, TOut>(input, output, aggregator, snapshotCodec));
        return output;
    }
}
