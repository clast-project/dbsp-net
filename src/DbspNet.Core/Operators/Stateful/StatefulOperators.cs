// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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
    /// <summary>
    /// Input-side lateness enforcement: drops rows whose <paramref name="monotone"/>
    /// value is strictly below the current frontier, and advances
    /// <paramref name="frontier"/> to <c>maxSeen − lateness</c>. Returns the
    /// filtered stream. Wire the same <paramref name="frontier"/> into
    /// downstream stateful operators (e.g. <see cref="IncrementalAggregate"/>)
    /// so they GC unreachable state.
    /// </summary>
    public static Stream<ZSet<TRow, Z64>> EnforceLateness<TRow>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, Z64>> input,
        Func<TRow, long> monotone,
        long lateness,
        MutableFrontier frontier)
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(monotone);
        ArgumentNullException.ThrowIfNull(frontier);

        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        builder.AddRawOperator(new LatenessOperator<TRow>(input, output, monotone, lateness, frontier));
        return output;
    }

    /// <summary>
    /// Temporal filter (the advancing-clock predicate): keep each input row
    /// only while the logical clock <paramref name="clock"/> lies inside the
    /// row's validity window — <c>now {&gt;|&gt;=} timeKey + appearOffset</c> and
    /// <c>now {&lt;|&lt;=} timeKey + disappearOffset</c> — emitting inserts and
    /// retractions as the clock advances with no new input. A null offset leaves
    /// that side unbounded; a null <paramref name="timeKey"/> value is never
    /// valid. Offsets are in the same unit as the clock (microseconds).
    /// </summary>
    public static Stream<ZSet<TRow, Z64>> TemporalFilter<TRow>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, Z64>> input,
        Func<TRow, long?> timeKey,
        long? appearOffsetMicros,
        bool appearInclusive,
        long? disappearOffsetMicros,
        bool disappearInclusive,
        IFrontier clock,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(timeKey);
        ArgumentNullException.ThrowIfNull(clock);

        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        builder.AddRawOperator(new TemporalFilterOp<TRow>(
            input, output, timeKey, appearOffsetMicros, appearInclusive,
            disappearOffsetMicros, disappearInclusive, clock, snapshotCodec));
        return output;
    }

    public static Stream<ZSet<TKey, TWeight>> Distinct<TKey, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TKey, TWeight>> input,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);

        var output = new Stream<ZSet<TKey, TWeight>>(ZSet<TKey, TWeight>.Empty);
        builder.AddRawOperator(new DistinctOp<TKey, TWeight>(input, output, snapshotCodec, frontier, monotoneKey));
        return output;
    }

    /// <summary>
    /// Incremental TOP-K (<c>ORDER BY … LIMIT [OFFSET]</c>): keep the rows
    /// occupying sort positions <c>[offset, offset + limit)</c> under
    /// <paramref name="comparer"/>'s total order, maintained as rows enter and
    /// leave the window under retraction. A null <paramref name="limit"/> means
    /// "to the end" (an <c>OFFSET</c> with no <c>LIMIT</c>).
    /// </summary>
    public static Stream<ZSet<TRow, Z64>> TopK<TRow>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, Z64>> input,
        IComparer<TRow> comparer,
        long offset,
        long? limit,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(comparer);

        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        builder.AddRawOperator(new TopKOp<TRow>(input, output, comparer, offset, limit, snapshotCodec));
        return output;
    }

    /// <summary>
    /// Incremental partitioned TOP-K — <c>ROW_NUMBER</c> / <c>RANK</c> /
    /// <c>DENSE_RANK</c> in the filter pattern
    /// <c>… OVER (PARTITION BY p ORDER BY o) &lt;= limit</c>. Keeps, per
    /// <paramref name="partitionOf"/> partition, the rows whose rank under
    /// <paramref name="order"/> is <c>&lt;= limit</c>, maintained under
    /// retraction. <paramref name="sortKeyOnly"/> orders by the <c>ORDER BY</c>
    /// keys alone so <paramref name="function"/> <c>Rank</c> / <c>DenseRank</c>
    /// can keep whole tie groups (ignored for <c>RowNumber</c>).
    /// </summary>
    public static Stream<ZSet<TRow, Z64>> PartitionedTopK<TRow, TKey>(
        this CircuitBuilder builder,
        Stream<ZSet<TRow, Z64>> input,
        Func<TRow, TKey> partitionOf,
        IComparer<TRow> order,
        IComparer<TRow> sortKeyOnly,
        RankFunction function,
        long limit,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TRow, Z64>? snapshotCodec = null)
        where TRow : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(sortKeyOnly);

        var output = new Stream<ZSet<TRow, Z64>>(ZSet<TRow, Z64>.Empty);
        builder.AddRawOperator(new PartitionedTopKOp<TRow, TKey>(
            input, output, partitionOf, order, sortKeyOnly, function, limit, partitionComparer, snapshotCodec));
        return output;
    }

    /// <summary>
    /// Incremental partitioned window aggregate — <c>agg(x) OVER (PARTITION BY p
    /// [ORDER BY o RANGE …])</c> emitted as new column(s) appended to every row.
    /// <paramref name="orderValueOf"/> null ⇒ a whole-partition frame;
    /// <paramref name="orderValueOf"/> set with <paramref name="preceding"/> null
    /// ⇒ a running frame; a non-null <paramref name="preceding"/> ⇒ a bounded
    /// RANGE frame (GC-able via <paramref name="frontier"/> for an ascending key).
    /// </summary>
    public static Stream<ZSet<StructuralRow, Z64>> PartitionedWindowAggregate<TKey>(
        this CircuitBuilder builder,
        Stream<ZSet<StructuralRow, Z64>> input,
        Func<StructuralRow, TKey> partitionOf,
        IComparer<StructuralRow> order,
        Func<StructuralRow, long>? orderValueOf,
        long? preceding,
        bool descending,
        IAggregator<StructuralRow, StructuralRow> aggregator,
        int aggCount,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<StructuralRow, Z64>? snapshotCodec = null,
        IFrontier? frontier = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(aggregator);

        var output = new Stream<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty);
        builder.AddRawOperator(new PartitionedWindowAggregateOp<TKey>(
            input, output, partitionOf, order, orderValueOf, preceding, descending,
            aggregator, aggCount, partitionComparer, snapshotCodec, frontier));
        return output;
    }

    /// <summary>
    /// Incremental partitioned <c>LAG</c> / <c>LEAD</c> — positional offset
    /// functions (<paramref name="specs"/>) emitted as new column(s) appended to
    /// every row, keyed by <paramref name="partitionOf"/> and ordered by
    /// <paramref name="order"/> (a total order).
    /// </summary>
    public static Stream<ZSet<StructuralRow, Z64>> PartitionedOffset<TKey>(
        this CircuitBuilder builder,
        Stream<ZSet<StructuralRow, Z64>> input,
        Func<StructuralRow, TKey> partitionOf,
        IComparer<StructuralRow> order,
        OffsetSpec[] specs,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<StructuralRow, Z64>? snapshotCodec = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(specs);

        var output = new Stream<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty);
        builder.AddRawOperator(new PartitionedOffsetOp<TKey>(
            input, output, partitionOf, order, specs, partitionComparer, snapshotCodec));
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
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null,
        Func<TOut, bool>? residual = null)
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
                left, right, output, combine, leftSnapshotCodec, rightSnapshotCodec, frontier, monotoneKey, residual));
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
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
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
                leftSnapshotCodec, rightSnapshotCodec, frontier, monotoneKey));
        return output;
    }

    /// <summary>
    /// Incremental FULL OUTER equi-join. Behaves like
    /// <see cref="IncrementalLeftJoin{TKey,TLeft,TRight,TOut,TWeight}"/> for the
    /// inner + left-preserved rows, and additionally emits a NULL-padded-left
    /// row (produced by <paramref name="nullPadLeftCombine"/>) for every right
    /// row whose key has no left-side match. Match-status flips on either side
    /// correctly retract / emit the affected NULL-padded rows.
    /// </summary>
    public static Stream<ZSet<TOut, TWeight>> IncrementalFullJoin<TKey, TLeft, TRight, TOut, TWeight>(
        this CircuitBuilder builder,
        Stream<IndexedZSet<TKey, TLeft, TWeight>> left,
        Stream<IndexedZSet<TKey, TRight, TWeight>> right,
        Func<TKey, TLeft, TRight, TOut> joinCombine,
        Func<TKey, TLeft, TOut> nullPadRightCombine,
        Func<TKey, TRight, TOut> nullPadLeftCombine,
        IIndexedZSetTraceCodec<TKey, TLeft, TWeight>? leftSnapshotCodec = null,
        IIndexedZSetTraceCodec<TKey, TRight, TWeight>? rightSnapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
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
        ArgumentNullException.ThrowIfNull(nullPadRightCombine);
        ArgumentNullException.ThrowIfNull(nullPadLeftCombine);

        var output = new Stream<ZSet<TOut, TWeight>>(ZSet<TOut, TWeight>.Empty);
        builder.AddRawOperator(
            new IncrementalFullJoinOp<TKey, TLeft, TRight, TOut, TWeight>(
                left, right, output, joinCombine, nullPadRightCombine, nullPadLeftCombine,
                leftSnapshotCodec, rightSnapshotCodec, frontier, monotoneKey));
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
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null)
        where TKey : notnull
        where TValue : notnull
        where TOut : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(aggregator);

        var output = new Stream<ZSet<(TKey, TOut), Z64>>(ZSet<(TKey, TOut), Z64>.Empty);
        builder.AddRawOperator(
            new IncrementalAggregateOp<TKey, TValue, TOut>(
                input, output, aggregator, snapshotCodec, frontier, monotoneKey));
        return output;
    }
}
