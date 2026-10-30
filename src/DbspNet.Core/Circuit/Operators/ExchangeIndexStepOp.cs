// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// <see cref="ExchangeIndexOp{TKey,TRow,TWeight}"/> with a fused pointwise pre-step: each
/// input row is first rewritten (or dropped) by <c>step</c>, then partitioned by its
/// rewritten form and gathered straight into the key index. The upstream map/filter's
/// own Z-set is never materialised (docs/design-state-representation.md §9.13).
/// </summary>
/// <remarks>
/// Exact: the step is a pure row function, and two input rows the step maps to the same
/// row land in the same bucket (they partition alike) and merge in the gather's
/// <see cref="IndexedZSetBuilder{TKey,TValue,TWeight}"/>, which accumulates weight per
/// (key, row) and drops zeros. Coordination is identical to the plain operator.
/// </remarks>
internal sealed class ExchangeIndexStepOp<TKey, TIn, TRow, TWeight> : IOperator
    where TKey : notnull
    where TIn : notnull
    where TRow : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TIn, TWeight>> _input;
    private readonly Stream<IndexedZSet<TKey, TRow, TWeight>> _output;
    private readonly Func<TIn, (bool Keep, TRow Row)> _step;
    private readonly Func<TRow, int> _partition;
    private readonly Func<TRow, TKey> _keyOf;
    private readonly ExchangeCoordinator<IReadOnlyList<KeyValuePair<TRow, TWeight>>> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal ExchangeIndexStepOp(
        Stream<ZSet<TIn, TWeight>> input,
        Stream<IndexedZSet<TKey, TRow, TWeight>> output,
        Func<TIn, (bool Keep, TRow Row)> step,
        Func<TRow, int> partition,
        Func<TRow, TKey> keyOf,
        ExchangeCoordinator<IReadOnlyList<KeyValuePair<TRow, TWeight>>> coordinator,
        int worker,
        CancellationToken abort)
    {
        _input = input;
        _output = output;
        _step = step;
        _partition = partition;
        _keyOf = keyOf;
        _coordinator = coordinator;
        _worker = worker;
        _workers = coordinator.Workers;
        _abort = abort;
    }

    public void Step()
    {
        var workers = _workers;
        var profile = StepProfiler.Enabled;
        var t0 = profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0L;

        // Map, then split by the mapped row's partition. Two inputs can map to one row;
        // both go to the same bucket and merge in the gather.
        var buckets = new IReadOnlyList<KeyValuePair<TRow, TWeight>>?[workers];
        long splitRows = 0;
        foreach (var (row, weight) in _input.Current)
        {
            var (keep, mapped) = _step(row);
            if (!keep)
            {
                continue;
            }

            var j = ((_partition(mapped) % workers) + workers) % workers;
            ExchangeBuckets.Add(ref buckets[j], new(mapped, weight));
            splitRows++;
        }

        for (var j = 0; j < workers; j++)
        {
            _coordinator.Publish(_worker, j, buckets[j]!);
        }

        if (profile)
        {
            var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordSplit(_worker, t1 - t0, splitRows);
            t0 = t1;
        }

        _coordinator.Wait(_abort);

        if (profile)
        {
            var t2 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordWait(_worker, t2 - t0);
            t0 = t2;
        }

        var indexed = new IndexedZSetBuilder<TKey, TRow, TWeight>();
        long gatherRows = 0;
        for (var src = 0; src < workers; src++)
        {
            var bucket = _coordinator.Read(src, _worker);
            if (bucket is not null)
            {
                foreach (var (row, weight) in ExchangeBuckets.Items(bucket))
                {
                    indexed.Add(_keyOf(row), row, weight);
                    gatherRows++;
                }
            }
        }

        _output.SetCurrent(indexed.Build());

        if (profile)
        {
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordGather(_worker, t3 - t0, gatherRows);
        }
    }
}
