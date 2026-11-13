// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// <see cref="ExchangeOp{TKey,TWeight}"/> with a fused pointwise pre-step (docs/design-bounded-memory.md
/// §5.56): each input row is first rewritten (or dropped) by <c>step</c>, then partitioned by its rewritten
/// form and gathered. The upstream map/filter's own Z-set is never materialised.
/// </summary>
/// <remarks>
/// Exact: the step is a pure row function, and two input rows the step maps to the same row land in the
/// same destination (they partition alike) and are summed by the gather, which already sums duplicate keys
/// (a column-dropping map upstream of a plain exchange can place one row on two source workers). Coordination
/// is identical to the plain operator.
/// </remarks>
internal sealed class ExchangeStepOp<TIn, TKey, TWeight> : IOperator
    where TIn : notnull
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TIn, TWeight>> _input;
    private readonly Func<TIn, (bool Keep, TKey Row)> _step;
    private readonly Stream<ZSet<TKey, TWeight>> _output;
    private readonly Func<TKey, int> _partition;
    private readonly ExchangeCoordinator<IReadOnlyList<KeyValuePair<TKey, TWeight>>> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal ExchangeStepOp(
        Stream<ZSet<TIn, TWeight>> input,
        Stream<ZSet<TKey, TWeight>> output,
        Func<TIn, (bool Keep, TKey Row)> step,
        Func<TKey, int> partition,
        ExchangeCoordinator<IReadOnlyList<KeyValuePair<TKey, TWeight>>> coordinator,
        int worker,
        CancellationToken abort)
    {
        _input = input;
        _output = output;
        _step = step;
        _partition = partition;
        _coordinator = coordinator;
        _worker = worker;
        _workers = coordinator.Workers;
        _abort = abort;
    }

    public void Step()
    {
        var workers = _workers;
        var profile = StepProfiler.Active;
        var t0 = profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0L;

        // Map, then split by the mapped row's partition. Two inputs can map to one row; both go to the
        // same destination and the gather sums them.
        var buckets = new IReadOnlyList<KeyValuePair<TKey, TWeight>>?[workers];
        long splitRows = 0;
        var format = _output.DeltaFormat;
        if (format is not null && _input.Current.IsExternal)
        {
            // A block-backed input: mapped here, where each row is decoded anyway, into one spilling
            // builder per destination (in the output's layout), as the plain operator splits (§5.27).
            splitRows = ExchangeBuckets.SplitSpilling(_input.Current, buckets, _step, _partition, format);
        }
        else
        {
            foreach (var kv in _input.Current)
            {
                var (keep, mapped) = _step(kv.Key);
                if (!keep)
                {
                    continue;
                }

                var j = ((_partition(mapped) % workers) + workers) % workers;
                ExchangeBuckets.Add(ref buckets[j], new KeyValuePair<TKey, TWeight>(mapped, kv.Value));
                splitRows++;
            }
        }

        // Publish our row of the grid, then rendezvous: after the barrier every
        // worker's row is visible, so reading our column below is race-free.
        // Publishing null for empty cells still overwrites last tick's payload.
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

        // Gather our column: the buckets every worker addressed to us, summed (the step can map two
        // rows to one, within a source or across sources).
        // Size the gather to the exact row total first. Reads are plain array
        // loads off the mailbox, so the counting pass is W loads; growing from
        // empty instead reallocates the backing ~log2(rows) times, which at
        // exchange widths is the dominant allocation of the gather
        // (docs/design-row-representation.md §16.7). The sum is an upper bound,
        // not exact, only when duplicate keys arrive from two sources (see
        // above) — over-sizing by the duplicate count is the safe direction.
        long gatherRows = 0;
        for (var src = 0; src < workers; src++)
        {
            gatherRows += _coordinator.Read(src, _worker)?.Count ?? 0;
        }

        // With a block format the gather spills past the step threshold like any delta builder
        // (§5.27): a skewed key can land most of a large delta on one worker.
        var column = new IReadOnlyList<KeyValuePair<TKey, TWeight>>?[workers];
        for (var src = 0; src < workers; src++)
        {
            column[src] = _coordinator.Take(src, _worker);
        }

        // Spilled buckets are summed as the union of their runs, never decoded here (§5.35).
        if (format is null || ExchangeBuckets.GatherBlocked(column, format) is not { } result)
        {
            var capacity = (int)Math.Min(gatherRows, int.MaxValue);
            var gathered = format is not null
                ? DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.For<TKey, TWeight>(format, capacity)
                : new ZSetBuilder<TKey, TWeight>(capacity);
            foreach (var bucket in column)
            {
                if (bucket is not null)
                {
                    gathered.AddRange(bucket);
                }
            }

            result = gathered.Build();
        }

        _output.SetCurrent(result);

        if (profile)
        {
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordGather(_worker, t3 - t0, gatherRows);
        }
    }
}
