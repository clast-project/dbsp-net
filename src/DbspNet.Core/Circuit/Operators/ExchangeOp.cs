// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// The all-to-all shuffle: re-partitions a sharded Z-set stream by
/// <c>hash(key) % W</c> so that, after it, every row for a given key is
/// co-located on one worker. One instance runs per replica; they coordinate
/// through a shared <see cref="ExchangeCoordinator{T}"/>.
/// </summary>
/// <remarks>
/// Self-synchronizing: because all replicas run the identical operator
/// sequence, every replica reaches its copy of this operator at the same loop
/// position, so the per-exchange barrier lines up with no external scheduler.
/// Correctness is independent of <c>W</c> — the shuffle is a redistribution, so
/// Z-set-summing the workers' outputs reconstructs the input multiset exactly.
/// </remarks>
internal sealed class ExchangeOp<TKey, TWeight> : IOperator
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TKey, TWeight>> _input;
    private readonly Stream<ZSet<TKey, TWeight>> _output;
    private readonly Func<TKey, int> _partition;
    private readonly ExchangeCoordinator<List<KeyValuePair<TKey, TWeight>>> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal ExchangeOp(
        Stream<ZSet<TKey, TWeight>> input,
        Stream<ZSet<TKey, TWeight>> output,
        Func<TKey, int> partition,
        ExchangeCoordinator<List<KeyValuePair<TKey, TWeight>>> coordinator,
        int worker,
        CancellationToken abort)
    {
        _input = input;
        _output = output;
        _partition = partition;
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

        // Split this shard's rows into one bucket per destination worker. The
        // input is a Z-set, so its keys are already distinct — a bucket never
        // merges, so a plain append-only list suffices (no per-row hash/probe
        // into a builder dictionary). Buckets are allocated lazily; an empty
        // destination publishes null. The key hash is paid once, here, at the
        // gather (vs. the previous build-then-rebuild which hashed every row
        // twice — once per bucket, once at the gather).
        var buckets = new List<KeyValuePair<TKey, TWeight>>?[workers];
        long splitRows = 0;
        foreach (var kv in _input.Current)
        {
            // Non-negative modulo: partition() may return a negative hash.
            var j = ((_partition(kv.Key) % workers) + workers) % workers;
            (buckets[j] ??= new List<KeyValuePair<TKey, TWeight>>()).Add(kv);
            splitRows++;
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

        // Gather our column: the buckets every worker addressed to us, summed.
        // The sum still goes through a builder because a column-dropping map
        // upstream can place the same row on two source workers, so distinct
        // sources may carry the same key.
        var gathered = new ZSetBuilder<TKey, TWeight>();
        long gatherRows = 0;
        for (var src = 0; src < workers; src++)
        {
            var bucket = _coordinator.Read(src, _worker);
            if (bucket is not null)
            {
                gathered.AddRange(bucket);
                gatherRows += bucket.Count;
            }
        }

        _output.SetCurrent(gathered.Build());

        if (profile)
        {
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordGather(_worker, t3 - t0, gatherRows);
        }
    }
}
