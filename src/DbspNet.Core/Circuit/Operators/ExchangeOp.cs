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
    private readonly ExchangeCoordinator<ZSet<TKey, TWeight>> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal ExchangeOp(
        Stream<ZSet<TKey, TWeight>> input,
        Stream<ZSet<TKey, TWeight>> output,
        Func<TKey, int> partition,
        ExchangeCoordinator<ZSet<TKey, TWeight>> coordinator,
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

        // Split this shard's rows into one bucket per destination worker.
        var buckets = new ZSetBuilder<TKey, TWeight>[workers];
        for (var j = 0; j < workers; j++)
        {
            buckets[j] = new ZSetBuilder<TKey, TWeight>();
        }

        foreach (var (key, weight) in _input.Current)
        {
            // Non-negative modulo: partition() may return a negative hash.
            var j = ((_partition(key) % workers) + workers) % workers;
            buckets[j].Add(key, weight);
        }

        // Publish our row of the grid, then rendezvous: after the barrier every
        // worker's row is visible, so reading our column below is race-free.
        for (var j = 0; j < workers; j++)
        {
            _coordinator.Publish(_worker, j, buckets[j].Build());
        }

        _coordinator.Wait(_abort);

        // Gather our column: the buckets every worker addressed to us, summed.
        var gathered = new ZSetBuilder<TKey, TWeight>();
        for (var src = 0; src < workers; src++)
        {
            gathered.AddRange(_coordinator.Read(src, _worker));
        }

        _output.SetCurrent(gathered.Build());
    }
}
