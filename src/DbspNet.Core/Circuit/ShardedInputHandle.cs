// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit;

/// <summary>
/// A single input handle over a <see cref="ParallelCircuit"/>'s W replicas: on
/// <see cref="Push"/> it splits a Z-set by <c>partition(key) % W</c> and delivers
/// each shard to the matching replica's <see cref="InputHandle{T}"/>. The host
/// pushes one logical input; the driver shards it.
/// </summary>
/// <remarks>
/// The partition may be any deterministic function — even round-robin —
/// because correctness is independent of <c>W</c>: a downstream
/// <c>exchange</c> re-shards by whatever key a key-sensitive operator needs.
/// Per-tick accumulation is the replica handles' own merge function (e.g.
/// <c>Plus</c>): two pushes before a step are merged per shard, exactly as a
/// single circuit would merge them.
/// </remarks>
public sealed class ShardedInputHandle<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly InputHandle<ZSet<TKey, TWeight>>[] _shards;
    private readonly Func<TKey, int> _partition;

    internal ShardedInputHandle(InputHandle<ZSet<TKey, TWeight>>[] shards, Func<TKey, int> partition)
    {
        _shards = shards;
        _partition = partition;
    }

    /// <summary>The replica count <c>W</c> this input is split across.</summary>
    public int Workers => _shards.Length;

    /// <summary>
    /// Split <paramref name="delta"/> by <c>partition(key) % W</c> and queue each
    /// shard on its replica for the next <see cref="ParallelCircuit.Step"/>.
    /// </summary>
    public void Push(ZSet<TKey, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        var workers = _shards.Length;
        if (workers == 1)
        {
            _shards[0].Push(delta);
            return;
        }

        // A hash split sends ~delta/W rows to each bucket; size to that rather
        // than growing all W backings from empty every push. Skew costs a
        // resize on the heavy bucket, which is what growing from empty already
        // paid on every bucket.
        var perBucket = (delta.Count / workers) + 1;
        var buckets = new ZSetBuilder<TKey, TWeight>[workers];
        for (var j = 0; j < workers; j++)
        {
            buckets[j] = new ZSetBuilder<TKey, TWeight>(perBucket);
        }

        foreach (var (key, weight) in delta)
        {
            // Non-negative modulo: partition() may return a negative hash.
            var j = ((_partition(key) % workers) + workers) % workers;
            buckets[j].Add(key, weight);
        }

        for (var j = 0; j < workers; j++)
        {
            var shard = buckets[j].Build();
            if (!shard.IsEmpty)
            {
                _shards[j].Push(shard);
            }
        }
    }

    /// <summary>Below this many rows <see cref="PushRows"/> splits on the calling thread.</summary>
    public const int ParallelSplitThreshold = 16_384;

    /// <summary>
    /// Split the rows <c>keys[0..count)</c> (weights alongside) by
    /// <c>partition(key) % W</c> and queue each shard on its replica — the same
    /// shards <see cref="Push"/> would queue for those rows built into one Z-set,
    /// without building it: equal keys share a partition, so each bucket merges its
    /// own duplicates. Large inputs split across up to <c>W</c> threads: one pass
    /// over row ranges computes every row's partition, then one thread per replica
    /// builds that replica's bucket from its rows in input order.
    /// </summary>
    public void PushRows(TKey[] keys, TWeight[] weights, int count)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(weights);
        if (count > keys.Length || count > weights.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        var workers = _shards.Length;
        if (workers == 1 || count < ParallelSplitThreshold)
        {
            var b = new ZSetBuilder<TKey, TWeight>(count);
            for (var i = 0; i < count; i++)
            {
                b.Add(keys[i], weights[i]);
            }

            Push(b.Build());
            return;
        }

        var target = new int[count];
        var options = new ParallelOptions { MaxDegreeOfParallelism = workers };
        Parallel.ForEach(
            System.Collections.Concurrent.Partitioner.Create(0, count, Math.Max(4096, count / (workers * 4))),
            options,
            range =>
            {
                for (var i = range.Item1; i < range.Item2; i++)
                {
                    target[i] = ((_partition(keys[i]) % workers) + workers) % workers;
                }
            });

        var perBucket = (count / workers) + 1;
        Parallel.For(0, workers, options, j =>
        {
            var b = new ZSetBuilder<TKey, TWeight>(perBucket);
            for (var i = 0; i < count; i++)
            {
                if (target[i] == j)
                {
                    b.Add(keys[i], weights[i]);
                }
            }

            var shard = b.Build();
            if (!shard.IsEmpty)
            {
                _shards[j].Push(shard);
            }
        });
    }
}
