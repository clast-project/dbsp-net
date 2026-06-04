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

        var buckets = new ZSetBuilder<TKey, TWeight>[workers];
        for (var j = 0; j < workers; j++)
        {
            buckets[j] = new ZSetBuilder<TKey, TWeight>();
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
}
