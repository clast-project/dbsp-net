// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit;

/// <summary>
/// A single output handle over a <see cref="ParallelCircuit"/>'s W replicas:
/// <see cref="Current"/> gathers the per-replica outputs into the one result a
/// single circuit would have produced.
/// </summary>
/// <remarks>
/// The gather is a Z-set sum. Because <c>Plus</c> is commutative and
/// associative the result is independent of the order the replicas are read, so
/// it is deterministic regardless of which worker finished first.
/// </remarks>
public sealed class ShardedOutputHandle<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly OutputHandle<ZSet<TKey, TWeight>>[] _shards;

    internal ShardedOutputHandle(OutputHandle<ZSet<TKey, TWeight>>[] shards)
    {
        _shards = shards;
    }

    /// <summary>The replica count <c>W</c> this output is gathered from.</summary>
    public int Workers => _shards.Length;

    /// <summary>
    /// The gathered output of the most recent <see cref="ParallelCircuit.Step"/>:
    /// the Z-set sum of every replica's shard.
    /// </summary>
    public ZSet<TKey, TWeight> Current
    {
        get
        {
            var gathered = new ZSetBuilder<TKey, TWeight>();
            foreach (var shard in _shards)
            {
                gathered.AddRange(shard.Current);
            }

            return gathered.Build();
        }
    }
}
