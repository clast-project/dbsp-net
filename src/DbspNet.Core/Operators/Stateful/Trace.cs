// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// In-memory, consolidated trace over a Z-set: holds the running integral of
/// all deltas folded in so far. <see cref="Integrate"/> mutates the trace's
/// backing dictionary in place, so <see cref="Current"/> is a stable
/// reference whose contents change across ticks. Callers must read
/// <see cref="Current"/> within a single <c>Step</c>, before the associated
/// <see cref="Integrate"/> call — never retain the reference across ticks.
/// </summary>
/// <remarks>
/// Each trace owns its own fresh backing dictionary — <c>ZSet.Empty</c> is a
/// shared singleton and must not be used as a mutable starting state.
/// </remarks>
internal sealed class ZSetTrace<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    public ZSet<TKey, TWeight> Current { get; } =
        new ZSet<TKey, TWeight>(new Dictionary<TKey, TWeight>());

    public void Integrate(ZSet<TKey, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        Current.MergeInPlace(delta);
    }

    /// <summary>
    /// Drops every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/> (frontier-driven GC),
    /// returning the number of keys removed. Scans the whole trace, so callers
    /// should only invoke it when the frontier has actually advanced. The
    /// non-indexed analogue of <see cref="IndexedZSetTrace{TKey,TValue,TWeight}.DropKeysBelow"/>,
    /// used by DISTINCT (where the key is the whole row).
    /// </summary>
    public int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey) =>
        Current.RemoveKeysBelow(threshold, monotoneKey);
}

/// <summary>
/// In-memory trace over an indexed Z-set. Like <see cref="ZSetTrace{TKey,TWeight}"/>,
/// <see cref="Integrate"/> mutates the backing state in place — cheap on the
/// common case (one key touched) and independent of total state size.
/// </summary>
internal sealed class IndexedZSetTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    public IndexedZSet<TKey, TValue, TWeight> Current { get; } =
        new IndexedZSet<TKey, TValue, TWeight>(new Dictionary<TKey, ZSet<TValue, TWeight>>());

    public void Integrate(IndexedZSet<TKey, TValue, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        Current.MergeInPlace(delta);
    }

    /// <summary>
    /// Drops every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/> (frontier-driven GC),
    /// returning the removed keys so the owning operator can drop any parallel
    /// per-key caches. Scans the whole trace, so callers should only invoke it
    /// when the frontier has actually advanced.
    /// </summary>
    public IReadOnlyList<TKey> DropKeysBelow(long threshold, Func<TKey, long> monotoneKey) =>
        Current.RemoveKeysBelow(threshold, monotoneKey);
}
