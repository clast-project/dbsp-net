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
internal sealed class ZSetTrace<TKey, TWeight> : IZSetCheckpointTracked<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    // Incremental checkpoints (docs/design-durable-state.md §1f, increment 3): null until
    // persistence starts a chain; then the sum of every change since the chain was last written.
    private CheckpointTracking<ZSet<TKey, TWeight>>? _tracking;

    public ZSetTrace() => CheckpointTracking.Register(Current, this);

    public ZSet<TKey, TWeight> Current { get; } = NewState();

    /// <summary>What persistence last wrote for this trace, and the change since (null: untracked).</summary>
    public CheckpointTracking<ZSet<TKey, TWeight>>? Tracking => _tracking;

    /// <summary>Start (or restart) tracking: <paramref name="chain"/> describes what was just
    /// written, which equals <see cref="Current"/>; the pending change is now empty.</summary>
    public void StartTracking(object chain) =>
        _tracking = new CheckpointTracking<ZSet<TKey, TWeight>>(chain, ZSet<TKey, TWeight>.NewMutable());

    /// <summary>Forget any chain: the next checkpoint writes the whole trace.</summary>
    public void StopTracking() => _tracking = null;

    /// <summary>Empty the trace (and forget any chain).</summary>
    public void Clear()
    {
        _tracking = null;
        if (!Current.IsEmpty)
        {
            Current.MergeInPlace(Current.Negate());
        }
    }

    private static ZSet<TKey, TWeight> NewState()
    {
        var z = ZSet<TKey, TWeight>.NewMutable();
        if (TraceAccessProfile.Enabled)
        {
            z.MarkTraceState();
        }

        return z;
    }

    public void Integrate(ZSet<TKey, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        if (CheckpointTracking.Restored(delta) is { } chain && Current.IsEmpty)
        {
            // State restored from a chain: keep extending it instead of rewriting it.
            Current.MergeInPlace(delta);
            StartTracking(chain);
            return;
        }

        Current.MergeInPlace(delta);
        if (_tracking?.Pending is { } pending)
        {
            pending.MergeInPlace(delta);
            _tracking.CheckOverflow(pending.Count, Current.Count);
        }
    }

    /// <summary>
    /// Drops every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/> (frontier-driven GC),
    /// returning the number of keys removed. Scans the whole trace, so callers
    /// should only invoke it when the frontier has actually advanced. The
    /// non-indexed analogue of <see cref="IndexedZSetTrace{TKey,TValue,TWeight}.DropKeysBelow"/>,
    /// used by DISTINCT (where the key is the whole row).
    /// </summary>
    public int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        if (_tracking?.Pending is { } pending)
        {
            // The dropped entries leave the state, so they enter the change negated.
            var dropped = new ZSetBuilder<TKey, TWeight>();
            foreach (var (key, weight) in Current.Entries())
            {
                if (monotoneKey(key) < threshold)
                {
                    dropped.Add(key, TWeight.Negate(weight));
                }
            }

            pending.MergeInPlace(dropped.Build());
            _tracking.CheckOverflow(pending.Count, Current.Count);
        }

        return Current.RemoveKeysBelow(threshold, monotoneKey);
    }
}

/// <summary>
/// In-memory trace over an indexed Z-set. Like <see cref="ZSetTrace{TKey,TWeight}"/>,
/// <see cref="Integrate"/> mutates the backing state in place — cheap on the
/// common case (one key touched) and independent of total state size.
/// </summary>
internal sealed class IndexedZSetTrace<TKey, TValue, TWeight> : IIndexedCheckpointTracked<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private CheckpointTracking<IndexedZSet<TKey, TValue, TWeight>>? _tracking;

    public IndexedZSetTrace() => CheckpointTracking.Register(Current, this);

    public IndexedZSet<TKey, TValue, TWeight> Current { get; } = NewState();

    /// <summary>What persistence last wrote for this trace, and the change since (null: untracked).</summary>
    public CheckpointTracking<IndexedZSet<TKey, TValue, TWeight>>? Tracking => _tracking;

    /// <summary>Start (or restart) tracking; see <see cref="ZSetTrace{TKey,TWeight}.StartTracking"/>.</summary>
    public void StartTracking(object chain) =>
        _tracking = new CheckpointTracking<IndexedZSet<TKey, TValue, TWeight>>(chain, IndexedZSet<TKey, TValue, TWeight>.NewMutable());

    /// <summary>Forget any chain: the next checkpoint writes the whole trace.</summary>
    public void StopTracking() => _tracking = null;

    private static IndexedZSet<TKey, TValue, TWeight> NewState()
    {
        var z = IndexedZSet<TKey, TValue, TWeight>.NewMutable();
        if (TraceAccessProfile.Enabled)
        {
            z.MarkTraceState();
        }

        return z;
    }

    public void Integrate(IndexedZSet<TKey, TValue, TWeight> delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.IsEmpty)
        {
            return;
        }

        if (CheckpointTracking.Restored(delta) is { } chain && Current.IsEmpty)
        {
            // Restored state: take over its cold source if it was loaded lazily (increment 4)
            // rather than reading it all, and keep extending the chain it came from.
            if (!Current.TryAdoptColdFrom(delta))
            {
                Current.MergeInPlace(delta);
            }

            StartTracking(chain);
            return;
        }

        Current.MergeInPlace(delta);
        if (_tracking?.Pending is { } pending)
        {
            pending.MergeInPlace(delta);
            _tracking.CheckOverflow(pending.GroupCount, Current.GroupCount);
        }
    }

    /// <summary>
    /// Drops every key whose <paramref name="monotoneKey"/> projection is
    /// strictly below <paramref name="threshold"/> (frontier-driven GC),
    /// returning the removed keys so the owning operator can drop any parallel
    /// per-key caches. Scans the whole trace, so callers should only invoke it
    /// when the frontier has actually advanced.
    /// </summary>
    public IReadOnlyList<TKey> DropKeysBelow(long threshold, Func<TKey, long> monotoneKey)
    {
        if (_tracking?.Pending is { } pending)
        {
            var dropped = new IndexedZSetBuilder<TKey, TValue, TWeight>();
            foreach (var (key, group) in Current)
            {
                if (monotoneKey(key) < threshold)
                {
                    foreach (var (value, weight) in group)
                    {
                        dropped.Add(key, value, TWeight.Negate(weight));
                    }
                }
            }

            pending.MergeInPlace(dropped.Build());
            _tracking.CheckOverflow(pending.GroupCount, Current.GroupCount);
        }

        return Current.RemoveKeysBelow(threshold, monotoneKey);
    }
}
