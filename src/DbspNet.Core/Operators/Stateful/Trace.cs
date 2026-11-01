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

    // The in-memory state; while _cold is set, only the part of the state read so far.
    private readonly ZSet<TKey, TWeight> _state = NewState();

    // Lazy restore (increment 6): the rows not read yet. A row is read the first time WeightOf or
    // Integrate asks for it; Resolved remembers which have been, so none is read twice. Anything
    // that needs every row (Current, GC) promotes the rest first.
    private ColdRows? _cold;

    private sealed class ColdRows(IColdZSet<TKey, TWeight> source)
    {
        public IColdZSet<TKey, TWeight> Source { get; } = source;

        public HashSet<TKey> Resolved { get; } = new();

        public int ResolvedPresent { get; set; }
    }

    public ZSetTrace() => CheckpointTracking.Register(_state, this);

    /// <summary>The whole state. On a lazily restored trace, reads every row not read yet first.</summary>
    public ZSet<TKey, TWeight> Current
    {
        get
        {
            if (_cold is not null)
            {
                Promote();
            }

            return _state;
        }
    }

    /// <summary>The state in memory — the whole state unless the trace was restored lazily and
    /// rows remain unread (persistence saves this, with <see cref="FullState"/> for the rest).</summary>
    internal ZSet<TKey, TWeight> State => _state;

    /// <summary>True while rows remain unread from a lazy restore.</summary>
    internal bool IsCold => _cold is not null;

    /// <summary>Rows read from cold so far (diagnostics and tests).</summary>
    internal int ColdResolvedCount => _cold?.Resolved.Count ?? 0;

    /// <inheritdoc/>
    public long? ExactCount => Count;

    /// <summary>The number of rows, exact, without reading any cold ones.</summary>
    public int Count => _cold is { } c ? c.Source.Count - c.ResolvedPresent + _state.Count : _state.Count;

    /// <summary><paramref name="key"/>'s weight, reading only that row from cold.</summary>
    public TWeight WeightOf(TKey key)
    {
        if (_cold is { } c)
        {
            Resolve(c, key);
        }

        return _state.WeightOf(key);
    }

    /// <summary>Every row, without reading cold rows into memory: the cold ones not read yet, then
    /// the in-memory state.</summary>
    public IEnumerable<KeyValuePair<TKey, TWeight>> EnumerateWhole()
    {
        if (_cold is { } c)
        {
            foreach (var (key, weight) in c.Source.ReadAll())
            {
                if (!c.Resolved.Contains(key) && !TWeight.IsZero(weight))
                {
                    yield return new KeyValuePair<TKey, TWeight>(key, weight);
                }
            }
        }

        foreach (var entry in _state)
        {
            yield return entry;
        }
    }

    /// <summary>While cold: the whole state, built without promoting (for a write that needs all of it).</summary>
    public Func<ZSet<TKey, TWeight>>? FullState => _cold is null ? null : () =>
    {
        var b = new ZSetBuilder<TKey, TWeight>(Count);
        foreach (var (key, weight) in EnumerateWhole())
        {
            b.Add(key, weight);
        }

        return b.Build();
    };

    /// <summary>Take <paramref name="source"/> as the state (empty now) — a lazy restore — and,
    /// if it was restored from a chain, keep extending that chain.</summary>
    internal void AdoptCold(IColdZSet<TKey, TWeight> source)
    {
        Clear();
        _cold = new ColdRows(source);
        if (CheckpointTracking.Restored(source) is { } chain)
        {
            StartTracking(chain);
        }
    }

    private void Resolve(ColdRows cold, TKey key)
    {
        if (!cold.Resolved.Add(key))
        {
            return;
        }

        var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
        var weight = cold.Source.WeightOf(key);
        ColdStats.ReadTime(System.Diagnostics.Stopwatch.GetTimestamp() - t0);
        ColdStats.RowResolve(!TWeight.IsZero(weight));
        if (!TWeight.IsZero(weight))
        {
            // Reading a row is not a change: straight into the state, not the pending change.
            _state.MergeInPlace(ZSet<TKey, TWeight>.Inline(key, weight));
            cold.ResolvedPresent++;
        }
    }

    private void Promote()
    {
        var cold = _cold!;
        var rest = new ZSetBuilder<TKey, TWeight>();
        foreach (var (key, weight) in cold.Source.ReadAll())
        {
            if (!cold.Resolved.Contains(key))
            {
                rest.Add(key, weight);
            }
        }

        _cold = null;
        _state.MergeInPlace(rest.Build());
    }

    /// <summary>What persistence last wrote for this trace, and the change since (null: untracked).</summary>
    public CheckpointTracking<ZSet<TKey, TWeight>>? Tracking => _tracking;

    /// <summary>Start (or restart) tracking: <paramref name="chain"/> describes what was just
    /// written, which equals the whole state; the pending change is now empty.</summary>
    public void StartTracking(object chain) =>
        _tracking = new CheckpointTracking<ZSet<TKey, TWeight>>(chain, ZSet<TKey, TWeight>.NewMutable());

    /// <summary>Forget any chain: the next checkpoint writes the whole trace.</summary>
    public void StopTracking() => _tracking = null;

    /// <summary>Empty the trace (and forget any chain and any cold rows).</summary>
    public void Clear()
    {
        _tracking = null;
        _cold = null;
        if (!_state.IsEmpty)
        {
            _state.MergeInPlace(_state.Negate());
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

        if (CheckpointTracking.Restored(delta) is { } chain && _cold is null && _state.IsEmpty)
        {
            // State restored from a chain: keep extending it instead of rewriting it.
            _state.MergeInPlace(delta);
            StartTracking(chain);
            return;
        }

        if (_cold is { } cold)
        {
            // Rows the delta changes are read first, so it adds to what was there.
            foreach (var (key, _) in delta)
            {
                Resolve(cold, key);
            }
        }

        _state.MergeInPlace(delta);
        if (_tracking?.Pending is { } pending)
        {
            pending.MergeInPlace(delta);
            _tracking.CheckOverflow(pending.Count, Count);
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
        var state = Current; // GC scans every row: read any still cold
        if (_tracking?.Pending is { } pending)
        {
            // The dropped entries leave the state, so they enter the change negated.
            var dropped = new ZSetBuilder<TKey, TWeight>();
            foreach (var (key, weight) in state.Entries())
            {
                if (monotoneKey(key) < threshold)
                {
                    dropped.Add(key, TWeight.Negate(weight));
                }
            }

            pending.MergeInPlace(dropped.Build());
            _tracking.CheckOverflow(pending.Count, state.Count);
        }

        return state.RemoveKeysBelow(threshold, monotoneKey);
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
