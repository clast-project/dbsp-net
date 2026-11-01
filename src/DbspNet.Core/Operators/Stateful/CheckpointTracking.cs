// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>A trace that can be checkpointed incrementally (<see cref="ZSetTrace{TKey,TWeight}"/>,
/// <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>).</summary>
internal interface ICheckpointTracked
{
}

/// <summary>
/// Incremental checkpoints (docs/design-durable-state.md §1f, increment 3): what persistence last
/// wrote for one trace (<see cref="Chain"/>, opaque to Core — the list of runs whose sum is the
/// state as written) and the sum of every change to the trace since (<see cref="Pending"/>). A
/// checkpoint writes <see cref="Pending"/> as one more run and restarts tracking with the longer
/// chain; with no pending change (it grew past half the state, or a drop could not be recorded)
/// the checkpoint writes the whole trace instead.
/// </summary>
internal sealed class CheckpointTracking<TPending>
    where TPending : class
{
    /// <summary>Below this many entries a pending change never counts as overflowing.</summary>
    internal const int OverflowFloor = 4096;

    internal CheckpointTracking(object chain, TPending pending)
    {
        Chain = chain;
        Pending = pending;
    }

    /// <summary>Persistence's record of what it wrote; Core never looks inside.</summary>
    public object Chain { get; }

    /// <summary>Σ changes since <see cref="Chain"/> was written; null once it has overflowed.</summary>
    public TPending? Pending { get; private set; }

    /// <summary>Stop recording once the change is over half the state: the next checkpoint
    /// rewrites the trace whole, and the memory is released now rather than held until then.</summary>
    internal void CheckOverflow(int pendingSize, int stateSize)
    {
        if (pendingSize > OverflowFloor && pendingSize > stateSize / 2)
        {
            Pending = null;
        }
    }
}

/// <summary>Side tables linking a trace's state to its trace, and a restored collection to the
/// chain it was read from — side tables because a field on every Z-set costs 8 bytes each.</summary>
internal static class CheckpointTracking
{
    private static readonly ConditionalWeakTable<object, ICheckpointTracked> Traces = new();
    private static readonly ConditionalWeakTable<object, object> RestoredChains = new();
    private static volatile bool _anyRestored;

    /// <summary>Called once per trace, at construction.</summary>
    internal static void Register(object state, ICheckpointTracked trace) => Traces.AddOrUpdate(state, trace);

    /// <summary>The trace whose state is <paramref name="state"/>, or null.</summary>
    internal static ICheckpointTracked? TraceOf(object state) => Traces.TryGetValue(state, out var t) ? t : null;

    /// <summary>Persistence read <paramref name="loaded"/> from <paramref name="chain"/>: a trace
    /// that integrates it into empty state adopts the chain, so its next checkpoint is a delta.</summary>
    internal static void MarkRestored(object loaded, object chain)
    {
        RestoredChains.AddOrUpdate(loaded, chain);
        _anyRestored = true;
    }

    /// <summary>The chain <paramref name="delta"/> was restored from, or null (one static read
    /// unless anything was ever restored from a chain).</summary>
    internal static object? Restored(object delta)
    {
        if (!_anyRestored || !RestoredChains.TryGetValue(delta, out var chain))
        {
            return null;
        }

        RestoredChains.Remove(delta); // adopted once
        return chain;
    }
}

/// <summary>A tracked Z-set state: what persistence reads and restarts at a checkpoint.</summary>
internal interface IZSetCheckpointTracked<TKey, TWeight> : ICheckpointTracked
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    CheckpointTracking<ZSet<TKey, TWeight>>? Tracking { get; }

    /// <summary>The state as just written is the sum of <paramref name="chain"/>; restart the pending change.</summary>
    void StartTracking(object chain);

    /// <summary>When set, runs of this state are ordered by this hash instead of the row's own
    /// (a partitioned operator: its partition key's hash, so a partition is one block read).</summary>
    Func<TKey, int>? OrderHash => null;

    /// <summary>When set, the attached snapshot may be partial (a lazily restored operator) and
    /// this produces the whole state, for a write that needs all of it.</summary>
    Func<ZSet<TKey, TWeight>>? FullState => null;

    /// <summary>The whole state's exact size when known without reading it (a lazily restored trace
    /// knows it); null otherwise. Recorded with the chain, so the next lazy restore knows it too.</summary>
    long? ExactCount => null;
}

/// <summary>A tracked indexed state; see <see cref="IZSetCheckpointTracked{TKey,TWeight}"/>.</summary>
internal interface IIndexedCheckpointTracked<TKey, TValue, TWeight> : ICheckpointTracked
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    CheckpointTracking<IndexedZSet<TKey, TValue, TWeight>>? Tracking { get; }

    void StartTracking(object chain);
}

/// <summary>
/// Change tracking for an operator whose state lives outside a trace but whose snapshot is the
/// integral of its input (the partitioned TOP-K / rank / offset / window operators): it records
/// each step's input delta and any rows GC drops, and the operator tags the snapshot it builds
/// with <see cref="Attach"/> so persistence can find the pending change for it.
/// </summary>
internal sealed class ZSetChangeTracker<TKey, TWeight> : IZSetCheckpointTracked<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private CheckpointTracking<ZSet<TKey, TWeight>>? _tracking;
    private int _stateSize;
    private int _attachedSize;

    public CheckpointTracking<ZSet<TKey, TWeight>>? Tracking => _tracking;

    public Func<TKey, int>? OrderHash { get; private set; }

    public Func<ZSet<TKey, TWeight>>? FullState { get; private set; }

    public void StartTracking(object chain)
    {
        _tracking = new CheckpointTracking<ZSet<TKey, TWeight>>(chain, ZSet<TKey, TWeight>.NewMutable());
        _stateSize = _attachedSize;
    }

    /// <summary>The operator integrated <paramref name="delta"/> into its state.</summary>
    public void Record(ZSet<TKey, TWeight> delta)
    {
        if (_tracking?.Pending is { } pending && !delta.IsEmpty)
        {
            pending.MergeInPlace(delta);
            _tracking.CheckOverflow(pending.Count, _stateSize);
        }
    }

    /// <summary>The operator dropped <paramref name="row"/> (held at <paramref name="weight"/>) from its state.</summary>
    public void RecordDropped(TKey row, TWeight weight)
    {
        if (_tracking?.Pending is { } pending)
        {
            pending.MergeInPlace(ZSet<TKey, TWeight>.Inline(row, TWeight.Negate(weight)));
            _tracking.CheckOverflow(pending.Count, _stateSize);
        }
    }

    /// <summary><paramref name="snapshot"/> is the operator's state, about to be saved — or, with
    /// <paramref name="fullState"/>, the part of it in memory (<paramref name="size"/> rows in all).</summary>
    public void Attach(
        ZSet<TKey, TWeight> snapshot, Func<TKey, int>? orderHash = null, Func<ZSet<TKey, TWeight>>? fullState = null, long? size = null)
    {
        _attachedSize = (int)Math.Min(int.MaxValue, size ?? snapshot.Count);
        OrderHash = orderHash;
        FullState = fullState;
        CheckpointTracking.Register(snapshot, this);
    }

    /// <summary>The operator rebuilt its state from <paramref name="loaded"/>: keep extending the
    /// chain it was read from, if any; otherwise forget any chain.</summary>
    public void Restored(ZSet<TKey, TWeight> loaded) => Restored(loaded, loaded.Count);

    /// <summary>As <see cref="Restored(ZSet{TKey,TWeight})"/>, for state restored cold
    /// (<paramref name="loaded"/> is the cold source, <paramref name="size"/> its rows).</summary>
    public void Restored(object loaded, long size)
    {
        if (CheckpointTracking.Restored(loaded) is { } chain)
        {
            _attachedSize = (int)Math.Min(int.MaxValue, size);
            StartTracking(chain);
        }
        else
        {
            _tracking = null;
        }
    }
}
