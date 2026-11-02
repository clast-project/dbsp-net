// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// What an outer join needs from each side's trace: a key's <b>consolidated</b> group (match
/// presence decides between joined and NULL-padded output, so cancelled entries must cancel),
/// integration, frontier GC, and its snapshot. Managed by default; held as blocks under
/// <c>DBSPNET_TRACE_BLOCKS=join</c> (docs/design-flat-trace-state.md §3.3).
/// </summary>
internal interface IJoinTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>The key's entries with their net weights (zeros dropped).</summary>
    ZSet<TValue, TWeight> GroupFor(TKey key);

    void Integrate(IndexedZSet<TKey, TValue, TWeight> delta);

    /// <summary>Drop keys below the frontier; returns how many were dropped now (a blocked trace
    /// drops lazily and counts when the entries go).</summary>
    int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey);

    int GroupCount { get; }

    /// <summary>A tick that brought this operator nothing (a blocked trace freezes then).</summary>
    void Idle();

    ValueTask SaveAsync(IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken);

    ValueTask LoadAsync(IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec, ISnapshotReader reader, string file, CancellationToken cancellationToken);
}

/// <summary>The managed <see cref="IJoinTrace{TKey,TValue,TWeight}"/>: an <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>.</summary>
internal sealed class ManagedJoinTrace<TKey, TValue, TWeight> : IJoinTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly IndexedZSetTrace<TKey, TValue, TWeight> _trace = new();

    /// <summary>The trace's state (tests reach it by reflection, as on the unwrapped trace).</summary>
    public IndexedZSet<TKey, TValue, TWeight> Current => _trace.Current;

    public ZSet<TValue, TWeight> GroupFor(TKey key) => _trace.Current.GroupFor(key);

    public void Integrate(IndexedZSet<TKey, TValue, TWeight> delta) => _trace.Integrate(delta);

    public int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey) => _trace.DropKeysBelow(threshold, monotoneKey).Count;

    public int GroupCount => _trace.Current.GroupCount;

    public void Idle()
    {
    }

    public ValueTask SaveAsync(IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken) =>
        codec.SaveAsync(writer, file, _trace.Current, cancellationToken);

    public async ValueTask LoadAsync(IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec, ISnapshotReader reader, string file, CancellationToken cancellationToken)
    {
        var state = await codec.LoadAsync(reader, file, cancellationToken).ConfigureAwait(false);
        var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
        _trace.Integrate(state);
        if (SnapshotRestoreProfile.Enabled)
        {
            SnapshotRestoreProfile.AddIntegrate(SnapshotRestoreProfile.MsSince(t0));
        }
    }
}
