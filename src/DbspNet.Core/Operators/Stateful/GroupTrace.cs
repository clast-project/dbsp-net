// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// A keyed trace read a group at a time: what an outer join needs from each side and an aggregate
/// from its input — a key's <b>consolidated</b> group (match presence and aggregate values depend on
/// net weights, so cancelled entries must cancel), integration, frontier GC, and its snapshot.
/// Managed by default; held as blocks under <c>DBSPNET_TRACE_BLOCKS</c>
/// (docs/design-flat-trace-state.md §3.3–§3.4).
/// </summary>
internal interface IGroupTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>The key's entries with their net weights (zeros dropped).</summary>
    ZSet<TValue, TWeight> GroupFor(TKey key);

    /// <summary>The same group as a read-only view, which a blocked trace answers without decoding
    /// (lookups, sums, emptiness); a managed trace returns <see cref="GroupFor"/>.</summary>
    IMultiset<TValue, TWeight> GroupView(TKey key);

    void Integrate(IndexedZSet<TKey, TValue, TWeight> delta);

    /// <summary>Drop keys below the frontier. Returns the keys dropped, or <see langword="null"/>
    /// when the trace drops lazily and cannot list them (a caller with per-key caches then scans
    /// those itself).</summary>
    IReadOnlyList<TKey>? DropKeysBelow(long threshold, Func<TKey, long> monotoneKey);

    /// <summary>Whether the state came back lazily from a snapshot, so per-key caches should be
    /// rebuilt on first touch rather than by scanning the whole trace.</summary>
    bool IsLazy { get; }

    int GroupCount { get; }

    /// <summary>A tick that brought this operator nothing (a blocked trace freezes then).</summary>
    void Idle();

    ValueTask SaveAsync(IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken);

    ValueTask LoadAsync(IIndexedZSetTraceCodec<TKey, TValue, TWeight> codec, ISnapshotReader reader, string file, CancellationToken cancellationToken);
}

/// <summary>The managed <see cref="IGroupTrace{TKey,TValue,TWeight}"/>: an <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/>.</summary>
internal sealed class ManagedGroupTrace<TKey, TValue, TWeight> : IGroupTrace<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly IndexedZSetTrace<TKey, TValue, TWeight> _trace = new();

    /// <summary>The trace's state (tests reach it by reflection, as on the unwrapped trace).</summary>
    public IndexedZSet<TKey, TValue, TWeight> Current => _trace.Current;

    public ZSet<TValue, TWeight> GroupFor(TKey key) => _trace.Current.GroupFor(key);

    public IMultiset<TValue, TWeight> GroupView(TKey key) => _trace.Current.GroupFor(key);

    public void Integrate(IndexedZSet<TKey, TValue, TWeight> delta) => _trace.Integrate(delta);

    public IReadOnlyList<TKey>? DropKeysBelow(long threshold, Func<TKey, long> monotoneKey) => _trace.DropKeysBelow(threshold, monotoneKey);

    public bool IsLazy => _trace.Current.IsCold;

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
