// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The consolidated Z-set state of DISTINCT and of a stored output view
/// (docs/design-flat-trace-state.md §3.7): a managed <see cref="ZSetTrace{TKey,TWeight}"/>
/// (<see cref="ManagedRowTrace{TKey,TWeight}"/>), or, for structural rows under
/// <c>DBSPNET_TRACE_BLOCKS</c>, one held as blocks (<see cref="Blocks.BlockedZSetTrace"/>).
/// </summary>
internal interface IRowTrace<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>Rows with a non-zero weight.</summary>
    int Count { get; }

    /// <summary>Rows held in memory (metrics).</summary>
    long RetainedRows { get; }

    /// <summary>True while rows remain unread from a lazy restore.</summary>
    bool IsCold { get; }

    /// <summary>The whole state, valid until the next change.</summary>
    ZSet<TKey, TWeight> Current { get; }

    TWeight WeightOf(TKey key);

    void Integrate(ZSet<TKey, TWeight> delta);

    /// <summary>Every row with a non-zero weight, without reading a lazily restored state into memory.</summary>
    IEnumerable<KeyValuePair<TKey, TWeight>> EnumerateWhole();

    /// <summary>Drop rows whose projection is below <paramref name="threshold"/>; the number dropped
    /// when known now (a blocked trace drops lazily and reports 0).</summary>
    int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey);

    /// <summary>A tick with no input: a blocked trace freezes.</summary>
    void Idle();

    ValueTask SaveAsync(IZSetTraceCodec<TKey, TWeight> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken);

    /// <summary>Restore into an empty trace; <paramref name="lazy"/> lets a managed trace stay cold.</summary>
    ValueTask LoadAsync(
        IZSetTraceCodec<TKey, TWeight> codec, ISnapshotReader reader, string file, bool lazy, CancellationToken cancellationToken);
}

/// <summary>The managed <see cref="IRowTrace{TKey,TWeight}"/>: a <see cref="ZSetTrace{TKey,TWeight}"/>.</summary>
internal sealed class ManagedRowTrace<TKey, TWeight> : IRowTrace<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    internal ZSetTrace<TKey, TWeight> Trace { get; } = new();

    public int Count => Trace.Count;

    public long RetainedRows => Trace.State.Count;

    public bool IsCold => Trace.IsCold;

    public ZSet<TKey, TWeight> Current => Trace.Current;

    public TWeight WeightOf(TKey key) => Trace.WeightOf(key);

    public void Integrate(ZSet<TKey, TWeight> delta) => Trace.Integrate(delta);

    public IEnumerable<KeyValuePair<TKey, TWeight>> EnumerateWhole() => Trace.EnumerateWhole();

    public int DropKeysBelow(long threshold, Func<TKey, long> monotoneKey) => Trace.DropKeysBelow(threshold, monotoneKey);

    public void Idle()
    {
    }

    public ValueTask SaveAsync(IZSetTraceCodec<TKey, TWeight> codec, ISnapshotWriter writer, string file, CancellationToken cancellationToken) =>
        codec.SaveAsync(writer, file, Trace.State, cancellationToken);

    public async ValueTask LoadAsync(
        IZSetTraceCodec<TKey, TWeight> codec, ISnapshotReader reader, string file, bool lazy, CancellationToken cancellationToken)
    {
        // Lazily (increment 6): a row-ordered chain stays cold until rows are asked for.
        if (lazy
            && codec is IColdZSetLoader<TKey, TWeight> coldLoader
            && await coldLoader.LoadColdZSetAsync(reader, file, cancellationToken).ConfigureAwait(false) is { } cold)
        {
            Trace.AdoptCold(cold);
            return;
        }

        var loaded = await codec.LoadAsync(reader, file, cancellationToken).ConfigureAwait(false);

        // Reset the (in-place-mutated) trace to the persisted contents.
        var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
        Trace.Clear();
        Trace.Integrate(loaded);
        if (SnapshotRestoreProfile.Enabled)
        {
            SnapshotRestoreProfile.AddIntegrate(SnapshotRestoreProfile.MsSince(t0));
        }
    }
}
