// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Core.Operators.Nested;

/// <summary>
/// Enables spine-backed import traces for a nested fixpoint. Pass to
/// <see cref="NestedOperators.SemiNaiveFixpoint{TRow}"/> to hold each imported
/// relation's integral in an LSM-style <see cref="SpineZSetTrace{TKey,TWeight}"/>
/// (tiered immutable batches, per-batch Arrow snapshot) instead of a flat
/// dictionary; omit it for flat traces.
/// </summary>
/// <param name="Comparer">Total order over rows for the sorted batches.</param>
/// <param name="Compaction">Compaction policy; null selects the tiered default.</param>
/// <typeparam name="TRow">The imported relation's row type.</typeparam>
public sealed record SpineImportConfig<TRow>(
    IComparer<TRow> Comparer,
    ICompactionStrategy? Compaction)
    where TRow : notnull;

/// <summary>
/// The integral of one imported relation inside a nested fixpoint: integrate
/// per-tick deltas, expose the consolidated Z-set the loop reads, and snapshot.
/// Flat (dictionary) and spine (LSM) implementations share this contract so the
/// fixpoint operator's algorithm is trace-family-agnostic.
/// </summary>
/// <typeparam name="TRow">The imported relation's row type.</typeparam>
internal interface IImportTrace<TRow>
    where TRow : notnull
{
    void Integrate(ZSet<TRow, Z64> delta);

    /// <summary>The running integral as a single consolidated Z-set.</summary>
    ZSet<TRow, Z64> Current { get; }

    bool CanSnapshot { get; }

    string SchemaFingerprint { get; }

    ValueTask SaveAsync(ISnapshotWriter writer, string prefix, CancellationToken cancellationToken);

    ValueTask LoadAsync(ISnapshotReader reader, string prefix, CancellationToken cancellationToken);
}

/// <summary>Flat dictionary-backed import trace (the default).</summary>
internal sealed class FlatImportTrace<TRow> : IImportTrace<TRow>
    where TRow : notnull
{
    private readonly ZSetTrace<TRow, Z64> _trace = new();
    private readonly IZSetTraceCodec<TRow, Z64>? _codec;

    public FlatImportTrace(IZSetTraceCodec<TRow, Z64>? codec) => _codec = codec;

    public void Integrate(ZSet<TRow, Z64> delta) => _trace.Integrate(delta);

    public ZSet<TRow, Z64> Current => _trace.Current;

    public bool CanSnapshot => _codec is not null;

    public string SchemaFingerprint => _codec?.SchemaFingerprint ?? string.Empty;

    public ValueTask SaveAsync(ISnapshotWriter writer, string prefix, CancellationToken cancellationToken) =>
        _codec!.SaveAsync(writer, prefix + ".arrows", _trace.Current, cancellationToken);

    public async ValueTask LoadAsync(ISnapshotReader reader, string prefix, CancellationToken cancellationToken) =>
        _trace.Integrate(await _codec!.LoadAsync(reader, prefix + ".arrows", cancellationToken).ConfigureAwait(false));
}

/// <summary>LSM-style spine-backed import trace.</summary>
internal sealed class SpineImportTrace<TRow> : IImportTrace<TRow>
    where TRow : notnull
{
    private readonly SpineZSetTrace<TRow, Z64> _trace;
    private readonly IZSetTraceCodec<TRow, Z64>? _codec;

    public SpineImportTrace(SpineImportConfig<TRow> config, IZSetTraceCodec<TRow, Z64>? codec)
    {
        ArgumentNullException.ThrowIfNull(config);
        _trace = new SpineZSetTrace<TRow, Z64>(config.Compaction ?? TieredCompactionStrategy.Default, config.Comparer);
        _codec = codec;
    }

    public void Integrate(ZSet<TRow, Z64> delta) => _trace.Integrate(delta);

    // The loop reads the whole integral; the spine consolidates its batches here.
    public ZSet<TRow, Z64> Current => _trace.Materialize();

    public bool CanSnapshot => _codec is not null;

    public string SchemaFingerprint => _codec?.SchemaFingerprint ?? string.Empty;

    public ValueTask SaveAsync(ISnapshotWriter writer, string prefix, CancellationToken cancellationToken) =>
        SpineSnapshot.SaveAsync(
            writer, prefix, _trace.GetBatches(),
            (name, batch) => _codec!.SaveAsync(writer, name, batch, cancellationToken),
            cancellationToken);

    public ValueTask LoadAsync(ISnapshotReader reader, string prefix, CancellationToken cancellationToken) =>
        SpineSnapshot.LoadAsync(
            reader, prefix,
            async name => _trace.Integrate(await _codec!.LoadAsync(reader, name, cancellationToken).ConfigureAwait(false)),
            cancellationToken);
}
