// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Snapshot codec for a typed-key Z-set trace, implemented as an
/// adapter over the structural <see cref="IZSetTraceCodec{StructuralRow,Z64}"/>.
/// At save time each typed key is projected to a
/// <see cref="StructuralRow"/> via <c>keyToStruct</c>; at load time
/// each <see cref="StructuralRow"/> read back is lifted to the typed
/// key via <c>structToKey</c>. The on-disk format is byte-identical
/// to the structural pipeline's so a snapshot written by one path
/// can be read by the other.
/// </summary>
internal sealed class TypedZSetTraceCodecAdapter<TKey>
    : IZSetTraceCodec<TKey, Z64>
    where TKey : notnull
{
    private readonly IZSetTraceCodec<StructuralRow, Z64> _inner;
    private readonly Func<TKey, StructuralRow> _keyToStruct;
    private readonly Func<StructuralRow, TKey> _structToKey;

    public TypedZSetTraceCodecAdapter(
        IZSetTraceCodec<StructuralRow, Z64> inner,
        Func<TKey, StructuralRow> keyToStruct,
        Func<StructuralRow, TKey> structToKey)
    {
        _inner = inner;
        _keyToStruct = keyToStruct;
        _structToKey = structToKey;
    }

    public string SchemaFingerprint => _inner.SchemaFingerprint;

    public async ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        ZSet<TKey, Z64> trace,
        CancellationToken cancellationToken = default)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (key, weight) in trace)
        {
            b.Add(_keyToStruct(key), weight);
        }

        await _inner.SaveAsync(writer, fileName, b.Build(), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<ZSet<TKey, Z64>> LoadAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default)
    {
        var loaded = await _inner.LoadAsync(reader, fileName, cancellationToken).ConfigureAwait(false);
        var b = new ZSetBuilder<TKey, Z64>();
        foreach (var (sk, w) in loaded)
        {
            b.Add(_structToKey(sk), w);
        }

        return b.Build();
    }
}

/// <summary>
/// Snapshot codec for a typed-keyed, typed-valued indexed Z-set
/// trace. Adapter shape mirrors
/// <see cref="TypedZSetTraceCodecAdapter{TKey}"/>: each (key, value)
/// pair is projected to a (StructuralRow, StructuralRow) pair for
/// the inner codec, and reconstituted at load time.
/// </summary>
internal sealed class TypedIndexedZSetTraceCodecAdapter<TKey, TValue>
    : IIndexedZSetTraceCodec<TKey, TValue, Z64>
    where TKey : notnull
    where TValue : notnull
{
    private readonly IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> _inner;
    private readonly Func<TKey, StructuralRow> _keyToStruct;
    private readonly Func<TValue, StructuralRow> _valToStruct;
    private readonly Func<StructuralRow, TKey> _structToKey;
    private readonly Func<StructuralRow, TValue> _structToVal;

    public TypedIndexedZSetTraceCodecAdapter(
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> inner,
        Func<TKey, StructuralRow> keyToStruct,
        Func<TValue, StructuralRow> valToStruct,
        Func<StructuralRow, TKey> structToKey,
        Func<StructuralRow, TValue> structToVal)
    {
        _inner = inner;
        _keyToStruct = keyToStruct;
        _valToStruct = valToStruct;
        _structToKey = structToKey;
        _structToVal = structToVal;
    }

    public string SchemaFingerprint => _inner.SchemaFingerprint;

    public async ValueTask SaveAsync(
        ISnapshotWriter writer,
        string fileName,
        IndexedZSet<TKey, TValue, Z64> trace,
        CancellationToken cancellationToken = default)
    {
        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (key, perKey) in trace)
        {
            var skey = _keyToStruct(key);
            foreach (var (val, weight) in perKey)
            {
                b.Add(skey, _valToStruct(val), weight);
            }
        }

        await _inner.SaveAsync(writer, fileName, b.Build(), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IndexedZSet<TKey, TValue, Z64>> LoadAsync(
        ISnapshotReader reader,
        string fileName,
        CancellationToken cancellationToken = default)
    {
        var loaded = await _inner.LoadAsync(reader, fileName, cancellationToken).ConfigureAwait(false);
        var b = new IndexedZSetBuilder<TKey, TValue, Z64>();
        foreach (var (sk, perKey) in loaded)
        {
            var tk = _structToKey(sk);
            foreach (var (sv, w) in perKey)
            {
                b.Add(tk, _structToVal(sv), w);
            }
        }

        return b.Build();
    }
}
