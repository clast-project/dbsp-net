// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Persistence.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// An open run (<see cref="RunFile"/>): its footer and schema message are read once — counts,
/// block index, Bloom filter — and nothing else until a key is asked for. A point lookup decodes
/// the one block whose hash range holds the key; <see cref="ScanZSetAsync"/> /
/// <see cref="ScanIndexedAsync"/> decode the blocks in order.
/// </summary>
internal sealed class RunReader : IAsyncDisposable, IDisposable
{
    private readonly IRandomAccessFile _file;
    private readonly Func<StructuralRow, int> _hash;
    private readonly RunBlock[] _blocks;
    private readonly RunBloom _bloom;
    private readonly byte[] _schemaMessage;

    private RunReader(
        IRandomAccessFile file, Func<StructuralRow, int> hash, Footer footer, byte[] schemaMessage)
    {
        _file = file;
        _hash = hash;
        Kind = footer.Kind;
        Entries = footer.Entries;
        Groups = footer.Groups;
        _blocks = footer.Blocks;
        _bloom = footer.Bloom;
        _schemaMessage = schemaMessage;
    }

    public RunFile.Kind Kind { get; }

    /// <summary>Entries (rows; for an indexed run, key/value pairs).</summary>
    public long Entries { get; }

    /// <summary>Distinct keys.</summary>
    public long Groups { get; }

    public IReadOnlyList<RunBlock> Blocks => _blocks;

    /// <summary>Open the run at <paramref name="path"/>; reads only the trailer, the footer and the
    /// schema message. <paramref name="hash"/> must be the function the run was written with.</summary>
    public static async ValueTask<RunReader> OpenAsync(
        ITableFileSystem fs, string path, Func<StructuralRow, int> hash, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        ArgumentNullException.ThrowIfNull(hash);
        var file = await fs.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
        return await OpenAsync(file, hash, path, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>True if <paramref name="file"/> ends in a run trailer.</summary>
    internal static async ValueTask<bool> IsRunAsync(IRandomAccessFile file, CancellationToken cancellationToken)
    {
        var length = await file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
        if (length < RunFile.TrailerBytes)
        {
            return false;
        }

        using var trailer = await file.ReadAsync(new FileRange(length - RunFile.TrailerBytes, RunFile.TrailerBytes), cancellationToken)
            .ConfigureAwait(false);
        return trailer.Memory.Span[8..16].SequenceEqual(RunFile.Magic);
    }

    /// <summary>Open a run over an already-open file, which the reader then owns.</summary>
    internal static async ValueTask<RunReader> OpenAsync(
        IRandomAccessFile file, Func<StructuralRow, int> hash, string path, CancellationToken cancellationToken)
    {
        try
        {
            var length = await file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
            if (length < RunFile.TrailerBytes)
            {
                throw new InvalidDataException($"run '{path}' is too short to be a run file");
            }

            long footerLength;
            using (var trailer = await file.ReadAsync(new FileRange(length - RunFile.TrailerBytes, RunFile.TrailerBytes), cancellationToken)
                .ConfigureAwait(false))
            {
                var t = trailer.Memory.Span;
                if (!t[8..16].SequenceEqual(RunFile.Magic))
                {
                    throw new InvalidDataException($"run '{path}' has no run trailer");
                }

                footerLength = BinaryPrimitives.ReadInt64LittleEndian(t);
            }

            if (footerLength <= 0 || footerLength > length - RunFile.TrailerBytes)
            {
                throw new InvalidDataException($"run '{path}' has a footer length of {footerLength}");
            }

            Footer footer;
            using (var bytes = await file.ReadAsync(
                new FileRange(length - RunFile.TrailerBytes - footerLength, footerLength), cancellationToken).ConfigureAwait(false))
            {
                footer = Parse(path, bytes.Memory.Span[..(int)footerLength]);
            }

            if (footer.Fingerprint != hash(RunFile.FingerprintRow))
            {
                throw new InvalidDataException(
                    $"run '{path}' was ordered by a different hash function than this process has; it cannot be read in place");
            }

            if (footer.SchemaLength <= 0 || footer.SchemaLength > length - RunFile.TrailerBytes - footerLength)
            {
                throw new InvalidDataException($"run '{path}' has a schema message length of {footer.SchemaLength}");
            }

            var schema = new byte[footer.SchemaLength];
            using (var bytes = await file.ReadAsync(new FileRange(0, footer.SchemaLength), cancellationToken).ConfigureAwait(false))
            {
                bytes.Memory.Span[..schema.Length].CopyTo(schema);
            }

            return new RunReader(file, hash, footer, schema);
        }
        catch
        {
            await file.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private readonly record struct Footer(
        RunFile.Kind Kind, int Fingerprint, long Entries, long Groups, int SchemaLength, RunBlock[] Blocks, RunBloom Bloom);

    private static Footer Parse(string path, ReadOnlySpan<byte> f)
    {
        if (!f[..8].SequenceEqual(RunFile.Magic))
        {
            throw new InvalidDataException($"run '{path}' footer has no magic");
        }

        var at = 8;
        var version = BinaryPrimitives.ReadInt32LittleEndian(f[at..]);
        at += 4;
        if (version != RunFile.FormatVersion)
        {
            throw new InvalidDataException($"run '{path}' has format version {version}, expected {RunFile.FormatVersion}");
        }

        var kind = (RunFile.Kind)BinaryPrimitives.ReadInt32LittleEndian(f[at..]);
        at += 4;
        var fingerprint = BinaryPrimitives.ReadInt32LittleEndian(f[at..]);
        at += 4;
        var entries = BinaryPrimitives.ReadInt64LittleEndian(f[at..]);
        at += 8;
        var groups = BinaryPrimitives.ReadInt64LittleEndian(f[at..]);
        at += 8;
        var schemaLength = BinaryPrimitives.ReadInt64LittleEndian(f[at..]);
        at += 8;
        if (schemaLength is <= 0 or > int.MaxValue)
        {
            throw new InvalidDataException($"run '{path}' has a schema message length of {schemaLength}");
        }

        var count = BinaryPrimitives.ReadInt32LittleEndian(f[at..]);
        at += 4;
        var blocks = new RunBlock[count];
        for (var i = 0; i < count; i++)
        {
            blocks[i] = new RunBlock(
                BinaryPrimitives.ReadUInt32LittleEndian(f[at..]),
                BinaryPrimitives.ReadUInt32LittleEndian(f[(at + 4)..]),
                BinaryPrimitives.ReadInt64LittleEndian(f[(at + 8)..]),
                BinaryPrimitives.ReadInt32LittleEndian(f[(at + 16)..]),
                BinaryPrimitives.ReadInt32LittleEndian(f[(at + 20)..]),
                BinaryPrimitives.ReadInt32LittleEndian(f[(at + 24)..]));
            at += RunFile.BlockIndexEntryBytes;
        }

        var words = BinaryPrimitives.ReadInt32LittleEndian(f[at..]);
        at += 4;
        var hashCount = BinaryPrimitives.ReadInt32LittleEndian(f[at..]);
        at += 4;
        var bits = new ulong[words];
        for (var i = 0; i < words; i++)
        {
            bits[i] = BinaryPrimitives.ReadUInt64LittleEndian(f[at..]);
            at += 8;
        }

        return new Footer(kind, fingerprint, entries, groups, (int)schemaLength, blocks, new RunBloom(bits, hashCount));
    }

    /// <summary>False when the run certainly does not hold <paramref name="key"/>.</summary>
    public bool MightContain(StructuralRow key)
    {
        var h = (uint)_hash(key);
        return _bloom.MightContain(h) && FindBlock(h) >= 0;
    }

    /// <summary>The weight of <paramref name="key"/> in a Z-set run (zero if absent).</summary>
    public async ValueTask<Z64> WeightOfAsync(
        StructuralRow key, IZSetRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        var h = (uint)_hash(key);
        if (!_bloom.MightContain(h) || FindBlock(h) is var b && b < 0)
        {
            return Z64.Zero;
        }

        var block = await DecodeZSetAsync(_blocks[b], codec, cancellationToken).ConfigureAwait(false);
        return block.WeightOf(key);
    }

    /// <summary>The values of <paramref name="key"/> in an indexed run (empty if absent).</summary>
    public async ValueTask<ZSet<StructuralRow, Z64>> GroupForAsync(
        StructuralRow key, IIndexedRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        var h = (uint)_hash(key);
        if (!_bloom.MightContain(h) || FindBlock(h) is var b && b < 0)
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        var block = await DecodeIndexedAsync(_blocks[b], codec, cancellationToken).ConfigureAwait(false);
        return block.GroupFor(key);
    }

    /// <summary>Every block of a Z-set run, decoded, in hash order.</summary>
    public async IAsyncEnumerable<ZSet<StructuralRow, Z64>> ScanZSetAsync(
        IZSetRunCodec codec,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        foreach (var b in _blocks)
        {
            yield return await DecodeZSetAsync(b, codec, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Every block of an indexed run, decoded, in hash order.</summary>
    public async IAsyncEnumerable<IndexedZSet<StructuralRow, StructuralRow, Z64>> ScanIndexedAsync(
        IIndexedRunCodec codec,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        foreach (var b in _blocks)
        {
            yield return await DecodeIndexedAsync(b, codec, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>The whole Z-set run, every block decoded straight into one builder.</summary>
    public async ValueTask<ZSet<StructuralRow, Z64>> ReadAllZSetAsync(IZSetRunCodec codec, CancellationToken cancellationToken = default)
    {
        var into = new ZSetBuilder<StructuralRow, Z64>((int)Math.Min(int.MaxValue, Entries));
        await ReadAllIntoAsync(codec, into, cancellationToken).ConfigureAwait(false);
        return into.Build();
    }

    /// <summary>Add every entry of this Z-set run to <paramref name="into"/> (weights sum).</summary>
    public async ValueTask ReadAllIntoAsync(IZSetRunCodec codec, ZSetBuilder<StructuralRow, Z64> into, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        foreach (var b in _blocks)
        {
            using var batch = await ReadBatchAsync(b, cancellationToken).ConfigureAwait(false);
            codec.DecodeBlock(batch.Batch, into);
        }
    }

    /// <summary>The whole indexed run, every block decoded straight into one builder.</summary>
    public async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> ReadAllIndexedAsync(
        IIndexedRunCodec codec, CancellationToken cancellationToken = default)
    {
        var into = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        await ReadAllIntoAsync(codec, into, cancellationToken).ConfigureAwait(false);
        return into.Build();
    }

    /// <summary>Add every entry of this indexed run to <paramref name="into"/> (weights sum).</summary>
    public async ValueTask ReadAllIntoAsync(
        IIndexedRunCodec codec, IndexedZSetBuilder<StructuralRow, StructuralRow, Z64> into, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        foreach (var b in _blocks)
        {
            using var batch = await ReadBatchAsync(b, cancellationToken).ConfigureAwait(false);
            codec.DecodeBlock(batch.Batch, into);
        }
    }

    public void Dispose() => _file.Dispose();

    public ValueTask DisposeAsync() => _file.DisposeAsync();

    private void Require(RunFile.Kind kind)
    {
        if (Kind != kind)
        {
            throw new InvalidOperationException($"this run holds a {Kind}, not a {kind}");
        }
    }

    // The block whose [first, last] hash range holds h, or -1. Ranges are disjoint and ascending.
    private int FindBlock(uint h)
    {
        int lo = 0, hi = _blocks.Length - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >>> 1;
            if (h < _blocks[mid].FirstHash)
            {
                hi = mid - 1;
            }
            else if (h > _blocks[mid].LastHash)
            {
                lo = mid + 1;
            }
            else
            {
                return mid;
            }
        }

        return -1;
    }

    // One block as a record batch: the cached schema message followed by the block's record-batch
    // message is a well-formed Arrow stream (with no end marker; the reader stops after one batch).
    // The reader stays alive with the batch, so nothing the batch points at is released early.
    private async ValueTask<Block> ReadBatchAsync(RunBlock block, CancellationToken cancellationToken)
    {
        var buffer = new byte[_schemaMessage.Length + block.Length];
        _schemaMessage.CopyTo(buffer, 0);
        using (var bytes = await _file.ReadAsync(new FileRange(block.Offset, block.Length), cancellationToken).ConfigureAwait(false))
        {
            bytes.Memory.Span[..block.Length].CopyTo(buffer.AsSpan(_schemaMessage.Length));
        }

        var reader = new ArrowStreamReader(new MemoryStream(buffer, writable: false), leaveOpen: false);
        try
        {
            var batch = reader.ReadNextRecordBatch()
                ?? throw new InvalidDataException($"run block at offset {block.Offset} holds no record batch");
            return new Block(reader, batch);
        }
        catch
        {
            reader.Dispose();
            throw;
        }
    }

    private readonly record struct Block(ArrowStreamReader Reader, RecordBatch Batch) : IDisposable
    {
        public void Dispose()
        {
            Batch.Dispose();
            Reader.Dispose();
        }
    }

    private async ValueTask<ZSet<StructuralRow, Z64>> DecodeZSetAsync(
        RunBlock block, IZSetRunCodec codec, CancellationToken cancellationToken)
    {
        using var batch = await ReadBatchAsync(block, cancellationToken).ConfigureAwait(false);
        var into = new ZSetBuilder<StructuralRow, Z64>(block.Entries);
        codec.DecodeBlock(batch.Batch, into);
        return into.Build();
    }

    private async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> DecodeIndexedAsync(
        RunBlock block, IIndexedRunCodec codec, CancellationToken cancellationToken)
    {
        using var batch = await ReadBatchAsync(block, cancellationToken).ConfigureAwait(false);
        var into = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        codec.DecodeBlock(batch.Batch, into);
        return into.Build();
    }
}
