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
    // Null when detached: the footer and schema stay in memory and the file is opened per block
    // read, so a lazily restored state can hold thousands of runs without as many open handles.
    private IRandomAccessFile? _file;
    private ITableFileSystem? _store;
    private string? _path;
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
        return await OpenAsync(file, hash, path, null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Open the run at <paramref name="path"/> <b>detached</b>: read its footer and schema
    /// message, close it, and reopen it only to read a block.</summary>
    internal static async ValueTask<RunReader> OpenDetachedAsync(
        ITableFileSystem store, string path, Func<StructuralRow, int> hash, Func<StructuralRow, int>? fingerprint = null,
        CancellationToken cancellationToken = default)
    {
        var file = await store.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
        var run = await OpenAsync(file, hash, path, fingerprint, cancellationToken).ConfigureAwait(false);
        await run._file!.DisposeAsync().ConfigureAwait(false);
        run._file = null;
        run._store = store;
        run._path = path;
        return run;
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
        IRandomAccessFile file, Func<StructuralRow, int> hash, string path, Func<StructuralRow, int>? fingerprint,
        CancellationToken cancellationToken)
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

            if (footer.Fingerprint != (fingerprint ?? hash)(RunFile.FingerprintRow))
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
        if (version < RunFile.OldestReadableVersion || version > RunFile.FormatVersion)
        {
            throw new InvalidDataException(
                $"run '{path}' has format version {version}, expected {RunFile.OldestReadableVersion}–{RunFile.FormatVersion}");
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
        var coded = version >= 5;
        for (var i = 0; i < count; i++)
        {
            var length = BinaryPrimitives.ReadInt32LittleEndian(f[(at + 16)..]);
            var rawLength = coded ? BinaryPrimitives.ReadInt32LittleEndian(f[(at + 28)..]) : length;
            var codec = coded ? (BlockCodec)BinaryPrimitives.ReadInt32LittleEndian(f[(at + 32)..]) : BlockCodec.None;
            if (codec is not (BlockCodec.None or BlockCodec.Zstd) || rawLength < 0)
            {
                throw new InvalidDataException($"run '{path}' block {i} has codec {(int)codec}, message length {rawLength}");
            }

            blocks[i] = new RunBlock(
                BinaryPrimitives.ReadUInt32LittleEndian(f[at..]),
                BinaryPrimitives.ReadUInt32LittleEndian(f[(at + 4)..]),
                BinaryPrimitives.ReadInt64LittleEndian(f[(at + 8)..]),
                length,
                BinaryPrimitives.ReadInt32LittleEndian(f[(at + 20)..]),
                BinaryPrimitives.ReadInt32LittleEndian(f[(at + 24)..]),
                rawLength,
                codec);
            at += coded ? RunFile.BlockIndexEntryBytes : RunFile.BlockIndexEntryBytesV4;
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
        return _bloom.MightContain(h) && FindBlocks(h).Count > 0;
    }

    /// <summary>The weight of <paramref name="key"/> in a Z-set run (zero if absent).</summary>
    public async ValueTask<Z64> WeightOfAsync(
        StructuralRow key, IZSetRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        var h = (uint)_hash(key);
        var sum = Z64.Zero;
        if (!_bloom.MightContain(h))
        {
            return sum;
        }

        var (first, count) = FindBlocks(h);
        for (var b = first; b < first + count; b++)
        {
            var block = await DecodeZSetAsync(_blocks[b], codec, cancellationToken).ConfigureAwait(false);
            sum = Z64.Add(sum, block.WeightOf(key));
        }

        return sum;
    }

    /// <summary>The first block that may hold <paramref name="key"/>, or -1 when the run certainly does
    /// not. A giant group's hash can continue into the next blocks: see <see cref="BlocksOf"/>.</summary>
    internal int BlockOf(StructuralRow key) => BlocksOf(key).First;

    /// <summary>The blocks that may hold <paramref name="key"/>: one, or a giant group's consecutive
    /// blocks (§5.18); count 0 when the run certainly does not.</summary>
    internal (int First, int Count) BlocksOf(StructuralRow key)
    {
        var h = (uint)_hash(key);
        return _bloom.MightContain(h) ? FindBlocks(h) : (-1, 0);
    }

    /// <summary><paramref name="key"/>'s weight in a Z-set run, decoding only the block's entries
    /// whose order hash is the key's (a binary search of the block's <c>__hash</c> column).</summary>
    internal async ValueTask<Z64> WeightOfRowAsync(StructuralRow key, IZSetRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        var into = new ZSetBuilder<StructuralRow, Z64>();
        await ReadEntriesOfHashAsync(_hash(key), codec, into, cancellationToken).ConfigureAwait(false);
        return into.Build().WeightOf(key);
    }

    /// <summary>Every entry of a Z-set run whose stored order hash is <paramref name="hash"/> (a
    /// partition-ordered run's partition), decoding only those entries, added to <paramref name="into"/>.</summary>
    internal async ValueTask ReadEntriesOfHashAsync(
        int hash, IZSetRunCodec codec, ZSetBuilder<StructuralRow, Z64> into, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        var (first, blocks) = BlocksOfHash(hash);
        for (var b = first; b < first + blocks; b++)
        {
            using var block = await ReadBatchAsync(_blocks[b], cancellationToken).ConfigureAwait(false);
            var (from, count) = HashRange(block.Batch, hash);
            if (count > 0)
            {
                codec.DecodeRange(block.Batch, from, count, into);
            }
        }
    }

    /// <summary><paramref name="key"/>'s group in an indexed run, decoding only its entries.</summary>
    internal async ValueTask<ZSet<StructuralRow, Z64>> GroupOfKeyAsync(
        StructuralRow key, IIndexedRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        var (first, blocks) = BlocksOfHash(_hash(key));
        if (blocks == 1)
        {
            return await GroupOfKeyInBlockAsync(key, first, codec, cancellationToken).ConfigureAwait(false);
        }

        var sum = new ZSetBuilder<StructuralRow, Z64>();
        for (var b = first; b < first + blocks; b++)
        {
            sum.AddRange(await GroupOfKeyInBlockAsync(key, b, codec, cancellationToken).ConfigureAwait(false));
        }

        return sum.Build();
    }

    /// <summary><paramref name="key"/>'s entries in block <paramref name="b"/> of an indexed run (a giant
    /// group's part, when its hash spans blocks), decoding only them.</summary>
    internal async ValueTask<ZSet<StructuralRow, Z64>> GroupOfKeyInBlockAsync(
        StructuralRow key, int b, IIndexedRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        using var block = await ReadBatchAsync(_blocks[b], cancellationToken).ConfigureAwait(false);
        var (from, count) = HashRange(block.Batch, _hash(key));
        if (count == 0)
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        var into = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        codec.DecodeRange(block.Batch, from, count, into);
        return into.Build().GroupFor(key);
    }

    // The entries of a block whose order hash is h: the block's last column holds the entries'
    // hashes in ascending unsigned order, so a binary search finds the run of equal ones.
    private static (int From, int Count) HashRange(RecordBatch batch, int hash)
    {
        var hashes = ((Int32Array)batch.Column(batch.ColumnCount - 1)).Values;
        var h = (uint)hash;
        int lo = 0, hi = hashes.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if ((uint)hashes[mid] < h)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        var end = lo;
        while (end < hashes.Length && (uint)hashes[end] == h)
        {
            end++;
        }

        return (lo, end - lo);
    }

    /// <summary>Decode block <paramref name="b"/> with <paramref name="decode"/>, alongside its entries'
    /// stored order hashes (compaction merges by them, never recomputing an order).</summary>
    internal async ValueTask<(T Entries, uint[] Hashes)> ReadBlockAsync<T>(
        int b, Func<RecordBatch, T> decode, CancellationToken cancellationToken = default)
    {
        using var block = await ReadBatchAsync(_blocks[b], cancellationToken).ConfigureAwait(false);
        var stored = ((Int32Array)block.Batch.Column(block.Batch.ColumnCount - 1)).Values;
        var hashes = new uint[stored.Length];
        for (var i = 0; i < hashes.Length; i++)
        {
            hashes[i] = (uint)stored[i];
        }

        return (decode(block.Batch), hashes);
    }

    /// <summary>Block <paramref name="b"/> as stored — its bytes and codec, to copy into another run of
    /// the same schema unchanged — and its distinct order hashes (read from the <c>__hash</c> column
    /// only; no row is decoded).</summary>
    internal async ValueTask<(StoredBlock Stored, List<uint> KeyHashes)> ReadBlockRawAsync(int b, CancellationToken cancellationToken = default)
    {
        var block = _blocks[b];
        var file = _file ?? await _store!.OpenReadAsync(_path!, cancellationToken).ConfigureAwait(false);
        byte[] stored;
        try
        {
            using var bytes = await file.ReadAsync(new FileRange(block.Offset, block.Length), cancellationToken).ConfigureAwait(false);
            stored = bytes.Memory.Span[..block.Length].ToArray();
        }
        finally
        {
            if (_file is null)
            {
                await file.DisposeAsync().ConfigureAwait(false);
            }
        }

        var buffer = new byte[_schemaMessage.Length + block.MessageLength];
        _schemaMessage.CopyTo(buffer, 0);
        BlockCompression.Decompress(block.Codec, stored, buffer.AsSpan(_schemaMessage.Length));
        using var reader = new ArrowStreamReader(new MemoryStream(buffer, writable: false), leaveOpen: false);
        using var batch = reader.ReadNextRecordBatch()
            ?? throw new InvalidDataException($"run block at offset {block.Offset} holds no record batch");
        var orderHashes = ((Int32Array)batch.Column(batch.ColumnCount - 1)).Values;
        var distinct = new List<uint>();
        foreach (var h in orderHashes)
        {
            if (distinct.Count == 0 || distinct[^1] != (uint)h)
            {
                distinct.Add((uint)h);
            }
        }

        return (new StoredBlock(stored, block.Codec, block.MessageLength), distinct);
    }

    /// <summary>The first block holding entries of order hash <paramref name="hash"/>, or -1 when the
    /// run certainly has none (see <see cref="BlocksOfHash"/>).</summary>
    internal int BlockOfHash(int hash) => BlocksOfHash(hash).First;

    /// <summary>The blocks holding entries of order hash <paramref name="hash"/> (more than one for a
    /// giant group, §5.18); count 0 when the run certainly has none.</summary>
    internal (int First, int Count) BlocksOfHash(int hash) => _bloom.MightContain((uint)hash) ? FindBlocks((uint)hash) : (-1, 0);

    /// <summary>Block <paramref name="b"/> of a Z-set run, decoded.</summary>
    internal ValueTask<ZSet<StructuralRow, Z64>> DecodeZSetBlockAsync(
        int b, IZSetRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.ZSet);
        return DecodeZSetAsync(_blocks[b], codec, cancellationToken);
    }

    /// <summary>Block <paramref name="b"/> of an indexed run, decoded.</summary>
    internal ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> DecodeIndexedBlockAsync(
        int b, IIndexedRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        return DecodeIndexedAsync(_blocks[b], codec, cancellationToken);
    }

    /// <summary>The values of <paramref name="key"/> in an indexed run (empty if absent).</summary>
    public async ValueTask<ZSet<StructuralRow, Z64>> GroupForAsync(
        StructuralRow key, IIndexedRunCodec codec, CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        var h = (uint)_hash(key);
        if (!_bloom.MightContain(h))
        {
            return ZSet<StructuralRow, Z64>.Empty;
        }

        var (first, count) = FindBlocks(h);
        var sum = new ZSetBuilder<StructuralRow, Z64>();
        for (var b = first; b < first + count; b++)
        {
            var block = await DecodeIndexedAsync(_blocks[b], codec, cancellationToken).ConfigureAwait(false);
            sum.AddRange(block.GroupFor(key));
        }

        return sum.Build();
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

    public void Dispose() => _file?.Dispose();

    public ValueTask DisposeAsync() => _file?.DisposeAsync() ?? ValueTask.CompletedTask;

    private void Require(RunFile.Kind kind)
    {
        if (Kind != kind)
        {
            throw new InvalidOperationException($"this run holds a {Kind}, not a {kind}");
        }
    }

    // The blocks whose [first, last] hash ranges hold h: ranges ascend, and consecutive ones share at
    // most a boundary hash (a giant group continuing, §5.18), so they are the first block whose last
    // hash is not below h, and those after it that begin with h. (-1, 0) when none does.
    private (int First, int Count) FindBlocks(uint h)
    {
        int lo = 0, hi = _blocks.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (_blocks[mid].LastHash < h)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        if (lo >= _blocks.Length || _blocks[lo].FirstHash > h)
        {
            return (-1, 0);
        }

        var end = lo + 1;
        while (end < _blocks.Length && _blocks[end].FirstHash == h)
        {
            end++;
        }

        return (lo, end - lo);
    }

    // One block as a record batch: the cached schema message followed by the block's record-batch
    // message is a well-formed Arrow stream (with no end marker; the reader stops after one batch).
    // The reader stays alive with the batch, so nothing the batch points at is released early.
    private async ValueTask<Block> ReadBatchAsync(RunBlock block, CancellationToken cancellationToken)
    {
        var buffer = new byte[_schemaMessage.Length + block.MessageLength];
        _schemaMessage.CopyTo(buffer, 0);
        var file = _file ?? await _store!.OpenReadAsync(_path!, cancellationToken).ConfigureAwait(false);
        try
        {
            using var bytes = await file.ReadAsync(new FileRange(block.Offset, block.Length), cancellationToken).ConfigureAwait(false);
            BlockCompression.Decompress(block.Codec, bytes.Memory.Span[..block.Length], buffer.AsSpan(_schemaMessage.Length));
        }
        finally
        {
            if (_file is null)
            {
                await file.DisposeAsync().ConfigureAwait(false);
            }
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
