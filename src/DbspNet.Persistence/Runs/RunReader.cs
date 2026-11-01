// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// An open run (<see cref="RunFile"/>): its footer is read once — counts, block index, Bloom
/// filter — and nothing else until a key is asked for. A point lookup decodes the one block whose
/// hash range holds the key; <see cref="ScanZSetAsync"/> / <see cref="ScanIndexedAsync"/> decode the
/// blocks in order.
/// </summary>
public sealed class RunReader : IAsyncDisposable, IDisposable
{
    private readonly IRandomAccessFile _file;
    private readonly Func<StructuralRow, int> _hash;
    private readonly RunBlock[] _blocks;
    private readonly RunBloom _bloom;

    private RunReader(
        IRandomAccessFile file, Func<StructuralRow, int> hash, RunFile.Kind kind, long entries, long groups,
        RunBlock[] blocks, RunBloom bloom)
    {
        _file = file;
        _hash = hash;
        Kind = kind;
        Entries = entries;
        Groups = groups;
        _blocks = blocks;
        _bloom = bloom;
    }

    public RunFile.Kind Kind { get; }

    /// <summary>Entries (rows; for an indexed run, key/value pairs).</summary>
    public long Entries { get; }

    /// <summary>Distinct keys.</summary>
    public long Groups { get; }

    public IReadOnlyList<RunBlock> Blocks => _blocks;

    /// <summary>Open the run at <paramref name="path"/>; reads only the trailer and the footer.
    /// <paramref name="hash"/> must be the function the run was written with.</summary>
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

            using var footer = await file.ReadAsync(
                new FileRange(length - RunFile.TrailerBytes - footerLength, footerLength), cancellationToken).ConfigureAwait(false);
            return Parse(file, hash, path, footer.Memory.Span[..(int)footerLength]);
        }
        catch
        {
            await file.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static RunReader Parse(IRandomAccessFile file, Func<StructuralRow, int> hash, string path, ReadOnlySpan<byte> f)
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
        var entries = BinaryPrimitives.ReadInt64LittleEndian(f[at..]);
        at += 8;
        var groups = BinaryPrimitives.ReadInt64LittleEndian(f[at..]);
        at += 8;
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

        return new RunReader(file, hash, kind, entries, groups, blocks, new RunBloom(bits, hashCount));
    }

    /// <summary>False when the run certainly does not hold <paramref name="key"/>.</summary>
    public bool MightContain(StructuralRow key)
    {
        var h = (uint)_hash(key);
        return _bloom.MightContain(h) && FindBlock(h) >= 0;
    }

    /// <summary>The weight of <paramref name="key"/> in a Z-set run (zero if absent).</summary>
    public async ValueTask<Z64> WeightOfAsync(
        StructuralRow key, IZSetTraceCodec<StructuralRow, Z64> codec, CancellationToken cancellationToken = default)
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
        StructuralRow key, IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec, CancellationToken cancellationToken = default)
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
        IZSetTraceCodec<StructuralRow, Z64> codec,
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
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Require(RunFile.Kind.Indexed);
        foreach (var b in _blocks)
        {
            yield return await DecodeIndexedAsync(b, codec, cancellationToken).ConfigureAwait(false);
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

    private async ValueTask<TableFileSystemSnapshotContext> BlockContextAsync(RunBlock block, CancellationToken cancellationToken)
    {
        using var bytes = await _file.ReadAsync(new FileRange(block.Offset, block.Length), cancellationToken).ConfigureAwait(false);
        var scratch = new InMemoryTableFileSystem();
        await scratch.WriteAllBytesAsync("s/b", bytes.Memory[..block.Length], cancellationToken).ConfigureAwait(false);
        return new TableFileSystemSnapshotContext(scratch, "s");
    }

    private async ValueTask<ZSet<StructuralRow, Z64>> DecodeZSetAsync(
        RunBlock block, IZSetTraceCodec<StructuralRow, Z64> codec, CancellationToken cancellationToken)
    {
        var ctx = await BlockContextAsync(block, cancellationToken).ConfigureAwait(false);
        return await codec.LoadAsync(ctx, "b", cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<IndexedZSet<StructuralRow, StructuralRow, Z64>> DecodeIndexedAsync(
        RunBlock block, IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec, CancellationToken cancellationToken)
    {
        var ctx = await BlockContextAsync(block, cancellationToken).ConfigureAwait(false);
        return await codec.LoadAsync(ctx, "b", cancellationToken).ConfigureAwait(false);
    }
}
