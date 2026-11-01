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
/// A <b>run</b>: an immutable file holding one Z-set (or indexed Z-set) — a trace's state, or the
/// change to it since the last checkpoint — laid out so a single key can be read without decoding
/// the rest (docs/design-durable-state.md §1f, increment 1).
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> Entries are ordered by the key's stable hash (as an unsigned 32-bit value)
/// and cut into blocks of about <c>blockEntries</c> entries. A block never splits one hash value,
/// so colliding keys — and every value of an indexed key — share a block. Each block is the
/// operator's own codec output (an Arrow IPC stream in the operator's schema), so a block decodes
/// exactly as today's snapshot file does. After the blocks comes a footer: counts, the block index
/// (hash range, offset, length, counts per block) and a Bloom filter over the key hashes; then a
/// 16-byte trailer (footer length, magic).</para>
/// <para><b>Reading one key</b> costs a Bloom check, a binary search of the index and one block
/// decode. The hash must be stable across processes (<c>StablePartitionHash</c>); the writer and
/// reader are handed the same function.</para>
/// </remarks>
public static class RunFile
{
    internal static ReadOnlySpan<byte> Magic => "DBSPRUN1"u8;

    internal const int FormatVersion = 1;
    internal const int TrailerBytes = 16;
    internal const int BlockIndexEntryBytes = 4 + 4 + 8 + 4 + 4 + 4;
    internal const int DefaultBlockEntries = 1024;

    /// <summary>What kind of collection a run holds.</summary>
    public enum Kind
    {
        ZSet = 0,
        Indexed = 1,
    }

    /// <summary>Write <paramref name="set"/> as a run at <paramref name="path"/>.</summary>
    public static ValueTask<RunInfo> WriteZSetAsync(
        ITableFileSystem fs,
        string path,
        ZSet<StructuralRow, Z64> set,
        IZSetTraceCodec<StructuralRow, Z64> codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        return WriteZSetAsync(ct => fs.CreateAsync(path, overwrite: true, ct), set, codec, hash, blockEntries, cancellationToken);
    }

    /// <summary>Write <paramref name="set"/> as a run to <paramref name="fileName"/> of a snapshot.</summary>
    internal static ValueTask<RunInfo> WriteZSetAsync(
        Core.Circuit.ISnapshotWriter writer,
        string fileName,
        ZSet<StructuralRow, Z64> set,
        IZSetTraceCodec<StructuralRow, Z64> codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default) =>
        WriteZSetAsync(ct => writer.CreateAsync(fileName, ct), set, codec, hash, blockEntries, cancellationToken);

    private static async ValueTask<RunInfo> WriteZSetAsync(
        Func<CancellationToken, ValueTask<ISequentialFile>> create,
        ZSet<StructuralRow, Z64> set,
        IZSetTraceCodec<StructuralRow, Z64> codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentNullException.ThrowIfNull(codec);
        var entries = new List<(uint Hash, StructuralRow Key, Z64 Weight)>(set.Count);
        foreach (var (key, weight) in set)
        {
            entries.Add(((uint)hash(key), key, weight));
        }

        entries.Sort(static (a, b) => a.Hash.CompareTo(b.Hash));
        var blocks = Cut(entries, e => e.Hash, _ => 1, blockEntries);
        return await WriteAsync(
            create, Kind.ZSet, entries.Count, entries.Count, blocks, entries.Select(e => e.Hash),
            async (from, to, scratch, ct) =>
            {
                var b = new ZSetBuilder<StructuralRow, Z64>(to - from);
                for (var i = from; i < to; i++)
                {
                    b.Add(entries[i].Key, entries[i].Weight);
                }

                await codec.SaveAsync(scratch, "b", b.Build(), ct).ConfigureAwait(false);
                return (to - from, to - from);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Write <paramref name="set"/> as a run at <paramref name="path"/>; the hash is over
    /// the key, and every value of a key lands in the same block.</summary>
    public static ValueTask<RunInfo> WriteIndexedAsync(
        ITableFileSystem fs,
        string path,
        IndexedZSet<StructuralRow, StructuralRow, Z64> set,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        return WriteIndexedAsync(ct => fs.CreateAsync(path, overwrite: true, ct), set, codec, hash, blockEntries, cancellationToken);
    }

    /// <summary>Write <paramref name="set"/> as a run to <paramref name="fileName"/> of a snapshot.</summary>
    internal static ValueTask<RunInfo> WriteIndexedAsync(
        Core.Circuit.ISnapshotWriter writer,
        string fileName,
        IndexedZSet<StructuralRow, StructuralRow, Z64> set,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default) =>
        WriteIndexedAsync(ct => writer.CreateAsync(fileName, ct), set, codec, hash, blockEntries, cancellationToken);

    private static async ValueTask<RunInfo> WriteIndexedAsync(
        Func<CancellationToken, ValueTask<ISequentialFile>> create,
        IndexedZSet<StructuralRow, StructuralRow, Z64> set,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentNullException.ThrowIfNull(codec);
        var groups = new List<(uint Hash, StructuralRow Key, ZSet<StructuralRow, Z64> Values)>(set.GroupCount);
        long total = 0;
        foreach (var g in set)
        {
            groups.Add(((uint)hash(g.Key), g.Key, g.Value));
            total += g.Value.Count;
        }

        groups.Sort(static (a, b) => a.Hash.CompareTo(b.Hash));
        var blocks = Cut(groups, g => g.Hash, g => g.Values.Count, blockEntries);
        return await WriteAsync(
            create, Kind.Indexed, total, groups.Count, blocks, groups.Select(g => g.Hash),
            async (from, to, scratch, ct) =>
            {
                var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                var n = 0;
                for (var i = from; i < to; i++)
                {
                    foreach (var (v, w) in groups[i].Values)
                    {
                        b.Add(groups[i].Key, v, w);
                        n++;
                    }
                }

                await codec.SaveAsync(scratch, "b", b.Build(), ct).ConfigureAwait(false);
                return (n, to - from);
            },
            cancellationToken).ConfigureAwait(false);
    }

    // Block boundaries [from, to) over sorted items: close a block once it holds blockEntries
    // entries, but only where the hash changes.
    private static List<(int From, int To)> Cut<T>(List<T> sorted, Func<T, uint> hashOf, Func<T, int> sizeOf, int blockEntries)
    {
        var blocks = new List<(int, int)>();
        var start = 0;
        var size = 0;
        for (var i = 0; i < sorted.Count; i++)
        {
            if (size >= blockEntries && hashOf(sorted[i]) != hashOf(sorted[i - 1]))
            {
                blocks.Add((start, i));
                start = i;
                size = 0;
            }

            size += sizeOf(sorted[i]);
        }

        if (start < sorted.Count)
        {
            blocks.Add((start, sorted.Count));
        }

        return blocks;
    }

    private static async ValueTask<RunInfo> WriteAsync(
        Func<CancellationToken, ValueTask<ISequentialFile>> create,
        Kind kind,
        long totalEntries,
        long totalGroups,
        List<(int From, int To)> cuts,
        IEnumerable<uint> hashes,
        Func<int, int, TableFileSystemSnapshotContext, CancellationToken, ValueTask<(int Entries, int Groups)>> encode,
        CancellationToken cancellationToken)
    {
        var hashList = hashes.ToList();
        var index = new List<RunBlock>(cuts.Count);
        long offset = 0;
        await using (var file = await create(cancellationToken).ConfigureAwait(false))
        {
            foreach (var (from, to) in cuts)
            {
                var scratchFs = new InMemoryTableFileSystem();
                var scratch = new TableFileSystemSnapshotContext(scratchFs, "s");
                var (entries, groups) = await encode(from, to, scratch, cancellationToken).ConfigureAwait(false);
                var bytes = await scratchFs.ReadAllBytesAsync("s/b", cancellationToken).ConfigureAwait(false);
                await file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                index.Add(new RunBlock(hashList[from], hashList[to - 1], offset, bytes.Length, entries, groups));
                offset += bytes.Length;
            }

            var bloom = RunBloom.Build(hashList);
            var footer = EncodeFooter(kind, totalEntries, totalGroups, index, bloom);
            await file.WriteAsync(footer, cancellationToken).ConfigureAwait(false);
            var trailer = new byte[TrailerBytes];
            BinaryPrimitives.WriteInt64LittleEndian(trailer, footer.Length);
            Magic.CopyTo(trailer.AsSpan(8));
            await file.WriteAsync(trailer, cancellationToken).ConfigureAwait(false);
            await file.FlushAsync(cancellationToken).ConfigureAwait(false);
            return new RunInfo(kind, totalEntries, totalGroups, index.Count, offset + footer.Length + TrailerBytes);
        }
    }

    private static byte[] EncodeFooter(Kind kind, long entries, long groups, List<RunBlock> index, RunBloom bloom)
    {
        var size = 8 + 4 + 4 + 8 + 8 + 4 + (index.Count * BlockIndexEntryBytes) + 4 + 4 + (bloom.Bits.Length * 8);
        var buf = new byte[size];
        var s = buf.AsSpan();
        Magic.CopyTo(s);
        var at = 8;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], FormatVersion);
        at += 4;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], (int)kind);
        at += 4;
        BinaryPrimitives.WriteInt64LittleEndian(s[at..], entries);
        at += 8;
        BinaryPrimitives.WriteInt64LittleEndian(s[at..], groups);
        at += 8;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], index.Count);
        at += 4;
        foreach (var b in index)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(s[at..], b.FirstHash);
            BinaryPrimitives.WriteUInt32LittleEndian(s[(at + 4)..], b.LastHash);
            BinaryPrimitives.WriteInt64LittleEndian(s[(at + 8)..], b.Offset);
            BinaryPrimitives.WriteInt32LittleEndian(s[(at + 16)..], b.Length);
            BinaryPrimitives.WriteInt32LittleEndian(s[(at + 20)..], b.Entries);
            BinaryPrimitives.WriteInt32LittleEndian(s[(at + 24)..], b.Groups);
            at += BlockIndexEntryBytes;
        }

        BinaryPrimitives.WriteInt32LittleEndian(s[at..], bloom.Bits.Length);
        at += 4;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], bloom.HashCount);
        at += 4;
        foreach (var word in bloom.Bits)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(s[at..], word);
            at += 8;
        }

        return buf;
    }
}

/// <summary>What a run holds, as the writer reports it.</summary>
public readonly record struct RunInfo(RunFile.Kind Kind, long Entries, long Groups, int Blocks, long Bytes);

/// <summary>One block of a run: the hash range it covers, where it is, and how much it holds.</summary>
public readonly record struct RunBlock(uint FirstHash, uint LastHash, long Offset, int Length, int Entries, int Groups);

/// <summary>A Bloom filter over 32-bit key hashes (~10 bits per key, 7 probes): rejects most
/// probes for keys a run does not hold without touching a block.</summary>
public sealed class RunBloom
{
    internal RunBloom(ulong[] bits, int hashCount)
    {
        Bits = bits;
        HashCount = hashCount;
    }

    internal ulong[] Bits { get; }

    internal int HashCount { get; }

    internal static RunBloom Build(IReadOnlyCollection<uint> hashes)
    {
        var words = Math.Max(1, (int)Math.Min(int.MaxValue / 64, ((long)hashes.Count * 10 + 63) / 64));
        var bloom = new RunBloom(new ulong[words], 7);
        foreach (var h in hashes)
        {
            bloom.Add(h);
        }

        return bloom;
    }

    public bool MightContain(uint hash)
    {
        var bitCount = (ulong)Bits.Length * 64;
        var (h1, h2) = Split(hash);
        for (var i = 0; i < HashCount; i++)
        {
            var bit = (h1 + ((ulong)i * h2)) % bitCount;
            if ((Bits[bit >> 6] & (1UL << (int)(bit & 63))) == 0)
            {
                return false;
            }
        }

        return true;
    }

    private void Add(uint hash)
    {
        var bitCount = (ulong)Bits.Length * 64;
        var (h1, h2) = Split(hash);
        for (var i = 0; i < HashCount; i++)
        {
            var bit = (h1 + ((ulong)i * h2)) % bitCount;
            Bits[bit >> 6] |= 1UL << (int)(bit & 63);
        }
    }

    // Two independent-looking 64-bit hashes from one 32-bit hash (double hashing).
    private static (ulong, ulong) Split(uint hash)
    {
        var x = (ulong)hash * 0x9E3779B97F4A7C15UL;
        x ^= x >> 31;
        x *= 0xBF58476D1CE4E5B9UL;
        x ^= x >> 29;
        var y = (x * 0x94D049BB133111EBUL) | 1UL;
        return (x, y);
    }
}
