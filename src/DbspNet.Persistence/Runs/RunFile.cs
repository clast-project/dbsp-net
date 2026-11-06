// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Apache.Arrow.Ipc;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Persistence.IO;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// A <b>run</b>: an immutable file holding one Z-set (or indexed Z-set) — a trace's state, or the
/// change to it since the last checkpoint — laid out so a single key can be read without decoding
/// the rest (docs/design-durable-state.md §1f).
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> Entries are ordered by the key's stable hash (as an unsigned 32-bit value)
/// and cut into blocks of about <c>blockEntries</c> entries. A block never splits one hash value,
/// so colliding keys — and every value of an indexed key — share a block. Each block also carries
/// every entry's order hash (<c>__hash</c>, ascending), so a point probe binary-searches it and
/// decodes only the matching entries rather than the whole block. The file opens as one
/// Arrow IPC stream in the operator's schema: the schema message once, then one record batch per
/// block, then the end-of-stream marker — so any Arrow reader sees the whole trace. After it comes
/// a footer: counts, the schema message's length, the block index (hash range, offset, length,
/// counts per block) and a Bloom filter over the key hashes; then a 16-byte trailer (footer length,
/// magic).</para>
/// <para><b>Reading one key</b> costs a Bloom check, a binary search of the index and one block
/// decode (the cached schema message plus that block's record batch). The hash must be stable
/// across processes; the trace codecs use the row's own cached hash (<c>StructuralRowHash</c>,
/// deterministic). The footer records the hash of <see cref="FingerprintRow"/> — one cell of every
/// type — so a run written under a different hash function is refused at open instead of being
/// probed in the wrong order.</para>
/// <para><b>Writing</b> sorts (hash, position) pairs and encodes each block straight from the
/// trace's own rows: no intermediate Z-set, no scratch file, one schema per file.</para>
/// </remarks>
internal static class RunFile
{
    internal static ReadOnlySpan<byte> Magic => "DBSPRUN1"u8;

    /// <summary>Version 5 adds each block's codec and message length to the index (whole-block
    /// compression, docs/design-bounded-memory.md §5.26).</summary>
    internal const int FormatVersion = 5;

    /// <summary>The oldest format a reader still opens: version 3 never lets a hash span blocks, which
    /// version 4 allows (docs/design-bounded-memory.md §5.18), so a v3 run reads the same either way.</summary>
    internal const int OldestReadableVersion = 3;

    /// <summary>The most bytes a giant group's file block holds before the group continues in the next
    /// block (§5.18): a block is one record-batch message read into one array, so it must stay well
    /// under 2 GiB. A test seam; <c>DBSPNET_RUN_SPAN_BLOCK_BYTES</c> sets it for a suite run.</summary>
    internal static long MaxSpanBlockBytes =
        long.TryParse(Environment.GetEnvironmentVariable("DBSPNET_RUN_SPAN_BLOCK_BYTES"), out var cap) && cap > 0 ? cap : 256L << 20;

    /// <summary>The most entries of one hash a compacted block holds before the hash continues in the
    /// next block (§5.18; compaction cuts by entries). A test seam; <c>DBSPNET_RUN_SPAN_BLOCK_ENTRIES</c>.</summary>
    internal static int MaxSpanBlockEntries =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_RUN_SPAN_BLOCK_ENTRIES"), out var cap) && cap > 0 ? cap : 1 << 18;
    internal const int TrailerBytes = 16;
    internal const int BlockIndexEntryBytes = 4 + 4 + 8 + 4 + 4 + 4 + 4 + 4;

    /// <summary>A version 3 or 4 index entry: no codec, no message length (every block raw).</summary>
    internal const int BlockIndexEntryBytesV4 = 4 + 4 + 8 + 4 + 4 + 4;
    internal const int DefaultBlockEntries = 1024;

    /// <summary>A row with one cell of every engine type (and a null): its hash pins the hash
    /// function a run was ordered by.</summary>
    internal static readonly StructuralRow FingerprintRow = new(
        7, 1L, true, 1.5, Utf8String.Of("a"), new Date32(1), new Timestamp(1), new Time64(1),
        new Decimal128((Int128)1), null);

    /// <summary>What kind of collection a run holds.</summary>
    public enum Kind
    {
        ZSet = 0,
        Indexed = 1,
    }

    /// <summary>Write <paramref name="set"/> as a run at <paramref name="path"/>, ordered by
    /// <paramref name="hash"/>; <paramref name="fingerprint"/> (default: <paramref name="hash"/>) is the
    /// row hash recorded to pin it — a run ordered by a partition key's hash pins the row hash it is built on.</summary>
    public static ValueTask<RunInfo> WriteZSetAsync(
        ITableFileSystem fs,
        string path,
        ZSet<StructuralRow, Z64> set,
        IZSetRunCodec codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        Func<StructuralRow, int>? fingerprint = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        return WriteZSetAsync(ct => fs.CreateAsync(path, overwrite: true, ct), set, codec, hash, blockEntries, fingerprint, cancellationToken);
    }

    /// <summary>Write <paramref name="set"/> as a run to <paramref name="fileName"/> of a snapshot.</summary>
    internal static ValueTask<RunInfo> WriteZSetAsync(
        Core.Circuit.ISnapshotWriter writer,
        string fileName,
        ZSet<StructuralRow, Z64> set,
        IZSetRunCodec codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default) =>
        WriteZSetAsync(ct => writer.CreateAsync(fileName, ct), set, codec, hash, blockEntries, null, cancellationToken);

    private static async ValueTask<RunInfo> WriteZSetAsync(
        Func<CancellationToken, ValueTask<ISequentialFile>> create,
        ZSet<StructuralRow, Z64> set,
        IZSetRunCodec codec,
        Func<StructuralRow, int> hash,
        int blockEntries,
        Func<StructuralRow, int>? fingerprint,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentNullException.ThrowIfNull(codec);
        var t = RunWriteProfile.Now();
        var n = set.Count;
        var hashes = new uint[n];
        var rows = new StructuralRow[n];
        var weights = new long[n];
        var i = 0;
        foreach (var (key, weight) in set)
        {
            hashes[i] = (uint)hash(key);
            rows[i] = key;
            weights[i] = weight.Value;
            i++;
        }

        RunWriteProfile.Add(0, t);
        t = RunWriteProfile.Now();
        var order = Identity(n);
        Array.Sort(hashes, order);
        RunWriteProfile.Add(1, t);
        t = RunWriteProfile.Now();
        var cuts = Cut(hashes, _ => 1, blockEntries, out var largest);
        RunWriteProfile.Add(2, t);
        RunWriteProfile.Write(n);

        var blockRows = new StructuralRow[largest];
        var blockWeights = new long[largest];
        var blockHashes = new int[largest];
        return await WriteAsync(
            create, Kind.ZSet, codec.RunSchema, (fingerprint ?? hash)(FingerprintRow), n, n, hashes, cuts,
            (from, to) =>
            {
                var count = to - from;
                for (var j = 0; j < count; j++)
                {
                    var at = order[from + j];
                    blockRows[j] = rows[at];
                    blockWeights[j] = weights[at];
                    blockHashes[j] = (int)hashes[from + j];
                }

                return (codec.EncodeBlock(blockRows, blockWeights, blockHashes, count), count);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Write <paramref name="set"/> as a run at <paramref name="path"/>; the hash is over
    /// the key, and every value of a key lands in the same block.</summary>
    public static ValueTask<RunInfo> WriteIndexedAsync(
        ITableFileSystem fs,
        string path,
        IndexedZSet<StructuralRow, StructuralRow, Z64> set,
        IIndexedRunCodec codec,
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
        IIndexedRunCodec codec,
        Func<StructuralRow, int> hash,
        int blockEntries = DefaultBlockEntries,
        CancellationToken cancellationToken = default) =>
        WriteIndexedAsync(ct => writer.CreateAsync(fileName, ct), set, codec, hash, blockEntries, cancellationToken);

    private static async ValueTask<RunInfo> WriteIndexedAsync(
        Func<CancellationToken, ValueTask<ISequentialFile>> create,
        IndexedZSet<StructuralRow, StructuralRow, Z64> set,
        IIndexedRunCodec codec,
        Func<StructuralRow, int> hash,
        int blockEntries,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentNullException.ThrowIfNull(codec);
        var t = RunWriteProfile.Now();
        var g = set.GroupCount;
        var hashes = new uint[g];
        var keys = new StructuralRow[g];
        var groups = new ZSet<StructuralRow, Z64>[g];
        long total = 0;
        var i = 0;
        foreach (var group in set)
        {
            hashes[i] = (uint)hash(group.Key);
            keys[i] = group.Key;
            groups[i] = group.Value;
            total += group.Value.Count;
            i++;
        }

        RunWriteProfile.Add(0, t);
        t = RunWriteProfile.Now();
        var order = Identity(g);
        Array.Sort(hashes, order);
        RunWriteProfile.Add(1, t);
        t = RunWriteProfile.Now();
        var cuts = Cut(hashes, j => groups[order[j]].Count, blockEntries, out var largest);
        RunWriteProfile.Add(2, t);
        RunWriteProfile.Write(total);

        var blockKeys = new StructuralRow[largest];
        var blockValues = new StructuralRow[largest];
        var blockWeights = new long[largest];
        var blockHashes = new int[largest];
        return await WriteAsync(
            create, Kind.Indexed, codec.RunSchema, hash(FingerprintRow), total, g, hashes, cuts,
            (from, to) =>
            {
                var count = 0;
                for (var j = from; j < to; j++)
                {
                    var at = order[j];
                    var key = keys[at];
                    foreach (var (value, weight) in groups[at])
                    {
                        blockKeys[count] = key;
                        blockValues[count] = value;
                        blockWeights[count] = weight.Value;
                        blockHashes[count] = (int)hashes[j];
                        count++;
                    }
                }

                return (codec.EncodeBlock(blockKeys, blockValues, blockWeights, blockHashes, count), count);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private static int[] Identity(int n)
    {
        var order = new int[n];
        for (var i = 0; i < n; i++)
        {
            order[i] = i;
        }

        return order;
    }

    // Block boundaries [from, to) over sorted hashes: close a block once it holds blockEntries
    // entries, but only where the hash changes — and also before a group that fills a block on its
    // own, so a giant group always has a block to itself (the lazy restore reads those at restore,
    // by the block index: one group, many entries). largest = the most entries any block holds.
    private static List<(int From, int To)> Cut(uint[] sorted, Func<int, int> sizeOf, int blockEntries, out int largest)
    {
        var blocks = new List<(int, int)>();
        var start = 0;
        var size = 0;
        largest = 0;
        for (var i = 0; i < sorted.Length; i++)
        {
            var next = sizeOf(i);
            if (size > 0 && (size >= blockEntries || next >= blockEntries) && sorted[i] != sorted[i - 1])
            {
                blocks.Add((start, i));
                largest = Math.Max(largest, size);
                start = i;
                size = 0;
            }

            size += next;
        }

        if (start < sorted.Length)
        {
            blocks.Add((start, sorted.Length));
            largest = Math.Max(largest, size);
        }

        return blocks;
    }

    private static async ValueTask<RunInfo> WriteAsync(
        Func<CancellationToken, ValueTask<ISequentialFile>> create,
        Kind kind,
        Apache.Arrow.Schema schema,
        int fingerprint,
        long totalEntries,
        long totalGroups,
        uint[] sortedHashes,
        List<(int From, int To)> cuts,
        Func<int, int, (Apache.Arrow.RecordBatch Batch, int Entries)> encode,
        CancellationToken cancellationToken)
    {
        var index = new List<RunBlock>(cuts.Count);
        await using var file = await create(cancellationToken).ConfigureAwait(false);

        // Not disposed: disposing the adapter disposes the file, which the using above owns.
        var stream = file.AsStream();
        long schemaLength;
        using (var ipc = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            await ipc.WriteStartAsync(cancellationToken).ConfigureAwait(false);
            schemaLength = file.Position;
            foreach (var (from, to) in cuts)
            {
                var t = RunWriteProfile.Now();
                var (batch, entries) = encode(from, to);
                RunWriteProfile.Add(3, t);
                t = RunWriteProfile.Now();
                using (batch)
                {
                    var offset = file.Position;
                    await ipc.WriteRecordBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                    var length = checked((int)(file.Position - offset));
                    index.Add(new RunBlock(sortedHashes[from], sortedHashes[to - 1], offset, length, entries, to - from, length));
                }

                RunWriteProfile.Add(4, t);
            }

            await ipc.WriteEndAsync(cancellationToken).ConfigureAwait(false);
        }

        var tf = RunWriteProfile.Now();
        var bloom = RunBloom.Build(sortedHashes);
        var footer = EncodeFooter(kind, fingerprint, totalEntries, totalGroups, schemaLength, index, bloom);
        await file.WriteAsync(footer, cancellationToken).ConfigureAwait(false);
        var trailer = new byte[TrailerBytes];
        BinaryPrimitives.WriteInt64LittleEndian(trailer, footer.Length);
        Magic.CopyTo(trailer.AsSpan(8));
        await file.WriteAsync(trailer, cancellationToken).ConfigureAwait(false);
        await file.FlushAsync(cancellationToken).ConfigureAwait(false);
        RunWriteProfile.Add(5, tf);
        return new RunInfo(kind, totalEntries, totalGroups, index.Count, file.Position);
    }

    internal static byte[] EncodeFooter(Kind kind, int fingerprint, long entries, long groups, long schemaLength, List<RunBlock> index, RunBloom bloom)
    {
        var size = 8 + 4 + 4 + 4 + 8 + 8 + 8 + 4 + (index.Count * BlockIndexEntryBytes) + 4 + 4 + (bloom.Bits.Length * 8);
        var buf = new byte[size];
        var s = buf.AsSpan();
        Magic.CopyTo(s);
        var at = 8;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], FormatVersion);
        at += 4;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], (int)kind);
        at += 4;
        BinaryPrimitives.WriteInt32LittleEndian(s[at..], fingerprint);
        at += 4;
        BinaryPrimitives.WriteInt64LittleEndian(s[at..], entries);
        at += 8;
        BinaryPrimitives.WriteInt64LittleEndian(s[at..], groups);
        at += 8;
        BinaryPrimitives.WriteInt64LittleEndian(s[at..], schemaLength);
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
            BinaryPrimitives.WriteInt32LittleEndian(s[(at + 28)..], b.MessageLength);
            BinaryPrimitives.WriteInt32LittleEndian(s[(at + 32)..], (int)b.Codec);
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
internal readonly record struct RunInfo(RunFile.Kind Kind, long Entries, long Groups, int Blocks, long Bytes);

/// <summary>One block of a run: the hash range it covers, where it is (<paramref name="Length"/> stored
/// bytes), how much it holds, and how it is stored — a <paramref name="Codec"/> other than none decodes
/// to a record-batch message of <paramref name="RawLength"/> bytes.</summary>
internal readonly record struct RunBlock(
    uint FirstHash, uint LastHash, long Offset, int Length, int Entries, int Groups, int RawLength = 0, BlockCodec Codec = BlockCodec.None)
{
    /// <summary>The block's record-batch message length, decoded.</summary>
    public int MessageLength => Codec == BlockCodec.None ? Length : RawLength;
}

/// <summary>A Bloom filter over 32-bit key hashes (~10 bits per key, 7 probes): rejects most
/// probes for keys a run does not hold without touching a block.</summary>
internal sealed class RunBloom
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
