// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.IO.Local;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>Whole-block run compression (docs/design-bounded-memory.md §5.26): blocks are stored as
/// zstd frames, written in order though compressed in parallel, and read back byte for byte, by every
/// read path; mixed and pre-compression (version 4) runs still read.</summary>
public sealed class RunCompressionTests
{
    private static readonly Schema RowSchema = new(
    [
        new SchemaColumn("k", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static BlockSchema Format() => (BlockSchema)TraceBlockFormats.Create(RowSchema, Schema.Empty)!;

    private static int Fingerprint => RunFile.FingerprintRow.GetHashCode();

    // Blocks of perBlock rows each, in hash order (compressible: a repeated string prefix).
    private static List<TraceBlock> Blocks(BlockSchema format, int blocks, int perBlock) =>
        Blocks(format, [.. Enumerable.Repeat(perBlock, blocks)]);

    // Blocks of the given sizes, in hash order.
    private static List<TraceBlock> Blocks(BlockSchema format, int[] sizes)
    {
        var all = Enumerable.Range(0, sizes.Sum())
            .Select(i => new StructuralRow(i, Utf8String.Of(new string('r', 40) + i)))
            .OrderBy(r => (uint)r.GetHashCode())
            .ToArray();
        var result = new List<TraceBlock>();
        var at = 0;
        foreach (var n in sizes)
        {
            var keys = all[at..(at + n)];
            at += n;
            result.Add(format.Encode(
                keys, [.. keys.Select(_ => BlockedIndexedTrace.NoValue)], [.. keys.Select(_ => 1L)], [.. keys.Select(k => k.GetHashCode())], n));
        }

        return result;
    }

    private static List<uint> Distinct(ReadOnlySpan<uint> hashes)
    {
        var d = new List<uint>();
        foreach (var h in hashes)
        {
            if (d.Count == 0 || d[^1] != h)
            {
                d.Add(h);
            }
        }

        return d;
    }

    private static List<StructuralRow> Keys(ITraceBlock block) => [.. Enumerable.Range(0, block.Count).Select(block.Key)];

    private static async Task<RunStreamWriter> CreateAsync(ITableFileSystem fs, string path, BlockSchema format) =>
        await RunStreamWriter.CreateAsync(fs, path, format.Codec.RunSchema, RunFile.Kind.Indexed, Fingerprint, CancellationToken.None);

    private static string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "dbspnet-compression-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    public static TheoryData<bool> Stores => new() { true, false };

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task A_streamed_run_is_compressed_and_reads_back_exactly(bool local)
    {
        var dir = TempDir();
        try
        {
            ITableFileSystem fs = local ? new LocalTableFileSystem(dir) : new InMemoryTableFileSystem();
            var format = Format();
            var blocks = Blocks(format, blocks: 60, perBlock: 50);
            using (BlockCompression.Use(BlockCodec.Zstd))
            {
                var sink = BlockedTraceCheckpoint.StreamedRunFile.Open(fs, "z.run", format.Codec, _ => new RunFileLease(() => { }));
                foreach (var b in blocks)
                {
                    sink.Append(b);
                }

                var (written, _, _) = sink.Finish();
                Assert.Equal(blocks.Count, written.Length);
            }

            await using var run = await RunReader.OpenDetachedAsync(fs, "z.run", r => r.GetHashCode());
            Assert.Equal(blocks.Count, run.Blocks.Count);
            for (var b = 0; b < blocks.Count; b++)
            {
                var index = run.Blocks[b];
                Assert.Equal(BlockCodec.Zstd, index.Codec);
                Assert.Equal(blocks[b].Message.Length, index.MessageLength);
                Assert.True(index.Length < index.MessageLength / 2, $"block {b}: {index.Length} of {index.MessageLength} bytes");

                // A fault reads it back byte for byte; so does the reader's own path.
                var lazy = new LazyTraceBlock(format, fs, "z.run", index);
                Assert.Equal(blocks[b].Message.ToArray(), lazy.Loaded.Message.ToArray());
                var (stored, hashes) = await run.ReadBlockRawAsync(b);
                Assert.Equal(BlockCodec.Zstd, stored.Codec);
                Assert.Equal(Distinct(blocks[b].Hashes), hashes);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task Without_compression_blocks_are_stored_raw()
    {
        var fs = new InMemoryTableFileSystem();
        var format = Format();
        var blocks = Blocks(format, blocks: 5, perBlock: 50);
        using (BlockCompression.Use(BlockCodec.None))
        {
            await using var w = await CreateAsync(fs, "n.run", format);
            foreach (var b in blocks)
            {
                await w.AppendRawAsync(b.Message, Distinct(b.Hashes), b.Count, Distinct(b.Hashes).Count, CancellationToken.None);
            }

            await w.FinishAsync(CancellationToken.None);
        }

        await using var run = await RunReader.OpenDetachedAsync(fs, "n.run", r => r.GetHashCode());
        Assert.All(run.Blocks, ix => Assert.Equal((BlockCodec.None, ix.Length), (ix.Codec, ix.MessageLength)));
        for (var b = 0; b < blocks.Count; b++)
        {
            Assert.Equal(Keys(blocks[b]), Keys(new LazyTraceBlock(format, fs, "n.run", run.Blocks[b])));
        }
    }

    [Fact]
    public async Task A_giant_block_streams_through_the_compressor()
    {
        var fs = new InMemoryTableFileSystem();
        var format = Format();
        var parts = Blocks(format, blocks: 8, perBlock: 200);
        var hashes = parts.SelectMany(p => Distinct(p.Hashes)).ToList();
        RunBlock? index = null;
        using (BlockCompression.Use(BlockCodec.Zstd))
        {
            await using var w = await CreateAsync(fs, "g.run", format);
            await w.AppendConcatAsync(format, parts, hashes, parts.Sum(p => p.Count), hashes.Count, CancellationToken.None, ix => index = ix);
            await w.FinishAsync(CancellationToken.None);
        }

        Assert.NotNull(index);
        Assert.Equal(BlockCodec.Zstd, index.Value.Codec);
        Assert.Equal(parts.Sum(p => p.Count), index.Value.Entries);
        Assert.True(index.Value.Length < index.Value.MessageLength / 2);
        var block = new LazyTraceBlock(format, fs, "g.run", index.Value);
        Assert.Equal(parts.SelectMany(Keys).ToList(), Keys(block));
    }

    [Fact]
    public async Task A_run_mixes_codecs_block_by_block_and_copies_stored_blocks_unchanged()
    {
        var fs = new InMemoryTableFileSystem();
        var format = Format();
        var blocks = Blocks(format, blocks: 4, perBlock: 50);
        using (BlockCompression.Use(BlockCodec.Zstd))
        {
            await using var w = await CreateAsync(fs, "z.run", format);
            foreach (var b in blocks)
            {
                await w.AppendRawAsync(b.Message, Distinct(b.Hashes), b.Count, Distinct(b.Hashes).Count, CancellationToken.None);
            }

            await w.FinishAsync(CancellationToken.None);
        }

        // An uncompressed writer copying the first two blocks as stored (as compaction does) and
        // appending the rest from their messages: zstd, zstd, none, none.
        await using (var source = await RunReader.OpenDetachedAsync(fs, "z.run", r => r.GetHashCode()))
        using (BlockCompression.Use(BlockCodec.None))
        {
            await using var w = await CreateAsync(fs, "mixed.run", format);
            for (var b = 0; b < blocks.Count; b++)
            {
                var hashes = Distinct(blocks[b].Hashes);
                if (b < 2)
                {
                    var (stored, keyHashes) = await source.ReadBlockRawAsync(b);
                    Assert.Equal(hashes, keyHashes);
                    await w.AppendStoredAsync(stored, keyHashes, blocks[b].Count, hashes.Count, CancellationToken.None);
                }
                else
                {
                    await w.AppendRawAsync(blocks[b].Message, hashes, blocks[b].Count, hashes.Count, CancellationToken.None);
                }
            }

            await w.FinishAsync(CancellationToken.None);
        }

        await using var run = await RunReader.OpenDetachedAsync(fs, "mixed.run", r => r.GetHashCode());
        Assert.Equal([BlockCodec.Zstd, BlockCodec.Zstd, BlockCodec.None, BlockCodec.None], run.Blocks.Select(ix => ix.Codec));
        for (var b = 0; b < blocks.Count; b++)
        {
            Assert.Equal(Keys(blocks[b]), Keys(new LazyTraceBlock(format, fs, "mixed.run", run.Blocks[b])));
        }
    }

    [Fact]
    public async Task Blocks_still_compressing_are_written_in_append_order()
    {
        var fs = new InMemoryTableFileSystem();
        var format = Format();

        // Big and small blocks alternating: the small ones finish compressing first.
        var blocks = Blocks(format, [.. Enumerable.Range(0, 40).Select(i => i % 2 == 0 ? 4000 : 10)]);
        var order = new List<int>();
        var saved = RunStreamWriter.MaxInFlight;
        try
        {
            RunStreamWriter.MaxInFlight = 8;
            using (BlockCompression.Use(BlockCodec.Zstd))
            {
                await using var w = await CreateAsync(fs, "o.run", format);
                for (var b = 0; b < blocks.Count; b++)
                {
                    var at = b;
                    var hashes = Distinct(blocks[b].Hashes);
                    await w.AppendRawAsync(blocks[b].Message, hashes, blocks[b].Count, hashes.Count, CancellationToken.None, _ => order.Add(at));
                }

                await w.FinishAsync(CancellationToken.None);
            }
        }
        finally
        {
            RunStreamWriter.MaxInFlight = saved;
        }

        Assert.Equal(Enumerable.Range(0, blocks.Count), order);
        await using var run = await RunReader.OpenDetachedAsync(fs, "o.run", r => r.GetHashCode());
        var offsets = run.Blocks.Select(ix => ix.Offset).ToList();
        Assert.Equal(offsets.Order(), offsets);
        for (var b = 0; b < blocks.Count; b++)
        {
            Assert.Equal(Keys(blocks[b]), Keys(new LazyTraceBlock(format, fs, "o.run", run.Blocks[b])));
        }
    }

    [Fact]
    public async Task A_version_4_run_still_reads()
    {
        var fs = new InMemoryTableFileSystem();
        var format = Format();
        var blocks = Blocks(format, blocks: 3, perBlock: 50);
        using (BlockCompression.Use(BlockCodec.None))
        {
            await using var w = await CreateAsync(fs, "v5.run", format);
            foreach (var b in blocks)
            {
                await w.AppendRawAsync(b.Message, Distinct(b.Hashes), b.Count, Distinct(b.Hashes).Count, CancellationToken.None);
            }

            await w.FinishAsync(CancellationToken.None);
        }

        // Rewrite the footer as version 4 wrote it: 28-byte index entries, no codec or message length.
        var bytes = await fs.ReadAllBytesAsync("v5.run");
        var footerLength = (int)BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(bytes.Length - 16));
        var footerAt = bytes.Length - 16 - footerLength;
        var f = bytes.AsSpan(footerAt, footerLength);
        var count = BinaryPrimitives.ReadInt32LittleEndian(f[44..]);
        var v4 = new List<byte>();
        v4.AddRange(f[..48].ToArray());
        BinaryPrimitives.WriteInt32LittleEndian(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(v4)[8..], 4);
        for (var i = 0; i < count; i++)
        {
            v4.AddRange(f.Slice(48 + (i * RunFile.BlockIndexEntryBytes), RunFile.BlockIndexEntryBytesV4).ToArray());
        }

        v4.AddRange(f[(48 + (count * RunFile.BlockIndexEntryBytes))..].ToArray());
        var trailer = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(trailer, v4.Count);
        RunFile.Magic.CopyTo(trailer.AsSpan(8));
        await fs.WriteAllBytesAsync("v4.run", (byte[])[.. bytes.AsSpan(0, footerAt).ToArray(), .. v4, .. trailer]);

        await using var run = await RunReader.OpenDetachedAsync(fs, "v4.run", r => r.GetHashCode());
        Assert.Equal(blocks.Count, run.Blocks.Count);
        for (var b = 0; b < blocks.Count; b++)
        {
            Assert.Equal((BlockCodec.None, blocks[b].Message.Length), (run.Blocks[b].Codec, run.Blocks[b].MessageLength));
            Assert.Equal(Keys(blocks[b]), Keys(new LazyTraceBlock(format, fs, "v4.run", run.Blocks[b])));
        }
    }
}
