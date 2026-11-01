// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The run file (docs/design-durable-state.md §1f, increment 1): a Z-set or indexed Z-set written
// in hash-ordered blocks must read back exactly — every key by point lookup, every entry by scan,
// absent keys as absent — whatever the size, the block size, and however many keys collide.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Persistence;

public class RunFileTests
{
    private static readonly Schema RowSchema = new([
        new SchemaColumn("k", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
        new SchemaColumn("d", new SqlBigintType(true)),
    ]);

    private static readonly Schema KeySchema = new([new SchemaColumn("k", new SqlIntegerType(false))]);

    private static Func<StructuralRow, int> Hash(string kind, Schema schema) => kind switch
    {
        "stable" => row => StablePartitionHash.OfWholeRow(row, schema),
        // Few distinct hash values: most keys collide, and blocks can only cut between them.
        "collide" => row => (int)row[0]! % 5,
        _ => throw new ArgumentException(kind),
    };

    private static StructuralRow Row(Random rng, int k) => new(
        k,
        rng.Next(4) == 0 ? null : Utf8String.Of("s" + rng.Next(1000)),
        rng.Next(5) == 0 ? null : (object)(long)rng.Next(-1000, 1000));

    private static ZSet<StructuralRow, Z64> RandomZSet(int n, int seed)
    {
        var rng = new Random(seed);
        var b = new ZSetBuilder<StructuralRow, Z64>();
        for (var i = 0; i < n; i++)
        {
            var w = rng.Next(1, 4) * (rng.Next(3) == 0 ? -1 : 1);
            b.Add(Row(rng, rng.Next(0, Math.Max(1, n * 2))), new Z64(w));
        }

        return b.Build();
    }

    [Theory]
    [InlineData(0, 16, "stable")]
    [InlineData(1, 16, "stable")]
    [InlineData(7, 1, "stable")]
    [InlineData(5000, 16, "stable")]
    [InlineData(5000, 1024, "stable")]
    [InlineData(2000, 1, "collide")]
    [InlineData(2000, 64, "collide")]
    public async Task ZSetRun_ReadsBackExactly(int n, int blockEntries, string hashKind)
    {
        var fs = new InMemoryTableFileSystem();
        var codec = new ArrowZSetTraceCodec(RowSchema);
        var hash = Hash(hashKind, RowSchema);
        var set = RandomZSet(n, n + blockEntries);
        var info = await RunFile.WriteZSetAsync(fs, "run", set, codec, hash, blockEntries);
        Assert.Equal(set.Count, info.Entries);

        await using var run = await RunReader.OpenAsync(fs, "run", hash);
        Assert.Equal(set.Count, run.Entries);
        Assert.Equal(set.Count, run.Groups);
        AssertBlocksAscendingAndDisjoint(run);

        foreach (var (row, weight) in set)
        {
            Assert.True(run.MightContain(row));
            Assert.Equal(weight, await run.WeightOfAsync(row, codec));
        }

        // Rows the set does not hold: same keys with other cells, and keys out of range.
        var rng = new Random(99);
        for (var i = 0; i < 300; i++)
        {
            var probe = Row(rng, rng.Next(-50, n * 3));
            Assert.Equal(set.WeightOf(probe), await run.WeightOfAsync(probe, codec));
        }

        var scanned = new ZSetBuilder<StructuralRow, Z64>();
        await foreach (var block in run.ScanZSetAsync(codec))
        {
            foreach (var (row, weight) in block)
            {
                scanned.Add(row, weight);
            }
        }

        Assert.Equal(Sorted(set), Sorted(scanned.Build()));
    }

    [Theory]
    [InlineData(300, 16, "stable")]
    [InlineData(300, 1, "stable")]
    [InlineData(300, 64, "collide")]
    public async Task IndexedRun_ReadsBackExactly_AndNeverSplitsAGroup(int keys, int blockEntries, string hashKind)
    {
        var fs = new InMemoryTableFileSystem();
        var codec = new ArrowIndexedZSetTraceCodec(KeySchema, RowSchema);
        var hash = Hash(hashKind, KeySchema);
        var rng = new Random(keys + blockEntries);
        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        for (var k = 0; k < keys; k++)
        {
            // One key far larger than a block; most small.
            var size = k == 17 ? 3000 : rng.Next(1, 6);
            for (var i = 0; i < size; i++)
            {
                b.Add(new StructuralRow(k), Row(rng, k), new Z64(rng.Next(1, 3)));
            }
        }

        var set = b.Build();
        await RunFile.WriteIndexedAsync(fs, "run", set, codec, hash, blockEntries);
        await using var run = await RunReader.OpenAsync(fs, "run", hash);
        Assert.Equal(set.GroupCount, run.Groups);
        AssertBlocksAscendingAndDisjoint(run);

        foreach (var g in set)
        {
            Assert.Equal(Sorted(g.Value), Sorted(await run.GroupForAsync(g.Key, codec)));
        }

        Assert.True((await run.GroupForAsync(new StructuralRow(-7), codec)).IsEmpty);

        var groups = 0;
        await foreach (var block in run.ScanIndexedAsync(codec))
        {
            groups += block.GroupCount;
        }

        Assert.Equal(set.GroupCount, groups); // a group split across blocks would be counted twice
    }

    [Fact]
    public async Task ACorruptRun_IsRefused()
    {
        var fs = new InMemoryTableFileSystem();
        var codec = new ArrowZSetTraceCodec(RowSchema);
        await RunFile.WriteZSetAsync(fs, "run", RandomZSet(100, 1), codec, Hash("stable", RowSchema), 16);
        var bytes = await fs.ReadAllBytesAsync("run");
        await fs.WriteAllBytesAsync("cut", bytes.AsMemory(0, bytes.Length - 3));
        await Assert.ThrowsAsync<InvalidDataException>(async () => await RunReader.OpenAsync(fs, "cut", Hash("stable", RowSchema)));
        bytes[^1] ^= 0xFF;
        await fs.WriteAllBytesAsync("magic", bytes);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await RunReader.OpenAsync(fs, "magic", Hash("stable", RowSchema)));
    }

    [Fact]
    public async Task TheBloomFilter_RejectsMostAbsentKeys()
    {
        var fs = new InMemoryTableFileSystem();
        var codec = new ArrowZSetTraceCodec(RowSchema);
        var hash = Hash("stable", RowSchema);
        var b = new ZSetBuilder<StructuralRow, Z64>();
        for (var k = 0; k < 20000; k++)
        {
            b.Add(new StructuralRow(k, null, null), Z64.One);
        }

        await RunFile.WriteZSetAsync(fs, "run", b.Build(), codec, hash);
        await using var run = await RunReader.OpenAsync(fs, "run", hash);
        var falsePositives = Enumerable.Range(1_000_000, 20000).Count(k => run.MightContain(new StructuralRow(k, null, null)));
        Assert.True(falsePositives < 20000 * 0.03, $"{falsePositives} false positives of 20000");
    }

    private static void AssertBlocksAscendingAndDisjoint(RunReader run)
    {
        for (var i = 1; i < run.Blocks.Count; i++)
        {
            Assert.True(run.Blocks[i - 1].LastHash < run.Blocks[i].FirstHash, $"blocks {i - 1} and {i} overlap");
        }
    }

    private static List<string> Sorted(ZSet<StructuralRow, Z64> set) =>
        set.Where(e => e.Value.Value != 0).Select(e => string.Join("|", Enumerable.Range(0, e.Key.Count).Select(i => e.Key[i]?.ToString() ?? "∅")) + "#" + e.Value.Value)
            .OrderBy(s => s, StringComparer.Ordinal).ToList();
}
