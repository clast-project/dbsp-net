// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A run gathered from other blocks' entries (docs/design-bounded-memory.md §3h), as an output row
// index takes a view's rows: it holds what the same entries integrated as rows hold, and its blocks
// are cut as a freeze cuts rows — by bytes as well as entries, never inside a hash — so wide rows
// make small-count blocks rather than blocks of blockEntries rows.
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

public class GatheredRunTests
{
    private static readonly Schema Key = new([
        new SchemaColumn("k", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static readonly Schema Address = new([new SchemaColumn("address", new SqlBigintType(false))]);

    [Theory]
    [InlineData(10, 1)]
    [InlineData(400, 2)]
    public void AGatheredRun_HoldsTheRowsRun_AndIsCutByBytes(int width, int seed)
    {
        var view = BlockSchema.TryCreate(Key, new Schema([]))!;
        var index = BlockSchema.TryCreate(Key, Address)!;
        var rng = new Random(seed);

        // Source blocks of view rows (keys only), in hash order as a view's runs are.
        var rows = Enumerable.Range(0, 3000)
            .Select(i => new StructuralRow([rng.Next(0, 50), rng.Next(6) == 0 ? null : (object)Utf8String.Of(new string((char)('a' + (i % 26)), rng.Next(0, width)))]))
            .OrderBy(r => (uint)r.GetHashCode())
            .ToList();
        var sources = new List<TraceBlock>();
        for (var at = 0; at < rows.Count; at += 500)
        {
            var part = rows.Skip(at).Take(500).ToArray();
            sources.Add(view.Encode(part, [.. part.Select(_ => new StructuralRow([]))], [.. part.Select(_ => 1L)], [.. part.Select(r => r.GetHashCode())], part.Length));
        }

        var blocks = sources.SelectMany(b => Enumerable.Range(0, b.Count).Select(_ => (ITraceBlock)b)).ToArray();
        var indexes = sources.SelectMany(b => Enumerable.Range(0, b.Count)).ToArray();
        var addresses = Enumerable.Range(0, rows.Count).Select(i => (long)i * 7).ToArray();

        const int BlockBytes = 4096;
        var gathered = new BlockedIndexedTrace(index, blockEntries: 1024, blockBytes: BlockBytes);
        Assert.True(gathered.IntegrateGathered(blocks, indexes, addresses));
        var byRows = new BlockedIndexedTrace(index, blockEntries: 1024, blockBytes: BlockBytes);
        byRows.IntegrateEntries(rows.Count, i => blocks[i].Key(indexes[i]), i => new StructuralRow([addresses[i]]), _ => 1);
        Assert.True(gathered.Materialize().Equals(byRows.Materialize()), "the gathered run holds other entries than the rows' run");

        // Every block but a hash's own stays within the budget plus one entry, and wide rows give
        // blocks well under blockEntries.
        var run = Assert.Single(gathered.Runs);
        foreach (var b in run.Blocks)
        {
            var bytes = Enumerable.Range(0, b.Count).Sum(i => ((ITraceBlockFormat)index).GatheredEntryBytes(Source(blocks, indexes, b, i), SourceIndex(blocks, indexes, b, i)));
            Assert.True(bytes < BlockBytes + 1024 || b.FirstHash == b.LastHash, $"a block of {b.Count} entries holds {bytes} bytes");
        }

        if (width > 100)
        {
            Assert.True(run.Blocks.Length > rows.Count / 1024 + 1, $"{run.Blocks.Length} blocks for {rows.Count} wide rows: cut by entries only");
        }
    }

    // The source entry of gathered block b's entry i: matched by key and address order.
    private static ITraceBlock Source(ITraceBlock[] blocks, int[] indexes, ITraceBlock b, int i) =>
        blocks[Find(blocks, indexes, b, i)];

    private static int SourceIndex(ITraceBlock[] blocks, int[] indexes, ITraceBlock b, int i) =>
        indexes[Find(blocks, indexes, b, i)];

    private static int Find(ITraceBlock[] blocks, int[] indexes, ITraceBlock b, int i) =>
        (int)((long)b.Value(i)[0]! / 7);
}
