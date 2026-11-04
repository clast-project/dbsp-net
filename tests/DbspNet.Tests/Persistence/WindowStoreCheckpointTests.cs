// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A window operator's block store checkpointed as its own files (docs/design-bounded-memory.md §3g):
// small partitions' record as the blocked trace's run chain, large partitions' chunks as a block set
// (a manifest naming each chunk's file and index entry, first in its chain under runs/). Nothing is
// decoded to write it, and an unchanged store writes nothing. Required,
// for LAG, RANK and a framed SUM OVER, with large partitions (chunks) and small ones (the record):
//
//   1. a restore adopts the files lazily (no block read, large partitions untouched), outputs match
//      an uninterrupted twin, and go on matching as ticks touch partitions;
//   2. a later checkpoint names chunks an earlier one wrote, writing only new ones, and a store
//      with no change names its whole chain again;
//   3. a block set past its file cap is rewritten whole, so files do not pile up;
//   4. a checkpoint into another store copies every chunk, and restores exactly from there.
using System.Reflection;
using System.Text;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Persistence;

[Collection(nameof(WindowStoreCheckpointTests))]
public class WindowStoreCheckpointTests
{
    private const int Large = 20;   // partitions 0..19: 40 rows each, chunks of 4
    private const int Small = 200;  // partitions 100..299: 2 rows each, the record

    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE VIEW lg AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS p FROM t",
        "CREATE VIEW rk AS SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW ws AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t",
        "CREATE VIEW wr AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t",
    ];

    private static readonly HashSet<string> Views = new(["lg", "rk", "ws", "wr"], StringComparer.Ordinal);

    // Window stores on, partitions past 8 rows as chunks of 4; the twin is compiled the same way.
    private static CompiledProgram Compile()
    {
        using var blocks = TraceBlocksMode.Use(join: true, window: true);
        using var chunks = TraceBlocksMode.UseChunks(promoteRows: 8, chunkRows: 4);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Load(CompiledProgram p)
    {
        for (var a = 0; a < Large; a++)
        {
            for (var b = 0; b < 40; b++)
            {
                p.Table("t").Insert(a, (a * 11 + b * 7) % 60);
            }
        }

        for (var a = 100; a < 100 + Small; a++)
        {
            p.Table("t").Insert(a, a % 13);
            p.Table("t").Insert(a, a % 17 + 20);
        }

        p.Step();
    }

    // A change to the given partitions: an insert, and a delete of a row Load inserted.
    private static void Touch(CompiledProgram p, int tick, params int[] partitions)
    {
        foreach (var a in partitions)
        {
            p.Table("t").Insert(a, (tick * 5 + a) % 60);
            if (a < Large)
            {
                p.Table("t").Delete(a, (a * 11 + tick * 7) % 60);
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    private static List<OrderedPartitionStore> Stores(CompiledProgram p) =>
        [.. p.Circuit.Operators
            .Select(o => o.GetType().GetField("_store", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(o) as OrderedPartitionStore)
            .OfType<OrderedPartitionStore>()];

    // The current snapshot's block-set chains: operator directory -> its manifest and chunk files.
    private static async Task<Dictionary<string, string[]>> ChunkChainsAsync(InMemoryTableFileSystem fs)
    {
        await CheckpointCleanup.WaitAsync(fs);
        var current = Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));
        var chains = new Dictionary<string, string[]>(StringComparer.Ordinal);
        await foreach (var f in fs.ListAsync(current + "/"))
        {
            if (!f.Path.EndsWith("/" + RunIndex.FileName, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var line in Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(f.Path)).Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = line.Split('\t');
                if (parts[0].EndsWith("store.chunks", StringComparison.Ordinal))
                {
                    chains[parts[0]] = parts[5..];
                }
            }
        }

        return chains;
    }

    [Fact]
    public async Task ARestore_AdoptsTheStoresFiles_Lazily_AndCarriesOnExactly()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile();
        var twin = Compile();
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        // No operator wrote its rows: each store wrote its own files.
        Assert.Equal(4, (await ChunkChainsAsync(fs)).Count);

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var stores = Stores(restored);
        // LAG and the two framed sums chunk large partitions; RANK reads whole partitions, which stay
        // small (all in its record).
        Assert.Equal(4, stores.Count);
        Assert.All(stores, s => Assert.Equal(0, s.Laziness.Read));
        var chunked = stores.Where(s => s.Laziness.Untouched > 0).ToList();
        Assert.Equal(3, chunked.Count);
        Assert.All(chunked, s => Assert.Equal(Large, s.Laziness.Untouched));

        Assert.Equal(Digest(twin), Digest(restored));

        // Untouched since the restore, a checkpoint names every chain again: nothing rewritten.
        var written = await ChunkChainsAsync(fs);
        await Snapshot.WriteAsync(restored.Circuit, fs);
        Assert.Equal(written.Values.SelectMany(r => r).Order(), (await ChunkChainsAsync(fs)).Values.SelectMany(r => r).Order());

        for (var tick = 1; tick <= 4; tick++)
        {
            Touch(restored, tick, tick, 100 + tick);
            Touch(twin, tick, tick, 100 + tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }

        // Only the touched large partitions were read in.
        Assert.All(chunked, s => Assert.Equal(Large - 4, s.Laziness.Untouched));

        // And every partition, once more through a checkpoint and a restore.
        await Snapshot.WriteAsync(restored.Circuit, fs);
        var again = Compile();
        await Snapshot.ReadAsync(again.Circuit, fs);
        Touch(again, 9, [.. Enumerable.Range(0, Large), .. Enumerable.Range(100, Small)]);
        Touch(twin, 9, [.. Enumerable.Range(0, Large), .. Enumerable.Range(100, Small)]);
        Assert.Equal(Digest(twin), Digest(again));
    }

    [Fact]
    public async Task ALaterCheckpoint_NamesChunksAnEarlierOneWrote()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile();
        var twin = Compile();
        Load(p);
        Load(twin);
        await Snapshot.WriteAsync(p.Circuit, fs);
        var first = await ChunkChainsAsync(fs);
        Assert.Equal(4, first.Count);
        Assert.Equal(3, first.Values.Count(runs => runs.Length == 2)); // manifest + one chunk file
        Assert.Single(first.Values, runs => runs.Length == 1);       // RANK: a manifest, no chunks

        // Nothing changed: every chain is named again as it was, manifest included.
        await Snapshot.WriteAsync(p.Circuit, fs);
        var same = await ChunkChainsAsync(fs);
        Assert.Equal(first.Values.SelectMany(r => r).Order(), same.Values.SelectMany(r => r).Order());

        // Only small partitions change: every chunk file is named again, no new one written.
        Touch(p, 1, 150, 160);
        Touch(twin, 1, 150, 160);
        await Snapshot.WriteAsync(p.Circuit, fs);
        var second = await ChunkChainsAsync(fs);
        Assert.Equal(first.Values.SelectMany(r => r.Skip(1)).Order(), second.Values.SelectMany(r => r.Skip(1)).Order());

        // A large partition changes: its new chunks go to one new file beside the old.
        Touch(p, 2, 3);
        Touch(twin, 2, 3);
        await Snapshot.WriteAsync(p.Circuit, fs);
        var third = await ChunkChainsAsync(fs);
        Assert.Equal(3, third.Values.Count(runs => runs.Length == 3));

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(twin), Digest(restored));
        Touch(restored, 3, [.. Enumerable.Range(0, Large)]);
        Touch(twin, 3, [.. Enumerable.Range(0, Large)]);
        Assert.Equal(Digest(twin), Digest(restored));
    }

    [Fact]
    public async Task ABlockSetPastItsFileCap_IsRewrittenWhole()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile();
        var twin = Compile();
        Load(p);
        Load(twin);
        await Snapshot.WriteAsync(p.Circuit, fs);

        // Each checkpoint rewrites one large partition's chunks into a new file; the set keeps
        // naming the others' files until it names more than the cap, then starts over in one.
        var longest = 0;
        var restarted = false;
        for (var k = 0; k < Large; k++)
        {
            Touch(p, 10 + k, k);
            Touch(twin, 10 + k, k);
            await Snapshot.WriteAsync(p.Circuit, fs);
            var lengths = (await ChunkChainsAsync(fs)).Values.Where(r => r.Length > 1).Select(r => r.Length - 1).ToList();
            longest = Math.Max(longest, lengths.Max());
            restarted |= k > 0 && lengths.Any(n => n == 1);
        }

        Assert.InRange(longest, BlockedTraceCheckpoint.MaxBlockSetFiles, BlockedTraceCheckpoint.MaxBlockSetFiles + 1);
        Assert.True(restarted, "no block set was rewritten whole");

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(twin), Digest(restored));
        Touch(restored, 99, [.. Enumerable.Range(0, Large)]);
        Touch(twin, 99, [.. Enumerable.Range(0, Large)]);
        Assert.Equal(Digest(twin), Digest(restored));
    }

    [Fact]
    public async Task ACheckpointIntoAnotherStore_CopiesEveryChunk()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile();
        var twin = Compile();
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Touch(restored, 1, 0, 100);
        Touch(twin, 1, 0, 100);

        // Untouched partitions are still file blocks in the first store: copied, not named.
        var other = new InMemoryTableFileSystem();
        await Snapshot.WriteAsync(restored.Circuit, other);
        Assert.All((await ChunkChainsAsync(other)).Values, runs => Assert.True(runs.All(r => r.StartsWith("runs/", StringComparison.Ordinal))));

        var fromOther = Compile();
        await Snapshot.ReadAsync(fromOther.Circuit, other);
        Assert.Equal(Digest(twin), Digest(fromOther));
        Touch(fromOther, 2, [.. Enumerable.Range(0, Large), .. Enumerable.Range(100, Small)]);
        Touch(twin, 2, [.. Enumerable.Range(0, Large), .. Enumerable.Range(100, Small)]);
        Assert.Equal(Digest(twin), Digest(fromOther));
    }
}
