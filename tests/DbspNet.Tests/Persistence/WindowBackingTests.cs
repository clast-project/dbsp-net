// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Reflection;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Window state under an evicting block cache (docs/design-bounded-memory.md §3.7): each checkpoint
/// also writes the small partitions' record as backing runs, and the large partitions' new chunks as a backing block file,, which the cache can drop and read
/// back. The program stays exactly its uncheckpointed twin, and the checkpoint (whose restore
/// ignores the backing file) restores exactly.
/// </summary>
[Collection(nameof(BlockedCheckpointTests))]
public class WindowBackingTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (g INT NOT NULL, ts INT NOT NULL, v BIGINT NOT NULL, s VARCHAR)",
        "CREATE VIEW agg AS SELECT g, ts, v, SUM(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) AS w FROM t",
        "CREATE VIEW lag AS SELECT g, ts, LAG(v, 1) OVER (PARTITION BY g ORDER BY ts) AS p FROM t",
        "CREATE VIEW rnk AS SELECT g, ts, v, RANK() OVER (PARTITION BY g ORDER BY v DESC) AS r FROM t",
    ];

    private static readonly HashSet<string> Views = new(["agg", "lag", "rnk"], StringComparer.Ordinal);

    private static CompiledProgram Compile()
    {
        using var mode = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 8, window: true);
        using var chunks = TraceBlocksMode.UseChunks(promoteRows: 8, chunkRows: 4);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    // Partition 0 grows large (ordered chunks); 1..30 stay small (the record).
    private static void Tick(CompiledProgram p, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < 25; i++)
        {
            var g = i % 5 == 0 ? 0 : rng.Next(1, 31);
            p.Table("t").Insert(g, (tick * 100) + i, (long)rng.Next(50), "s" + rng.Next(4));
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 4; i++)
            {
                var g = i % 5 == 0 ? 0 : old.Next(1, 31);
                p.Table("t").Delete(g, ((tick - 1) * 100) + i, (long)old.Next(50), "s" + old.Next(4));
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) => string.Join(" || ", Views.Order(StringComparer.Ordinal).Select(v =>
        string.Join("; ", p.Outputs[v].CurrentView.Select(e => e.Key + "#" + e.Value.Value).Order(StringComparer.Ordinal))));

    private static IEnumerable<OrderedPartitionStore> Stores(CompiledProgram p) =>
        p.Circuit.Operators
            .Select(op => op.GetType().GetField("_store", BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(op))
            .OfType<OrderedPartitionStore>();

    [Theory]
    [InlineData(1L)]
    [InlineData(4096L)]
    public async Task Record_IsBackedByRunFiles_AndStaysExact(long budget)
    {
        using var layout = StateLayout.Use(true);
        var (saved, savedAlign) = (BlockCache.Budget, BlockAccess.AlignSpans);
        BlockCache.Budget = budget;
        BlockAccess.AlignSpans = true;
        try
        {
            var fs = new InMemoryTableFileSystem();
            var p = Compile();
            var twin = Compile();
            Assert.Equal(3, Stores(p).Count());
            var evictions = BlockCache.Evictions;
            for (var tick = 0; tick < 24; tick++)
            {
                Tick(p, tick);
                Tick(twin, tick);
                if (tick % 3 == 2)
                {
                    await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
                    await CheckpointCleanup.WaitAsync(fs); // retired snapshots swept: pinned files must survive
                    foreach (var store in Stores(p))
                    {
                        var record = store.ColdRecord!;
                        Assert.NotEmpty(record.Runs);
                        Assert.All(record.Runs.SelectMany(r => r.Blocks), b => Assert.IsType<LazyTraceBlock>(b));
                        Assert.All(store.ChunkBlocks(), b => Assert.IsType<LazyTraceBlock>(b));
                    }
                }

                Assert.Equal(Digest(twin), Digest(p));
            }

            Assert.True(BlockCache.Evictions > evictions, "the budget should have evicted record blocks");
            Assert.Contains(Stores(p), s => s.LargePartitions > 0);

            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.Equal(Digest(p), Digest(restored));
            for (var tick = 100; tick < 104; tick++)
            {
                Tick(p, tick);
                Tick(restored, tick);
                Assert.Equal(Digest(p), Digest(restored));
            }
        }
        finally
        {
            BlockCache.Budget = saved;
            BlockAccess.AlignSpans = savedAlign;
            BlockCache.Clear();
        }
    }
}
