// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The window write-back budget (docs/design-bounded-memory.md §5.8): a window store that ends a step
// with the stores' estimated cache bytes over the budget drops its cache into its blocked record,
// and partitions touched later reload from there. Required, for LAG, RANK and framed SUM OVER, with
// small partitions (the record) and large ones (chunks): outputs every tick equal an unbounded twin's,
// and the budget really dropped the caches (the unbounded twin kept them).
using System.Reflection;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

public class WindowWriteBackBudgetTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c VARCHAR)",
        "CREATE VIEW lg AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS p FROM t",
        "CREATE VIEW rk AS SELECT a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW ws AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t",
    ];

    private static readonly HashSet<string> Views = new(["lg", "rk", "ws"], StringComparer.Ordinal);

    private static CompiledProgram Compile(long budget)
    {
        using var blocks = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 4, window: true);
        using var chunks = TraceBlocksMode.UseChunks(promoteRows: 8, chunkRows: 4);
        using var writeBack = SpillSettings.UseWindowWriteBack(budget);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static List<OrderedPartitionStore> Stores(CompiledProgram p) =>
        [.. p.Circuit.Operators
            .Select(o => o.GetType().GetField("_store", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(o) as OrderedPartitionStore)
            .OfType<OrderedPartitionStore>()];

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    // One tick of changes: tick 0 loads 20 large partitions (40 rows) and 200 small ones (2 rows);
    // later ticks insert into and delete from a spread of both, so cached, dropped and never-cached
    // partitions all get touched.
    private static void Tick(CompiledProgram p, int tick)
    {
        var t = p.Table("t");
        if (tick == 0)
        {
            for (var a = 0; a < 20; a++)
            {
                for (var b = 0; b < 40; b++)
                {
                    t.Insert(a, (a * 11 + b * 7) % 60, "v" + b);
                }
            }

            for (var a = 100; a < 300; a++)
            {
                t.Insert(a, a % 13, null);
                t.Insert(a, (a % 17) + 20, "s" + a);
            }
        }
        else
        {
            for (var a = tick % 3; a < 300; a += 7)
            {
                if (a is >= 20 and < 100)
                {
                    continue;
                }

                t.Insert(a, (tick * 5 + a) % 60, "n" + tick);
                if (a < 20)
                {
                    t.Delete(a, (a * 11 + tick * 7) % 60, "v" + tick);
                }
                else if (tick % 2 == 0)
                {
                    t.Delete(a, a % 13, null);
                    t.Insert(a, a % 13, null);
                }
            }
        }

        p.Step();
    }

    [Fact]
    public void A_zero_budget_drops_every_cache_and_changes_no_output()
    {
        var bounded = Compile(0);
        var unbounded = Compile(long.MaxValue);
        Assert.NotEmpty(Stores(bounded));
        var drops0 = Interlocked.Read(ref OrderedPartitionStore.WriteBackDrops);
        var unboundedKept = false;
        for (var tick = 0; tick < 12; tick++)
        {
            Tick(bounded, tick);
            Tick(unbounded, tick);
            Assert.Equal(Digest(unbounded), Digest(bounded));

            // Every bounded store ended its step with nothing cached.
            Assert.All(Stores(bounded), s => Assert.Equal(0, s.SmallManaged.Partitions));
            unboundedKept |= Stores(unbounded).Any(s => s.SmallManaged.Partitions > 0);
        }

        // An idle tick (no input) after the drops, then more changes.
        bounded.Step();
        unbounded.Step();
        Tick(bounded, 12);
        Tick(unbounded, 12);
        Assert.Equal(Digest(unbounded), Digest(bounded));

        Assert.True(unboundedKept, "the unbounded twin should keep cached partitions between ticks");
        Assert.True(Interlocked.Read(ref OrderedPartitionStore.WriteBackDrops) > drops0);
    }

    [Fact]
    public void A_dropped_cache_releases_a_huge_ticks_capacity()
    {
        // §5.30: Dictionary.Clear keeps its arrays, so a cache that once held a huge tick's partitions
        // kept that capacity, empty, after every drop (~4 GB at extended TPC-DS tick 17). After a drop
        // past the budget, neither the cache nor its queue may keep more than a small capacity, and the
        // outputs still match an unbounded twin's.
        const int Partitions = 20_000;
        var bounded = Compile(0);
        var unbounded = Compile(long.MaxValue);
        foreach (var p in new[] { bounded, unbounded })
        {
            var t = p.Table("t");
            for (var a = 0; a < Partitions; a++)
            {
                t.Insert(1000 + a, a % 13, "h" + a);
            }

            p.Step();
        }

        Assert.Equal(Digest(unbounded), Digest(bounded));
        Assert.All(Stores(bounded), s =>
        {
            Assert.Equal(0, s.SmallManaged.Partitions);
            Assert.True(s.CacheCapacity.Small <= OrderedPartitionStore.KeepCapacity, $"cache capacity {s.CacheCapacity.Small}");
            Assert.True(s.CacheCapacity.Pending <= OrderedPartitionStore.KeepCapacity, $"queue capacity {s.CacheCapacity.Pending}");
        });

        // The twin kept its cache, so the tick really was huge.
        Assert.Contains(Stores(unbounded), s => s.CacheCapacity.Small >= Partitions);

        for (var tick = 1; tick < 6; tick++)
        {
            Tick(bounded, tick);
            Tick(unbounded, tick);
            Assert.Equal(Digest(unbounded), Digest(bounded));
        }
    }

    [Fact]
    public void A_budget_above_the_caches_keeps_them()
    {
        using var pressure = MemoryPressure.Use(PressureLevel.Low); // asserts the Low thresholds (§5.28)
        var p = Compile(1L << 40);
        for (var tick = 0; tick < 4; tick++)
        {
            Tick(p, tick);
        }

        Assert.Contains(Stores(p), s => s.SmallManaged.Partitions > 0 && s.CacheBytes > 0);
    }

    [Fact]
    public void The_setting_parses_mib_and_off()
    {
        Assert.Equal(1024L << 20, SpillSettings.ParseMib(null, 1024));
        Assert.Equal(1024L << 20, SpillSettings.ParseMib("junk", 1024));
        Assert.Equal(0, SpillSettings.ParseMib("0", 1024));
        Assert.Equal(64L << 20, SpillSettings.ParseMib(" 64 ", 1024));
        Assert.Equal(long.MaxValue, SpillSettings.ParseMib("off", 1024));
        Assert.Equal(long.MaxValue, SpillSettings.ParseMib("NONE", 1024));
    }
}
