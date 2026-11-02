// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Lazy restore of partitioned operators (docs/design-durable-state.md §1f, increment 5). Their rows
// are saved in partition-key-hash order; a restore hands them back cold, and each partition is read
// (and its window rebuilt) the first time a delta touches it. Required, for LAG, RANK and a framed
// SUM OVER (a ROW_NUMBER TOP-K view rides along: its narrowing operator restores eagerly):
//
//   1. restored operators stay lazy — only touched partitions are read — and every output matches
//      an uninterrupted twin, through ticks, a delta checkpoint and a second restore;
//   2. a checkpoint that must write the whole state while partitions are still cold (here: into a
//      different store, which no chain can extend) writes every row, not just the touched ones.
using System.Reflection;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class LazyPartitionTests
{
    private const int Partitions = 400;

    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE VIEW lg AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS p FROM t",
        "CREATE VIEW rk AS SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW top AS SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t) x WHERE rn <= 2",
        "CREATE VIEW ws AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t",
        // Window shapes a lazily faulted partition rebuilds only where a step needs it: running
        // (UNBOUNDED PRECEDING) and bounded, ascending and descending.
        "CREATE VIEW wr AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t",
        "CREATE VIEW wm AS SELECT a, b, MIN(b) OVER (PARTITION BY a ORDER BY b DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS m FROM t",
        "CREATE VIEW wd AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b DESC RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS s FROM t",
        "CREATE VIEW wa AS SELECT a, b, AVG(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 20 PRECEDING AND CURRENT ROW) AS v FROM t",
    ];

    private static readonly HashSet<string> Views = new(["lg", "rk", "top", "ws", "wr", "wm", "wd", "wa"], StringComparer.Ordinal);

    // blocks: the window aggregate's input held as blocks (docs/design-flat-trace-state.md §3.13),
    // tiny so the state is in runs; a twin is always managed, so outputs also compare the two.
    private static CompiledProgram Compile(bool blocks = false)
    {
        using var mode = blocks ? DbspNet.Core.Operators.Stateful.Blocks.TraceBlocksMode.Use(join: true, freezeEntries: 4, blockEntries: 2, window: true) : null;
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Load(CompiledProgram p)
    {
        for (var a = 0; a < Partitions; a++)
        {
            for (var b = 0; b < 6; b++)
            {
                p.Table("t").Insert(a, (a * 7 + b * 3) % 40);
            }
        }

        p.Step();
    }

    private static void Touch(CompiledProgram p, int tick, int from, int to)
    {
        for (var a = from; a < to; a++)
        {
            p.Table("t").Insert(a, (tick * 13 + a) % 50);
            if (tick % 2 == 0)
            {
                p.Table("t").Delete(a, (a * 7) % 40);
            }
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.Order(StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ",
            p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .Order(StringComparer.Ordinal))));

    // Every partitioned operator with a lazy path, and its cold partitions (null when everything is
    // in memory). The TOP-K variants that narrow or compact rows (PartitionedTopKNarrowOp, the
    // ROW_NUMBER view here; PartitionedTopKCompactOp) restore eagerly — not yet covered.
    private static List<(string Op, object? Lazy)> LazyStates(CompiledProgram p) =>
        p.Circuit.Operators
            .Select(o => (Op: o, Field: o.GetType().GetField("_lazy", BindingFlags.NonPublic | BindingFlags.Instance)))
            .Where(x => x.Field is not null)
            .Select(x => (x.Op.GetType().Name, x.Field!.GetValue(x.Op)))
            .ToList();

    private static int Resolved(object lazy) => (int)lazy.GetType().GetProperty("Resolved")!.GetValue(lazy)!;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RestoredPartitionedOperators_StayLazy_ReadOnlyTouchedPartitions_AndMatchTheirTwin(bool blocks)
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile(blocks);
        var twin = Compile();
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = Compile(blocks);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var states = LazyStates(restored);
        Assert.Equal(
            ["PartitionedOffsetOp`3", "PartitionedRankOp`3", "PartitionedWindowAggregateOp`4"],
            states.Select(s => s.Op).Distinct().Order(StringComparer.Ordinal));
        Assert.All(states, s => Assert.True(s.Lazy is not null, $"{s.Op} restored eagerly"));
        Assert.Equal(Digest(twin), Digest(restored));

        for (var tick = 1; tick <= 4; tick++)
        {
            Touch(restored, tick, 10 * tick, 10 * tick + 5);
            Touch(twin, tick, 10 * tick, 10 * tick + 5);
            Assert.Equal(Digest(twin), Digest(restored));
        }

        Assert.All(LazyStates(restored), s =>
        {
            Assert.True(s.Lazy is not null, $"{s.Op} was promoted by small ticks");
            Assert.InRange(Resolved(s.Lazy!), 1, 20);
        });

        // A delta checkpoint while partitions are cold, a second restore, then every partition.
        await Snapshot.WriteAsync(restored.Circuit, fs);
        var again = Compile(blocks);
        await Snapshot.ReadAsync(again.Circuit, fs);
        Touch(again, 5, 0, Partitions);
        Touch(twin, 5, 0, Partitions);
        Assert.Equal(Digest(twin), Digest(again));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWholeStateWrite_WhilePartitionsAreCold_WritesEveryRow(bool blocks)
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile(blocks);
        var twin = Compile();
        Load(producer);
        Load(twin);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var restored = Compile(blocks);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Touch(restored, 1, 0, 3);
        Touch(twin, 1, 0, 3);

        // Another store: no chain extends into it, so every operator writes its whole state.
        var other = new InMemoryTableFileSystem();
        await Snapshot.WriteAsync(restored.Circuit, other);
        var fromOther = Compile(blocks);
        await Snapshot.ReadAsync(fromOther.Circuit, other);
        Touch(fromOther, 2, 0, Partitions);
        Touch(twin, 2, 0, Partitions);
        Assert.Equal(Digest(twin), Digest(fromOther));
    }

    [Fact]
    public async Task GcAfterALazyRestore_CollectsColdPartitionsToo()
    {
        // LATENESS GC drops window-aggregate rows below the frontier in every partition — including
        // partitions still cold after a lazy restore. Skipping those is invisible in the outputs
        // (expired rows are never touched again) but leaves them in the state for good: so compare
        // the row count a whole-state write records against a never-restored twin's.
        using var layout = StateLayout.Use(true);
        string[] ddl = ["CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)"];
        const string Query = "SELECT ts, x, SUM(x) OVER (PARTITION BY x ORDER BY ts RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM a";
        static DbspNet.Sql.Compiler.CompiledQuery CompileQuery(string[] ddl, string query)
        {
            var resolver = new DbspNet.Sql.Plan.Resolver(new DbspNet.Sql.Plan.Catalog());
            foreach (var statement in ddl)
            {
                resolver.Resolve(DbspNet.Sql.Parser.Parser.ParseStatement(statement));
            }

            return PlanToCircuit.Compile(
                ((DbspNet.Sql.Plan.SelectPlan)resolver.Resolve(DbspNet.Sql.Parser.Parser.ParseStatement(query))).Query,
                ArrowSqlSnapshotCodecs.Instance);
        }

        static void Step(DbspNet.Sql.Compiler.CompiledQuery q, long t)
        {
            q.Table("a").Insert(t, (int)t); // a partition per row: restored partitions are never touched again
            q.Step();
        }

        static long Retained(DbspNet.Sql.Compiler.CompiledQuery q) =>
            (long)q.Circuit.Operators.Single(o => o.GetType().Name.StartsWith("PartitionedWindowAggregateOp", StringComparison.Ordinal))
                .GetType().GetProperty("RetainedRows")!.GetValue(q.Circuit.Operators.Single(o => o.GetType().Name.StartsWith("PartitionedWindowAggregateOp", StringComparison.Ordinal)))!;

        var fs = new InMemoryTableFileSystem();
        var producer = CompileQuery(ddl, Query);
        var twin = CompileQuery(ddl, Query);
        for (var t = 0L; t < 200; t++)
        {
            Step(producer, t);
            Step(twin, t);
        }

        await Snapshot.WriteAsync(producer.Circuit, fs);
        var restored = CompileQuery(ddl, Query);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        var windowOp = restored.Circuit.Operators.Single(o => o.GetType().Name.StartsWith("PartitionedWindowAggregateOp", StringComparison.Ordinal));
        var lazyField = windowOp.GetType().GetField("_lazy", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Assert.True(lazyField.GetValue(windowOp) is not null, "the window aggregate was not restored lazily");
        for (var t = 200L; t < 260; t++)
        {
            Step(restored, t); // the frontier moves past every row restored cold
            Step(twin, t);
        }

        var other = new InMemoryTableFileSystem();
        await Snapshot.WriteAsync(restored.Circuit, other); // a whole-state write
        await CheckpointCleanup.WaitAsync(other);
        long recorded = -1;
        await foreach (var f in other.ListAsync("snap-"))
        {
            if (f.Path.EndsWith("/" + RunIndex.FileName, StringComparison.Ordinal))
            {
                foreach (var line in System.Text.Encoding.UTF8.GetString(await other.ReadAllBytesAsync(f.Path)).Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    var parts = line.Split('\t');
                    if (parts[4] == "p")
                    {
                        recorded = long.Parse(parts[3], System.Globalization.CultureInfo.InvariantCulture);
                    }
                }
            }
        }

        Assert.True(Retained(twin) == recorded, $"twin retains {Retained(twin)} rows, the whole-state write recorded {recorded} (restored retains {Retained(restored)}; producer had {Retained(producer)})");
    }

    [Fact]
    public async Task AReshard_KeepsPartitionOrder_SoTheRestoreAfterItStaysLazy()
    {
        // Increment 7: the re-shard rewrites each partitioned operator's file in that operator's
        // own run order (ISnapshotOrder), so restoring at the new worker count reads partitions
        // on demand instead of all of them.
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var twin = Compile();
        Load(twin);
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 3, out var producer, ArrowSqlSnapshotCodecs.Instance));
        using (producer)
        {
            for (var a = 0; a < Partitions; a++)
            {
                for (var b = 0; b < 6; b++)
                {
                    producer!.Table("t").Insert(a, (a * 7 + b * 3) % 40);
                }
            }

            producer!.Step();
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 2, out var restored, ArrowSqlSnapshotCodecs.Instance));
        using (restored)
        {
            Assert.Equal(3, await ParallelReshard.RewriteAsync(fs, restored!));
            await ParallelSnapshot.ReadAsync(restored!.Circuit, fs);
            var lazy = restored.Circuit.Replicas
                .SelectMany(r => r.Operators)
                .Select(o => o.GetType().GetField("_lazy", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(o))
                .Where(l => l is not null)
                .ToList();
            Assert.True(lazy.Count >= 3 * 2, $"expected the partitioned operators on both workers lazy, found {lazy.Count}");

            foreach (var tick in new[] { 1, 2 })
            {
                for (var a = 10 * tick; a < (10 * tick) + 5; a++)
                {
                    restored.Table("t").Insert(a, (tick * 13 + a) % 50); // as Touch does
                    if (tick % 2 == 0)
                    {
                        restored.Table("t").Delete(a, (a * 7) % 40);
                    }
                }

                restored.Step();
                Touch(twin, tick, 10 * tick, (10 * tick) + 5);
            }

            foreach (var v in Views)
            {
                Assert.Equal(
                    string.Join("; ", twin.Outputs[v].CurrentView.Select(e => e.Key.ToString() + "#" + e.Value.Value).Order(StringComparer.Ordinal)),
                    string.Join("; ", restored.Outputs[v].CurrentView.Select(e => e.Key.ToString() + "#" + e.Value.Value).Order(StringComparer.Ordinal)));
            }
        }
    }
}
