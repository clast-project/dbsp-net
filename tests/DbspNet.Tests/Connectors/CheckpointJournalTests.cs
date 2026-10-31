// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The A2 checkpoint policy (docs/design-durable-state.md §1c): every batch commits a tick-journal
// entry, a snapshot is taken only when the policy says so, and a restart restores the last
// snapshot (or the empty circuit) and replays the journal after it — landing on exactly the state
// and tick of an uninterrupted run.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DbspNet.Connectors.Abstractions;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

public class CheckpointJournalTests
{
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, c INT NOT NULL)",
        "CREATE VIEW totals AS SELECT a, SUM(b) AS s, COUNT(*) AS n FROM t GROUP BY a",
        "CREATE VIEW joined AS SELECT t.a, t.b, u.c FROM t JOIN u ON t.a = u.a",
    ];

    private static readonly HashSet<string> OutputViews = new(["totals", "joined"], StringComparer.Ordinal);

    private static SqlSchema Schema(string second) =>
        new([new SchemaColumn("a", new SqlIntegerType(false)), new SchemaColumn(second, new SqlIntegerType(false))]);

    private static List<(object?[] Row, long Weight)> V(params (int A, int X, long W)[] rows) =>
        rows.Select(r => (new object?[] { r.A, r.X }, r.W)).ToList();

    // What each batch makes visible: (new t versions, new u versions). Batch 5 is idle.
    private static readonly (IReadOnlyList<(object?[] Row, long Weight)>[] T, IReadOnlyList<(object?[] Row, long Weight)>[] U)[] Schedule =
    [
        ([V((1, 10, 1), (2, 5, 1)), V((1, 20, 1))], [V((1, 100, 1))]),
        ([V((3, 7, 1))], []),
        ([V((2, 5, -1), (1, 1, 1))], [V((2, 200, 1)), V((3, 300, 1))]),
        ([V((3, 9, 1))], []),
        ([], []),
    ];

    private sealed class Feed
    {
        public List<IReadOnlyList<(object?[] Row, long Weight)>> T { get; } = [];

        public List<IReadOnlyList<(object?[] Row, long Weight)>> U { get; } = [];

        public void Reveal(int batch)
        {
            T.AddRange(Schedule[batch].T);
            U.AddRange(Schedule[batch].U);
        }

        public static Feed Through(int batches)
        {
            var f = new Feed();
            for (var i = 0; i < batches; i++)
            {
                f.Reveal(i);
            }

            return f;
        }

        public List<IInputConnector> Connectors() =>
            [new FakeInputConnector("t", Schema("b"), T), new FakeInputConnector("u", Schema("c"), U)];
    }

    private static List<IOutputConnector> Outputs() =>
        OutputViews.Select(v => (IOutputConnector)new FakeOutputConnector(v, OutputMode.Truncate)).ToList();

    private static CompiledProgram Compile() => SqlProgram.Compile(ProgramSql, OutputViews, ArrowSqlSnapshotCodecs.Instance);

    private static ParallelCompiledProgram CompileParallel(int workers)
    {
        Assert.True(SqlProgram.TryCompileParallel(ProgramSql, OutputViews, workers, out var p, ArrowSqlSnapshotCodecs.Instance));
        return p!;
    }

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> view)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in view)
        {
            var key = string.Join("|", Enumerable.Range(0, kv.Key.Count).Select(i => kv.Key[i]?.ToString() ?? "<null>"));
            var w = map.GetValueOrDefault(key) + kv.Value.Value;
            if (w == 0)
            {
                map.Remove(key);
            }
            else
            {
                map[key] = w;
            }
        }

        return map;
    }

    private static Dictionary<string, Dictionary<string, long>> Views(CompiledProgram p) =>
        OutputViews.ToDictionary(v => v, v => Materialize(p.Outputs[v].CurrentView), StringComparer.Ordinal);

    private static Dictionary<string, Dictionary<string, long>> Views(ParallelCompiledProgram p) =>
        OutputViews.ToDictionary(v => v, v => Materialize(p.Outputs[v].CurrentView), StringComparer.Ordinal);

    private static void AssertSameViews(Dictionary<string, Dictionary<string, long>> expected, Dictionary<string, Dictionary<string, long>> actual)
    {
        foreach (var v in OutputViews)
        {
            Assert.Equal(expected[v], actual[v]);
        }
    }

    /// <summary>An uninterrupted run over the first <paramref name="batches"/> batches, no store.</summary>
    private static async Task<(Dictionary<string, Dictionary<string, long>> Views, long Tick)> Reference(int batches)
    {
        var feed = new Feed();
        var p = Compile();
        var r = await ProgramRunner.CreateAsync(p, feed.Connectors(), Outputs());
        for (var i = 0; i < batches; i++)
        {
            feed.Reveal(i);
            await r.RunBatchAsync();
        }

        return (Views(p), p.Circuit.TickCount);
    }

    private static async Task<long> JournalFileCount(InMemoryTableFileSystem fs)
    {
        long n = 0;
        await foreach (var f in fs.ListAsync("journal/"))
        {
            n++;
        }

        return n;
    }

    [Fact]
    public async Task JournalOnly_WritesNoSnapshot_AndRecoversByReplayingFromTickZero()
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = new CheckpointPolicy { Journal = true };
        var feed = new Feed();
        var p1 = Compile();
        var r1 = await ProgramRunner.CreateAsync(p1, feed.Connectors(), Outputs(), store, policy);
        for (var i = 0; i < Schedule.Length; i++)
        {
            feed.Reveal(i);
            await r1.RunBatchAsync();
        }

        Assert.False(await Snapshot.PointerExistsAsync(fs));
        Assert.Equal(4, await JournalFileCount(fs)); // one per batch that ticked; batch 5 was idle

        var (expected, tick) = await Reference(Schedule.Length);
        Assert.Equal(tick, p1.Circuit.TickCount);

        var p2 = Compile();
        var r2 = await ProgramRunner.CreateAsync(p2, Feed.Through(Schedule.Length).Connectors(), Outputs(), store, policy);
        Assert.Equal(tick, await r2.RestoreAsync());
        AssertSameViews(expected, Views(p2));

        // The restored runner continues from the journal's last cursors: nothing re-read.
        Assert.Equal(0, await r2.RunBatchAsync());
    }

    [Fact]
    public async Task SnapshotEveryTwoBatches_TruncatesTheJournal_AndRecoversFromSnapshotPlusJournal()
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = new CheckpointPolicy { Journal = true, SnapshotEveryBatches = 2 };
        var feed = new Feed();
        var p1 = Compile();
        var r1 = await ProgramRunner.CreateAsync(p1, feed.Connectors(), Outputs(), store, policy);
        for (var i = 0; i < 2; i++)
        {
            feed.Reveal(i);
            await r1.RunBatchAsync();
        }

        Assert.True(await Snapshot.PointerExistsAsync(fs));
        Assert.Equal(0, await JournalFileCount(fs)); // the snapshot after batch 2 covers both

        feed.Reveal(2);
        await r1.RunBatchAsync();
        Assert.Equal(1, await JournalFileCount(fs)); // batch 3 is journaled, not yet snapshotted

        var (expected, tick) = await Reference(3);
        var p2 = Compile();
        var r2 = await ProgramRunner.CreateAsync(p2, Feed.Through(3).Connectors(), Outputs(), store, policy);
        Assert.Equal(tick, await r2.RestoreAsync());
        AssertSameViews(expected, Views(p2));

        // ... and carries on as if never interrupted.
        var feed2 = Feed.Through(4);
        var p3 = Compile();
        var r3 = await ProgramRunner.CreateAsync(p3, feed2.Connectors(), Outputs(), store, policy);
        await r3.RestoreAsync();
        await r3.RunBatchAsync();
        var (expected4, tick4) = await Reference(4);
        Assert.Equal(tick4, p3.Circuit.TickCount);
        AssertSameViews(expected4, Views(p3));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ParallelProgram_RecoversFromSnapshotPlusJournal(int workers)
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = new CheckpointPolicy { Journal = true, SnapshotEveryBatches = 2 };
        var feed = new Feed();
        using (var p1 = CompileParallel(workers))
        {
            var r1 = await ProgramRunner.CreateAsync(p1, feed.Connectors(), Outputs(), store, policy);
            for (var i = 0; i < 3; i++)
            {
                feed.Reveal(i);
                await r1.RunBatchAsync();
            }
        }

        // A snapshot after batch 2, and batch 3 in the journal only.
        Assert.True(await Snapshot.PointerExistsAsync(fs));
        Assert.Equal(1, await JournalFileCount(fs));

        var (expected, tick) = await Reference(3);
        using var p2 = CompileParallel(workers);
        var r2 = await ProgramRunner.CreateAsync(p2, Feed.Through(3).Connectors(), Outputs(), store, policy);
        Assert.Equal(tick, await r2.RestoreAsync());
        AssertSameViews(expected, Views(p2));
    }

    [Fact]
    public async Task ParallelProgram_JournalOnly_RecoversFromTickZero()
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = new CheckpointPolicy { Journal = true };
        var feed = new Feed();
        using (var p1 = CompileParallel(2))
        {
            var r1 = await ProgramRunner.CreateAsync(p1, feed.Connectors(), Outputs(), store, policy);
            for (var i = 0; i < 4; i++)
            {
                feed.Reveal(i);
                await r1.RunBatchAsync();
            }
        }

        var (expected, tick) = await Reference(4);
        using var p2 = CompileParallel(2);
        var r2 = await ProgramRunner.CreateAsync(p2, Feed.Through(4).Connectors(), Outputs(), store, policy);
        Assert.Equal(tick, await r2.RestoreAsync());
        AssertSameViews(expected, Views(p2));
    }

    [Fact]
    public async Task Replay_FailsLoudly_WhenTheSourceNoLongerHasAJournaledVersion()
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = new CheckpointPolicy { Journal = true };
        var feed = new Feed();
        var r1 = await ProgramRunner.CreateAsync(Compile(), feed.Connectors(), Outputs(), store, policy);
        for (var i = 0; i < 3; i++)
        {
            feed.Reveal(i);
            await r1.RunBatchAsync();
        }

        // The restarted engine's sources have lost the last batch's versions.
        var r2 = await ProgramRunner.CreateAsync(Compile(), Feed.Through(2).Connectors(), Outputs(), store, policy);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () => await r2.RestoreAsync());
        Assert.Contains("journal records tick", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Restore_IgnoresAJournalFileTheSnapshotAlreadyCovers()
    {
        // A crash after the snapshot's pointer moved but before the journal was truncated leaves
        // files at or below the snapshot tick behind; they must not be replayed.
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = new CheckpointPolicy { Journal = true, SnapshotEveryBatches = 2 };
        var feed = new Feed();
        var p1 = Compile();
        var r1 = await ProgramRunner.CreateAsync(p1, feed.Connectors(), Outputs(), store, policy);
        for (var i = 0; i < 2; i++)
        {
            feed.Reveal(i);
            await r1.RunBatchAsync();
        }

        await store.Journal.AppendAsync([new JournalTick(1, "t", "0"), new JournalTick(2, "t", "1")]);

        var (expected, tick) = await Reference(2);
        var p2 = Compile();
        var r2 = await ProgramRunner.CreateAsync(p2, Feed.Through(2).Connectors(), Outputs(), store, policy);
        Assert.Equal(tick, await r2.RestoreAsync());
        AssertSameViews(expected, Views(p2));
    }

    [Fact]
    public async Task EveryBatchPolicy_StillReplaysAJournalLeftByAnEarlierRun()
    {
        // Switching back to snapshot-every-batch between runs must not drop journaled ticks.
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var feed = new Feed();
        var r1 = await ProgramRunner.CreateAsync(Compile(), feed.Connectors(), Outputs(), store, new CheckpointPolicy { Journal = true });
        for (var i = 0; i < 3; i++)
        {
            feed.Reveal(i);
            await r1.RunBatchAsync();
        }

        var (expected, tick) = await Reference(3);
        var p2 = Compile();
        var r2 = await ProgramRunner.CreateAsync(p2, Feed.Through(3).Connectors(), Outputs(), store, CheckpointPolicy.EveryBatch);
        Assert.Equal(tick, await r2.RestoreAsync());
        AssertSameViews(expected, Views(p2));
    }

    [Fact]
    public async Task TickJournal_RejectsAGap_AndANonContiguousAppend()
    {
        var journal = new TickJournal(new InMemoryTableFileSystem());
        await journal.AppendAsync([new JournalTick(1, "t", "0"), new JournalTick(2, "t", "1")]);
        await journal.AppendAsync([new JournalTick(4, "t", "3")]);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await journal.ReadAfterAsync(0));
        Assert.Single(await journal.ReadAfterAsync(3));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await journal.AppendAsync([new JournalTick(5, "t", "4"), new JournalTick(7, "t", "6")]));
    }

    [Fact]
    public async Task TickJournal_TruncateDropsOnlyFilesWhollyCovered()
    {
        var fs = new InMemoryTableFileSystem();
        var journal = new TickJournal(fs);
        await journal.AppendAsync([new JournalTick(1, "t", "0"), new JournalTick(2, "u", "0")]);
        await journal.AppendAsync([new JournalTick(3, "t", "1"), new JournalTick(4, "t", "2")]);
        await journal.TruncateThroughAsync(3);
        Assert.Equal(1, await JournalFileCount(fs));
        Assert.Equal([4L], (await journal.ReadAfterAsync(3)).Select(t => t.Tick));
    }

    [Theory]
    [InlineData(null, true, 60.0)]
    [InlineData("", true, 60.0)]
    [InlineData("30", true, 30.0)]
    [InlineData("0.25", true, 1.0)]      // clamped up, like Feldera
    [InlineData("100000", true, 3600.0)] // clamped down
    public void Parse_GivesAJournaledPolicyWithAClampedInterval(string? setting, bool journal, double seconds)
    {
        var p = CheckpointPolicy.Parse(setting);
        Assert.Equal(journal, p.Journal);
        Assert.Equal(TimeSpan.FromSeconds(seconds), p.SnapshotInterval);
    }

    [Fact]
    public void Parse_BatchAndNone_AndRejectsGarbage()
    {
        Assert.Same(CheckpointPolicy.EveryBatch, CheckpointPolicy.Parse("batch"));
        var none = CheckpointPolicy.Parse("none");
        Assert.True(none.Journal);
        Assert.Null(none.SnapshotInterval);
        Assert.Throws<ArgumentException>(() => CheckpointPolicy.Parse("soon"));
        Assert.Throws<ArgumentException>(() => CheckpointPolicy.Parse("-5"));
    }
}
