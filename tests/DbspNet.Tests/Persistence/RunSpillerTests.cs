// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Mid-batch spilling, S1a (docs/design-bounded-memory.md §5.9): once a freeze or merge leaves a run of
// at least the threshold without a file, the run is written to a scratch file and its blocks become
// file blocks the cache can evict. Required, on a join with a giant group (spanning blocks in memory):
//   1. every tick's output equals a twin that never spills, with nearly every body evicted;
//   2. spilled runs are file-backed; a checkpoint still writes its own copy and restores exactly;
//   3. a file is deleted once its run is gone, and nothing is left once the program is;
//   4. runs below the threshold are not spilled.
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Persistence;

[Collection(nameof(BlockedCheckpointTests))] // sets the process-wide cache budget and spiller
public class RunSpillerTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE a (k INT NOT NULL, s VARCHAR, x BIGINT)",
        "CREATE TABLE b (k INT NOT NULL, t VARCHAR)",
        "CREATE VIEW j AS SELECT a.k, a.s, a.x, b.t FROM a JOIN b ON a.k = b.k",
    ];

    private static readonly HashSet<string> Views = new(["j"], StringComparer.Ordinal);

    private static CompiledProgram Compile()
    {
        using var mode = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 64);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    // Key 0 is a giant group in the first ticks; later ticks insert and delete around it.
    private static void Tick(CompiledProgram p, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < (tick < 3 ? 400 : 40); i++)
        {
            var giant = tick < 3;
            p.Table("a").Insert(giant ? 0 : rng.Next(1, 12), (giant ? new string('g', 100) : "s") + tick + "-" + i, (long)rng.Next(100));
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 6; i++)
            {
                var giant = tick - 1 < 3;
                p.Table("a").Delete(giant ? 0 : old.Next(1, 12), (giant ? new string('g', 100) : "s") + (tick - 1) + "-" + i, (long)old.Next(100));
            }
        }

        p.Table("b").Insert(rng.Next(0, 12), "t" + rng.Next(3));
        if (tick == 0)
        {
            p.Table("b").Insert(0, "giant");
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("; ", p.Outputs["j"].CurrentView
            .Select(e => string.Join("|", Enumerable.Range(0, 4).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
            .Order(StringComparer.Ordinal));

    private static BlockedIndexedTrace Left(CompiledProgram p) =>
        p.Circuit.Operators.OfType<BlockedJoinOp>().Single().LeftTrace;

    private static int FilesIn(string dir) => Directory.GetFiles(dir, "*.run").Length;

    // Twin digests per tick, computed with nothing installed.
    private static List<string> TwinDigests(int ticks)
    {
        var twin = Compile();
        var digests = new List<string>();
        for (var tick = 0; tick < ticks; tick++)
        {
            Tick(twin, tick);
            digests.Add(Digest(twin));
        }

        return digests;
    }

    private static async Task WithSpillerAsync(long minBytes, Func<RunSpiller, string, Task> body)
    {
        using var layout = StateLayout.Use(true);
        var savedBudget = BlockCache.Budget;
        var savedSpiller = BlockAccess.Spiller;
        var dir = Path.Combine(Path.GetTempPath(), "dbspnet-runspiller-test-" + Guid.NewGuid().ToString("N"));
        var spiller = new RunSpiller(dir, minBytes);
        BlockCache.Budget = 1; // every admitted body is evicted at once
        BlockAccess.Spiller = spiller;
        try
        {
            await body(spiller, dir);
        }
        finally
        {
            BlockAccess.Spiller = savedSpiller;
            BlockCache.Budget = savedBudget;
            BlockCache.Clear();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task Spilled_runs_are_file_backed_and_change_no_output()
    {
        const int Ticks = 24;
        var expected = TwinDigests(Ticks);
        await WithSpillerAsync(0, async (spiller, dir) =>
        {
            var runs0 = Interlocked.Read(ref RunSpiller.Runs);
            var p = Compile();
            for (var tick = 0; tick < Ticks; tick++)
            {
                Tick(p, tick);
                Assert.Equal(expected[tick], Digest(p));

                // Every run the trace holds has a file, and its blocks are file blocks.
                Assert.All(Left(p).Runs, r =>
                {
                    Assert.NotNull(r.Persisted);
                    Assert.All(r.Blocks, b => Assert.IsType<LazyTraceBlock>(b));
                });
            }

            Assert.True(Interlocked.Read(ref RunSpiller.Runs) > runs0);

            // Files of runs merged away go once their runs are collected; the live ones stay.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            spiller.Sweep();
            Assert.Equal(spiller.Files, FilesIn(dir));
            Assert.True(spiller.Files >= Left(p).Runs.Count);

            // A checkpoint writes its own copy of every run; the restore matches and goes on matching.
            var fs = new InMemoryTableFileSystem();
            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.Equal(Digest(p), Digest(restored));
            Tick(p, Ticks);
            Tick(restored, Ticks);
            Assert.Equal(Digest(p), Digest(restored));

            // Once the programs are gone, so are their files.
            p = null!;
            restored = null!;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            spiller.Sweep();
            Assert.Equal(0, spiller.Files);
            Assert.Equal(0, FilesIn(dir));
        });
    }

    [Fact]
    public async Task Runs_below_the_threshold_stay_in_memory()
    {
        await WithSpillerAsync(1L << 40, (spiller, dir) =>
        {
            var p = Compile();
            for (var tick = 0; tick < 12; tick++)
            {
                Tick(p, tick);
            }

            Assert.All(Left(p).Runs, r => Assert.Null(r.Persisted));
            Assert.Equal(0, spiller.Files);
            Assert.Equal(0, FilesIn(dir));
            return Task.CompletedTask;
        });
    }
}
