// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Checkpoints of block-held join traces (docs/design-flat-trace-state.md §2.6–§2.7): written from
/// the blocks, named again rather than rewritten while a run is unchanged, restored lazily without
/// decoding, and readable across modes — a managed program restores a blocked checkpoint and the
/// other way round. Every restore continues exactly like an uninterrupted twin.
/// </summary>
[Collection(nameof(BlockedCheckpointTests))]
public class BlockedCheckpointTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE a (k INT NOT NULL, s VARCHAR, x BIGINT)",
        "CREATE TABLE b (k INT NOT NULL, t VARCHAR)",
        "CREATE VIEW j AS SELECT a.k, a.s, a.x, b.t FROM a JOIN b ON a.k = b.k",
    ];

    private static readonly HashSet<string> Views = new(["j"], StringComparer.Ordinal);

    public BlockedCheckpointTests() => RunCheckpoint.Verify = true;

    private static CompiledProgram Compile(bool blocked)
    {
        using var mode = TraceBlocksMode.Use(join: blocked, freezeEntries: 16, blockEntries: 8);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    // Key 0 is a giant group: 1,200 rows of 100-byte strings put more than a block's byte budget
    // under one hash, so once its runs merge it spans blocks in memory and is one block on disk.
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

    private static async Task<string[]> ChainOfAsync(InMemoryTableFileSystem fs, string traceFile)
    {
        await CheckpointCleanup.WaitAsync(fs);
        var current = Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));
        var index = Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(current + "/" + RunIndex.FileName));
        return index.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split('\t'))
            .Single(f => f[0].EndsWith(traceFile, StringComparison.Ordinal))[5..];
    }

    private static BlockedJoinOp Op(CompiledProgram p) => p.Circuit.Operators.OfType<BlockedJoinOp>().Single();

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Checkpoint_RestoresExactly_InEitherMode(bool writeBlocked, bool readBlocked)
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile(writeBlocked);
        for (var tick = 0; tick < 8; tick++)
        {
            Tick(p, tick);
        }

        if (writeBlocked)
        {
            // The giant group spans blocks in memory, so the write joined it back into one block.
            Op(p).LeftTrace.PrepareCheckpoint();
            Assert.Contains(Op(p).LeftTrace.Runs, r => Enumerable.Range(1, r.Blocks.Length - 1).Any(b => r.Blocks[b].FirstHash == r.Blocks[b - 1].LastHash));
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var restored = Compile(readBlocked);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(p), Digest(restored));

        if (readBlocked)
        {
            // Restored lazily: blocks are placeholders until probed.
            var left = Op(restored).LeftTrace;
            Assert.NotEmpty(left.Runs);
            Assert.All(left.Runs.SelectMany(r => r.Blocks), b => Assert.IsType<LazyTraceBlock>(b));
        }

        var twin = Compile(readBlocked);
        for (var tick = 0; tick < 8; tick++)
        {
            Tick(twin, tick);
        }

        for (var tick = 100; tick < 106; tick++)
        {
            Tick(twin, tick);
            Tick(restored, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }
    }

    [Fact]
    public async Task UnchangedRuns_AreNamedAgain_NotRewritten()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile(blocked: true);
        for (var tick = 0; tick < 8; tick++)
        {
            Tick(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var first = await ChainOfAsync(fs, "left.arrows");
        Assert.NotEmpty(first);
        Assert.All(Op(p).LeftTrace.Runs, r => Assert.NotNull(r.Persisted));

        // A tick too small to freeze: the next checkpoint freezes it into one new run (which may
        // merge with the newest) and names every run it did not replace again.
        p.Table("a").Insert(5, "late", 1L);
        p.Step();
        await Snapshot.WriteAsync(p.Circuit, fs);
        var second = await ChainOfAsync(fs, "left.arrows");
        var kept = first.Where(path => Op(p).LeftTrace.Runs.Any(r => r.Persisted?.Path == path)).ToHashSet();
        Assert.NotEmpty(kept);
        Assert.Subset(second.ToHashSet(), kept);
        Assert.Contains(second, path => !first.Contains(path));

        var restored = Compile(blocked: true);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(p), Digest(restored));
    }
}
