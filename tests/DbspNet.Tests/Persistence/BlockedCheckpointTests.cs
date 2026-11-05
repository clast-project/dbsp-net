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

    private static CompiledProgram Compile(bool blocked, int blockEntries = 8)
    {
        using var mode = TraceBlocksMode.Use(join: blocked, freezeEntries: 16, blockEntries: blockEntries);
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

    // Giant row i of an early tick t (key 0), as Tick wrote it: its x is the i-th draw of Random(t).
    private static object[] GiantRow(int t, int i)
    {
        var rng = new Random(t);
        long x = 0;
        for (var j = 0; j <= i; j++)
        {
            x = rng.Next(100);
        }

        return [0, new string('g', 100) + t + "-" + i, x];
    }

    // docs/design-bounded-memory.md §5.18: a giant group past the span cap is written as several file
    // blocks that share its hash. It restores exactly into the blocked and the managed (lazy cold group)
    // join; incremental checkpoints whose deltas delete giant rows compact through the span (a delta
    // entry carried across the giant's blocks), and each restores exactly.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_giant_past_the_span_cap_spans_file_blocks_and_restores_exactly(bool readBlocked)
    {
        using var layout = StateLayout.Use(true);
        var savedCap = RunFile.MaxSpanBlockBytes;
        var savedEntries = RunFile.MaxSpanBlockEntries;
        RunFile.MaxSpanBlockBytes = 16 << 10;
        RunFile.MaxSpanBlockEntries = 64;
        try
        {
            var fs = new InMemoryTableFileSystem();
            var p = Compile(true);
            var twin = Compile(readBlocked);
            for (var tick = 0; tick < 8; tick++)
            {
                Tick(p, tick);
                Tick(twin, tick);
            }

            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);

            // The left trace's file: key 0's hash in several consecutive blocks, each only that hash.
            var giant = (uint)new DbspNet.Core.Collections.StructuralRow([0]).GetHashCode();
            var spanned = 0;
            foreach (var path in await ChainOfAsync(fs, "left.arrows"))
            {
                await using var run = await RunReader.OpenAsync(fs, path, static r => r.GetHashCode());
                var at = run.Blocks.Select((b, i) => (b, i)).Where(x => x.b.FirstHash <= giant && giant <= x.b.LastHash).ToList();
                Assert.Equal(Enumerable.Range(at.Count == 0 ? 0 : at[0].i, at.Count), at.Select(x => x.i));
                if (at.Count > 1)
                {
                    Assert.All(at, x => Assert.Equal((giant, giant, 1), (x.b.FirstHash, x.b.LastHash, x.b.Groups)));

                    // A point lookup reads the whole span, not its first block.
                    Assert.Equal((at[0].i, at.Count), run.BlocksOf(new DbspNet.Core.Collections.StructuralRow([0])));
                    spanned++;
                }
            }

            Assert.True(spanned > 0, "the giant should span file blocks");

            var restored = Compile(readBlocked);
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.Equal(Digest(twin), Digest(restored));

            // Deltas that delete giant rows, checkpointed incrementally until chains compact.
            for (var k = 0; k < 8; k++)
            {
                foreach (var q in new[] { twin, restored })
                {
                    q.Table("a").Delete(GiantRow(k % 3, 10 + k));
                    q.Table("a").Insert(0, "late-" + k, (long)k);
                    q.Table("b").Insert(0, "probe-" + k); // joins the whole giant: reads every block of its span
                    q.Step();
                }

                Assert.Equal(Digest(twin), Digest(restored));
                await Snapshot.WriteAsync(restored.Circuit, fs, retainCount: 1);
                await RunCheckpoint.WhenAllCompactedAsync();
                var again = Compile(readBlocked);
                await Snapshot.ReadAsync(again.Circuit, fs);
                Assert.Equal(Digest(twin), Digest(again));

                // Also into a blocked join, which needs every run consolidated (a managed chain's
                // compaction carried delta entries across the giant's blocks).
                var blocked = Compile(true);
                await Snapshot.ReadAsync(blocked.Circuit, fs);
                Assert.Equal(Digest(twin), Digest(blocked));
                Op(blocked).LeftTrace.CheckRuns();
            }
        }
        finally
        {
            RunFile.MaxSpanBlockBytes = savedCap;
            RunFile.MaxSpanBlockEntries = savedEntries;
        }
    }

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
            // (Unless a mid-batch spill, §5.9, already wrote the run: its file holds the giant whole.)
            if (DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Spiller is null)
            {
                Assert.Contains(Op(p).LeftTrace.Runs, r => Enumerable.Range(1, r.Blocks.Length - 1).Any(b => r.Blocks[b].FirstHash == r.Blocks[b - 1].LastHash));
            }
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

    // docs/design-bounded-memory.md §3.1: with a block budget, a committed checkpoint rebinds each
    // run it wrote to the run file, and the cache drops bodies past the budget. Probes, merges and
    // later checkpoints read them back — across checkpoints that retire older snapshots — and the
    // program stays exactly its uncheckpointed twin. Budget 1 byte: every body is dropped as soon
    // as another is read.
    [Theory]
    [InlineData(1L)]
    [InlineData(4096L)]
    public async Task BoundedCache_EvictsPersistedBlocks_AndStaysExact(long budget)
    {
        using var layout = StateLayout.Use(true);
        var saved = BlockCache.Budget;
        var savedAlign = BlockAccess.AlignSpans;
        BlockCache.Budget = budget;
        BlockAccess.AlignSpans = true; // follows the cache (env) in production
        try
        {
            var fs = new InMemoryTableFileSystem();
            // 64-entry blocks: a giant hash shares its first and last blocks with its neighbours
            // (as with ivm-bench's 1,024), so isolating it in the file is observable.
            var p = Compile(blocked: true, blockEntries: 64);
            var twin = Compile(blocked: true, blockEntries: 64);
            var evictions = BlockCache.Evictions;
            for (var tick = 0; tick < 30; tick++)
            {
                Tick(p, tick);
                Tick(twin, tick);
                if (tick % 3 == 2)
                {
                    // A hash spanning blocks in memory holds them alone (§3.5): the block builder
                    // cuts its neighbours off, so the write concatenates without re-encoding.
                    foreach (var r in Op(p).LeftTrace.Runs)
                    {
                        for (var b = 1; b < r.Blocks.Length; b++)
                        {
                            if (r.Blocks[b].FirstHash == r.Blocks[b - 1].LastHash)
                            {
                                Assert.Equal(r.Blocks[b - 1].FirstHash, r.Blocks[b - 1].LastHash);
                                Assert.Equal(r.Blocks[b].FirstHash, r.Blocks[b].LastHash);
                            }
                        }
                    }

                    await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
                    Assert.Contains(Op(p).LeftTrace.Runs, r => r.Blocks.Length > 0 && r.Blocks.All(b => b is LazyTraceBlock));

                    // A hash spanning blocks in memory has file blocks of its own; its neighbours
                    // stay in ordinary-sized blocks (§3.5), so probing them never reads the giant. One
                    // file block unless it outgrows RunFile.MaxSpanBlockBytes (§5.18): then several,
                    // consecutive, each holding only that hash.
                    var giant = (uint)new DbspNet.Core.Collections.StructuralRow([0]).GetHashCode();
                    var giantBlocks = 0;
                    foreach (var r in Op(p).LeftTrace.Runs)
                    {
                        var lazy = r.Blocks.OfType<LazyTraceBlock>().ToList();
                        var at = lazy.Select((b, i) => (b, i)).Where(x => x.b.FirstHash <= giant && giant <= x.b.LastHash).Select(x => x.i).ToList();
                        if (at.Count == 0)
                        {
                            continue;
                        }

                        Assert.Equal(Enumerable.Range(at[0], at.Count), at);
                        var holding = at.Select(i => lazy[i]).ToList();
                        if (holding.Sum(b => b.Count) > 100)
                        {
                            Assert.All(holding, big => Assert.True(
                                big.Groups == 1, $"key 0 block: {big.Count} entries {big.Groups} hashes; run blocks {string.Join(",", r.Blocks.Select(x => x.Count + "/" + ((LazyTraceBlock)x).Groups))}"));
                            Assert.True(holding.Count == 1 || RunFile.MaxSpanBlockBytes < 1 << 20, "a giant splits only past the cap");
                            giantBlocks++;
                        }
                    }

                    Assert.True(giantBlocks > 0, "key 0 should be a giant group in some run");
                }

                Assert.Equal(Digest(twin), Digest(p));
            }

            Assert.True(BlockCache.Evictions > evictions, "the budget should have evicted bodies");

            // And the checkpoint it last wrote restores exactly.
            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
            var restored = Compile(blocked: true, blockEntries: 64);
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.Equal(Digest(p), Digest(restored));
        }
        finally
        {
            BlockCache.Budget = saved;
            BlockAccess.AlignSpans = savedAlign;
            BlockCache.Clear();
        }
    }
}

/// <summary>The block cache's budget (and the span alignment and backing writes it switches) is
/// process-wide: tests that set it must not run beside any other test.</summary>
[CollectionDefinition(nameof(BlockedCheckpointTests), DisableParallelization = true)]
public sealed class BlockedCheckpointCollection
{
}
