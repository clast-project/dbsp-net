// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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

/// <summary>
/// Per-batch state persistence on the <b>program</b> path (the shape the ivm-bench
/// server drives): a <see cref="ProgramRunner"/> wired to an
/// <see cref="ICheckpointStore"/> snapshots engine state + every source's committed
/// cursor at the end of each <see cref="ProgramRunner.RunBatchAsync"/>, and
/// <see cref="ProgramRunner.RestoreAsync"/> resumes exactly there — so a restarted
/// engine neither re-ingests nor skips a version. Mirrors
/// <see cref="PipelineRunner"/>'s single-query checkpointing.
/// </summary>
public class ProgramRunnerCheckpointTests
{
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE VIEW totals AS SELECT a, SUM(b) AS s, COUNT(*) AS c FROM t GROUP BY a",
        "CREATE VIEW distinct_a AS SELECT DISTINCT a FROM t",
    ];

    private static readonly HashSet<string> OutputViews =
        new(["totals", "distinct_a"], StringComparer.Ordinal);

    private static SqlSchema TwoInts() =>
        new([
            new SchemaColumn("a", new SqlIntegerType(false)),
            new SchemaColumn("b", new SqlIntegerType(false)),
        ]);

    // Four source versions; the checkpoint is taken after the first two.
    private static List<IReadOnlyList<(object?[] Row, long Weight)>> Versions() =>
    [
        [(new object?[] { 1, 10 }, 1L), (new object?[] { 2, 5 }, 1L)],
        [(new object?[] { 1, 20 }, 1L), (new object?[] { 3, 7 }, 1L)],
        [(new object?[] { 2, 5 }, -1L), (new object?[] { 1, 1 }, 1L)],
        [(new object?[] { 3, 100 }, 1L)],
    ];

    private static CompiledProgram CompilePersistent() =>
        SqlProgram.Compile(ProgramSql, OutputViews, ArrowSqlSnapshotCodecs.Instance);

    private static Dictionary<string, long> Materialize(ProgramOutput output)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in output.CurrentView)
        {
            var cells = new string[output.Schema.Count];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = kv.Key[i]?.ToString() ?? "<null>";
            }

            var key = string.Join("|", cells);
            map[key] = map.GetValueOrDefault(key) + kv.Value.Value;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static async ValueTask<ProgramRunner> RunnerOver(
        CompiledProgram program,
        IReadOnlyList<IReadOnlyList<(object?[] Row, long Weight)>> versions,
        ICheckpointStore? checkpoint)
    {
        var input = new FakeInputConnector("t", TwoInts(), versions);
        var outputs = OutputViews
            .Select(v => (IOutputConnector)new FakeOutputConnector(v, OutputMode.Truncate))
            .ToList();
        return await ProgramRunner.CreateAsync(program, [input], outputs, checkpoint);
    }

    /// <summary>
    /// Batch 1 checkpoints; a fresh engine restores it and drains only the versions
    /// appended since — landing on exactly the state an uninterrupted run reaches.
    /// If restore had lost operator state (or replayed the committed versions), the
    /// SUM/COUNT aggregates would differ.
    /// </summary>
    [Fact]
    public async Task RestoreAfterBatch_ResumesWithoutReplayingOrSkipping()
    {
        var all = Versions();
        var firstTwo = all.Take(2).ToList();
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);

        // Run 1: drain the first two versions, then checkpoint (end of RunBatchAsync).
        var p1 = CompilePersistent();
        var r1 = await RunnerOver(p1, firstTwo, store);
        Assert.True(r1.HasCheckpoint);
        Assert.Equal(2, await r1.RunBatchAsync());

        // Run 2 ("restart"): a brand-new engine over the full version list. Restore
        // puts it at tick 2 with the cursor on version 1, so the batch drains 2 and 3.
        var p2 = CompilePersistent();
        var r2 = await RunnerOver(p2, all, store);
        Assert.Equal(2, await r2.RestoreAsync());
        Assert.Equal(2, p2.Circuit.TickCount);
        Assert.Equal(2, await r2.RunBatchAsync());

        // Reference: the same four versions, uninterrupted.
        var reference = CompilePersistent();
        var rr = await RunnerOver(reference, all, checkpoint: null);
        Assert.False(rr.HasCheckpoint);
        Assert.Equal(0, await rr.RestoreAsync());
        Assert.Equal(4, await rr.RunBatchAsync());

        foreach (var view in OutputViews)
        {
            Assert.Equal(Materialize(reference.Outputs[view]), Materialize(p2.Outputs[view]));
        }

        Assert.Equal(reference.Circuit.TickCount, p2.Circuit.TickCount);
    }

    /// <summary>The second batch checkpoints too — each <c>RunBatchAsync</c> ends
    /// durable, so a restart after batch N resumes at batch N+1.</summary>
    [Fact]
    public async Task EveryBatchCheckpoints_RestartAfterSecondBatch()
    {
        var all = Versions();
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);

        var p1 = CompilePersistent();
        var r1 = await RunnerOver(p1, all.Take(2).ToList(), store);
        await r1.RunBatchAsync();

        // Batch 2 on the same runner, over the appended versions.
        var p2 = CompilePersistent();
        var r2 = await RunnerOver(p2, all.Take(3).ToList(), store);
        await r2.RestoreAsync();
        await r2.RunBatchAsync();

        // Restart after batch 2: restore must land on tick 3, not tick 2.
        var p3 = CompilePersistent();
        var r3 = await RunnerOver(p3, all, store);
        Assert.Equal(3, await r3.RestoreAsync());
        Assert.Equal(1, await r3.RunBatchAsync());

        var reference = CompilePersistent();
        var rr = await RunnerOver(reference, all, checkpoint: null);
        await rr.RunBatchAsync();

        foreach (var view in OutputViews)
        {
            Assert.Equal(Materialize(reference.Outputs[view]), Materialize(p3.Outputs[view]));
        }
    }

    /// <summary>
    /// A program compiled <em>without</em> snapshot codecs Steps identically to one
    /// compiled with them — the codecs are registered at operator construction and
    /// read only by Save/Load, so turning persistence on never changes results.
    /// (The scaling harness measures the same claim in wall-clock.)
    /// </summary>
    [Fact]
    public async Task SnapshotCodecs_DoNotChangeResults()
    {
        var all = Versions();

        var withCodecs = CompilePersistent();
        await (await RunnerOver(withCodecs, all, checkpoint: null)).RunBatchAsync();

        var without = SqlProgram.Compile(ProgramSql, OutputViews);
        await (await RunnerOver(without, all, checkpoint: null)).RunBatchAsync();

        foreach (var view in OutputViews)
        {
            Assert.Equal(Materialize(without.Outputs[view]), Materialize(withCodecs.Outputs[view]));
        }
    }
}
