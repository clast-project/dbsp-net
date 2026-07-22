// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DbspNet.Connectors.Abstractions;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// Guards the interaction between the structural CSE pass (<see cref="Optimizer.PlanCse"/>)
/// and per-batch state persistence (<see cref="ProgramRunner"/> checkpointing, added on
/// the program path). CSE collapses a duplicated stateful subplan to a <em>single</em>
/// shared operator, so a persisted circuit has fewer stateful operators than a naïve
/// compile would — and snapshot state is addressed <b>positionally</b> by operator index
/// (<c>op-{i}</c> in <c>RootCircuit.Operators</c>). The two features are reconciled only
/// because save- and restore-time circuits are compiled the same deterministic way; this
/// fixture proves a round-trip over a genuinely CSE-collapsed circuit resumes correctly
/// rather than mis-mapping (or fail-guarding) the shrunken operator list.
///
/// <para>The stock <see cref="ProgramRunnerCheckpointTests"/> exercise only GROUP BY +
/// DISTINCT views, which contain no duplicated subplan for CSE to share — so this shape
/// was uncovered until now.</para>
/// </summary>
[Collection("PlanCseCompileCounter")]
public class ProgramCheckpointCseTests
{
    // A q5-shaped hot-items view: the identical `(SELECT k, COUNT(*) FROM t GROUP BY k)`
    // subplan appears twice — once for the per-key output, once inside the global MAX it
    // is band-joined against. CSE interns the two COUNT aggregates into one shared
    // operator, so the compiled circuit carries a single COUNT that BOTH the output and
    // the MAX read. That shared, stateful COUNT is exactly what the checkpoint must
    // round-trip.
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)",
        @"CREATE VIEW hot AS
            SELECT ab.k AS k, ab.c AS c
            FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) AS ab
            JOIN (
              SELECT MAX(cb.c) AS maxc
              FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) AS cb
            ) AS mb
            ON ab.c >= mb.maxc",
    ];

    private static readonly HashSet<string> OutputViews =
        new(["hot"], StringComparer.Ordinal);

    private static SqlSchema TwoInts() =>
        new([
            new SchemaColumn("k", new SqlIntegerType(false)),
            new SchemaColumn("v", new SqlIntegerType(false)),
        ]);

    // Four source versions. The hottest key changes every batch, so per-key COUNT state
    // must survive the checkpoint: if restore lost it, the counts would restart from the
    // post-restore batch and the MAX band-join would select the wrong key.
    //   after v0: k1=2, k2=1        -> max 2 -> hot (1,2)
    //   after v1: k1=2, k2=3        -> max 3 -> hot (2,3)   <-- checkpoint here
    //   after v2: k1=5, k2=3        -> max 5 -> hot (1,5)
    //   after v3: k1=5, k2=3, k3=6  -> max 6 -> hot (3,6)
    private static List<IReadOnlyList<(object?[] Row, long Weight)>> Versions() =>
    [
        [(new object?[] { 1, 10 }, 1L), (new object?[] { 1, 11 }, 1L), (new object?[] { 2, 20 }, 1L)],
        [(new object?[] { 2, 21 }, 1L), (new object?[] { 2, 22 }, 1L)],
        [(new object?[] { 1, 12 }, 1L), (new object?[] { 1, 13 }, 1L), (new object?[] { 1, 14 }, 1L)],
        [(new object?[] { 3, 30 }, 1L), (new object?[] { 3, 31 }, 1L), (new object?[] { 3, 32 }, 1L),
         (new object?[] { 3, 33 }, 1L), (new object?[] { 3, 34 }, 1L), (new object?[] { 3, 35 }, 1L)],
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
    /// Precondition guard: the persistent program compile really is CSE-collapsed. The
    /// shared COUNT subplan is served from the per-reference compile memo instead of being
    /// re-emitted, so a snapshot of this circuit covers one COUNT operator, not two. If a
    /// future change stopped CSE firing here, the round-trip test below would silently
    /// degrade into re-testing the ordinary (non-shared) path — this catches that.
    /// </summary>
    [Fact]
    public void DuplicatedSubplan_CompilesCollapsed_OnThePersistedPath()
    {
        PlanToCircuit.MemoHits = 0;
        PlanToCircuit.MemoMisses = 0;
        _ = CompilePersistent();
        Assert.True(
            PlanToCircuit.MemoHits > 0,
            "expected the duplicated COUNT subplan to compile once and share (CSE collapse) on the program path");
    }

    /// <summary>
    /// Batch 1 checkpoints a CSE-collapsed circuit; a fresh engine restores it and drains
    /// only the versions appended since — landing on exactly the state an uninterrupted run
    /// reaches. The shared COUNT aggregate sits at a positional snapshot slot that differs
    /// from the pre-CSE (duplicated) layout, so this exercises that save/restore agree on
    /// the collapsed operator list.
    /// </summary>
    [Fact]
    public async Task RestoreOverCseCollapsedCircuit_ResumesWithoutReplayingOrSkipping()
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

        // Run 2 ("restart"): a brand-new engine over the full version list. Restore puts
        // it at tick 2 with the cursor on version 1, so the batch drains 2 and 3. This is
        // the load that would throw on a shape mismatch or mis-map on a stale layout.
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
}
