// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Per-batch state persistence on the <b>structural-parallel program</b> path
/// (docs/design-structural-parallel.md §10): <c>SqlProgram.TryCompileParallel</c>
/// threads <see cref="ArrowSqlSnapshotCodecs"/> to every replica, so a program's
/// operator state checkpoints through <see cref="ParallelSnapshot"/> as W disjoint
/// <c>worker-{w}/</c> shards.
/// <para>
/// <b>The former coverage limit is closed (§10.4).</b> Output views used to be
/// integrated on the <em>driver</em>, outside the per-worker snapshot, so restore
/// reproduced operator state exactly but the integrated view restarted from empty.
/// Views are now integrated <em>in-circuit, one integral per replica</em>, so they
/// are ordinary <c>ISnapshotable</c> state inside the <c>worker-{w}/</c> subtrees.
/// The tests below assert the full view survives a restore — not merely the delta —
/// which is what a parallel pause/resume needs.
/// </para>
/// </summary>
public class ParallelProgramSnapshotTests
{
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)",
        "CREATE VIEW per_k AS SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k",
        "CREATE VIEW distinct_k AS SELECT DISTINCT k FROM t",
        // Deliberately NOT distinct and NOT grouped: distinct input rows collapse to
        // the same output row, and `t` shards by whole-row hash, so one output row can
        // be produced by several workers with weights that must be SUMMED. per_k and
        // distinct_k both partition by k and so have shard-disjoint outputs — they
        // would pass even if §10.4's shard combine were a union rather than a sum.
        "CREATE VIEW k_only AS SELECT k FROM t",
    ];

    private static readonly HashSet<string> OutputViews =
        new(["per_k", "distinct_k", "k_only"], StringComparer.Ordinal);

    private static ParallelCompiledProgram CompileParallel(int workers, bool persistent)
    {
        Assert.True(
            SqlProgram.TryCompileParallel(
                ProgramSql, OutputViews, workers, out var p,
                snapshotCodecs: persistent ? ArrowSqlSnapshotCodecs.Instance : null),
            "parallel program compile refused");
        return p!;
    }

    // One definition of the op-script, so serial and parallel are driven from the
    // same data (the §10.4 oracle below needs both).
    private static readonly (object?[] Row, long Weight)[][] TickScript =
    [
        [([1, 10L], 1), ([1, 20L], 1), ([2, 5L], 1), ([3, 7L], 1)],
        [([2, 50L], 1), ([1, 10L], -1), ([4, 1L], 1)],
        [([3, 300L], 1), ([4, 1L], -1), ([5, 9L], 1)],
    ];

    private static void Apply(ParallelCompiledProgram p, int tick)
    {
        foreach (var (row, weight) in TickScript[tick])
        {
            if (weight > 0)
            {
                p.Table("t").Insert(row);
            }
            else
            {
                p.Table("t").Delete(row);
            }
        }
    }

    private static void Apply(CompiledProgram p, int tick)
    {
        foreach (var (row, weight) in TickScript[tick])
        {
            if (weight > 0)
            {
                p.Table("t").Insert(row);
            }
            else
            {
                p.Table("t").Delete(row);
            }
        }
    }

    private static void Tick1(ParallelCompiledProgram p) => Apply(p, 0);

    private static void Tick2(ParallelCompiledProgram p) => Apply(p, 1);

    private static void Tick3(ParallelCompiledProgram p) => Apply(p, 2);

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var cells = new string[width];
            for (var i = 0; i < width; i++)
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

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task SnapshotAndRestore_Program_ReproducesViewAndOperatorState(int workers)
    {
        var fs = new InMemoryTableFileSystem();

        using var reference = CompileParallel(workers, persistent: true);
        using var producer = CompileParallel(workers, persistent: true);

        Tick1(reference);
        Tick1(producer);
        reference.Step();
        producer.Step();

        Tick2(reference);
        Tick2(producer);
        reference.Step();
        producer.Step();

        // The producer's views must match the reference before we checkpoint —
        // otherwise a post-restore match would prove nothing.
        foreach (var (name, o) in reference.Outputs)
        {
            var producerOut = producer.Outputs[name];
            Assert.Equal(
                Materialize(o.CurrentView, o.Schema.Count),
                Materialize(producerOut.CurrentView, producerOut.Schema.Count));
        }

        // Checkpoint the producer's W shards. Threading the codecs is what makes
        // this non-empty — with snapshotCodecs null (the pre-§10 behaviour) no
        // operator registers a codec and nothing is persisted.
        var ops = await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        Assert.True(ops > 0, "no operator state persisted — snapshot codecs did not reach the replicas");
        Assert.True(await ParallelSnapshot.ExistsAsync(fs));

        // Restart: a fresh program at the same W, restored, then Stepped once more.
        using var restored = CompileParallel(workers, persistent: true);
        var loaded = await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(ops, loaded);
        Assert.Equal(producer.Circuit.TickCount, restored.Circuit.TickCount);

        // Immediately after restore — before any further tick — the full view must
        // already be back. This is the assertion the driver-side design could not make.
        foreach (var (name, o) in reference.Outputs)
        {
            var restoredOut = restored.Outputs[name];
            Assert.Equal(
                Materialize(o.CurrentView, o.Schema.Count),
                Materialize(restoredOut.CurrentView, restoredOut.Schema.Count));
        }

        // And it keeps tracking: one more tick on both sides stays equal, so the
        // restored integral is genuinely live state, not a one-shot reload.
        Tick3(reference);
        Tick3(restored);
        reference.Step();
        restored.Step();

        foreach (var (name, o) in reference.Outputs)
        {
            var restoredOut = restored.Outputs[name];
            Assert.Equal(
                Materialize(o.CurrentView, o.Schema.Count),
                Materialize(restoredOut.CurrentView, restoredOut.Schema.Count));
        }
    }

    /// <summary>
    /// The oracle for §10.4's shard sum: a parallel program's <c>CurrentView</c> is
    /// formed by summing W per-replica integrals, so it must equal the <b>serial</b>
    /// program's single in-circuit integral at every W. Without this, the other tests
    /// in this file would pass while being consistently wrong — they compare parallel
    /// against parallel.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void ParallelView_EqualsSerialView_AtEveryTick(int workers)
    {
        var serial = SqlProgram.Compile(ProgramSql, OutputViews);
        using var parallel = CompileParallel(workers, persistent: false);

        for (var tick = 0; tick < TickScript.Length; tick++)
        {
            Apply(serial, tick);
            Apply(parallel, tick);
            serial.Step();
            parallel.Step();

            foreach (var name in OutputViews)
            {
                var s = serial.Outputs[name];
                var p = parallel.Outputs[name];
                Assert.Equal(
                    Materialize(s.CurrentView, s.Schema.Count),
                    Materialize(p.CurrentView, p.Schema.Count));
            }
        }
    }

    /// <summary>W is part of the persisted state (the partition is
    /// <c>StableHash(key) % W</c>), so recovery at a different W is refused rather
    /// than silently misplacing keys.</summary>
    [Fact]
    public async Task Restore_AtDifferentWorkerCount_IsRefused()
    {
        var fs = new InMemoryTableFileSystem();
        using var producer = CompileParallel(4, persistent: true);
        Tick1(producer);
        producer.Step();
        await ParallelSnapshot.WriteAsync(producer.Circuit, fs);

        using var wrong = CompileParallel(2, persistent: true);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await ParallelSnapshot.ReadAsync(wrong.Circuit, fs));
    }

    /// <summary>Compiling with codecs must not change what the program computes —
    /// they are registered at construction and touched only by Save/Load.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void SnapshotCodecs_DoNotChangeResults(int workers)
    {
        using var withCodecs = CompileParallel(workers, persistent: true);
        using var without = CompileParallel(workers, persistent: false);

        foreach (var tick in new Action<ParallelCompiledProgram>[] { Tick1, Tick2, Tick3 })
        {
            tick(withCodecs);
            tick(without);
            withCodecs.Step();
            without.Step();
        }

        foreach (var (name, o) in without.Outputs)
        {
            Assert.Equal(
                Materialize(o.CurrentView, o.Schema.Count),
                Materialize(withCodecs.Outputs[name].CurrentView, withCodecs.Outputs[name].Schema.Count));
        }
    }
}
