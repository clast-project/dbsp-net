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
/// <b>Coverage limit, asserted explicitly below.</b> A parallel program integrates
/// each output view on the <em>driver</em>, not in-circuit, so the materialised view
/// is outside the per-worker snapshot. Restore therefore reproduces operator state
/// exactly — the next tick's gathered delta matches an uninterrupted run's — but the
/// integrated view restarts from empty. (The serial program path has no such gap: its
/// outputs are in-circuit <c>Integrate</c> operators.)
/// </para>
/// </summary>
public class ParallelProgramSnapshotTests
{
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)",
        "CREATE VIEW per_k AS SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k",
        "CREATE VIEW distinct_k AS SELECT DISTINCT k FROM t",
    ];

    private static readonly HashSet<string> OutputViews =
        new(["per_k", "distinct_k"], StringComparer.Ordinal);

    private static ParallelCompiledProgram CompileParallel(int workers, bool persistent)
    {
        Assert.True(
            SqlProgram.TryCompileParallel(
                ProgramSql, OutputViews, workers, out var p,
                snapshotCodecs: persistent ? ArrowSqlSnapshotCodecs.Instance : null),
            "parallel program compile refused");
        return p!;
    }

    private static void Tick1(ParallelCompiledProgram p)
    {
        p.Table("t").Insert(1, 10L);
        p.Table("t").Insert(1, 20L);
        p.Table("t").Insert(2, 5L);
        p.Table("t").Insert(3, 7L);
    }

    private static void Tick2(ParallelCompiledProgram p)
    {
        p.Table("t").Insert(2, 50L);
        p.Table("t").Delete(1, 10L);
        p.Table("t").Insert(4, 1L);
    }

    private static void Tick3(ParallelCompiledProgram p)
    {
        p.Table("t").Insert(3, 300L);
        p.Table("t").Delete(4, 1L);
        p.Table("t").Insert(5, 9L);
    }

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

    // reference-after-tick3 minus reference-after-tick2 == the delta a restored
    // engine (whose driver view starts empty) accumulates on tick 3.
    private static Dictionary<string, long> Difference(
        Dictionary<string, long> after, Dictionary<string, long> before)
    {
        var d = new Dictionary<string, long>(after, StringComparer.Ordinal);
        foreach (var (k, v) in before)
        {
            d[k] = d.GetValueOrDefault(k) - v;
            if (d[k] == 0)
            {
                d.Remove(k);
            }
        }

        return d;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task SnapshotAndRestore_Program_ReproducesOperatorState(int workers)
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

        var referenceAfter2 = new Dictionary<string, Dictionary<string, long>>(StringComparer.Ordinal);
        foreach (var (name, o) in reference.Outputs)
        {
            referenceAfter2[name] = Materialize(o.CurrentView, o.Schema.Count);
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

        Tick3(reference);
        Tick3(restored);
        reference.Step();
        restored.Step();

        foreach (var (name, o) in reference.Outputs)
        {
            var expectedDelta = Difference(
                Materialize(o.CurrentView, o.Schema.Count), referenceAfter2[name]);
            var restoredOut = restored.Outputs[name];
            Assert.Equal(
                expectedDelta, Materialize(restoredOut.CurrentView, restoredOut.Schema.Count));
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
