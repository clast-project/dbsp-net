// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Crash-injection matrix for the PARALLEL snapshot commit (docs/design-durable-state.md,
// increment 1). The same contract as CrashSafeCheckpointTests, for W replicas written
// concurrently: crash before every mutation, as a process crash and as a power loss, and the
// restart must find all W workers at ONE of the two commit points — never a mix — with state and
// continuation identical to an uninterrupted run.
//
// The old layout failed this: each worker had its own pointer and pruned on its own, so a crash
// partway through left some workers at the new tick and some at the old one, and restore did not
// notice.
using System.Text.Json.Nodes;
using DbspNet.Core.Circuit;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class ParallelCrashSafeCheckpointTests
{
    private const int Workers = 3;

    private static readonly string[] Sql =
    [
        "CREATE TABLE t (k INT NOT NULL, v DOUBLE NOT NULL)",
        "CREATE TABLE u (k INT NOT NULL, w BIGINT NOT NULL)",
        "CREATE VIEW agg AS SELECT k, AVG(v) AS a, COUNT(*) AS c FROM t GROUP BY k",
        "CREATE VIEW j AS SELECT t.k AS k, t.v AS v, u.w AS w FROM t JOIN u ON t.k = u.k",
        "CREATE VIEW d AS SELECT DISTINCT k FROM u",
    ];

    private static readonly HashSet<string> Views = new(["agg", "j", "d"], StringComparer.Ordinal);

    private static ParallelCompiledProgram Compile(string family, int workers = Workers)
    {
        Assert.True(
            SqlProgram.TryCompileParallel(
                Sql,
                Views,
                workers,
                out var p,
                ArrowSqlSnapshotCodecs.Instance,
                new CompileOptions { TraceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat }),
            "parallel compile refused");
        return p!;
    }

    private static void PhaseA(ParallelCompiledProgram p)
    {
        for (var i = 0; i < 3; i++)
        {
            p.Table("t").Insert(1, 1e16);
            p.Table("t").Insert(2, 0.5 + i);
            p.Table("t").Insert(4 + i, 2.0 * i);
            p.Table("u").Insert(1, 10L + i);
            p.Table("u").Insert(5 + i, 3L);
            p.Step();
        }
    }

    private static void PhaseB(ParallelCompiledProgram p)
    {
        p.Table("t").Insert(1, 1.0);
        p.Table("u").Insert(2, 7L);
        p.Step();
        p.Table("t").Delete(2, 0.5);
        p.Table("t").Insert(3, 4.25);
        p.Table("u").Insert(6, 9L);
        p.Step();
    }

    private static void PhaseC(ParallelCompiledProgram p)
    {
        p.Table("t").Insert(1, 2.0);
        p.Table("t").Insert(6, 1.5);
        p.Table("u").Delete(1, 10L);
        p.Table("u").Insert(3, 1L);
        p.Step();
    }

    private static string Digest(ParallelCompiledProgram p) =>
        string.Join("\n", Views.OrderBy(v => v, StringComparer.Ordinal).Select(v =>
            v + ": " + string.Join("; ", p.Outputs[v].CurrentView
                .Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count)
                    .Select(i => FormattableString.Invariant($"{e.Key[i]:R}"))) + "#" + e.Value.Value)
                .OrderBy(s => s, StringComparer.Ordinal))));

    private sealed record Oracle(long Tick, string AtCheckpoint, string AfterContinuing);

    private static Oracle OracleAt(string family, bool withPhaseB)
    {
        using var p = Compile(family);
        PhaseA(p);
        if (withPhaseB)
        {
            PhaseB(p);
        }

        var tick = p.Circuit.TickCount;
        var at = Digest(p);
        PhaseC(p);
        return new Oracle(tick, at, Digest(p));
    }

    public static TheoryData<string, bool> Cases() => new()
    {
        { "flat", false },
        { "flat", true },
        { "spine", false },
        { "spine", true },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task CrashAtEveryMutation_RecoversAllWorkersAtOneCommitPoint(string family, bool laterTick)
    {
        var before = OracleAt(family, withPhaseB: false);
        var after = laterTick ? OracleAt(family, withPhaseB: true) : before;

        long mutations;
        {
            var fs = new CrashSimulatingFileSystem();
            using var p = Compile(family);
            PhaseA(p);
            await ParallelSnapshot.WriteAsync(p.Circuit, fs, retainCount: 2);
            if (laterTick)
            {
                PhaseB(p);
            }

            var m0 = fs.Mutations;
            await ParallelSnapshot.WriteAsync(p.Circuit, fs, retainCount: 2);
            mutations = fs.Mutations - m0;
        }

        Assert.True(mutations > 3 * Workers, $"expected a multi-step commit, saw {mutations} mutations");

        for (var k = 0L; k <= mutations; k++)
        {
            var fs = new CrashSimulatingFileSystem();
            using (var producer = Compile(family))
            {
                PhaseA(producer);
                await ParallelSnapshot.WriteAsync(producer.Circuit, fs, retainCount: 2);
                if (laterTick)
                {
                    PhaseB(producer);
                }

                fs.CrashAt = fs.Mutations + k;
                var completed = true;
                try
                {
                    await ParallelSnapshot.WriteAsync(producer.Circuit, fs, retainCount: 2);
                }
                catch (SimulatedCrashException)
                {
                    completed = false;
                }

                Assert.Equal(k == mutations, completed);

                foreach (var powerLoss in new[] { false, true })
                {
                    var restart = powerLoss ? await fs.AfterPowerLossAsync() : await fs.AfterProcessCrashAsync();
                    var where = $"{family} laterTick={laterTick} crash before mutation {k}/{mutations} " +
                                (powerLoss ? "(power loss)" : "(process crash)");

                    Assert.True(await ParallelSnapshot.ExistsAsync(restart), $"{where}: commit lost");

                    using var recovered = Compile(family);
                    await ParallelSnapshot.ReadAsync(recovered.Circuit, restart);

                    var tick = recovered.Circuit.TickCount;
                    var expected = completed || tick == after.Tick ? after : before;
                    if (completed)
                    {
                        Assert.True(tick == after.Tick, $"{where}: completed write did not survive (tick {tick})");
                    }

                    Assert.True(
                        tick == before.Tick || tick == after.Tick,
                        $"{where}: restored tick {tick} is neither commit point ({before.Tick}, {after.Tick})");
                    Assert.True(expected.AtCheckpoint == Digest(recovered), $"{where}: restored state differs");

                    PhaseC(recovered);
                    Assert.True(expected.AfterContinuing == Digest(recovered), $"{where}: continued run differs");

                    await ParallelSnapshot.WriteAsync(recovered.Circuit, restart, retainCount: 2);
                    Assert.True((await Snapshot.ListSnapshotsAsync(restart)).Count <= 2, $"{where}: prune left debris");
                }
            }
        }
    }

    // The operators the parallel restore tests did not reach (inventory brief §6): partitioned
    // top-K, rank, window aggregates and offsets, and a LATENESS-bounded aggregate (per-worker
    // maxSeen), alongside a join re-keyed by an aggregate.
    private static readonly string[] WideSql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, c INT NOT NULL)",
        "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, g INT NOT NULL, v INT NOT NULL)",
        "CREATE VIEW by_c AS SELECT u.c, SUM(t.b) AS s FROM t JOIN u ON t.a = u.a GROUP BY u.c",
        "CREATE VIEW top2 AS SELECT a, b FROM (" +
        "  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t) s WHERE rn <= 2",
        "CREATE VIEW ranked AS SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW running AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b) AS rs FROM t",
        "CREATE VIEW prev AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS pb FROM t",
        "CREATE VIEW per_ts AS SELECT ts, g, SUM(v) AS s FROM e GROUP BY ts, g",
    ];

    private static readonly HashSet<string> WideViews =
        new(["by_c", "top2", "ranked", "running", "prev", "per_ts"], StringComparer.Ordinal);

    private static void WideTick(ParallelCompiledProgram p, Random rng, List<object?[]> tLive, int tick)
    {
        for (var i = 0; i < 6; i++)
        {
            var row = new object?[] { rng.Next(0, 5), rng.Next(0, 50) };
            p.Table("t").Insert(row);
            tLive.Add(row);
        }

        if (tLive.Count > 4 && rng.Next(2) == 0)
        {
            var victim = tLive[rng.Next(tLive.Count)];
            tLive.Remove(victim);
            p.Table("t").Delete(victim);
        }

        p.Table("u").Insert(rng.Next(0, 5), rng.Next(0, 4));
        p.Table("e").Insert((long)(tick * 3 + rng.Next(0, 3)), rng.Next(0, 3), rng.Next(1, 9));
        p.Step();
    }

    private static string WideDigest(ParallelCompiledProgram p) =>
        string.Join("\n", WideViews.OrderBy(v => v, StringComparer.Ordinal).Select(v =>
            v + ": " + string.Join("; ", p.Outputs[v].CurrentView
                .Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count)
                    .Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
                .OrderBy(s => s, StringComparer.Ordinal))));

    [Theory]
    [InlineData("flat", 2)]
    [InlineData("flat", 4)]
    [InlineData("spine", 4)]
    public async Task WideProgram_RestoreThenContinue_MatchesUninterrupted(string family, int workers)
    {
        ParallelCompiledProgram CompileWide()
        {
            Assert.True(
                SqlProgram.TryCompileParallel(
                    WideSql, WideViews, workers, out var p, ArrowSqlSnapshotCodecs.Instance,
                    new CompileOptions { TraceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat }),
                "parallel compile refused: " + string.Join("; ", SqlProgram.ParallelBlockers(WideSql, WideViews)
                    .Select(b => b.View + " [" + string.Join(", ", b.Blockers) + "]")));
            return p!;
        }

        var fs = new CrashSimulatingFileSystem();
        using var reference = CompileWide();
        var refRng = new Random(7);
        var refLive = new List<object?[]>();

        // The restored run continues with the producer's RNG and live set.
        var rng = new Random(7);
        var live = new List<object?[]>();
        using (var producer = CompileWide())
        {
            for (var tick = 0; tick < 6; tick++)
            {
                WideTick(producer, rng, live, tick);
                WideTick(reference, refRng, refLive, tick);
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = CompileWide();
        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(WideDigest(reference), WideDigest(restored));

        for (var tick = 6; tick < 10; tick++)
        {
            WideTick(reference, refRng, refLive, tick);
            WideTick(restored, rng, live, tick);
            Assert.True(WideDigest(reference) == WideDigest(restored), $"tick {tick}: continued run differs");
        }
    }

    [Fact]
    public async Task SameTickRewrite_NeverTouchesTheCommittedDirectory()
    {
        var fs = new CrashSimulatingFileSystem();
        using var p = Compile("flat");
        PhaseA(p);
        await ParallelSnapshot.WriteAsync(p.Circuit, fs, retainCount: 2);

        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        await foreach (var e in fs.ListAsync("snap-"))
        {
            committed[e.Path] = await fs.ReadAllBytesAsync(e.Path);
        }

        await ParallelSnapshot.WriteAsync(p.Circuit, fs, retainCount: 2);

        foreach (var (path, bytes) in committed)
        {
            Assert.True(await fs.ExistsAsync(path), $"{path} was removed");
            Assert.Equal(bytes, await fs.ReadAllBytesAsync(path));
        }

        var pointer = System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));
        Assert.Equal($"snap-{p.Circuit.TickCount}.1", pointer);
    }

    /// <summary>Belt and braces: even if a worker's files did come from another tick (a bug, or a
    /// hand-edited store), restore refuses rather than loading inconsistent workers.</summary>
    [Fact]
    public async Task WorkerAtAnotherTick_IsRefused_BeforeAnyStateLoads()
    {
        var fs = new CrashSimulatingFileSystem();
        using (var p = Compile("flat"))
        {
            PhaseA(p);
            await ParallelSnapshot.WriteAsync(p.Circuit, fs);
        }

        var dir = System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));
        var key = dir + "/worker-1/manifest.json";
        var json = JsonNode.Parse(await fs.ReadAllBytesAsync(key))!;
        json["tick"] = json["tick"]!.GetValue<long>() + 1;
        await fs.WriteAllBytesAsync(key, System.Text.Encoding.UTF8.GetBytes(json.ToJsonString()));

        using var recovered = Compile("flat");
        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await ParallelSnapshot.ReadAsync(recovered.Circuit, fs));
        Assert.Contains("worker 1", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, recovered.Circuit.TickCount);
    }

    [Fact]
    public async Task Metadata_CommitsWithTheWorkers()
    {
        var fs = new CrashSimulatingFileSystem();
        using var p = Compile("flat");
        PhaseA(p);
        await ParallelSnapshot.WriteAsync(p.Circuit, fs, new Dictionary<string, string> { ["k"] = "a" });
        PhaseB(p);
        await ParallelSnapshot.WriteAsync(p.Circuit, fs, new Dictionary<string, string> { ["k"] = "b" }, retainCount: 2);

        var metadata = await ParallelSnapshot.ReadMetadataAsync(fs);
        Assert.Equal("b", metadata!["k"]);
        Assert.Equal(Workers, await ParallelSnapshot.RecordedWorkersAsync(fs));
    }

    [Fact]
    public async Task SerialAndParallelSnapshots_RefuseEachOther_WithTheWorkerCount()
    {
        var parallelFs = new CrashSimulatingFileSystem();
        using (var p = Compile("flat"))
        {
            PhaseA(p);
            await ParallelSnapshot.WriteAsync(p.Circuit, parallelFs);
        }

        var serial = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await Snapshot.ReadAsync(serial.Circuit, parallelFs));
        Assert.Contains($"{Workers} workers", ex.Message, StringComparison.Ordinal);

        var serialFs = new CrashSimulatingFileSystem();
        serial.Table("t").Insert(1, 1.0);
        serial.Step();
        await Snapshot.WriteAsync(serial.Circuit, serialFs);
        Assert.Equal(1, await ParallelSnapshot.RecordedWorkersAsync(serialFs));

        using var parallel = Compile("flat");
        var ex2 = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await ParallelSnapshot.ReadAsync(parallel.Circuit, serialFs));
        Assert.Contains("serial program", ex2.Message, StringComparison.Ordinal);
    }
}
