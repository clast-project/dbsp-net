// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Crash-injection matrix for the snapshot commit (docs/design-durable-state.md, increment 0).
//
// For every mutation a checkpoint performs, crash just before it — as a process crash (the OS
// keeps every write) and as a power loss (only fsynced data survives) — then restart, restore,
// and require:
//
//   1. a snapshot is always found: a previously committed checkpoint is never lost, and a crash
//      can never leave "no pointer" (which made the runner silently start from scratch);
//   2. the restored state is EXACTLY the producer's state at one of the two commit points —
//      the previous checkpoint, or the new one;
//   3. once the write has returned, the new checkpoint is what restores, even after power loss;
//   4. the restored program, driven further, matches an uninterrupted run (the §7.2 property).
//
// Two scenarios: the new checkpoint is at a later tick, or at the SAME tick as the committed
// one (an idle batch). The second is the one the old protocol failed: it rewrote the committed
// directory in place, so a crash mid-rewrite destroyed the only snapshot.
using DbspNet.Core.Circuit;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class CrashSafeCheckpointTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (k INT NOT NULL, v DOUBLE NOT NULL)",
        "CREATE TABLE u (k INT NOT NULL, w BIGINT NOT NULL)",
        "CREATE VIEW agg AS SELECT k, AVG(v) AS a, COUNT(*) AS c FROM t GROUP BY k",
        "CREATE VIEW j AS SELECT t.k AS k, t.v AS v, u.w AS w FROM t JOIN u ON t.k = u.k",
        "CREATE VIEW d AS SELECT DISTINCT k FROM u",
    ];

    private static readonly HashSet<string> Views = new(["agg", "j", "d"], StringComparer.Ordinal);

    private static CompiledProgram Compile(string family) =>
        SqlProgram.Compile(
            Sql,
            Views,
            ArrowSqlSnapshotCodecs.Instance,
            new CompileOptions { TraceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat });

    // Phase A runs before the committed checkpoint, B between it and the crashed one, C after
    // the restore. Repeated rows coalesce trace weights (what exposes a lossy reload fold).
    private static void PhaseA(CompiledProgram p)
    {
        for (var i = 0; i < 3; i++)
        {
            p.Table("t").Insert(1, 1e16);
            p.Table("t").Insert(2, 0.5 + i);
            p.Table("u").Insert(1, 10L + i);
            p.Step();
        }
    }

    private static void PhaseB(CompiledProgram p)
    {
        p.Table("t").Insert(1, 1.0);
        p.Table("u").Insert(2, 7L);
        p.Step();
        p.Table("t").Delete(2, 0.5);
        p.Table("t").Insert(3, 4.25);
        p.Step();
    }

    private static void PhaseC(CompiledProgram p)
    {
        p.Table("t").Insert(1, 2.0);
        p.Table("u").Delete(1, 10L);
        p.Table("u").Insert(3, 1L);
        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("\n", Views.OrderBy(v => v, StringComparer.Ordinal).Select(v =>
            v + ": " + string.Join("; ", p.Outputs[v].CurrentView
                .Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count)
                    .Select(i => FormattableString.Invariant($"{e.Key[i]:R}"))) + "#" + e.Value.Value)
                .OrderBy(s => s, StringComparer.Ordinal))));

    private sealed record Oracle(long Tick, string AtCheckpoint, string AfterContinuing);

    // The uninterrupted run's state at a commit point, and after continuing from it with phase C.
    private static Oracle OracleAt(string family, bool withPhaseB)
    {
        var p = Compile(family);
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
    public async Task CrashAtEveryMutation_RecoversExactlyTheOldOrTheNewCheckpoint(string family, bool laterTick)
    {
        var before = OracleAt(family, withPhaseB: false);
        var after = laterTick ? OracleAt(family, withPhaseB: true) : before;

        // How many mutations one checkpoint performs — every one is a crash point, plus "none".
        long mutations;
        {
            var fs = new CrashSimulatingFileSystem();
            var p = Compile(family);
            PhaseA(p);
            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 2);
            if (laterTick)
            {
                PhaseB(p);
            }

            var m0 = fs.Mutations;
            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 2);
            mutations = fs.Mutations - m0;
        }

        Assert.True(mutations > 3, $"expected a multi-step commit, saw {mutations} mutations");

        for (var k = 0L; k <= mutations; k++)
        {
            var fs = new CrashSimulatingFileSystem();
            var producer = Compile(family);
            PhaseA(producer);
            await Snapshot.WriteAsync(producer.Circuit, fs, retainCount: 2);
            if (laterTick)
            {
                PhaseB(producer);
            }

            fs.CrashAt = fs.Mutations + k;
            var completed = true;
            try
            {
                await Snapshot.WriteAsync(producer.Circuit, fs, retainCount: 2);
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

                Assert.True(await Snapshot.PointerExistsAsync(restart), $"{where}: commit pointer lost");

                var recovered = Compile(family);
                await Snapshot.ReadAsync(recovered.Circuit, restart);

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

                // And the store stays usable: the next checkpoint commits and prunes the debris.
                await Snapshot.WriteAsync(recovered.Circuit, restart, retainCount: 2);
                Assert.True((await Snapshot.ListSnapshotsAsync(restart)).Count <= 2, $"{where}: prune left debris");
            }
        }
    }

    [Fact]
    public async Task SameTickRewrite_NeverTouchesTheCommittedDirectory()
    {
        var fs = new CrashSimulatingFileSystem();
        var p = Compile("flat");
        PhaseA(p);
        await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 2);

        var committed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        await foreach (var e in fs.ListAsync("snap-"))
        {
            committed[e.Path] = await fs.ReadAllBytesAsync(e.Path);
        }

        await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 2);

        foreach (var (path, bytes) in committed)
        {
            Assert.True(await fs.ExistsAsync(path), $"{path} was removed");
            Assert.Equal(bytes, await fs.ReadAllBytesAsync(path));
        }

        var pointer = System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));
        Assert.Equal($"snap-{p.Circuit.TickCount}.1", pointer);
    }

    [Fact]
    public async Task OrphanAheadOfCurrent_DoesNotCrowdOutTheCurrentSnapshot()
    {
        // A crashed write at a later tick leaves an orphan directory; the restarted run restores the
        // older snapshot, ticks to an intermediate tick and commits there. Under retainCount 1 the old
        // prune kept the highest-numbered directory — the orphan — and deleted the new current one.
        var fs = new CrashSimulatingFileSystem();
        var p = Compile("flat");
        PhaseA(p);
        await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
        var baseTick = p.Circuit.TickCount;

        PhaseB(p);
        PhaseC(p);
        fs.CrashAt = fs.Mutations + 3; // a few op files in, before the manifest
        await Assert.ThrowsAsync<SimulatedCrashException>(async () => await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1));

        var restart = await fs.AfterProcessCrashAsync();
        var resumed = Compile("flat");
        await Snapshot.ReadAsync(resumed.Circuit, restart);
        Assert.Equal(baseTick, resumed.Circuit.TickCount);

        PhaseB(resumed);
        var expected = Digest(resumed);
        await Snapshot.WriteAsync(resumed.Circuit, restart, retainCount: 1);

        var check = Compile("flat");
        await Snapshot.ReadAsync(check.Circuit, restart);
        Assert.Equal(resumed.Circuit.TickCount, check.Circuit.TickCount);
        Assert.Equal(expected, Digest(check));
        Assert.Equal([resumed.Circuit.TickCount], await Snapshot.ListSnapshotsAsync(restart));
    }
}
