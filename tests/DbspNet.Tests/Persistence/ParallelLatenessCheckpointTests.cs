// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Checkpoint, restore and re-shard of a parallel program that declares LATENESS
// (docs/design-parallel-frontier.md §2 Q4): every replica persists the same, global maxSeen, so a
// restore at the same W reloads it per replica and a re-shard copies it to every new worker. The
// feed sends late rows every tick, on every shard, so a replica restored without its frontier
// admits rows the serial path drops, and the comparison against an uninterrupted SERIAL run fails.
using DbspNet.Core.Circuit;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class ParallelLatenessCheckpointTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 4, v INT NOT NULL)",
        "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 4, w INT NOT NULL)",
        "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
        "CREATE VIEW joined AS SELECT p.ts, p.s, b.w FROM per_ts p JOIN b ON p.ts = b.ts",
        "CREATE VIEW dist AS SELECT DISTINCT ts FROM b",
    ];

    private static readonly HashSet<string> Views = new(["per_ts", "joined", "dist"], StringComparer.Ordinal);

    private static CompileOptions Options(string family) =>
        new() { TraceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat };

    private static ParallelCompiledProgram Compile(int workers, string family = "flat")
    {
        Assert.True(
            SqlProgram.TryCompileParallel(Sql, Views, workers, out var p, ArrowSqlSnapshotCodecs.Instance, Options(family)),
            "parallel compile refused");
        return p!;
    }

    private static CompiledProgram CompileSerial(string family = "flat") =>
        SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance, Options(family));

    // Tick k is the same in every run: on-time rows near the advancing clock, and late rows well
    // below the frontier (dropped by the serial path), with distinct values so they spread over the
    // shards.
    private static void Tick(int tick, Action<string, object?[]> insert)
    {
        var rng = new Random(1000 + tick);
        long clock = 3L * tick;
        for (var i = 0; i < 6; i++)
        {
            insert("a", [clock - rng.Next(0, 4), rng.Next(1, 9)]);
            insert("b", [clock - rng.Next(0, 4), rng.Next(1, 9)]);
        }

        for (var i = 0; i < 8; i++)
        {
            insert(i % 2 == 0 ? "a" : "b", [clock - 8 - rng.Next(0, 6), 100 + i]);
        }
    }

    private static void Tick(ParallelCompiledProgram p, int tick)
    {
        Tick(tick, (t, row) => p.Table(t).Insert(row));
        p.Step();
    }

    private static void Tick(CompiledProgram p, int tick)
    {
        Tick(tick, (t, row) => p.Table(t).Insert(row));
        p.Step();
    }

    private static string Digest(Func<string, IEnumerable<string>> rows) =>
        string.Join("\n", Views.OrderBy(v => v, StringComparer.Ordinal)
            .Select(v => v + ": " + string.Join("; ", rows(v).OrderBy(s => s, StringComparer.Ordinal))));

    private static string Row(DbspNet.Core.Collections.StructuralRow r, int n) =>
        string.Join("|", Enumerable.Range(0, n).Select(i => FormattableString.Invariant($"{r[i]}")));

    private static string Digest(ParallelCompiledProgram p) =>
        Digest(v => p.Outputs[v].CurrentView.Select(e => Row(e.Key, p.Outputs[v].Schema.Count) + "#" + e.Value.Value));

    private static string Digest(CompiledProgram p) =>
        Digest(v => p.Outputs[v].CurrentView.Select(e => Row(e.Key, p.Outputs[v].Schema.Count) + "#" + e.Value.Value));

    private static List<long?> LatenessFrontiers(RootCircuit circuit) =>
        circuit.CollectStats().Where(s => s.Name == "Lateness").Select(s => s.GcFrontier).ToList();

    // Every replica holds the serial path's frontiers, column for column.
    private static void AssertSerialFrontiers(CompiledProgram reference, ParallelCompiledProgram p)
    {
        var expected = LatenessFrontiers(reference.Circuit);
        Assert.Equal(2, expected.Count);
        foreach (var replica in p.Circuit.Replicas)
        {
            Assert.Equal(expected, LatenessFrontiers(replica));
        }
    }

    public static TheoryData<string, int> SameW() => new()
    {
        { "flat", 2 },
        { "flat", 3 },
        { "flat", 4 },
        { "spine", 2 },
        { "spine", 4 },
    };

    [Theory]
    [MemberData(nameof(SameW))]
    public async Task RestoreAtTheSameW_ThenContinue_MatchesAnUninterruptedSerialRun(string family, int workers)
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        var reference = CompileSerial(family);
        using (var producer = Compile(workers, family))
        {
            for (var tick = 0; tick < 6; tick++)
            {
                Tick(producer, tick);
                Tick(reference, tick);
            }

            Assert.Equal(Digest(reference), Digest(producer));
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Compile(workers, family);
        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(reference), Digest(restored));

        // Every replica restored the same, global frontier — the serial one.
        AssertSerialFrontiers(reference, restored);

        for (var tick = 6; tick < 12; tick++)
        {
            Tick(reference, tick);
            Tick(restored, tick);
            Assert.True(Digest(reference) == Digest(restored), $"tick {tick}: the restored run differs from serial");
        }
    }

    [Theory]
    [InlineData(2, 3)]
    [InlineData(3, 2)]
    [InlineData(4, 2)]
    [InlineData(2, 4)]
    public async Task Reshard_ThenContinue_MatchesAnUninterruptedSerialRun(int oldW, int newW)
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        var reference = CompileSerial();
        using (var producer = Compile(oldW))
        {
            for (var tick = 0; tick < 6; tick++)
            {
                Tick(producer, tick);
                Tick(reference, tick);
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Compile(newW);
        Assert.Equal(oldW, await ParallelReshard.RewriteAsync(fs, restored));
        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(reference), Digest(restored));
        AssertSerialFrontiers(reference, restored);

        for (var tick = 6; tick < 12; tick++)
        {
            Tick(reference, tick);
            Tick(restored, tick);
            Assert.True(Digest(reference) == Digest(restored), $"tick {tick}: the re-sharded run differs from serial");
        }
    }

    [Fact]
    public void EveryStatefulOperator_HasAStatedPlacement_IncludingTheLatenessFrontiers()
    {
        using var p = Compile(3);
        var replica = p.Circuit.Replicas[0];
        var stateful = Enumerable.Range(0, replica.Operators.Count).Where(i => replica.Operators[i] is ISnapshotable).ToList();
        foreach (var i in stateful)
        {
            Assert.True(p.ReshardSpecs.TryGetValue(i, out var spec), $"operator {i} ({replica.Operators[i].GetType().Name}) has no spec");
            Assert.Null(spec!.Unsupported);
        }

        Assert.Equal(2, p.ReshardSpecs.Values.Count(s => s.Files.Any(f => f is IdenticalBlobReshardFile)));
    }

    [Fact]
    public async Task Reshard_RefusesFrontiersThatDisagree_AndCommitsNothing()
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        using (var producer = Compile(2))
        {
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(producer, tick);
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        var dir = System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));
        string? victim = null;
        await foreach (var e in fs.ListAsync(dir + "/worker-1/"))
        {
            if (e.Path.EndsWith("/frontier.bin", StringComparison.Ordinal))
            {
                victim = e.Path;
                break;
            }
        }

        Assert.NotNull(victim);
        var bytes = await fs.ReadAllBytesAsync(victim!);
        bytes[1] ^= 1;
        await fs.WriteAllBytesAsync(victim!, bytes);

        using var restored = Compile(3);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () => await ParallelReshard.RewriteAsync(fs, restored));
        Assert.Contains("identical on every worker", ex.Message, StringComparison.Ordinal);
        Assert.Equal(dir, System.Text.Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt")));
    }
}
