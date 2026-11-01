// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Snapshots in run format (docs/design-durable-state.md §1f, increment 2): with StateLayout.Runs on
// every trace file is a run; a snapshot in either layout restores whatever the setting is now; and
// the restored program carries on exactly as one restored from today's layout.
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class StateLayoutTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, s VARCHAR NOT NULL)",
        "CREATE VIEW agg AS SELECT a, SUM(b) AS s, COUNT(*) AS n FROM t GROUP BY a",
        "CREATE VIEW j AS SELECT t.a, t.b, u.s FROM t JOIN u ON t.a = u.a",
        "CREATE VIEW d AS SELECT DISTINCT b FROM t",
        "CREATE VIEW top AS SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t) x WHERE rn <= 2",
    ];

    private static readonly HashSet<string> Views = new(["agg", "j", "d", "top"], StringComparer.Ordinal);

    private static void Feed(Action<string, object?[], bool> push, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < 20; i++)
        {
            push("t", [rng.Next(0, 8), rng.Next(0, 50)], true);
        }

        push("u", [rng.Next(0, 8), "s" + rng.Next(0, 5)], true);
    }

    private static void Tick(CompiledProgram p, int tick)
    {
        Feed((table, row, ins) => p.Table(table).Insert(row), tick);
        p.Step();
    }

    private static void Tick(ParallelCompiledProgram p, int tick)
    {
        Feed((table, row, ins) => p.Table(table).Insert(row), tick);
        p.Step();
    }

    private static string Digest(Func<string, IEnumerable<string>> rows) =>
        string.Join("\n", Views.OrderBy(v => v, StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ", rows(v).OrderBy(x => x, StringComparer.Ordinal))));

    private static string Digest(CompiledProgram p) =>
        Digest(v => p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value));

    private static string Digest(ParallelCompiledProgram p) =>
        Digest(v => p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value));

    private static async Task<(int Runs, int Other)> CountLayoutsAsync(InMemoryTableFileSystem fs)
    {
        int runs = 0, other = 0;
        await foreach (var f in fs.ListAsync(""))
        {
            if (!f.Path.EndsWith(".arrows", StringComparison.Ordinal) || f.Path.EndsWith("aggstate.arrows", StringComparison.Ordinal))
            {
                continue; // keyed blobs stay in today's layout until increment 4
            }

            // A run in place, a run list over runs/ (increment 3), or a run under runs/.
            await using var file = await fs.OpenReadAsync(f.Path);
            if (await RunReader.IsRunAsync(file, default) || await RunChain.IsListAsync(file, default))
            {
                runs++;
            }
            else
            {
                other++;
            }
        }

        return (runs, other);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task SerialSnapshot_RestoresAcrossLayouts_AndCarriesOn(bool writeRuns, bool readRuns)
    {
        var fs = new InMemoryTableFileSystem();
        var reference = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        var producer = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        for (var tick = 0; tick < 4; tick++)
        {
            Tick(reference, tick);
            Tick(producer, tick);
        }

        using (StateLayout.Use(writeRuns))
        {
            await Snapshot.WriteAsync(producer.Circuit, fs);
        }

        var (runs, other) = await CountLayoutsAsync(fs);
        Assert.True(writeRuns ? runs > 0 && other == 0 : runs == 0 && other > 0, $"runs {runs}, other {other}");

        var restored = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        using (StateLayout.Use(readRuns))
        {
            await Snapshot.ReadAsync(restored.Circuit, fs);
        }

        Assert.Equal(Digest(reference), Digest(restored));
        for (var tick = 4; tick < 7; tick++)
        {
            Tick(reference, tick);
            Tick(restored, tick);
            Assert.Equal(Digest(reference), Digest(restored));
        }
    }

    [Fact]
    public async Task ParallelSnapshotInRuns_RestoresAndReshards()
    {
        var fs = new InMemoryTableFileSystem();
        var reference = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 3, out var producer, ArrowSqlSnapshotCodecs.Instance));
        using (producer)
        {
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(reference, tick);
                Tick(producer!, tick);
            }

            using (StateLayout.Use(true))
            {
                await ParallelSnapshot.WriteAsync(producer!.Circuit, fs);
            }
        }

        Assert.True((await CountLayoutsAsync(fs)).Other == 0);

        // Re-shard 3 -> 2 with runs on: the rewrite reads runs and writes runs.
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 2, out var restored, ArrowSqlSnapshotCodecs.Instance));
        using (restored)
        {
            using (StateLayout.Use(true))
            {
                Assert.Equal(3, await ParallelReshard.RewriteAsync(fs, restored!));
            }

            await ParallelSnapshot.ReadAsync(restored!.Circuit, fs);
            Assert.Equal(Digest(reference), Digest(restored));
            for (var tick = 4; tick < 7; tick++)
            {
                Tick(reference, tick);
                Tick(restored, tick);
                Assert.Equal(Digest(reference), Digest(restored));
            }
        }
    }
}
