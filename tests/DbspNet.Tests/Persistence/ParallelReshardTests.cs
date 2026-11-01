// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Re-sharding a parallel snapshot to another worker count (docs/design-durable-state.md §1d): the
// rewritten snapshot restores into a program at the new W, which then carries on exactly as an
// uninterrupted run — for every kind of stateful operator the parallel compile builds, including
// the placements that are not a plain hash of the stored key.
using DbspNet.Core.Circuit;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class ParallelReshardTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, c INT NOT NULL)",
        "CREATE TABLE d (a INT NOT NULL, name VARCHAR NOT NULL)",
        // hash join, then an aggregate keyed differently (its exchange is kept)
        "CREATE VIEW by_c AS SELECT u.c, SUM(t.b) AS s FROM t JOIN u ON t.a = u.a GROUP BY u.c",
        // grouped by the join key — a projection sits between join and aggregate, so the exchange stays
        "CREATE VIEW by_a AS SELECT t.a, u.c, COUNT(*) AS n FROM t JOIN u ON t.a = u.a GROUP BY t.a, u.c",
        // an aggregate over an aggregate, grouped by a superset of its key: its exchange IS elided,
        // so its groups sit where the inner aggregate hashed them, not on OfWholeRow(its own key)
        "CREATE VIEW nested AS SELECT k, s2, COUNT(*) AS n FROM (SELECT a AS k, SUM(b) AS s2 FROM t GROUP BY a) x GROUP BY k, s2",
        "CREATE VIEW dist AS SELECT DISTINCT b FROM t",
        "CREATE VIEW lo AS SELECT t.a, t.b, d.name FROM t LEFT JOIN d ON t.a = d.a",
        "CREATE VIEW ro AS SELECT t.b, d.a, d.name FROM t RIGHT JOIN d ON t.a = d.a",
        "CREATE VIEW fo AS SELECT t.b, d.name FROM t FULL OUTER JOIN d ON t.a = d.a",
        // outer join with a residual: NonZeroSet distincts placed by the left key of the joined row
        "CREATE VIEW lor AS SELECT t.a, t.b, u.c FROM t LEFT JOIN u ON t.a = u.a AND t.b > u.c",
        "CREATE VIEW semi AS SELECT a, b FROM t WHERE a IN (SELECT a FROM u)",
        "CREATE VIEW anti AS SELECT a, b FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
        // global aggregate (empty key), and a broadcast cross join against a one-row side
        "CREATE VIEW total AS SELECT COUNT(*) AS n, SUM(b) AS s FROM t",
        "CREATE VIEW share AS SELECT t.a, t.b, g.n FROM t CROSS JOIN (SELECT COUNT(*) AS n FROM u) g",
        "CREATE VIEW top2 AS SELECT a, b FROM (" +
        "  SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t) s WHERE rn <= 2",
        "CREATE VIEW ranked AS SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW running AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b) AS rs FROM t",
        "CREATE VIEW prev AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS pb FROM t",
        // gathered to worker 0: a global TOP-K and an unpartitioned window
        "CREATE VIEW best AS SELECT a, b FROM t ORDER BY b DESC, a LIMIT 3",
        "CREATE VIEW grun AS SELECT a, b, SUM(b) OVER (ORDER BY b) AS rs FROM t",
    ];

    private static readonly HashSet<string> Views =
        new(["by_c", "by_a", "nested", "dist", "lo", "ro", "fo", "lor", "semi", "anti", "total", "share", "top2", "ranked",
             "running", "prev", "best", "grun"], StringComparer.Ordinal);

    private static ParallelCompiledProgram Compile(int workers, TraceFamily family = TraceFamily.Flat)
    {
        Assert.True(
            SqlProgram.TryCompileParallel(
                Sql, Views, workers, out var p, ArrowSqlSnapshotCodecs.Instance,
                new CompileOptions { TraceFamily = family }),
            "parallel compile refused: " + string.Join("; ", SqlProgram.ParallelBlockers(Sql, Views)
                .Select(b => b.View + " [" + string.Join(", ", b.Blockers) + "]")));
        return p!;
    }

    // A deterministic feed with inserts and deletes; tick k is the same in every run.
    private sealed class Feed(int seed)
    {
        private readonly Random _rng = new(seed);
        private readonly List<object?[]> _t = [];
        private readonly List<object?[]> _d = [];

        public void Tick(Action<string, object?[], bool> push)
        {
            for (var i = 0; i < 8; i++)
            {
                var row = new object?[] { _rng.Next(0, 6), _rng.Next(0, 40) };
                _t.Add(row);
                push("t", row, true);
            }

            if (_t.Count > 4 && _rng.Next(2) == 0)
            {
                var victim = _t[_rng.Next(_t.Count)];
                _t.Remove(victim);
                push("t", victim, false);
            }

            push("u", [_rng.Next(0, 6), _rng.Next(0, 30)], true);
            var drow = new object?[] { _rng.Next(0, 8), "n" + _rng.Next(0, 4) };
            _d.Add(drow);
            push("d", drow, true);
            if (_d.Count > 3 && _rng.Next(3) == 0)
            {
                var victim = _d[_rng.Next(_d.Count)];
                _d.Remove(victim);
                push("d", victim, false);
            }
        }
    }

    private static void Tick(ParallelCompiledProgram p, Feed feed)
    {
        feed.Tick((table, row, insert) =>
        {
            if (insert)
            {
                p.Table(table).Insert(row);
            }
            else
            {
                p.Table(table).Delete(row);
            }
        });
        p.Step();
    }

    private static void Tick(CompiledProgram p, Feed feed)
    {
        feed.Tick((table, row, insert) =>
        {
            if (insert)
            {
                p.Table(table).Insert(row);
            }
            else
            {
                p.Table(table).Delete(row);
            }
        });
        p.Step();
    }

    private static string Digest(IEnumerable<string> views, Func<string, IEnumerable<string>> rows) =>
        string.Join("\n", views.OrderBy(v => v, StringComparer.Ordinal)
            .Select(v => v + ": " + string.Join("; ", rows(v).OrderBy(s => s, StringComparer.Ordinal))));

    private static string Digest(ParallelCompiledProgram p) =>
        Digest(Views, v => p.Outputs[v].CurrentView.Select(e => Row(e.Key, p.Outputs[v].Schema.Count) + "#" + e.Value.Value));

    private static string Digest(CompiledProgram p) =>
        Digest(Views, v => p.Outputs[v].CurrentView.Select(e => Row(e.Key, p.Outputs[v].Schema.Count) + "#" + e.Value.Value));

    private static string Row(DbspNet.Core.Collections.StructuralRow r, int n) =>
        string.Join("|", Enumerable.Range(0, n).Select(i => FormattableString.Invariant($"{r[i]}")));

    [Fact]
    public void EveryStatefulOperator_HasAStatedPlacement()
    {
        using var p = Compile(3);
        var replica = p.Circuit.Replicas[0];
        var stateful = Enumerable.Range(0, replica.Operators.Count).Where(i => replica.Operators[i] is ISnapshotable).ToList();
        Assert.NotEmpty(stateful);
        foreach (var i in stateful)
        {
            Assert.True(p.ReshardSpecs.TryGetValue(i, out var spec), $"operator {i} ({replica.Operators[i].GetType().Name}) has no spec");
            Assert.Null(spec!.Unsupported);
            Assert.NotEmpty(spec.Files);
        }

        Assert.Equal(stateful.Count, p.ReshardSpecs.Count);
    }

    [Theory]
    [InlineData(2, 3)]
    [InlineData(3, 2)]
    [InlineData(4, 12)]
    [InlineData(12, 4)]
    [InlineData(5, 2)]
    public async Task Reshard_ThenContinue_MatchesAnUninterruptedRun(int oldW, int newW)
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        var reference = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        var refFeed = new Feed(11);
        var feed = new Feed(11);
        using (var producer = Compile(oldW))
        {
            for (var tick = 0; tick < 6; tick++)
            {
                Tick(producer, feed);
                Tick(reference, refFeed);
            }

            Assert.Equal(Digest(reference), Digest(producer));
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Compile(newW);
        Assert.Equal(oldW, await ParallelReshard.RewriteAsync(fs, restored));
        Assert.Equal(newW, await ParallelSnapshot.RecordedWorkersAsync(fs));
        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(reference), Digest(restored));

        for (var tick = 6; tick < 11; tick++)
        {
            Tick(reference, refFeed);
            Tick(restored, feed);
            Assert.True(Digest(reference) == Digest(restored), $"tick {tick}: the re-sharded run differs");
        }
    }

    [Fact]
    public async Task Rewrite_IsANoOp_AtTheSameWorkerCount_OrWithoutASnapshot()
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        using var p = Compile(3);
        Assert.Null(await ParallelReshard.RewriteAsync(fs, p));
        Tick(p, new Feed(1));
        await ParallelSnapshot.WriteAsync(p.Circuit, fs);
        Assert.Null(await ParallelReshard.RewriteAsync(fs, p));
    }

    [Fact]
    public async Task AWrongPlacement_IsCaughtOnTheOldData_AndNothingIsCommitted()
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        using (var producer = Compile(3))
        {
            var feed = new Feed(5);
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(producer, feed);
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var target = Compile(2);
        // Claim every hash-placed file hashes to the constant 0: entries on workers 1 and 2 contradict it.
        var wrong = target.ReshardSpecs.ToDictionary(
            kv => kv.Key,
            kv => kv.Value with
            {
                Files = kv.Value.Files.Select(f => f.Placement is ReshardPlacement.Hash
                    ? f switch
                    {
                        ZSetReshardFile z => (ReshardFile)(z with { Placement = new ReshardPlacement.Hash(_ => 0) }),
                        IndexedReshardFile i => i with { Placement = new ReshardPlacement.Hash(_ => 0) },
                        KeyedBlobsReshardFile k => k with { Placement = new ReshardPlacement.Hash(_ => 0) },
                        _ => f,
                    }
                    : f).ToList(),
            });
        var lying = new ParallelCompiledProgram(target.Circuit, target.Inputs, target.Outputs, wrong);

        var pointer = await fs.ReadAllBytesAsync("current.txt");
        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () => await ParallelReshard.RewriteAsync(fs, lying));
        Assert.Contains("does not match where the state actually is", ex.Message, StringComparison.Ordinal);
        Assert.Equal(pointer, await fs.ReadAllBytesAsync("current.txt"));
        Assert.Equal(3, await ParallelSnapshot.RecordedWorkersAsync(fs));
    }

    [Fact]
    public async Task SpineTraces_AreRefused()
    {
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        using (var producer = Compile(2, TraceFamily.Spine))
        {
            Tick(producer, new Feed(3));
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var target = Compile(3, TraceFamily.Spine);
        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () => await ParallelReshard.RewriteAsync(fs, target));
        Assert.Contains("spine", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACrashAnywhereInTheRewrite_LeavesARestorableSnapshot()
    {
        // Counts mutations: the cleanup a checkpoint defers must happen inside the count.
        using var inlineCleanup = CheckpointCleanup.Inline();
        // Build the source snapshot once, then crash the rewrite at every mutation in turn. After
        // each crash (process crash and power loss), the store must restore either at the old W
        // (before the commit) or at the new W (after it), and either way equal the reference.
        var source = new CrashSimulatingFileSystem();
        var reference = SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
        var refFeed = new Feed(21);
        var feed = new Feed(21);
        using (var producer = Compile(3))
        {
            for (var tick = 0; tick < 3; tick++)
            {
                Tick(producer, feed);
                Tick(reference, refFeed);
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, source);
        }

        var expected = Digest(reference);
        var sawOld = false;
        var sawNew = false;
        for (var k = 0; ; k++)
        {
            var fs = await source.AfterProcessCrashAsync();
            fs.CrashAt = fs.Mutations + k;
            using var target = Compile(2);
            var completed = true;
            try
            {
                await ParallelReshard.RewriteAsync(fs, target);
            }
            catch (SimulatedCrashException)
            {
                completed = false;
            }

            foreach (var after in new[] { await fs.AfterProcessCrashAsync(), await fs.AfterPowerLossAsync() })
            {
                var w = await ParallelSnapshot.RecordedWorkersAsync(after);
                Assert.True(w is 3 or 2, $"crash point {k}: recorded W {w}");
                sawOld |= w == 3;
                sawNew |= w == 2;
                using var restored = Compile(w!.Value);
                await ParallelSnapshot.ReadAsync(restored.Circuit, after);
                Assert.True(expected == Digest(restored), $"crash point {k}: restored state differs (W={w})");
            }

            if (completed)
            {
                break;
            }
        }

        Assert.True(sawOld && sawNew);
    }
}
