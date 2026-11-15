// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Checkpoint, restore and re-shard of a parallel program with NOW() temporal filters
// (docs/design-program-temporal.md §2 Q3). Each worker's TemporalFilterOp holds the rows that
// arrived there; a re-shard must send each row where the filter's input will deliver its future
// deltas. The `nested` view is the case where that is load-bearing: the filter sits on an
// aggregate's output, passes its partition through, and the aggregate above elides its exchange, so
// a misplaced row meets the wrong worker's groups. Every run is compared with an uninterrupted
// SERIAL run driven by the same clock.
using DbspNet.Core.Circuit;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Persistence;

public class ParallelTemporalCheckpointTests
{
    private const long Sec = 1_000_000L;

    private static readonly string[] Sql =
    [
        "CREATE TABLE e (ts TIMESTAMP NOT NULL, v INT NOT NULL)",
        "CREATE VIEW recent AS SELECT ts, v FROM e WHERE ts > NOW() - INTERVAL '6' SECOND",
        "CREATE VIEW per_ts AS SELECT ts, COUNT(*) AS c, SUM(v) AS s FROM recent GROUP BY ts",
        "CREATE VIEW nested AS SELECT ts, MAX(s) AS m, COUNT(*) AS c FROM "
            + "(SELECT ts, s FROM (SELECT ts, SUM(v) AS s FROM e GROUP BY ts) x WHERE ts > NOW() - INTERVAL '6' SECOND) y "
            + "GROUP BY ts",
    ];

    private static readonly HashSet<string> Views = new(["recent", "per_ts", "nested"], StringComparer.Ordinal);

    private static CompileOptions Options(string family) =>
        new() { TraceFamily = family == "spine" ? TraceFamily.Spine : TraceFamily.Flat };

    private static ParallelCompiledProgram Compile(int workers, string family = "flat", string[]? sql = null, HashSet<string>? views = null)
    {
        Assert.True(
            SqlProgram.TryCompileParallel(sql ?? Sql, views ?? Views, workers, out var p, ArrowSqlSnapshotCodecs.Instance, Options(family)),
            "parallel compile refused");
        return p!;
    }

    private static CompiledProgram CompileSerial(string family = "flat") =>
        SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance, Options(family));

    // Tick k's rows, the same in every run: timestamps around the clock (some already outside the
    // window), several per timestamp so a SUM changes, spread over the shards by their values.
    private static List<object?[]> Rows(int tick)
    {
        var rng = new Random(2000 + tick);
        var clock = 2 * tick;
        var rows = new List<object?[]>();
        for (var i = 0; i < 10; i++)
        {
            rows.Add([new Timestamp((clock - rng.Next(0, 9)) * Sec), rng.Next(1, 50)]);
        }

        return rows;
    }

    // Insert tick k's rows and delete two of tick k−3's (the deletes reach the worker the insert did;
    // after a re-shard, only if the filter's rows were re-placed there).
    private static void Tick(int tick, Action<object?[], long> push, Action<long> clock, Action step)
    {
        clock(2 * tick * Sec);
        foreach (var row in Rows(tick))
        {
            push(row, 1);
        }

        if (tick >= 3)
        {
            var old = Rows(tick - 3);
            push(old[0], -1);
            push(old[5], -1);
        }

        step();
    }

    private static void Tick(ParallelCompiledProgram p, int tick) =>
        Tick(tick, (row, w) => Push(p.Table("e"), row, w), p.AdvanceClock, p.Step);

    private static void Tick(CompiledProgram p, int tick) =>
        Tick(tick, (row, w) => Push(p.Table("e"), row, w), p.AdvanceClock, p.Step);

    private static void Push(ShardedTableInput t, object?[] row, long w)
    {
        if (w > 0)
        {
            t.Insert(row);
        }
        else
        {
            t.Delete(row);
        }
    }

    private static void Push(TableInput t, object?[] row, long w)
    {
        if (w > 0)
        {
            t.Insert(row);
        }
        else
        {
            t.Delete(row);
        }
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

    public static TheoryData<string, int> SameW() => new()
    {
        { "flat", 2 },
        { "flat", 3 },
        { "flat", 4 },
        { "spine", 2 },
        { "spine", 3 },
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
                Assert.True(Digest(reference) == Digest(producer), $"tick {tick}: parallel differs from serial");
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Compile(workers, family);
        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(10 * Sec, restored.LogicalTime);
        Assert.All(restored.Circuit.Replicas, r => Assert.Equal(10 * Sec, r.LogicalTime));
        Assert.Equal(Digest(reference), Digest(restored));

        for (var tick = 6; tick < 14; tick++)
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
        Assert.All(restored.Circuit.Replicas, r => Assert.Equal(10 * Sec, r.LogicalTime));
        Assert.Equal(Digest(reference), Digest(restored));

        for (var tick = 6; tick < 14; tick++)
        {
            Tick(reference, tick);
            Tick(restored, tick);
            Assert.True(Digest(reference) == Digest(restored), $"tick {tick}: the re-sharded run differs from serial");
        }
    }

    /// <summary>
    /// The placements the compiler states: whole-row over the table, and the aggregate's own hash
    /// in `nested`, whose outer aggregate elided its exchange.
    /// </summary>
    [Fact]
    public void TemporalFilters_StateTheirInputsPlacement()
    {
        using var p = Compile(3);
        var replica = p.Circuit.Replicas[0];
        var stateful = Enumerable.Range(0, replica.Operators.Count).Where(i => replica.Operators[i] is ISnapshotable).ToList();
        foreach (var i in stateful)
        {
            Assert.True(p.ReshardSpecs.TryGetValue(i, out var spec), $"operator {i} ({replica.Operators[i].GetType().Name}) has no spec");
            Assert.Null(spec!.Unsupported);
        }

        var filters = stateful.Where(i => replica.Operators[i] is TemporalFilterOp<DbspNet.Core.Collections.StructuralRow>).ToList();
        Assert.Equal(2, filters.Count);
        Assert.All(filters, i => Assert.IsType<ReshardPlacement.Hash>(Assert.Single(p.ReshardSpecs[i].Files).Placement));
    }

    /// <summary>The partition passes through the filter, so the aggregate above it elides its
    /// exchange: `nested` has one exchange fewer than the same view with the filter replaced by a
    /// plain filter would need if the partition were dropped.</summary>
    [Fact]
    public void FilterOverAnAggregate_LetsTheOuterAggregateElideItsExchange()
    {
        string[] sql = [Sql[0], Sql[3]];
        var views = new HashSet<string>(["nested"], StringComparer.Ordinal);
        using var p = Compile(3, sql: sql, views: views);
        var exchanges = p.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.Contains("Exchange", StringComparison.Ordinal));
        Assert.Equal(1, exchanges); // the inner aggregate's; the outer one reuses it
    }

    /// <summary>After a projection the input's placement is not a reproducible hash: the filter
    /// records why, and a re-shard refuses rather than guessing; same-W restore still works.</summary>
    [Fact]
    public async Task FilterAfterAProjection_IsNotReshardable_ButRestoresAtTheSameW()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts TIMESTAMP NOT NULL, v INT NOT NULL)",
            "CREATE VIEW shifted AS SELECT ts, v + 1 AS w FROM e",
            "CREATE VIEW recent AS SELECT ts, w FROM shifted WHERE ts > NOW() - INTERVAL '6' SECOND",
        ];
        var views = new HashSet<string>(["recent"], StringComparer.Ordinal);
        var fs = new DbspNet.Persistence.IO.InMemoryTableFileSystem();
        using (var producer = Compile(2, sql: sql, views: views))
        {
            Assert.Contains(producer.ReshardSpecs.Values, s => s.Unsupported?.Contains("temporal filter", StringComparison.Ordinal) == true);
            producer.AdvanceClock(10 * Sec);
            producer.Table("e").Insert(new Timestamp(8 * Sec), 1);
            producer.Step();
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using (var same = Compile(2, sql: sql, views: views))
        {
            await ParallelSnapshot.ReadAsync(same.Circuit, fs);
            Assert.Equal(1, same.Outputs["recent"].RowCount);
        }

        using var other = Compile(3, sql: sql, views: views);
        await Assert.ThrowsAnyAsync<Exception>(async () => await ParallelReshard.RewriteAsync(fs, other));
    }
}
