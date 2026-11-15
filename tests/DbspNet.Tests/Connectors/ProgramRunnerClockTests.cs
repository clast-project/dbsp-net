// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Connectors.Abstractions;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// The program runner drives the logical clock that <c>NOW()</c> reads
/// (docs/design-program-temporal.md §2 Q1): sampled once per step at the clock's resolution,
/// held (never moved back) when the time source steps backward, advanced by a clock-only tick at
/// the end of a batch, and journaled so replay reproduces the run.
/// </summary>
public class ProgramRunnerClockTests
{
    private const long Sec = 1_000_000L;

    private static readonly string[] PastSql =
    [
        "CREATE TABLE e (id INT NOT NULL, ts TIMESTAMP NOT NULL)",
        "CREATE VIEW past AS SELECT id, ts FROM e WHERE ts <= NOW()",
    ];

    private static readonly string[] RecentSql =
    [
        "CREATE TABLE e (id INT NOT NULL, ts TIMESTAMP NOT NULL)",
        "CREATE VIEW recent AS SELECT id, ts FROM e WHERE ts > NOW() - INTERVAL '10' SECOND",
    ];

    private static readonly HashSet<string> Recent = new(["recent"], StringComparer.Ordinal);

    private static SqlSchema E() =>
        new([
            new SchemaColumn("id", new SqlIntegerType(false)),
            new SchemaColumn("ts", new SqlTimestampType(false)),
        ]);

    /// <summary>A time source the test sets.</summary>
    private sealed class ManualTime(long micros) : TimeProvider
    {
        public long Micros { get; set; } = micros;

        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(Micros * 10);
    }

    private static List<(object?[] Row, long Weight)> Rows(params (int Id, long TsSec)[] rows) =>
        rows.Select(r => (new object?[] { r.Id, new Timestamp(r.TsSec * Sec) }, 1L)).ToList();

    private static FakeOutputConnector Out(string view) => new(view, OutputMode.Truncate);

    [Fact]
    public async Task DefaultRunner_NowView_SeesTheWallClock()
    {
        // 2020-01-01: in the past by any wall clock, so `ts <= NOW()` admits it, unless the
        // clock was never advanced, in which case NOW() is logical −∞ and nothing is admitted.
        var past = new Timestamp(1_577_836_800_000_000L);
        var program = SqlProgram.Compile(PastSql, new HashSet<string>(["past"], StringComparer.Ordinal));
        var input = new FakeInputConnector("e", E(), [[([1, past], 1L)]]);
        var runner = await ProgramRunner.CreateAsync(program, [input], [Out("past")]);

        await runner.RunBatchAsync();

        Assert.True(program.UsesClock);
        Assert.Equal(1, program.Outputs["past"].RowCount);
    }

    [Fact]
    public async Task IdleBatch_TakesAClockOnlyTick_ThatRetractsAgedOutRows()
    {
        var time = new ManualTime(100 * Sec);
        var program = SqlProgram.Compile(RecentSql, Recent);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>> { Rows((1, 95), (2, 99)) };
        var runner = await ProgramRunner.CreateAsync(
            program, [new FakeInputConnector("e", E(), versions)], [Out("recent")], clock: new ProgramClock(time));

        Assert.Equal(1, await runner.RunBatchAsync());
        Assert.Equal(100 * Sec, program.LogicalTime);
        Assert.Equal(2, program.Outputs["recent"].RowCount);

        // No new input; time passes row 1's window (95 + 10 = 105) but not row 2's.
        time.Micros = 106 * Sec;
        Assert.Equal(1, await runner.RunBatchAsync());
        Assert.Equal(1, program.Outputs["recent"].RowCount);

        // Time did not move: no tick at all.
        Assert.Equal(0, await runner.RunBatchAsync());

        time.Micros = 200 * Sec;
        Assert.Equal(1, await runner.RunBatchAsync());
        Assert.Equal(0, program.Outputs["recent"].RowCount);
    }

    [Fact]
    public async Task Resolution_RoundsTheClockDown()
    {
        var time = new ManualTime((100 * Sec) + 999_999);
        var program = SqlProgram.Compile(RecentSql, Recent);
        var runner = await ProgramRunner.CreateAsync(
            program, [new FakeInputConnector("e", E(), [Rows((1, 95))])], [Out("recent")], clock: new ProgramClock(time));
        await runner.RunBatchAsync();
        Assert.Equal(100 * Sec, program.LogicalTime);
    }

    [Fact]
    public async Task ProgramWithoutNow_IsNeverClocked()
    {
        string[] sql =
        [
            "CREATE TABLE e (id INT NOT NULL, ts TIMESTAMP NOT NULL)",
            "CREATE VIEW all_rows AS SELECT id, ts FROM e",
        ];
        var time = new ManualTime(100 * Sec);
        var program = SqlProgram.Compile(sql, new HashSet<string>(["all_rows"], StringComparer.Ordinal));
        var runner = await ProgramRunner.CreateAsync(
            program, [new FakeInputConnector("e", E(), [Rows((1, 95))])], [Out("all_rows")], clock: new ProgramClock(time));

        Assert.False(program.UsesClock);
        Assert.Equal(1, await runner.RunBatchAsync());
        time.Micros = 500 * Sec;
        Assert.Equal(0, await runner.RunBatchAsync()); // no clock-only tick
        Assert.Equal(long.MinValue, program.LogicalTime);
    }

    [Fact]
    public async Task TimeGoingBackward_HoldsTheClock()
    {
        var time = new ManualTime(100 * Sec);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>> { Rows((1, 95)) };
        var program = SqlProgram.Compile(RecentSql, Recent);
        var runner = await ProgramRunner.CreateAsync(
            program, [new FakeInputConnector("e", E(), versions)], [Out("recent")], clock: new ProgramClock(time));
        await runner.RunBatchAsync();

        time.Micros = 50 * Sec;
        versions.Add(Rows((2, 98)));
        Assert.Equal(1, await runner.RunBatchAsync()); // the data tick; no clock-only tick
        Assert.Equal(100 * Sec, program.LogicalTime);
        Assert.Equal(2, program.Outputs["recent"].RowCount);
    }

    [Fact]
    public async Task ParallelRunner_DrivesTheClockOnEveryReplica()
    {
        var time = new ManualTime(100 * Sec);
        Assert.True(SqlProgram.TryCompileParallel(RecentSql, Recent, 3, out var program));
        using var p = program!;
        Assert.True(p.UsesClock);
        var runner = await ProgramRunner.CreateAsync(
            p, [new FakeInputConnector("e", E(), [Rows((1, 95), (2, 99), (3, 97))])], [Out("recent")],
            clock: new ProgramClock(time));
        await runner.RunBatchAsync();
        Assert.Equal(3, p.Outputs["recent"].RowCount);

        time.Micros = 108 * Sec; // row 1 (95) and row 3 (97) age out
        await runner.RunBatchAsync();
        Assert.Equal(1, p.Outputs["recent"].RowCount);
        Assert.All(p.Circuit.Replicas, r => Assert.Equal(108 * Sec, r.LogicalTime));
    }

    private static Dictionary<(int, long), long> Contents(ZSet<StructuralRow, Z64> view) =>
        view.ToDictionary(kv => ((int)kv.Key[0]!, ((Timestamp)kv.Key[1]!).Microseconds), kv => kv.Value.Value);

    /// <summary>
    /// A journal-only run with clock-only ticks between data ticks; a fresh runner replays the
    /// journal from tick 0 at a much later wall time and must land on the original clock, tick and
    /// view: the journaled clock, not the wall clock, drives replay.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task JournalReplay_UsesTheJournaledClock(bool snapshotFirstBatch)
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var policy = snapshotFirstBatch
            ? new CheckpointPolicy { Journal = true, SnapshotEveryBatches = 100 }
            : new CheckpointPolicy { Journal = true };
        var time = new ManualTime(100 * Sec);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>> { Rows((1, 95), (2, 99)) };
        var p1 = SqlProgram.Compile(RecentSql, Recent, ArrowSqlSnapshotCodecs.Instance);
        var r1 = await ProgramRunner.CreateAsync(
            p1, [new FakeInputConnector("e", E(), versions)], [Out("recent")], store, policy, new ProgramClock(time));
        await r1.RunBatchAsync();
        if (snapshotFirstBatch)
        {
            await r1.CheckpointAsync();
        }

        time.Micros = 106 * Sec; // clock-only: row 1 ages out
        await r1.RunBatchAsync();
        time.Micros = 107 * Sec;
        versions.Add(Rows((3, 104)));
        await r1.RunBatchAsync(); // a data tick at 107 (no clock-only tick: time did not move after it)
        var expected = Contents(p1.Outputs["recent"].CurrentView);
        Assert.Equal([2, 3], expected.Keys.Select(k => k.Item1).Order());

        time.Micros = 10_000 * Sec; // replay must not see this
        var p2 = SqlProgram.Compile(RecentSql, Recent, ArrowSqlSnapshotCodecs.Instance);
        var r2 = await ProgramRunner.CreateAsync(
            p2, [new FakeInputConnector("e", E(), versions)], [Out("recent")], store, policy, new ProgramClock(time));
        Assert.Equal(p1.Circuit.TickCount, await r2.RestoreAsync());
        Assert.Equal(107 * Sec, p2.LogicalTime);
        Assert.Equal(expected, Contents(p2.Outputs["recent"].CurrentView));

        // And it continues on the wall clock: everything ages out.
        await r2.RunBatchAsync();
        Assert.Equal(0, p2.Outputs["recent"].RowCount);
    }

    [Fact]
    public async Task RestoreWithTimeBehindTheSnapshot_HoldsTheClock()
    {
        var fs = new InMemoryTableFileSystem();
        var store = new SnapshotCheckpointStore(fs);
        var time = new ManualTime(100 * Sec);
        var versions = new List<IReadOnlyList<(object?[] Row, long Weight)>> { Rows((1, 95)) };
        var p1 = SqlProgram.Compile(RecentSql, Recent, ArrowSqlSnapshotCodecs.Instance);
        var r1 = await ProgramRunner.CreateAsync(
            p1, [new FakeInputConnector("e", E(), versions)], [Out("recent")], store, clock: new ProgramClock(time));
        await r1.RunBatchAsync(); // snapshots (EveryBatch)

        time.Micros = 10 * Sec; // a host whose clock is far behind
        versions.Add(Rows((2, 99)));
        var p2 = SqlProgram.Compile(RecentSql, Recent, ArrowSqlSnapshotCodecs.Instance);
        var r2 = await ProgramRunner.CreateAsync(
            p2, [new FakeInputConnector("e", E(), versions)], [Out("recent")], store, clock: new ProgramClock(time));
        await r2.RestoreAsync();
        Assert.Equal(100 * Sec, p2.LogicalTime);
        await r2.RunBatchAsync();
        Assert.Equal(100 * Sec, p2.LogicalTime);
        Assert.Equal(2, p2.Outputs["recent"].RowCount);
    }
}
