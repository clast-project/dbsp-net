// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// S3: spill thresholds that follow memory pressure (docs/design-bounded-memory.md §5.28). Required:
//   1. the raw level of used bytes against the limit, and Feldera's hysteresis (High holds until Low,
//      Critical until Moderate or below), as tables; the level knob parses or falls back;
//   2. Moderate writes every run to a file, below the spill threshold too (Low leaves them in memory);
//   3. High freezes a trace on one insert (Low keeps the managed level);
//   4. entering High pushes every listed state trace's managed level at the next operator boundary, once
//      per epoch, and never lists a delta's private trace (Low pushes nothing);
//   5. Critical freezes an in-step delta chunk at the floor (Low builds it managed);
//   6. every output is unchanged at each pinned level.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

[Collection(nameof(BlockedCheckpointTests))] // sets the process-wide cache budget and spiller
public class MemoryPressureTests
{
    private const long Gib = 1L << 30;

    private static readonly (double, double, double) Levels = (0.85, 0.90, 0.95);

    [Theory]
    [InlineData(0L, (int)PressureLevel.Low)]
    [InlineData(84L, (int)PressureLevel.Low)]
    [InlineData(85L, (int)PressureLevel.Moderate)]
    [InlineData(89L, (int)PressureLevel.Moderate)]
    [InlineData(90L, (int)PressureLevel.High)]
    [InlineData(95L, (int)PressureLevel.Critical)]
    [InlineData(120L, (int)PressureLevel.Critical)]
    public void The_raw_level_is_the_used_fraction_of_the_limit(long usedPercent, int expectedLevel)
    {
        var expected = (PressureLevel)expectedLevel;
        Assert.Equal(expected, MemoryPressure.Raw(usedPercent << 20, 100L << 20, Levels));
        Assert.Equal(PressureLevel.Low, MemoryPressure.Raw(Gib, 0, Levels)); // no limit: never pressure
    }

    [Theory]
    [InlineData((int)PressureLevel.Low, (int)PressureLevel.Critical, (int)PressureLevel.Critical)]
    [InlineData((int)PressureLevel.Moderate, (int)PressureLevel.Low, (int)PressureLevel.Low)]
    [InlineData((int)PressureLevel.Moderate, (int)PressureLevel.High, (int)PressureLevel.High)]
    [InlineData((int)PressureLevel.High, (int)PressureLevel.Moderate, (int)PressureLevel.High)]
    [InlineData((int)PressureLevel.High, (int)PressureLevel.Low, (int)PressureLevel.Low)]
    [InlineData((int)PressureLevel.High, (int)PressureLevel.Critical, (int)PressureLevel.Critical)]
    [InlineData((int)PressureLevel.Critical, (int)PressureLevel.High, (int)PressureLevel.Critical)]
    [InlineData((int)PressureLevel.Critical, (int)PressureLevel.Moderate, (int)PressureLevel.Moderate)]
    [InlineData((int)PressureLevel.Critical, (int)PressureLevel.Low, (int)PressureLevel.Low)]
    public void Hysteresis_is_Felderas(int previous, int raw, int expected) =>
        Assert.Equal((PressureLevel)expected, MemoryPressure.Next((PressureLevel)previous, (PressureLevel)raw));

    [Fact]
    public void The_levels_knob_parses_or_falls_back()
    {
        Assert.Equal((0.5, 0.6, 0.7), MemoryPressure.ParseLevels("50, 60,70"));
        Assert.Equal((0.85, 0.90, 0.95), MemoryPressure.ParseLevels(null));
        Assert.Equal((0.85, 0.90, 0.95), MemoryPressure.ParseLevels("90,80,95")); // out of order
        Assert.Equal((0.85, 0.90, 0.95), MemoryPressure.ParseLevels("80,90"));
    }

    private static readonly string[] Sql =
    [
        "CREATE TABLE a (k INT NOT NULL, x BIGINT)",
        "CREATE TABLE b (k INT NOT NULL, y BIGINT)",
        "CREATE TABLE c (k INT NOT NULL, x BIGINT)",
        "CREATE TABLE d (k INT NOT NULL, y BIGINT)",
        "CREATE VIEW ab AS SELECT a.k, a.x, b.y FROM a JOIN b ON a.k = b.k",
        "CREATE VIEW cd AS SELECT c.k, c.x, d.y FROM c JOIN d ON c.k = d.k",
    ];

    private static readonly HashSet<string> Views = new(["ab", "cd"], StringComparer.Ordinal);

    private static CompiledProgram Compile(int freezeEntries)
    {
        using var mode = TraceBlocksMode.Use(join: true, freezeEntries: freezeEntries, blockEntries: 64);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Tick(CompiledProgram p, int tick, bool both = true)
    {
        var rng = new Random(tick);
        foreach (var (left, right) in both ? new[] { ("a", "b"), ("c", "d") } : [("a", "b")])
        {
            for (var i = 0; i < 40; i++)
            {
                p.Table(left).Insert(rng.Next(0, 8), (long)(tick * 1000 + i));
            }

            p.Table(right).Insert(rng.Next(0, 8), (long)tick);
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p, string view) =>
        string.Join("; ", p.Outputs[view].CurrentView
            .Select(e => FormattableString.Invariant($"{e.Key[0]}|{e.Key[1]}|{e.Key[2]}#{e.Value.Value}"))
            .Order(StringComparer.Ordinal));

    private static BlockedJoinOp Join(CompiledProgram p, int which) => p.Circuit.Operators.OfType<BlockedJoinOp>().ElementAt(which);

    private static (List<string> Ab, List<string> Cd) TwinDigests(int ticks, Func<int, bool> both)
    {
        var twin = Compile(1 << 20);
        var (ab, cd) = (new List<string>(), new List<string>());
        for (var tick = 0; tick < ticks; tick++)
        {
            Tick(twin, tick, both(tick));
            ab.Add(Digest(twin, "ab"));
            cd.Add(Digest(twin, "cd"));
        }

        return (ab, cd);
    }

    private static async Task WithSpillerAsync(long minBytes, Func<RunSpiller, string, Task> body)
    {
        using var layout = StateLayout.Use(true);
        var savedBudget = BlockCache.Budget;
        var savedSpiller = BlockAccess.Spiller;
        var dir = Path.Combine(Path.GetTempPath(), "dbspnet-pressure-test-" + Guid.NewGuid().ToString("N"));
        var spiller = new RunSpiller(dir, minBytes);
        BlockCache.Budget = 1;
        BlockAccess.Spiller = spiller;
        try
        {
            await body(spiller, dir);
        }
        finally
        {
            BlockAccess.Spiller = savedSpiller;
            BlockCache.Budget = savedBudget;
            BlockCache.Clear();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Theory]
    [InlineData((int)PressureLevel.Low)]
    [InlineData((int)PressureLevel.Moderate)]
    public async Task Moderate_writes_every_run_to_a_file(int levelValue)
    {
        var level = (PressureLevel)levelValue;
        using var low = MemoryPressure.Use(PressureLevel.Low); // the setup's level, whatever DBSPNET_PRESSURE pins
        using var noStepSpill = SpillSettings.UseStepSpill(null); // deltas adopted as runs would hide the managed levels
        var expected = TwinDigests(12, _ => true);
        await WithSpillerAsync(1L << 40, (spiller, dir) =>
        {
            using var pressure = MemoryPressure.Use(level);
            var p = Compile(16);
            for (var tick = 0; tick < 12; tick++)
            {
                Tick(p, tick);
                Assert.Equal(expected.Ab[tick], Digest(p, "ab"));
                Assert.Equal(expected.Cd[tick], Digest(p, "cd"));
            }

            var runs = Join(p, 0).LeftTrace.Runs;
            Assert.NotEmpty(runs);
            if (level == PressureLevel.Moderate)
            {
                Assert.All(runs, r => Assert.NotNull(r.Persisted));
                Assert.True(spiller.Files > 0);
            }
            else
            {
                Assert.All(runs, r => Assert.Null(r.Persisted));
                Assert.Equal(0, spiller.Files);
            }

            return Task.CompletedTask;
        });
    }

    [Theory]
    [InlineData((int)PressureLevel.Low)]
    [InlineData((int)PressureLevel.High)]
    [InlineData((int)PressureLevel.Critical)]
    public void High_freezes_a_trace_on_every_insert(int levelValue)
    {
        var level = (PressureLevel)levelValue;
        using var low = MemoryPressure.Use(PressureLevel.Low); // the setup's level, whatever DBSPNET_PRESSURE pins
        using var noStepSpill = SpillSettings.UseStepSpill(null); // deltas adopted as runs would hide the managed levels
        var expected = TwinDigests(6, _ => true);
        using var pressure = MemoryPressure.Use(level);
        var p = Compile(1 << 20);
        for (var tick = 0; tick < 6; tick++)
        {
            Tick(p, tick);
            Assert.Equal(expected.Ab[tick], Digest(p, "ab"));
            Assert.Equal(expected.Cd[tick], Digest(p, "cd"));
        }

        var left = Join(p, 0).LeftTrace;
        if (level >= PressureLevel.High)
        {
            Assert.Equal(0, left.ManagedEntries);
            Assert.NotEmpty(left.Runs);
        }
        else
        {
            Assert.True(left.ManagedEntries > 0);
            Assert.Empty(left.Runs);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Entering_High_pushes_every_listed_traces_managed_level_once_per_epoch(bool high)
    {
        using var low = MemoryPressure.Use(PressureLevel.Low); // the setup's level, whatever DBSPNET_PRESSURE pins
        using var noStepSpill = SpillSettings.UseStepSpill(null); // deltas adopted as runs would hide the managed levels
        // A trace whose operator already stepped keeps its managed level until something freezes it; the
        // push is what does, at the next operator boundary (called here between ticks, on the same thread).
        var expected = TwinDigests(6, _ => true);
        var p = Compile(1 << 20);
        for (var tick = 0; tick < 4; tick++)
        {
            Tick(p, tick);
        }

        var traces = new[] { Join(p, 0).LeftTrace, Join(p, 0).RightTrace, Join(p, 1).LeftTrace, Join(p, 1).RightTrace };
        var managed = traces.Sum(t => t.ManagedEntries);
        Assert.All(traces, t => Assert.True(t.ManagedEntries > 0));
        Assert.All(traces, t => Assert.Empty(t.Runs));
        Assert.True(p.Circuit.Relief.Listed >= traces.Length);

        // The Low case sees a newer epoch too (a High that has already passed): the level gates the push.
        if (!high)
        {
            using (MemoryPressure.Use(PressureLevel.High))
            {
            }
        }

        var pushes0 = Interlocked.Read(ref PressureRelief.Pushes);
        using (MemoryPressure.Use(high ? PressureLevel.High : PressureLevel.Low))
        {
            p.Circuit.Relief.AtBoundary();
            if (high)
            {
                Assert.All(traces, t => Assert.Equal(0, t.ManagedEntries));
                Assert.All(traces, t => Assert.NotEmpty(t.Runs));
                Assert.All(traces, t => Assert.False(t.PressureRegistered));
                Assert.Equal(0, p.Circuit.Relief.Listed);
                Assert.Equal(pushes0 + 1, Interlocked.Read(ref PressureRelief.Pushes));
            }
            else
            {
                Assert.Equal(managed, traces.Sum(t => t.ManagedEntries));
                Assert.Equal(pushes0, Interlocked.Read(ref PressureRelief.Pushes));
            }

            // Once per epoch: a second boundary in the same epoch pushes nothing.
            var pushes1 = Interlocked.Read(ref PressureRelief.Pushes);
            p.Circuit.Relief.AtBoundary();
            Assert.Equal(pushes1, Interlocked.Read(ref PressureRelief.Pushes));

            for (var tick = 4; tick < 6; tick++)
            {
                Tick(p, tick);
                Assert.Equal(expected.Ab[tick], Digest(p, "ab"));
                Assert.Equal(expected.Cd[tick], Digest(p, "cd"));
            }
        }
    }

    private static readonly Schema RowSchema = new([new SchemaColumn("k", new SqlBigintType(false))]);

    private static ITraceBlockFormat RowFormat =>
        TraceBlockFormats.Create(RowSchema, Schema.Empty) ?? throw new InvalidOperationException("no block format");

    [Fact]
    public void A_deltas_private_trace_is_never_listed_but_a_state_trace_is()
    {
        using var low = MemoryPressure.Use(PressureLevel.Low); // the setup's level, whatever DBSPNET_PRESSURE pins
        using var noStepSpill = SpillSettings.UseStepSpill(null); // deltas adopted as runs would hide the managed levels
        var relief = new PressureRelief();
        var outer = PressureRelief.Current;
        PressureRelief.Current = relief;
        try
        {
            var delta = new BlockedIndexedTrace(RowFormat, freezeEntries: 1 << 20, compact: false);
            var state = new BlockedIndexedTrace(RowFormat, freezeEntries: 1 << 20);
            var rows = new ZSetBuilder<StructuralRow, Z64>();
            for (var i = 0; i < 10; i++)
            {
                rows.Add(new StructuralRow((long)i), new Z64(1));
            }

            delta.IntegrateRows(rows.Build());
            Assert.Equal(0, relief.Listed);
            state.IntegrateRows(new ZSet<StructuralRow, Z64>(new Dictionary<StructuralRow, Z64> { [new StructuralRow(1L)] = new Z64(1) }));
            Assert.Equal(1, relief.Listed);
            Assert.True(state.ManagedEntries > 0);

            relief.Push();
            Assert.Equal(0, state.ManagedEntries);
            Assert.True(delta.ManagedEntries > 0); // never listed, never pushed
            Assert.Equal(0, relief.Listed);
        }
        finally
        {
            PressureRelief.Current = outer;
        }
    }

    [Theory]
    [InlineData((int)PressureLevel.Low)]
    [InlineData((int)PressureLevel.High)]
    [InlineData((int)PressureLevel.Critical)]
    public void Critical_freezes_an_in_step_delta_chunk_at_the_floor(int levelValue)
    {
        var level = (PressureLevel)levelValue;
        using var low = MemoryPressure.Use(PressureLevel.Low); // the setup's level, whatever DBSPNET_PRESSURE pins
        using var noStepSpill = SpillSettings.UseStepSpill(null); // deltas adopted as runs would hide the managed levels
        using var pressure = MemoryPressure.Use(level);
        var rows = MemoryPressure.CriticalStepRows + 10;
        var store = DeltaSpill.Store(RowFormat, rows: 1 << 30, alwaysBlocked: false, capacity: 0);
        var managed = new ZSetBuilder<StructuralRow, Z64>();
        var builder = new ZSetBuilder<StructuralRow, Z64>(store);
        for (var i = 0; i < rows; i++)
        {
            builder.Add(new StructuralRow((long)i), new Z64(1));
            managed.Add(new StructuralRow((long)i), new Z64(1));
        }

        var built = builder.Build();
        Assert.Equal(level == PressureLevel.Critical, built.External is BlockedRows);
        Assert.Equal(rows, built.Count);
        Assert.True(built.Equals(managed.Build()));
    }
}
