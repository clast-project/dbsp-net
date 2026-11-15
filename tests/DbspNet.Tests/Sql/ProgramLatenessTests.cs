// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// LATENESS on the multi-view program path (issue #3): late rows are dropped at the shared table
/// input, and frontiers cross view boundaries, so a stateful operator in a view that scans another
/// view GCs exactly as it would with that view written inline. The parallel program path enforces
/// LATENESS against a frontier agreed by all workers (docs/design-parallel-frontier.md).
/// The incremental≡batch oracle is <c>ProgramLatenessPbtTests</c>.
/// </summary>
public class ProgramLatenessTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
        "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM e GROUP BY ts",
    ];

    private static readonly HashSet<string> Views = new(["per_ts"], StringComparer.Ordinal);

    [Fact]
    public void SerialProgram_DropsLateRows()
    {
        var program = SqlProgram.Compile(Sql, Views);
        program.Table("e").Insert(100L, 1);
        program.Step();

        // 10 is more than 5 below the max seen: dropped as late, as on the single-query path.
        program.Table("e").Insert(10L, 1);
        program.Step();
        Assert.Equal(1, program.Outputs["per_ts"].RowCount);

        // 96 is within the bound: admitted.
        program.Table("e").Insert(96L, 1);
        program.Step();
        Assert.Equal(2, program.Outputs["per_ts"].RowCount);
    }

    [Fact]
    public void SerialProgram_MatchesSingleQueryCompile()
    {
        var program = SqlProgram.Compile(Sql, Views);
        var single = DbspNet.Tests.EndToEnd.IncrementalOracle.CompileQuery(
            [Sql[0]], "SELECT ts, SUM(v) AS s FROM e GROUP BY ts");

        long[][] ticks = [[100, 3], [10, 101, 95], [94, 96, 200], [150, 196, 199]];
        var accum = DbspNet.Core.Collections.ZSet<DbspNet.Core.Collections.StructuralRow, DbspNet.Core.Algebra.Z64>.Empty;
        foreach (var tick in ticks)
        {
            foreach (var ts in tick)
            {
                program.Table("e").Insert(ts, 1);
                single.Table("e").Insert(ts, 1);
            }

            program.Step();
            single.Step();
            accum += single.Current;
        }

        Assert.Equal(accum, program.Outputs["per_ts"].CurrentView);
    }

    [Fact]
    public void GroupByInDirectView_BoundsState()
    {
        var program = SqlProgram.Compile(Sql, Views);
        Stream(program, "e", 200);

        var agg = Assert.Single(GcSites(program));
        Assert.Equal(195, agg.GcFrontier);
        Assert.InRange(agg.RetainedRows, 1, 6);
    }

    [Theory]
    [InlineData(true)] // mid is an output: compiled to its own stream, scanned by `out` (seeded verdicts)
    [InlineData(false)] // mid is single-use: inlined into `out` (analyzed through its body)
    public void GroupByOverAnotherView_BoundsState(bool midIsOutput)
    {
        string[] sql =
        [
            "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
            "CREATE VIEW mid AS SELECT ts + 2 AS t2, v FROM e WHERE v > 0",
            "CREATE VIEW out AS SELECT t2, COUNT(*) AS c FROM mid GROUP BY t2",
        ];
        var program = SqlProgram.Compile(sql, midIsOutput ? new HashSet<string>(["mid", "out"]) : new HashSet<string>(["out"]));
        Stream(program, "e", 200);

        var agg = Assert.Single(GcSites(program));
        Assert.Equal(195, agg.GcFrontier);
        Assert.InRange(agg.RetainedRows, 1, 8);
    }

    [Fact]
    public void JoinOfAggregateViewWithTable_BoundsBothSides()
    {
        string[] sql =
        [
            "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
            "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 5, w INT NOT NULL)",
            "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
            "CREATE VIEW joined AS SELECT p.ts, p.s, b.w FROM per_ts p JOIN b ON p.ts = b.ts",
        ];

        // per_ts is an output too, so the join reads its stream across a view boundary.
        var program = SqlProgram.Compile(sql, new HashSet<string>(["per_ts", "joined"]));
        for (long t = 0; t <= 200; t++)
        {
            program.Table("a").Insert(t, 1);
            program.Table("b").Insert(t, 1);
            program.Step();
        }

        var sites = GcSites(program);
        Assert.Contains(sites, s => s.Name.Contains("Join", StringComparison.Ordinal));
        Assert.Contains(sites, s => s.Name.Contains("Aggregate", StringComparison.Ordinal));
        Assert.All(sites, s => Assert.InRange(s.RetainedRows, 0, 16));
        Assert.Equal(201, program.Outputs["joined"].RowCount);
    }

    [Fact]
    public void NonMonotoneConsumer_DoesNotGc()
    {
        // Grouping on v: the view's output is not monotone on its key, so no GC is wired there.
        string[] sql =
        [
            "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
            "CREATE VIEW mid AS SELECT ts, v FROM e",
            "CREATE VIEW out AS SELECT v, COUNT(*) AS c FROM mid GROUP BY v",
        ];
        var program = SqlProgram.Compile(sql, new HashSet<string>(["mid", "out"]));
        Stream(program, "e", 50);
        Assert.Empty(GcSites(program));
    }

    [Fact]
    public void ParallelProgram_CompilesLateness_WithNoBlocker()
    {
        Assert.Empty(SqlProgram.ParallelBlockers(Sql, Views));
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 2, out var compiled));
        compiled!.Dispose();
    }

    [Fact]
    public void ParallelProgram_DropsLateRowsAgainstTheGlobalMax()
    {
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 4, out var p));
        using (p)
        {
            p!.Table("e").Insert(100L, 1);
            p.Step();

            // Whichever shard 10 and 96 land on, the frontier they meet is the global one (95).
            p.Table("e").Insert(10L, 1);
            p.Table("e").Insert(96L, 1);
            p.Step();
            Assert.Equal(2, p.Outputs["per_ts"].RowCount);
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void ParallelProgram_GcFiresOnEveryWorker(int workers)
    {
        string[] sql =
        [
            "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
            "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 5, w INT NOT NULL)",
            "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
            "CREATE VIEW joined AS SELECT p.ts, p.s, b.w FROM per_ts p JOIN b ON p.ts = b.ts",
        ];
        Assert.True(SqlProgram.TryCompileParallel(sql, new HashSet<string>(["per_ts", "joined"]), workers, out var p));
        using (p)
        {
            for (long t = 0; t <= 200; t++)
            {
                p!.Table("a").Insert(t, 1);
                p.Table("b").Insert(t, 1);
                p.Step();
            }

            Assert.Equal(201, p!.Outputs["joined"].RowCount);
            foreach (var replica in p.Circuit.Replicas)
            {
                var sites = replica.CollectStats()
                    .Where(s => s.GcFrontier is not null && s.Name != "Lateness")
                    .ToList();
                Assert.Contains(sites, s => s.Name.Contains("Join", StringComparison.Ordinal));
                Assert.Contains(sites, s => s.Name.Contains("Aggregate", StringComparison.Ordinal));
                Assert.All(sites, s => Assert.Equal(195, s.GcFrontier));
                Assert.All(sites, s => Assert.InRange(s.RetainedRows, 0, 16));
            }
        }
    }

    [Fact]
    public void ProgramWithoutLateness_StillCompilesInParallel()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts BIGINT NOT NULL, v INT NOT NULL)",
            "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM e GROUP BY ts",
        ];
        Assert.True(SqlProgram.TryCompileParallel(sql, Views, 2, out var compiled));
        compiled!.Dispose();
    }

    private static void Stream(CompiledProgram program, string table, long last)
    {
        for (long t = 0; t <= last; t++)
        {
            program.Table(table).Insert(t, 1);
            program.Step();
        }
    }

    // Stateful operators with a GC frontier wired. The input-side LatenessOperator reports its
    // watermark too, so it is excluded; output Integrates are never GC'd.
    private static List<OperatorStat> GcSites(CompiledProgram program) =>
        program.Circuit.CollectStats()
            .Where(s => s.GcFrontier is not null && s.Name != "Lateness")
            .ToList();
}
