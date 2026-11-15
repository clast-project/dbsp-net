// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Temporal filters (<c>NOW()</c>) on the multi-view program path
/// (docs/design-program-temporal.md): the clock frontier is identified by the filter's window, so
/// two windows over one column never share a frontier, and it crosses view boundaries like a
/// LATENESS source, so a stateful operator downstream GCs. The incremental≡batch oracle is the
/// program arm of <c>TemporalFilterPbtTests</c>.
/// </summary>
public class ProgramTemporalFilterTests
{
    private const long Sec = 1_000_000L;

    /// <summary>
    /// The UNION ALL of a 1 s and a 5 s window, grouped. With the frontier keyed by the base column,
    /// the aggregate GC'd against the 1 s window; the 5 s branch's later retraction then met an
    /// evicted group and left two phantom rows. Single-query path: a pre-existing bug.
    /// </summary>
    [Fact]
    public void SingleQuery_UnionOfTwoWindows_RetractsCleanly()
    {
        var q = DbspNet.Tests.EndToEnd.IncrementalOracle.CompileQuery(
            ["CREATE TABLE e (ts TIMESTAMP NOT NULL)"],
            "SELECT ts, COUNT(*) AS c FROM ("
            + "SELECT ts FROM e WHERE ts > NOW() - INTERVAL '1' SECOND "
            + "UNION ALL SELECT ts FROM e WHERE ts > NOW() - INTERVAL '5' SECOND) u GROUP BY ts");
        var acc = ZSet<StructuralRow, Z64>.Empty;
        foreach (var clock in new[] { 8.5, 9.5, 13.5 })
        {
            q.AdvanceClock((long)(clock * Sec));
            if (clock == 8.5)
            {
                q.Table("e").Insert(new Timestamp(8 * Sec));
            }

            q.Step();
            acc += q.Current;
        }

        Assert.Equal(ZSet<StructuralRow, Z64>.Empty, acc);
    }

    [Fact]
    public void TwoViewsWithDifferentWindows_EachGcsAgainstItsOwn()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts TIMESTAMP NOT NULL)",
            "CREATE VIEW short AS SELECT ts, COUNT(*) AS c FROM e WHERE ts > NOW() - INTERVAL '1' SECOND GROUP BY ts",
            "CREATE VIEW long AS SELECT ts, COUNT(*) AS c FROM e WHERE ts > NOW() - INTERVAL '5' SECOND GROUP BY ts",
        ];
        var p = SqlProgram.Compile(sql, Set("short", "long"));
        p.AdvanceClock((8 * Sec) + (Sec / 2));
        p.Table("e").Insert(new Timestamp(8 * Sec));
        p.Step();
        p.AdvanceClock((9 * Sec) + (Sec / 2));
        p.Step();
        Assert.Equal(0, p.Outputs["short"].RowCount);
        Assert.Equal(1, p.Outputs["long"].RowCount);
        p.AdvanceClock((13 * Sec) + (Sec / 2));
        p.Step();
        Assert.Equal(0, p.Outputs["long"].RowCount);

        var frontiers = GcSites(p).Select(s => s.GcFrontier!.Value).Order().ToArray();
        Assert.Equal([(8 * Sec) + (Sec / 2), (12 * Sec) + (Sec / 2)], frontiers);
    }

    [Fact]
    public void GroupByInTheFilteringView_BoundsState()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts TIMESTAMP NOT NULL, v INT NOT NULL)",
            "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM e WHERE ts > NOW() - INTERVAL '10' SECOND GROUP BY ts",
        ];
        var p = SqlProgram.Compile(sql, Set("per_ts"));
        Stream(p, 200);

        var agg = Assert.Single(GcSites(p));
        Assert.Equal(190 * Sec, agg.GcFrontier);
        Assert.InRange(agg.RetainedRows, 1, 12);
        Assert.Equal(10, p.Outputs["per_ts"].RowCount);
    }

    [Theory]
    [InlineData(true)] // recent is an output: its own stream, scanned by `out` (seeded verdicts)
    [InlineData(false)] // recent is single-use: inlined into `out` (analyzed through its body)
    public void GroupByOverAFilteringView_BoundsState(bool recentIsOutput)
    {
        string[] sql =
        [
            "CREATE TABLE e (ts TIMESTAMP NOT NULL, v INT NOT NULL)",
            "CREATE VIEW recent AS SELECT ts, v FROM e WHERE ts > NOW() - INTERVAL '10' SECOND",
            "CREATE VIEW out AS SELECT ts, COUNT(*) AS c FROM recent GROUP BY ts",
        ];
        var p = SqlProgram.Compile(sql, recentIsOutput ? Set("recent", "out") : Set("out"));
        Stream(p, 200);

        var agg = Assert.Single(GcSites(p));
        Assert.Equal(190 * Sec, agg.GcFrontier);
        Assert.InRange(agg.RetainedRows, 1, 12);
        Assert.Equal(10, p.Outputs["out"].RowCount);
    }

    /// <summary>
    /// A temporal filter over an aggregate's output (a view): the window bounds the group key from
    /// below whatever is underneath, so a GROUP BY above it GCs too. (The aggregate below cannot: its
    /// input is the raw table.)
    /// </summary>
    [Fact]
    public void FilterOverAnAggregateView_BoundsTheConsumer()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts TIMESTAMP NOT NULL, v INT NOT NULL)",
            "CREATE VIEW per_ts AS SELECT ts, SUM(v) AS s FROM e GROUP BY ts",
            "CREATE VIEW recent AS SELECT ts, s FROM per_ts WHERE ts > NOW() - INTERVAL '10' SECOND",
            "CREATE VIEW out AS SELECT ts, MAX(s) AS m FROM recent GROUP BY ts",
        ];
        var p = SqlProgram.Compile(sql, Set("per_ts", "out"));
        Stream(p, 200);

        var gc = Assert.Single(GcSites(p));
        Assert.Equal(190 * Sec, gc.GcFrontier);
        Assert.InRange(gc.RetainedRows, 1, 12);
        Assert.Equal(10, p.Outputs["out"].RowCount);
        Assert.Equal(201, p.Outputs["per_ts"].RowCount);
    }

    /// <summary>The serial program equals the same query compiled alone, driven by the same clock.</summary>
    [Fact]
    public void SerialProgram_MatchesSingleQueryCompile()
    {
        string[] sql =
        [
            "CREATE TABLE e (ts TIMESTAMP NOT NULL, v INT NOT NULL)",
            "CREATE VIEW recent AS SELECT ts, v FROM e WHERE ts > NOW() - INTERVAL '3' SECOND",
            "CREATE VIEW out AS SELECT ts, SUM(v) AS s FROM recent GROUP BY ts",
        ];
        var p = SqlProgram.Compile(sql, Set("out"));
        var single = DbspNet.Tests.EndToEnd.IncrementalOracle.CompileQuery(
            [sql[0]],
            "SELECT ts, SUM(v) AS s FROM (SELECT ts, v FROM e WHERE ts > NOW() - INTERVAL '3' SECOND) r GROUP BY ts");

        long[][] ticks = [[1, 2], [], [3, 1, 6], [], [], [9, 10], []];
        var acc = ZSet<StructuralRow, Z64>.Empty;
        long clock = 0;
        foreach (var tick in ticks)
        {
            clock += 2 * Sec;
            p.AdvanceClock(clock);
            single.AdvanceClock(clock);
            foreach (var ts in tick)
            {
                p.Table("e").Insert(new Timestamp(ts * Sec), (int)ts);
                single.Table("e").Insert(new Timestamp(ts * Sec), (int)ts);
            }

            p.Step();
            single.Step();
            acc += single.Current;
            Assert.Equal(acc, p.Outputs["out"].CurrentView);
        }
    }

    private static HashSet<string> Set(params string[] names) => new(names, StringComparer.Ordinal);

    // One event per second, ts = t s, with the clock tracking the newest event.
    private static void Stream(CompiledProgram program, long last)
    {
        for (long t = 0; t <= last; t++)
        {
            program.AdvanceClock(t * Sec);
            program.Table("e").Insert(new Timestamp(t * Sec), 1);
            program.Step();
        }
    }

    // Stateful operators with a GC frontier wired, other than the temporal filter itself (which
    // reports the clock); output Integrates are never GC'd.
    private static List<OperatorStat> GcSites(CompiledProgram program) =>
        program.Circuit.CollectStats()
            .Where(s => s.GcFrontier is not null && s.Name != "TemporalFilter")
            .ToList();
}
