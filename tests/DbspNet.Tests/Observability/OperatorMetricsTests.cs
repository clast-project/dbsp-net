// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Linq;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Observability;

/// <summary>
/// Opt-in runtime observability — <see cref="CompiledQuery.CollectStats"/> /
/// <c>RootCircuit.CollectStats</c> report per-operator state size, last-tick
/// output size, GC frontier and cumulative GC drops. The headline use is
/// watching trace state stay bounded as a LATENESS frontier advances.
/// </summary>
public class OperatorMetricsTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    [Fact]
    public void GroupBy_ReportsGroupCountAndOutput()
    {
        var q = Compile(["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");
        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, 20);
        q.Table("t").Insert(2, 5);
        q.Step();

        var agg = Assert.Single(q.CollectStats(), s => s.Name == "IncrementalAggregate");
        Assert.Equal(2, agg.RetainedRows);     // two groups (k=1, k=2)
        Assert.Equal(2, agg.LastOutputRows);   // two group rows emitted
        Assert.Null(agg.GcFrontier);           // no LATENESS → no GC frontier
        Assert.Equal(0, agg.GcDroppedTotal);
        Assert.True(q.Circuit.LastStepDuration >= TimeSpan.Zero);
    }

    [Fact]
    public void LatenessGroupBy_StateStaysBoundedAndGcCounted()
    {
        var q = Compile(["CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)"],
            "SELECT ts, COUNT(*) FROM events GROUP BY ts");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("events").Insert(t, 1);
            q.Step();
        }

        var stats = q.CollectStats();
        var agg = Assert.Single(stats, s => s.Name == "IncrementalAggregate");

        // 201 distinct group keys streamed; the frontier (max − 10 = 190) keeps
        // only the trailing window [190, 200] = 11 groups. The other 190 were GC'd.
        Assert.Equal(11, agg.RetainedRows);
        Assert.Equal(190L, agg.GcFrontier);
        Assert.Equal(190, agg.GcDroppedTotal);

        // The input-side LATENESS operator advertises the same watermark.
        var lateness = Assert.Single(stats, s => s.Name == "Lateness");
        Assert.Equal(190L, lateness.GcFrontier);
    }

    [Fact]
    public void WindowAggregate_AppearsInStats()
    {
        var q = Compile(["CREATE TABLE s (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, v, SUM(v) OVER (PARTITION BY g) AS total FROM s");
        q.Table("s").Insert(1, 10);
        q.Table("s").Insert(1, 20);
        q.Table("s").Insert(2, 5);
        q.Step();

        var win = Assert.Single(q.CollectStats(), s => s.Name == "WindowAggregate");
        Assert.Equal(3, win.RetainedRows);   // three input rows retained
        Assert.Equal(3, win.LastOutputRows); // three widened rows emitted
    }

    [Fact]
    public void StatsAreIndexedAndStringFormatted()
    {
        var q = Compile(["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");
        q.Table("t").Insert(1, 10);
        q.Step();

        var stats = q.CollectStats();
        Assert.NotEmpty(stats);
        // Indices are the operators' registration positions (ascending, distinct).
        Assert.Equal(stats.Select(s => s.Index).OrderBy(i => i), stats.Select(s => s.Index));
        Assert.Contains("IncrementalAggregate", stats.Single().ToString());
    }
}
