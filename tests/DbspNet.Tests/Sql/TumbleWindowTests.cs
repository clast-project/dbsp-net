// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Event-time tumbling windows — the <c>GROUP BY TUMBLE(t, size)</c> /
/// <c>TUMBLE_START</c> / <c>TUMBLE_END</c> surface (Feldera's Nexmark q7/q8/q12
/// form). Tumbling windows lower to the internal monotone <c>tumble_start</c>
/// scalar (a fixed-bucket floor) so they ride the existing GROUP BY-expression-key,
/// monotonicity-GC, and aggregate machinery — no new plan node or operator. These
/// tests cover the scalar value, the q7 (max-per-window) and q8 (windowed join)
/// shapes, and the LATENESS-driven trace GC (a window is dropped only once the
/// watermark passes <c>start + size</c>).
/// </summary>
public class TumbleWindowTests
{
    private const long Sec = 1_000_000L;
    private const long Day = 86_400_000_000L;
    private const long Hour = 3_600_000_000L;

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

    private static int RetainedGroups(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single()
            .RetainedGroupCount;

    [Fact]
    public void TumbleStart_FloorsToWindowBucket()
    {
        // A bare projection: TUMBLE_START(t, 10s) = floor(t / 10s) * 10s.
        var q = Compile(
            ["CREATE TABLE bid (price BIGINT NOT NULL, date_time TIMESTAMP NOT NULL)"],
            "SELECT TUMBLE_START(date_time, INTERVAL '10' SECOND) AS ws FROM bid");

        q.Table("bid").Insert(100L, new Timestamp(17 * Sec)); // window [10s, 20s)
        q.Table("bid").Insert(100L, new Timestamp(10 * Sec)); // exact boundary → 10s
        q.Table("bid").Insert(100L, new Timestamp(3 * Sec));  // window [0s, 10s)
        q.Step();

        Assert.Equal(2, q.WeightOf(new Timestamp(10 * Sec)).Value); // 17s and 10s both floor to 10s
        Assert.Equal(1, q.WeightOf(new Timestamp(0L)).Value);       // 3s floors to 0s
    }

    [Fact]
    public void TumbleStart_OverDate_WholeDayBucket()
    {
        // A weekly (7-day) tumble over a DATE column: floor(dayNumber / 7) * 7.
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT TUMBLE_START(d, INTERVAL '7' DAY) AS ws FROM t");

        q.Table("t").Insert(new Date32(10)); // 10/7 = 1 → day 7
        q.Table("t").Insert(new Date32(13)); // 13/7 = 1 → day 7
        q.Table("t").Insert(new Date32(20)); // 20/7 = 2 → day 14
        q.Step();

        Assert.Equal(2, q.WeightOf(new Date32(7)).Value);
        Assert.Equal(1, q.WeightOf(new Date32(14)).Value);
    }

    [Fact]
    public void Q7Shape_MaxPricePerTumblingWindow()
    {
        // q7: highest bid per 10s window, with the window start AND end projected.
        // GROUP BY TUMBLE matches SELECT TUMBLE_START / TUMBLE_END via AstEqual.
        var q = Compile(
            ["CREATE TABLE bid (price BIGINT NOT NULL, date_time TIMESTAMP NOT NULL)"],
            @"SELECT MAX(price) AS maxprice,
                     TUMBLE_START(date_time, INTERVAL '10' SECOND) AS wstart,
                     TUMBLE_END(date_time, INTERVAL '10' SECOND) AS wend
              FROM bid
              GROUP BY TUMBLE(date_time, INTERVAL '10' SECOND)");

        q.Table("bid").Insert(100L, new Timestamp(3 * Sec));  // [0,10)
        q.Table("bid").Insert(250L, new Timestamp(7 * Sec));  // [0,10)
        q.Table("bid").Insert(50L, new Timestamp(12 * Sec));  // [10,20)
        q.Table("bid").Insert(400L, new Timestamp(25 * Sec)); // [20,30)
        q.Step();

        Assert.Equal(1, q.WeightOf(250L, new Timestamp(0L), new Timestamp(10 * Sec)).Value);
        Assert.Equal(1, q.WeightOf(50L, new Timestamp(10 * Sec), new Timestamp(20 * Sec)).Value);
        Assert.Equal(1, q.WeightOf(400L, new Timestamp(20 * Sec), new Timestamp(30 * Sec)).Value);
    }

    [Fact]
    public void Q7Shape_RetractionUpdatesWindowMax()
    {
        // A higher bid lands in window [0,10), then is retracted — the window max
        // must fall back, proving the windowed aggregate is incremental.
        var q = Compile(
            ["CREATE TABLE bid (price BIGINT NOT NULL, date_time TIMESTAMP NOT NULL)"],
            @"SELECT MAX(price) AS maxprice,
                     TUMBLE_START(date_time, INTERVAL '10' SECOND) AS wstart
              FROM bid
              GROUP BY TUMBLE(date_time, INTERVAL '10' SECOND)");

        q.Table("bid").Insert(100L, new Timestamp(3 * Sec));
        q.Table("bid").Insert(250L, new Timestamp(7 * Sec));
        q.Step();
        Assert.Equal(1, q.WeightOf(250L, new Timestamp(0L)).Value);

        q.Table("bid").Delete(250L, new Timestamp(7 * Sec));
        q.Step();
        // This step's delta retracts the old window max and emits the new one.
        Assert.Equal(-1, q.WeightOf(250L, new Timestamp(0L)).Value); // old max retracted
        Assert.Equal(1, q.WeightOf(100L, new Timestamp(0L)).Value);  // fell back to 100
    }

    [Fact]
    public void Q8Shape_WindowedJoinOnWindowBounds()
    {
        // q8: person ⋈ auction within the same 10s tumbling window. The join keys
        // include the window start/end, so only same-window matches survive.
        var q = Compile(
            [
                "CREATE TABLE person (id BIGINT NOT NULL, date_time TIMESTAMP NOT NULL)",
                "CREATE TABLE auction (seller BIGINT NOT NULL, date_time TIMESTAMP NOT NULL)",
            ],
            @"SELECT P.id, P.starttime
              FROM (
                SELECT P.id,
                       TUMBLE_START(P.date_time, INTERVAL '10' SECOND) AS starttime
                FROM person P
                GROUP BY P.id, TUMBLE(P.date_time, INTERVAL '10' SECOND)
              ) P
              JOIN (
                SELECT A.seller,
                       TUMBLE_START(A.date_time, INTERVAL '10' SECOND) AS starttime
                FROM auction A
                GROUP BY A.seller, TUMBLE(A.date_time, INTERVAL '10' SECOND)
              ) A
              ON P.id = A.seller AND P.starttime = A.starttime");

        q.Table("person").Insert(1L, new Timestamp(3 * Sec));    // window [0,10)
        q.Table("auction").Insert(1L, new Timestamp(8 * Sec));   // window [0,10) → match
        q.Table("auction").Insert(1L, new Timestamp(15 * Sec));  // window [10,20) → no match
        q.Step();

        Assert.Equal(1, q.WeightOf(1L, new Timestamp(0L)).Value);
        Assert.Equal(0, q.WeightOf(1L, new Timestamp(10 * Sec)).Value);
    }

    [Fact]
    public void GroupByTumble_BoundsStateUnderLateness()
    {
        // A 1-day tumble over a LATENESS=2-day column: identical to date_trunc('day')
        // — the bucket-floor frontier transform keeps the trailing window set bounded.
        var q = Compile(
            ["CREATE TABLE events (ts TIMESTAMP NOT NULL LATENESS 172800000000, v INT NOT NULL)"],
            @"SELECT TUMBLE_START(ts, INTERVAL '1' DAY) AS ws, COUNT(*) AS c
              FROM events
              GROUP BY TUMBLE(ts, INTERVAL '1' DAY)");

        for (long i = 0; i <= 20; i++)
        {
            q.Table("events").Insert(new Timestamp(i * Day + 12 * Hour), 1); // noon each day
            q.Step();
        }

        // Retained windows = {18, 19, 20} (trailing 2-day window + current day).
        Assert.Equal(3, RetainedGroups(q));
        Assert.Equal(1, q.WeightOf(new Timestamp(20 * Day), 1L).Value); // window day 20, count 1
    }

    [Fact]
    public void GroupByTumble_InWindowRowUpdatesRetainedWindow_NotResurrected()
    {
        // Soundness witness: after the watermark advances, a fresh row still inside
        // a retained window must UPDATE it (count → 2), proving it was kept in the
        // trace — an unsound (raw-frontier) GC would have collected and recreated it.
        var q = Compile(
            ["CREATE TABLE events (ts TIMESTAMP NOT NULL LATENESS 172800000000, v INT NOT NULL)"],
            @"SELECT TUMBLE_START(ts, INTERVAL '1' DAY) AS ws, COUNT(*) AS c
              FROM events
              GROUP BY TUMBLE(ts, INTERVAL '1' DAY)");

        for (long i = 0; i <= 20; i++)
        {
            q.Table("events").Insert(new Timestamp(i * Day + 12 * Hour), 1);
            q.Step();
        }

        // 1pm day 18 ≥ frontier (noon day 18) → admitted; its window (day 18) is retained.
        q.Table("events").Insert(new Timestamp(18 * Day + 13 * Hour), 1);
        q.Step();

        Assert.Equal(3, RetainedGroups(q)); // still bounded, no resurrected window
        Assert.Equal(1, q.WeightOf(new Timestamp(18 * Day), 2L).Value); // day-18 window now count 2
    }
}
