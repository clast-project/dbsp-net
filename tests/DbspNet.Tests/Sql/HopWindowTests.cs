// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Sliding (HOP) event-time windows via the <c>TABLE(HOP(TABLE t,
/// DESCRIPTOR(col), slide, size))</c> table-valued function (Feldera's Nexmark q5
/// form). HOP lowers to a <c>UNION ALL</c> of <c>size/slide</c> shifted
/// projections — each row fans out to every overlapping window it belongs to,
/// exposing <c>window_start</c> / <c>window_end</c> columns — so it rides existing
/// plan nodes with no new operator.
/// </summary>
public class HopWindowTests
{
    private const long Sec = 1_000_000L;

    private static readonly string[] Ddl =
    {
        "CREATE TABLE bid (auction BIGINT NOT NULL, price BIGINT NOT NULL, date_time TIMESTAMP NOT NULL)",
    };

    private static CompiledQuery Compile(string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static List<object?[]> PositiveRows(CompiledQuery q, int ncols)
    {
        var rows = new List<object?[]>();
        foreach (var (row, w) in q.Current)
        {
            if (w.Value <= 0)
            {
                continue;
            }

            var cols = new object?[ncols];
            for (var i = 0; i < ncols; i++)
            {
                cols[i] = row[i];
            }

            rows.Add(cols);
        }

        return rows;
    }

    [Fact]
    public void Hop_FansRowOutToOverlappingWindows()
    {
        // slide=2s, size=10s → 5 windows per row. A bid at t=8s belongs to windows
        // starting at {0,2,4,6,8}s (each [start, start+10s) contains 8s).
        var q = Compile(
            @"SELECT window_start, window_end, COUNT(*) AS num
              FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
              GROUP BY window_start, window_end");

        q.Table("bid").Insert(1L, 100L, new Timestamp(8 * Sec));
        q.Step();

        var rows = PositiveRows(q, 3);
        Assert.Equal(5, rows.Count);
        var starts = rows.Select(r => ((Timestamp)r[0]!).Microseconds / Sec).OrderBy(s => s).ToList();
        Assert.Equal(new long[] { 0, 2, 4, 6, 8 }, starts);
        // window_end = start + 10s, and every window has the single bid.
        Assert.All(rows, r => Assert.Equal(
            ((Timestamp)r[0]!).Microseconds + 10 * Sec, ((Timestamp)r[1]!).Microseconds));
        Assert.All(rows, r => Assert.Equal(1L, r[2]));
    }

    [Fact]
    public void Hop_OverlappingBidsAccumulatePerWindow()
    {
        // Two bids at 8s and 12s (slide=2s, size=10s). Window [4s,14s) contains
        // both → count 2; window [0s,10s) contains only 8s → count 1; window
        // [10s,20s) contains only 12s → count 1.
        var q = Compile(
            @"SELECT window_start, COUNT(*) AS num
              FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
              GROUP BY window_start, window_end");

        q.Table("bid").Insert(1L, 100L, new Timestamp(8 * Sec));
        q.Table("bid").Insert(1L, 200L, new Timestamp(12 * Sec));
        q.Step();

        var byStart = PositiveRows(q, 2)
            .ToDictionary(r => ((Timestamp)r[0]!).Microseconds / Sec, r => (long)r[1]!);
        Assert.Equal(2L, byStart[4]);  // [4,14) holds 8s and 12s
        Assert.Equal(1L, byStart[0]);  // [0,10) holds only 8s
        Assert.Equal(1L, byStart[10]); // [10,20) holds only 12s
    }

    [Fact]
    public void Hop_RetractionUpdatesWindowCounts()
    {
        var q = Compile(
            @"SELECT window_start, COUNT(*) AS num
              FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
              GROUP BY window_start, window_end");

        q.Table("bid").Insert(1L, 100L, new Timestamp(8 * Sec));
        q.Table("bid").Insert(1L, 200L, new Timestamp(12 * Sec));
        q.Step();
        q.Table("bid").Delete(1L, 200L, new Timestamp(12 * Sec));
        q.Step();

        // After the delete, window [4,14)'s count falls 2 → 1 (this step's delta).
        Assert.Equal(-1, q.WeightOf(new Timestamp(4 * Sec), 2L).Value);
        Assert.Equal(1, q.WeightOf(new Timestamp(4 * Sec), 1L).Value);
    }

    [Fact]
    public void Q5Shape_HotItems_SlidingWindowPopularity()
    {
        // q5: the auction(s) with the most bids in each sliding window. Auction 1
        // gets 2 bids in window [0,10), auction 2 gets 1 → auction 1 is the hot item.
        var q = Compile(
            @"SELECT AuctionBids.auction, AuctionBids.num
              FROM (
                SELECT B1.auction, COUNT(*) AS num, window_start AS starttime, window_end AS endtime
                FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) AS B1
                GROUP BY B1.auction, window_start, window_end
              ) AS AuctionBids
              JOIN (
                SELECT MAX(CountBids.num) AS maxn, CountBids.starttime, CountBids.endtime
                FROM (
                  SELECT COUNT(*) AS num, window_start AS starttime, window_end AS endtime
                  FROM TABLE(HOP(TABLE bid, DESCRIPTOR(date_time), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) AS B2
                  GROUP BY B2.auction, window_start, window_end
                ) AS CountBids
                GROUP BY CountBids.starttime, CountBids.endtime
              ) AS MaxBids
              ON AuctionBids.starttime = MaxBids.starttime
                AND AuctionBids.endtime = MaxBids.endtime
                AND AuctionBids.num >= MaxBids.maxn");

        q.Table("bid").Insert(1L, 100L, new Timestamp(3 * Sec));
        q.Table("bid").Insert(1L, 110L, new Timestamp(5 * Sec));
        q.Table("bid").Insert(2L, 200L, new Timestamp(7 * Sec));
        q.Step();

        // In the window [0,10) (and its overlaps), auction 1 has 2 bids — the max —
        // so it is reported as a hot item with num=2.
        var rows = PositiveRows(q, 2);
        Assert.Contains(rows, r => (long)r[0]! == 1L && (long)r[1]! == 2L);
        // Auction 2 (1 bid) is never the per-window max while auction 1 outbids it.
        Assert.DoesNotContain(rows, r => (long)r[0]! == 2L && (long)r[1]! == 2L);
    }
}
