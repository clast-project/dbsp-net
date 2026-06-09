// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Compile + correctness for the Nexmark queries newly enabled in
/// <c>NexmarkQueries.All</c> (q17 statistics-by-day, q18 dedup, q19 auction
/// TOP-10, q20 filtered join). The SQL here mirrors that benchmark's q17–q20
/// verbatim; this pins both that they compile in the DbspNet dialect and that
/// they produce the right rows on a small hand-built dataset.
/// </summary>
public class NexmarkNewQueriesTests
{
    private static readonly string[] Ddl =
    {
        @"CREATE TABLE auction (
            id BIGINT NOT NULL, item_name VARCHAR NOT NULL, description VARCHAR NOT NULL,
            initial_bid BIGINT NOT NULL, reserve BIGINT NOT NULL, date_time TIMESTAMP NOT NULL,
            expires TIMESTAMP NOT NULL, seller BIGINT NOT NULL, category BIGINT NOT NULL, extra VARCHAR NOT NULL)",
        @"CREATE TABLE bid (
            auction BIGINT NOT NULL, bidder BIGINT NOT NULL, price BIGINT NOT NULL,
            channel VARCHAR NOT NULL, url VARCHAR NOT NULL, date_time TIMESTAMP NOT NULL, extra VARCHAR NOT NULL)",
    };

    private const string Q17 =
        @"SELECT b.auction, CAST(b.date_time AS DATE) AS day,
                 COUNT(*) AS total_bids,
                 SUM(CASE WHEN b.price < 10000 THEN 1 ELSE 0 END) AS rank1_bids,
                 SUM(CASE WHEN b.price >= 10000 AND b.price < 1000000 THEN 1 ELSE 0 END) AS rank2_bids,
                 SUM(CASE WHEN b.price >= 1000000 THEN 1 ELSE 0 END) AS rank3_bids,
                 MIN(b.price) AS min_price, MAX(b.price) AS max_price,
                 AVG(b.price) AS avg_price, SUM(b.price) AS sum_price
          FROM bid b
          GROUP BY b.auction, CAST(b.date_time AS DATE)";

    private const string Q18 =
        @"SELECT auction, bidder, price, channel, url, date_time, extra
          FROM (
              SELECT auction, bidder, price, channel, url, date_time, extra,
                     ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number
              FROM bid
          ) ranked
          WHERE rank_number <= 1";

    private const string Q19 =
        @"SELECT auction, bidder, price, channel, url, date_time, extra
          FROM (
              SELECT auction, bidder, price, channel, url, date_time, extra,
                     ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number
              FROM bid
          ) ranked
          WHERE rank_number <= 10";

    private const string Q20 =
        @"SELECT b.auction, b.bidder, b.price, b.channel, b.url,
                 b.date_time AS bid_date_time, b.extra AS bid_extra,
                 a.item_name, a.description, a.initial_bid, a.reserve,
                 a.date_time AS auction_date_time, a.expires, a.seller, a.category, a.extra AS auction_extra
          FROM bid b JOIN auction a ON b.auction = a.id
          WHERE a.category = 10";

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

    private static void InsertBid(CompiledQuery q, long auction, long bidder, long price, long timeMicros) =>
        q.Table("bid").Insert(auction, bidder, price, "ch", "url", new Timestamp(timeMicros), "x");

    private static void InsertAuction(CompiledQuery q, long id, long category) =>
        q.Table("auction").Insert(id, "item", "desc", 1L, 1L, new Timestamp(0), new Timestamp(0), 99L, category, "x");

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
    public void Q17_AuctionStatistics_CountsMinMaxSumPerGroup()
    {
        var q = Compile(Q17);
        // One auction, one day (all timestamps within day 0): two rank1 (<10k),
        // one rank2 ([10k,1M)), one rank3 (>=1M).
        InsertBid(q, auction: 1, bidder: 1, price: 5000, timeMicros: 1_000_000);
        InsertBid(q, auction: 1, bidder: 2, price: 8000, timeMicros: 2_000_000);
        InsertBid(q, auction: 1, bidder: 3, price: 50000, timeMicros: 3_000_000);
        InsertBid(q, auction: 1, bidder: 4, price: 2000000, timeMicros: 4_000_000);
        q.Step();

        var rows = PositiveRows(q, 10);
        Assert.Single(rows);
        var r = rows[0];
        Assert.Equal(1L, r[0]);              // auction
        Assert.Equal(new Date32(0), r[1]);   // day bucket (epoch day 0)
        Assert.Equal(4L, r[2]);              // total_bids
        Assert.Equal(2L, r[3]);              // rank1_bids (<10k)
        Assert.Equal(1L, r[4]);              // rank2_bids
        Assert.Equal(1L, r[5]);              // rank3_bids
        Assert.Equal(5000L, r[6]);           // min_price
        Assert.Equal(2000000L, r[7]);        // max_price
        Assert.Equal(2063000L, r[9]);        // sum_price (5000+8000+50000+2000000)
    }

    [Fact]
    public void Q18_FindLastBid_KeepsLatestPerBidderAuction()
    {
        var q = Compile(Q18);
        // (bidder 1, auction 1): three bids; latest is at t=300 (price 30).
        InsertBid(q, auction: 1, bidder: 1, price: 10, timeMicros: 100);
        InsertBid(q, auction: 1, bidder: 1, price: 30, timeMicros: 300);
        InsertBid(q, auction: 1, bidder: 1, price: 20, timeMicros: 200);
        // (bidder 2, auction 1): one bid.
        InsertBid(q, auction: 1, bidder: 2, price: 5, timeMicros: 50);
        q.Step();

        var rows = PositiveRows(q, 7);
        Assert.Equal(2, rows.Count);
        // (bidder 1, auction 1) → the latest bid, price 30.
        Assert.Equal(30L, rows.Single(r => (long)r[1]! == 1L && (long)r[0]! == 1L)[2]);
        // (bidder 2, auction 1) → its only bid, price 5.
        Assert.Equal(5L, rows.Single(r => (long)r[1]! == 2L && (long)r[0]! == 1L)[2]);
    }

    [Fact]
    public void Q19_AuctionTopTen_KeepsTenHighestPrices()
    {
        var q = Compile(Q19);
        // 12 bids on auction 1 with prices 1..12 → top-10 are 3..12.
        for (long p = 1; p <= 12; p++)
        {
            InsertBid(q, auction: 1, bidder: p, price: p, timeMicros: p);
        }

        q.Step();

        var rows = PositiveRows(q, 7);
        Assert.Equal(10, rows.Count);
        var prices = rows.Select(r => (long)r[2]!).OrderBy(p => p).ToList();
        Assert.Equal(3L, prices.First());    // the two lowest (1, 2) are dropped
        Assert.Equal(12L, prices.Last());
    }

    [Fact]
    public void Q20_ExpandBidWithAuction_FiltersCategoryTen()
    {
        var q = Compile(Q20);
        InsertAuction(q, id: 1, category: 10);   // kept
        InsertAuction(q, id: 2, category: 5);    // filtered out
        InsertBid(q, auction: 1, bidder: 11, price: 100, timeMicros: 1);
        InsertBid(q, auction: 2, bidder: 22, price: 200, timeMicros: 2);
        InsertBid(q, auction: 1, bidder: 33, price: 300, timeMicros: 3);
        q.Step();

        var rows = PositiveRows(q, 16);
        // Only the two auction-1 bids survive; both carry category 10.
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(10L, r[14]));   // category column
        Assert.Equal(new HashSet<long> { 100L, 300L }, rows.Select(r => (long)r[2]!).ToHashSet());
    }
}
