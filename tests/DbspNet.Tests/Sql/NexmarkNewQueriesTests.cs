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
        @"CREATE TABLE person (
            id BIGINT NOT NULL, name VARCHAR NOT NULL, email_address VARCHAR NOT NULL,
            credit_card VARCHAR NOT NULL, city VARCHAR NOT NULL, state VARCHAR NOT NULL,
            date_time TIMESTAMP NOT NULL, extra VARCHAR NOT NULL)",
    };

    private const long Sec = 1_000_000L;

    // q7: highest bid per 10s tumbling window, joined back to the bids matching that
    // max price within the window's [start − size, start] band (Feldera's verbatim form).
    private const string Q7 =
        @"SELECT B.auction, B.price, B.bidder, B.date_time, B.extra
          FROM bid B
          JOIN (
            SELECT MAX(B1.price) AS maxprice,
                   TUMBLE_START(B1.date_time, INTERVAL '10' SECOND) AS date_time
            FROM bid B1
            GROUP BY TUMBLE(B1.date_time, INTERVAL '10' SECOND)
          ) B1
          ON B.price = B1.maxprice
          WHERE B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND AND B1.date_time";

    // q8: people who entered an auction (as seller) within the same 10s window they
    // were created in — windowed person ⋈ auction on (id, window start, window end).
    private const string Q8 =
        @"SELECT P.id, P.name, P.starttime
          FROM (
            SELECT P.id, P.name,
                   TUMBLE_START(P.date_time, INTERVAL '10' SECOND) AS starttime,
                   TUMBLE_END(P.date_time, INTERVAL '10' SECOND) AS endtime
            FROM person P
            GROUP BY P.id, P.name, TUMBLE(P.date_time, INTERVAL '10' SECOND)
          ) P
          JOIN (
            SELECT A.seller,
                   TUMBLE_START(A.date_time, INTERVAL '10' SECOND) AS starttime,
                   TUMBLE_END(A.date_time, INTERVAL '10' SECOND) AS endtime
            FROM auction A
            GROUP BY A.seller, TUMBLE(A.date_time, INTERVAL '10' SECOND)
          ) A
          ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime";

    // q5: hot items — per-auction bid counts in 10s windows sliding every 2s
    // (HOP TVF), self-joined to the per-window max to surface the hot item(s).
    private const string Q5 =
        @"SELECT AuctionBids.auction, AuctionBids.num
          FROM (
            SELECT B1.auction, COUNT(*) AS num,
                   window_start AS starttime, window_end AS endtime
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
            AND AuctionBids.num >= MaxBids.maxn";

    // q12: per-bidder bid counts within each 10s (event-time) tumbling window.
    private const string Q12 =
        @"SELECT B.bidder, COUNT(*) AS bid_count,
                 TUMBLE_START(B.date_time, INTERVAL '10' SECOND) AS starttime,
                 TUMBLE_END(B.date_time, INTERVAL '10' SECOND) AS endtime
          FROM bid B
          GROUP BY B.bidder, TUMBLE(B.date_time, INTERVAL '10' SECOND)";

    private const string Q15 =
        @"SELECT CAST(date_time AS DATE) AS day,
                 COUNT(*) AS total_bids,
                 SUM(CASE WHEN price < 10000 THEN 1 ELSE 0 END) AS rank1_bids,
                 SUM(CASE WHEN price >= 10000 AND price < 1000000 THEN 1 ELSE 0 END) AS rank2_bids,
                 SUM(CASE WHEN price >= 1000000 THEN 1 ELSE 0 END) AS rank3_bids,
                 COUNT(DISTINCT bidder) AS total_bidders,
                 COUNT(DISTINCT CASE WHEN price < 10000 THEN bidder END) AS rank1_bidders,
                 COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 1000000 THEN bidder END) AS rank2_bidders,
                 COUNT(DISTINCT CASE WHEN price >= 1000000 THEN bidder END) AS rank3_bidders,
                 COUNT(DISTINCT auction) AS total_auctions,
                 COUNT(DISTINCT CASE WHEN price < 10000 THEN auction END) AS rank1_auctions,
                 COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 1000000 THEN auction END) AS rank2_auctions,
                 COUNT(DISTINCT CASE WHEN price >= 1000000 THEN auction END) AS rank3_auctions
          FROM bid
          GROUP BY CAST(date_time AS DATE)";

    private const string Q16 =
        @"SELECT channel, CAST(date_time AS DATE) AS day,
                 COUNT(*) AS total_bids,
                 SUM(CASE WHEN price < 10000 THEN 1 ELSE 0 END) AS rank1_bids,
                 SUM(CASE WHEN price >= 10000 AND price < 1000000 THEN 1 ELSE 0 END) AS rank2_bids,
                 SUM(CASE WHEN price >= 1000000 THEN 1 ELSE 0 END) AS rank3_bids,
                 COUNT(DISTINCT bidder) AS total_bidders,
                 COUNT(DISTINCT CASE WHEN price < 10000 THEN bidder END) AS rank1_bidders,
                 COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 1000000 THEN bidder END) AS rank2_bidders,
                 COUNT(DISTINCT CASE WHEN price >= 1000000 THEN bidder END) AS rank3_bidders,
                 COUNT(DISTINCT auction) AS total_auctions,
                 COUNT(DISTINCT CASE WHEN price < 10000 THEN auction END) AS rank1_auctions,
                 COUNT(DISTINCT CASE WHEN price >= 10000 AND price < 1000000 THEN auction END) AS rank2_auctions,
                 COUNT(DISTINCT CASE WHEN price >= 1000000 THEN auction END) AS rank3_auctions
          FROM bid
          GROUP BY channel, CAST(date_time AS DATE)";

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

    private const string Q22 =
        @"SELECT auction, bidder, price, channel,
                 SPLIT_INDEX(url, '/', 3) AS dir1,
                 SPLIT_INDEX(url, '/', 4) AS dir2,
                 SPLIT_INDEX(url, '/', 5) AS dir3
          FROM bid";

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

    private static void InsertBidCh(CompiledQuery q, long auction, long bidder, long price, string channel, long timeMicros) =>
        q.Table("bid").Insert(auction, bidder, price, channel, "url", new Timestamp(timeMicros), "x");

    private static void InsertAuction(CompiledQuery q, long id, long category) =>
        q.Table("auction").Insert(id, "item", "desc", 1L, 1L, new Timestamp(0), new Timestamp(0), 99L, category, "x");

    private static void InsertSeller(CompiledQuery q, long seller, long timeMicros) =>
        q.Table("auction").Insert(0L, "item", "desc", 1L, 1L, new Timestamp(timeMicros), new Timestamp(0), seller, 1L, "x");

    private static void InsertPerson(CompiledQuery q, long id, string name, long timeMicros) =>
        q.Table("person").Insert(id, name, "e", "cc", "city", "st", new Timestamp(timeMicros), "x");

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
    public void Q15_BiddingStatistics_DistinctBiddersAndAuctionsPerDay()
    {
        var q = Compile(Q15);
        // All bids within epoch day 0 (micros < one day), so a single group.
        // Rank bands: <10k (rank1), [10k,1M) (rank2), >=1M (rank3).
        InsertBid(q, auction: 1, bidder: 1, price: 5000, timeMicros: 1_000_000);     // rank1
        InsertBid(q, auction: 1, bidder: 2, price: 8000, timeMicros: 2_000_000);     // rank1
        InsertBid(q, auction: 2, bidder: 1, price: 9000, timeMicros: 3_000_000);     // rank1 (bidder 1 again)
        InsertBid(q, auction: 2, bidder: 3, price: 50000, timeMicros: 4_000_000);    // rank2
        InsertBid(q, auction: 3, bidder: 4, price: 2000000, timeMicros: 5_000_000);  // rank3
        InsertBid(q, auction: 3, bidder: 4, price: 3000000, timeMicros: 6_000_000);  // rank3 (bidder 4 again)
        q.Step();

        var rows = PositiveRows(q, 13);
        Assert.Single(rows);
        var r = rows[0];
        Assert.Equal(new Date32(0), r[0]);   // day bucket
        Assert.Equal(6L, r[1]);              // total_bids
        Assert.Equal(3L, r[2]);              // rank1_bids
        Assert.Equal(1L, r[3]);              // rank2_bids
        Assert.Equal(2L, r[4]);              // rank3_bids
        Assert.Equal(4L, r[5]);              // total_bidders {1,2,3,4}
        Assert.Equal(2L, r[6]);              // rank1_bidders {1,2}
        Assert.Equal(1L, r[7]);              // rank2_bidders {3}
        Assert.Equal(1L, r[8]);              // rank3_bidders {4}
        Assert.Equal(3L, r[9]);              // total_auctions {1,2,3}
        Assert.Equal(2L, r[10]);             // rank1_auctions {1,2}
        Assert.Equal(1L, r[11]);             // rank2_auctions {2}
        Assert.Equal(1L, r[12]);             // rank3_auctions {3}
    }

    [Fact]
    public void Q16_ChannelStatistics_DistinctCountsPerChannelDay()
    {
        var q = Compile(Q16);
        // Channel "a": one rank1 bid (bidder 1) and one rank3 bid (bidder 2) on auction 1.
        // Channel "b": one rank2 bid (bidder 1) on auction 2. All within day 0.
        InsertBidCh(q, auction: 1, bidder: 1, price: 5000, channel: "a", timeMicros: 1_000_000);
        InsertBidCh(q, auction: 1, bidder: 2, price: 2000000, channel: "a", timeMicros: 2_000_000);
        InsertBidCh(q, auction: 2, bidder: 1, price: 50000, channel: "b", timeMicros: 3_000_000);
        q.Step();

        var rows = PositiveRows(q, 14);
        Assert.Equal(2, rows.Count);

        var a = rows.Single(row => row[0]!.ToString() == "a");
        Assert.Equal(new Date32(0), a[1]);   // day
        Assert.Equal(2L, a[2]);              // total_bids
        Assert.Equal(1L, a[3]);              // rank1_bids
        Assert.Equal(0L, a[4]);              // rank2_bids
        Assert.Equal(1L, a[5]);              // rank3_bids
        Assert.Equal(2L, a[6]);              // total_bidders {1,2}
        Assert.Equal(1L, a[7]);              // rank1_bidders {1}
        Assert.Equal(0L, a[8]);              // rank2_bidders {}
        Assert.Equal(1L, a[9]);              // rank3_bidders {2}
        Assert.Equal(1L, a[10]);             // total_auctions {1}
        Assert.Equal(1L, a[11]);             // rank1_auctions {1}
        Assert.Equal(0L, a[12]);             // rank2_auctions {}
        Assert.Equal(1L, a[13]);             // rank3_auctions {1}

        var b = rows.Single(row => row[0]!.ToString() == "b");
        Assert.Equal(1L, b[2]);              // total_bids
        Assert.Equal(1L, b[4]);              // rank2_bids
        Assert.Equal(1L, b[6]);              // total_bidders {1}
        Assert.Equal(1L, b[8]);              // rank2_bidders {1}
        Assert.Equal(1L, b[10]);             // total_auctions {2}
        Assert.Equal(1L, b[12]);             // rank2_auctions {2}
    }

    [Fact]
    public void Q22_GetUrlDirectories_SplitsPathSegments()
    {
        var q = Compile(Q22);
        // A URL with three path segments after the host.
        q.Table("bid").Insert(
            1L, 2L, 100L, "ch", "https://www.nexmark.com/aaa/bbb/item.html", new Timestamp(0), "x");
        q.Step();

        var rows = PositiveRows(q, 7);
        var r = Assert.Single(rows);
        Assert.Equal("aaa", r[4]!.ToString());        // dir1 = segment 3
        Assert.Equal("bbb", r[5]!.ToString());        // dir2 = segment 4
        Assert.Equal("item.html", r[6]!.ToString());  // dir3 = segment 5
    }

    [Fact]
    public void Q22_OutOfRangeSegment_IsNull()
    {
        var q = Compile(Q22);
        // Only segments 0..4 exist (the generator's shallow URL), so dir3
        // (index 5) is NULL.
        q.Table("bid").Insert(
            1L, 2L, 100L, "ch", "https://www.nexmark.com/ch/item.html", new Timestamp(0), "x");
        q.Step();

        var r = Assert.Single(PositiveRows(q, 7));
        Assert.Equal("ch", r[4]!.ToString());          // dir1 = segment 3 (channel)
        Assert.Equal("item.html", r[5]!.ToString());   // dir2 = segment 4
        Assert.Null(r[6]);                             // dir3 = segment 5 (out of range)
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
    public void Q5_HotItems_SlidingWindowPopularity()
    {
        var q = Compile(Q5);
        // Auction 1 gets 2 bids and auction 2 gets 1, all inside one 10s window —
        // auction 1 is the hot item (per-window max count = 2).
        InsertBid(q, auction: 1, bidder: 1, price: 100, timeMicros: 3 * Sec);
        InsertBid(q, auction: 1, bidder: 2, price: 110, timeMicros: 5 * Sec);
        InsertBid(q, auction: 2, bidder: 3, price: 200, timeMicros: 7 * Sec);
        q.Step();

        var rows = PositiveRows(q, 2);
        Assert.Contains(rows, r => (long)r[0]! == 1L && (long)r[1]! == 2L);
        Assert.DoesNotContain(rows, r => (long)r[0]! == 2L && (long)r[1]! == 2L);
    }

    [Fact]
    public void Q7_HighestBidPerTumblingWindow_Compiles()
    {
        var q = Compile(Q7);
        // Window [0,10): bids 100@3s, 250@7s → max 250, window start 0s.
        // Window [10,20): bid 250@12s → max 250, window start 10s.
        InsertBid(q, auction: 1, bidder: 1, price: 100, timeMicros: 3 * Sec);
        InsertBid(q, auction: 1, bidder: 2, price: 250, timeMicros: 7 * Sec);
        InsertBid(q, auction: 1, bidder: 3, price: 250, timeMicros: 12 * Sec);
        q.Step();

        // Every surviving bid carries a window-max price; the literal BETWEEN band
        // [start − 10s, start] keeps the 250@7s bid via window-1's start (10s).
        var rows = PositiveRows(q, 5);
        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.Equal(250L, (long)r[1]!)); // price column = a window max
        Assert.Contains(rows, r => (long)r[2]! == 2L);          // bidder 2 (250@7s) survives
    }

    [Fact]
    public void Q8_WindowedPersonAuctionJoin_MatchesSameWindow()
    {
        var q = Compile(Q8);
        InsertPerson(q, id: 1, name: "alice", timeMicros: 3 * Sec);   // window [0,10)
        InsertSeller(q, seller: 1, timeMicros: 8 * Sec);              // window [0,10) → match
        InsertSeller(q, seller: 1, timeMicros: 15 * Sec);             // window [10,20) → no match
        InsertPerson(q, id: 2, name: "bob", timeMicros: 4 * Sec);     // no matching seller
        q.Step();

        var rows = PositiveRows(q, 3);
        var r = Assert.Single(rows);
        Assert.Equal(1L, r[0]);                       // id
        Assert.Equal("alice", r[1]!.ToString());      // name
        Assert.Equal(new Timestamp(0L), r[2]);        // window start (same window as the auction)
    }

    [Fact]
    public void Q12_PerBidderWindowCounts()
    {
        var q = Compile(Q12);
        InsertBid(q, auction: 1, bidder: 1, price: 10, timeMicros: 3 * Sec);  // bidder1, window0
        InsertBid(q, auction: 1, bidder: 1, price: 20, timeMicros: 7 * Sec);  // bidder1, window0
        InsertBid(q, auction: 1, bidder: 1, price: 30, timeMicros: 12 * Sec); // bidder1, window1
        InsertBid(q, auction: 1, bidder: 2, price: 40, timeMicros: 4 * Sec);  // bidder2, window0
        q.Step();

        var rows = PositiveRows(q, 4);
        Assert.Equal(3, rows.Count);
        // (bidder, count, wstart, wend)
        Assert.Equal(2L, rows.Single(r => (long)r[0]! == 1L && (Timestamp)r[2]! == new Timestamp(0L))[1]);
        Assert.Equal(1L, rows.Single(r => (long)r[0]! == 1L && (Timestamp)r[2]! == new Timestamp(10 * Sec))[1]);
        Assert.Equal(1L, rows.Single(r => (long)r[0]! == 2L && (Timestamp)r[2]! == new Timestamp(0L))[1]);
        // window end = start + 10s
        Assert.Equal(new Timestamp(10 * Sec), rows.First(r => (Timestamp)r[2]! == new Timestamp(0L))[3]);
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
