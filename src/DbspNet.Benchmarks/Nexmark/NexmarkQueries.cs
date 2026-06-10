// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Benchmarks.Nexmark;

/// <summary>
/// Nexmark schema (DDL) and the SQL for the subset of queries DbspNet can
/// compile, kept as close to Feldera's published Nexmark SQL as the DbspNet
/// dialect allows. Each query reads one or more of <c>person</c>,
/// <c>auction</c>, <c>bid</c>.
/// </summary>
internal static class NexmarkQueries
{
    public static readonly string[] Ddl =
    {
        @"CREATE TABLE person (
            id BIGINT NOT NULL,
            name VARCHAR NOT NULL,
            email_address VARCHAR NOT NULL,
            credit_card VARCHAR NOT NULL,
            city VARCHAR NOT NULL,
            state VARCHAR NOT NULL,
            date_time TIMESTAMP NOT NULL,
            extra VARCHAR NOT NULL)",
        @"CREATE TABLE auction (
            id BIGINT NOT NULL,
            item_name VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            initial_bid BIGINT NOT NULL,
            reserve BIGINT NOT NULL,
            date_time TIMESTAMP NOT NULL,
            expires TIMESTAMP NOT NULL,
            seller BIGINT NOT NULL,
            category BIGINT NOT NULL,
            extra VARCHAR NOT NULL)",
        @"CREATE TABLE bid (
            auction BIGINT NOT NULL,
            bidder BIGINT NOT NULL,
            price BIGINT NOT NULL,
            channel VARCHAR NOT NULL,
            url VARCHAR NOT NULL,
            date_time TIMESTAMP NOT NULL,
            extra VARCHAR NOT NULL)",
    };

    /// <summary>A single benchmarkable query and the tables it consumes.</summary>
    public sealed record Query(
        string Id,
        string Description,
        string Sql,
        NexmarkGenerator.NexmarkTable[] Tables);

    private static readonly NexmarkGenerator.NexmarkTable[] BidOnly =
        { NexmarkGenerator.NexmarkTable.Bid };

    private static readonly NexmarkGenerator.NexmarkTable[] AuctionPerson =
        { NexmarkGenerator.NexmarkTable.Auction, NexmarkGenerator.NexmarkTable.Person };

    private static readonly NexmarkGenerator.NexmarkTable[] AuctionBid =
        { NexmarkGenerator.NexmarkTable.Auction, NexmarkGenerator.NexmarkTable.Bid };

    /// <summary>
    /// The candidate queries. Not all are guaranteed to compile on every
    /// build of DbspNet — the harness attempts each and reports which
    /// succeed. Windowed queries that require TUMBLE / HOP table functions
    /// (q5, q7, q8) are intentionally omitted: DbspNet does not yet expose
    /// those windowing primitives.
    /// </summary>
    public static readonly Query[] All =
    {
        new(
            "q0",
            "passthrough — SELECT * FROM bid",
            @"SELECT auction, bidder, price, channel, url, date_time, extra FROM bid",
            BidOnly),
        new(
            "q1",
            "currency conversion — map a column",
            @"SELECT auction, bidder, 0.908 * price AS price, channel, url, date_time, extra FROM bid",
            BidOnly),
        new(
            "q2",
            "selection — WHERE auction % 123 = 0",
            @"SELECT auction, price FROM bid WHERE auction % 123 = 0",
            BidOnly),
        new(
            "q3",
            "local item suggestion — auction ⋈ person, filtered",
            @"SELECT p.name, p.city, p.state, a.id
              FROM auction a JOIN person p ON a.seller = p.id
              WHERE a.category = 10
                AND (p.state = 'or' OR p.state = 'id' OR p.state = 'ca')",
            AuctionPerson),
        new(
            "q4",
            "average closing price by category",
            @"SELECT q.category, AVG(q.final) AS avg_final
              FROM (
                  SELECT MAX(b.price) AS final, a.category
                  FROM auction a JOIN bid b ON a.id = b.auction
                  WHERE b.date_time BETWEEN a.date_time AND a.expires
                  GROUP BY a.id, a.category
              ) q
              GROUP BY q.category",
            AuctionBid),
        new(
            "q9",
            "winning bids — top bid per auction",
            @"SELECT a.id, a.item_name, a.category, w.bidder, w.price, w.bid_time
              FROM auction a
              JOIN (
                  SELECT auction, bidder, price, bid_time
                  FROM (
                      SELECT b.auction, b.bidder, b.price, b.date_time AS bid_time,
                             ROW_NUMBER() OVER (
                                 PARTITION BY b.auction
                                 ORDER BY b.price DESC, b.date_time ASC) AS rn
                      FROM bid b
                  ) ranked
                  WHERE rn <= 1
              ) w ON a.id = w.auction",
            AuctionBid),
        new(
            "q15",
            "bidding statistics report — per-day bid/bidder/auction counts",
            // Feldera uses COUNT(*) FILTER / COUNT(DISTINCT …) FILTER over
            // to_char(date_time,'YYYY-MM-DD'). DbspNet-dialect equivalents:
            // CAST(date_time AS DATE) for the day bucket, SUM(CASE …) for the
            // conditional plain counts, and COUNT(DISTINCT CASE WHEN p THEN x END)
            // for the conditional distinct counts (the CASE yields NULL when the
            // predicate is false and COUNT(DISTINCT) ignores NULL — exactly the
            // FILTER semantics).
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
              GROUP BY CAST(date_time AS DATE)",
            BidOnly),
        new(
            "q16",
            "channel statistics report — per-channel/day bid/bidder/auction counts",
            // As q15 but keyed by channel and day. Feldera's cosmetic `minute`
            // column (max of to_char(date_time,'HH:mm')) is omitted — DbspNet has
            // no minute-format scalar and it does not exercise COUNT(DISTINCT).
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
              GROUP BY channel, CAST(date_time AS DATE)",
            BidOnly),
        new(
            "q17",
            "auction statistics by day",
            // Feldera uses COUNT(*) FILTER (WHERE …) + DATE_FORMAT; the DbspNet
            // dialect equivalent is SUM(CASE …) for the conditional counts and
            // CAST(date_time AS DATE) for the day bucket (group-by-expression key).
            @"SELECT b.auction, CAST(b.date_time AS DATE) AS day,
                     COUNT(*) AS total_bids,
                     SUM(CASE WHEN b.price < 10000 THEN 1 ELSE 0 END) AS rank1_bids,
                     SUM(CASE WHEN b.price >= 10000 AND b.price < 1000000 THEN 1 ELSE 0 END) AS rank2_bids,
                     SUM(CASE WHEN b.price >= 1000000 THEN 1 ELSE 0 END) AS rank3_bids,
                     MIN(b.price) AS min_price, MAX(b.price) AS max_price,
                     AVG(b.price) AS avg_price, SUM(b.price) AS sum_price
              FROM bid b
              GROUP BY b.auction, CAST(b.date_time AS DATE)",
            BidOnly),
        new(
            "q18",
            "find last bid — dedup latest bid per (bidder, auction)",
            @"SELECT auction, bidder, price, channel, url, date_time, extra
              FROM (
                  SELECT auction, bidder, price, channel, url, date_time, extra,
                         ROW_NUMBER() OVER (
                             PARTITION BY bidder, auction
                             ORDER BY date_time DESC) AS rank_number
                  FROM bid
              ) ranked
              WHERE rank_number <= 1",
            BidOnly),
        new(
            "q19",
            "auction TOP-10 — ten highest bids per auction",
            @"SELECT auction, bidder, price, channel, url, date_time, extra
              FROM (
                  SELECT auction, bidder, price, channel, url, date_time, extra,
                         ROW_NUMBER() OVER (
                             PARTITION BY auction
                             ORDER BY price DESC) AS rank_number
                  FROM bid
              ) ranked
              WHERE rank_number <= 10",
            BidOnly),
        new(
            "q20",
            "expand bid with auction — filtered bid ⋈ auction",
            @"SELECT b.auction, b.bidder, b.price, b.channel, b.url,
                     b.date_time AS bid_date_time, b.extra AS bid_extra,
                     a.item_name, a.description, a.initial_bid, a.reserve,
                     a.date_time AS auction_date_time, a.expires, a.seller,
                     a.category, a.extra AS auction_extra
              FROM bid b JOIN auction a ON b.auction = a.id
              WHERE a.category = 10",
            AuctionBid),
        new(
            "q22",
            "get URL directories — split the bid URL into path segments",
            // Verbatim Feldera q22: SPLIT_INDEX picks the n-th (0-based) '/'-
            // delimited segment of the URL. Indices 3/4/5 are the path segments
            // after the scheme/host (segments 0='https:', 1='', 2=host).
            @"SELECT auction, bidder, price, channel,
                     SPLIT_INDEX(url, '/', 3) AS dir1,
                     SPLIT_INDEX(url, '/', 4) AS dir2,
                     SPLIT_INDEX(url, '/', 5) AS dir3
              FROM bid",
            BidOnly),
    };
}
