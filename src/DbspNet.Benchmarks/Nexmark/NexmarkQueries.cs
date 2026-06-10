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

    /// <summary>
    /// A standard Nexmark query DbspNet cannot yet compile, with the reason.
    /// These are emitted as explicit rows in the throughput report so a
    /// side-by-side comparison shows a declared capability gap rather than a
    /// silent omission that reads as "the runner didn't run it".
    /// </summary>
    public sealed record Unsupported(string Id, string Description, string Reason);

    /// <summary>
    /// The standard Nexmark queries DbspNet does not run. All five need an
    /// event-time / processing-time windowing table function (TUMBLE / HOP /
    /// SESSION) that DbspNet does not yet expose — see
    /// <c>docs/skipped.md</c> (window functions). Feldera additionally omits
    /// q6/q10/q11/q13/q14/q21 from its own published set, so they are not listed
    /// here as DbspNet-specific gaps.
    /// </summary>
    public static readonly Unsupported[] NotSupported =
    {
        new("q5", "hot items — sliding-window auction popularity",
            "needs a HOP (sliding) windowing table function"),
        new("q7", "highest bid by window — tumbling-window max price + join",
            "needs a TUMBLE (tumbling) windowing table function"),
        new("q8", "monitor new users — windowed person ⋈ auction",
            "needs a TUMBLE windowing table function"),
        new("q11", "user sessions — session-window bid counts",
            "needs a SESSION windowing table function"),
        new("q12", "processing-time windows — per-bidder bid counts",
            "needs processing-time TUMBLE windows"),
    };

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
            // Verbatim Feldera form: COUNT(*) FILTER (WHERE …) for the conditional
            // plain counts and COUNT(DISTINCT x) FILTER (WHERE …) for the
            // conditional distinct counts. DbspNet's FILTER is parser sugar that
            // lowers agg(x) FILTER (WHERE p) → agg(CASE WHEN p THEN x END) (and
            // COUNT(*) FILTER → COUNT(CASE WHEN p THEN 1 END)), so results are
            // identical to the prior SUM(CASE 1/0) form. Day bucket uses
            // CAST(date_time AS DATE) (Feldera's to_char(date_time,'YYYY-MM-DD')).
            @"SELECT CAST(date_time AS DATE) AS day,
                     COUNT(*) AS total_bids,
                     COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids,
                     COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids,
                     COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids,
                     COUNT(DISTINCT bidder) AS total_bidders,
                     COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders,
                     COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders,
                     COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders,
                     COUNT(DISTINCT auction) AS total_auctions,
                     COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions,
                     COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions,
                     COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions
              FROM bid
              GROUP BY CAST(date_time AS DATE)",
            BidOnly),
        new(
            "q16",
            "channel statistics report — per-channel/day bid/bidder/auction counts",
            // As q15 (verbatim Feldera FILTER form) but keyed by channel and day.
            // Feldera's cosmetic `minute` column (max of to_char(date_time,'HH:mm'))
            // is omitted — DbspNet has no minute-format scalar and it does not
            // exercise COUNT(DISTINCT).
            @"SELECT channel, CAST(date_time AS DATE) AS day,
                     COUNT(*) AS total_bids,
                     COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids,
                     COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids,
                     COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids,
                     COUNT(DISTINCT bidder) AS total_bidders,
                     COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders,
                     COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders,
                     COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders,
                     COUNT(DISTINCT auction) AS total_auctions,
                     COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions,
                     COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions,
                     COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions
              FROM bid
              GROUP BY channel, CAST(date_time AS DATE)",
            BidOnly),
        new(
            "q17",
            "auction statistics by day",
            // Verbatim Feldera form: COUNT(*) FILTER (WHERE …) for the conditional
            // counts (DbspNet lowers FILTER → COUNT(CASE …), identical to the prior
            // SUM(CASE 1/0) results) and CAST(date_time AS DATE) for the day bucket
            // (group-by-expression key, Feldera's DATE_FORMAT).
            @"SELECT b.auction, CAST(b.date_time AS DATE) AS day,
                     COUNT(*) AS total_bids,
                     COUNT(*) FILTER (WHERE b.price < 10000) AS rank1_bids,
                     COUNT(*) FILTER (WHERE b.price >= 10000 AND b.price < 1000000) AS rank2_bids,
                     COUNT(*) FILTER (WHERE b.price >= 1000000) AS rank3_bids,
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
