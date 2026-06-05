// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks.Nexmark;

/// <summary>
/// In-process Nexmark event generator. Produces a deterministic stream of
/// Person / Auction / Bid events in event-id order, following the standard
/// Nexmark proportions (1 person : 3 auctions : 46 bids per 50 events),
/// id schemes (persons + auctions both start at 1000, categories 10..14),
/// and the foreign-key relationships the benchmark queries depend on:
/// <list type="bullet">
///   <item>auction.seller references an already-generated person,</item>
///   <item>bid.auction references a <em>recent</em> auction (so a bid's
///   timestamp usually lands inside that auction's [date_time, expires]
///   window — required for q4 / q9 to produce output),</item>
///   <item>bid.bidder references an already-generated person.</item>
/// </list>
/// This is an independent re-implementation matching Feldera's Nexmark
/// schema and workload shape (join selectivity, group cardinalities,
/// event-time semantics) rather than a byte-exact port of their RNG, so it
/// is suitable for cross-system throughput comparison but not for diffing
/// individual output rows against Feldera.
/// </summary>
internal static class NexmarkGenerator
{
    // Standard Nexmark proportions (sum = 50).
    private const int PersonProportion = 1;
    private const int AuctionProportion = 3;
    private const int BidProportion = 46;
    private const int TotalProportion = PersonProportion + AuctionProportion + BidProportion;

    private const long FirstPersonId = 1000;
    private const long FirstAuctionId = 1000;
    private const long FirstCategoryId = 10;
    private const int NumCategories = 5; // categories 10..14; q3 filters category = 10.

    // Event-time model: events are spaced 1 ms apart in simulated time.
    private const long BaseTimeMicros = 1_700_000_000_000_000L; // ~2023-11-14 in micros-since-epoch.
    private const long InterEventMicros = 1_000;                // 1 ms between consecutive events.
    private const long MinAuctionDurationMicros = 5_000_000;    // 5 s.
    private const long MaxAuctionDurationMicros = 30_000_000;   // 30 s.

    // Bids point at one of the most recent auctions so the bid timestamp
    // tends to fall inside the auction's open window.
    private const int RecentAuctionWindow = 1000;

    // Lowercase state codes — q3 selects 'or' / 'id' / 'ca'.
    private static readonly string[] States =
    {
        "az", "ca", "id", "or", "wa", "wy", "nv", "ut", "co", "mt",
    };

    private static readonly string[] FirstNames =
    {
        "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    };

    private static readonly string[] LastNames =
    {
        "adams", "baker", "clark", "davis", "evans", "ford", "green", "hill",
    };

    private static readonly string[] Cities =
    {
        "portland", "boise", "seattle", "reno", "denver", "phoenix",
    };

    private static readonly string[] Channels =
    {
        "apple", "google", "facebook", "baidu",
    };

    /// <summary>One generated event, tagged with the table it belongs to.</summary>
    public readonly record struct Event(NexmarkTable Table, object?[] Row);

    public enum NexmarkTable
    {
        Person,
        Auction,
        Bid,
    }

    /// <summary>
    /// Generate <paramref name="count"/> events in event-id order. The same
    /// seed always yields the same stream.
    /// </summary>
    public static List<Event> Generate(int count, int seed = 42)
    {
        var rng = new Random(seed);
        var events = new List<Event>(count);

        long personCount = 0;
        long auctionCount = 0;

        for (var eventId = 0; eventId < count; eventId++)
        {
            var when = BaseTimeMicros + (eventId * InterEventMicros);
            var rem = eventId % TotalProportion;

            if (rem < PersonProportion && personCount < int.MaxValue)
            {
                var id = FirstPersonId + personCount;
                events.Add(new Event(NexmarkTable.Person, MakePerson(id, when, rng)));
                personCount++;
            }
            else if (rem < PersonProportion + AuctionProportion && personCount > 0)
            {
                var id = FirstAuctionId + auctionCount;
                var seller = FirstPersonId + NextLong(rng, personCount);
                events.Add(new Event(NexmarkTable.Auction, MakeAuction(id, seller, when, rng)));
                auctionCount++;
            }
            else if (auctionCount > 0 && personCount > 0)
            {
                // Bid against one of the most recent auctions.
                var lo = Math.Max(0, auctionCount - RecentAuctionWindow);
                var auctionIndex = lo + NextLong(rng, auctionCount - lo);
                var auction = FirstAuctionId + auctionIndex;
                var bidder = FirstPersonId + NextLong(rng, personCount);
                events.Add(new Event(NexmarkTable.Bid, MakeBid(auction, bidder, when, rng)));
            }
            else
            {
                // Warm-up region before any person/auction exists: emit a
                // person so downstream foreign keys have something to point at.
                var id = FirstPersonId + personCount;
                events.Add(new Event(NexmarkTable.Person, MakePerson(id, when, rng)));
                personCount++;
            }
        }

        return events;
    }

    // person: id, name, email_address, credit_card, city, state, date_time, extra
    private static object?[] MakePerson(long id, long when, Random rng)
    {
        var name = Pick(FirstNames, rng) + " " + Pick(LastNames, rng);
        return new object?[]
        {
            id,
            name,
            name.Replace(' ', '.') + "@example.com",
            rng.Next(1000, 9999).ToString() + rng.Next(1000, 9999),
            Pick(Cities, rng),
            Pick(States, rng),
            new Timestamp(when),
            "p",
        };
    }

    // auction: id, item_name, description, initial_bid, reserve, date_time,
    //          expires, seller, category, extra
    private static object?[] MakeAuction(long id, long seller, long when, Random rng)
    {
        var initialBid = (long)rng.Next(1, 1000);
        var reserve = initialBid + rng.Next(0, 5000);
        var duration = MinAuctionDurationMicros +
            (long)(rng.NextDouble() * (MaxAuctionDurationMicros - MinAuctionDurationMicros));
        var category = FirstCategoryId + rng.Next(NumCategories);
        return new object?[]
        {
            id,
            "item-" + id,
            "description of item " + id,
            initialBid,
            reserve,
            new Timestamp(when),
            new Timestamp(when + duration),
            seller,
            category,
            "a",
        };
    }

    // bid: auction, bidder, price, channel, url, date_time, extra
    private static object?[] MakeBid(long auction, long bidder, long when, Random rng)
    {
        var channel = Pick(Channels, rng);
        return new object?[]
        {
            auction,
            bidder,
            (long)rng.Next(1, 100_000),
            channel,
            "https://www.nexmark.com/" + channel + "/item.html",
            new Timestamp(when),
            "b",
        };
    }

    private static string Pick(string[] values, Random rng) => values[rng.Next(values.Length)];

    private static long NextLong(Random rng, long exclusiveMax) =>
        exclusiveMax <= 1 ? 0 : rng.NextInt64(exclusiveMax);
}
