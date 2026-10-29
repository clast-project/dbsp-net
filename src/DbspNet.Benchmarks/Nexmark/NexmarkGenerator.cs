// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
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
/// <para>That description is the <see cref="GeneratorProfile.Legacy"/> profile
/// (still the default). <see cref="GeneratorProfile.Feldera"/> is a
/// distribution-faithful port of Feldera's own generator, selected with
/// <c>NEXMARK_GEN=feldera</c> — see <c>FelderaStream</c> below.</para>
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

    /// <summary>Which generator produces the stream.</summary>
    public enum GeneratorProfile
    {
        /// <summary>The original simplified generator (constant extras, 4 URLs, uniform-recent ids).</summary>
        Legacy,

        /// <summary>A distribution-faithful port of Feldera's generator (<c>crates/nexmark/src/generator</c>).</summary>
        Feldera,
    }

    /// <summary>
    /// Process-wide default profile: <c>NEXMARK_GEN=feldera</c> selects
    /// <see cref="GeneratorProfile.Feldera"/>; anything else (or unset) is
    /// <see cref="GeneratorProfile.Legacy"/>.
    /// </summary>
    public static GeneratorProfile DefaultProfile { get; } =
        string.Equals(Environment.GetEnvironmentVariable("NEXMARK_GEN"), "feldera", StringComparison.OrdinalIgnoreCase)
            ? GeneratorProfile.Feldera
            : GeneratorProfile.Legacy;

    /// <summary>
    /// Generate <paramref name="count"/> events in event-id order using
    /// <see cref="DefaultProfile"/>. The same seed always yields the same stream.
    /// </summary>
    public static List<Event> Generate(int count, int seed = 42) => Generate(count, seed, DefaultProfile);

    /// <summary>Generate <paramref name="count"/> events with an explicit profile.</summary>
    public static List<Event> Generate(int count, int seed, GeneratorProfile profile)
    {
        var events = new List<Event>(count);
        events.AddRange(Stream(count, seed, profile));
        return events;
    }

    /// <summary>
    /// Lazily yield the same stream <see cref="Generate(int, int)"/> materializes
    /// (using <see cref="DefaultProfile"/>). Lets a long-running benchmark feed
    /// millions of events without keeping them all live — a pre-generated list
    /// is itself a heap every full GC must trace.
    /// </summary>
    public static IEnumerable<Event> Stream(int count, int seed = 42) => Stream(count, seed, DefaultProfile);

    /// <summary>Lazily yield <paramref name="count"/> events with an explicit profile.</summary>
    public static IEnumerable<Event> Stream(int count, int seed, GeneratorProfile profile) =>
        profile == GeneratorProfile.Feldera ? FelderaStream(count, seed) : LegacyStream(count, seed);

    private static IEnumerable<Event> LegacyStream(int count, int seed)
    {
        var rng = new Random(seed);

        long personCount = 0;
        long auctionCount = 0;

        for (var eventId = 0; eventId < count; eventId++)
        {
            var when = BaseTimeMicros + (eventId * InterEventMicros);
            var rem = eventId % TotalProportion;

            if (rem < PersonProportion && personCount < int.MaxValue)
            {
                var id = FirstPersonId + personCount;
                yield return new Event(NexmarkTable.Person, MakePerson(id, when, rng));
                personCount++;
            }
            else if (rem < PersonProportion + AuctionProportion && personCount > 0)
            {
                var id = FirstAuctionId + auctionCount;
                var seller = FirstPersonId + NextLong(rng, personCount);
                yield return new Event(NexmarkTable.Auction, MakeAuction(id, seller, when, rng));
                auctionCount++;
            }
            else if (auctionCount > 0 && personCount > 0)
            {
                // Bid against one of the most recent auctions.
                var lo = Math.Max(0, auctionCount - RecentAuctionWindow);
                var auctionIndex = lo + NextLong(rng, auctionCount - lo);
                var auction = FirstAuctionId + auctionIndex;
                var bidder = FirstPersonId + NextLong(rng, personCount);
                yield return new Event(NexmarkTable.Bid, MakeBid(auction, bidder, when, rng));
            }
            else
            {
                // Warm-up region before any person/auction exists: emit a
                // person so downstream foreign keys have something to point at.
                var id = FirstPersonId + personCount;
                yield return new Event(NexmarkTable.Person, MakePerson(id, when, rng));
                personCount++;
            }
        }
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

    // ------------------------------------------------------------------
    // Feldera profile: a port of ../feldera/crates/nexmark/src/generator
    // (bids.rs, auctions.rs, people.rs, strings.rs, price.rs, config.rs) with
    // the defaults of crates/nexmark/src/config.rs and the configuration the
    // Feldera SQL `nexmark` input connector uses (num_event_generators = 1,
    // base_time = 0; crates/adapters/src/transport/nexmark.rs).
    //
    // Faithful in distribution and shape, not bit-for-bit: Feldera seeds a
    // SmallRng per event id; we draw from one System.Random(seed) in event
    // order, so the same seed always gives the same stream. The RNG draws are
    // made in the same order as Feldera's so the code reads side by side.
    //
    // Deliberate deviations (all keep our DDL / query text unchanged):
    //  * person.state is lowercase ("or", not "OR") because our q3 filters
    //    lowercase literals; same length, same 3-of-6 selectivity.
    //  * timestamps keep our BaseTimeMicros origin (Feldera: epoch 0) and are
    //    microseconds (Feldera: milliseconds); the 10 ms event interval and
    //    the auction-length horizon are Feldera's.
    //  * strings picked from constant tables (city, state, hot channel/url,
    //    cached per-channel name/url) are fresh string instances per row, as a
    //    connector would deliver them (Feldera's `to_string()` / `clone()`),
    //    not shared references — so retained-state byte counts are honest.
    // ------------------------------------------------------------------
    private const long FelderaEventIntervalMs = 10;
    private const long FelderaNumInFlightAuctions = 100;
    private const long FelderaNumActivePeople = 1000;
    private const long FelderaPersonIdLead = 10;
    private const long FelderaHotAuctionRatio = 2;   // options.hot_auction_ratio
    private const long FelderaHotBiddersRatio = 4;   // options.hot_bidders_ratio
    private const long FelderaHotSellersRatio = 4;   // options.hot_sellers_ratio
    private const long HotAuctionBucket = 100;       // bids.rs HOT_AUCTION_RATIO
    private const long HotBidderBucket = 100;        // bids.rs HOT_BIDDER_RATIO
    private const long HotSellerBucket = 100;        // auctions.rs HOT_SELLER_RATIO
    private const int HotChannelsRatio = 100;
    private const int ChannelsNumber = 10_000;
    private const int AvgPersonByteSize = 200;
    private const int AvgAuctionByteSize = 500;
    private const int AvgBidByteSize = 100;
    private const int MinStringLength = 3;
    private const int BaseUrlPathLength = 5;

    private const string Alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static readonly string[] FelderaStates = { "az", "ca", "id", "or", "wa", "wy" };

    private static readonly string[] FelderaCities =
    {
        "Phoenix", "Los Angeles", "San Francisco", "Boise", "Portland", "Bend", "Redmond", "Seattle", "Kent", "Cheyenne",
    };

    private static readonly string[] FelderaFirstNames =
    {
        "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
    };

    private static readonly string[] FelderaLastNames =
    {
        "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
    };

    private static readonly string[] HotChannels = { "Google", "Facebook", "Baidu", "Apple" };

    private static readonly string[] HotUrls =
    {
        "https://www.nexmark.com/googl/item.htm?query=1",
        "https://www.nexmark.com/meta/item.htm?query=1",
        "https://www.nexmark.com/bidu/item.htm?query=1",
        "https://www.nexmark.com/aapl/item.htm?query=1",
    };

    private static IEnumerable<Event> FelderaStream(int count, int seed)
    {
        var rng = new Random(seed);
        var channelCache = new (string Name, string Url)?[ChannelsNumber];

        for (long eventId = 0; eventId < count; eventId++)
        {
            var timestampMs = eventId * FelderaEventIntervalMs;
            var rem = eventId % TotalProportion;
            if (rem < PersonProportion)
            {
                yield return new Event(NexmarkTable.Person, FelderaPerson(eventId, timestampMs, rng));
            }
            else if (rem < PersonProportion + AuctionProportion)
            {
                yield return new Event(NexmarkTable.Auction, FelderaAuction(eventId, timestampMs, rng));
            }
            else
            {
                yield return new Event(NexmarkTable.Bid, FelderaBid(eventId, timestampMs, rng, channelCache));
            }
        }
    }

    private static Timestamp FelderaTimestamp(long ms) => new(BaseTimeMicros + (ms * 1000));

    // people.rs next_person
    private static object?[] FelderaPerson(long eventId, long timestampMs, Random rng)
    {
        var id = LastBase0PersonId(eventId) + FirstPersonId;
        var name = FelderaFirstNames[rng.Next(FelderaFirstNames.Length)] + " " +
                   FelderaLastNames[rng.Next(FelderaLastNames.Length)];
        var email = NextString(rng, 7) + "@" + NextString(rng, 5) + ".com";
        var creditCard = string.Create(CultureInfo.InvariantCulture,
            $"{rng.Next(10_000):D4} {rng.Next(10_000):D4} {rng.Next(10_000):D4} {rng.Next(10_000):D4}");
        var city = Fresh(FelderaCities[rng.Next(FelderaCities.Length)]);
        var state = Fresh(FelderaStates[rng.Next(FelderaStates.Length)]);
        var currentSize = sizeof(ulong) + name.Length + email.Length + creditCard.Length + city.Length + state.Length;
        return new object?[]
        {
            id,
            name,
            email,
            creditCard,
            city,
            state,
            FelderaTimestamp(timestampMs),
            NextExtra(rng, currentSize, AvgPersonByteSize),
        };
    }

    // auctions.rs next_auction
    private static object?[] FelderaAuction(long eventId, long timestampMs, Random rng)
    {
        var id = LastBase0AuctionId(eventId) + FirstAuctionId;
        var seller = (rng.NextInt64(FelderaHotSellersRatio) == 0
            ? NextBase0PersonId(eventId, rng)
            : (LastBase0PersonId(eventId) / HotSellerBucket) * HotSellerBucket) + FirstPersonId;
        var category = FirstCategoryId + rng.NextInt64(NumCategories);
        var initialBid = NextPrice(rng);
        var lengthMs = NextAuctionLengthMs(eventId, timestampMs, rng);
        var itemName = NextString(rng, 20);
        var description = NextString(rng, 100);
        var reserve = initialBid + NextPrice(rng);
        var currentSize = sizeof(ulong) + itemName.Length + description.Length + (sizeof(ulong) * 3) + (sizeof(ulong) * 2);
        return new object?[]
        {
            id,
            itemName,
            description,
            initialBid,
            reserve,
            FelderaTimestamp(timestampMs),
            FelderaTimestamp(timestampMs + lengthMs),
            seller,
            category,
            NextExtra(rng, currentSize, AvgAuctionByteSize),
        };
    }

    // bids.rs next_bid
    private static object?[] FelderaBid(long eventId, long timestampMs, Random rng, (string Name, string Url)?[] channelCache)
    {
        var auction = (rng.NextInt64(FelderaHotAuctionRatio) == 0
            ? NextBase0AuctionId(eventId, rng)
            : (LastBase0AuctionId(eventId) / HotAuctionBucket) * HotAuctionBucket) + FirstAuctionId;
        var bidder = (rng.NextInt64(FelderaHotBiddersRatio) == 0
            ? NextBase0PersonId(eventId, rng)
            : ((LastBase0PersonId(eventId) / HotBidderBucket) * HotBidderBucket) + 1) + FirstPersonId;
        var price = NextPrice(rng);

        var channelNumber = rng.Next(ChannelsNumber);
        string channel, url;
        if (rng.Next(1, HotChannelsRatio + 1) == HotChannelsRatio)
        {
            var instance = channelCache[channelNumber] ??= NewChannelInstance(channelNumber);
            (channel, url) = (Fresh(instance.Name), Fresh(instance.Url));
        }
        else
        {
            var hot = rng.Next(HotChannels.Length);
            (channel, url) = (Fresh(HotChannels[hot]), Fresh(HotUrls[hot]));
        }

        var currentSize = (sizeof(ulong) * 3) + sizeof(ulong);
        return new object?[]
        {
            auction,
            bidder,
            price,
            channel,
            url,
            FelderaTimestamp(timestampMs),
            NextExtra(rng, currentSize, AvgBidByteSize),
        };
    }

    // bids.rs get_new_channel_instance: seeded per channel, so a channel's URL
    // is the same whichever event first asks for it.
    private static (string Name, string Url) NewChannelInstance(int channelNumber)
    {
        var rng = new Random(channelNumber);
        var url = "https://www.nexmark.com/" + NextString(rng, BaseUrlPathLength) + "/item.htm?query=1";
        if (rng.Next(10) != 9)
        {
            url += "&channel_id=" + ReverseBits((uint)channelNumber).ToString(CultureInfo.InvariantCulture);
        }

        return ("channel-" + channelNumber.ToString(CultureInfo.InvariantCulture), url);
    }

    private static uint ReverseBits(uint v)
    {
        uint r = 0;
        for (var i = 0; i < 32; i++)
        {
            r = (r << 1) | (v & 1);
            v >>= 1;
        }

        return r;
    }

    // people.rs last_base0_person_id
    private static long LastBase0PersonId(long eventId)
    {
        var epoch = eventId / TotalProportion;
        var offset = eventId % TotalProportion;
        if (offset >= PersonProportion)
        {
            offset = PersonProportion - 1;
        }

        return (epoch * PersonProportion) + offset;
    }

    // people.rs next_base0_person_id: one of the last num_active_people, or up
    // to PERSON_ID_LEAD ids not yet generated (as in Feldera, a bid/auction can
    // reference a person that does not exist yet).
    private static long NextBase0PersonId(long eventId, Random rng)
    {
        var numPeople = LastBase0PersonId(eventId) + 1;
        var activePeople = Math.Min(numPeople, FelderaNumActivePeople);
        var n = rng.NextInt64(activePeople + FelderaPersonIdLead);
        return numPeople - activePeople + n;
    }

    // auctions.rs last_base0_auction_id
    private static long LastBase0AuctionId(long eventId)
    {
        var epoch = eventId / TotalProportion;
        var offset = eventId % TotalProportion;
        if (offset < PersonProportion)
        {
            if (epoch == 0)
            {
                return 0;
            }

            epoch--;
            offset = AuctionProportion - 1;
        }
        else if (offset >= PersonProportion + AuctionProportion)
        {
            offset = AuctionProportion - 1;
        }
        else
        {
            offset -= PersonProportion;
        }

        return (epoch * AuctionProportion) + offset;
    }

    // auctions.rs next_base0_auction_id: one of the last num_in_flight_auctions.
    private static long NextBase0AuctionId(long eventId, Random rng)
    {
        var max = LastBase0AuctionId(eventId);
        var min = Math.Max(0, max - FelderaNumInFlightAuctions);
        return min + rng.NextInt64(max - min + 1);
    }

    // auctions.rs next_auction_length_ms: uniform in [1, 2 * horizon], where the
    // horizon is the event-time span of num_in_flight_auctions auctions.
    private static long NextAuctionLengthMs(long eventId, long timestampMs, Random rng)
    {
        var numEventsForAuctions = (FelderaNumInFlightAuctions * TotalProportion) / AuctionProportion;
        var futureAuction = (eventId + numEventsForAuctions) * FelderaEventIntervalMs;
        var horizon = Math.Max(0, futureAuction - timestampMs);
        return 1 + rng.NextInt64(Math.Max(horizon * 2, 1));
    }

    // price.rs next_price: ceil(10^(U[0,1) * 6) * 100), in f32 as Feldera does.
    private static long NextPrice(Random rng) =>
        (long)MathF.Ceiling(MathF.Pow(10.0f, rng.NextSingle() * 6.0f) * 100.0f);

    // strings.rs next_string: alphanumeric, length uniform in [3, maxLength].
    private static string NextString(Random rng, int maxLength) =>
        RandomAlphanumeric(rng, rng.Next(MinStringLength, maxLength + 1));

    // strings.rs next_extra: pad the row toward desiredAverageSize, length
    // uniform in avg ± round(20% of avg).
    private static string NextExtra(Random rng, int currentSize, int desiredAverageSize)
    {
        if (currentSize > desiredAverageSize)
        {
            return string.Empty;
        }

        var avgExtraSize = desiredAverageSize - currentSize;
        var delta = (int)MathF.Round(avgExtraSize * 0.2f, MidpointRounding.AwayFromZero);
        if (delta == 0)
        {
            return string.Empty;
        }

        var desiredSize = rng.Next(Math.Max(0, avgExtraSize - delta), avgExtraSize + delta + 1);
        return RandomAlphanumeric(rng, desiredSize);
    }

    private static string RandomAlphanumeric(Random rng, int length) =>
        string.Create(length, rng, static (span, r) =>
        {
            for (var i = 0; i < span.Length; i++)
            {
                span[i] = Alphanumeric[r.Next(Alphanumeric.Length)];
            }
        });

    private static string Fresh(string s) => new(s.AsSpan());

    private static string Pick(string[] values, Random rng) => values[rng.Next(values.Length)];

    private static long NextLong(Random rng, long exclusiveMax) =>
        exclusiveMax <= 1 ? 0 : rng.NextInt64(exclusiveMax);
}
