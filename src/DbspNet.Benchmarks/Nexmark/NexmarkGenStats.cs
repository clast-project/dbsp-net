// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using DbspNet.Sql.TypeSystem;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks.Nexmark;

/// <summary>
/// Generator sanity probe: `dotnet run -- genstats [events] [legacy|feldera]`.
/// Prints per-table counts, distinct counts of the columns that drive state
/// size, average string bytes per row (vs Feldera's avg_*_byte_size targets),
/// and a SHA-256 digest of the stream so a profile's output can be pinned.
/// </summary>
internal static class NexmarkGenStats
{
    public static int Run(string[] args)
    {
        var count = args.Length > 1 && int.TryParse(args[1], CultureInfo.InvariantCulture, out var c) ? c : 1_000_000;
        var profile = args.Length > 2
            ? (string.Equals(args[2], "feldera", StringComparison.OrdinalIgnoreCase) ? GeneratorProfile.Feldera : GeneratorProfile.Legacy)
            : DefaultProfile;

        var perTable = new long[3];
        var stringBytes = new long[3];
        var extraBytes = new long[3];
        var channels = new HashSet<string>(StringComparer.Ordinal);
        var urls = new HashSet<string>(StringComparer.Ordinal);
        var extras = new HashSet<string>(StringComparer.Ordinal);
        var pairs = new HashSet<(long, long)>();
        var bidAuctions = new HashSet<long>();
        var bidders = new HashSet<long>();
        var auctionIds = new HashSet<long>();
        var sellers = new HashSet<long>();
        long priceSum = 0, durationSum = 0, bidsInWindow = 0;
        var auctionWindows = new Dictionary<long, (long Start, long End)>();
        var states = new Dictionary<string, long>(StringComparer.Ordinal);
        var categories = new Dictionary<long, long>();

        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var sb = new StringBuilder();

        foreach (var e in Stream(count, 42, profile))
        {
            var t = (int)e.Table;
            perTable[t]++;
            sb.Clear().Append(t);
            foreach (var v in e.Row)
            {
                sb.Append('|');
                switch (v)
                {
                    case string s:
                        stringBytes[t] += Encoding.UTF8.GetByteCount(s);
                        sb.Append(s);
                        break;
                    case Timestamp ts:
                        sb.Append('T').Append(ts.Microseconds);
                        break;
                    case long l:
                        sb.Append(l);
                        break;
                    default:
                        sb.Append(v);
                        break;
                }
            }

            sb.Append('\n');
            sha.AppendData(Encoding.UTF8.GetBytes(sb.ToString()));

            var extra = e.Row[^1]!.ToString()!;
            extraBytes[t] += extra.Length;
            switch (e.Table)
            {
                case NexmarkTable.Bid:
                {
                    var auction = (long)e.Row[0]!;
                    var bidder = (long)e.Row[1]!;
                    priceSum += (long)e.Row[2]!;
                    channels.Add(e.Row[3]!.ToString()!);
                    urls.Add(e.Row[4]!.ToString()!);
                    extras.Add(extra);
                    pairs.Add((bidder, auction));
                    bidAuctions.Add(auction);
                    bidders.Add(bidder);
                    var when = ((Timestamp)e.Row[5]!).Microseconds;
                    if (auctionWindows.TryGetValue(auction, out var w) && when >= w.Start && when <= w.End)
                    {
                        bidsInWindow++;
                    }

                    break;
                }

                case NexmarkTable.Auction:
                {
                    var id = (long)e.Row[0]!;
                    auctionIds.Add(id);
                    sellers.Add((long)e.Row[7]!);
                    var start = ((Timestamp)e.Row[5]!).Microseconds;
                    var end = ((Timestamp)e.Row[6]!).Microseconds;
                    durationSum += end - start;
                    auctionWindows[id] = (start, end);
                    var cat = (long)e.Row[8]!;
                    categories[cat] = categories.GetValueOrDefault(cat) + 1;
                    break;
                }

                case NexmarkTable.Person:
                {
                    var st = e.Row[5]!.ToString()!;
                    states[st] = states.GetValueOrDefault(st) + 1;
                    break;
                }
            }
        }

        var names = new[] { "person", "auction", "bid" };
        Console.WriteLine($"Nexmark generator profile={profile}, events={count:N0}, seed=42");
        for (var i = 0; i < 3; i++)
        {
            Console.WriteLine(
                $"  {names[i],-8} rows {perTable[i],10:N0}   avg string bytes/row {stringBytes[i] / (double)Math.Max(1, perTable[i]),7:F1}" +
                $"   avg extra bytes/row {extraBytes[i] / (double)Math.Max(1, perTable[i]),7:F1}");
        }

        Console.WriteLine($"  bid distinct: channel {channels.Count:N0}, url {urls.Count:N0}, extra {extras.Count:N0}, " +
            $"(bidder,auction) {pairs.Count:N0}, auction {bidAuctions.Count:N0}, bidder {bidders.Count:N0}");
        Console.WriteLine($"  bid avg price {priceSum / (double)Math.Max(1, perTable[2]):F0}, " +
            $"bids inside their auction's [date_time, expires] {100.0 * bidsInWindow / Math.Max(1, perTable[2]):F1}%");
        Console.WriteLine($"  auction distinct ids {auctionIds.Count:N0}, sellers {sellers.Count:N0}, " +
            $"avg duration {durationSum / (double)Math.Max(1, perTable[1]) / 1e6:F2}s, categories " +
            string.Join(" ", categories.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}:{kv.Value}")));
        Console.WriteLine("  person states " + string.Join(" ", states.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}:{kv.Value}")));
        Console.WriteLine($"  digest sha256 {Convert.ToHexString(sha.GetHashAndReset())}");
        return 0;
    }
}
