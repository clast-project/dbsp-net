// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;

namespace DbspNet.Core.Collections;

/// <summary>
/// Groups held outside memory that an <see cref="IndexedZSet{TKey,TValue,TWeight}"/> reads on
/// demand after a lazy restore (docs/design-durable-state.md §1f, increment 4). Reads are
/// synchronous: they happen on the step thread, from the operator that asked for the key.
/// </summary>
internal interface IColdGroups<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>The number of non-empty groups held — exact, known without reading any.</summary>
    int GroupCount { get; }

    /// <summary>The group for <paramref name="key"/> (empty if none).</summary>
    ZSet<TValue, TWeight> Read(TKey key);

    /// <summary>Every non-empty group.</summary>
    IEnumerable<KeyValuePair<TKey, ZSet<TValue, TWeight>>> ReadAll();

    /// <summary>Keys to read at once rather than on demand — groups so large that a first read
    /// mid-step would cost more than holding them (the restore resolves these, then calls
    /// <see cref="ReleasePreload"/>).</summary>
    IReadOnlyCollection<TKey> PreloadKeys { get; }

    /// <summary>The preload has been resolved; drop anything held for it.</summary>
    void ReleasePreload();
}

/// <summary>Process-wide counters for the cold paths of lazy restore (diagnostics; only the cold
/// paths touch them).</summary>
internal static class ColdStats
{
    private static long _resolves;
    private static long _resolvedPresent;
    private static long _promotions;
    private static long _promotedGroups;
    private static long _readTicks;
    private static long _promoteTicks;

    internal static double ReadMs => Interlocked.Read(ref _readTicks) * 1000.0 / System.Diagnostics.Stopwatch.Frequency;

    internal static double PromoteMs => Interlocked.Read(ref _promoteTicks) * 1000.0 / System.Diagnostics.Stopwatch.Frequency;

    internal static void ReadTime(long ticks) => Interlocked.Add(ref _readTicks, ticks);

    internal static void PromoteTime(long ticks) => Interlocked.Add(ref _promoteTicks, ticks);

    internal static long Resolves => Interlocked.Read(ref _resolves);

    internal static long ResolvedPresent => Interlocked.Read(ref _resolvedPresent);

    internal static long Promotions => Interlocked.Read(ref _promotions);

    internal static long PromotedGroups => Interlocked.Read(ref _promotedGroups);

    /// <summary>Who promoted (stack summaries), when DBSPNET_COLD_TRACE=1.</summary>
    internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> Promoters = new();

    private static readonly bool TracePromoters = Environment.GetEnvironmentVariable("DBSPNET_COLD_TRACE") == "1";

    internal static void Resolve(bool present)
    {
        Interlocked.Increment(ref _resolves);
        if (present)
        {
            Interlocked.Increment(ref _resolvedPresent);
        }
    }

    internal static void Promote(long groups)
    {
        Interlocked.Increment(ref _promotions);
        Interlocked.Add(ref _promotedGroups, groups);
        if (TracePromoters)
        {
            var frames = new System.Diagnostics.StackTrace(2, false).GetFrames()
                .Select(f => f.GetMethod())
                .Where(m => m?.DeclaringType?.Namespace?.StartsWith("DbspNet", StringComparison.Ordinal) == true)
                .Take(4)
                .Select(m => m!.DeclaringType!.Name + "." + m.Name);
            Promoters.AddOrUpdate(string.Join(" < ", frames) + " groups~", (int)Math.Min(int.MaxValue, groups), (_, n) => (int)Math.Min(int.MaxValue, n + groups));
        }
    }
}
