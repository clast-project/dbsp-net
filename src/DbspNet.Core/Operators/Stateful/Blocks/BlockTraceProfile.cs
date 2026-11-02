// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// Where a blocked trace's time goes (DBSPNET_TRACE_BLOCKS_PROFILE=1): freeze, merge and probe
/// time summed over every trace and thread, with entry counts. Off: a static bool check.
/// </summary>
internal static class BlockTraceProfile
{
    internal static readonly bool Enabled = Environment.GetEnvironmentVariable("DBSPNET_TRACE_BLOCKS_PROFILE") == "1";

    private static long _freezeTicks;
    private static long _freezeEntries;
    private static long _freezes;
    private static long _mergeTicks;
    private static long _mergeEntries;
    private static long _merges;
    private static long _probeTicks;
    private static long _probes;
    private static long _encodeTicks;
    private static long _blocks;

    private static readonly long[] Sections = new long[16];
    private static readonly long[] SectionBytes = new long[16];

    internal static long Now() => Enabled ? Stopwatch.GetTimestamp() : 0;

    /// <summary>Time since <paramref name="start"/> (and a byte count) into section <paramref name="slot"/>.</summary>
    internal static void Section(int slot, long start, long bytes = 0)
    {
        Interlocked.Add(ref Sections[slot], Stopwatch.GetTimestamp() - start);
        Interlocked.Add(ref SectionBytes[slot], bytes);
    }

    internal static void Freeze(long start, long entries)
    {
        Interlocked.Add(ref _freezeTicks, Stopwatch.GetTimestamp() - start);
        Interlocked.Add(ref _freezeEntries, entries);
        Interlocked.Increment(ref _freezes);
    }

    internal static void Merge(long start, long entries)
    {
        Interlocked.Add(ref _mergeTicks, Stopwatch.GetTimestamp() - start);
        Interlocked.Add(ref _mergeEntries, entries);
        Interlocked.Increment(ref _merges);
    }

    internal static void Probe(long start)
    {
        Interlocked.Add(ref _probeTicks, Stopwatch.GetTimestamp() - start);
        Interlocked.Increment(ref _probes);
    }

    internal static void Encode(long start)
    {
        Interlocked.Add(ref _encodeTicks, Stopwatch.GetTimestamp() - start);
        Interlocked.Increment(ref _blocks);
    }

    /// <summary>One line: totals since the last report, which resets them.</summary>
    internal static string Report()
    {
        static double Ms(ref long ticks) => Interlocked.Exchange(ref ticks, 0) * 1000.0 / Stopwatch.Frequency;
        static long Take(ref long v) => Interlocked.Exchange(ref v, 0);
        var inv = System.Globalization.CultureInfo.InvariantCulture;
        return string.Create(inv, $"BLOCKS freeze_ms={Ms(ref _freezeTicks):F0} freezes={Take(ref _freezes)} frozen_entries={Take(ref _freezeEntries)} ") +
            string.Create(inv, $"merge_ms={Ms(ref _mergeTicks):F0} merges={Take(ref _merges)} merged_entries={Take(ref _mergeEntries)} ") +
            string.Create(inv, $"encode_ms={Ms(ref _encodeTicks):F0} blocks={Take(ref _blocks)} probe_ms={Ms(ref _probeTicks):F0} probes={Take(ref _probes)}") +
            string.Join(string.Empty, Enumerable.Range(0, Sections.Length)
                .Where(i => Volatile.Read(ref Sections[i]) != 0)
                .Select(i => string.Create(inv, $" s{i}_ms={Ms(ref Sections[i]):F0}/{Take(ref SectionBytes[i]) / 1e6:F0}MB")));
    }
}
