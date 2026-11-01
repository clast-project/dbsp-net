// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Measurement seam: where a run write's time goes, summed over every write (all threads) in the
/// process. <c>DBSPNET_RUN_PROFILE=/path</c> appends the totals to that file at process exit.
/// Off by default; when off, each phase costs one static bool read.
/// </summary>
internal static class RunWriteProfile
{
    private static readonly string? Path = Environment.GetEnvironmentVariable("DBSPNET_RUN_PROFILE");

    internal static readonly bool Enabled = !string.IsNullOrEmpty(Path);

    internal static readonly string[] Names = ["collect", "sort", "cut", "encode", "write", "footer", "monolithic"];

    private static readonly long[] Ticks = new long[Names.Length];
    private static long _writes;
    private static long _entries;

    static RunWriteProfile()
    {
        if (Enabled)
        {
            AppDomain.CurrentDomain.ProcessExit += (_, _) => Dump();
        }
    }

    internal static long Now() => Enabled ? Stopwatch.GetTimestamp() : 0;

    internal static void Add(int phase, long since)
    {
        if (Enabled)
        {
            Interlocked.Add(ref Ticks[phase], Stopwatch.GetTimestamp() - since);
        }
    }

    internal static void Write(long entries)
    {
        if (Enabled)
        {
            Interlocked.Increment(ref _writes);
            Interlocked.Add(ref _entries, entries);
        }
    }

    private static void Dump()
    {
        var parts = Names.Select((n, i) => FormattableString.Invariant($"{n}_ms={Ticks[i] * 1000.0 / Stopwatch.Frequency:F0}"));
        File.AppendAllText(Path!, string.Create(CultureInfo.InvariantCulture,
            $"RUNPROFILE writes={_writes} entries={_entries} {string.Join(' ', parts)}\n"));
    }
}
