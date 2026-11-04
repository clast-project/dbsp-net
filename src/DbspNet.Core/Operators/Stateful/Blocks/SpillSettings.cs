// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// The knobs of mid-batch spilling (docs/design-bounded-memory.md §5.5): each an environment
/// variable (the hosts' path) with an override in an async context (tests, embedders), read when
/// the state that uses it is built.
/// <list type="bullet">
/// <item><c>DBSPNET_WINDOW_WRITEBACK_MIB</c> (§5.8): the estimated managed bytes all window stores'
/// write-back caches may hold together before a store that ends a step over it drops its cache
/// into its blocked record. Default 1024; <c>off</c> leaves the caches unbounded (dropped only on an
/// idle tick past <c>CacheCap</c> partitions, or at a checkpoint), as before.</item>
/// <item><c>DBSPNET_SPILL</c> = <c>on</c>/<c>off</c> (§5.9): write a blocked trace's runs to files as
/// freezes and merges make them, so the block cache (<c>DBSPNET_BLOCK_CACHE_MIB</c>, which spilling
/// needs) can evict them mid-batch. Off by default until measured.</item>
/// <item><c>DBSPNET_SPILL_MIN_MIB</c> (§5.9): the smallest run spilled, in MiB of blocks (Feldera's
/// <c>min_spill_size</c>). Default 10; 0 spills every run.</item>
/// </list>
/// </summary>
internal static class SpillSettings
{
    private static readonly long DefaultWindowWriteBack = ParseMib(Environment.GetEnvironmentVariable("DBSPNET_WINDOW_WRITEBACK_MIB"), 1024);

    private static readonly AsyncLocal<long?> WindowWriteBackOverride = new();

    /// <summary>The write-back budget in bytes; <see cref="long.MaxValue"/> is unbounded.</summary>
    public static long WindowWriteBackBytes => WindowWriteBackOverride.Value ?? DefaultWindowWriteBack;

    /// <summary><see cref="WindowWriteBackBytes"/> in the current async context until disposed.</summary>
    public static IDisposable UseWindowWriteBack(long bytes)
    {
        var previous = WindowWriteBackOverride.Value;
        WindowWriteBackOverride.Value = bytes;
        return new Restore(() => WindowWriteBackOverride.Value = previous);
    }

    /// <summary>Whether runs spill (<c>DBSPNET_SPILL</c>).</summary>
    public static bool Spill { get; } = string.Equals(Environment.GetEnvironmentVariable("DBSPNET_SPILL"), "on", StringComparison.OrdinalIgnoreCase);

    /// <summary>The smallest run spilled, in bytes (<c>DBSPNET_SPILL_MIN_MIB</c>; <c>off</c> never).</summary>
    public static long SpillMinBytes { get; } = ParseMib(Environment.GetEnvironmentVariable("DBSPNET_SPILL_MIN_MIB"), 10);

    /// <summary>A MiB setting: unset or unparsable is <paramref name="defaultMib"/>; <c>off</c> (or
    /// <c>none</c>) is unbounded; otherwise that many MiB (0 allowed).</summary>
    internal static long ParseMib(string? setting, long defaultMib)
    {
        var s = setting?.Trim();
        if (string.IsNullOrEmpty(s))
        {
            return defaultMib << 20;
        }

        if (s.Equals("off", StringComparison.OrdinalIgnoreCase) || s.Equals("none", StringComparison.OrdinalIgnoreCase))
        {
            return long.MaxValue;
        }

        return long.TryParse(s, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var mib) && mib >= 0
            ? mib << 20
            : defaultMib << 20;
    }

    private sealed class Restore(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
