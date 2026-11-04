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
/// <item><c>DBSPNET_STEP_SPILL_ROWS</c> (§5.14): in-step delta spilling. A delta builder the compiler
/// gave a block format freezes its managed map into a block run each time it reaches this many rows,
/// and a delta with a frozen run is built block backed (and its large runs spill through
/// <c>DBSPNET_SPILL</c> like a trace's). Unset or <c>off</c>: off, and the compiler attaches no formats.
/// 0 is the FORCED mode for tests: every such delta is built as blocks, freezing every
/// <see cref="ForcedChunkRows"/> rows. Structural path only.</item>
/// </list>
/// </summary>
internal static class SpillSettings
{
    private static readonly long? DefaultStepSpillRows = ParseRows(Environment.GetEnvironmentVariable("DBSPNET_STEP_SPILL_ROWS"));

    private static readonly AsyncLocal<(long? Rows, int Chunk)?> StepSpillOverride = new();

    // Whether any override was ever installed: until then the setting is a static read.
    private static volatile bool _stepSpillOverridden;

    /// <summary>The in-step spill threshold in rows (0 forced), or null when off.</summary>
    public static long? StepSpillRows =>
        _stepSpillOverridden && StepSpillOverride.Value is { } o ? o.Rows : DefaultStepSpillRows;

    // DBSPNET_STEP_SPILL_FORCED_CHUNK: a test knob, so a forced suite run can merge many chunks.
    private static readonly int DefaultForcedChunk =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_STEP_SPILL_FORCED_CHUNK"), out var c) && c > 0 ? c : 4096;

    /// <summary>The chunk a forced-mode builder freezes at (default 4096;
    /// <c>DBSPNET_STEP_SPILL_FORCED_CHUNK</c> for test runs).</summary>
    public static int ForcedChunkRows =>
        _stepSpillOverridden && StepSpillOverride.Value is { } o ? o.Chunk : DefaultForcedChunk;

    /// <summary>In-step spilling at <paramref name="rows"/> (null off, 0 forced, freezing every
    /// <paramref name="forcedChunk"/> rows) in the current async context until disposed.</summary>
    public static IDisposable UseStepSpill(long? rows, int forcedChunk = 4096)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(forcedChunk, 1);
        _stepSpillOverridden = true;
        var previous = StepSpillOverride.Value;
        StepSpillOverride.Value = (rows, forcedChunk);
        return new Restore(() => StepSpillOverride.Value = previous);
    }

    internal static long? ParseRows(string? setting)
    {
        var s = setting?.Trim();
        if (string.IsNullOrEmpty(s) || s.Equals("off", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return long.TryParse(s, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var rows) && rows >= 0
            ? rows
            : null;
    }

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
