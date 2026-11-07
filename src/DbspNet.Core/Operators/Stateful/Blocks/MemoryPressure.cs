// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>Memory pressure, as Feldera's levels (docs/design-bounded-memory.md §5.28).</summary>
internal enum PressureLevel
{
    /// <summary>Below the Moderate threshold: the fixed spill thresholds.</summary>
    Low = 0,

    /// <summary>Every run is written to a file.</summary>
    Moderate = 1,

    /// <summary>Every insert freezes, every managed level is pushed once per epoch, window caches drop.</summary>
    High = 2,

    /// <summary>In-step delta chunks freeze at a floor too.</summary>
    Critical = 3,
}

/// <summary>
/// S3 (docs/design-bounded-memory.md §5.28): a memory-pressure level that the spill thresholds follow.
/// The measure is the GC heap still in use after the most recent collection
/// (<c>HeapSizeBytes - FragmentedBytes</c>), against <c>DBSPNET_MEMORY_LIMIT_MIB</c> or the GC's
/// <c>TotalAvailableMemoryBytes</c>. It is recomputed once per collection by a sentinel finalizer, so a
/// reader pays one static load. Levels (<c>DBSPNET_PRESSURE_LEVELS</c>, default <c>85,90,95</c>) and
/// hysteresis are Feldera's: High holds until the raw level is Low, Critical until it is Moderate or below.
/// Each entry into High or Critical from below bumps <see cref="Epoch"/>. <c>DBSPNET_PRESSURE</c>:
/// <c>on</c> (default), <c>off</c> (always Low), or a level name to pin it (forced suite runs).
/// </summary>
internal static class MemoryPressure
{
    private static readonly string? Setting = Environment.GetEnvironmentVariable("DBSPNET_PRESSURE")?.Trim().ToLowerInvariant();

    /// <summary>Whether the monitor runs (<c>DBSPNET_PRESSURE</c> not <c>off</c> and not a pinned level).</summary>
    public static bool Monitoring { get; } = Setting is null or "" or "on";

    private static readonly PressureLevel? Pinned = Setting switch
    {
        "low" => PressureLevel.Low,
        "moderate" => PressureLevel.Moderate,
        "high" => PressureLevel.High,
        "critical" => PressureLevel.Critical,
        _ => null,
    };

    /// <summary>Moderate, High and Critical thresholds, as fractions of the limit.</summary>
    public static (double Moderate, double High, double Critical) Levels { get; } =
        ParseLevels(Environment.GetEnvironmentVariable("DBSPNET_PRESSURE_LEVELS"));

    /// <summary>Managed entries a trace keeps at High or above (<c>DBSPNET_PRESSURE_HIGH_FREEZE</c>, default 0:
    /// every insert freezes).</summary>
    public static int HighFreezeEntries { get; } = NonNegative("DBSPNET_PRESSURE_HIGH_FREEZE", 0);

    /// <summary>The in-step delta chunk at Critical (<c>DBSPNET_PRESSURE_CRITICAL_STEP_ROWS</c>, default 65,536).</summary>
    public static int CriticalStepRows { get; } = Math.Max(1, NonNegative("DBSPNET_PRESSURE_CRITICAL_STEP_ROWS", 1 << 16));

    private static readonly long LimitSetting = ParseLimit(Environment.GetEnvironmentVariable("DBSPNET_MEMORY_LIMIT_MIB"));

    private static volatile int _level = (int)(Pinned ?? PressureLevel.Low);
    private static int _epoch = Pinned >= PressureLevel.High ? 1 : 0;

    // Tests pin a level in their async context (worker threads inherit it); until one does, a static read.
    private static readonly AsyncLocal<PressureLevel?> Override = new();
    private static volatile bool _overridden;

    /// <summary>Level changes since start, and the most used bytes seen (diagnostics).</summary>
    internal static long Changes, PeakUsedBytes, LastUsedBytes;

    /// <summary>Called on each level change (host logging); runs on the finalizer thread.</summary>
    internal static Action<PressureLevel, PressureLevel, long, long>? Changed { get; set; }

    static MemoryPressure()
    {
        if (Monitoring)
        {
            _ = new Sentinel();
        }
    }

    /// <summary>The current level: one volatile load.</summary>
    public static PressureLevel Current =>
        _overridden && Override.Value is { } o ? o : (PressureLevel)_level;

    /// <summary>Bumped on each entry into High or Critical from below: a circuit that last pushed in an older
    /// epoch pushes its managed levels at its next operator boundary.</summary>
    public static int Epoch => Volatile.Read(ref _epoch);

    /// <summary>The limit pressure is measured against, in bytes.</summary>
    public static long LimitBytes => LimitSetting > 0 ? LimitSetting : GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;

    /// <summary>One line for a host's start-up log.</summary>
    public static string Describe() => string.Create(
        CultureInfo.InvariantCulture,
        $"pressure={(Pinned is { } p ? "pinned " + p : Monitoring ? "on" : "off")} limit_mib={LimitBytes >> 20} levels={Levels.Moderate:P0}/{Levels.High:P0}/{Levels.Critical:P0} high_freeze={HighFreezeEntries} critical_step_rows={CriticalStepRows}");

    /// <summary>Pin the level in the current async context until disposed (tests), bumping the epoch as a
    /// real entry into High would.</summary>
    public static IDisposable Use(PressureLevel level)
    {
        var previous = Override.Value;
        _overridden = true;
        Override.Value = level;
        if (level >= PressureLevel.High && (previous ?? (PressureLevel)_level) < PressureLevel.High)
        {
            Interlocked.Increment(ref _epoch);
        }

        return new Restore(() => Override.Value = previous);
    }

    /// <summary>The raw level of <paramref name="used"/> bytes against <paramref name="limit"/>.</summary>
    internal static PressureLevel Raw(long used, long limit, (double Moderate, double High, double Critical) levels)
    {
        if (limit <= 0)
        {
            return PressureLevel.Low;
        }

        var f = (double)used / limit;
        return f >= levels.Critical ? PressureLevel.Critical
            : f >= levels.High ? PressureLevel.High
            : f >= levels.Moderate ? PressureLevel.Moderate
            : PressureLevel.Low;
    }

    /// <summary>Feldera's hysteresis: Low and Moderate follow the raw level; High holds until it is Low;
    /// Critical holds while it is High.</summary>
    internal static PressureLevel Next(PressureLevel previous, PressureLevel raw) => previous switch
    {
        PressureLevel.High => raw switch
        {
            PressureLevel.Critical => PressureLevel.Critical,
            PressureLevel.Low => PressureLevel.Low,
            _ => PressureLevel.High,
        },
        PressureLevel.Critical => raw >= PressureLevel.High ? PressureLevel.Critical : raw,
        _ => raw,
    };

    // Once per collection, on the finalizer thread.
    private static void Update()
    {
        var info = GC.GetGCMemoryInfo(GCKind.Any);
        var used = Math.Max(0, info.HeapSizeBytes - info.FragmentedBytes);
        LastUsedBytes = used;
        if (used > PeakUsedBytes)
        {
            PeakUsedBytes = used;
        }

        var limit = LimitSetting > 0 ? LimitSetting : info.TotalAvailableMemoryBytes;
        var previous = (PressureLevel)_level;
        var next = Next(previous, Raw(used, limit, Levels));
        if (next == previous)
        {
            return;
        }

        if (previous < PressureLevel.High && next >= PressureLevel.High)
        {
            Interlocked.Increment(ref _epoch);
        }

        _level = (int)next;
        Changes++;
        try
        {
            Changed?.Invoke(previous, next, used, limit);
        }
        catch
        {
            // A logging hook must not take the finalizer thread down.
        }
    }

    internal static (double, double, double) ParseLevels(string? setting)
    {
        var parts = (setting ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 3
            && double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out var m)
            && double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out var h)
            && double.TryParse(parts[2], NumberStyles.Float, CultureInfo.InvariantCulture, out var c)
            && m > 0 && m <= h && h <= c)
        {
            return (m / 100, h / 100, c / 100);
        }

        return (0.85, 0.90, 0.95);
    }

    private static long ParseLimit(string? setting) =>
        long.TryParse(setting, NumberStyles.Integer, CultureInfo.InvariantCulture, out var mib) && mib > 0 ? mib << 20 : 0;

    private static int NonNegative(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v >= 0 ? v : fallback;

    // Dies in the next collection; its finalizer measures and makes the next one.
    private sealed class Sentinel
    {
        ~Sentinel()
        {
            if (Environment.HasShutdownStarted)
            {
                return;
            }

            try
            {
                Update();
            }
            catch
            {
                // Never let a measurement fault stop the monitor.
            }

            _ = new Sentinel();
        }
    }

    private sealed class Restore(Action undo) : IDisposable
    {
        public void Dispose() => undo();
    }
}
