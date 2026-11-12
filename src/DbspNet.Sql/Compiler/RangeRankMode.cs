// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether a global <c>ROW_NUMBER</c> / <c>RANK</c> (no <c>PARTITION BY</c>) at W &gt; 1 ranks in ranges of
/// its ORDER BY keys, one per worker (docs/design-bounded-memory.md §5.50), instead of gathering to
/// worker 0. On by default; <c>DBSPNET_RANGE_RANK=off</c> or <see cref="Enabled"/> gathers, and with it
/// checkpoints written before the range rank restore. Below <see cref="MinRows"/> rows
/// (<c>DBSPNET_RANGE_RANK_MIN_ROWS</c>, default 65,536) the rank stays on worker 0.
/// </summary>
internal static class RangeRankMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_RANGE_RANK")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    private static readonly long EnvironmentMinRows =
        long.TryParse(Environment.GetEnvironmentVariable("DBSPNET_RANGE_RANK_MIN_ROWS"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n > 0
            ? n
            : 65_536;

    // Null = the default. A [ThreadStatic] field's zero value is what every thread sees,
    // so a default of "on" has to live behind a nullable.
    [ThreadStatic]
    private static bool? _enabled;

    [ThreadStatic]
    private static long? _minRows;

    /// <summary>Range ranks compiled since start (diagnostics).</summary>
    internal static long Built;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }

    internal static long MinRows
    {
        get => _minRows ?? EnvironmentMinRows;
        set => _minRows = value;
    }
}
