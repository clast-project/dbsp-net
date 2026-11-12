// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether a run of LEFT joins against small dimensions, every key computed from the run's input
/// row, compiles as one star operator (docs/design-bounded-memory.md §5.48). On by default;
/// <c>DBSPNET_STAR_JOIN=off</c> or setting <see cref="Enabled"/> compiles the chain of joins, and
/// with it checkpoints written before the star restore.
/// </summary>
internal static class StarJoinMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_STAR_JOIN")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Null = the default. A [ThreadStatic] field's zero value is what every thread sees,
    // so a default of "on" has to live behind a nullable.
    [ThreadStatic]
    private static bool? _enabled;

    /// <summary>Star operators compiled since start (diagnostics).</summary>
    internal static long Built;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }
}
