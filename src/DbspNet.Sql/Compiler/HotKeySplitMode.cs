// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Whether a hashed LEFT join whose left key has a hot default value (every row a lookup padded shares
/// it) splits on that key: those rows stay on their worker and join a broadcast of the right side's
/// group for it (docs/design-bounded-memory.md §5.49). On by default; <c>DBSPNET_HOT_KEY_SPLIT=off</c>
/// or setting <see cref="Enabled"/> compiles the plain hashed join.
/// </summary>
internal static class HotKeySplitMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_HOT_KEY_SPLIT")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Null = the default. A [ThreadStatic] field's zero value is what every thread sees,
    // so a default of "on" has to live behind a nullable.
    [ThreadStatic]
    private static bool? _enabled;

    /// <summary>Joins split on a hot key since start (diagnostics).</summary>
    internal static long Built;

    /// <summary>The hot key of the last join this thread split (tests).</summary>
    [ThreadStatic]
    internal static Core.Collections.StructuralRow? LastHotKey;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }
}
