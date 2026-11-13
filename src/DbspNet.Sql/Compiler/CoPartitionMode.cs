// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Sql.Compiler;

/// <summary>
/// Co-partitioning (docs/design-bounded-memory.md §5.57): at W&gt;1 the aggregates and DISTINCTs under a
/// hashed LEFT join partition on the join's bare key columns instead of their own whole key, so the join
/// elides both of its exchanges, and two of them over one input share one exchange. On by default;
/// <c>DBSPNET_COPARTITION=off</c>, or the thread-static <see cref="Enabled"/> setter (tests).
/// </summary>
internal static class CoPartitionMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_COPARTITION")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

    // Null = the default ([ThreadStatic] fields start at their zero value on every thread).
    [ThreadStatic]
    private static bool? _enabled;

    internal static bool Enabled
    {
        get => _enabled ?? !EnvironmentOff;
        set => _enabled = value;
    }

    /// <summary>Clear the override (tests).</summary>
    internal static void Reset() => _enabled = null;
}
