// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// The bulk-load fast path of blocked partitioned TOP-K and rank (docs/design-bounded-memory.md §5.52):
/// a partition the store held nothing for when the step began is sorted once and written to the record as
/// part of one run, instead of being probed for, cached, queued and flushed. On by default;
/// <c>DBSPNET_TOPK_BULK=off</c>, or the thread-static <see cref="Enabled"/> setter (tests, the
/// <c>topkbulk</c> stand-in). Read when the operator is built.
/// </summary>
internal static class PartitionedBulkMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_TOPK_BULK")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

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
