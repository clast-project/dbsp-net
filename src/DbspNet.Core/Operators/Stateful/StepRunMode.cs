// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// One record run per step for the bulk-load fast path (docs/design-bounded-memory.md §5.53): a sliced
/// step's slices are contiguous ranges of the partition-key hash, stepped in ascending order, so their
/// new partitions' runs concatenate into one run of the record, published at the step's end, instead of
/// a run per slice merged up the levels. On by default; <c>DBSPNET_STEP_RUN=off</c> (mixed-hash slices,
/// a run per slice), or the thread-static <see cref="Enabled"/> setter (tests, the <c>topkbulk</c>
/// stand-in). Read when the operator and its store are built.
/// </summary>
internal static class StepRunMode
{
    private static readonly bool EnvironmentOff = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_STEP_RUN")?.Trim(), "off", StringComparison.OrdinalIgnoreCase);

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
