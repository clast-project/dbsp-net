// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Runs a live lazily restored state still reads (docs/design-durable-state.md §1f, increments 4–6).
/// A cold source pins its chain's runs for as long as it is reachable; the sweep never deletes a
/// pinned run. Without this, a checkpoint that rewrites a cold state whole (a new base) stops
/// referencing the runs it was restored from, the sweep deletes them, and the next cold read fails.
/// Pins are weak: once the state has been read whole (or the program is dropped) the source is
/// collected and its runs are swept by a later checkpoint.
/// </summary>
internal static class RunPins
{
    private sealed record Pin(ITableFileSystem Store, IReadOnlyList<string> Paths);

    private static readonly ConditionalWeakTable<object, Pin> Pins = new();

    internal static void Add(object holder, ITableFileSystem store, IReadOnlyList<string> paths) =>
        Pins.AddOrUpdate(holder, new Pin(store, paths));

    /// <summary>Every run a live cold source over <paramref name="fs"/> still reads.</summary>
    internal static HashSet<string> In(ITableFileSystem fs)
    {
        var pinned = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, pin) in Pins)
        {
            if (SameStore(pin.Store, fs))
            {
                pinned.UnionWith(pin.Paths);
            }
        }

        return pinned;
    }

    private static bool SameStore(ITableFileSystem a, ITableFileSystem b) =>
        ReferenceEquals(a, b)
        || (a is IO.Local.LocalTableFileSystem la && b is IO.Local.LocalTableFileSystem lb
            && string.Equals(la.RootPath, lb.RootPath, StringComparison.Ordinal));
}
