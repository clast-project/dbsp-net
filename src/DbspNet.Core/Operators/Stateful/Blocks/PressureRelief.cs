// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// High pressure's push (docs/design-bounded-memory.md §5.28), Feldera's <c>memory_pressure_epoch</c>:
/// one per root circuit, listing (weakly) the state traces that took a managed level while it stepped.
/// At each operator boundary the circuit calls <see cref="AtBoundary"/> on its own thread. If the level is
/// High or above and the epoch is newer than the one it last pushed in, every listed trace freezes its
/// managed levels (and so spills them). Traces belong to their worker's thread, so only the stepping
/// circuit may freeze them. A spilling delta's private trace is never listed.
/// </summary>
internal sealed class PressureRelief
{
    [ThreadStatic]
    private static PressureRelief? _current;

    private readonly List<WeakReference<BlockedIndexedTrace>> _traces = [];
    private int _pruneAt = 64;
    private int _seenEpoch = MemoryPressure.Epoch;

    /// <summary>Pushes done, and managed entries they froze, since start (diagnostics).</summary>
    internal static long Pushes, PushedEntries;

    /// <summary>The circuit stepping on this thread (null outside a step).</summary>
    internal static PressureRelief? Current
    {
        get => _current;
        set => _current = value;
    }

    /// <summary>Traces listed now (tests).</summary>
    internal int Listed => _traces.Count;

    /// <summary>List <paramref name="trace"/> in the stepping circuit's relief; nothing outside a step.</summary>
    internal static void Register(BlockedIndexedTrace trace)
    {
        if (_current is not { } relief)
        {
            return;
        }

        trace.PressureRegistered = true;
        relief._traces.Add(new WeakReference<BlockedIndexedTrace>(trace));
        if (relief._traces.Count >= relief._pruneAt)
        {
            relief._traces.RemoveAll(static w => !w.TryGetTarget(out _));
            relief._pruneAt = Math.Max(64, relief._traces.Count * 2);
        }
    }

    /// <summary>At an operator boundary, on the circuit's own thread: push if a new High epoch began.</summary>
    internal void AtBoundary()
    {
        if (MemoryPressure.Current < PressureLevel.High)
        {
            return;
        }

        var epoch = MemoryPressure.Epoch;
        if (epoch == _seenEpoch)
        {
            return;
        }

        _seenEpoch = epoch;
        Push();
    }

    /// <summary>Freeze every listed trace's managed levels, and empty the list.</summary>
    internal void Push()
    {
        long entries = 0;
        foreach (var w in _traces)
        {
            if (w.TryGetTarget(out var trace))
            {
                trace.PressureRegistered = false;
                if (trace.ManagedEntries > 0)
                {
                    entries += trace.ManagedEntries;
                    trace.Freeze();
                }
            }
        }

        _traces.Clear();
        _pruneAt = 64;
        Interlocked.Increment(ref Pushes);
        Interlocked.Add(ref PushedEntries, entries);
    }
}
