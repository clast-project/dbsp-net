// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Collections;

/// <summary>
/// Opt-in measurement of <b>how much restored state a resumed pipeline actually touches</b>
/// (docs/design-incremental-persistence.md §11, question 2). Lazy / file-backed restore only
/// removes work if the ticks after a resume probe a small share of each trace's keys; if they
/// scan, laziness defers cost rather than removing it. This counts, per trace-state collection:
/// key probes, distinct keys probed, and whole-collection enumerations.
/// </summary>
/// <remarks>
/// <para><b>Free when off.</b> <see cref="Enabled"/> is a <see langword="static"/>
/// <see langword="readonly"/> read of an environment variable, so with
/// <c>DBSPNET_TRACE_ACCESS_PROFILE</c> unset the JIT folds every call site in
/// <see cref="ZSet{TKey,TWeight}"/> / <see cref="IndexedZSet{TKey,TValue,TWeight}"/> away —
/// the join probe path keeps its exact shipping code. Nothing here is on a hot path in a
/// normal process.</para>
/// <para>Only collections a trace marks via <c>MarkTraceState</c> are counted: a delta Z-set is
/// enumerated constantly and is not state, so counting it would drown the signal.</para>
/// </remarks>
internal static class TraceAccessProfile
{
    /// <summary>Set <c>DBSPNET_TRACE_ACCESS_PROFILE=1</c> in the environment before the process
    /// starts. Static readonly so the JIT can treat it as a constant.</summary>
    internal static readonly bool Enabled =
        Environment.GetEnvironmentVariable("DBSPNET_TRACE_ACCESS_PROFILE") is "1" or "true" or "TRUE";

    /// <summary>Armed window: counting only happens between <see cref="Arm"/> and
    /// <see cref="Disarm"/>, so a restore's own construction is not charged to the replay.</summary>
    internal static bool Counting;

    private static readonly List<TraceAccessCounter> Counters = new();

    // Side table rather than a field on the collection: a field would grow every ZSet and every
    // per-key inner group by 8 bytes, which the w1profile B/ev instrument sees (+2..4 B/event on the
    // join queries). Off, the lookup folds away with Enabled; on, a ConditionalWeakTable probe is
    // irrelevant next to what is being measured.
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<object, TraceAccessCounter> Table = new();

    internal static void Mark(object collection, string kind, Func<int> stateKeys)
    {
        if (!Enabled)
        {
            return;
        }

        var c = new TraceAccessCounter(kind, stateKeys);
        Table.AddOrUpdate(collection, c);
        lock (Counters)
        {
            Counters.Add(c);
        }
    }

    internal static TraceAccessCounter? For(object collection) =>
        Table.TryGetValue(collection, out var c) ? c : null;

    internal static void Arm() => Counting = true;

    internal static void Disarm() => Counting = false;

    internal static IReadOnlyList<TraceAccessCounter> Snapshot()
    {
        lock (Counters)
        {
            return Counters.ToArray();
        }
    }

    internal static void ResetCounts()
    {
        lock (Counters)
        {
            foreach (var c in Counters)
            {
                c.Reset();
            }
        }
    }
}

/// <summary>Per-collection access tallies; see <see cref="TraceAccessProfile"/>.</summary>
internal sealed class TraceAccessCounter(string kind, Func<int> stateKeys)
{
    private readonly HashSet<object> _touched = new();

    public string Kind { get; } = kind;

    /// <summary>Keys held right now — the denominator of the touch fraction.</summary>
    public int StateKeys => stateKeys();

    public long Probes { get; private set; }

    public long Hits { get; private set; }

    /// <summary>Times the whole collection was enumerated (a scan touches everything).</summary>
    public long Scans { get; private set; }

    public int DistinctProbed => _touched.Count;

    public void Probe(object key, bool hit)
    {
        Probes++;
        if (hit)
        {
            Hits++;
        }

        _touched.Add(key);
    }

    public void Scan() => Scans++;

    public void Reset()
    {
        Probes = 0;
        Hits = 0;
        Scans = 0;
        _touched.Clear();
    }
}
