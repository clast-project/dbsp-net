// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;

namespace DbspNet.Core.Circuit;

/// <summary>
/// Benchmark-only per-worker decomposition of a parallel-circuit
/// <see cref="ParallelCircuit.Step"/> into its sub-phases, so the W-scaling
/// ceiling can be attributed to <b>movement</b> (the all-to-all shuffle of wide
/// rows), <b>coordination</b> (idle time at the exchange barrier), or
/// <b>operator compute</b> — and the residual load imbalance read off the spread
/// of per-worker busy time (docs/design-row-representation.md §15).
/// </summary>
/// <remarks>
/// <para>Each worker thread accumulates, per Step, the time it spends in three
/// exchange phases — <c>split</c> (bucket this shard's rows by
/// <c>hash(key)%W</c>), <c>wait</c> (block at the per-exchange rendezvous, i.e.
/// idle waiting for the slowest peer to finish splitting + the barrier's own
/// latency), and <c>gather</c> (read its column and rebuild the post-shuffle
/// indexed Z-set, re-hashing full rows) — plus the whole replica's
/// <see cref="RootCircuit.Step"/> wall time. The residual
/// <c>op = step − split − wait − gather</c> is the actual operator work
/// (join / aggregate / TOP-K). A query with several exchanges sums their phases
/// into the same per-worker slot, so the figures are the step-total movement and
/// idle cost, not a single exchange's.</para>
/// <para>Default <see cref="Enabled"/> is <see langword="false"/>; only the
/// parallel benchmark driver turns it on. The W worker threads write disjoint
/// <c>[worker]</c> array slots during a Step (no write/write race) and the
/// controlling thread reads them after the end-of-tick barrier publishes the
/// writes — so unlike <c>SpineStagingConfig</c> no thread-static scoping is
/// needed (it is never enabled from the racing xUnit test threads). When
/// disabled every instrumented site pays only one predictable-branch
/// <see cref="bool"/> test.</para>
/// </remarks>
internal static class StepProfiler
{
    private static long[] _stepTicks = [];
    private static long[] _splitTicks = [];
    private static long[] _waitTicks = [];
    private static long[] _gatherTicks = [];
    private static long[] _splitRows = [];
    private static long[] _gatherRows = [];

    /// <summary>Master switch — leave <see langword="false"/> outside benchmarks.</summary>
    internal static bool Enabled { get; set; }

    /// <summary>Number of completed steps recorded since the last <see cref="Configure"/>.</summary>
    internal static long Steps { get; private set; }

    /// <summary>Stopwatch tick frequency, for converting the raw tick totals to seconds.</summary>
    internal static long Frequency => Stopwatch.Frequency;

    /// <summary>The W this profiler is currently sized for.</summary>
    internal static int Workers => _stepTicks.Length;

    /// <summary>
    /// Size the per-worker accumulators for a <paramref name="workers"/>-wide run
    /// and zero them. Call once before a measured pass, with the profiler enabled.
    /// </summary>
    internal static void Configure(int workers)
    {
        _stepTicks = new long[workers];
        _splitTicks = new long[workers];
        _waitTicks = new long[workers];
        _gatherTicks = new long[workers];
        _splitRows = new long[workers];
        _gatherRows = new long[workers];
        Steps = 0;
    }

    /// <summary>Reset just the accumulated totals, keeping the current width.</summary>
    internal static void Reset()
    {
        Array.Clear(_stepTicks);
        Array.Clear(_splitTicks);
        Array.Clear(_waitTicks);
        Array.Clear(_gatherTicks);
        Array.Clear(_splitRows);
        Array.Clear(_gatherRows);
        Steps = 0;
    }

    internal static void RecordSplit(int worker, long ticks, long rows)
    {
        _splitTicks[worker] += ticks;
        _splitRows[worker] += rows;
    }

    internal static void RecordWait(int worker, long ticks) => _waitTicks[worker] += ticks;

    internal static void RecordGather(int worker, long ticks, long rows)
    {
        _gatherTicks[worker] += ticks;
        _gatherRows[worker] += rows;
    }

    internal static void RecordStep(int worker, long ticks) => _stepTicks[worker] += ticks;

    /// <summary>Count one completed Step (call once per controller Step).</summary>
    internal static void CountStep() => Steps++;

    internal static long StepTicksOf(int worker) => _stepTicks[worker];

    internal static long SplitTicksOf(int worker) => _splitTicks[worker];

    internal static long WaitTicksOf(int worker) => _waitTicks[worker];

    internal static long GatherTicksOf(int worker) => _gatherTicks[worker];

    internal static long SplitRowsOf(int worker) => _splitRows[worker];

    internal static long GatherRowsOf(int worker) => _gatherRows[worker];
}
