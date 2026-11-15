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

    /// <summary>
    /// Per-operator exchange accounting for the operator profile (<see cref="RootCircuit.ProfileOperators"/>):
    /// while a profiled circuit steps an operator, its sink is current on that thread, and the exchange
    /// records below also add to that operator's barrier wait and row counts.
    /// </summary>
    internal sealed class OperatorSink(int operators)
    {
        public readonly long[] WaitTicks = new long[operators];
        public readonly long[] SplitRows = new long[operators];
        public readonly long[] GatherRows = new long[operators];

        /// <summary>Per-operator exchange phase costs, <see cref="XPhases"/> slots per operator
        /// (<c>[op * XPhases + phase]</c>): Stopwatch ticks, bytes allocated on this thread, and
        /// GC pause (TimeSpan ticks) that elapsed inside the phase.</summary>
        public readonly long[] PhaseTicks = new long[operators * XPhases];
        public readonly long[] PhaseAlloc = new long[operators * XPhases];
        public readonly long[] PhasePause = new long[operators * XPhases];
        public int Index;

        public void Clear()
        {
            Array.Clear(WaitTicks);
            Array.Clear(SplitRows);
            Array.Clear(GatherRows);
            Array.Clear(PhaseTicks);
            Array.Clear(PhaseAlloc);
            Array.Clear(PhasePause);
        }
    }

    /// <summary>Exchange sub-phases recorded per operator for the operator profile (docs/ivm-bench-gap-analysis.md,
    /// exchange decomposition). <see cref="Split"/>, <see cref="Wait"/> and <see cref="Gather"/> partition the
    /// operator's step; <see cref="Map"/> (the fused pre-step) is part of <see cref="Split"/>, and <see cref="Key"/>
    /// (the gather's key projection) and <see cref="Build"/> (the gather's final index build) are parts of
    /// <see cref="Gather"/>. <see cref="Map"/> and <see cref="Key"/> are timed per row, so they are recorded only
    /// under <see cref="PhaseDetail"/>.</summary>
    internal enum XPhase
    {
        Map = 0,
        Split = 1,
        Wait = 2,
        Key = 3,
        Gather = 4,
        Build = 5,
    }

    internal const int XPhases = 6;

    /// <summary>Time the per-row sub-phases (<see cref="XPhase.Map"/>, <see cref="XPhase.Key"/>) as well:
    /// <c>DBSPNET_PROFILE_XPHASE=1</c>. Adds a few timer reads per row, which land in the enclosing phase.</summary>
    internal static readonly bool PhaseDetail = Environment.GetEnvironmentVariable("DBSPNET_PROFILE_XPHASE") == "1";

    /// <summary>A phase start: timestamp, this thread's allocated bytes, and the process's total GC pause.</summary>
    internal readonly struct PhaseMark(long ticks, long alloc, long pause)
    {
        public readonly long Ticks = ticks;
        public readonly long Alloc = alloc;
        public readonly long Pause = pause;

        public static PhaseMark Now() =>
            new(Stopwatch.GetTimestamp(), GC.GetAllocatedBytesForCurrentThread(), GC.GetTotalPauseDuration().Ticks);
    }

    /// <summary>Charge the time since <paramref name="since"/> to <paramref name="phase"/> of the profiled
    /// operator on this thread (if any), and restart the mark.</summary>
    internal static void RecordPhase(XPhase phase, ref PhaseMark since)
    {
        var now = PhaseMark.Now();
        AddPhase(phase, now.Ticks - since.Ticks, now.Alloc - since.Alloc, now.Pause - since.Pause);
        since = now;
    }

    /// <summary>Add raw totals (accumulated by a per-row loop) to <paramref name="phase"/>.</summary>
    internal static void AddPhase(XPhase phase, long ticks, long alloc, long pause)
    {
        if (_operator is { } op)
        {
            var at = (op.Index * XPhases) + (int)phase;
            op.PhaseTicks[at] += ticks;
            op.PhaseAlloc[at] += alloc;
            op.PhasePause[at] += pause;
        }
    }

    [ThreadStatic]
    private static OperatorSink? _operator;

    /// <summary>The profiled operator stepping on this thread, if any.</summary>
    internal static OperatorSink? CurrentOperator
    {
        get => _operator;
        set => _operator = value;
    }

    /// <summary>Whether an exchange should time its phases: the per-worker profiler is on, or a
    /// profiled circuit is stepping it.</summary>
    internal static bool Active => Enabled || _operator is not null;

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
        if (Enabled)
        {
            _splitTicks[worker] += ticks;
            _splitRows[worker] += rows;
        }

        if (_operator is { } op)
        {
            op.SplitRows[op.Index] += rows;
        }
    }

    internal static void RecordWait(int worker, long ticks)
    {
        if (Enabled)
        {
            _waitTicks[worker] += ticks;
        }

        if (_operator is { } op)
        {
            op.WaitTicks[op.Index] += ticks;
        }
    }

    internal static void RecordGather(int worker, long ticks, long rows)
    {
        if (Enabled)
        {
            _gatherTicks[worker] += ticks;
            _gatherRows[worker] += rows;
        }

        if (_operator is { } op)
        {
            op.GatherRows[op.Index] += rows;
        }
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
