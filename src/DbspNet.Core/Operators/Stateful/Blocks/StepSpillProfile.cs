// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// Where in-step delta spilling spends its time and allocation (docs/design-bounded-memory.md §5.16):
/// with <c>DBSPNET_STEP_SPILL_PROFILE=1</c> each phase is a scope charged its wall time and the bytes
/// its thread allocated, EXCLUSIVE of nested scopes (a spill inside a chunk freeze is charged to the
/// spill). Off by default and free when off (a static readonly flag the JIT folds).
/// </summary>
internal static class StepSpillProfile
{
    public static readonly bool Enabled = Environment.GetEnvironmentVariable("DBSPNET_STEP_SPILL_PROFILE") == "1";

    public const int ChunkAdd = 0;      // adds into a spilling builder's managed chunk
    public const int ChunkFreeze = 1;   // a chunk frozen into a run (sort, encode, geometric merges)
    public const int BuildCount = 2;    // a built set's exact count: the hash-only walk over its runs (§5.17)
    public const int Enumerate = 3;     // decoding a block-backed set's rows for a reader
    public const int Probe = 4;         // WeightOf / Contains on a block-backed set
    public const int PlusMerge = 5;     // a sum with a block-backed set (run merge)
    public const int Copy = 6;          // a block-backed set copied through a builder or into a managed map
    public const int Spill = 7;         // S1 writing runs to files (any trace)
    public const int RunMerge = 8;      // a two-way run merge (any trace; nested in freeze / final merge)

    private static readonly string[] Names = ["chunk_add", "chunk_freeze", "build_count", "enumerate", "probe", "plus_merge", "copy", "spill", "run_merge"];

    private static readonly long[] Ticks = new long[Names.Length];
    private static readonly long[] Bytes = new long[Names.Length];
    private static readonly long[] Calls = new long[Names.Length];

    /// <summary>Rows decoded for readers, enumerations started, rows built as blocks (read amplification).</summary>
    public static long RowsDecoded, Enumerations;

    /// <summary>The operator this thread is stepping (set by the circuit when profiling), so each phase is
    /// also charged to the operator that ran it (§5.39); -1 outside a step.</summary>
    [ThreadStatic]
    public static int CurrentOp;

    private const int MaxOps = 8192;
    private static readonly long[] OpBytes = new long[MaxOps * Names.Length];
    private static readonly long[] OpCalls = new long[MaxOps * Names.Length];
    private static readonly long[] OpRowsDecoded = new long[MaxOps];

    /// <summary>A row decoded for a reader, charged to the running operator too.</summary>
    public static void RowDecoded(long n = 1)
    {
        Interlocked.Add(ref RowsDecoded, n);
        if ((uint)CurrentOp < MaxOps)
        {
            Interlocked.Add(ref OpRowsDecoded[CurrentOp], n);
        }
    }

    [ThreadStatic]
    private static long _childTicks;

    [ThreadStatic]
    private static long _childBytes;

    /// <summary>Open a scope; pass the result to <see cref="End"/>.</summary>
    public static Mark Begin()
    {
        var m = new Mark(Stopwatch.GetTimestamp(), GC.GetAllocatedBytesForCurrentThread(), _childTicks, _childBytes);
        _childTicks = 0;
        _childBytes = 0;
        return m;
    }

    public static void End(int slot, in Mark m)
    {
        var ticks = Stopwatch.GetTimestamp() - m.Ticks;
        var bytes = GC.GetAllocatedBytesForCurrentThread() - m.Bytes;
        Interlocked.Add(ref Ticks[slot], ticks - _childTicks);
        Interlocked.Add(ref Bytes[slot], bytes - _childBytes);
        Interlocked.Increment(ref Calls[slot]);
        if ((uint)CurrentOp < MaxOps)
        {
            Interlocked.Add(ref OpBytes[CurrentOp * Names.Length + slot], bytes - _childBytes);
            Interlocked.Increment(ref OpCalls[CurrentOp * Names.Length + slot]);
        }

        _childTicks = m.ParentChildTicks + ticks;
        _childBytes = m.ParentChildBytes + bytes;
    }

    /// <summary>One line, and reset.</summary>
    public static string TakeReport()
    {
        var sb = new StringBuilder("STEPPROF");
        for (var i = 0; i < Names.Length; i++)
        {
            var t = Interlocked.Exchange(ref Ticks[i], 0);
            var b = Interlocked.Exchange(ref Bytes[i], 0);
            var c = Interlocked.Exchange(ref Calls[i], 0);
            sb.Append(System.Globalization.CultureInfo.InvariantCulture, $" {Names[i]}={t * 1000.0 / Stopwatch.Frequency:F0}ms/{b / (double)(1L << 30):F2}GiB/{c}");
        }

        sb.Append(System.Globalization.CultureInfo.InvariantCulture, $" rows_decoded={Interlocked.Exchange(ref RowsDecoded, 0)} enumerations={Interlocked.Exchange(ref Enumerations, 0)}");
        return sb.ToString();
    }

    /// <summary>One STEPOP line per operator whose phases allocated at least <paramref name="minBytes"/>
    /// since the last call (GiB and calls per phase, rows decoded), and reset.</summary>
    public static IEnumerable<string> TakeOpReport(long minBytes)
    {
        var lines = new List<string>();
        for (var op = 0; op < MaxOps; op++)
        {
            long total = 0;
            for (var i = 0; i < Names.Length; i++)
            {
                total += Volatile.Read(ref OpBytes[op * Names.Length + i]);
            }

            var rows = Interlocked.Exchange(ref OpRowsDecoded[op], 0);
            if (total < minBytes)
            {
                for (var i = 0; i < Names.Length; i++)
                {
                    Interlocked.Exchange(ref OpBytes[op * Names.Length + i], 0);
                    Interlocked.Exchange(ref OpCalls[op * Names.Length + i], 0);
                }

                continue;
            }

            var sb = new StringBuilder();
            sb.Append(System.Globalization.CultureInfo.InvariantCulture, $"STEPOP op={op} total_gib={total / (double)(1L << 30):F2}");
            for (var i = 0; i < Names.Length; i++)
            {
                var b = Interlocked.Exchange(ref OpBytes[op * Names.Length + i], 0);
                var c = Interlocked.Exchange(ref OpCalls[op * Names.Length + i], 0);
                if (c > 0)
                {
                    sb.Append(System.Globalization.CultureInfo.InvariantCulture, $" {Names[i]}={b / (double)(1L << 30):F2}/{c}");
                }
            }

            sb.Append(System.Globalization.CultureInfo.InvariantCulture, $" rows_decoded={rows}");
            lines.Add(sb.ToString());
        }

        return lines;
    }

    public readonly record struct Mark(long Ticks, long Bytes, long ParentChildTicks, long ParentChildBytes);
}
