// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// The bytes of file-backed blocks held in memory, bounded (docs/design-bounded-memory.md §3.1):
/// every <see cref="LazyTraceBlock"/> that has read its body is admitted here with its size, and
/// once the total passes the budget a CLOCK hand drops the bodies not read since it last passed.
/// A dropped block reads its body from its run file again on next use. Only blocks with a durable
/// copy are ever admitted, so eviction never loses state.
/// </summary>
/// <remarks>
/// <para><b>Budget.</b> <c>DBSPNET_BLOCK_CACHE_MIB</c> — process-wide, all workers. Unset (or 0):
/// unbounded, today's behaviour, and nothing is admitted.</para>
/// <para><b>Scans.</b> A body read by a merge (<see cref="Core.Operators.Stateful.Blocks.BlockAccess.InMaintenance"/>)
/// is admitted unreferenced, so the hand drops it first.</para>
/// <para><b>Safety.</b> Dropping a body only clears the block's reference to it: a reader that
/// already holds the body (a span, an aliased string) keeps it alive, and the next read loads it
/// again. Any thread may evict.</para>
/// </remarks>
internal static class BlockCache
{
    private static readonly object Gate = new();
    private static readonly List<LazyTraceBlock> Ring = new();
    private static int _hand;
    private static long _bytes;

    /// <summary>The budget in bytes; 0: unbounded (the cache is off).</summary>
    public static long Budget { get; set; } = ParseBudget(Environment.GetEnvironmentVariable("DBSPNET_BLOCK_CACHE_MIB"));

    /// <summary>Whether bodies are admitted and evicted.</summary>
    public static bool Enabled => Budget > 0;

    /// <summary>Bytes of admitted bodies now held.</summary>
    public static long Bytes => Interlocked.Read(ref _bytes);

    /// <summary>Bodies dropped since start.</summary>
    public static long Evictions { get; private set; }

    /// <summary>Bodies admitted since start (each read from a file, or bound after a checkpoint).</summary>
    public static long Admissions { get; private set; }

    internal static long ParseBudget(string? setting) =>
        long.TryParse(setting, NumberStyles.Integer, CultureInfo.InvariantCulture, out var mib) && mib > 0 ? mib << 20 : 0;

    /// <summary>Admit <paramref name="block"/>'s body of <paramref name="size"/> bytes; evict while
    /// over budget.</summary>
    public static void Admit(LazyTraceBlock block, long size, bool referenced)
    {
        lock (Gate)
        {
            block.CacheSize = size;
            block.Referenced = referenced;
            Ring.Add(block);
            _bytes += size;
            Admissions++;
            EvictOverBudget();
        }
    }

    private static void EvictOverBudget()
    {
        // Two sweeps clear every reference bit, so the loop ends.
        var budget = Budget;
        var steps = 0;
        while (_bytes > budget && Ring.Count > 0 && steps++ < (Ring.Count * 2) + 2)
        {
            if (_hand >= Ring.Count)
            {
                _hand = 0;
            }

            var b = Ring[_hand];
            if (b.Referenced)
            {
                b.Referenced = false;
                _hand++;
                continue;
            }

            // Swap-remove: the last entry takes the hand's place and is looked at next.
            Ring[_hand] = Ring[^1];
            Ring.RemoveAt(Ring.Count - 1);
            _bytes -= b.CacheSize;
            b.DropBody();
            Evictions++;
            steps = 0;
        }
    }

    // The access census (DBSPNET_BLOCK_ACCESS=1): evicted bodies, to see which stay alive anyway.
    private static readonly List<(WeakReference<byte[]> Body, long Bytes)> Dropped = new();

    internal static void TrackDropped(TraceBlock body)
    {
        lock (Dropped)
        {
            Dropped.Add((new WeakReference<byte[]>(body.Bytes), body.Bytes.Length));
        }
    }

    /// <summary>Evicted bodies still reachable (call after a full GC): count and bytes. Forgets the
    /// collected ones.</summary>
    internal static (long Count, long Bytes) DroppedAlive()
    {
        lock (Dropped)
        {
            Dropped.RemoveAll(d => !d.Body.TryGetTarget(out _));
            return (Dropped.Count, Dropped.Sum(d => d.Bytes));
        }
    }

    /// <summary>Drop every admitted body (tests).</summary>
    internal static void Clear()
    {
        lock (Gate)
        {
            foreach (var b in Ring)
            {
                b.DropBody();
            }

            Ring.Clear();
            _bytes = 0;
            _hand = 0;
        }
    }
}
