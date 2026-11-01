// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime;

namespace DbspNet.Persistence;

/// <summary>
/// Turns background (concurrent) GC off for the duration of a checkpoint or restore
/// (docs/design-durable-state.md §1b, "the slow first checkpoint is background-GC waiting").
/// </summary>
/// <remarks>
/// <para>The save path allocates ~11 GiB of mostly large buffers per 4.6 GiB checkpoint on
/// ivm-bench SF=3. While a background gen-2 collection runs, every large-object allocation waits
/// for it, so a checkpoint that lands on one stalls ~3 s per collection — invisible to
/// <see cref="GC.GetTotalPauseDuration"/>. <see cref="GCLatencyMode.Batch"/> makes those
/// collections ordinary blocking ones, which are shorter and land where nothing waits on them: no
/// step runs during a checkpoint or restore (both runners await them between ticks).</para>
/// <para>The mode is process-wide, so anything else running concurrently — a server answering
/// reads, a runner decoding its next batch ahead — sees blocking full collections for that time.
/// Scopes nest (a reference count), so overlapping checkpoints restore the caller's mode only when
/// the last one ends.</para>
/// <para>On by default since 2026-10-31: ivm-bench SF=3, 5 runs per arm, first checkpoint
/// W=12 11.5 → 5.1 s, W=1 15.5 → 9.4 s; restore W=12 7.2 → 5.5 s, W=1 10.9 → 7.1 s; drain
/// unchanged (design-durable-state §1b). <c>DBSPNET_CHECKPOINT_GC=default</c> opts out.</para>
/// </remarks>
internal static class CheckpointGcMode
{
    internal static readonly bool Enabled =
        Environment.GetEnvironmentVariable("DBSPNET_CHECKPOINT_GC") != "default";

    private static readonly object Gate = new();
    private static int _depth;
    private static GCLatencyMode _saved;

    /// <summary>
    /// After a lazy restore (runs layout, docs/design-durable-state.md §1f): one blocking, compacting
    /// collection, while the heap is still small. Without it the first allocation-heavy work after the
    /// restore — the first full output write — took ~1 s of GC pauses on ivm-bench SF=3 (first write
    /// 2.4–2.8 s against 1.3 s; with it 1.34–1.39 s, for a collection of ~0.18 s at the restored heap
    /// of ~5.7 GiB). An eager restore leaves a heap of tens of GiB, where the same collection costs
    /// ~1 s and buys nothing, so it is skipped there. <c>DBSPNET_RESTORE_GC=0</c> opts out.
    /// </summary>
    internal static void AfterLazyRestore()
    {
        if (!Runs.StateLayout.Runs || !Runs.RunColdGroups.Enabled
            || Environment.GetEnvironmentVariable("DBSPNET_RESTORE_GC") == "0"
            || GC.GetTotalMemory(forceFullCollection: false) < (1L << 30)) // a small heap has no debt worth paying early
        {
            return;
        }

        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
    }

    public static Scope Enter()
    {
        if (!Enabled)
        {
            return default;
        }

        lock (Gate)
        {
            if (_depth++ == 0)
            {
                _saved = GCSettings.LatencyMode;
                GCSettings.LatencyMode = GCLatencyMode.Batch;
            }
        }

        return new Scope(active: true);
    }

    internal readonly struct Scope(bool active) : IDisposable
    {
        public void Dispose()
        {
            if (!active)
            {
                return;
            }

            lock (Gate)
            {
                if (--_depth == 0)
                {
                    GCSettings.LatencyMode = _saved;
                }
            }
        }
    }
}
