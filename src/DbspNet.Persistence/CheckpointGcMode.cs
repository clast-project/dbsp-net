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
/// <para>EXPERIMENT seam: on only with <c>DBSPNET_CHECKPOINT_GC=batch</c> until measured.</para>
/// </remarks>
internal static class CheckpointGcMode
{
    internal static readonly bool Enabled =
        Environment.GetEnvironmentVariable("DBSPNET_CHECKPOINT_GC") == "batch";

    private static readonly object Gate = new();
    private static int _depth;
    private static GCLatencyMode _saved;

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
