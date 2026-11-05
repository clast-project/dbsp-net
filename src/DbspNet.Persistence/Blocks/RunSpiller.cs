// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.IO.Local;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// Mid-batch spilling, S1a (docs/design-bounded-memory.md §5.9): once a freeze or merge leaves a
/// blocked trace with a run of at least <see cref="SpillSettings.SpillMinBytes"/> that has no file,
/// write it to a scratch run file and rebind it there, so the block cache can evict its blocks —
/// Feldera's low-pressure mode (merge outputs of 10 MiB or more built on storage). The files are a
/// cache, not state: a checkpoint still writes the run into its own store (S1b will let it name the
/// spilled file instead). A file is deleted as soon as nothing reads it (§5.20): each carries a
/// <see cref="RunFileLease"/> its trace releases when the run leaves its levels (merged away, or
/// replaced by the checkpoint's rebind), and that lazy readers hold until they are collected.
/// </summary>
internal sealed class RunSpiller : IRunSpiller
{
    private readonly LocalTableFileSystem _fs;
    private readonly long _minBytes;
    private readonly List<RunFileLease> _files = [];
    private int _sinceSweep;

    /// <summary>Runs spilled, and their block bytes, since start (diagnostics).</summary>
    internal static long Runs, Bytes;

    /// <summary>Spill files on disk now, their bytes, and the most bytes on disk at once; files
    /// deleted at a holder's release, and by a sweep after a collection (diagnostics, §5.20).</summary>
    internal static long LiveFiles, LiveFileBytes, PeakFileBytes, DeletedOnRelease, DeletedBySweep;

    [ThreadStatic]
    private static bool _sweeping;

    internal RunSpiller(string dir, long minBytes)
    {
        Directory.CreateDirectory(dir);
        _fs = new LocalTableFileSystem(dir);
        _minBytes = minBytes;
    }

    /// <summary>Bytes of the files on disk by their strongest live holder (§5.20): a state trace that lists
    /// the run, else a block-backed delta's own trace, else a lazy reader; Dead: no holder alive (a sweep
    /// deletes it). Read after a full collection, Dead is what only a GC frees.</summary>
    internal (long State, long Delta, long Reader, long Dead, int Files) Census()
    {
        RunFileLease[] leases;
        lock (_files)
        {
            leases = [.. _files];
        }

        long state = 0, delta = 0, reader = 0, dead = 0;
        var files = 0;
        foreach (var lease in leases)
        {
            if (lease.Deleted)
            {
                continue;
            }

            files++;
            var holders = lease.AliveHolders();
            if (holders.Any(static h => h is BlockedIndexedTrace { Compacts: true }))
            {
                state += lease.Bytes;
            }
            else if (holders.Any(static h => h is BlockedIndexedTrace))
            {
                delta += lease.Bytes;
            }
            else if (holders.Count > 0)
            {
                reader += lease.Bytes;
            }
            else
            {
                dead += lease.Bytes;
            }
        }

        return (state, delta, reader, dead, files);
    }

    /// <summary>Files on disk (tests).</summary>
    internal int Files
    {
        get
        {
            lock (_files)
            {
                return _files.Count(static l => !l.Deleted);
            }
        }
    }

    public void AfterCompact(BlockedIndexedTrace trace)
    {
        var m = StepSpillProfile.Enabled ? StepSpillProfile.Begin() : default;
        AfterCompactCore(trace);
        if (StepSpillProfile.Enabled)
        {
            StepSpillProfile.End(StepSpillProfile.Spill, m);
        }
    }

    private void AfterCompactCore(BlockedIndexedTrace trace)
    {
        if (!BlockCache.Enabled || trace.Format is not BlockSchema schema)
        {
            return; // without eviction a spill only adds a write
        }

        var runs = trace.Runs;
        for (var r = 0; r < runs.Count; r++)
        {
            var run = runs[r];
            if (run.Persisted is not null || Size(run) is var bytes && bytes < _minBytes)
            {
                continue;
            }

            var path = Guid.NewGuid().ToString("N") + ".run";
            if (BlockedTraceCheckpoint.SpillRun(_fs, path, schema.Codec, trace, run) is { } rebound)
            {
                var length = new FileInfo(_fs.FullPath(path)).Length;
                var lease = new RunFileLease(() => Delete(path, length)) { Bytes = length };
                rebound.Lease = lease;
                lease.Hold(trace);
                lock (_files)
                {
                    _files.Add(lease);
                }

                Interlocked.Increment(ref Runs);
                Interlocked.Add(ref Bytes, bytes);
                Interlocked.Increment(ref LiveFiles);
                var live = Interlocked.Add(ref LiveFileBytes, length);
                long peak;
                while (live > (peak = Interlocked.Read(ref PeakFileBytes)) && Interlocked.CompareExchange(ref PeakFileBytes, live, peak) != peak)
                {
                }
            }
            else
            {
                _fs.DeleteAsync(path).AsTask().GetAwaiter().GetResult();
            }
        }

        if (++_sinceSweep >= 64)
        {
            _sinceSweep = 0;
            Sweep();
        }
    }

    // A run's in-memory block bytes; a block already lazy counts nothing.
    private static long Size(BlockedIndexedTrace.Run run)
    {
        long bytes = 0;
        foreach (var b in run.Blocks)
        {
            if (b is TraceBlock t)
            {
                bytes += t.ResidentBytes;
            }
        }

        return bytes;
    }

    // The lease's last holder let go: the file goes now.
    private void Delete(string path, long length)
    {
        _fs.DeleteAsync(path).AsTask().GetAwaiter().GetResult();
        Interlocked.Decrement(ref LiveFiles);
        Interlocked.Add(ref LiveFileBytes, -length);
        if (_sweeping)
        {
            Interlocked.Increment(ref DeletedBySweep);
        }
        else
        {
            Interlocked.Increment(ref DeletedOnRelease);
        }
    }

    /// <summary>Delete the files whose every holder is gone without a release — readers collected,
    /// or a trace dropped whole (tests call it after a GC).</summary>
    internal void Sweep()
    {
        RunFileLease[] leases;
        lock (_files)
        {
            _files.RemoveAll(static l => l.Deleted);
            leases = [.. _files];
        }

        _sweeping = true;
        try
        {
            foreach (var lease in leases)
            {
                lease.TryDelete();
            }
        }
        finally
        {
            _sweeping = false;
        }

        lock (_files)
        {
            _files.RemoveAll(static l => l.Deleted);
        }
    }

    /// <summary>The process's spiller under <c>DBSPNET_BLOCK_SPILL_DIR</c> (default temp), removed at exit.</summary>
    internal static RunSpiller CreateDefault()
    {
        var dir = Path.Combine(BlockSpill.RootPath, "runs");
        BlockSpill.RegisterCleanup();
        return new RunSpiller(dir, SpillSettings.SpillMinBytes);
    }
}
