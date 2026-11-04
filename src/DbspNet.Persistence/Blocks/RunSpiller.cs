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
/// spilled file instead), and a file is deleted once its run is gone (merged away, or replaced by
/// the checkpoint's rebind).
/// </summary>
internal sealed class RunSpiller : IRunSpiller
{
    private readonly LocalTableFileSystem _fs;
    private readonly long _minBytes;
    private readonly List<(WeakReference<BlockedIndexedTrace.Run> Run, string Path)> _files = [];
    private int _sinceSweep;

    /// <summary>Runs spilled, and their block bytes, since start (diagnostics).</summary>
    internal static long Runs, Bytes;

    internal RunSpiller(string dir, long minBytes)
    {
        Directory.CreateDirectory(dir);
        _fs = new LocalTableFileSystem(dir);
        _minBytes = minBytes;
    }

    /// <summary>Files held (tests).</summary>
    internal int Files
    {
        get
        {
            lock (_files)
            {
                return _files.Count;
            }
        }
    }

    public void AfterCompact(BlockedIndexedTrace trace)
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
                lock (_files)
                {
                    _files.Add((new WeakReference<BlockedIndexedTrace.Run>(rebound), path));
                }

                Interlocked.Increment(ref Runs);
                Interlocked.Add(ref Bytes, bytes);
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

    /// <summary>Delete the files whose runs are gone (tests call it after a GC).</summary>
    internal void Sweep()
    {
        List<string> dead = [];
        lock (_files)
        {
            for (var i = _files.Count - 1; i >= 0; i--)
            {
                if (!_files[i].Run.TryGetTarget(out _))
                {
                    dead.Add(_files[i].Path);
                    _files[i] = _files[^1];
                    _files.RemoveAt(_files.Count - 1);
                }
            }
        }

        foreach (var path in dead)
        {
            _fs.DeleteAsync(path).AsTask().GetAwaiter().GetResult();
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
