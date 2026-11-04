// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.IO.Local;

namespace DbspNet.Persistence.Blocks;

/// <summary>
/// Run files that back a trace which no checkpoint writes (the output connector's row index,
/// docs/design-bounded-memory.md §3.9), so an evicting <see cref="BlockCache"/> can drop its blocks.
/// Not state: the files live in a private scratch directory, are written after each batch for the
/// runs that have none, are deleted once merges retire their runs, and go with the process.
/// </summary>
internal sealed class BlockSpill
{
    private static readonly string Root = Path.Combine(
        Environment.GetEnvironmentVariable("DBSPNET_BLOCK_SPILL_DIR") is { Length: > 0 } dir ? dir : Path.GetTempPath(),
        "dbspnet-spill-" + Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static int _registered;

    /// <summary>This process's spill root.</summary>
    internal static string RootPath => Root;

    /// <summary>Delete the spill root at process exit (once).</summary>
    internal static void RegisterCleanup()
    {
        if (Interlocked.Exchange(ref _registered, 1) == 0)
        {
            AppDomain.CurrentDomain.ProcessExit += (_, _) =>
            {
                try
                {
                    Directory.Delete(Root, recursive: true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            };
        }
    }

    private readonly LocalTableFileSystem _fs;
    private readonly HashSet<string> _files = new(StringComparer.Ordinal);

    private BlockSpill(string dir) => _fs = new LocalTableFileSystem(dir);

    /// <summary>A spill for one trace, or null when no cache can evict.</summary>
    public static BlockSpill? Create()
    {
        if (!BlockCache.Enabled)
        {
            return null;
        }

        RegisterCleanup();
        var dir = Path.Combine(Root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return new BlockSpill(dir);
    }

    /// <summary>Write every run of <paramref name="trace"/> that has no file here and rebind it to
    /// its file; delete the files of runs merged away since.</summary>
    public void Spill(BlockedIndexedTrace trace, BlockSchema schema)
    {
        foreach (var run in trace.Runs.ToList())
        {
            if (run.Persisted is not null)
            {
                continue; // a file already (here, a checkpoint's, or a mid-batch spill's, §5.9)
            }

            var path = Guid.NewGuid().ToString("N") + ".run";
            BlockedTraceCheckpoint.SpillRun(_fs, path, schema.Codec, trace, run);
            _files.Add(path);
        }

        var live = trace.Runs.Select(r => r.Persisted?.Path).OfType<string>().ToHashSet(StringComparer.Ordinal);
        foreach (var stale in _files.Where(f => !live.Contains(f)).ToList())
        {
            _fs.DeleteAsync(stale).AsTask().GetAwaiter().GetResult();
            _files.Remove(stale);
        }
    }

    /// <summary>Files held (tests).</summary>
    internal int Files => _files.Count;

    /// <summary>Delete every file (the trace was replaced).</summary>
    public void Clear()
    {
        foreach (var f in _files)
        {
            _fs.DeleteAsync(f).AsTask().GetAwaiter().GetResult();
        }

        _files.Clear();
    }
}
