// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// Mark-and-sweep for shared runs (docs/design-durable-state.md §1f, increment 3). Runs outlive the
/// snapshot that wrote them — later snapshots' chains extend it — so pruning a snapshot directory
/// cannot delete them. After each prune, every run under <c>runs/</c> that no retained snapshot's
/// <see cref="CheckpointSession.RefsFile"/> names is deleted.
/// </summary>
/// <remarks>
/// Safe against crashes at any point: the sweep runs only after a commit, deletes only files no
/// retained snapshot names, and a snapshot's refs file is durable before its manifest, so a
/// complete snapshot always names its runs. A snapshot directory with no refs file was written
/// without a session (runs off, or re-shard), and its trace files are self-contained.
/// </remarks>
internal static class RunSweep
{
    internal const string RunsPrefix = "runs/";

    internal static async ValueTask SweepAsync(
        ITableFileSystem fs, IEnumerable<string> retainedSnapDirs, CancellationToken cancellationToken)
    {
        // Nothing to do unless some run exists: one listing of an (often absent) prefix.
        var runs = new List<string>();
        await foreach (var entry in fs.ListAsync(RunsPrefix, cancellationToken).ConfigureAwait(false))
        {
            runs.Add(entry.Path);
        }

        if (runs.Count == 0)
        {
            return;
        }

        var live = new HashSet<string>(StringComparer.Ordinal);
        foreach (var dir in retainedSnapDirs)
        {
            await foreach (var entry in fs.ListAsync(dir + "/", cancellationToken).ConfigureAwait(false))
            {
                if (!entry.Path.EndsWith("/" + CheckpointSession.RefsFile, StringComparison.Ordinal))
                {
                    continue;
                }

                var bytes = await fs.ReadAllBytesAsync(entry.Path, cancellationToken).ConfigureAwait(false);
                foreach (var line in Encoding.UTF8.GetString(bytes).Split('\n', StringSplitOptions.RemoveEmptyEntries))
                {
                    live.Add(line);
                }
            }
        }

        foreach (var path in runs)
        {
            if (live.Contains(path))
            {
                continue;
            }

            // Best-effort, like the prune: a leftover is retried by the next sweep.
            try
            {
                await fs.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
            }
            catch (IOException)
            {
            }
        }
    }
}
