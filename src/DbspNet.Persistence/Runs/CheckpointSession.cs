// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// One checkpoint's run bookkeeping (docs/design-durable-state.md §1f, increment 3): which runs each
/// snapshot directory's run lists name (written to <see cref="RefsFile"/> so the sweep can find
/// them without opening every file), and what to do once the checkpoint commits — restart each
/// trace's change tracking on its new chain. Tracking restarts only after the commit, so a
/// checkpoint that fails leaves every trace's pending change intact for the next one.
/// </summary>
internal sealed class CheckpointSession
{
    /// <summary>Per snapshot (worker) directory: every run its run lists name.</summary>
    internal const string RefsFile = "runs.txt";

    private readonly object _gate = new();
    private readonly List<(string Prefix, IReadOnlyList<string> Paths)> _refs = new();
    private readonly List<Action> _onCommit = new();

    internal void Reference(string prefix, IReadOnlyList<string> paths)
    {
        lock (_gate)
        {
            _refs.Add((prefix, paths));
        }
    }

    internal void OnCommit(Action action)
    {
        lock (_gate)
        {
            _onCommit.Add(action);
        }
    }

    /// <summary>Write <c>{dir}/runs.txt</c>: every run named by a run list under <paramref name="dir"/>.
    /// Written for every snapshot directory, even with none, so its absence means a snapshot from
    /// before runs existed.</summary>
    internal async ValueTask WriteRefsAsync(ITableFileSystem fs, string dir, CancellationToken cancellationToken)
    {
        var under = dir.EndsWith('/') ? dir : dir + "/";
        var paths = new SortedSet<string>(StringComparer.Ordinal);
        lock (_gate)
        {
            foreach (var (prefix, list) in _refs)
            {
                if (prefix.StartsWith(under, StringComparison.Ordinal))
                {
                    paths.UnionWith(list);
                }
            }
        }

        var bytes = Encoding.UTF8.GetBytes(string.Concat(paths.Select(p => p + "\n")));
        await using var file = await fs.CreateAsync(under + RefsFile, overwrite: true, cancellationToken).ConfigureAwait(false);
        await file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        await file.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The checkpoint committed: restart tracking on the chains just written.</summary>
    internal void Commit()
    {
        lock (_gate)
        {
            foreach (var action in _onCommit)
            {
                action();
            }

            _onCommit.Clear();
        }
    }
}
