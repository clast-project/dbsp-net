// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.IO;

namespace DbspNet.Persistence.Runs;

/// <summary>
/// One checkpoint's run bookkeeping (docs/design-durable-state.md §1f, increment 3): the chain each
/// trace file now stands for (written per snapshot directory as one <see cref="RunIndex"/>), and what
/// to do once the checkpoint commits — restart each trace's change tracking on its new chain.
/// Tracking restarts only after the commit, so a checkpoint that fails leaves every trace's pending
/// change intact for the next one.
/// </summary>
internal sealed class CheckpointSession
{
    private readonly object _gate = new();
    private readonly List<(string Path, RunChain Chain)> _chains = new();
    private readonly List<Action> _onCommit = new();

    /// <summary>The trace file at <paramref name="path"/> (store-absolute) is <paramref name="chain"/>.</summary>
    internal void Record(string path, RunChain chain)
    {
        lock (_gate)
        {
            _chains.Add((path, chain));
        }
    }

    internal void OnCommit(Action action)
    {
        lock (_gate)
        {
            _onCommit.Add(action);
        }
    }

    /// <summary>Write <c>{dir}/runs.txt</c>: the chain of every trace file under <paramref name="dir"/>.</summary>
    internal async ValueTask WriteIndexAsync(ITableFileSystem fs, string dir, CancellationToken cancellationToken)
    {
        var under = dir.EndsWith('/') ? dir : dir + "/";
        List<(string, RunChain)> mine;
        lock (_gate)
        {
            mine = _chains.Where(c => c.Path.StartsWith(under, StringComparison.Ordinal))
                .Select(c => (c.Path[under.Length..], c.Chain)).ToList();
        }

        var bytes = RunIndex.Encode(mine);
        await using var file = await fs.CreateAsync(under + RunIndex.FileName, overwrite: true, cancellationToken).ConfigureAwait(false);
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
