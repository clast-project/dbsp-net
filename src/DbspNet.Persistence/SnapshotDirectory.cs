// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;

namespace DbspNet.Persistence;

/// <summary>
/// <see cref="ITableFileSystem"/>-backed implementation of
/// <see cref="ISnapshotWriter"/> and <see cref="ISnapshotReader"/>.
/// One context wraps an <see cref="ITableFileSystem"/> and a key prefix
/// (the operator's subdirectory inside the snapshot tree) — operators
/// read and write whatever named artifacts they need under that prefix.
/// </summary>
internal sealed class TableFileSystemSnapshotContext : ISnapshotWriter, ISnapshotReader
{
    private readonly ITableFileSystem _fs;
    private readonly string _prefix;

    public TableFileSystemSnapshotContext(ITableFileSystem fs, string prefix, Runs.CheckpointSession? session = null)
    {
        _fs = fs;
        // Normalise: ensure prefix ends with '/' so file keys concatenate cleanly.
        _prefix = prefix.EndsWith('/') ? prefix : prefix + "/";
        Session = session;
    }

    /// <summary>The store, for files outside this prefix (runs, which snapshots share).</summary>
    internal ITableFileSystem FileSystem => _fs;

    /// <summary>This context's key prefix, ending in '/'.</summary>
    internal string Prefix => _prefix;

    /// <summary>The checkpoint this writer belongs to, when it may write incremental runs
    /// (docs/design-durable-state.md §1f, increment 3); null otherwise.</summary>
    internal Runs.CheckpointSession? Session { get; }

    public ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return _fs.CreateAsync(_prefix + filename, overwrite: true, cancellationToken);
    }

    public ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return _fs.OpenReadAsync(_prefix + filename, cancellationToken);
    }

    public async ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return await _fs.ExistsAsync(_prefix + filename, cancellationToken).ConfigureAwait(false)
            || await ChainAsync(filename, cancellationToken).ConfigureAwait(false) is not null;
    }

    // The run index of the directory this operator directory sits in ("{dir}/op-{i}/" → "{dir}/runs.txt"),
    // read once per context (docs/design-durable-state.md §1f, increment 3).
    private Task<Runs.RunIndex?>? _index;

    /// <summary>The chain the parent directory's run index records for <paramref name="filename"/>, or null.</summary>
    internal async ValueTask<Runs.RunChain?> ChainAsync(string filename, CancellationToken cancellationToken = default)
    {
        var trimmed = _prefix.TrimEnd('/');
        var slash = trimmed.LastIndexOf('/');
        if (slash < 0)
        {
            return null;
        }

        _index ??= Runs.RunIndex.ReadAsync(_fs, trimmed[..slash], cancellationToken).AsTask();
        var index = await _index.ConfigureAwait(false);
        return index?.Find(trimmed[(slash + 1)..] + "/" + filename, _fs);
    }
}
