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

    public TableFileSystemSnapshotContext(ITableFileSystem fs, string prefix)
    {
        _fs = fs;
        // Normalise: ensure prefix ends with '/' so file keys concatenate cleanly.
        _prefix = prefix.EndsWith('/') ? prefix : prefix + "/";
    }

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

    public ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return _fs.ExistsAsync(_prefix + filename, cancellationToken);
    }
}
