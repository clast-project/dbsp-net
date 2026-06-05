// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.IO;

/// <summary>
/// Re-roots an <see cref="ITableFileSystem"/> under a fixed key prefix: every key
/// is transparently prepended with the prefix, and <see cref="ListAsync"/> strips
/// it back off so callers see paths relative to the prefixed root. Lets snapshot
/// machinery written against a flat root operate inside a subtree (e.g. one
/// worker's slice of a parallel snapshot) with no awareness of the prefix.
/// </summary>
internal sealed class PrefixedTableFileSystem : ITableFileSystem
{
    private readonly ITableFileSystem _inner;
    private readonly string _prefix;

    public PrefixedTableFileSystem(ITableFileSystem inner, string prefix)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(prefix);
        _inner = inner;
        _prefix = prefix.EndsWith('/') ? prefix : prefix + "/";
    }

    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var entry in _inner.ListAsync(_prefix + prefix, cancellationToken).ConfigureAwait(false))
        {
            // Inner returns paths relative to the inner root (prefix-included);
            // hand back paths relative to this re-rooted view.
            var stripped = entry.Path.StartsWith(_prefix, StringComparison.Ordinal)
                ? entry.Path[_prefix.Length..]
                : entry.Path;
            yield return entry with { Path = stripped };
        }
    }

    public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.OpenReadAsync(_prefix + path, cancellationToken);

    public ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
        _inner.CreateAsync(_prefix + path, overwrite, cancellationToken);

    public ValueTask<bool> RenameAsync(
        string sourcePath, string targetPath, CancellationToken cancellationToken = default) =>
        _inner.RenameAsync(_prefix + sourcePath, _prefix + targetPath, cancellationToken);

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.DeleteAsync(_prefix + path, cancellationToken);

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.ExistsAsync(_prefix + path, cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
        _inner.ReadAllBytesAsync(_prefix + path, cancellationToken);

    public ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        _inner.WriteAllBytesAsync(_prefix + path, data, cancellationToken);
}
