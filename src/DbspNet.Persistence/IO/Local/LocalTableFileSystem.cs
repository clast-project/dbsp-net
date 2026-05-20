// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.IO.Local;

/// <summary>
/// <see cref="ITableFileSystem"/> implementation for local filesystems.
/// Uses <see cref="LocalRandomAccessFile"/> and <see cref="LocalSequentialFile"/>
/// for file handle operations.
/// </summary>
public sealed class LocalTableFileSystem : ITableFileSystem
{
    private readonly string _rootPath;
    private readonly BufferAllocator? _allocator;

    /// <summary>
    /// Creates a new local filesystem rooted at the specified directory.
    /// All paths are resolved relative to this root.
    /// </summary>
    public LocalTableFileSystem(string rootPath, BufferAllocator? allocator = null)
    {
        _rootPath = Path.GetFullPath(rootPath);
        _allocator = allocator;
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_rootPath))
            yield break;

        // Enumerate every file under the root, then filter by full-relative-path
        // prefix. This is correct for both filename prefixes ("table_part_") and
        // directory prefixes ("snap-5/", "snap-") — the upstream's search-pattern
        // approach only handles the filename case. Persistence-layer file counts
        // are small enough that the full-tree walk is negligible.
        var entries = Directory.EnumerateFiles(_rootPath, "*", SearchOption.AllDirectories)
            .OrderBy(p => p, StringComparer.Ordinal);

        foreach (string fullPath in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string relativePath = Path.GetRelativePath(_rootPath, fullPath)
                .Replace('\\', '/');

            if (!relativePath.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var info = new FileInfo(fullPath);
            yield return new TableFileInfo(
                relativePath,
                info.Length,
                new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero));
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);
        return new ValueTask<IRandomAccessFile>(
            new LocalRandomAccessFile(fullPath, _allocator));
    }

    /// <inheritdoc/>
    public ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);

        if (!overwrite && File.Exists(fullPath))
            throw new IOException($"File already exists: {path}");

        string? directory = Path.GetDirectoryName(fullPath);
        if (directory is not null)
            Directory.CreateDirectory(directory);

        return new ValueTask<ISequentialFile>(new LocalSequentialFile(fullPath));
    }

    /// <inheritdoc/>
    public ValueTask<bool> RenameAsync(
        string sourcePath, string targetPath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullSource = ResolvePath(sourcePath);
        string fullTarget = ResolvePath(targetPath);

        if (File.Exists(fullTarget))
            return new ValueTask<bool>(false);

        string? directory = Path.GetDirectoryName(fullTarget);
        if (directory is not null)
            Directory.CreateDirectory(directory);

        File.Move(fullSource, fullTarget);
        return new ValueTask<bool>(true);
    }

    /// <inheritdoc/>
    public ValueTask DeleteAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);

        if (File.Exists(fullPath))
            File.Delete(fullPath);

        return default;
    }

    /// <inheritdoc/>
    public ValueTask<bool> ExistsAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<bool>(File.Exists(ResolvePath(path)));
    }

    /// <inheritdoc/>
    public ValueTask<byte[]> ReadAllBytesAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<byte[]>(File.ReadAllBytes(ResolvePath(path)));
    }

    /// <inheritdoc/>
    public ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string fullPath = ResolvePath(path);

        string? directory = Path.GetDirectoryName(fullPath);
        if (directory is not null)
            Directory.CreateDirectory(directory);

        File.WriteAllBytes(fullPath, data.Span.ToArray());
        return default;
    }

    private string ResolvePath(string relativePath) =>
        Path.GetFullPath(Path.Combine(_rootPath, relativePath.Replace('/', Path.DirectorySeparatorChar)));
}
