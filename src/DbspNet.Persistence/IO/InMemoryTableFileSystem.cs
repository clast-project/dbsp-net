using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DbspNet.Core.IO;

namespace DbspNet.Persistence.IO;

/// <summary>
/// In-memory <see cref="ITableFileSystem"/> implementation backed by a
/// thread-safe dictionary. Useful as a test double for cloud backends
/// and as an in-process test target where filesystem I/O would slow
/// tests down.
/// </summary>
/// <remarks>
/// <para>The store is fully ephemeral — its contents disappear when
/// the instance is collected.</para>
/// <para><b>Atomic-write semantics.</b> A file produced by
/// <see cref="CreateAsync"/> becomes visible as soon as the stream
/// is created (an empty entry is added immediately) — matching the
/// upstream <c>LocalTableFileSystem</c>, which also creates the file
/// before writes begin. Use <see cref="RenameAsync"/> for atomic
/// publish (write to <c>{key}.tmp</c>, then rename).
/// <see cref="WriteAllBytesAsync"/> writes are atomic per call —
/// readers see the prior value (or absence) until the call returns.</para>
/// </remarks>
public sealed class InMemoryTableFileSystem : ITableFileSystem
{
    private readonly ConcurrentDictionary<string, FileEntry> _files = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public async IAsyncEnumerable<TableFileInfo> ListAsync(
        string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Snapshot the keys so enumeration is stable against concurrent mutation.
        var keys = _files.Keys
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_files.TryGetValue(key, out var entry))
            {
                yield return new TableFileInfo(key, entry.Bytes.Length, entry.LastModified);
            }
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask<IRandomAccessFile> OpenReadAsync(
        string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_files.TryGetValue(path, out var entry))
        {
            throw new FileNotFoundException($"file '{path}' not found", path);
        }

        return new ValueTask<IRandomAccessFile>(new InMemoryRandomAccessFile(entry.Bytes));
    }

    /// <inheritdoc/>
    public ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!overwrite && _files.ContainsKey(path))
        {
            throw new IOException($"File already exists: {path}");
        }

        // Mirror LocalTableFileSystem: the file exists immediately;
        // writes append to it as they happen.
        var entry = new FileEntry(Array.Empty<byte>(), DateTimeOffset.UtcNow);
        _files[path] = entry;
        return new ValueTask<ISequentialFile>(new InMemorySequentialFile(this, path));
    }

    /// <inheritdoc/>
    public ValueTask<bool> RenameAsync(
        string sourcePath, string targetPath,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_files.ContainsKey(targetPath))
        {
            return new ValueTask<bool>(false);
        }

        if (!_files.TryRemove(sourcePath, out var entry))
        {
            throw new FileNotFoundException($"file '{sourcePath}' not found", sourcePath);
        }

        _files[targetPath] = entry;
        return new ValueTask<bool>(true);
    }

    /// <inheritdoc/>
    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _files.TryRemove(path, out _);
        return default;
    }

    /// <inheritdoc/>
    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<bool>(_files.ContainsKey(path));
    }

    /// <inheritdoc/>
    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_files.TryGetValue(path, out var entry))
        {
            throw new FileNotFoundException($"file '{path}' not found", path);
        }

        // Defensive copy so the caller can't mutate stored bytes.
        var copy = new byte[entry.Bytes.Length];
        entry.Bytes.AsSpan().CopyTo(copy);
        return new ValueTask<byte[]>(copy);
    }

    /// <inheritdoc/>
    public ValueTask WriteAllBytesAsync(
        string path, ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var bytes = new byte[data.Length];
        data.Span.CopyTo(bytes);
        _files[path] = new FileEntry(bytes, DateTimeOffset.UtcNow);
        return default;
    }

    private void AppendBytes(string path, ReadOnlySpan<byte> data)
    {
        // Sequential-file append: replace the entry with a grown copy.
        // Not concurrency-safe across multiple writers on the same path,
        // which matches the upstream LocalSequentialFile contract.
        if (!_files.TryGetValue(path, out var existing))
        {
            throw new InvalidOperationException(
                $"file '{path}' was deleted while a sequential writer was open");
        }

        var combined = new byte[existing.Bytes.Length + data.Length];
        existing.Bytes.AsSpan().CopyTo(combined);
        data.CopyTo(combined.AsSpan(existing.Bytes.Length));
        _files[path] = new FileEntry(combined, DateTimeOffset.UtcNow);
    }

    private readonly record struct FileEntry(byte[] Bytes, DateTimeOffset LastModified);

    private sealed class InMemorySequentialFile : ISequentialFile
    {
        private readonly InMemoryTableFileSystem _fs;
        private readonly string _path;
        private long _position;

        public InMemorySequentialFile(InMemoryTableFileSystem fs, string path)
        {
            _fs = fs;
            _path = path;
        }

        public long Position => _position;

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _fs.AppendBytes(_path, data.Span);
            _position += data.Length;
            return default;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return default;
        }

        public void Dispose() { }

        public ValueTask DisposeAsync() => default;
    }

    private sealed class InMemoryRandomAccessFile : IRandomAccessFile
    {
        private readonly byte[] _bytes;

        public InMemoryRandomAccessFile(byte[] bytes) => _bytes = bytes;

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<long>(_bytes.LongLength);
        }

        public ValueTask<IMemoryOwner<byte>> ReadAsync(
            FileRange range, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (range.Offset + range.Length > _bytes.Length)
            {
                throw new IOException(
                    $"range [{range.Offset}, {range.End}) extends past end of file (length {_bytes.Length})");
            }

            var buffer = PooledBufferAllocator.Default.Allocate(checked((int)range.Length));
            _bytes.AsSpan(checked((int)range.Offset), checked((int)range.Length))
                .CopyTo(buffer.Memory.Span);
            return new ValueTask<IMemoryOwner<byte>>(buffer);
        }

        public async ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            var results = new IMemoryOwner<byte>[ranges.Count];
            try
            {
                for (int i = 0; i < ranges.Count; i++)
                {
                    results[i] = await ReadAsync(ranges[i], cancellationToken).ConfigureAwait(false);
                }
                return results;
            }
            catch
            {
                foreach (var buf in results)
                    buf?.Dispose();
                throw;
            }
        }

        public void Dispose() { }

        public ValueTask DisposeAsync() => default;
    }
}
