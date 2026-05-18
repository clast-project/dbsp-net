using System.Buffers;
using DbspNet.Core.IO;
using Microsoft.Win32.SafeHandles;

namespace DbspNet.Persistence.IO.Local;

/// <summary>
/// <see cref="IRandomAccessFile"/> implementation for local files using the
/// <see cref="RandomAccess"/> API. Supports fully concurrent offset-based reads
/// with no shared position cursor.
/// </summary>
public sealed class LocalRandomAccessFile : IRandomAccessFile
{
    private readonly SafeFileHandle _handle;
    private readonly BufferAllocator _allocator;
    private long _cachedLength = -1;

    public LocalRandomAccessFile(string path, BufferAllocator? allocator = null)
    {
        _allocator = allocator ?? PooledBufferAllocator.Default;
        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read,
            FileShare.Read);
    }

    public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_cachedLength >= 0)
            return new ValueTask<long>(_cachedLength);

        long length = RandomAccess.GetLength(_handle);
        _cachedLength = length;
        return new ValueTask<long>(length);
    }

    public ValueTask<IMemoryOwner<byte>> ReadAsync(
        FileRange range, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (range.Length == 0)
            return new ValueTask<IMemoryOwner<byte>>(_allocator.Allocate(0));

        IMemoryOwner<byte> buffer = _allocator.Allocate(checked((int)range.Length));
        try
        {
            ReadExact(buffer.Memory.Span, range.Offset);
            return new ValueTask<IMemoryOwner<byte>>(buffer);
        }
        catch
        {
            buffer.Dispose();
            throw;
        }
    }

    public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
        IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (ranges.Count == 0)
            return new ValueTask<IReadOnlyList<IMemoryOwner<byte>>>(
                (IReadOnlyList<IMemoryOwner<byte>>)[]);

        // For local files, sequential sync reads are fastest: no thread-pool
        // scheduling overhead and the OS page cache serves contiguous ranges efficiently.
        var buffers = new IMemoryOwner<byte>[ranges.Count];
        try
        {
            for (int i = 0; i < ranges.Count; i++)
            {
                var range = ranges[i];
                if (range.Length == 0)
                {
                    buffers[i] = _allocator.Allocate(0);
                    continue;
                }

                var buf = _allocator.Allocate(checked((int)range.Length));
                try
                {
                    ReadExact(buf.Memory.Span, range.Offset);
                    buffers[i] = buf;
                }
                catch
                {
                    buf.Dispose();
                    throw;
                }
            }

            return new ValueTask<IReadOnlyList<IMemoryOwner<byte>>>(
                (IReadOnlyList<IMemoryOwner<byte>>)buffers);
        }
        catch
        {
            foreach (IMemoryOwner<byte>? buf in buffers)
                buf?.Dispose();
            throw;
        }
    }

    private void ReadExact(Span<byte> buffer, long offset)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int bytesRead = RandomAccess.Read(
                _handle, buffer[totalRead..], offset + totalRead);

            if (bytesRead == 0)
                throw new IOException(
                    $"Unexpected end of file at offset {offset + totalRead}. " +
                    $"Expected {buffer.Length} bytes starting at offset {offset}.");

            totalRead += bytesRead;
        }
    }

    public void Dispose() => _handle.Dispose();

    public ValueTask DisposeAsync()
    {
        _handle.Dispose();
        return default;
    }
}
