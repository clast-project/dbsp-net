namespace DbspNet.Core.IO;

/// <summary>
/// Adapters that present <see cref="ISequentialFile"/> and
/// <see cref="IRandomAccessFile"/> as <see cref="Stream"/> instances, for
/// callers that integrate with legacy Stream-shaped APIs (Arrow IPC
/// writers/readers, <c>System.Text.Json.JsonSerializer</c>, etc.).
/// </summary>
public static class IoStreamAdapters
{
    /// <summary>
    /// Wraps <paramref name="file"/> as a write-only, non-seekable Stream.
    /// Disposing the stream disposes the underlying <see cref="ISequentialFile"/>.
    /// </summary>
    public static Stream AsStream(this ISequentialFile file) => new SequentialFileStream(file);

    /// <summary>
    /// Wraps <paramref name="file"/> as a read-only, seekable Stream
    /// starting at offset 0. Reads at the current position are translated
    /// into <see cref="IRandomAccessFile.ReadAsync"/> calls.
    /// Disposing the stream disposes the underlying <see cref="IRandomAccessFile"/>.
    /// </summary>
    public static Stream AsStream(this IRandomAccessFile file) => new RandomAccessFileStream(file);

    private sealed class SequentialFileStream : Stream
    {
        private readonly ISequentialFile _file;

        public SequentialFileStream(ISequentialFile file) => _file = file;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _file.Position;
        public override long Position
        {
            get => _file.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() =>
            _file.FlushAsync().AsTask().GetAwaiter().GetResult();

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            _file.FlushAsync(cancellationToken).AsTask();

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            _file.WriteAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(buffer.Length);
            try
            {
                buffer.CopyTo(rented);
                _file.WriteAsync(rented.AsMemory(0, buffer.Length))
                    .AsTask().GetAwaiter().GetResult();
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _file.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            _file.WriteAsync(buffer, cancellationToken);

        public override async ValueTask DisposeAsync()
        {
            await _file.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _file.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    private sealed class RandomAccessFileStream : Stream
    {
        private readonly IRandomAccessFile _file;
        private long _position;
        private long _length = -1;

        public RandomAccessFileStream(IRandomAccessFile file) => _file = file;

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;

        public override long Length
        {
            get
            {
                if (_length < 0)
                {
                    _length = _file.GetLengthAsync().AsTask().GetAwaiter().GetResult();
                }
                return _length;
            }
        }

        public override long Position
        {
            get => _position;
            set
            {
                ArgumentOutOfRangeException.ThrowIfNegative(value);
                _position = value;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPos = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                SeekOrigin.End => Length + offset,
                _ => throw new ArgumentOutOfRangeException(nameof(origin)),
            };
            if (newPos < 0)
            {
                throw new IOException("Cannot seek before the beginning of the file.");
            }
            _position = newPos;
            return _position;
        }

        public override void Flush() { }
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer.AsMemory(offset, count), default).AsTask().GetAwaiter().GetResult();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_length < 0)
            {
                _length = await _file.GetLengthAsync(cancellationToken).ConfigureAwait(false);
            }

            long remaining = _length - _position;
            if (remaining <= 0 || buffer.Length == 0)
            {
                return 0;
            }

            int toRead = (int)Math.Min(buffer.Length, remaining);
            using var owner = await _file.ReadAsync(
                new FileRange(_position, toRead), cancellationToken).ConfigureAwait(false);
            owner.Memory.Span[..toRead].CopyTo(buffer.Span);
            _position += toRead;
            return toRead;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask DisposeAsync()
        {
            await _file.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _file.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
