using System.Collections.Concurrent;

namespace DbspNet.Persistence;

/// <summary>
/// In-memory <see cref="IBlobStore"/> implementation backed by a
/// thread-safe dictionary. Useful as a reference impl, a test double
/// for cloud backends (the contract semantics — no atomic rename, no
/// directory ops, atomic single-blob commit on stream Dispose — match
/// real cloud stores), and an in-process test target where filesystem
/// I/O would slow tests down.
/// </summary>
/// <remarks>
/// <para>The store is fully ephemeral — its contents disappear when
/// the instance is collected. To persist, use
/// <see cref="LocalFileBlobStore"/> or a cloud-flavored
/// implementation.</para>
/// <para>Concurrency: the underlying dictionary is thread-safe; an
/// <see cref="OpenWrite"/> stream commits to the dictionary atomically
/// on <c>Dispose</c>. Concurrent readers during an in-flight write
/// continue to see the prior blob (or absence) until the writer
/// disposes — matching the cloud "object visibility" semantic.</para>
/// </remarks>
public sealed class InMemoryBlobStore : IBlobStore
{
    private readonly ConcurrentDictionary<string, byte[]> _blobs = new(StringComparer.Ordinal);

    public Stream OpenWrite(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new CommitOnDisposeStream(this, key);
    }

    public Stream OpenRead(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!_blobs.TryGetValue(key, out var bytes))
        {
            throw new FileNotFoundException($"blob '{key}' not found", key);
        }

        // A fresh read-only MemoryStream over a snapshot of the bytes —
        // mutations to the underlying blob via subsequent OpenWrite
        // don't affect already-issued readers.
        return new MemoryStream(bytes, writable: false);
    }

    public bool Exists(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _blobs.ContainsKey(key);
    }

    public void Delete(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        _blobs.TryRemove(key, out _);
    }

    public IEnumerable<string> ListKeys(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        // Snapshot the key set so the enumeration is stable against
        // concurrent mutation. Order is not guaranteed.
        foreach (var key in _blobs.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                yield return key;
            }
        }
    }

    /// <summary>
    /// Buffers writes in a <see cref="MemoryStream"/> and on
    /// <c>Dispose</c> atomically inserts the bytes into the parent
    /// store. Until <c>Dispose</c>, the destination key is invisible
    /// (or holds its prior value).
    /// </summary>
    private sealed class CommitOnDisposeStream : Stream
    {
        private readonly InMemoryBlobStore _store;
        private readonly string _key;
        private readonly MemoryStream _buffer = new();
        private bool _disposed;

        public CommitOnDisposeStream(InMemoryBlobStore store, string key)
        {
            _store = store;
            _key = key;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _buffer.Length;
        public override long Position
        {
            get => _buffer.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() => _buffer.Flush();
        public override void Write(byte[] buffer, int offset, int count) =>
            _buffer.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _buffer.Write(buffer);
        public override void WriteByte(byte value) => _buffer.WriteByte(value);
        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();
        public override void SetLength(long value) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                base.Dispose(disposing);
                return;
            }

            _disposed = true;
            try
            {
                if (disposing)
                {
                    _store._blobs[_key] = _buffer.ToArray();
                    _buffer.Dispose();
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
