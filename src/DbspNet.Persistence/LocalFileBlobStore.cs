namespace DbspNet.Persistence;

/// <summary>
/// Filesystem-backed <see cref="IBlobStore"/>. Maps slash-delimited
/// keys to filesystem paths under a fixed root directory. Provides the
/// "atomic single-blob write" contract by staging writes to a sibling
/// <c>.tmp</c> file and renaming on stream <c>Dispose</c>.
/// </summary>
/// <remarks>
/// Slashes in keys map to <see cref="Path.DirectorySeparatorChar"/>;
/// intermediate directories are created on demand. Cleanup of empty
/// directories is best-effort and not strictly necessary because the
/// persistence layer doesn't depend on directory structure beyond the
/// key prefix.
/// </remarks>
public sealed class LocalFileBlobStore : IBlobStore
{
    private readonly string _root;

    public LocalFileBlobStore(string root)
    {
        ArgumentNullException.ThrowIfNull(root);
        _root = root;
        Directory.CreateDirectory(_root);
    }

    /// <summary>The root directory this store is rooted at.</summary>
    public string Root => _root;

    public Stream OpenWrite(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var path = ResolvePath(key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return new AtomicWriteStream(path);
    }

    public Stream OpenRead(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var path = ResolvePath(key);
        return new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
    }

    public bool Exists(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return File.Exists(ResolvePath(key));
    }

    public void Delete(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        var path = ResolvePath(key);
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    public IEnumerable<string> ListKeys(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        if (!Directory.Exists(_root))
        {
            yield break;
        }

        var prefixPath = ResolvePath(prefix);
        // The prefix may not be a directory boundary — it could match a
        // partial filename (e.g. "snap-" matching "snap-5/manifest.json").
        // The simplest correct approach is to enumerate every file under
        // the root, then filter by key prefix.
        var rootLen = _root.Length + (_root.EndsWith(Path.DirectorySeparatorChar) ? 0 : 1);
        foreach (var path in Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories))
        {
            if (path.Length <= rootLen)
            {
                continue;
            }

            var key = path.Substring(rootLen).Replace(Path.DirectorySeparatorChar, '/');
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                yield return key;
            }
        }
    }

    private string ResolvePath(string key) =>
        Path.Combine(_root, key.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Stages writes to <c>{path}.tmp</c> and atomically renames on
    /// <c>Dispose</c>. Provides the single-blob atomicity that
    /// <see cref="IBlobStore"/> promises.
    /// </summary>
    private sealed class AtomicWriteStream : Stream
    {
        private readonly string _finalPath;
        private readonly string _tmpPath;
        private readonly FileStream _inner;
        private bool _disposed;

        public AtomicWriteStream(string finalPath)
        {
            _finalPath = finalPath;
            _tmpPath = finalPath + ".tmp";
            _inner = new FileStream(
                _tmpPath, FileMode.Create, FileAccess.Write, FileShare.None);
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() => _inner.Flush();
        public override void Write(byte[] buffer, int offset, int count) =>
            _inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) =>
            _inner.Write(buffer);
        public override void WriteByte(byte value) => _inner.WriteByte(value);
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
                _inner.Dispose();
                if (File.Exists(_finalPath))
                {
                    File.Replace(_tmpPath, _finalPath, destinationBackupFileName: null);
                }
                else
                {
                    File.Move(_tmpPath, _finalPath);
                }
            }
            catch
            {
                // If the rename failed, drop the staging file so we don't
                // leave a corrupt half-written sibling on disk. Swallow
                // secondary errors during cleanup.
                try
                {
                    if (File.Exists(_tmpPath))
                    {
                        File.Delete(_tmpPath);
                    }
                }
                catch
                {
                }

                throw;
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
