using DbspNet.Core.Circuit;

namespace DbspNet.Persistence;

/// <summary>
/// Blob-store-backed implementation of <see cref="ISnapshotWriter"/>
/// and <see cref="ISnapshotReader"/>. One context wraps an
/// <see cref="IBlobStore"/> and a key prefix (the operator's
/// subdirectory inside the snapshot tree) — operators write whatever
/// named artifacts they need under that prefix.
/// </summary>
internal sealed class BlobStoreSnapshotContext : ISnapshotWriter, ISnapshotReader
{
    private readonly IBlobStore _store;
    private readonly string _prefix;

    public BlobStoreSnapshotContext(IBlobStore store, string prefix)
    {
        _store = store;
        // Normalise: ensure prefix ends with '/' so OpenWrite/OpenRead
        // get clean key concatenation.
        _prefix = prefix.EndsWith('/') ? prefix : prefix + "/";
    }

    public Stream OpenWrite(string filename)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return _store.OpenWrite(_prefix + filename);
    }

    public Stream OpenRead(string filename)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return _store.OpenRead(_prefix + filename);
    }

    public bool Exists(string filename)
    {
        ArgumentNullException.ThrowIfNull(filename);
        return _store.Exists(_prefix + filename);
    }
}
