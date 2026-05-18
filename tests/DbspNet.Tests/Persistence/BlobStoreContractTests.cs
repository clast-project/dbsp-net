using System.Text;
using DbspNet.Persistence;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Behavioral conformance suite that pins every <see cref="IBlobStore"/>
/// implementation to identical observable semantics. Each
/// implementation gets a concrete subclass that overrides
/// <see cref="CreateStore"/>; xUnit inherits the [Fact] methods, so
/// the same assertions run against every backend.
/// </summary>
public abstract class BlobStoreContractTests : IDisposable
{
    private readonly IBlobStore _store;

    protected BlobStoreContractTests()
    {
        _store = CreateStore();
    }

    /// <summary>Build a fresh, empty store for one test.</summary>
    protected abstract IBlobStore CreateStore();

    /// <summary>Override to clean up impl-specific state (e.g. temp dirs).</summary>
    protected virtual void Cleanup()
    {
    }

    public void Dispose()
    {
        Cleanup();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void OpenWrite_ThenOpenRead_RoundTripsBytes()
    {
        WriteText(_store, "hello/world.txt", "greetings");
        Assert.Equal("greetings", ReadText(_store, "hello/world.txt"));
    }

    [Fact]
    public void OpenWrite_AtomicOnDispose_BlobInvisibleUntilDispose()
    {
        // While the writer is still open, Exists must report the prior
        // state (here: false, no prior). On Dispose, the new blob
        // becomes visible. This is the load-bearing atomicity contract
        // — the persistence layer relies on it for cloud-native
        // commit (write blobs first, commit via current.txt).
        var stream = _store.OpenWrite("a.txt");
        try
        {
            using var sw = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true);
            sw.Write("partial");
            sw.Flush();
            Assert.False(_store.Exists("a.txt"));
        }
        finally
        {
            stream.Dispose();
        }

        Assert.True(_store.Exists("a.txt"));
        Assert.Equal("partial", ReadText(_store, "a.txt"));
    }

    [Fact]
    public void OpenWrite_OverwritesExistingKey_LastWriteWins()
    {
        WriteText(_store, "a.txt", "first");
        WriteText(_store, "a.txt", "second");
        Assert.Equal("second", ReadText(_store, "a.txt"));
    }

    [Fact]
    public void OpenRead_OnMissingKey_ThrowsFileNotFound()
    {
        Assert.Throws<FileNotFoundException>(() => _store.OpenRead("missing.txt"));
    }

    [Fact]
    public void Exists_OnMissingKey_ReturnsFalse()
    {
        Assert.False(_store.Exists("nope"));
    }

    [Fact]
    public void Exists_OnPresentKey_ReturnsTrue()
    {
        WriteText(_store, "present", "x");
        Assert.True(_store.Exists("present"));
    }

    [Fact]
    public void Delete_OnPresentKey_RemovesBlob()
    {
        WriteText(_store, "doomed", "x");
        _store.Delete("doomed");
        Assert.False(_store.Exists("doomed"));
    }

    [Fact]
    public void Delete_OnMissingKey_IsIdempotent()
    {
        // Two deletes in a row on a non-existent key — neither throws.
        _store.Delete("never-existed");
        _store.Delete("never-existed");
    }

    [Fact]
    public void ListKeys_EmptyStore_ReturnsEmpty()
    {
        Assert.Empty(_store.ListKeys(""));
    }

    [Fact]
    public void ListKeys_ReturnsKeysMatchingPrefix()
    {
        WriteText(_store, "snap-1/manifest.json", "{}");
        WriteText(_store, "snap-1/op-0/value.txt", "1");
        WriteText(_store, "snap-2/manifest.json", "{}");
        WriteText(_store, "current.txt", "snap-2");

        var snap1 = _store.ListKeys("snap-1/").OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "snap-1/manifest.json", "snap-1/op-0/value.txt" }, snap1);

        var snap = _store.ListKeys("snap-").OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(3, snap.Count);
        Assert.DoesNotContain("current.txt", snap);
    }

    [Fact]
    public void ListKeys_NoMatches_ReturnsEmpty()
    {
        WriteText(_store, "a.txt", "x");
        WriteText(_store, "b.txt", "y");
        Assert.Empty(_store.ListKeys("z"));
    }

    [Fact]
    public void ListKeys_AfterDelete_ExcludesDeletedKey()
    {
        WriteText(_store, "a.txt", "x");
        WriteText(_store, "b.txt", "y");
        _store.Delete("a.txt");

        var remaining = _store.ListKeys("").OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "b.txt" }, remaining);
    }

    [Fact]
    public void OpenRead_DuringInflightWriteToSameKey_SeesPriorValue()
    {
        // The atomic-on-Dispose contract implies a reader can hold a
        // stream open against the prior value while a writer is in
        // flight. After the writer disposes, *new* readers see the
        // new value. This is the cloud "object visibility" semantic.
        WriteText(_store, "k", "old");

        var writer = _store.OpenWrite("k");
        try
        {
            using (var sw = new StreamWriter(writer, Encoding.UTF8, leaveOpen: true))
            {
                sw.Write("new");
                sw.Flush();
            }

            // Reader opened mid-write sees the old value.
            using var reader = _store.OpenRead("k");
            using var sr = new StreamReader(reader, Encoding.UTF8);
            Assert.Equal("old", sr.ReadToEnd());
        }
        finally
        {
            writer.Dispose();
        }

        // After Dispose, a fresh reader sees the new value.
        Assert.Equal("new", ReadText(_store, "k"));
    }

    private static void WriteText(IBlobStore store, string key, string contents)
    {
        using var stream = store.OpenWrite(key);
        using var sw = new StreamWriter(stream, Encoding.UTF8);
        sw.Write(contents);
    }

    private static string ReadText(IBlobStore store, string key)
    {
        using var stream = store.OpenRead(key);
        using var sr = new StreamReader(stream, Encoding.UTF8);
        return sr.ReadToEnd();
    }
}

public sealed class LocalFileBlobStoreContractTests : BlobStoreContractTests
{
    private readonly string _root = Path.Combine(
        Path.GetTempPath(), "dbspnet-contract-" + Guid.NewGuid().ToString("N"));

    protected override IBlobStore CreateStore() => new LocalFileBlobStore(_root);

    protected override void Cleanup()
    {
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }
}

public sealed class InMemoryBlobStoreContractTests : BlobStoreContractTests
{
    protected override IBlobStore CreateStore() => new InMemoryBlobStore();
}
