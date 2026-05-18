namespace DbspNet.Persistence;

/// <summary>
/// Storage abstraction for the persistence layer. The contract is
/// modeled on cloud object stores (S3, GCS, Azure Blob): keys are
/// slash-delimited identifiers, single-blob writes are atomic, and
/// there are no directory operations or atomic multi-blob renames.
/// A filesystem-backed implementation simulates the atomicity locally.
/// </summary>
/// <remarks>
/// <para><b>Atomic single-blob write.</b> The <see cref="Stream"/>
/// returned by <see cref="OpenWrite"/> doesn't make the blob visible
/// (or replace its prior content) until <c>Dispose</c> is called.
/// Crashes before <c>Dispose</c> leave the prior content (if any)
/// intact. The persistence layer relies on this property — it commits
/// new state by writing a small pointer blob (<c>current.txt</c>)
/// last, after every other blob is durably written.</para>
/// <para><b>No multi-blob atomicity.</b> There is no rename, no copy,
/// no batch commit. To replace state across multiple blobs, callers
/// write all the new blobs first under their final keys, then write
/// the pointer blob — partial writes are invisible until the pointer
/// commits. Cleanup of orphaned blobs from a crashed write is the
/// caller's responsibility (typically lazy: prune on next write).</para>
/// <para><b>Keys.</b> Slash-delimited, ASCII-printable, no leading
/// slash. <c>"snap-5/op-3/trace.arrows"</c> is a typical shape.
/// Implementations must preserve key contents byte-for-byte (no
/// normalization, no Unicode folding).</para>
/// </remarks>
public interface IBlobStore
{
    /// <summary>
    /// Open a stream for writing the blob at <paramref name="key"/>.
    /// The blob is committed atomically on stream <c>Dispose</c>; until
    /// then, readers see the prior value (or no blob, if none existed).
    /// Caller disposes the stream.
    /// </summary>
    Stream OpenWrite(string key);

    /// <summary>
    /// Open a stream for reading the blob at <paramref name="key"/>.
    /// Throws <see cref="FileNotFoundException"/> (or equivalent) if
    /// the blob doesn't exist. Caller disposes the stream.
    /// </summary>
    Stream OpenRead(string key);

    /// <summary>
    /// True if a blob with the given key currently exists.
    /// </summary>
    bool Exists(string key);

    /// <summary>
    /// Delete the blob at <paramref name="key"/> if it exists. No-op if
    /// it doesn't. Idempotent.
    /// </summary>
    void Delete(string key);

    /// <summary>
    /// Enumerate the full keys of every blob whose key starts with
    /// <paramref name="prefix"/>. Order is implementation-defined;
    /// callers that need a specific order must sort. On cloud stores
    /// this maps to a paginated <c>ListObjectsV2</c> call.
    /// </summary>
    IEnumerable<string> ListKeys(string prefix);
}
