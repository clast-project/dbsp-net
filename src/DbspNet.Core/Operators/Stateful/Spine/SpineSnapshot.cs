using System.Globalization;
using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Per-batch snapshot helper. Each non-empty spine batch is written
/// as its own file via a caller-supplied per-batch save delegate;
/// a small manifest file records the count so symmetric load can
/// stream the batches back without directory enumeration.
/// </summary>
/// <remarks>
/// <para>File-naming convention, given a <paramref name="prefix"/> of
/// <c>"trace"</c>:</para>
/// <list type="bullet">
///   <item><c>trace.manifest</c> — ASCII decimal count of batches.</item>
///   <item><c>trace.batch_0.arrows</c>, <c>trace.batch_1.arrows</c>,
///   … — one file per batch, ordered as the spine emitted them
///   (oldest level first).</item>
/// </list>
/// <para>Multi-trace operators (e.g. joins) pass distinct prefixes
/// (<c>"left"</c>, <c>"right"</c>) so the files don't collide.</para>
/// <para>This format is the staging point for phase 4 (disk spill):
/// each batch is already an independently addressable file.</para>
/// </remarks>
internal static class SpineSnapshot
{
    public static string ManifestFileName(string prefix) => prefix + ".manifest";

    public static string BatchFileName(string prefix, int index) =>
        prefix + ".batch_" + index.ToString(CultureInfo.InvariantCulture) + ".arrows";

    /// <summary>
    /// Writes each batch via <paramref name="saveOne"/> and a tiny manifest.
    /// </summary>
    public static async ValueTask SaveAsync<TBatch>(
        ISnapshotWriter writer,
        string prefix,
        IReadOnlyList<TBatch> batches,
        Func<string, TBatch, ValueTask> saveOne,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(batches);
        ArgumentNullException.ThrowIfNull(saveOne);

        await using (var manifest = await writer.CreateAsync(ManifestFileName(prefix), cancellationToken).ConfigureAwait(false))
        await using (var stream = manifest.AsStream())
        {
            var bytes = Encoding.ASCII.GetBytes(batches.Count.ToString(CultureInfo.InvariantCulture));
            await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        }

        for (var i = 0; i < batches.Count; i++)
        {
            await saveOne(BatchFileName(prefix, i), batches[i]).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads the manifest and yields each batch via <paramref name="loadOne"/>,
    /// in original order. If the manifest does not exist (cold snapshot),
    /// returns without invoking <paramref name="loadOne"/>.
    /// </summary>
    public static async ValueTask LoadAsync(
        ISnapshotReader reader,
        string prefix,
        Func<string, ValueTask> loadOne,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(loadOne);

        var manifestName = ManifestFileName(prefix);
        if (!await reader.ExistsAsync(manifestName, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        int count;
        await using (var file = await reader.OpenReadAsync(manifestName, cancellationToken).ConfigureAwait(false))
        await using (var stream = file.AsStream())
        {
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
            var text = Encoding.ASCII.GetString(ms.ToArray()).Trim();
            count = int.Parse(text, CultureInfo.InvariantCulture);
        }

        for (var i = 0; i < count; i++)
        {
            await loadOne(BatchFileName(prefix, i)).ConfigureAwait(false);
        }
    }
}
