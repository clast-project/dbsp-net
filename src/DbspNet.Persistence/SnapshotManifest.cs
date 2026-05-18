using System.IO.Hashing;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DbspNet.Core.Circuit;

namespace DbspNet.Persistence;

/// <summary>
/// Top-level snapshot manifest. One per snapshot directory. Atomic write
/// via tmp+rename. The plan fingerprint is hashed over the circuit's
/// operator types in order — adding, removing, or reordering operators
/// changes the fingerprint and makes the snapshot refuse to load.
/// </summary>
public sealed record SnapshotManifest(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("plan_fingerprint")] string PlanFingerprint,
    [property: JsonPropertyName("schema_fingerprint")] string SchemaFingerprint,
    [property: JsonPropertyName("tick")] long Tick,
    [property: JsonPropertyName("operator_count")] int OperatorCount,
    [property: JsonPropertyName("snapshotted_indices")] IReadOnlyList<int> SnapshottedIndices)
{
    /// <summary>
    /// v1 → v2: added <see cref="SchemaFingerprint"/>. v1 caught operator-
    /// type drift only; v2 also catches schema drift (VARCHAR length,
    /// DECIMAL precision/scale, intermediate column reorders) that the
    /// operator-type fingerprint misses.
    /// </summary>
    public const int CurrentSchemaVersion = 2;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
    };

    public static SnapshotManifest Read(IBlobStore store, string key)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(key);
        using var stream = store.OpenRead(key);
        using var reader = new StreamReader(stream, System.Text.Encoding.UTF8);
        var json = reader.ReadToEnd();
        return JsonSerializer.Deserialize<SnapshotManifest>(json, JsonOptions)
            ?? throw new InvalidDataException($"snapshot manifest at '{key}' is empty");
    }

    /// <summary>
    /// Convenience overload for tests / debugging that operate on local
    /// filesystem paths. Resolves the path's parent as the blob-store
    /// root and the filename as the key.
    /// </summary>
    public static SnapshotManifest Read(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        var dir = Path.GetDirectoryName(path) ?? ".";
        var name = Path.GetFileName(path);
        return Read(new LocalFileBlobStore(dir), name);
    }

    public void Write(IBlobStore store, string key)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(key);
        // The blob store's atomic-single-blob-write contract handles
        // the tmp+rename equivalent internally; we just stream JSON.
        using var stream = store.OpenWrite(key);
        using var writer = new StreamWriter(stream, System.Text.Encoding.UTF8);
        writer.Write(JsonSerializer.Serialize(this, JsonOptions));
    }

    /// <summary>
    /// Hash the operator list's runtime types. Captures structural
    /// position drift: same plan → same operator order → same
    /// fingerprint. Generic operators include their type arguments in
    /// <see cref="Type.FullName"/>, so e.g. an aggregate over
    /// <c>(int, decimal, decimal)</c> hashes differently from one over
    /// <c>(long, decimal, decimal)</c>.
    /// </summary>
    internal static string ComputeCircuitFingerprint(RootCircuit circuit)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            var op = circuit.Operators[i];
            sb.Append(i).Append(':').Append(op.GetType().FullName).Append(';');
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        var hash = XxHash3.HashToUInt64(bytes);
        return hash.ToString("x16", System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Hash every snapshottable operator's <see cref="ISnapshotable.SchemaFingerprint"/>
    /// in positional order. Catches schema drift the operator-type
    /// fingerprint misses — e.g. a VARCHAR(8) column widened to
    /// VARCHAR(16) leaves operator types and Arrow types unchanged but
    /// changes the codec's <see cref="SqlType.Display"/>-based
    /// fingerprint.
    /// </summary>
    internal static string ComputeSchemaFingerprint(RootCircuit circuit)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is not ISnapshotable s)
            {
                continue;
            }

            sb.Append(i).Append(':').Append(s.SchemaFingerprint).Append(';');
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        var hash = XxHash3.HashToUInt64(bytes);
        return hash.ToString("x16", System.Globalization.CultureInfo.InvariantCulture);
    }
}
