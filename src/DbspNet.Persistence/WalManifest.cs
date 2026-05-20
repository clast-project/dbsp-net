// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.IO.Hashing;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DbspNet.Core.IO;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;

namespace DbspNet.Persistence;

/// <summary>
/// One segment of the WAL — a single recording session's worth of data
/// across all tables. A WAL accumulates segments over the lifetime of a
/// circuit; replay iterates them in id order.
/// </summary>
/// <remarks>
/// <para><see cref="StartTick"/> is the absolute tick number of the
/// segment's first batch — i.e. the cumulative tick count of all
/// preceding segments. The hybrid snapshot/WAL replay path uses it to
/// fast-skip whole segments whose entire range was already captured by a
/// snapshot. Schema-version 1 manifests didn't carry it; on read it's
/// reconstructed cumulatively.</para>
/// </remarks>
public sealed record WalSegment(
    [property: JsonPropertyName("id")] int Id,
    [property: JsonPropertyName("ticks")] int Ticks,
    [property: JsonPropertyName("start_tick")] long StartTick);

/// <summary>
/// Top-level WAL manifest. Pinned to a schema-version + plan-fingerprint;
/// the recorder verifies these match on reopen. Written via
/// <see cref="ITableFileSystem.WriteAllBytesAsync"/>.
/// </summary>
public sealed record WalManifest(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("plan_fingerprint")] string PlanFingerprint,
    [property: JsonPropertyName("tables")] IReadOnlyList<string> Tables,
    [property: JsonPropertyName("segments")] IReadOnlyList<WalSegment> Segments)
{
    /// <summary>
    /// v1 → v2 added <see cref="WalSegment.StartTick"/>. v2 manifests are
    /// read by older builds as v1 (with default 0 StartTick), which would
    /// silently break hybrid replay — so the recorder rejects unknown
    /// schema versions on reopen.
    /// </summary>
    public const int CurrentSchemaVersion = 2;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
    };

    public static async ValueTask<WalManifest> ReadAsync(
        ITableFileSystem fs, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        ArgumentNullException.ThrowIfNull(key);
        var bytes = await fs.ReadAllBytesAsync(key, cancellationToken).ConfigureAwait(false);
        var raw = JsonSerializer.Deserialize<WalManifest>(bytes, JsonOptions)
            ?? throw new InvalidDataException($"WAL manifest at '{key}' is empty");

        // v1 manifests pre-date the StartTick field; reconstruct it
        // cumulatively from segment Ticks and promote to v2 in memory. This
        // keeps existing on-disk WALs openable after the schema bump; the
        // next WriteAsync persists the promotion.
        if (raw.SchemaVersion == 1)
        {
            var rewritten = new WalSegment[raw.Segments.Count];
            long running = 0;
            for (var i = 0; i < raw.Segments.Count; i++)
            {
                var s = raw.Segments[i];
                rewritten[i] = new WalSegment(s.Id, s.Ticks, running);
                running += s.Ticks;
            }

            return raw with { SchemaVersion = CurrentSchemaVersion, Segments = rewritten };
        }

        return raw;
    }

    public ValueTask WriteAsync(ITableFileSystem fs, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        ArgumentNullException.ThrowIfNull(key);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(this, JsonOptions);
        return fs.WriteAllBytesAsync(key, bytes, cancellationToken);
    }

    /// <summary>
    /// Convenience overload for tests / debugging that operate on local
    /// filesystem paths.
    /// </summary>
    public static ValueTask<WalManifest> ReadAsync(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        var dir = Path.GetDirectoryName(path) ?? ".";
        var name = Path.GetFileName(path);
        return ReadAsync(new LocalTableFileSystem(dir), name);
    }

    /// <summary>
    /// Compute a stable fingerprint of a query's input table schemas. The
    /// fingerprint changes if any table's column set, types, or
    /// nullabilities change — but is independent of the SELECT body, so an
    /// (A) input WAL stays valid across query refactors that don't touch
    /// the input schema.
    /// </summary>
    public static string ComputePlanFingerprint(CompiledQuery query)
    {
        var sb = new StringBuilder();
        var orderedTables = query.Inputs.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();
        foreach (var name in orderedTables)
        {
            sb.Append(name).Append(":[");
            var schema = query.Inputs[name].Schema;
            for (var c = 0; c < schema.Count; c++)
            {
                if (c > 0)
                {
                    sb.Append(',');
                }

                var col = schema[c];
                sb.Append(col.Name).Append(' ').Append(col.Type.Display);
            }

            sb.Append("];");
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        var hash = XxHash3.HashToUInt64(bytes);
        return hash.ToString("x16", System.Globalization.CultureInfo.InvariantCulture);
    }
}
