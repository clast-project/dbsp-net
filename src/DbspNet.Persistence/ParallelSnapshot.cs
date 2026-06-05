// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text.Json;
using System.Text.Json.Serialization;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.IO.Local;

namespace DbspNet.Persistence;

/// <summary>
/// Per-worker snapshots for a <see cref="ParallelCircuit"/>: each of the <c>W</c>
/// replicas is snapshotted into its own <c>worker-{w}/</c> subtree via the
/// single-circuit <see cref="Snapshot"/> machinery, and a top-level
/// <c>parallel.json</c> records <c>W</c>. Recovery requires the same <c>W</c> —
/// matching Feldera's fixed-worker constraint: the hash partition
/// (<c>StableHash(key) % W</c>) is part of the persisted state, so a different
/// <c>W</c> would place keys on different replicas than the snapshot holds.
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> Under the snapshot root: one <c>parallel.json</c> plus a
/// <c>worker-{w}/</c> subtree per replica, each a self-contained single-circuit
/// snapshot (<c>current.txt</c>, <c>snap-{tick}/</c>, per-op files). The
/// <c>parallel.json</c> marker is written <em>last</em>, after every worker
/// subtree commits, so a reader that finds it can trust all <c>W</c> subtrees are
/// present.</para>
/// <para><b>Stability.</b> The per-key worker assignment is reproduced on recovery
/// only because the SQL planner partitions with
/// <see cref="DbspNet.Core.Circuit.StableHash"/> (a process-independent hash),
/// not <see cref="object.GetHashCode"/>. Same plan + same <c>W</c> ⇒ same shard
/// for every key.</para>
/// </remarks>
public static class ParallelSnapshot
{
    private const string ParallelManifestKey = "parallel.json";

    /// <summary>
    /// Snapshot every replica of <paramref name="circuit"/> into its own subtree
    /// and write the top-level <c>parallel.json</c> marker. Per-worker writes run
    /// concurrently (disjoint subtrees, disjoint replica state). Returns the total
    /// number of operators snapshotted across all workers.
    /// </summary>
    public static async ValueTask<int> WriteAsync(
        ParallelCircuit circuit,
        ITableFileSystem fs,
        int retainCount = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(fs);

        var replicas = circuit.Replicas;
        var tasks = new Task<int>[replicas.Count];
        for (var w = 0; w < replicas.Count; w++)
        {
            var workerFs = new PrefixedTableFileSystem(fs, WorkerPrefix(w));
            tasks[w] = Snapshot.WriteAsync(replicas[w], workerFs, retainCount, cancellationToken).AsTask();
        }

        var counts = await Task.WhenAll(tasks).ConfigureAwait(false);

        // Commit marker last: only after every worker subtree is durable does the
        // parallel snapshot become visible to ReadAsync.
        var manifest = new ParallelSnapshotManifest(
            ParallelSnapshotManifest.CurrentSchemaVersion, replicas.Count, circuit.TickCount);
        await manifest.WriteAsync(fs, ParallelManifestKey, cancellationToken).ConfigureAwait(false);

        var total = 0;
        foreach (var c in counts)
        {
            total += c;
        }

        return total;
    }

    /// <summary>
    /// Local-filesystem convenience overload.
    /// </summary>
    public static ValueTask<int> WriteAsync(
        ParallelCircuit circuit,
        string snapshotDir,
        int retainCount = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return WriteAsync(circuit, new LocalTableFileSystem(snapshotDir), retainCount, cancellationToken);
    }

    /// <summary>
    /// True iff <paramref name="fs"/> holds a committed parallel snapshot
    /// (its <c>parallel.json</c> marker exists).
    /// </summary>
    public static ValueTask<bool> ExistsAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        return fs.ExistsAsync(ParallelManifestKey, cancellationToken);
    }

    /// <summary>
    /// Restore every replica of <paramref name="circuit"/> from its subtree. The
    /// circuit must be built from the same plan and have the same <c>W</c> as the
    /// snapshot. Returns the total number of operators restored.
    /// </summary>
    /// <exception cref="FileNotFoundException">No committed parallel snapshot present.</exception>
    /// <exception cref="InvalidDataException">
    /// Worker-count mismatch, or any replica's plan/schema fingerprint mismatch.
    /// </exception>
    public static async ValueTask<int> ReadAsync(
        ParallelCircuit circuit,
        ITableFileSystem fs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(fs);

        if (!await fs.ExistsAsync(ParallelManifestKey, cancellationToken).ConfigureAwait(false))
        {
            throw new FileNotFoundException(
                "no parallel snapshot: parallel.json marker missing", ParallelManifestKey);
        }

        var manifest = await ParallelSnapshotManifest.ReadAsync(fs, ParallelManifestKey, cancellationToken)
            .ConfigureAwait(false);
        if (manifest.SchemaVersion != ParallelSnapshotManifest.CurrentSchemaVersion)
        {
            throw new InvalidDataException(
                $"parallel snapshot schema version {manifest.SchemaVersion} not supported " +
                $"(this build expects {ParallelSnapshotManifest.CurrentSchemaVersion})");
        }

        var replicas = circuit.Replicas;
        if (manifest.Workers != replicas.Count)
        {
            throw new InvalidDataException(
                $"parallel snapshot was written with {manifest.Workers} workers but this circuit " +
                $"has {replicas.Count}. The worker count is fixed across a run and its recovery " +
                "(the hash partition depends on W); rebuild the circuit with the recorded worker count.");
        }

        // Restore each replica concurrently; disjoint subtrees, disjoint state. A
        // per-worker fingerprint mismatch surfaces as an InvalidDataException.
        var tasks = new Task<int>[replicas.Count];
        for (var w = 0; w < replicas.Count; w++)
        {
            var workerFs = new PrefixedTableFileSystem(fs, WorkerPrefix(w));
            tasks[w] = Snapshot.ReadAsync(replicas[w], workerFs, cancellationToken).AsTask();
        }

        var counts = await Task.WhenAll(tasks).ConfigureAwait(false);
        var total = 0;
        foreach (var c in counts)
        {
            total += c;
        }

        return total;
    }

    /// <summary>
    /// Local-filesystem convenience overload.
    /// </summary>
    public static ValueTask<int> ReadAsync(
        ParallelCircuit circuit,
        string snapshotDir,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return ReadAsync(circuit, new LocalTableFileSystem(snapshotDir), cancellationToken);
    }

    private static string WorkerPrefix(int worker) => $"worker-{worker}/";
}

/// <summary>
/// Top-level marker for a parallel snapshot. Records the replica count <c>W</c>
/// (enforced equal on recovery) and the end-of-tick at which the snapshot was
/// taken. Per-replica plan/schema fingerprints live in each worker subtree's own
/// <see cref="SnapshotManifest"/>.
/// </summary>
public sealed record ParallelSnapshotManifest(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("workers")] int Workers,
    [property: JsonPropertyName("tick")] long Tick)
{
    public const int CurrentSchemaVersion = 1;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    public static async ValueTask<ParallelSnapshotManifest> ReadAsync(
        ITableFileSystem fs, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        ArgumentNullException.ThrowIfNull(key);
        var bytes = await fs.ReadAllBytesAsync(key, cancellationToken).ConfigureAwait(false);
        return JsonSerializer.Deserialize<ParallelSnapshotManifest>(bytes, JsonOptions)
            ?? throw new InvalidDataException($"parallel snapshot manifest at '{key}' is empty");
    }

    public ValueTask WriteAsync(ITableFileSystem fs, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        ArgumentNullException.ThrowIfNull(key);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(this, JsonOptions);
        return fs.WriteAllBytesAsync(key, bytes, cancellationToken);
    }
}
