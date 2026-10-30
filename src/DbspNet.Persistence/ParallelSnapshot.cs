// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text.Json;
using System.Text.Json.Serialization;
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Persistence.IO.Local;

namespace DbspNet.Persistence;

/// <summary>
/// Snapshots of a <see cref="ParallelCircuit"/>: all <c>W</c> replicas at one tick, committed
/// together. Recovery requires the same <c>W</c> — matching Feldera's fixed-worker constraint: the
/// hash partition (<c>StableHash(key) % W</c>) is part of the persisted state, so a different
/// <c>W</c> would place keys on different replicas than the snapshot holds.
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> The same pointer and directory naming as <see cref="Snapshot"/>: a
/// top-level <c>current.txt</c> names one <c>snap-{tick}[.{gen}]/</c> directory, which holds a
/// <c>worker-{w}/</c> subtree per replica (that replica's op files and <c>manifest.json</c>) and a
/// <c>parallel.json</c> recording <c>W</c>, the tick and any metadata (connector offsets).</para>
/// <para><b>Commit protocol.</b> The replicas are snapshotted concurrently into a directory no
/// committed snapshot occupies. Every file of every worker is then made durable in one sync,
/// <c>parallel.json</c> is written and made durable, and only then is <c>current.txt</c> replaced
/// atomically — the single commit point for all <c>W</c> workers. A crash before it leaves the
/// previous snapshot current; a crash after it leaves the new one, complete. A reader therefore
/// never sees workers at different ticks, and restore checks that every worker's manifest agrees
/// with <c>parallel.json</c> anyway. Pruning runs after the commit and treats a directory as
/// complete only when its <c>parallel.json</c> exists.</para>
/// <para>This is why no exchange state is snapshotted: the circuit is checkpointed between steps,
/// when every exchange has drained and the workers are parked on the step barrier.</para>
/// <para><b>Stability.</b> The per-key worker assignment is reproduced on recovery only because the
/// SQL planner partitions with <see cref="DbspNet.Core.Circuit.StableHash"/> (a process-independent
/// hash), not <see cref="object.GetHashCode"/>. Same plan + same <c>W</c> ⇒ same shard for every
/// key.</para>
/// </remarks>
public static class ParallelSnapshot
{
    /// <summary>The commit marker inside a parallel snapshot directory.</summary>
    internal const string ParallelManifestFile = "parallel.json";

    /// <summary>
    /// Snapshot every replica of <paramref name="circuit"/> and commit them as one snapshot.
    /// Returns the total number of operators snapshotted across all workers.
    /// </summary>
    public static ValueTask<int> WriteAsync(
        ParallelCircuit circuit,
        ITableFileSystem fs,
        int retainCount = 1,
        CancellationToken cancellationToken = default) =>
        WriteAsync(circuit, fs, null, retainCount, cancellationToken);

    /// <summary>
    /// As <see cref="WriteAsync(ParallelCircuit, ITableFileSystem, int, CancellationToken)"/>, also
    /// recording <paramref name="metadata"/> (e.g. connector source offsets) in <c>parallel.json</c>,
    /// so it commits atomically with every worker's state. Read it back with
    /// <see cref="ReadMetadataAsync"/>.
    /// </summary>
    public static async ValueTask<int> WriteAsync(
        ParallelCircuit circuit,
        ITableFileSystem fs,
        IReadOnlyDictionary<string, string>? metadata,
        int retainCount = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(fs);
        if (retainCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retainCount), retainCount, "retainCount must be at least 1");
        }

        var replicas = circuit.Replicas;
        var (tick, generation) = await Snapshot.NextSnapDirAsync(fs, circuit.TickCount, cancellationToken)
            .ConfigureAwait(false);
        var snapName = Snapshot.SnapDirName(tick, generation);
        var durable = Snapshot.DurableStore(fs);

        // Disjoint subtrees, disjoint replica state. No per-worker sync: the commit marker is
        // parallel.json, so one sync over the whole directory before it is enough, and costs one
        // device flush instead of two per worker.
        var tasks = new Task<int>[replicas.Count];
        for (var w = 0; w < replicas.Count; w++)
        {
            tasks[w] = Snapshot.WriteDirAsync(
                replicas[w], fs, WorkerDir(snapName, w), metadata: null, durable: null, cancellationToken).AsTask();
        }

        var counts = await Task.WhenAll(tasks).ConfigureAwait(false);

        if (durable is not null)
        {
            await Snapshot.SyncTreeAsync(fs, durable, snapName + "/", cancellationToken).ConfigureAwait(false);
        }

        var manifestKey = snapName + "/" + ParallelManifestFile;
        var manifest = new ParallelSnapshotManifest(
            ParallelSnapshotManifest.CurrentSchemaVersion, replicas.Count, tick, metadata);
        await manifest.WriteAsync(fs, manifestKey, cancellationToken).ConfigureAwait(false);
        if (durable is not null)
        {
            await durable.SyncAsync([manifestKey], cancellationToken).ConfigureAwait(false);
        }

        // Commit: one pointer for all W workers.
        await Snapshot.WriteCurrentPointerAsync(fs, durable, snapName, cancellationToken).ConfigureAwait(false);

        await Snapshot.PruneAsync(fs, (tick, generation), retainCount, ParallelManifestFile, cancellationToken)
            .ConfigureAwait(false);

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
    /// True iff <paramref name="fs"/> holds a committed parallel snapshot: <c>current.txt</c> names
    /// a directory whose <c>parallel.json</c> exists.
    /// </summary>
    public static async ValueTask<bool> ExistsAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        var current = await Snapshot.TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        return current is not null
            && await fs.ExistsAsync(current + "/" + ParallelManifestFile, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The worker count the current snapshot in <paramref name="fs"/> was written with: <c>W</c>
    /// for a parallel snapshot, 1 for a serial one (<see cref="Snapshot"/>), or <c>null</c> when
    /// there is no commit pointer. A deployment uses it to pin <c>W</c> to the checkpoint it
    /// resumes, as Feldera does. A pointer to a damaged directory reads as 1; the restore that
    /// follows then fails loudly.
    /// </summary>
    public static async ValueTask<int?> RecordedWorkersAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        var current = await Snapshot.TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            return null;
        }

        var key = current + "/" + ParallelManifestFile;
        if (!await fs.ExistsAsync(key, cancellationToken).ConfigureAwait(false))
        {
            return 1;
        }

        var manifest = await ParallelSnapshotManifest.ReadAsync(fs, key, cancellationToken).ConfigureAwait(false);
        return manifest.Workers;
    }

    /// <summary>
    /// Restore every replica of <paramref name="circuit"/> from the current snapshot. The circuit
    /// must be built from the same plan and have the same <c>W</c> as the snapshot. Returns the
    /// total number of operators restored.
    /// </summary>
    /// <exception cref="FileNotFoundException">No snapshot present, or its files are missing.</exception>
    /// <exception cref="InvalidDataException">
    /// Worker-count mismatch, a serial snapshot, workers at different ticks, or any replica's
    /// plan/schema fingerprint mismatch.
    /// </exception>
    public static async ValueTask<int> ReadAsync(
        ParallelCircuit circuit,
        ITableFileSystem fs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(fs);

        var current = await Snapshot.TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            if (await fs.ExistsAsync(ParallelManifestFile, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    "this is a parallel snapshot in the old layout (a top-level parallel.json with " +
                    "per-worker pointers), which was not crash-safe and is no longer read; take a new snapshot");
            }

            throw new FileNotFoundException("no snapshot: current.txt missing or empty", "current.txt");
        }

        var manifestKey = current + "/" + ParallelManifestFile;
        if (!await fs.ExistsAsync(manifestKey, cancellationToken).ConfigureAwait(false))
        {
            if (await fs.ExistsAsync(current + "/" + Snapshot.ManifestFile, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidDataException(
                    $"snapshot '{current}' was written by a serial program (1 worker), but this circuit " +
                    $"has {circuit.Workers}. The worker count is fixed across a run and its recovery.");
            }

            throw new FileNotFoundException(
                $"snapshot '{current}' is missing its parallel manifest at '{manifestKey}' " +
                "(possibly pruned or corrupted)",
                manifestKey);
        }

        var manifest = await ParallelSnapshotManifest.ReadAsync(fs, manifestKey, cancellationToken)
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

        // Validate every worker before restoring any, so a mismatch leaves the circuit untouched.
        var workerManifests = new SnapshotManifest[replicas.Count];
        for (var w = 0; w < replicas.Count; w++)
        {
            var m = await Snapshot.ReadManifestAsync(replicas[w], fs, WorkerDir(current, w), cancellationToken)
                .ConfigureAwait(false);
            if (m.Tick != manifest.Tick)
            {
                throw new InvalidDataException(
                    $"parallel snapshot '{current}' is at tick {manifest.Tick} but worker {w}'s state is " +
                    $"at tick {m.Tick}; the workers would not be consistent");
            }

            workerManifests[w] = m;
        }

        // The replicas are already W-way concurrent, so each one restores its own operators with a
        // SHARE of Snapshot.DefaultRestoreParallelism rather than all of it: the product is what
        // decides how many operator files decode at once, and that is what bounds peak memory
        // (docs/design-incremental-persistence.md §11.4). Each replica also holds only 1/W of the
        // state, so there is less per-replica work to spread.
        var perReplica = Math.Max(1, Snapshot.DefaultRestoreParallelism / replicas.Count);
        var tasks = new Task<int>[replicas.Count];
        for (var w = 0; w < replicas.Count; w++)
        {
            tasks[w] = Snapshot.LoadDirAsync(
                replicas[w], fs, WorkerDir(current, w), workerManifests[w], perReplica, cancellationToken).AsTask();
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

    /// <summary>
    /// The metadata recorded with the current parallel snapshot, or <c>null</c> if there is none.
    /// Reads only <c>parallel.json</c>, which commits together with the state
    /// <see cref="ReadAsync(ParallelCircuit, ITableFileSystem, CancellationToken)"/> restores.
    /// </summary>
    public static async ValueTask<IReadOnlyDictionary<string, string>?> ReadMetadataAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        var current = await Snapshot.TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            return null;
        }

        var key = current + "/" + ParallelManifestFile;
        if (!await fs.ExistsAsync(key, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var manifest = await ParallelSnapshotManifest.ReadAsync(fs, key, cancellationToken).ConfigureAwait(false);
        return manifest.Metadata ?? new Dictionary<string, string>();
    }

    private static string WorkerDir(string snapName, int worker) => $"{snapName}/worker-{worker}";
}

/// <summary>
/// The commit marker of a parallel snapshot. Records the replica count <c>W</c> (enforced equal on
/// recovery), the end-of-tick at which the snapshot was taken (every worker's manifest must agree),
/// and optional metadata such as connector offsets. Per-replica plan/schema fingerprints live in
/// each worker subtree's own <see cref="SnapshotManifest"/>.
/// </summary>
public sealed record ParallelSnapshotManifest(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("workers")] int Workers,
    [property: JsonPropertyName("tick")] long Tick,
    [property: JsonPropertyName("metadata")] IReadOnlyDictionary<string, string>? Metadata = null)
{
    /// <summary>2: one directory per snapshot under a single pointer (1 had a pointer per worker).</summary>
    public const int CurrentSchemaVersion = 2;

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
