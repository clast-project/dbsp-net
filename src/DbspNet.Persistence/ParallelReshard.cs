// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Sql.Compiler;

namespace DbspNet.Persistence;

/// <summary>
/// Moves a parallel snapshot to a different worker count (docs/design-durable-state.md §1d): the
/// committed snapshot, written at W_old, is rewritten as a new snapshot at W_new, which the ordinary
/// <see cref="ParallelSnapshot.ReadAsync(ParallelCircuit, ITableFileSystem, CancellationToken)"/>
/// then restores.
/// </summary>
/// <remarks>
/// <para>Each operator's files are decoded once per old worker through the operator's own codec,
/// and every entry goes where <see cref="ParallelCompiledProgram.ReshardSpecs"/> says — the
/// placement the compiler stated when it built the operator. An operator with no stated placement,
/// a spine operator, or a file no spec names makes the rewrite refuse before anything is written.</para>
/// <para><b>Self-check.</b> The old placement is known, so it is checked on the way: an entry read
/// from old worker <c>w</c> under a <see cref="ReshardPlacement.Hash"/> must hash to <c>w</c> at
/// W_old; <see cref="ReshardPlacement.Worker0"/> state must be on worker 0 only; a
/// <see cref="ReshardPlacement.Replicated"/> file must hold the same number of entries on every
/// worker. A mismatch means the stated placement is wrong, and nothing is committed.</para>
/// <para><b>Crash safety.</b> The new snapshot goes into a directory nothing names, is made durable,
/// and is committed by the same atomic pointer swap as any checkpoint; until then the old one stays
/// current. The old snapshot is retained (as the previous one) when <c>retainCount</c> allows.</para>
/// </remarks>
public static class ParallelReshard
{
    /// <summary>
    /// If the committed snapshot in <paramref name="fs"/> is a parallel snapshot at a worker count
    /// other than <paramref name="program"/>'s, rewrite it at <paramref name="program"/>'s and commit
    /// that. Returns the old worker count when it rewrote, null when there was nothing to do (no
    /// snapshot, a serial one, or the same W).
    /// </summary>
    public static async ValueTask<int?> RewriteAsync(
        ITableFileSystem fs,
        ParallelCompiledProgram program,
        int retainCount = 2,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        ArgumentNullException.ThrowIfNull(program);
        using var gcMode = CheckpointGcMode.Enter();
        await CheckpointCleanup.WaitAsync(fs).ConfigureAwait(false);

        var current = await Snapshot.TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            return null;
        }

        var parallelKey = current + "/" + ParallelSnapshot.ParallelManifestFile;
        if (!await fs.ExistsAsync(parallelKey, cancellationToken).ConfigureAwait(false))
        {
            return null; // a serial snapshot: not ours to move (serial <-> parallel is out of scope)
        }

        var manifest = await ParallelSnapshotManifest.ReadAsync(fs, parallelKey, cancellationToken).ConfigureAwait(false);
        var oldW = manifest.Workers;
        var newW = program.Workers;
        if (oldW == newW)
        {
            return null;
        }

        // Validate every old worker's manifest against the new program's replica 0: same plan,
        // same schemas, same operator count — the operator graph is the same for every W >= 2.
        var replica0 = program.Circuit.Replicas[0];
        var workerManifests = new SnapshotManifest[oldW];
        for (var w = 0; w < oldW; w++)
        {
            workerManifests[w] = await Snapshot.ReadManifestAsync(replica0, fs, WorkerDir(current, w), cancellationToken)
                .ConfigureAwait(false);
            if (workerManifests[w].Tick != manifest.Tick)
            {
                throw new InvalidDataException(
                    $"snapshot '{current}': worker {w} is at tick {workerManifests[w].Tick}, the snapshot at {manifest.Tick}");
            }
        }

        // One listing of the whole snapshot, grouped by (worker, op): a listing per operator walks
        // the whole tree on the local file system, which made the rewrite quadratic (70-145 s on
        // ivm-bench SF=3, nearly all of it in getdirentries).
        var persisted = await ListPersistedAsync(fs, current, cancellationToken).ConfigureAwait(false);

        var indices = workerManifests[0].SnapshottedIndices;
        var specs = new List<(int Index, ReshardSpec Spec)>(indices.Count);
        foreach (var i in indices)
        {
            if (!program.ReshardSpecs.TryGetValue(i, out var spec))
            {
                throw new InvalidOperationException(
                    $"operator {i} ({replica0.Operators[i].GetType().Name}) has no stated placement, so the snapshot cannot be re-sharded");
            }

            if (spec.Unsupported is { } why)
            {
                throw new NotSupportedException($"operator {i} ({spec.OperatorType}): {why}");
            }

            CheckFilesCovered(persisted, oldW, i, spec);
            specs.Add((i, spec));
        }

        var (tick, generation) = await Snapshot.NextSnapDirAsync(fs, manifest.Tick, cancellationToken).ConfigureAwait(false);
        var snapName = Snapshot.SnapDirName(tick, generation);

        // Operators own disjoint directories, so they rewrite concurrently — at the restore's
        // degree, which also bounds how many operators' state is decoded at once.
        await Parallel.ForEachAsync(
            specs,
            new ParallelOptions { MaxDegreeOfParallelism = Snapshot.DefaultRestoreParallelism, CancellationToken = cancellationToken },
            async (entry, ct) =>
            {
                foreach (var file in entry.Spec.Files)
                {
                    await RewriteFileAsync(fs, current, snapName, oldW, newW, entry.Index, file, ct).ConfigureAwait(false);
                }
            }).ConfigureAwait(false);

        // The worker manifests are identical across workers (same plan, tick, operators).
        var manifestBytes = await fs.ReadAllBytesAsync(WorkerDir(current, 0) + "/" + Snapshot.ManifestFile, cancellationToken)
            .ConfigureAwait(false);
        for (var w = 0; w < newW; w++)
        {
            await fs.WriteAllBytesAsync(WorkerDir(snapName, w) + "/" + Snapshot.ManifestFile, manifestBytes, cancellationToken)
                .ConfigureAwait(false);
        }

        var durable = Snapshot.DurableStore(fs);
        if (durable is not null)
        {
            await Snapshot.SyncTreeAsync(fs, durable, snapName + "/", cancellationToken).ConfigureAwait(false);
        }

        var newKey = snapName + "/" + ParallelSnapshot.ParallelManifestFile;
        var rewritten = new ParallelSnapshotManifest(
            ParallelSnapshotManifest.CurrentSchemaVersion, newW, manifest.Tick, manifest.Metadata);
        await rewritten.WriteAsync(fs, newKey, cancellationToken).ConfigureAwait(false);
        if (durable is not null)
        {
            await durable.SyncAsync([newKey], cancellationToken).ConfigureAwait(false);
        }

        await Snapshot.WriteCurrentPointerAsync(fs, durable, snapName, cancellationToken).ConfigureAwait(false);
        await CheckpointCleanup.RunAsync(
            fs, ct => Snapshot.PruneAsync(fs, (tick, generation), retainCount, ParallelSnapshot.ParallelManifestFile, ct), cancellationToken)
            .ConfigureAwait(false);
        return oldW;
    }

    private static string WorkerDir(string snapName, int worker) => $"{snapName}/worker-{worker}";

    private static string OpDir(string snapName, int worker, int op) => $"{WorkerDir(snapName, worker)}/op-{op}";

    private static int Bucket(int hash, int workers) => ((hash % workers) + workers) % workers;

    // Every file under the snapshot, by (worker, op): "{snap}/worker-{w}/op-{i}/{name}".
    private static async ValueTask<Dictionary<(int Worker, int Op), List<string>>> ListPersistedAsync(
        ITableFileSystem fs, string current, CancellationToken cancellationToken)
    {
        var result = new Dictionary<(int, int), List<string>>();
        var prefix = current + "/";
        await foreach (var f in fs.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
        {
            var rel = f.Path.StartsWith(prefix, StringComparison.Ordinal) ? f.Path[prefix.Length..] : f.Path;
            var parts = rel.Split('/');
            if (parts.Length != 3
                || !parts[0].StartsWith("worker-", StringComparison.Ordinal)
                || !parts[1].StartsWith("op-", StringComparison.Ordinal)
                || !int.TryParse(parts[0].AsSpan(7), System.Globalization.CultureInfo.InvariantCulture, out var w)
                || !int.TryParse(parts[1].AsSpan(3), System.Globalization.CultureInfo.InvariantCulture, out var op))
            {
                continue;
            }

            if (!result.TryGetValue((w, op), out var names))
            {
                names = [];
                result[(w, op)] = names;
            }

            names.Add(parts[2]);
        }

        return result;
    }

    // Every file the operator wrote must be one its spec names, or the rewrite would silently
    // drop it.
    private static void CheckFilesCovered(
        Dictionary<(int Worker, int Op), List<string>> persisted, int oldW, int op, ReshardSpec spec)
    {
        var named = spec.Files.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        for (var w = 0; w < oldW; w++)
        {
            if (!persisted.TryGetValue((w, op), out var names))
            {
                continue;
            }

            foreach (var name in names)
            {
                if (!named.Contains(name))
                {
                    throw new InvalidOperationException(
                        $"operator {op} ({spec.OperatorType}) persisted '{name}', for which the compiler stated no placement");
                }
            }
        }
    }

    private static async ValueTask RewriteFileAsync(
        ITableFileSystem fs, string current, string snapName, int oldW, int newW, int op, ReshardFile file,
        CancellationToken cancellationToken)
    {
        switch (file)
        {
            case ZSetReshardFile z:
                {
                    var buckets = NewBuckets(newW, () => new ZSetBuilder<StructuralRow, Z64>());
                    var present = false;
                    long? replicatedCount = null;
                    for (var w = 0; w < oldW; w++)
                    {
                        var reader = new TableFileSystemSnapshotContext(fs, OpDir(current, w, op));
                        if (!await reader.ExistsAsync(z.Name, cancellationToken).ConfigureAwait(false))
                        {
                            continue;
                        }

                        present = true;
                        var set = await z.Codec.LoadAsync(reader, z.Name, cancellationToken).ConfigureAwait(false);
                        if (Skip(z, w, set.Count, ref replicatedCount, op))
                        {
                            continue;
                        }

                        foreach (var (row, weight) in set)
                        {
                            foreach (var target in Targets(z.Placement, row, w, oldW, newW, op, z.Name))
                            {
                                buckets[target].Add(row, weight);
                            }
                        }
                    }

                    if (!present)
                    {
                        return;
                    }

                    for (var w = 0; w < newW; w++)
                    {
                        var writer = new TableFileSystemSnapshotContext(fs, OpDir(snapName, w, op));
                        await z.Codec.SaveAsync(writer, z.Name, buckets[w].Build(), cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

            case IndexedReshardFile ix:
                {
                    var buckets = NewBuckets(newW, () => new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>());
                    var present = false;
                    long? replicatedCount = null;
                    for (var w = 0; w < oldW; w++)
                    {
                        var reader = new TableFileSystemSnapshotContext(fs, OpDir(current, w, op));
                        if (!await reader.ExistsAsync(ix.Name, cancellationToken).ConfigureAwait(false))
                        {
                            continue;
                        }

                        present = true;
                        var trace = await ix.Codec.LoadAsync(reader, ix.Name, cancellationToken).ConfigureAwait(false);
                        if (Skip(ix, w, trace.GroupCount, ref replicatedCount, op))
                        {
                            continue;
                        }

                        foreach (var group in trace)
                        {
                            foreach (var target in Targets(ix.Placement, group.Key, w, oldW, newW, op, ix.Name))
                            {
                                foreach (var (value, weight) in group.Value)
                                {
                                    buckets[target].Add(group.Key, value, weight);
                                }
                            }
                        }
                    }

                    if (!present)
                    {
                        return;
                    }

                    for (var w = 0; w < newW; w++)
                    {
                        var writer = new TableFileSystemSnapshotContext(fs, OpDir(snapName, w, op));
                        await ix.Codec.SaveAsync(writer, ix.Name, buckets[w].Build(), cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

            case KeyedBlobsReshardFile kb:
                {
                    var buckets = NewBuckets(newW, () => new List<(StructuralRow Key, byte[] Blob)>());
                    var present = false;
                    long? replicatedCount = null;
                    for (var w = 0; w < oldW; w++)
                    {
                        var reader = new TableFileSystemSnapshotContext(fs, OpDir(current, w, op));
                        if (!await reader.ExistsAsync(kb.Name, cancellationToken).ConfigureAwait(false))
                        {
                            continue;
                        }

                        present = true;
                        var entries = await kb.Codec.LoadKeyedBlobsAsync(reader, kb.Name, cancellationToken).ConfigureAwait(false);
                        if (Skip(kb, w, entries.Count, ref replicatedCount, op))
                        {
                            continue;
                        }

                        foreach (var entry in entries)
                        {
                            foreach (var target in Targets(kb.Placement, entry.Key, w, oldW, newW, op, kb.Name))
                            {
                                buckets[target].Add(entry);
                            }
                        }
                    }

                    if (!present)
                    {
                        return;
                    }

                    for (var w = 0; w < newW; w++)
                    {
                        var writer = new TableFileSystemSnapshotContext(fs, OpDir(snapName, w, op));
                        await kb.Codec.SaveKeyedBlobsAsync(writer, kb.Name, buckets[w], cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

            default:
                throw new NotSupportedException($"unknown reshard file kind {file.GetType().Name}");
        }
    }

    private static T[] NewBuckets<T>(int n, Func<T> make)
    {
        var b = new T[n];
        for (var i = 0; i < n; i++)
        {
            b[i] = make();
        }

        return b;
    }

    // Replicated state: every worker holds the same copy; check that, and keep worker 0's.
    private static bool Skip(ReshardFile file, int w, long count, ref long? replicatedCount, int op)
    {
        if (file.Placement is not ReshardPlacement.Replicated)
        {
            return false;
        }

        if (replicatedCount is null)
        {
            replicatedCount = count;
            return false;
        }

        if (replicatedCount != count)
        {
            throw new InvalidDataException(
                $"operator {op} '{file.Name}' is stated replicated, but worker {w} holds {count} entries and worker 0 {replicatedCount}");
        }

        return true;
    }

    // Where an entry from old worker w goes — after checking it was where the placement says.
    private static IEnumerable<int> Targets(
        ReshardPlacement placement, StructuralRow entry, int w, int oldW, int newW, int op, string file)
    {
        switch (placement)
        {
            case ReshardPlacement.Hash h:
                {
                    var hash = h.Of(entry);
                    if (Bucket(hash, oldW) != w)
                    {
                        throw new InvalidDataException(
                            $"operator {op} '{file}': an entry on worker {w} hashes to worker {Bucket(hash, oldW)} at W={oldW}; " +
                            "the stated placement does not match where the state actually is");
                    }

                    return [Bucket(hash, newW)];
                }

            case ReshardPlacement.Worker0:
                if (w != 0)
                {
                    throw new InvalidDataException(
                        $"operator {op} '{file}' is stated to live on worker 0, but worker {w} holds state");
                }

                return [0];

            case ReshardPlacement.Replicated:
                return Enumerable.Range(0, newW);

            case ReshardPlacement.Linear:
                // Any placement sums to the same result; spread deterministically.
                return [Bucket(entry.GetHashCode(), newW)];

            default:
                throw new NotSupportedException($"unknown placement {placement.GetType().Name}");
        }
    }
}
