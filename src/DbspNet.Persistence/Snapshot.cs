// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence.IO.Local;

namespace DbspNet.Persistence;

/// <summary>
/// Approach (B) end-of-tick state snapshots. Walks a
/// <see cref="RootCircuit"/>'s operators, calls
/// <see cref="ISnapshotable.SaveAsync"/> on those that opt in, and writes
/// a manifest. <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/>
/// validates the manifest's plan and schema fingerprints, then walks the
/// rebuilt circuit and calls <see cref="ISnapshotable.LoadAsync"/> on the
/// matching positions.
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> Each snapshot lives under
/// <c>snap-{tick}/</c> with its own <c>manifest.json</c> and
/// <c>op-{i}/</c> subkeys. A top-level <c>current.txt</c> names the
/// latest snapshot (e.g. <c>"snap-42"</c>) and is the source of truth
/// for which one <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/>
/// loads. Older snapshots are retained up to <c>retainCount</c> from
/// <see cref="WriteAsync(RootCircuit, ITableFileSystem, int, CancellationToken)"/>;
/// pruning happens after the new snapshot commits.</para>
/// <para><b>Cloud-native commit.</b> Per-op files and the snap-T
/// manifest are written directly to their final keys via
/// <see cref="ITableFileSystem.CreateAsync"/> /
/// <see cref="ITableFileSystem.WriteAllBytesAsync"/>. The commit happens
/// when <c>current.txt</c> is rotated in last via the tmp+rename pattern
/// — before that, the new snapshot is invisible to readers (they're
/// still using the prior <c>current.txt</c>).</para>
/// <para>Plan + schema fingerprints in the manifest catch operator-
/// type drift and schema drift; if a snapshot is loaded into a
/// circuit that doesn't match, an
/// <see cref="InvalidDataException"/> surfaces before any state is
/// restored.</para>
/// </remarks>
public static class Snapshot
{
    private const string CurrentKey = "current.txt";

    /// <summary>
    /// Snapshot the circuit to <paramref name="fs"/>. Each call produces a
    /// new snapshot under <c>snap-{tick}/</c> and updates <c>current.txt</c>
    /// to point at it. Up to <paramref name="retainCount"/> most-recent
    /// snapshots are kept; older ones are pruned. Returns the number of
    /// operators that participated.
    /// </summary>
    public static ValueTask<int> WriteAsync(
        RootCircuit circuit,
        ITableFileSystem fs,
        int retainCount = 1,
        CancellationToken cancellationToken = default) =>
        WriteAsync(circuit, fs, null, retainCount, cancellationToken);

    /// <summary>
    /// As <see cref="WriteAsync(RootCircuit, ITableFileSystem, int, CancellationToken)"/>,
    /// but also records <paramref name="metadata"/> (e.g. connector source offsets) in
    /// the snapshot's manifest, so it commits <b>atomically</b> with the operator state
    /// — the manifest is written before the <c>current.txt</c> pointer that makes the
    /// snapshot visible. Read it back with <see cref="ReadMetadataAsync(ITableFileSystem, CancellationToken)"/>.
    /// </summary>
    public static async ValueTask<int> WriteAsync(
        RootCircuit circuit,
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

        var tick = circuit.TickCount;
        var snapName = SnapDirName(tick);

        // Write per-op files directly under their final keys. If a Save
        // throws partway through, the partially-written files are orphans
        // — invisible to readers because current.txt hasn't been updated.
        // Future writes either at the same tick (which overwrite the same
        // keys) or different ticks (which leave the orphans for retention
        // prune) handle cleanup.
        var snapshotted = new List<int>();
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is not ISnapshotable s)
            {
                continue;
            }

            var ctx = new TableFileSystemSnapshotContext(fs, snapName + "/op-" + i);
            await s.SaveAsync(ctx, cancellationToken).ConfigureAwait(false);
            snapshotted.Add(i);
        }

        var manifest = new SnapshotManifest(
            SnapshotManifest.CurrentSchemaVersion,
            SnapshotManifest.ComputeCircuitFingerprint(circuit),
            SnapshotManifest.ComputeSchemaFingerprint(circuit),
            circuit.TickCount,
            circuit.Operators.Count,
            snapshotted,
            circuit.LogicalTime,
            metadata);
        await manifest.WriteAsync(fs, snapName + "/manifest.json", cancellationToken).ConfigureAwait(false);

        // Commit. After this, the new snapshot is the latest; before, the
        // pointer still names the prior snapshot (or doesn't exist).
        await WriteCurrentPointerAsync(fs, snapName, cancellationToken).ConfigureAwait(false);

        await PruneOlderSnapshotsAsync(fs, retainCount, cancellationToken).ConfigureAwait(false);

        return snapshotted.Count;
    }

    /// <summary>
    /// Convenience overload for the local filesystem case: opens a
    /// <see cref="LocalTableFileSystem"/> rooted at
    /// <paramref name="snapshotDir"/> and forwards to
    /// <see cref="WriteAsync(RootCircuit, ITableFileSystem, int, CancellationToken)"/>.
    /// </summary>
    public static ValueTask<int> WriteAsync(
        RootCircuit circuit,
        string snapshotDir,
        int retainCount = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return WriteAsync(circuit, new LocalTableFileSystem(snapshotDir), retainCount, cancellationToken);
    }

    /// <summary>
    /// True iff <paramref name="fs"/> holds at least one readable snapshot
    /// — i.e. <c>current.txt</c> exists and names a snap-T directory whose
    /// <c>manifest.json</c> exists.
    /// </summary>
    public static async ValueTask<bool> ExistsAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        var current = await TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            return false;
        }

        return await fs.ExistsAsync(current + "/manifest.json", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Convenience overload for the local filesystem case.
    /// </summary>
    public static ValueTask<bool> ExistsAsync(
        string snapshotDir, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        if (!Directory.Exists(snapshotDir))
        {
            return ValueTask.FromResult(false);
        }

        return ExistsAsync(new LocalTableFileSystem(snapshotDir), cancellationToken);
    }

    /// <summary>
    /// Tick numbers of all retained snapshots in <paramref name="fs"/>,
    /// in ascending order.
    /// </summary>
    public static async ValueTask<IReadOnlyList<long>> ListSnapshotsAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        var ticks = new HashSet<long>();
        await foreach (var entry in fs.ListAsync("snap-", cancellationToken).ConfigureAwait(false))
        {
            // Extract the snap-T directory name (the part before the
            // first '/' after the prefix). A path like "snap-5/op-3/x"
            // yields "snap-5".
            var slash = entry.Path.IndexOf('/');
            var name = slash < 0 ? entry.Path : entry.Path[..slash];
            if (TryParseSnapDir(name, out var tick))
            {
                ticks.Add(tick);
            }
        }

        var sorted = ticks.ToList();
        sorted.Sort();
        return sorted;
    }

    /// <summary>
    /// Convenience overload for the local filesystem case.
    /// </summary>
    public static ValueTask<IReadOnlyList<long>> ListSnapshotsAsync(
        string snapshotDir, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        if (!Directory.Exists(snapshotDir))
        {
            return ValueTask.FromResult<IReadOnlyList<long>>(Array.Empty<long>());
        }

        return ListSnapshotsAsync(new LocalTableFileSystem(snapshotDir), cancellationToken);
    }

    /// <summary>
    /// Restore <paramref name="circuit"/>'s operator state from the
    /// snapshot named by <c>current.txt</c>. The circuit must already
    /// be built from the same plan that produced the snapshot. Returns
    /// the number of operators restored.
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// Plan-fingerprint, schema-fingerprint, or schema-version mismatch.
    /// </exception>
    /// <exception cref="FileNotFoundException">
    /// No snapshot present (missing or unreadable <c>current.txt</c>).
    /// </exception>
    /// <summary>
    /// Opt-in diagnostic: when set before <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/>,
    /// each operator's <see cref="ISnapshotable.LoadAsync"/> is timed individually and the result
    /// left in <see cref="LastLoadProfile"/>.
    /// </summary>
    /// <remarks>
    /// Restore is not uniform across operator kinds: an operator whose state <em>is</em> a trace
    /// (<c>DistinctOp</c>, <c>IncrementalJoinOp</c>) restores in deserialize time, while one that
    /// keeps a differently-shaped runtime structure (a partition-keyed <c>SortedDictionary</c> plus a
    /// materialised window — <c>PartitionedTopKOp</c>, <c>PartitionedWindowAggregateOp</c>) has to
    /// rebuild it, re-partitioning and re-sorting every row. This flag apportions those. Off by
    /// default and free when off.
    /// </remarks>
    public static bool ProfileLoad { get; set; }

    /// <summary>
    /// Degree of concurrency <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/>
    /// uses when no explicit degree is passed. Restore is a per-operator walk with no data
    /// dependency between operators — snapshotted operators own disjoint state — so it parallelises
    /// directly, and doing so is the largest resume-latency lever available without changing the
    /// storage format (docs/design-incremental-persistence.md §11.4).
    /// </summary>
    /// <remarks>
    /// <para>Default is half the machine's processors, capped at 8. The cap is measured, not
    /// arbitrary: on a 14-core M4 Pro, degree 8 restored a 4 GiB snapshot in 9.6–15.4 s against
    /// 21.9 s sequential, while degree <b>14 was worse</b> (17.1 s) — a restore allocates ~10× the
    /// state it rebuilds, so past a point what is being scheduled is garbage collection, and
    /// performance cores and efficiency cores are not interchangeable. Peak memory also rises with
    /// the degree, since that many operator files decode at once.</para>
    /// <para><c>DBSPNET_RESTORE_PARALLEL</c> overrides the default for measurement. Setting this
    /// property to 1 restores the sequential walk.</para>
    /// </remarks>
    public static int DefaultRestoreParallelism { get; set; } =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_RESTORE_PARALLEL"), out var p) && p > 0
            ? p
            : Math.Clamp(Environment.ProcessorCount / 2, 1, 8);

    private static IReadOnlyList<(int Index, string Operator, double Ms)> _lastLoadProfile =
        System.Array.Empty<(int, string, double)>();

    /// <summary>Per-operator <c>LoadAsync</c> durations from the most recent
    /// <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/> that ran with
    /// <see cref="ProfileLoad"/> set. Empty otherwise.</summary>
    public static IReadOnlyList<(int Index, string Operator, double Ms)> LastLoadProfile => _lastLoadProfile;

    public static ValueTask<int> ReadAsync(
        RootCircuit circuit,
        ITableFileSystem fs,
        CancellationToken cancellationToken = default) =>
        ReadAsync(circuit, fs, DefaultRestoreParallelism, cancellationToken);

    /// <summary>
    /// As <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/>, restoring up to
    /// <paramref name="parallelism"/> operators concurrently. Operators that participate in a
    /// snapshot own disjoint state — the compiler refuses to share an arrangement across joins when
    /// snapshot codecs are present — so the walk carries no data dependency and the restored state
    /// does not depend on the degree.
    /// </summary>
    /// <param name="parallelism">
    /// Operators to load at once; 1 is the sequential walk. See
    /// <see cref="DefaultRestoreParallelism"/> for why the default is capped well below the
    /// processor count. Ignored (forced to 1) while <see cref="ProfileLoad"/> is set, because
    /// per-operator timings taken from overlapping loads would not mean anything.
    /// </param>
    public static async ValueTask<int> ReadAsync(
        RootCircuit circuit,
        ITableFileSystem fs,
        int parallelism,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(fs);
        if (parallelism < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(parallelism), parallelism, "parallelism must be at least 1");
        }

        var current = await TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            throw new FileNotFoundException(
                "no snapshot: current.txt missing or empty", CurrentKey);
        }

        var manifestKey = current + "/manifest.json";
        if (!await fs.ExistsAsync(manifestKey, cancellationToken).ConfigureAwait(false))
        {
            throw new FileNotFoundException(
                $"snapshot '{current}' is missing its manifest at '{manifestKey}' " +
                "(possibly pruned or corrupted)",
                manifestKey);
        }

        var manifest = await SnapshotManifest.ReadAsync(fs, manifestKey, cancellationToken).ConfigureAwait(false);
        if (manifest.SchemaVersion != SnapshotManifest.CurrentSchemaVersion)
        {
            throw new InvalidDataException(
                $"snapshot schema version {manifest.SchemaVersion} not supported " +
                $"(this build expects {SnapshotManifest.CurrentSchemaVersion})");
        }

        var actual = SnapshotManifest.ComputeCircuitFingerprint(circuit);
        if (manifest.PlanFingerprint != actual)
        {
            throw new InvalidDataException(
                "snapshot plan fingerprint mismatch — the circuit's operator " +
                "structure differs from when this snapshot was written. " +
                $"Recorded: {manifest.PlanFingerprint}; current: {actual}.");
        }

        var actualSchema = SnapshotManifest.ComputeSchemaFingerprint(circuit);
        if (manifest.SchemaFingerprint != actualSchema)
        {
            throw new InvalidDataException(
                "snapshot schema fingerprint mismatch — operator types match " +
                "but at least one stateful operator's row/key/value schemas " +
                "differ from when this snapshot was written. Common causes: " +
                "VARCHAR length change, DECIMAL precision/scale change, or an " +
                "intermediate column rename/reorder. " +
                $"Recorded: {manifest.SchemaFingerprint}; current: {actualSchema}.");
        }

        if (manifest.OperatorCount != circuit.Operators.Count)
        {
            throw new InvalidDataException(
                $"snapshot operator count {manifest.OperatorCount} does not " +
                $"match circuit's operator count {circuit.Operators.Count}");
        }

        // Restore the tick counter and the logical clock *before* loading
        // operators: a temporal-filter operator recomputes its emitted window
        // against the clock during LoadAsync, so the clock must already hold its
        // end-of-tick value or the post-load delta would be wrong.
        circuit.RestoreTickCount(manifest.Tick);
        circuit.RestoreLogicalTime(manifest.LogicalTime);

        var restored = 0;
        var profile = ProfileLoad ? new List<(int, string, double)>(manifest.SnapshottedIndices.Count) : null;

        if (parallelism > 1 && !ProfileLoad && manifest.SnapshottedIndices.Count > 1)
        {
            await RestoreConcurrentlyAsync(circuit, fs, current, manifest, parallelism, cancellationToken)
                .ConfigureAwait(false);
            return manifest.SnapshottedIndices.Count;
        }

        // Stage apportionment inside deserialize (§11): the codecs and the operators' own
        // integrate/rebuild legs write into SnapshotRestoreProfile while this is set. Sequential
        // only — the counters are not thread-safe, and overlapping legs would not be readable.
        SnapshotRestoreProfile.Enabled = ProfileLoad;
        try
        {
            foreach (var i in manifest.SnapshottedIndices)
            {
                var s = Snapshottable(circuit, i);
                var ctx = new TableFileSystemSnapshotContext(fs, current + "/op-" + i);
                if (profile is null)
                {
                    await s.LoadAsync(ctx, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
                    await s.LoadAsync(ctx, cancellationToken).ConfigureAwait(false);
                    var ms = (System.Diagnostics.Stopwatch.GetTimestamp() - t0) * 1000.0
                        / System.Diagnostics.Stopwatch.Frequency;
                    profile.Add((i, circuit.Operators[i].GetType().Name, ms));
                }

                restored++;
            }
        }
        finally
        {
            SnapshotRestoreProfile.Enabled = false;
        }

        if (profile is not null)
        {
            _lastLoadProfile = profile;
        }

        return restored;
    }

    private static ISnapshotable Snapshottable(RootCircuit circuit, int i) =>
        circuit.Operators[i] as ISnapshotable
        ?? throw new InvalidDataException(
            $"snapshot expected operator {i} to be ISnapshotable, " +
            $"but actual type {circuit.Operators[i].GetType().Name} is not");

    private static async Task RestoreConcurrentlyAsync(
        RootCircuit circuit,
        ITableFileSystem fs,
        string current,
        SnapshotManifest manifest,
        int parallelism,
        CancellationToken cancellationToken)
    {
        try
        {
            await Parallel.ForEachAsync(
                manifest.SnapshottedIndices,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = parallelism,
                    CancellationToken = cancellationToken,
                },
                async (i, ct) =>
                {
                    var s = Snapshottable(circuit, i);
                    var ctx = new TableFileSystemSnapshotContext(fs, current + "/op-" + i);
                    await s.LoadAsync(ctx, ct).ConfigureAwait(false);
                }).ConfigureAwait(false);
        }
        catch (AggregateException ex) when (ex.InnerExceptions.Count > 0)
        {
            // A failing load must surface exactly as it does on the sequential path — callers
            // catch InvalidDataException / NotSupportedException from ReadAsync, not a wrapper.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(ex.InnerExceptions[0]).Throw();
        }
    }

    /// <summary>
    /// Convenience overload for the local filesystem case.
    /// </summary>
    public static ValueTask<int> ReadAsync(
        RootCircuit circuit,
        string snapshotDir,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return ReadAsync(circuit, new LocalTableFileSystem(snapshotDir), cancellationToken);
    }

    /// <summary>
    /// Local-filesystem overload of
    /// <see cref="ReadAsync(RootCircuit, ITableFileSystem, int, CancellationToken)"/>.
    /// </summary>
    public static ValueTask<int> ReadAsync(
        RootCircuit circuit,
        string snapshotDir,
        int parallelism,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return ReadAsync(circuit, new LocalTableFileSystem(snapshotDir), parallelism, cancellationToken);
    }

    /// <summary>
    /// The <see cref="SnapshotManifest.Metadata"/> recorded with the current snapshot
    /// (the section written by <see cref="WriteAsync(RootCircuit, ITableFileSystem, IReadOnlyDictionary{string, string}, int, CancellationToken)"/>),
    /// or <see langword="null"/> if no snapshot exists. Reads only the manifest — it
    /// does not restore operator state. Because the metadata lives in the same manifest
    /// as the tick, it is always consistent with the state <see cref="ReadAsync(RootCircuit, ITableFileSystem, CancellationToken)"/>
    /// restores from the same snapshot.
    /// </summary>
    public static async ValueTask<IReadOnlyDictionary<string, string>?> ReadMetadataAsync(
        ITableFileSystem fs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fs);
        var current = await TryReadCurrentPointerAsync(fs, cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            return null;
        }

        var manifestKey = current + "/manifest.json";
        if (!await fs.ExistsAsync(manifestKey, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var manifest = await SnapshotManifest.ReadAsync(fs, manifestKey, cancellationToken).ConfigureAwait(false);
        return manifest.Metadata ?? new Dictionary<string, string>();
    }

    private static string SnapDirName(long tick) => $"snap-{tick}";

    private static bool TryParseSnapDir(string name, out long tick)
    {
        tick = 0;
        if (!name.StartsWith("snap-", StringComparison.Ordinal))
        {
            return false;
        }

        return long.TryParse(
            name.AsSpan("snap-".Length),
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture,
            out tick);
    }

    private static async ValueTask<string?> TryReadCurrentPointerAsync(
        ITableFileSystem fs, CancellationToken cancellationToken)
    {
        if (!await fs.ExistsAsync(CurrentKey, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        var bytes = await fs.ReadAllBytesAsync(CurrentKey, cancellationToken).ConfigureAwait(false);
        var contents = System.Text.Encoding.UTF8.GetString(bytes).Trim();
        return string.IsNullOrEmpty(contents) ? null : contents;
    }

    private static async ValueTask WriteCurrentPointerAsync(
        ITableFileSystem fs, string snapName, CancellationToken cancellationToken)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(snapName);
        var tmpKey = CurrentKey + ".tmp";

        // Best-effort cleanup of any stale tmp from a previous failed write.
        await fs.DeleteAsync(tmpKey, cancellationToken).ConfigureAwait(false);

        await using (var file = await fs.CreateAsync(tmpKey, overwrite: true, cancellationToken).ConfigureAwait(false))
        {
            await file.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
        }

        // RenameAsync returns false if the target already exists, so we
        // delete the old pointer first. There is a microsecond-scale
        // window where no pointer exists; the failure mode is "transient
        // ExistsAsync returns false" which is equivalent to the snapshot
        // not yet being written, and harmless under the single-writer
        // assumption that holds throughout the persistence layer.
        await fs.DeleteAsync(CurrentKey, cancellationToken).ConfigureAwait(false);
        var renamed = await fs.RenameAsync(tmpKey, CurrentKey, cancellationToken).ConfigureAwait(false);
        if (!renamed)
        {
            throw new IOException($"failed to rename '{tmpKey}' to '{CurrentKey}'");
        }
    }

    private static async ValueTask PruneOlderSnapshotsAsync(
        ITableFileSystem fs, int retainCount, CancellationToken cancellationToken)
    {
        var ticks = await ListSnapshotsAsync(fs, cancellationToken).ConfigureAwait(false);
        if (ticks.Count <= retainCount)
        {
            return;
        }

        var pruneCount = ticks.Count - retainCount;
        for (var i = 0; i < pruneCount; i++)
        {
            var snapName = SnapDirName(ticks[i]);
            // Delete every file under the snap-T prefix. Best-effort: a
            // failure here leaves orphan files but doesn't break ReadAsync.
            var prefix = snapName + "/";
            var toDelete = new List<string>();
            await foreach (var entry in fs.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
            {
                toDelete.Add(entry.Path);
            }

            foreach (var path in toDelete)
            {
                try
                {
                    await fs.DeleteAsync(path, cancellationToken).ConfigureAwait(false);
                }
                catch (IOException)
                {
                }
            }
        }
    }
}
