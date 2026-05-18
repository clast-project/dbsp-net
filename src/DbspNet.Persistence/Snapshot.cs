using DbspNet.Core.Circuit;

namespace DbspNet.Persistence;

/// <summary>
/// Approach (B) end-of-tick state snapshots. Walks a
/// <see cref="RootCircuit"/>'s operators, calls
/// <see cref="ISnapshotable.Save"/> on those that opt in, and writes a
/// manifest. <see cref="Read(RootCircuit, IBlobStore)"/> validates the
/// manifest's plan and schema fingerprints, then walks the rebuilt
/// circuit and calls <see cref="ISnapshotable.Load"/> on the matching
/// positions.
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> Each snapshot lives under
/// <c>snap-{tick}/</c> with its own <c>manifest.json</c> and
/// <c>op-{i}/</c> subkeys. A top-level <c>current.txt</c> names the
/// latest snapshot (e.g. <c>"snap-42"</c>) and is the source of truth
/// for which one <see cref="Read(RootCircuit, IBlobStore)"/> loads.
/// Older snapshots are retained up to <c>retainCount</c> from
/// <see cref="Write(RootCircuit, IBlobStore, int)"/>; pruning happens
/// after the new snapshot commits.</para>
/// <para><b>Cloud-native commit.</b> Per-op blobs and the snap-T
/// manifest are written directly to their final keys. The commit
/// happens when <c>current.txt</c> is atomically updated last —
/// before that, the new snapshot is invisible to readers (they're
/// still using the prior <c>current.txt</c>). This works on cloud
/// stores (where directory rename doesn't exist) and on a filesystem
/// store (which simulates atomic single-blob writes via tmp+rename
/// internally).</para>
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
    /// Snapshot the circuit to <paramref name="store"/>. Each call
    /// produces a new snapshot under <c>snap-{tick}/</c> and updates
    /// <c>current.txt</c> to point at it. Up to
    /// <paramref name="retainCount"/> most-recent snapshots are kept;
    /// older ones are pruned. Returns the number of operators that
    /// participated.
    /// </summary>
    public static int Write(RootCircuit circuit, IBlobStore store, int retainCount = 1)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(store);
        if (retainCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(retainCount), retainCount, "retainCount must be at least 1");
        }

        var tick = circuit.TickCount;
        var snapName = SnapDirName(tick);

        // Write per-op blobs directly under their final keys. If a Save
        // throws partway through, the partially-written blobs are
        // orphans — invisible to readers because current.txt hasn't
        // been updated. Future writes either at the same tick (which
        // overwrite the same keys) or different ticks (which leave the
        // orphans for retention prune) handle cleanup.
        var snapshotted = new List<int>();
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is not ISnapshotable s)
            {
                continue;
            }

            var ctx = new BlobStoreSnapshotContext(store, snapName + "/op-" + i);
            s.Save(ctx);
            snapshotted.Add(i);
        }

        var manifest = new SnapshotManifest(
            SnapshotManifest.CurrentSchemaVersion,
            SnapshotManifest.ComputeCircuitFingerprint(circuit),
            SnapshotManifest.ComputeSchemaFingerprint(circuit),
            circuit.TickCount,
            circuit.Operators.Count,
            snapshotted);
        manifest.Write(store, snapName + "/manifest.json");

        // Atomic commit. After this single-blob write, the new snapshot
        // is the latest; before it, current.txt still names the prior
        // snapshot (or doesn't exist).
        WriteCurrentPointer(store, snapName);

        // Best-effort prune of older retained snapshots.
        PruneOlderSnapshots(store, retainCount);

        return snapshotted.Count;
    }

    /// <summary>
    /// Convenience overload for the local filesystem case: opens a
    /// <see cref="LocalFileBlobStore"/> rooted at
    /// <paramref name="snapshotDir"/> and forwards to
    /// <see cref="Write(RootCircuit, IBlobStore, int)"/>.
    /// </summary>
    public static int Write(RootCircuit circuit, string snapshotDir, int retainCount = 1)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return Write(circuit, new LocalFileBlobStore(snapshotDir), retainCount);
    }

    /// <summary>
    /// True iff <paramref name="store"/> holds at least one readable
    /// snapshot — i.e. <c>current.txt</c> exists and names a snap-T
    /// directory whose <c>manifest.json</c> exists.
    /// </summary>
    public static bool Exists(IBlobStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        var current = TryReadCurrentPointer(store);
        if (current is null)
        {
            return false;
        }

        return store.Exists(current + "/manifest.json");
    }

    /// <summary>
    /// Convenience overload for the local filesystem case.
    /// </summary>
    public static bool Exists(string snapshotDir)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        if (!Directory.Exists(snapshotDir))
        {
            return false;
        }

        return Exists(new LocalFileBlobStore(snapshotDir));
    }

    /// <summary>
    /// Tick numbers of all retained snapshots in <paramref name="store"/>,
    /// in ascending order.
    /// </summary>
    public static IReadOnlyList<long> ListSnapshots(IBlobStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        var ticks = new HashSet<long>();
        foreach (var key in store.ListKeys("snap-"))
        {
            // Extract the snap-T directory name (the part before the
            // first '/' after the prefix). A key like "snap-5/op-3/x"
            // yields "snap-5".
            var slash = key.IndexOf('/');
            var name = slash < 0 ? key : key.Substring(0, slash);
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
    public static IReadOnlyList<long> ListSnapshots(string snapshotDir)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        if (!Directory.Exists(snapshotDir))
        {
            return Array.Empty<long>();
        }

        return ListSnapshots(new LocalFileBlobStore(snapshotDir));
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
    public static int Read(RootCircuit circuit, IBlobStore store)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(store);

        var current = TryReadCurrentPointer(store);
        if (current is null)
        {
            throw new FileNotFoundException(
                "no snapshot: current.txt missing or empty", CurrentKey);
        }

        var manifestKey = current + "/manifest.json";
        if (!store.Exists(manifestKey))
        {
            throw new FileNotFoundException(
                $"snapshot '{current}' is missing its manifest at '{manifestKey}' " +
                "(possibly pruned or corrupted)",
                manifestKey);
        }

        var manifest = SnapshotManifest.Read(store, manifestKey);
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

        var restored = 0;
        foreach (var i in manifest.SnapshottedIndices)
        {
            if (circuit.Operators[i] is not ISnapshotable s)
            {
                throw new InvalidDataException(
                    $"snapshot expected operator {i} to be ISnapshotable, " +
                    $"but actual type {circuit.Operators[i].GetType().Name} is not");
            }

            var ctx = new BlobStoreSnapshotContext(store, current + "/op-" + i);
            s.Load(ctx);
            restored++;
        }

        circuit.RestoreTickCount(manifest.Tick);
        return restored;
    }

    /// <summary>
    /// Convenience overload for the local filesystem case.
    /// </summary>
    public static int Read(RootCircuit circuit, string snapshotDir)
    {
        ArgumentNullException.ThrowIfNull(snapshotDir);
        return Read(circuit, new LocalFileBlobStore(snapshotDir));
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

    private static string? TryReadCurrentPointer(IBlobStore store)
    {
        if (!store.Exists(CurrentKey))
        {
            return null;
        }

        using var stream = store.OpenRead(CurrentKey);
        using var reader = new StreamReader(stream, System.Text.Encoding.UTF8);
        var contents = reader.ReadToEnd().Trim();
        return string.IsNullOrEmpty(contents) ? null : contents;
    }

    private static void WriteCurrentPointer(IBlobStore store, string snapName)
    {
        using var stream = store.OpenWrite(CurrentKey);
        using var writer = new StreamWriter(stream, System.Text.Encoding.UTF8);
        writer.Write(snapName);
    }

    private static void PruneOlderSnapshots(IBlobStore store, int retainCount)
    {
        var ticks = ListSnapshots(store);
        if (ticks.Count <= retainCount)
        {
            return;
        }

        var pruneCount = ticks.Count - retainCount;
        for (var i = 0; i < pruneCount; i++)
        {
            var snapName = SnapDirName(ticks[i]);
            // Delete every key under the snap-T prefix. Best-effort: a
            // failure here leaves orphan blobs but doesn't break Read.
            var prefix = snapName + "/";
            foreach (var key in store.ListKeys(prefix).ToList())
            {
                try
                {
                    store.Delete(key);
                }
                catch (IOException)
                {
                }
            }
        }
    }
}
