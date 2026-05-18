using Apache.Arrow;
using Apache.Arrow.Ipc;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Persistence;

/// <summary>
/// Approach (A) input replay. Captures every per-tick input delta to a
/// durable Arrow IPC log, and replays existing logs on reopen so the
/// engine reaches the same state as the last write.
/// </summary>
/// <remarks>
/// <para><b>Lifecycle.</b> Construct with the compiled query and an
/// <see cref="IBlobStore"/> (or a local directory path via the
/// convenience constructor). The recorder discovers any existing WAL
/// segments, validates the manifest's plan fingerprint, calls
/// <c>Replay</c> to bring the engine to the post-WAL state, and
/// then opens a fresh segment for new appends. <see cref="Step"/>
/// takes over <see cref="CompiledQuery.Step"/> on the recorder side:
/// it flushes the per-tick input buffer, then steps the circuit. The
/// new segment is closed atomically on
/// <see cref="IDisposable.Dispose"/>; the manifest is rewritten to
/// include it.</para>
/// <para><b>Plan fingerprint.</b> The manifest stores a hash of the
/// query's input table schemas (column names + types). On reopen, a
/// mismatch throws — preventing a WAL recorded against schema A from
/// being silently replayed into schema B. The fingerprint deliberately
/// ignores the SELECT body, so changes to the query don't invalidate
/// the WAL.</para>
/// <para><b>Storage.</b> Backed by <see cref="IBlobStore"/>: blob
/// keys are <c>manifest.json</c> and per-(table, segment) entries
/// like <c>{table}.{segmentId}.arrows</c>. Each segment is a long-
/// lived <c>OpenWrite</c> stream that the cloud impl backs with a
/// multipart upload — finalized on segment rotation or
/// <c>Dispose</c>.</para>
/// </remarks>
public sealed class WalRecorder : IDisposable
{
    private const string ManifestKey = "manifest.json";

    private readonly CompiledQuery _query;
    private readonly IBlobStore _store;
    private readonly IBlobStore? _snapshotStore;
    private readonly string _planFingerprint;

    // Per-table per-tick accumulator. Multiple Push calls within one tick
    // are summed (Z-set semantics) so the WAL records one batch per
    // (table, tick) regardless of how many user calls produced it.
    private readonly Dictionary<string, Dictionary<StructuralRow, long>> _tickBuffers
        = new(StringComparer.Ordinal);

    // Subscriptions on each TableInput — kept so we can detach on Dispose.
    private readonly List<(TableInput Input, Action<ZSet<StructuralRow, Z64>> Handler)>
        _subscriptions = new();

    // Open writers for the current recording segment.
    private readonly Dictionary<string, ArrowDeltaWriter> _writers
        = new(StringComparer.Ordinal);

    private List<WalSegment> _segments = new();
    private int _currentSegmentTicks;
    private int _currentSegmentId;
    private long _currentSegmentStartTick;
    private bool _disposed;

    /// <summary>
    /// Convenience constructor for the local-filesystem case. Equivalent
    /// to <c>new WalRecorder(query, new LocalFileBlobStore(walPath),
    /// snapshotDir is null ? null : new LocalFileBlobStore(snapshotDir))</c>.
    /// </summary>
    public WalRecorder(CompiledQuery query, string walPath, string? snapshotDir = null)
        : this(query,
               new LocalFileBlobStore(walPath ?? throw new ArgumentNullException(nameof(walPath))),
               snapshotDir is null ? null : new LocalFileBlobStore(snapshotDir))
    {
    }

    public WalRecorder(CompiledQuery query, IBlobStore walStore, IBlobStore? snapshotStore = null)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(walStore);
        _query = query;
        _store = walStore;
        _snapshotStore = snapshotStore;
        _planFingerprint = WalManifest.ComputePlanFingerprint(query);

        // Hybrid recovery: load the snapshot first (if present), then
        // replay the WAL from snapshotTick onwards. The snapshot brings
        // the circuit to tick T quickly; the WAL replays only the
        // incremental delta since.
        long snapshotTick = 0;
        if (snapshotStore is not null && Snapshot.Exists(snapshotStore))
        {
            Snapshot.Read(query.Circuit, snapshotStore);
            snapshotTick = query.Circuit.TickCount;
        }

        if (_store.Exists(ManifestKey))
        {
            var manifest = WalManifest.Read(_store, ManifestKey);
            if (manifest.SchemaVersion != WalManifest.CurrentSchemaVersion)
            {
                throw new InvalidDataException(
                    $"WAL schema version {manifest.SchemaVersion} not supported " +
                    $"(this build expects {WalManifest.CurrentSchemaVersion})");
            }

            if (manifest.PlanFingerprint != _planFingerprint)
            {
                throw new InvalidDataException(
                    "WAL plan fingerprint mismatch — the table schemas in this " +
                    "store differ from the current query. Recorded: " +
                    $"{manifest.PlanFingerprint}; current: {_planFingerprint}.");
            }

            ValidateTablesMatch(manifest);
            ValidateSnapshotPairing(manifest, snapshotTick);
            Replay(manifest, snapshotTick);
            _segments = manifest.Segments.ToList();
            _currentSegmentId = (_segments.Count == 0 ? 0 : _segments[^1].Id) + 1;
            _currentSegmentStartTick = _segments.Count == 0
                ? 0
                : _segments[^1].StartTick + _segments[^1].Ticks;
        }
        else
        {
            // No WAL on store yet, but we may have just loaded a snapshot.
            // Future segments append after the snapshot tick so absolute
            // tick numbers stay consistent across the session boundary.
            _currentSegmentStartTick = snapshotTick;
        }

        SubscribeToInputs();
        OpenWriters();
        WriteManifest();  // ensure manifest exists from session start
    }

    /// <summary>
    /// Equivalent to <c>query.Step()</c>, but first flushes the per-tick
    /// input buffer to the WAL so the durable record sees this tick's
    /// inputs before they hit the circuit. Every table writes one batch
    /// per tick (possibly empty), so segment batch indices stay aligned
    /// with tick numbers across all tables.
    /// </summary>
    public void Step()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (var table in _writers.Keys)
        {
            var schema = _query.Inputs[table].Schema;
            var buffer = _tickBuffers[table];
            var delta = MaterialiseTickBuffer(schema, buffer);
            _writers[table].WriteDelta(delta);
            buffer.Clear();
        }

        _currentSegmentTicks++;
        _query.Step();
    }

    /// <summary>
    /// Take an end-of-tick state snapshot, prune the WAL prefix that's
    /// fully covered by the snapshot, and rotate to a fresh WAL segment.
    /// Requires the recorder to have been constructed with a snapshot
    /// store. Must be called between <see cref="Step"/> invocations —
    /// i.e. with no pending input pushes.
    /// </summary>
    public void WriteSnapshot(int snapshotRetainCount = 1)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_snapshotStore is null)
        {
            throw new InvalidOperationException(
                "WriteSnapshot: this WalRecorder was constructed without a snapshot store; " +
                "pass one to the constructor to enable snapshotting.");
        }

        EnsureBuffersEmpty();

        // Close current segment writers — IPC streams need the trailer
        // before they can be replayed by another reader.
        foreach (var w in _writers.Values)
        {
            w.Dispose();
        }

        _writers.Clear();

        // Move the just-closed pending segment into _segments so the
        // prune step can drop it (its end-tick equals snapshotTick).
        if (_currentSegmentTicks > 0)
        {
            _segments.Add(new WalSegment(
                _currentSegmentId, _currentSegmentTicks, _currentSegmentStartTick));
        }

        Snapshot.Write(_query.Circuit, _snapshotStore, snapshotRetainCount);
        var snapshotTick = _query.Circuit.TickCount;

        // Identify segments to prune (the ones fully covered by the
        // snapshot tick) and keepers (the ones that still hold replayable
        // ticks). Update the manifest BEFORE deleting any blobs so a
        // crash mid-cleanup leaves orphan blobs that aren't referenced —
        // replay never tries to open them, and the next WriteSnapshot
        // would clean them on subsequent retention prune. The reverse
        // order would leave the manifest pointing at deleted blobs.
        var keepers = new List<WalSegment>();
        var pruned = new List<WalSegment>();
        foreach (var seg in _segments)
        {
            if (seg.StartTick + seg.Ticks <= snapshotTick)
            {
                pruned.Add(seg);
            }
            else
            {
                keepers.Add(seg);
            }
        }

        _segments = keepers;

        // Rotate to a fresh segment for further recording. Segment ids
        // are monotonic — never reused, even across prunes — so on-store
        // key names can't collide with stale entries.
        _currentSegmentId++;
        _currentSegmentStartTick = snapshotTick;
        _currentSegmentTicks = 0;
        OpenWriters();
        WriteManifest();

        // Manifest is now committed without the pruned segments.
        // Best-effort cleanup of the orphan blobs.
        foreach (var seg in pruned)
        {
            foreach (var table in _query.Inputs.Keys)
            {
                _store.Delete(SegmentKey(table, seg.Id));
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        foreach (var (input, handler) in _subscriptions)
        {
            input.OnPushed -= handler;
        }

        foreach (var w in _writers.Values)
        {
            w.Dispose();
        }

        // WriteManifest already includes the current pending segment if
        // _currentSegmentTicks > 0 — no need to double-add to _segments
        // here.
        WriteManifest();
        _disposed = true;
    }

    private static string SegmentKey(string tableName, int segmentId) =>
        $"{tableName}.{segmentId}.arrows";

    private void EnsureBuffersEmpty()
    {
        foreach (var (table, buf) in _tickBuffers)
        {
            if (buf.Count > 0)
            {
                throw new InvalidOperationException(
                    $"WriteSnapshot: pending input on table '{table}' — call Step() to " +
                    "commit unpushed deltas before snapshotting, or avoid pushing inputs " +
                    "between Step and WriteSnapshot.");
            }
        }
    }

    private void SubscribeToInputs()
    {
        foreach (var (table, input) in _query.Inputs)
        {
            _tickBuffers[table] = new Dictionary<StructuralRow, long>();
            var local = table;
            void Handler(ZSet<StructuralRow, Z64> zset) => Accumulate(local, zset);
            input.OnPushed += Handler;
            _subscriptions.Add((input, Handler));
        }
    }

    private void Accumulate(string table, ZSet<StructuralRow, Z64> zset)
    {
        var buf = _tickBuffers[table];
        foreach (var (row, weight) in zset)
        {
            if (buf.TryGetValue(row, out var existing))
            {
                var sum = existing + weight.Value;
                if (sum == 0)
                {
                    buf.Remove(row);
                }
                else
                {
                    buf[row] = sum;
                }
            }
            else
            {
                buf[row] = weight.Value;
            }
        }
    }

    private void OpenWriters()
    {
        foreach (var (table, input) in _query.Inputs)
        {
            var dataSchema = ArrowSchemaBridge.ToArrow(input.Schema);
            var schemaWithWeight = ArrowIpcExtensions.AppendWeightField(dataSchema);
            var stream = _store.OpenWrite(SegmentKey(table, _currentSegmentId));
            _writers[table] = new ArrowDeltaWriter(stream, schemaWithWeight, leaveOpen: false);
        }
    }

    private void Replay(WalManifest manifest, long snapshotTick)
    {
        foreach (var segment in manifest.Segments)
        {
            // Fast-skip whole segments that lie entirely inside the snapshot
            // range — no need to open the IPC streams at all.
            if (segment.StartTick + segment.Ticks <= snapshotTick)
            {
                continue;
            }

            ReplaySegment(segment, snapshotTick);
        }
    }

    private void ReplaySegment(WalSegment segment, long snapshotTick)
    {
        var readers = new Dictionary<string, ArrowStreamReader>(StringComparer.Ordinal);
        try
        {
            foreach (var table in _query.Inputs.Keys)
            {
                var key = SegmentKey(table, segment.Id);
                if (!_store.Exists(key))
                {
                    throw new FileNotFoundException(
                        $"WAL segment blob '{key}' missing — manifest says segment {segment.Id} should exist", key);
                }

                var stream = _store.OpenRead(key);
                readers[table] = new ArrowStreamReader(stream, leaveOpen: false);
            }

            for (var tick = 0; tick < segment.Ticks; tick++)
            {
                var absoluteTick = segment.StartTick + tick;
                var alreadyApplied = absoluteTick < snapshotTick;

                foreach (var (table, reader) in readers)
                {
                    var batch = reader.ReadNextRecordBatch();
                    if (batch is null)
                    {
                        throw new InvalidDataException(
                            $"WAL segment {segment.Id}, table '{table}': " +
                            $"expected {segment.Ticks} batches, got {tick}");
                    }

                    using (batch)
                    {
                        // Ticks already absorbed by the snapshot get their
                        // batches read-and-discarded — the IPC stream is
                        // forward-only, so we still have to advance past
                        // them, but we don't push them into the circuit.
                        if (alreadyApplied || batch.Length == 0)
                        {
                            continue;
                        }

                        // Ingest via the standard IPC path: detect
                        // __weight column and forward to PushArrow.
                        using var memory = new MemoryStream();
                        using (var w = new ArrowStreamWriter(memory, batch.Schema, leaveOpen: true))
                        {
                            w.WriteRecordBatch(batch);
                            w.WriteEnd();
                        }

                        memory.Position = 0;
                        _query.Inputs[table].ReadArrowStream(memory);
                    }
                }

                if (!alreadyApplied)
                {
                    _query.Step();
                }
            }
        }
        finally
        {
            foreach (var r in readers.Values)
            {
                r.Dispose();
            }
        }
    }

    private static void ValidateSnapshotPairing(WalManifest manifest, long snapshotTick)
    {
        if (snapshotTick == 0)
        {
            return;
        }

        // The snapshot says we're at tick T; the WAL must contain ticks up
        // to at least T-1 inclusive, otherwise the WAL is older than the
        // snapshot and there's no consistent way to continue.
        var walTotalTicks = 0L;
        if (manifest.Segments.Count > 0)
        {
            var last = manifest.Segments[^1];
            walTotalTicks = last.StartTick + last.Ticks;
        }

        if (walTotalTicks < snapshotTick)
        {
            throw new InvalidDataException(
                $"snapshot/WAL pairing inconsistent: snapshot is at tick {snapshotTick} " +
                $"but the WAL only covers {walTotalTicks} ticks. The WAL is older than " +
                "the snapshot — check that both stores belong to the same session.");
        }
    }

    private void ValidateTablesMatch(WalManifest manifest)
    {
        var current = _query.Inputs.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();
        var recorded = manifest.Tables.OrderBy(k => k, StringComparer.Ordinal).ToArray();
        if (!current.SequenceEqual(recorded, StringComparer.Ordinal))
        {
            throw new InvalidDataException(
                $"WAL table set {{{string.Join(",", recorded)}}} does not match " +
                $"current query's input tables {{{string.Join(",", current)}}}");
        }
    }

    private void WriteManifest()
    {
        // Always include the current pending segment, even with 0 ticks.
        // Preserves the StartTick high-water mark across a WriteSnapshot
        // that pruned every committed segment — without it, a reopen
        // would see an empty manifest and falsely conclude the WAL is
        // older than the snapshot. An empty segment file is fine: replay
        // opens it, reads 0 batches, closes.
        var snapshot = _segments.ToList();
        snapshot.Add(new WalSegment(_currentSegmentId, _currentSegmentTicks, _currentSegmentStartTick));

        var manifest = new WalManifest(
            WalManifest.CurrentSchemaVersion,
            _planFingerprint,
            _query.Inputs.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            snapshot);
        manifest.Write(_store, ManifestKey);
    }

    private static ArrowDelta MaterialiseTickBuffer(
        SqlSchema schema,
        Dictionary<StructuralRow, long> buffer)
    {
        var rowCount = buffer.Count;
        var columnCount = schema.Count;
        var perColumn = new object?[columnCount][];
        for (var c = 0; c < columnCount; c++)
        {
            perColumn[c] = new object?[rowCount];
        }

        var weights = new long[rowCount];
        var i = 0;
        foreach (var (row, weight) in buffer)
        {
            for (var c = 0; c < columnCount; c++)
            {
                perColumn[c][i] = row[c];
            }

            weights[i] = weight;
            i++;
        }

        var arrays = new IArrowArray[columnCount];
        for (var c = 0; c < columnCount; c++)
        {
            arrays[c] = ArrowColumnsAccessor.Build(schema[c].Type, perColumn[c]);
        }

        var arrowSchema = ArrowSchemaBridge.ToArrow(schema);
        var batch = new RecordBatch(arrowSchema, arrays, rowCount);
        return new ArrowDelta(batch, weights);
    }
}

/// <summary>
/// Bridge to <c>ArrowColumns.Build</c> — that helper is internal to
/// <c>DbspNet.Arrow</c>. Persistence sees it via
/// <c>InternalsVisibleTo</c>.
/// </summary>
internal static class ArrowColumnsAccessor
{
    public static IArrowArray Build(SqlType type, object?[] values) =>
        ArrowColumns.Build(type, values);
}
