using Apache.Arrow;
using Apache.Arrow.Ipc;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Persistence.IO.Local;
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
/// <para><b>Lifecycle.</b> Construct via
/// <see cref="CreateAsync(CompiledQuery, ITableFileSystem, ITableFileSystem?, CancellationToken)"/>
/// (or the path-based convenience overload). The factory discovers any
/// existing WAL segments, validates the manifest's plan fingerprint,
/// replays existing segments to bring the engine to the post-WAL state,
/// then opens a fresh segment for new appends. <see cref="StepAsync"/>
/// takes over <see cref="CompiledQuery.Step"/> on the recorder side: it
/// flushes the per-tick input buffer, then steps the circuit. The new
/// segment is closed atomically on <see cref="IAsyncDisposable.DisposeAsync"/>;
/// the manifest is rewritten to include it.</para>
/// <para><b>Plan fingerprint.</b> The manifest stores a hash of the
/// query's input table schemas (column names + types). On reopen, a
/// mismatch throws — preventing a WAL recorded against schema A from
/// being silently replayed into schema B. The fingerprint deliberately
/// ignores the SELECT body, so changes to the query don't invalidate
/// the WAL.</para>
/// <para><b>Storage.</b> Backed by <see cref="ITableFileSystem"/>:
/// file paths are <c>manifest.json</c> and per-(table, segment) entries
/// like <c>{table}.{segmentId}.arrows</c>. Each segment is a long-
/// lived sequential file that the cloud impl backs with a multipart
/// upload — finalized on segment rotation or
/// <see cref="DisposeAsync"/>.</para>
/// </remarks>
public sealed class WalRecorder : IAsyncDisposable, IDisposable
{
    private const string ManifestKey = "manifest.json";

    private readonly CompiledQuery _query;
    private readonly ITableFileSystem _fs;
    private readonly ITableFileSystem? _snapshotFs;
    private readonly string _planFingerprint;

    // Per-table per-tick accumulator. Multiple Push calls within one tick
    // are summed (Z-set semantics) so the WAL records one batch per
    // (table, tick) regardless of how many user calls produced it.
    private readonly Dictionary<string, Dictionary<StructuralRow, long>> _tickBuffers
        = new(StringComparer.Ordinal);

    // Subscriptions on each TableInput — kept so we can detach on Dispose.
    private readonly List<(TableInput Input, Action<ZSet<StructuralRow, Z64>> Handler)>
        _subscriptions = new();

    // Open writers for the current recording segment, paired with the
    // ISequentialFile each one drains into so we can close both cleanly.
    private readonly Dictionary<string, (ArrowDeltaWriter Writer, ISequentialFile File)> _writers
        = new(StringComparer.Ordinal);

    private List<WalSegment> _segments = new();
    private int _currentSegmentTicks;
    private int _currentSegmentId;
    private long _currentSegmentStartTick;
    private bool _disposed;

    private WalRecorder(CompiledQuery query, ITableFileSystem walFs, ITableFileSystem? snapshotFs)
    {
        _query = query;
        _fs = walFs;
        _snapshotFs = snapshotFs;
        _planFingerprint = WalManifest.ComputePlanFingerprint(query);
    }

    /// <summary>
    /// Convenience factory for the local-filesystem case. Equivalent to
    /// <c>CreateAsync(query, new LocalTableFileSystem(walPath),
    /// snapshotDir is null ? null : new LocalTableFileSystem(snapshotDir))</c>.
    /// </summary>
    public static ValueTask<WalRecorder> CreateAsync(
        CompiledQuery query,
        string walPath,
        string? snapshotDir = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(walPath);
        return CreateAsync(
            query,
            new LocalTableFileSystem(walPath),
            snapshotDir is null ? null : new LocalTableFileSystem(snapshotDir),
            cancellationToken);
    }

    public static async ValueTask<WalRecorder> CreateAsync(
        CompiledQuery query,
        ITableFileSystem walFs,
        ITableFileSystem? snapshotFs = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(walFs);
        var recorder = new WalRecorder(query, walFs, snapshotFs);
        await recorder.InitializeAsync(cancellationToken).ConfigureAwait(false);
        return recorder;
    }

    private async ValueTask InitializeAsync(CancellationToken cancellationToken)
    {
        // Hybrid recovery: load the snapshot first (if present), then
        // replay the WAL from snapshotTick onwards. The snapshot brings
        // the circuit to tick T quickly; the WAL replays only the
        // incremental delta since.
        long snapshotTick = 0;
        if (_snapshotFs is not null && await Snapshot.ExistsAsync(_snapshotFs, cancellationToken).ConfigureAwait(false))
        {
            await Snapshot.ReadAsync(_query.Circuit, _snapshotFs, cancellationToken).ConfigureAwait(false);
            snapshotTick = _query.Circuit.TickCount;
        }

        if (await _fs.ExistsAsync(ManifestKey, cancellationToken).ConfigureAwait(false))
        {
            var manifest = await WalManifest.ReadAsync(_fs, ManifestKey, cancellationToken).ConfigureAwait(false);
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
            await ReplayAsync(manifest, snapshotTick, cancellationToken).ConfigureAwait(false);
            _segments = manifest.Segments.ToList();
            _currentSegmentId = (_segments.Count == 0 ? 0 : _segments[^1].Id) + 1;
            _currentSegmentStartTick = _segments.Count == 0
                ? 0
                : _segments[^1].StartTick + _segments[^1].Ticks;
        }
        else
        {
            // No WAL yet, but we may have just loaded a snapshot. Future
            // segments append after the snapshot tick so absolute tick
            // numbers stay consistent across the session boundary.
            _currentSegmentStartTick = snapshotTick;
        }

        SubscribeToInputs();
        await OpenWritersAsync(cancellationToken).ConfigureAwait(false);
        await WriteManifestAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Equivalent to <c>query.Step()</c>, but first flushes the per-tick
    /// input buffer to the WAL so the durable record sees this tick's
    /// inputs before they hit the circuit. Every table writes one batch
    /// per tick (possibly empty), so segment batch indices stay aligned
    /// with tick numbers across all tables.
    /// </summary>
    public ValueTask StepAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (var table in _writers.Keys)
        {
            var schema = _query.Inputs[table].Schema;
            var buffer = _tickBuffers[table];
            var delta = MaterialiseTickBuffer(schema, buffer);
            _writers[table].Writer.WriteDelta(delta);
            buffer.Clear();
        }

        _currentSegmentTicks++;
        _query.Step();
        return default;
    }

    /// <summary>
    /// Take an end-of-tick state snapshot, prune the WAL prefix that's
    /// fully covered by the snapshot, and rotate to a fresh WAL segment.
    /// Requires the recorder to have been constructed with a snapshot
    /// store. Must be called between <see cref="StepAsync"/> invocations —
    /// i.e. with no pending input pushes.
    /// </summary>
    public async ValueTask WriteSnapshotAsync(
        int snapshotRetainCount = 1, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_snapshotFs is null)
        {
            throw new InvalidOperationException(
                "WriteSnapshotAsync: this WalRecorder was constructed without a snapshot store; " +
                "pass one to CreateAsync to enable snapshotting.");
        }

        EnsureBuffersEmpty();

        // Close current segment writers — IPC streams need the trailer
        // before they can be replayed by another reader.
        foreach (var (writer, file) in _writers.Values)
        {
            writer.Dispose();
            await file.DisposeAsync().ConfigureAwait(false);
        }

        _writers.Clear();

        // Move the just-closed pending segment into _segments so the
        // prune step can drop it (its end-tick equals snapshotTick).
        if (_currentSegmentTicks > 0)
        {
            _segments.Add(new WalSegment(
                _currentSegmentId, _currentSegmentTicks, _currentSegmentStartTick));
        }

        await Snapshot.WriteAsync(_query.Circuit, _snapshotFs, snapshotRetainCount, cancellationToken).ConfigureAwait(false);
        var snapshotTick = _query.Circuit.TickCount;

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

        // Rotate to a fresh segment. Segment ids are monotonic — never
        // reused, even across prunes — so on-store key names can't
        // collide with stale entries.
        _currentSegmentId++;
        _currentSegmentStartTick = snapshotTick;
        _currentSegmentTicks = 0;
        await OpenWritersAsync(cancellationToken).ConfigureAwait(false);
        await WriteManifestAsync(cancellationToken).ConfigureAwait(false);

        // Manifest committed without the pruned segments. Best-effort
        // cleanup of the orphan files.
        foreach (var seg in pruned)
        {
            foreach (var table in _query.Inputs.Keys)
            {
                await _fs.DeleteAsync(SegmentKey(table, seg.Id), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        foreach (var (input, handler) in _subscriptions)
        {
            input.OnPushed -= handler;
        }

        foreach (var (writer, file) in _writers.Values)
        {
            writer.Dispose();
            await file.DisposeAsync().ConfigureAwait(false);
        }

        await WriteManifestAsync(default).ConfigureAwait(false);
        _disposed = true;
    }

    /// <summary>
    /// Synchronous fallback for callers that haven't migrated to
    /// <see cref="DisposeAsync"/>. Blocks on the underlying async work.
    /// </summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    private static string SegmentKey(string tableName, int segmentId) =>
        $"{tableName}.{segmentId}.arrows";

    private void EnsureBuffersEmpty()
    {
        foreach (var (table, buf) in _tickBuffers)
        {
            if (buf.Count > 0)
            {
                throw new InvalidOperationException(
                    $"WriteSnapshotAsync: pending input on table '{table}' — call StepAsync() to " +
                    "commit unpushed deltas before snapshotting, or avoid pushing inputs " +
                    "between StepAsync and WriteSnapshotAsync.");
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

    private async ValueTask OpenWritersAsync(CancellationToken cancellationToken)
    {
        foreach (var (table, input) in _query.Inputs)
        {
            var dataSchema = ArrowSchemaBridge.ToArrow(input.Schema);
            var schemaWithWeight = ArrowIpcExtensions.AppendWeightField(dataSchema);
            var file = await _fs.CreateAsync(SegmentKey(table, _currentSegmentId), overwrite: true, cancellationToken).ConfigureAwait(false);
            var stream = file.AsStream();
            var writer = new ArrowDeltaWriter(stream, schemaWithWeight, leaveOpen: false);
            _writers[table] = (writer, file);
        }
    }

    private async ValueTask ReplayAsync(WalManifest manifest, long snapshotTick, CancellationToken cancellationToken)
    {
        foreach (var segment in manifest.Segments)
        {
            // Fast-skip whole segments that lie entirely inside the snapshot
            // range — no need to open the IPC streams at all.
            if (segment.StartTick + segment.Ticks <= snapshotTick)
            {
                continue;
            }

            await ReplaySegmentAsync(segment, snapshotTick, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ReplaySegmentAsync(WalSegment segment, long snapshotTick, CancellationToken cancellationToken)
    {
        var readers = new Dictionary<string, (ArrowStreamReader Reader, IRandomAccessFile File, Stream Stream)>(StringComparer.Ordinal);
        try
        {
            foreach (var table in _query.Inputs.Keys)
            {
                var key = SegmentKey(table, segment.Id);
                if (!await _fs.ExistsAsync(key, cancellationToken).ConfigureAwait(false))
                {
                    throw new FileNotFoundException(
                        $"WAL segment file '{key}' missing — manifest says segment {segment.Id} should exist", key);
                }

                var file = await _fs.OpenReadAsync(key, cancellationToken).ConfigureAwait(false);
                var stream = file.AsStream();
                readers[table] = (new ArrowStreamReader(stream, leaveOpen: false), file, stream);
            }

            for (var tick = 0; tick < segment.Ticks; tick++)
            {
                var absoluteTick = segment.StartTick + tick;
                var alreadyApplied = absoluteTick < snapshotTick;

                foreach (var (table, entry) in readers)
                {
                    var batch = entry.Reader.ReadNextRecordBatch();
                    if (batch is null)
                    {
                        throw new InvalidDataException(
                            $"WAL segment {segment.Id}, table '{table}': " +
                            $"expected {segment.Ticks} batches, got {tick}");
                    }

                    using (batch)
                    {
                        if (alreadyApplied || batch.Length == 0)
                        {
                            continue;
                        }

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
            foreach (var entry in readers.Values)
            {
                entry.Reader.Dispose();
                await entry.File.DisposeAsync().ConfigureAwait(false);
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

    private ValueTask WriteManifestAsync(CancellationToken cancellationToken)
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
        return manifest.WriteAsync(_fs, ManifestKey, cancellationToken);
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
