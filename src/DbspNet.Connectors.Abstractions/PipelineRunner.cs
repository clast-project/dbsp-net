// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Threading.Channels;
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// Drives a <see cref="CompiledQuery"/> from input connectors to output connectors,
/// one engine tick per source version (the chosen CDF-follow granularity), with
/// checkpoint/recovery. The runner owns the loop; connectors own their formats. See
/// <c>docs/design-connectors.md</c>.
/// </summary>
public sealed class PipelineRunner
{
    private readonly CompiledQuery _query;
    private readonly List<InputBinding> _inputs;
    private readonly IReadOnlyList<IOutputConnector> _outputs;
    private readonly ICheckpointStore? _checkpoint;
    private readonly int _checkpointEveryTicks;
    private long _ticksSinceCheckpoint;

    private PipelineRunner(
        CompiledQuery query,
        List<InputBinding> inputs,
        IReadOnlyList<IOutputConnector> outputs,
        ICheckpointStore? checkpoint,
        int checkpointEveryTicks)
    {
        _query = query;
        _inputs = inputs;
        _outputs = outputs;
        _checkpoint = checkpoint;
        _checkpointEveryTicks = checkpointEveryTicks;
    }

    /// <summary>The compiled query this runner drives (for inspection/tests).</summary>
    public CompiledQuery Query => _query;

    /// <summary>
    /// Wire a pipeline: resolve each input's schema (infer-unless-declared) and register
    /// it in <paramref name="catalog"/>, <paramref name="compile"/> the query against the
    /// now-complete catalog, bind the outputs to the view schema, and return a runner.
    /// The <paramref name="compile"/> callback owns compile options — it must enable
    /// <see cref="CompileOptions.StoredOutput"/> if any output is
    /// <see cref="OutputMode.Truncate"/>, and pass snapshot codecs if
    /// <paramref name="checkpoint"/> is set.
    /// </summary>
    public static async ValueTask<PipelineRunner> CreateAsync(
        Catalog catalog,
        IReadOnlyList<(IInputConnector Connector, Schema? Declared)> inputs,
        Func<Catalog, CompiledQuery> compile,
        IReadOnlyList<IOutputConnector> outputs,
        ICheckpointStore? checkpoint = null,
        int checkpointEveryTicks = 1,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(inputs);
        ArgumentNullException.ThrowIfNull(compile);
        ArgumentNullException.ThrowIfNull(outputs);
        if (checkpointEveryTicks < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(checkpointEveryTicks), checkpointEveryTicks, "must be >= 1");
        }

        // Schema handshake, then register — the catalog must be complete before compile.
        foreach (var (connector, declared) in inputs)
        {
            var schema = await connector.ResolveSchemaAsync(declared, cancellationToken).ConfigureAwait(false);
            catalog.Register(connector.Name, schema);
        }

        var query = compile(catalog);

        if (outputs.Any(o => o.Mode == OutputMode.Truncate) && !query.HasStoredOutput)
        {
            throw new InvalidOperationException(
                "a Truncate output requires the query to be compiled with CompileOptions.StoredOutput " +
                "(the full view is read via CurrentView).");
        }

        foreach (var o in outputs)
        {
            await o.BindSchemaAsync(query.OutputSchema, cancellationToken).ConfigureAwait(false);
        }

        var bindings = inputs
            .Select(i => new InputBinding(i.Connector, query.Table(i.Connector.Name), i.Connector.InitialOffset))
            .ToList();

        return new PipelineRunner(query, bindings, outputs, checkpoint, checkpointEveryTicks);
    }

    /// <summary>Restore engine state and per-source cursors from the last checkpoint, if
    /// any. Call once before <see cref="DrainAsync"/> to resume a prior run.</summary>
    public async ValueTask RestoreAsync(CancellationToken cancellationToken = default)
    {
        if (_checkpoint is null)
        {
            return;
        }

        var state = await _checkpoint.TryRestoreAsync(_query.Circuit, cancellationToken).ConfigureAwait(false);
        if (state is null)
        {
            return;
        }

        foreach (var sc in state.Offsets)
        {
            var b = _inputs.FirstOrDefault(x => string.Equals(x.Connector.Name, sc.SourceName, StringComparison.Ordinal))
                ?? throw new InvalidDataException($"checkpoint names unknown source '{sc.SourceName}'");
            b.Cursor = b.Connector.ParseOffset(sc.Offset);
        }
    }

    /// <summary>
    /// Process every version currently available across all sources — one engine tick
    /// per version, round-robin across sources — writing each tick's result to the
    /// outputs and checkpointing on cadence. Returns the number of ticks processed.
    /// Idempotent to re-invoke: it resumes from each source's cursor.
    /// </summary>
    public async ValueTask<long> DrainAsync(CancellationToken cancellationToken = default)
    {
        long ticks = 0;
        bool progressed;
        do
        {
            progressed = false;
            foreach (var b in _inputs)
            {
                var latest = await b.Connector.LatestOffsetAsync(cancellationToken).ConfigureAwait(false);
                if (latest is null || latest.CompareTo(b.Cursor) <= 0)
                {
                    continue;
                }

                var batch = await b.Connector.NextAsync(b.Cursor, cancellationToken).ConfigureAwait(false);
                if (batch is null)
                {
                    continue;
                }

                // A version may stream several batches; push each (PushArrow copies the
                // values out, so only one Arrow batch is live at a time), then Step once
                // — one engine tick per source version.
                await foreach (var vb in batch.Content.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    b.TableInput.PushArrow(vb.Batch, vb.Weights);
                }

                _query.Step();
                ticks++;
                b.Cursor = batch.Offset;

                await WriteOutputsAsync(cancellationToken).ConfigureAwait(false);

                if (_checkpoint is not null && ++_ticksSinceCheckpoint >= _checkpointEveryTicks)
                {
                    await CheckpointAsync(cancellationToken).ConfigureAwait(false);
                }

                progressed = true;
            }
        }
        while (progressed);

        return ticks;
    }

    /// <summary>
    /// As <see cref="DrainAsync"/>, but overlaps source read + Arrow decode with engine
    /// compute: a background reader task reads and decodes up to <paramref name="prefetch"/>
    /// versions ahead into a bounded channel while the engine Steps the current version
    /// (and writes its output). The engine is still single-threaded and processes versions
    /// in the same round-robin order, so the result is identical to <see cref="DrainAsync"/>;
    /// only the source-I/O and decode latency is hidden behind compute. The reader decodes
    /// off the engine thread (touching only immutable schema) and hands each version's
    /// deltas to the engine thread, which alone pushes and Steps — two versions never merge
    /// into one tick. Checkpointing records the <em>committed</em> cursor (the engine has
    /// Stepped it), so read-ahead is transparent to exactly-once (a crash re-reads the
    /// un-committed versions from a replayable source).
    /// </summary>
    public async ValueTask<long> DrainPipelinedAsync(int prefetch = 2, CancellationToken cancellationToken = default)
    {
        if (prefetch < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(prefetch), prefetch, "must be >= 1");
        }

        var channel = Channel.CreateBounded<DecodedVersion>(
            new BoundedChannelOptions(prefetch) { SingleReader = true, SingleWriter = true });
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // The reader advances its own cursors (read-ahead); the engine thread advances the
        // committed cursors (b.Cursor). They start equal (post-restore) and end equal.
        var readCursors = _inputs.Select(b => b.Cursor).ToArray();
        var reader = Task.Run(() => ReadAheadAsync(channel.Writer, readCursors, cts.Token), cts.Token);

        // Write-behind (1-deep): the previous tick's output write runs on a background task
        // while the engine Steps + materialises the next tick, hiding the sink I/O. Ordering
        // is preserved by awaiting the pending write before starting the next; durability is
        // preserved by awaiting it before each checkpoint and at the end.
        long ticks = 0;
        var pendingWrite = Task.CompletedTask;
        try
        {
            await foreach (var dv in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                var b = _inputs[dv.SourceIndex];
                b.TableInput.Push(dv.Deltas);
                _query.Step();
                ticks++;
                b.Cursor = dv.Offset;

                // Materialise on the engine thread (CurrentView is live and mutates on the
                // next Step); the actual writes run on a background task.
                var writes = MaterialiseOutputs();
                await pendingWrite.ConfigureAwait(false); // finish + surface the previous write
                pendingWrite = writes.Count == 0
                    ? Task.CompletedTask
                    : Task.Run(() => RunWritesAsync(writes, cancellationToken), cancellationToken);

                if (_checkpoint is not null && ++_ticksSinceCheckpoint >= _checkpointEveryTicks)
                {
                    await pendingWrite.ConfigureAwait(false); // flush output before checkpointing
                    pendingWrite = Task.CompletedTask;
                    await CheckpointAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            await pendingWrite.ConfigureAwait(false); // flush the last write
            await reader.ConfigureAwait(false);       // surface any reader fault after a clean drain
        }
        catch
        {
            await cts.CancelAsync().ConfigureAwait(false);
            try
            {
                await reader.ConfigureAwait(false);
            }
            catch
            {
                // The primary fault is being propagated; ignore the reader's cancellation.
            }

            try
            {
                await pendingWrite.ConfigureAwait(false);
            }
            catch
            {
                // Ditto for an in-flight write.
            }

            throw;
        }

        return ticks;
    }

    private List<OutputWrite> MaterialiseOutputs()
    {
        if (_outputs.Count == 0)
        {
            return [];
        }

        var tick = _query.Circuit.TickCount;
        var writes = new List<OutputWrite>(_outputs.Count);
        foreach (var o in _outputs)
        {
            if (o.Mode == OutputMode.Truncate)
            {
                var view = _query.ToArrowView();
                writes.Add(new OutputWrite(o, view.Rows, view.Weights, tick, IsView: true));
            }
            else
            {
                var delta = _query.ToArrowDelta();
                writes.Add(new OutputWrite(o, delta.Rows, delta.Weights, tick, IsView: false));
            }
        }

        return writes;
    }

    private static async Task RunWritesAsync(List<OutputWrite> writes, CancellationToken cancellationToken)
    {
        foreach (var w in writes)
        {
            if (w.IsView)
            {
                await w.Connector.WriteViewAsync(w.Batch, w.Weights, w.Tick, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await w.Connector.WriteDeltaAsync(w.Batch, w.Weights, w.Tick, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private readonly record struct OutputWrite(
        IOutputConnector Connector,
        Apache.Arrow.RecordBatch Batch,
        long[] Weights,
        long Tick,
        bool IsView);

    // Reader loop: same round-robin as DrainAsync, but only reads + decodes (no push/Step),
    // enqueuing each version's decoded deltas for the engine thread. Bounded-channel writes
    // apply backpressure so read-ahead stays within `prefetch`.
    private async Task ReadAheadAsync(
        ChannelWriter<DecodedVersion> writer, IConnectorOffset[] readCursors, CancellationToken cancellationToken)
    {
        try
        {
            bool progressed;
            do
            {
                progressed = false;
                for (var s = 0; s < _inputs.Count; s++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var b = _inputs[s];
                    var latest = await b.Connector.LatestOffsetAsync(cancellationToken).ConfigureAwait(false);
                    if (latest is null || latest.CompareTo(readCursors[s]) <= 0)
                    {
                        continue;
                    }

                    var batch = await b.Connector.NextAsync(readCursors[s], cancellationToken).ConfigureAwait(false);
                    if (batch is null)
                    {
                        continue;
                    }

                    var deltas = new List<(object?[] Values, long Weight)>();
                    await foreach (var vb in batch.Content.WithCancellation(cancellationToken).ConfigureAwait(false))
                    {
                        deltas.AddRange(b.TableInput.DecodeArrowDeltas(vb.Batch, vb.Weights));
                    }

                    readCursors[s] = batch.Offset;
                    await writer.WriteAsync(new DecodedVersion(s, batch.Offset, deltas), cancellationToken).ConfigureAwait(false);
                    progressed = true;
                }
            }
            while (progressed);

            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.Complete(ex);
        }
    }

    private readonly record struct DecodedVersion(
        int SourceIndex, IConnectorOffset Offset, List<(object?[] Values, long Weight)> Deltas);

    /// <summary>Force a checkpoint now (e.g. before a clean shutdown).</summary>
    public ValueTask CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (_checkpoint is null)
        {
            return ValueTask.CompletedTask;
        }

        _ticksSinceCheckpoint = 0;
        var offsets = _inputs
            .Select(b => new SourceCheckpoint(b.Connector.Name, b.Cursor.Serialize()))
            .ToList();
        return _checkpoint.SaveAsync(_query.Circuit, offsets, cancellationToken);
    }

    private async ValueTask WriteOutputsAsync(CancellationToken cancellationToken)
    {
        if (_outputs.Count == 0)
        {
            return;
        }

        var tick = _query.Circuit.TickCount;
        foreach (var o in _outputs)
        {
            if (o.Mode == OutputMode.Truncate)
            {
                var view = _query.ToArrowView();
                await o.WriteViewAsync(view.Rows, view.Weights, tick, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var delta = _query.ToArrowDelta();
                await o.WriteDeltaAsync(delta.Rows, delta.Weights, tick, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private sealed class InputBinding(IInputConnector connector, TableInput tableInput, IConnectorOffset cursor)
    {
        public IInputConnector Connector { get; } = connector;

        public TableInput TableInput { get; } = tableInput;

        public IConnectorOffset Cursor { get; set; } = cursor;
    }
}
