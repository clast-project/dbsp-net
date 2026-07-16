// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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

                b.TableInput.PushArrow(batch.Rows, batch.Weights);
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
