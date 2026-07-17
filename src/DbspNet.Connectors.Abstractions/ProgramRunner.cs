// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// Drives a whole <see cref="CompiledProgram"/> (a DAG of views maintained as one
/// circuit) from N input connectors (one per source table, matched by
/// <see cref="IInputConnector.Name"/>) to M output connectors (one per output view,
/// matched by <see cref="IOutputConnector.ViewName"/>). Table schemas are <b>declared</b>
/// by the program (from <c>CREATE TABLE</c>), so each input connector validates its Delta
/// source against the declared schema. Output is per-batch (truncate): drain all currently
/// available source versions, then write every output view's full contents once — the
/// natural model for an ivm-bench batch. See <c>docs/design-connectors.md</c>.
/// </summary>
public sealed class ProgramRunner
{
    private readonly CompiledProgram _program;
    private readonly List<InputBinding> _inputs;
    private readonly List<(IOutputConnector Connector, ProgramOutput Output)> _outputs;

    private ProgramRunner(
        CompiledProgram program,
        List<InputBinding> inputs,
        List<(IOutputConnector, ProgramOutput)> outputs)
    {
        _program = program;
        _inputs = inputs;
        _outputs = outputs;
    }

    public CompiledProgram Program => _program;

    /// <summary>
    /// Wire connectors to a compiled program: validate each input source against the
    /// program's declared table schema, and bind each output connector to its view's
    /// schema. Every input connector's <see cref="IInputConnector.Name"/> must name a
    /// program source table; every output connector's <see cref="IOutputConnector.ViewName"/>
    /// must name a program output view.
    /// </summary>
    public static async ValueTask<ProgramRunner> CreateAsync(
        CompiledProgram program,
        IReadOnlyList<IInputConnector> inputs,
        IReadOnlyList<IOutputConnector> outputs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(program);
        ArgumentNullException.ThrowIfNull(inputs);
        ArgumentNullException.ThrowIfNull(outputs);

        var inputBindings = new List<InputBinding>(inputs.Count);
        foreach (var connector in inputs)
        {
            if (!program.Inputs.TryGetValue(connector.Name, out var tableInput))
            {
                throw new ArgumentException(
                    $"input connector '{connector.Name}' does not match any program source table");
            }

            // Declared schema wins — validate/bind the source onto it.
            await connector.ResolveSchemaAsync(tableInput.Schema, cancellationToken).ConfigureAwait(false);
            inputBindings.Add(new InputBinding(connector, tableInput, connector.InitialOffset));
        }

        var outputBindings = new List<(IOutputConnector, ProgramOutput)>(outputs.Count);
        foreach (var connector in outputs)
        {
            if (!program.Outputs.TryGetValue(connector.ViewName, out var programOutput))
            {
                throw new ArgumentException(
                    $"output connector '{connector.ViewName}' does not match any program output view");
            }

            await connector.BindSchemaAsync(programOutput.Schema, cancellationToken).ConfigureAwait(false);
            outputBindings.Add((connector, programOutput));
        }

        return new ProgramRunner(program, inputBindings, outputBindings);
    }

    /// <summary>Ingest every source version currently available (one engine tick per
    /// version, round-robin across sources) without writing outputs. Returns the number of
    /// ticks processed.</summary>
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

                await foreach (var vb in batch.Content.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    b.TableInput.PushArrow(vb.Batch, vb.Weights);
                }

                _program.Step();
                ticks++;
                b.Cursor = batch.Offset;
                progressed = true;
            }
        }
        while (progressed);

        return ticks;
    }

    /// <summary>Write every output view's full current contents to its sink (truncate).
    /// Call after <see cref="DrainAsync"/> to materialise a batch's results.</summary>
    public async ValueTask WriteOutputsAsync(CancellationToken cancellationToken = default)
    {
        var tick = _program.Circuit.TickCount;
        foreach (var (connector, output) in _outputs)
        {
            var arrow = ArrowExtensions.ToArrowView(output.Schema, output.CurrentView);
            await connector.WriteViewAsync(arrow.Rows, arrow.Weights, tick, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Convenience: drain all available versions, then write every output.</summary>
    public async ValueTask<long> RunBatchAsync(CancellationToken cancellationToken = default)
    {
        var ticks = await DrainAsync(cancellationToken).ConfigureAwait(false);
        await WriteOutputsAsync(cancellationToken).ConfigureAwait(false);
        return ticks;
    }

    private sealed class InputBinding(IInputConnector connector, DbspNet.Sql.Compiler.TableInput tableInput, IConnectorOffset cursor)
    {
        public IInputConnector Connector { get; } = connector;

        public DbspNet.Sql.Compiler.TableInput TableInput { get; } = tableInput;

        public IConnectorOffset Cursor { get; set; } = cursor;
    }
}
