// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Sql.Compiler;

namespace DbspNet.Server;

/// <summary>
/// The DbspNet control engine — the analogue of Feldera's pipeline-manager. Deploy a
/// multi-view SQL program bound to Delta input/output tables, then drive it a batch at a
/// time: <see cref="Resume"/> starts ingesting all newly-available source versions in the
/// background (the batch timer starts here), and <see cref="WaitAsync"/> blocks until that
/// batch has drained and every output view has been truncate-written — the point the
/// benchmark stops the timer. Cursors persist across batches, so each resume picks up only
/// the versions appended since the last one.
/// </summary>
public sealed class DbspNetEngine
{
    private readonly object _gate = new();
    private readonly List<IInputConnector> _inputs = new();
    private readonly List<IOutputConnector> _outputs = new();
    private ProgramRunner? _runner;
    private CompiledProgram? _program;
    private Task<long>? _batch;
    private long _resumedAtEpochS;
    private readonly Stopwatch _batchTimer = new();

    /// <summary>Compile the program, wire its Delta connectors, and validate each source
    /// against its declared table schema. Idempotent-replace: a second deploy discards the
    /// prior program. Returns the compile time (excluded from measured batch duration).</summary>
    public async Task<DeployResult> DeployAsync(ProgramSpec spec, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(spec);
        var sw = Stopwatch.StartNew();

        var outputViews = spec.Outputs.Select(o => o.View).ToHashSet(StringComparer.Ordinal);
        // ivm-bench / Spark / DuckDB / Feldera all coerce numeric<->string comparisons.
        var program = SqlProgram.Compile(spec.Program, outputViews, numericStringCoercion: true);

        var inputs = spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, i.Uri))
            .ToList();
        var outputs = spec.Outputs
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, o.Uri, OutputMode.Truncate))
            .ToList();

        var runner = await ProgramRunner.CreateAsync(program, inputs, outputs, cancellationToken)
            .ConfigureAwait(false);

        lock (_gate)
        {
            _runner = runner;
            _program = program;
            _inputs.Clear();
            _inputs.AddRange(inputs);
            _outputs.Clear();
            _outputs.AddRange(outputs);
        }

        sw.Stop();
        return new DeployResult(sw.Elapsed.TotalSeconds, inputs.Count, outputs.Count);
    }

    /// <summary>Start ingesting the current batch (all source versions available now) in
    /// the background; the batch timer starts. Returns the epoch second it started.</summary>
    public ResumeResult Resume()
    {
        lock (_gate)
        {
            var runner = _runner ?? throw new InvalidOperationException("deploy a program before resuming");
            if (_batch is { IsCompleted: false })
            {
                throw new InvalidOperationException("a batch is already running; wait for it first");
            }

            _resumedAtEpochS = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            _batchTimer.Restart();
            _batch = Task.Run(() => runner.RunBatchAsync().AsTask());
            return new ResumeResult(_resumedAtEpochS);
        }
    }

    /// <summary>Block until the resumed batch has drained and its outputs are written;
    /// stop the timer and report the duration + per-output row counts.</summary>
    public async Task<WaitResult> WaitAsync(CancellationToken cancellationToken = default)
    {
        Task<long> batch;
        lock (_gate)
        {
            batch = _batch ?? throw new InvalidOperationException("resume a batch before waiting");
        }

        var ticks = await batch.WaitAsync(cancellationToken).ConfigureAwait(false);

        lock (_gate)
        {
            _batchTimer.Stop();
            return new WaitResult(_batchTimer.Elapsed.TotalSeconds, ticks, OutputStats());
        }
    }

    /// <summary>Dry-run: compile the whole program into one circuit (no connectors, no
    /// Delta) and report success or the first error. Lets the harness validate that the
    /// model DAG compiles before deploying — the analogue of Feldera's compile step.</summary>
    public static CompileResult Compile(CompileSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        try
        {
            var program = SqlProgram.Compile(
                spec.Program, spec.Outputs.ToHashSet(StringComparer.Ordinal), numericStringCoercion: true);
            return new CompileResult(true, program.Outputs.Count, null);
        }
        catch (Exception ex)
        {
            return new CompileResult(false, 0, ex.Message);
        }
    }

    /// <summary>Point-in-time stats — for polling / observability.</summary>
    public EngineStats GetStats()
    {
        lock (_gate)
        {
            return new EngineStats(
                Deployed: _program is not null,
                BatchRunning: _batch is { IsCompleted: false },
                TickCount: _program?.Circuit.TickCount ?? 0,
                Outputs: OutputStats());
        }
    }

    private List<OutputStat> OutputStats() =>
        _program is null
            ? new List<OutputStat>()
            : _program.Outputs.Select(kv => new OutputStat(kv.Key, kv.Value.CurrentView.Count)).ToList();
}
