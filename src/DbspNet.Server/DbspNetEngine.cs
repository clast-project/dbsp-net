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
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

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
    // How output tables are written. Incremental (the default) truncates each table once and
    // then writes only each batch's net change, which is what a "truncate" binding means in
    // Feldera's Delta connector; DBSPNET_OUTPUT_MODE=truncate rewrites every view each batch.
    private static readonly OutputMode OutputModeSetting = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_OUTPUT_MODE")?.Trim(), "truncate", StringComparison.OrdinalIgnoreCase)
        ? OutputMode.Truncate
        : OutputMode.Incremental;

    // Worker count W. DBSPNET_WORKERS > 1 compiles the program data-parallel across W
    // replicas when every view shards, and falls back to the serial program (logged) when
    // one does not. Default 1: the serial program, today's behaviour.
    private static readonly int WorkersSetting =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_WORKERS"), out var w) && w > 1 ? w : 1;

    private readonly int _workers;
    private readonly object _gate = new();
    private readonly List<IInputConnector> _inputs = new();
    private readonly List<IOutputConnector> _outputs = new();
    private ProgramRunner? _runner;
    private Task<long>? _batch;
    private long _resumedAtEpochS;
    private readonly Stopwatch _batchTimer = new();

    /// <summary>An engine whose worker count comes from <c>DBSPNET_WORKERS</c> (default 1).</summary>
    public DbspNetEngine()
        : this(WorkersSetting)
    {
    }

    /// <summary>An engine that runs programs across <paramref name="workers"/> replicas when
    /// they shard (see <see cref="DeployAsync"/>).</summary>
    public DbspNetEngine(int workers)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(workers, 1);
        _workers = workers;
    }

    /// <summary>Compile the program, wire its Delta connectors, and validate each source
    /// against its declared table schema. Idempotent-replace: a second deploy discards the
    /// prior program. Returns the compile time (excluded from measured batch duration).
    /// <para>
    /// When a snapshot directory is configured (<see cref="ProgramSpec.SnapshotDir"/> or
    /// <c>DBSPNET_SNAPSHOT_DIR</c>), the program is compiled with filesystem snapshot
    /// codecs and wired to a <see cref="SnapshotCheckpointStore"/>, so every batch ends
    /// durable and a redeploy resumes from the last checkpoint. Restore happens here, in
    /// deploy — outside the measured batch span, like the compile.
    /// </para>
    /// <para>
    /// With more than one worker (<c>DBSPNET_WORKERS</c>), the program is compiled
    /// data-parallel (<see cref="SqlProgram.TryCompileParallel"/>) and driven by the same
    /// runner. It falls back to the serial program, and logs why to stderr, when any view the
    /// outputs reach cannot be sharded, and also when persistence is on: the runner's
    /// checkpoint store snapshots one serial circuit and the per-worker parallel snapshot has
    /// no offset-atomic store yet, so a persistent deploy is always serial.
    /// <see cref="DeployResult.Workers"/> and <see cref="DeployResult.SerialFallback"/> report
    /// what was chosen.
    /// </para></summary>
    public async Task<DeployResult> DeployAsync(ProgramSpec spec, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(spec);
        var sw = Stopwatch.StartNew();

        var snapshotDir = spec.SnapshotDir is { Length: > 0 } d
            ? d
            : Environment.GetEnvironmentVariable("DBSPNET_SNAPSHOT_DIR");
        var persistent = !string.IsNullOrEmpty(snapshotDir);

        // Everything the program must COMPUTE: the written views plus any stored-but-unwritten
        // ones. A view missing from this set is dead-view-pruned along with its upstream chain,
        // so leaving StoredViews out silently deletes work the benchmark is measuring.
        var outputViews = spec.Outputs.Select(o => o.View)
            .Concat(spec.StoredViews ?? Array.Empty<string>())
            .ToHashSet(StringComparer.Ordinal);
        // ivm-bench / Spark / DuckDB / Feldera all coerce numeric<->string comparisons,
        // and Feldera (Calcite) sorts NULLs low by default (last under DESC).
        // The snapshot codecs are registered at construction and touched only by
        // Save/Load — a persistent compile Steps identically to a non-persistent one.
        ParallelCompiledProgram? parallel = null;
        string? fallback = null;
        if (_workers > 1 && persistent)
        {
            fallback = "checkpointing is configured, and only the serial program checkpoints";
        }
        else if (_workers > 1
            && !SqlProgram.TryCompileParallel(
                spec.Program, outputViews, _workers, out parallel,
                numericStringCoercion: true, nullCollation: NullCollation.Low))
        {
            var blockers = SqlProgram.ParallelBlockers(
                spec.Program, outputViews, numericStringCoercion: true, nullCollation: NullCollation.Low);
            fallback = "views that do not shard: " + (blockers.Count == 0
                ? "(none reported)"
                : string.Join("; ", blockers.Select(b => b.View + " [" + string.Join(", ", b.Blockers) + "]")));
        }

        if (fallback is not null)
        {
            Console.Error.WriteLine(
                $"dbspnet: DBSPNET_WORKERS={_workers} but running the program SERIALLY — {fallback}");
        }

        var program = parallel is null
            ? SqlProgram.Compile(
                spec.Program, outputViews,
                snapshotCodecs: persistent ? ArrowSqlSnapshotCodecs.Instance : null,
                numericStringCoercion: true, nullCollation: NullCollation.Low)
            : null;

        var inputs = spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, i.Uri))
            .ToList();
        var outputs = spec.Outputs
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, o.Uri, OutputModeSetting))
            .ToList();

        var checkpoint = persistent
            ? new SnapshotCheckpointStore(new LocalTableFileSystem(snapshotDir!))
            : null;

        ProgramRunner runner;
        try
        {
            runner = parallel is not null
                ? await ProgramRunner.CreateAsync(parallel, inputs, outputs, cancellationToken).ConfigureAwait(false)
                : await ProgramRunner.CreateAsync(program!, inputs, outputs, checkpoint, cancellationToken)
                    .ConfigureAwait(false);
        }
        catch
        {
            parallel?.Dispose();
            throw;
        }

        var restoredTick = await runner.RestoreAsync(cancellationToken).ConfigureAwait(false);

        ProgramRunner? previous;
        lock (_gate)
        {
            previous = _runner;
            _runner = runner;
            _inputs.Clear();
            _inputs.AddRange(inputs);
            _outputs.Clear();
            _outputs.AddRange(outputs);
        }

        // A replaced parallel program owns W worker threads.
        previous?.ParallelProgram?.Dispose();

        sw.Stop();
        return new DeployResult(
            sw.Elapsed.TotalSeconds, inputs.Count, outputs.Count, persistent, restoredTick,
            runner.Workers, fallback);
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
                spec.Program, spec.Outputs.ToHashSet(StringComparer.Ordinal),
                numericStringCoercion: true, nullCollation: NullCollation.Low);
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
                Deployed: _runner is not null,
                BatchRunning: _batch is { IsCompleted: false },
                TickCount: _runner?.TickCount ?? 0,
                Outputs: OutputStats());
        }
    }

    private List<OutputStat> OutputStats() =>
        _runner?.ViewRowCounts().Select(kv => new OutputStat(kv.Key, kv.Value)).ToList()
            ?? new List<OutputStat>();
}
