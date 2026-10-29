// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// Drives a whole <see cref="CompiledProgram"/> (a DAG of views maintained as one
/// circuit) — or its data-parallel counterpart, a <see cref="ParallelCompiledProgram"/>
/// (the same DAG replicated across W workers, inputs sharded, outputs gathered) — from N
/// input connectors (one per source table, matched by
/// <see cref="IInputConnector.Name"/>) to M output connectors (one per output view,
/// matched by <see cref="IOutputConnector.ViewName"/>). Table schemas are <b>declared</b>
/// by the program (from <c>CREATE TABLE</c>), so each input connector validates its Delta
/// source against the declared schema. Output is per-batch (truncate): drain all currently
/// available source versions, then write every output view's full contents once — the
/// natural model for an ivm-bench batch. See <c>docs/design-connectors.md</c>.
/// <para>
/// The two program kinds differ only behind <see cref="RunnerProgram"/> (ingest, step,
/// output reads); the batch loop, incremental-output bookkeeping and profile are shared.
/// A parallel program cannot be checkpointed through this runner (<see cref="ICheckpointStore"/>
/// snapshots a single circuit), so <see cref="CreateAsync(ParallelCompiledProgram, IReadOnlyList{IInputConnector}, IReadOnlyList{IOutputConnector}, CancellationToken)"/>
/// takes no store.
/// </para>
/// </summary>
public sealed class ProgramRunner
{
    // Opt-in phase/operator profiling for localizing a batch's wall-clock (set
    // DBSPNET_PROFILE=1). Zero cost and no path change when off.
    private static readonly bool ProfileEnabled =
        Environment.GetEnvironmentVariable("DBSPNET_PROFILE") is "1" or "true" or "TRUE";

    private readonly RunnerProgram _program;
    private readonly List<InputBinding> _inputs;
    private readonly List<(IOutputConnector Connector, RunnerOutput Output)> _outputs;

    // Incremental-mode outputs: each one's net change since its last write, summed over the
    // ticks in between, and whether it has been written in this process (the first write
    // carries the whole view, onto a sink the connector truncates).
    private readonly Dictionary<IOutputConnector, IncrementalOutput> _incremental = new(ReferenceEqualityComparer.Instance);

    private sealed class IncrementalOutput
    {
        public ZSetBuilder<StructuralRow, Z64> Pending { get; set; } = new();

        public bool Written { get; set; }
    }
    private readonly ICheckpointStore? _checkpoint;
    private readonly BatchProfile? _profile = ProfileEnabled ? new BatchProfile() : null;

    private ProgramRunner(
        RunnerProgram program,
        CompiledProgram? serial,
        ParallelCompiledProgram? parallel,
        List<InputBinding> inputs,
        List<(IOutputConnector, RunnerOutput)> outputs,
        ICheckpointStore? checkpoint)
    {
        _program = program;
        Program = serial;
        ParallelProgram = parallel;
        _inputs = inputs;
        _outputs = outputs;
        foreach (var (connector, _) in outputs)
        {
            if (connector.Mode == OutputMode.Incremental)
            {
                _incremental[connector] = new IncrementalOutput();
            }
        }
        _checkpoint = checkpoint;
    }

    /// <summary>The serial program this runner drives, or <c>null</c> when it drives a
    /// <see cref="ParallelProgram"/>.</summary>
    public CompiledProgram? Program { get; }

    /// <summary>The data-parallel program this runner drives, or <c>null</c> when it
    /// drives a serial <see cref="Program"/>.</summary>
    public ParallelCompiledProgram? ParallelProgram { get; }

    /// <summary>The worker count W (1 for a serial program).</summary>
    public int Workers => ParallelProgram?.Workers ?? 1;

    /// <summary>Engine ticks completed so far.</summary>
    public long TickCount => _program.TickCount;

    /// <summary>Every output view the program materialises — including views computed but
    /// bound to no connector — with its current distinct-row count.</summary>
    public IEnumerable<KeyValuePair<string, long>> ViewRowCounts() => _program.ViewRowCounts();

    /// <summary>True when this runner was wired with a checkpoint store, so each
    /// <see cref="RunBatchAsync"/> ends with a durable state snapshot.</summary>
    public bool HasCheckpoint => _checkpoint is not null;

    /// <summary>
    /// Wire connectors to a compiled program: validate each input source against the
    /// program's declared table schema, and bind each output connector to its view's
    /// schema. Every input connector's <see cref="IInputConnector.Name"/> must name a
    /// program source table; every output connector's <see cref="IOutputConnector.ViewName"/>
    /// must name a program output view.
    /// <para>
    /// Pass <paramref name="checkpoint"/> to make each batch durable: the runner
    /// snapshots engine state + every source's committed cursor at the end of each
    /// <see cref="RunBatchAsync"/> (batch granularity — the natural commit point for
    /// this runner, whose whole contract is "drain, then write every output once"),
    /// and <see cref="RestoreAsync"/> resumes from it. The program must have been
    /// compiled with snapshot codecs
    /// (<c>SqlProgram.Compile(..., snapshotCodecs: ArrowSqlSnapshotCodecs.Instance)</c>)
    /// or the snapshot will hold no operator state.
    /// </para>
    /// </summary>
    public static async ValueTask<ProgramRunner> CreateAsync(
        CompiledProgram program,
        IReadOnlyList<IInputConnector> inputs,
        IReadOnlyList<IOutputConnector> outputs,
        ICheckpointStore? checkpoint = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(program);
        var runnerProgram = RunnerProgram.Of(program);
        var (ins, outs) = await BindAsync(runnerProgram, inputs, outputs, cancellationToken).ConfigureAwait(false);
        return new ProgramRunner(runnerProgram, program, null, ins, outs, checkpoint);
    }

    /// <summary>
    /// Wire connectors to a data-parallel program: the same binding and validation as the
    /// serial overload, with each source's versions pushed into a sharded input (split
    /// across the replicas by whole-row hash) and each output read as the gathered
    /// (replica-summed) view and per-tick delta. Batch semantics and output modes are
    /// identical to the serial runner. No checkpoint store: the parallel program's state
    /// is per-worker (<c>ParallelSnapshot</c>), which <see cref="ICheckpointStore"/> does not
    /// model.
    /// </summary>
    public static async ValueTask<ProgramRunner> CreateAsync(
        ParallelCompiledProgram program,
        IReadOnlyList<IInputConnector> inputs,
        IReadOnlyList<IOutputConnector> outputs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(program);
        var runnerProgram = RunnerProgram.Of(program);
        var (ins, outs) = await BindAsync(runnerProgram, inputs, outputs, cancellationToken).ConfigureAwait(false);
        return new ProgramRunner(runnerProgram, null, program, ins, outs, checkpoint: null);
    }

    private static async ValueTask<(List<InputBinding>, List<(IOutputConnector, RunnerOutput)>)> BindAsync(
        RunnerProgram program,
        IReadOnlyList<IInputConnector> inputs,
        IReadOnlyList<IOutputConnector> outputs,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(inputs);
        ArgumentNullException.ThrowIfNull(outputs);

        var inputBindings = new List<InputBinding>(inputs.Count);
        foreach (var connector in inputs)
        {
            if (!program.TryGetInput(connector.Name, out var tableInput))
            {
                throw new ArgumentException(
                    $"input connector '{connector.Name}' does not match any program source table");
            }

            // Declared schema wins — validate/bind the source onto it.
            await connector.ResolveSchemaAsync(tableInput.Schema, cancellationToken).ConfigureAwait(false);
            inputBindings.Add(new InputBinding(connector, tableInput, connector.InitialOffset));
        }

        var outputBindings = new List<(IOutputConnector, RunnerOutput)>(outputs.Count);
        foreach (var connector in outputs)
        {
            if (!program.TryGetOutput(connector.ViewName, out var programOutput))
            {
                throw new ArgumentException(
                    $"output connector '{connector.ViewName}' does not match any program output view");
            }

            await connector.BindSchemaAsync(programOutput.Schema, cancellationToken).ConfigureAwait(false);
            outputBindings.Add((connector, programOutput));
        }

        return (inputBindings, outputBindings);
    }

    /// <summary>
    /// Restore engine state and every source's committed cursor from the last
    /// checkpoint, if any. Call once after <see cref="CreateAsync"/> (before the
    /// first <see cref="RunBatchAsync"/>) to resume a prior run; a no-op when no
    /// checkpoint store is wired or none has been written yet. Returns the engine
    /// tick restored to (0 for a fresh start).
    /// </summary>
    public async ValueTask<long> RestoreAsync(CancellationToken cancellationToken = default)
    {
        if (_checkpoint is null)
        {
            return 0;
        }

        var state = await _checkpoint.TryRestoreAsync(_program.SerialCircuit!, cancellationToken).ConfigureAwait(false);
        if (state is null)
        {
            return 0;
        }

        foreach (var sc in state.Offsets)
        {
            var b = _inputs.FirstOrDefault(x => string.Equals(x.Connector.Name, sc.SourceName, StringComparison.Ordinal))
                ?? throw new InvalidDataException($"checkpoint names unknown source '{sc.SourceName}'");
            b.Cursor = b.Connector.ParseOffset(sc.Offset);
        }

        return state.Tick;
    }

    /// <summary>
    /// Snapshot engine state together with every source's committed cursor. The two
    /// commit atomically (the offsets ride in the snapshot manifest), so engine tick
    /// T and the cursors can never diverge. Called automatically at the end of
    /// <see cref="RunBatchAsync"/> when a checkpoint store is wired; exposed for a
    /// forced checkpoint (e.g. before a clean shutdown).
    /// </summary>
    public ValueTask CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (_checkpoint is null)
        {
            return ValueTask.CompletedTask;
        }

        var offsets = _inputs
            .Select(b => new SourceCheckpoint(b.Connector.Name, b.Cursor.Serialize()))
            .ToList();
        return _checkpoint.SaveAsync(_program.SerialCircuit!, offsets, cancellationToken);
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
                var readStart = _profile is null ? 0 : Stopwatch.GetTimestamp();
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

                long pushTicks = 0;
                long rows = 0;
                await foreach (var vb in batch.Content.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (_profile is null)
                    {
                        b.TableInput.PushArrow(vb.Batch, vb.Weights);
                    }
                    else
                    {
                        var p0 = Stopwatch.GetTimestamp();
                        b.TableInput.PushArrow(vb.Batch, vb.Weights);
                        pushTicks += Stopwatch.GetTimestamp() - p0;
                        rows += vb.Batch.Length;
                    }
                }

                if (_profile is not null)
                {
                    // read+decode wall = whole ingest span for this version minus the push time.
                    var ingestTicks = Stopwatch.GetTimestamp() - readStart;
                    _profile.Source(b.Connector.Name).Add(ingestTicks - pushTicks, pushTicks, rows);
                }

                var stepStart = _profile is null ? 0 : Stopwatch.GetTimestamp();
                _program.Step();
                if (_profile is not null)
                {
                    _profile.StepTicks += Stopwatch.GetTimestamp() - stepStart;
                }

                AccumulateIncrementalChanges();
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
        var tick = _program.TickCount;
        foreach (var (connector, output) in _outputs)
        {
            if (_incremental.TryGetValue(connector, out var incremental))
            {
                await WriteIncrementalAsync(connector, output, incremental, tick, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (_profile is null)
            {
                var arrow = ArrowExtensions.ToArrowView(output.Schema, output.CurrentView);
                await connector.WriteViewAsync(arrow.Rows, arrow.Weights, tick, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var m0 = Stopwatch.GetTimestamp();
                var arrow = ArrowExtensions.ToArrowView(output.Schema, output.CurrentView);
                var matTicks = Stopwatch.GetTimestamp() - m0;
                long rows = arrow.Rows.Length;
                var w0 = Stopwatch.GetTimestamp();
                await connector.WriteViewAsync(arrow.Rows, arrow.Weights, tick, cancellationToken).ConfigureAwait(false);
                _profile.Output(connector.ViewName).Add(matTicks, Stopwatch.GetTimestamp() - w0, rows);
            }
        }
    }

    private void AccumulateIncrementalChanges()
    {
        foreach (var (connector, output) in _outputs)
        {
            if (_incremental.TryGetValue(connector, out var incremental) && incremental.Written)
            {
                foreach (var (row, weight) in output.LastDelta)
                {
                    incremental.Pending.Add(row, weight);
                }
            }
        }
    }

    private async ValueTask WriteIncrementalAsync(
        IOutputConnector connector, RunnerOutput output, IncrementalOutput incremental, long tick,
        CancellationToken cancellationToken)
    {
        // The first write in this process carries the whole view (the connector truncates
        // the sink first); later writes carry the net change since the previous one.
        var m0 = Stopwatch.GetTimestamp();
        var net = incremental.Written ? incremental.Pending.Build() : output.CurrentView;
        incremental.Pending = new ZSetBuilder<StructuralRow, Z64>();
        var changes = ArrowExtensions.ToArrowChanges(output.Schema, net);
        var matTicks = Stopwatch.GetTimestamp() - m0;

        var w0 = Stopwatch.GetTimestamp();
        await connector.ApplyChangesAsync(changes.Inserts, changes.InsertKeys, changes.Deletes, tick, cancellationToken)
            .ConfigureAwait(false);
        incremental.Written = true;
        _profile?.Output(connector.ViewName).Add(
            matTicks, Stopwatch.GetTimestamp() - w0, changes.Inserts.Length + changes.Deletes.Count);
    }

    /// <summary>
    /// Convenience: drain all available versions, write every output, then — if a
    /// checkpoint store is wired — snapshot engine state + cursors. The checkpoint
    /// is inside the returned batch's measured span on purpose: a batch is only
    /// really "done" once its state is durable, so the honest per-batch number is
    /// step + output write + checkpoint.
    /// </summary>
    public async ValueTask<long> RunBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_profile is not null)
        {
            // Per-batch report: clear the prior batch's accumulators so each
            // Resume→RunBatch prints a clean profile (comparable across batches).
            _profile.Reset();
            _profile.BeginGc();
            _program.ProfileOperators = true;
            _program.ResetOperatorProfile();
        }

        var wall0 = Stopwatch.GetTimestamp();
        var ticks = await DrainAsync(cancellationToken).ConfigureAwait(false);
        await WriteOutputsAsync(cancellationToken).ConfigureAwait(false);

        if (_checkpoint is not null)
        {
            var c0 = _profile is null ? 0 : Stopwatch.GetTimestamp();
            await CheckpointAsync(cancellationToken).ConfigureAwait(false);
            if (_profile is not null)
            {
                _profile.CheckpointTicks = Stopwatch.GetTimestamp() - c0;
            }
        }

        if (_profile is not null)
        {
            _profile.TotalTicks = Stopwatch.GetTimestamp() - wall0;
            _profile.EngineTicks = ticks;
            var report = _profile.BuildReport(_program.CollectOperatorProfile(), Workers);
            Console.Error.WriteLine(report);

            // Optionally persist the report so it survives harness container teardown:
            // point DBSPNET_PROFILE_FILE at a mounted path (e.g. /data/processed/dbspnet).
            var file = Environment.GetEnvironmentVariable("DBSPNET_PROFILE_FILE");
            if (!string.IsNullOrEmpty(file))
            {
                try
                {
                    File.AppendAllText(file, report + Environment.NewLine);
                }
                catch (IOException)
                {
                    // Best-effort: a profile-file write must never fail the batch.
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        return ticks;
    }

    private sealed class InputBinding(IInputConnector connector, RunnerInput tableInput, IConnectorOffset cursor)
    {
        public IInputConnector Connector { get; } = connector;

        public RunnerInput TableInput { get; } = tableInput;

        public IConnectorOffset Cursor { get; set; } = cursor;
    }

    // Accumulates a batch's wall-clock across the four phases (input read+decode,
    // input push, engine step, output materialize + write), split by source /
    // output, plus the per-operator profile from the circuit. Only allocated when
    // DBSPNET_PROFILE is set. Ticks are Stopwatch ticks throughout.
    private sealed class BatchProfile
    {
        private static readonly double MsPerTick = 1000.0 / Stopwatch.Frequency;

        private readonly Dictionary<string, PhaseAcc> _sources = new(StringComparer.Ordinal);
        private readonly Dictionary<string, PhaseAcc> _outputs = new(StringComparer.Ordinal);

        public long StepTicks;
        public long CheckpointTicks;
        public long TotalTicks;
        public long EngineTicks;

        private long _allocStart;
        private int _gen0Start;
        private int _gen1Start;
        private int _gen2Start;

        public void Reset()
        {
            _sources.Clear();
            _outputs.Clear();
            StepTicks = 0;
            CheckpointTicks = 0;
            TotalTicks = 0;
            EngineTicks = 0;
        }

        public void BeginGc()
        {
            _allocStart = GC.GetTotalAllocatedBytes();
            _gen0Start = GC.CollectionCount(0);
            _gen1Start = GC.CollectionCount(1);
            _gen2Start = GC.CollectionCount(2);
        }

        public PhaseAcc Source(string name) => Get(_sources, name);

        public PhaseAcc Output(string name) => Get(_outputs, name);

        private static PhaseAcc Get(Dictionary<string, PhaseAcc> map, string name)
        {
            if (!map.TryGetValue(name, out var acc))
            {
                acc = new PhaseAcc();
                map[name] = acc;
            }

            return acc;
        }

        public string BuildReport(IReadOnlyList<OperatorProfile> operators, int workers)
        {
            static double Ms(long ticks) => ticks * MsPerTick;

            var sb = new StringBuilder();
            sb.AppendLine();
            sb.AppendLine("===== DBSPNET BATCH PROFILE =====");
            // Self-stamp the running build so a profile is never ambiguous about
            // which engine commit produced it (DBSPNET_COMMIT is baked into the
            // image by the ivm-bench Dockerfile).
            var commit = Environment.GetEnvironmentVariable("DBSPNET_COMMIT");
            sb.AppendLine(FormattableString.Invariant(
                $"engine build: {(string.IsNullOrEmpty(commit) ? "(DBSPNET_COMMIT unset)" : commit)}"));
            sb.AppendLine(FormattableString.Invariant(
                $"batch wall {Ms(TotalTicks):F0} ms over {EngineTicks} engine tick(s), workers W={workers}"));
            if (workers > 1)
            {
                // Operator rows are summed over the W replicas: worker time, not wall, so
                // their shares of "engine step" (a wall time) can exceed 100%.
                sb.AppendLine("operator times are summed over workers (CPU time, not wall)");
            }
            var allocGiB = (GC.GetTotalAllocatedBytes() - _allocStart) / (1024.0 * 1024.0 * 1024.0);
            sb.AppendLine(FormattableString.Invariant(
                $"allocated {allocGiB:F2} GiB   GC gen0/1/2 = {GC.CollectionCount(0) - _gen0Start}/{GC.CollectionCount(1) - _gen1Start}/{GC.CollectionCount(2) - _gen2Start}   ServerGC={System.Runtime.GCSettings.IsServerGC} procs={Environment.ProcessorCount}"));

            var readMs = _sources.Values.Sum(v => Ms(v.ReadTicks));
            var pushMs = _sources.Values.Sum(v => Ms(v.PushTicks));
            var matMs = _outputs.Values.Sum(v => Ms(v.ReadTicks));
            var writeMs = _outputs.Values.Sum(v => Ms(v.PushTicks));
            var stepMs = Ms(StepTicks);
            sb.AppendLine();
            sb.AppendLine("-- phases (share of batch wall) --");
            AppendPhase(sb, "input read+decode", readMs, TotalTicks);
            AppendPhase(sb, "input push (obj[])", pushMs, TotalTicks);
            AppendPhase(sb, "engine step", stepMs, TotalTicks);
            AppendPhase(sb, "output materialize", matMs, TotalTicks);
            AppendPhase(sb, "output write (Delta)", writeMs, TotalTicks);
            AppendPhase(sb, "checkpoint save", Ms(CheckpointTicks), TotalTicks);

            sb.AppendLine();
            sb.AppendLine("-- per-source ingest (read+decode / push, rows) --");
            foreach (var (name, v) in _sources.OrderByDescending(kv => kv.Value.ReadTicks + kv.Value.PushTicks))
            {
                sb.AppendLine(FormattableString.Invariant(
                    $"  {name,-28} read {Ms(v.ReadTicks),8:F0} ms  push {Ms(v.PushTicks),7:F0} ms  rows {v.Rows}"));
            }

            sb.AppendLine();
            sb.AppendLine("-- per-output write (materialize / write, rows) --");
            foreach (var (name, v) in _outputs.OrderByDescending(kv => kv.Value.ReadTicks + kv.Value.PushTicks))
            {
                sb.AppendLine(FormattableString.Invariant(
                    $"  {name,-28} mat {Ms(v.ReadTicks),8:F0} ms  write {Ms(v.PushTicks),7:F0} ms  rows {v.Rows}"));
            }

            sb.AppendLine();
            sb.AppendLine("-- top operators by cumulative step time --");
            foreach (var op in operators.OrderByDescending(o => o.CumulativeMs).Take(30))
            {
                var share = stepMs > 0 ? op.CumulativeMs / stepMs * 100.0 : 0.0;
                sb.AppendLine(FormattableString.Invariant(
                    $"  [{op.Index,4}] {op.Name,-24} {op.CumulativeMs,8:F1} ms ({share,4:F1}%)  alloc {op.AllocBytes / (1024.0 * 1024.0),8:F1} MiB  state={op.RetainedRows,-8} out={op.LastOutputRows,-6} {op.Label}"));
            }

            // By VIEW: sums time and allocation across every operator a view owns, so the
            // costliest views (and how much of each is ApplyOp) can be ranked directly.
            sb.AppendLine();
            sb.AppendLine("-- by view (cumulative; top 20) --");
            var byView = operators
                .GroupBy(o => o.Label)
                .Select(g => (
                    View: g.Key,
                    Count: g.Count(),
                    Ms: g.Sum(o => o.CumulativeMs),
                    Alloc: g.Sum(o => o.AllocBytes),
                    ApplyMs: g.Where(o => o.Name == "ApplyOp").Sum(o => o.CumulativeMs)))
                .OrderByDescending(g => g.Ms)
                .Take(20);
            foreach (var g in byView)
            {
                var msShare = stepMs > 0 ? g.Ms / stepMs * 100.0 : 0.0;
                sb.AppendLine(FormattableString.Invariant(
                    $"  {g.View,-28} x{g.Count,-4} {g.Ms,8:F1} ms ({msShare,4:F1}%)  alloc {g.Alloc / (1024.0 * 1024.0),8:F1} MiB  of which ApplyOp {g.ApplyMs,8:F1} ms"));
            }

            // DBSPNET_PROFILE_VIEW=<view>[,<view>...]: every operator of each view, in
            // circuit order, so a view's whole chain (not just its top-30 members) can be read.
            if (Environment.GetEnvironmentVariable("DBSPNET_PROFILE_VIEW") is { Length: > 0 } views)
            {
                foreach (var view in views.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    sb.AppendLine();
                    sb.AppendLine("-- all operators of view '" + view + "' (circuit order) --");
                    foreach (var op in operators.Where(o => o.Label == view).OrderBy(o => o.Index))
                    {
                        sb.AppendLine(FormattableString.Invariant(
                            $"  [{op.Index,4}] {op.Name,-24} {op.CumulativeMs,8:F1} ms  alloc {op.AllocBytes / (1024.0 * 1024.0),8:F1} MiB  state={op.RetainedRows,-8} out={op.LastOutputRows,-6}"));
                    }
                }
            }

            // By operator KIND: sums time AND allocation across every operator of a
            // kind, so the ApplyOp (map/filter/project) tail's total share of the
            // batch pops out directly — the number the row-rep / columnar arc weighs.
            sb.AppendLine();
            sb.AppendLine("-- by operator kind (cumulative) --");
            var totalAlloc = operators.Sum(o => o.AllocBytes);
            var byKind = operators
                .GroupBy(o => o.Name)
                .Select(g => (Kind: g.Key, Count: g.Count(), Ms: g.Sum(o => o.CumulativeMs), Alloc: g.Sum(o => o.AllocBytes)))
                .OrderByDescending(g => g.Alloc);
            foreach (var g in byKind)
            {
                var msShare = stepMs > 0 ? g.Ms / stepMs * 100.0 : 0.0;
                var allocShare = totalAlloc > 0 ? (double)g.Alloc / totalAlloc * 100.0 : 0.0;
                sb.AppendLine(FormattableString.Invariant(
                    $"  {g.Kind,-26} x{g.Count,-4} {g.Ms,8:F1} ms ({msShare,4:F1}%)  alloc {g.Alloc / (1024.0 * 1024.0),9:F1} MiB ({allocShare,4:F1}%)"));
            }

            sb.AppendLine("===== END BATCH PROFILE =====");
            return sb.ToString();
        }

        private static void AppendPhase(StringBuilder sb, string label, double ms, long totalTicks)
        {
            var share = totalTicks > 0 ? ms / (totalTicks * MsPerTick) * 100.0 : 0.0;
            sb.AppendLine(FormattableString.Invariant($"  {label,-22} {ms,9:F0} ms ({share,4:F1}%)"));
        }

        public sealed class PhaseAcc
        {
            public long ReadTicks;
            public long PushTicks;
            public long Rows;

            public void Add(long readTicks, long pushTicks, long rows)
            {
                ReadTicks += readTicks;
                PushTicks += pushTicks;
                Rows += rows;
            }
        }
    }
}
