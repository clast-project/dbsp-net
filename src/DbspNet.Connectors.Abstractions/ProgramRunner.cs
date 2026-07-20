// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using DbspNet.Arrow;
using DbspNet.Core.Circuit;
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
    // Opt-in phase/operator profiling for localizing a batch's wall-clock (set
    // DBSPNET_PROFILE=1). Zero cost and no path change when off.
    private static readonly bool ProfileEnabled =
        Environment.GetEnvironmentVariable("DBSPNET_PROFILE") is "1" or "true" or "TRUE";

    private readonly CompiledProgram _program;
    private readonly List<InputBinding> _inputs;
    private readonly List<(IOutputConnector Connector, ProgramOutput Output)> _outputs;
    private readonly BatchProfile? _profile = ProfileEnabled ? new BatchProfile() : null;

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

    /// <summary>Convenience: drain all available versions, then write every output.</summary>
    public async ValueTask<long> RunBatchAsync(CancellationToken cancellationToken = default)
    {
        if (_profile is not null)
        {
            // Per-batch report: clear the prior batch's accumulators so each
            // Resume→RunBatch prints a clean profile (comparable across batches).
            _profile.Reset();
            _profile.BeginGc();
            _program.Circuit.ProfileOperators = true;
            _program.Circuit.ResetOperatorProfile();
        }

        var wall0 = Stopwatch.GetTimestamp();
        var ticks = await DrainAsync(cancellationToken).ConfigureAwait(false);
        await WriteOutputsAsync(cancellationToken).ConfigureAwait(false);

        if (_profile is not null)
        {
            _profile.TotalTicks = Stopwatch.GetTimestamp() - wall0;
            _profile.EngineTicks = ticks;
            var report = _profile.BuildReport(_program.Circuit.CollectOperatorProfile());
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

    private sealed class InputBinding(IInputConnector connector, DbspNet.Sql.Compiler.TableInput tableInput, IConnectorOffset cursor)
    {
        public IInputConnector Connector { get; } = connector;

        public DbspNet.Sql.Compiler.TableInput TableInput { get; } = tableInput;

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

        public string BuildReport(IReadOnlyList<OperatorProfile> operators)
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
                $"batch wall {Ms(TotalTicks):F0} ms over {EngineTicks} engine tick(s)"));
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
                    $"  [{op.Index,4}] {op.Name,-24} {op.CumulativeMs,8:F1} ms ({share,4:F1}%)  state={op.RetainedRows,-8} out={op.LastOutputRows,-6} {op.Label}"));
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
