// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// What <see cref="ProgramRunner"/> needs from a compiled program, whichever kind it is: the
/// serial <see cref="CompiledProgram"/> (one circuit) or the data-parallel
/// <see cref="ParallelCompiledProgram"/> (W replicas). The runner's batch semantics — drain
/// every version one tick at a time, then write each output once — are the same for both;
/// only ingest, step, output reads, profiling and checkpointing differ, and they differ
/// here rather than in a forked runner.
/// </summary>
internal abstract class RunnerProgram
{
    public abstract long TickCount { get; }

    /// <summary>The serial circuit, for checkpointing; <c>null</c> for a parallel program
    /// (the checkpoint store snapshots a single <see cref="RootCircuit"/>).</summary>
    public abstract RootCircuit? SerialCircuit { get; }

    public abstract bool TryGetInput(string table, out RunnerInput input);

    public abstract bool TryGetOutput(string view, out RunnerOutput output);

    public abstract void Step();

    /// <summary>Every output view the program materialises (bound to a connector or not)
    /// and its current distinct-row count.</summary>
    public abstract IEnumerable<KeyValuePair<string, long>> ViewRowCounts();

    public abstract bool ProfileOperators { set; }

    public abstract void ResetOperatorProfile();

    public abstract IReadOnlyList<OperatorProfile> CollectOperatorProfile();

    /// <summary>Per-worker operator profiles of a parallel program; <c>null</c> when serial.</summary>
    public virtual IReadOnlyList<IReadOnlyList<OperatorProfile>>? CollectOperatorProfilePerWorker() => null;

    public static RunnerProgram Of(CompiledProgram program) => new Serial(program);

    public static RunnerProgram Of(ParallelCompiledProgram program) => new Parallel(program);

    private sealed class Serial(CompiledProgram program) : RunnerProgram
    {
        public override long TickCount => program.Circuit.TickCount;

        public override RootCircuit SerialCircuit => program.Circuit;

        public override bool TryGetInput(string table, out RunnerInput input)
        {
            input = program.Inputs.TryGetValue(table, out var t) ? new SerialInput(t) : null!;
            return input is not null;
        }

        public override bool TryGetOutput(string view, out RunnerOutput output)
        {
            output = program.Outputs.TryGetValue(view, out var o) ? new SerialOutput(o) : null!;
            return output is not null;
        }

        public override void Step() => program.Step();

        public override IEnumerable<KeyValuePair<string, long>> ViewRowCounts() =>
            program.Outputs.Select(kv => KeyValuePair.Create(kv.Key, (long)kv.Value.CurrentView.Count));

        public override bool ProfileOperators
        {
            set => program.Circuit.ProfileOperators = value;
        }

        public override void ResetOperatorProfile() => program.Circuit.ResetOperatorProfile();

        public override IReadOnlyList<OperatorProfile> CollectOperatorProfile() =>
            program.Circuit.CollectOperatorProfile();
    }

    private sealed class Parallel(ParallelCompiledProgram program) : RunnerProgram
    {
        public override long TickCount => program.Circuit.TickCount;

        public override RootCircuit? SerialCircuit => null;

        public override bool TryGetInput(string table, out RunnerInput input)
        {
            input = program.Inputs.TryGetValue(table, out var t) ? new ShardedInput(t) : null!;
            return input is not null;
        }

        public override bool TryGetOutput(string view, out RunnerOutput output)
        {
            output = program.Outputs.TryGetValue(view, out var o) ? new ParallelOutput(o) : null!;
            return output is not null;
        }

        public override void Step() => program.Step();

        // Kept current per tick from the deltas once first read (ParallelProgramOutput.RowCount),
        // not formed from the W replica integrals on every read.
        public override IEnumerable<KeyValuePair<string, long>> ViewRowCounts() =>
            program.Outputs.Select(kv => KeyValuePair.Create(kv.Key, kv.Value.RowCount));

        public override bool ProfileOperators
        {
            set => program.Circuit.ProfileOperators = value;
        }

        public override void ResetOperatorProfile() => program.Circuit.ResetOperatorProfile();

        public override IReadOnlyList<OperatorProfile> CollectOperatorProfile() =>
            program.Circuit.CollectOperatorProfile();

        public override IReadOnlyList<IReadOnlyList<OperatorProfile>> CollectOperatorProfilePerWorker() =>
            program.Circuit.CollectOperatorProfilePerWorker();
    }

    private sealed class SerialInput(TableInput table) : RunnerInput
    {
        public override SqlSchema Schema => table.Schema;

        public override void PushArrow(RecordBatch batch, ReadOnlySpan<long> weights) => table.PushArrow(batch, weights);
    }

    private sealed class ShardedInput(ShardedTableInput table) : RunnerInput
    {
        public override SqlSchema Schema => table.Schema;

        public override void PushArrow(RecordBatch batch, ReadOnlySpan<long> weights) => table.PushArrow(batch, weights);
    }

    private sealed class SerialOutput(ProgramOutput output) : RunnerOutput
    {
        public override SqlSchema Schema => output.Schema;

        public override ZSet<StructuralRow, Z64> CurrentView => output.CurrentView;

        public override ZSet<StructuralRow, Z64> LastDelta => output.View.LastDelta;
    }

    private sealed class ParallelOutput(ParallelProgramOutput output) : RunnerOutput
    {
        public override SqlSchema Schema => output.Schema;

        // The sum of the W per-replica integrals (cached per tick).
        public override ZSet<StructuralRow, Z64> CurrentView => output.CurrentView;

        // The last tick's delta gathered (Z-set sum) across the replicas — the same
        // Z-set the serial program's in-circuit integral was fed that tick.
        public override ZSet<StructuralRow, Z64> LastDelta => output.CurrentDelta;
    }
}

/// <summary>One source table of a <see cref="RunnerProgram"/>.</summary>
internal abstract class RunnerInput
{
    public abstract SqlSchema Schema { get; }

    public abstract void PushArrow(RecordBatch batch, ReadOnlySpan<long> weights);
}

/// <summary>One output view of a <see cref="RunnerProgram"/>: the full current view and
/// the change the last tick applied to it.</summary>
internal abstract class RunnerOutput
{
    public abstract SqlSchema Schema { get; }

    public abstract ZSet<StructuralRow, Z64> CurrentView { get; }

    public abstract ZSet<StructuralRow, Z64> LastDelta { get; }
}
