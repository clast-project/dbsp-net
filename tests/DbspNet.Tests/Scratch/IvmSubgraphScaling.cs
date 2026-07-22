// Increment 1 measurement (docs/design-structural-parallel.md §5): does the
// STRUCTURAL data-parallel compile scale on a real hot fact subgraph of the
// ivm-bench SF=3 batch-1 program? Targets `holdings_history` — the holdings fact
// path (staging_holding_history + trade lineage → trades_history → trades →
// holdings_history), all inner-join / aggregate views, so the whole closure
// shards. Reads the real Delta source tables once, drives the closure serial and
// data-parallel at W=1/2/4/8, asserts the gathered output is byte-identical to
// serial for every W, and reports realized engine-step scaling.
//
// MUST run with DOTNET_gcServer=1 (batch-1's GC mode; workstation GC misleads).
//   DOTNET_gcServer=1 IVM_DATA_ROOT=D:/ivm-data/raw/3/delta IVM_SPEC=D:/ivm-data/ivm_spec.json \
//     dotnet test --filter FullyQualifiedName~IvmSubgraphScaling
using System.Diagnostics;
using System.Text.Json;
using DbspNet.Arrow;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmSubgraphScaling
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    // The target hot view and the source tables its closure consumes (feeding any
    // others is wasted read time; the program prunes the rest by dead-view elim).
    // Both overridable via IVM_TARGET / IVM_SOURCES (comma-separated) to triangulate
    // across subgraphs without recompiling.
    private static string TargetView =>
        Environment.GetEnvironmentVariable("IVM_TARGET") is { Length: > 0 } t ? t : "holdings_history";

    private static string[] SourceTables =>
        Environment.GetEnvironmentVariable("IVM_SOURCES") is { Length: > 0 } s
            ? s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : new[]
            {
                "staging_holding_history", "staging_trade",
                "batch1_trade_history", "batch1_status_type", "batch1_trade_type",
            };

    private static readonly int[] WorkerSweep = { 1, 2, 4, 8 };

    // Base-table row counts (loaded source sizes), supplied to the production
    // broadcast size gate so the compiler can estimate dimension-view sizes.
    private IReadOnlyDictionary<string, long> _rowCounts = new Dictionary<string, long>(StringComparer.Ordinal);

    // Scaling levers, toggled by env:
    //   IVM_FUSE=1          single-barrier join exchange
    //   IVM_BCAST=1         broadcast ALL leaf-right dimensions (experimental override)
    //   IVM_BCAST_MAXROWS=N production size gate — broadcast a dimension only when its
    //                       estimated rows <= N (derived from the loaded base counts)
    // Where the per-batch checkpoints are written (Windows-visible path; the test
    // process is a Windows process). Wiped before every measured save.
    private static string SnapshotRoot =>
        Environment.GetEnvironmentVariable("IVM_SNAPSHOT_DIR") is { Length: > 0 } s
            ? s
            : Path.Combine(Path.GetTempPath(), "dbspnet-ivm-snap");

    private CompileOptions ParOptions => new()
    {
        CoalesceJoinExchange = Environment.GetEnvironmentVariable("IVM_FUSE") is "1" or "true",
        BroadcastSmallDimensionJoins = Environment.GetEnvironmentVariable("IVM_BCAST") is "1" or "true",
        BroadcastMaxRows = long.TryParse(Environment.GetEnvironmentVariable("IVM_BCAST_MAXROWS"), out var m) ? m : 0,
        RelationRowCounts = _rowCounts,
    };

    private readonly ITestOutputHelper _out;

    public IvmSubgraphScaling(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task MeasureHoldingsPathScaling()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = new HashSet<string>(new[] { TargetView }, StringComparer.Ordinal);
        string InUri(string uri) => Path.Combine(dataRoot, StripPrefix(uri, "/data/raw/delta/"));
        var inputUriByTable = spec.Inputs.ToDictionary(i => i.Table, i => InUri(i.Uri), StringComparer.Ordinal);

        // Confirm the closure is parallelizable (should be — all inner joins/aggs).
        if (!SqlProgram.TryCompileParallel(
                spec.Program, outputViews, 2, out var probe, options: ParOptions,
                numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low))
        {
            _out.WriteLine($"'{TargetView}' closure is NOT parallelizable — cannot measure. Pick another target.");
            return;
        }

        probe!.Dispose();

        // Read each needed source table's rows into memory ONCE (decode via a serial
        // program's declared schema), so serial and every W push identical input.
        var serialForDecode = SqlProgram.Compile(
            spec.Program, outputViews,
            numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low);

        var tableRows = new Dictionary<string, List<(object?[] Values, long Weight)>>(StringComparer.Ordinal);
        long totalRows = 0;
        foreach (var table in SourceTables)
        {
            if (!inputUriByTable.TryGetValue(table, out var uri) || !serialForDecode.Inputs.ContainsKey(table))
            {
                continue;
            }

            var rows = await ReadTableRows(table, uri, serialForDecode.Table(table));
            tableRows[table] = rows;
            totalRows += rows.Count;
        }

        // Feed the loaded base-table sizes to the production broadcast size gate;
        // the compiler estimates each dimension view's size from these.
        _rowCounts = tableRows.ToDictionary(kv => kv.Key, kv => (long)kv.Value.Count, StringComparer.Ordinal);

        _out.WriteLine($"ServerGC={System.Runtime.GCSettings.IsServerGC} procs={Environment.ProcessorCount}");
        _out.WriteLine($"target='{TargetView}'  source rows loaded={totalRows}");
        if (totalRows == 0)
        {
            _out.WriteLine("no source rows read — check IVM_DATA_ROOT / spec URIs.");
            return;
        }

        // Serial reference (also the byte-identity oracle), median of repeats.
        var (serialMs, serialOut) = RunSerial(spec, outputViews, tableRows);
        _out.WriteLine($"serial: engine step {serialMs:F1} ms (median), {serialOut.Count} distinct output rows");

        // Warm up the parallel path (JIT the exchange operators + worker threads)
        // before timing so the first measured W is not penalised.
        RunParallel(spec, outputViews, tableRows, 2, repeats: 1);

        double? w1Ms = null;
        _out.WriteLine("W    engine_ms   vs_serial   vs_W1    eff    output_match");
        foreach (var w in WorkerSweep)
        {
            var (ms, output) = RunParallel(spec, outputViews, tableRows, w, repeats: Repeats);
            w1Ms ??= ms;
            var match = DictEquals(serialOut, output);
            var vsSerial = serialMs / ms;
            var vsW1 = w1Ms.Value / ms;
            var eff = vsW1 / w * 100.0;
            _out.WriteLine($"{w,-4} {ms,9:F1}  {vsSerial,7:F2}x   {vsW1,6:F2}x  {eff,4:F0}%   {(match ? "OK" : "MISMATCH")}");
            Assert.True(match, $"W={w} output differs from serial (correctness)");
        }

        // Localize the wall: per-worker phase decomposition of one step.
        //   move% = split+gather (all-to-all shuffle of wide rows)
        //   wait% = idle at the exchange barrier (coordination / straggler)
        //   op%   = actual join/aggregate compute
        //   imbalance = slowest worker's busy time / mean busy (skew straggler)
        _out.WriteLine("W    step_ms   move%  wait%   op%   imbalance");
        foreach (var w in WorkerSweep)
        {
            var s = ProfilePhases(spec, outputViews, tableRows, w);
            _out.WriteLine($"{w,-4} {s.Step,7:F1}   {s.MovePct,4:F0}%  {s.WaitPct,4:F0}%  {s.OpPct,4:F0}%   {s.Imbalance,4:F2}x");
        }

        // ---- per-batch state persistence -------------------------------------
        // A batch is only really done once its state is durable, so the honest
        // per-batch cost is step + checkpoint. Two questions:
        //   (1) do the snapshot codecs erode the STEP? They are registered at
        //       operator construction and touched only by Save/Load, so they must
        //       not — compare step-with-codecs against the codec-free sweep above.
        //   (2) what does the checkpoint itself cost, and does it scale with W?
        //       Serial → Snapshot.WriteAsync (one circuit). Parallel →
        //       ParallelSnapshot.WriteAsync (W disjoint worker-{w}/ subtrees,
        //       written concurrently). A broadcast dimension is held by every
        //       worker, so it persists W× — bounded by BroadcastMaxRows, hence
        //       small, but it shows up in the bytes column.
        _out.WriteLine(string.Empty);
        _out.WriteLine($"-- per-batch checkpoint (snapshot root: {SnapshotRoot}) --");

        var serialSnap = await MeasureSerialCheckpoint(spec, outputViews, tableRows);
        _out.WriteLine(
            $"serial  step {serialSnap.StepMs,8:F1} ms  save {serialSnap.SaveMs,8:F1} ms  " +
            $"total {serialSnap.StepMs + serialSnap.SaveMs,8:F1} ms  ops {serialSnap.Ops,-5} {serialSnap.MiB,7:F1} MiB");
        // >1.00 = the codecs cost the step. They are registered at construction and
        // read only by Save/Load, so the expectation is 1.00 — and anything inside the
        // run-to-run drift between two separately-timed serial compiles (~±15% here)
        // is consistent with "free". The parallel step column below is the stronger
        // check: its W-sweep must reproduce the codec-free sweep's shape.
        _out.WriteLine(
            $"        codec step / codec-free step {serialSnap.StepMs / serialMs,5:F2}x " +
            "(>1.00 = codecs cost the step)");

        double? pw1Step = null, pw1Total = null;
        _out.WriteLine("W    step_ms   save_ms  total_ms   step_vs_W1  total_vs_W1   ops   MiB    match");
        foreach (var w in WorkerSweep)
        {
            var s = await MeasureParallelCheckpoint(spec, outputViews, tableRows, w);
            var total = s.StepMs + s.SaveMs;
            pw1Step ??= s.StepMs;
            pw1Total ??= total;
            var match = DictEquals(serialOut, s.Output);
            _out.WriteLine(
                $"{w,-4} {s.StepMs,8:F1}  {s.SaveMs,8:F1}  {total,8:F1}   {pw1Step.Value / s.StepMs,8:F2}x  " +
                $"{pw1Total.Value / total,9:F2}x  {s.Ops,-5} {s.MiB,6:F1}   {(match ? "OK" : "MISMATCH")}");
            Assert.True(match, $"W={w} persistent-compile output differs from serial (correctness)");
        }
    }

    private readonly record struct CheckpointSummary(
        double StepMs, double SaveMs, int Ops, double MiB, Dictionary<string, long> Output);

    // Serial program compiled WITH snapshot codecs: time the step, then the whole
    // Snapshot.WriteAsync. Serial outputs are in-circuit Integrate operators, so
    // the materialised views are part of this snapshot.
    private async Task<CheckpointSummary> MeasureSerialCheckpoint(
        Spec spec, HashSet<string> outputViews,
        Dictionary<string, List<(object?[] Values, long Weight)>> tableRows)
    {
        var steps = new List<double>();
        var saves = new List<double>();
        var ops = 0;
        double mib = 0;
        Dictionary<string, long> output = new();

        for (var r = 0; r <= CheckpointRepeats; r++) // r==0 warm-up, discarded
        {
            var program = SqlProgram.Compile(
                spec.Program, outputViews, snapshotCodecs: ArrowSqlSnapshotCodecs.Instance,
                numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low);
            foreach (var (table, rows) in tableRows)
            {
                program.Table(table).Push(rows);
            }

            var sw = Stopwatch.StartNew();
            program.Step();
            sw.Stop();

            var view = program.Outputs[TargetView];
            output = Materialize(view.CurrentView, view.Schema.Count);

            var dir = FreshSnapshotDir("serial");
            var sw2 = Stopwatch.StartNew();
            ops = await Snapshot.WriteAsync(program.Circuit, dir);
            sw2.Stop();
            mib = DirectoryMiB(dir);

            if (r > 0)
            {
                steps.Add(sw.Elapsed.TotalMilliseconds);
                saves.Add(sw2.Elapsed.TotalMilliseconds);
            }
        }

        return new CheckpointSummary(Median(steps), Median(saves), ops, mib, output);
    }

    // Parallel program compiled WITH snapshot codecs threaded through to every
    // replica: time the step, then ParallelSnapshot.WriteAsync (per-worker shards,
    // written concurrently).
    private async Task<CheckpointSummary> MeasureParallelCheckpoint(
        Spec spec, HashSet<string> outputViews,
        Dictionary<string, List<(object?[] Values, long Weight)>> tableRows, int workers)
    {
        var steps = new List<double>();
        var saves = new List<double>();
        var ops = 0;
        double mib = 0;
        Dictionary<string, long> output = new();

        for (var r = 0; r <= CheckpointRepeats; r++) // r==0 warm-up, discarded
        {
            Assert.True(SqlProgram.TryCompileParallel(
                spec.Program, outputViews, workers, out var program,
                snapshotCodecs: ArrowSqlSnapshotCodecs.Instance, options: ParOptions,
                numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low));

            using (program)
            {
                foreach (var (table, rows) in tableRows)
                {
                    program!.Table(table).Push(rows);
                }

                var sw = Stopwatch.StartNew();
                program!.Step();
                sw.Stop();

                var view = program.Outputs[TargetView];
                output = Materialize(view.CurrentView, view.Schema.Count);

                var dir = FreshSnapshotDir($"par-w{workers}");
                var sw2 = Stopwatch.StartNew();
                ops = await ParallelSnapshot.WriteAsync(program.Circuit, dir);
                sw2.Stop();
                mib = DirectoryMiB(dir);

                if (r > 0)
                {
                    steps.Add(sw.Elapsed.TotalMilliseconds);
                    saves.Add(sw2.Elapsed.TotalMilliseconds);
                }
            }
        }

        return new CheckpointSummary(Median(steps), Median(saves), ops, mib, output);
    }

    private const int CheckpointRepeats = 2;

    private static string FreshSnapshotDir(string name)
    {
        var dir = Path.Combine(SnapshotRoot, name);
        if (Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }

        Directory.CreateDirectory(dir);
        return dir;
    }

    private static double DirectoryMiB(string dir) =>
        Directory.Exists(dir)
            ? new DirectoryInfo(dir).EnumerateFiles("*", SearchOption.AllDirectories).Sum(f => f.Length)
                / (1024.0 * 1024.0)
            : 0.0;

    private readonly record struct PhaseSummary(
        double Step, double MovePct, double WaitPct, double OpPct, double Imbalance);

    private PhaseSummary ProfilePhases(
        Spec spec, HashSet<string> outputViews,
        Dictionary<string, List<(object?[] Values, long Weight)>> tableRows, int workers)
    {
        Assert.True(SqlProgram.TryCompileParallel(
            spec.Program, outputViews, workers, out var program, options: ParOptions,
            numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low));

        using (program)
        {
            foreach (var (table, rows) in tableRows)
            {
                program!.Table(table).Push(rows);
            }

            StepProfiler.Enabled = true;
            StepProfiler.Configure(workers);
            try
            {
                program!.Step();
                return Summarize(workers);
            }
            finally
            {
                StepProfiler.Enabled = false;
            }
        }
    }

    private static PhaseSummary Summarize(int workers)
    {
        var steps = Math.Max(1, StepProfiler.Steps);
        double ToMs(long ticks) => ticks * 1000.0 / StepProfiler.Frequency / steps;

        double sumStep = 0, sumSplit = 0, sumWait = 0, sumGather = 0, sumOp = 0, sumBusy = 0, maxBusy = 0;
        for (var w = 0; w < workers; w++)
        {
            var step = ToMs(StepProfiler.StepTicksOf(w));
            var split = ToMs(StepProfiler.SplitTicksOf(w));
            var wait = ToMs(StepProfiler.WaitTicksOf(w));
            var gather = ToMs(StepProfiler.GatherTicksOf(w));
            var op = Math.Max(0, step - split - wait - gather);
            var busy = step - wait;
            sumStep += step; sumSplit += split; sumWait += wait; sumGather += gather; sumOp += op; sumBusy += busy;
            maxBusy = Math.Max(maxBusy, busy);
        }

        var meanStep = sumStep / workers;
        var meanBusy = sumBusy / workers;
        return new PhaseSummary(
            meanStep,
            meanStep > 0 ? 100.0 * (sumSplit + sumGather) / workers / meanStep : 0,
            meanStep > 0 ? 100.0 * sumWait / workers / meanStep : 0,
            meanStep > 0 ? 100.0 * sumOp / workers / meanStep : 0,
            meanBusy > 0 ? maxBusy / meanBusy : 1.0);
    }

    private const int Repeats = 3;

    private static double Median(List<double> xs)
    {
        xs.Sort();
        return xs[xs.Count / 2];
    }

    private (double Ms, Dictionary<string, long> Output) RunSerial(
        Spec spec, HashSet<string> outputViews,
        Dictionary<string, List<(object?[] Values, long Weight)>> tableRows)
    {
        var times = new List<double>();
        Dictionary<string, long> output = new();
        for (var r = 0; r <= Repeats; r++) // r==0 is a warm-up, discarded
        {
            var program = SqlProgram.Compile(
                spec.Program, outputViews,
                numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low);
            foreach (var (table, rows) in tableRows)
            {
                program.Table(table).Push(rows);
            }

            var sw = Stopwatch.StartNew();
            program.Step();
            sw.Stop();
            var view = program.Outputs[TargetView];
            output = Materialize(view.CurrentView, view.Schema.Count);
            if (r > 0)
            {
                times.Add(sw.Elapsed.TotalMilliseconds);
            }
        }

        return (Median(times), output);
    }

    private (double Ms, Dictionary<string, long> Output) RunParallel(
        Spec spec, HashSet<string> outputViews,
        Dictionary<string, List<(object?[] Values, long Weight)>> tableRows, int workers, int repeats)
    {
        var times = new List<double>();
        Dictionary<string, long> output = new();
        for (var r = 0; r < repeats; r++)
        {
            Assert.True(SqlProgram.TryCompileParallel(
                spec.Program, outputViews, workers, out var program, options: ParOptions,
                numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low));

            using (program)
            {
                foreach (var (table, rows) in tableRows)
                {
                    program!.Table(table).Push(rows);
                }

                var sw = Stopwatch.StartNew();
                program!.Step();
                sw.Stop();
                times.Add(sw.Elapsed.TotalMilliseconds);
                var view = program.Outputs[TargetView];
                output = Materialize(view.CurrentView, view.Schema.Count);
            }
        }

        return (Median(times), output);
    }

    private static async Task<List<(object?[] Values, long Weight)>> ReadTableRows(
        string table, string uri, TableInput decodeInput)
    {
        var rows = new List<(object?[], long)>();
        await using var connector = new DeltaInputConnector(table, uri);
        await connector.ResolveSchemaAsync(decodeInput.Schema, CancellationToken.None);
        var batch = await connector.NextAsync(connector.InitialOffset, CancellationToken.None);
        if (batch is null)
        {
            return rows;
        }

        await foreach (var vb in batch.Content)
        {
            rows.AddRange(decodeInput.DecodeArrowDeltas(vb.Batch, vb.Weights));
        }

        return rows;
    }

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var cells = new string[width];
            for (var i = 0; i < width; i++)
            {
                cells[i] = kv.Key[i]?.ToString() ?? "<null>";
            }

            var key = string.Join("|", cells);
            map[key] = map.GetValueOrDefault(key) + kv.Value.Value;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static bool DictEquals(Dictionary<string, long> a, Dictionary<string, long> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach (var (k, v) in a)
        {
            if (!b.TryGetValue(k, out var bv) || bv != v)
            {
                return false;
            }
        }

        return true;
    }

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed record Spec(
        List<string> Program,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
