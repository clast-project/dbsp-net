// A/B/C micro-benchmark isolating the inter-view SEAM cost from the typed
// state-representation cost, on a single typed->typed adjacency:
//   staging_daily_market (source) -> brokerage_daily_market (stateless projection)
//                                  -> daily_market (heavy stateful window aggregate).
// The seam between the two views carries ~1.28M rows. Three configs, each driven by
// the SAME pre-read Arrow input (encoding excluded), measuring ONLY circuit.Step():
//
//   S (structural)   : both views structural       — object[] seam, object[] state (baseline)
//   H (hybrid)       : both typed, STRUCTURAL seam  — lift/adapt round-trip at the seam (today's program path)
//   T (typed-seam)   : both typed, TYPED seam       — brokerage's typed stream feeds daily directly
//
// Decomposition:
//   H - S = round-trip + typed state + boundary lifts/adapts
//   T - S = typed state + boundary lifts/adapts (NO middle round-trip)  == the user's typed-seam design
//   H - T = the inter-view seam round-trip ALONE
//
// Gated on the same env as IvmBatchProfile (no-op otherwise):
//   IVM_DATA_ROOT  local dir mirroring /data/raw/delta
//   IVM_SPEC       deploy spec JSON (for the 3 statements + the staging URI)
using System.Globalization;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using System.Text.Json;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class TypedSeamAbBenchmark
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public TypedSeamAbBenchmark(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task DailyMarketSeam_AbC()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;

        // The 3 statements for this slice (in dependency order).
        var tableSql = FindStmt(spec.Program, "TABLE", "staging_daily_market");
        var brokSql = FindStmt(spec.Program, "VIEW", "brokerage_daily_market");
        var dailySql = FindStmt(spec.Program, "VIEW", "daily_market");
        var stmts = new[] { tableSql, brokSql, dailySql };
        var outputs = new HashSet<string>(StringComparer.Ordinal) { "daily_market" };

        // Resolve once (numeric-string coercion + Calcite NULL collation, matching the
        // real deploy) — modes are applied at resolve time and baked into the plans.
        var resolved = SqlProgram.Resolve(stmts, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);
        var stagingSchema = resolved.Tables.Single(t => t.TableName == "staging_daily_market").Schema;
        var brokPlan = PlanOptimizer.Optimize(resolved.Views.Single(v => v.ViewName == "brokerage_daily_market").Query);
        var dailyPlan = PlanOptimizer.Optimize(resolved.Views.Single(v => v.ViewName == "daily_market").Query);

        // Read the source ONCE into memory (Arrow batches, projected to the SQL schema),
        // so every config replays the identical input and ingest is held constant.
        var stagingUri = Path.Combine(dataRoot, StripPrefix(
            spec.Inputs.Single(i => i.Table == "staging_daily_market").Uri, "/data/raw/delta/"));
        var batches = await ReadAllAsync(stagingSchema, stagingUri);
        var totalRows = batches.Sum(b => (long)b.Batch.Length);
        _out.WriteLine($"source staging_daily_market: {batches.Count} batch(es), {totalRows} rows");

        // --- config builders (fresh circuit each call) ---
        Built BuildProgram(bool typed)
        {
            var opts = typed ? new CompileOptions { TypeEligibleProgramViews = true } : CompileOptions.Default;
            var program = PlanToCircuit.CompileProgram(resolved.Tables, resolved.Views, null, opts);
            return new Built(
                program.Table("staging_daily_market"),
                program.Step,
                () => program.Outputs["daily_market"].CurrentView.Count);
        }

        (long Alloc, double Ms, long Rows) RunProgram(bool typed)
        {
            var b = BuildProgram(typed);
            foreach (var vb in batches) b.Input.PushArrow(vb.Batch, vb.Weights);
            return Measure(b.Step, b.RowCount);
        }

        (long Alloc, double Ms, long Rows) RunTypedSeam()
        {
            InputHandle<ZSet<StructuralRow, Z64>>? handle = null;
            IntegratedViewHandle<StructuralRow>? view = null;
            var circuit = RootCircuit.Build(builder =>
            {
                var (h, stream) = builder.ZSetInput<StructuralRow, Z64>();
                handle = h;
                var scans = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal)
                {
                    ["staging_daily_market"] = stream,
                };
                var chain = new (string, LogicalPlan)[]
                {
                    ("brokerage_daily_market", brokPlan),
                    ("daily_market", dailyPlan),
                };
                var outStream = TypedPlanCompiler.TryCompileTypedSeamChain(
                    builder, chain, scans, StructuralRowCodec.Instance)
                    ?? throw new InvalidOperationException("typed-seam chain did not compile");
                view = builder.Integrate(outStream).View;
            });
            var input = new TableInput(handle!, stagingSchema, StructuralRowCodec.Instance);
            foreach (var vb in batches) input.PushArrow(vb.Batch, vb.Weights);
            return Measure(circuit.Step, () => view!.Current.Count);
        }

        // 1 warmup (discarded) + 3 measured passes per config, fresh circuit each pass.
        var results = new List<(string Cfg, long Alloc, double Ms, long Rows)>();
        foreach (var (cfg, run) in new (string, Func<(long, double, long)>)[]
        {
            ("S structural", () => RunProgram(false)),
            ("H hybrid-seam", () => RunProgram(true)),
            ("T typed-seam", RunTypedSeam),
        })
        {
            run(); // warmup
            var passes = new List<(long Alloc, double Ms, long Rows)>();
            for (var i = 0; i < 3; i++) passes.Add(run());
            var med = passes.OrderBy(p => p.Ms).ElementAt(1);
            var allocMed = passes.OrderBy(p => p.Alloc).ElementAt(1).Alloc;
            results.Add((cfg, allocMed, med.Ms, med.Rows));
        }

        var sb = new StringBuilder();
        sb.AppendLine("===== TYPED-SEAM A/B/C (daily_market slice, Step-only, median of 3) =====");
        sb.AppendLine(CultureInfo.InvariantCulture,
            $"input {totalRows} rows   ServerGC={System.Runtime.GCSettings.IsServerGC} procs={Environment.ProcessorCount}");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"  {"config",-16} {"step ms",10} {"alloc MiB",12} {"out rows",10}");
        foreach (var r in results)
        {
            sb.AppendLine(CultureInfo.InvariantCulture,
                $"  {r.Cfg,-16} {r.Ms,10:F1} {r.Alloc / (1024.0 * 1024.0),12:F1} {r.Rows,10}");
        }

        var s = results[0];
        var h = results[1];
        var t = results[2];
        double PctA(long a, long b0) => (a - b0) / (double)b0 * 100.0;
        double PctM(double a, double b0) => (a - b0) / b0 * 100.0;
        sb.AppendLine();
        sb.AppendLine("-- decomposition (relative to S) --");
        sb.AppendLine(CultureInfo.InvariantCulture,
            $"  H-S (hybrid total)     alloc {PctA(h.Alloc, s.Alloc),6:F1}%   time {PctM(h.Ms, s.Ms),6:F1}%");
        sb.AppendLine(CultureInfo.InvariantCulture,
            $"  T-S (typed-seam design) alloc {PctA(t.Alloc, s.Alloc),6:F1}%   time {PctM(t.Ms, s.Ms),6:F1}%   <- the user's model");
        sb.AppendLine(CultureInfo.InvariantCulture,
            $"  H-T (seam round-trip)   alloc {PctA(h.Alloc, t.Alloc),6:F1}%   time {PctM(h.Ms, t.Ms),6:F1}%");
        sb.AppendLine();
        sb.AppendLine(s.Rows == h.Rows && h.Rows == t.Rows
            ? $"row counts MATCH across all three ({s.Rows}) — apples-to-apples."
            : $"WARNING: row counts differ! S={s.Rows} H={h.Rows} T={t.Rows}");

        var report = sb.ToString();
        _out.WriteLine(report);
        var outFile = Environment.GetEnvironmentVariable("IVM_SEAM_FILE");
        if (!string.IsNullOrEmpty(outFile)) File.WriteAllText(outFile, report);

        Assert.Equal(s.Rows, t.Rows); // typed-seam must be correctness-equivalent
    }

    private static (long Alloc, double Ms, long Rows) Measure(Action step, Func<long> rowCount)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var a0 = GC.GetTotalAllocatedBytes(precise: true);
        var sw = Stopwatch.StartNew();
        step();
        sw.Stop();
        var alloc = GC.GetTotalAllocatedBytes(precise: true) - a0;
        return (alloc, sw.Elapsed.TotalMilliseconds, rowCount());
    }

    private static async Task<List<VersionBatch>> ReadAllAsync(Schema sqlSchema, string uri)
    {
        var conn = new DeltaInputConnector("staging_daily_market", uri);
        await conn.ResolveSchemaAsync(sqlSchema, CancellationToken.None);
        var batch = await conn.NextAsync(conn.InitialOffset, CancellationToken.None)
                    ?? throw new InvalidOperationException("no source batch");
        var list = new List<VersionBatch>();
        await foreach (var vb in batch.Content)
        {
            list.Add(vb);
        }

        return list;
    }

    private static string FindStmt(List<string> program, string kind, string name)
    {
        foreach (var p in program)
        {
            var m = Regex.Match(p, @"^\s*CREATE\s+" + kind + @"\s+(\w+)", RegexOptions.IgnoreCase);
            if (m.Success && string.Equals(m.Groups[1].Value, name, StringComparison.Ordinal))
            {
                return p;
            }
        }

        throw new InvalidOperationException($"statement CREATE {kind} {name} not found in spec");
    }

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed record Built(TableInput Input, Action Step, Func<long> RowCount);

    private sealed record Spec(List<string> Program, List<InputBinding> Inputs);

    private sealed record InputBinding(string Table, string Uri, string Mode);
}
