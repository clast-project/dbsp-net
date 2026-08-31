// Analysis-only diagnostic for the program-level column-liveness pass
// (docs/design-column-liveness.md). Resolves the ivm-bench deploy program with
// the REAL compiler front-end and runs PlanColumnLiveness — NO plan mutation, NO
// circuit build — reporting per-view DEAD output columns (produced but read by no
// output and no live downstream view). Confirms the daily_market fifty_two_week_*
// finding against the live compiler before any rewrite exists.
//
// Gated: IVM_SPEC = the dbt_to_program.py deploy spec (same file IvmBatchProfile
// uses). No-op otherwise.
using System.Text.Json;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class ColumnLivenessProbe
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public ColumnLivenessProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Report()
    {
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_SPEC not set — skipping column-liveness probe.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);

        // Same resolve knobs as DbspNetEngine.DeployAsync / IvmBatchProfile.
        var resolved = SqlProgram.Resolve(
            spec.Program, outputViews, numericStringCoercion: true, nullCollation: NullCollation.Low);

        var live = PlanColumnLiveness.ComputeProgramLiveColumns(resolved.Views);

        // Optional: dump one view's plan tree (node kinds + schema arity) to see
        // how CTEs are represented (shared CteScanPlan vs inlined).
        var dumpView = Environment.GetEnvironmentVariable("IVM_DUMP_VIEW");
        if (!string.IsNullOrEmpty(dumpView))
        {
            var v = resolved.Views.First(x => string.Equals(x.ViewName, dumpView, StringComparison.OrdinalIgnoreCase));
            _out.WriteLine($"=== plan tree for {v.ViewName} (schema {v.Query.Schema.Count} cols) ===");
            DumpPlan(v.Query, 0);
            _out.WriteLine("=== end plan ===");
        }

        _out.WriteLine($"views: {resolved.Views.Count}   outputs: {outputViews.Count}");

        var deadViews = new List<string>();
        var deadColViews = 0;
        long deadColTotal = 0;

        foreach (var v in resolved.Views)
        {
            var schema = v.Query.Schema;
            if (!live.TryGetValue(v.ViewName, out var liveCols))
            {
                deadViews.Add(v.ViewName); // unreached: whole view dead
                continue;
            }

            var dead = Enumerable.Range(0, schema.Count).Where(i => !liveCols.Contains(i)).ToList();
            if (dead.Count == 0)
            {
                continue;
            }

            deadColViews++;
            deadColTotal += dead.Count;
            var tag = v.IsOutput ? " (OUTPUT!)" : "";
            var names = string.Join(", ", dead.Select(i => schema.Columns[i].Name));
            _out.WriteLine($"  {v.ViewName}{tag}: {dead.Count}/{schema.Count} dead output cols -> [{names}]");
        }

        _out.WriteLine($"\ndead VIEWS (unreached, whole-view): {deadViews.Count} -> [{string.Join(", ", deadViews)}]");
        _out.WriteLine($"views with dead COLUMNS: {deadColViews}   total dead cols: {deadColTotal}");

        // An OUTPUT view must never have dead columns (its connector writes the
        // full schema) — a sanity check on the seeding.
        foreach (var v in resolved.Views.Where(v => v.IsOutput))
        {
            if (live.TryGetValue(v.ViewName, out var lc))
            {
                Assert.Equal(v.Query.Schema.Count, lc.Count);
            }
        }
    }

    private void DumpPlan(DbspNet.Sql.Plan.LogicalPlan p, int depth)
    {
        var indent = new string(' ', depth * 2);
        var kind = p.GetType().Name;
        _out.WriteLine($"{indent}{kind} [{p.Schema.Count} cols]");
        switch (p)
        {
            case DbspNet.Sql.Plan.ScanPlan s:
                _out.WriteLine($"{indent}  scan {s.TableName}");
                break;
            case DbspNet.Sql.Plan.CteScanPlan c:
                _out.WriteLine($"{indent}  cte-body:");
                DumpPlan(c.Cte.Plan, depth + 2);
                break;
            case DbspNet.Sql.Plan.ProjectPlan pr:
                DumpPlan(pr.Input, depth + 1);
                break;
            case DbspNet.Sql.Plan.FilterPlan f:
                DumpPlan(f.Input, depth + 1);
                break;
            case DbspNet.Sql.Plan.WindowAggregatePlan wa:
                DumpPlan(wa.Input, depth + 1);
                break;
            case DbspNet.Sql.Plan.WindowOffsetPlan wo:
                DumpPlan(wo.Input, depth + 1);
                break;
            case DbspNet.Sql.Plan.AggregatePlan a:
                DumpPlan(a.Input, depth + 1);
                break;
            case DbspNet.Sql.Plan.JoinPlan j:
                DumpPlan(j.Left, depth + 1);
                DumpPlan(j.Right, depth + 1);
                break;
        }
    }

    private sealed record Spec(List<string> Program, List<OutputBinding> Output_Bindings);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
