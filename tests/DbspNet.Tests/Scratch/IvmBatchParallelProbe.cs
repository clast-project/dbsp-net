// Increment 1/2 measurement probe (docs/design-structural-parallel.md): does the
// real ivm-bench SF=3 batch-1 program compile data-parallel, and which views (if
// any) block it? Gated on IVM_SPEC so it is a no-op in CI.
//
//   IVM_SPEC=<scratch>/ivm_spec.json \
//     dotnet test --filter FullyQualifiedName~IvmBatchParallelProbe.ReportParallelizability
using System.Text.Json;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmBatchParallelProbe
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmBatchParallelProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void ReportParallelizability()
    {
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_SPEC not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputViews = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);

        // Resolve the program the same way SqlProgram.Compile does (ivm-bench
        // coercion + NULL collation), then report which reachable views the
        // exchange-insertion guard admits.
        var resolved = SqlProgram.Resolve(
            spec.Program, outputViews, numericStringCoercion: true, nullCollation: NullCollation.Low);

        _out.WriteLine($"program: {resolved.Tables.Count} tables, {resolved.Views.Count} views, {outputViews.Count} outputs");

        // Per-view verdict (optimize each view as the program compile would).
        var refused = new List<string>();
        foreach (var v in resolved.Views)
        {
            var optimized = DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(v.Query);
            var ok = InvokeCanCompileParallel(optimized);
            if (!ok)
            {
                refused.Add(v.ViewName);
                _out.WriteLine($"  REFUSED  {v.ViewName}: {DescribeBlockers(optimized)}");
            }
        }

        var parallelOk = SqlProgram.TryCompileParallel(
            spec.Program, outputViews, 4, out var parallel,
            numericStringCoercion: true, nullCollation: NullCollation.Low);
        parallel?.Dispose();

        _out.WriteLine(parallelOk
            ? "WHOLE PROGRAM: parallelizable at W=4 ✓"
            : $"WHOLE PROGRAM: NOT parallelizable — {refused.Count} view(s) block it: {string.Join(", ", refused)}");
    }

    // The guard is private to PlanToCircuit; probe it structurally here so the
    // report can name blockers without exposing the internal API.
    private static bool InvokeCanCompileParallel(LogicalPlan plan) => DescribeBlockers(plan).Length == 0;

    private static string DescribeBlockers(LogicalPlan plan)
    {
        var blockers = new List<string>();
        Walk(plan, blockers);
        return string.Join("; ", blockers.Distinct());
    }

    private static void Walk(LogicalPlan plan, List<string> blockers)
    {
        switch (plan)
        {
            case ScanPlan:
                return;
            case CteScanPlan c:
                Walk(c.Cte.Plan, blockers);
                return;
            case FilterPlan f:
                Walk(f.Input, blockers);
                return;
            case ProjectPlan p:
                Walk(p.Input, blockers);
                return;
            case JoinPlan j:
                if (j.JoinType != DbspNet.Sql.Parser.Ast.JoinType.Inner)
                {
                    blockers.Add($"{j.JoinType} join");
                }

                Walk(j.Left, blockers);
                Walk(j.Right, blockers);
                return;
            case AggregatePlan a:
                Walk(a.Input, blockers);
                return;
            case UnionAllPlan u:
                foreach (var b in u.Branches)
                {
                    Walk(b, blockers);
                }

                return;
            case DistinctPlan d:
                Walk(d.Input, blockers);
                return;
            case PartitionedTopKPlan pt:
                if (pt.PartitionKeys.Count == 0)
                {
                    blockers.Add("global top-K");
                }

                Walk(pt.Input, blockers);
                return;
            case PartitionedRankPlan pr:
                if (pr.PartitionKeys.Count == 0)
                {
                    blockers.Add("global rank");
                }

                Walk(pr.Input, blockers);
                return;
            case WindowAggregatePlan wa:
                if (wa.PartitionKeys.Count == 0)
                {
                    blockers.Add("global window aggregate");
                }

                Walk(wa.Input, blockers);
                return;
            case WindowOffsetPlan wo:
                if (wo.PartitionKeys.Count == 0)
                {
                    blockers.Add("global window offset");
                }

                Walk(wo.Input, blockers);
                return;
            case ScalarSubqueryJoinPlan:
                blockers.Add("scalar-subquery join");
                return;
            case CorrelatedScalarSubqueryJoinPlan:
                blockers.Add("correlated scalar subquery");
                return;
            case SemiJoinPlan:
                blockers.Add("semi-join");
                return;
            case DifferencePlan:
                blockers.Add("set difference (EXCEPT)");
                return;
            case RecursiveCtePlan:
                blockers.Add("recursive CTE");
                return;
            case TemporalFilterPlan:
                blockers.Add("temporal filter");
                return;
            case TopKPlan:
                blockers.Add("global top-K");
                return;
            default:
                blockers.Add(plan.GetType().Name);
                return;
        }
    }

    private sealed record Spec(
        List<string> Program,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
