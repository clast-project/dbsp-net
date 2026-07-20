// Diagnostic: dump the optimized plan tree of a program view (default
// broker_performance) so we can see why the semi-join narrowing rule does or
// doesn't fire. Gated on IVM_SPEC (no-op otherwise).
using System.Text;
using System.Text.Json;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class BrokerPlanDump
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public BrokerPlanDump(ITestOutputHelper output) => _out = output;

    [Fact]
    public void DumpViewPlan()
    {
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        var viewName = Environment.GetEnvironmentVariable("IVM_DUMP_VIEW") ?? "broker_performance";
        var dumpFile = Environment.GetEnvironmentVariable("IVM_DUMP_FILE")
                       ?? Path.Combine(Path.GetTempPath(), "ivm-plan-dump.txt");
        if (string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_SPEC not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputs = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);
        var resolved = SqlProgram.Resolve(spec.Program, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);

        var view = resolved.Views.First(v => string.Equals(v.ViewName, viewName, StringComparison.Ordinal));
        var sb = new StringBuilder();
        sb.AppendLine("===== " + viewName + " — RAW plan =====");
        Dump(sb, view.Query, 0);
        sb.AppendLine();
        sb.AppendLine("===== " + viewName + " — OPTIMIZED plan =====");
        Dump(sb, PlanOptimizer.Optimize(view.Query), 0);

        File.WriteAllText(dumpFile, sb.ToString());
        _out.WriteLine($"dump → {dumpFile}");
        _out.WriteLine(sb.ToString());
    }

    private static void Dump(StringBuilder sb, LogicalPlan plan, int depth)
    {
        var indent = new string(' ', depth * 2);
        void Line(string s) => sb.AppendLine(indent + s);
        switch (plan)
        {
            case ScanPlan s:
                Line($"Scan({s.TableName}) [cols={s.Schema.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)}]");
                break;
            case ProjectPlan p:
                Line($"Project [cols={p.Schema.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)}] exprs=[{string.Join(", ", p.Projections.Select(x => x.Expression.GetType().Name))}]");
                Dump(sb, p.Input, depth + 1);
                break;
            case FilterPlan f:
                Line("Filter");
                Dump(sb, f.Input, depth + 1);
                break;
            case JoinPlan j:
                Line($"Join({j.JoinType}) equiKeys={j.EquiKeys.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)} residual={(j.Residual is null ? "no" : "YES")} nullKeys={j.AllowNullKeys} leftCols={j.Left.Schema.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
                Dump(sb, j.Left, depth + 1);
                Dump(sb, j.Right, depth + 1);
                break;
            case SemiJoinPlan sj:
                Line($"SemiJoin(anti={sj.IsAnti}) equiKeys=[{string.Join(", ", sj.EquiKeys.Select(k => k.OuterKey.GetType().Name + "->" + k.InnerColumnIndex.ToString(System.Globalization.CultureInfo.InvariantCulture)))}]");
                Line("  INPUT:");
                Dump(sb, sj.Input, depth + 2);
                Line("  SUBQUERY:");
                Dump(sb, sj.Subquery, depth + 2);
                break;
            case AggregatePlan a:
                Line($"Aggregate groupKeys={a.GroupKeys.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)} aggs={a.Aggregates.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
                Dump(sb, a.Input, depth + 1);
                break;
            case DistinctPlan d:
                Line("Distinct");
                Dump(sb, d.Input, depth + 1);
                break;
            default:
                Line(plan.GetType().Name);
                foreach (var child in ChildrenOf(plan))
                {
                    Dump(sb, child, depth + 1);
                }

                break;
        }
    }

    private static IEnumerable<LogicalPlan> ChildrenOf(LogicalPlan plan) => plan switch
    {
        UnionAllPlan u => u.Branches,
        DifferencePlan diff => new[] { diff.Left, diff.Right },
        ScalarSubqueryJoinPlan s => new[] { s.Input }.Concat(s.Subqueries),
        CorrelatedScalarSubqueryJoinPlan c => new[] { c.Input, c.Subquery },
        _ => Enumerable.Empty<LogicalPlan>(),
    };

    private sealed record Spec(List<string> Program, List<OutputBinding> Output_Bindings);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
