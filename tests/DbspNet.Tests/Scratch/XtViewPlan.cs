// One view's OPTIMIZED logical plan from a deployed program (xt work/program.json), as the parallel
// program compile sees it. Gated on XT_PROGRAM_JSON + XT_VIEW; a no-op otherwise.
using System.Collections;
using System.Reflection;
using System.Text.Json;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class XtViewPlan
{
    private readonly ITestOutputHelper _out;

    public XtViewPlan(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Dump()
    {
        var path = Environment.GetEnvironmentVariable("XT_PROGRAM_JSON");
        var view = Environment.GetEnvironmentVariable("XT_VIEW");
        if (string.IsNullOrEmpty(path) || string.IsNullOrEmpty(view))
        {
            _out.WriteLine("XT_PROGRAM_JSON / XT_VIEW not set — skipping.");
            return;
        }

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var program = doc.RootElement.GetProperty("program").EnumerateArray().Select(e => e.GetString()!).ToList();
        var outputs = doc.RootElement.GetProperty("outputs").EnumerateArray().Select(e => e.GetProperty("view").GetString()!).ToHashSet(StringComparer.Ordinal);
        var resolved = SqlProgram.Resolve(program, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);
        var v = resolved.Views.First(x => x.ViewName == view);
        var live = PlanColumnLiveness.ComputeProgramLiveColumns(resolved.Views);
        var q = live.TryGetValue(view, out var lo) ? PlanColumnLiveness.PruneDeadColumns(v.Query, lo) : v.Query;
        Walk(PlanOptimizer.Optimize(q), 0, new Dictionary<LogicalPlan, int>(ReferenceEqualityComparer.Instance));
    }

    private void Walk(LogicalPlan p, int depth, Dictionary<LogicalPlan, int> seen)
    {
        var ind = new string(' ', depth * 2);
        if (seen.TryGetValue(p, out var id))
        {
            _out.WriteLine($"{ind}{p.GetType().Name} [shared #{id}]");
            return;
        }

        seen[p] = seen.Count;
        var cols = string.Join(",", p.Schema.Columns.Select(c => c.Name));
        var detail = p switch
        {
            ScanPlan s => $" {s.TableName}",
            AggregatePlan a => $" keys=[{string.Join("; ", a.GroupKeys.Select(Short))}] aggs={a.Aggregates.Count}",
            JoinPlan j => $" {j.JoinType} eq=[{string.Join(" ", j.EquiKeys.Select(e => $"{e.LeftIndex}={e.RightIndex}"))}] allowNull={j.AllowNullKeys} residual={(j.Residual is null ? "-" : Short(j.Residual))}",
            ProjectPlan pr => $" [{string.Join("; ", pr.Projections.Select(Short))}]",
            FilterPlan f => $" {Short(f.Predicate)}",
            _ => "",
        };
        _out.WriteLine($"{ind}{p.GetType().Name} #{seen[p]}{detail}  ({cols})");
        foreach (var c in Children(p))
        {
            Walk(c, depth + 1, seen);
        }
    }

    private static string Short(object e)
    {
        var s = e.ToString() ?? "";
        return s.Length > 160 ? s[..160] + "…" : s;
    }

    private static IEnumerable<LogicalPlan> Children(LogicalPlan plan)
    {
        foreach (var p in plan.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (typeof(LogicalPlan).IsAssignableFrom(p.PropertyType))
            {
                if (p.GetValue(plan) is LogicalPlan child)
                {
                    yield return child;
                }
            }
            else if (p.PropertyType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(p.PropertyType) && p.GetValue(plan) is IEnumerable seq)
            {
                foreach (var item in seq)
                {
                    if (item is LogicalPlan child)
                    {
                        yield return child;
                    }
                }
            }
        }
    }
}
