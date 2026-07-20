// Coverage census: for each ivm-bench view, attempt the typed compile path
// (TryCompileWithStructuralBoundary) against placeholder structural upstream
// streams, and report typed vs fell-back-to-structural + the plan node types
// present. Gated on IVM_SPEC (no-op otherwise). Answers "how much of the program
// could the typed path handle today", esp. for the HOT views.
using System.Globalization;
using System.Text;
using System.Text.Json;
using DbspNet.Core.Algebra;
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

public class TypedCoverageCensus
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    // The views that dominate batch-1 step time (from the labeled profile).
    private static readonly HashSet<string> Hot = new(StringComparer.Ordinal)
    {
        "daily_market", "watches", "fact_holdings", "fact_watches", "market_volatility",
        "dim_trade", "watches_history", "trades", "trades_history", "fact_trade",
        "fact_cash_transactions", "fact_cash_balances", "holdings_history",
    };

    private readonly ITestOutputHelper _out;

    public TypedCoverageCensus(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Census()
    {
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        var outFile = Environment.GetEnvironmentVariable("IVM_CENSUS_FILE")
                      ?? Path.Combine(Path.GetTempPath(), "ivm-typed-census.txt");
        if (string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_SPEC not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputs = spec.Output_Bindings.Select(o => o.View).ToHashSet(StringComparer.Ordinal);
        var resolved = SqlProgram.Resolve(spec.Program, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);

        var results = new List<(string View, bool Typed, bool IsOutput, string Nodes)>();

        RootCircuit.Build(builder =>
        {
            // Placeholder structural input stream for every table + view name, so
            // a scan of any of them resolves. The typed lift uses the ScanPlan's
            // own schema, so a placeholder input suffices.
            var streams = new Dictionary<string, Stream<ZSet<StructuralRow, Z64>>>(StringComparer.Ordinal);
            foreach (var t in resolved.Tables)
            {
                streams[t.TableName] = builder.ZSetInput<StructuralRow, Z64>().Stream;
            }

            foreach (var v in resolved.Views)
            {
                if (!streams.ContainsKey(v.ViewName))
                {
                    streams[v.ViewName] = builder.ZSetInput<StructuralRow, Z64>().Stream;
                }
            }

            foreach (var v in resolved.Views)
            {
                var optimized = PlanOptimizer.Optimize(v.Query);
                bool typed;
                try
                {
                    var s = TypedPlanCompiler.TryCompileWithStructuralBoundary(
                        builder, optimized, streams, StructuralRowCodec.Instance);
                    typed = s is not null;
                }
                catch (Exception ex)
                {
                    typed = false;
                    results.Add((v.ViewName, false, v.IsOutput, "ERROR:" + ex.GetType().Name));
                    continue;
                }

                results.Add((v.ViewName, typed, v.IsOutput, string.Join(",", NodeTypes(optimized).OrderBy(x => x))));
            }
        });

        var sb = new StringBuilder();
        var typedCount = results.Count(r => r.Typed);
        var hotResults = results.Where(r => Hot.Contains(r.View)).ToList();
        var hotTyped = hotResults.Count(r => r.Typed);

        sb.AppendLine("===== TYPED COVERAGE CENSUS =====");
        sb.AppendLine(CultureInfo.InvariantCulture, $"views: {results.Count}   typed: {typedCount}   fell-back: {results.Count - typedCount}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"HOT views: {hotResults.Count}   typed: {hotTyped}   fell-back: {hotResults.Count - hotTyped}");
        sb.AppendLine();
        sb.AppendLine("-- HOT views (dominate batch-1) --");
        foreach (var r in hotResults.OrderBy(r => r.Typed))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  [{(r.Typed ? "TYPED " : "struct")}] {r.View,-24} {(r.Typed ? "" : NonTrivialNodes(r.Nodes))}");
        }

        sb.AppendLine();
        sb.AppendLine("-- fell-back views (non-hot), with node types --");
        foreach (var r in results.Where(r => !r.Typed && !Hot.Contains(r.View)).OrderBy(r => r.View))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {r.View,-26} {NonTrivialNodes(r.Nodes)}");
        }

        sb.AppendLine();
        sb.AppendLine("-- typed views --");
        sb.AppendLine("  " + string.Join(", ", results.Where(r => r.Typed).Select(r => r.View)));

        File.WriteAllText(outFile, sb.ToString());
        _out.WriteLine($"census → {outFile}");
        _out.WriteLine(sb.ToString());
    }

    // Highlight the node types most likely to be the bail reason (skip the always-
    // supported Scan/Project/Filter noise).
    private static string NonTrivialNodes(string nodes)
    {
        var interesting = nodes.Split(',')
            .Where(n => n is not ("ScanPlan" or "ProjectPlan" or "FilterPlan" or "CteScanPlan"))
            .ToList();
        return interesting.Count == 0 ? "(only scan/project/filter/cte)" : string.Join(",", interesting);
    }

    private static HashSet<string> NodeTypes(LogicalPlan plan)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        var seenCtes = new HashSet<CteRef>();
        void Walk(LogicalPlan p)
        {
            set.Add(p.GetType().Name);
            foreach (var c in Children(p, seenCtes))
            {
                Walk(c);
            }
        }

        Walk(plan);
        return set;
    }

    private static LogicalPlan[] Children(LogicalPlan p, HashSet<CteRef> seenCtes) => p switch
    {
        FilterPlan f => new[] { f.Input },
        ProjectPlan pr => new[] { pr.Input },
        JoinPlan j => new[] { j.Left, j.Right },
        AggregatePlan a => new[] { a.Input },
        DistinctPlan d => new[] { d.Input },
        TopKPlan t => new[] { t.Input },
        PartitionedTopKPlan pt => new[] { pt.Input },
        PartitionedRankPlan pr => new[] { pr.Input },
        WindowAggregatePlan wa => new[] { wa.Input },
        WindowOffsetPlan wo => new[] { wo.Input },
        SemiJoinPlan sj => new[] { sj.Input, sj.Subquery },
        ScalarSubqueryJoinPlan s => new[] { s.Input }.Concat(s.Subqueries).ToArray(),
        CorrelatedScalarSubqueryJoinPlan c => new[] { c.Input, c.Subquery },
        UnionAllPlan u => u.Branches.ToArray(),
        DifferencePlan diff => new[] { diff.Left, diff.Right },
        CteScanPlan cte when seenCtes.Add(cte.Cte) => new[] { cte.Cte.Plan },
        _ => Array.Empty<LogicalPlan>(),
    };

    private sealed record Spec(List<string> Program, List<OutputBinding> Output_Bindings);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
