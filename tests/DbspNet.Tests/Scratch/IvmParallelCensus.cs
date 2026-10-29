// What keeps each ivm-bench view out of the parallel (multicore) program compile: every
// construct PlanToCircuit's shardability check refuses, per reachable view, on the same
// optimized plan the parallel compile checks. Scopes the "run ivm-bench multicore" work.
//
// Gated on IVM_SPEC (a dbt_to_program.py spec); a no-op otherwise.
using System.Text.Json;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmParallelCensus
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmParallelCensus(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Census()
    {
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_SPEC not set — skipping.");
            return;
        }

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        var outputs = IvmSpecViews.ToCompile(spec.Outputs, spec.Output_Bindings.Select(b => b.View));
        var resolved = SqlProgram.Resolve(spec.Program, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);

        var tally = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var v in resolved.Views)
        {
            var optimized = DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(v.Query);
            var blockers = PlanToCircuit.ParallelBlockers(optimized);
            _out.WriteLine($"VIEW {v.ViewName}{(v.IsOutput ? " (output)" : "")}: "
                + (blockers.Count == 0 ? "shards" : string.Join("; ", blockers)));
            foreach (var b in blockers.Distinct())
            {
                tally[b] = tally.GetValueOrDefault(b) + 1;
            }
        }

        _out.WriteLine("-- views blocked, by construct --");
        foreach (var (b, n) in tally.OrderByDescending(kv => kv.Value))
        {
            _out.WriteLine($"TALLY {n,3}  {b}");
        }
    }

    private sealed record Spec(List<string> Program, List<string>? Outputs, List<Binding> Output_Bindings);

    private sealed record Binding(string View);
}
