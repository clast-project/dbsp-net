// Census of adjacent pointwise operators in the ivm-bench program (state-representation
// arc §9.8 lead): how many ApplyOp → ApplyOp edges exist where the producer's output has
// exactly one consumer (so the intermediate Z-set could be fused away), where each ApplyOp
// comes from in the compiler, and what the producers allocate on SF=3 batch 1.
//
// Gated on the same env vars as IvmBatchProfile, so it is a no-op in CI / normal runs:
//   IVM_DATA_ROOT, IVM_SPEC [, IVM_OUT_ROOT, IVM_CENSUS_FILE]
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.Json;
using DbspNet.Connectors.Abstractions;
using DbspNet.Connectors.EngineeredWood;
using DbspNet.Core.Circuit;
using DbspNet.Core.Circuit.Operators;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IvmApplyChainCensus
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly ITestOutputHelper _out;

    public IvmApplyChainCensus(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task Census_Local()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        var specPath = Environment.GetEnvironmentVariable("IVM_SPEC");
        if (string.IsNullOrEmpty(dataRoot) || string.IsNullOrEmpty(specPath))
        {
            _out.WriteLine("IVM_DATA_ROOT / IVM_SPEC not set — skipping the apply-chain census.");
            return;
        }

        var outRoot = Environment.GetEnvironmentVariable("IVM_OUT_ROOT")
                      ?? Path.Combine(Path.GetTempPath(), "ivm-census-out");
        var reportFile = Environment.GetEnvironmentVariable("IVM_CENSUS_FILE")
                         ?? Path.Combine(Path.GetTempPath(), "ivm-apply-census.txt");

        // The profiler's per-operator allocation needs DBSPNET_PROFILE on before
        // ProgramRunner is first touched.
        Environment.SetEnvironmentVariable("DBSPNET_PROFILE", "1");
        Environment.SetEnvironmentVariable(
            "DBSPNET_PROFILE_FILE", Path.Combine(Path.GetTempPath(), "ivm-census-profile.txt"));

        var spec = JsonSerializer.Deserialize<Spec>(File.ReadAllText(specPath), JsonOpts)!;
        string InUri(string uri) => Path.Combine(dataRoot, StripPrefix(uri, "/data/raw/delta/"));
        var outputViews = IvmSpecViews.ToCompile(spec.Outputs, spec.Output_Bindings.Select(o => o.View));
        var program = SqlProgram.Compile(
            spec.Program, outputViews, numericStringCoercion: true, nullCollation: NullCollation.Low);

        var inputs = spec.Inputs
            .Select(i => (IInputConnector)new DeltaInputConnector(i.Table, InUri(i.Uri)))
            .ToList();
        var outputs = spec.Output_Bindings
            .Select(o => (IOutputConnector)new DeltaOutputConnector(o.View, Path.Combine(outRoot, o.View), OutputMode.Truncate))
            .ToList();
        var runner = await ProgramRunner.CreateAsync(program, inputs, outputs);
        await runner.RunBatchAsync();

        var circuit = program.Circuit;
        var ops = circuit.Operators;
        var profile = circuit.CollectOperatorProfile().ToDictionary(p => p.Index);

        // Graph by reflection: each operator's Stream<> fields, split by name into the
        // stream it writes ("output"/"_out") and the streams it reads (everything else).
        var producerOf = new Dictionary<object, int>(ReferenceEqualityComparer.Instance);
        var readsOf = new List<object>[ops.Count];
        for (var i = 0; i < ops.Count; i++)
        {
            readsOf[i] = [];
            foreach (var (name, stream) in StreamFields(ops[i]))
            {
                if (IsOutputField(name))
                {
                    producerOf.TryAdd(stream, i);
                }
                else
                {
                    readsOf[i].Add(stream);
                }
            }
        }

        var consumersOf = new Dictionary<object, List<int>>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < ops.Count; i++)
        {
            foreach (var s in readsOf[i])
            {
                if (!consumersOf.TryGetValue(s, out var list))
                {
                    consumersOf[s] = list = [];
                }

                list.Add(i);
            }
        }

        bool IsApply(int i) => ops[i].GetType().IsGenericType
            && ops[i].GetType().GetGenericTypeDefinition() == typeof(ApplyOp<,>);

        var sb = new StringBuilder();
        var applyCount = Enumerable.Range(0, ops.Count).Count(IsApply);
        long applyAlloc = Enumerable.Range(0, ops.Count).Where(IsApply).Sum(i => Alloc(profile, i));
        sb.AppendLine(CultureInfo.InvariantCulture, $"operators {ops.Count}, ApplyOp {applyCount}, ApplyOp alloc {Mib(applyAlloc)} MiB");

        // Every ApplyOp by origin.
        sb.AppendLine();
        sb.AppendLine("-- ApplyOp by origin (count, alloc) --");
        foreach (var g in Enumerable.Range(0, ops.Count).Where(IsApply)
                     .GroupBy(i => Origin(ops[i]))
                     .OrderByDescending(g => g.Sum(i => Alloc(profile, i))))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {g.Count(),4}  {Mib(g.Sum(i => Alloc(profile, i))),9} MiB  {g.Key}");
        }

        // Single-consumer Apply → Apply edges: the producer's Z-set exists only to feed one
        // pointwise consumer.
        var edges = new List<(int From, int To)>();
        for (var i = 0; i < ops.Count; i++)
        {
            if (!IsApply(i))
            {
                continue;
            }

            foreach (var s in readsOf[i])
            {
                if (producerOf.TryGetValue(s, out var p) && IsApply(p)
                    && consumersOf.TryGetValue(s, out var c) && c.Count == 1)
                {
                    edges.Add((p, i));
                }
            }
        }

        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"-- single-consumer ApplyOp → ApplyOp edges: {edges.Count}, producer alloc {Mib(edges.Sum(e => Alloc(profile, e.From)))} MiB --");
        foreach (var g in edges
                     .GroupBy(e => $"{Origin(ops[e.From])}  →  {Origin(ops[e.To])}")
                     .OrderByDescending(g => g.Sum(e => Alloc(profile, e.From))))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {g.Count(),4}  {Mib(g.Sum(e => Alloc(profile, e.From))),9} MiB  {g.Key}");
        }

        // Longest single-consumer chains, with the views they belong to.
        sb.AppendLine();
        sb.AppendLine("-- top single-consumer chains by producer alloc (index: origin <view>) --");
        var next = edges.ToDictionary(e => e.From, e => e.To);
        var hasPrev = edges.Select(e => e.To).ToHashSet();
        var chains = next.Keys.Where(k => !hasPrev.Contains(k))
            .Select(start =>
            {
                var chain = new List<int> { start };
                while (next.TryGetValue(chain[^1], out var n))
                {
                    chain.Add(n);
                }

                return chain;
            })
            .OrderByDescending(ch => ch.Take(ch.Count - 1).Sum(i => Alloc(profile, i)))
            .Take(25);
        foreach (var ch in chains)
        {
            var links = string.Join("  →  ", ch.Select(i => $"[{i}] {Origin(ops[i])} <{profile.GetValueOrDefault(i).Label}>"));
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {Mib(ch.Take(ch.Count - 1).Sum(i => Alloc(profile, i))),7} MiB  {links}");
        }

        File.WriteAllText(reportFile, sb.ToString());
        _out.WriteLine(sb.ToString());
    }

    private static long Alloc(Dictionary<int, OperatorProfile> profile, int i) =>
        profile.TryGetValue(i, out var p) ? p.AllocBytes : 0;

    private static string Mib(long bytes) => (bytes / (1024.0 * 1024.0)).ToString("F1", System.Globalization.CultureInfo.InvariantCulture);

    private static bool IsOutputField(string name) =>
        name.Contains("output", StringComparison.OrdinalIgnoreCase) || name is "_out" or "<Output>k__BackingField";

    private static IEnumerable<(string Name, object Stream)> StreamFields(object op)
    {
        for (var t = op.GetType(); t is not null && t != typeof(object); t = t.BaseType)
        {
            foreach (var f in t.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly))
            {
                if (f.FieldType.IsGenericType && f.FieldType.GetGenericTypeDefinition() == typeof(Stream<>)
                    && f.GetValue(op) is { } s)
                {
                    yield return (f.Name, s);
                }
            }
        }
    }

    // "<builder method> / <compiler method>": the CircuitBuilder / LinearOperators method that
    // created the transform, and the first DbspNet.Sql lambda found inside its closure.
    private static string Origin(IOperator op)
    {
        var transform = op.GetType().GetField("_transform", BindingFlags.Instance | BindingFlags.NonPublic)
            ?.GetValue(op) as Delegate;
        if (transform is null)
        {
            return "?";
        }

        var builder = Enclosing(transform.Method);
        var sql = FindSqlLambda(transform.Target, depth: 0, new HashSet<object>(ReferenceEqualityComparer.Instance));
        return sql is null ? builder : $"{builder} / {sql}";
    }

    private static string? FindSqlLambda(object? target, int depth, HashSet<object> seen)
    {
        if (target is null || depth > 4 || !seen.Add(target))
        {
            return null;
        }

        foreach (var f in target.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public))
        {
            var v = f.GetValue(target);
            if (v is Delegate d)
            {
                if (d.Method.DeclaringType?.Namespace?.StartsWith("DbspNet.Sql", StringComparison.Ordinal) == true)
                {
                    return Enclosing(d.Method);
                }

                if (FindSqlLambda(d.Target, depth + 1, seen) is { } inner)
                {
                    return inner;
                }
            }
            else if (v is not null && v.GetType().Name.Contains("DisplayClass", StringComparison.Ordinal))
            {
                if (FindSqlLambda(v, depth + 1, seen) is { } inner)
                {
                    return inner;
                }
            }
        }

        return null;
    }

    // Compiler-generated lambdas are named "<EnclosingMethod>b__N_M"; report the enclosing method.
    private static string Enclosing(MethodInfo m)
    {
        var name = m.Name;
        var open = name.IndexOf('<', StringComparison.Ordinal);
        var close = name.IndexOf('>', StringComparison.Ordinal);
        var method = open >= 0 && close > open ? name[(open + 1)..close] : name;
        var type = m.DeclaringType;
        while (type?.DeclaringType is not null && type.Name.StartsWith('<'))
        {
            type = type.DeclaringType;
        }

        return $"{type?.Name}.{method}";
    }

    private static string StripPrefix(string s, string prefix) =>
        s.StartsWith(prefix, StringComparison.Ordinal) ? s[prefix.Length..] : s.TrimStart('/');

    private sealed record Spec(
        List<string> Program,
        List<string>? Outputs,
        List<InputBinding> Inputs,
        List<OutputBinding> Output_Bindings);

    private sealed record InputBinding(string Table, string Uri, string Mode);

    private sealed record OutputBinding(string View, string Uri, string Mode);
}
