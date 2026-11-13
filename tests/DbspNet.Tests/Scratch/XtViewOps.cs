// One view's operators in a deployed program's parallel compile: type, stream wiring and delegates
// (docs/design-bounded-memory.md §5.55). Gated on XT_PROGRAM_JSON (an xt run's work/program.json) and
// XT_VIEW; a no-op otherwise. XT_WORKERS (default 12).
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class XtViewOps
{
    private readonly ITestOutputHelper _out;

    public XtViewOps(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Describe()
    {
        var path = Environment.GetEnvironmentVariable("XT_PROGRAM_JSON");
        var view = Environment.GetEnvironmentVariable("XT_VIEW");
        if (string.IsNullOrEmpty(path) || string.IsNullOrEmpty(view))
        {
            _out.WriteLine("XT_PROGRAM_JSON / XT_VIEW not set — skipping.");
            return;
        }

        var workers = int.Parse(Environment.GetEnvironmentVariable("XT_WORKERS") ?? "12", CultureInfo.InvariantCulture);
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var program = doc.RootElement.GetProperty("program").EnumerateArray().Select(e => e.GetString()!).ToList();
        var all = view == "*";
        var outputs = all
            ? doc.RootElement.GetProperty("outputs").EnumerateArray().Select(e => e.GetProperty("view").GetString()!).ToHashSet(StringComparer.Ordinal)
            : new HashSet<string>([view], StringComparer.Ordinal);
        Assert.True(SqlProgram.TryCompileParallel(program, outputs, workers, out var p, numericStringCoercion: true, nullCollation: NullCollation.Low));
        using (p)
        {
            var c = p!.Circuit.Replicas[0];
            var producer = new Dictionary<object, int>(ReferenceEqualityComparer.Instance);
            for (var k = 0; k < c.Operators.Count; k++)
            {
                var o = c.Operators[k];
                var ins = new List<string>();
                var outs = new List<string>();
                var dels = new List<string>();
                foreach (var f in o.GetType().GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public))
                {
                    var v = f.GetValue(o);
                    if (v is null)
                    {
                        continue;
                    }

                    if (v.GetType().Name.StartsWith("Stream", StringComparison.Ordinal))
                    {
                        if (f.Name.Contains("output", StringComparison.OrdinalIgnoreCase) || f.Name.Contains("out", StringComparison.Ordinal))
                        {
                            producer.TryAdd(v, k);
                            outs.Add(f.Name);
                        }
                        else
                        {
                            ins.Add($"{f.Name}<-" + (producer.TryGetValue(v, out var src) ? src.ToString(CultureInfo.InvariantCulture) : $"s{RuntimeHelpers.GetHashCode(v):x}"));
                        }
                    }
                    else if (v is Delegate d)
                    {
                        dels.Add($"{f.Name}={d.Method.DeclaringType?.Name}.{d.Method.Name}");
                    }
                }

                var label = c.OperatorLabel(k) ?? "-";
                if (all || label == view)
                {
                    _out.WriteLine($"OP {k} {o.GetType().Name.Split('`')[0]} view={label} in[{string.Join(' ', ins)}] {string.Join(' ', dels)}");
                }
            }
        }
    }
}
