// Which views of a deployed program the rank → TOP-K rewrite changes (docs/design-bounded-memory.md
// §5.51): ranks and TOP-Ks per view, serial compile, rewrite off and on. Gated on XT_PROGRAM_JSON (an
// xt run's work/program.json); a no-op otherwise.
using System.Text.Json;
using DbspNet.Core.Circuit;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class XtRankTopKCensus
{
    private readonly ITestOutputHelper _out;

    public XtRankTopKCensus(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Census()
    {
        var path = Environment.GetEnvironmentVariable("XT_PROGRAM_JSON");
        if (string.IsNullOrEmpty(path))
        {
            _out.WriteLine("XT_PROGRAM_JSON not set — skipping.");
            return;
        }

        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var program = doc.RootElement.GetProperty("program").EnumerateArray().Select(e => e.GetString()!).ToList();
        var outputs = doc.RootElement.GetProperty("outputs").EnumerateArray()
            .Select(e => e.GetProperty("view").GetString()!).ToHashSet(StringComparer.Ordinal);

        Dictionary<string, (int Ranks, int TopKs)> Count(bool on)
        {
            var prev = RankTopKMode.Enabled;
            RankTopKMode.Enabled = on;
            try
            {
                var p = SqlProgram.Compile(program, outputs, snapshotCodecs: null, numericStringCoercion: true, nullCollation: NullCollation.Low);
                var c = p.Circuit;
                var result = new Dictionary<string, (int, int)>(StringComparer.Ordinal);
                for (var i = 0; i < c.Operators.Count; i++)
                {
                    var name = c.Operators[i].GetType().Name;
                    var label = c.OperatorLabel(i) ?? "-";
                    var (r, t) = result.GetValueOrDefault(label);
                    if (name.StartsWith("PartitionedRankOp", StringComparison.Ordinal) || name.StartsWith("RangeRankOp", StringComparison.Ordinal))
                    {
                        r++;
                    }
                    else if (name.StartsWith("PartitionedTopK", StringComparison.Ordinal))
                    {
                        t++;
                    }

                    result[label] = (r, t);
                }

                return result;
            }
            finally
            {
                RankTopKMode.Enabled = prev;
            }
        }

        var before = Interlocked.Read(ref RankTopKMode.Rewritten);
        var off = Count(false);
        var on = Count(true);
        foreach (var view in off.Keys.Union(on.Keys).Order(StringComparer.Ordinal))
        {
            var a = off.GetValueOrDefault(view);
            var b = on.GetValueOrDefault(view);
            if (a != b)
            {
                _out.WriteLine($"RANKTOPK view={view} ranks {a.Ranks}->{b.Ranks} topks {a.TopKs}->{b.TopKs}");
            }
        }

        _out.WriteLine($"RANKTOPK total ranks {off.Values.Sum(v => v.Ranks)}->{on.Values.Sum(v => v.Ranks)} " +
                       $"topks {off.Values.Sum(v => v.TopKs)}->{on.Values.Sum(v => v.TopKs)} rewrites={Interlocked.Read(ref RankTopKMode.Rewritten) - before}");
    }
}
