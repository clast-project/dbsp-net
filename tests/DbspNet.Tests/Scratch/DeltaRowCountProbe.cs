// Validates the connector-layer DeltaRowCounts helper against the real SF=3 Delta
// tables (gated on IVM_DATA_ROOT — no-op in CI). Confirms the transaction-log
// numRecords sum a deployment would feed to CompileOptions.RelationRowCounts.
//   IVM_DATA_ROOT=D:/ivm-data/raw/3/delta dotnet test --filter FullyQualifiedName~DeltaRowCountProbe
using DbspNet.Connectors.EngineeredWood;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class DeltaRowCountProbe
{
    private readonly ITestOutputHelper _out;

    public DeltaRowCountProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ReadsRowCountsFromDeltaMetadata()
    {
        var dataRoot = Environment.GetEnvironmentVariable("IVM_DATA_ROOT");
        if (string.IsNullOrEmpty(dataRoot))
        {
            _out.WriteLine("IVM_DATA_ROOT not set — skipping.");
            return;
        }

        // A tiny reference dimension and a large fact source.
        var tables = new (string Name, string Directory)[]
        {
            ("status_type", Path.Combine(dataRoot, "batch1", "status_type")),
            ("trade_type", Path.Combine(dataRoot, "batch1", "trade_type")),
            ("trade", Path.Combine(dataRoot, "staging", "trade")),
            ("holding_history", Path.Combine(dataRoot, "staging", "holding_history")),
        };

        var counts = await DeltaRowCounts.ReadAsync(tables);
        foreach (var (name, dir) in tables)
        {
            var text = counts.TryGetValue(name, out var n)
                ? n.ToString(System.Globalization.CultureInfo.InvariantCulture)
                : "unknown";
            _out.WriteLine($"  {name,-16} = {text}   ({dir})");
        }

        // The reference dimensions are tiny (broadcastable); the fact sources are
        // orders of magnitude larger (hash-joined). This is the exact signal the
        // production broadcast size gate keys on.
        Assert.True(counts["status_type"] < 100, "status_type should be a tiny dimension");
        Assert.True(counts["trade_type"] < 100, "trade_type should be a tiny dimension");
        Assert.True(counts["trade"] > 100_000, "staging trade should be a large fact");
        Assert.True(counts["status_type"] * 1000 < counts["trade"], "dimension ≪ fact");
    }
}
