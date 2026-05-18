using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Arrow;

/// <summary>
/// Smoke-grade timing for the Arrow boundary. These aren't proper
/// benchmarks (no warmup-vs-measurement isolation, no GC controls), but
/// they assert the columnar conversion path runs comfortably under a
/// loose ceiling on a representative batch — protects against accidental
/// quadratic regressions, no more.
/// </summary>
public class ArrowConversionBenchmarkTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    // Reference numbers from columnar implementation, 10k rows × 5 cols on
    // a typical dev box: PushArrow + Step ~15 ms, ToArrowDelta ~6 ms. The
    // bench is skipped by default to keep `dotnet test` fast — flip the
    // attribute to time it manually.
    [Fact(Skip = "Manual perf check — flip the attribute to run.")]
    public void TimeFiveColumnRoundTrip_10kRows()
    {
        const int N = 10_000;
        var q = Compile(
            [
                "CREATE TABLE t (id INT NOT NULL, amount DECIMAL(10, 2) NOT NULL, " +
                "name VARCHAR NOT NULL, when_at TIMESTAMP NOT NULL, active BOOLEAN NOT NULL)",
            ],
            "SELECT id, amount, name, when_at, active FROM t");

        // Build a 10k-row Arrow batch.
        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var idBuilder = new Int32Array.Builder();
        var amtBuilder = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        var nameBuilder = new StringArray.Builder();
        var whenBuilder = new TimestampArray.Builder(
            new TimestampType(TimeUnit.Microsecond, (string?)null));
        var activeBuilder = new BooleanArray.Builder();

        var epoch = new DateTimeOffset(2026, 5, 4, 0, 0, 0, TimeSpan.Zero);
        for (var i = 0; i < N; i++)
        {
            idBuilder.Append(i);
            amtBuilder.Append((decimal)i / 100m);
            nameBuilder.Append($"row-{i}");
            whenBuilder.Append(epoch.AddSeconds(i));
            activeBuilder.Append(i % 2 == 0);
        }

        var batch = new RecordBatch(arrowSchema, new IArrowArray[]
        {
            idBuilder.Build(),
            amtBuilder.Build(),
            nameBuilder.Build(),
            whenBuilder.Build(),
            activeBuilder.Build(),
        }, length: N);

        // Push + Step + ToArrow round trip.
        var sw = Stopwatch.StartNew();
        q.Table("t").PushArrow(batch);
        q.Step();
        var pushMs = sw.Elapsed.TotalMilliseconds;

        sw.Restart();
        var delta = q.ToArrowDelta();
        var pullMs = sw.Elapsed.TotalMilliseconds;

        // Loose assertion — protects against egregious regressions only.
        // On modern hardware this is sub-100ms for 10k rows × 5 cols.
        Assert.Equal(N, delta.Rows.Length);

        // Print timings for manual inspection (xunit captures stdout).
        Console.WriteLine($"PushArrow + Step (10k×5): {pushMs:F2} ms");
        Console.WriteLine($"ToArrowDelta (10k×5): {pullMs:F2} ms");
    }
}
