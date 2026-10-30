// Where the parallel ingest push spends its time: 10 daily_market-shaped Arrow batches of
// 128k rows pushed into a W-worker program, all into one tick (pending deltas merge) vs a
// step after each push. Gated on INGEST_PROBE=1.
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class IngestPushProbe
{
    private static readonly string[] Program =
    [
        "CREATE TABLE dm (dm_date DATE NOT NULL, dm_s_symb VARCHAR NOT NULL, dm_close DECIMAL(8, 2), dm_high DECIMAL(8, 2), dm_low DECIMAL(8, 2), dm_vol INT)",
        "CREATE VIEW v AS SELECT dm_s_symb, COUNT(*) AS n FROM dm GROUP BY dm_s_symb",
    ];

    private readonly ITestOutputHelper _out;

    public IngestPushProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void Probe()
    {
        if (Environment.GetEnvironmentVariable("INGEST_PROBE") != "1")
        {
            return;
        }

        var outputs = new HashSet<string>(["v"], StringComparer.Ordinal);
        var schema = SqlProgram.Compile(Program, outputs).Table("dm").Schema;
        var batches = Enumerable.Range(0, 10).Select(i => Batch(schema, 128_000, i)).ToList();
        foreach (var w in new[] { 1, 8 })
        {
            foreach (var stepEach in new[] { false, true })
            {
                for (var rep = 0; rep < 3; rep++)
                {
                    Assert.True(SqlProgram.TryCompileParallel(Program, outputs, w, out var p));
                    using (p)
                    {
                        long push = 0;
                        var sw = new Stopwatch();
                        foreach (var (b, wts) in batches)
                        {
                            sw.Restart();
                            p!.Table("dm").PushArrow(b, wts);
                            push += sw.ElapsedTicks;
                            if (stepEach)
                            {
                                p.Step();
                            }
                        }

                        if (!stepEach)
                        {
                            p!.Step();
                        }

                        _out.WriteLine(string.Create(
                            CultureInfo.InvariantCulture,
                            $"W={w} stepEach={stepEach} rep={rep} push_ms={push * 1000.0 / Stopwatch.Frequency:F0}"));
                    }
                }
            }
        }
    }

    private static (RecordBatch, long[]) Batch(DbspNet.Sql.Plan.Schema schema, int rows, int seed)
    {
        var rng = new Random(seed);
        var date = new Date32Array.Builder();
        var symb = new StringArray.Builder();
        var t = new Decimal128Type(8, 2);
        var close = new Decimal128Array.Builder(t);
        var high = new Decimal128Array.Builder(t);
        var low = new Decimal128Array.Builder(t);
        var vol = new Int32Array.Builder();
        for (var i = 0; i < rows; i++)
        {
            date.Append(new DateTime(2020, 1, 1).AddDays(rng.Next(1500)));
            symb.Append("SYM" + rng.Next(5000));
            close.Append(rng.Next(100000) / 100m);
            high.Append(rng.Next(100000) / 100m);
            low.Append(rng.Next(100000) / 100m);
            vol.Append(rng.Next(1_000_000));
        }

        var batch = new RecordBatch(
            ArrowSchemaBridge.ToArrow(schema),
            new IArrowArray[] { date.Build(), symb.Build(), close.Build(), high.Build(), low.Build(), vol.Build() },
            rows);
        return (batch, Enumerable.Repeat(1L, rows).ToArray());
    }
}
