// Scratch (§5.36): how much of a giant left join's working memory is its left delta group, materialized?
// Extended TPC-DS tick 18 lands 7.2M left rows of one key on each of three workers (ops 1683-1703); the
// concurrent peak of those joins runs the restored tick out of a 48 GiB heap. A: the live size of the
// delta's giant group once resolved into a managed Z-set (what TouchedKeys hands the join). B: the peak
// live during a compiled left join's step over such a delta, sampled by a thread forcing full GCs.
// XT_MICRO=1; XT_MICRO_ROWS (default 2M). Run with DBSPNET_SPILL=on.
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class GiantJoinPeakProbe(ITestOutputHelper output)
{
    private const string FactDdl =
        "CREATE TABLE f (k INT NOT NULL, id BIGINT NOT NULL, a VARCHAR, b VARCHAR, c INT, d DOUBLE, e VARCHAR, g BIGINT, h VARCHAR, i INT)";

    private const string DimDdl = "CREATE TABLE dm (k INT NOT NULL, y BIGINT NOT NULL)";

    private static object?[] FactRow(int i) =>
        [1, (long)i, "household/" + (i % 50_000).ToString(CultureInfo.InvariantCulture), i % 3 == 0 ? null : "store",
         i % 1000, i * 0.5, "platform-" + (i % 7).ToString(CultureInfo.InvariantCulture), (long)(i % 99_999), "path/" + (i % 777).ToString(CultureInfo.InvariantCulture), i % 13];

    private static long Live()
    {
        GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
        return GC.GetTotalMemory(false);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private long MeasureGroup(ITraceBlockFormat format, int n)
    {
        long groupBytes;
            var b = DeltaSpillGroups.For<StructuralRow, StructuralRow, Z64>(format);
            for (var i = 0; i < n; i++)
            {
                var row = new StructuralRow(DbspNet.Tests.Sql.SqlTestHelpers.EncodeStrings(FactRow(i)));
                b.Add(new StructuralRow([1]), row, new Z64(1));
            }

            var delta = b.Build();
            output.WriteLine($"A: delta external={delta.External?.GetType().Name} groups={delta.GroupCount}");
            var before = Live();
            ZSet<StructuralRow, Z64>? held = null;
            foreach (var (_, group) in delta)
            {
                held = group;
            }

            var after = Live();
            groupBytes = after - before;
            output.WriteLine(FormattableString.Invariant(
                $"A: materialized group rows={held!.Count} live={groupBytes / (double)(1L << 30):F2} GiB ({groupBytes / (double)n:F0} B/row)"));
            GC.KeepAlive(held);
            GC.KeepAlive(delta);
                return groupBytes;
    }

    [Fact]
    public void Measure()
    {
        if (Environment.GetEnvironmentVariable("XT_MICRO") is not "1")
        {
            output.WriteLine("XT_MICRO not set");
            return;
        }

        _ = ArrowSqlSnapshotCodecs.Instance; // load Persistence: its block formats
        var n = int.Parse(Environment.GetEnvironmentVariable("XT_MICRO_ROWS") ?? "2000000", CultureInfo.InvariantCulture);
        var spillRows = n / 8;

        var statements = new[] { FactDdl, DimDdl,
            "CREATE VIEW j AS SELECT f.k, f.id, f.a, f.b, f.c, f.d, f.e, f.g, f.h, f.i, dm.y FROM f LEFT JOIN dm ON f.k = dm.k" };
        Assert.True(SqlProgram.TryCompileParallel(statements, new HashSet<string> { "j" }, 2, out var compiled,
            snapshotCodecs: ArrowSqlSnapshotCodecs.Instance, numericStringCoercion: true, nullCollation: DbspNet.Sql.TypeSystem.NullCollation.Low));
        using var q = compiled!;
        using var spill = SpillSettings.UseStepSpill(spillRows, 4096);
        q.Table("dm").Insert(1, 7L);
        q.Step();

        // A: the left delta as the join receives it (the fact rows indexed by k, block backed), and its one
        // group resolved, as TouchedKeys enumerates it.
        var schema = q.Table("f").Schema;
        var format = TraceBlockFormats.Create(
            new Schema([schema[0]]), schema) ?? throw new InvalidOperationException("no format");
        var groupBytes = MeasureGroup(format, n);

        // B: the compiled join's step over the same rows, peak live sampled.
        var empty = Live();
        q.Table("f").Push(Enumerable.Range(0, n).Select(i => (FactRow(i), 1L)));

        var baseline = Live();
        long peak = baseline;
        var peakAt = 0.0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var timeline = new List<string>();
        DbspNet.Core.Circuit.RootCircuit.AfterOperatorStep = (c, i) => { lock (timeline) timeline.Add($"{sw.Elapsed.TotalSeconds:F1}s   op {i} {c.Operators[i].GetType().Name} done (thread {Environment.CurrentManagedThreadId})"); };
        using var stop = new CancellationTokenSource();
        var sampler = new Thread(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                var live = Live();
                lock (timeline) timeline.Add($"{sw.Elapsed.TotalSeconds:F1}s live={live / (double)(1L << 30):F2}");
                if (live > peak)
                {
                    peak = live;
                    peakAt = sw.Elapsed.TotalSeconds;
                }

                Thread.Sleep(100);
            }
        }) { IsBackground = true };
        sampler.Start();
        q.Step();
        var stepS = sw.Elapsed.TotalSeconds;
        stop.Cancel();
        sampler.Join();
        DbspNet.Core.Circuit.RootCircuit.AfterOperatorStep = null;
        var end = Live();
        foreach (var line in timeline) output.WriteLine("T " + line);
        output.WriteLine(FormattableString.Invariant(
            $"B: step {stepS:F1}s out=? baseline={baseline / (double)(1L << 30):F2} GiB peak={peak / (double)(1L << 30):F2} GiB at {peakAt:F1}s end={end / (double)(1L << 30):F2} GiB"));
        output.WriteLine(FormattableString.Invariant(
            $"B: before inserts={empty / (double)(1L << 30):F2} GiB; input held={(baseline - empty) / (double)(1L << 30):F2} GiB; peak over empty={(peak - empty) / (double)(1L << 30):F2} GiB; retained after={(end - empty) / (double)(1L << 30):F2} GiB"));
        output.WriteLine(FormattableString.Invariant(
            $"B: rise to peak={(peak - baseline) / (double)(1L << 30):F2} GiB; materialized group (A) = {groupBytes / (double)(1L << 30):F2} GiB = {100.0 * groupBytes / Math.Max(1, peak - baseline):F0}% of it"));
    }
}
