// Apportions ONE representative projection ApplyOp's per-row allocation into the
// four terms the columnar/row-rep question turns on, by reproducing the exact
// LinearOperators.MapFilterRows inner loop and differencing variants:
//
//   E  enumerate-only            → input-enumeration alloc
//   F  filter (keep all, reuse)  → output container + dict entries         = a
//   I  identity copy to fresh    → + per-row object[] + StructuralRow       (I-F = b)
//   P  project + compute one col → + boxing of computed values             (P-I = c)
//
//   a (container)          = F - E
//   b (row materialization)= I - F
//   c (compute / boxing)   = P - I
//
// TWO scenarios (design-columnar-batch1.md §7 #1 — confirm the single-view ceiling
// on REAL data, not just the synthetic numeric shape):
//   NUMERIC  14→12 col, boxed longs/decimals/DateTimes + one computed long
//            (daily_market / trades_history SCD shape) — the original microbench.
//   WATCHES  8→10 col, strings + DateTimes, two CASE-passthrough computed cols
//            (`case action_type when 'Activate' then watch_timestamp else null`)
//            that reuse an already-boxed reference → ~zero (c) boxing. This is the
//            real `watches` s1 apply-chain (ops 290/291, ~1.7 GiB of the batch's
//            ApplyOp alloc) and the (b)-dominated best case for object-array columnar.
//
// Allocation is deterministic (GC.GetAllocatedBytesForCurrentThread is exact), so
// one measured pass after a warmup pass is enough — no median-of-runs needed.
// Gated on IVM_MICRO=1 so it is a no-op in CI.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class ApplyOpAllocSplit
{
    private readonly ITestOutputHelper _out;

    public ApplyOpAllocSplit(ITestOutputHelper output) => _out = output;

    private const int N = 200_000;

    [Fact]
    public void Split()
    {
        if (Environment.GetEnvironmentVariable("IVM_MICRO") is not ("1" or "true" or "TRUE"))
        {
            _out.WriteLine("IVM_MICRO not set — skipping ApplyOp alloc-split microbench.");
            return;
        }

        RunScenario(
            "NUMERIC (14->12, boxed long/decimal/DateTime + 1 computed long)",
            inWidth: 14, outWidth: 12,
            build: BuildNumeric,
            projectRow: ProjectNumericRow,
            projectCol: ProjectNumericCol);

        RunScenario(
            "WATCHES  (8->10, string/DateTime + 2 CASE-passthrough cols, ~0 boxing)",
            inWidth: 8, outWidth: 10,
            build: BuildWatches,
            projectRow: ProjectWatchesRow,
            projectCol: ProjectWatchesCol);
    }

    private void RunScenario(
        string title, int inWidth, int outWidth,
        Func<int, ZSet<StructuralRow, Z64>> build,
        Func<ZSet<StructuralRow, Z64>, int, int> projectRow,
        Func<ZSet<StructuralRow, Z64>, int, long> projectCol)
    {
        var input = build(N);

        // Warm up every variant once (JIT + first-use excluded from measured pass).
        _ = Enumerate(input);
        _ = Filter(input, outWidth);
        _ = IdentityCopy(input, outWidth);
        _ = projectRow(input, outWidth);
        _ = projectCol(input, outWidth);

        var e = Measure(() => Enumerate(input));
        var f = Measure(() => Filter(input, outWidth));
        var i = Measure(() => IdentityCopy(input, outWidth));
        var p = Measure(() => projectRow(input, outWidth));
        var col = Measure(() => projectCol(input, outWidth));

        double PerRow(long total) => (double)total / N;

        var a = f - e;                 // container + entries
        var b = i - f;                 // row object[] + StructuralRow
        var c = p - i;                 // compute / boxing

        _out.WriteLine("");
        _out.WriteLine($"===== {title} =====");
        _out.WriteLine($"rows={N}  in_width={inWidth}  out_width={outWidth}");
        _out.WriteLine($"E  enumerate-only      {PerRow(e),8:F1} B/row   ({e / (1024.0 * 1024.0):F1} MiB)");
        _out.WriteLine($"F  filter (reuse row)  {PerRow(f),8:F1} B/row   ({f / (1024.0 * 1024.0):F1} MiB)");
        _out.WriteLine($"I  identity copy       {PerRow(i),8:F1} B/row   ({i / (1024.0 * 1024.0):F1} MiB)");
        _out.WriteLine($"P  project + compute   {PerRow(p),8:F1} B/row   ({p / (1024.0 * 1024.0):F1} MiB)");
        _out.WriteLine("--- apportioned (of P, the full projection ApplyOp) ---");
        _out.WriteLine($"(a) output container + entries  {PerRow(a),8:F1} B/row   {100.0 * a / p,5:F1}%   <- columnar-storage captures");
        _out.WriteLine($"(b) row object[] + StructuralRow {PerRow(b),7:F1} B/row   {100.0 * b / p,5:F1}%   <- captured only if vectorized-write");
        _out.WriteLine($"(c) compute / boxing            {PerRow(c),8:F1} B/row   {100.0 * c / p,5:F1}%   <- stranded (needs expr vectorization)");
        _out.WriteLine($"    enumeration (in all)        {PerRow(e),8:F1} B/row   {100.0 * e / p,5:F1}%");
        _out.WriteLine("--- columnar (SoA) same projection, no per-row StructuralRow ---");
        _out.WriteLine($"COL columnar output    {PerRow(col),8:F1} B/row   ({col / (1024.0 * 1024.0):F1} MiB)");
        _out.WriteLine($"    row-wise P vs columnar: {100.0 * (p - col) / p,5:F1}% alloc removed by columnarizing the ApplyOp");
        _out.WriteLine($"    of total alloc (ApplyOp=47.3%): columnar ApplyOps would save ~{47.3 * (p - col) / p,4:F1}% of the batch");

        Assert.True(p > 0);
    }

    private static long Measure(Func<long> body)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        var sink = body();
        var after = GC.GetAllocatedBytesForCurrentThread();
        GC.KeepAlive(sink);
        return after - before;
    }

    // ---- shape-independent variants (mirror MapFilterRows' inner body) ----

    private static long Enumerate(ZSet<StructuralRow, Z64> z)
    {
        long sink = 0;
        foreach (var (row, w) in z)
        {
            sink += row.Count + w.Value;
        }

        return sink;
    }

    private static int Filter(ZSet<StructuralRow, Z64> z, int outWidth)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>(z.Count);
        foreach (var (row, w) in z)
        {
            // pure filter: keep, reuse the SAME row reference (no realloc/rehash)
            b.Add(row, w);
        }

        return b.Build().Count;
    }

    private static int IdentityCopy(ZSet<StructuralRow, Z64> z, int outWidth)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>(z.Count);
        foreach (var (row, w) in z)
        {
            // fresh row of outWidth columns, all copied verbatim (references, no
            // boxing) — isolates the object[] + StructuralRow + hash cost.
            var cols = new object?[outWidth];
            for (var k = 0; k < outWidth; k++)
            {
                cols[k] = row[k % row.Count];
            }

            b.Add(new StructuralRow(cols), w);
        }

        return b.Build().Count;
    }

    // ---- NUMERIC scenario (original 14->12 boxed shape) ----

    private static int ProjectNumericRow(ZSet<StructuralRow, Z64> z, int outWidth)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>(z.Count);
        foreach (var (row, w) in z)
        {
            var cols = new object?[outWidth];
            for (var k = 0; k < outWidth - 1; k++)
            {
                cols[k] = row[k];
            }

            // one computed column: arithmetic that boxes (dm_high - dm_low shape)
            // + a CASE reference-select — representative per-row compute.
            var hi = (long)row[2]!;
            var lo = (long)row[3]!;
            cols[outWidth - 1] = hi - lo;                       // boxes one long
            if (hi == lo)
            {
                cols[0] = row[4];                                // CASE branch, no alloc
            }

            b.Add(new StructuralRow(cols), w);
        }

        return b.Build().Count;
    }

    private static long ProjectNumericCol(ZSet<StructuralRow, Z64> z, int outWidth)
    {
        var n = z.Count;
        var cols = new object?[outWidth][];
        for (var c = 0; c < outWidth; c++)
        {
            cols[c] = new object?[n];
        }

        var weights = new long[n];
        var r = 0;
        foreach (var (row, w) in z)
        {
            for (var c = 0; c < outWidth - 1; c++)
            {
                cols[c][r] = row[c];
            }

            var hi = (long)row[2]!;
            var lo = (long)row[3]!;
            cols[outWidth - 1][r] = hi - lo; // still boxes the computed long (term c)
            weights[r] = w.Value;
            r++;
        }

        return cols.Length + weights.Length;
    }

    private static ZSet<StructuralRow, Z64> BuildNumeric(int n)
    {
        var builder = new ZSetBuilder<StructuralRow, Z64>(n);
        var baseDate = new DateTime(2020, 1, 1);
        for (var r = 0; r < n; r++)
        {
            var cols = new object?[14];
            cols[0] = (long)r;
            cols[1] = "SYM" + (r % 4096);                        // string col
            cols[2] = (long)(100 + (r % 500));                   // dm_high
            cols[3] = (long)(50 + (r % 400));                    // dm_low
            cols[4] = baseDate.AddDays(r % 365);                 // boxed DateTime
            cols[5] = (long)(r % 1_000_000);
            cols[6] = 12.34m + r % 7;                            // boxed decimal
            cols[7] = "status" + (r % 3);
            cols[8] = (long)(r % 97);
            cols[9] = baseDate.AddMinutes(r);
            cols[10] = (long)(r % 13);
            cols[11] = "exec_" + (r % 2000);
            cols[12] = (long)(r % 5);
            cols[13] = r % 2 == 0 ? "Cash" : "Margin";
            builder.Add(new StructuralRow(cols), new Z64(1));
        }

        return builder.Build();
    }

    // ---- WATCHES scenario (real silver/watches s1 apply-chain shape) ----
    // Input (8 cols, from watches_history join securities): customer_id(str),
    // symbol(str), watch_timestamp(DateTime), action_type(str), company_id(str),
    // company_name(str), exchange_id(str), security_status(str).
    // Output (10 cols): 8 passthrough + placed_timestamp + removed_timestamp, each
    // a CASE on action_type selecting the already-boxed watch_timestamp ref or null.
    // No arithmetic → ~zero new boxing; pure (b) row-materialization cost.
    private static int ProjectWatchesRow(ZSet<StructuralRow, Z64> z, int outWidth)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>(z.Count);
        foreach (var (row, w) in z)
        {
            var cols = new object?[10];
            for (var k = 0; k < 8; k++)
            {
                cols[k] = row[k];
            }

            var action = (string)row[3]!;
            var ts = row[2];                                     // already-boxed DateTime ref
            cols[8] = action == "Activate" ? ts : null;         // placed_timestamp (ref-select, no box)
            cols[9] = action == "Cancelled" ? ts : null;        // removed_timestamp
            b.Add(new StructuralRow(cols), w);
        }

        return b.Build().Count;
    }

    private static long ProjectWatchesCol(ZSet<StructuralRow, Z64> z, int outWidth)
    {
        var n = z.Count;
        var cols = new object?[10][];
        for (var c = 0; c < 10; c++)
        {
            cols[c] = new object?[n];
        }

        var weights = new long[n];
        var r = 0;
        foreach (var (row, w) in z)
        {
            for (var c = 0; c < 8; c++)
            {
                cols[c][r] = row[c];
            }

            var action = (string)row[3]!;
            var ts = row[2];
            cols[8][r] = action == "Activate" ? ts : null;
            cols[9][r] = action == "Cancelled" ? ts : null;
            weights[r] = w.Value;
            r++;
        }

        return cols.Length + weights.Length;
    }

    private static ZSet<StructuralRow, Z64> BuildWatches(int n)
    {
        var builder = new ZSetBuilder<StructuralRow, Z64>(n);
        var baseDate = new DateTime(2020, 1, 1);
        var actions = new[] { "Activate", "Cancelled", "Activate" };
        for (var r = 0; r < n; r++)
        {
            var cols = new object?[8];
            cols[0] = "CID" + (r % 50000);                       // customer_id
            cols[1] = "SYM" + (r % 4096);                        // symbol
            cols[2] = baseDate.AddMinutes(r);                    // watch_timestamp (boxed DateTime)
            cols[3] = actions[r % 3];                            // action_type
            cols[4] = "CO" + (r % 1500);                         // company_id
            cols[5] = "Company " + (r % 1500);                   // company_name
            cols[6] = r % 2 == 0 ? "NYSE" : "NASDAQ";            // exchange_id
            cols[7] = "ACTV";                                    // security_status
            builder.Add(new StructuralRow(cols), new Z64(1));
        }

        return builder.Build();
    }
}
