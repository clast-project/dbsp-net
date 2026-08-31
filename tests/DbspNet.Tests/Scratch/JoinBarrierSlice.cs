// Reclaimability microbench for the columnar "one barrier op + neighbours" first
// increment (design-columnar-batch1.md §7.2). Answers the question a pure-ApplyOp
// microbench cannot: across a REAL stateful barrier, what fraction of the slice's
// allocation is CAPTURABLE by a columnar output interface (the combine / output-row
// materialisation) vs UNCAPTURABLE internal state (the trace integration of the
// streamed side, identical in both representations)?
//
// Models the `watches_history` slice — the cleanest validation target: a
// projection -> INNER JOIN(securities) -> projection chain, string-heavy (zero
// boxing, the §7.1 best case), ~900K probe rows, tiny build side (securities,
// ~3.6K symbols). Reuses the REAL join kernel (IncrementalJoinCore.JoinInto) and
// the REAL trace (IndexedZSetTrace) so the matching + integration costs are exact;
// the ONLY thing that differs between the two measured paths is the output sink:
//
//   ROW  join combine -> StructuralRow into ZSetBuilder ; downstream projection
//        StructuralRow -> StructuralRow   (the current inter-op interface)
//   COL  join combine -> append to per-column object?[] arrays ; downstream
//        projection col -> col ; materialise StructuralRow ONCE at the view boundary
//
// The trace-integrate allocation is measured on its own so the report can state
// the realistic op-slice capturable %, not the idealised pure-projection ceiling.
//
// Allocation is deterministic (GC.GetAllocatedBytesForCurrentThread exact); one
// measured pass after a warmup. Gated on IVM_MICRO=1 (no-op in CI).
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class JoinBarrierSlice
{
    private readonly ITestOutputHelper _out;

    public JoinBarrierSlice(ITestOutputHelper output) => _out = output;

    private const int LeftRows = 900_000;   // brokerage_watch_history probe side
    private const int Symbols = 3626;       // securities build side (join key cardinality)
    private const int Ticks = 20;           // bulk load is chunked into ~20 engine ticks

    // watches_history final row: s1.*(customer_id, symbol, watch_ts, action_type) +
    // securities(company_id, company_name, exchange_id, security_status) = 8 cols.
    private const int OutWidth = 8;

    [Fact]
    public void Measure()
    {
        if (Environment.GetEnvironmentVariable("IVM_MICRO") is not ("1" or "true" or "TRUE"))
        {
            _out.WriteLine("IVM_MICRO not set — skipping join-barrier-slice microbench.");
            return;
        }

        var leftDeltas = BuildLeftDeltas();
        var right = BuildRight();

        // Warm up both full paths + the trace-integrate probe once.
        RunRow(leftDeltas, right);
        RunCol(leftDeltas, right);
        RunTraceOnly(leftDeltas, right);

        RunColPooled(leftDeltas, right);

        var row = MeasureAlloc(() => RunRow(leftDeltas, right));
        var col = MeasureAlloc(() => RunCol(leftDeltas, right));
        var colPooled = MeasureAlloc(() => RunColPooled(leftDeltas, right));
        var traceOnly = MeasureAlloc(() => RunTraceOnly(leftDeltas, right));

        // traceOnly = the left-trace integration (dl folded into the trace each
        // tick) + the join MATCH enumeration with a no-op sink. This is the
        // representation-INVARIANT floor — present identically in ROW and COL.
        var rowInterface = row - traceOnly;   // combine StructuralRows + builder + downstream projection
        var colInterface = col - traceOnly;   // column appends + one boundary materialise

        double Gib(long b) => b / (1024.0 * 1024.0 * 1024.0);
        double PerLeft(long b) => (double)b / LeftRows;

        _out.WriteLine($"left_rows={LeftRows}  symbols={Symbols}  ticks={Ticks}  out_width={OutWidth}");
        _out.WriteLine("");
        _out.WriteLine($"ROW  full slice (join+combine+builder + projection)   {Gib(row):F3} GiB   {PerLeft(row),6:F1} B/left-row");
        _out.WriteLine($"COL  full slice (columnar join out + col proj + 1 mat) {Gib(col):F3} GiB   {PerLeft(col),6:F1} B/left-row");
        _out.WriteLine($"     trace-integrate + match (invariant floor)         {Gib(traceOnly):F3} GiB   {PerLeft(traceOnly),6:F1} B/left-row");
        _out.WriteLine("");
        _out.WriteLine($"     ROW interface term (row - floor)   {Gib(rowInterface):F3} GiB   {100.0 * rowInterface / row,5:F1}% of ROW slice   <- capturable surface");
        _out.WriteLine($"     COL interface term (col - floor)   {Gib(colInterface):F3} GiB   {100.0 * colInterface / row,5:F1}% of ROW slice");
        _out.WriteLine($"     uncapturable floor (both)          {Gib(traceOnly):F3} GiB   {100.0 * traceOnly / row,5:F1}% of ROW slice");
        _out.WriteLine("");
        _out.WriteLine($"  interface-term saving (col vs row):   {100.0 * (rowInterface - colInterface) / Math.Max(1, rowInterface),5:F1}%  (pure-projection §7.1 ceiling was -47.5%)");
        _out.WriteLine($"  WHOLE-SLICE saving (col vs row):      {100.0 * (row - col) / row,5:F1}%   ({Gib(row - col):F3} GiB of the slice)");
        _out.WriteLine($"  WHOLE-SLICE saving (col POOLED cols): {100.0 * (row - colPooled) / row,5:F1}%   ({Gib(row - colPooled):F3} GiB) — join-output column buffers reused across ticks (§20)");
        _out.WriteLine("");
        _out.WriteLine("  Realistic single-slice batch impact: this slice is ~1.28 GiB of the 44.7 GiB batch");
        _out.WriteLine($"  → columnarising it removes ~{Gib(row - col):F2} GiB ≈ {100.0 * Gib(row - col) / 44.7,4:F2}% of the batch.");

        Assert.True(row > 0 && col > 0);
    }

    private static long MeasureAlloc(Func<long> body)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        var sink = body();
        var after = GC.GetAllocatedBytesForCurrentThread();
        GC.KeepAlive(sink);
        return after - before;
    }

    // ---- ROW path: current inter-op interface (StructuralRow throughout) ----
    private static long RunRow(List<IndexedZSet<string, StructuralRow, Z64>> leftDeltas, IndexedZSet<string, StructuralRow, Z64> right)
    {
        var leftTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        var rightTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        rightTrace.Integrate(right);                 // build side loaded once, tick 0
        long sink = 0;

        foreach (var dl in leftDeltas)
        {
            // dl ⋈ R_t  (R constant after tick 0; L_{t-1} ⋈ dr term is 0 — bulk insert)
            var builder = new ZSetBuilder<StructuralRow, Z64>(dl.GroupCount == 0 ? 0 : EstimateOut(dl));
            IncrementalJoinCore.JoinInto(dl, rightTrace.Current, CombineRow, null, builder);
            var joined = builder.Build();

            // downstream projection op289: StructuralRow -> StructuralRow (passthrough reshape)
            var proj = new ZSetBuilder<StructuralRow, Z64>(joined.Count);
            foreach (var (r, w) in joined)
            {
                var cols = new object?[OutWidth];
                for (var k = 0; k < OutWidth; k++)
                {
                    cols[k] = r[k];
                }

                proj.Add(new StructuralRow(cols), w);
            }

            var outZ = proj.Build();
            sink += outZ.Count;
            leftTrace.Integrate(dl);
        }

        return sink;
    }

    // ---- COL path: columnar join output + columnar projection + 1 boundary materialise ----
    private static long RunCol(List<IndexedZSet<string, StructuralRow, Z64>> leftDeltas, IndexedZSet<string, StructuralRow, Z64> right)
    {
        var leftTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        var rightTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        rightTrace.Integrate(right);
        long sink = 0;

        foreach (var dl in leftDeltas)
        {
            var cap = EstimateOut(dl);
            // columnar join output: per-column object?[] + weight[] (adaptive-presized to the tick)
            var cols = new object?[OutWidth][];
            for (var c = 0; c < OutWidth; c++)
            {
                cols[c] = new object?[cap];
            }

            var weights = new long[cap];
            var n = 0;

            // Same match structure as JoinInto, but the sink appends columns
            // (no StructuralRow, no per-row hash, no dict). Faithful to a columnar
            // combine emitting into a ColumnBatch.
            var r = rightTrace.Current;
            foreach (var (key, lGroup) in dl)
            {
                var rGroup = r.GroupFor(key);
                if (rGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (lv, lw) in lGroup)
                {
                    foreach (var (rv, rw) in rGroup)
                    {
                        if (n == cols[0].Length)
                        {
                            Grow(cols, ref weights);
                        }

                        // combine into columns (customer_id, symbol, watch_ts, action_type | company_id, company_name, exchange_id, status)
                        cols[0][n] = lv[0];
                        cols[1][n] = lv[1];
                        cols[2][n] = lv[2];
                        cols[3][n] = lv[3];
                        cols[4][n] = rv[1];
                        cols[5][n] = rv[2];
                        cols[6][n] = rv[3];
                        cols[7][n] = rv[4 % rv.Count];
                        weights[n] = Z64.Multiply(lw, rw).Value;
                        n++;
                    }
                }
            }

            // downstream projection op289 FUSED into the boundary materialise: read
            // the join-output columns and build the projected StructuralRow in ONE
            // pass (no intermediate column copy — a real columnar pipeline fuses the
            // passthrough projection into the single materialisation the boundary
            // needs). This is the honest best case for the columnar slice.
            var outZ = new ZSetBuilder<StructuralRow, Z64>(n);
            for (var i = 0; i < n; i++)
            {
                var rowCols = new object?[OutWidth];
                for (var c = 0; c < OutWidth; c++)
                {
                    rowCols[c] = cols[c][i];
                }

                outZ.Add(new StructuralRow(rowCols), new Z64(weights[i]));
            }

            sink += outZ.Build().Count;
            leftTrace.Integrate(dl);
        }

        return sink;
    }

    // ---- COL path with POOLED join-output column buffers (reused across ticks, §20) ----
    // Upper bound: the join-output columns cost nothing after tick 0 (reused); COL
    // then pays only the invariant floor + the one boundary materialise.
    private static long RunColPooled(List<IndexedZSet<string, StructuralRow, Z64>> leftDeltas, IndexedZSet<string, StructuralRow, Z64> right)
    {
        var leftTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        var rightTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        rightTrace.Integrate(right);
        long sink = 0;

        // Pooled column + weight buffers, grown on demand, reused every tick.
        var cols = new object?[OutWidth][];
        var cap = 0;
        long[] weights = System.Array.Empty<long>();

        foreach (var dl in leftDeltas)
        {
            var need = EstimateOut(dl);
            if (need > cap)
            {
                cap = need;
                for (var c = 0; c < OutWidth; c++)
                {
                    cols[c] = new object?[cap];
                }

                weights = new long[cap];
            }

            var n = 0;
            var r = rightTrace.Current;
            foreach (var (key, lGroup) in dl)
            {
                var rGroup = r.GroupFor(key);
                if (rGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (lv, lw) in lGroup)
                {
                    foreach (var (rv, rw) in rGroup)
                    {
                        cols[0][n] = lv[0];
                        cols[1][n] = lv[1];
                        cols[2][n] = lv[2];
                        cols[3][n] = lv[3];
                        cols[4][n] = rv[1];
                        cols[5][n] = rv[2];
                        cols[6][n] = rv[3];
                        cols[7][n] = rv[4 % rv.Count];
                        weights[n] = Z64.Multiply(lw, rw).Value;
                        n++;
                    }
                }
            }

            var outZ = new ZSetBuilder<StructuralRow, Z64>(n);
            for (var i = 0; i < n; i++)
            {
                var rowCols = new object?[OutWidth];
                for (var c = 0; c < OutWidth; c++)
                {
                    rowCols[c] = cols[c][i];
                }

                outZ.Add(new StructuralRow(rowCols), new Z64(weights[i]));
            }

            sink += outZ.Build().Count;
            leftTrace.Integrate(dl);
        }

        return sink;
    }

    // ---- trace-integrate + match ONLY (the representation-invariant floor) ----
    private static long RunTraceOnly(List<IndexedZSet<string, StructuralRow, Z64>> leftDeltas, IndexedZSet<string, StructuralRow, Z64> right)
    {
        var leftTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        var rightTrace = new IndexedZSetTrace<string, StructuralRow, Z64>();
        rightTrace.Integrate(right);
        long sink = 0;

        foreach (var dl in leftDeltas)
        {
            // same match enumeration, no-op sink (count matches only)
            var r = rightTrace.Current;
            foreach (var (key, lGroup) in dl)
            {
                var rGroup = r.GroupFor(key);
                if (rGroup.IsEmpty)
                {
                    continue;
                }

                foreach (var (lv, lw) in lGroup)
                {
                    foreach (var (rv, rw) in rGroup)
                    {
                        sink += lv.Count + rv.Count;
                    }
                }
            }

            leftTrace.Integrate(dl);
        }

        return sink;
    }

    private static StructuralRow CombineRow(string key, StructuralRow l, StructuralRow r)
    {
        var cols = new object?[OutWidth];
        cols[0] = l[0];
        cols[1] = l[1];
        cols[2] = l[2];
        cols[3] = l[3];
        cols[4] = r[1];
        cols[5] = r[2];
        cols[6] = r[3];
        cols[7] = r[4 % r.Count];
        return new StructuralRow(cols);
    }

    private static int EstimateOut(IndexedZSet<string, StructuralRow, Z64> dl)
    {
        // each left row matches ~1 security → output ≈ input rows this tick
        var rows = 0;
        foreach (var (_, g) in dl)
        {
            rows += g.Count;
        }

        return Math.Max(4, rows);
    }

    private static void Grow(object?[][] cols, ref long[] weights)
    {
        for (var c = 0; c < cols.Length; c++)
        {
            var bigger = new object?[cols[c].Length * 2];
            Array.Copy(cols[c], bigger, cols[c].Length);
            cols[c] = bigger;
        }

        var w = new long[weights.Length * 2];
        Array.Copy(weights, w, weights.Length);
        weights = w;
    }

    // ---- input construction ----
    private static List<IndexedZSet<string, StructuralRow, Z64>> BuildLeftDeltas()
    {
        var deltas = new List<IndexedZSet<string, StructuralRow, Z64>>(Ticks);
        var baseDate = new DateTime(2020, 1, 1);
        var actions = new[] { "ACTV", "CNCL", "ACTV" };
        var perTick = LeftRows / Ticks;
        var symbols = BuildSymbols();
        var r = 0;
        for (var t = 0; t < Ticks; t++)
        {
            var b = new IndexedZSetBuilder<string, StructuralRow, Z64>();
            var count = t == Ticks - 1 ? LeftRows - r : perTick;
            for (var i = 0; i < count; i++, r++)
            {
                var sym = symbols[r % Symbols];
                var cols = new object?[4];
                cols[0] = "CID" + (r % 50000);        // customer_id
                cols[1] = sym;                         // symbol
                cols[2] = baseDate.AddMinutes(r);      // watch_timestamp
                cols[3] = actions[r % 3];              // action_type
                b.Add(sym, new StructuralRow(cols), new Z64(1));
            }

            deltas.Add(b.Build());
        }

        return deltas;
    }

    private static IndexedZSet<string, StructuralRow, Z64> BuildRight()
    {
        var b = new IndexedZSetBuilder<string, StructuralRow, Z64>();
        var symbols = BuildSymbols();
        foreach (var sym in symbols)
        {
            // securities: symbol, company_id, company_name, exchange_id, status
            var cols = new object?[]
            {
                sym, "CO" + sym, "Company " + sym, sym.GetHashCode() % 2 == 0 ? "NYSE" : "NASDAQ", "ACTV",
            };
            b.Add(sym, new StructuralRow(cols), new Z64(1));
        }

        return b.Build();
    }

    private static string[] BuildSymbols()
    {
        var s = new string[Symbols];
        for (var i = 0; i < Symbols; i++)
        {
            s[i] = "SYM" + i;
        }

        return s;
    }
}
