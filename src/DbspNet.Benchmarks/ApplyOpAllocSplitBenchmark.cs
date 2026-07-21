// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Benchmarks;

/// <summary>
/// ApplyOp per-row allocation split — row-wise vs columnar (SoA) projection
/// (docs/design-columnar-batch1.md §1 apportionment + §2.3 the B ceiling).
///
/// <para>
/// RECONSTRUCTION NOTE. The original <c>ApplyOpAllocSplit</c> that produced the
/// doc's numbers (P = 207.5 B/row, COL = 128.0 B/row, −38.3%) was authored on
/// another machine and never committed. This file re-derives the same
/// measurement from the doc's spec, grounded on the real
/// <see cref="StructuralRow"/> (an <c>object?[]</c> row with a cached
/// <see cref="HashCode"/> hash) and the projection ApplyOp path (a fresh
/// <c>object[]</c> + <c>StructuralRow</c> built per output row into a fresh
/// <see cref="ZSetBuilder{TKey,TWeight}"/>). Absolute B/row will differ from the
/// doc (different host/GC); the portable, decision-relevant claim is the
/// <b>relative</b> P→COL reduction and its apportionment.
/// </para>
///
/// <para>
/// The workload models one hot batch-1 projection ApplyOp: a wide SCD-ish input
/// row of 14 columns (8 boxed <c>long</c> + 6 <c>string</c>, pre-boxed once in a
/// pool so passthrough columns copy a <em>reference</em>, never re-box) projected
/// to 13 output columns = 12 passthrough + 1 freshly-computed <c>long</c> (the
/// only new box per row). This is the §69 (b) sub-term made concrete.
/// </para>
///
/// <list type="number">
/// <item><c>P·fresh</c> — today's model: a fresh <c>ZSetBuilder&lt;StructuralRow,Z64&gt;</c>
///   (new <c>Dictionary</c>) per tick; per row an <c>object[13]</c>, a
///   <c>StructuralRow</c> wrapper (which computes the structural hash), and a
///   dict insert.</item>
/// <item><c>P·pooled</c> — identical per-row row build, but the output dict is
///   pooled (<c>Clear()</c> + reuse). <c>fresh − pooled</c> = (a) the fresh output
///   container + dict-entry alloc.</item>
/// <item><c>P·noWrap</c> — pooled path but the <c>StructuralRow</c> wrapper + hash
///   are dropped (the <c>object[13]</c> + boxed compute still built, parked in a
///   reused list). <c>pooled − noWrap</c> = (b′) the <c>StructuralRow</c> wrapper +
///   per-row hash share.</item>
/// <item><c>COL</c> — columnar SoA: 13 pre-sized <c>object?[]</c> column arrays +
///   one <c>long[]</c> weight array per tick (amortised over the tick's rows), no
///   per-row <c>object[]</c>, no wrapper, no hash. <c>noWrap − COL</c> = the
///   per-row <c>object[]</c> header/body itself; <c>COL</c> itself = (c) the
///   irreducible boxed-compute + amortised-column residual object columns still
///   pay.</item>
/// </list>
/// <c>P·fresh − COL</c> is the headline §2.3 ceiling: what columnarising the
/// inter-op interface with object columns removes (the wrappers, hashes, and
/// object[] headers) — but not the boxing (the computed long stays boxed in both).
/// </summary>
internal static class ApplyOpAllocSplitBenchmark
{
    private const int InCols = 14;
    private const int OutPass = 12; // passthrough columns copied by reference
    private const int OutCols = OutPass + 1; // + 1 computed long

    public static void Run(StringBuilder o, int ticks, int delta, int runs)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"=== ApplyOp alloc split: row-wise vs columnar (ticks={ticks:N0}, D={delta}, runs={runs}) ===");

        var pool = BuildInputPool(Math.Max(delta * 16, 65_536));

        var fresh = TimeMedian(runs, () => RunRowWise(ticks, delta, pool, pooledDict: false));
        var pooled = TimeMedian(runs, () => RunRowWise(ticks, delta, pool, pooledDict: true));
        var noWrap = TimeMedian(runs, () => RunNoWrap(ticks, delta, pool));
        var col = TimeMedian(runs, () => RunColumnar(ticks, delta, pool));

        var container = fresh.Bytes - pooled.Bytes; // (a)
        var wrapHash = pooled.Bytes - noWrap.Bytes;  // (b′) StructuralRow wrapper + hash
        var objArray = noWrap.Bytes - col.Bytes;     // per-row object[] header/body
        var computeBox = col.Bytes;                  // (c) boxed compute + amortised cols
        var total = fresh.Bytes;
        var reduction = total > 0 ? 100.0 * (fresh.Bytes - col.Bytes) / fresh.Bytes : 0;

        double Pct(double x) => total > 0 ? 100.0 * x / total : 0;

        o.AppendLine("## ApplyOp alloc split: row-wise vs columnar (§1 / §2.3)");
        o.AppendLine();
        o.AppendLine(
            "**Reconstruction** of the uncommitted `ApplyOpAllocSplit` from " +
            "`docs/design-columnar-batch1.md` §1/§2.3, grounded on the real " +
            "`StructuralRow` + projection-ApplyOp path. One hot projection: a " +
            $"{InCols}-col SCD-ish input row (8 boxed `long` + 6 `string`, pre-boxed) " +
            $"→ {OutCols} output cols ({OutPass} passthrough by reference + 1 computed " +
            "`long`, the only new box/row). Absolute B/row is host-specific; the " +
            "portable claim is the **relative** P→COL reduction.");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {ticks:N0} ticks × {delta} rows/tick, median of {runs} runs. " +
            $"`B/row` is managed bytes per output row (`GC.GetAllocatedBytesForCurrentThread`). " +
            $"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores, Server GC.");
        o.AppendLine();
        o.AppendLine("| variant | ns/row | B/row |");
        o.AppendLine("|:--|--:|--:|");
        o.AppendLine($"| P·fresh (row-wise, current) | {fresh.Ns:F1} | {fresh.Bytes:F1} |");
        o.AppendLine($"| P·pooled (pooled output dict) | {pooled.Ns:F1} | {pooled.Bytes:F1} |");
        o.AppendLine($"| P·noWrap (object[] only, no wrapper/hash) | {noWrap.Ns:F1} | {noWrap.Bytes:F1} |");
        o.AppendLine($"| **COL (columnar SoA, object arrays)** | {col.Ns:F1} | **{col.Bytes:F1}** |");
        o.AppendLine();
        o.AppendLine($"**P·fresh → COL: −{reduction:F1}%** (doc ceiling: −38.3%).");
        o.AppendLine();
        o.AppendLine("Apportionment of P·fresh's per-row alloc:");
        o.AppendLine();
        o.AppendLine("| term | B/row | % of P |");
        o.AppendLine("|:--|--:|--:|");
        o.AppendLine($"| (a) output container + dict entries | {container:F1} | {Pct(container):F1}% |");
        o.AppendLine($"| (b) StructuralRow wrapper + hash | {wrapHash:F1} | {Pct(wrapHash):F1}% |");
        o.AppendLine($"| (b) per-row object[] header/body | {objArray:F1} | {Pct(objArray):F1}% |");
        o.AppendLine($"| (c) boxed compute + amortised columns (COL floor) | {computeBox:F1} | {Pct(computeBox):F1}% |");
        o.AppendLine();

        Console.WriteLine(
            $"  P·fresh {fresh.Bytes,6:F1}B  P·pooled {pooled.Bytes,6:F1}B  " +
            $"P·noWrap {noWrap.Bytes,6:F1}B  COL {col.Bytes,6:F1}B  | reduction −{reduction:F1}%");
        Console.WriteLine(
            $"  apportion: (a) container {Pct(container):F1}%  (b) wrap+hash {Pct(wrapHash):F1}%  " +
            $"(b) object[] {Pct(objArray):F1}%  (c) box+cols {Pct(computeBox):F1}%");
    }

    // A pool of wide input rows, boxed ONCE (real internal rows already exist — a
    // projection copies references out of them; it does not re-box passthrough
    // values). 8 boxed longs + 6 pooled strings.
    private static StructuralRow[] BuildInputPool(int poolSize)
    {
        var strs = new[] { "ACTIVE", "closed-2024", "region-north", "USD", "tier-gold", "n/a" };
        var pool = new StructuralRow[poolSize];
        for (var i = 0; i < poolSize; i++)
        {
            var v = new object?[InCols];
            for (var c = 0; c < 8; c++)
            {
                v[c] = (long)(i * 31 + c); // boxed long
            }

            for (var c = 8; c < InCols; c++)
            {
                v[c] = strs[(i + c) % strs.Length]; // shared string ref
            }

            pool[i] = new StructuralRow(v);
        }

        return pool;
    }

    // Today's model: fresh (or pooled) output dict per tick; per input row build an
    // object[OutCols] (12 passthrough refs + 1 boxed computed long), wrap in a
    // StructuralRow (computes the structural hash), insert into the output dict.
    private static (long, long) RunRowWise(int ticks, int delta, StructuralRow[] pool, bool pooledDict)
    {
        long n = 0, sink = 0;
        Dictionary<StructuralRow, Z64>? reuse = pooledDict ? new Dictionary<StructuralRow, Z64>(delta) : null;
        for (var t = 0; t < ticks; t++)
        {
            var dict = pooledDict ? reuse! : new Dictionary<StructuralRow, Z64>(delta);
            if (pooledDict)
            {
                dict.Clear();
            }

            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var src = pool[Idx(seed, i, pool.Length)];
                var outv = new object?[OutCols];
                for (var c = 0; c < OutPass; c++)
                {
                    outv[c] = src[c]; // passthrough: copy reference, no re-box
                }

                outv[OutPass] = (long)src[0]! + (long)src[1]!; // computed: the one new box
                var row = new StructuralRow(outv); // wrapper + structural hash
                dict[row] = new Z64(1);
                n++;
            }

            foreach (var (_, w) in dict)
            {
                sink += w.Value;
            }
        }

        return (n, sink);
    }

    // Drop the StructuralRow wrapper + hash + dict: still build the object[OutCols]
    // and box the computed long, but park each in a reused list. Isolates the
    // wrapper+hash cost as (pooled − noWrap).
    private static (long, long) RunNoWrap(int ticks, int delta, StructuralRow[] pool)
    {
        long n = 0, sink = 0;
        var park = new List<object?[]>(delta);
        for (var t = 0; t < ticks; t++)
        {
            park.Clear();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var src = pool[Idx(seed, i, pool.Length)];
                var outv = new object?[OutCols];
                for (var c = 0; c < OutPass; c++)
                {
                    outv[c] = src[c];
                }

                outv[OutPass] = (long)src[0]! + (long)src[1]!;
                park.Add(outv);
                n++;
            }

            sink += park.Count;
        }

        return (n, sink);
    }

    // Columnar SoA: per tick, 13 pre-sized object?[] column arrays + a long[] weight
    // array (allocated once per tick, amortised over delta rows); per row write cells
    // into the columns. No per-row object[], no StructuralRow, no hash. The computed
    // long still boxes into its object column (the boxing residual columnar keeps).
    private static (long, long) RunColumnar(int ticks, int delta, StructuralRow[] pool)
    {
        long n = 0, sink = 0;
        for (var t = 0; t < ticks; t++)
        {
            var cols = new object?[OutCols][];
            for (var c = 0; c < OutCols; c++)
            {
                cols[c] = new object?[delta];
            }

            var weights = new long[delta];

            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var src = pool[Idx(seed, i, pool.Length)];
                for (var c = 0; c < OutPass; c++)
                {
                    cols[c][i] = src[c]; // passthrough ref into column
                }

                cols[OutPass][i] = (long)src[0]! + (long)src[1]!; // boxed compute
                weights[i] = 1;
                n++;
            }

            for (var i = 0; i < delta; i++)
            {
                sink += weights[i];
            }

            GC.KeepAlive(cols);
        }

        return (n, sink);
    }

    private readonly record struct Measured(double Ns, double Bytes);

    private static Measured TimeMedian(int runs, Func<(long rows, long sink)> body)
    {
        body(); // warmup
        var samples = new List<double>();
        double bytes = 0;
        for (var r = 0; r < runs; r++)
        {
            var b0 = GC.GetAllocatedBytesForCurrentThread();
            var sw = Stopwatch.StartNew();
            var (rows, sink) = body();
            sw.Stop();
            var b1 = GC.GetAllocatedBytesForCurrentThread();
            GC.KeepAlive(sink);
            samples.Add(sw.Elapsed.Ticks * 100.0 / rows);
            bytes = (b1 - b0) / (double)rows;
        }

        samples.Sort();
        return new Measured(samples[samples.Count / 2], bytes);
    }

    private static int Idx(long seed, int i, int len) => (int)(((seed + i) % len + len) % len);
}
