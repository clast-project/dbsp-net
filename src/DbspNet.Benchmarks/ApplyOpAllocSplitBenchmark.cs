// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Benchmarks;

/// <summary>
/// ApplyOp per-row allocation split — row-wise vs object-columnar vs typed-columnar
/// projection (docs/design-columnar-batch1.md §1 apportionment, §2.3 the B ceiling,
/// §7 #3 the typed-column upside).
///
/// <para>
/// RECONSTRUCTION NOTE. The original <c>ApplyOpAllocSplit</c> that produced the
/// doc's numbers (P = 207.5 B/row, COL = 128.0 B/row, −38.3%) was authored on
/// another machine and never committed. This re-derives it from the doc's spec,
/// grounded on the real <see cref="StructuralRow"/> (an <c>object?[]</c> row with a
/// cached <see cref="HashCode"/> hash) and the projection ApplyOp path (a fresh
/// <c>object[]</c> + <c>StructuralRow</c> built per output row into a fresh
/// <see cref="ZSetBuilder{TKey,TWeight}"/>). Absolute B/row is host-specific; the
/// portable claim is the <b>relative</b> reductions.
/// </para>
///
/// <para>
/// Three representations of the same projection, each measured at two scenarios:
/// <list type="bullet">
/// <item><b>P·fresh</b> — today's model: fresh output dict per tick; per row an
///   <c>object[OutCols]</c> + a <c>StructuralRow</c> wrapper (structural hash) +
///   dict insert.</item>
/// <item><b>COL</b> — object-columnar SoA: <c>OutCols</c> pre-sized
///   <c>object?[]</c> column arrays + a <c>long[]</c> weight array per tick. No
///   per-row <c>object[]</c>, wrapper, or hash. Passthrough numerics copy the input
///   row's already-boxed reference (no re-box, per §23); each <em>computed</em>
///   numeric still boxes into its object column — the boxing residual §2.3 keeps.</item>
/// <item><b>TCOL</b> — typed-columnar SoA: numeric columns are <c>long[]</c>, string
///   columns are <c>string[]</c>, read from a typed SoA input pool. Zero boxing
///   anywhere. This is the §7 #3 end-state.</item>
/// </list>
/// </para>
///
/// <para>
/// KEY 64-bit FACT the ladder exposes: a <c>long[]</c> element and an
/// <c>object?[]</c> element are both 8 bytes, so TCOL does <b>not</b> shrink the
/// column storage vs COL — its entire marginal win is removing the separate heap
/// box per <em>freshly-produced</em> numeric value. Passthrough numerics share
/// their box by reference (§23), so typed's payoff scales with the count of
/// <em>computed</em> numeric outputs — tiny on a passthrough-heavy projection, real
/// on a numeric-heavy one. The two scenarios make that scaling visible.
/// </para>
/// </summary>
internal static class ApplyOpAllocSplitBenchmark
{
    private const int InNum = 8;  // input numeric columns (boxed long / typed long[])
    private const int InStr = 6;  // input string columns

    // A projection layout: how many input numerics pass through, how many numeric
    // outputs are freshly computed, how many strings pass through.
    private readonly record struct Layout(string Name, int PassNum, int CompNum, int PassStr)
    {
        public int OutCols => PassNum + CompNum + PassStr;
    }

    public static void Run(StringBuilder o, int ticks, int delta, int runs)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"=== ApplyOp alloc split: row-wise vs object-col vs typed-col " +
            $"(ticks={ticks:N0}, D={delta}, runs={runs}) ===");

        // Mixed: 8 numeric + 4 string passthrough + 1 computed = 13 out cols, 1 fresh
        // box/row. Numeric-heavy: 2 numeric passthrough + 10 computed + 1 string = 13
        // out cols, 10 fresh boxes/row (the compute-heavy projection where typed pays).
        var mixed = new Layout("mixed (8+4 pass, 1 computed)", PassNum: 8, CompNum: 1, PassStr: 4);
        var numeric = new Layout("numeric-heavy (2 pass, 10 computed)", PassNum: 2, CompNum: 10, PassStr: 1);

        var objPool = BuildObjInputPool(Math.Max(delta * 16, 65_536));
        var typedPool = BuildTypedInputPool(objPool.Length);

        o.AppendLine("## ApplyOp alloc split: row-wise → object-col → typed-col (§1 / §2.3 / §7 #3)");
        o.AppendLine();
        o.AppendLine(
            "**Reconstruction** of the uncommitted `ApplyOpAllocSplit` from " +
            "`docs/design-columnar-batch1.md`, grounded on the real `StructuralRow` + " +
            $"projection-ApplyOp path. Input row is {InNum} boxed `long` + {InStr} " +
            "`string` (pre-boxed once; passthrough copies references). Absolute B/row " +
            "is host-specific; the portable claim is the **relative** reductions.");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {ticks:N0} ticks × {delta} rows/tick, median of {runs} runs. " +
            $"`B/row` is managed bytes per output row (`GC.GetAllocatedBytesForCurrentThread`). " +
            $"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores, Server GC.");
        o.AppendLine();

        // --- Table 1: apportionment of P·fresh (mixed scenario) -----------------
        var apFresh = TimeMedian(runs, () => RunRowWise(mixed, ticks, delta, objPool, pooledDict: false));
        var apPooled = TimeMedian(runs, () => RunRowWise(mixed, ticks, delta, objPool, pooledDict: true));
        var apNoWrap = TimeMedian(runs, () => RunNoWrap(mixed, ticks, delta, objPool));
        var apCol = TimeMedian(runs, () => RunColObj(mixed, ticks, delta, objPool));

        var container = apFresh.Bytes - apPooled.Bytes;
        var wrapHash = apPooled.Bytes - apNoWrap.Bytes;
        var objArray = apNoWrap.Bytes - apCol.Bytes;
        var colFloor = apCol.Bytes;
        double Pct(double x) => apFresh.Bytes > 0 ? 100.0 * x / apFresh.Bytes : 0;

        o.AppendLine($"### Apportionment of P·fresh — {mixed.Name}");
        o.AppendLine();
        o.AppendLine("| term | B/row | % of P |");
        o.AppendLine("|:--|--:|--:|");
        o.AppendLine($"| (a) output container + dict entries | {container:F1} | {Pct(container):F1}% |");
        o.AppendLine($"| (b) StructuralRow wrapper + hash | {wrapHash:F1} | {Pct(wrapHash):F1}% |");
        o.AppendLine($"| (b) per-row object[] header/body | {objArray:F1} | {Pct(objArray):F1}% |");
        o.AppendLine($"| (c) boxed compute + amortised columns (COL floor) | {colFloor:F1} | {Pct(colFloor):F1}% |");
        o.AppendLine();

        // --- Table 2: the three-rung ladder, both scenarios ---------------------
        o.AppendLine("### Ladder: P·fresh → COL (object) → TCOL (typed), by projection shape");
        o.AppendLine();
        o.AppendLine(
            "`COL↓` = COL reduction vs P·fresh (the §2.3 object-columnar ceiling). " +
            "`TCOL↓` = TCOL reduction vs P·fresh. `typed↓` = TCOL's **marginal** gain " +
            "over COL (the §7 #3 typed-column upside = boxing of freshly-computed numerics).");
        o.AppendLine();
        o.AppendLine("| scenario | fresh boxes/row | P·fresh B | COL B | TCOL B | COL↓ | TCOL↓ | typed↓ vs COL |");
        o.AppendLine("|:--|--:|--:|--:|--:|--:|--:|--:|");

        foreach (var lay in new[] { mixed, numeric })
        {
            var p = TimeMedian(runs, () => RunRowWise(lay, ticks, delta, objPool, pooledDict: false));
            var c = TimeMedian(runs, () => RunColObj(lay, ticks, delta, objPool));
            var tc = TimeMedian(runs, () => RunColTyped(lay, ticks, delta, typedPool));

            var colDown = p.Bytes > 0 ? 100.0 * (p.Bytes - c.Bytes) / p.Bytes : 0;
            var tcolDown = p.Bytes > 0 ? 100.0 * (p.Bytes - tc.Bytes) / p.Bytes : 0;
            var typedMarg = c.Bytes > 0 ? 100.0 * (c.Bytes - tc.Bytes) / c.Bytes : 0;

            o.AppendLine(
                $"| {lay.Name} | {lay.CompNum} | {p.Bytes:F1} | {c.Bytes:F1} | {tc.Bytes:F1} | " +
                $"−{colDown:F1}% | −{tcolDown:F1}% | −{typedMarg:F1}% |");
            Console.WriteLine(
                $"  {lay.Name,-34} P {p.Bytes,6:F1}B  COL {c.Bytes,6:F1}B  TCOL {tc.Bytes,6:F1}B  " +
                $"| COL −{colDown:F1}%  TCOL −{tcolDown:F1}%  typed-marg −{typedMarg:F1}%");
        }

        o.AppendLine();
        o.AppendLine(
            "Reading: `long[]` and `object?[]` are both 8 B/element, so TCOL never " +
            "shrinks column storage — `typed↓ vs COL` is purely the eliminated boxes of " +
            "computed numerics. It is small when the projection mostly passes values " +
            "through (mixed) and grows with computed-numeric width (numeric-heavy), " +
            "confirming §2.3/§23: object-columnar captures the bulk; typed-columnar's " +
            "extra upside is the residual boxing term, worth its larger scope only where " +
            "operators newly produce many numeric columns.");
        o.AppendLine();
    }

    // Object input pool: StructuralRow of InNum boxed longs + InStr shared strings,
    // boxed ONCE (a projection copies references out; it does not re-box passthrough).
    private static StructuralRow[] BuildObjInputPool(int poolSize)
    {
        var strs = new[] { "ACTIVE", "closed-2024", "region-north", "USD", "tier-gold", "n/a" };
        var pool = new StructuralRow[poolSize];
        for (var i = 0; i < poolSize; i++)
        {
            var v = new object?[InNum + InStr];
            for (var c = 0; c < InNum; c++)
            {
                v[c] = (long)(i * 31 + c);
            }

            for (var c = 0; c < InStr; c++)
            {
                v[InNum + c] = strs[(i + c) % strs.Length];
            }

            pool[i] = new StructuralRow(v);
        }

        return pool;
    }

    // Typed SoA input pool: parallel long[] / string[] columns (the typed end-state,
    // where nothing is boxed). Same logical values as the object pool.
    private static (long[][] Nums, string[][] Strs) BuildTypedInputPool(int poolSize)
    {
        var strs = new[] { "ACTIVE", "closed-2024", "region-north", "USD", "tier-gold", "n/a" };
        var nums = new long[InNum][];
        for (var c = 0; c < InNum; c++)
        {
            nums[c] = new long[poolSize];
        }

        var scols = new string[InStr][];
        for (var c = 0; c < InStr; c++)
        {
            scols[c] = new string[poolSize];
        }

        for (var i = 0; i < poolSize; i++)
        {
            for (var c = 0; c < InNum; c++)
            {
                nums[c][i] = i * 31 + c;
            }

            for (var c = 0; c < InStr; c++)
            {
                scols[c][i] = strs[(i + c) % strs.Length];
            }
        }

        return (nums, scols);
    }

    // Today's model: fresh (or pooled) output dict per tick; per input row build an
    // object[OutCols] (PassNum passthrough refs + CompNum boxed computed longs +
    // PassStr string refs), wrap in a StructuralRow (structural hash), dict insert.
    private static (long, long) RunRowWise(Layout lay, int ticks, int delta, StructuralRow[] pool, bool pooledDict)
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
                var outv = new object?[lay.OutCols];
                var k = 0;
                for (var c = 0; c < lay.PassNum; c++)
                {
                    outv[k++] = src[c]; // passthrough numeric: shared box
                }

                for (var c = 0; c < lay.CompNum; c++)
                {
                    outv[k++] = (long)src[0]! + (long)src[1]! + c; // computed: new box
                }

                for (var c = 0; c < lay.PassStr; c++)
                {
                    outv[k++] = src[InNum + c]; // passthrough string ref
                }

                dict[new StructuralRow(outv)] = new Z64(1);
                n++;
            }

            foreach (var (_, w) in dict)
            {
                sink += w.Value;
            }
        }

        return (n, sink);
    }

    // Drop the StructuralRow wrapper + hash + dict: still build the object[OutCols] and
    // box the computed longs, but park each in a reused list. Isolates wrapper+hash.
    private static (long, long) RunNoWrap(Layout lay, int ticks, int delta, StructuralRow[] pool)
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
                var outv = new object?[lay.OutCols];
                var k = 0;
                for (var c = 0; c < lay.PassNum; c++)
                {
                    outv[k++] = src[c];
                }

                for (var c = 0; c < lay.CompNum; c++)
                {
                    outv[k++] = (long)src[0]! + (long)src[1]! + c;
                }

                for (var c = 0; c < lay.PassStr; c++)
                {
                    outv[k++] = src[InNum + c];
                }

                park.Add(outv);
                n++;
            }

            sink += park.Count;
        }

        return (n, sink);
    }

    // Object-columnar SoA: OutCols pre-sized object?[] column arrays + a long[] weight
    // array per tick (amortised over the tick's rows). Passthrough numerics copy the
    // shared box; computed numerics box into their object column (the §2.3 residual).
    private static (long, long) RunColObj(Layout lay, int ticks, int delta, StructuralRow[] pool)
    {
        long n = 0, sink = 0;
        for (var t = 0; t < ticks; t++)
        {
            var cols = new object?[lay.OutCols][];
            for (var c = 0; c < lay.OutCols; c++)
            {
                cols[c] = new object?[delta];
            }

            var weights = new long[delta];
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var src = pool[Idx(seed, i, pool.Length)];
                var k = 0;
                for (var c = 0; c < lay.PassNum; c++)
                {
                    cols[k++][i] = src[c]; // shared box into column
                }

                for (var c = 0; c < lay.CompNum; c++)
                {
                    cols[k++][i] = (long)src[0]! + (long)src[1]! + c; // boxed compute
                }

                for (var c = 0; c < lay.PassStr; c++)
                {
                    cols[k++][i] = src[InNum + c];
                }

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

    // Typed-columnar SoA: numeric outputs are long[], string outputs are string[],
    // read from the typed SoA input pool. Zero boxing anywhere (the §7 #3 end-state).
    private static (long, long) RunColTyped(Layout lay, int ticks, int delta, (long[][] Nums, string[][] Strs) pool)
    {
        var (nums, strs) = pool;
        var poolLen = nums[0].Length;
        long n = 0, sink = 0;
        var numOut = lay.PassNum + lay.CompNum;
        for (var t = 0; t < ticks; t++)
        {
            var ncols = new long[numOut][];
            for (var c = 0; c < numOut; c++)
            {
                ncols[c] = new long[delta];
            }

            var scols = new string[lay.PassStr][];
            for (var c = 0; c < lay.PassStr; c++)
            {
                scols[c] = new string[delta];
            }

            var weights = new long[delta];
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var idx = Idx(seed, i, poolLen);
                var k = 0;
                for (var c = 0; c < lay.PassNum; c++)
                {
                    ncols[k++][i] = nums[c][idx]; // typed passthrough: no box
                }

                for (var c = 0; c < lay.CompNum; c++)
                {
                    ncols[k++][i] = nums[0][idx] + nums[1][idx] + c; // inline compute: no box
                }

                for (var c = 0; c < lay.PassStr; c++)
                {
                    scols[c][i] = strs[c][idx];
                }

                weights[i] = 1;
                n++;
            }

            for (var i = 0; i < delta; i++)
            {
                sink += weights[i];
            }

            GC.KeepAlive(ncols);
            GC.KeepAlive(scols);
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
