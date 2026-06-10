// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;

namespace DbspNet.Benchmarks;

/// <summary>
/// Microbenchmark for the row-representation design
/// (docs/design-row-representation.md §14.6): does replacing whole-row
/// dictionary keys with interned <see cref="int"/> surrogates pay off, and at
/// what re-touch count <c>R</c>?
/// </summary>
/// <remarks>
/// <para>The flat aggregate hot path re-hashes a group's whole inner Z-set every
/// tick it is touched (<c>IncrementalAggregateOp.cs:126</c>,
/// <c>afterGroup = beforeGroup + groupDelta</c> → <c>ZSetBuilder.From</c>, a
/// genuine per-entry re-hash — verified). For a group of distinct value rows
/// touched <c>R</c> times over its life, the struct path pays
/// <c>R</c> whole-row hashes per row; the surrogate path pays <b>one</b>
/// whole-row hash to intern the row, then <c>R</c> cheap <c>int</c> hashes.</para>
///
/// <para>§14.3(a): the surrogate wins iff a row is re-touched enough times to
/// amortise its one-time intern cost — the <b>R-crossover</b>. This bench
/// locates that crossover across row <b>width</b> (narrow longs → wide rows with
/// a string column, as in a Nexmark bid) so the design can predict, from a
/// query's per-operator re-touch count, which queries can win <i>before</i> any
/// operator is wired.</para>
///
/// <para>Two tables:</para>
/// <list type="number">
///   <item><b>Crossover sweep</b> — a fixed set of <c>M</c> distinct rows
///   rebuilt + scanned <c>R</c> times (the decoupled crossover); surrogate total
///   <i>includes</i> the one-time intern. Speedup = struct / surrogate; the row
///   where it crosses 1.0 is <c>R*</c>.</item>
///   <item><b>Growing group</b> — the faithful aggregate shape: one group grows
///   to size <c>K</c>, rebuilt every tick, so each row is re-hashed ~<c>K</c>
///   times (O(K²) total). This is what q4's per-auction bid groups actually
///   do.</item>
/// </list>
/// </remarks>
internal static class SurrogateKeyBenchmark
{
    private const int M = 1_000;            // distinct rows in the crossover group
    private static readonly int[] ReTouch = { 1, 2, 4, 8, 16, 32, 64 };
    private static readonly int[] GroupSizes = { 16, 64, 256, 1_024 };
    private const int Warmups = 3;
    private const int Samples = 21;

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Surrogate-key microbench (whole-row hash vs interned int) ===");

        output.AppendLine("## Surrogate-key microbench — whole-row hash vs interned int");
        output.AppendLine();
        output.AppendLine(
            "Microbench for docs/design-row-representation.md §14.6. Models the flat " +
            "aggregate's per-tick group rebuild (`ZSetBuilder.From`, a verified " +
            "per-entry re-hash). The **struct** path keys a `Dictionary` on the whole " +
            "row (the emitted typed row's multi-field `GetHashCode`); the " +
            "**surrogate** path interns each distinct row to an `int` once, then keys " +
            "a `Dictionary<int,…>`. A rebuild does one `d[key]=w` insert per row " +
            "(mirroring `ZSetBuilder.From`) plus one `TryGetValue` scan (mirroring the " +
            "aggregator read) — two hashes per row per rebuild.");
        output.AppendLine();
        output.AppendLine(
            "Row **widths**: `W2`/`W4`/`W8` are 2/4/8 `long` columns; `WStr` is " +
            "3 `long`s + a `string` (a Nexmark-bid-like row — string hashing is the " +
            "expensive case). Surrogate totals **include** the one-time intern, so the " +
            "speedup already pays for interning.");
        output.AppendLine();

        EmitCrossover(output);
        EmitGrowingGroup(output);
    }

    // ---- Table 1: decoupled R-crossover ----

    private static void EmitCrossover(StringBuilder output)
    {
        output.AppendLine("### Crossover sweep — " + M.ToString("N0") + " distinct rows, rebuilt + scanned R times");
        output.AppendLine();
        output.AppendLine(
            "`struct`/`surr` are median ns for the whole (R rebuilds of " + M.ToString("N0") +
            " rows); **Speedup** = struct/surr (>1 = surrogate wins). The first R " +
            "where Speedup ≥ 1.0 is the crossover R\\* for that width.");
        output.AppendLine();

        EmitCrossoverFor(output, "W2", BuildW2(M));
        EmitCrossoverFor(output, "W4", BuildW4(M));
        EmitCrossoverFor(output, "W8", BuildW8(M));
        EmitCrossoverFor(output, "WStr", BuildWStr(M));
    }

    private static void EmitCrossoverFor<T>(StringBuilder output, string width, T[] rows)
        where T : notnull
    {
        output.AppendLine($"#### Width {width}");
        output.AppendLine();
        output.AppendLine("| R (re-touches) | struct | surrogate | Speedup |");
        output.AppendLine("|---------------:|-------:|----------:|--------:|");

        foreach (var r in ReTouch)
        {
            var structNs = MedianTotalNs(() => StructRebuilds(rows, r));
            var surrNs = MedianTotalNs(() => SurrogateRebuilds(rows, r));
            var speedup = surrNs > 0 ? structNs / surrNs : 0.0;

            Console.WriteLine(
                $"    {width,-5} R={r,-4} struct={FmtNs(structNs)} surr={FmtNs(surrNs)} " +
                $"{BenchmarkHarness.FormatRatio(speedup).Trim()}");
            output.AppendLine(
                $"| {r,14} | {FmtNs(structNs)} | {FmtNs(surrNs)} | {BenchmarkHarness.FormatRatio(speedup).Trim()} |");
        }

        output.AppendLine();
    }

    // ---- Table 2: faithful growing-group rebuild ----

    private static void EmitGrowingGroup(StringBuilder output)
    {
        output.AppendLine("### Growing group — one group grows to K, rebuilt every tick (O(K²))");
        output.AppendLine();
        output.AppendLine(
            "The actual aggregate shape: a group accumulates one new row per tick and " +
            "is rebuilt each tick, so a row added early is re-hashed ~K times. " +
            "`struct`/`surr` are median ns for the **whole K-tick growth**; surrogate " +
            "interns each new row once. This is the q4 per-auction-bid case (rows are " +
            "distinct, so §6.5's 'hashed once' assumption is wrong — incremental " +
            "maintenance re-hashes them K times).");
        output.AppendLine();
        output.AppendLine("| Width | K | struct | surrogate | Speedup |");
        output.AppendLine("|:------|--:|-------:|----------:|--------:|");

        foreach (var k in GroupSizes)
        {
            EmitGrowRow(output, "W2", BuildW2(k), k);
            EmitGrowRow(output, "W4", BuildW4(k), k);
            EmitGrowRow(output, "W8", BuildW8(k), k);
            EmitGrowRow(output, "WStr", BuildWStr(k), k);
        }

        output.AppendLine();
    }

    private static void EmitGrowRow<T>(StringBuilder output, string width, T[] rows, int k)
        where T : notnull
    {
        var structNs = MedianTotalNs(() => StructGrow(rows));
        var surrNs = MedianTotalNs(() => SurrogateGrow(rows));
        var speedup = surrNs > 0 ? structNs / surrNs : 0.0;

        Console.WriteLine(
            $"    grow {width,-5} K={k,-5} struct={FmtNs(structNs)} surr={FmtNs(surrNs)} " +
            $"{BenchmarkHarness.FormatRatio(speedup).Trim()}");
        output.AppendLine(
            $"| {width} | {k} | {FmtNs(structNs)} | {FmtNs(surrNs)} | {BenchmarkHarness.FormatRatio(speedup).Trim()} |");
    }

    // ---- Work kernels ----

    // One rebuild = insert every row (re-hash, mirroring ZSetBuilder.From) then
    // scan every row (mirroring the aggregator read). R rebuilds of the same set.
    private static long StructRebuilds<T>(T[] rows, int r) where T : notnull
    {
        long sink = 0;
        for (var i = 0; i < r; i++)
        {
            var d = new Dictionary<T, long>(rows.Length);
            foreach (var row in rows)
            {
                d[row] = 1;
            }

            foreach (var row in rows)
            {
                if (d.TryGetValue(row, out var w))
                {
                    sink += w;
                }
            }
        }

        return sink;
    }

    // Surrogate: intern each distinct row once (the one-time whole-row hash),
    // then R rebuilds over the int ids. Intern cost is INCLUDED in the timing.
    private static long SurrogateRebuilds<T>(T[] rows, int r) where T : notnull
    {
        var ids = Intern(rows);
        long sink = 0;
        for (var i = 0; i < r; i++)
        {
            var d = new Dictionary<int, long>(ids.Length);
            foreach (var id in ids)
            {
                d[id] = 1;
            }

            foreach (var id in ids)
            {
                if (d.TryGetValue(id, out var w))
                {
                    sink += w;
                }
            }
        }

        return sink;
    }

    // Growing group: tick t adds rows[t] and rebuilds the dict of the first t+1
    // rows. Total re-hashing is O(K²) on the struct path.
    private static long StructGrow<T>(T[] rows) where T : notnull
    {
        long sink = 0;
        for (var t = 0; t < rows.Length; t++)
        {
            var d = new Dictionary<T, long>(t + 1);
            for (var i = 0; i <= t; i++)
            {
                d[rows[i]] = 1;
            }

            sink += d.Count;
        }

        return sink;
    }

    private static long SurrogateGrow<T>(T[] rows) where T : notnull
    {
        var forward = new Dictionary<T, int>(rows.Length);
        var ids = new int[rows.Length]; // inner multiset stored as surrogate ids
        long sink = 0;
        for (var t = 0; t < rows.Length; t++)
        {
            // Intern ONLY the one newly-arrived row (one whole-row hash, once
            // ever); a real surrogate operator stores ids, so the rebuild below
            // never touches the struct again.
            if (!forward.TryGetValue(rows[t], out var id))
            {
                id = forward.Count;
                forward[rows[t]] = id;
            }

            ids[t] = id;

            var d = new Dictionary<int, long>(t + 1);
            for (var i = 0; i <= t; i++)
            {
                d[ids[i]] = 1; // int hash only — no struct re-hash
            }

            sink += d.Count;
        }

        return sink;
    }

    private static int[] Intern<T>(T[] rows) where T : notnull
    {
        var forward = new Dictionary<T, int>(rows.Length);
        var ids = new int[rows.Length];
        for (var i = 0; i < rows.Length; i++)
        {
            if (!forward.TryGetValue(rows[i], out var id))
            {
                id = forward.Count;
                forward[rows[i]] = id;
            }

            ids[i] = id;
        }

        return ids;
    }

    // ---- Row generators (distinct rows; mirror TypedRowEmitter: sealed struct,
    //      IEquatable, HashCode.Combine over all fields) ----

    private static W2[] BuildW2(int n)
    {
        var a = new W2[n];
        for (var i = 0; i < n; i++)
        {
            a[i] = new W2(i, (long)i * 31 + 7);
        }

        return a;
    }

    private static W4[] BuildW4(int n)
    {
        var a = new W4[n];
        for (var i = 0; i < n; i++)
        {
            a[i] = new W4(i, (long)i * 31 + 7, (long)i * 131 + 11, (long)i * 977 + 3);
        }

        return a;
    }

    private static W8[] BuildW8(int n)
    {
        var a = new W8[n];
        for (var i = 0; i < n; i++)
        {
            long b = i;
            a[i] = new W8(b, b * 31 + 7, b * 131 + 11, b * 977 + 3, b * 7919 + 5, b * 104729 + 13, b * 1299709 + 17, b * 15485863 + 19);
        }

        return a;
    }

    private static WStr[] BuildWStr(int n)
    {
        var a = new WStr[n];
        for (var i = 0; i < n; i++)
        {
            // Bid-like: auction, price, bidder + a url/extra string column.
            a[i] = new WStr(i, (long)i * 977 + 3, (long)i * 31 + 7, "https://nexmark.example/item/" + i);
        }

        return a;
    }

    // ---- Timing ----

    private static double MedianTotalNs(Func<long> work)
    {
        // Normalise the GC state before each cell so a collection triggered by a
        // prior heavy cell doesn't land mid-sample and skew the median.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long sink = 0;
        for (var i = 0; i < Warmups; i++)
        {
            sink += work();
        }

        var times = new List<double>(Samples);
        var sw = new Stopwatch();
        for (var s = 0; s < Samples; s++)
        {
            sw.Restart();
            sink += work();
            sw.Stop();
            times.Add(sw.Elapsed.TotalNanoseconds);
        }

        GC.KeepAlive(sink);
        times.Sort();
        return times[times.Count / 2];
    }

    private static string FmtNs(double ns) =>
        ns switch
        {
            < 1_000.0 => $"{ns,8:F0} ns",
            < 1_000_000.0 => $"{ns / 1_000.0,8:F2} µs",
            _ => $"{ns / 1_000_000.0,8:F2} ms",
        };

    // ---- Row types: sealed value types with field-combine hash/equals,
    //      matching what TypedRowEmitter emits for the typed parallel path. ----

    private readonly record struct W2(long A, long B);

    private readonly record struct W4(long A, long B, long C, long D);

    private readonly record struct W8(long A, long B, long C, long D, long E, long F, long G, long H);

    private readonly record struct WStr(long A, long B, long C, string S);
}
