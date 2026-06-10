// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Benchmarks;

/// <summary>
/// Representation-vs-execution decomposition microbench
/// (docs/design-row-representation.md §17). The §16 arc established the
/// per-tuple gap is allocation-bound by <em>correlation</em> (ns/event tracks
/// B/event across the query ladder). This isolates, within the Layer-A floor,
/// how much per-tuple time is the <strong>representation</strong> axis
/// (heap allocation throughput + wide value-type key hashing) versus the
/// <strong>execution</strong> axis (delegate dispatch + the
/// ZSet/IZRing/Optional generic abstraction). It models the universal per-tick
/// hot loop every stateful operator runs (§16.3): build a delta dictionary of
/// D rows keyed by a wide value-type row, fold it into retained state, and
/// enumerate the result — four ways, at three key widths:
/// <list type="number">
/// <item><c>gen·fresh</c> — today's model: a fresh <see cref="ZSetBuilder{TKey,TWeight}"/>
///   (new <see cref="Dictionary{TKey,TValue}"/>) per tick, generic ZSet ops, a
///   delegate transform per row.</item>
/// <item><c>gen·pooled</c> — identical generic ops but the per-tick dictionary is
///   pooled (<c>Clear()</c> + reuse). <c>fresh − pooled</c> = the allocation tax.</item>
/// <item><c>mono·deleg</c> — a raw <c>Dictionary&lt;Row,long&gt;</c> pooled, no ZSet
///   wrapper, but the row transform still goes through a <c>Func&lt;&gt;</c>.
///   <c>pooled − mono·deleg</c> = the generic-abstraction tax.</item>
/// <item><c>mono·inline</c> — raw pooled dictionary, transform inlined, no delegate.
///   <c>mono·deleg − mono·inline</c> = the pure delegate-dispatch tax;
///   <c>mono·inline</c> itself = the irreducible compute floor (wide-key hash +
///   arithmetic) that a monomorphised columnar engine still pays.</item>
/// </list>
/// Sweeping width (2 longs / 8 longs / 3 longs+string) shows how the
/// irreducible floor scales with row width — the part per-column (SoA / columnar)
/// work could attack and the part it could not.
/// </summary>
internal static class ReprDecompBenchmark
{
    // Wide value-type keys of increasing width. Equality/hash are the compiler-
    // generated structural ones (the analogue of TypedRowEmitter's emitted hash).
    private readonly record struct W2(long A0, long A1);

    private readonly record struct W8(long A0, long A1, long A2, long A3, long A4, long A5, long A6, long A7);

    // Nexmark-bid-like: a few numeric columns plus a string (the WStr case from
    // surrogatebench) — the string hash is the width term columnar can't vectorise.
    private readonly record struct WStr(long A0, long A1, long A2, string S);

    public static void Run(StringBuilder o, int ticks, int delta, int runs)
    {
        Console.WriteLine();
        Console.WriteLine(
            $"=== representation/execution decomposition (ticks={ticks:N0}, D={delta}, runs={runs}) ===");

        o.AppendLine("## Representation-vs-execution decomposition (§17)");
        o.AppendLine();
        o.AppendLine(
            "Per-tick hot loop (build a delta dictionary of `D` wide-row keys, fold it into " +
            "retained state, enumerate the changed entries) timed four ways at three key widths. " +
            "`gen·fresh` is today's model. The successive deltas apportion the per-tuple floor:");
        o.AppendLine();
        o.AppendLine("- **alloc tax** = `gen·fresh − gen·pooled` (heap throughput: fresh `Dictionary`/tick).");
        o.AppendLine("- **abstraction tax** = `gen·pooled − mono·deleg` (ZSet/IZRing/Optional generic wrapper).");
        o.AppendLine("- **dispatch tax** = `mono·deleg − mono·inline` (per-row `Func<>` delegate call).");
        o.AppendLine("- **compute floor** = `mono·inline` (irreducible wide-key hash + arithmetic).");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {ticks:N0} ticks × {delta} rows/tick, median of {runs} runs. " +
            $"`ns/row` is per delta row; `B/row` is managed bytes per delta row " +
            $"(`GC.GetAllocatedBytesForCurrentThread`). Host: .NET {Environment.Version}, " +
            $"{Environment.ProcessorCount} cores, Server GC.");
        o.AppendLine();
        o.AppendLine("| Width | gen·fresh ns | gen·pooled ns | mono·deleg ns | mono·inline ns | alloc | abstraction | dispatch | floor |");
        o.AppendLine("|:--|--:|--:|--:|--:|--:|--:|--:|--:|");

        MeasureWidth(o, "W2 (2 long)", ticks, delta, runs,
            i => new W2(i, i ^ 0x5bd1), r => new W2(r.A0, r.A1 + 1), r => r.A0);
        MeasureWidth(o, "W8 (8 long)", ticks, delta, runs,
            i => new W8(i, i ^ 0x5bd1, i * 3, i + 7, i ^ 0x1234, i * 5, i + 11, i ^ 0x9999),
            r => new W8(r.A0, r.A1 + 1, r.A2, r.A3, r.A4, r.A5, r.A6, r.A7), r => r.A0);
        MeasureWidth(o, "WStr (3 long+str)", ticks, delta, runs,
            i => new WStr(i, i ^ 0x5bd1, i * 3, "bidder-" + (i & 0x3ff)),
            r => new WStr(r.A0, r.A1 + 1, r.A2, r.S), r => r.A0);

        o.AppendLine();
        Console.WriteLine($"Report rows written.");
    }

    private static void MeasureWidth<TRow>(
        StringBuilder o, string label, int ticks, int delta, int runs,
        Func<long, TRow> make, Func<TRow, TRow> transform, Func<TRow, long> keyOf)
        where TRow : notnull
    {
        // Pre-generate a pool of distinct rows ONCE (outside the timed loop), so the
        // hot loop never allocates a row (real internal rows already exist — a string
        // column is not re-built per probe). Each tick indexes a rotating D-window so
        // state churns. This makes the `floor` a clean pure-hash+probe measurement,
        // not a row-materialisation artefact.
        var poolSize = Math.Max(delta * 16, 65_536);
        var rows = new TRow[poolSize];
        for (var i = 0; i < poolSize; i++)
        {
            rows[i] = make(i);
        }

        var fresh = TimeMedian(runs, () => RunGenericFresh(ticks, delta, rows, transform));
        var pooled = TimeMedian(runs, () => RunGenericPooled(ticks, delta, rows, transform));
        var monoD = TimeMedian(runs, () => RunMonoDelegate(ticks, delta, rows, transform));
        var monoI = TimeMedian(runs, () => RunMonoInline(ticks, delta, rows));

        double Ns(Measured m) => m.Ns;
        var allocTax = Ns(fresh) - Ns(pooled);
        var abstractionTax = Ns(pooled) - Ns(monoD);
        var dispatchTax = Ns(monoD) - Ns(monoI);

        o.AppendLine(
            $"| {label} | {fresh.Ns:F1} ({fresh.Bytes:F0}B) | {pooled.Ns:F1} ({pooled.Bytes:F0}B) | " +
            $"{monoD.Ns:F1} ({monoD.Bytes:F0}B) | {monoI.Ns:F1} ({monoI.Bytes:F0}B) | " +
            $"{allocTax:F1} | {abstractionTax:F1} | {dispatchTax:F1} | {monoI.Ns:F1} |");
        Console.WriteLine(
            $"  {label,-18} fresh {fresh.Ns,6:F1}ns/{fresh.Bytes,5:F0}B  pooled {pooled.Ns,6:F1}/{pooled.Bytes,4:F0}B  " +
            $"monoD {monoD.Ns,6:F1}  monoI {monoI.Ns,6:F1}  | alloc {allocTax,5:F1} abs {abstractionTax,5:F1} disp {dispatchTax,5:F1} floor {monoI.Ns,5:F1}");
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

    // (1) gen·fresh — fresh ZSetBuilder (new Dictionary) per tick; generic ZSet
    // ops; a Func<> transform per row; fold into a retained ZSet via Plus.
    private static (long, long) RunGenericFresh<TRow>(
        int ticks, int delta, TRow[] rows, Func<TRow, TRow> transform)
        where TRow : notnull
    {
        long n = 0, sink = 0;
        var state = new ZSetBuilder<TRow, Z64>().Build();
        for (var t = 0; t < ticks; t++)
        {
            var b = new ZSetBuilder<TRow, Z64>();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                b.Add(transform(rows[Idx(seed, i, rows.Length)]), new Z64(1));
                n++;
            }

            var d = b.Build();
            state = state.Plus(d);
            foreach (var (_, w) in d)
            {
                sink += w.Value;
            }

            // Bound retained state (model a churning window, not unbounded growth):
            // subtract the delta back out so the next tick starts near-empty state.
            state = state.Plus(d.Negate());
        }

        return (n, sink);
    }

    // (2) gen·pooled — same generic-weight (Z64) zero-suppressing add, but the
    // per-tick delta dictionary is pooled via Clear()+reuse (the §16.7 lever-1
    // prize). fresh − this = the allocation tax.
    private static (long, long) RunGenericPooled<TRow>(
        int ticks, int delta, TRow[] rows, Func<TRow, TRow> transform)
        where TRow : notnull
    {
        long n = 0, sink = 0;
        var pool = new Dictionary<TRow, Z64>(delta);
        var state = new Dictionary<TRow, Z64>();
        for (var t = 0; t < ticks; t++)
        {
            pool.Clear();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                AddZRing(pool, transform(rows[Idx(seed, i, rows.Length)]), new Z64(1));
                n++;
            }

            foreach (var (row, w) in pool)
            {
                AddZRing(state, row, w);
                sink += w.Value;
            }

            foreach (var (row, w) in pool)
            {
                AddZRing(state, row, Z64.Negate(w));
            }
        }

        return (n, sink);
    }

    // (3) mono·deleg — raw Dictionary<Row,long>, pooled, no Z64/IZRing wrapper, but
    // the transform still goes through a Func<>. pooled − this = the generic
    // Z64-weight abstraction tax; this − inline = the pure delegate-dispatch tax.
    private static (long, long) RunMonoDelegate<TRow>(
        int ticks, int delta, TRow[] rows, Func<TRow, TRow> transform)
        where TRow : notnull
    {
        long n = 0, sink = 0;
        var pool = new Dictionary<TRow, long>(delta);
        var state = new Dictionary<TRow, long>();
        for (var t = 0; t < ticks; t++)
        {
            pool.Clear();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                AddLong(pool, transform(rows[Idx(seed, i, rows.Length)]), 1);
                n++;
            }

            foreach (var (row, w) in pool)
            {
                AddLong(state, row, w);
                sink += w;
            }

            foreach (var (row, w) in pool)
            {
                AddLong(state, row, -w);
            }
        }

        return (n, sink);
    }

    // (4) mono·inline — raw pooled Dictionary, transform inlined (no delegate).
    // The irreducible compute floor: wide value-type key hash + dict probe + add.
    private static (long, long) RunMonoInline<TRow>(
        int ticks, int delta, TRow[] rows)
        where TRow : notnull
    {
        long n = 0, sink = 0;
        var pool = new Dictionary<TRow, long>(delta);
        var state = new Dictionary<TRow, long>();
        for (var t = 0; t < ticks; t++)
        {
            pool.Clear();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                AddLong(pool, rows[Idx(seed, i, rows.Length)], 1);
                n++;
            }

            foreach (var (row, w) in pool)
            {
                AddLong(state, row, w);
                sink += w;
            }

            foreach (var (row, w) in pool)
            {
                AddLong(state, row, -w);
            }
        }

        return (n, sink);
    }

    private static void AddZRing<TRow>(Dictionary<TRow, Z64> d, TRow key, Z64 w)
        where TRow : notnull
    {
        if (Z64.IsZero(w))
        {
            return;
        }

        if (d.TryGetValue(key, out var e))
        {
            var sum = Z64.Add(e, w);
            if (Z64.IsZero(sum))
            {
                d.Remove(key);
            }
            else
            {
                d[key] = sum;
            }
        }
        else
        {
            d[key] = w;
        }
    }

    private static void AddLong<TRow>(Dictionary<TRow, long> d, TRow key, long w)
        where TRow : notnull
    {
        if (w == 0)
        {
            return;
        }

        if (d.TryGetValue(key, out var e))
        {
            var sum = e + w;
            if (sum == 0)
            {
                d.Remove(key);
            }
            else
            {
                d[key] = sum;
            }
        }
        else
        {
            d[key] = w;
        }
    }
}
