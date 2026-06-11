// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

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

        MeasureJoinTrace(o, ticks, delta, runs);

        o.AppendLine();
        Console.WriteLine($"Report rows written.");
    }

    // The join-trace integrate hot loop (§18-immune term-2 site, §21). Models what
    // IncrementalJoinOp does to ONE trace per tick: integrate a delta of D rows into
    // an IndexedZSet<joinKey, storedRow> (MergeInPlace → inner ZSet keyed by the WHOLE
    // stored row), then probe the touched groups (the join cross-product enumerates
    // GroupFor(key)). §18's aggregate-input narrowing sits ABOVE the join and never
    // touches these stored rows; the optimizer has NO column-liveness-through-join rule
    // (PlanOptimizer.cs:38). So the inner multiset whole-row-hashes the full source row.
    //
    // The decisive A/B: store the WIDE row (today) vs a NARROW projection of it (what a
    // projection-pushdown-through-join / column-pruning rule, or a columnar SoA inner
    // multiset, would buy). `wide − narrow` = the term-2 prize at the join, the number
    // that decides whether the cheap optimizer rule suffices or columnar is required.
    private static void MeasureJoinTrace(StringBuilder o, int ticks, int delta, int runs)
    {
        // ~delta/8 distinct join keys touched per tick (a few rows per key, like q4's
        // bids-per-auction), so the inner multiset genuinely accumulates and the
        // cross-product probe re-touches stored rows — the §14.2 recurrence.
        var groups = Math.Max(delta / 8, 1);

        o.AppendLine("## Join-trace integrate: wide-stored vs narrow-stored inner multiset (§21)");
        o.AppendLine();
        o.AppendLine(
            "The term-2 site §18 narrowing cannot reach: `IncrementalJoinOp`'s trace is an " +
            "`IndexedZSet<joinKey, storedRow>` whose inner `ZSet` is keyed by the **whole stored " +
            "row**, hashed on every `MergeInPlace` integrate and re-touched by the cross-product " +
            "probe. The optimizer has no column-liveness-through-join rule, so the full source row " +
            "is stored even when only a few columns are live above the join (q4: bid needs " +
            "`{auction,price,date_time}` of 7, auction `{id,category,date_time,expires}` of 10). " +
            "`wide` stores the full row; `narrow` stores a 2-column projection — the prize a " +
            "projection-pushdown rule (or columnar SoA) would capture.");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {ticks:N0} ticks × {delta} rows/tick over ~{groups} join keys, median of " +
            $"{runs} runs. Same delta/probe/key distribution; the ONLY difference is the stored " +
            "inner-row width.");
        o.AppendLine();
        o.AppendLine("| Stored row | wide ns | wide B | narrow ns | narrow B | term-2 prize (ns) | prize % |");
        o.AppendLine("|:--|--:|--:|--:|--:|--:|--:|");

        MeasureTracePair(o, "W8 (8 long) → W2", ticks, delta, runs, groups,
            i => new W8(i, i ^ 0x5bd1, i * 3, i + 7, i ^ 0x1234, i * 5, i + 11, i ^ 0x9999),
            w => new W2(w.A0, w.A2));
        MeasureTracePair(o, "WStr (3 long+str) → W2", ticks, delta, runs, groups,
            i => new WStr(i, i ^ 0x5bd1, i * 3, "bidder-" + (i & 0x3ff)),
            w => new W2(w.A0, w.A2));
    }

    private static void MeasureTracePair<TWide>(
        StringBuilder o, string label, int ticks, int delta, int runs, int groups,
        Func<long, TWide> makeWide, Func<TWide, W2> narrow)
        where TWide : notnull
    {
        var poolSize = Math.Max(delta * 16, 65_536);
        var wide = new TWide[poolSize];
        var thin = new W2[poolSize];
        for (var i = 0; i < poolSize; i++)
        {
            wide[i] = makeWide(i);
            thin[i] = narrow(wide[i]);
        }

        var w = TimeMedian(runs, () => RunJoinTrace(ticks, delta, groups, wide));
        var nrw = TimeMedian(runs, () => RunJoinTrace(ticks, delta, groups, thin));
        var prize = w.Ns - nrw.Ns;
        var prizePct = w.Ns > 0 ? 100.0 * prize / w.Ns : 0;

        o.AppendLine(
            $"| {label} | {w.Ns:F1} | {w.Bytes:F0} | {nrw.Ns:F1} | {nrw.Bytes:F0} | " +
            $"{prize:F1} | {prizePct:F0}% |");
        Console.WriteLine(
            $"  join-trace {label,-22} wide {w.Ns,6:F1}ns/{w.Bytes,5:F0}B  narrow {nrw.Ns,6:F1}ns/{nrw.Bytes,5:F0}B  " +
            $"| term-2 prize {prize,5:F1}ns ({prizePct:F0}%)");
    }

    // One join trace's per-tick integrate + probe, today's model: a fresh
    // IndexedZSetBuilder per tick, MergeInPlace into the retained trace (inner ZSet
    // keyed by the whole stored row), probe the touched groups, then retract to bound
    // state (model a churning window, not unbounded growth).
    private static (long, long) RunJoinTrace<TRow>(int ticks, int delta, int groups, TRow[] rows)
        where TRow : notnull
    {
        long n = 0, sink = 0;
        var trace = new IndexedZSetTrace<long, TRow, Z64>();
        for (var t = 0; t < ticks; t++)
        {
            var seed = (long)t * delta;
            var b = new IndexedZSetBuilder<long, TRow, Z64>();
            for (var i = 0; i < delta; i++)
            {
                var key = (seed + i) % groups;
                b.Add(key, rows[Idx(seed, i, rows.Length)], new Z64(1));
                n++;
            }

            var d = b.Build();
            trace.Integrate(d);

            // Probe: the join enumerates the stored group for each delta key (the
            // cross-product touch — re-hashing/re-reading the wide stored rows).
            foreach (var (key, _) in d)
            {
                foreach (var (_, gw) in trace.Current.GroupFor(key))
                {
                    sink += gw.Value;
                }
            }

            // Retract this tick's delta to keep the trace a bounded churning window.
            var nb = new IndexedZSetBuilder<long, TRow, Z64>();
            foreach (var (key, g) in d)
            {
                foreach (var (v, gw) in g)
                {
                    nb.Add(key, v, Z64.Negate(gw));
                }
            }

            trace.Integrate(nb.Build());
        }

        return (n, sink);
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
