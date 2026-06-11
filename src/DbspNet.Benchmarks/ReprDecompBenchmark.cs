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

    // The full Nexmark bid row q18/q19 retain in TOP-K state and emit (SELECT * —
    // all 7 columns, incl. the url/extra strings that dominate the structural hash).
    private readonly record struct WBid(
        long Auction, long Bidder, long Price, long DateTime,
        string Channel, string Url, string Extra);

    // The narrow ranking key TOP-K actually needs: the ORDER BY value plus a back-
    // reference into the wide-row pool (recovered only for the ≤k survivors). The
    // partition key is extracted from the wide input row before narrowing (it is the
    // OUTER dict key), so it need not be carried here.
    private readonly record struct NarrowKey(long Order, long Ref);

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

        MeasureTopK(o, delta, runs);

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

    // The partitioned-TOP-K state site (§22, q18/q19). PartitionedTopKOp keeps, per
    // partition, a SortedDictionary<wideRow,long> (`_accum`) ordered by the ORDER BY
    // key with a full-row tiebreak, plus a Dictionary<wideRow,long> window — both keyed
    // by the WHOLE 7-column bid row (incl. url/extra strings), though ranking needs only
    // {partition, order}. This A/Bs storing the WIDE row vs a NARROW {order, rowRef} that
    // recovers the full row from a pool only for the ≤k survivors it must output. Output
    // is wide in BOTH variants (the fetch-back recovers it), so the measured prize is
    // exactly the cheaper in-state compare+hash — and nothing it cannot reach (the wide
    // output build is charged identically to both).
    private static void MeasureTopK(StringBuilder o, int delta, int runs)
    {
        // Size the TOP-K workload to ~1M append-only inserts, independent of the outer
        // width/join ticks. q18/q19 never retract (bids only insert), so per-partition
        // state GROWS — that growth, and the whole-row compare/hash it drives, is the cost.
        const int inserts = 1_000_000;
        var tkTicks = Math.Max(1, inserts / delta);

        o.AppendLine("## Partitioned TOP-K: wide-stored vs narrow-stored ranking state (§22)");
        o.AppendLine();
        o.AppendLine(
            "The q18/q19 site. `PartitionedTopKOp` keeps, per partition, a " +
            "`SortedDictionary<wideRow,long>` (`_accum`) ordered by the ORDER BY key with a " +
            "full-row tiebreak, plus a `Dictionary<wideRow,long>` window — both keyed by the " +
            "**whole 7-column bid row** (incl. the `url`/`extra` strings), even though ranking " +
            "needs only `{partition, order}`. `wide` stores the full row in that state; " +
            "`narrow` stores `{order, rowRef}` and **materialises the full row from a pool only " +
            "for the ≤k survivors** it outputs. Output is wide in BOTH (the fetch-back recovers " +
            "it), so the prize is purely the cheaper in-state compare+hash — the in-`Step` op " +
            "term the W>1 step decomposition attributes to the operator (not the exchange).");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {tkTicks:N0} ticks × {delta} rows/tick (~{inserts:N0} append-only inserts), " +
            $"median of {runs} runs. `narrow+fetch` is the real net (recovers wide survivors); " +
            "`narrow-raw` emits the narrow row (no recovery) — the unreachable lower bound.");
        o.AppendLine();
        o.AppendLine(
            "`wide·sorted` keeps the WIDE row but swaps the `Dictionary` window for a " +
            "comparison-based `SortedDictionary` (no whole-row HASH; the prize §19's " +
            "container swap left on the table — but a TOP-K container change is off-limits " +
            "after §19's unexplained q9 regression). `wide → wide·sorted` = the *kill-the-hash* " +
            "share; `wide·sorted → narrow+fetch` = the *shrink-the-key* residual the real " +
            "row-narrowing rewrite must earn against its extractor + fetch-back cost.");
        o.AppendLine();
        o.AppendLine("| Shape | wide ns | wide·sorted ns | narrow+fetch ns | n+f B | total prize % | kill-hash % | shrink-key % | narrow-raw ns |");
        o.AppendLine("|:--|--:|--:|--:|--:|--:|--:|--:|--:|");

        // q18: TOP-1, PARTITION BY (bidder,auction), ORDER BY date_time DESC. Many tiny
        // partitions (most pairs see ~1 bid) — large span ⇒ near-size-1 sorted dicts.
        MeasureTopKPair(o, "q18 (TOP-1, tiny partitions)", tkTicks, delta, runs, 1, 512_000);

        // q19: TOP-10, PARTITION BY auction, ORDER BY price DESC. Few partitions, each
        // accumulating many bids ⇒ growing sorted tree + churning top-10.
        MeasureTopKPair(o, "q19 (TOP-10, accumulating)", tkTicks, delta, runs, 10, 4_096);
    }

    private static void MeasureTopKPair(
        StringBuilder o, string label, int ticks, int delta, int runs, long limit, int partitionSpan)
    {
        // Pre-generate a pool of wide bid rows ONCE (outside the timed loop). The order
        // value is pseudo-shuffled so the sorted dict genuinely churns and the top-k
        // moves; the partition is index % span (the partition cardinality dial). The
        // url/extra strings come from a small pooled set (real hash cost, no per-row alloc).
        var poolSize = Math.Max(delta * 64, 1 << 20);
        var pool = new WBid[poolSize];
        var channels = new[] { "channel-A", "channel-B", "channel-C", "channel-D" };
        var urls = new[]
        {
            "https://www.nexmark.com/item/12345/details",
            "https://www.nexmark.com/item/67890/bid",
            "https://auction.example.org/p/abcdef/view",
        };
        for (var i = 0; i < poolSize; i++)
        {
            var order = (i * 2654435761L) & 0x7fff_ffff; // pseudo-shuffle the ORDER BY key
            var part = i % partitionSpan;
            pool[i] = new WBid(
                part, part >> 1, order, order,
                channels[i & 3], urls[i % 3], "extra-payload-" + (i & 0xff));
        }

        var wide = TimeMedian(runs, () => RunTopKWide(ticks, delta, limit, pool, sortedWindow: false));
        var wideSorted = TimeMedian(runs, () => RunTopKWide(ticks, delta, limit, pool, sortedWindow: true));
        var nf = TimeMedian(runs, () => RunTopKNarrow(ticks, delta, limit, pool, fetch: true));
        var raw = TimeMedian(runs, () => RunTopKNarrow(ticks, delta, limit, pool, fetch: false));
        var totalPct = wide.Ns > 0 ? 100.0 * (wide.Ns - nf.Ns) / wide.Ns : 0;
        var killHashPct = wide.Ns > 0 ? 100.0 * (wide.Ns - wideSorted.Ns) / wide.Ns : 0;
        var shrinkKeyPct = wide.Ns > 0 ? 100.0 * (wideSorted.Ns - nf.Ns) / wide.Ns : 0;

        o.AppendLine(
            $"| {label} | {wide.Ns:F1} | {wideSorted.Ns:F1} | {nf.Ns:F1} | {nf.Bytes:F0} | " +
            $"{totalPct:F0}% | {killHashPct:F0}% | {shrinkKeyPct:F0}% | {raw.Ns:F1} |");
        Console.WriteLine(
            $"  topk {label,-30} wide {wide.Ns,6:F1}  w·sorted {wideSorted.Ns,6:F1}  n+fetch {nf.Ns,6:F1}  raw {raw.Ns,6:F1} " +
            $"| total {totalPct:F0}% killhash {killHashPct:F0}% shrinkkey {shrinkKeyPct:F0}%");
    }

    // Order WBid by the ORDER BY value DESC, with a full-row tiebreak (the operator's
    // `_order` refines the sort-key-only comparer to a total order over whole rows).
    private static readonly IComparer<WBid> WideOrder = Comparer<WBid>.Create((a, b) =>
    {
        var c = b.Price.CompareTo(a.Price); // DESC on the ORDER BY value (Price≡DateTime here)
        if (c != 0)
        {
            return c;
        }

        // Total-order tiebreak over the WHOLE row (the operator's `_order` refines the
        // sort-key-only comparer to a full-row order, so distinct rows are distinct keys).
        c = a.Auction.CompareTo(b.Auction);
        if (c != 0)
        {
            return c;
        }

        c = a.Bidder.CompareTo(b.Bidder);
        if (c != 0)
        {
            return c;
        }

        c = a.DateTime.CompareTo(b.DateTime);
        if (c != 0)
        {
            return c;
        }

        c = string.CompareOrdinal(a.Url, b.Url);
        if (c != 0)
        {
            return c;
        }

        c = string.CompareOrdinal(a.Extra, b.Extra);
        return c != 0 ? c : string.CompareOrdinal(a.Channel, b.Channel);
    });

    private static readonly IComparer<NarrowKey> NarrowOrder = Comparer<NarrowKey>.Create((a, b) =>
    {
        var c = b.Order.CompareTo(a.Order); // DESC, matching WideOrder
        return c != 0 ? c : a.Ref.CompareTo(b.Ref);
    });

    // Today's model: store the WHOLE wide row in `_accum` (sorted) and `_window`
    // (hashed). Per tick: partition + sorted-insert each delta row, then for each touched
    // partition recompute the top-k window (Dictionary<wideRow,long>), diff it against
    // last tick's, and emit the diff to a wide output builder.
    private static (long, long) RunTopKWide(int ticks, int delta, long limit, WBid[] pool, bool sortedWindow)
    {
        long n = 0, sink = 0;
        var accum = new Dictionary<long, SortedDictionary<WBid, long>>();
        // window keyed by the WHOLE wide row: a Dictionary (whole-row HASH each recompute)
        // or a comparison-based SortedDictionary (no hash — the §19-forbidden container).
        var window = new Dictionary<long, IDictionary<WBid, long>>();
        var touched = new HashSet<long>();
        for (var t = 0; t < ticks; t++)
        {
            touched.Clear();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var row = pool[Idx(seed, i, pool.Length)];
                var key = row.Auction; // PARTITION BY (encoded into Auction)
                touched.Add(key);
                if (!accum.TryGetValue(key, out var bucket))
                {
                    bucket = new SortedDictionary<WBid, long>(WideOrder);
                    accum[key] = bucket;
                }

                bucket.TryGetValue(row, out var cur);
                bucket[row] = cur + 1;
                n++;
            }

            var builder = new ZSetBuilder<WBid, Z64>();
            foreach (var key in touched)
            {
                IDictionary<WBid, long> newWindow = sortedWindow
                    ? new SortedDictionary<WBid, long>(WideOrder)
                    : new Dictionary<WBid, long>();
                if (accum.TryGetValue(key, out var bucket))
                {
                    long pos = 0;
                    foreach (var (row, w) in bucket)
                    {
                        var take = Math.Min(w, limit - pos);
                        if (take > 0)
                        {
                            newWindow[row] = take;
                        }

                        pos += w;
                        if (pos >= limit)
                        {
                            break;
                        }
                    }
                }

                window.TryGetValue(key, out var oldWindow);
                foreach (var (row, w) in newWindow)
                {
                    var old = 0L;
                    oldWindow?.TryGetValue(row, out old);
                    if (w != old)
                    {
                        builder.Add(row, new Z64(w - old));
                    }
                }

                if (oldWindow is not null)
                {
                    foreach (var (row, old) in oldWindow)
                    {
                        if (!newWindow.ContainsKey(row))
                        {
                            builder.Add(row, new Z64(-old));
                        }
                    }
                }

                window[key] = newWindow;
            }

            foreach (var (_, w) in builder.Build())
            {
                sink += w.Value;
            }
        }

        return (n, sink);
    }

    // The §22 candidate: store NARROW {order, rowRef} in `_accum`/`_window`; recover the
    // wide row from the pool only for the ≤k survivors the output needs (fetch=true). The
    // partition key is still taken from the wide input row before narrowing (identical to
    // `wide`). fetch=false emits the narrow row (the unreachable lower bound — no real
    // output). Everything else mirrors RunTopKWide exactly.
    private static (long, long) RunTopKNarrow(int ticks, int delta, long limit, WBid[] pool, bool fetch)
    {
        long n = 0, sink = 0;
        var accum = new Dictionary<long, SortedDictionary<NarrowKey, long>>();
        var window = new Dictionary<long, Dictionary<NarrowKey, long>>();
        var touched = new HashSet<long>();
        for (var t = 0; t < ticks; t++)
        {
            touched.Clear();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var refIdx = Idx(seed, i, pool.Length);
                var row = pool[refIdx];
                var key = row.Auction;
                touched.Add(key);
                if (!accum.TryGetValue(key, out var bucket))
                {
                    bucket = new SortedDictionary<NarrowKey, long>(NarrowOrder);
                    accum[key] = bucket;
                }

                var stored = new NarrowKey(row.Price, refIdx);
                bucket.TryGetValue(stored, out var cur);
                bucket[stored] = cur + 1;
                n++;
            }

            var builder = new ZSetBuilder<WBid, Z64>();
            foreach (var key in touched)
            {
                var newWindow = new Dictionary<NarrowKey, long>();
                if (accum.TryGetValue(key, out var bucket))
                {
                    long pos = 0;
                    foreach (var (row, w) in bucket)
                    {
                        var take = Math.Min(w, limit - pos);
                        if (take > 0)
                        {
                            newWindow[row] = take;
                        }

                        pos += w;
                        if (pos >= limit)
                        {
                            break;
                        }
                    }
                }

                window.TryGetValue(key, out var oldWindow);
                foreach (var (row, w) in newWindow)
                {
                    var old = 0L;
                    oldWindow?.TryGetValue(row, out old);
                    if (w != old)
                    {
                        // Survivor → recover the wide row from the pool for output.
                        if (fetch)
                        {
                            builder.Add(pool[row.Ref], new Z64(w - old));
                        }
                        else
                        {
                            sink += w - old;
                        }
                    }
                }

                if (oldWindow is not null)
                {
                    foreach (var (row, old) in oldWindow)
                    {
                        if (!newWindow.ContainsKey(row))
                        {
                            if (fetch)
                            {
                                builder.Add(pool[row.Ref], new Z64(-old));
                            }
                            else
                            {
                                sink -= old;
                            }
                        }
                    }
                }

                window[key] = newWindow;
            }

            if (fetch)
            {
                foreach (var (_, w) in builder.Build())
                {
                    sink += w.Value;
                }
            }
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
