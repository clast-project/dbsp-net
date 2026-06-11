// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Benchmarks;

/// <summary>
/// §22.8 — prices the <b>only</b> width-attributable phase of q18's parallel step that
/// a shuffle-narrowing build could touch: the exchange <b>gather</b>'s whole-row hash.
/// <c>stepprofile</c> (§15/§22.2) splits the step into <c>split</c> (bucket rows by
/// <c>hash(partitionKey)%W</c> — copies a row <i>reference</i> and hashes only the 2-col
/// partition key, so width-independent), <c>wait</c> (barrier — coordination, not a
/// target), <c>op</c> (the TOP-K operator, q18's per-row floor), and <c>gather</c>: rebuild
/// the post-shuffle <see cref="IndexedZSet{TKey,TRow,TWeight}"/>, hashing each delta row
/// into the inner per-key Z-set. That inner hash is the wide row; narrowing the payload
/// flowing through the shuffle to <c>{order, ref}</c> shrinks it. This A/Bs exactly that
/// inner build — wide 7-col bid row vs narrow <c>{order, ref}</c> — over a per-tick delta,
/// mirroring <see cref="DbspNet.Core.Circuit.Operators.ExchangeIndexOp{TKey,TRow,TWeight}"/>'s
/// gather loop.
/// </summary>
/// <remarks>
/// The row is a value-type <see cref="record struct"/> with a structural (uncached) hash —
/// faithful to the typed W&gt;1 path the parallel q18 actually runs (the single-circuit
/// <c>StructuralRow</c> caches its hash, §22.6). The gather processes the per-tick delta
/// (≈ batch/W rows), <i>not</i> accumulated state, so unlike the TOP-K <c>_accum</c> its
/// inner groups are delta-bounded; the partition span dials how many rows share a key per
/// tick. <c>wide → narrow</c> is the whole-row-hash share of gather — the ceiling of the
/// shuffle-narrowing lever, to be multiplied by gather's small share of step.
/// </remarks>
internal static class ExchangeGatherBenchmark
{
    private readonly record struct WBid(
        long Auction, long Bidder, long Price, long DateTime,
        string Channel, string Url, string Extra);

    // The payload a shuffle-narrowing build would route through the exchange instead of
    // the wide bid: the partition key (carried so the gather can re-index), the ORDER BY
    // value, and a back-reference into a wide-row recovery store. q18 partitions by
    // (bidder, auction) — two longs — so the narrow payload is 4 longs vs the bid's
    // 4 longs + 3 string refs.
    private readonly record struct NarrowMove(long Bidder, long Auction, long Order, long Ref);

    private const int Workers = 8;

    public static void Run(StringBuilder o, int delta, int runs)
    {
        const int inserts = 1_000_000;
        var ticks = Math.Max(1, inserts / delta);

        Console.WriteLine();
        Console.WriteLine($"=== exchange-gather wide vs narrow (ticks={ticks:N0}, D={delta}, runs={runs}) ===");

        o.AppendLine("# DbspNet — exchange-gather whole-row-hash share (§22.8)");
        o.AppendLine();
        o.AppendLine(
            "The exchange `gather` rebuilds the post-shuffle `IndexedZSet` by hashing each " +
            "per-tick delta row into the inner per-key Z-set (`ExchangeIndexOp.Step`). That " +
            "whole-row hash is the **only** width-attributable cost a shuffle-narrowing build " +
            "could remove — `split` copies a row reference and hashes only the partition key " +
            "(width-independent), `wait` is coordination, `op` is the TOP-K floor. This A/Bs " +
            "the gather inner build with the wide 7-col bid row vs a narrow `{order, ref}` " +
            "payload; the row is a value-type with an uncached structural hash (the typed W>1 " +
            "path q18 runs).");
        o.AppendLine();
        o.AppendLine(
            $"Stream: {ticks:N0} ticks × {delta} rows/tick (~{inserts:N0} delta rows), median " +
            $"of {runs} runs. `wide→narrow %` is the whole-row-hash share of gather — the " +
            "shuffle-narrowing ceiling, before multiplying by gather's (small) share of step.");
        o.AppendLine();
        o.AppendLine(
            "Two phases per shape: **gather** (the index-rebuild hash only) and **split+gather** " +
            $"(the full per-worker move — bucket each row into W={Workers} destinations by " +
            "`hash(partitionKey)%W`, then rebuild). The typed path exchanges *value structs*, so " +
            "`split` copies the whole payload by value into the bucket list — making it " +
            "width-attributable too, not just gather.");
        o.AppendLine();
        o.AppendLine("| Shape | phase | wide ns/row | narrow ns/row | wide→narrow % | wide B/row | narrow B/row |");
        o.AppendLine("|:--|:--|--:|--:|--:|--:|--:|");

        // q18: PARTITION BY (bidder,auction) — huge span ⇒ ~1 delta row per key per tick.
        MeasureShape(o, "q18 (tiny partitions, ~1 row/key/tick)", ticks, delta, runs, 512_000);
        // q19: PARTITION BY auction — small span ⇒ several delta rows per key per tick.
        MeasureShape(o, "q19 (accumulating, several rows/key/tick)", ticks, delta, runs, 4_096);

        o.AppendLine();
        o.AppendLine(
            "**Reading it.** `wide→narrow %` is how much of that phase the narrow payload saves. " +
            "Multiply the **split+gather** save by the movement share of step (`stepprofile`: q18 " +
            "split+gather ≈ 25–37 % of step; the rest is `wait` coordination and `op`, the TOP-K " +
            "per-row floor §22.6 measured flat to narrowing on q18's size-1 partitions). The lever " +
            "cannot touch `op`, so its whole-query ceiling is `(split+gather save) × move%`.");
        o.AppendLine();
    }

    private static void MeasureShape(StringBuilder o, string label, int ticks, int delta, int runs, int span)
    {
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
            var order = (i * 2654435761L) & 0x7fff_ffff;
            var part = i % span;
            pool[i] = new WBid(part, part >> 1, order, order, channels[i & 3], urls[i % 3], "extra-payload-" + (i & 0xff));
        }

        Emit(o, label, "gather",
            TimeMedian(runs, () => RunGatherWide(ticks, delta, pool)),
            TimeMedian(runs, () => RunGatherNarrow(ticks, delta, pool)));
        Emit(o, label, "split+gather",
            TimeMedian(runs, () => RunMoveWide(ticks, delta, pool)),
            TimeMedian(runs, () => RunMoveNarrow(ticks, delta, pool)));
    }

    private static void Emit(StringBuilder o, string label, string phase, (long Ns, long Bytes) wide, (long Ns, long Bytes) narrow)
    {
        var pct = wide.Ns > 0 ? 100.0 * (wide.Ns - narrow.Ns) / wide.Ns : 0;
        o.AppendLine(
            $"| {label} | {phase} | {wide.Ns:F1} | {narrow.Ns:F1} | {pct:F0}% | {wide.Bytes:F0} | {narrow.Bytes:F0} |");
        Console.WriteLine(
            $"  {phase,-12} {label,-42} wide {wide.Ns,6:F1}  narrow {narrow.Ns,6:F1}  save {pct:F0}%  " +
            $"B {wide.Bytes:F0}/{narrow.Bytes:F0}");
    }

    // ---- gather only: the index-rebuild hash (mirrors ExchangeIndexOp's gather loop) ----

    private static (long Ns, long Bytes) RunGatherWide(int ticks, int delta, WBid[] pool)
    {
        long sink = 0;
        var b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (var t = 0; t < ticks; t++)
        {
            var indexed = new IndexedZSetBuilder<long, WBid, Z64>();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var row = pool[Idx(seed, i, pool.Length)];
                indexed.Add(row.Auction, row, new Z64(1));
            }

            sink += indexed.Build().GroupCount;
        }

        sw.Stop();
        var bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
        return (NsPerRow(sw, ticks, delta, sink), bytes / (long)ticks / delta);
    }

    private static (long Ns, long Bytes) RunGatherNarrow(int ticks, int delta, WBid[] pool)
    {
        long sink = 0;
        var b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (var t = 0; t < ticks; t++)
        {
            var indexed = new IndexedZSetBuilder<long, NarrowMove, Z64>();
            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var idx = Idx(seed, i, pool.Length);
                var row = pool[idx];
                indexed.Add(row.Auction, new NarrowMove(row.Bidder, row.Auction, row.Price, idx), new Z64(1));
            }

            sink += indexed.Build().GroupCount;
        }

        sw.Stop();
        var bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
        return (NsPerRow(sw, ticks, delta, sink), bytes / (long)ticks / delta);
    }

    // ---- split + gather: the full per-worker move (mirrors ExchangeIndexOp.Step) ----
    // Bucket each delta row into W destinations by hash(partitionKey)%W, copying the
    // payload by value into the bucket list, then gather all buckets into the index.

    private static (long Ns, long Bytes) RunMoveWide(int ticks, int delta, WBid[] pool)
    {
        long sink = 0;
        var buckets = new List<WBid>[Workers];
        for (var j = 0; j < Workers; j++)
        {
            buckets[j] = new List<WBid>(delta);
        }

        var b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (var t = 0; t < ticks; t++)
        {
            for (var j = 0; j < Workers; j++)
            {
                buckets[j].Clear();
            }

            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var row = pool[Idx(seed, i, pool.Length)];
                buckets[PartHash(row.Bidder, row.Auction) % Workers].Add(row); // value-copy the wide struct
            }

            var indexed = new IndexedZSetBuilder<long, WBid, Z64>();
            for (var j = 0; j < Workers; j++)
            {
                foreach (var row in buckets[j])
                {
                    indexed.Add(row.Auction, row, new Z64(1));
                }
            }

            sink += indexed.Build().GroupCount;
        }

        sw.Stop();
        var bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
        return (NsPerRow(sw, ticks, delta, sink), bytes / (long)ticks / delta);
    }

    private static (long Ns, long Bytes) RunMoveNarrow(int ticks, int delta, WBid[] pool)
    {
        long sink = 0;
        var buckets = new List<NarrowMove>[Workers];
        for (var j = 0; j < Workers; j++)
        {
            buckets[j] = new List<NarrowMove>(delta);
        }

        var b0 = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (var t = 0; t < ticks; t++)
        {
            for (var j = 0; j < Workers; j++)
            {
                buckets[j].Clear();
            }

            var seed = (long)t * delta;
            for (var i = 0; i < delta; i++)
            {
                var idx = Idx(seed, i, pool.Length);
                var row = pool[idx];
                // narrow the payload at the source, then value-copy the small struct.
                buckets[PartHash(row.Bidder, row.Auction) % Workers].Add(new NarrowMove(row.Bidder, row.Auction, row.Price, idx));
            }

            var indexed = new IndexedZSetBuilder<long, NarrowMove, Z64>();
            for (var j = 0; j < Workers; j++)
            {
                foreach (var nm in buckets[j])
                {
                    indexed.Add(nm.Auction, nm, new Z64(1));
                }
            }

            sink += indexed.Build().GroupCount;
        }

        sw.Stop();
        var bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
        return (NsPerRow(sw, ticks, delta, sink), bytes / (long)ticks / delta);
    }

    private static int PartHash(long bidder, long auction)
        => (int)((uint)HashCode.Combine(bidder, auction) & 0x7fff_ffff);

    private static long Idx(long seed, int i, int n) => (long)((ulong)((seed + i) * 2654435761L) % (ulong)n);

    private static long NsPerRow(Stopwatch sw, int ticks, int delta, long guard)
    {
        _ = guard;
        return (long)(sw.Elapsed.TotalNanoseconds / ((double)ticks * delta));
    }

    private static (long Ns, long Bytes) TimeMedian(int runs, Func<(long Ns, long Bytes)> f)
    {
        f(); // warmup
        var ns = new List<long>();
        long bytes = 0;
        for (var r = 0; r < runs; r++)
        {
            var (n, b) = f();
            ns.Add(n);
            bytes = b;
        }

        ns.Sort();
        return (ns[ns.Count / 2], bytes);
    }
}
