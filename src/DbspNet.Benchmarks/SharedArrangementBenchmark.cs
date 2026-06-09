// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Benchmarks;

/// <summary>
/// Gate for the cross-operator shared-arrangement increment
/// (docs/design-row-representation.md §6.2 — Option 2). When a relation
/// <c>R</c> is arranged by the same key for <c>F</c> downstream joins, the
/// UNSHARED baseline integrates <c>R</c>'s per-tick delta into <c>F</c> private
/// right traces; the SHARED build integrates it once (an
/// <see cref="ArrangeOp{TKey,TValue,TWeight}"/>) and the <c>F</c> joins read
/// that one integral via <c>IncrementalInnerJoinSharedRight</c>. This A/Bs the
/// two builds in one process — same inputs, same join math, same output — and
/// reports the per-Step speedup as a function of fan-out <c>F</c> and tick width
/// <c>D</c>.
/// </summary>
/// <remarks>
/// Both builds run the identical cross-product / output build / left-trace
/// integrate per join; the ONLY difference is how many times <c>R</c>'s delta is
/// integrated (<c>F</c> vs 1). So the speedup measures exactly the duplicated
/// shared-relation maintenance that the arrangement removes — it grows with
/// <c>F</c> and with <c>R</c>'s share of per-tick work. <c>R</c> rows are
/// deliberately wide so the integrate (a structural hash/merge of the value
/// rows) is a real cost. This is the FLAT arrangement; the spine variant — where
/// the saved cost is the heavier sorted-batch build, the §8.3 q4 bottleneck — is
/// a follow-up.
/// </remarks>
internal static class SharedArrangementBenchmark
{
    private static readonly int[] FanOuts = { 2, 4, 8 };
    private static readonly int[] DeltaWidths = { 256, 1024, 4096 };
    private const int StateKeys = 20_000;
    private const int GroupSize = 2;

    // IComparable so the spine traces (sorted-columnar batches) can order rows.
    private readonly record struct LeftRow(int Key) : IComparable<LeftRow>
    {
        public int CompareTo(LeftRow other) => Key.CompareTo(other.Key);
    }

    // Wide shared-relation row: the integrate the arrangement deduplicates pays
    // a real structural hash (flat) or sorted-batch build (spine) per value row.
    private readonly record struct Fact(int Key, long F1, long F2, long F3, long F4, long F5) : IComparable<Fact>
    {
        public int CompareTo(Fact other)
        {
            var c = Key.CompareTo(other.Key);
            if (c != 0) { return c; }
            c = F1.CompareTo(other.F1);
            if (c != 0) { return c; }
            c = F2.CompareTo(other.F2);
            if (c != 0) { return c; }
            c = F3.CompareTo(other.F3);
            if (c != 0) { return c; }
            c = F4.CompareTo(other.F4);
            return c != 0 ? c : F5.CompareTo(other.F5);
        }
    }

    private readonly record struct OutRow(int Key, long V);

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Shared arrangement (build R's index once vs per-consumer) ===");

        output.AppendLine("## Shared arrangement — one shared index vs F private right traces");
        output.AppendLine();
        output.AppendLine(
            "Gate for docs/design-row-representation.md §6.2 / §9 (Option 2, cross-operator " +
            "shared arrangements). A relation `R` (wide rows) is joined by `F` downstream " +
            "joins on the same key. **Unshared** = `F` plain inner joins, each maintaining " +
            "its own right trace. **Shared** = one `Arrange(R)` + `F` shared-right joins " +
            "reading that single integral. Same inputs, same join math, output verified " +
            "identical. Each tick pushes a `D`-key delta into `R` and into each of the " +
            "`F` left relations, alternating +1/-1 so state stays bounded. Times are " +
            "median ns per **Step**; **Speedup** is unshared/shared (>1 = sharing wins). " +
            "Both substrates are measured: **flat** (dictionary trace) and **spine** " +
            "(LSM sorted-columnar trace — where the deduplicated maintenance is the " +
            $"expensive batch build, the §8.3 q4 cost). State: {StateKeys:N0} keys/relation, " +
            $"group size {GroupSize}.");
        output.AppendLine();

        // Correctness self-check before timing (cheap insurance the A/B is honest).
        VerifyEquivalence(spine: false);
        VerifyEquivalence(spine: true);
        Console.WriteLine("  Output equivalence (shared == unshared, flat + spine): OK");
        output.AppendLine("Output equivalence (shared vs unshared) verified before timing on both substrates. ");
        output.AppendLine();

        EmitGrid(output, spine: false);
        EmitGrid(output, spine: true);

        output.AppendLine("## Reading");
        output.AppendLine();
        output.AppendLine(
            "- **Sharing wins on both substrates, and the win grows with fan-out.** The " +
            "unshared build maintains `R`'s delta in `F` private right traces; the shared " +
            "build does it once, so the duplicated maintenance removed grows with `F`. " +
            "Across repeated runs both substrates land ~1.0–1.1× at `F=2` and climb to " +
            "~1.2–1.4× at `F=8` (cells carry ~±0.1× jitter; read the trend in `F`).");
        output.AppendLine(
            "- **Spine sharing is NOT bigger than flat sharing — it is comparable, often " +
            "slightly smaller in ratio.** This refutes the §8.3-derived hypothesis. The " +
            "reason is in the absolutes: at, say, `F=8, D=1024`, sharing removes a *similar " +
            "absolute* per-tick cost on both substrates (~1.3–1.8 ms), but spine's total " +
            "Step is ~1.5–2× the flat Step because its probe (`GroupForManySorted` across " +
            "the batches) is heavier — so the same absolute saving is a *smaller fraction* " +
            "of the spine baseline, and the ratio is diluted.");
        output.AppendLine(
            "- **Why: the deduplicated cost is not the dominant per-tick term.** What " +
            "sharing removes is `R`'s per-tick maintenance (the integrate — a dict merge on " +
            "flat, a small batch build + amortised compaction on spine). What it does NOT " +
            "remove is the per-consumer **probe** of `R_t` (each join still probes with its " +
            "own left delta) plus the cross-product, output build, and left integrate. On " +
            "the spine the probe is the larger term, so deduplicating the integrate moves " +
            "the ratio less, not more.");
        output.AppendLine();
        output.AppendLine("## Verdict");
        output.AppendLine();
        output.AppendLine(
            "Cross-operator shared arrangements work and are correct on both substrates " +
            "(output verified identical to independent joins, here and in " +
            "`SharedArrangementTests`). The realistic win is **modest and fan-out-scaling " +
            "on both — ~1.0–1.4×**, and — contrary to the §8.3 expectation — the spine win " +
            "is **not** larger than the flat win. The honest correction to §8.3: " +
            "cross-operator sharing deduplicates a relation's per-tick *maintenance*, but " +
            "the spine substrate's disadvantage on q4 is the per-tick *rebuild* paid even " +
            "with no reuse — a cross-tick / amortisation problem, not a cross-operator one — " +
            "and q4 has no shareable arrangement anyway (§6.2). So sharing is a real, " +
            "broad-surface win for fan-out / star-schema / repeated-CTE shapes, **not** the " +
            "lever that flips spine past flat. The reusable abstraction (`IArrangement` / " +
            "`ISpineArrangement` + `Arrange` / `SpineArrange` + the shared-right joins) is " +
            "in place on both substrates; the remaining follow-up is an **arrangement-CSE " +
            "optimizer rule** so real SQL with a relation joined by the same key in ≥2 " +
            "places routes through one arrangement automatically (today the feature is " +
            "reachable only via the Core builder API).");
        output.AppendLine();
    }

    /// <summary>Emits the F×D unshared/shared/speedup table for one substrate.</summary>
    private static void EmitGrid(StringBuilder output, bool spine)
    {
        var name = spine ? "Spine" : "Flat";
        Console.WriteLine($"  --- {name} substrate ---");
        output.AppendLine($"### {name} substrate");
        output.AppendLine();

        // Process-wide warmup so the first timed config of this substrate doesn't
        // eat tiered-JIT promotion of its generic operator instantiations.
        _ = MeasureStep(FanOuts[^1], DeltaWidths[^1], shared: false, spine: spine);
        _ = MeasureStep(FanOuts[^1], DeltaWidths[^1], shared: true, spine: spine);

        output.AppendLine("| Fan-out F | D (keys/tick) | Unshared | Shared | Speedup |");
        output.AppendLine("|----------:|--------------:|---------:|-------:|--------:|");

        foreach (var f in FanOuts)
        {
            foreach (var d in DeltaWidths)
            {
                var unshared = MeasureStep(f, d, shared: false, spine: spine);
                var shared = MeasureStep(f, d, shared: true, spine: spine);
                var speedup = shared > 0 ? unshared / shared : 0.0;

                Console.WriteLine(
                    $"    F={f} D={d,-5} unshared={FmtNs(unshared)} shared={FmtNs(shared)} " +
                    $"{BenchmarkHarness.FormatRatio(speedup).Trim()}");
                output.AppendLine(
                    $"| {f,9} | {d,13} | {FmtNs(unshared)} | {FmtNs(shared)} | " +
                    $"{BenchmarkHarness.FormatRatio(speedup).Trim()} |");
            }
        }

        output.AppendLine();
    }

    /// <summary>
    /// Median ns per <c>Step</c> for a fan-out-<paramref name="f"/> pipeline with
    /// <paramref name="d"/>-key per-tick deltas, either sharing one arrangement of
    /// <c>R</c> or giving each join a private right trace.
    /// </summary>
    private static double MeasureStep(int f, int d, bool shared, bool spine)
    {
        var (circuit, lefts, r, _) = Build(f, StateKeys, shared, spine);

        // D keys evenly spread over the populated keyspace, sorted.
        var keys = new int[d];
        for (var i = 0; i < d; i++)
        {
            keys[i] = (int)((long)i * StateKeys / d);
        }

        var rPlus = ZSet.FromEntries(keys.SelectMany(k => Enumerable.Range(0, GroupSize)
            .Select(g => (MakeFact(k, g), Z64.One))));
        var rMinus = ZSet.FromEntries(keys.SelectMany(k => Enumerable.Range(0, GroupSize)
            .Select(g => (MakeFact(k, g), new Z64(-1)))));
        var lPlus = ZSet.FromEntries(keys.Select(k => (new LeftRow(k), Z64.One)));
        var lMinus = ZSet.FromEntries(keys.Select(k => (new LeftRow(k), new Z64(-1))));

        void PushPair(bool plus)
        {
            r.Push(plus ? rPlus : rMinus);
            foreach (var lh in lefts)
            {
                lh.Push(plus ? lPlus : lMinus);
            }
        }

        // Warm up (JIT + caches) and settle state back to the preloaded baseline.
        for (var i = 0; i < 20; i++)
        {
            PushPair(i % 2 == 0);
            circuit.Step();
        }

        // Reset heap state before timing so a prior cell's garbage doesn't
        // trigger a collection mid-sample (the dominant source of cell variance).
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();

        const int samples = 41;
        var times = new List<double>(samples);
        var sw = new Stopwatch();
        for (var s = 0; s < samples; s++)
        {
            sw.Restart();
            PushPair(true);
            circuit.Step();
            PushPair(false);
            circuit.Step();
            sw.Stop();
            times.Add(sw.Elapsed.TotalNanoseconds / 2.0);
        }

        times.Sort();
        return times[times.Count / 2];
    }

    /// <summary>
    /// Builds the fan-out pipeline. Preloads <c>R</c> and each left relation with
    /// <paramref name="n"/> distinct keys so the joins probe a realistic state.
    /// </summary>
    private static (RootCircuit Circuit, InputHandle<ZSet<LeftRow, Z64>>[] Lefts, InputHandle<ZSet<Fact, Z64>> R, OutputHandle<ZSet<OutRow, Z64>>[] Outs)
        Build(int f, int n, bool shared, bool spine)
    {
        var lefts = new InputHandle<ZSet<LeftRow, Z64>>[f];
        var outs = new OutputHandle<ZSet<OutRow, Z64>>[f];
        InputHandle<ZSet<Fact, Z64>>? ri = null;

        var circuit = RootCircuit.Build(b =>
        {
            var (rh, rs) = b.ZSetInput<Fact, Z64>();
            ri = rh;
            var rIdx = b.IndexBy(rs, x => x.Key);
            IArrangement<int, Fact, Z64>? flatArr = shared && !spine ? b.Arrange(rIdx) : null;
            ISpineArrangement<int, Fact, Z64>? spineArr = shared && spine ? b.SpineArrange(rIdx) : null;

            for (var i = 0; i < f; i++)
            {
                var (lh, ls) = b.ZSetInput<LeftRow, Z64>();
                lefts[i] = lh;
                var lIdx = b.IndexBy(ls, x => x.Key);
                Stream<ZSet<OutRow, Z64>> joined = (shared, spine) switch
                {
                    (true, true) => b.SpineIncrementalInnerJoinSharedRight(
                        lIdx, rIdx, spineArr!, (key, _, fact) => new OutRow(key, fact.F1)),
                    (false, true) => b.SpineIncrementalInnerJoin(
                        lIdx, rIdx, (key, _, fact) => new OutRow(key, fact.F1)),
                    (true, false) => b.IncrementalInnerJoinSharedRight(
                        lIdx, rIdx, flatArr!, (key, _, fact) => new OutRow(key, fact.F1)),
                    (false, false) => b.IncrementalInnerJoin(
                        lIdx, rIdx, (key, _, fact) => new OutRow(key, fact.F1)),
                };
                outs[i] = b.Output(joined);
            }
        });

        // Preload state in chunks: R first, then each left, all with keys [0, n).
        const int chunk = 25_000;
        for (var start = 0; start < n; start += chunk)
        {
            var end = Math.Min(start + chunk, n);
            var rBuf = new List<(Fact, Z64)>((end - start) * GroupSize);
            var lBuf = new List<(LeftRow, Z64)>(end - start);
            for (var k = start; k < end; k++)
            {
                for (var g = 0; g < GroupSize; g++)
                {
                    rBuf.Add((MakeFact(k, g), Z64.One));
                }

                lBuf.Add((new LeftRow(k), Z64.One));
            }

            ri!.Push(ZSet.FromEntries(rBuf));
            var lz = ZSet.FromEntries(lBuf);
            foreach (var lh in lefts)
            {
                lh.Push(lz);
            }

            circuit.Step();
        }

        return (circuit, lefts, ri!, outs);
    }

    private static Fact MakeFact(int key, int g) =>
        new(key, key * 2L + g, key * 3L + g, key * 5L + g, key * 7L + g, key * 11L + g);

    /// <summary>
    /// Drives a small shared and unshared build with identical random deltas and
    /// asserts the per-tick outputs match, so the timed A/B is comparing equal work.
    /// </summary>
    private static void VerifyEquivalence(bool spine)
    {
        const int f = 4;
        var (sc, sl, sr, sOut) = Build(f, 200, shared: true, spine: spine);
        var (uc, ul, ur, uOut) = Build(f, 200, shared: false, spine: spine);

        var rng = new Random(20260608);
        for (var tick = 0; tick < 40; tick++)
        {
            var rz = ZSet.FromEntries(Enumerable.Range(0, rng.Next(0, 6)).Select(_ =>
            {
                var k = rng.Next(0, 50);
                return (MakeFact(k, rng.Next(0, GroupSize)), rng.Next(0, 2) == 0 ? Z64.One : new Z64(-1));
            }));
            sr.Push(rz);
            ur.Push(rz);
            for (var i = 0; i < f; i++)
            {
                var lz = ZSet.FromEntries(Enumerable.Range(0, rng.Next(0, 4)).Select(_ =>
                    (new LeftRow(rng.Next(0, 50)), rng.Next(0, 2) == 0 ? Z64.One : new Z64(-1))));
                sl[i].Push(lz);
                ul[i].Push(lz);
            }

            sc.Step();
            uc.Step();

            for (var i = 0; i < f; i++)
            {
                AssertEqual(uOut[i].Current, sOut[i].Current, tick, i);
            }
        }
    }

    private static void AssertEqual(ZSet<OutRow, Z64> expected, ZSet<OutRow, Z64> actual, int tick, int branch)
    {
        if (expected.Count != actual.Count)
        {
            throw new InvalidOperationException(
                $"shared != unshared at tick {tick} branch {branch}: count {actual.Count} vs {expected.Count}");
        }

        foreach (var (row, w) in expected)
        {
            if (!actual.WeightOf(row).Equals(w))
            {
                throw new InvalidOperationException(
                    $"shared != unshared at tick {tick} branch {branch}: weight of {row} is {actual.WeightOf(row)} vs {w}");
            }
        }
    }

    private static string FmtNs(double ns) =>
        ns switch
        {
            < 1_000.0 => $"{ns,7:F0} ns",
            < 1_000_000.0 => $"{ns / 1_000.0,7:F2} µs",
            _ => $"{ns / 1_000_000.0,7:F2} ms",
        };
}
