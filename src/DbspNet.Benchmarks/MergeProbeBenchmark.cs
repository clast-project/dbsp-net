// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Benchmarks;

/// <summary>
/// Prototype benchmark for the row-representation design
/// (docs/design-row-representation.md §6.1): does a batched, galloping
/// <b>merge</b> probe over the sorted spine batches beat a loop of
/// independent point probes for one tick's worth of group lookups?
/// </summary>
/// <remarks>
/// <para>Both strategies read the same <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/>
/// (the trace <c>SpineIncrementalAggregateOp</c> / the join operators hold) and
/// return the same integrated groups:</para>
/// <list type="bullet">
///   <item><b>Point probe</b> (baseline = today's operator inner loop): call
///   <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupFor"/> once per
///   delta key — a bloom-gated binary search across every batch, rebuilding a
///   <c>ZSetBuilder</c> (hashing each value) per probe.</item>
///   <item><b>Merge probe</b> (prototype): one
///   <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}.GroupForManySorted"/>
///   call over the pre-sorted delta keys — each batch's outer-key column is
///   walked once with a galloping cursor and matched groups are sliced straight
///   from the sorted value columns (no per-probe hashing).</item>
/// </list>
/// <para>The sweep is over state size <c>N</c> (distinct keys retained) and
/// delta width <c>D</c> (keys probed this tick). The §5 caveat predicts the
/// merge only wins once <c>D</c> is large enough to amortise the per-batch
/// cursor setup against the saved point-probe work; this table locates that
/// crossover. <c>present</c> rows probe keys that exist (the aggregate's own
/// changed groups); <c>absent</c> rows probe misses (the common case on a
/// join's probe side).</para>
/// </remarks>
internal static class MergeProbeBenchmark
{
    private static readonly int[] StateSizes = { 1_000, 100_000, 1_000_000 };
    private static readonly int[] DeltaWidths = { 1, 8, 64, 512, 4096 };
    private const int GroupSize = 4;

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Merge-probe prototype (spine indexed trace) ===");

        output.AppendLine("## Merge-probe prototype — point probe vs galloping merge");
        output.AppendLine();
        output.AppendLine(
            "Prototype for docs/design-row-representation.md §6.1. The spine " +
            "indexed trace is probed for one tick of `D` group keys two ways: a " +
            "loop of `GroupFor` point probes (today's `SpineIncrementalAggregateOp` " +
            "inner loop — bloom + binary search per batch, rebuilding a `ZSet` per " +
            "probe) versus a single `GroupForManySorted` call (each sorted batch " +
            "walked once with a galloping cursor, matched groups sliced from the " +
            "value columns with no rehash). Each group holds " + GroupSize +
            " values. Times are median ns for the **whole D-key tick**; " +
            "**Speedup** is point/merge (>1 = merge wins). `present` probes hit " +
            "existing keys; `absent` probes miss.");
        output.AppendLine();

        foreach (var n in StateSizes)
        {
            Console.WriteLine($"  Building spine trace N={n} (group size {GroupSize})…");
            var trace = BuildTrace(n, GroupSize);

            // Correctness gate: the prototype must return exactly what the
            // point-probe loop does before any timing is meaningful.
            Verify(trace, MakeProbeKeys(n, 257, present: true));
            Verify(trace, MakeProbeKeys(n, 257, present: false));
            Verify(trace, MakeProbeKeys(n, 13, present: true));

            EmitTable(output, trace, n, present: true);
            EmitTable(output, trace, n, present: false);
        }
    }

    private static void EmitTable(StringBuilder output, SpineIndexedZSetTrace<int, int, Z64> trace, int n, bool present)
    {
        output.AppendLine($"### N = {n:N0}, {(present ? "present" : "absent")} keys");
        output.AppendLine();
        output.AppendLine("| D (keys/tick) | Point probe | Merge probe | Speedup |");
        output.AppendLine("|--------------:|------------:|------------:|--------:|");

        foreach (var d in DeltaWidths)
        {
            var keys = MakeProbeKeys(n, d, present);

            var pointNs = MedianNsPerTick(() => PointProbe(trace, keys), d);
            var mergeNs = MedianNsPerTick(() => MergeProbe(trace, keys), d);
            var speedup = mergeNs > 0 ? pointNs / mergeNs : 0.0;

            Console.WriteLine(
                $"    N={n,-8} {(present ? "present" : "absent "),-7} D={d,-5} " +
                $"point={FmtNs(pointNs)} merge={FmtNs(mergeNs)} {BenchmarkHarness.FormatRatio(speedup).Trim()}");
            output.AppendLine(
                $"| {d,13} | {FmtNs(pointNs)} | {FmtNs(mergeNs)} | {BenchmarkHarness.FormatRatio(speedup).Trim()} |");
        }

        output.AppendLine();
    }

    private static long PointProbe(SpineIndexedZSetTrace<int, int, Z64> trace, int[] keys)
    {
        long sink = 0;
        foreach (var k in keys)
        {
            var g = trace.GroupFor(k);
            foreach (var (v, _) in g)
            {
                sink += v;
            }
        }

        return sink;
    }

    private static long MergeProbe(SpineIndexedZSetTrace<int, int, Z64> trace, int[] keys)
    {
        long sink = 0;
        foreach (var (_, group) in trace.GroupForManySorted(keys))
        {
            foreach (var (v, _) in group)
            {
                sink += v;
            }
        }

        return sink;
    }

    private static SpineIndexedZSetTrace<int, int, Z64> BuildTrace(int n, int groupSize)
    {
        var trace = new SpineIndexedZSetTrace<int, int, Z64>(TieredCompactionStrategy.Default);
        for (var i = 0; i < n; i++)
        {
            var b = new IndexedZSetBuilder<int, int, Z64>();
            for (var v = 0; v < groupSize; v++)
            {
                b.Add(i, v, new Z64(1));
            }

            trace.Integrate(b.Build());
        }

        return trace;
    }

    /// <summary>
    /// D probe keys, sorted ascending (the merge contract). "present" keys are
    /// spread evenly across <c>[0, N)</c>; "absent" keys across <c>[N, 2N)</c>.
    /// </summary>
    private static int[] MakeProbeKeys(int n, int d, bool present)
    {
        var keys = new int[d];
        var baseOffset = present ? 0 : n;
        for (var i = 0; i < d; i++)
        {
            // Even spread, distinct and already sorted for d <= n.
            keys[i] = baseOffset + (int)((long)i * n / d);
        }

        return keys;
    }

    /// <summary>
    /// Times one tick (a full D-key probe batch). Picks the inner repeat count
    /// so each sample does ~2k probes regardless of D — small-D ticks would
    /// otherwise round to zero against the stopwatch, large-D ticks are already
    /// long enough at one repeat.
    /// </summary>
    private static double MedianNsPerTick(Func<long> tick, int d)
    {
        var itersPerSample = Math.Max(1, 2048 / Math.Max(1, d));
        const int warmups = 5;
        const int samples = 25;

        long sink = 0;
        for (var i = 0; i < warmups; i++)
        {
            sink += tick();
        }

        var times = new List<double>(samples);
        var sw = new Stopwatch();
        for (var s = 0; s < samples; s++)
        {
            sw.Restart();
            for (var i = 0; i < itersPerSample; i++)
            {
                sink += tick();
            }

            sw.Stop();
            times.Add(sw.Elapsed.TotalNanoseconds / itersPerSample);
        }

        GC.KeepAlive(sink);
        times.Sort();
        return times[times.Count / 2];
    }

    private static void Verify(SpineIndexedZSetTrace<int, int, Z64> trace, int[] keys)
    {
        var merged = new Dictionary<int, (int Value, Z64 Weight)[]>();
        foreach (var (key, group) in trace.GroupForManySorted(keys))
        {
            merged[key] = group;
        }

        foreach (var k in keys)
        {
            var expected = trace.GroupFor(k);
            if (expected.IsEmpty)
            {
                if (merged.ContainsKey(k))
                {
                    throw new InvalidOperationException(
                        $"GroupForManySorted returned a group for absent key {k}.");
                }

                continue;
            }

            if (!merged.TryGetValue(k, out var actual))
            {
                throw new InvalidOperationException(
                    $"GroupForManySorted missed present key {k}.");
            }

            var expectedPairs = new SortedDictionary<int, long>();
            foreach (var (v, w) in expected)
            {
                expectedPairs[v] = expectedPairs.TryGetValue(v, out var e) ? e + w.Value : w.Value;
            }

            if (actual.Length != expectedPairs.Count)
            {
                throw new InvalidOperationException(
                    $"Key {k}: merge group size {actual.Length} != point-probe size {expectedPairs.Count}.");
            }

            foreach (var (v, w) in actual)
            {
                if (!expectedPairs.TryGetValue(v, out var ew) || ew != w.Value)
                {
                    throw new InvalidOperationException(
                        $"Key {k}: merge value {v} weight {w.Value} disagrees with point probe.");
                }
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
