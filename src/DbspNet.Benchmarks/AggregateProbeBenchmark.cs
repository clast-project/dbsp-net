// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Benchmarks;

/// <summary>
/// Operator-level gate for wiring the merge probe into the spine aggregate
/// (docs/design-row-representation.md §8, the IMultiset increment). Drives the
/// whole <see cref="SpineIncrementalAggregateOp{TKey,TValue,TOut}.Step"/> two
/// ways: the point-probe path (today's <c>GroupFor</c> + Z-set rebuild, which
/// hashes every value row of the after-group twice) versus the batched
/// <c>GroupForManySorted</c> merge feeding a <see cref="SortedRunMultiset{T}"/>
/// (no rehash — values are compared, not hashed).
/// </summary>
/// <remarks>
/// <para>The two paths run the identical operator; only
/// <see cref="SpineAggregateProbeMode.ForcePointProbe"/> differs. Each measured
/// tick updates <c>D</c> existing groups (the aggregate only ever touches keys
/// in its delta), so unlike the join there is no "absent" case. Ticks alternate
/// adding then retracting one extra (above-min) value per group, so group
/// contents — and the per-key caches — return to their starting state every
/// pair: bounded state, identical work each tick. The grouped value is a wide
/// struct so the after-group rebuild pays a realistic per-row hash, mirroring
/// q4's nested aggregate over wide bid rows.</para>
/// </remarks>
internal static class AggregateProbeBenchmark
{
    private static readonly int[] StateSizes = { 1_000, 100_000, 1_000_000 };
    private static readonly int[] DeltaWidths = { 1, 8, 64, 512, 4096 };
    private const int GroupSize = 4;

    // A wide, comparable grouped value — MIN compares it and the after-group
    // rebuild hashes it, so the merge's "compare don't hash" win is visible.
    private readonly record struct Val(long V, long F1, long F2) : IComparable<Val>
    {
        public int CompareTo(Val other)
        {
            var c = V.CompareTo(other.V);
            if (c != 0) return c;
            c = F1.CompareTo(other.F1);
            return c != 0 ? c : F2.CompareTo(other.F2);
        }
    }

    private readonly record struct AggRow(int Key, Val Value);

    // Above every preloaded value, so adding/retracting it never changes a
    // group's MIN — the measured cost is the after-group build, not output churn.
    private static readonly Val Extra = new(long.MaxValue / 2, 0, 0);

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Aggregate merge-probe (SpineIncrementalAggregateOp.Step) ===");

        output.AppendLine("## Aggregate merge-probe — point probe vs galloping merge at the operator");
        output.AppendLine();
        output.AppendLine(
            "Operator-level gate for docs/design-row-representation.md §8 (the " +
            "IMultiset increment): the whole `SpineIncrementalAggregateOp.Step` is " +
            "timed with the per-key `GroupFor` + Z-set rebuild (`ForcePointProbe`, " +
            "today's baseline — two hashing passes over the after-group) versus the " +
            "batched `GroupForManySorted` merge feeding a `SortedRunMultiset` (no " +
            "rehash). Each tick updates `D` existing groups (group size " + GroupSize +
            ", wide values); ticks alternate add/retract so state stays bounded. " +
            "Times are median ns per **Step**; **Speedup** is point/merge " +
            "(>1 = merge wins). Aggregates only touch keys in their delta, so every " +
            "probe hits an existing group (no absent case).");
        output.AppendLine();

        output.AppendLine("| N | D=1 | D=8 | D=64 | D=512 | D=4096 |");
        output.AppendLine("|--:|----:|----:|-----:|------:|-------:|");
        foreach (var n in StateSizes)
        {
            Console.WriteLine($"  Building spine aggregate N={n} (group size {GroupSize})…");
            var row = new StringBuilder($"| {n:N0} ");
            foreach (var d in DeltaWidths)
            {
                var pointNs = MeasureStep(n, d, forcePointProbe: true);
                var mergeNs = MeasureStep(n, d, forcePointProbe: false);
                var speedup = mergeNs > 0 ? pointNs / mergeNs : 0.0;
                Console.WriteLine(
                    $"    N={n,-8} D={d,-5} point={FmtNs(pointNs)} merge={FmtNs(mergeNs)} " +
                    BenchmarkHarness.FormatRatio(speedup).Trim());
                row.Append($"| {BenchmarkHarness.FormatRatio(speedup).Trim()} ");
            }

            output.AppendLine(row.Append('|').ToString());
        }

        output.AppendLine();
        output.AppendLine(
            "Reading: the merge wins on multi-key ticks (`D > 1`) by skipping the " +
            "two whole-group hashing passes the Z-set rebuild pays; `D == 1` keeps " +
            "the point probe (the trace-level soft spot), so that column is ~1.0× by " +
            "construction. The win is the after-group build cost the IMultiset " +
            "abstraction removes — the aggregators are unchanged.");
        output.AppendLine();
    }

    private static double MeasureStep(int n, int d, bool forcePointProbe)
    {
        var (circuit, input) = BuildAggregate(n);

        // D existing group keys, evenly spread across [0, N).
        var keys = new int[d];
        for (var i = 0; i < d; i++)
        {
            keys[i] = (int)((long)i * n / d);
        }

        var plus = ZSet.FromEntries(keys.Select(k => (new AggRow(k, Extra), Z64.One)));
        var minus = ZSet.FromEntries(keys.Select(k => (new AggRow(k, Extra), new Z64(-1))));

        SpineAggregateProbeMode.ForcePointProbe = forcePointProbe;
        try
        {
            for (var i = 0; i < 6; i++)
            {
                input.Push(i % 2 == 0 ? plus : minus);
                circuit.Step();
            }

            const int samples = 21;
            var times = new List<double>(samples);
            var sw = new Stopwatch();
            for (var s = 0; s < samples; s++)
            {
                sw.Restart();
                input.Push(plus);
                circuit.Step();
                input.Push(minus);
                circuit.Step();
                sw.Stop();
                times.Add(sw.Elapsed.TotalNanoseconds / 2.0);
            }

            times.Sort();
            return times[times.Count / 2];
        }
        finally
        {
            SpineAggregateProbeMode.ForcePointProbe = false;
        }
    }

    /// <summary>
    /// Builds a spine MIN aggregate grouped on <c>int</c> and preloads
    /// <paramref name="n"/> groups (<see cref="GroupSize"/> wide values each).
    /// </summary>
    private static (RootCircuit Circuit, InputHandle<ZSet<AggRow, Z64>> Input) BuildAggregate(int n)
    {
        InputHandle<ZSet<AggRow, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<AggRow, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, r => r.Key, r => r.Value);
            _ = b.Output(b.SpineIncrementalAggregate(grouped, MinMaxAggregator<Val>.Min()));
        });

        const int chunk = 50_000;
        var buffer = new List<(AggRow, Z64)>(chunk * GroupSize);
        for (var start = 0; start < n; start += chunk)
        {
            buffer.Clear();
            var end = Math.Min(start + chunk, n);
            for (var k = start; k < end; k++)
            {
                for (var g = 0; g < GroupSize; g++)
                {
                    buffer.Add((new AggRow(k, new Val(k * 2L + g, k, g)), Z64.One));
                }
            }

            ih!.Push(ZSet.FromEntries(buffer));
            circuit.Step();
        }

        return (circuit, ih!);
    }

    private static string FmtNs(double ns) =>
        ns switch
        {
            < 1_000.0 => $"{ns,7:F0} ns",
            < 1_000_000.0 => $"{ns / 1_000.0,7:F2} µs",
            _ => $"{ns / 1_000_000.0,7:F2} ms",
        };
}
