using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Benchmarks;

/// <summary>
/// Isolated head-to-head between the flat <see cref="ZSetTrace{TKey,TWeight}"/>
/// / <see cref="IndexedZSetTrace{TKey,TValue,TWeight}"/> implementations
/// and the LSM <see cref="SpineZSetTrace{TKey,TWeight}"/> /
/// <see cref="SpineIndexedZSetTrace{TKey,TValue,TWeight}"/> variants. The
/// goal is to attribute end-to-end perf changes that show up in the
/// joined / aggregate benchmarks to the trace itself rather than the
/// surrounding operator and SQL machinery.
/// </summary>
/// <remarks>
/// <para>Each trace is built by integrating <c>N</c> singleton deltas —
/// the steady-state pattern operators produce. Then four operations are
/// measured per trace via <see cref="BenchmarkHarness.MedianPerStepMicros"/>:</para>
/// <list type="bullet">
///   <item><b>Probe (present)</b>: <c>WeightOf</c> / <c>GroupFor</c> on a
///   key drawn from <c>[0, N)</c> — guaranteed hit.</item>
///   <item><b>Probe (absent)</b>: same op on a key drawn from <c>[N, 2N)</c>
///   — guaranteed miss. Important because absent-key probes are the
///   common case in <see cref="IncrementalJoinOp"/> (the probe-side trace
///   only matches a fraction of keys).</item>
///   <item><b>Integrate (1-key delta)</b>: one new entry — the typical
///   per-tick volume from an aggregate or filter.</item>
///   <item><b>Integrate (100-key delta)</b>: a bulkier delta — exercises
///   the spine's amortised compaction cost vs the flat trace's
///   merge-in-place cost.</item>
/// </list>
/// <para>Two spine variants are compared: the default tiered N=4 and a
/// leveled-like tiered N=2 (lower read amplification, higher write
/// amplification). End-to-end benchmarks will pick whichever wins at
/// realistic trace sizes; this microbench tells you why.</para>
/// </remarks>
internal static class PureTraceBenchmark
{
    private static readonly int[] Sizes = { 1_000, 10_000, 100_000, 1_000_000 };
    private const int IntegrateBulkSize = 100;

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Pure-trace microbench ===");

        output.AppendLine("## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`");
        output.AppendLine();
        output.AppendLine(
            "Direct comparison between the flat in-place trace and the LSM-style " +
            "`SpineZSetTrace` at varying trace sizes. Each trace is built by " +
            "integrating N singleton `(key=i, weight=+1)` deltas, then four " +
            "per-step ops are measured: `WeightOf` on a present key, `WeightOf` " +
            "on an absent key, `Integrate` of a 1-key delta, and `Integrate` of " +
            "a " + IntegrateBulkSize + "-key delta. Spine variants are tiered with " +
            "4 batches per level (default) and 2 batches per level (leveled-like). " +
            "Probe latencies are amortised across 1000 back-to-back calls per " +
            "sample (stopwatch resolution would otherwise round sub-100ns flat-trace " +
            "probes to zero); `Integrate` is per-call because it mutates the trace.");
        output.AppendLine();
        RunZSetTable(output);

        output.AppendLine();
        output.AppendLine("## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`");
        output.AppendLine();
        output.AppendLine(
            "Same shape, against the indexed trace that " +
            "`IncrementalAggregateOp` and the join operators hold. The trace " +
            "shape is one value per key (the join-trace pattern); `GroupFor` " +
            "stands in for `WeightOf`.");
        output.AppendLine();
        RunIndexedZSetTable(output);
    }

    // ---------- Z-set trace ----------

    private static void RunZSetTable(StringBuilder output)
    {
        output.AppendLine("| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |");
        output.AppendLine("|-----------:|:-------------------|------------:|------------:|------------:|");

        foreach (var n in Sizes)
        {
            Console.WriteLine($"  Z-set N={n}");
            var (flatProbePresent, spine4ProbePresent, spine2ProbePresent) = MeasureZSetProbe(n, present: true);
            var (flatProbeAbsent,  spine4ProbeAbsent,  spine2ProbeAbsent)  = MeasureZSetProbe(n, present: false);
            var (flatInt1,        spine4Int1,        spine2Int1)         = MeasureZSetIntegrate(n, deltaSize: 1);
            var (flatIntBulk,     spine4IntBulk,     spine2IntBulk)      = MeasureZSetIntegrate(n, deltaSize: IntegrateBulkSize);

            AppendRow(output, n, "WeightOf(present)", flatProbePresent, spine4ProbePresent, spine2ProbePresent);
            AppendRow(output, n, "WeightOf(absent)",  flatProbeAbsent,  spine4ProbeAbsent,  spine2ProbeAbsent);
            AppendRow(output, n, "Integrate(1)",      flatInt1,         spine4Int1,         spine2Int1);
            AppendRow(output, n, $"Integrate({IntegrateBulkSize})", flatIntBulk, spine4IntBulk, spine2IntBulk);
        }
    }

    private static (double Flat, double Spine4, double Spine2) MeasureZSetProbe(int n, bool present)
    {
        // Probes are non-mutating — use the amortising harness so flat-trace
        // sub-100ns probes don't round to zero on the stopwatch.
        var flatNs = BenchmarkHarness.MedianPerStepNanos(
            setup: () => (Trace: BuildFlatZSet(n), Probe: new ProbeSequence(n, present)),
            oneStep: state => _ = state.Trace.Current.WeightOf(state.Probe.Next()));

        var spine4Ns = BenchmarkHarness.MedianPerStepNanos(
            setup: () => (Trace: BuildSpineZSet(n, batchesPerLevel: 4), Probe: new ProbeSequence(n, present)),
            oneStep: state => _ = state.Trace.WeightOf(state.Probe.Next()));

        var spine2Ns = BenchmarkHarness.MedianPerStepNanos(
            setup: () => (Trace: BuildSpineZSet(n, batchesPerLevel: 2), Probe: new ProbeSequence(n, present)),
            oneStep: state => _ = state.Trace.WeightOf(state.Probe.Next()));

        // FormatMicros auto-scales sub-1µs values to ns, so convert from
        // ns → µs at the boundary and keep one downstream formatter.
        return (flatNs / 1000.0, spine4Ns / 1000.0, spine2Ns / 1000.0);
    }

    private static (double Flat, double Spine4, double Spine2) MeasureZSetIntegrate(int n, int deltaSize)
    {
        // Integrate adds new keys disjoint from the seed range so we
        // don't churn the structural layout (no cancellation, no
        // existing-key collisions). Each oneStep step shifts the delta
        // window by deltaSize so successive integrates are distinct.

        var flat = BenchmarkHarness.MedianPerStepMicros(
            setup: () => (Trace: BuildFlatZSet(n), Next: n),
            oneStep: state =>
            {
                state.Trace.Integrate(BuildZSetDelta(state.Next, deltaSize));
                state.Next += deltaSize;
            });

        var spine4 = BenchmarkHarness.MedianPerStepMicros(
            setup: () => (Trace: BuildSpineZSet(n, batchesPerLevel: 4), Next: n),
            oneStep: state =>
            {
                state.Trace.Integrate(BuildZSetDelta(state.Next, deltaSize));
                state.Next += deltaSize;
            });

        var spine2 = BenchmarkHarness.MedianPerStepMicros(
            setup: () => (Trace: BuildSpineZSet(n, batchesPerLevel: 2), Next: n),
            oneStep: state =>
            {
                state.Trace.Integrate(BuildZSetDelta(state.Next, deltaSize));
                state.Next += deltaSize;
            });

        return (flat, spine4, spine2);
    }

    private static ZSetTrace<int, Z64> BuildFlatZSet(int n)
    {
        var t = new ZSetTrace<int, Z64>();
        for (var i = 0; i < n; i++)
        {
            t.Integrate(ZSet.Singleton(i, new Z64(1)));
        }

        return t;
    }

    private static SpineZSetTrace<int, Z64> BuildSpineZSet(int n, int batchesPerLevel)
    {
        var t = new SpineZSetTrace<int, Z64>(new TieredCompactionStrategy(batchesPerLevel));
        for (var i = 0; i < n; i++)
        {
            t.Integrate(ZSet.Singleton(i, new Z64(1)));
        }

        return t;
    }

    private static ZSet<int, Z64> BuildZSetDelta(int startKey, int count)
    {
        if (count == 1)
        {
            return ZSet.Singleton(startKey, new Z64(1));
        }

        var b = new ZSetBuilder<int, Z64>();
        for (var i = 0; i < count; i++)
        {
            b.Add(startKey + i, new Z64(1));
        }

        return b.Build();
    }

    // ---------- Indexed Z-set trace ----------

    private static void RunIndexedZSetTable(StringBuilder output)
    {
        output.AppendLine("| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |");
        output.AppendLine("|-----------:|:-------------------|------------:|------------:|------------:|");

        foreach (var n in Sizes)
        {
            Console.WriteLine($"  Indexed Z-set N={n}");
            var (flatProbePresent, spine4ProbePresent, spine2ProbePresent) = MeasureIndexedProbe(n, present: true);
            var (flatProbeAbsent,  spine4ProbeAbsent,  spine2ProbeAbsent)  = MeasureIndexedProbe(n, present: false);
            var (flatInt1,        spine4Int1,        spine2Int1)         = MeasureIndexedIntegrate(n, deltaSize: 1);
            var (flatIntBulk,     spine4IntBulk,     spine2IntBulk)      = MeasureIndexedIntegrate(n, deltaSize: IntegrateBulkSize);

            AppendRow(output, n, "GroupFor(present)", flatProbePresent, spine4ProbePresent, spine2ProbePresent);
            AppendRow(output, n, "GroupFor(absent)",  flatProbeAbsent,  spine4ProbeAbsent,  spine2ProbeAbsent);
            AppendRow(output, n, "Integrate(1)",      flatInt1,         spine4Int1,         spine2Int1);
            AppendRow(output, n, $"Integrate({IntegrateBulkSize})", flatIntBulk, spine4IntBulk, spine2IntBulk);
        }
    }

    private static (double Flat, double Spine4, double Spine2) MeasureIndexedProbe(int n, bool present)
    {
        var flatNs = BenchmarkHarness.MedianPerStepNanos(
            setup: () => (Trace: BuildFlatIndexed(n), Probe: new ProbeSequence(n, present)),
            oneStep: state => _ = state.Trace.Current.GroupFor(state.Probe.Next()));

        var spine4Ns = BenchmarkHarness.MedianPerStepNanos(
            setup: () => (Trace: BuildSpineIndexed(n, batchesPerLevel: 4), Probe: new ProbeSequence(n, present)),
            oneStep: state => _ = state.Trace.GroupFor(state.Probe.Next()));

        var spine2Ns = BenchmarkHarness.MedianPerStepNanos(
            setup: () => (Trace: BuildSpineIndexed(n, batchesPerLevel: 2), Probe: new ProbeSequence(n, present)),
            oneStep: state => _ = state.Trace.GroupFor(state.Probe.Next()));

        return (flatNs / 1000.0, spine4Ns / 1000.0, spine2Ns / 1000.0);
    }

    private static (double Flat, double Spine4, double Spine2) MeasureIndexedIntegrate(int n, int deltaSize)
    {
        var flat = BenchmarkHarness.MedianPerStepMicros(
            setup: () => (Trace: BuildFlatIndexed(n), Next: n),
            oneStep: state =>
            {
                state.Trace.Integrate(BuildIndexedDelta(state.Next, deltaSize));
                state.Next += deltaSize;
            });

        var spine4 = BenchmarkHarness.MedianPerStepMicros(
            setup: () => (Trace: BuildSpineIndexed(n, batchesPerLevel: 4), Next: n),
            oneStep: state =>
            {
                state.Trace.Integrate(BuildIndexedDelta(state.Next, deltaSize));
                state.Next += deltaSize;
            });

        var spine2 = BenchmarkHarness.MedianPerStepMicros(
            setup: () => (Trace: BuildSpineIndexed(n, batchesPerLevel: 2), Next: n),
            oneStep: state =>
            {
                state.Trace.Integrate(BuildIndexedDelta(state.Next, deltaSize));
                state.Next += deltaSize;
            });

        return (flat, spine4, spine2);
    }

    private static IndexedZSetTrace<int, int, Z64> BuildFlatIndexed(int n)
    {
        var t = new IndexedZSetTrace<int, int, Z64>();
        for (var i = 0; i < n; i++)
        {
            t.Integrate(IndexedSingleton(i, i, new Z64(1)));
        }

        return t;
    }

    private static SpineIndexedZSetTrace<int, int, Z64> BuildSpineIndexed(int n, int batchesPerLevel)
    {
        var t = new SpineIndexedZSetTrace<int, int, Z64>(new TieredCompactionStrategy(batchesPerLevel));
        for (var i = 0; i < n; i++)
        {
            t.Integrate(IndexedSingleton(i, i, new Z64(1)));
        }

        return t;
    }

    private static IndexedZSet<int, int, Z64> BuildIndexedDelta(int startKey, int count)
    {
        var b = new IndexedZSetBuilder<int, int, Z64>();
        for (var i = 0; i < count; i++)
        {
            b.Add(startKey + i, startKey + i, new Z64(1));
        }

        return b.Build();
    }

    private static IndexedZSet<int, int, Z64> IndexedSingleton(int key, int value, Z64 weight)
    {
        var b = new IndexedZSetBuilder<int, int, Z64>();
        b.Add(key, value, weight);
        return b.Build();
    }

    // ---------- Output ----------

    private static void AppendRow(
        StringBuilder output, int n, string op,
        double flatUs, double spine4Us, double spine2Us)
    {
        output.AppendLine(
            $"| {n,10} | {op,-18} | {BenchmarkHarness.FormatMicros(flatUs).Trim()} | " +
            $"{BenchmarkHarness.FormatMicros(spine4Us).Trim()} | {BenchmarkHarness.FormatMicros(spine2Us).Trim()} |");
    }

    // ---------- Helpers ----------

    /// <summary>
    /// Deterministic sequence of probe keys, alternating across the
    /// keyspace so the same set of probes runs in warmup and measurement.
    /// "Present" mode walks <c>[0, N)</c>; "absent" walks <c>[N, 2N)</c>.
    /// </summary>
    private sealed class ProbeSequence
    {
        private readonly int _start;
        private readonly int _length;
        private int _i;

        public ProbeSequence(int n, bool present)
        {
            _start = present ? 0 : n;
            _length = n;
            _i = 0;
        }

        public int Next()
        {
            var k = _start + (_i % _length);
            _i++;
            return k;
        }
    }
}
