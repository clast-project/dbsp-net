// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;

namespace DbspNet.Benchmarks;

/// <summary>
/// Head-to-head for circuit-level operator fusion: a <c>map → filter → map</c>
/// chain wired as three separate linear operators (the pre-fusion shape) versus
/// the same chain folded into one <see cref="LinearOperators.MapFilterRows{TIn,TOut,TWeight}"/>
/// pass (what the SQL compiler now emits for a run of adjacent Filter/Project
/// nodes). The rows are <see cref="StructuralRow"/>s built exactly as the
/// structural compile path builds them, so the numbers reflect the real
/// per-tick cost the compiler change removes.
/// </summary>
/// <remarks>
/// <para>The unfused chain materializes an intermediate Z-set after each stage
/// and allocates a fresh <see cref="StructuralRow"/> for <em>every</em> input
/// row at the first map (before the filter has a chance to drop it), then again
/// at the second map. The fused pass iterates once, allocates a single output
/// Z-set, and builds one row only for the survivors. So the benchmark reports
/// both per-step latency and bytes allocated per step — the allocation column is
/// the cleanest read on what fusion saves.</para>
/// <para>Each step pushes the same N-row batch through the (stateless, linear)
/// chain, so per-step work is constant — no trace, no state growth.</para>
/// </remarks>
internal static class FusionBenchmark
{
    private static readonly int[] Sizes = { 100, 1_000, 10_000, 100_000 };

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Operator fusion (map→filter→map) ===");

        output.AppendLine("## Operator fusion — `map → filter → map` chain");
        output.AppendLine();
        output.AppendLine(
            "A three-stage linear chain (`MapRows` → `Filter` → `MapRows`) wired as " +
            "three separate operators versus folded into one `MapFilterRows` pass — " +
            "the pre- and post-fusion shapes for a run of adjacent Filter/Project plan " +
            "nodes. Rows are `StructuralRow`s built as the structural compile path " +
            "builds them; the filter keeps ~half the rows. Each step pushes the same " +
            "N-row batch through the stateless chain. **Latency** is the median " +
            "per-step time; **alloc/step** is bytes allocated per step (the unfused " +
            "chain allocates an intermediate Z-set per stage plus a row for every " +
            "input at the first map, before the filter can drop it).");
        output.AppendLine();
        output.AppendLine(
            "| N (batch)  | Unfused       | Fused          | Speedup | Unfused alloc | Fused alloc | Alloc saved |");
        output.AppendLine(
            "|-----------:|--------------:|---------------:|--------:|--------------:|------------:|------------:|");

        foreach (var n in Sizes)
        {
            var unfusedUs = BenchmarkHarness.MedianPerStepMicros(
                setup: () => Setup(n, fused: false), oneStep: OneStep);
            var fusedUs = BenchmarkHarness.MedianPerStepMicros(
                setup: () => Setup(n, fused: true), oneStep: OneStep);

            var unfusedAlloc = AllocBytesPerStep(() => Setup(n, fused: false), OneStep);
            var fusedAlloc = AllocBytesPerStep(() => Setup(n, fused: true), OneStep);

            var speedup = fusedUs > 0 ? unfusedUs / fusedUs : 0.0;
            var allocSaved = unfusedAlloc > 0 ? (double)(unfusedAlloc - fusedAlloc) / unfusedAlloc : 0.0;

            Console.WriteLine(
                $"  N={n,-7} unfused={BenchmarkHarness.FormatMicros(unfusedUs)} " +
                $"fused={BenchmarkHarness.FormatMicros(fusedUs)} " +
                $"speedup={BenchmarkHarness.FormatRatio(speedup)} " +
                $"alloc {FormatBytes(unfusedAlloc)}→{FormatBytes(fusedAlloc)}");

            output.AppendLine(
                $"| {n,10} | {BenchmarkHarness.FormatMicros(unfusedUs).Trim()} | " +
                $"{BenchmarkHarness.FormatMicros(fusedUs).Trim()} | " +
                $"{BenchmarkHarness.FormatRatio(speedup).Trim()} | " +
                $"{FormatBytes(unfusedAlloc)} | {FormatBytes(fusedAlloc)} | {allocSaved * 100.0:F0}% |");
        }

        output.AppendLine();
    }

    private sealed class ChainState
    {
        public RootCircuit Circuit { get; init; } = null!;
        public InputHandle<ZSet<StructuralRow, Z64>> Input { get; init; } = null!;
        public ZSet<StructuralRow, Z64> Batch { get; init; } = null!;
    }

    private static ChainState Setup(int n, bool fused)
    {
        // map1: value → value*2;  filter: keep value > n (≈ half survive after the
        // doubling, since input values are 0..n-1);  map2: value → value+1.
        var threshold = (long)n;

        InputHandle<ZSet<StructuralRow, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<StructuralRow, Z64>();
            ih = h;

            if (fused)
            {
                var stream = b.MapFilterRows<StructuralRow, StructuralRow, Z64>(s, row =>
                {
                    var id = row[0];
                    var v1 = (long)row[1]! * 2;          // map1
                    if (v1 <= threshold)                  // filter
                    {
                        return (false, null!);
                    }

                    return (true, new StructuralRow(id, v1 + 1)); // map2
                });
                _ = b.Output(stream);
            }
            else
            {
                var m1 = b.MapRows(s, row => new StructuralRow(row[0], (long)row[1]! * 2));
                var f = b.Filter(m1, row => (long)row[1]! > threshold);
                var m2 = b.MapRows(f, row => new StructuralRow(row[0], (long)row[1]! + 1));
                _ = b.Output(m2);
            }
        });

        var builder = new ZSetBuilder<StructuralRow, Z64>();
        for (var i = 0; i < n; i++)
        {
            builder.Add(new StructuralRow(i, (long)i), new Z64(1));
        }

        return new ChainState { Circuit = circuit, Input = ih!, Batch = builder.Build() };
    }

    private static void OneStep(ChainState state)
    {
        state.Input.Push(state.Batch);
        state.Circuit.Step();
    }

    /// <summary>Median-free bytes-allocated-per-step over a warmed circuit.</summary>
    private static long AllocBytesPerStep(
        Func<ChainState> setup, Action<ChainState> oneStep, int warmup = 20, int measure = 100)
    {
        var state = setup();
        for (var i = 0; i < warmup; i++)
        {
            oneStep(state);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < measure; i++)
        {
            oneStep(state);
        }

        var after = GC.GetAllocatedBytesForCurrentThread();
        return (after - before) / measure;
    }

    private static string FormatBytes(long bytes) =>
        bytes switch
        {
            < 1024 => $"{bytes,7} B",
            < 1024 * 1024 => $"{bytes / 1024.0,6:F1} KB",
            _ => $"{bytes / (1024.0 * 1024.0),6:F1} MB",
        };
}
