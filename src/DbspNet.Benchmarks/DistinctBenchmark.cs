// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Benchmarks;

/// <summary>
/// End-to-end head-to-head between the flat <c>DistinctOp</c> and the
/// LSM-backed <c>SpineDistinctOp</c>. Each Step probes the trace once
/// per delta-key and then integrates the delta — both costs that the
/// pure-trace microbench captures in isolation. This bench reports
/// how those microbench differences translate at the operator level
/// after the surrounding circuit, Z-set construction, and output-builder
/// costs are baked in.
/// </summary>
/// <remarks>
/// <para>Two per-step shapes are timed at varying pre-loaded trace sizes:</para>
/// <list type="bullet">
///   <item><b>new key</b>: push a key not yet in the trace. Distinct
///   emits a +1, integrates the delta. Probe is a guaranteed miss.</item>
///   <item><b>existing key</b>: push a key already in the trace, with
///   weight +1. Distinct emits nothing (key was already positive),
///   integrates the delta. Probe is a guaranteed hit.</item>
/// </list>
/// <para>The existing-key path is the more common steady-state pattern
/// (idempotent inserts in a `SELECT DISTINCT` over a slowly-changing
/// table) and the one the spine should be most penalised on — every
/// step probes every batch.</para>
/// </remarks>
internal static class DistinctBenchmark
{
    private static readonly int[] Sizes = { 1_000, 10_000, 100_000 };

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Distinct (flat vs spine) ===");

        output.AppendLine("## DistinctOp — flat vs spine (per-Step latency)");
        output.AppendLine();
        output.AppendLine(
            "Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is " +
            "pre-loaded with N distinct integer keys. Three delta shapes: a single " +
            "**new key** (probe miss + integrate), a single **existing key** at +1 " +
            "(probe hit + no-op integrate), and a **bulk-10** delta of mixed new + " +
            "existing keys (probe + integrate work scaled 10×, swamping the per-step " +
            "overhead that obscures the trace cost in singleton-delta measurements). " +
            "Spine variants are tiered with 4 batches per level (default) and 2 " +
            "batches per level (leveled-like).");
        output.AppendLine();
        output.AppendLine("| N          | Op           | Flat        | Spine(N=4)  | Spine(N=2)  | Spine(4) vs Flat |");
        output.AppendLine("|-----------:|:-------------|------------:|------------:|------------:|-----------------:|");

        foreach (var n in Sizes)
        {
            Console.WriteLine($"  N={n}");
            var (flatNew, spine4New, spine2New) = MeasureStep(n, deltaShape: DeltaShape.NewKey);
            var (flatExisting, spine4Existing, spine2Existing) = MeasureStep(n, deltaShape: DeltaShape.ExistingKey);
            var (flatBulk, spine4Bulk, spine2Bulk) = MeasureStep(n, deltaShape: DeltaShape.Bulk10);

            AppendRow(output, n, "new key",       flatNew,       spine4New,       spine2New);
            AppendRow(output, n, "existing key",  flatExisting,  spine4Existing,  spine2Existing);
            AppendRow(output, n, "bulk-10 mixed", flatBulk,      spine4Bulk,      spine2Bulk);
        }
    }

    private enum DeltaShape
    {
        /// <summary>Push a key not yet in the trace each step.</summary>
        NewKey,

        /// <summary>Push a key already in the trace (with weight +1) each step.</summary>
        ExistingKey,

        /// <summary>
        /// Push a 10-key delta — five guaranteed-new keys plus five
        /// guaranteed-existing keys. Probe + integrate work scales 10×
        /// over the singleton variants, lifting per-step latency out of
        /// the stopwatch-resolution floor.
        /// </summary>
        Bulk10,
    }

    private static (double Flat, double Spine4, double Spine2) MeasureStep(int n, DeltaShape deltaShape)
    {
        var flatUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () => SetupFlat(n),
            oneStep: state => OneStep(state, deltaShape));

        var spine4Us = BenchmarkHarness.MedianPerStepMicros(
            setup: () => SetupSpine(n, batchesPerLevel: 4),
            oneStep: state => OneStep(state, deltaShape));

        var spine2Us = BenchmarkHarness.MedianPerStepMicros(
            setup: () => SetupSpine(n, batchesPerLevel: 2),
            oneStep: state => OneStep(state, deltaShape));

        return (flatUs, spine4Us, spine2Us);
    }

    private sealed class DistinctState
    {
        public RootCircuit Circuit { get; init; } = null!;
        public InputHandle<ZSet<int, Z64>> Input { get; init; } = null!;
        public int InitialCount { get; init; }
        public int NextNewKey { get; set; }
        public int NextExistingKey { get; set; }
    }

    private static DistinctState SetupFlat(int n)
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            _ = b.Output(b.Distinct(s));
        });

        return PreloadAndPrepare(circuit, ih!, n);
    }

    private static DistinctState SetupSpine(int n, int batchesPerLevel)
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            _ = b.Output(b.SpineDistinct(s, new TieredCompactionStrategy(batchesPerLevel)));
        });

        return PreloadAndPrepare(circuit, ih!, n);
    }

    private static DistinctState PreloadAndPrepare(
        RootCircuit circuit, InputHandle<ZSet<int, Z64>> input, int n)
    {
        // Pre-load N distinct keys at weight +1, one per Step. This
        // mirrors steady-state operator usage — many small deltas — so
        // the spine's batch layout reflects what real workloads produce.
        for (var i = 0; i < n; i++)
        {
            input.Push(ZSet.Singleton(i, new Z64(1)));
            circuit.Step();
        }

        return new DistinctState
        {
            Circuit = circuit,
            Input = input,
            InitialCount = n,
            NextNewKey = n,             // keys [n, 2n) are guaranteed-absent
            NextExistingKey = 0,        // keys [0, n) are guaranteed-present
        };
    }

    private static void OneStep(DistinctState state, DeltaShape shape)
    {
        switch (shape)
        {
            case DeltaShape.NewKey:
                state.Input.Push(ZSet.Singleton(state.NextNewKey, new Z64(1)));
                state.NextNewKey++;
                break;

            case DeltaShape.ExistingKey:
                state.Input.Push(ZSet.Singleton(state.NextExistingKey, new Z64(1)));
                state.NextExistingKey = (state.NextExistingKey + 1) % state.InitialCount;
                break;

            case DeltaShape.Bulk10:
                {
                    var b = new ZSetBuilder<int, Z64>();
                    for (var i = 0; i < 5; i++)
                    {
                        b.Add(state.NextNewKey++, new Z64(1));
                    }

                    for (var i = 0; i < 5; i++)
                    {
                        b.Add(state.NextExistingKey, new Z64(1));
                        state.NextExistingKey = (state.NextExistingKey + 1) % state.InitialCount;
                    }

                    state.Input.Push(b.Build());
                    break;
                }
        }

        state.Circuit.Step();
    }

    private static void AppendRow(
        StringBuilder output, int n, string op,
        double flatUs, double spine4Us, double spine2Us)
    {
        var ratio = flatUs > 0 ? spine4Us / flatUs : 0.0;
        output.AppendLine(
            $"| {n,10} | {op,-12} | {BenchmarkHarness.FormatMicros(flatUs).Trim()} | " +
            $"{BenchmarkHarness.FormatMicros(spine4Us).Trim()} | " +
            $"{BenchmarkHarness.FormatMicros(spine2Us).Trim()} | " +
            $"{BenchmarkHarness.FormatRatio(ratio).Trim()} |");
    }
}
