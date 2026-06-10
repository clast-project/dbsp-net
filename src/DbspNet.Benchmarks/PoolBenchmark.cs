// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;

namespace DbspNet.Benchmarks;

/// <summary>
/// Lever-1 reclaimability microbench (docs/design-row-representation.md §16.5/§16.7).
/// The W=1 profile (§16.2) showed the engine is allocation-bound per tuple, and
/// §16.3 attributed ~55–60% of that to "Layer A": every stateful operator builds a
/// fresh <c>Dictionary</c>-backed Z-set delta every tick (`new ZSetBuilder` →
/// `Build()` → `SetCurrent`, universal across all 25 stateful-op files). Lever 1 is
/// to <b>pool/reuse those delta dictionaries</b> instead of allocating fresh.
///
/// <para>This microbench answers the two questions that gate the lever <b>before</b>
/// any operator is wired (mirroring how `surrogatebench` retired an XL option in
/// §14.9): <b>(A) the prize</b> — how many bytes per dict per tick pooling reclaims,
/// across the delta sizes that span q4/q18/q19's operating points; and <b>(B) the
/// constraint</b> — whether the `Build()`-ownership + z⁻¹/trace-retention rules
/// permit reuse (answered from the verified Core lifecycle, see the report text).</para>
///
/// <para>It models the exact lifecycle: today's path is <c>new
/// Dictionary&lt;K,V&gt;()</c> (the bare <c>ZSetBuilder()</c> ctor — no capacity
/// hint) filled with D entries then handed to a <c>ZSet</c> and dropped; the pooled
/// path reuses one dictionary via <c>Clear()</c> (which keeps the backing arrays,
/// so a stable-size delta re-fills with zero allocation). A pre-sized column shows
/// what `ZSetBuilder.From`'s capacity hint already buys.</para>
/// </summary>
internal static class PoolBenchmark
{
    // Delta sizes per dict per tick: D=1 is the fine-tick steady state (the
    // `profile` regime); 9,216 ≈ a bid-only query's per-tick output at batch 10k
    // (the realistic Nexmark operating point, q18/q19); the middle values bracket.
    private static readonly int[] DeltaSizes = { 1, 16, 256, 1_024, 4_096, 9_216 };
    private const int Warmups = 3;
    private const int Samples = 15;

    public static void Run(StringBuilder output)
    {
        Console.WriteLine();
        Console.WriteLine("=== Pool reclaimability microbench (fresh vs pre-sized vs pooled delta dict) ===");

        output.AppendLine("## Pool reclaimability microbench — delta Z-set dictionary reuse");
        output.AppendLine();
        output.AppendLine(
            "Microbench for docs/design-row-representation.md §16.5 lever 1. The W=1 " +
            "profile found the engine allocation-bound per tuple, with the dominant " +
            "term being the fresh `Dictionary`-backed Z-set delta every stateful " +
            "operator builds every tick (`new ZSetBuilder` → `Build()`). Lever 1 pools " +
            "those dictionaries. This measures the **prize** (bytes/dict/tick reclaimed) " +
            "across the delta sizes `D` that span q4/q18/q19's operating points.");
        output.AppendLine();
        output.AppendLine(
            "- **fresh** — `new Dictionary<K,V>()` (bare `ZSetBuilder()`, no capacity), " +
            "fill D entries, hand off, drop. *Today's path.*");
        output.AppendLine(
            "- **presized** — `new Dictionary<K,V>(D)` (what `ZSetBuilder.From`'s " +
            "capacity hint already buys when the source size is known).");
        output.AppendLine(
            "- **pooled** — reuse one dictionary via `Clear()` (keeps backing arrays; " +
            "a stable-size delta re-fills with ~zero allocation). *The lever's ceiling.*");
        output.AppendLine();
        output.AppendLine(
            "`B/tick` is managed bytes allocated per built dictionary " +
            "(`GC.GetAllocatedBytesForCurrentThread`); **reclaim** = fresh − pooled = " +
            "the per-dict-per-tick prize. Multiply by the number of delta-producing " +
            "operators in a query (~5–10 for q4) for the per-tick total.");
        output.AppendLine();
        output.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();

        EmitTable(output, "long → long (narrow agg key, e.g. q4 outer group / q18 dedup weight)",
            d => MeasureLong(d));
        EmitTable(output, "(long,long) → long (composite group key, e.g. q4 (id,category))",
            d => MeasurePair(d));

        EmitConstraint(output);
    }

    private static void EmitTable(StringBuilder output, string title, Func<int, (double Fresh, double Presized, double Pooled)> measure)
    {
        Console.WriteLine($"  {title}");
        output.AppendLine($"### {title}");
        output.AppendLine();
        output.AppendLine("| D (entries) | fresh B/tick | presized B/tick | pooled B/tick | reclaim (fresh−pooled) | reclaim % |");
        output.AppendLine("|---:|---:|---:|---:|---:|---:|");
        foreach (var d in DeltaSizes)
        {
            var (fresh, presized, pooled) = measure(d);
            var reclaim = fresh - pooled;
            var pct = fresh > 0 ? 100.0 * reclaim / fresh : 0;
            Console.WriteLine($"    D={d,-5} fresh={fresh,9:F0} presized={presized,9:F0} pooled={pooled,7:F0} reclaim={reclaim,9:F0} ({pct:F0}%)");
            output.AppendLine($"| {d:N0} | {fresh:F0} | {presized:F0} | {pooled:F0} | {reclaim:F0} | {pct:F0}% |");
        }

        output.AppendLine();
    }

    // ---- per-shape allocation measurement ----

    private static (double, double, double) MeasureLong(int d)
    {
        // Distinct keys so the dictionary genuinely grows to D entries.
        var keys = new long[d];
        for (var i = 0; i < d; i++)
        {
            keys[i] = i * 2_654_435_761L; // spread to avoid trivial bucket clustering
        }

        long sink = 0;
        var fresh = AllocPerTick(() =>
        {
            var dict = new Dictionary<long, long>();
            for (var i = 0; i < d; i++)
            {
                dict[keys[i]] = i;
            }

            sink += dict.Count;
        });

        var presized = AllocPerTick(() =>
        {
            var dict = new Dictionary<long, long>(d);
            for (var i = 0; i < d; i++)
            {
                dict[keys[i]] = i;
            }

            sink += dict.Count;
        });

        var pool = new Dictionary<long, long>();
        var pooled = AllocPerTick(() =>
        {
            pool.Clear();
            for (var i = 0; i < d; i++)
            {
                pool[keys[i]] = i;
            }

            sink += pool.Count;
        });

        GC.KeepAlive(sink);
        return (fresh, presized, pooled);
    }

    private static (double, double, double) MeasurePair(int d)
    {
        var keys = new (long, long)[d];
        for (var i = 0; i < d; i++)
        {
            keys[i] = (i * 2_654_435_761L, i);
        }

        long sink = 0;
        var fresh = AllocPerTick(() =>
        {
            var dict = new Dictionary<(long, long), long>();
            for (var i = 0; i < d; i++)
            {
                dict[keys[i]] = i;
            }

            sink += dict.Count;
        });

        var presized = AllocPerTick(() =>
        {
            var dict = new Dictionary<(long, long), long>(d);
            for (var i = 0; i < d; i++)
            {
                dict[keys[i]] = i;
            }

            sink += dict.Count;
        });

        var pool = new Dictionary<(long, long), long>();
        var pooled = AllocPerTick(() =>
        {
            pool.Clear();
            for (var i = 0; i < d; i++)
            {
                pool[keys[i]] = i;
            }

            sink += pool.Count;
        });

        GC.KeepAlive(sink);
        return (fresh, presized, pooled);
    }

    /// <summary>
    /// Median managed bytes allocated by one invocation of <paramref name="buildOneTick"/>,
    /// each invocation modelling one operator building its delta dictionary for one tick.
    /// </summary>
    private static double AllocPerTick(Action buildOneTick)
    {
        for (var w = 0; w < Warmups; w++)
        {
            buildOneTick();
        }

        var samples = new double[Samples];
        for (var s = 0; s < Samples; s++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            const int reps = 64; // amortise the per-call measurement noise
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var r = 0; r < reps; r++)
            {
                buildOneTick();
            }

            var after = GC.GetAllocatedBytesForCurrentThread();
            samples[s] = (after - before) / (double)reps;
        }

        Array.Sort(samples);
        return samples[samples.Length / 2];
    }

    private static void EmitConstraint(StringBuilder output)
    {
        output.AppendLine("### The retention constraint (verified from the Core lifecycle)");
        output.AppendLine();
        output.AppendLine(
            "Pooling reuses one `Dictionary` across ticks, so it is only sound where a " +
            "tick's delta dictionary is **dead after the tick that produced it**. The " +
            "verified Core rules:");
        output.AppendLine();
        output.AppendLine(
            "- **`ZSet` takes ownership of the builder's dict** (`ZSet.cs` ctor: " +
            "\"callers must not retain a reference to the dict\"); `Build()` nulls the " +
            "builder's reference (`ZSetBuilder.cs:75`). So one dict ↔ one `ZSet`.");
        output.AppendLine(
            "- **Trace `Integrate(delta)` folds the delta into the trace's *own* dict " +
            "in place** (`Trace.cs` → `ZSet.MergeInPlace`) — it does **not** retain the " +
            "delta dict. So after a stateful op integrates, its input delta is free.");
        output.AppendLine(
            "- **A `Stream.Current` holds a tick's output only until the next tick's " +
            "`SetCurrent` overwrites it**, and all same-tick consumers read it before " +
            "then. So an output delta is dead at the next tick — **unless** a `z⁻¹` " +
            "captures it.");
        output.AppendLine(
            "- **`DelayOp` aliases its input `ZSet` by reference** (`DelayOp.cs:34`: " +
            "`_nextOutput = _input.Current`) and re-emits it next tick. A delta on an " +
            "edge feeding a `z⁻¹` is therefore **retained one tick** and is **NOT " +
            "poolable**.");
        output.AppendLine();
        output.AppendLine(
            "**Consequence:** pooling is sound *per edge*, gated on \"this delta edge " +
            "feeds no `z⁻¹`/`DelayOp` and is not otherwise captured.\" In q4/q18/q19's " +
            "flat pipelines the only delays are **trace-internal** (the join's " +
            "`L_{t-1}` is `_leftTrace.Current`, a trace-owned dict, not a delta), so " +
            "their delta edges are dead-after-tick and poolable. Explicit `z⁻¹` on a " +
            "delta edge appears in nested/recursive circuits (differentiate/integrate, " +
            "fixpoint) — those edges must be excluded. The compiler knows the graph, so " +
            "this is a per-edge analysis, not a global switch.");
        output.AppendLine();
    }
}
