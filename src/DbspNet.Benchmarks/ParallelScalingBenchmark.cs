// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks;

/// <summary>
/// Phase 6 — data-parallel scaling. Sweeps the worker count <c>W</c> over a fixed
/// large batch and reports throughput vs <c>W=1</c>, measuring two regimes
/// separately because they answer different questions:
/// <list type="bullet">
///   <item><b>Driver / sharded-I/O scaling</b> — an <em>exchange-free</em> query
///     (filter/project) has no cross-worker barrier, so each replica processes its
///     shard independently; this should scale ~linearly until memory bandwidth or
///     core count caps it. It isolates the driver + sharded-I/O overhead.</item>
///   <item><b>Exchange tax</b> — a GROUP BY shuffles every tick through a full
///     <c>Barrier(W)</c> + per-tick mailbox re-bucketing (the correctness-first v1
///     exchange). This shows where, if ever, <c>W&gt;1</c> beats <c>W=1</c> once the
///     shuffle cost is paid, and how big a batch must be to amortize it.</item>
/// </list>
/// </summary>
/// <remarks>
/// Throughput, not latency: <c>W&gt;1</c> only pays on large batches (a single-row
/// delta is pure barrier overhead), so this measures one big push. <c>W</c> is
/// capped to the physical core count — beyond that we'd be timing context-switch
/// contention, not scaling. Each measured run builds a fresh circuit (W threads)
/// and disposes it, so trace state never carries between runs.
/// </remarks>
internal static class ParallelScalingBenchmark
{
    public static void Run(StringBuilder output, int? maxWorkersOverride = null)
    {
        var maxWorkers = Math.Min(maxWorkersOverride ?? Environment.ProcessorCount, Environment.ProcessorCount);
        var workerCounts = WorkerCounts(maxWorkers);

        Console.WriteLine();
        Console.WriteLine($"=== Parallel scaling (W up to {maxWorkers}) ===");

        output.AppendLine("## Parallel scaling — throughput vs worker count");
        output.AppendLine();
        output.AppendLine(
            "Data-parallel sharding (`ParallelCircuit`): `W` identical replicas, each " +
            "processing a hash-partition of one large batch. The single-threaded " +
            "incremental hot path is untouched — this is the throughput axis, so the " +
            "metric is **rows/second on one big push**, not per-step latency. `W` is " +
            "capped to the host's core count. Two shapes, measured apart:");
        output.AppendLine();
        output.AppendLine(
            "- **Filter (exchange-free)** — no key-sensitive operator, so no shuffle " +
            "and no per-tick barrier. Tests whether the driver + sharded I/O scale.");
        output.AppendLine(
            "- **GROUP BY (exchange)** — an exchange re-shards by group key every tick " +
            "through a full `Barrier(W)` + mailbox re-bucketing. Tests the exchange tax.");
        output.AppendLine();
        output.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();

        // Filter: exchange-free. Large N so per-worker work dominates fixed overhead.
        RunShape(
            output,
            title: "Filter (exchange-free) — `WHERE value > 500`",
            ddl: ["CREATE TABLE events (id INT NOT NULL, value INT NOT NULL)"],
            sql: "SELECT id, value FROM events WHERE value > 500",
            n: 1_000_000,
            buildDelta: static n =>
            {
                var rng = new Random(42);
                var delta = new List<(object?[] Values, long Weight)>(n);
                for (var i = 0; i < n; i++)
                {
                    delta.Add((new object?[] { i, rng.Next(0, 1000) }, 1L));
                }

                return ("events", delta);
            },
            workerCounts);

        // GROUP BY: one exchange per tick. 1000 groups so partitions stay balanced.
        RunShape(
            output,
            title: "GROUP BY (exchange) — `SUM(v), COUNT(*)` over 1000 groups",
            ddl: ["CREATE TABLE data (k INT NOT NULL, v INT NOT NULL)"],
            sql: "SELECT k, SUM(v) AS s, COUNT(*) AS c FROM data GROUP BY k",
            n: 1_000_000,
            buildDelta: static n =>
            {
                var rng = new Random(7);
                var delta = new List<(object?[] Values, long Weight)>(n);
                for (var i = 0; i < n; i++)
                {
                    delta.Add((new object?[] { rng.Next(0, 1000), rng.Next(0, 10_000) }, 1L));
                }

                return ("data", delta);
            },
            workerCounts);

        output.AppendLine(
            "**Reading it.** The exchange-free filter's *Step* scales close to linearly " +
            "in W — the replicas share nothing during a tick, so the driver and isolated " +
            "operator state deliver the data-parallel win the model promises. The GROUP " +
            "BY *Step* scales sublinearly: every tick pays one full `Barrier(W)` plus a " +
            "rebuild of W mailbox buckets, so the exchange tax eats into the gain, though " +
            "W>1 still beats W=1 on a batch this large. The serial *split* column is the " +
            "honest caveat — input sharding and output gather still run single-threaded, " +
            "so end-to-end wall-clock is ingest-bound today; the parallel Step scaling is " +
            "the headroom a parallel/zero-copy ingest path would unlock.");
        output.AppendLine();
    }

    /// <summary>Console entry point for <c>dotnet run -- parallel [maxWorkers]</c>.</summary>
    public static int RunConsole(string[] args)
    {
        int? maxWorkers = null;
        if (args.Length > 1 && int.TryParse(args[1], NumberStyles.None, CultureInfo.InvariantCulture, out var w))
        {
            maxWorkers = Math.Max(1, w);
        }

        var output = new StringBuilder();
        Run(output, maxWorkers);
        Console.WriteLine();
        Console.WriteLine(output.ToString());
        return 0;
    }

    private static void RunShape(
        StringBuilder output,
        string title,
        string[] ddl,
        string sql,
        int n,
        Func<int, (string Table, List<(object?[] Values, long Weight)> Delta)> buildDelta,
        IReadOnlyList<int> workerCounts)
    {
        var plan = PlanOptimizer.Optimize(CompilePlan(ddl, sql));
        var (table, delta) = buildDelta(n);

        // Cross-check determinism: every W must produce the W=1 result, else the
        // throughput numbers describe a broken circuit.
        var baseline = MaterializeOnce(plan, 1, table, delta);

        Console.WriteLine();
        Console.WriteLine($"  {title}  (N={n:N0})");

        output.AppendLine($"### {title}");
        output.AppendLine();
        output.AppendLine(
            $"Batch of N={n:N0} rows. **Step** is the parallel region (the W replicas' " +
            "concurrent operator loops); **split** is the serial ingest on the calling " +
            "thread — row encoding plus the hash shard into W buckets " +
            "(`ShardedInputHandle.Push`). They are timed apart so the driver's parallel " +
            "scaling is visible separately from the v1 serial ingest, which is the " +
            "current wall-clock bottleneck (and the obvious next thing to parallelize).");
        output.AppendLine();
        output.AppendLine("| W  | Step          | Step throughput | Step speedup | Serial split  |");
        output.AppendLine("|---:|--------------:|----------------:|-------------:|--------------:|");

        double baseStepMs = 0;
        foreach (var workers in workerCounts)
        {
            var result = MaterializeOnce(plan, workers, table, delta);
            if (!SameResult(baseline, result))
            {
                throw new InvalidOperationException(
                    $"parallel result for W={workers} diverged from W=1 on '{title}' — aborting benchmark");
            }

            var (splitMs, stepMs) = MedianParallelSplitStepMs(
                build: () => CompileParallel(plan, workers),
                table,
                delta);

            if (workers == 1)
            {
                baseStepMs = stepMs;
            }

            var stepRowsPerSec = n / (stepMs / 1000.0);
            var speedup = baseStepMs > 0 ? baseStepMs / stepMs : 1.0;

            Console.WriteLine(
                $"    W={workers,-2} step={BenchmarkHarness.FormatMs(stepMs)}  " +
                $"{FormatThroughput(stepRowsPerSec)}  speedup={BenchmarkHarness.FormatRatio(speedup)}  " +
                $"split={BenchmarkHarness.FormatMs(splitMs)}");
            output.AppendLine(
                $"| {workers,2} | {BenchmarkHarness.FormatMs(stepMs).Trim()} | " +
                $"{FormatThroughput(stepRowsPerSec).Trim()} | {BenchmarkHarness.FormatRatio(speedup).Trim()} | " +
                $"{BenchmarkHarness.FormatMs(splitMs).Trim()} |");
        }

        output.AppendLine();
    }

    /// <summary>
    /// Median wall-clock of the serial input split and the parallel step, timed
    /// apart. Builds (and disposes) a fresh circuit each run so trace state never
    /// accumulates and the W worker threads are joined between runs.
    /// </summary>
    private static (double SplitMs, double StepMs) MedianParallelSplitStepMs(
        Func<ParallelTypedCompiledQuery> build,
        string table,
        List<(object?[] Values, long Weight)> delta,
        int warmups = 1,
        int runs = 5)
    {
        var splits = new List<double>(runs);
        var steps = new List<double>(runs);
        for (var i = 0; i < warmups + runs; i++)
        {
            using var q = build();

            var sw = Stopwatch.StartNew();
            q.Table(table).Push(delta);
            sw.Stop();
            var splitMs = sw.Elapsed.TotalMilliseconds;

            sw.Restart();
            q.Step();
            sw.Stop();
            var stepMs = sw.Elapsed.TotalMilliseconds;

            if (i >= warmups)
            {
                splits.Add(splitMs);
                steps.Add(stepMs);
            }
        }

        splits.Sort();
        steps.Sort();
        return (splits[splits.Count / 2], steps[steps.Count / 2]);
    }

    private static Dictionary<string, long> MaterializeOnce(
        LogicalPlan plan, int workers, string table, List<(object?[] Values, long Weight)> delta)
    {
        using var q = CompileParallel(plan, workers);
        q.Table(table).Push(delta);
        q.Step();

        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in q.Current)
        {
            var key = string.Join("|", values.Select(v => v?.ToString() ?? "<null>"));
            map[key] = map.GetValueOrDefault(key) + weight;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static bool SameResult(Dictionary<string, long> a, Dictionary<string, long> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach (var (k, v) in a)
        {
            if (!b.TryGetValue(k, out var bv) || bv != v)
            {
                return false;
            }
        }

        return true;
    }

    private static ParallelTypedCompiledQuery CompileParallel(LogicalPlan plan, int workers)
    {
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q))
        {
            throw new InvalidOperationException("plan is outside the typed parallel pipeline");
        }

        return q!;
    }

    private static LogicalPlan CompilePlan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    /// <summary>Powers of two from 1 up to and including <paramref name="maxWorkers"/> (plus the cap itself).</summary>
    private static List<int> WorkerCounts(int maxWorkers)
    {
        var counts = new List<int>();
        for (var w = 1; w <= maxWorkers; w *= 2)
        {
            counts.Add(w);
        }

        if (counts[^1] != maxWorkers && maxWorkers > 1)
        {
            counts.Add(maxWorkers);
        }

        return counts;
    }

    private static string FormatThroughput(double rowsPerSec) =>
        rowsPerSec >= 1_000_000.0
            ? $"{rowsPerSec / 1_000_000.0,7:F2} M rows/s"
            : $"{rowsPerSec / 1_000.0,7:F1} K rows/s";
}
