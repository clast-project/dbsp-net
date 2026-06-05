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
        output.AppendLine(
            "- **VARCHAR project (exchange-free)** — string columns make the boundary " +
            "encode/decode the dominant cost, so it shows the parallel ingest/egest win.");
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

        // VARCHAR project (exchange-free): the boundary encode/decode (string ↔
        // Utf8String) dominates, so this is where parallel ingest/egest pays most.
        RunShape(
            output,
            title: "VARCHAR project (exchange-free) — string encode/decode dominates",
            ddl: ["CREATE TABLE docs (id INT NOT NULL, label VARCHAR NOT NULL, note VARCHAR NOT NULL)"],
            sql: "SELECT id, label, note FROM docs WHERE id >= 0",
            n: 1_000_000,
            buildDelta: static n =>
            {
                var rng = new Random(11);
                var delta = new List<(object?[] Values, long Weight)>(n);
                for (var i = 0; i < n; i++)
                {
                    delta.Add((new object?[] { i, $"label-{rng.Next(0, 100_000)}", $"note-{i}-payload" }, 1L));
                }

                return ("docs", delta);
            },
            workerCounts);

        output.AppendLine(
            "**Reading it.** The exchange-free filter's *Step* scales close to linearly " +
            "in W — the replicas share nothing during a tick, so the driver and isolated " +
            "operator state deliver the data-parallel win the model promises. The GROUP " +
            "BY *Step* scales sublinearly: every tick pays one full `Barrier(W)` plus a " +
            "rebuild of W mailbox buckets, so the exchange tax eats into the gain, though " +
            "W>1 still beats W=1 on a batch this large. *Split* and *Gather* now scale too — " +
            "the formerly serial ingest/egest are no longer the wall-clock floor. Ingest " +
            "encodes and shards on the worker threads; egest, on the exchange-free shapes " +
            "whose shards are key-disjoint, decodes per-worker and concatenates with no " +
            "serial combine. The VARCHAR shape, where the string encode/decode dominates, " +
            "is where both wins show most. The GROUP BY *Gather* stays flat by design: its " +
            "aggregate output is not disjoint (it falls back to the serial Z-set sum) but " +
            "is tiny, so it never gates wall-clock. Allocation-heavy parallel ingest/egest " +
            "needs Server GC to scale — workstation GC stops every worker on each " +
            "collection and serializes exactly the work being parallelized.");
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
            $"Batch of N={n:N0} rows, all three phases timed apart. **Split** is ingest — " +
            "boundary encode plus the hash shard into W buckets, now run on the worker " +
            "threads (`ParallelIngestor`). **Step** is the parallel operator region. " +
            "**Gather** is egest — when the compiler proves the output shards are " +
            "key-disjoint, each worker decodes its own shard and the results concatenate " +
            "(no serial combine); otherwise it falls back to the serial Z-set sum. Each " +
            "column shows its own speedup vs W=1, so the ingest/egest scaling is visible " +
            "alongside the operator scaling.");
        output.AppendLine();
        output.AppendLine("| W  | Split         | Split↑ | Step          | Step↑ | Gather        | Gather↑ |");
        output.AppendLine("|---:|--------------:|-------:|--------------:|------:|--------------:|--------:|");

        double baseSplitMs = 0;
        double baseStepMs = 0;
        double baseGatherMs = 0;
        foreach (var workers in workerCounts)
        {
            var result = MaterializeOnce(plan, workers, table, delta);
            if (!SameResult(baseline, result))
            {
                throw new InvalidOperationException(
                    $"parallel result for W={workers} diverged from W=1 on '{title}' — aborting benchmark");
            }

            var (splitMs, stepMs, gatherMs) = MedianParallelPhasesMs(
                build: () => CompileParallel(plan, workers),
                table,
                delta);

            if (workers == 1)
            {
                baseSplitMs = splitMs;
                baseStepMs = stepMs;
                baseGatherMs = gatherMs;
            }

            var splitUp = baseSplitMs > 0 ? baseSplitMs / splitMs : 1.0;
            var stepUp = baseStepMs > 0 ? baseStepMs / stepMs : 1.0;
            var gatherUp = baseGatherMs > 0 ? baseGatherMs / gatherMs : 1.0;

            Console.WriteLine(
                $"    W={workers,-2} split={BenchmarkHarness.FormatMs(splitMs)} ({BenchmarkHarness.FormatRatio(splitUp).Trim()})  " +
                $"step={BenchmarkHarness.FormatMs(stepMs)} ({BenchmarkHarness.FormatRatio(stepUp).Trim()})  " +
                $"gather={BenchmarkHarness.FormatMs(gatherMs)} ({BenchmarkHarness.FormatRatio(gatherUp).Trim()})");
            output.AppendLine(
                $"| {workers,2} | {BenchmarkHarness.FormatMs(splitMs).Trim()} | {BenchmarkHarness.FormatRatio(splitUp).Trim()} | " +
                $"{BenchmarkHarness.FormatMs(stepMs).Trim()} | {BenchmarkHarness.FormatRatio(stepUp).Trim()} | " +
                $"{BenchmarkHarness.FormatMs(gatherMs).Trim()} | {BenchmarkHarness.FormatRatio(gatherUp).Trim()} |");
        }

        output.AppendLine();
    }

    /// <summary>
    /// Median wall-clock of the three phases, timed apart: the input <b>split</b>
    /// (now parallel — encode + shard on the worker threads), the parallel
    /// <b>step</b>, and the output <b>gather</b> (Z-set sum + the parallel boundary
    /// decode, forced by enumerating <see cref="ParallelTypedCompiledQuery.Current"/>).
    /// Builds (and disposes) a fresh circuit each run so trace state never
    /// accumulates and the W worker threads are joined between runs.
    /// </summary>
    private static (double SplitMs, double StepMs, double GatherMs) MedianParallelPhasesMs(
        Func<ParallelTypedCompiledQuery> build,
        string table,
        List<(object?[] Values, long Weight)> delta,
        int warmups = 1,
        int runs = 5)
    {
        var splits = new List<double>(runs);
        var steps = new List<double>(runs);
        var gathers = new List<double>(runs);
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

            sw.Restart();
            var rows = 0;
            foreach (var _ in q.Current)   // the getter gathers + decodes; enumeration forces it
            {
                rows++;
            }

            sw.Stop();
            var gatherMs = sw.Elapsed.TotalMilliseconds;
            GC.KeepAlive(rows);

            if (i >= warmups)
            {
                splits.Add(splitMs);
                steps.Add(stepMs);
                gathers.Add(gatherMs);
            }
        }

        splits.Sort();
        steps.Sort();
        gathers.Sort();
        return (splits[splits.Count / 2], steps[steps.Count / 2], gathers[gathers.Count / 2]);
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
