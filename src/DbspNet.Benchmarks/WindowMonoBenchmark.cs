// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks;

/// <summary>
/// Competitive A/B for the monomorphized window-aggregate order key
/// (docs/design-row-representation.md §23.7,
/// <see cref="CompileOptions.MonomorphizeWindowOrderKey"/>). The workload is the
/// fraud rolling-window feature view — per-customer <c>SUM/COUNT OVER (PARTITION
/// BY cust_id ORDER BY ts RANGE … PRECEDING)</c> over a TIMESTAMP order key — the
/// same shape the §23.7 microcosm (daily_market DATE key over large sorted
/// partitions) measured at W=1. Each config compiles the <b>same</b> plan twice
/// via <see cref="TypedPlanCompiler.TryCompileParallel"/>: once with the boxed
/// one-key <c>SortKeyComparer</c> (default) and once with the unboxed
/// <see cref="DbspNet.Core.Collections.LongKeyComparer{TRow}"/>; only the order
/// comparer differs. Reports step throughput and total allocation at W=1 and W,
/// cross-checking the two outputs byte-identical.
/// </summary>
/// <remarks>
/// The prize is workload-dependent, and this workload is chosen to be near
/// <b>best case</b>: a value-type (TIMESTAMP) order key over large per-customer
/// sorted partitions, where the boxed comparator boxes the key on every
/// <c>SortedDictionary</c> comparison (O(log n) per insert). String keys don't
/// box and small partitions do few comparisons, so a real mixed workload sees
/// less. Key-monomorphization narrows the typed-vs-structural gap; it does not
/// reach parity on wide rows (the struct-copy residual is columnar territory).
/// </remarks>
internal static class WindowMonoBenchmark
{
    private const long Day = 86_400_000_000L; // micros per day.
    private const long BaseTimeMicros = 1_700_000_000_000_000L;

    private static readonly string[] Ddl =
    {
        "CREATE TABLE customers (id BIGINT NOT NULL, name VARCHAR NOT NULL, zip VARCHAR NOT NULL)",
        @"CREATE TABLE transactions (
            txn_id BIGINT NOT NULL,
            cust_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            ts TIMESTAMP NOT NULL)",
    };

    private const string FeatureSql =
        @"SELECT t.txn_id, t.cust_id, c.zip,
            COUNT(*)      OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)  AS cnt_1d,
            SUM(t.amount) OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)  AS sum_1d,
            COUNT(*)      OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)  AS cnt_7d,
            SUM(t.amount) OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)  AS sum_7d,
            COUNT(*)      OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS cnt_30d,
            SUM(t.amount) OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS sum_30d
          FROM transactions t JOIN customers c ON t.cust_id = c.id";

    public static void Run(StringBuilder output, int historyTxns, int customers, int batchSize, int? workersOverride, int runs)
    {
        var workers = Math.Clamp(workersOverride ?? 14, 1, Environment.ProcessorCount);

        Console.WriteLine();
        Console.WriteLine(
            $"=== Window-agg order-key monomorphization A/B (txns={historyTxns:N0}, customers={customers:N0}, " +
            $"batch={batchSize:N0}, W={workers}, runs={runs}) ===");

        var plan = BuildPlan(Ddl, FeatureSql);
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            Console.WriteLine("  feature view has no parallel plan on this build — cannot run the gate");
            output.AppendLine("> The fraud feature view does not parallelize on this build — gate skipped.");
            output.AppendLine();
            return;
        }

        probe!.Dispose();

        Console.WriteLine("Generating data…");
        var custRows = BuildCustomers(customers);
        var txnRows = BuildTransactions(historyTxns, customers, seed: 7);
        var avgPartition = (double)historyTxns / customers;

        output.AppendLine("# DbspNet — window-agg order-key monomorphization gate (§23.7)");
        output.AppendLine();
        output.AppendLine(
            "Competitive A/B of keying the typed `PARTITION BY … ORDER BY` window's per-partition " +
            "ordered state on the **unboxed** monotone long (`LongKeyComparer`) vs the boxed " +
            "`SortKeyComparer`. Workload: the fraud rolling-window feature view (per-customer " +
            "1d/7d/30d `SUM`/`COUNT OVER … RANGE PRECEDING`, TIMESTAMP order key). Only the order " +
            "comparer differs between configs; the mono output is cross-checked byte-identical to boxed.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {historyTxns:N0} transactions over {customers:N0} customers " +
            $"(~{avgPartition:N0} txns/partition), {batchSize:N0}-event micro-batches, median of {runs} " +
            $"run(s) after one warmup. Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
        output.AppendLine();
        output.AppendLine(
            "| W | Config | Throughput (events/s) | Speedup vs boxed | Alloc (MiB) | Alloc vs boxed |");
        output.AppendLine(
            "|--:|:-------|----------------------:|-----------------:|------------:|---------------:|");

        var workerCounts = workers > 1 ? new[] { 1, workers } : new[] { 1 };
        foreach (var w in workerCounts)
        {
            RunAt(output, plan, w, custRows, txnRows, batchSize, runs);
        }

        output.AppendLine();
        output.AppendLine(
            "**Reading it.** *Speedup vs boxed* > 1 means the monomorphized comparer sped the step up; " +
            "*Alloc vs boxed* < 1 means it allocated less. This is a near-best-case workload (value-type " +
            "TIMESTAMP key over large sorted partitions); string keys and small partitions see less.");
        output.AppendLine();
    }

    private static void RunAt(
        StringBuilder output, LogicalPlan plan, int workers,
        List<(object?[], long)> custRows, List<(object?[], long)> txnRows, int batchSize, int runs)
    {
        Console.WriteLine($"  -- W={workers} --");

        // Explicit false/true so the A/B is unaffected by the (now default-on) gate.
        var boxed = new CompileOptions { MonomorphizeWindowOrderKey = false };
        var mono = new CompileOptions { MonomorphizeWindowOrderKey = true };

        // Reference output (boxed) — the mono output must reproduce it exactly.
        var reference = RunStream(plan, workers, boxed, custRows, txnRows, batchSize, out _, out _);

        var (boxedEps, boxedAlloc) = Measure(plan, workers, boxed, custRows, txnRows, batchSize, runs);
        var (monoEps, monoAlloc, monoOut) = MeasureChecked(plan, workers, mono, custRows, txnRows, batchSize, runs);

        if (!SameMultiset(reference, monoOut))
        {
            throw new InvalidOperationException($"mono output diverged from boxed at W={workers} — aborting gate");
        }

        EmitRow(output, workers, "boxed", boxedEps, boxedAlloc, boxedEps, boxedAlloc);
        EmitRow(output, workers, "mono", monoEps, monoAlloc, boxedEps, boxedAlloc);
    }

    private static void EmitRow(
        StringBuilder output, int workers, string label,
        double eps, double allocMiB, double baselineEps, double baselineAlloc)
    {
        var speedup = baselineEps > 0 ? eps / baselineEps : 1.0;
        var allocRatio = baselineAlloc > 0 ? allocMiB / baselineAlloc : 1.0;
        Console.WriteLine(
            $"    {label,-6} {eps,14:N0} ev/s  {BenchmarkHarness.FormatRatio(speedup).Trim(),7}  " +
            $"alloc={allocMiB,9:N1} MiB  ×{allocRatio:F3}");
        output.AppendLine(
            $"| {workers} | {label} | {eps:N0} | {BenchmarkHarness.FormatRatio(speedup).Trim()} | " +
            $"{allocMiB:N1} | {allocRatio:F3} |");
    }

    /// <summary>Median step throughput (events/s) + median total allocation (MiB).</summary>
    private static (double EventsPerSec, double AllocMiB) Measure(
        LogicalPlan plan, int workers, CompileOptions options,
        List<(object?[], long)> custRows, List<(object?[], long)> txnRows, int batchSize, int runs)
    {
        var times = new List<double>();
        var allocs = new List<double>();
        for (var run = 0; run < runs + 1; run++) // first run warmup.
        {
            _ = RunStream(plan, workers, options, custRows, txnRows, batchSize, out var eps, out var allocMiB);
            if (run > 0)
            {
                times.Add(eps);
                allocs.Add(allocMiB);
            }
        }

        times.Sort();
        allocs.Sort();
        return (times[times.Count / 2], allocs[allocs.Count / 2]);
    }

    private static (double EventsPerSec, double AllocMiB, Dictionary<string, long> Output) MeasureChecked(
        LogicalPlan plan, int workers, CompileOptions options,
        List<(object?[], long)> custRows, List<(object?[], long)> txnRows, int batchSize, int runs)
    {
        var (eps, alloc) = Measure(plan, workers, options, custRows, txnRows, batchSize, runs);
        var output = RunStream(plan, workers, options, custRows, txnRows, batchSize, out _, out _);
        return (eps, alloc, output);
    }

    /// <summary>
    /// Streams the transaction history through a fresh W-worker circuit compiled
    /// with <paramref name="options"/> (customers loaded once first, untimed), and
    /// returns the final materialized output. The transaction stream's wall-clock
    /// and total allocation are reported via the out params.
    /// </summary>
    private static Dictionary<string, long> RunStream(
        LogicalPlan plan, int workers, CompileOptions options,
        List<(object?[], long)> custRows, List<(object?[], long)> txnRows, int batchSize,
        out double eventsPerSec, out double allocMiB)
    {
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q, null, options))
        {
            throw new InvalidOperationException("parallel compile unexpectedly failed");
        }

        using (q)
        {
            q!.Table("customers").Push(custRows);
            q.Step();

            var allocBefore = GC.GetTotalAllocatedBytes();
            var sw = Stopwatch.StartNew();
            var batch = new List<(object?[], long)>(batchSize);
            foreach (var row in txnRows)
            {
                batch.Add(row);
                if (batch.Count >= batchSize)
                {
                    q.Table("transactions").Push(batch);
                    q.Step();
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                q.Table("transactions").Push(batch);
                q.Step();
            }

            sw.Stop();
            allocMiB = (GC.GetTotalAllocatedBytes() - allocBefore) / (1024.0 * 1024.0);
            eventsPerSec = txnRows.Count / sw.Elapsed.TotalSeconds;

            var map = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var (values, weight) in q.Current)
            {
                var key = string.Join("|", values.Select(v => v?.ToString() ?? " "));
                map[key] = map.GetValueOrDefault(key) + weight;
                if (map[key] == 0)
                {
                    map.Remove(key);
                }
            }

            return map;
        }
    }

    private static bool SameMultiset(Dictionary<string, long> a, Dictionary<string, long> b)
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

    private static long TxnStep(int total) => (90 * Day) / Math.Max(1, total);

    private static List<(object?[], long)> BuildCustomers(int count)
    {
        var rows = new List<(object?[], long)>(count);
        for (var i = 0; i < count; i++)
        {
            rows.Add((new object?[] { (long)i, "cust-" + i, (10000 + (i % 90000)).ToString() }, 1L));
        }

        return rows;
    }

    private static List<(object?[], long)> BuildTransactions(int count, int customers, int seed)
    {
        var rng = new Random(seed);
        var step = TxnStep(count);
        var rows = new List<(object?[], long)>(count);
        for (var i = 0; i < count; i++)
        {
            rows.Add((
                new object?[]
                {
                    (long)i,
                    (long)rng.Next(customers),
                    (long)rng.Next(1, 100_000),
                    new Timestamp(BaseTimeMicros + (i * step)),
                },
                1L));
        }

        return rows;
    }

    private static LogicalPlan BuildPlan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanOptimizer.Optimize(plan);
    }
}
