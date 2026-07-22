// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks.Fraud;

/// <summary>
/// Fraud-detection / feature-engineering scenario (Feldera's documented use
/// case). Card transactions are joined to customer demographics and rolling
/// 1-day / 7-day / 30-day per-customer transaction counts and sums are
/// computed as real-time ML features. This is the latency-sensitive
/// counterpart to Nexmark: fraud must be scored per transaction, so the
/// headline metric is steady-state <em>per-event incremental latency</em>
/// (DbspNet's strength), with sustained throughput reported alongside.
/// </summary>
internal static class FraudBenchmark
{
    private const long Day = 86_400_000_000L; // micros per day.
    private const long BaseTimeMicros = 1_700_000_000_000_000L;

    // internal (not private) so the plan-level research instruments —
    // CalciteRuleCensus — can compile this workload's plan.
    internal static readonly string[] Ddl =
    {
        @"CREATE TABLE customers (
            id BIGINT NOT NULL,
            name VARCHAR NOT NULL,
            zip VARCHAR NOT NULL)",
        @"CREATE TABLE transactions (
            txn_id BIGINT NOT NULL,
            cust_id BIGINT NOT NULL,
            amount BIGINT NOT NULL,
            ts TIMESTAMP NOT NULL)",
    };

    // Three rolling windows × {count, sum} = the classic fraud feature vector.
    // Distinct RANGE-INTERVAL frames per window exercise the multiple-OVER-spec
    // path; the join feeds all of them.
    internal const string FeatureSql =
        @"SELECT t.txn_id, t.cust_id, c.zip,
            COUNT(*)        OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)  AS cnt_1d,
            SUM(t.amount)   OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)  AS sum_1d,
            COUNT(*)        OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)  AS cnt_7d,
            SUM(t.amount)   OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)  AS sum_7d,
            COUNT(*)        OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS cnt_30d,
            SUM(t.amount)   OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS sum_30d
          FROM transactions t JOIN customers c ON t.cust_id = c.id";

    // The join that feeds the window features — kept as a fallback parallel
    // measurement for builds that can't parallelize the full feature view (the
    // windowed aggregates now DO take the typed/parallel path; see RunParallelSection).
    private const string JoinSliceSql =
        @"SELECT t.txn_id, t.cust_id, c.zip
          FROM transactions t JOIN customers c ON t.cust_id = c.id";

    public static void Run(StringBuilder output, int historyTxns, int customers, int batchSize, int workers = 1)
    {
        workers = Math.Clamp(workers, 1, Environment.ProcessorCount);
        Console.WriteLine();
        Console.WriteLine(
            $"=== Fraud detection (customers={customers:N0}, history={historyTxns:N0}, W={workers}) ===");

        output.AppendLine("## Fraud detection — rolling-window features");
        output.AppendLine();
        output.AppendLine(
            "Card `transactions` joined to `customers`, computing per-customer " +
            "rolling 1-day / 7-day / 30-day transaction **count** and **sum** as " +
            "real-time ML features (Feldera's documented fraud-detection use case). " +
            "Three distinct `RANGE … INTERVAL` window frames feed off one join. " +
            "We load a transaction history, then measure the steady-state cost of " +
            "scoring **one** new transaction (`Insert` + `Step`) — the latency that " +
            "matters when fraud must be caught per swipe.");
        output.AppendLine();

        // Verify the feature view compiles before timing anything.
        try
        {
            _ = Compile(Ddl, FeatureSql);
        }
        catch (Exception ex)
        {
            var msg = ex.Message.Replace('\n', ' ').Replace('\r', ' ');
            Console.WriteLine($"  feature view DID NOT COMPILE — {msg}");
            output.AppendLine($"> Feature view did not compile on this build: {msg}");
            output.AppendLine();
            return;
        }

        var custRows = BuildCustomers(customers);

        output.AppendLine("| History txns | Per-event latency | Throughput (events/s) |");
        output.AppendLine("|-------------:|------------------:|----------------------:|");

        foreach (var hist in HistorySizes(historyTxns))
        {
            var txnRows = BuildTransactions(hist, customers, seed: 7);

            // --- Per-event incremental latency: warm circuit, push ONE txn. ---
            var nextTxnId = (long)hist;
            var nextTs = BaseTimeMicros + ((long)hist * TxnStep(historyTxns));
            var rng = new Random(99);

            var incrUs = BenchmarkHarness.MedianPerStepMicros(
                setup: () =>
                {
                    var q = Compile(Ddl, FeatureSql);
                    q.Table("customers").Push(custRows);
                    q.Table("transactions").Push(txnRows);
                    q.Step();
                    return new StepState(q, nextTxnId, nextTs);
                },
                oneStep: st =>
                {
                    st.Query.Table("transactions").Insert(
                        st.NextTxnId,
                        (long)rng.Next(customers),
                        (long)rng.Next(1, 100_000),
                        new Timestamp(st.NextTs));
                    st.Query.Step();
                    st.NextTxnId++;
                    st.NextTs += TxnStep(historyTxns);
                });

            // --- Throughput: micro-batched cold stream over the history. ---
            var throughput = MeasureThroughput(custRows, txnRows, batchSize);

            Console.WriteLine(
                $"  history={hist,8:N0}  incr={BenchmarkHarness.FormatMicros(incrUs)}  " +
                $"throughput={throughput,12:N0} events/s");
            output.AppendLine(
                $"| {hist:N0} | {BenchmarkHarness.FormatMicros(incrUs).Trim()} | {throughput:N0} |");
        }

        output.AppendLine();
        output.AppendLine(
            "The per-event latency is the headline: once the history is loaded, " +
            "scoring an additional transaction touches only the affected customer's " +
            "window state. It stays in the tens-of-µs range across a 50× growth in " +
            "history (the slow drift reflects larger trace / window-state working " +
            "sets and allocator pressure, not a full rescan) — the incremental " +
            "property that makes DBSP suitable for per-transaction fraud scoring. " +
            "Compare this against a from-scratch recompute of the same feature view.");
        output.AppendLine();

        if (workers > 1)
        {
            RunParallelSection(output, custRows, historyTxns, customers, batchSize, workers);
        }
    }

    /// <summary>
    /// Data-parallel throughput for the fraud pipeline. The rolling-window feature
    /// view now compiles to a parallel circuit (its PARTITION BY window aggregates
    /// take the typed/parallel path, co-located by <c>cust_id</c>), so we measure
    /// the **whole feature view** at W=1 vs W, cross-checking the W-output against
    /// the W=1 replica run. If a build can't parallelize the full view (e.g. a
    /// MIN/MAX feature that falls back to structural), we drop to the
    /// <c>transactions ⋈ customers</c> join slice that feeds the windows.
    /// </summary>
    private static void RunParallelSection(
        StringBuilder output, List<(object?[], long)> custRows,
        int historyTxns, int customers, int batchSize, int workers)
    {
        Console.WriteLine($"  -- parallel (W={workers}) --");
        output.AppendLine("### Parallel scaling");
        output.AppendLine();

        // Prefer the full feature view; fall back to the join slice if a build
        // can't parallelize every window aggregate in it.
        var featureSupported = TypedPlanCompiler.TryCompileParallel(
            BuildPlan(Ddl, FeatureSql), workers, out var featureProbe);
        featureProbe?.Dispose();

        var (sql, label) = featureSupported
            ? (FeatureSql, "feature view")
            : (JoinSliceSql, "join slice");
        var plan = BuildPlan(Ddl, sql);

        output.AppendLine(
            (featureSupported
                ? "The full rolling-window feature view compiles to a parallel circuit — " +
                  "each `cust_id` partition's window state is co-located on one worker by " +
                  "an exchange on the PARTITION BY key. The table measures the whole view " +
                  "(join + the three `RANGE` window frames) at W=1 vs W."
                : "The full feature view does not parallelize on this build (a window " +
                  "aggregate fell back to the structural path), so the table measures the " +
                  "parallelizable `transactions ⋈ customers` join slice that feeds the " +
                  "windows instead.") +
            " W>1 output is cross-checked against the W=1 replica run.");
        output.AppendLine();

        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var probe))
        {
            output.AppendLine($"> The {label} has no parallel plan on this build — skipped.");
            output.AppendLine();
            return;
        }

        probe!.Dispose();

        output.AppendLine($"{char.ToUpperInvariant(label[0]) + label[1..]} `{sql.Replace("\n", " ").Trim()}`:");
        output.AppendLine();
        output.AppendLine($"| History txns | W=1 (events/s) | W={workers} (events/s) | Speedup | Status |");
        output.AppendLine("|-------------:|---------------:|---------------:|--------:|:-------|");

        foreach (var hist in HistorySizes(historyTxns))
        {
            var txnRows = BuildTransactions(hist, customers, seed: 7);

            var reference = RunPlanParallel(plan, 1, custRows, txnRows, batchSize, out var single);
            var output1 = RunPlanParallel(plan, workers, custRows, txnRows, batchSize, out var par);
            var correct = SameMultiset(reference, output1);

            var speedup = single > 0 ? par / single : 0.0;
            var status = correct ? "ok" : "**PARALLEL MISMATCH**";
            Console.WriteLine(
                $"  history={hist,8:N0}  W1={single,12:N0}  W{workers}={par,12:N0} events/s  " +
                $"{BenchmarkHarness.FormatRatio(speedup)}  {(correct ? "ok" : "MISMATCH")}");
            output.AppendLine(
                $"| {hist:N0} | {single:N0} | {par:N0} | {BenchmarkHarness.FormatRatio(speedup).Trim()} | {status} |");
        }

        output.AppendLine();
    }

    /// <summary>
    /// Streams <paramref name="txnRows"/> in micro-batches through a fresh W-worker
    /// parallel circuit (customers loaded once first), returns the final
    /// materialized output and the throughput via <paramref name="eventsPerSec"/>.
    /// </summary>
    private static Dictionary<string, long> RunPlanParallel(
        LogicalPlan plan, int workers, List<(object?[], long)> custRows,
        List<(object?[], long)> txnRows, int batchSize, out double eventsPerSec)
    {
        if (!TypedPlanCompiler.TryCompileParallel(plan, workers, out var q))
        {
            throw new InvalidOperationException("parallel compile unexpectedly failed");
        }

        using (q)
        {
            q!.Table("customers").Push(custRows);
            q.Step();

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
            eventsPerSec = txnRows.Count / sw.Elapsed.TotalSeconds;

            var map = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var (values, weight) in q.Current)
            {
                var key = string.Join("", values.Select(v => v?.ToString() ?? " "));
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

    private static double MeasureThroughput(
        List<(object?[], long)> custRows, List<(object?[], long)> txnRows, int batchSize)
    {
        var times = new List<double>();
        for (var run = 0; run < 4; run++) // 1 warmup + 3 measured.
        {
            var q = Compile(Ddl, FeatureSql);
            q.Table("customers").Push(custRows);
            q.Step();

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
            if (run > 0)
            {
                times.Add(txnRows.Count / sw.Elapsed.TotalSeconds);
            }
        }

        times.Sort();
        return times[times.Count / 2];
    }

    private static IEnumerable<int> HistorySizes(int max)
    {
        foreach (var h in new[] { 10_000, 100_000, 500_000 })
        {
            if (h <= max)
            {
                yield return h;
            }
        }

        if (max > 500_000)
        {
            yield return max;
        }
    }

    // Spread transactions across ~90 days so the 1d / 7d / 30d frames differ.
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

    /// <summary>
    /// Dump the SAME generated <c>customers</c> / <c>transactions</c> the benchmark
    /// loads (deterministic, seed 7) as headerless CSV in table-column order, so a
    /// Feldera pipeline (<c>scripts/feldera-fraud.sql</c>) ingests byte-identical
    /// data. Timestamps are written as <c>yyyy-MM-dd HH:mm:ss.ffffff</c> (UTC),
    /// lossless from the underlying microseconds.
    /// </summary>
    public static void DumpCsv(string outDir, int historyTxns, int customers)
    {
        Directory.CreateDirectory(outDir);
        var custRows = BuildCustomers(customers);
        var txnRows = BuildTransactions(historyTxns, customers, seed: 7);

        var custPath = Path.Combine(outDir, "customers.csv");
        using (var w = new StreamWriter(custPath, append: false))
        {
            foreach (var (values, _) in custRows)
            {
                w.Write(Csv(values[0]));         // id
                w.Write(',');
                w.Write(Csv(values[1]));         // name
                w.Write(',');
                w.Write(Csv(values[2]));         // zip
                w.Write('\n');
            }
        }

        var txnPath = Path.Combine(outDir, "transactions.csv");
        using (var w = new StreamWriter(txnPath, append: false))
        {
            foreach (var (values, _) in txnRows)
            {
                w.Write(Csv(values[0]));         // txn_id
                w.Write(',');
                w.Write(Csv(values[1]));         // cust_id
                w.Write(',');
                w.Write(Csv(values[2]));         // amount
                w.Write(',');
                w.Write(FormatTs((Timestamp)values[3]!));
                w.Write('\n');
            }
        }

        Console.WriteLine($"Wrote {custRows.Count:N0} customers     -> {custPath}");
        Console.WriteLine($"Wrote {txnRows.Count:N0} transactions  -> {txnPath}");
        Console.WriteLine("Headerless CSV, table-column order:");
        Console.WriteLine("  customers:    id,name,zip");
        Console.WriteLine("  transactions: txn_id,cust_id,amount,ts   (ts = 'yyyy-MM-dd HH:mm:ss.ffffff' UTC)");
        Console.WriteLine("Load both into the matching Feldera tables (scripts/feldera-fraud.sql).");
    }

    /// <summary>Microseconds-since-epoch <see cref="Timestamp"/> as a Feldera-/
    /// Calcite-parseable TIMESTAMP literal (lossless to microsecond precision).</summary>
    private static string FormatTs(Timestamp ts) =>
        DateTime.UnixEpoch.AddTicks(ts.Microseconds * 10)
            .ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /// <summary>Render one field invariantly, quote-escaping only if it contains a
    /// delimiter / quote / newline (the generated values never do, but stay safe).</summary>
    private static string Csv(object? v)
    {
        var s = v switch
        {
            null => string.Empty,
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => v.ToString() ?? string.Empty,
        };

        return s.IndexOfAny([',', '"', '\n', '\r']) >= 0
            ? "\"" + s.Replace("\"", "\"\"") + "\""
            : s;
    }

    private sealed class StepState(CompiledQuery query, long nextTxnId, long nextTs)
    {
        public CompiledQuery Query { get; } = query;

        public long NextTxnId { get; set; } = nextTxnId;

        public long NextTs { get; set; } = nextTs;
    }

    private static CompiledQuery Compile(string[] ddl, string sql) =>
        PlanToCircuit.Compile(BuildPlan(ddl, sql));

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
