// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;
using DbspNet.Benchmarks;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

// Profiling sub-command: `dotnet run -- profile {typed|structural|handwired} [seconds]`
if (args.Length > 0 && args[0] == "profile")
{
    return DbspNet.Benchmarks.ProfileHotPath.Run(args);
}

// Cross-system comparison sub-commands (Feldera-compatible workloads).
//   dotnet run -- nexmark    [totalEvents] [batchSize] [runs]
//   dotnet run -- fraud      [historyTxns] [customers] [batchSize]
//   dotnet run -- comparison                       (runs both with defaults)
if (args.Length > 0 && args[0] is "nexmark" or "fraud" or "comparison")
{
    return DbspNet.Benchmarks.ComparisonBenchmarks.Run(args);
}

// Parallel-scaling sub-command: `dotnet run -- parallel [maxWorkers]`
if (args.Length > 0 && args[0] == "parallel")
{
    return DbspNet.Benchmarks.ParallelScalingBenchmark.RunConsole(args);
}

// Merge-probe prototype sub-command: `dotnet run -- mergeprobe`
// Point probe vs galloping merge over the spine indexed trace
// (docs/design-row-representation.md §6.1).
if (args.Length > 0 && args[0] == "mergeprobe")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — merge-probe prototype");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.MergeProbeBenchmark.Run(sb);
    var mpPath = Path.Combine(FindDocsDir(), "merge-probe-bench.md");
    File.WriteAllText(mpPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(mpPath)}");
    return 0;
}

// q4 end-to-end flat lazy-view gate: `dotnet run -- q4flat [events] [workers] [runs]`
// Whole q4 parallel pipeline, flat·eager vs flat·lazy aggregate
// (docs/design-row-representation.md §14.10).
if (args.Length > 0 && args[0] == "q4flat")
{
    int Arg(int i, int fallback) =>
        args.Length > i && int.TryParse(args[i], System.Globalization.NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var v) ? v : fallback;

    var totalEvents = Arg(1, 1_000_000);
    int? workers = args.Length > 2 && int.TryParse(args[2], System.Globalization.NumberStyles.Integer,
        CultureInfo.InvariantCulture, out var w) ? w : null;
    var runs = Arg(3, 3);

    var sb = new StringBuilder();
    DbspNet.Benchmarks.Q4FlatBenchmark.Run(sb, totalEvents, workers, runs);
    var q4fPath = Path.Combine(FindDocsDir(), "q4-flat-bench.md");
    File.WriteAllText(q4fPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(q4fPath)}");
    return 0;
}

// Flat aggregate lazy merge-view A/B gate: `dotnet run -- flatagg`
// Eager rebuild vs lazy LazyMergeMultiset on the q4 growing-group shape
// (docs/design-row-representation.md §14.9).
if (args.Length > 0 && args[0] == "flatagg")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — flat aggregate lazy merge-view");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.FlatAggBenchmark.Run(sb);
    var faPath = Path.Combine(FindDocsDir(), "flat-agg-bench.md");
    File.WriteAllText(faPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(faPath)}");
    return 0;
}

// Surrogate-key microbench: `dotnet run -- surrogatebench`
// Whole-row dictionary keys vs interned int surrogates — locates the §14.3(a)
// re-touch crossover (docs/design-row-representation.md §14.6).
if (args.Length > 0 && args[0] == "surrogatebench")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — surrogate-key microbench");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.SurrogateKeyBenchmark.Run(sb);
    var skPath = Path.Combine(FindDocsDir(), "surrogate-key-bench.md");
    File.WriteAllText(skPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(skPath)}");
    return 0;
}

// Shared-arrangement gate sub-command: `dotnet run -- sharedarr`
// One shared index vs F private right traces across a join fan-out
// (docs/design-row-representation.md §6.2, Option 2 — cross-operator sharing).
if (args.Length > 0 && args[0] == "sharedarr")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — shared arrangement");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.SharedArrangementBenchmark.Run(sb);
    var saPath = Path.Combine(FindDocsDir(), "shared-arrangement-bench.md");
    File.WriteAllText(saPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(saPath)}");
    return 0;
}

// Spine + memtable fuller evaluation: `dotnet run -- spineeval [events] [W] [runs]`
// flat vs spine·merge·staged across stateful Nexmark queries + a q4 capacity sweep
// (docs/design-row-representation.md §10 — the "flip the default?" decision).
if (args.Length > 0 && args[0] == "spineeval")
{
    int Arg(int i, int fallback) =>
        args.Length > i && int.TryParse(args[i], System.Globalization.NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var v) ? v : fallback;

    var sb = new StringBuilder();
    DbspNet.Benchmarks.SpineEvalBenchmark.Run(sb, Arg(1, 1_000_000), Arg(2, Environment.ProcessorCount), Arg(3, 3));
    var sePath = Path.Combine(FindDocsDir(), "spine-eval-bench.md");
    File.WriteAllText(sePath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(sePath)}");
    return 0;
}

// DISTINCT flat-vs-spine-vs-staged gate: `dotnet run -- distinct`
// Shows the SpineZSetTrace memtable (docs §13) closing the spine DISTINCT gap.
if (args.Length > 0 && args[0] == "distinct")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — distinct (flat vs spine vs staged)");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.DistinctBenchmark.Run(sb);
    var dPath = Path.Combine(FindDocsDir(), "distinct-bench.md");
    File.WriteAllText(dPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(dPath)}");
    return 0;
}

// Arrangement-CSE optimizer-rule gate: `dotnet run -- sharedarrsql`
// Star-schema SQL (F facts joined to one wide dim) compiled with vs without
// CompileOptions.ShareArrangements (docs/design-row-representation.md §9.6).
if (args.Length > 0 && args[0] == "sharedarrsql")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — shared arrangement (SQL optimizer rule)");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.SharedArrangementSqlBenchmark.Run(sb);
    var sqPath = Path.Combine(FindDocsDir(), "shared-arrangement-sql-bench.md");
    File.WriteAllText(sqPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(sqPath)}");
    return 0;
}

// Join merge-probe sub-command: `dotnet run -- joinprobe`
// Point probe vs galloping merge driving the whole SpineIncrementalJoinOp.Step
// (docs/design-row-representation.md §8).
if (args.Length > 0 && args[0] == "joinprobe")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — join merge-probe");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.JoinProbeBenchmark.Run(sb);
    var jpPath = Path.Combine(FindDocsDir(), "join-probe-bench.md");
    File.WriteAllText(jpPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(jpPath)}");
    return 0;
}

// Aggregate merge-probe sub-command: `dotnet run -- aggprobe`
// Point probe vs galloping merge driving SpineIncrementalAggregateOp.Step
// (docs/design-row-representation.md §8, the IMultiset increment).
if (args.Length > 0 && args[0] == "aggprobe")
{
    var sb = new StringBuilder();
    sb.AppendLine("# DbspNet — aggregate merge-probe");
    sb.AppendLine();
    sb.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
    sb.AppendLine();
    DbspNet.Benchmarks.AggregateProbeBenchmark.Run(sb);
    var apPath = Path.Combine(FindDocsDir(), "aggregate-probe-bench.md");
    File.WriteAllText(apPath, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(apPath)}");
    return 0;
}

// q4 spine merge-probe gate: `dotnet run -- q4spine [totalEvents] [workers] [runs]`
// End-to-end test of the spine merge-probe on Nexmark q4 at the host core count
// (docs/design-row-representation.md §8 — the deferred "q4 gate").
if (args.Length > 0 && args[0] == "q4spine")
{
    int Arg(int i, int fallback) =>
        args.Length > i && int.TryParse(args[i], System.Globalization.NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var v) ? v : fallback;

    var totalEvents = Arg(1, 1_000_000);
    int? workers = args.Length > 2 && int.TryParse(args[2], System.Globalization.NumberStyles.Integer,
        CultureInfo.InvariantCulture, out var w) ? w : null;
    var runs = Arg(3, 3);

    var sb = new StringBuilder();
    DbspNet.Benchmarks.Q4SpineBenchmark.Run(sb, totalEvents, workers, runs);
    var q4Path = Path.Combine(FindDocsDir(), "q4-spine-bench.md");
    File.WriteAllText(q4Path, sb.ToString());
    Console.WriteLine();
    Console.WriteLine($"Report written to {Path.GetFullPath(q4Path)}");
    return 0;
}

var output = new StringBuilder();
output.AppendLine("# DbspNet — benchmarks");
output.AppendLine();
output.AppendLine(
    "Cold-batch vs. steady-state incremental latency. The query shapes below cover " +
    "the canonical SQL surface (filter, joined group-by, transitive closure via " +
    "`WITH RECURSIVE`) plus row-layout variants (nullable column, emitted-equality " +
    "codec, hand-wired typed rows) and pure-trace microbenchmarks against the " +
    "spine.");
output.AppendLine(
    "Each row: load the circuit fresh, measure the full compile-load-step time " +
    "(\"batch\"); separately, load the circuit once, push one additional row and " +
    "measure just `Step()` (\"incremental\"). Both are medians of multiple runs " +
    "with a warmup pass.");
output.AppendLine();
output.AppendLine("Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).");
output.AppendLine();
output.AppendLine($"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores.");
output.AppendLine();

RunFilterBenchmark(output);
FusionBenchmark.Run(output);
RunMultiAggregateBenchmark(output);
RunJoinedGroupByBenchmark(output);
RunJoinedGroupBySpineBenchmark(output);
RunJoinedGroupByNullableBenchmark(output);
RunJoinedGroupByEmittedCodecBenchmark(output);
RunJoinedGroupByTypedBenchmark(output);
RunTransitiveClosureBenchmark(output);
PureTraceBenchmark.Run(output);
DistinctBenchmark.Run(output);
ParallelScalingBenchmark.Run(output);

AppendInterpretation(output);

var outPath = args.Length > 0 ? args[0] : Path.Combine("..", "..", "docs", "benchmarks.md");
File.WriteAllText(outPath, output.ToString());
Console.WriteLine();
Console.WriteLine($"Report written to {Path.GetFullPath(outPath)}");
return 0;

// ---------- Shared helpers ----------

static CompiledQuery Compile(string[] ddl, string sql)
{
    var catalog = new Catalog();
    var resolver = new Resolver(catalog);
    foreach (var s in ddl)
    {
        resolver.Resolve(Parser.ParseStatement(s));
    }

    var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    return PlanToCircuit.Compile(PlanOptimizer.Optimize(plan));
}

static CompiledQuery CompileWithEmittedCodec(string[] ddl, string sql)
{
    var catalog = new Catalog();
    var resolver = new Resolver(catalog);
    foreach (var s in ddl)
    {
        resolver.Resolve(Parser.ParseStatement(s));
    }

    var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    return PlanToCircuit.Compile(PlanOptimizer.Optimize(plan), EmittedEqualityCodec.Instance);
}

static CompiledQuery CompileSpine(string[] ddl, string sql)
{
    var catalog = new Catalog();
    var resolver = new Resolver(catalog);
    foreach (var s in ddl)
    {
        resolver.Resolve(Parser.ParseStatement(s));
    }

    var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    return PlanToCircuit.Compile(
        PlanOptimizer.Optimize(plan),
        snapshotCodecs: null,
        new CompileOptions { TraceFamily = TraceFamily.Spine });
}

// Walk up from the current directory to the repo's docs/ folder, so the
// report lands in the right place regardless of the run's working directory.
static string FindDocsDir()
{
    var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
    while (dir is not null)
    {
        var candidate = Path.Combine(dir.FullName, "docs");
        if (Directory.Exists(candidate) && File.Exists(Path.Combine(dir.FullName, "README.md")))
        {
            return candidate;
        }

        dir = dir.Parent;
    }

    return Directory.GetCurrentDirectory();
}

static void PrintHeader(string name)
{
    Console.WriteLine();
    Console.WriteLine($"=== {name} ===");
}

static void PrintRow(int n, double batchMs, double incrUs)
{
    Console.WriteLine(
        $"  N={n,-7} batch={BenchmarkHarness.FormatMs(batchMs)}  incr={BenchmarkHarness.FormatMicros(incrUs)}  speedup={BenchmarkHarness.FormatRatio(batchMs * 1000.0 / incrUs)}");
}

static void AppendTable(StringBuilder sb, string title, string description,
    IReadOnlyList<(int N, double BatchMs, double IncrUs)> rows)
{
    sb.AppendLine($"## {title}");
    sb.AppendLine();
    sb.AppendLine(description);
    sb.AppendLine();
    sb.AppendLine("| N          | Batch         | Incremental    | Speedup |");
    sb.AppendLine("|-----------:|--------------:|---------------:|--------:|");
    foreach (var (n, batchMs, incrUs) in rows)
    {
        sb.AppendLine(
            $"| {n,10} | {BenchmarkHarness.FormatMs(batchMs).Trim()} | {BenchmarkHarness.FormatMicros(incrUs).Trim()} | {BenchmarkHarness.FormatRatio(batchMs * 1000.0 / incrUs).Trim()} |");
    }

    sb.AppendLine();
}

// ---------- Benchmark 1: Filter (pipelined, stateless) ----------

static void RunFilterBenchmark(StringBuilder output)
{
    PrintHeader("Filter");
    const string sql = "SELECT id, value FROM events WHERE value > 500 AND status = 'active'";
    var ddl = new[] { "CREATE TABLE events (id INT NOT NULL, value INT NOT NULL, status VARCHAR NOT NULL)" };

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        var rng = new Random(42);
        var data = GenerateEvents(n, rng);
        var eventsDelta = data.Select(d => (Values: new object?[] { d.Id, d.Value, d.Status }, Weight: 1L)).ToList();

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => Compile(ddl, sql),
            run: q =>
            {
                // Bulk push — all N rows become a single Z-set before Step(),
                // so the circuit sees this as one batch. Using Insert() in a
                // loop is O(N²) because each Insert re-merges into the
                // pending delta; not representative of batch behavior.
                q.Table("events").Push(eventsDelta);
                q.Step();
            });

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = Compile(ddl, sql);
                q.Table("events").Push(eventsDelta);
                q.Step();
                return (Query: q, Next: n);
            },
            oneStep: state =>
            {
                state.Query.Table("events").Insert(state.Next, 750, "active");
                state.Query.Step();
                state.Next++;
            });

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Filter — `WHERE value > 500 AND status = 'active'`",
        "Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.",
        rows);
}

static List<(int Id, int Value, string Status)> GenerateEvents(int n, Random rng)
{
    var data = new List<(int, int, string)>(n);
    for (var i = 0; i < n; i++)
    {
        data.Add((i, rng.Next(0, 1000), rng.Next(2) == 0 ? "active" : "inactive"));
    }

    return data;
}

// ---------- Benchmark 2: Multi-aggregate GROUP BY ----------

static void RunMultiAggregateBenchmark(StringBuilder output)
{
    PrintHeader("Multi-aggregate GROUP BY");
    const string sql =
        "SELECT k, SUM(v) AS s, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx FROM data GROUP BY k";
    var ddl = new[] { "CREATE TABLE data (k INT NOT NULL, v INT NOT NULL)" };

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        var rng = new Random(7);
        var data = new List<(int K, int V)>(n);
        for (var i = 0; i < n; i++)
        {
            data.Add((rng.Next(0, 100), rng.Next(0, 10_000)));
        }

        var dataDelta = data.Select(d => (Values: new object?[] { d.K, d.V }, Weight: 1L)).ToList();

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => Compile(ddl, sql),
            run: q =>
            {
                q.Table("data").Push(dataDelta);
                q.Step();
            });

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = Compile(ddl, sql);
                q.Table("data").Push(dataDelta);
                q.Step();
                return (Query: q, Rng: new Random(99));
            },
            oneStep: state =>
            {
                state.Query.Table("data").Insert(state.Rng.Next(0, 100), state.Rng.Next(0, 10_000));
                state.Query.Step();
            });

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups",
        "Stateful composite aggregator with retractions on every group-key hit. " +
        "All four aggregators are now O(|delta|) per changed group: SUM / COUNT " +
        "fold the delta into running state; MIN / MAX maintain a per-group " +
        "sorted set of distinct values with positive net weight, indexed for " +
        "O(log n) extremum lookup.",
        rows);
}

// ---------- Benchmark 3: Joined GROUP BY ----------

static void RunJoinedGroupByBenchmark(StringBuilder output)
{
    PrintHeader("Joined GROUP BY");
    const string sql =
        "SELECT c.region, SUM(o.amount) AS total " +
        "FROM orders o JOIN customers c ON o.cust_id = c.id " +
        "GROUP BY c.region";
    var ddl = new[]
    {
        "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
        "CREATE TABLE orders (cust_id INT NOT NULL, amount INT NOT NULL)",
    };

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        // Fix customer cardinality at 100 (10 regions × 10 customers per region);
        // vary orders count.
        var rng = new Random(13);
        var custCount = Math.Min(100, n / 4 + 1);
        var regionCount = Math.Min(10, custCount);
        var customers = new List<(int Id, string Region)>(custCount);
        for (var i = 0; i < custCount; i++)
        {
            customers.Add((i, "r" + (i % regionCount)));
        }

        var orders = new List<(int CustId, int Amount)>(n);
        for (var i = 0; i < n; i++)
        {
            orders.Add((rng.Next(custCount), rng.Next(1, 1_000)));
        }

        var customersDelta = customers.Select(c => (Values: new object?[] { c.Id, c.Region }, Weight: 1L)).ToList();
        var ordersDelta = orders.Select(o => (Values: new object?[] { o.CustId, o.Amount }, Weight: 1L)).ToList();

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => Compile(ddl, sql),
            run: q =>
            {
                q.Table("customers").Push(customersDelta);
                q.Table("orders").Push(ordersDelta);
                q.Step();
            });

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = Compile(ddl, sql);
                q.Table("customers").Push(customersDelta);
                q.Table("orders").Push(ordersDelta);
                q.Step();
                return (Query: q, Rng: new Random(99), CustCount: custCount);
            },
            oneStep: state =>
            {
                state.Query.Table("orders").Insert(state.Rng.Next(state.CustCount), state.Rng.Next(1, 1_000));
                state.Query.Step();
            });

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`",
        "Inner join feeding a SUM aggregator. Per-update cost = probing the " +
        "fixed customers trace + folding the one new order's amount into the " +
        "per-region running sum.",
        rows);
}

// ---------- Benchmark 3c: Joined GROUP BY, flat vs spine trace family ----------

static void RunJoinedGroupBySpineBenchmark(StringBuilder output)
{
    PrintHeader("Joined GROUP BY (flat vs spine)");
    const string sql =
        "SELECT c.region, SUM(o.amount) AS total " +
        "FROM orders o JOIN customers c ON o.cust_id = c.id " +
        "GROUP BY c.region";
    var ddl = new[]
    {
        "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
        "CREATE TABLE orders (cust_id INT NOT NULL, amount INT NOT NULL)",
    };

    output.AppendLine("## Joined GROUP BY — flat vs spine trace family");
    output.AppendLine();
    output.AppendLine(
        "The same query and data as the Joined GROUP BY benchmark above, " +
        "compiled once onto the flat dictionary traces and once onto the spine " +
        "(LSM) traces via `CompileOptions { TraceFamily = TraceFamily.Spine }`. " +
        "Both run through the structural compile (spine mode skips the typed " +
        "fast path), so the delta is the trace family alone: the spine pays a " +
        "per-batch bloom + binary search on each probe where the flat trace does " +
        "one dictionary lookup, in exchange for immutable batches that snapshot " +
        "per-file and spill to disk. Operators: `IncrementalJoinOp` + " +
        "`IncrementalAggregateOp` (flat) vs `SpineIncrementalJoinOp` + " +
        "`SpineIncrementalAggregateOp`.");
    output.AppendLine();
    output.AppendLine("| N          | Flat batch    | Flat incr      | Spine batch   | Spine incr     | Spine incr vs flat |");
    output.AppendLine("|-----------:|--------------:|---------------:|--------------:|---------------:|-------------------:|");

    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        var rng = new Random(13);
        var custCount = Math.Min(100, n / 4 + 1);
        var regionCount = Math.Min(10, custCount);
        var customers = new List<(int Id, string Region)>(custCount);
        for (var i = 0; i < custCount; i++)
        {
            customers.Add((i, "r" + (i % regionCount)));
        }

        var orders = new List<(int CustId, int Amount)>(n);
        for (var i = 0; i < n; i++)
        {
            orders.Add((rng.Next(custCount), rng.Next(1, 1_000)));
        }

        var customersDelta = customers.Select(c => (Values: new object?[] { c.Id, c.Region }, Weight: 1L)).ToList();
        var ordersDelta = orders.Select(o => (Values: new object?[] { o.CustId, o.Amount }, Weight: 1L)).ToList();

        var (flatBatch, flatIncr) = MeasureGroupByFamily(() => Compile(ddl, sql), customersDelta, ordersDelta, custCount);
        var (spineBatch, spineIncr) = MeasureGroupByFamily(() => CompileSpine(ddl, sql), customersDelta, ordersDelta, custCount);

        var ratio = flatIncr > 0 ? spineIncr / flatIncr : 0.0;
        Console.WriteLine(
            $"  N={n,-7} flat batch={BenchmarkHarness.FormatMs(flatBatch)} incr={BenchmarkHarness.FormatMicros(flatIncr)}" +
            $"  spine batch={BenchmarkHarness.FormatMs(spineBatch)} incr={BenchmarkHarness.FormatMicros(spineIncr)}");
        output.AppendLine(
            $"| {n,10} | {BenchmarkHarness.FormatMs(flatBatch).Trim()} | {BenchmarkHarness.FormatMicros(flatIncr).Trim()} | " +
            $"{BenchmarkHarness.FormatMs(spineBatch).Trim()} | {BenchmarkHarness.FormatMicros(spineIncr).Trim()} | " +
            $"{BenchmarkHarness.FormatRatio(ratio).Trim()} |");
    }

    output.AppendLine();
}

static (double BatchMs, double IncrUs) MeasureGroupByFamily(
    Func<CompiledQuery> compile,
    List<(object?[] Values, long Weight)> customersDelta,
    List<(object?[] Values, long Weight)> ordersDelta,
    int custCount)
{
    var batchMs = BenchmarkHarness.MedianColdMs(
        setup: compile,
        run: q =>
        {
            q.Table("customers").Push(customersDelta);
            q.Table("orders").Push(ordersDelta);
            q.Step();
        });

    var incrUs = BenchmarkHarness.MedianPerStepMicros(
        setup: () =>
        {
            var q = compile();
            q.Table("customers").Push(customersDelta);
            q.Table("orders").Push(ordersDelta);
            q.Step();
            return (Query: q, Rng: new Random(99), CustCount: custCount);
        },
        oneStep: state =>
        {
            state.Query.Table("orders").Insert(state.Rng.Next(state.CustCount), state.Rng.Next(1, 1_000));
            state.Query.Step();
        });

    return (batchMs, incrUs);
}

// ---------- Benchmark 3.5: Joined GROUP BY, nullable amount ----------

static void RunJoinedGroupByNullableBenchmark(StringBuilder output)
{
    PrintHeader("Joined GROUP BY (nullable amount)");
    const string sql =
        "SELECT c.region, SUM(o.amount) AS total " +
        "FROM orders o JOIN customers c ON o.cust_id = c.id " +
        "GROUP BY c.region";
    // Same shape as Benchmark 3, but `orders.amount` is nullable —
    // exercises the per-column nullability (N4) path: typed SUM
    // routes through TypedSumLongNullableAggregator with HasValue
    // checks and DistinctNonNullRows bookkeeping. Join key
    // (cust_id) and group key (region) stay NOT NULL because
    // those gates are still on (deferred to a later phase).
    var ddl = new[]
    {
        "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
        "CREATE TABLE orders (cust_id INT NOT NULL, amount INT)",
    };

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        var rng = new Random(13);
        var custCount = Math.Min(100, n / 4 + 1);
        var regionCount = Math.Min(10, custCount);
        var customers = new List<(int Id, string Region)>(custCount);
        for (var i = 0; i < custCount; i++)
        {
            customers.Add((i, "r" + (i % regionCount)));
        }

        // ~10% of orders carry NULL amount.
        var orders = new List<(int CustId, int? Amount)>(n);
        for (var i = 0; i < n; i++)
        {
            int? amt = rng.Next(10) == 0 ? null : rng.Next(1, 1_000);
            orders.Add((rng.Next(custCount), amt));
        }

        var customersDelta = customers.Select(c => (Values: new object?[] { c.Id, c.Region }, Weight: 1L)).ToList();
        var ordersDelta = orders.Select(o => (Values: new object?[] { o.CustId, o.Amount }, Weight: 1L)).ToList();

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => Compile(ddl, sql),
            run: q =>
            {
                q.Table("customers").Push(customersDelta);
                q.Table("orders").Push(ordersDelta);
                q.Step();
            });

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = Compile(ddl, sql);
                q.Table("customers").Push(customersDelta);
                q.Table("orders").Push(ordersDelta);
                q.Step();
                return (Query: q, Rng: new Random(99), CustCount: custCount);
            },
            oneStep: state =>
            {
                int? amt = state.Rng.Next(10) == 0 ? null : state.Rng.Next(1, 1_000);
                state.Query.Table("orders").Insert(state.Rng.Next(state.CustCount), amt);
                state.Query.Step();
            });

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Joined GROUP BY (nullable `amount`) — same query shape",
        "Identical query to Benchmark 3, but `orders.amount` is nullable " +
        "and ~10% of input rows have `NULL amount`. Exercises the typed " +
        "pipeline's nullable-arg SUM path (Phase N4): per-row `HasValue` " +
        "check, `DistinctNonNullRows` bookkeeping for the linear gate, " +
        "and `Nullable<long>`-typed aggregate output slot. Compare to " +
        "Benchmark 3 to read off the per-row overhead of the nullable " +
        "wrapper versus the non-null fast path.",
        rows);
}

// ---------- Benchmark 3a: Joined GROUP BY, EmittedEqualityCodec ----------

static void RunJoinedGroupByEmittedCodecBenchmark(StringBuilder output)
{
    PrintHeader("Joined GROUP BY (emitted-eq codec)");
    const string sql =
        "SELECT c.region, SUM(o.amount) AS total " +
        "FROM orders o JOIN customers c ON o.cust_id = c.id " +
        "GROUP BY c.region";
    var ddl = new[]
    {
        "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
        "CREATE TABLE orders (cust_id INT NOT NULL, amount INT NOT NULL)",
    };

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        var rng = new Random(13);
        var custCount = Math.Min(100, n / 4 + 1);
        var regionCount = Math.Min(10, custCount);
        var customers = new List<(int Id, string Region)>(custCount);
        for (var i = 0; i < custCount; i++)
        {
            customers.Add((i, "r" + (i % regionCount)));
        }

        var orders = new List<(int CustId, int Amount)>(n);
        for (var i = 0; i < n; i++)
        {
            orders.Add((rng.Next(custCount), rng.Next(1, 1_000)));
        }

        var customersDelta = customers.Select(c => (Values: new object?[] { c.Id, c.Region }, Weight: 1L)).ToList();
        var ordersDelta = orders.Select(o => (Values: new object?[] { o.CustId, o.Amount }, Weight: 1L)).ToList();

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => CompileWithEmittedCodec(ddl, sql),
            run: q =>
            {
                q.Table("customers").Push(customersDelta);
                q.Table("orders").Push(ordersDelta);
                q.Step();
            });

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = CompileWithEmittedCodec(ddl, sql);
                q.Table("customers").Push(customersDelta);
                q.Table("orders").Push(ordersDelta);
                q.Step();
                return (Query: q, Rng: new Random(99), CustCount: custCount);
            },
            oneStep: state =>
            {
                state.Query.Table("orders").Insert(state.Rng.Next(state.CustCount), state.Rng.Next(1, 1_000));
                state.Query.Step();
            });

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Joined GROUP BY (`EmittedEqualityCodec`) — same query shape",
        "Identical query and circuit shape as the preceding Joined GROUP BY " +
        "benchmark, but compiled with `EmittedEqualityCodec` — input rows, " +
        "join outputs (via `MergeRows`), and group keys (via `ExtractKey`) " +
        "are constructed as per-schema emitted subclasses of " +
        "`StructuralRow`. Stays inside the existing pipeline (no generic " +
        "lift). Measures the perf delta achievable from typed equality " +
        "alone, before any field-access optimisation.",
        rows);
}

// ---------- Benchmark 3b: Joined GROUP BY, typed rows ----------

static void RunJoinedGroupByTypedBenchmark(StringBuilder output)
{
    PrintHeader("Joined GROUP BY (typed rows)");

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 100, 1_000, 10_000, 100_000 })
    {
        var rng = new Random(13);
        var custCount = Math.Min(100, n / 4 + 1);
        var regionCount = Math.Min(10, custCount);
        var customers = new List<(int Id, string Region)>(custCount);
        for (var i = 0; i < custCount; i++)
        {
            customers.Add((i, "r" + (i % regionCount)));
        }

        var orders = new List<(int CustId, int Amount)>(n);
        for (var i = 0; i < n; i++)
        {
            orders.Add((rng.Next(custCount), rng.Next(1, 1_000)));
        }

        var customersDelta = TypedJoinedGroupBy.BuildCustomersDelta(customers);
        var ordersDelta = TypedJoinedGroupBy.BuildOrdersDelta(orders);

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => TypedJoinedGroupBy.Build(),
            run: q =>
            {
                q.Customers.Push(customersDelta);
                q.Orders.Push(ordersDelta);
                q.Circuit.Step();
            });

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = TypedJoinedGroupBy.Build();
                q.Customers.Push(customersDelta);
                q.Orders.Push(ordersDelta);
                q.Circuit.Step();
                return (Query: q, Rng: new Random(99), CustCount: custCount);
            },
            oneStep: state =>
            {
                var oneOrder = ZSet.Singleton(
                    new TypedJoinedGroupBy.OrderRow(state.Rng.Next(state.CustCount), state.Rng.Next(1, 1_000)),
                    new Z64(1));
                state.Query.Orders.Push(oneOrder);
                state.Query.Circuit.Step();
            });

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Joined GROUP BY (typed rows, hand-wired) — same query shape",
        "Identical circuit to the preceding Joined GROUP BY benchmark, but " +
        "hand-wired in the Core using `readonly record struct` rows — no " +
        "`StructuralRow`, no `object?[]`, no SQL compilation. Establishes the " +
        "perf ceiling for a future typed-row pipeline.",
        rows);
}

// ---------- Benchmark 4: Transitive closure (recursive CTE) ----------

static void RunTransitiveClosureBenchmark(StringBuilder output)
{
    PrintHeader("Transitive closure (recursive)");
    const string sql =
        "WITH RECURSIVE reach AS ( " +
        "    SELECT src, dst FROM edges " +
        "    UNION ALL " +
        "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
        "SELECT src, dst FROM reach";
    var ddl = new[] { "CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)" };

    var rows = new List<(int, double, double)>();
    foreach (var n in new[] { 50, 100, 200, 500 })
    {
        // Path graph: 1→2→3→…→n. Closure has n(n−1)/2 pairs (quadratic
        // output). The semi-naïve loop converges in O(n) iterations.
        var edgesDelta = new List<(object?[] Values, long Weight)>(n - 1);
        for (var i = 1; i < n; i++)
        {
            edgesDelta.Add((new object?[] { i, i + 1 }, 1L));
        }

        var batchMs = BenchmarkHarness.MedianColdMs(
            setup: () => Compile(ddl, sql),
            run: q =>
            {
                q.Table("edges").Push(edgesDelta);
                q.Step();
            },
            warmups: 1,
            runs: 3);

        var incrUs = BenchmarkHarness.MedianPerStepMicros(
            setup: () =>
            {
                var q = Compile(ddl, sql);
                q.Table("edges").Push(edgesDelta);
                q.Step();
                return (Query: q, Next: n);
            },
            oneStep: state =>
            {
                // Extend the path by one edge: n → n+1, n+1 → n+2, …
                state.Query.Table("edges").Insert(state.Next, state.Next + 1);
                state.Query.Step();
                state.Next++;
            },
            warmupSteps: 5,
            measureSteps: 30);

        PrintRow(n, batchMs, incrUs);
        rows.Add((n, batchMs, incrUs));
    }

    AppendTable(output, "Transitive closure — recursive CTE over a path graph",
        "`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).",
        rows);
}

// ---------- Interpretation ----------

static void AppendInterpretation(StringBuilder output)
{
    output.AppendLine("## Interpretation");
    output.AppendLine();
    output.AppendLine(
        "The number to read is the **Speedup** column — how many times faster an " +
        "incremental update is than a cold recompute. Absolute numbers vary by host; " +
        "the shape of the curve is the interesting signal.");
    output.AppendLine();
    output.AppendLine("### What went well");
    output.AppendLine();
    output.AppendLine(
        "- **Filter** matches the ideal shape exactly: batch scales linearly in N, " +
        "incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, " +
        "and by 100k rows the incremental path is ~5 orders of magnitude faster. " +
        "This is the easy case — a pipelined stateless operator, no trace to maintain.");
    output.AppendLine(
        "- **Operator fusion** turns a `map → filter → map` chain from three " +
        "operators (each materializing an intermediate Z-set, and the first map " +
        "allocating a row for every input before the filter can drop it) into one " +
        "`MapFilterRows` pass. The result is a flat ~2–4.5× per-step speedup and a " +
        "steady ~72% drop in bytes allocated per step, independent of N — the fused " +
        "pass allocates one output Z-set and builds a row only for the survivors. " +
        "This is what the SQL compiler now emits for any run of adjacent " +
        "Filter/Project plan nodes, on both the structural and typed paths.");
    output.AppendLine(
        "- **Transitive closure** shows the quadratic-speedup shape the DBSP paper " +
        "advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at " +
        "convergence), incremental is O(n) for one leaf-edge insertion, so speedup " +
        "grows quadratically. By N=500 edges incremental is ~2500× faster.");
    output.AppendLine();
    output.AppendLine("### Where the curves end up");
    output.AppendLine();
    output.AppendLine(
        "- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup " +
        "now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds " +
        "the per-group delta into running state, and the indexed traces " +
        "(`IndexedZSetTrace`) integrate in place rather than rebuilding. The " +
        "residual sub-linear growth in the incremental column is dominated by " +
        "allocator / cache effects on a larger running state, not an algorithmic " +
        "scan.");
    output.AppendLine(
        "- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply " +
        "with N, reaching ~2000× at N=100k. All four aggregators are " +
        "incremental: SUM / COUNT fold the delta; MIN / MAX maintain a " +
        "per-group sorted set of distinct positive-weight values for " +
        "O(log n) extremum lookup. Per-update cost is dominated by trace " +
        "and aggregate-state dictionary ops at this point — the visible " +
        "ceiling is now the same allocator / cache effect that limits " +
        "Joined GROUP BY at scale.");
    output.AppendLine();
    _ = CultureInfo.InvariantCulture;
}
