// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks;

/// <summary>
/// Focused harness for profiling the incremental hot path of the
/// Joined GROUP BY benchmark at N=100k. Runs warmup (1000 steps),
/// then a long measured loop so a CPU sampler (e.g. dotnet-trace
/// or PerfView) lands its samples on the suspect path. Three modes:
/// <list type="bullet">
/// <item><c>typed</c>: default <see cref="PlanToCircuit.Compile"/> —
/// routes through the typed-row pipeline.</item>
/// <item><c>structural</c>: <see cref="PlanToCircuit.Compile"/> with
/// <see cref="EmittedEqualityCodec"/> — the alternative codec gates
/// the typed fast path off, exercising the structural compile.</item>
/// <item><c>handwired</c>: bypasses the SQL compiler entirely and
/// builds the join+aggregate circuit directly in Core, establishing
/// the ceiling.</item>
/// </list>
/// <para>
/// Invocation pattern with dotnet-trace:
/// <code>
/// cd src/DbspNet.Benchmarks/bin/Release/net10.0
/// dotnet-trace collect --providers Microsoft-DotNETCore-SampleProfiler \
///   -o /tmp/typed-cpu.nettrace -- ./DbspNet.Benchmarks.exe profile typed 20
/// dotnet-trace convert /tmp/typed-cpu.nettrace --format speedscope
/// </code>
/// Speedscope JSON can be aggregated with a small Python script (or
/// loaded into the speedscope.app UI) to find hot inclusive frames.
/// </para>
/// </summary>
internal static class ProfileHotPath
{
    public static int Run(string[] args)
    {
        var mode = args.Length > 1 ? args[1] : "typed";
        var seconds = args.Length > 2 ? double.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture) : 10.0;

        const int n = 100_000;
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

        Console.WriteLine($"Mode: {mode}, N={n}, seconds={seconds}");

        switch (mode)
        {
            case "typed":
                RunTyped(customers, orders, custCount, seconds);
                break;
            case "structural":
                RunStructural(customers, orders, custCount, seconds);
                break;
            case "handwired":
                RunHandwired(customers, orders, custCount, seconds);
                break;
            default:
                Console.WriteLine("Mode must be: typed | structural | handwired");
                return 1;
        }

        return 0;
    }

    private static void RunTyped(
        List<(int Id, string Region)> customers,
        List<(int CustId, int Amount)> orders,
        int custCount,
        double seconds)
    {
        const string sql =
            "SELECT c.region, SUM(o.amount) AS total " +
            "FROM orders o JOIN customers c ON o.cust_id = c.id " +
            "GROUP BY c.region";
        var ddl = new[]
        {
            "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
            "CREATE TABLE orders (cust_id INT NOT NULL, amount INT NOT NULL)",
        };

        var q = Compile(ddl, sql);
        q.Table("customers").Push(customers.Select(c => (Values: new object?[] { c.Id, c.Region }, Weight: 1L)));
        q.Table("orders").Push(orders.Select(o => (Values: new object?[] { o.CustId, o.Amount }, Weight: 1L)));
        q.Step();

        // Warmup.
        var stepRng = new Random(99);
        for (var i = 0; i < 1000; i++)
        {
            q.Table("orders").Insert(stepRng.Next(custCount), stepRng.Next(1, 1_000));
            q.Step();
        }

        // Sampled loop.
        Console.WriteLine("Sampling start...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long steps = 0;
        var deadline = TimeSpan.FromSeconds(seconds);
        while (sw.Elapsed < deadline)
        {
            q.Table("orders").Insert(stepRng.Next(custCount), stepRng.Next(1, 1_000));
            q.Step();
            steps++;
        }

        sw.Stop();
        Console.WriteLine($"Steps: {steps}, per-step: {sw.Elapsed.TotalMicroseconds / steps:F2} µs");
    }

    private static void RunStructural(
        List<(int Id, string Region)> customers,
        List<(int CustId, int Amount)> orders,
        int custCount,
        double seconds)
    {
        const string sql =
            "SELECT c.region, SUM(o.amount) AS total " +
            "FROM orders o JOIN customers c ON o.cust_id = c.id " +
            "GROUP BY c.region";
        var ddl = new[]
        {
            "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
            "CREATE TABLE orders (cust_id INT NOT NULL, amount INT NOT NULL)",
        };

        var q = CompileWithCodec(ddl, sql);
        q.Table("customers").Push(customers.Select(c => (Values: new object?[] { c.Id, c.Region }, Weight: 1L)));
        q.Table("orders").Push(orders.Select(o => (Values: new object?[] { o.CustId, o.Amount }, Weight: 1L)));
        q.Step();

        var stepRng = new Random(99);
        for (var i = 0; i < 1000; i++)
        {
            q.Table("orders").Insert(stepRng.Next(custCount), stepRng.Next(1, 1_000));
            q.Step();
        }

        Console.WriteLine("Sampling start...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long steps = 0;
        var deadline = TimeSpan.FromSeconds(seconds);
        while (sw.Elapsed < deadline)
        {
            q.Table("orders").Insert(stepRng.Next(custCount), stepRng.Next(1, 1_000));
            q.Step();
            steps++;
        }

        sw.Stop();
        Console.WriteLine($"Steps: {steps}, per-step: {sw.Elapsed.TotalMicroseconds / steps:F2} µs");
    }

    private static void RunHandwired(
        List<(int Id, string Region)> customers,
        List<(int CustId, int Amount)> orders,
        int custCount,
        double seconds)
    {
        var q = TypedJoinedGroupBy.Build();
        q.Customers.Push(TypedJoinedGroupBy.BuildCustomersDelta(customers));
        q.Orders.Push(TypedJoinedGroupBy.BuildOrdersDelta(orders));
        q.Circuit.Step();

        var stepRng = new Random(99);
        for (var i = 0; i < 1000; i++)
        {
            q.Orders.Push(ZSet.Singleton(
                new TypedJoinedGroupBy.OrderRow(stepRng.Next(custCount), stepRng.Next(1, 1_000)),
                new Z64(1)));
            q.Circuit.Step();
        }

        Console.WriteLine("Sampling start...");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long steps = 0;
        var deadline = TimeSpan.FromSeconds(seconds);
        while (sw.Elapsed < deadline)
        {
            q.Orders.Push(ZSet.Singleton(
                new TypedJoinedGroupBy.OrderRow(stepRng.Next(custCount), stepRng.Next(1, 1_000)),
                new Z64(1)));
            q.Circuit.Step();
            steps++;
        }

        sw.Stop();
        Console.WriteLine($"Steps: {steps}, per-step: {sw.Elapsed.TotalMicroseconds / steps:F2} µs");
    }

    private static CompiledQuery Compile(string[] ddl, string sql)
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

    private static CompiledQuery CompileWithCodec(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        // EmittedEqualityCodec forces the structural compile path
        // (the typed fast path is gated off for non-default codecs).
        return PlanToCircuit.Compile(PlanOptimizer.Optimize(plan), EmittedEqualityCodec.Instance);
    }
}
