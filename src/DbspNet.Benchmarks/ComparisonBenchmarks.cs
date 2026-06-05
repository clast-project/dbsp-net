// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;
using DbspNet.Benchmarks.Fraud;
using DbspNet.Benchmarks.Nexmark;

namespace DbspNet.Benchmarks;

/// <summary>
/// Entry point for the Feldera-compatible cross-system comparison
/// benchmarks (Nexmark throughput + fraud-detection latency). These are
/// kept out of the default <c>docs/benchmarks.md</c> report because they are
/// heavier and meant to be run on demand against a matching Feldera setup.
/// </summary>
internal static class ComparisonBenchmarks
{
    public static int Run(string[] args)
    {
        var mode = args[0];
        var output = new StringBuilder();
        output.AppendLine("# DbspNet ↔ Feldera comparison benchmarks");
        output.AppendLine();
        output.AppendLine(
            "Feldera-compatible workloads for cross-system performance comparison " +
            "(see `research/dbsp/performance_test.md`). Both systems run the same " +
            "SQL over in-process generated data; the DbspNet side is below. Run on " +
            "the same host as Feldera, pinning the same core count, for an " +
            "apples-to-apples read.");
        output.AppendLine();
        output.AppendLine(
            $"Host: .NET {Environment.Version}, {Environment.ProcessorCount} cores, " +
            $"`{System.Runtime.InteropServices.RuntimeInformation.OSDescription.Trim()}`.");
        output.AppendLine();

        string defaultOut;
        switch (mode)
        {
            case "nexmark":
            {
                var totalEvents = ArgInt(args, 1, 1_000_000);
                var batchSize = ArgInt(args, 2, 10_000);
                var runs = ArgInt(args, 3, 3);
                NexmarkBenchmark.Run(output, totalEvents, batchSize, runs);
                defaultOut = "benchmarks-nexmark.md";
                break;
            }

            case "fraud":
            {
                var history = ArgInt(args, 1, 500_000);
                var customers = ArgInt(args, 2, 10_000);
                var batchSize = ArgInt(args, 3, 10_000);
                FraudBenchmark.Run(output, history, customers, batchSize);
                defaultOut = "benchmarks-fraud.md";
                break;
            }

            default: // "comparison" — run both with defaults.
            {
                NexmarkBenchmark.Run(output, totalEvents: 1_000_000, batchSize: 10_000, runs: 3);
                FraudBenchmark.Run(output, historyTxns: 500_000, customers: 10_000, batchSize: 10_000);
                defaultOut = "benchmarks-comparison.md";
                break;
            }
        }

        var outPath = Path.Combine("..", "..", "docs", defaultOut);
        File.WriteAllText(outPath, output.ToString());
        Console.WriteLine();
        Console.WriteLine($"Report written to {Path.GetFullPath(outPath)}");
        _ = CultureInfo.InvariantCulture;
        return 0;
    }

    private static int ArgInt(string[] args, int index, int fallback) =>
        args.Length > index && int.TryParse(args[index], NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)
            ? v
            : fallback;
}
