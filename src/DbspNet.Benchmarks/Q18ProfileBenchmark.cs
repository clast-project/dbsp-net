// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text;
using DbspNet.Benchmarks.Nexmark;
using DbspNet.Sql.Compiler;
using static DbspNet.Benchmarks.Nexmark.NexmarkGenerator;

namespace DbspNet.Benchmarks;

/// <summary>
/// Profiles Nexmark q18 (0.44× vs Feldera, the worst non-inherent gap) to answer
/// the question the W14 snapshot deferred: is the cost the partitioned TOP-K
/// <b>operator</b> (re-sorting per tick) or the <b>movement of wide rows</b>
/// (ingest split / inter-worker exchange inside step / egest gather)?
/// </summary>
/// <remarks>
/// <para>q18 dedups the latest bid per <c>(bidder, auction)</c> — TOP-1 over a
/// huge number of <em>tiny</em> partitions (most pairs see ~one bid), keeping all
/// seven wide bid columns (incl. the <c>url</c>/<c>extra</c> strings). So
/// <c>ComputeWindow</c> is O(1) per touched partition and the per-partition
/// <c>SortedDictionary</c> is trivial; the suspected cost is moving wide rows, not
/// the rank logic.</para>
/// <para>The harness times three phases: <b>split</b> (push/ingest-encode),
/// <b>step</b> (all operators incl. the inter-worker exchange shuffle), and
/// <b>gather</b> (materialise/egest the full output). Sweeping W isolates what
/// scales: operator work should fall ~linearly with W; an exchange/coordination
/// or boundary-transcode bottleneck stays flat. Comparing W=1 step (no real
/// shuffle) against W=host step × host quantifies the parallel-scaling loss.</para>
/// </remarks>
internal static class Q18ProfileBenchmark
{
    public static void Run(StringBuilder output, int totalEvents, int runs, string[]? queryIds = null)
    {
        var host = Environment.ProcessorCount;
        var ids = queryIds is { Length: > 0 } ? queryIds : new[] { "q18", "q19" };

        Console.WriteLine();
        Console.WriteLine($"=== TOP-K egest profile ({string.Join(",", ids)}, events={totalEvents:N0}, runs={runs}, host={host} cores) ===");
        Console.WriteLine("Generating event stream…");
        var events = Generate(totalEvents);

        // W values: 1 (operator cost, no real shuffle), host/2, host.
        var widths = new[] { 1, Math.Max(2, host / 2), host }.Distinct().OrderBy(w => w).ToArray();

        output.AppendLine("# DbspNet — partitioned TOP-K egest profile (out-of-`Step` output)");
        output.AppendLine();
        output.AppendLine(
            "The q18/q19 partitioned-TOP-K queries (worst remaining gaps vs Feldera). The " +
            "whole pipeline runs as a `W`-replica parallel circuit; **split** (ingest), " +
            "**step** (operators incl. the inter-worker exchange), and **gather** (egest the " +
            "full output) are timed separately, and W is swept to see what scales. *gather* " +
            "here is the **out-of-`Step` output** materialisation — phase (d) in the §22 " +
            "4-way decomposition; confirming it is ~0 localises the gap to the in-`Step` work.");
        output.AppendLine();
        output.AppendLine(
            $"Stream: {totalEvents:N0} events, batch 10k, median of {runs} run(s) after one " +
            $"warmup. Host: .NET {Environment.Version}, {host} cores.");
        output.AppendLine();

        foreach (var id in ids)
        {
            var query = Array.Find(NexmarkQueries.All, q => q.Id == id);
            if (query is null)
            {
                Console.WriteLine($"  (skipping unknown query {id})");
                continue;
            }

            Console.WriteLine($"  {id}…");
            var consumed = query.Tables.ToHashSet();
            var plan = SpineParallelHarness.BuildPlan(NexmarkQueries.Ddl, query.Sql);

            output.AppendLine($"## {id} — {query.Description}");
            output.AppendLine();
            output.AppendLine("| W | Split (ms) | Step (ms) | Gather (ms) | Total (ms) | Step ev/s | Step↑ vs W=1 | Gather% | Output rows |");
            output.AppendLine("|--:|-----------:|----------:|------------:|-----------:|----------:|-------------:|--------:|------------:|");

            double w1Step = 0;
            foreach (var w in widths)
            {
                Console.WriteLine($"    W={w}…");
                var (splitMs, stepMs, gatherMs, outputRows, _) =
                    SpineParallelHarness.MeasureMedian(
                        plan, w, SpineParallelHarness.Flat, events, consumed, batchSize: 10_000, runs);

                if (w == 1)
                {
                    w1Step = stepMs;
                }

                var total = splitMs + stepMs + gatherMs;
                var stepEvSec = stepMs > 0 ? totalEvents / (stepMs / 1000.0) : 0.0;
                var stepUp = stepMs > 0 ? w1Step / stepMs : 1.0;
                var gatherPct = total > 0 ? 100.0 * gatherMs / total : 0.0;

                Console.WriteLine(
                    $"      split={splitMs,8:F1}  step={stepMs,8:F1}  gather={gatherMs,8:F1}  " +
                    $"({stepEvSec,12:N0} ev/s)  {BenchmarkHarness.FormatRatio(stepUp).Trim()}");
                output.AppendLine(
                    $"| {w} | {splitMs:F1} | {stepMs:F1} | {gatherMs:F1} | {total:F1} | {stepEvSec:N0} | " +
                    $"{BenchmarkHarness.FormatRatio(stepUp).Trim()} | {gatherPct:F0}% | {outputRows:N0} |");
            }

            output.AppendLine();
        }

        output.AppendLine(
            "**Reading it.** A small **Gather%** confirms output materialisation (phase d) is " +
            "**not** the cost — the parallel path decodes output lazily after `sw.Stop()`, so " +
            "the gap is the in-`Step` work (TOP-K op + exchange) the step decomposition splits. " +
            "If **step** falls ~`1/W` the operator parallelises and the residual is coordination.");
        output.AppendLine();
        _ = CultureInfo.InvariantCulture;
    }
}
