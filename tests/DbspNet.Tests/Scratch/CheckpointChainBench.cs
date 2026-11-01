// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Increment 7 measurement (docs/design-durable-state.md §1f): checkpoint cost over a long run of
// small ticks, one checkpoint per tick, with incremental runs — where chains reach RunChain.MaxRuns
// and are compacted. Gated: CHAIN_BENCH=1. CHAIN_ROWS (default 2,000,000 per table), CHAIN_TICKS
// (default 40), CHAIN_TICK_ROWS (default 2,000), CHAIN_WORKERS (default 12; 1 = serial), CHAIN_GAP_MS
// (pause before each checkpoint, default 0). Local file
// system under CHAIN_DIR (default: a temp dir). Prints one CHAIN line per checkpoint.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class CheckpointChainBench(ITestOutputHelper output)
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, s VARCHAR NOT NULL)",
        "CREATE TABLE u (a BIGINT NOT NULL, c BIGINT NOT NULL)",
        "CREATE VIEW j AS SELECT t.a, t.b, t.s, u.c FROM t JOIN u ON t.a = u.a",
        "CREATE VIEW g AS SELECT b % 1000 AS k, COUNT(*) AS n, SUM(a) AS sa FROM t GROUP BY b % 1000",
    ];

    private static readonly HashSet<string> Views = new(["j", "g"], StringComparer.Ordinal);

    private static int Env(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : fallback;

    [Fact]
    public async Task Measure()
    {
        if (Environment.GetEnvironmentVariable("CHAIN_BENCH") != "1")
        {
            output.WriteLine("CHAIN_BENCH not set — skipping.");
            return;
        }

        var rows = Env("CHAIN_ROWS", 2_000_000);
        var ticks = Env("CHAIN_TICKS", 40);
        var tickRows = Env("CHAIN_TICK_ROWS", 2_000);
        var workers = Env("CHAIN_WORKERS", 12);
        var dir = Environment.GetEnvironmentVariable("CHAIN_DIR") ?? Path.Combine(Path.GetTempPath(), "chain-bench-" + Environment.ProcessId);
        if (Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }

        Directory.CreateDirectory(dir);
        try
        {
            var fs = new LocalTableFileSystem(dir);
            Assert.True(SqlProgram.TryCompileParallel(Sql, Views, workers, out var program, ArrowSqlSnapshotCodecs.Instance));
            using (program)
            {
                var rng = new Random(11);
                var sw = Stopwatch.StartNew();
                for (long i = 0; i < rows; i++)
                {
                    program!.Table("t").Insert(i, rng.NextInt64(1_000_000), Utf8String.Of("s" + (i % 977)));
                    program.Table("u").Insert(i, i * 3);
                }

                program!.Step();
                output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                    $"LOAD rows={rows} workers={workers} ms={sw.Elapsed.TotalMilliseconds:F0} runs={StateLayout.Runs}"));

                for (var tick = 0; tick <= ticks; tick++)
                {
                    if (tick > 0)
                    {
                        for (var r = 0; r < tickRows; r++)
                        {
                            var a = rng.NextInt64(rows);
                            program.Table("t").Insert(a, rng.NextInt64(1_000_000), Utf8String.Of("x" + tick));
                            program.Table("u").Insert((long)rows + ((long)tick * tickRows) + r, a);
                        }

                        program.Step();
                    }

                    // CHAIN_GAP_MS: time between checkpoints (real deployments checkpoint every ~60 s;
                    // back to back is the stress case for background compaction).
                    if (Env("CHAIN_GAP_MS", 0) is > 0 and var gap)
                    {
                        await Task.Delay(gap);
                    }

                    sw.Restart();
                    await ParallelSnapshot.WriteAsync(program.Circuit, fs, retainCount: 2);
                    var ms = sw.Elapsed.TotalMilliseconds;
                    // What this checkpoint wrote: chains that are a single run written now (a whole
                    // rewrite), and chains whose front is a merged run (an adopted compaction).
                    var current = File.ReadAllText(Path.Combine(dir, "current.txt")).Trim();
                    int bases = 0, merged = 0, longest = 0;
                    foreach (var index in Directory.EnumerateFiles(Path.Combine(dir, current), "runs.txt", SearchOption.AllDirectories))
                    {
                        foreach (var line in File.ReadAllLines(index))
                        {
                            var runs = line.Split('\t')[5..];
                            longest = Math.Max(longest, runs.Length);
                            bases += runs.Length == 1 && runs[0].StartsWith("runs/" + current + "/", StringComparison.Ordinal) ? 1 : 0;
                            merged += runs[0].EndsWith(".c", StringComparison.Ordinal) ? 1 : 0;
                        }
                    }

                    output.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"CHAIN tick={tick} checkpoint_ms={ms:F0} whole_rewrites={bases} merged_fronts={merged} longest_chain={longest}"));
                }

                await CheckpointCleanup.WaitAsync(fs);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
