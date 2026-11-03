// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Compiler;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

/// <summary>ivm-bench's dim_customer shape (docs/design-flat-trace-state.md §3.17): many running
/// COUNTs over tiny partitions, fed over 20 ticks. WINDOW_SMALL_BENCH=1 prints time and allocation
/// for managed and blocked window state.</summary>
public sealed class WindowSmallPartitionBench(ITestOutputHelper output)
{
    [Fact]
    public void ManyCountsOverTinyPartitions()
    {
        if (Environment.GetEnvironmentVariable("WINDOW_SMALL_BENCH") != "1")
        {
            return;
        }

        var cols = Enumerable.Range(0, 15).Select(i => $"c{i}").ToArray();
        string[] sql =
        [
            "CREATE TABLE s (customer_id INT NOT NULL, ts BIGINT NOT NULL, " + string.Join(", ", cols.Select(c => c + " INT")) + ")",
            "CREATE VIEW w AS SELECT customer_id, ts, " + string.Join(", ", cols.Select(c => $"COUNT({c}) OVER win AS {c}_grp")) +
                " FROM s WINDOW win AS (PARTITION BY customer_id ORDER BY ts)",
        ];
        foreach (var blocks in new[] { false, true, false, true })
        {
            CompiledProgram program;
            using (TraceBlocksMode.Use(join: false, window: blocks))
            {
                program = SqlProgram.Compile(sql, new HashSet<string>(["w"], StringComparer.Ordinal));
            }

            var rnd = new Random(1);
            var alloc0 = GC.GetTotalAllocatedBytes(precise: true);
            var t0 = Stopwatch.GetTimestamp();
            for (var tick = 0; tick < 20; tick++)
            {
                var t = program.Table("s");
                for (var n = 0; n < 6440 / 20; n++)
                {
                    var row = new object?[17];
                    row[0] = rnd.Next(2200);
                    row[1] = (long)(tick * 1000 + n);
                    for (var c = 0; c < 15; c++)
                    {
                        row[2 + c] = rnd.Next(3) == 0 ? null : rnd.Next(100);
                    }

                    t.Insert(row);
                }

                program.Step();
            }

            output.WriteLine($"{(blocks ? "blocked" : "managed")} ms={Stopwatch.GetElapsedTime(t0).TotalMilliseconds:F0} " +
                $"alloc_mb={(GC.GetTotalAllocatedBytes(precise: true) - alloc0) / 1e6:F0} rows={program.Outputs["w"].CurrentView.Count}");
        }
    }
}
