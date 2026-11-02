// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Text;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

/// <summary>Where a first write's index build goes (docs/design-flat-trace-state.md §3.10):
/// OUTPUT_INDEX_BENCH=&lt;rows&gt; inserts that many distinct rows in one batch, in random hash
/// order as a formed view yields them, then deletes 1%.</summary>
public sealed class OutputRowIndexBench(ITestOutputHelper output)
{
    [Fact]
    public void FirstWrite()
    {
        if (!int.TryParse(Environment.GetEnvironmentVariable("OUTPUT_INDEX_BENCH"), out var n))
        {
            return;
        }

        var schema = new Schema([
            new SchemaColumn("id", new SqlBigintType(false)),
            new SchemaColumn("name", new SqlVarcharType(null, false)),
            new SchemaColumn("city", new SqlVarcharType(null, true)),
            new SchemaColumn("amount", new SqlDoubleType(true)),
            new SchemaColumn("n", new SqlIntegerType(false)),
        ]);
        var rows = new StructuralRow[n];
        for (var i = 0; i < n; i++)
        {
            rows[i] = new StructuralRow(
                (long)i,
                Utf8String.FromBytes(Encoding.UTF8.GetBytes("customer-" + i)),
                Utf8String.FromBytes(Encoding.UTF8.GetBytes("city" + (i % 997))),
                i * 0.5,
                i % 100);
        }

        foreach (var managed in new[] { true, false, false })
        {
            var index = managed
                ? OutputRowIndex.CreateManaged()
                : OutputRowIndex.CreateBlocked(schema, TraceBlocksMode.FreezeEntries, TraceBlocksMode.BlockEntries);
            BlockTraceProfile.Report();
            var t0 = Stopwatch.GetTimestamp();
            for (var i = 0; i < n; i++)
            {
                index.Add(rows[i], i);
            }

            var tAdd = Stopwatch.GetElapsedTime(t0);
            var pause0 = GC.GetTotalPauseDuration();
            var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
            index.Commit();
            var tCommit = Stopwatch.GetElapsedTime(t0) - tAdd;
            var commitPause = (GC.GetTotalPauseDuration() - pause0).TotalMilliseconds;
            var commitAlloc = (GC.GetTotalAllocatedBytes(precise: false) - alloc0) / 1e6;
            var t1 = Stopwatch.GetTimestamp();
            var taken = new List<long>();
            for (var i = 0; i < n; i += 100)
            {
                index.Take(rows[i], 1, taken);
            }

            index.Commit();
            var tTake = Stopwatch.GetElapsedTime(t1);
            output.WriteLine(
                $"{(managed ? "managed" : "blocked")} n={n} add_ms={tAdd.TotalMilliseconds:F0} commit_ms={tCommit.TotalMilliseconds:F0} commit_gc_ms={commitPause:F0} commit_alloc_mb={commitAlloc:F0} " +
                $"take1pct_ms={tTake.TotalMilliseconds:F0} {(BlockTraceProfile.Enabled ? BlockTraceProfile.Report() : string.Empty)}");
        }
    }
}
