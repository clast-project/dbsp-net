// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Increment 1 microbenchmark (docs/design-durable-state.md §1f): the run file against today's
// monolithic snapshot file for the same Z-set. Write time and bytes, open (footer only), point
// lookups (present / absent keys), full scan. Synthetic TPC-DI-ish rows. Local file system in a
// temp dir; reads hit a warm page cache. Gated: RUNFILE_BENCH=1 (RUNFILE_ROWS, default 1M).
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO.Local;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class RunFileBench(ITestOutputHelper output)
{
    private static readonly Schema Wide = new([
        new SchemaColumn("sk_id", new SqlBigintType(false)),
        new SchemaColumn("customer_id", new SqlBigintType(false)),
        new SchemaColumn("status", new SqlVarcharType(null, false)),
        new SchemaColumn("name", new SqlVarcharType(null, true)),
        new SchemaColumn("city", new SqlVarcharType(null, true)),
        new SchemaColumn("tier", new SqlIntegerType(true)),
        new SchemaColumn("balance", new SqlDoubleType(true)),
        new SchemaColumn("opened", new SqlTimestampType(false)),
        new SchemaColumn("note", new SqlVarcharType(null, true)),
        new SchemaColumn("flag", new SqlBooleanType(false)),
    ]);

    [Fact]
    public async Task Measure()
    {
        if (Environment.GetEnvironmentVariable("RUNFILE_BENCH") != "1")
        {
            output.WriteLine("RUNFILE_BENCH not set — skipping.");
            return;
        }

        var rows = int.TryParse(Environment.GetEnvironmentVariable("RUNFILE_ROWS"), out var r) ? r : 1_000_000;
        var dir = Path.Combine(Path.GetTempPath(), "runfile-bench-" + Environment.ProcessId);
        Directory.CreateDirectory(dir);
        try
        {
            var fs = new LocalTableFileSystem(dir);
            var codec = new ArrowZSetTraceCodec(Wide);
            Func<StructuralRow, int> hash = row => row.GetHashCode();
            var rng = new Random(5);
            var b = new ZSetBuilder<StructuralRow, Z64>(rows);
            var keys = new StructuralRow[rows];
            for (var i = 0; i < rows; i++)
            {
                var row = new StructuralRow(
                    (long)i, (long)rng.Next(0, rows / 3), Utf8String.Of(rng.Next(3) == 0 ? "ACTV" : "INAC"),
                    Utf8String.Of("name-" + rng.Next(100000)), Utf8String.Of("city-" + rng.Next(500)),
                    rng.Next(4) == 0 ? null : (object)rng.Next(1, 4), rng.NextDouble() * 10000,
                    new Timestamp(1_600_000_000_000_000L + (long)i * 1000), Utf8String.Of(new string('n', rng.Next(0, 40))),
                    rng.Next(2) == 0);
                keys[i] = row;
                b.Add(row, Z64.One);
            }

            var set = b.Build();
            var absent = Enumerable.Range(0, 2000).Select(i => new StructuralRow(
                (long)(rows + i), 0L, Utf8String.Of("X"), null, null, null, null, new Timestamp(0), null, false)).ToArray();
            var present = Enumerable.Range(0, 2000).Select(_ => keys[rng.Next(rows)]).ToArray();

            // Baseline: today's monolithic snapshot file.
            var ctx = new TableFileSystemSnapshotContext(fs, "mono");
            var sw = Stopwatch.StartNew();
            await codec.SaveAsync(ctx, "trace.arrows", set);
            var monoWrite = sw.Elapsed.TotalMilliseconds;
            var monoBytes = new FileInfo(Path.Combine(dir, "mono", "trace.arrows")).Length;
            sw.Restart();
            var loaded = await codec.LoadAsync(ctx, "trace.arrows");
            var monoLoad = sw.Elapsed.TotalMilliseconds;
            output.WriteLine(Inv($"MONO rows={rows} bytes_mib={monoBytes / 1048576.0:F1} write_ms={monoWrite:F0} full_load_ms={monoLoad:F0} (loaded {loaded.Count})"));

            foreach (var blockEntries in new[] { 256, 1024, 4096 })
            {
                var name = "run-" + blockEntries;
                sw.Restart();
                var info = await RunFile.WriteZSetAsync(fs, name, set, codec, hash, blockEntries);
                var write = sw.Elapsed.TotalMilliseconds;

                sw.Restart();
                await using var run = await RunReader.OpenAsync(fs, name, hash);
                var open = sw.Elapsed.TotalMilliseconds;

                // Warm the JIT on a few probes, then time.
                for (var i = 0; i < 20; i++)
                {
                    await run.WeightOfAsync(present[i], codec);
                }

                sw.Restart();
                foreach (var key in present)
                {
                    if ((await run.WeightOfAsync(key, codec)).Value != 1)
                    {
                        throw new InvalidOperationException("probe missed a present key");
                    }
                }

                var presentUs = sw.Elapsed.TotalMicroseconds / present.Length;
                sw.Restart();
                foreach (var key in absent)
                {
                    if ((await run.WeightOfAsync(key, codec)).Value != 0)
                    {
                        throw new InvalidOperationException("probe found an absent key");
                    }
                }

                var absentUs = sw.Elapsed.TotalMicroseconds / absent.Length;

                sw.Restart();
                long scanned = 0;
                await foreach (var block in run.ScanZSetAsync(codec))
                {
                    scanned += block.Count;
                }

                var scan = sw.Elapsed.TotalMilliseconds;
                output.WriteLine(Inv(
                    $"RUN block_entries={blockEntries} blocks={info.Blocks} bytes_mib={info.Bytes / 1048576.0:F1} overhead={(double)info.Bytes / monoBytes - 1:P1} write_ms={write:F0} open_ms={open:F2} probe_present_us={presentUs:F0} probe_absent_us={absentUs:F1} full_scan_ms={scan:F0} (scanned {scanned})"));
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private static string Inv(FormattableString s) => s.ToString(CultureInfo.InvariantCulture);
}
