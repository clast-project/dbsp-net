// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Prices UTF-8 validation at ingest (docs/design-connectors.md, G5 item 4): VARCHAR decode with
// StringDecoding.Copy alone, plus the whole-column check (ArrowColumns.EnsureValidUtf8), plus a
// naive per-cell Utf8.IsValid for comparison. Gated on UTF8_VALIDATION_BENCH=1; prints BENCH lines.
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Sql.TypeSystem;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class Utf8ValidationCost(ITestOutputHelper output)
{
    [Fact]
    public void Measure()
    {
        if (Environment.GetEnvironmentVariable("UTF8_VALIDATION_BENCH") != "1")
        {
            output.WriteLine("UTF8_VALIDATION_BENCH not set — skipping.");
            return;
        }

        var varchar = new SqlVarcharType(null, false);
        // Shapes: short codes (ivm-bench status/state columns), Nexmark-like URLs/extras, and
        // non-ASCII text (the slower, non-ASCII path through the validator).
        foreach (var (name, make) in new (string, Func<Random, string>)[]
        {
            ("short-ascii-6B", r => "ST" + r.Next(1000, 9999)),
            ("url-ascii-60B", r => $"https://www.nexmark.com/{r.Next()}/item.html?query={r.Next()}"),
            ("extra-ascii-150B", r => new string((char)('a' + r.Next(26)), 150)),
            ("mixed-utf8-40B", r => $"Café Zürich 東京 {r.Next()} naïve"),
        })
        {
            const int Rows = 1_000_000;
            var rng = new Random(1);
            var b = new StringArray.Builder();
            long bytes = 0;
            for (var i = 0; i < Rows; i++)
            {
                var v = make(rng);
                bytes += System.Text.Encoding.UTF8.GetByteCount(v);
                b.Append(v);
            }

            var a = b.Build();
            double Time(Action act)
            {
                act();
                var best = double.MaxValue;
                for (var run = 0; run < 7; run++)
                {
                    var sw = Stopwatch.StartNew();
                    act();
                    best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
                }

                return best;
            }

            var copyMs = Time(() => ArrowColumns.Extract(a, varchar, 0, Rows, StringDecoding.Copy));
            var columnMs = Time(() => ArrowColumns.EnsureValidUtf8(a, 0, Rows));
            var perCellMs = Time(() =>
            {
                var offsets = a.ValueOffsets;
                var span = a.ValueBuffer.Span;
                for (var i = 0; i < Rows; i++)
                {
                    if (!System.Text.Unicode.Utf8.IsValid(span[offsets[i]..offsets[i + 1]]))
                    {
                        throw new InvalidDataException();
                    }
                }
            });
            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"BENCH shape={name} rows={Rows} bytes={bytes} copy_ms={copyMs:F1} column_check_ms={columnMs:F1} " +
                $"per_cell_check_ms={perCellMs:F1} column_vs_copy={columnMs / copyMs:P1} per_cell_vs_copy={perCellMs / copyMs:P1} " +
                $"column_GBps={bytes / columnMs / 1e6:F1}"));
        }
    }
}
