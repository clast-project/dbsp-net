// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using Xunit;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Tests.Arrow;

/// <summary>
/// The parallel bulk-ingest path: <see cref="ArrowExtensions.PushArrow(ShardedTableInput, RecordBatch, ReadOnlySpan{long})"/>
/// on a batch past <see cref="ShardedInputHandle{TKey,TWeight}.ParallelSplitThreshold"/>
/// decodes, builds and splits its rows across threads. It must load exactly what the serial
/// <see cref="TableInput"/> push loads: duplicates merged, weight-0 rows dropped, negative
/// weights kept, two pushes in one tick summed.
/// </summary>
public class ParallelArrowIngestTests
{
    private static readonly string[] Program =
    [
        "CREATE TABLE t (k INT NOT NULL, amount DECIMAL(10, 2), name VARCHAR, r REAL)",
        "CREATE VIEW v_all AS SELECT k, amount, name, r FROM t",
        "CREATE VIEW v_agg AS SELECT k, COUNT(*) AS n, SUM(amount) AS s FROM t GROUP BY k",
    ];

    private static readonly string[] Views = ["v_all", "v_agg"];

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void LargeBatches_LoadWhatTheSerialPushLoads(int workers)
    {
        var outputs = new HashSet<string>(Views, StringComparer.Ordinal);
        var serial = SqlProgram.Compile(Program, outputs);
        Assert.True(SqlProgram.TryCompileParallel(Program, outputs, workers, out var parallel));
        using (parallel)
        {
            var schema = serial.Table("t").Schema;
            var ticks = new[]
            {
                new[] { Batch(schema, 40_000, seed: 1), Batch(schema, 20_000, seed: 2) },
                new[] { Batch(schema, 30_000, seed: 3, retract: true) },
            };

            foreach (var tick in ticks)
            {
                foreach (var (batch, weights) in tick)
                {
                    Assert.True(batch.Length >= ShardedInputHandle<StructuralRow, Z64>.ParallelSplitThreshold);
                    serial.Table("t").PushArrow(batch, weights);
                    parallel!.Table("t").PushArrow(batch, weights);
                }

                serial.Step();
                parallel!.Step();
                foreach (var view in Views)
                {
                    var expected = Materialize(serial.Outputs[view].CurrentView);
                    Assert.NotEmpty(expected);
                    Assert.Equal(expected, Materialize(parallel.Outputs[view].CurrentView));
                    Assert.Equal(serial.Outputs[view].CurrentView.Count, parallel.Outputs[view].RowCount);
                }
            }
        }
    }

    // Many duplicate rows (small domains), NULLs, every 10th row weight 0; a retract batch
    // carries negative weights too.
    private static (RecordBatch Batch, long[] Weights) Batch(SqlSchema schema, int rows, int seed, bool retract = false)
    {
        var rng = new Random(seed);
        var k = new Int32Array.Builder();
        var amount = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        var name = new StringArray.Builder();
        var r = new FloatArray.Builder();
        var weights = new long[rows];
        for (var i = 0; i < rows; i++)
        {
            k.Append(rng.Next(97));
            if (rng.Next(11) == 0)
            {
                amount.AppendNull();
            }
            else
            {
                amount.Append(rng.Next(13) * 1.25m);
            }

            if (rng.Next(9) == 0)
            {
                name.AppendNull();
            }
            else
            {
                name.Append("n" + rng.Next(50));
            }

            r.Append(rng.Next(4) * 0.5f);
            weights[i] = (i % 10) switch
            {
                0 => 0,
                1 or 2 when retract => -1,
                3 => 2,
                _ => 1,
            };
        }

        var batch = new RecordBatch(
            ArrowSchemaBridge.ToArrow(schema),
            new IArrowArray[] { k.Build(), amount.Build(), name.Build(), r.Build() },
            rows);
        return (batch, weights);
    }

    private static SortedDictionary<string, long> Materialize(ZSet<StructuralRow, Z64> view)
    {
        var map = new SortedDictionary<string, long>(StringComparer.Ordinal);
        foreach (var (row, weight) in view)
        {
            var key = string.Join("|", row.ToArray().Select(v => v?.ToString() ?? "<null>"));
            map[key] = map.GetValueOrDefault(key) + weight.Value;
        }

        return map;
    }
}
