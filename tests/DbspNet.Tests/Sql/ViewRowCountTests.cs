// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// An output view's RowCount is its weights summed (SQL COUNT(*), duplicates counted), kept by each
// IntegrateOp once first read: the first read sums the view, every later step adds its delta's
// weights. A parallel program sums its W shards' totals, which needs no lookups of a delta's rows (a
// distinct count did, serially, W per row: ~440 s of extended TPC-DS batch 2's ~450 s tick).
// Required: serial and parallel counts equal the serial view's weight sum after every tick, from
// whichever tick the count is first read; duplicates count; and a restore (also onto another worker
// count) recounts rather than carrying a stale total.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class ViewRowCountTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (id INT NOT NULL, g INT NOT NULL, v BIGINT NOT NULL)",
        "CREATE TABLE u (g INT NOT NULL, name VARCHAR NOT NULL)",
        // Dropping id leaves equal (g, v) rows: duplicates, on whichever workers their t rows hashed to.
        "CREATE VIEW projected AS SELECT g, v FROM t",
        "CREATE VIEW agg AS SELECT g, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY g",
        "CREATE VIEW joined AS SELECT t.g, t.v, u.name FROM t JOIN u ON t.g = u.g",
        "CREATE VIEW dist AS SELECT DISTINCT g, v FROM t",
    ];

    private static readonly HashSet<string> Views = new(["projected", "agg", "joined", "dist"], StringComparer.Ordinal);

    private static long Weights(ZSet<StructuralRow, Z64> view) => view.Sum(kv => kv.Value.Value);

    // One tick of changes: inserts with many duplicates, deletes of live rows, a join side changing.
    private static void Push(Random rng, List<object?[]> live, Action<object?[]> insert, Action<object?[]> delete,
        Action<object?[]> insertU, Action<object?[]> deleteU, int tick)
    {
        for (var i = 0; i < 25; i++)
        {
            if (live.Count > 0 && rng.Next(3) == 0)
            {
                var row = live[rng.Next(live.Count)];
                delete(row);
                live.Remove(row);
            }
            else
            {
                var row = new object?[] { rng.Next(0, 30), rng.Next(0, 4), (long)rng.Next(0, 3) };
                insert(row);
                live.Add(row);
            }
        }

        if (tick % 4 == 0)
        {
            var urow = new object?[] { rng.Next(0, 4), "n" + rng.Next(0, 2) };
            if (rng.Next(3) != 0)
            {
                insertU(urow);
            }
            else
            {
                deleteU(urow);
            }
        }
    }

    [Fact]
    public void A_duplicate_row_counts_twice()
    {
        var serial = SqlProgram.Compile(Sql, Views);
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 3, out var parallel));
        using var p = parallel!;
        foreach (var row in new[] { new object?[] { 1, 2, 7L }, new object?[] { 2, 2, 7L }, new object?[] { 3, 2, 7L } })
        {
            serial.Table("t").Insert(row);
            p.Table("t").Insert(row);
        }

        serial.Step();
        p.Step();
        Assert.Single(serial.Outputs["projected"].CurrentView);
        Assert.Equal(3, serial.Outputs["projected"].RowCount);
        Assert.Equal(3, p.Outputs["projected"].RowCount);
    }

    [Theory]
    [InlineData(1, 1, 0)]
    [InlineData(2, 2, 0)]
    [InlineData(4, 3, 5)]
    [InlineData(7, 4, 11)]
    public void Kept_counts_equal_the_view_weights(int workers, int seed, int firstRead)
    {
        var serial = SqlProgram.Compile(Sql, Views);
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, workers, out var parallel));
        using var p = parallel!;
        var rng = new Random(seed);
        var live = new List<object?[]>();
        for (var tick = 0; tick < 30; tick++)
        {
            Push(rng, live,
                r => { serial.Table("t").Insert(r); p.Table("t").Insert(r); },
                r => { serial.Table("t").Delete(r); p.Table("t").Delete(r); },
                r => { serial.Table("u").Insert(r); p.Table("u").Insert(r); },
                r => { serial.Table("u").Delete(r); p.Table("u").Delete(r); },
                tick);
            serial.Step();
            p.Step();
            if (tick < firstRead)
            {
                continue; // untracked until first read
            }

            foreach (var name in Views)
            {
                var expected = Weights(serial.Outputs[name].CurrentView);
                Assert.True(expected == serial.Outputs[name].RowCount && expected == p.Outputs[name].RowCount,
                    $"view {name}, W={workers}, tick {tick}: serial {serial.Outputs[name].RowCount}, parallel {p.Outputs[name].RowCount}, expected {expected}");
            }
        }
    }

    [Theory]
    [InlineData(4, 4)]
    [InlineData(4, 3)]
    public async Task A_restore_recounts(int from, int to)
    {
        var fs = new InMemoryTableFileSystem();
        var serial = SqlProgram.Compile(Sql, Views);
        var rng = new Random(9);
        var live = new List<object?[]>();
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, from, out var producer, ArrowSqlSnapshotCodecs.Instance));
        using (producer)
        {
            for (var tick = 0; tick < 8; tick++)
            {
                Push(rng, live,
                    r => { serial.Table("t").Insert(r); producer!.Table("t").Insert(r); },
                    r => { serial.Table("t").Delete(r); producer!.Table("t").Delete(r); },
                    r => { serial.Table("u").Insert(r); producer!.Table("u").Insert(r); },
                    r => { serial.Table("u").Delete(r); producer!.Table("u").Delete(r); },
                    tick);
                serial.Step();
                producer!.Step();
                foreach (var name in Views)
                {
                    Assert.Equal(Weights(serial.Outputs[name].CurrentView), producer.Outputs[name].RowCount);
                }
            }

            await ParallelSnapshot.WriteAsync(producer!.Circuit, fs);
        }

        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, to, out var restored, ArrowSqlSnapshotCodecs.Instance));
        using var r = restored!;
        if (from != to)
        {
            Assert.True(await ParallelReshard.RewriteAsync(fs, r) > 0);
        }

        // Read before the restore: the totals are tracked (at 0) when the snapshot loads over them.
        Assert.All(Views, name => Assert.Equal(0, r.Outputs[name].RowCount));
        await ParallelSnapshot.ReadAsync(r.Circuit, fs);
        for (var tick = 8; tick < 14; tick++)
        {
            Push(rng, live,
                x => { serial.Table("t").Insert(x); r.Table("t").Insert(x); },
                x => { serial.Table("t").Delete(x); r.Table("t").Delete(x); },
                x => { serial.Table("u").Insert(x); r.Table("u").Insert(x); },
                x => { serial.Table("u").Delete(x); r.Table("u").Delete(x); },
                tick);
            serial.Step();
            r.Step();
            foreach (var name in Views)
            {
                Assert.Equal(Weights(serial.Outputs[name].CurrentView), r.Outputs[name].RowCount);
            }
        }
    }
}
