// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A global ROW_NUMBER / RANK at W>1 ranks in ranges of its ORDER BY keys, one per worker
// (docs/design-bounded-memory.md §5.50). Required: every view equals the serial program on every tick
// while the rank grows from a tiny first tick (one range on worker 0) past the re-split threshold,
// re-splits as it doubles (moving every row), shrinks and grows again; inserts land at the front, middle
// and end; rows come in with weight two and leave; NULL keys sort first and last; DESC keys; RANK tie
// groups larger than a range. DENSE_RANK keeps the gather. A checkpoint continues at the same W and
// re-sharded; the off switch restores the gather; each range rank reaches only its own streams.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

[Collection("RangeRank")]
public class RangeRankTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (id INT NOT NULL, k1 INT, k2 VARCHAR, v BIGINT NOT NULL)",
        "CREATE VIEW rn AS SELECT id, k1, k2, v, ROW_NUMBER() OVER (ORDER BY k1 NULLS FIRST, k2 NULLS FIRST) AS r FROM t",
        "CREATE VIEW rnk AS SELECT id, k1, v, RANK() OVER (ORDER BY k1 DESC NULLS LAST) AS r FROM t",
        "CREATE VIEW tail AS SELECT id, k2, ROW_NUMBER() OVER (ORDER BY k2 NULLS LAST, id DESC) AS r FROM t",
        "CREATE VIEW dense AS SELECT id, k1, DENSE_RANK() OVER (ORDER BY k1) AS r FROM t",
        "CREATE VIEW agg AS SELECT k1, COUNT(*) AS n, MAX(r) AS top, SUM(r) AS s FROM rn GROUP BY k1",
    ];

    private static readonly string[] Views = ["rn", "rnk", "tail", "dense", "agg"];

    private const long MinRows = 50;

    // The ticks, generated once against a model of the table so deletes remove rows that are there.
    // Tick 0 is tiny (below MinRows: one range); tick 1 crosses it (the first split); later ticks insert
    // at the front (k1 = -5), middle and end (k1 = 40), delete, double the table (re-splits that move
    // rows), shrink it to a handful and grow it again.
    private static readonly List<List<(object?[] Row, long Weight)>> Ticks = Generate(18);

    private static List<List<(object?[] Row, long Weight)>> Generate(int ticks)
    {
        var rng = new Random(20261112);
        var table = new List<object?[]>();
        var result = new List<List<(object?[] Row, long Weight)>>();
        object?[] Row(int k1Lo, int k1Hi) =>
        [
            rng.Next(0, 5_000),
            rng.Next(8) == 0 ? null : rng.Next(k1Lo, k1Hi),
            rng.Next(6) == 0 ? null : ((char)('a' + rng.Next(6))).ToString(),
            (long)rng.Next(100),
        ];

        for (var t = 0; t < ticks; t++)
        {
            var delta = new List<(object?[] Row, long Weight)>();
            void Insert(object?[] row, long weight = 1)
            {
                delta.Add((row, weight));
                for (var i = 0; i < weight; i++)
                {
                    table.Add(row);
                }
            }

            void Delete(int count)
            {
                for (var i = 0; i < count && table.Count > 0; i++)
                {
                    var at = rng.Next(table.Count);
                    delta.Add((table[at], -1));
                    table.RemoveAt(at);
                }
            }

            var inserts = t switch
            {
                0 => 5,
                1 => 80,
                2 or 3 => 10,
                4 => 120,
                5 => 0,
                6 => 260,
                10 => 0,
                12 => 400,
                _ => 15,
            };

            for (var i = 0; i < inserts; i++)
            {
                var row = (i % 5) switch
                {
                    0 when t % 3 == 0 => Row(-5, -4), // the front
                    1 when t % 4 == 1 => Row(40, 41), // the end
                    _ => Row(0, 20),
                };
                Insert(row, rng.Next(7) == 0 ? 2 : 1);
            }

            Delete(t switch
            {
                5 => 30,
                10 => table.Count - 6, // shrink to a handful
                0 or 1 => 0,
                _ => 6,
            });
            result.Add(delta);
        }

        return result;
    }

    private static T With<T>(bool range, Func<T> compile)
    {
        var (prevOn, prevMin) = (RangeRankMode.Enabled, RangeRankMode.MinRows);
        RangeRankMode.Enabled = range;
        RangeRankMode.MinRows = MinRows;
        try
        {
            return compile();
        }
        finally
        {
            (RangeRankMode.Enabled, RangeRankMode.MinRows) = (prevOn, prevMin);
        }
    }

    private static CompiledProgram Serial() =>
        SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance);

    private static ParallelCompiledProgram Parallel(int workers, bool range = true) =>
        With(range, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(
                Sql, new HashSet<string>(Views, StringComparer.Ordinal), workers, out var p, ArrowSqlSnapshotCodecs.Instance));
            return p!;
        });

    private static string Digest(Func<string, (ZSet<StructuralRow, Z64> View, int Width)> output) =>
        string.Join(" / ", Views.Select(v =>
        {
            var (view, width) = output(v);
            var d = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var kv in view)
            {
                var key = string.Join("|", Enumerable.Range(0, width).Select(i => kv.Key[i] is { } x ? FormattableString.Invariant($"{x}") : "<null>"));
                d[key] = d.GetValueOrDefault(key) + kv.Value.Value;
            }

            return v + ": " + string.Join("; ", d.Where(e => e.Value != 0).Select(e => e.Key + "#" + e.Value).Order(StringComparer.Ordinal));
        }));

    private static string Digest(CompiledProgram p) => Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static string Digest(ParallelCompiledProgram p) => Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static void Step(CompiledProgram p, int tick)
    {
        p.Table("t").Push(Ticks[tick]);
        p.Step();
    }

    private static void Step(ParallelCompiledProgram p, int tick)
    {
        p.Table("t").Push(Ticks[tick]);
        p.Step();
    }

    private static List<RangeRankOp> Ranges(ParallelCompiledProgram p) =>
        [.. p.Circuit.Replicas.SelectMany(r => r.Operators).OfType<RangeRankOp>()];

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    public void Range_rank_equals_serial_on_every_tick(int workers)
    {
        var serial = Serial();
        using var parallel = Parallel(workers);

        // rn, rnk and tail range; dense gathers.
        Assert.All(parallel.Circuit.Replicas, r => Assert.Equal(3, r.Operators.OfType<RangeRankOp>().Count()));
        var spread = false;
        for (var tick = 0; tick < Ticks.Count; tick++)
        {
            Step(serial, tick);
            Step(parallel, tick);
            Assert.Equal(Digest(serial), Digest(parallel));
            if (tick == 0)
            {
                Assert.All(Ranges(parallel), o => Assert.Equal(0, o.Resplits));
            }

            // Rows on more than one worker once split.
            spread |= Ranges(parallel).GroupBy(o => o.MetricName).Any(_ => true)
                && parallel.Circuit.Replicas.Count(r => r.Operators.OfType<RangeRankOp>().First().LocalRows > 0) > 1;
        }

        Assert.True(spread, "the rank never spread over the workers");

        // The first split, then re-splits as the table doubles (rows move), on every replica alike.
        var resplits = Ranges(parallel).Select(o => o.Resplits).Distinct().ToList();
        Assert.Single(resplits);
        Assert.True(resplits[0] >= 3, $"{resplits[0]} re-splits");
    }

    [Fact]
    public void Below_the_threshold_the_rank_stays_on_worker_zero()
    {
        using var parallel = Parallel(4);
        Step(parallel, 0);
        Assert.All(Ranges(parallel), o => Assert.Empty(o.Splitters));
        Assert.All(parallel.Circuit.Replicas.Skip(1), r => Assert.All(r.Operators.OfType<RangeRankOp>(), o => Assert.Equal(0, o.LocalRows)));
    }

    [Fact]
    public void Off_gathers_as_before()
    {
        var serial = Serial();
        using var parallel = Parallel(4, range: false);
        Assert.Empty(Ranges(parallel));
        for (var tick = 0; tick < 8; tick++)
        {
            Step(serial, tick);
            Step(parallel, tick);
            Assert.Equal(Digest(serial), Digest(parallel));
        }
    }

    [Theory]
    [InlineData(4, 4, 3)]
    [InlineData(4, 4, 8)]
    [InlineData(4, 3, 8)]
    [InlineData(2, 5, 8)]
    [InlineData(3, 2, 11)]
    public async Task A_range_rank_survives_a_checkpoint(int from, int to, int at)
    {
        var fs = new InMemoryTableFileSystem();
        var serial = Serial();
        using (var producer = Parallel(from))
        {
            for (var tick = 0; tick < at; tick++)
            {
                Step(serial, tick);
                Step(producer, tick);
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Parallel(to);
        if (from != to)
        {
            Assert.True(await ParallelReshard.RewriteAsync(fs, restored) > 0);
        }

        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        for (var tick = at; tick < Ticks.Count; tick++)
        {
            Step(serial, tick);
            Step(restored, tick);
            Assert.Equal(Digest(serial), Digest(restored));
        }

        if (from != to)
        {
            // Re-sharded: one range on worker 0, re-split on the first step after.
            Assert.All(Ranges(restored), o => Assert.True(o.Resplits >= 1));
        }
    }

    // The release planner finds the streams an operator reads by walking its fields: a range rank
    // reaches its input, its output and its private rank's two streams, in every replica.
    [Fact]
    public void Each_range_rank_reaches_only_its_own_streams()
    {
        using var parallel = Parallel(4);
        foreach (var replica in parallel.Circuit.Replicas)
        {
            var ops = replica.Operators;
            var readers = StreamRelease.ScanReaders(ops);
            var ranges = Enumerable.Range(0, ops.Count).Where(i => ops[i] is RangeRankOp).ToList();
            Assert.Equal(3, ranges.Count);
            foreach (var i in ranges)
            {
                Assert.Equal(4, readers.ByStream.Count(kv => kv.Value.Contains(i)));
            }
        }
    }
}
