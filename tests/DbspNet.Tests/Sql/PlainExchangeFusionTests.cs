// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Plain-exchange fusion (docs/design-bounded-memory.md §5.56): at W>1 the Filter/Project chain in front of a
// DISTINCT's or a partitioned TOP-K's plain exchange runs in the exchange's sender. Required: every view
// equals the serial program on every tick (fusion on and off, managed, block-backed and forced deltas), the
// fused layout is really built, and a fused program survives a checkpoint, resharded too.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class PlainExchangeFusionTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (id INT NOT NULL, k INT, s VARCHAR, v BIGINT NOT NULL, p VARCHAR)",
        "CREATE TABLE u (k INT, s VARCHAR, w BIGINT NOT NULL)",
        // A DISTINCT over a projection that maps many rows to one, within a worker and across workers.
        "CREATE VIEW d1 AS SELECT DISTINCT COALESCE(k, -1) % 3 AS g, p FROM t WHERE v % 5 <> 0",
        // A DISTINCT over a filter and a widening projection.
        "CREATE VIEW d2 AS SELECT DISTINCT k, UPPER(s) AS s2, v * 2 AS v2 FROM t WHERE v > 2",
        // A DISTINCT straight over a table (nothing to fuse).
        "CREATE VIEW d3 AS SELECT DISTINCT k, s FROM u",
        // A DISTINCT whose chain drops nearly every row: every bucket tiny or empty.
        "CREATE VIEW d4 AS SELECT DISTINCT k FROM t WHERE id = 7",
        // A partitioned TOP-K over a filter + projection (the partition is a projected column).
        "CREATE VIEW tk1 AS SELECT id, g, v2 FROM (SELECT id, g, v2, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v2 DESC, id) AS rn " +
            "FROM (SELECT id, COALESCE(k, 0) % 3 AS g, v * 2 AS v2 FROM t WHERE v <> 3) x) y WHERE rn <= 2",
        // RANK = 1 (the rank-to-TOP-K rewrite, §5.51) over a filter, partitioned by a nullable string.
        "CREATE VIEW tk2 AS SELECT s, k, v, id FROM (SELECT s, k, v, id, RANK() OVER (PARTITION BY s ORDER BY v, id, k) AS r " +
            "FROM t WHERE p IS NOT NULL) q WHERE r = 1",
        // A TOP-K over a LEFT join's projection (the join builds the projected row, §5.47; NULL-padded rows).
        "CREATE VIEW tk3 AS SELECT id, k, w FROM (SELECT t.id, t.k, u.w, ROW_NUMBER() OVER (PARTITION BY t.k ORDER BY u.w DESC, t.id) AS rn " +
            "FROM t LEFT JOIN u ON t.k = u.k AND t.s = u.s) q WHERE rn <= 3",
        // Chains over operator outputs, whose deltas are block backed when deltas spill (the sender's spilling
        // split): a DISTINCT over an aggregate's projection (many groups to one row), and a TOP-K over an
        // inner join's filter + projection.
        "CREATE VIEW d5 AS SELECT DISTINCT sv % 4 AS a, COALESCE(k, -1) % 2 AS b FROM (SELECT id, k, SUM(v) AS sv FROM t GROUP BY id, k) q WHERE sv % 3 <> 0",
        "CREATE VIEW tk4 AS SELECT id, w2 FROM (SELECT id, w2, ROW_NUMBER() OVER (PARTITION BY id ORDER BY w2 DESC) AS rn " +
            "FROM (SELECT x.id, y.w * 3 AS w2 FROM t x JOIN u y ON x.k = y.k WHERE y.w > 1) j) q WHERE rn <= 2",
    ];

    private static readonly string[] Views = ["d1", "d2", "d3", "d4", "d5", "tk1", "tk2", "tk3", "tk4"];

    private static List<(string Table, object?[] Row, int Weight)> Tick(int tick)
    {
        var rng = new Random(1000 + tick);
        var rows = new List<(string Table, object?[] Row, int Weight)>();
        string? S(int i) => i % 5 == 0 ? null : i % 5 == 1 ? "" : "s" + (i % 4);
        for (var i = 0; i < 80; i++)
        {
            var id = rng.Next(0, 40);
            int? k = rng.Next(5) == 0 ? null : rng.Next(0, 8);
            var row = new object?[] { id, k, S(rng.Next(20)), (long)rng.Next(0, 12), rng.Next(4) == 0 ? null : "p" + rng.Next(3) };
            rows.Add(("t", row, 1));
            if (tick > 0 && rng.Next(3) == 0)
            {
                rows.Add(("t", row, -1)); // inserted and retracted in one tick
            }
        }

        for (var i = 0; i < 12; i++)
        {
            int? k = rng.Next(4) == 0 ? null : rng.Next(0, 8);
            rows.Add(("u", [k, S(rng.Next(20))?.ToUpperInvariant(), (long)rng.Next(-1, 9)], 1));
        }

        return rows;
    }

    // Each tick also retracts some of an earlier tick's rows, so joins lose matches and groups shrink.
    private static List<(string Table, object?[] Row, int Weight)> Changes(int tick)
    {
        var changes = Tick(tick);
        if (tick >= 2)
        {
            changes.AddRange(Tick(tick - 2).Where((_, i) => i % 3 == 0).Select(c => (c.Table, c.Row, -c.Weight)));
        }

        return changes;
    }

    private static void Push(Func<string, object> table, int tick)
    {
        foreach (var (name, row, weight) in Changes(tick))
        {
            switch (table(name))
            {
                case TableInput ti when weight > 0: ti.Insert(row); break;
                case TableInput ti: ti.Delete(row); break;
                case ShardedTableInput si when weight > 0: si.Insert(row); break;
                case ShardedTableInput si: si.Delete(row); break;
                default: throw new InvalidOperationException();
            }
        }
    }

    private static T With<T>(bool fusion, Func<T> f)
    {
        var prev = PlainExchangeFusionMode.Enabled;
        PlainExchangeFusionMode.Enabled = fusion;
        try
        {
            return f();
        }
        finally
        {
            PlainExchangeFusionMode.Enabled = prev;
        }
    }
    private static CompiledProgram Serial()
    {
        var prev = IndexFusionMode.Enabled;
        IndexFusionMode.Enabled = false;
        try
        {
            return SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal));
        }
        finally
        {
            IndexFusionMode.Enabled = prev;
        }
    }

    private static ParallelCompiledProgram Parallel(int workers, bool fusion, bool snapshots = false) => With(fusion, () =>
    {
        Assert.True(SqlProgram.TryCompileParallel(
            Sql, new HashSet<string>(Views, StringComparer.Ordinal), workers, out var p,
            snapshots ? ArrowSqlSnapshotCodecs.Instance : null));
        return p!;
    });

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var d = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var key = string.Join("|", Enumerable.Range(0, width).Select(i => kv.Key[i] is { } v ? FormattableString.Invariant($"{v}") : "<null>"));
            d[key] = d.GetValueOrDefault(key) + kv.Value.Value;
            if (d[key] == 0)
            {
                d.Remove(key);
            }
        }

        return d;
    }

    private static void AssertSame(CompiledProgram serial, ParallelCompiledProgram parallel, int tick)
    {
        foreach (var view in Views)
        {
            var width = serial.Outputs[view].Schema.Count;
            var expected = Materialize(serial.Outputs[view].CurrentView, width);
            Assert.True(expected.Count > 0 || view == "d4", $"{view} is empty at tick {tick}: the test exercises nothing");
            Assert.Equal(expected, Materialize(parallel.Outputs[view].CurrentView, width));
        }
    }

    private static void AssertMatchesSerial(int workers, bool fusion)
    {
        var serial = Serial();
        using var parallel = Parallel(workers, fusion);
        for (var tick = 0; tick < 6; tick++)
        {
            Push(name => serial.Table(name), tick);
            Push(name => parallel.Table(name), tick);
            serial.Step();
            parallel.Step();
            AssertSame(serial, parallel, tick);
        }
    }

    [Theory]
    [InlineData(2, true)]
    [InlineData(4, true)]
    [InlineData(8, true)]
    [InlineData(4, false)]
    public void Every_view_matches_serial(int workers, bool fusion) => AssertMatchesSerial(workers, fusion);

    // Block-backed deltas: the senders' grouped spilling split (rows 16) and the forced mode (every
    // builder blocked, 2-row chunks), where a selective chain leaves tiny buckets.
    // Unfused too: a plain exchange-index's grouped split can leave a column of managed buckets as well.
    [Theory]
    [InlineData(16L, 4096, true)]
    [InlineData(0L, 2, true)]
    [InlineData(16L, 4096, false)]
    public void Every_view_matches_serial_with_block_backed_deltas(long rows, int chunk, bool fusion)
    {
        using (SpillSettings.UseStepSpill(rows, chunk))
        {
            AssertMatchesSerial(4, fusion);
        }
    }

    [Fact]
    public void The_fused_layout_is_built()
    {
        static (int Steps, int Applies, int Exchanges) Shape(ParallelCompiledProgram p)
        {
            var ops = p.Circuit.Replicas[0].Operators.Select(o => o.GetType().Name).ToList();
            return (ops.Count(n => n.StartsWith("ExchangeStepOp", StringComparison.Ordinal)),
                ops.Count(n => n.StartsWith("ApplyOp", StringComparison.Ordinal)),
                ops.Count(n => n.StartsWith("Exchange", StringComparison.Ordinal)));
        }

        using var on = Parallel(4, fusion: true);
        using var off = Parallel(4, fusion: false);
        var (a, b) = (Shape(on), Shape(off));
        Assert.Equal(0, b.Steps);
        var c = on.Circuit.Replicas[0];
        var fused = Enumerable.Range(0, c.Operators.Count)
            .Where(k => c.Operators[k].GetType().Name.StartsWith("ExchangeStepOp", StringComparison.Ordinal))
            .Select(k => c.OperatorLabel(k)).ToList();
        // d3's chain is its projection of u; tk3's projection is folded into the LEFT join (§5.47), so
        // nothing is left to run in its sender and it keeps a plain exchange.
        Assert.Equal("d1,d2,d3,d4,tk1,tk2,d5,tk4", string.Join(",", fused));
        Assert.True(a.Applies <= b.Applies - 4, $"fused {a.Applies} ApplyOps, unfused {b.Applies}");
        Assert.Equal(b.Exchanges, a.Exchanges); // one exchange for each the unfused layout has
    }
    [Theory]
    [InlineData(4, 4)]
    [InlineData(4, 3)]
    public async Task A_fused_program_survives_a_checkpoint(int from, int to)
    {
        var fs = new InMemoryTableFileSystem();
        var serial = Serial();
        using (var producer = Parallel(from, fusion: true, snapshots: true))
        {
            for (var tick = 0; tick < 3; tick++)
            {
                Push(name => serial.Table(name), tick);
                serial.Step();
                Push(name => producer.Table(name), tick);
                producer.Step();
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Parallel(to, fusion: true, snapshots: true);
        if (from != to)
        {
            Assert.True(await ParallelReshard.RewriteAsync(fs, restored) > 0);
        }

        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        for (var tick = 3; tick < 6; tick++)
        {
            Push(name => serial.Table(name), tick);
            serial.Step();
            Push(name => restored.Table(name), tick);
            restored.Step();
            AssertSame(serial, restored, tick);
        }
    }
}
