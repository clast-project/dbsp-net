// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Exchange fusion (docs/design-bounded-memory.md §5.55): at W>1 an exchanged-and-indexed input runs its
// own Filter/Project chain and NULL-key check in the exchange's sender, and a hashed LEFT join's preserved
// side is exchanged and indexed in one operator, NULL keys indexed and padded by the join. Required: every
// view equals the serial program on every tick (fusion on and off, managed, block-backed and forced
// deltas), the fused layout is really built, and a fused program survives a checkpoint, resharded too.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class ExchangeFusionTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (id INT NOT NULL, k INT, s VARCHAR, v BIGINT NOT NULL, p VARCHAR)",
        "CREATE TABLE u (k INT, s VARCHAR, w BIGINT NOT NULL)",
        // An aggregate over a chain whose projection maps many rows to one (the sender must merge them).
        "CREATE VIEW agg1 AS SELECT g, SUM(v) AS sv, COUNT(*) AS n FROM (SELECT COALESCE(k, -1) % 3 AS g, v FROM t WHERE v % 5 <> 0) x GROUP BY g",
        // Partitioned by (id, k) inside one view (a view boundary drops the claim); a filter-only chain
        // keeps that partition, so the outer aggregate elides its exchange.
        "CREATE VIEW agg2 AS SELECT id, k, COUNT(*) AS n, SUM(sv) AS s FROM (SELECT id, k, SUM(v) AS sv FROM t GROUP BY id, k) q WHERE sv > 3 GROUP BY id, k",
        // A projection puts other columns at the partition's indices: must exchange (the stale-claim trap).
        "CREATE VIEW agg3 AS SELECT a, b, COUNT(*) AS n FROM (SELECT sv % 4 AS a, id % 2 AS b FROM (SELECT id, k, SUM(v) AS sv FROM t GROUP BY id, k) q0) q GROUP BY a, b",
        // An inner join with chains on both sides and NULL keys on both.
        "CREATE VIEW ij AS SELECT x.id, x.k1, y.w FROM (SELECT id, k + 1 AS k1 FROM t WHERE v > 2) x " +
            "JOIN (SELECT k + 1 AS k1, w FROM u WHERE w <> 7) y ON x.k1 = y.k1",
        // A hashed LEFT join: chains on both sides, NULL keys on both, rows that gain and lose matches.
        "CREATE VIEW lj AS SELECT x.id, x.s2, y.w FROM (SELECT id, UPPER(s) AS s2, k FROM t WHERE v <> 4) x " +
            "LEFT JOIN (SELECT LOWER(s) AS s3, k, w FROM u WHERE w > 0) y ON x.k = y.k AND x.s2 = UPPER(y.s3)",
        // Issue #2's eligibility view's shape: an aggregate LEFT JOIN an aggregate over a DISTINCT, on the
        // group key with IS NOT DISTINCT FROM and COALESCE equalities.
        "CREATE VIEW elig AS SELECT a.k, a.s, a.p, a.n, COALESCE(m.cs, 0) AS cs " +
            "FROM (SELECT k, s, p, COUNT(DISTINCT id) AS n FROM t GROUP BY k, s, p) a " +
            "LEFT JOIN (SELECT k, s, p, SUM(v) AS cs FROM (SELECT DISTINCT k, s, p, id, v FROM t WHERE p IS NOT NULL) d GROUP BY k, s, p) m " +
            "ON a.k IS NOT DISTINCT FROM m.k AND COALESCE(a.s, '') = COALESCE(m.s, '') AND a.p = m.p",
        // A chain that drops nearly every row: every exchange bucket is tiny.
        "CREATE VIEW rare AS SELECT k, SUM(v) AS sv FROM t WHERE id = 7 GROUP BY k",
    ];

    private static readonly string[] Views = ["agg1", "agg2", "agg3", "ij", "lj", "elig", "rare"];

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
        var prev = ExchangeFusionMode.Enabled;
        ExchangeFusionMode.Enabled = fusion;
        try
        {
            return f();
        }
        finally
        {
            ExchangeFusionMode.Enabled = prev;
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
            Assert.True(expected.Count > 0 || view == "rare", $"{view} is empty at tick {tick}: the test exercises nothing");
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
            return (ops.Count(n => n.StartsWith("ExchangeIndexStepOp", StringComparison.Ordinal)),
                ops.Count(n => n.StartsWith("ApplyOp", StringComparison.Ordinal)),
                ops.Count(n => n.StartsWith("Exchange", StringComparison.Ordinal)));
        }

        using var on = Parallel(4, fusion: true);
        using var off = Parallel(4, fusion: false);
        var (a, b) = (Shape(on), Shape(off));
        Assert.Equal(0, b.Steps);
        Assert.True(a.Steps >= 8, $"{a.Steps} fused exchanges");
        Assert.True(a.Applies <= b.Applies - 8, $"fused {a.Applies} ApplyOps, unfused {b.Applies}");
        Assert.Equal(b.Exchanges, a.Exchanges); // one exchange for each the unfused layout has
    }

    // agg2's outer aggregate reads a filter-only chain over an input already partitioned by a subset of
    // its group key: fused into the index pass, it still elides its exchange (only the inner one is left).
    [Fact]
    public void A_filter_only_chain_keeps_its_partition()
    {
        using var p = With(true, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(Sql, new HashSet<string>(["agg2"], StringComparer.Ordinal), 4, out var q));
            return q!;
        });
        Assert.Equal(1, p.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.StartsWith("Exchange", StringComparison.Ordinal)));
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
