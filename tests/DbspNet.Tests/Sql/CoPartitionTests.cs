// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Co-partitioning (docs/design-bounded-memory.md §5.57): at W>1 the aggregates / DISTINCTs under a hashed
// LEFT join partition on the join's bare key columns, the join elides both exchanges, and two of them over
// one input share one exchange. Partition claims follow bare-column projections and an elided aggregate's
// claim is its input's. Required: every view equals the serial program on every tick (managed and
// block-backed deltas), the co-partitioned layout is really built, and it survives a checkpoint, resharded
// too (a wrong placement sends a key to a worker its partner never reaches).
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class CoPartitionTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (id INT NOT NULL, k INT, s VARCHAR, v BIGINT NOT NULL, p VARCHAR)",
        "CREATE TABLE u (k INT, s VARCHAR, w BIGINT NOT NULL)",
        "CREATE TABLE w (a INT, b INT, x BIGINT NOT NULL)",
        "CREATE TABLE v (a INT, b INT)",
        // Issue #2's eligibility shape: an aggregate LEFT JOIN an aggregate over a DISTINCT, one input, on the
        // group key with IS NOT DISTINCT FROM and COALESCE equalities (only p is a bare equi-key).
        "CREATE VIEW elig AS SELECT a.k, a.s, a.p, a.n, COALESCE(m.cs, 0) AS cs " +
            "FROM (SELECT k, s, p, COUNT(DISTINCT id) AS n FROM t GROUP BY k, s, p) a " +
            "LEFT JOIN (SELECT k, s, p, SUM(v) AS cs FROM (SELECT DISTINCT k, s, p, id, v FROM t WHERE p IS NOT NULL) d GROUP BY k, s, p) m " +
            "ON a.k IS NOT DISTINCT FROM m.k AND COALESCE(a.s, '') = COALESCE(m.s, '') AND a.p = m.p",
        // Two aggregates over one table, joined on part of the wider one's key: one shared exchange.
        "CREATE VIEW aa AS SELECT a.k, a.s, a.p, a.n, m.sv " +
            "FROM (SELECT k, s, p, COUNT(*) AS n FROM t GROUP BY k, s, p) a " +
            "LEFT JOIN (SELECT s, k, SUM(v) AS sv FROM t WHERE v > 2 GROUP BY s, k) m ON a.s = m.s AND a.k = m.k",
        // A DISTINCT LEFT JOIN an aggregate of another table: each exchanged by the join's key, join elided.
        "CREATE VIEW ad AS SELECT d.k, d.p, m.sw FROM (SELECT DISTINCT k, p FROM t WHERE id % 3 <> 0) d " +
            "LEFT JOIN (SELECT k, SUM(w) AS sw FROM u GROUP BY k) m ON d.k = m.k",
        // One computed equi-key (traced to no group column) and one bare: co-partitioned on s alone.
        "CREATE VIEW comp AS SELECT a.k, a.s, a.n, m.sw FROM (SELECT k, s, COUNT(*) AS n FROM t GROUP BY k, s) a " +
            "LEFT JOIN (SELECT k, s, SUM(w) AS sw FROM u GROUP BY k, s) m ON a.k + 1 = m.k + 1 AND a.s = m.s",
        // Aggregates elided twice in a row through projections: the outer one's placement is the inner
        // exchange's hash of id, not of its own (id, b) key (an elided aggregate's claim is its input's).
        "CREATE VIEW chain3 AS SELECT id, b, c2, COUNT(*) AS n FROM (SELECT id, b, c % 2 AS c2 FROM " +
            "(SELECT id, b, COUNT(*) AS c FROM (SELECT id, sv % 3 AS b FROM (SELECT id, SUM(v) AS sv FROM t GROUP BY id) q0) q1 GROUP BY id, b) q2) q3 " +
            "GROUP BY id, b, c2",
        // Claims that pair the wrong way: the aggregate is hashed on (a, b), the inner join's output on
        // (b, a) (its key order). Equal (a, b) pairs need not share a worker: the join must exchange.
        "CREATE VIEW cross_pair AS SELECT l.a, l.b, l.n, r.x FROM (SELECT a, b, COUNT(*) AS n FROM w GROUP BY a, b) l " +
            "LEFT JOIN (SELECT w2.b, w2.a, w2.x FROM w w2 JOIN v ON w2.b = v.b AND w2.a = v.a) r ON l.a = r.a AND l.b = r.b",
    ];

    private static readonly string[] Views = ["elig", "aa", "ad", "comp", "chain3", "cross_pair"];

    private static List<(string Table, object?[] Row, int Weight)> Tick(int tick)
    {
        var rng = new Random(2000 + tick);
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
            rows.Add(("u", [k, S(rng.Next(20)), (long)rng.Next(-1, 9)], 1));
        }

        int? Small() => rng.Next(6) == 0 ? null : rng.Next(0, 5);
        for (var i = 0; i < 30; i++)
        {
            rows.Add(("w", [Small(), Small(), (long)rng.Next(0, 9)], 1));
        }

        for (var i = 0; i < 8; i++)
        {
            rows.Add(("v", [Small(), Small()], 1));
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

    private static T With<T>(bool coPartition, Func<T> f)
    {
        var prev = CoPartitionMode.Enabled;
        CoPartitionMode.Enabled = coPartition;
        try
        {
            return f();
        }
        finally
        {
            CoPartitionMode.Enabled = prev;
        }
    }

    private static CompiledProgram Serial() => SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal));

    private static ParallelCompiledProgram Parallel(int workers, bool coPartition, bool snapshots = false, params string[] views) =>
        With(coPartition, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(
                Sql, new HashSet<string>(views.Length > 0 ? views : Views, StringComparer.Ordinal), workers, out var p,
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
            Assert.True(expected.Count > 0, $"{view} is empty at tick {tick}: the test exercises nothing");
            Assert.Equal(expected, Materialize(parallel.Outputs[view].CurrentView, width));
        }
    }

    private static void AssertMatchesSerial(int workers, bool coPartition)
    {
        var serial = Serial();
        using var parallel = Parallel(workers, coPartition);
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
    [InlineData(3, true)]
    [InlineData(4, true)]
    [InlineData(8, true)]
    [InlineData(4, false)]
    public void Every_view_matches_serial(int workers, bool coPartition) => AssertMatchesSerial(workers, coPartition);

    // Block-backed deltas: the shared exchange's split and the local index passes over spilled input.
    [Theory]
    [InlineData(16L, 4096)]
    [InlineData(0L, 2)]
    public void Every_view_matches_serial_with_block_backed_deltas(long rows, int chunk)
    {
        using (SpillSettings.UseStepSpill(rows, chunk))
        {
            AssertMatchesSerial(4, coPartition: true);
        }
    }

    private static int Exchanges(ParallelCompiledProgram p, string view)
    {
        var c = p.Circuit.Replicas[0];
        return Enumerable.Range(0, c.Operators.Count)
            .Count(k => c.OperatorLabel(k) == view && c.Operators[k].GetType().Name.StartsWith("Exchange", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("elig", 1, 5)] // one shared exchange of t; the DISTINCT, its aggregate and the join elide
    [InlineData("aa", 1, 4)] // one shared exchange of t on (s, k)
    [InlineData("ad", 2, 4)] // DISTINCT and aggregate each exchanged by k (different inputs); join elided
    [InlineData("comp", 2, 4)] // both aggregates exchanged by s; join elided
    [InlineData("cross_pair", 5, 5)] // the claims pair the wrong way: nothing elides
    [InlineData("chain3", 1, 3)] // the inner aggregate's exchange; both outer aggregates elide (off: a projection drops the claim)
    public void The_co_partitioned_layout_is_built(string view, int on, int off)
    {
        using var a = Parallel(4, coPartition: true, views: view);
        using var b = Parallel(4, coPartition: false, views: view);
        Assert.Equal(on, Exchanges(a, view));
        Assert.Equal(off, Exchanges(b, view));
    }

    [Theory]
    [InlineData(4, 4)]
    [InlineData(4, 3)]
    [InlineData(3, 5)]
    public async Task A_co_partitioned_program_survives_a_checkpoint(int from, int to)
    {
        var fs = new InMemoryTableFileSystem();
        var serial = Serial();
        using (var producer = Parallel(from, coPartition: true, snapshots: true))
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

        using var restored = Parallel(to, coPartition: true, snapshots: true);
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
