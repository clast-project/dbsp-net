// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A LEFT join whose rows only a projection reads builds the projected row straight from its two
// input rows (docs/design-bounded-memory.md §5.47). Required: every view equals the unfolded
// program on every tick (serial managed and blocked, indexed fused and unfused, parallel with
// broadcast and hashed joins, across a checkpoint); the fold really happens; and a join two
// parents share is never folded (it would compile twice).
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class JoinProjectionFoldTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE f (id INT NOT NULL, code INT, tier VARCHAR, amt BIGINT NOT NULL)",
        "CREATE TABLE d (code INT, label VARCHAR NOT NULL)",
        "CREATE TABLE t (tier VARCHAR NOT NULL, rank_ INT NOT NULL)",
        // A chain: each join's output projected (expressions over both sides, padded columns
        // included) before the next join keys on it.
        "CREATE VIEW chain AS SELECT x.id, x.amt, x.label, t2.rank_ + 1 AS r " +
            "FROM (SELECT f.id, f.amt * 2 AS amt, COALESCE(d.label, 'none') AS label, UPPER(COALESCE(f.tier, 'x')) AS tk " +
            "FROM f LEFT JOIN d ON f.code = d.code) x LEFT JOIN t t2 ON x.tk = t2.tier",
        // A filter between the join and its projection: not folded, still right.
        "CREATE VIEW filt AS SELECT id, label FROM (SELECT f.id, d.label, f.amt FROM f LEFT JOIN d ON f.code = d.code) z WHERE amt <> 2",
        // A residual ON conjunct (the anti-join rewrite), then a projection.
        "CREATE VIEW resid AS SELECT f.id, LENGTH(d.label) AS n FROM f LEFT JOIN d ON f.code = d.code AND f.amt > 1",
        // The same join under two projections: CSE shares it, so neither folds.
        "CREATE VIEW twice AS SELECT f.id, d.label FROM f LEFT JOIN d ON f.code = d.code " +
            "UNION ALL SELECT f.id, UPPER(d.label) FROM f LEFT JOIN d ON f.code = d.code",
        "CREATE VIEW agg AS SELECT label, r, COUNT(*) AS n, SUM(amt) AS s FROM chain GROUP BY label, r",
        // Hashed at W>1, the join's rows sit by f.code (index 1); the projection puts amt there.
        // Grouped on amt: only right if the folded output claims no partition.
        "CREATE VIEW moved AS SELECT a, COUNT(*) AS n FROM (SELECT f.id, f.amt AS a, d.label FROM f LEFT JOIN d ON f.code = d.code) z GROUP BY a",
    ];

    private static readonly string[] Views = ["chain", "filt", "resid", "twice", "agg", "moved"];

    private static readonly Dictionary<string, long> Counts = new(StringComparer.Ordinal)
    {
        ["f"] = 10_000_000, ["d"] = 10, ["t"] = 3,
    };

    private static T With<T>(bool fold, bool indexFusion, Func<T> compile)
    {
        var (prevFold, prevFusion) = (JoinProjectionFoldMode.Enabled, IndexFusionMode.Enabled);
        JoinProjectionFoldMode.Enabled = fold;
        IndexFusionMode.Enabled = indexFusion;
        try
        {
            return compile();
        }
        finally
        {
            (JoinProjectionFoldMode.Enabled, IndexFusionMode.Enabled) = (prevFold, prevFusion);
        }
    }

    private static CompiledProgram Compile(bool fold, bool blocked = false, bool indexFusion = true) =>
        With(fold, indexFusion, () =>
        {
            using var mode = TraceBlocksMode.Use(join: blocked, freezeEntries: 8, blockEntries: 4);
            return SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance);
        });

    private static ParallelCompiledProgram CompileParallel(bool fold, int workers, IReadOnlyDictionary<string, long> counts) =>
        With(fold, indexFusion: true, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(
                Sql, new HashSet<string>(Views, StringComparer.Ordinal), workers, out var p,
                options: new CompileOptions { BroadcastMaxRows = 100, RelationRowCounts = counts }));
            return p!;
        });

    // NULL codes on both sides, codes with no dimension row (padded), dimension rows that come
    // and go (padded rows gain and lose matches), fact deletes, and duplicate rows.
    private static void Tick(Func<string, object> table, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < 20; i++)
        {
            int? code = rng.Next(4) == 0 ? null : rng.Next(0, 6);
            var tier = rng.Next(5) == 0 ? null : "abc"[rng.Next(3)].ToString();
            Insert(table("f"), rng.Next(0, 30), code, tier, (long)rng.Next(4));
        }

        if (rng.Next(2) == 0)
        {
            Insert(table("d"), rng.Next(3) == 0 ? null : rng.Next(0, 7), "L" + new string('x', rng.Next(4)));
        }

        if (tick == 0)
        {
            Insert(table("t"), "A", 1);
            Insert(table("t"), "B", 2);
            Insert(table("t"), "X", 0);
        }
        else if (tick % 4 == 2)
        {
            Delete(table("t"), "A", 1);
            Insert(table("t"), "A", 5);
        }
        else if (tick % 4 == 0)
        {
            Delete(table("t"), "A", 5);
            Insert(table("t"), "A", 1);
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 6; i++)
            {
                int? code = old.Next(4) == 0 ? null : old.Next(0, 6);
                var tier = old.Next(5) == 0 ? null : "abc"[old.Next(3)].ToString();
                Delete(table("f"), old.Next(0, 30), code, tier, (long)old.Next(4));
            }

            if (old.Next(2) == 0 && tick % 3 == 0)
            {
                Delete(table("d"), old.Next(3) == 0 ? null : old.Next(0, 7), "L" + new string('x', old.Next(4)));
            }
        }
    }

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

    private static string Digest(CompiledProgram p) =>
        Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static string Digest(ParallelCompiledProgram p) =>
        Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static void Step(CompiledProgram p, int tick)
    {
        Tick(p.Table, tick);
        p.Step();
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public void Folded_equals_unfolded_on_every_tick(bool blocked, bool indexFusion)
    {
        var folded = Compile(fold: true, blocked, indexFusion);
        var unfolded = Compile(fold: false, blocked, indexFusion);
        for (var tick = 0; tick < 14; tick++)
        {
            Step(folded, tick);
            Step(unfolded, tick);
            Assert.Equal(Digest(unfolded), Digest(folded));
        }
    }

    // t (3 rows) and d (10) broadcast; with no counts every join hashes its preserved side.
    [Theory]
    [InlineData(4, true)]
    [InlineData(4, false)]
    [InlineData(8, true)]
    public void Parallel_folded_equals_serial(int workers, bool hinted)
    {
        var serial = Compile(fold: false);
        using var parallel = CompileParallel(fold: true, workers, hinted ? Counts : new Dictionary<string, long>());
        for (var tick = 0; tick < 10; tick++)
        {
            Step(serial, tick);
            Tick(parallel.Table, tick);
            parallel.Step();
            Assert.Equal(Digest(serial), Digest(parallel));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task A_folded_program_survives_a_checkpoint(bool writeBlocked, bool readBlocked)
    {
        RunCheckpoint.Verify = true;
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile(fold: true, writeBlocked);
        for (var tick = 0; tick < 8; tick++)
        {
            Step(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var restored = Compile(fold: true, readBlocked);
        await Snapshot.ReadAsync(restored.Circuit, fs);

        var twin = Compile(fold: false, readBlocked);
        for (var tick = 0; tick < 8; tick++)
        {
            Step(twin, tick);
        }

        for (var tick = 100; tick < 108; tick++)
        {
            Step(twin, tick);
            Step(restored, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }
    }

    private static int Ops(CompiledProgram p, string prefix) =>
        p.Circuit.Operators.Count(o => o.GetType().Name.StartsWith(prefix, StringComparison.Ordinal));

    // Folding drops a projection pass per folded join and adds no join: the shared join in
    // `twice` stays one operator.
    [Fact]
    public void The_fold_drops_projection_passes_and_never_duplicates_a_join()
    {
        var folded = Compile(fold: true);
        var unfolded = Compile(fold: false);
        Assert.Equal(Ops(unfolded, "IncrementalLeftJoinOp"), Ops(folded, "IncrementalLeftJoinOp"));
        Assert.True(
            Ops(folded, "ApplyOp") <= Ops(unfolded, "ApplyOp") - 2,
            $"folded {Ops(folded, "ApplyOp")} ApplyOps, unfolded {Ops(unfolded, "ApplyOp")}");
    }

    [Fact]
    public void A_join_two_projections_share_is_not_folded()
    {
        var sql = new[] { Sql[0], Sql[1], Sql[6] };
        int Joins(bool fold) => With(fold, indexFusion: true, () =>
        {
            var p = SqlProgram.Compile(sql, new HashSet<string>(["twice"], StringComparer.Ordinal));
            return Ops(p, "IncrementalLeftJoinOp");
        });

        Assert.Equal(1, Joins(fold: false));
        Assert.Equal(1, Joins(fold: true));
    }

    // The release planner finds the streams an operator reads by walking its fields and closures. A
    // delegate whose closure reached the compile context would reach every stream compiled so far
    // and hold them all until that operator steps: each LEFT join must reach exactly its two inputs
    // and its output, folded or not, serial or in every replica.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Each_left_join_reaches_only_its_own_streams(bool fold)
    {
        void Check(IReadOnlyList<IOperator> ops)
        {
            var readers = StreamRelease.ScanReaders(ops);
            var joins = Enumerable.Range(0, ops.Count).Where(i => ops[i].GetType().Name.StartsWith("IncrementalLeftJoinOp", StringComparison.Ordinal)).ToList();
            Assert.NotEmpty(joins);
            foreach (var i in joins)
            {
                Assert.Equal(3, readers.ByStream.Count(kv => kv.Value.Contains(i)));
            }

            Assert.All(Enumerable.Range(0, ops.Count), i => Assert.True(
                readers.ByStream.Count(kv => kv.Value.Contains(i)) <= 8,
                $"op {i} ({ops[i].GetType().Name}) reaches {readers.ByStream.Count(kv => kv.Value.Contains(i))} streams"));
        }

        Check(Compile(fold).Circuit.Operators);
        using var parallel = CompileParallel(fold, 4, Counts);
        foreach (var replica in parallel.Circuit.Replicas)
        {
            Check(replica.Operators);
        }
    }

    private static void Insert(object table, params object?[] values)
    {
        switch (table)
        {
            case TableInput t: t.Insert(values); break;
            case ShardedTableInput s: s.Insert(values); break;
            default: throw new InvalidOperationException();
        }
    }

    private static void Delete(object table, params object?[] values)
    {
        switch (table)
        {
            case TableInput t: t.Delete(values); break;
            case ShardedTableInput s: s.Delete(values); break;
            default: throw new InvalidOperationException();
        }
    }
}
