// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A run of LEFT joins against small dimensions compiles as one star operator
// (docs/design-bounded-memory.md §5.48). Required: every view equals the chain of joins (and a
// from-scratch recompute) on every tick, while the dimensions change under a non-empty fact; the star
// really forms where it should and not where it should not; parallel W=2/4/8 equals serial; a
// checkpoint in four layouts continues as the chain does; a directory past its cap falls back to a
// key scan and stays right; and each star reads exactly its own streams.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

[Collection("StarJoin")]
public class StarJoinTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE f (id INT NOT NULL, a INT, b VARCHAR, p1 VARCHAR, p2 VARCHAR, h INT, amt BIGINT NOT NULL)",
        "CREATE TABLE d1 (a INT, x VARCHAR NOT NULL)",
        "CREATE TABLE d2 (p1 VARCHAR NOT NULL, p2 VARCHAR NOT NULL, y INT NOT NULL)",
        "CREATE TABLE d3 (b VARCHAR, lo BIGINT NOT NULL, z INT NOT NULL)",
        "CREATE TABLE d4 (h INT NOT NULL, hk INT NOT NULL)",
        "CREATE TABLE d5 (x VARCHAR NOT NULL, w INT NOT NULL)",

        // Four members: a plain key, a two-column COALESCE key, a computed key with a pair residual,
        // a COALESCE key; the consumer projection reads padded columns.
        "CREATE VIEW star AS SELECT f.id, f.amt, COALESCE(d1.x, 'none') AS x, COALESCE(d2.y, -1) AS y, d3.z, " +
            "COALESCE(d4.hk, 1) AS hk " +
            "FROM f LEFT JOIN d1 ON f.a = d1.a " +
            "LEFT JOIN d2 ON COALESCE(f.p1, 'u') = d2.p1 AND COALESCE(f.p2, 'U') = d2.p2 " +
            "LEFT JOIN d3 ON UPPER(f.b) = d3.b AND f.amt >= d3.lo " +
            "LEFT JOIN d4 ON COALESCE(f.h, -1) = d4.h",

        // No consumer projection: the joined row itself.
        "CREATE VIEW whole AS SELECT * FROM f LEFT JOIN d1 ON f.a = d1.a LEFT JOIN d4 ON f.h = d4.h",

        // d5 keys on d1's column: a star of (d1, d4) under it, the d5 join, a star of (d2, d3) over it.
        "CREATE VIEW split AS SELECT f.id, d1.x, d4.hk, d5.w, d2.y, d3.z " +
            "FROM f LEFT JOIN d1 ON f.a = d1.a LEFT JOIN d4 ON f.h = d4.h " +
            "LEFT JOIN d5 ON d1.x = d5.x " +
            "LEFT JOIN d2 ON f.p1 = d2.p1 AND f.p2 = d2.p2 " +
            "LEFT JOIN d3 ON f.b = d3.b",

        // A filter between two joins ends the run: no star of three.
        "CREATE VIEW filt AS SELECT z.id, z.x, d2.y FROM " +
            "(SELECT f.id, f.p1, f.p2, d1.x FROM f LEFT JOIN d1 ON f.a = d1.a WHERE COALESCE(d1.x, '') <> 'two') z " +
            "LEFT JOIN d2 ON z.p1 = d2.p1 AND z.p2 = d2.p2",

        "CREATE VIEW agg AS SELECT x, y, COUNT(*) AS n, SUM(amt) AS s FROM star GROUP BY x, y",
    ];

    private static readonly string[] Views = ["star", "whole", "split", "filt", "agg"];

    private static readonly Dictionary<string, long> Counts = new(StringComparer.Ordinal)
    {
        ["f"] = 10_000_000, ["d1"] = 10, ["d2"] = 10, ["d3"] = 10, ["d4"] = 10, ["d5"] = 10,
    };

    private static readonly CompileOptions Options = new() { BroadcastMaxRows = 100, RelationRowCounts = Counts };

    private static T With<T>(bool star, Func<T> compile)
    {
        var prev = StarJoinMode.Enabled;
        StarJoinMode.Enabled = star;
        try
        {
            return compile();
        }
        finally
        {
            StarJoinMode.Enabled = prev;
        }
    }

    private static CompiledProgram Compile(bool star, bool blocked = false) =>
        With(star, () =>
        {
            using var mode = TraceBlocksMode.Use(join: blocked, freezeEntries: 8, blockEntries: 4);
            return SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance, Options);
        });

    private static ParallelCompiledProgram CompileParallel(bool star, int workers) =>
        With(star, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(
                Sql, new HashSet<string>(Views, StringComparer.Ordinal), workers, out var p, options: Options));
            return p!;
        });

    // One tick's pushes, as (table, row, weight). Fact rows with NULL keys and no dimension row, a
    // fact row inserted twice, fact deletes; dimension rows that come and go while the fact holds
    // rows (a key gaining and losing its only row, a key with two rows), NULL-keyed dimension rows.
    private static List<(string Table, object?[] Row, long Weight)> Tick(int tick)
    {
        var pushes = new List<(string, object?[], long)>();
        object?[] Fact(Random rng) =>
        [
            rng.Next(0, 25),
            rng.Next(5) == 0 ? null : rng.Next(0, 5),
            rng.Next(4) == 0 ? null : "abc"[rng.Next(3)].ToString(),
            rng.Next(4) == 0 ? null : "uv"[rng.Next(2)].ToString(),
            rng.Next(4) == 0 ? null : "UV"[rng.Next(2)].ToString(),
            rng.Next(4) == 0 ? null : rng.Next(-1, 4),
            (long)rng.Next(4),
        ];

        var rng = new Random(tick * 7919);
        for (var i = 0; i < 12; i++)
        {
            var row = Fact(rng);
            pushes.Add(("f", row, rng.Next(6) == 0 ? 2 : 1));
        }

        if (tick > 0)
        {
            var old = new Random((tick - 1) * 7919);
            for (var i = 0; i < 4; i++)
            {
                var row = Fact(old);
                pushes.Add(("f", row, -(old.Next(6) == 0 ? 2 : 1)));
            }
        }

        // Dimensions: a fixed core loaded at tick 0, then churn every tick.
        if (tick == 0)
        {
            pushes.Add(("d1", [1, "one"], 1));
            pushes.Add(("d1", [2, "two"], 1));
            pushes.Add(("d1", [2, "deux"], 1));
            pushes.Add(("d1", [null, "nul"], 1));
            pushes.Add(("d2", ["u", "U", 10], 1));
            pushes.Add(("d2", ["v", "V", 20], 1));
            pushes.Add(("d3", ["A", 1L, 100], 1));
            pushes.Add(("d3", ["A", 3L, 300], 1));
            pushes.Add(("d3", ["B", 0L, 200], 1));
            pushes.Add(("d3", [null, 0L, 999], 1));
            pushes.Add(("d4", [-1, 7], 1));
            pushes.Add(("d4", [1, 8], 1));
            pushes.Add(("d5", ["one", 11], 1));
            pushes.Add(("d5", ["deux", 22], 1));
        }

        switch (tick % 4)
        {
            case 1:
                pushes.Add(("d1", [3, "three"], 1));        // key 3 gains its only row
                pushes.Add(("d2", ["u", "V", 30], 1));
                pushes.Add(("d4", [2, 9], 1));
                pushes.Add(("d3", ["C", 2L, 400], 1));
                break;
            case 2:
                pushes.Add(("d1", [2, "deux"], -1));        // key 2 drops to one row
                pushes.Add(("d3", ["A", 1L, 100], -1));
                pushes.Add(("d5", ["three", 33], 1));
                break;
            case 3:
                pushes.Add(("d1", [3, "three"], -1));       // key 3 loses its only row
                pushes.Add(("d2", ["u", "V", 30], -1));
                pushes.Add(("d4", [2, 9], -1));
                pushes.Add(("d3", ["C", 2L, 400], -1));
                break;
            case 0 when tick > 0:
                pushes.Add(("d1", [2, "deux"], 1));
                pushes.Add(("d3", ["A", 1L, 100], 1));
                pushes.Add(("d5", ["three", 33], -1));
                pushes.Add(("d1", [4, "four"], tick % 8 == 0 ? 1 : 0));
                break;
        }

        return pushes.Where(p => p.Item3 != 0).Select(p => (p.Item1, p.Item2, p.Item3)).ToList();
    }

    private static void Push(Func<string, object> table, List<(string Table, object?[] Row, long Weight)> pushes)
    {
        foreach (var group in pushes.GroupBy(p => p.Table))
        {
            var rows = group.Select(p => (p.Row, p.Weight)).ToList();
            switch (table(group.Key))
            {
                case TableInput t: t.Push(rows); break;
                case ShardedTableInput s: s.Push(rows); break;
                default: throw new InvalidOperationException();
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

    private static string Digest(CompiledProgram p) => Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static string Digest(ParallelCompiledProgram p) => Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count));

    private static void Step(CompiledProgram p, int tick)
    {
        Push(p.Table, Tick(tick));
        p.Step();
    }

    private static int Stars(IEnumerable<IOperator> ops) => ops.Count(o => o is StarLeftJoinOp);

    // Every input pushed so far, at once, into a fresh chain: the batch answer.
    private static string Recompute(int ticks)
    {
        var all = Enumerable.Range(0, ticks).SelectMany(Tick).ToList();
        var fresh = Compile(star: false);
        Push(fresh.Table, all);
        fresh.Step();
        return Digest(fresh);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Star_equals_the_chain_and_a_recompute_on_every_tick(bool blocked)
    {
        var star = Compile(star: true, blocked);
        var chain = Compile(star: false, blocked);
        Assert.True(Stars(star.Circuit.Operators) >= 3, $"{Stars(star.Circuit.Operators)} star operators");
        Assert.Equal(0, Stars(chain.Circuit.Operators));
        for (var tick = 0; tick < 16; tick++)
        {
            Step(star, tick);
            Step(chain, tick);
            Assert.Equal(Digest(chain), Digest(star));
            if (tick % 5 == 4)
            {
                Assert.Equal(Recompute(tick + 1), Digest(star));
            }
        }

        var ops = star.Circuit.Operators.OfType<StarLeftJoinOp>().ToList();
        Assert.Contains(ops, o => o.AffectedRows > 0);
    }

    // star: one run of four; whole: two; split: (d1, d4) under d5 and (d2, d3) over it; filt: none.
    [Fact]
    public void The_runs_form_where_they_should()
    {
        int[] Members(string view) => With(true, () =>
        {
            var p = SqlProgram.Compile(Sql, new HashSet<string>([view], StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance, Options);
            return p.Circuit.Operators.OfType<StarLeftJoinOp>().Select(o => o.MemberCount).Order().ToArray();
        });

        Assert.Equal([4], Members("star"));
        Assert.Equal([2], Members("whole"));
        Assert.Equal([2, 2], Members("split"));
        Assert.Empty(Members("filt"));
    }

    // Without sizes nothing is small: no star.
    [Fact]
    public void No_star_without_sizes()
    {
        var p = With(true, () => SqlProgram.Compile(Sql, new HashSet<string>(["star"], StringComparer.Ordinal)));
        Assert.Equal(0, Stars(p.Circuit.Operators));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Parallel_star_equals_serial_chain(int workers)
    {
        var serial = Compile(star: false);
        using var parallel = CompileParallel(star: true, workers);
        Assert.All(parallel.Circuit.Replicas, r => Assert.True(Stars(r.Operators) >= 3));
        for (var tick = 0; tick < 12; tick++)
        {
            Step(serial, tick);
            Push(parallel.Table, Tick(tick));
            parallel.Step();
            Assert.Equal(Digest(serial), Digest(parallel));
        }
    }

    // A restored star has no directory: its first dimension change scans the fact's keys.
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task A_star_survives_a_checkpoint(bool writeBlocked, bool readBlocked)
    {
        RunCheckpoint.Verify = true;
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile(star: true, writeBlocked);
        for (var tick = 0; tick < 7; tick++)
        {
            Step(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var restored = Compile(star: true, readBlocked);
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.All(restored.Circuit.Operators.OfType<StarLeftJoinOp>(), o => Assert.False(o.HasDirectory));

        var twin = Compile(star: false, readBlocked);
        for (var tick = 0; tick < 7; tick++)
        {
            Step(twin, tick);
        }

        for (var tick = 7; tick < 15; tick++)
        {
            Step(twin, tick);
            Step(restored, tick);
            Assert.Equal(Digest(twin), Digest(restored));
        }

        Assert.Contains(restored.Circuit.Operators.OfType<StarLeftJoinOp>(), o => o.KeyScans > 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Past_the_directory_cap_a_change_scans_the_keys(bool blocked)
    {
        var prev = StarLeftJoinOp.DirectoryCap;
        StarLeftJoinOp.DirectoryCap = 3;
        try
        {
            var star = Compile(star: true, blocked);
            var chain = Compile(star: false, blocked);
            for (var tick = 0; tick < 10; tick++)
            {
                Step(star, tick);
                Step(chain, tick);
                Assert.Equal(Digest(chain), Digest(star));
            }

            Assert.Contains(star.Circuit.Operators.OfType<StarLeftJoinOp>(), o => o.KeyScans > 0);
        }
        finally
        {
            StarLeftJoinOp.DirectoryCap = prev;
        }
    }

    // The release planner finds the streams an operator reads by walking its fields and closures: a
    // star reaches its fact, each member's right side, and its output, serial and in every replica.
    [Fact]
    public void Each_star_reaches_only_its_own_streams()
    {
        void Check(IReadOnlyList<IOperator> ops)
        {
            var readers = StreamRelease.ScanReaders(ops);
            var stars = Enumerable.Range(0, ops.Count).Where(i => ops[i] is StarLeftJoinOp).ToList();
            Assert.NotEmpty(stars);
            foreach (var i in stars)
            {
                Assert.Equal(((StarLeftJoinOp)ops[i]).MemberCount + 2, readers.ByStream.Count(kv => kv.Value.Contains(i)));
            }
        }

        Check(Compile(star: true).Circuit.Operators);
        using var parallel = CompileParallel(star: true, 4);
        foreach (var replica in parallel.Circuit.Replicas)
        {
            Check(replica.Operators);
        }
    }
}
