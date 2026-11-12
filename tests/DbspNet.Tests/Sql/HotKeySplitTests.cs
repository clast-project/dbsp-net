// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A hashed LEFT join whose left key has a hot default value splits on it
// (docs/design-bounded-memory.md §5.49): rows a lookup padded all share LOWER(COALESCE(x, 'NOT
// APPLICABLE')) and would all hash to one worker. Required: parallel equals serial on every tick while
// the right side's hot group comes and goes; the split forms only where the key has such a value; a
// checkpoint re-sharded to another worker count continues right.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using Xunit;

namespace DbspNet.Tests.Sql;

public class HotKeySplitTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE f (id INT NOT NULL, a INT, amt BIGINT NOT NULL)",
        "CREATE TABLE lk (a INT NOT NULL, cp VARCHAR)",
        "CREATE TABLE prof (cp VARCHAR, p INT NOT NULL)",

        // Every f row without a lookup row (and every lookup row with a NULL cp) gets 'not applicable'.
        "CREATE VIEW hot AS SELECT x.id, x.cp, prof.p FROM " +
            "(SELECT f.id, LOWER(COALESCE(lk.cp, 'NOT APPLICABLE')) AS cp FROM f LEFT JOIN lk ON f.a = lk.a) x " +
            "LEFT JOIN prof ON x.cp = prof.cp",

        // With a pair residual, and an aggregate over it.
        "CREATE VIEW resid AS SELECT x.id, prof.p FROM " +
            "(SELECT f.id, f.amt, COALESCE(lk.cp, 'none') AS cp FROM f LEFT JOIN lk ON f.a = lk.a) x " +
            "LEFT JOIN prof ON x.cp = prof.cp AND x.amt > prof.p",
        "CREATE VIEW agg AS SELECT cp, COUNT(*) AS n, SUM(p) AS s FROM hot GROUP BY cp",

        // A hot default, and a NULL key on some rows (amt = 0): those bypass the split join, padded.
        "CREATE VIEW nullk AS SELECT x.id, x.cp, prof.p FROM " +
            "(SELECT f.id, CASE WHEN f.amt = 0 THEN NULL ELSE LOWER(COALESCE(lk.cp, 'NOT APPLICABLE')) END AS cp " +
            "FROM f LEFT JOIN lk ON f.a = lk.a) x LEFT JOIN prof ON x.cp = prof.cp",

        // The key computed in a CTE (the parallel program compile does not inline CTEs).
        "CREATE VIEW cte AS WITH bf AS (SELECT f.id, LOWER(COALESCE(lk.cp, 'NOT APPLICABLE')) AS cp FROM f LEFT JOIN lk ON f.a = lk.a) " +
            "SELECT bf.id, bf.cp, prof.p FROM bf LEFT JOIN prof ON bf.cp = prof.cp",

        // A key that is NULL when its source is: no hot value, no split.
        "CREATE VIEW plain AS SELECT x.id, prof.p FROM " +
            "(SELECT f.id, LOWER(lk.cp) AS cp FROM f LEFT JOIN lk ON f.a = lk.a) x LEFT JOIN prof ON x.cp = prof.cp",
    ];

    private static readonly string[] Views = ["hot", "resid", "agg", "nullk", "cte", "plain"];

    private static T With<T>(bool split, Func<T> compile)
    {
        var prev = HotKeySplitMode.Enabled;
        HotKeySplitMode.Enabled = split;
        try
        {
            return compile();
        }
        finally
        {
            HotKeySplitMode.Enabled = prev;
        }
    }

    private static CompiledProgram Serial() =>
        SqlProgram.Compile(Sql, new HashSet<string>(Views, StringComparer.Ordinal), ArrowSqlSnapshotCodecs.Instance);

    private static ParallelCompiledProgram Parallel(int workers, bool split = true, string[]? views = null) =>
        With(split, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(
                Sql, new HashSet<string>(views ?? Views, StringComparer.Ordinal), workers, out var p, ArrowSqlSnapshotCodecs.Instance));
            return p!;
        });

    // Most f rows miss the lookup (a in 0..9, lookup rows for a in 0..2); prof's 'not applicable' and
    // 'none' rows come and go, so the hot rows gain and lose their match; deletes; NULL a.
    private static List<(string Table, object?[] Row, long Weight)> Tick(int tick)
    {
        var pushes = new List<(string, object?[], long)>();
        object?[] Fact(Random rng) => [rng.Next(0, 40), rng.Next(6) == 0 ? null : rng.Next(0, 10), (long)rng.Next(5)];
        var rng = new Random(tick * 104729);
        for (var i = 0; i < 30; i++)
        {
            pushes.Add(("f", Fact(rng), 1));
        }

        if (tick > 0)
        {
            var old = new Random((tick - 1) * 104729);
            for (var i = 0; i < 8; i++)
            {
                pushes.Add(("f", Fact(old), -1));
            }
        }

        if (tick == 0)
        {
            pushes.Add(("lk", [0, "/C/X"], 1));
            pushes.Add(("lk", [1, null], 1));
            pushes.Add(("lk", [2, "Not Applicable"], 1));
            pushes.Add(("prof", ["/c/x", 1], 1));
            pushes.Add(("prof", ["/C/X", 2], 1));
            pushes.Add(("prof", [null, 9], 1));
        }

        switch (tick % 3)
        {
            case 1:
                pushes.Add(("prof", ["not applicable", 3], 1));
                pushes.Add(("prof", ["none", 2], 1));
                pushes.Add(("lk", [3, "/c/y"], 1));
                break;
            case 2:
                pushes.Add(("prof", ["not applicable", 4], 1));
                pushes.Add(("prof", ["none", 2], -1));
                break;
            case 0 when tick > 0:
                pushes.Add(("prof", ["not applicable", 3], -1));
                pushes.Add(("prof", ["not applicable", 4], -1));
                pushes.Add(("lk", [3, "/c/y"], -1));
                break;
        }

        return pushes.Select(p => (p.Item1, p.Item2, p.Item3)).ToList();
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

    private static string Digest(Func<string, (ZSet<StructuralRow, Z64> View, int Width)> output, string[]? views = null) =>
        string.Join(" / ", (views ?? Views).Select(v =>
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

    private static int LeftJoins(ParallelCompiledProgram p) =>
        p.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.StartsWith("IncrementalLeftJoinOp", StringComparison.Ordinal));

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Split_parallel_equals_serial_on_every_tick(int workers)
    {
        var serial = Serial();
        using var parallel = Parallel(workers);
        for (var tick = 0; tick < 12; tick++)
        {
            Push(serial.Table, Tick(tick));
            serial.Step();
            Push(parallel.Table, Tick(tick));
            parallel.Step();
            Assert.Equal(Digest(serial), Digest(parallel));
        }
    }

    // hot, resid: split (one more join each); plain: not.
    [Theory]
    [InlineData("hot", 1)]
    [InlineData("resid", 1)]
    [InlineData("nullk", 1)]
    [InlineData("cte", 1)]
    [InlineData("plain", 0)]
    public void The_split_forms_where_the_key_has_a_hot_value(string view, int extra)
    {
        using var split = Parallel(4, split: true, views: [view]);
        using var unsplit = Parallel(4, split: false, views: [view]);
        Assert.Equal(LeftJoins(unsplit) + extra, LeftJoins(split));
    }

    // The split is right for any value; it only helps for the one the padded rows share. The key is
    // LOWER(COALESCE(lk.cp, 'NOT APPLICABLE')), so that value is 'not applicable', and the residual
    // view's COALESCE(lk.cp, 'none') is 'none'.
    [Theory]
    [InlineData("hot", "not applicable")]
    [InlineData("resid", "none")]
    public void The_hot_value_is_what_a_padded_row_gets(string view, string value)
    {
        HotKeySplitMode.LastHotKey = null;
        using var p = Parallel(4, views: [view]);
        Assert.Equal(value, HotKeySplitMode.LastHotKey?[0]?.ToString());
    }

    // Serial compiles never split (nothing is exchanged).
    [Fact]
    public void No_split_at_one_worker()
    {
        var before = Interlocked.Read(ref HotKeySplitMode.Built);
        _ = With(true, Serial);
        Assert.Equal(before, Interlocked.Read(ref HotKeySplitMode.Built));
    }

    [Theory]
    [InlineData(4, 4)]
    [InlineData(4, 3)]
    public async Task A_split_join_survives_a_resharded_checkpoint(int from, int to)
    {
        var fs = new InMemoryTableFileSystem();
        var serial = Serial();
        using (var producer = Parallel(from))
        {
            for (var tick = 0; tick < 6; tick++)
            {
                Push(serial.Table, Tick(tick));
                serial.Step();
                Push(producer.Table, Tick(tick));
                producer.Step();
            }

            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var restored = Parallel(to);
        if (from != to)
        {
            Assert.True(await ParallelReshard.RewriteAsync(fs, restored) > 0);
        }

        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);
        for (var tick = 6; tick < 12; tick++)
        {
            Push(serial.Table, Tick(tick));
            serial.Step();
            Push(restored.Table, Tick(tick));
            restored.Step();
            Assert.Equal(Digest(serial), Digest(restored));
        }
    }
}
