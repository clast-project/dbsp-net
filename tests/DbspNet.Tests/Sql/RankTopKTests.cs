// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A rank that a filter bounds (rn = 1, rn <= k) compiles as a TOP-K (docs/design-bounded-memory.md
// §5.51), including when the filter is in a later CTE or above an INNER JOIN. Required: the rewritten
// program equals the unrewritten one and a batch recompute of the unoptimized plan on every tick,
// parallel equals serial, and the rewrite forms exactly where the filter reaches the rank.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Core.Circuit;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Sql;

public class RankTopKTests
{
    private static readonly string[] Tables =
    [
        "CREATE TABLE t (id INT NOT NULL, p INT, o INT, v INT)",
        "CREATE TABLE d (k INT NOT NULL, name VARCHAR)",
    ];

    // name → (query, ranks with the rewrite, ranks without, batch-checkable)
    private static readonly (string Name, string Query, int RanksOn, int RanksOff, bool Batch)[] Views =
    [
        // fact_engagement_status_monthly_snapshot's shape: the rank in one CTE, rn = 1 in the next, an
        // aggregate in a third.
        ("cte",
            "WITH latest AS (SELECT id, p, o, v, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC) AS rn " +
            "FROM (SELECT id, p, o, v FROM t) AS s WHERE v IS NOT NULL), " +
            "deduped AS (SELECT id, p, o, v FROM latest WHERE rn = 1), " +
            "agg AS (SELECT p, COUNT(*) AS n, SUM(v) AS s FROM deduped GROUP BY p) " +
            "SELECT p, n, s FROM agg WHERE n > 0", 0, 1, true),

        // int_engagement_assessment_key_latest's inner query: rn = 1 in the WHERE after an INNER JOIN.
        ("joined",
            "SELECT d.name, x.id, x.o FROM (SELECT id, p, o, " +
            "ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC, id DESC NULLS LAST) AS rn FROM t) AS x " +
            "INNER JOIN d ON x.p = d.k WHERE x.rn = 1", 0, 1, true),

        // ... and the whole view, with its outer arm_rn = 1 (the resolver's own TOP-K; no batch oracle).
        ("joinedtop",
            "SELECT name, id FROM (SELECT d.name, x.id, " +
            "ROW_NUMBER() OVER (PARTITION BY d.name ORDER BY x.o DESC, x.id DESC NULLS LAST) AS arm_rn " +
            "FROM (SELECT id, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC) AS rn FROM t) AS x " +
            "INNER JOIN d ON x.p = d.k WHERE x.rn = 1) AS latest WHERE arm_rn = 1", 0, 1, false),

        // rn read after rn = 1, RANK with ties.
        ("rnsel",
            "WITH r AS (SELECT id, p, o, RANK() OVER (PARTITION BY p ORDER BY o) AS rn FROM t) " +
            "SELECT id, p, rn, rn + 10 AS r10 FROM r WHERE rn = 1", 0, 1, true),

        // rn <= 2 with rn read, and a residual on the input: rule (2), a rank over the TOP-K.
        ("le2",
            "WITH r AS (SELECT id, p, o, v, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC) AS rn FROM t) " +
            "SELECT id, p, v, rn FROM r WHERE rn <= 2 AND v > 0", 1, 1, true),

        // DENSE_RANK, reversed bound (2 >= rn), rn read.
        ("dense",
            "WITH r AS (SELECT id, p, o, DENSE_RANK() OVER (PARTITION BY p ORDER BY o NULLS LAST) AS rn FROM t) " +
            "SELECT id, o, rn FROM r WHERE 2 >= rn", 1, 1, true),

        // A global rank (no PARTITION BY), rn < 3 → limit 2.
        ("global",
            "WITH r AS (SELECT id, o, ROW_NUMBER() OVER (ORDER BY o DESC NULLS LAST) AS rn FROM t) " +
            "SELECT id, o, rn FROM r WHERE rn < 3", 1, 1, true),

        // A global rank, rn = 1.
        ("global1",
            "SELECT id, o FROM (SELECT id, o, RANK() OVER (ORDER BY o) AS rn FROM t) AS g " +
            "INNER JOIN d ON g.id = d.k WHERE g.rn = 1", 0, 1, true),

        // The CTE is scanned twice and the second scan reads every rank: no inlining, the rank stays.
        ("twice",
            "WITH r AS (SELECT id, p, ROW_NUMBER() OVER (PARTITION BY p ORDER BY id) AS rn FROM t) " +
            "SELECT a.id, b.id AS bid, b.rn FROM r AS a INNER JOIN r AS b ON a.p = b.p WHERE a.rn = 1", 1, 1, true),

        // The filter is above a LEFT JOIN on the ranked, NULL-padded side: not pushed, not rewritten.
        ("leftj",
            "SELECT d.k, x.id FROM d LEFT JOIN (SELECT id, p, " +
            "ROW_NUMBER() OVER (PARTITION BY p ORDER BY id) AS rn FROM t) AS x ON d.k = x.p WHERE x.rn = 1", 1, 1, true),

        // rn = 2 is not a TOP-K.
        ("eq2",
            "WITH r AS (SELECT id, p, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rn FROM t) " +
            "SELECT id, p FROM r WHERE rn = 2", 1, 1, true),
    ];

    private static string[] Program(IEnumerable<string>? names = null)
    {
        var keep = names?.ToHashSet(StringComparer.Ordinal);
        return [.. Tables, .. Views.Where(v => keep?.Contains(v.Name) != false).Select(v => $"CREATE VIEW {v.Name} AS {v.Query}")];
    }

    private static T With<T>(bool on, Func<T> compile)
    {
        var prev = RankTopKMode.Enabled;
        RankTopKMode.Enabled = on;
        try
        {
            return compile();
        }
        finally
        {
            RankTopKMode.Enabled = prev;
        }
    }

    private static HashSet<string> Names(IEnumerable<string>? names) =>
        new(names ?? Views.Select(v => v.Name), StringComparer.Ordinal);

    private static CompiledProgram Serial(bool on, string[]? names = null) =>
        With(on, () => SqlProgram.Compile(Program(names), Names(names), ArrowSqlSnapshotCodecs.Instance));

    private static ParallelCompiledProgram Parallel(int workers, bool on = true)
    {
        return With(on, () =>
        {
            Assert.True(SqlProgram.TryCompileParallel(Program(), Names(null), workers, out var p, ArrowSqlSnapshotCodecs.Instance));
            return p!;
        });
    }

    // p in 0..3 or NULL, o in 0..4 or NULL (ties, broken by the whole row), v in -1..3 or NULL; weight-2
    // rows; deletes of earlier ticks' rows, so a partition's first row leaves and the next one takes over.
    private static List<(string Table, object?[] Row, long Weight)> Tick(int tick)
    {
        var pushes = new List<(string, object?[], long)>();
        object?[] Fact(Random rng) =>
        [
            rng.Next(0, 12),
            rng.Next(7) == 0 ? null : rng.Next(0, 4),
            rng.Next(8) == 0 ? null : rng.Next(0, 5),
            rng.Next(6) == 0 ? null : rng.Next(-1, 4),
        ];

        var rng = new Random(tick * 7919 + 3);
        for (var i = 0; i < 14; i++)
        {
            pushes.Add(("t", Fact(rng), rng.Next(5) == 0 ? 2 : 1));
        }

        if (tick > 0)
        {
            // Retract the first six rows of the previous tick, at the weights they went in with.
            var old = new Random((tick - 1) * 7919 + 3);
            for (var i = 0; i < 6; i++)
            {
                var row = Fact(old);
                pushes.Add(("t", row, old.Next(5) == 0 ? -2 : -1));
            }
        }

        if (tick == 0)
        {
            pushes.Add(("d", [0, "zero"], 1));
            pushes.Add(("d", [1, "one"], 1));
            pushes.Add(("d", [2, "two"], 1));
            pushes.Add(("d", [5, "five"], 1));
        }

        if (tick == 4)
        {
            pushes.Add(("d", [3, "two"], 1)); // a second key with the same name (joinedtop's partition)
            pushes.Add(("d", [1, "one"], -1));
        }

        if (tick == 8)
        {
            pushes.Add(("d", [1, "uno"], 1));
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

    private static string Rows(IEnumerable<KeyValuePair<StructuralRow, Z64>> rows, int width)
    {
        var d = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in rows)
        {
            var key = string.Join("|", Enumerable.Range(0, width).Select(i => kv.Key[i] is { } x ? FormattableString.Invariant($"{x}") : "<null>"));
            d[key] = d.GetValueOrDefault(key) + kv.Value.Value;
        }

        return string.Join("; ", d.Where(e => e.Value != 0).Select(e => e.Key + "#" + e.Value).Order(StringComparer.Ordinal));
    }

    private static string Digest(Func<string, (ZSet<StructuralRow, Z64> View, int Width)> output, IEnumerable<string> names) =>
        string.Join(" / ", names.Select(v =>
        {
            var (view, width) = output(v);
            return v + ": " + Rows(view, width);
        }));

    private static string Digest(CompiledProgram p, IEnumerable<string>? names = null) =>
        Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count), names ?? Views.Select(v => v.Name));

    private static string Digest(ParallelCompiledProgram p) =>
        Digest(v => (p.Outputs[v].CurrentView, p.Outputs[v].Schema.Count), Views.Select(v => v.Name));

    // The batch oracle: each view's query resolved on its own, NOT optimized, evaluated over the inputs
    // integrated so far.
    private static string Batch(IEnumerable<(string Table, object?[] Row, long Weight)> pushed, IEnumerable<string> names)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var ddl in Tables)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var name in new[] { "t", "d" })
        {
            var b = new ZSetBuilder<StructuralRow, Z64>();
            foreach (var (table, row, w) in pushed)
            {
                if (table == name)
                {
                    b.Add(new StructuralRow((object?[])row.Clone()), new Z64(w));
                }
            }

            tables[name] = b.Build();
        }

        return string.Join(" / ", names.Select(n =>
        {
            var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Views.Single(v => v.Name == n).Query))).Query;
            var result = BatchPlanEvaluator.Evaluate(plan, new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>()));
            return n + ": " + Rows(result, plan.Schema.Count);
        }));
    }

    private static int Count(RootCircuit circuit, string view, string prefix) =>
        circuit.Operators.Count(o => o.GetType().Name.StartsWith(prefix, StringComparison.Ordinal));

    [Fact]
    public void Rewritten_equals_unrewritten_and_a_batch_recompute_on_every_tick()
    {
        var on = Serial(on: true);
        var off = Serial(on: false);
        var batchViews = Views.Where(v => v.Batch).Select(v => v.Name).ToList();
        var pushed = new List<(string, object?[], long)>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var tick = 0; tick < 14; tick++)
        {
            var pushes = Tick(tick);
            pushed.AddRange(pushes);
            Push(on.Table, pushes);
            on.Step();
            Push(off.Table, pushes);
            off.Step();
            Assert.Equal(Digest(off), Digest(on));
            Assert.Equal(Batch(pushed, batchViews), Digest(on, batchViews));
            foreach (var v in Views)
            {
                if (on.Outputs[v.Name].CurrentView.Count > 0)
                {
                    seen.Add(v.Name);
                }
            }
        }

        // No view is vacuously equal.
        Assert.Equal(Views.Length, seen.Count);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void Parallel_equals_serial_on_every_tick(int workers)
    {
        var serial = Serial(on: false);
        using var parallel = Parallel(workers);
        for (var tick = 0; tick < 14; tick++)
        {
            Push(serial.Table, Tick(tick));
            serial.Step();
            Push(parallel.Table, Tick(tick));
            parallel.Step();
            Assert.Equal(Digest(serial), Digest(parallel));
        }
    }

    // Where the filter reaches the rank, the rank is gone (or ranks only the TOP-K's rows).
    [Fact]
    public void The_rewrite_forms_where_the_filter_reaches_the_rank()
    {
        foreach (var v in Views)
        {
            var on = Serial(on: true, names: [v.Name]);
            var off = Serial(on: false, names: [v.Name]);
            Assert.True(
                (v.RanksOn, v.RanksOff) == (Count(on.Circuit, v.Name, "PartitionedRankOp"), Count(off.Circuit, v.Name, "PartitionedRankOp")),
                $"{v.Name}: ranks on/off = {Count(on.Circuit, v.Name, "PartitionedRankOp")}/{Count(off.Circuit, v.Name, "PartitionedRankOp")}, " +
                $"expected {v.RanksOn}/{v.RanksOff}");
            var rewritten = v.RanksOn < v.RanksOff || v.Name is "le2" or "dense" or "global";
            var topKsOff = Count(off.Circuit, v.Name, "PartitionedTopK");
            Assert.Equal(topKsOff + (rewritten ? 1 : 0), Count(on.Circuit, v.Name, "PartitionedTopK"));
        }
    }

    // The CTE pair compiles with no rank at W > 1 as well (the parallel program compiles CTE bodies
    // apart from the view's plan).
    [Fact]
    public void Parallel_cte_view_has_no_rank()
    {
        using var p = Parallel(4);
        var ops = p.Circuit.Replicas[0].Operators;
        var withRank = Parallel(4, on: false);
        Assert.True(
            ops.Count(o => o.GetType().Name.StartsWith("PartitionedRankOp", StringComparison.Ordinal)
                         || o.GetType().Name.StartsWith("RangeRankOp", StringComparison.Ordinal))
            < withRank.Circuit.Replicas[0].Operators.Count(o => o.GetType().Name.StartsWith("PartitionedRankOp", StringComparison.Ordinal)
                         || o.GetType().Name.StartsWith("RangeRankOp", StringComparison.Ordinal)));
        withRank.Dispose();
    }
}
