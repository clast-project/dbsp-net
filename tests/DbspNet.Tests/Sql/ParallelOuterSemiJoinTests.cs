// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Parallel ≡ serial for the joins the structural parallel program compile used to
/// refuse: equi LEFT / RIGHT / FULL outer joins, outer joins with a residual (the
/// anti-join lowering keyed on the whole preserved row), and semi / anti joins
/// (EXISTS / IN / NOT EXISTS / NOT IN). The same program is compiled serially
/// (<see cref="SqlProgram.Compile"/>) and at W = 1/2/4
/// (<see cref="SqlProgram.TryCompileParallel"/>), driven with the same random
/// <b>arbitrary signed</b> deltas (weights ±1/±2, including deletes of rows never
/// inserted, so accumulated weights go negative), and every output view's integrated
/// contents must match after every tick.
/// <para>
/// Keys come from tiny domains with NULLs, so preserved-side NULL keys, duplicate
/// rows and many-to-many matches all occur. Several views aggregate a join's output
/// on its key, which exercises the partition each join records: a dishonest
/// partition would let the aggregate skip its shuffle and split a group.
/// </para>
/// </summary>
public class ParallelOuterSemiJoinTests
{
    private static readonly string[] Tables =
    [
        "CREATE TABLE a (k1 INT, k2 VARCHAR, v BIGINT, ts INT)",
        "CREATE TABLE b (k1 INT, k2 VARCHAR, w BIGINT, eff INT, en INT)",
        // NOT NULL operands, deliberately of different types (INT probe, BIGINT
        // subquery column) so the IN / NOT IN key takes the resolver's common-type cast.
        "CREATE TABLE c (x INT NOT NULL, tag VARCHAR NOT NULL)",
        "CREATE TABLE d (y BIGINT NOT NULL, g INT NOT NULL)",
    ];

    private static readonly (string Name, string Sql)[] Views =
    [
        // Equi outer joins: single key, multi-column key, all three join types.
        ("lj", "SELECT a.k1, a.k2, a.v, b.w FROM a LEFT JOIN b ON a.k1 = b.k1"),
        ("lj2", "SELECT a.k1, a.k2, a.v, b.w FROM a LEFT JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2"),
        ("rj2", "SELECT a.v, b.k1, b.k2, b.w FROM a RIGHT JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2"),
        ("fj", "SELECT a.k1, a.v, b.k1 AS bk, b.w FROM a FULL JOIN b ON a.k1 = b.k1"),
        // INT = BIGINT equi-key: both sides must hash the coerced key alike.
        ("lj_mixed", "SELECT c.x, c.tag, d.g FROM c LEFT JOIN d ON c.x = d.y"),
        ("fj_mixed", "SELECT c.x, d.y, d.g FROM c FULL JOIN d ON c.x = d.y"),

        // Outer joins with a residual: the temporal-SCD BETWEEN shape on LEFT, plus
        // RIGHT and FULL so both preserved-side anti-joins run.
        ("scd", "SELECT a.k1, a.ts, a.v, b.w FROM a LEFT JOIN b ON a.k1 = b.k1 AND a.ts BETWEEN b.eff AND b.en"),
        ("rres", "SELECT a.v, b.k1, b.w FROM a RIGHT JOIN b ON a.k1 = b.k1 AND a.v < b.w"),
        ("fres", "SELECT a.k1, a.v, b.k2, b.w FROM a FULL JOIN b ON a.k2 = b.k2 AND a.ts >= b.eff"),

        // An outer join feeding an aggregate, grouped on the join key (LEFT: the
        // recorded left-key partition lets the aggregate skip its shuffle; RIGHT: the
        // offset right-key partition; FULL: unpartitioned, so it must re-shuffle).
        ("lj_agg", "SELECT a.k1, COUNT(*) AS n, SUM(b.w) AS sw FROM a LEFT JOIN b ON a.k1 = b.k1 GROUP BY a.k1"),
        ("rj_agg", "SELECT b.k1, b.k2, COUNT(*) AS n, SUM(a.v) AS sv FROM a RIGHT JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2 GROUP BY b.k1, b.k2"),
        ("fj_agg", "SELECT a.k1, COUNT(*) AS n FROM a FULL JOIN b ON a.k1 = b.k1 GROUP BY a.k1"),
        ("scd_agg", "SELECT a.k1, COUNT(*) AS n, SUM(b.w) AS sw FROM a LEFT JOIN b ON a.k1 = b.k1 AND a.ts BETWEEN b.eff AND b.en GROUP BY a.k1"),

        // The views above reach their aggregate through a column-pruning projection,
        // which drops partition tracking. These use every join column, so the
        // aggregate reads the join output DIRECTLY and consults the partition the join
        // recorded. Grouping by a.k1 makes a dishonest claim bite: the NULL a.k1 group
        // (LEFT: NULL-key bypass rows; RIGHT: unmatched right rows) would split.
        ("lj_direct", "SELECT a.k1, COUNT(*) AS n, SUM(b.w) AS sw, SUM(b.k1) AS sk FROM a LEFT JOIN b ON a.k1 = b.k1 GROUP BY a.k1"),
        ("rj_direct", "SELECT a.k1, COUNT(*) AS n, SUM(a.v) AS sv, SUM(b.k1) AS sk FROM a RIGHT JOIN b ON a.k1 = b.k1 GROUP BY a.k1"),
        ("fj_direct", "SELECT a.k1, COUNT(*) AS n, SUM(a.v) AS sv, SUM(b.k1) AS sk FROM a FULL JOIN b ON a.k1 = b.k1 GROUP BY a.k1"),

        // Semi / anti joins: uncorrelated and correlated, EXISTS and IN.
        ("ex", "SELECT a.k1, a.v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k1 = a.k1)"),
        ("nex", "SELECT a.k1, a.k2, a.v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k1 = a.k1 AND b.k2 = a.k2)"),
        ("inq", "SELECT a.k1, a.v FROM a WHERE a.k1 IN (SELECT k1 FROM b)"),
        ("inq_corr", "SELECT a.k1, a.k2 FROM a WHERE a.k2 IN (SELECT k2 FROM b WHERE b.k1 = a.k1)"),
        ("notin", "SELECT c.x, c.tag FROM c WHERE c.x NOT IN (SELECT y FROM d)"),
        ("notin_corr", "SELECT c.x, c.tag FROM c WHERE c.x NOT IN (SELECT y FROM d WHERE d.g = c.x)"),
        ("ex_agg", "SELECT a.k1, COUNT(*) AS n FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k1 = a.k1) GROUP BY a.k1"),
        // Semi / anti join read directly by an aggregate (every outer column used).
        ("ex_direct", "SELECT a.k1, COUNT(*) AS n, SUM(a.v) AS sv, MIN(a.k2) AS mk, SUM(a.ts) AS st FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k1 = a.k1) GROUP BY a.k1"),
        ("nex_direct", "SELECT a.k1, COUNT(*) AS n, SUM(a.v) AS sv, MIN(a.k2) AS mk, SUM(a.ts) AS st FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k1 = a.k1) GROUP BY a.k1"),
    ];

    private static string[] Program(params (string Name, string Sql)[] views) =>
        [.. Tables, .. views.Select(v => $"CREATE VIEW {v.Name} AS {v.Sql}")];

    private static Dictionary<string, long> Materialize(ZSet<StructuralRow, Z64> zset, int width)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var kv in zset)
        {
            var cells = new string[width];
            for (var i = 0; i < width; i++)
            {
                cells[i] = kv.Key[i]?.ToString() ?? "<null>";
            }

            var key = string.Join("|", cells);
            map[key] = map.GetValueOrDefault(key) + kv.Value.Value;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static object? Pick(Random rng, params object?[] domain) => domain[rng.Next(domain.Length)];

    // One tick's deltas per table. Weights are ±1 / ±2 with no regard for what was
    // inserted before, so deletes of never-inserted rows (negative accumulations) occur.
    private static Dictionary<string, List<(object?[] Values, long Weight)>> RandomTick(Random rng)
    {
        long W() => rng.Next(4) switch { 0 => -2, 1 => -1, 2 => 1, _ => 2 };
        List<(object?[], long)> Rows(int max, Func<object?[]> row)
        {
            var list = new List<(object?[], long)>();
            for (var n = rng.Next(max + 1); n > 0; n--)
            {
                list.Add((row(), W()));
            }

            return list;
        }

        return new Dictionary<string, List<(object?[] Values, long Weight)>>(StringComparer.Ordinal)
        {
            ["a"] = Rows(6, () => [Pick(rng, null, 1, 2, 3), Pick(rng, null, "x", "y"), Pick(rng, 1L, 2L, 5L), Pick(rng, null, 0, 1, 2, 3)]),
            ["b"] = Rows(6, () => [Pick(rng, null, 1, 2, 3), Pick(rng, null, "x", "y"), Pick(rng, 1L, 3L, 6L), Pick(rng, null, 0, 1), Pick(rng, 1, 2, 3)]),
            ["c"] = Rows(4, () => [Pick(rng, 1, 2, 3, 4)!, Pick(rng, "p", "q")!]),
            ["d"] = Rows(4, () => [Pick(rng, 1L, 2L, 3L)!, Pick(rng, 1, 2, 3, 4)!]),
        };
    }

    private static void AssertParallelMatchesSerial(string[] program, string[] outputs, int workers, int seed, int ticks = 25)
    {
        var outputSet = new HashSet<string>(outputs, StringComparer.Ordinal);
        var serial = SqlProgram.Compile(program, outputSet);
        Assert.True(
            SqlProgram.TryCompileParallel(program, outputSet, workers, out var parallel),
            "parallel program compile refused");

        // Non-vacuity: every view must be non-empty at some tick, and (see below) hold
        // a negative weight at some tick (the arbitrary-signed case), across the run.
        var sawRows = new HashSet<string>(StringComparer.Ordinal);
        var sawNegative = new HashSet<string>(StringComparer.Ordinal);
        using (parallel)
        {
            var rng = new Random(seed);
            for (var t = 0; t < ticks; t++)
            {
                foreach (var (table, deltas) in RandomTick(rng))
                {
                    serial.Table(table).Push(deltas);
                    parallel!.Table(table).Push(deltas);
                }

                serial.Step();
                parallel!.Step();

                foreach (var name in outputs)
                {
                    var s = serial.Outputs[name];
                    var p = parallel.Outputs[name];
                    var expected = Materialize(s.CurrentView, s.Schema.Count);
                    var actual = Materialize(p.CurrentView, p.Schema.Count);
                    if (expected.Count > 0)
                    {
                        sawRows.Add(name);
                    }

                    if (expected.Values.Any(w => w < 0))
                    {
                        sawNegative.Add(name);
                    }

                    if (!expected.OrderBy(kv => kv.Key).SequenceEqual(actual.OrderBy(kv => kv.Key)))
                    {
                        Assert.Fail(
                            $"view {name}, W={workers}, seed={seed}, tick {t}: parallel != serial\n"
                            + $"serial:   {string.Join(", ", expected.OrderBy(kv => kv.Key).Select(kv => $"[{kv.Key}]x{kv.Value}"))}\n"
                            + $"parallel: {string.Join(", ", actual.OrderBy(kv => kv.Key).Select(kv => $"[{kv.Key}]x{kv.Value}"))}");
                    }
                }
            }
        }

        Assert.True(sawRows.SetEquals(outputs), "some view never produced a row: " + string.Join(", ", outputs.Except(sawRows)));
        // An aggregate emits each group at weight +1 (its COUNT/SUM carry the sign), so
        // only the non-aggregate views must surface a negative row weight.
        var signed = outputs.Where(o => !o.EndsWith("_agg", StringComparison.Ordinal) && !o.EndsWith("_direct", StringComparison.Ordinal) && o != "top").ToArray();
        Assert.True(
            sawNegative.IsSupersetOf(signed),
            "some view never held a negative weight: " + string.Join(", ", signed.Except(sawNegative)));
    }

    public static TheoryData<string, int> ViewCases()
    {
        var data = new TheoryData<string, int>();
        foreach (var (name, _) in Views)
        {
            foreach (var w in new[] { 1, 2, 4 })
            {
                data.Add(name, w);
            }
        }

        return data;
    }

    /// <summary>One view per program, so a refusal or mismatch names its construct.</summary>
    [Theory]
    [MemberData(nameof(ViewCases))]
    public void View_ParallelMatchesSerial_ArbitrarySignedStreams(string view, int workers)
    {
        var program = Program(Views.Single(v => v.Name == view));
        for (var seed = 1; seed <= 8; seed++)
        {
            AssertParallelMatchesSerial(program, [view], workers, seed);
        }
    }

    /// <summary>
    /// Every view in one program: views share the table inputs, and the parallel
    /// compile must accept the whole program.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void WholeProgram_ParallelMatchesSerial(int workers)
    {
        AssertParallelMatchesSerial(
            Program(Views), Views.Select(v => v.Name).ToArray(), workers, seed: 4242, ticks: 40);
    }

    /// <summary>
    /// A view reading another view's outer-join output: the downstream join and
    /// aggregate see a stream compiled in a different view's context.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void ChainedViews_ParallelMatchesSerial(int workers)
    {
        var program = Program(
            ("base_lj", "SELECT a.k1, a.k2, a.v, b.w FROM a LEFT JOIN b ON a.k1 = b.k1 AND a.ts BETWEEN b.eff AND b.en"),
            ("top", "SELECT base_lj.k1, COUNT(*) AS n FROM base_lj WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k2 = base_lj.k2) GROUP BY base_lj.k1"));
        for (var seed = 1; seed <= 5; seed++)
        {
            AssertParallelMatchesSerial(program, ["base_lj", "top"], workers, seed);
        }
    }

    /// <summary>
    /// NOT IN over NULLable operands still falls back to the serial compile: the
    /// resolver layers SQL's three-valued NOT IN as a hidden NULL-count column joined
    /// in by a (correlated) scalar subquery, and those plans are not sharded yet. The
    /// anti-join itself no longer blocks.
    /// </summary>
    [Fact]
    public void NotIn_OverNullableOperands_StillRefusedByScalarSubquery()
    {
        foreach (var sql in new[]
                 {
                     "SELECT a.k1, a.v FROM a WHERE a.k1 NOT IN (SELECT k1 FROM b)",
                     "SELECT a.k1, a.v FROM a WHERE a.k1 NOT IN (SELECT k1 FROM b WHERE b.k2 = a.k2)",
                 })
        {
            var outputs = new HashSet<string>(["v"], StringComparer.Ordinal);
            var program = Program(("v", sql));
            Assert.False(SqlProgram.TryCompileParallel(program, outputs, 2, out _));

            var resolved = SqlProgram.Resolve(program, outputs);
            var blockers = PlanToCircuit.ParallelBlockers(
                DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(resolved.Views.Single().Query));
            Assert.Contains(blockers, b => b.Contains("scalar subquery", StringComparison.Ordinal));
            Assert.DoesNotContain(blockers, b => b.Contains("anti-join", StringComparison.Ordinal));
        }
    }
}
