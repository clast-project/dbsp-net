// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit.Operators;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Index fusion (docs/design-state-representation.md §9.9, <see cref="IndexFusionMode"/>):
/// on the structural path at W≤1 a join input's Filter/Project chain and NULL-key check run
/// inside the pass that indexes it by the join key. Map and filter are pure row functions
/// and Z-set addition is associative, so the fusion is exact for <b>arbitrary signed</b>
/// streams. These tests check fused ≡ unfused ≡ batch oracle on that full surface.
/// </summary>
public class IndexFusionTests
{
    private static readonly string[] Ddl =
    {
        "CREATE TABLE t (k INT NOT NULL, a INT NOT NULL, b INT)",
        "CREATE TABLE u (k INT NOT NULL, c INT NOT NULL, d INT)",
        "CREATE TABLE n (k INT, a INT NOT NULL)",
        "CREATE TABLE m (k INT, c INT NOT NULL)",
    };

    public static readonly TheoryData<string> Queries = new()
    {
        // Filter/Project chains on both join inputs.
        "SELECT x.a, y.c FROM (SELECT k, a + 1 AS a FROM t WHERE b > 0) x " +
        "JOIN (SELECT k, c FROM u WHERE d < 2) y ON x.k = y.k",
        // NULLable keys, no chain: only the NULL-key check is fused.
        "SELECT n.a, m.c FROM n JOIN m ON n.k = m.k",
        // NULLable keys under a chain: the check runs on the mapped row.
        "SELECT x.a, m.c FROM (SELECT k, a * 2 AS a FROM n WHERE a <> 1) x JOIN m ON x.k = m.k",
        // A projection that collapses distinct input rows onto one row (weights must sum).
        "SELECT x.k2, u.c FROM (SELECT k AS k2 FROM t) x JOIN u ON x.k2 = u.k",
        // A computed key.
        "SELECT x.a, u.c FROM (SELECT k + 1 AS k1, a FROM t) x JOIN u ON x.k1 = u.k",
        // CTE consumed once, and a CTE consumed twice (the second reuses the cached stream).
        "WITH s AS (SELECT k, a FROM t WHERE a > 0) SELECT s.a, u.c FROM s JOIN u ON s.k = u.k",
        "WITH s AS (SELECT k, a FROM t WHERE a > 0) SELECT s1.a, s2.a FROM s s1 JOIN s s2 ON s1.k = s2.k",
        // Three-way join: the middle join's output is itself an indexed input.
        "SELECT t.a, u.c, m.c FROM t JOIN u ON t.k = u.k JOIN m ON u.k = m.k",
        // The same right relation and key in two joins: the shared-arrangement branch.
        "SELECT t.a, u.c FROM t JOIN u ON t.k = u.k " +
        "UNION ALL SELECT n.a, u.c FROM n JOIN u ON n.k = u.k",
    };

    [Theory]
    [MemberData(nameof(Queries))]
    public void Fused_EqualsUnfusedAndBatch_OverArbitrarySignedStreams(string sql)
    {
        for (var seed = 0; seed < 200; seed++)
        {
            var ticks = GenerateSigned(seed);
            var unfused = Run(sql, fused: false, ticks);
            var fused = Run(sql, fused: true, ticks);
            Assert.True(
                unfused.Equals(fused),
                $"seed {seed}: fused diverged.\nunfused: {unfused}\nfused:   {fused}");

            var batch = Batch(sql, ticks);
            Assert.True(batch.Equals(fused), $"seed {seed}: fused != batch.\nbatch: {batch}\nfused: {fused}");
        }
    }

    public static readonly TheoryData<string, bool> AggregateQueries = new()
    {
        // (query, linear): linear aggregates are also checked against the batch oracle.
        { "SELECT k2, COUNT(*) AS n, SUM(a) AS s FROM (SELECT k + 1 AS k2, a FROM t WHERE a > 0) x GROUP BY k2", true },
        { "SELECT k, SUM(a2) AS s FROM (SELECT k, a * 2 AS a2 FROM t) x GROUP BY k", true },
        // A projection that collapses rows before grouping.
        { "SELECT a, COUNT(*) AS n FROM (SELECT a FROM t WHERE k <> 1) x GROUP BY a", true },
        { "WITH s AS (SELECT k, a FROM t WHERE b IS NOT NULL) SELECT k, COUNT(*) AS n FROM s GROUP BY k", true },
        // Non-linear aggregates: fused vs unfused only (the batch comparison of MIN/MAX on
        // arbitrary signed streams is a separate domain question, unrelated to fusion).
        { "SELECT k, MIN(a2) AS lo, MAX(a2) AS hi FROM (SELECT k, a + b AS a2 FROM t) x GROUP BY k", false },
        { "SELECT k, COUNT(DISTINCT a) AS d FROM (SELECT k, a FROM n WHERE a <> 2) x GROUP BY k", false },
    };

    [Theory]
    [MemberData(nameof(AggregateQueries))]
    public void FusedAggregateInput_EqualsUnfused_OverArbitrarySignedStreams(string sql, bool linear)
    {
        for (var seed = 0; seed < 200; seed++)
        {
            var ticks = GenerateSigned(seed);
            var unfused = Run(sql, fused: false, ticks);
            var fused = Run(sql, fused: true, ticks);
            Assert.True(
                unfused.Equals(fused),
                $"seed {seed}: fused diverged.\nunfused: {unfused}\nfused:   {fused}");
            if (linear)
            {
                var batch = Batch(sql, ticks);
                Assert.True(batch.Equals(fused), $"seed {seed}: fused != batch.\nbatch: {batch}\nfused: {fused}");
            }
        }
    }

    [Fact]
    public void Fusion_RemovesThePointwiseOperatorsAroundTheJoin()
    {
        // Unfused: per side a chain operator, a NULL-key filter (b, d are nullable but the
        // keys are not, so the filter is already gone) and the index. Fused: the index only.
        const string sql =
            "SELECT x.a, y.c FROM (SELECT k, a + 1 AS a FROM t WHERE b > 0) x " +
            "JOIN (SELECT k, c FROM u WHERE d < 2) y ON x.k = y.k";
        Assert.True(
            ApplyOps(sql, fused: true) < ApplyOps(sql, fused: false),
            $"fused {ApplyOps(sql, fused: true)} ApplyOps, unfused {ApplyOps(sql, fused: false)}");
    }

    private static int ApplyOps(string sql, bool fused) =>
        Compile(sql, fused).Circuit.Operators.Count(o =>
            o.GetType().IsGenericType && o.GetType().GetGenericTypeDefinition() == typeof(ApplyOp<,>));

    private static CompiledQuery Compile(string sql, bool fused)
    {
        var prev = IndexFusionMode.Enabled;
        IndexFusionMode.Enabled = fused;
        try
        {
            // ShareArrangements is implemented on the structural path only, so it forces that
            // compile (the path the fusion lives on) and enables the shared-arrangement branch.
            return PlanToCircuit.Compile(Parse(sql), options: new CompileOptions { ShareArrangements = true });
        }
        finally
        {
            IndexFusionMode.Enabled = prev;
        }
    }

    private static ZSet<StructuralRow, Z64> Run(string sql, bool fused, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var compiled = Compile(sql, fused);
        var filtered = ticks
            .Select(t => (IReadOnlyList<InputEvent>)t.Where(e => compiled.Inputs.ContainsKey(e.Table)).ToList())
            .ToList();
        return IncrementalOracle.RunAndAccumulate(compiled, filtered);
    }

    private static ZSet<StructuralRow, Z64> Batch(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var events = ticks.SelectMany(t => t).ToList();
        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var name in new[] { "t", "u", "n", "m" })
        {
            tables[name] = IncrementalOracle.NetTable(events, name);
        }

        return BatchPlanEvaluator.Evaluate(
            Parse(sql), new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>()));
    }

    private static LogicalPlan Parse(string sql)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var ddl in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    // Arbitrary ±1 events, including deletes of rows never inserted; small ranges so keys
    // match often and projections collide; NULL keys on n and m.
    private static List<IReadOnlyList<InputEvent>> GenerateSigned(int seed)
    {
        var rng = new Random(seed);
        var ticks = new List<IReadOnlyList<InputEvent>>();
        var tables = new[] { "t", "u", "n", "m" };
        for (var t = 0; t < 3 + rng.Next(6); t++)
        {
            var tick = new List<InputEvent>();
            for (var i = rng.Next(10); i > 0; i--)
            {
                var table = tables[rng.Next(tables.Length)];
                object? key = table is "n" or "m" && rng.Next(4) == 0 ? null : rng.Next(0, 3);
                object?[] row = table is "t" or "u"
                    ? [key, rng.Next(0, 3), rng.Next(4) == 0 ? null : rng.Next(0, 3)]
                    : [key, rng.Next(0, 3)];
                tick.Add(new InputEvent(table, row, rng.Next(2) == 0 ? 1 : -1));
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
