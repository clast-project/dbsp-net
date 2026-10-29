// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Correctness gate for projection pushdown through a join
/// (docs/design-row-representation.md §21, <see cref="JoinColumnPruningMode"/>): the
/// optimizer narrows each join input to the columns some consumer actually reads
/// (parent + the join's combine/residual/equi-key), so the join trace stores and
/// whole-row-hashes only live columns.
///
/// <para>Unlike the §18 aggregate-input narrowing, this is <b>unconditionally sound</b>
/// — ordinary relational projection pushdown: dropping a column no consumer reads
/// cannot change which output rows the join emits or how they consolidate, for
/// <i>arbitrary signed Z-sets</i>. So the strongest possible check applies: reuse the
/// random-query oracle with pruning ON over the <b>full</b> ±1 surface (3000 iters,
/// including deletes of never-inserted rows — outside §18's well-formed envelope), and
/// require the pruned circuit's accumulated output to equal the batch re-computation of
/// the original plan every time. The complementary guard is the full suite staying
/// green with the seam OFF (default), proving byte-identical-when-disabled.</para>
///
/// <para><b>Outer joins</b> (no residual) are pruned too, and that is <b>not</b>
/// unconditional: narrowing a NULL-padded side merges rows whose weights then sum, so a
/// key can look unmatched when its integrated rows have cancelling signs. It is exact on
/// well-formed streams (no negative integrated row weight), which is what the outer-join
/// tests check; <see cref="Corner_OuterJoinUnderNegativeRowWeights_FollowsSummedKeyWeight"/>
/// pins the divergence. This is the ivm-bench fact_market_history win (§9.6 of
/// docs/design-state-representation.md).</para>
/// </summary>
public class JoinColumnPruningTests
{
    [Fact]
    public void RandomQuery_PrunedCircuitEqualsBatch()
    {
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks)
            .Sample((sql, ticks) => CheckOnePruned(sql, ticks), iter: 3000);
    }

    private static bool CheckOnePruned(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;

        // Pruning is applied at Optimize time, so enable the seam across Optimize, then
        // compile the pruned plan. Thread-static: this test body runs synchronously.
        var prev = JoinColumnPruningMode.Enabled;
        JoinColumnPruningMode.Enabled = true;
        ZSet<StructuralRow, Z64> accumulated;
        try
        {
            var optimized = PlanOptimizer.Optimize(plan);
            var compiled = PlanToCircuit.Compile(optimized);
            var filteredTicks = ticks
                .Select(tick => (IReadOnlyList<InputEvent>)tick
                    .Where(e => compiled.Inputs.ContainsKey(e.Table))
                    .ToList())
                .ToList();
            accumulated = IncrementalOracle.RunAndAccumulate(compiled, filteredTicks);
        }
        finally
        {
            JoinColumnPruningMode.Enabled = prev;
        }

        var allFiltered = ticks
            .Select(tick => (IReadOnlyList<InputEvent>)tick.ToList())
            .ToList();
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            var open = ddl.IndexOf('(', StringComparison.Ordinal);
            var name = ddl.Substring("CREATE TABLE ".Length, open - "CREATE TABLE ".Length).Trim();
            tableStates[name] = IncrementalOracle.NetTable(allFiltered.SelectMany(t => t), name);
        }

        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        if (RandomQuery.OutsideNullableSumDomain(sql, allFiltered.SelectMany(t => t)))
        {
            return true;
        }

        if (!accumulated.Equals(batch))
        {
            Console.Error.WriteLine($"SQL (pruned): {sql}");
            Console.Error.WriteLine("accumulated (pruned circuit): " + accumulated);
            Console.Error.WriteLine("batch       (oracle):         " + batch);
            return false;
        }

        return true;
    }

    private static readonly string[] Ddl =
    {
        "CREATE TABLE l (k INT NOT NULL, a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE r (k INT NOT NULL, c INT NOT NULL, d INT NOT NULL)",
    };

    // The Project reads only l.a and r.c; the equi-key reads l.k and r.k. So l.b and
    // r.d are live to no consumer and must be pruned from BOTH stored join inputs.
    private const string JoinSql = "SELECT l.a, r.c FROM l JOIN r ON l.k = r.k";

    [Fact]
    public void PruningActuallyFires_NarrowsBothJoinInputs_WhenEnabled()
    {
        // Guard against a vacuous pass: confirm the seam genuinely narrows both stored
        // join-input schemas (3 → 2 columns each, dropping b and d), and does NOT when
        // disabled.
        var plan = ParsePlan(JoinSql);

        var off = FindJoin(WithPruning(false, () => PlanOptimizer.Optimize(plan)));
        var on = FindJoin(WithPruning(true, () => PlanOptimizer.Optimize(plan)));

        Assert.Equal(3, off.Left.Schema.Count);  // k, a, b (unchanged)
        Assert.Equal(3, off.Right.Schema.Count); // k, c, d (unchanged)
        Assert.Equal(2, on.Left.Schema.Count);   // k, a (b dropped)
        Assert.Equal(2, on.Right.Schema.Count);  // k, c (d dropped)
    }

    [Fact]
    public void PrunedJoin_EqualsFullJoin_OverArbitrarySignedStreams()
    {
        // Direct seam-ON vs seam-OFF circuit equivalence over arbitrary ±1 streams
        // (including unmatched deletes) — isolates this rule from the rest of the
        // optimizer and exercises the unconditional-soundness claim end-to-end.
        for (var seed = 0; seed < 200; seed++)
        {
            var ticks = GenerateSigned(seed);

            var full = RunCompiled(prune: false, ticks);
            var pruned = RunCompiled(prune: true, ticks);

            Assert.True(
                full.Equals(pruned),
                $"seed {seed}: pruned join diverged from full-row.\nfull:   {full}\npruned: {pruned}");
        }
    }

    // ---- outer joins (no residual) ----

    private static readonly string[] OuterDdl =
    {
        "CREATE TABLE l (k INT NOT NULL, a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE r (k INT NOT NULL, c INT NOT NULL, d INT NOT NULL)",
        "CREATE TABLE ln (k INT, a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE rn (k INT, c INT NOT NULL, d INT NOT NULL)",
    };

    // Each reads a strict subset of each side, so pruning fires on every one.
    public static readonly TheoryData<string> OuterJoinQueries = new()
    {
        "SELECT l.a, r.c FROM l LEFT JOIN r ON l.k = r.k",
        // ivm-bench fact_market_history's shape: no right column is read, so the right
        // side narrows to its key and a fan-out becomes a weight multiply.
        "SELECT l.a, l.b FROM l LEFT JOIN r ON l.k = r.k",
        "SELECT r.c FROM l RIGHT JOIN r ON l.k = r.k",
        "SELECT l.a, r.c FROM l FULL JOIN r ON l.k = r.k",
        "SELECT ln.a, rn.c FROM ln LEFT JOIN rn ON ln.k = rn.k",
        "SELECT ln.a, rn.c FROM ln FULL JOIN rn ON ln.k = rn.k",
        "SELECT l.a, COUNT(*) AS n, SUM(r.c) AS s FROM l LEFT JOIN r ON l.k = r.k GROUP BY l.a",
    };

    [Theory]
    [MemberData(nameof(OuterJoinQueries))]
    public void OuterJoin_PruningFires_AndKeepsPaddedSideNullable(string sql)
    {
        var plan = ParseOuterPlan(sql);
        var off = FindJoin(WithPruning(false, () => PlanOptimizer.Optimize(plan)));
        var on = FindJoin(WithPruning(true, () => PlanOptimizer.Optimize(plan)));

        Assert.True(
            on.Left.Schema.Count + on.Right.Schema.Count < off.Left.Schema.Count + off.Right.Schema.Count,
            "pruning did not narrow either side");
        Assert.Equal(on.Left.Schema.Count + on.Right.Schema.Count, on.Schema.Count);

        // A padded side's columns stay nullable in the join's schema.
        var leftCount = on.Left.Schema.Count;
        for (var i = 0; i < on.Schema.Count; i++)
        {
            var padded = i < leftCount
                ? on.JoinType is JoinType.RightOuter or JoinType.FullOuter
                : on.JoinType is JoinType.LeftOuter or JoinType.FullOuter;
            if (padded)
            {
                Assert.True(on.Schema[i].Type.Nullable, $"column {i} of a padded side lost its nullability");
            }
        }
    }

    [Fact]
    public void OuterJoin_FanOutShape_NarrowsRightToTheKey()
    {
        var on = FindJoin(WithPruning(true, () => PlanOptimizer.Optimize(
            ParseOuterPlan("SELECT l.a, l.b FROM l LEFT JOIN r ON l.k = r.k"))));
        Assert.Equal(1, on.Right.Schema.Count);
    }

    [Fact]
    public void OuterJoin_WithResidual_IsNotNarrowed()
    {
        var plan = ParseOuterPlan("SELECT l.a FROM l LEFT JOIN r ON l.k = r.k AND r.d > 1");
        var on = FindJoin(WithPruning(true, () => PlanOptimizer.Optimize(plan)));
        Assert.Equal(3, on.Left.Schema.Count);
        Assert.Equal(3, on.Right.Schema.Count);
    }

    [Theory]
    [MemberData(nameof(OuterJoinQueries))]
    public void OuterJoin_PrunedEqualsUnprunedAndBatch_OnWellFormedStreams(string sql)
    {
        // Well-formed: deletes retract live rows only, so no integrated row weight is
        // ever negative — the domain where narrowing a padded side is exact.
        foreach (var structural in new[] { false, true })
        {
            for (var seed = 0; seed < 150; seed++)
            {
                var ticks = GenerateWellFormed(seed);
                var full = RunOuter(sql, prune: false, structural, ticks);
                var pruned = RunOuter(sql, prune: true, structural, ticks);
                Assert.True(
                    full.Equals(pruned),
                    $"{sql} seed {seed} structural={structural}: pruned diverged.\nfull:   {full}\npruned: {pruned}");
                var batch = BatchOuter(sql, ticks);
                Assert.True(
                    batch.Equals(pruned),
                    $"{sql} seed {seed} structural={structural}: pruned != batch.\nbatch:  {batch}\npruned: {pruned}");
            }
        }
    }

    [Fact]
    public void Corner_OuterJoinUnderNegativeRowWeights_FollowsSummedKeyWeight()
    {
        // The accepted divergence. The right side holds +1 (k=0, c=1, d=0) and
        // -1 (k=0, c=2, d=1): a negative integrated row weight no SQL table has. Unpruned,
        // key 0 holds two rows, so it is matched and the projected product cancels to
        // nothing. Pruned, the right side is just k, whose weights sum to 0, so key 0 is
        // unmatched and the left row is NULL-padded.
        const string sql = "SELECT l.a, l.b FROM l LEFT JOIN r ON l.k = r.k";
        var ticks = new List<IReadOnlyList<InputEvent>>
        {
            new List<InputEvent>
            {
                new("l", new object?[] { 0, 5, 6 }, 1),
                new("r", new object?[] { 0, 1, 0 }, 1),
                new("r", new object?[] { 0, 2, 1 }, -1),
            },
        };

        foreach (var structural in new[] { false, true })
        {
            Assert.Equal(0, RunOuter(sql, prune: false, structural, ticks).Count);
            var pruned = RunOuter(sql, prune: true, structural, ticks);
            Assert.Equal(1L, Assert.Single(pruned).Value.Value);
        }
    }

    private static ZSet<StructuralRow, Z64> RunOuter(
        string sql, bool prune, bool structural, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var optimized = WithPruning(prune, () => PlanOptimizer.Optimize(ParseOuterPlan(sql)));
        // ShareArrangements is implemented on the structural path only, so it forces that
        // compile (ivm-bench's path); these queries have no shareable arrangement.
        var compiled = PlanToCircuit.Compile(
            optimized, options: structural ? new CompileOptions { ShareArrangements = true } : null);
        var filtered = ticks
            .Select(t => (IReadOnlyList<InputEvent>)t.Where(e => compiled.Inputs.ContainsKey(e.Table)).ToList())
            .ToList();
        return IncrementalOracle.RunAndAccumulate(compiled, filtered);
    }

    private static ZSet<StructuralRow, Z64> BatchOuter(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var events = ticks.SelectMany(t => t).ToList();
        var tables = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal);
        foreach (var name in new[] { "l", "r", "ln", "rn" })
        {
            tables[name] = IncrementalOracle.NetTable(events, name);
        }

        return BatchPlanEvaluator.Evaluate(
            ParseOuterPlan(sql), new BatchEvalContext(tables, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>()));
    }

    private static LogicalPlan ParseOuterPlan(string sql)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var ddl in OuterDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    // Inserts with duplicates and colliding kept columns, NULL keys on ln/rn, and deletes
    // drawn only from live rows (a row may be live with weight > 1).
    private static List<IReadOnlyList<InputEvent>> GenerateWellFormed(int seed)
    {
        var rng = new Random(seed);
        var live = new List<InputEvent>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
        var tables = new[] { "l", "r", "ln", "rn" };
        for (var t = 0; t < 4 + rng.Next(6); t++)
        {
            var tick = new List<InputEvent>();
            for (var i = rng.Next(8); i > 0; i--)
            {
                if (live.Count > 0 && rng.Next(3) == 0)
                {
                    var idx = rng.Next(live.Count);
                    var e = live[idx];
                    live.RemoveAt(idx);
                    tick.Add(e with { Weight = -1 });
                }
                else
                {
                    var table = tables[rng.Next(tables.Length)];
                    object? key = table.EndsWith('n') && rng.Next(4) == 0 ? null : rng.Next(0, 3);
                    var e = new InputEvent(table, new object?[] { key, rng.Next(0, 3), rng.Next(0, 3) }, 1);
                    live.Add(e);
                    tick.Add(e);
                }
            }

            ticks.Add(tick);
        }

        return ticks;
    }

    // ---- helpers ----

    private static ZSet<StructuralRow, Z64> RunCompiled(
        bool prune, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var plan = ParsePlan(JoinSql);
        var optimized = WithPruning(prune, () => PlanOptimizer.Optimize(plan));
        var compiled = PlanToCircuit.Compile(optimized);
        return IncrementalOracle.RunAndAccumulate(compiled, ticks);
    }

    private static T WithPruning<T>(bool enabled, Func<T> body)
    {
        var prev = JoinColumnPruningMode.Enabled;
        JoinColumnPruningMode.Enabled = enabled;
        try
        {
            return body();
        }
        finally
        {
            JoinColumnPruningMode.Enabled = prev;
        }
    }

    private static LogicalPlan ParsePlan(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    private static JoinPlan FindJoin(LogicalPlan plan) => plan switch
    {
        JoinPlan j => j,
        ProjectPlan p => FindJoin(p.Input),
        FilterPlan f => FindJoin(f.Input),
        AggregatePlan a => FindJoin(a.Input),
        _ => throw new InvalidOperationException("no JoinPlan found"),
    };

    // Arbitrary signed stream: each tick emits a few +1 and a few -1 events with rows
    // drawn from small ranges (so {k} matches frequently and {b,d} collide on kept
    // columns). The -1 events are NOT constrained to retract live rows — this is the
    // full signed surface where §18-style narrowing would be unsound but projection
    // pushdown is sound.
    private static List<IReadOnlyList<InputEvent>> GenerateSigned(int seed)
    {
        var rng = new Random(seed);
        var ticks = new List<IReadOnlyList<InputEvent>>();
        var tickCount = 4 + rng.Next(6);

        for (var t = 0; t < tickCount; t++)
        {
            var tick = new List<InputEvent>();
            var n = rng.Next(6);
            for (var i = 0; i < n; i++)
            {
                var table = rng.Next(2) == 0 ? "l" : "r";
                var row = new object?[]
                {
                    rng.Next(0, 3), // k
                    rng.Next(0, 4), // a / c
                    rng.Next(0, 3), // b / d (droppable)
                };
                var weight = rng.Next(2) == 0 ? 1 : -1;
                tick.Add(new InputEvent(table, row, weight));
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
