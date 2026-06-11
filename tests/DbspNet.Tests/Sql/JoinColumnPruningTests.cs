// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Correctness gate for projection pushdown through an INNER join
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
