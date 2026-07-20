// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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
/// Coverage for the semi-join subquery narrowing rule
/// (<c>PlanOptimizer.NarrowSemiJoinSubquery</c>): an EXISTS/NOT-EXISTS body that
/// joins two tables only to test existence has its inner join rewritten to a
/// semi-join, collapsing the product. Models ivm-bench <c>broker_performance</c>.
/// </summary>
public class SemiJoinNarrowingTests
{
    private static readonly string[] Ddl =
    {
        "CREATE TABLE b (id INT NOT NULL)",
        "CREATE TABLE t (bid INT NOT NULL, acct INT NOT NULL)",
        "CREATE TABLE c (acct INT NOT NULL)",
    };

    // broker_performance shape: the subquery joins t⋈c only to test existence.
    private const string NotExistsJoin =
        "SELECT id FROM b WHERE NOT EXISTS (" +
        "SELECT 1 FROM t JOIN c ON t.acct = c.acct WHERE t.bid = b.id)";

    private const string ExistsJoin =
        "SELECT id FROM b WHERE EXISTS (" +
        "SELECT 1 FROM t JOIN c ON t.acct = c.acct WHERE t.bid = b.id)";

    private static LogicalPlan Resolve(string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static SemiJoinPlan? FindSemiJoin(LogicalPlan plan) => plan switch
    {
        SemiJoinPlan sj => sj,
        ProjectPlan p => FindSemiJoin(p.Input),
        FilterPlan f => FindSemiJoin(f.Input),
        _ => null,
    };

    [Theory]
    [InlineData(NotExistsJoin)]
    [InlineData(ExistsJoin)]
    public void Optimizer_NarrowsExistsBodyJoin_ToSemiJoin(string query)
    {
        var opt = PlanOptimizer.Optimize(Resolve(query));
        var outerSemi = FindSemiJoin(opt);
        Assert.NotNull(outerSemi);

        // The subquery body's inner join must have become a nested semi-join
        // (Project → SemiJoin, where it was Project → Join before the rule).
        var proj = Assert.IsType<ProjectPlan>(outerSemi!.Subquery);
        Assert.IsType<SemiJoinPlan>(proj.Input);
    }

    [Fact]
    public void RawPlan_HasInnerJoinInSubquery()
    {
        // Sanity that the rule is what changes the shape: unoptimized, the body is
        // a Join. (Guards against the structural test above being vacuous.)
        var raw = Resolve(NotExistsJoin);
        var outerSemi = FindSemiJoin(raw);
        var proj = Assert.IsType<ProjectPlan>(outerSemi!.Subquery);
        Assert.IsType<JoinPlan>(proj.Input);
    }

    [Theory]
    [InlineData(NotExistsJoin)]
    [InlineData(ExistsJoin)]
    public void OptimizedEqualsUnoptimizedAndBatch(string query)
    {
        for (var seed = 0; seed < 16; seed++)
        {
            var optimized = PlanToCircuit.Compile(PlanOptimizer.Optimize(Resolve(query)));
            var raw = PlanToCircuit.Compile(Resolve(query));

            var ticks = RandomTicks(seed);
            var optAcc = IncrementalOracle.RunAndAccumulate(optimized, ticks);
            var rawAcc = IncrementalOracle.RunAndAccumulate(raw, ticks);

            // Result-preservation: the rewrite must not change results.
            Assert.True(
                optAcc.Equals(rawAcc),
                $"optimized != unoptimized  seed={seed} query={query}\n  opt={optAcc}\n  raw={rawAcc}");

            // And both equal the independent batch oracle over the un-optimized plan.
            var all = ticks.SelectMany(x => x).ToList();
            var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
            {
                ["b"] = IncrementalOracle.NetTable(all, "b"),
                ["t"] = IncrementalOracle.NetTable(all, "t"),
                ["c"] = IncrementalOracle.NetTable(all, "c"),
            };
            var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var batch = BatchPlanEvaluator.Evaluate(Resolve(query), ctx);
            Assert.True(optAcc.Equals(batch), $"optimized != batch  seed={seed} query={query}");
        }
    }

    // Small domains so joins fan out (many t per acct, many c per acct) and
    // existence flips as rows are inserted/deleted.
    private static List<IReadOnlyList<InputEvent>> RandomTicks(int seed)
    {
        var rng = new Random(seed);
        var present = new List<InputEvent>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
        for (var t = 0; t < 14; t++)
        {
            var tick = new List<InputEvent>();
            var ops = rng.Next(1, 4);
            for (var o = 0; o < ops; o++)
            {
                if (present.Count > 0 && rng.NextDouble() < 0.35)
                {
                    var idx = rng.Next(present.Count);
                    var ev = present[idx];
                    present.RemoveAt(idx);
                    tick.Add(new InputEvent(ev.Table, ev.Row, -1));
                }
                else
                {
                    InputEvent ev = rng.Next(3) switch
                    {
                        0 => new InputEvent("b", new object?[] { rng.Next(0, 5) }, 1),
                        1 => new InputEvent("t", new object?[] { rng.Next(0, 5), rng.Next(0, 4) }, 1),
                        _ => new InputEvent("c", new object?[] { rng.Next(0, 4) }, 1),
                    };
                    present.Add(ev);
                    tick.Add(ev);
                }
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
