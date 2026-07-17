// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Correctness + behaviour gate for join residual pushdown (see the
/// residual-pushdown finding): a non-equi ON conjunct spanning both sides (a
/// temporal-SCD <c>ts BETWEEN lo AND hi</c>) is folded INTO the flat join's
/// cross-product enumeration so rows failing it never enter the output Z-set,
/// instead of materialising the full per-key L×R product and filtering it above.
///
/// <para>The result set is identical either way — <c>matched = σ_residual(L ⋈_key R)</c>
/// — so the strong check is incremental ≡ the independent batch oracle over signed
/// streams, for INNER (pushed into the join combine) and LEFT / FULL OUTER (pushed
/// into the <c>matched</c> inner join of the anti-join rewrite). The complementary
/// behavioural check proves the pushdown actually fires: the join operator emits
/// only the survivors, not the full cross product.</para>
/// </summary>
public sealed class JoinResidualPushdownTests
{
    private static CompiledQuery Compile(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static LogicalPlan Plan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    private static readonly string[] Ddl =
    {
        "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
        "CREATE TABLE r (k INT NOT NULL, lo INT NOT NULL, hi INT NOT NULL)",
    };

    // Equi on k, residual BETWEEN on v within [lo, hi] — the ivm-bench SCD shape.
    // Under a shared key the join iterates the full L×R product but only rows with
    // lo <= v <= hi survive.
    private const string InnerSql =
        "SELECT l.k, l.v, r.lo, r.hi FROM l JOIN r ON l.k = r.k AND l.v BETWEEN r.lo AND r.hi";
    private const string LeftSql =
        "SELECT l.k, l.v, r.lo, r.hi FROM l LEFT JOIN r ON l.k = r.k AND l.v BETWEEN r.lo AND r.hi";
    private const string FullSql =
        "SELECT l.k, l.v, r.lo, r.hi FROM l FULL JOIN r ON l.k = r.k AND l.v BETWEEN r.lo AND r.hi";

    [Fact]
    public void PushdownFires_InnerJoinEmitsOnlySurvivors_NotFullProduct()
    {
        // 20 left rows (v = 0..19) and 5 right ranges, all under key 0. The join
        // ITERATES the full 20×5 = 100 product, but each narrow range [i, i] admits
        // exactly one v, so only 5 rows survive the residual. With pushdown the join
        // op emits those 5 directly — without it, it would emit 100 and a downstream
        // Filter would cut 95.
        var q = Compile(Ddl, InnerSql);

        for (var v = 0; v < 20; v++)
        {
            q.Table("l").Insert(0, v);
        }

        foreach (var i in new[] { 0, 5, 10, 15, 19 })
        {
            q.Table("r").Insert(0, i, i);
        }

        q.Step();

        var plan = Plan(Ddl, InnerSql);
        var join0 = (JoinPlan)((ProjectPlan)plan).Input;
        Assert.NotNull(join0.Residual); // the BETWEEN is the join residual, not a separate Filter

        var join = Assert.Single(q.CollectStats(), s => s.Name == "IncrementalInnerJoin");
        Assert.Equal(5, join.LastOutputRows); // survivors only — NOT the 100-row product
        Assert.Equal(5, q.Current.Count);
    }

    [Fact]
    public void InnerResidual_IncrementalEqualsBatch()
    {
        for (var seed = 0; seed < 200; seed++)
        {
            AssertIncrementalEqualsBatch(InnerSql, GenerateSigned(seed));
        }
    }

    [Fact]
    public void LeftOuterResidual_IncrementalEqualsBatch()
    {
        // A left row whose only key-matches all FAIL the residual must still emit,
        // NULL-padded — the anti-join rewrite computes that from `matched`, which is
        // the same set with or without pushdown.
        for (var seed = 0; seed < 200; seed++)
        {
            AssertIncrementalEqualsBatch(LeftSql, GenerateSigned(seed));
        }
    }

    [Fact]
    public void FullOuterResidual_IncrementalEqualsBatch()
    {
        for (var seed = 0; seed < 200; seed++)
        {
            AssertIncrementalEqualsBatch(FullSql, GenerateSigned(seed));
        }
    }

    private static void AssertIncrementalEqualsBatch(
        string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks)
    {
        var compiled = Compile(Ddl, sql);
        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, ticks);

        var all = ticks.SelectMany(t => t).ToList();
        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["l"] = IncrementalOracle.NetTable(all, "l"),
            ["r"] = IncrementalOracle.NetTable(all, "r"),
        };
        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(Plan(Ddl, sql), ctx);

        Assert.True(
            accumulated.Equals(batch),
            $"residual join diverged from batch oracle.\nsql:  {sql}\nincr: {accumulated}\nbatch: {batch}");
    }

    // Well-formed signed stream: maintain a live multiset per table and either
    // insert a fresh row or retract a live one. Small domains (k in {0,1}, v in
    // {0..5}, lo/hi in {0..5}) make keys collide and drive a fat per-key product
    // where most pairs fail the residual — the case the pushdown targets — while
    // still flipping outer-join match presence across ticks.
    private static List<IReadOnlyList<InputEvent>> GenerateSigned(int seed)
    {
        var rng = new Random(seed);
        var liveL = new List<object?[]>();
        var liveR = new List<object?[]>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
        var tickCount = 4 + rng.Next(6);

        for (var t = 0; t < tickCount; t++)
        {
            var tick = new List<InputEvent>();
            var n = 1 + rng.Next(5);
            for (var i = 0; i < n; i++)
            {
                var left = rng.Next(2) == 0;
                var live = left ? liveL : liveR;

                if (live.Count > 0 && rng.Next(2) == 0)
                {
                    var idx = rng.Next(live.Count);
                    var row = live[idx];
                    live.RemoveAt(idx);
                    tick.Add(new InputEvent(left ? "l" : "r", row, -1));
                }
                else if (left)
                {
                    var row = new object?[] { rng.Next(0, 2), rng.Next(0, 6) };
                    liveL.Add(row);
                    tick.Add(new InputEvent("l", row, 1));
                }
                else
                {
                    var lo = rng.Next(0, 6);
                    var hi = rng.Next(lo, 6);
                    var row = new object?[] { rng.Next(0, 2), lo, hi };
                    liveR.Add(row);
                    tick.Add(new InputEvent("r", row, 1));
                }
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
