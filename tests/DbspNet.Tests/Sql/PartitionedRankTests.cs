// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for rank-in-output — <c>ROW_NUMBER</c> / <c>RANK</c> /
/// <c>DENSE_RANK</c> <c>OVER (PARTITION BY p ORDER BY o …)</c> emitted as a new
/// output column (the general form, not the <c>… &lt;= k</c> TOP-K filter). Split
/// into resolver (recognition / rejection), behavioural (tie semantics,
/// retraction re-ranking, the CASE-nested and unpartitioned shapes), and a
/// randomized incremental ≡ batch differential test — the test of record.
/// </summary>
public class PartitionedRankTests
{
    private const string Emp = "CREATE TABLE emp (name VARCHAR NOT NULL, dept INT NOT NULL, sal INT NOT NULL)";

    private static CompiledQuery Compile(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static LogicalPlan ResolvePlan(string ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---- Resolver: recognition + rejection ----------------------------------

    private static PartitionedRankPlan RankPlanOf(string query)
    {
        var plan = ResolvePlan(Emp, query);
        var proj = Assert.IsType<ProjectPlan>(plan);
        return Assert.IsType<PartitionedRankPlan>(proj.Input);
    }

    [Fact]
    public void Resolve_UnpartitionedRank_IsGlobalPartition()
    {
        var rp = RankPlanOf("SELECT name, sal, RANK() OVER (ORDER BY sal DESC) AS r FROM emp");
        Assert.Empty(rp.PartitionKeys);            // whole relation = one partition
        Assert.Single(rp.SortKeys);
        Assert.True(rp.SortKeys[0].Descending);
        Assert.Equal(RankFunction.Rank, rp.Function);
        // Base columns + one appended rank column.
        Assert.Equal(4, rp.Schema.Count);
    }

    [Fact]
    public void Resolve_PartitionedRowNumber()
    {
        var rp = RankPlanOf(
            "SELECT name, sal, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal DESC) AS r FROM emp");
        Assert.Single(rp.PartitionKeys);
        Assert.Equal(RankFunction.RowNumber, rp.Function);
    }

    [Fact]
    public void Resolve_DenseRank_MultiKeyOrder()
    {
        // market_volatility shape: DESC value with a tiebreaker column.
        var rp = RankPlanOf("SELECT name, sal, DENSE_RANK() OVER (ORDER BY sal DESC, name) AS r FROM emp");
        Assert.Equal(RankFunction.DenseRank, rp.Function);
        Assert.Equal(2, rp.SortKeys.Count);
    }

    [Fact]
    public void Resolve_TwoRankFunctions_OneSpec_ChainsTwoNodes()
    {
        // RANK and DENSE_RANK over the same OVER spec are distinct operators (each
        // emits one column) — chained, not merged into a single rank op.
        var plan = ResolvePlan(Emp,
            "SELECT sal, RANK() OVER (ORDER BY sal DESC) AS r, " +
            "DENSE_RANK() OVER (ORDER BY sal DESC) AS d FROM emp");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var outer = Assert.IsType<PartitionedRankPlan>(proj.Input);
        var inner = Assert.IsType<PartitionedRankPlan>(outer.Input);
        Assert.NotEqual(outer.Function, inner.Function);
    }

    [Fact]
    public void Resolve_RankWithoutOrderBy_Throws() =>
        Assert.Throws<ResolveException>(() =>
            ResolvePlan(Emp, "SELECT sal, RANK() OVER (PARTITION BY dept) AS r FROM emp"));

    [Fact]
    public void Resolve_RankWithFrame_Throws() =>
        Assert.Throws<ResolveException>(() => ResolvePlan(Emp,
            "SELECT sal, RANK() OVER (ORDER BY sal RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS r FROM emp"));

    // ---- Behavioural: tie semantics (1,1,3 vs 1,1,2 vs 1,2,3) ----------------

    [Fact]
    public void Rank_TiesShareRank_AndGap()
    {
        // Unpartitioned RANK: tied rows share the smaller rank, the next distinct
        // value skips (1, 2, 2, 4).
        var q = Compile(Emp, "SELECT name, sal, RANK() OVER (ORDER BY sal DESC) AS r FROM emp");
        q.Table("emp").Insert("a", 1, 100);
        q.Table("emp").Insert("b", 1, 90);
        q.Table("emp").Insert("c", 1, 90);   // tie with b
        q.Table("emp").Insert("d", 1, 80);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a", 100, 1L));
        Assert.Equal(1, WeightOf(q.Current, "b", 90, 2L));
        Assert.Equal(1, WeightOf(q.Current, "c", 90, 2L));   // shares rank 2
        Assert.Equal(1, WeightOf(q.Current, "d", 80, 4L));   // gap: 3 is skipped
    }

    [Fact]
    public void DenseRank_TiesShareRank_NoGap()
    {
        // DENSE_RANK: tied rows share a rank with no gaps (1, 2, 2, 3).
        var q = Compile(Emp, "SELECT name, sal, DENSE_RANK() OVER (ORDER BY sal DESC) AS r FROM emp");
        q.Table("emp").Insert("a", 1, 100);
        q.Table("emp").Insert("b", 1, 90);
        q.Table("emp").Insert("c", 1, 90);
        q.Table("emp").Insert("d", 1, 80);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a", 100, 1L));
        Assert.Equal(1, WeightOf(q.Current, "b", 90, 2L));
        Assert.Equal(1, WeightOf(q.Current, "c", 90, 2L));
        Assert.Equal(1, WeightOf(q.Current, "d", 80, 3L));   // no gap
    }

    [Fact]
    public void RowNumber_DistinctRanks_NoTies()
    {
        // ROW_NUMBER: distinct positions even for equal values (1, 2, 3). With
        // distinct ORDER BY values the assignment is fully deterministic.
        var q = Compile(Emp, "SELECT name, sal, ROW_NUMBER() OVER (ORDER BY sal DESC) AS r FROM emp");
        q.Table("emp").Insert("a", 1, 100);
        q.Table("emp").Insert("b", 1, 90);
        q.Table("emp").Insert("c", 1, 80);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a", 100, 1L));
        Assert.Equal(1, WeightOf(q.Current, "b", 90, 2L));
        Assert.Equal(1, WeightOf(q.Current, "c", 80, 3L));
    }

    [Fact]
    public void Rank_Partitioned_IndependentPerPartition()
    {
        var q = Compile(Emp,
            "SELECT name, dept, sal, RANK() OVER (PARTITION BY dept ORDER BY sal DESC) AS r FROM emp");
        q.Table("emp").Insert("a", 1, 100);
        q.Table("emp").Insert("b", 1, 90);
        q.Table("emp").Insert("c", 2, 50);   // different partition — ranks restart
        q.Table("emp").Insert("d", 2, 60);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a", 1, 100, 1L));
        Assert.Equal(1, WeightOf(q.Current, "b", 1, 90, 2L));
        Assert.Equal(1, WeightOf(q.Current, "d", 2, 60, 1L));
        Assert.Equal(1, WeightOf(q.Current, "c", 2, 50, 2L));
    }

    [Fact]
    public void Rank_RetractionRerankSurvivingRows()
    {
        var q = Compile(Emp, "SELECT name, sal, RANK() OVER (ORDER BY sal DESC) AS r FROM emp");
        q.Table("emp").Insert("a", 1, 100);
        q.Table("emp").Insert("b", 1, 90);
        q.Table("emp").Insert("c", 1, 80);
        q.Step();

        // Retract the top row: 90 and 80 shift up to ranks 1 and 2. The delta must
        // retract the old ranks and insert the new ones.
        q.Table("emp").Delete("a", 1, 100);
        q.Step();

        Assert.Equal(-1, WeightOf(q.Current, "a", 100, 1L)); // gone
        Assert.Equal(-1, WeightOf(q.Current, "b", 90, 2L));  // 90 was rank 2 …
        Assert.Equal(1, WeightOf(q.Current, "b", 90, 1L));   // … now rank 1
        Assert.Equal(-1, WeightOf(q.Current, "c", 80, 3L));  // 80 was rank 3 …
        Assert.Equal(1, WeightOf(q.Current, "c", 80, 2L));   // … now rank 2
    }

    [Fact]
    public void RowNumber_NestedInCase_IsCurrentMarker()
    {
        // The financials `is_current` shape: ROW_NUMBER() OVER (PARTITION BY …
        // ORDER BY … DESC) = 1 nested in a CASE, marking the newest row per group.
        var q = Compile(Emp,
            "SELECT name, dept, CASE WHEN ROW_NUMBER() OVER " +
            "(PARTITION BY dept ORDER BY sal DESC) = 1 THEN 1 ELSE 0 END AS is_cur FROM emp");
        q.Table("emp").Insert("a", 1, 100);   // dept 1 top → current
        q.Table("emp").Insert("b", 1, 90);
        q.Table("emp").Insert("c", 2, 50);    // dept 2 top → current
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "a", 1, 1));
        Assert.Equal(1, WeightOf(q.Current, "b", 1, 0));
        Assert.Equal(1, WeightOf(q.Current, "c", 2, 1));
    }

    [Fact]
    public void Rank_MultiplicityDuplicateRows()
    {
        // A base row inserted twice (weight 2). RANK gives every copy the group's
        // shared rank, so the widened row keeps weight 2.
        var q = Compile(Emp, "SELECT name, sal, RANK() OVER (ORDER BY sal DESC) AS r FROM emp");
        q.Table("emp").Insert("a", 1, 100);
        q.Table("emp").Insert("a", 1, 100);   // duplicate → weight 2
        q.Table("emp").Insert("b", 1, 90);
        q.Step();

        Assert.Equal(2, WeightOf(q.Current, "a", 100, 1L)); // both copies rank 1
        Assert.Equal(1, WeightOf(q.Current, "b", 90, 3L));  // 1 + 2 rows before
    }

    // ---- Randomized incremental ≡ batch (the test of record) -----------------

    private const string W = "CREATE TABLE w (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)";

    [Theory]
    [InlineData("SELECT g, v, RANK() OVER (ORDER BY v DESC) AS r FROM w")]
    [InlineData("SELECT g, v, DENSE_RANK() OVER (ORDER BY v DESC) AS r FROM w")]
    [InlineData("SELECT g, v, ROW_NUMBER() OVER (ORDER BY v DESC, ts) AS r FROM w")]
    [InlineData("SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v DESC) AS r FROM w")]
    [InlineData("SELECT g, v, DENSE_RANK() OVER (PARTITION BY g ORDER BY v DESC) AS r FROM w")]
    [InlineData("SELECT g, ts, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC, ts) AS r FROM w")]
    // Multi-key ORDER BY with mixed directions (market_volatility / financials).
    [InlineData("SELECT g, ts, v, RANK() OVER (PARTITION BY g ORDER BY v DESC, ts ASC) AS r FROM w")]
    // Rank nested in a CASE (financials is_current).
    [InlineData("SELECT g, v, CASE WHEN ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC, ts) = 1 THEN 1 ELSE 0 END AS c FROM w")]
    public void IncrementalEqualsBatch_RandomInsertsAndDeletes(string query)
    {
        for (var seed = 0; seed < 16; seed++)
        {
            AssertIncrementalEqualsBatch(query, seed);
        }
    }

    private static void AssertIncrementalEqualsBatch(string query, int seed)
    {
        var rng = new Random(seed);
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(W));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        var compiled = PlanToCircuit.Compile(plan);

        var present = new List<object?[]>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
        for (var t = 0; t < 14; t++)
        {
            var tick = new List<InputEvent>();
            var ops = rng.Next(1, 4);
            for (var o = 0; o < ops; o++)
            {
                var del = present.Count > 0 && rng.NextDouble() < 0.35;
                if (del)
                {
                    var idx = rng.Next(present.Count);
                    var row = present[idx];
                    present.RemoveAt(idx);
                    tick.Add(new InputEvent("w", row, -1));
                }
                else
                {
                    // Small value domain so ties (equal ORDER BY values) occur
                    // frequently — the RANK / DENSE_RANK tie-group path and the
                    // ROW_NUMBER tiebreak are what this test exercises.
                    var row = new object?[] { rng.Next(0, 3), rng.Next(0, 4), rng.Next(0, 4) };
                    present.Add(row);
                    tick.Add(new InputEvent("w", row, 1));
                }
            }

            ticks.Add(tick);
        }

        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, ticks);

        var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["w"] = IncrementalOracle.NetTable(ticks.SelectMany(x => x), "w"),
        };
        var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        Assert.True(
            accumulated.Equals(batch),
            $"seed={seed} query={query}\n  accumulated={accumulated}\n  batch={batch}");
    }
}
