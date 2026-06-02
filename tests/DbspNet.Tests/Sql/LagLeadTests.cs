// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Tests.EndToEnd;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for the window offset functions <c>LAG</c> / <c>LEAD</c> —
/// <c>LAG/LEAD(expr [, offset [, default]]) OVER (PARTITION BY p ORDER BY o)</c>
/// emitted as a new output column. Positional (by row, not value), maintained
/// incrementally by per-partition recompute-and-diff.
/// </summary>
public class LagLeadTests
{
    private const string W = "CREATE TABLE w (g INT NOT NULL, ts INT NOT NULL, v INT NOT NULL)";

    private static LogicalPlan ResolvePlan(string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(W));
        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static WindowOffsetPlan OffsetPlanOf(string query)
    {
        var proj = Assert.IsType<ProjectPlan>(ResolvePlan(query));
        return Assert.IsType<WindowOffsetPlan>(proj.Input);
    }

    private static CompiledQuery Compile(string query) => PlanToCircuit.Compile(ResolvePlan(query));

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---- Resolver ------------------------------------------------------------

    [Fact]
    public void Resolve_Lag_DefaultsOffset1()
    {
        var wo = OffsetPlanOf("SELECT g, ts, LAG(v) OVER (PARTITION BY g ORDER BY ts) AS p FROM w");
        Assert.Single(wo.Functions);
        Assert.Equal(1, wo.Functions[0].Offset);
        Assert.False(wo.Functions[0].IsLead);
        Assert.Null(wo.Functions[0].Default);
    }

    [Fact]
    public void Resolve_LeadWithOffsetAndDefault()
    {
        var wo = OffsetPlanOf("SELECT g, ts, LEAD(v, 2, 0) OVER (PARTITION BY g ORDER BY ts) AS n FROM w");
        Assert.True(wo.Functions[0].IsLead);
        Assert.Equal(2, wo.Functions[0].Offset);
        Assert.Equal(0, wo.Functions[0].Default);
    }

    [Fact]
    public void Rejects_NoOrderBy() => Assert.Throws<ResolveException>(() =>
        ResolvePlan("SELECT g, LAG(v) OVER (PARTITION BY g) AS p FROM w"));

    [Fact]
    public void Rejects_Frame() => Assert.Throws<ResolveException>(() =>
        ResolvePlan("SELECT g, LAG(v) OVER (PARTITION BY g ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS p FROM w"));

    [Fact]
    public void Rejects_NonConstantOffset() => Assert.Throws<ResolveException>(() =>
        ResolvePlan("SELECT g, LAG(v, ts) OVER (PARTITION BY g ORDER BY ts) AS p FROM w"));

    [Fact]
    public void Rejects_MixedWithAggregate() => Assert.Throws<ResolveException>(() =>
        ResolvePlan("SELECT g, LAG(v) OVER (PARTITION BY g ORDER BY ts) AS p, " +
            "SUM(v) OVER (PARTITION BY g ORDER BY ts) AS s FROM w"));

    // ---- Behavioural ---------------------------------------------------------

    [Fact]
    public void Lag_PreviousRowPerPartition()
    {
        var q = Compile("SELECT g, ts, v, LAG(v) OVER (PARTITION BY g ORDER BY ts) AS prev FROM w");
        q.Table("w").Insert(1, 1, 10);
        q.Table("w").Insert(1, 2, 20);
        q.Table("w").Insert(1, 3, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10, null));  // first row: no predecessor
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 20, 10));
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 30, 20));
    }

    [Fact]
    public void Lead_NextRowPerPartition()
    {
        var q = Compile("SELECT g, ts, v, LEAD(v) OVER (PARTITION BY g ORDER BY ts) AS nx FROM w");
        q.Table("w").Insert(1, 1, 10);
        q.Table("w").Insert(1, 2, 20);
        q.Table("w").Insert(1, 3, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10, 20));
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 20, 30));
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 30, null)); // last row: no successor
    }

    [Fact]
    public void LagWithOffsetAndDefault()
    {
        var q = Compile("SELECT g, ts, v, LAG(v, 2, -1) OVER (PARTITION BY g ORDER BY ts) AS p FROM w");
        q.Table("w").Insert(1, 1, 10);
        q.Table("w").Insert(1, 2, 20);
        q.Table("w").Insert(1, 3, 30);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10, -1)); // no row 2 back → default
        Assert.Equal(1, WeightOf(q.Current, 1, 2, 20, -1));
        Assert.Equal(1, WeightOf(q.Current, 1, 3, 30, 10)); // 2 back = row 1
    }

    [Fact]
    public void MiddleInsert_ShiftsOnlyNeighbor()
    {
        var q = Compile("SELECT g, ts, v, LAG(v) OVER (PARTITION BY g ORDER BY ts) AS prev FROM w");
        q.Table("w").Insert(1, 1, 10);
        q.Table("w").Insert(1, 2, 20);
        q.Table("w").Insert(1, 3, 30);
        q.Step();

        // Insert another row at ts=1 (v=5). Total order ties break by full row, so
        // (1,1,5) precedes (1,1,10). Only (1,1,10)'s predecessor changes (NULL→5).
        q.Table("w").Insert(1, 1, 5);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 5, null));   // new first row
        Assert.Equal(-1, WeightOf(q.Current, 1, 1, 10, null)); // old value retracted
        Assert.Equal(1, WeightOf(q.Current, 1, 1, 10, 5));     // new predecessor
        // (1,2,20) and (1,3,30) are unchanged — no delta for them.
        Assert.Equal(0, WeightOf(q.Current, 1, 2, 20, 10));
        Assert.Equal(0, WeightOf(q.Current, 1, 3, 30, 20));
    }

    // ---- Randomized incremental ≡ batch --------------------------------------

    [Theory]
    [InlineData("SELECT g, ts, v, LAG(v) OVER (PARTITION BY g ORDER BY ts) AS p FROM w")]
    [InlineData("SELECT g, ts, v, LEAD(v) OVER (PARTITION BY g ORDER BY ts) AS n FROM w")]
    [InlineData("SELECT g, ts, v, LAG(v, 2, -1) OVER (PARTITION BY g ORDER BY ts) AS p FROM w")]
    [InlineData("SELECT ts, v, LAG(v) OVER (ORDER BY ts) AS p, LEAD(v) OVER (ORDER BY ts) AS n FROM w")]
    public void IncrementalEqualsBatch_RandomInsertsAndDeletes(string query)
    {
        for (var seed = 0; seed < 16; seed++)
        {
            var rng = new Random(seed);
            var compiled = Compile(query);
            var plan = ResolvePlan(query);

            var present = new List<object?[]>();
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
                        tick.Add(new InputEvent("w", present[idx], -1));
                        present.RemoveAt(idx);
                    }
                    else
                    {
                        var row = new object?[] { rng.Next(0, 3), rng.Next(0, 6), rng.Next(0, 5) };
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
}
