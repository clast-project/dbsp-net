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
/// Stored / integrated output (<see cref="CompileOptions.StoredOutput"/>): the
/// circuit still emits deltas, but <see cref="CompiledQuery.CurrentView"/> holds
/// the full materialized view contents. The test of record is differential — the
/// integrated view must equal both the externally-accumulated deltas and the batch
/// re-computation, across the SQL surface (filter, join, aggregate, DISTINCT,
/// rank-in-output, UNION ALL multiplicity).
/// </summary>
public class StoredOutputTests
{
    private static readonly CompileOptions Stored = new() { StoredOutput = true };

    private static (CompiledQuery Query, LogicalPlan Plan) CompileStored(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return (PlanToCircuit.Compile(plan, snapshotCodecs: null, Stored), plan);
    }

    [Fact]
    public void DeltaStillFlows_AlongsideView()
    {
        var (q, _) = CompileStored(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a, b FROM t WHERE b > 0");
        Assert.True(q.HasStoredOutput);

        q.Table("t").Insert(1, 10);
        q.Step();
        var row1 = new StructuralRow(new object?[] { 1, 10 });
        Assert.Equal(1, q.Current.WeightOf(row1).Value);      // delta
        Assert.Equal(1, q.CurrentView.WeightOf(row1).Value);  // view

        q.Table("t").Insert(2, 20);
        q.Step();
        // Delta carries only the new row; the view retains both.
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(2, q.CurrentView.Count);
    }

    [Fact]
    public void CurrentView_Throws_WhenNotStored()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement("SELECT a FROM t"))).Query;
        var q = PlanToCircuit.Compile(plan); // no StoredOutput

        Assert.False(q.HasStoredOutput);
        q.Table("t").Insert(1);
        q.Step();
        Assert.Throws<InvalidOperationException>(() => q.CurrentView);
    }

    [Theory]
    [InlineData("SELECT a, b FROM t WHERE b > 1")]
    [InlineData("SELECT a, SUM(b) AS s FROM t GROUP BY a")]
    [InlineData("SELECT a, COUNT(*) AS c FROM t GROUP BY a")]
    [InlineData("SELECT DISTINCT a FROM t")]
    // Rank-in-output (the feature just shipped) integrated to full state.
    [InlineData("SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b DESC) AS r FROM t")]
    [InlineData("SELECT a, b, DENSE_RANK() OVER (ORDER BY b DESC) AS r FROM t")]
    // UNION ALL retains multiplicity in the view.
    [InlineData("SELECT a, b FROM t WHERE b > 1 UNION ALL SELECT a, b FROM t WHERE b < 3")]
    public void ViewEqualsAccumulatedDeltasAndBatch_SingleTable(string sql)
    {
        const string ddl = "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)";
        for (var seed = 0; seed < 12; seed++)
        {
            var (q, plan) = CompileStored([ddl], sql);
            var ticks = RandomTicks(seed, "t", cols: 2, domain: 4);

            // Run the stored-output circuit; RunAndAccumulate sums the DELTAS.
            var accumulated = IncrementalOracle.RunAndAccumulate(q, ticks);

            // Law 1: the integrated view equals the externally-accumulated deltas.
            Assert.True(
                q.CurrentView.Equals(accumulated),
                $"view≠accumulated seed={seed} sql={sql}\n  view={q.CurrentView}\n  accum={accumulated}");

            // Law 2: and equals the batch re-computation over the net input.
            var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
            {
                ["t"] = IncrementalOracle.NetTable(ticks.SelectMany(x => x), "t"),
            };
            var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var batch = BatchPlanEvaluator.Evaluate(plan, ctx);
            Assert.True(
                q.CurrentView.Equals(batch),
                $"view≠batch seed={seed} sql={sql}\n  view={q.CurrentView}\n  batch={batch}");
        }
    }

    [Fact]
    public void ViewEqualsBatch_Join()
    {
        string[] ddl =
        [
            "CREATE TABLE l (k INT NOT NULL, x INT NOT NULL)",
            "CREATE TABLE r (k INT NOT NULL, y INT NOT NULL)",
        ];
        const string sql = "SELECT l.x, r.y FROM l JOIN r ON l.k = r.k";

        for (var seed = 0; seed < 12; seed++)
        {
            var (q, plan) = CompileStored(ddl, sql);
            var rng = new Random(seed);
            var ticks = new List<IReadOnlyList<InputEvent>>();
            var presentL = new List<object?[]>();
            var presentR = new List<object?[]>();
            for (var t = 0; t < 12; t++)
            {
                var tick = new List<InputEvent>();
                foreach (var (table, present) in new[] { ("l", presentL), ("r", presentR) })
                {
                    var del = present.Count > 0 && rng.NextDouble() < 0.3;
                    if (del)
                    {
                        var idx = rng.Next(present.Count);
                        tick.Add(new InputEvent(table, present[idx], -1));
                        present.RemoveAt(idx);
                    }
                    else
                    {
                        var row = new object?[] { rng.Next(0, 3), rng.Next(0, 5) };
                        present.Add(row);
                        tick.Add(new InputEvent(table, row, 1));
                    }
                }

                ticks.Add(tick);
            }

            IncrementalOracle.RunAndAccumulate(q, ticks);

            var tableStates = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
            {
                ["l"] = IncrementalOracle.NetTable(ticks.SelectMany(x => x), "l"),
                ["r"] = IncrementalOracle.NetTable(ticks.SelectMany(x => x), "r"),
            };
            var ctx = new BatchEvalContext(tableStates, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            var batch = BatchPlanEvaluator.Evaluate(plan, ctx);
            Assert.True(q.CurrentView.Equals(batch), $"view≠batch seed={seed}");
        }
    }

    private static List<IReadOnlyList<InputEvent>> RandomTicks(int seed, string table, int cols, int domain)
    {
        var rng = new Random(seed);
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
                    tick.Add(new InputEvent(table, present[idx], -1));
                    present.RemoveAt(idx);
                }
                else
                {
                    var row = new object?[cols];
                    for (var i = 0; i < cols; i++)
                    {
                        row[i] = rng.Next(0, domain);
                    }

                    present.Add(row);
                    tick.Add(new InputEvent(table, row, 1));
                }
            }

            ticks.Add(tick);
        }

        return ticks;
    }
}
