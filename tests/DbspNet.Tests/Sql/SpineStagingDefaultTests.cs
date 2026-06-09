// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The spine memtable is on by default whenever <see cref="TraceFamily.Spine"/>
/// is selected (docs §11): <see cref="CompileOptions.SpineStagingCapacity"/>
/// defaults to the §11 sweep knee (8,192) and the compiler realises it at trace
/// construction. These tests pin the default and confirm a spine compile with
/// the memtable on produces results identical to flat — across a join and an
/// aggregate, and with the memtable explicitly disabled.
/// </summary>
public class SpineStagingDefaultTests
{
    [Fact]
    public void DefaultCapacity_IsTheSweepKnee()
    {
        Assert.Equal(8192, new CompileOptions().SpineStagingCapacity);
        Assert.Equal(8192, CompileOptions.Default.SpineStagingCapacity);
    }

    private static LogicalPlan Plan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Theory]
    // Default spine (memtable on, capacity 8,192) and spine with the memtable
    // explicitly off must both equal flat.
    [InlineData(8192)]
    [InlineData(0)]
    public void SpineWithStaging_MatchesFlat_OnAggregate(int capacity)
    {
        var ddl = new[] { "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)" };
        var plan = Plan(ddl, "SELECT k, SUM(v) AS s FROM t GROUP BY k");

        var flat = PlanToCircuit.Compile(plan);
        var spine = PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine, SpineStagingCapacity = capacity });

        var rng = new Random(7);
        for (var tick = 0; tick < 40; tick++)
        {
            for (var i = 0; i < rng.Next(0, 5); i++)
            {
                int k = rng.Next(0, 10), v = rng.Next(0, 50);
                var w = rng.Next(0, 3) == 0 ? -1 : 1;
                if (w > 0) { flat.Table("t").Insert(k, v); spine.Table("t").Insert(k, v); }
                else { flat.Table("t").Delete(k, v); spine.Table("t").Delete(k, v); }
            }

            flat.Step();
            spine.Step();
            AssertSameDelta(flat.Current, spine.Current, tick);
        }
    }

    [Fact]
    public void SpineWithStaging_MatchesFlat_OnJoin()
    {
        var ddl = new[]
        {
            "CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
            "CREATE TABLE b (k INT NOT NULL, y INT NOT NULL)",
        };
        var plan = Plan(ddl, "SELECT a.x, b.y FROM a JOIN b ON a.k = b.k");

        var flat = PlanToCircuit.Compile(plan);
        var spine = PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine });

        var rng = new Random(11);
        for (var tick = 0; tick < 40; tick++)
        {
            foreach (var table in new[] { "a", "b" })
            {
                for (var i = 0; i < rng.Next(0, 4); i++)
                {
                    int k = rng.Next(0, 8), col = rng.Next(0, 50);
                    var w = rng.Next(0, 3) == 0 ? -1 : 1;
                    if (w > 0) { flat.Table(table).Insert(k, col); spine.Table(table).Insert(k, col); }
                    else { flat.Table(table).Delete(k, col); spine.Table(table).Delete(k, col); }
                }
            }

            flat.Step();
            spine.Step();
            AssertSameDelta(flat.Current, spine.Current, tick);
        }
    }

    private static void AssertSameDelta(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual, int tick)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (row, w) in expected)
        {
            Assert.True(w.Value == actual.WeightOf(row).Value, $"tick {tick}: weight of {row} differs");
        }
    }
}
