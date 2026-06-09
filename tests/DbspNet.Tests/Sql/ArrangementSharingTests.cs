// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Arrangement common-subexpression elimination
/// (<see cref="CompileOptions.ShareArrangements"/>, docs §9.6): when a relation
/// is the right input of ≥2 INNER joins on the same key, the compiler builds ONE
/// shared <c>Arrange</c>/<c>SpineArrange</c> and routes those joins through the
/// shared-right join. These tests pin both that the rule fires (operator
/// structure) and that it is semantically transparent (identical results).
/// </summary>
public class ArrangementSharingTests
{
    // dim is the shared dimension: right input of two facts' inner joins on k.
    private static readonly string[] StarSchema =
    {
        "CREATE TABLE dim (k INT NOT NULL, v INT NOT NULL)",
        "CREATE TABLE fact1 (k INT NOT NULL, a INT NOT NULL)",
        "CREATE TABLE fact2 (k INT NOT NULL, b INT NOT NULL)",
    };

    private const string StarQuery =
        "SELECT f.a AS x, dim.v AS v FROM fact1 f JOIN dim ON f.k = dim.k " +
        "UNION ALL " +
        "SELECT g.b AS x, dim.v AS v FROM fact2 g JOIN dim ON g.k = dim.k";

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

    // Force the structural path with the SAME codec on both arms, varying only
    // ShareArrangements — an honest A/B of the CSE rule.
    private static CompiledQuery CompileStructural(LogicalPlan plan, bool share, TraceFamily family) =>
        PlanToCircuit.Compile(
            plan,
            new CompileOptions { TraceFamily = family, ShareArrangements = share },
            EmittedEqualityCodec.Instance);

    private static int OpCount(CompiledQuery q, string namePrefix) =>
        q.Circuit.Operators.Count(o => o.GetType().Name.StartsWith(namePrefix, StringComparison.Ordinal));

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Theory]
    [InlineData(TraceFamily.Flat, "ArrangeOp", "IncrementalJoinSharedRightOp")]
    [InlineData(TraceFamily.Spine, "SpineArrangeOp", "SpineIncrementalJoinSharedRightOp")]
    public void Rule_Fires_OneSharedArrangement_TwoSharedJoins(
        TraceFamily family, string arrangeOp, string sharedJoinOp)
    {
        var plan = Plan(StarSchema, StarQuery);

        var shared = CompileStructural(plan, share: true, family);
        // dim is arranged ONCE and read by BOTH joins.
        Assert.Equal(1, OpCount(shared, arrangeOp));
        Assert.Equal(2, OpCount(shared, sharedJoinOp));

        var unshared = CompileStructural(plan, share: false, family);
        // Without the rule: no arrangement op, no shared-right joins.
        Assert.Equal(0, OpCount(unshared, arrangeOp));
        Assert.Equal(0, OpCount(unshared, sharedJoinOp));
    }

    [Fact]
    public void Rule_DoesNotFire_WhenRelationJoinedOnce()
    {
        // dim is joined by only fact1 — no reuse, so nothing to share.
        var plan = Plan(StarSchema, "SELECT f.a AS x, dim.v AS v FROM fact1 f JOIN dim ON f.k = dim.k");
        var q = CompileStructural(plan, share: true, TraceFamily.Flat);
        Assert.Equal(0, OpCount(q, "ArrangeOp"));
        Assert.Equal(0, OpCount(q, "IncrementalJoinSharedRightOp"));
    }

    [Theory]
    [InlineData(TraceFamily.Flat)]
    [InlineData(TraceFamily.Spine)]
    public void SharedResults_MatchUnshared(TraceFamily family)
    {
        var plan = Plan(StarSchema, StarQuery);
        var shared = CompileStructural(plan, share: true, family);
        var unshared = CompileStructural(plan, share: false, family);

        var rng = new Random(4242);
        for (var tick = 0; tick < 30; tick++)
        {
            // Generate this tick's mutations ONCE and apply the identical set to
            // both circuits, so any output difference is the CSE rule's doing,
            // not divergent inputs.
            var ops = new[] { "dim", "fact1", "fact2" }.SelectMany(t => GenerateOps(t, rng)).ToList();
            foreach (var (table, isDelete, k, v) in ops)
            {
                if (isDelete)
                {
                    shared.Table(table).Delete(k, v);
                    unshared.Table(table).Delete(k, v);
                }
                else
                {
                    shared.Table(table).Insert(k, v);
                    unshared.Table(table).Insert(k, v);
                }
            }

            shared.Step();
            unshared.Step();
            AssertSameDelta(unshared.Current, shared.Current, tick);
        }
    }

    private static System.Collections.Generic.IEnumerable<(string Table, bool IsDelete, int K, int V)> GenerateOps(
        string table, Random rng)
    {
        var n = rng.Next(0, 3);
        for (var i = 0; i < n; i++)
        {
            yield return (table, rng.Next(0, 3) == 0, rng.Next(0, 8), rng.Next(0, 100));
        }
    }

    private static void AssertSameDelta(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual, int tick)
    {
        Assert.Equal(expected.Count, actual.Count);
        foreach (var (row, w) in expected)
        {
            Assert.True(
                w.Value == actual.WeightOf(row).Value,
                $"tick {tick}: weight of {row} differs ({actual.WeightOf(row).Value} vs {w.Value})");
        }
    }
}
