// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Circuit-level operator fusion: a maximal run of consecutive Filter/Project
/// plan nodes lowers to a single fused <c>Apply</c> pass instead of one operator
/// per node, on both the typed and structural compile paths. These tests pin
/// both the fusion (operator count) and its semantic transparency (results
/// identical to the staged form).
/// </summary>
public class OperatorFusionTests
{
    public enum Mode
    {
        /// <summary>Default codec → the typed fast path.</summary>
        Typed,

        /// <summary>Non-default codec disables the typed path → structural lowering.</summary>
        Structural,
    }

    // No PlanOptimizer pass, so the resolver's nested Filter/Project nodes
    // survive to the compiler — fusion is what collapses them.
    private static CompiledQuery Compile(Mode mode, string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return mode == Mode.Structural
            ? PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance)
            : PlanToCircuit.Compile(plan);
    }

    // Pointwise linear passes (Filter / non-identity Project) compile to a Core
    // ApplyOp. Scans (and, on the typed path, boundary conversions) contribute a
    // fixed number of their own ApplyOps, so chain length is measured relative to
    // a passthrough baseline over the same table.
    private static int ApplyOpCount(CompiledQuery q) =>
        q.Circuit.Operators.Count(o =>
            o.GetType().Name.StartsWith("ApplyOp", System.StringComparison.Ordinal));

    [Theory]
    [InlineData(Mode.Typed)]
    [InlineData(Mode.Structural)]
    public void MapFilterMapFilter_FusesToSingleApply(Mode mode)
    {
        // Project(Filter(Project(Filter(Scan)))) — four pointwise stages over t.
        var chain = Compile(
            mode,
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT x * 2 AS y FROM (SELECT a + 1 AS x FROM t WHERE a > 0) s WHERE x < 100");

        // SELECT * is an identity projection — pure passthrough, no runtime
        // stage — so its ApplyOp count is the scan/boundary-only baseline.
        var baseline = Compile(mode, ["CREATE TABLE t (a INT NOT NULL)"], "SELECT * FROM t");

        // The four-stage chain adds exactly ONE ApplyOp over the baseline.
        // Without fusion it would add four.
        Assert.Equal(ApplyOpCount(baseline) + 1, ApplyOpCount(chain));
    }

    [Theory]
    [InlineData(Mode.Typed)]
    [InlineData(Mode.Structural)]
    public void ChainLength_DoesNotChangeApplyCount(Mode mode)
    {
        // A two-stage and a four-stage chain both fold to a single Apply, so
        // their ApplyOp counts are identical.
        var two = Compile(
            mode,
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT a + 1 AS x FROM t WHERE a > 0");
        var four = Compile(
            mode,
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT x * 2 AS y FROM (SELECT a + 1 AS x FROM t WHERE a > 0) s WHERE x < 100");

        Assert.Equal(ApplyOpCount(two), ApplyOpCount(four));
    }

    [Theory]
    [InlineData(Mode.Typed)]
    [InlineData(Mode.Structural)]
    public void FusedChain_ProducesCorrectResultsUnderInsertAndDelete(Mode mode)
    {
        // Semantic transparency: the fused pass must compute exactly what the
        // staged map→filter→map→filter chain would.
        var q = Compile(
            mode,
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT x * 2 AS y FROM (SELECT a + 1 AS x FROM t WHERE a > 0) s WHERE x < 100");

        q.Table("t").Insert(-5);   // a > 0 fails → dropped
        q.Table("t").Insert(3);    // x=4   → y=8
        q.Table("t").Insert(49);   // x=50  → y=100
        q.Table("t").Insert(100);  // x=101 → x < 100 fails → dropped
        q.Table("t").Insert(200);  // a > 0 but x=201 → x < 100 fails → dropped
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, q.WeightOf(8).Value);
        Assert.Equal(1, q.WeightOf(100).Value);

        // Delete one survivor and one already-filtered row. Current holds the
        // per-tick DELTA: just the retraction of y=8 (the filtered row's delete
        // never reached the output).
        q.Table("t").Delete(3);    // retracts y=8
        q.Table("t").Delete(200);  // no-op on the output (was filtered)
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(-1, q.WeightOf(8).Value);
        Assert.Equal(0, q.WeightOf(100).Value);
    }

    [Theory]
    [InlineData(Mode.Typed)]
    [InlineData(Mode.Structural)]
    public void FusedChain_AccumulatesCollidingProjectionOutputs(Mode mode)
    {
        // Two distinct inputs that the projection collapses to the same output
        // row must accumulate weights — the property that makes per-row folding
        // equivalent to staged MapKeys.
        var q = Compile(
            mode,
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT a % 2 AS p FROM t WHERE a >= 0");

        q.Table("t").Insert(1);  // p=1
        q.Table("t").Insert(3);  // p=1
        q.Table("t").Insert(2);  // p=0
        q.Table("t").Insert(-4); // dropped by filter
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(2, q.WeightOf(1).Value); // 1 and 3 collide
        Assert.Equal(1, q.WeightOf(0).Value);
    }
}
