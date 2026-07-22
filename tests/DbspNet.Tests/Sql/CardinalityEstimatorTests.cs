// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// The rough row-count estimate that gates production broadcast joins: base
/// relations resolve to their supplied count, an unknown relation is "large"
/// (<see cref="CardinalityEstimator.Unknown"/>), and the estimate propagates
/// through the plan operators the broadcast heuristic can see.
/// </summary>
public class CardinalityEstimatorTests
{
    private static LogicalPlan Plan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query);
    }

    private static Func<string, long?> Counts(params (string Name, long Rows)[] entries)
    {
        var map = entries.ToDictionary(e => e.Name, e => e.Rows, StringComparer.Ordinal);
        return name => map.TryGetValue(name, out var n) ? n : (long?)null;
    }

    [Fact]
    public void BaseScan_UsesSuppliedCount()
    {
        var plan = Plan(["CREATE TABLE t (k INT NOT NULL)"], "SELECT k FROM t");
        Assert.Equal(42, CardinalityEstimator.Estimate(plan, Counts(("t", 42))));
    }

    [Fact]
    public void UnknownRelation_IsLarge()
    {
        var plan = Plan(["CREATE TABLE t (k INT NOT NULL)"], "SELECT k FROM t");
        Assert.Equal(CardinalityEstimator.Unknown, CardinalityEstimator.Estimate(plan, Counts()));
    }

    [Fact]
    public void FilterAndProject_PassThrough()
    {
        var plan = Plan(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT v + 1 FROM t WHERE k > 3");
        Assert.Equal(100, CardinalityEstimator.Estimate(plan, Counts(("t", 100))));
    }

    [Fact]
    public void InnerJoin_TakesMaxOfSides()
    {
        var plan = Plan(
            [
                "CREATE TABLE fact (k INT NOT NULL)",
                "CREATE TABLE dim (k INT NOT NULL)",
            ],
            "SELECT fact.k FROM fact JOIN dim ON fact.k = dim.k");
        Assert.Equal(1000, CardinalityEstimator.Estimate(plan, Counts(("fact", 1000), ("dim", 5))));
    }

    [Fact]
    public void UnionAll_SumsBranches()
    {
        var plan = Plan(
            [
                "CREATE TABLE a (k INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL)",
            ],
            "SELECT k FROM a UNION ALL SELECT k FROM b");
        Assert.Equal(30, CardinalityEstimator.Estimate(plan, Counts(("a", 10), ("b", 20))));
    }

    [Fact]
    public void Aggregate_AtMostInput()
    {
        var plan = Plan(
            ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");
        // Estimate stays the input bound (one row per group <= input rows).
        Assert.Equal(500, CardinalityEstimator.Estimate(plan, Counts(("t", 500))));
    }

    [Fact]
    public void UnknownSide_PoisonsToLarge()
    {
        // A join with one unknown side is large (never a broadcastable dimension).
        var plan = Plan(
            [
                "CREATE TABLE fact (k INT NOT NULL)",
                "CREATE TABLE dim (k INT NOT NULL)",
            ],
            "SELECT fact.k FROM fact JOIN dim ON fact.k = dim.k");
        Assert.Equal(CardinalityEstimator.Unknown, CardinalityEstimator.Estimate(plan, Counts(("dim", 5))));
    }
}
