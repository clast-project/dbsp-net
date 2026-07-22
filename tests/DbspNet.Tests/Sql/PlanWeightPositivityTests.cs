// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Unit tests for the lineage analysis behind the non-linear narrowing unblock
/// (docs/design-row-representation.md §18.6): can a plan's integrated Z-set ever
/// hold a negative weight?
///
/// <para>The analysis is deliberately one-sided — a wrong <c>false</c> costs an
/// optimization, a wrong <c>true</c> costs correctness — so these tests pin both the
/// cases it must accept and the cases it must refuse.</para>
/// </summary>
public class PlanWeightPositivityTests
{
    private const string PlainDdl =
        "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL, x INT NOT NULL)";

    private const string AppendOnlyDdl =
        "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL, x INT NOT NULL) " +
        "WITH ('append_only' = 'true')";

    private const string AppendOnlyDdl2 =
        "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL) WITH ('append_only' = 'true')";

    [Fact]
    public void UndeclaredTable_IsNotProvablyNonNegative()
    {
        // No declaration ⇒ the engine's contract allows an arbitrary signed Z-set
        // (the random-query PBT deliberately emits deletes of never-inserted rows).
        Assert.False(Analyze("SELECT * FROM t"));
    }

    [Fact]
    public void AppendOnlyTable_IsNonNegative()
    {
        Assert.True(Analyze("SELECT * FROM a"));
    }

    [Theory]
    // Filter / Project / Join / UnionAll are weight-preserving or multiplicative:
    // non-negative exactly when their inputs are.
    [InlineData("SELECT k, v FROM a WHERE v > 1", true)]
    [InlineData("SELECT k, v FROM t WHERE v > 1", false)]
    [InlineData("SELECT a.k FROM a JOIN b ON a.k = b.k", true)]
    [InlineData("SELECT a.k FROM a JOIN t ON a.k = t.k", false)]
    [InlineData("SELECT k FROM a UNION ALL SELECT k FROM b", true)]
    [InlineData("SELECT k FROM a UNION ALL SELECT k FROM t", false)]
    public void PropagatesThroughWeightPreservingOperators(string sql, bool expected)
    {
        Assert.Equal(expected, Analyze(sql));
    }

    [Theory]
    // DISTINCT and aggregation launder sign: their cumulative output is 0/1 per row
    // (DistinctOp) or one row per live group, whatever the input weights were.
    [InlineData("SELECT DISTINCT k, v FROM t")]
    [InlineData("SELECT k, SUM(v) FROM t GROUP BY k")]
    [InlineData("SELECT k FROM t UNION SELECT k FROM t")]
    public void LaunderingOperators_AreNonNegative_WithoutAnyDeclaration(string sql)
    {
        Assert.True(Analyze(sql));
    }

    [Fact]
    public void Difference_IsRefused_EvenWhenBothSidesAreAppendOnly()
    {
        // `EXCEPT` lowers to Distinct(a) − Intersect(a, b) — a DifferencePlan, which
        // is precisely how a negative weight is created. (In this particular shape
        // the result happens to stay non-negative, since the subtrahend is a subset;
        // the analysis does not reason about that, and accepting the false negative
        // is the right trade for a rule whose failure mode is silent wrong answers.)
        Assert.False(Analyze("SELECT k, v FROM a EXCEPT SELECT k, v FROM b"));
    }

    [Fact]
    public void CteReferencedTwice_IsNotMistakenForACycle()
    {
        // The walk pops each CteRef after descending, so a CTE used in two sibling
        // positions resolves on its merits both times rather than tripping the
        // recursion guard.
        Assert.True(Analyze(
            "WITH c AS (SELECT k, v FROM a) " +
            "SELECT c1.k FROM c AS c1 JOIN c AS c2 ON c1.k = c2.k"));

        Assert.False(Analyze(
            "WITH c AS (SELECT k, v FROM t) " +
            "SELECT c1.k FROM c AS c1 JOIN c AS c2 ON c1.k = c2.k"));
    }

    [Fact]
    public void RecursiveCte_IsRefused()
    {
        // Conservative: the analysis does not reason about a fixpoint's weights.
        Assert.False(Analyze(
            "WITH RECURSIVE r AS (" +
            "  SELECT k, v FROM a " +
            "  UNION ALL " +
            "  SELECT r.k + 1, r.v FROM r WHERE r.k < 3) " +
            "SELECT k, v FROM r"));
    }

    private static bool Analyze(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in new[] { PlainDdl, AppendOnlyDdl, AppendOnlyDdl2 })
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanWeightPositivity.IsNonNegative(plan);
    }
}
