// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for IN / NOT IN / EXISTS / NOT EXISTS appearing in SELECT,
/// HAVING, and nested-boolean positions (i.e. outside the WHERE-conjunct
/// row-filter shape that already lifts to <see cref="SemiJoinPlan"/>).
/// </summary>
public class NonWhereSubqueryTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Fact]
    public void CorrelatedExists_InSelect_AppendsPerRowBoolean()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region) AS has_vip FROM orders");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 2);
        q.Table("vips").Insert(99, 1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 2, false));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void CorrelatedNotExists_InSelect_AppendsPerRowBoolean()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, NOT EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region) AS no_vip FROM orders");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 2);
        q.Table("vips").Insert(99, 1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, false));
        Assert.Equal(1, WeightOf(q.Current, 2, true));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void UncorrelatedIn_InSelect_AppendsPerRowBoolean()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, cust IN (SELECT vid FROM vips) AS is_vip FROM orders");

        q.Table("orders").Insert(1);
        q.Table("orders").Insert(2);
        q.Table("orders").Insert(3);
        q.Table("vips").Insert(1);
        q.Table("vips").Insert(3);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 2, false));
        Assert.Equal(1, WeightOf(q.Current, 3, true));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void CorrelatedIn_InSelect_AppendsPerRowBoolean()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region, " +
            "cust IN (SELECT vid FROM vips WHERE vips.region = orders.region) AS hot FROM orders");

        q.Table("orders").Insert(1, 1);  // region 1 cust 1 — vips in r1 contains 1 → hot
        q.Table("orders").Insert(2, 1);  // region 1 cust 2 — not in vips for r1 → cold
        q.Table("orders").Insert(7, 2);  // region 2 cust 7 — no vips in r2 → cold
        q.Table("vips").Insert(1, 1);    // r1: vip 1
        q.Table("vips").Insert(99, 1);   // r1: vip 99
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 2, 1, false));
        Assert.Equal(1, WeightOf(q.Current, 7, 2, false));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void CorrelatedNotIn_InSelect_AppendsPerRowBoolean()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT cust, region, " +
            "cust NOT IN (SELECT vid FROM vips WHERE vips.region = orders.region) AS not_hot FROM orders");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 1);
        q.Table("orders").Insert(7, 2);
        q.Table("vips").Insert(1, 1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 1, false));
        Assert.Equal(1, WeightOf(q.Current, 2, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 7, 2, true));
        Assert.Equal(3, q.Current.Count);
    }

    // ---------- Nullable-operand IN / NOT IN: full SQL three-valued logic ----------

    [Fact]
    public void NullableProbe_InSelectIn_ThreeValued()
    {
        // vips column is NOT NULL, so the only NULL source is the probe.
        //   cust=1    → TRUE   (matches)
        //   cust=3    → FALSE  (no match, subquery has no NULLs)
        //   cust=NULL → NULL   (NULL probe against a non-empty subquery)
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, cust IN (SELECT vid FROM vips) AS is_vip FROM orders");

        q.Table("vips").Insert(1);
        q.Table("vips").Insert(2);
        q.Table("orders").Insert(1);
        q.Table("orders").Insert(3);
        q.Table("orders").Insert((object?)null);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 3, false));
        Assert.Equal(1, WeightOf(q.Current, null, null));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void NullableSubqueryColumn_InSelectIn_ThreeValued()
    {
        // vips contains a NULL value, so a non-matching probe is UNKNOWN.
        //   cust=1 → TRUE   (matches the non-null 1)
        //   cust=3 → NULL   (no match, but the subquery contains a NULL)
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL)",
                "CREATE TABLE vips (vid INT)",
            ],
            "SELECT cust, cust IN (SELECT vid FROM vips) AS is_vip FROM orders");

        q.Table("vips").Insert(1);
        q.Table("vips").Insert((object?)null);
        q.Table("orders").Insert(1);
        q.Table("orders").Insert(3);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 3, null));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void NullableSubqueryColumn_NotInSelect_ThreeValued()
    {
        // The classic NOT IN + NULL gotcha: NOT IN is the 3VL negation.
        //   cust=1 → FALSE  (1 IN → TRUE)
        //   cust=3 → NULL   (3 IN → NULL, NOT NULL = NULL)
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL)",
                "CREATE TABLE vips (vid INT)",
            ],
            "SELECT cust, cust NOT IN (SELECT vid FROM vips) AS not_vip FROM orders");

        q.Table("vips").Insert(1);
        q.Table("vips").Insert((object?)null);
        q.Table("orders").Insert(1);
        q.Table("orders").Insert(3);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, false));
        Assert.Equal(1, WeightOf(q.Current, 3, null));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void NullableProbe_InSelectIn_EmptySubquery_IsFalseNotNull()
    {
        // `x IN (empty)` is FALSE for every x — including a NULL probe. This is
        // the edge the per-group total count exists to get right.
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, cust IN (SELECT vid FROM vips) AS is_vip FROM orders");

        // vips stays empty.
        q.Table("orders").Insert(5);
        q.Table("orders").Insert((object?)null);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5, false));
        Assert.Equal(1, WeightOf(q.Current, null, false));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void NullableCorrelatedIn_InSelect_ThreeValuedPerGroup()
    {
        // Correlated nullable IN: total / null / match counts are per
        // correlation group (region).
        //   (1,10) → TRUE   (1 ∈ {1,NULL} for region 10)
        //   (2,10) → NULL   (no match, region-10 group has a NULL)
        //   (5,20) → TRUE   (5 ∈ {5} for region 20)
        //   (9,30) → FALSE  (region-30 group is empty)
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT, region INT NOT NULL)",
            ],
            "SELECT cust, cust IN (SELECT vid FROM vips WHERE vips.region = orders.region) AS is_vip " +
            "FROM orders");

        q.Table("vips").Insert(1, 10);
        q.Table("vips").Insert((object?)null, 10);
        q.Table("vips").Insert(5, 20);
        q.Table("orders").Insert(1, 10);
        q.Table("orders").Insert(2, 10);
        q.Table("orders").Insert(5, 20);
        q.Table("orders").Insert(9, 30);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, true));
        Assert.Equal(1, WeightOf(q.Current, 2, null));
        Assert.Equal(1, WeightOf(q.Current, 5, true));
        Assert.Equal(1, WeightOf(q.Current, 9, false));
        Assert.Equal(4, q.Current.Count);
    }

    // ---------- HAVING positions ----------

    [Fact]
    public void UncorrelatedIn_InHaving_FiltersGroups()
    {
        // After GROUP BY, HAVING restricts to groups whose key passes the
        // IN-test. NOT NULL operands.
        var q = Compile(
            [
                "CREATE TABLE orders (region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE hot_regions (r INT NOT NULL)",
            ],
            "SELECT region, COUNT(*) FROM orders GROUP BY region " +
            "HAVING region IN (SELECT r FROM hot_regions)");

        q.Table("orders").Insert(1, 10);
        q.Table("orders").Insert(1, 20);
        q.Table("orders").Insert(2, 30);  // region 2: not hot
        q.Table("orders").Insert(3, 40);  // region 3: hot
        q.Table("hot_regions").Insert(1);
        q.Table("hot_regions").Insert(3);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 2L));  // region 1: 2 rows
        Assert.Equal(1, WeightOf(q.Current, 3, 1L));  // region 3: 1 row
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void CorrelatedExists_InHaving_FiltersGroups()
    {
        // HAVING with correlated EXISTS on the group-by column.
        var q = Compile(
            [
                "CREATE TABLE orders (region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL, region INT NOT NULL)",
            ],
            "SELECT region, COUNT(*) FROM orders GROUP BY region " +
            "HAVING EXISTS (SELECT 1 FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 10);
        q.Table("orders").Insert(1, 20);
        q.Table("orders").Insert(2, 30);
        q.Table("vips").Insert(99, 1);  // only region 1 has vips
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 2L));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void UncorrelatedIn_InAggregateSelectProjection_AppendsBoolean()
    {
        // SELECT-position IN-subquery alongside aggregates: appears as a
        // boolean column emitted per group.
        var q = Compile(
            [
                "CREATE TABLE orders (region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE hot_regions (r INT NOT NULL)",
            ],
            "SELECT region, COUNT(*), region IN (SELECT r FROM hot_regions) AS hot " +
            "FROM orders GROUP BY region");

        q.Table("orders").Insert(1, 10);
        q.Table("orders").Insert(2, 20);
        q.Table("orders").Insert(3, 30);
        q.Table("hot_regions").Insert(1);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 1L, true));
        Assert.Equal(1, WeightOf(q.Current, 2, 1L, false));
        Assert.Equal(1, WeightOf(q.Current, 3, 1L, false));
        Assert.Equal(3, q.Current.Count);
    }
}
