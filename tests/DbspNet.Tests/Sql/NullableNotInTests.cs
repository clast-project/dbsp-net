// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Coverage for <c>x NOT IN (subquery)</c> with nullable probe or
/// nullable subquery column. SQL three-valued logic drops the row
/// when either <c>x</c> is NULL or any value in the subquery (per
/// correlation group, when correlated) is NULL. The resolver layers
/// a per-correlation-group null-count hidden column then filters on
/// <c>probe IS NOT NULL AND (null_count IS NULL OR null_count = 0)</c>
/// before the anti-semi-join.
/// </summary>
public class NullableNotInTests
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

    // ---------- Uncorrelated NOT IN with nullable operands ----------

    [Fact]
    public void Uncorrelated_NullProbe_Filtered()
    {
        // NULL probe → predicate is NULL → row drops, even when the
        // subquery is empty (which would otherwise let any non-NULL probe
        // pass).
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("orders").Insert(null, 50);
        q.Step();

        // cust=1 not in (empty) → passes. cust=NULL → drops (NULL predicate).
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Uncorrelated_NullInSubquery_DropsAllNonMatched()
    {
        // sq has a NULL → predicate is NULL for any non-matched probe.
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("orders").Insert(2, 50);
        q.Table("vips").Insert(3);   // not matching either order
        q.Table("vips").Insert((object?)null); // the load-bearing NULL
        q.Step();

        // 1 and 2 are non-matched but sq has NULL → both drop.
        Assert.True(q.Current.IsEmpty);
    }

    [Fact]
    public void Uncorrelated_RemovingLoneNull_UnDropsRows()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        q.Table("orders").Insert(1, 100);
        q.Table("vips").Insert((object?)null);
        q.Step();
        Assert.True(q.Current.IsEmpty);

        // Retract the NULL — order re-emerges.
        q.Table("vips").Delete((object?)null);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100));
    }

    [Fact]
    public void Uncorrelated_NullableButNoRuntimeNulls_MatchesNotNullPath()
    {
        // Both columns are typed nullable but no actual NULLs at runtime.
        // Output should match the NOT NULL version row-for-row.
        var nullable = Compile(
            [
                "CREATE TABLE orders (cust INT, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        var notnull = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT NOT NULL)",
            ],
            "SELECT cust, amt FROM orders WHERE cust NOT IN (SELECT vid FROM vips)");

        void Tick(Action<CompiledQuery> apply)
        {
            apply(nullable); apply(notnull);
            nullable.Step(); notnull.Step();
            Assert.Equal(notnull.Current, nullable.Current);
        }

        Tick(q => { q.Table("orders").Insert(1, 100); q.Table("orders").Insert(2, 50); });
        Tick(q => { q.Table("vips").Insert(1); });
        Tick(q => { q.Table("orders").Insert(3, 25); });
    }

    // ---------- Correlated NOT IN with nullable operands ----------

    [Fact]
    public void Correlated_NullInOneRegion_DropsOnlyThatRegionsOrders()
    {
        // SQL 3VL is per-correlation-group: a NULL in region A's vips drops
        // region A's orders but leaves region B's orders intact.
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL, amt INT NOT NULL)",
                "CREATE TABLE vips (vid INT, region INT NOT NULL)",
            ],
            "SELECT cust, region, amt FROM orders " +
            "WHERE cust NOT IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1, 100);  // region 1 cust 1
        q.Table("orders").Insert(2, 2, 50);   // region 2 cust 2
        q.Table("vips").Insert(99, 1);        // region 1: a non-matching vip
        q.Table("vips").Insert(null, 1);      // region 1: a NULL — drops cust 1
        q.Table("vips").Insert(7, 2);         // region 2: vip 7 (cust 2 ≠ 7) — no NULL → passes
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 2, 2, 50));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Correlated_EmptyCorrelationGroup_PassesOuter()
    {
        // No inner rows for an outer row's correlation group → the LEFT JOIN
        // for null_count returns NULL, the predicate accepts NULL count, and
        // the anti-semi-join trivially admits the row (no match).
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, region INT NOT NULL)",
                "CREATE TABLE vips (vid INT, region INT NOT NULL)",
            ],
            "SELECT cust, region FROM orders " +
            "WHERE cust NOT IN (SELECT vid FROM vips WHERE vips.region = orders.region)");

        q.Table("orders").Insert(1, 1);
        q.Table("orders").Insert(2, 2);
        // Only insert vips for region 2; region 1 has no inner rows at all.
        q.Table("vips").Insert(99, 2);
        q.Step();

        // Both orders pass: region 1 has no matching vips at all; region 2
        // has a non-matching vip.
        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 2, 2));
        Assert.Equal(2, q.Current.Count);
    }
}
