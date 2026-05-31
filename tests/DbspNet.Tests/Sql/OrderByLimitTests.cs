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
/// Coverage for <c>ORDER BY</c> / <c>LIMIT</c> / <c>OFFSET</c> / <c>FETCH FIRST</c>
/// (incremental TOP-K). Every behavioural test runs on both compiler paths —
/// <c>typed: true</c> drives the typed-row fast path (default compile);
/// <c>typed: false</c> forces the structural fallback by supplying a non-default
/// row codec. Output asserts top-k <em>membership</em> and multiplicity; row
/// order is unobservable in a Z-set.
/// </summary>
public class OrderByLimitTests
{
    private static CompiledQuery Compile(string ddl, string query, bool typed)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;

        // Default compile tries the typed fast path first; supplying a
        // non-default IRowCodec disables it and forces the structural compile.
        return typed
            ? PlanToCircuit.Compile(plan)
            : PlanToCircuit.Compile(plan, EmittedEqualityCodec.Instance);
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Limit_WithoutOrderBy_UsesImplicitTotalOrder(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t LIMIT 2", typed);
        q.Table("t").Insert(3);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        // Implicit total order is ascending by row value: smallest two survive.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByAsc_Limit(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a LIMIT 2", typed);
        q.Table("t").Insert(5);
        q.Table("t").Insert(2);
        q.Table("t").Insert(8);
        q.Table("t").Insert(1);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByDesc_Limit(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a DESC LIMIT 2", typed);
        q.Table("t").Insert(5);
        q.Table("t").Insert(2);
        q.Table("t").Insert(8);
        q.Table("t").Insert(1);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 8));
        Assert.Equal(1, WeightOf(q.Current, 5));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Offset_SkipsLeadingRows(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL)",
            "SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1",
            typed);
        q.Table("t").Insert(5);
        q.Table("t").Insert(2);
        q.Table("t").Insert(8);
        q.Table("t").Insert(1);
        q.Step();

        // Sorted: 1, 2, 5, 8 — skip 1 (offset), take 2, 5.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 5));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Multiplicity_SpansWindowEdge(bool typed)
    {
        // Row {1} has weight 2 and fills both window positions; {2} is excluded.
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a LIMIT 2", typed);
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(2, WeightOf(q.Current, 1));
        Assert.Equal(0, WeightOf(q.Current, 2));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IsIncremental_RetractionPromotesNextRow(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a LIMIT 2", typed);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));

        // Retract the current top row: 1 leaves the window, 3 is promoted in.
        q.Table("t").Delete(1);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(-1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(0, WeightOf(q.Current, 2)); // already in window — no change
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void NullsLast_IsTheAscDefault(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT)", "SELECT a FROM t ORDER BY a LIMIT 2", typed);
        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        // ASC default sorts NULL last, so the two non-null rows survive.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void NullsFirst_Explicit(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT)", "SELECT a FROM t ORDER BY a NULLS FIRST LIMIT 1", typed);
        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, new object?[] { null }));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByOrdinal(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT a, b FROM t ORDER BY 2 LIMIT 1",
            typed);
        q.Table("t").Insert(1, 30);
        q.Table("t").Insert(2, 10);
        q.Table("t").Insert(3, 20);
        q.Step();

        // Ordinal 2 = column b; smallest b is 10 → row (2, 10).
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2, 10));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByAlias(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL)",
            "SELECT a AS x FROM t ORDER BY x DESC LIMIT 1",
            typed);
        q.Table("t").Insert(1);
        q.Table("t").Insert(7);
        q.Table("t").Insert(3);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 7));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MultiKey_AscThenDesc(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT a, b FROM t ORDER BY a ASC, b DESC LIMIT 2",
            typed);
        q.Table("t").Insert(1, 5);
        q.Table("t").Insert(1, 9);
        q.Table("t").Insert(2, 1);
        q.Step();

        // a ASC then b DESC: (1,9) then (1,5).
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 9));
        Assert.Equal(1, WeightOf(q.Current, 1, 5));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void FetchFirst_RowsOnly(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL)",
            "SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS ONLY",
            typed);
        q.Table("t").Insert(5);
        q.Table("t").Insert(2);
        q.Table("t").Insert(8);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 5));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Limit0_IsEmpty(bool typed)
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a LIMIT 0", typed);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        Assert.Equal(0, q.Current.Count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BareOrderBy_IsNoOp(bool typed)
    {
        // No LIMIT/OFFSET: row order is unobservable in the output Z-set, so the
        // result set is the full input with its multiplicities preserved.
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a DESC", typed);
        q.Table("t").Insert(1);
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(2, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void LimitInSubquery(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL)",
            "SELECT a FROM (SELECT a FROM t ORDER BY a LIMIT 2) s",
            typed);
        q.Table("t").Insert(5);
        q.Table("t").Insert(2);
        q.Table("t").Insert(8);
        q.Table("t").Insert(1);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
    }

    [Fact]
    public void TypedPath_CompilesTopKOverEmittedRow()
    {
        // Proof the typed fast path actually compiled TOP-K (rather than
        // silently falling back to the structural compile): a TopKOp closed
        // over an emitted TypedRow_* struct, not StructuralRow.
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a LIMIT 2", typed: true);
        var topK = q.Circuit.Operators
            .Where(op => op.GetType().Name.StartsWith("TopKOp", StringComparison.Ordinal))
            .ToList();
        Assert.NotEmpty(topK);
        Assert.Contains(topK, op => op.GetType().GetGenericArguments()
            .Any(t => t.Name.StartsWith("TypedRow_", StringComparison.Ordinal)));
    }

    [Fact]
    public void StructuralPath_CompilesTopKOverStructuralRow()
    {
        var q = Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY a LIMIT 2", typed: false);
        var topK = q.Circuit.Operators
            .Where(op => op.GetType().Name.StartsWith("TopKOp", StringComparison.Ordinal))
            .ToList();
        Assert.NotEmpty(topK);
        Assert.Contains(topK, op => op.GetType().GetGenericArguments().Any(t => t == typeof(StructuralRow)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderBy_NonSelectedColumn(bool typed)
    {
        // `b` is not in the select list; carried as a hidden ORDER BY column.
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT a FROM t ORDER BY b LIMIT 2",
            typed);
        q.Table("t").Insert(10, 3);
        q.Table("t").Insert(20, 1);
        q.Table("t").Insert(30, 2);
        q.Step();

        // Sorted by b: (20,1), (30,2), (10,3) — take 2, project a.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 20));
        Assert.Equal(1, WeightOf(q.Current, 30));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderBy_NonSelectedColumn_Desc(bool typed)
    {
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT a FROM t ORDER BY b DESC LIMIT 1",
            typed);
        q.Table("t").Insert(10, 3);
        q.Table("t").Insert(20, 1);
        q.Table("t").Insert(30, 2);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 10)); // largest b is 3 → row (10,3)
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderBy_NonSelectedExpression_Mixed(bool typed)
    {
        // Expression mixes a selected (a) and non-selected (b) column.
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT a FROM t ORDER BY a + b DESC LIMIT 1",
            typed);
        q.Table("t").Insert(1, 5);  // 6
        q.Table("t").Insert(2, 1);  // 3
        q.Table("t").Insert(3, 10); // 13 — max
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderBy_Aggregate_ByAggregateExpr(bool typed)
    {
        // ORDER BY an aggregate that is not in the select list.
        var q = Compile(
            "CREATE TABLE t (dept INT NOT NULL, amt INT NOT NULL)",
            "SELECT dept FROM t GROUP BY dept ORDER BY SUM(amt) DESC LIMIT 1",
            typed);
        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, 20); // dept 1 sum = 30
        q.Table("t").Insert(2, 5);  // dept 2 sum = 5
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BareOrderBy_NonSelectedColumn_IsNoOp(bool typed)
    {
        // No bound: validated (b exists) then discarded — full input survives.
        var q = Compile(
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "SELECT a FROM t ORDER BY b",
            typed);
        q.Table("t").Insert(10, 3);
        q.Table("t").Insert(20, 1);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 10));
        Assert.Equal(1, WeightOf(q.Current, 20));
    }

    [Fact]
    public void OrderByDistinct_NonSelectedColumn_Throws()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Compile(
                "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
                "SELECT DISTINCT a FROM t ORDER BY b LIMIT 1",
                typed: true));
        Assert.Contains("DISTINCT", ex.Message);
    }

    [Fact]
    public void OrderBySetOp_NonSelectedColumn_Throws()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Compile(
                "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
                "SELECT a FROM t UNION ALL SELECT a FROM t ORDER BY b LIMIT 1",
                typed: true));
        Assert.Contains("set operation", ex.Message);
    }

    [Fact]
    public void OrderByAggregate_NonGroupedColumn_Throws()
    {
        // amt is neither grouped nor aggregated — invalid even as an ORDER BY key.
        Assert.Throws<ResolveException>(() =>
            Compile(
                "CREATE TABLE t (dept INT NOT NULL, amt INT NOT NULL)",
                "SELECT dept FROM t GROUP BY dept ORDER BY amt LIMIT 1",
                typed: true));
    }

    [Fact]
    public void OrderByOrdinal_OutOfRange_Throws()
    {
        var ex = Assert.Throws<ResolveException>(() =>
            Compile("CREATE TABLE t (a INT NOT NULL)", "SELECT a FROM t ORDER BY 2 LIMIT 1", typed: true));
        Assert.Contains("out of range", ex.Message);
    }

    [Fact]
    public void NegativeLimit_Throws()
    {
        Assert.Throws<ParseException>(() =>
            Parser.ParseStatement("SELECT a FROM t LIMIT -1"));
    }
}
