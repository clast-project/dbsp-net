// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class ResolverTests
{
    private static (Catalog Catalog, Resolver Resolver) NewResolver(params string[] ddl)
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        foreach (var s in ddl)
        {
            r.Resolve(Parser.ParseStatement(s));
        }

        return (cat, r);
    }

    private static LogicalPlan ResolveQuery(Resolver r, string sql) =>
        ((SelectPlan)r.Resolve(Parser.ParseStatement(sql))).Query;

    private static CreateViewPlan ResolveView(Resolver r, string sql) =>
        (CreateViewPlan)r.Resolve(Parser.ParseStatement(sql));

    // --- CREATE TABLE ---

    [Fact]
    public void CreateTable_RegistersSchema_WithNullability()
    {
        var (cat, _) = NewResolver("CREATE TABLE t (id INTEGER NOT NULL, name VARCHAR(10))");
        var s = cat.Get("t");
        Assert.Equal(2, s.Count);
        Assert.False(s[0].Type.Nullable);
        Assert.True(s[1].Type.Nullable);
    }

    [Fact]
    public void CreateTable_DuplicateColumn_Throws()
    {
        Assert.Throws<ResolveException>(() => NewResolver("CREATE TABLE t (x INT, x INT)"));
    }

    // --- Projection / column resolution ---

    [Fact]
    public void SelectStar_ExpandsToAllColumns()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT NOT NULL, b VARCHAR)");
        var plan = ResolveQuery(r, "SELECT * FROM t");
        Assert.Equal(2, plan.Schema.Count);
        Assert.Equal("a", plan.Schema[0].Name);
        Assert.Equal("b", plan.Schema[1].Name);
    }

    [Fact]
    public void SelectAliases_OverrideColumnName()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT)");
        var plan = ResolveQuery(r, "SELECT a AS x FROM t");
        Assert.Equal("x", plan.Schema[0].Name);
    }

    [Fact]
    public void UnknownColumn_Throws()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT)");
        Assert.Throws<ResolveException>(() => ResolveQuery(r, "SELECT b FROM t"));
    }

    [Fact]
    public void AmbiguousColumn_Throws()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT)",
            "CREATE TABLE b (x INT)");
        Assert.Throws<ResolveException>(() => ResolveQuery(r, "SELECT x FROM a JOIN b ON a.x = b.x"));
    }

    [Fact]
    public void QualifiedColumn_Disambiguates()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (x INT NOT NULL)");
        // Should not throw.
        var plan = ResolveQuery(r, "SELECT a.x FROM a JOIN b ON a.x = b.x");
        Assert.Equal(1, plan.Schema.Count);
    }

    // --- Join extraction ---

    [Fact]
    public void InnerJoinOn_ExtractsEquiKey()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (k INT NOT NULL, v INT)",
            "CREATE TABLE b (k INT NOT NULL, w INT)");
        var plan = ResolveQuery(r, "SELECT * FROM a JOIN b ON a.k = b.k");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Single(join.EquiKeys);
        Assert.Equal(0, join.EquiKeys[0].LeftIndex);
        Assert.Equal(0, join.EquiKeys[0].RightIndex);
        Assert.Null(join.Residual);
    }

    [Fact]
    public void InnerJoin_WithResidualPredicate_KeepsItInFilter()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
            "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a JOIN b ON a.k = b.k AND a.v > b.w");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Single(join.EquiKeys);
        Assert.NotNull(join.Residual);
    }

    [Fact]
    public void InnerJoin_WithoutEquiKey_BuildsKeylessJoin()
    {
        // A non-equi (or cross) INNER join: no equi-key, the whole ON predicate
        // becomes the residual, evaluated over the unit-key cross product.
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a JOIN b ON a.x > b.y");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Empty(join.EquiKeys);
        Assert.NotNull(join.Residual);
    }

    [Fact]
    public void InnerJoin_ComputedEquiKey_HoistsToBareColumnKey()
    {
        // CAST(a.x) = CAST(b.y) is side-pure on both operands, so it is a real
        // equi-key — hoisted into synthetic columns rather than left as a
        // residual over a keyless cross product.
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y BIGINT NOT NULL)");
        var plan = ResolveQuery(r,
            "SELECT * FROM a JOIN b ON CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)");

        // Outermost projection strips the synthetic key columns.
        var strip = Assert.IsType<ProjectPlan>(plan);
        var proj = Assert.IsType<ProjectPlan>(strip.Input);
        var join = Assert.IsType<JoinPlan>(proj.Input);

        Assert.Single(join.EquiKeys);
        Assert.Null(join.Residual);

        // Each side is widened by exactly one synthetic column, keyed on it.
        Assert.Equal(1, join.EquiKeys[0].LeftIndex);   // a.x=0, __jkl0=1
        Assert.Equal(1, join.EquiKeys[0].RightIndex);  // b.y=0, __jkr0=1
        Assert.IsType<ProjectPlan>(join.Left);
        Assert.IsType<ProjectPlan>(join.Right);

        // Caller-visible schema is unchanged: [a.x, b.y].
        Assert.Equal(2, strip.Schema.Count);
        Assert.Equal("x", strip.Schema[0].Name);
        Assert.Equal("y", strip.Schema[1].Name);
    }

    [Fact]
    public void InnerJoin_OneSideBareColumn_HoistsOnlyTheExpressionSide()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (x VARCHAR NOT NULL)",
            "CREATE TABLE b (y VARCHAR NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a JOIN b ON UPPER(a.x) = b.y");
        var strip = Assert.IsType<ProjectPlan>(plan);
        var proj = Assert.IsType<ProjectPlan>(strip.Input);
        var join = Assert.IsType<JoinPlan>(proj.Input);

        Assert.Single(join.EquiKeys);
        Assert.Equal(1, join.EquiKeys[0].LeftIndex);   // hoisted UPPER(a.x)
        Assert.Equal(0, join.EquiKeys[0].RightIndex);  // b.y reused in place
        Assert.IsType<ProjectPlan>(join.Left);
        Assert.IsType<ScanPlan>(join.Right);           // right not widened
    }

    [Fact]
    public void LeftJoin_ComputedEquiKey_IsAccepted()
    {
        // Previously "LEFT JOIN requires at least one equi-key (v1)" — the cast
        // equality was classified as a residual, leaving zero equi-keys.
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y BIGINT NOT NULL)");
        var plan = ResolveQuery(r,
            "SELECT * FROM a LEFT JOIN b ON CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)");
        var strip = Assert.IsType<ProjectPlan>(plan);
        var proj = Assert.IsType<ProjectPlan>(strip.Input);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Single(join.EquiKeys);
        Assert.Null(join.Residual);

        // The right column is nullable in the output (LEFT JOIN), and the
        // synthetic columns are gone.
        Assert.Equal(2, strip.Schema.Count);
        Assert.True(strip.Schema[1].Type.Nullable);
    }

    [Fact]
    public void InnerJoin_ConstantOperand_StaysResidual()
    {
        // `a.x = 5` is a filter, not a join key — it must not be hoisted.
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a JOIN b ON a.x = 5");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Empty(join.EquiKeys);
        Assert.NotNull(join.Residual);
    }

    [Fact]
    public void InnerJoin_MixedSideOperand_StaysResidual()
    {
        // `a.x + b.y = 10` reads both inputs on one operand — never a join key.
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a JOIN b ON a.x + b.y = 10");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Empty(join.EquiKeys);
        Assert.NotNull(join.Residual);
    }

    [Fact]
    public void CrossJoin_BuildsKeylessJoin()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a CROSS JOIN b");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Empty(join.EquiKeys);
        Assert.Equal(DbspNet.Sql.Parser.Ast.JoinType.Inner, join.JoinType);
    }

    [Fact]
    public void FullJoin_MakesBothSidesNullable()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
            "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT a.v, b.w FROM a FULL JOIN b ON a.k = b.k");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Equal(DbspNet.Sql.Parser.Ast.JoinType.FullOuter, join.JoinType);
        // Every join-output column is nullable (either side can be unmatched).
        Assert.All(join.Schema.Columns, c => Assert.True(c.Type.Nullable));
    }

    [Fact]
    public void FullJoin_WithoutEquiKey_BuildsKeylessJoinWithResidual()
    {
        // Was rejected ("requires at least one equi-key") while outer-join
        // match-presence was per-key. The anti-join rewrite in PlanToCircuit
        // doesn't use the keyed operator, so a keyless outer join lowers fine —
        // as a unit-key cross product: correct, quadratic.
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a FULL JOIN b ON a.x > b.y");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Empty(join.EquiKeys);
        Assert.NotNull(join.Residual);
        Assert.Equal(JoinType.FullOuter, join.JoinType);
    }

    [Fact]
    public void LeftJoin_WithoutEquiKey_BuildsKeylessJoinWithResidual()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT * FROM a LEFT JOIN b ON a.x > b.y");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Empty(join.EquiKeys);
        Assert.NotNull(join.Residual);
        Assert.Equal(JoinType.LeftOuter, join.JoinType);
    }

    [Fact]
    public void LeftJoin_WithResidual_KeepsEquiKeyAndResidual()
    {
        // The TPC-DI SCD2 temporal-validity shape.
        var (_, r) = NewResolver(
            "CREATE TABLE a (k INT NOT NULL, ts INT NOT NULL)",
            "CREATE TABLE b (k INT NOT NULL, lo INT NOT NULL, hi INT NOT NULL)");
        var plan = ResolveQuery(r,
            "SELECT * FROM a LEFT JOIN b ON a.k = b.k AND a.ts BETWEEN b.lo AND b.hi");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.Single(join.EquiKeys);
        Assert.NotNull(join.Residual);
        Assert.Equal(JoinType.LeftOuter, join.JoinType);
    }

    // --- Type inference ---

    [Fact]
    public void Arithmetic_PromotesToCommonNumericType()
    {
        var (_, r) = NewResolver("CREATE TABLE t (i INT NOT NULL, b BIGINT NOT NULL, d DOUBLE PRECISION NOT NULL)");
        var plan = ResolveQuery(r, "SELECT i + b AS ib, i + d AS id FROM t");
        Assert.IsType<SqlBigintType>(plan.Schema[0].Type);
        Assert.IsType<SqlDoubleType>(plan.Schema[1].Type);
    }

    [Fact]
    public void ComparisonAcrossStringAndNumber_Throws()
    {
        var (_, r) = NewResolver("CREATE TABLE t (n INT NOT NULL, s VARCHAR NOT NULL)");
        Assert.Throws<ResolveException>(() => ResolveQuery(r, "SELECT * FROM t WHERE n = s"));
    }

    [Fact]
    public void NullabilityPropagates_ThroughArithmetic()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT NOT NULL, b INT)"); // b nullable
        var plan = ResolveQuery(r, "SELECT a + b AS c FROM t");
        Assert.True(plan.Schema[0].Type.Nullable);
    }

    [Fact]
    public void NotBooleanPredicate_Throws()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT NOT NULL)");
        Assert.Throws<ResolveException>(() => ResolveQuery(r, "SELECT * FROM t WHERE a"));
    }

    [Fact]
    public void Coalesce_NonNullArg_YieldsNonNullResult()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT)"); // nullable
        var plan = ResolveQuery(r, "SELECT COALESCE(a, 0) AS c FROM t");
        Assert.False(plan.Schema[0].Type.Nullable);
    }

    [Fact]
    public void Coalesce_AllNullableArgs_YieldsNullableResult()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT, b INT)"); // both nullable
        var plan = ResolveQuery(r, "SELECT COALESCE(a, b) AS c FROM t");
        Assert.True(plan.Schema[0].Type.Nullable);
    }

    // --- GROUP BY / aggregates ---

    [Fact]
    public void GroupBy_WithAggregate_BuildsAggregatePlan()
    {
        var (_, r) = NewResolver("CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT dept, SUM(salary) AS total FROM e GROUP BY dept");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var agg = Assert.IsType<AggregatePlan>(proj.Input);
        Assert.Single(agg.GroupKeys);
        Assert.Single(agg.Aggregates);
        Assert.Equal(AggregateKind.Sum, agg.Aggregates[0].Kind);
        Assert.IsType<SqlBigintType>(proj.Schema[1].Type); // SUM(INT NOT NULL) -> BIGINT NULL
        Assert.True(proj.Schema[1].Type.Nullable);
    }

    [Fact]
    public void SelectNonGroupedColumn_Throws()
    {
        var (_, r) = NewResolver("CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)");
        Assert.Throws<ResolveException>(
            () => ResolveQuery(r, "SELECT dept, salary FROM e GROUP BY dept"));
    }

    [Fact]
    public void CountStar_ReturnsBigint()
    {
        var (_, r) = NewResolver("CREATE TABLE e (dept VARCHAR NOT NULL)");
        var plan = ResolveQuery(r, "SELECT dept, COUNT(*) FROM e GROUP BY dept");
        Assert.IsType<SqlBigintType>(plan.Schema[1].Type);
        Assert.False(plan.Schema[1].Type.Nullable); // COUNT is never NULL
    }

    [Fact]
    public void AggregateWithoutGroupBy_IsAllowedAsSingleGroup()
    {
        var (_, r) = NewResolver("CREATE TABLE e (salary INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT SUM(salary) FROM e");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var agg = Assert.IsType<AggregatePlan>(proj.Input);
        Assert.Empty(agg.GroupKeys);
        Assert.Single(agg.Aggregates);
    }

    [Fact]
    public void Having_ReferencingAggregate_IsAllowed()
    {
        var (_, r) = NewResolver("CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT dept FROM e GROUP BY dept HAVING SUM(salary) > 100");
        var proj = Assert.IsType<ProjectPlan>(plan);
        var filter = Assert.IsType<FilterPlan>(proj.Input);
        Assert.IsType<AggregatePlan>(filter.Input);
    }

    [Fact]
    public void Having_WithoutGroupByOrAggregate_Throws()
    {
        var (_, r) = NewResolver("CREATE TABLE t (x INT NOT NULL)");
        Assert.Throws<ResolveException>(() => ResolveQuery(r, "SELECT * FROM t HAVING x > 0"));
    }

    [Fact]
    public void SumOfNonNumeric_Throws()
    {
        var (_, r) = NewResolver("CREATE TABLE t (s VARCHAR NOT NULL)");
        Assert.Throws<ResolveException>(() => ResolveQuery(r, "SELECT SUM(s) FROM t"));
    }

    [Fact]
    public void Avg_Integer_ReturnsDouble()
    {
        var (_, r) = NewResolver("CREATE TABLE t (x INT NOT NULL)");
        var plan = ResolveQuery(r, "SELECT AVG(x) FROM t");
        Assert.IsType<SqlDoubleType>(plan.Schema[0].Type);
    }

    // --- CREATE VIEW ---

    [Fact]
    public void CreateView_ResolvesQuery()
    {
        var (_, r) = NewResolver("CREATE TABLE t (a INT NOT NULL)");
        var v = ResolveView(r, "CREATE VIEW v AS SELECT a + 1 AS a1 FROM t");
        Assert.Equal("v", v.ViewName);
        Assert.Equal(1, v.Query.Schema.Count);
        Assert.Equal("a1", v.Query.Schema[0].Name);
    }
}
