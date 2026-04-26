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
    public void InnerJoin_WithoutEquiKey_Throws()
    {
        var (_, r) = NewResolver(
            "CREATE TABLE a (x INT NOT NULL)",
            "CREATE TABLE b (y INT NOT NULL)");
        Assert.Throws<ResolveException>(
            () => ResolveQuery(r, "SELECT * FROM a JOIN b ON a.x > b.y"));
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
