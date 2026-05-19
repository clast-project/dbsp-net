using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class PlanOptimizerTests
{
    private static LogicalPlan Plan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    // ---- Filter(Filter) → merged filter ----

    [Fact]
    public void AdjacentFiltersMerge()
    {
        // SELECT v FROM t WHERE v > 0 projects (FilterPlan(Scan)) then the
        // user-visible Project wraps it. We construct manually for shape.
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (v INT NOT NULL)"));

        var original = Plan(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT v FROM t WHERE v > 0");

        // Compose a second filter on top via raw construction.
        var outerFilter = new FilterPlan(
            original,
            new ResolvedBinary(
                BinaryOperator.Less,
                new ResolvedColumn(0, original.Schema[0].Type),
                new ResolvedLiteral(LiteralKind.Integer, 100, original.Schema[0].Type),
                new DbspNet.Sql.TypeSystem.SqlBooleanType(false)));

        var optimized = PlanOptimizer.Optimize(outerFilter);

        // The two filters should have collapsed into one. Because the
        // original `Project(Filter(Scan))` also got pushed past, the final
        // shape should be Project(Filter(Scan, conjunction)).
        var proj = Assert.IsType<ProjectPlan>(optimized);
        var filter = Assert.IsType<FilterPlan>(proj.Input);
        Assert.IsType<ScanPlan>(filter.Input);

        // The filter's predicate should be an AND.
        Assert.IsType<ResolvedBinary>(filter.Predicate);
        var bin = (ResolvedBinary)filter.Predicate;
        Assert.Equal(BinaryOperator.And, bin.Operator);
    }

    // ---- Filter(Project) → Project(Filter) ----

    [Fact]
    public void FilterOnProjection_PushesIntoInput()
    {
        // `SELECT v + 1 AS w FROM t WHERE v > 5` — the Filter lives on t
        // (pre-projection) already via WHERE. But `SELECT w FROM (SELECT v
        // AS w FROM t) WHERE w > 5` produces a Project-over-Filter that
        // should flatten.
        var plan = Plan(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT x.w FROM (SELECT v AS w FROM t) x WHERE x.w > 5");

        var optimized = PlanOptimizer.Optimize(plan);

        // Top is a Project (alias re-qualification from derived table + the
        // outer SELECT's projection, composed). Below it: Filter(Scan).
        // Essentially the filter has been pushed down past both projections.
        var proj = (ProjectPlan)optimized;
        var filter = Assert.IsType<FilterPlan>(proj.Input);
        Assert.IsType<ScanPlan>(filter.Input);
    }

    // ---- Filter(InnerJoin) → conjuncts split to left / right / above ----

    [Fact]
    public void FilterOnInnerJoin_SplitsConjuncts()
    {
        // `SELECT … FROM a JOIN b ON a.k = b.k WHERE a.v > 0 AND b.v > 0`
        // should push a.v>0 to a's filter, b.v>0 to b's filter, leaving the
        // join free of WHERE above it.
        var plan = Plan(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.v, b.v FROM a JOIN b ON a.k = b.k WHERE a.v > 0 AND b.v > 0");

        var optimized = PlanOptimizer.Optimize(plan);

        var proj = Assert.IsType<ProjectPlan>(optimized);
        // No Filter above the join any more — both conjuncts pushed.
        var join = Assert.IsType<JoinPlan>(proj.Input);

        // Left side of the join: Filter(Scan(a), a.v > 0).
        Assert.IsType<FilterPlan>(join.Left);
        Assert.IsType<FilterPlan>(join.Right);
    }

    [Fact]
    public void FilterOnInnerJoin_CrossConjunct_StaysAbove()
    {
        // `WHERE a.v > b.v` references both sides — cannot be pushed to
        // either. The optimizer must leave it as a Filter above the Join.
        var plan = Plan(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.v FROM a JOIN b ON a.k = b.k WHERE a.v > b.v");

        var optimized = PlanOptimizer.Optimize(plan);

        var proj = (ProjectPlan)optimized;
        var filter = Assert.IsType<FilterPlan>(proj.Input);
        Assert.IsType<JoinPlan>(filter.Input);
    }

    // ---- Filter(LeftJoin) → right-only pred stays above, left-only pushes ----

    [Fact]
    public void FilterOnLeftJoin_RightOnlyPredicate_StaysAbove()
    {
        // Classic case: `LEFT JOIN … WHERE b.v > 5` effectively converts to
        // an inner-join semantics from the user's perspective. Pushing the
        // right predicate into the right input would wrongly emit
        // NULL-padded rows for unmatched left rows that should have been
        // filtered out. The optimizer must leave the WHERE above the JOIN.
        var plan = Plan(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.v FROM a LEFT JOIN b ON a.k = b.k WHERE b.v > 5");

        var optimized = PlanOptimizer.Optimize(plan);

        var proj = (ProjectPlan)optimized;
        // WHERE b.v > 5 (right-only) must remain as a Filter above the JOIN.
        var filter = Assert.IsType<FilterPlan>(proj.Input);
        Assert.IsType<JoinPlan>(filter.Input);
    }

    [Fact]
    public void FilterOnLeftJoin_LeftOnlyPredicate_PushesToLeft()
    {
        var plan = Plan(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.v FROM a LEFT JOIN b ON a.k = b.k WHERE a.v > 5");

        var optimized = PlanOptimizer.Optimize(plan);

        var proj = (ProjectPlan)optimized;
        // Left-only predicate should have been pushed into the left input.
        var join = Assert.IsType<JoinPlan>(proj.Input);
        Assert.IsType<FilterPlan>(join.Left);
    }

    // ---- Filter(UnionAll) → push into branches ----

    [Fact]
    public void FilterOnUnionAll_PushesIntoBranches()
    {
        var plan = Plan(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM (SELECT v FROM t UNION ALL SELECT v FROM u) x WHERE x.v > 0");

        var optimized = PlanOptimizer.Optimize(plan);

        // Walk: the top-level Project for output, then a UnionAllPlan,
        // and each branch has its own Filter(Scan).
        var proj = (ProjectPlan)optimized;
        var union = Assert.IsType<UnionAllPlan>(proj.Input);
        Assert.True(union.Branches.All(b => ContainsFilter(b)));
    }

    private static bool ContainsFilter(LogicalPlan p) => p switch
    {
        FilterPlan => true,
        ProjectPlan pp => ContainsFilter(pp.Input),
        _ => false,
    };

    // ---- Project(Project) composition ----

    [Fact]
    public void AdjacentProjections_Compose()
    {
        // Derived table produces an intermediate projection; outer SELECT
        // produces another. The two should fuse into one.
        var plan = Plan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT x.a + 1 FROM (SELECT a, b FROM t) x");

        var optimized = PlanOptimizer.Optimize(plan);

        // After composition: a single Project directly over the Scan.
        var proj = (ProjectPlan)optimized;
        Assert.IsType<ScanPlan>(proj.Input);
    }

    // ---- Idempotence ----

    [Fact]
    public void NarrowAggregateInput_FiresOnJoinedGroupBy()
    {
        // Direct test for the column-pruning rule: the JOIN output is
        // [cust_id, amount, id, region] (4 cols) but the aggregate
        // only uses {amount, region}. Optimizer should insert a
        // narrowing Project between the AggregatePlan and the JoinPlan.
        var plan = Plan(
            [
                "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
                "CREATE TABLE orders (cust_id INT NOT NULL, amount INT NOT NULL)",
            ],
            "SELECT c.region, SUM(o.amount) FROM orders o " +
            "JOIN customers c ON o.cust_id = c.id " +
            "GROUP BY c.region");

        var opt = PlanOptimizer.Optimize(plan);

        // Walk the optimized tree, find the AggregatePlan, check its input.
        var agg = FindAggregate(opt);
        Assert.NotNull(agg);
        Assert.Equal(2, agg!.Input.Schema.Count);
    }

    private static AggregatePlan? FindAggregate(LogicalPlan p) => p switch
    {
        AggregatePlan a => a,
        ProjectPlan proj => FindAggregate(proj.Input),
        FilterPlan f => FindAggregate(f.Input),
        _ => null,
    };

    [Fact]
    public void Optimizer_IsIdempotent()
    {
        var plan = Plan(
            [
                "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE u (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT a.v, b.v FROM t a JOIN u b ON a.k = b.k WHERE a.v > 0 AND b.v > 5");

        var opt1 = PlanOptimizer.Optimize(plan);
        var opt2 = PlanOptimizer.Optimize(opt1);

        // Second run should produce a structurally-equal plan.
        // (We can't test reference equality because record constructors
        // always produce fresh instances, but record equality compares
        // fields — and we should converge structurally.)
        Assert.Equal(opt1, opt2);
    }
}
