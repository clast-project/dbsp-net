using DbspNet.Core.Algebra;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase 1.1 — end-to-end behaviour of the typed-row pipeline on the
/// narrow plan shape it currently supports (<c>SELECT * FROM t</c>).
/// Round-trips data through the typed pipeline and verifies it matches
/// the existing <see cref="CompiledQuery"/> behaviour byte-for-byte at
/// the boundary.
/// </summary>
public class TypedPlanCompilerTests
{
    private static LogicalPlan CompilePlan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    [Fact]
    public void TryCompile_SupportedScan_ReturnsTyped()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT * FROM t");

        var ok = TypedPlanCompiler.TryCompile(plan, out var typed);
        Assert.True(ok);
        Assert.NotNull(typed);
        Assert.Equal(2, typed!.OutputSchema.Count);
        Assert.True(typed.OutputRowType.IsValueType);
    }

    [Fact]
    public void TryCompile_RejectsPlansBeyondScanFilterProject()
    {
        // GROUP BY introduces an AggregatePlan between scan and the
        // top-level Project — still outside the Phase 1.3 plan shapes.
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a, COUNT(*) FROM t GROUP BY a");

        var ok = TypedPlanCompiler.TryCompile(plan, out var typed);
        Assert.False(ok);
        Assert.Null(typed);
    }

    [Fact]
    public void TryCompile_RejectsNullableSchema()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT)"],   // nullable
            "SELECT * FROM t");

        var ok = TypedPlanCompiler.TryCompile(plan, out _);
        Assert.False(ok);
    }

    [Fact]
    public void Insert_RoundTrip_OneRow()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT * FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, "alice");
        typed.Step();

        var rows = typed.Current.ToList();
        Assert.Single(rows);
        var (vals, weight) = rows[0];
        Assert.Equal(2, vals.Length);
        Assert.Equal(1, vals[0]);
        // VARCHAR comes back as Utf8String (matches boundary encoder semantics).
        Assert.Equal(DbspNet.Sql.TypeSystem.Utf8String.Of("alice"), vals[1]);
        Assert.Equal(1, weight);
    }

    [Fact]
    public void Insert_BatchPush_MultipleRowsAndWeights()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b BIGINT NOT NULL)"],
            "SELECT * FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Push(new[]
        {
            (Values: new object?[] { 1, 10L }, Weight: 1L),
            (Values: new object?[] { 2, 20L }, Weight: 1L),
            (Values: new object?[] { 1, 10L }, Weight: 2L),   // same row, weight sums
        });
        typed.Step();

        // (1, 10) has weight 3, (2, 20) has weight 1. Current is a delta;
        // since this is the first Step it equals the full state.
        Assert.Equal(3L, typed.WeightOf(1, 10L).Value);
        Assert.Equal(1L, typed.WeightOf(2, 20L).Value);
        Assert.Equal(0L, typed.WeightOf(99, 0L).Value);
    }

    [Fact]
    public void Delete_RoundTrip_NegativeWeight()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT * FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, "alice");
        typed.Step();

        typed.Table("t").Delete(1, "alice");
        typed.Step();

        Assert.Equal(-1L, typed.WeightOf(1, "alice").Value);
    }

    [Fact]
    public void Project_DropsColumns()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Equal(1, typed!.OutputSchema.Count);
        typed.Table("t").Insert(1, 10);
        typed.Table("t").Insert(2, 20);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1).Value);
        Assert.Equal(1L, typed.WeightOf(2).Value);
    }

    [Fact]
    public void Project_ReordersColumns()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT b, a FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Step();

        var rows = typed.Current.ToList();
        Assert.Single(rows);
        Assert.Equal(10, rows[0].Values[0]);
        Assert.Equal(1, rows[0].Values[1]);
        Assert.Equal(1L, typed.WeightOf(10, 1).Value);
    }

    [Fact]
    public void Project_DuplicatesColumn()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT a, a FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(5);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(5, 5).Value);
    }

    [Fact]
    public void Project_CollapsedRowsSumWeights()
    {
        // Dropping a non-key column makes two source rows collide on
        // the output. MapRows is linear, so the output weights sum.
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Step();

        Assert.Equal(2L, typed.WeightOf(1).Value);
    }

    [Fact]
    public void Project_Arithmetic_OnInt()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a + b, a * 2 FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(3, 4);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(7, 6).Value);
    }

    [Fact]
    public void Project_Comparison_ReturnsBool()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a > b FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(5, 3);
        typed.Table("t").Insert(2, 7);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(true).Value);
        Assert.Equal(1L, typed.WeightOf(false).Value);
    }

    [Fact]
    public void Project_RejectsExpressionsWithFunctionCalls()
    {
        // ABS is a function call — outside TypedExpressionCompiler's scope.
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT ABS(a) FROM t");

        Assert.False(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Null(typed);
    }

    [Fact]
    public void Project_DifferentialAgainstStructural()
    {
        var planTyped = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT b, a FROM t");
        var planStructural = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT b, a FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 2, 20 }, 1L),
            (new object?[] { 3, 30 }, 2L),
            (new object?[] { 1, 10 }, -1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            for (var a = 0; a <= 3; a++)
            {
                for (var b = 0; b <= 30; b += 10)
                {
                    // structural output schema is (b, a); look up
                    // both with the same key order.
                    Assert.Equal(
                        structural.WeightOf(b, a).Value,
                        typed.WeightOf(b, a).Value);
                }
            }
        }
    }

    [Fact]
    public void Output_MatchesStructuralCompile_ForSupportedSchemas()
    {
        // Differential test: typed and structural compile paths should
        // emit byte-identical (boundary-encoded) output on the same
        // input sequence.
        var planTyped = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT * FROM t");
        var planStructural = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT * FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 2, 20 }, 1L),
            (new object?[] { 3, 30 }, 2L),
            (new object?[] { 1, 10 }, -1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            // Weights must match for every key seen so far.
            for (var a = 0; a <= 3; a++)
            {
                for (var b = 0; b <= 30; b += 10)
                {
                    Assert.Equal(
                        structural.WeightOf(a, b).Value,
                        typed.WeightOf(a, b).Value);
                }
            }
        }
    }

    // ----- Filter -----

    [Fact]
    public void Filter_ColumnComparedToLiteral()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT * FROM t WHERE a > 5");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(3, 10);   // dropped
        typed.Table("t").Insert(6, 20);    // kept
        typed.Table("t").Insert(7, 30);    // kept
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(3, 10).Value);
        Assert.Equal(1L, typed.WeightOf(6, 20).Value);
        Assert.Equal(1L, typed.WeightOf(7, 30).Value);
    }

    [Fact]
    public void Filter_AndOfTwoComparisons()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT * FROM t WHERE a > 0 AND b < 100");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(0, 50);    // a > 0 fails
        typed.Table("t").Insert(5, 200);    // b < 100 fails
        typed.Table("t").Insert(5, 50);     // both ok
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(0, 50).Value);
        Assert.Equal(0L, typed.WeightOf(5, 200).Value);
        Assert.Equal(1L, typed.WeightOf(5, 50).Value);
    }

    [Fact]
    public void Filter_OnVarcharEquality()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT * FROM t WHERE name = 'alice'");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, "alice");
        typed.Table("t").Insert(2, "bob");
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, "alice").Value);
        Assert.Equal(0L, typed.WeightOf(2, "bob").Value);
    }

    [Fact]
    public void Filter_WithProjection()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT b FROM t WHERE a > 0");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Equal(1, typed!.OutputSchema.Count);
        typed.Table("t").Insert(0, 99);    // filtered out
        typed.Table("t").Insert(1, 10);
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(99).Value);
        Assert.Equal(1L, typed.WeightOf(10).Value);
    }

    [Fact]
    public void Filter_DifferentialAgainstStructural()
    {
        const string sql = "SELECT a, b FROM t WHERE a > 0 AND b < 100";
        var planTyped = CompilePlan(["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"], sql);
        var planStructural = CompilePlan(["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"], sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 0, 50 }, 1L),
            (new object?[] { 5, 200 }, 1L),
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 1, 10 }, -1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            foreach (var a in new[] { 0, 1, 5 })
            {
                foreach (var b in new[] { 10, 50, 200 })
                {
                    Assert.Equal(
                        structural.WeightOf(a, b).Value,
                        typed.WeightOf(a, b).Value);
                }
            }
        }
    }
}
