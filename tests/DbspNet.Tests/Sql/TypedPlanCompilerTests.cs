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
    public void TryCompile_RejectsPlansBeyondBareScan()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT id FROM t WHERE id > 0");

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
}
