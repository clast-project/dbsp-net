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
    public void TryCompile_RejectsUnsupportedPlanNodes()
    {
        // UNION ALL is not yet supported by the typed path; it should
        // bail out rather than partially compile.
        var plan = CompilePlan(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL)",
            ],
            "SELECT a FROM t UNION ALL SELECT a FROM u");

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

    // ----- Aggregate -----

    [Fact]
    public void Aggregate_CountStar_SingleGroup()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a, COUNT(*) FROM t GROUP BY a");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Table("t").Insert(2, 30);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 2L).Value);
        Assert.Equal(1L, typed.WeightOf(2, 1L).Value);
    }

    [Fact]
    public void Aggregate_SumLong()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Table("t").Insert(2, 7);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 30L).Value);
        Assert.Equal(1L, typed.WeightOf(2, 7L).Value);
    }

    [Fact]
    public void Aggregate_SumDouble()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v DOUBLE PRECISION NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 1.5);
        typed.Table("t").Insert(1, 2.5);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 4.0).Value);
    }

    [Fact]
    public void Aggregate_Avg()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, AVG(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Table("t").Insert(1, 30);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 20.0).Value);
    }

    [Fact]
    public void Aggregate_MultiAggregate()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, COUNT(*), SUM(v), AVG(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 2L, 30L, 15.0).Value);
    }

    [Fact]
    public void Aggregate_MultiColumnGroupBy()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL)"],
            "SELECT a, b, SUM(v) FROM t GROUP BY a, b");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 1, 100);
        typed.Table("t").Insert(1, 1, 50);
        typed.Table("t").Insert(1, 2, 200);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 1, 150L).Value);
        Assert.Equal(1L, typed.WeightOf(1, 2, 200L).Value);
    }

    [Fact]
    public void Aggregate_RejectsMin()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, MIN(v) FROM t GROUP BY k");

        Assert.False(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Null(typed);
    }

    [Fact]
    public void Aggregate_DecimalAvg()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v DECIMAL(10, 2) NOT NULL)"],
            "SELECT k, AVG(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, new Clast.DatabaseDecimal.Values.Decimal128(100));   // 1.00
        typed.Table("t").Insert(1, new Clast.DatabaseDecimal.Values.Decimal128(200));   // 2.00
        typed.Table("t").Insert(1, new Clast.DatabaseDecimal.Values.Decimal128(300));   // 3.00
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, new Clast.DatabaseDecimal.Values.Decimal128(200)).Value);
    }

    [Fact]
    public void Filter_OnDecimalColumn()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, amt DECIMAL(10, 2) NOT NULL)"],
            "SELECT id FROM t WHERE amt > CAST(5 AS DECIMAL(10, 2))");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, new Clast.DatabaseDecimal.Values.Decimal128(300));   // 3.00 — filtered
        typed.Table("t").Insert(2, new Clast.DatabaseDecimal.Values.Decimal128(750));   // 7.50 — kept
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(1).Value);
        Assert.Equal(1L, typed.WeightOf(2).Value);
    }

    [Fact]
    public void Aggregate_DecimalSum()
    {
        // Phase 1.8: SUM(DECIMAL) is in scope.
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v DECIMAL(10, 2) NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, new Clast.DatabaseDecimal.Values.Decimal128(150));   // 1.50
        typed.Table("t").Insert(1, new Clast.DatabaseDecimal.Values.Decimal128(250));    // 2.50
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, new Clast.DatabaseDecimal.Values.Decimal128(400)).Value);
    }

    [Fact]
    public void Aggregate_IncrementalRetraction()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Step();
        Assert.Equal(1L, typed.WeightOf(1, 2L, 30L).Value);

        // Retract one row; the group should now show (1, 1, 10) and
        // the previous (1, 2, 30) row should have been retracted.
        typed.Table("t").Delete(1, 20);
        typed.Step();
        Assert.Equal(-1L, typed.WeightOf(1, 2L, 30L).Value);
        Assert.Equal(1L, typed.WeightOf(1, 1L, 10L).Value);
    }

    [Fact]
    public void Aggregate_DifferentialAgainstStructural()
    {
        const string sql = "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k";
        var ddl = new[] { "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)" };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 1, 20 }, 1L),
            (new object?[] { 2, 30 }, 1L),
            (new object?[] { 1, 10 }, -1L),
            (new object?[] { 2, 30 }, 1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            for (var k = 1; k <= 2; k++)
            {
                for (var cnt = 0L; cnt <= 5L; cnt++)
                {
                    for (var sum = 0L; sum <= 100L; sum += 10L)
                    {
                        Assert.Equal(
                            structural.WeightOf(k, cnt, sum).Value,
                            typed.WeightOf(k, cnt, sum).Value);
                    }
                }
            }
        }
    }

    // ----- Inner Join -----

    [Fact]
    public void Join_SingleKey_TwoTables()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE orders (id INT NOT NULL, cust_id INT NOT NULL)",
                "CREATE TABLE customers (cust_id INT NOT NULL, name VARCHAR NOT NULL)",
            ],
            "SELECT id, name FROM orders INNER JOIN customers ON orders.cust_id = customers.cust_id");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Equal(2, typed!.OutputSchema.Count);

        typed.Table("orders").Insert(1, 100);
        typed.Table("orders").Insert(2, 200);
        typed.Table("customers").Insert(100, "alice");
        typed.Table("customers").Insert(200, "bob");
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, "alice").Value);
        Assert.Equal(1L, typed.WeightOf(2, "bob").Value);
        Assert.Equal(0L, typed.WeightOf(1, "bob").Value);
    }

    [Fact]
    public void Join_NoMatch_EmptyOutput()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT l.k, v, w FROM l INNER JOIN r ON l.k = r.k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("l").Insert(1, 10);
        typed.Table("r").Insert(2, 20);
        typed.Step();

        Assert.Empty(typed.Current);
    }

    [Fact]
    public void Join_MultiColumnKey()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (a INT NOT NULL, b INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT v, w FROM l INNER JOIN r ON l.a = r.a AND l.b = r.b");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("l").Insert(1, 1, 100);
        typed.Table("l").Insert(1, 2, 200);
        typed.Table("r").Insert(1, 1, 999);
        typed.Table("r").Insert(1, 3, 888);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(100, 999).Value);
        Assert.Equal(0L, typed.WeightOf(200, 999).Value);
        Assert.Equal(0L, typed.WeightOf(100, 888).Value);
    }

    [Fact]
    public void Join_WithFilterOnInput()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT v, w FROM l INNER JOIN r ON l.k = r.k WHERE l.v > 5");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("l").Insert(1, 3);    // filtered out
        typed.Table("l").Insert(1, 10);
        typed.Table("r").Insert(1, 100);
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(3, 100).Value);
        Assert.Equal(1L, typed.WeightOf(10, 100).Value);
    }

    [Fact]
    public void Join_RejectsLeftOuter()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT v, w FROM l LEFT JOIN r ON l.k = r.k");

        Assert.False(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Null(typed);
    }

    [Fact]
    public void Join_DifferentialAgainstStructural()
    {
        const string sql = "SELECT id, v, w FROM l INNER JOIN r ON l.k = r.k";
        var ddl = new[]
        {
            "CREATE TABLE l (id INT NOT NULL, k INT NOT NULL, v INT NOT NULL)",
            "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
        };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var lDeltas = new[]
        {
            (new object?[] { 1, 100, 10 }, 1L),
            (new object?[] { 2, 100, 20 }, 1L),
            (new object?[] { 3, 200, 30 }, 1L),
            (new object?[] { 1, 100, 10 }, -1L),
        };
        var rDeltas = new[]
        {
            (new object?[] { 100, 1000 }, 1L),
            (new object?[] { 200, 2000 }, 1L),
            (new object?[] { 100, 1000 }, 1L),  // duplicate right
        };

        for (var step = 0; step < Math.Max(lDeltas.Length, rDeltas.Length); step++)
        {
            if (step < lDeltas.Length)
            {
                typed!.Table("l").Push(new[] { lDeltas[step] });
                structural.Table("l").Push(new[] { lDeltas[step] });
            }

            if (step < rDeltas.Length)
            {
                typed!.Table("r").Push(new[] { rDeltas[step] });
                structural.Table("r").Push(new[] { rDeltas[step] });
            }

            typed!.Step();
            structural.Step();

            for (var id = 0; id <= 3; id++)
            {
                foreach (var v in new[] { 10, 20, 30 })
                {
                    foreach (var w in new[] { 1000, 2000 })
                    {
                        Assert.Equal(
                            structural.WeightOf(id, v, w).Value,
                            typed.WeightOf(id, v, w).Value);
                    }
                }
            }
        }
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
