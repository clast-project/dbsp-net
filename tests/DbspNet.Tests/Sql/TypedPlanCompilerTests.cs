// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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
    public void UnionAll_TwoBranches_SumsWeights()
    {
        // UNION ALL is Z-set addition: matching rows on each side
        // accumulate weight in the output. Non-matching rows pass
        // through with their original weight.
        var plan = CompilePlan(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL)",
            ],
            "SELECT a FROM t UNION ALL SELECT a FROM u");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1);
        typed.Table("t").Insert(2);
        typed.Table("u").Insert(2);   // appears in both → weight 2
        typed.Table("u").Insert(3);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1).Value);
        Assert.Equal(2L, typed.WeightOf(2).Value);
        Assert.Equal(1L, typed.WeightOf(3).Value);
    }

    [Fact]
    public void UnionAll_ThreeBranches_OverFilter()
    {
        // Each branch is a typed Filter; UNION ALL stitches them.
        var plan = CompilePlan(
            [
                "CREATE TABLE a (v INT NOT NULL)",
                "CREATE TABLE b (v INT NOT NULL)",
                "CREATE TABLE c (v INT NOT NULL)",
            ],
            "SELECT v FROM a WHERE v > 0 UNION ALL " +
            "SELECT v FROM b WHERE v > 10 UNION ALL " +
            "SELECT v FROM c WHERE v > 100");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("a").Insert(5);   // kept
        typed.Table("a").Insert(0);    // dropped
        typed.Table("b").Insert(20);   // kept
        typed.Table("b").Insert(5);    // dropped
        typed.Table("c").Insert(200);  // kept
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(5).Value);
        Assert.Equal(1L, typed.WeightOf(20).Value);
        Assert.Equal(1L, typed.WeightOf(200).Value);
        Assert.Equal(0L, typed.WeightOf(0).Value);
    }

    [Fact]
    public void Distinct_ViaUnionSetSemantics()
    {
        // UNION (without ALL) is UnionAll + Distinct: rows present on
        // either side appear once in the output, regardless of input
        // multiplicity.
        var plan = CompilePlan(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL)",
            ],
            "SELECT a FROM t UNION SELECT a FROM u");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1);
        typed.Table("t").Insert(1);    // duplicate → DISTINCT collapses
        typed.Table("t").Insert(2);
        typed.Table("u").Insert(2);    // appears in both → still weight 1
        typed.Table("u").Insert(3);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1).Value);
        Assert.Equal(1L, typed.WeightOf(2).Value);
        Assert.Equal(1L, typed.WeightOf(3).Value);
    }

    [Fact]
    public void Distinct_ViaIntersect()
    {
        // INTERSECT decomposes through Distinct+join in the resolver;
        // exercises the typed Distinct path on both operands.
        var plan = CompilePlan(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL)",
            ],
            "SELECT a FROM t INTERSECT SELECT a FROM u");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1);
        typed.Table("t").Insert(2);
        typed.Table("t").Insert(3);
        typed.Table("u").Insert(2);
        typed.Table("u").Insert(3);
        typed.Table("u").Insert(4);
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(1).Value);     // only in t
        Assert.Equal(1L, typed.WeightOf(2).Value);     // in both
        Assert.Equal(1L, typed.WeightOf(3).Value);     // in both
        Assert.Equal(0L, typed.WeightOf(4).Value);     // only in u
    }

    [Fact]
    public void Distinct_FreshInsertEmitsWeightOne()
    {
        // Fresh insert of multiplicity 3 produces a DISTINCT output
        // row at weight 1 (set semantics).
        var plan = CompilePlan(
            [
                "CREATE TABLE t (a INT NOT NULL)",
                "CREATE TABLE u (a INT NOT NULL)",
            ],
            "SELECT a FROM t UNION SELECT a FROM u");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(7);
        typed.Table("t").Insert(7);
        typed.Table("t").Insert(7);
        typed.Step();
        Assert.Equal(1L, typed.WeightOf(7).Value);
    }

    [Fact]
    public void Recursive_StandaloneTryCompileBails()
    {
        // Standalone mode doesn't have ctx.StructuralScans to feed
        // the structural recursive op; only the boundary-adapter
        // path (PlanToCircuit) supports it. TryCompile must fall
        // back cleanly so PlanToCircuit can route to structural.
        var plan = CompilePlan(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS (" +
            "  SELECT src, dst FROM edges " +
            "  UNION ALL " +
            "  SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        Assert.False(TypedPlanCompiler.TryCompile(plan, out var typed));
        Assert.Null(typed);
    }

    [Fact]
    public void Recursive_BoundaryAdapter_TransitiveClosure_MatchesStructural()
    {
        // PlanToCircuit's typed fast path goes through
        // TryCompileWithStructuralBoundary (which DOES have
        // StructuralScans). The recursive op stays structural
        // internally; output is MapRows-lifted to typed for the
        // surrounding pipeline. End-to-end behavior should match
        // a pure-structural compile.
        const string sql =
            "WITH RECURSIVE reach AS (" +
            "  SELECT src, dst FROM edges " +
            "  UNION ALL " +
            "  SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach";
        var ddl = new[] { "CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)" };

        var qTyped = PlanToCircuit.Compile(CompilePlan(ddl, sql));
        var qStruct = PlanToCircuit.Compile(
            CompilePlan(ddl, sql),
            DbspNet.Sql.Compiler.EmittedEqualityCodec.Instance);  // non-default codec → forces structural compile path

        var edges = new[] { (1, 2), (2, 3), (3, 4), (2, 5) };
        foreach (var (src, dst) in edges)
        {
            qTyped.Table("edges").Insert(src, dst);
            qStruct.Table("edges").Insert(src, dst);
        }
        qTyped.Step();
        qStruct.Step();

        // Spot-check several known-reachable pairs.
        object?[][] pairs = [[1, 2], [2, 3], [3, 4], [2, 5], [1, 3], [1, 4], [1, 5], [2, 4]];
        foreach (var p in pairs)
        {
            Assert.Equal(qStruct.WeightOf(p).Value, qTyped.WeightOf(p).Value);
        }
        Assert.Equal(qStruct.Current.Count, qTyped.Current.Count);
    }

    [Fact]
    public void Recursive_BoundaryAdapter_IncrementalDelta_MatchesStructural()
    {
        const string sql =
            "WITH RECURSIVE reach AS (" +
            "  SELECT src, dst FROM edges " +
            "  UNION ALL " +
            "  SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach";
        var ddl = new[] { "CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)" };

        var qTyped = PlanToCircuit.Compile(CompilePlan(ddl, sql));
        var qStruct = PlanToCircuit.Compile(
            CompilePlan(ddl, sql), DbspNet.Sql.Compiler.EmittedEqualityCodec.Instance);

        // Tick 1: 1->2, 2->3 (chain)
        qTyped.Table("edges").Insert(1, 2); qTyped.Table("edges").Insert(2, 3);
        qStruct.Table("edges").Insert(1, 2); qStruct.Table("edges").Insert(2, 3);
        qTyped.Step(); qStruct.Step();
        Assert.Equal(qStruct.Current.Count, qTyped.Current.Count);

        // Tick 2: add 3->4 (extends chain)
        qTyped.Table("edges").Insert(3, 4);
        qStruct.Table("edges").Insert(3, 4);
        qTyped.Step(); qStruct.Step();

        object?[][] pairs = [[1, 2], [2, 3], [1, 3], [3, 4], [2, 4], [1, 4]];
        foreach (var p in pairs)
        {
            Assert.Equal(qStruct.WeightOf(p).Value, qTyped.WeightOf(p).Value);
        }
    }

    [Fact]
    public void ScalarSubquery_AppendsColumn()
    {
        // SELECT-list scalar subquery becomes ScalarSubqueryJoinPlan
        // — a LEFT JOIN on a unit key that broadcasts the
        // subquery's single value over every outer row.
        var plan = CompilePlan(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (y INT NOT NULL)",
            ],
            "SELECT x, (SELECT SUM(y) FROM u) AS total FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1);
        typed.Table("t").Insert(2);
        typed.Table("u").Insert(10);
        typed.Table("u").Insert(20);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 30L).Value);
        Assert.Equal(1L, typed.WeightOf(2, 30L).Value);
    }

    [Fact]
    public void ScalarSubquery_EmptySubquery_NullPads()
    {
        // u is empty → SUM(y) yields NULL; every outer row carries
        // NULL via the null-pad combine.
        var plan = CompilePlan(
            [
                "CREATE TABLE t (x INT NOT NULL)",
                "CREATE TABLE u (y INT NOT NULL)",
            ],
            "SELECT x, (SELECT SUM(y) FROM u) AS total FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(new object?[] { 1, null }).Value);
    }

    [Fact]
    public void ScalarSubquery_DifferentialAgainstStructural()
    {
        const string sql = "SELECT x, (SELECT SUM(y) FROM u) AS total FROM t";
        var ddl = new[]
        {
            "CREATE TABLE t (x INT NOT NULL)",
            "CREATE TABLE u (y INT NOT NULL)",
        };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (string Table, object?[] Values, long Weight)[]
        {
            ("t", [1], 1L), ("u", [10], 1L), ("t", [2], 1L),
            ("u", [20], 1L), ("u", [10], -1L), ("t", [1], -1L),
        };

        foreach (var (table, vs, w) in deltas)
        {
            typed!.Table(table).Push(new[] { (vs, w) });
            structural.Table(table).Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            // Sweep candidate (x, total) pairs across the possible
            // outputs of this small workload.
            object?[][] candidates =
            [
                [1, null], [2, null],
                [1, 10L], [2, 10L], [1, 20L], [2, 20L],
                [1, 30L], [2, 30L],
            ];
            foreach (var cand in candidates)
            {
                Assert.Equal(
                    structural.WeightOf(cand).Value,
                    typed.WeightOf(cand).Value);
            }
        }
    }

    [Fact]
    public void Cte_SingleReference()
    {
        // Non-recursive CTE referenced once. The CTE body is a
        // typed-supported pipeline (Scan + Filter); the outer
        // SELECT just reads from it.
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL)"],
            "WITH big AS (SELECT a FROM t WHERE a > 10) SELECT a FROM big");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(5);
        typed.Table("t").Insert(15);
        typed.Table("t").Insert(20);
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(5).Value);
        Assert.Equal(1L, typed.WeightOf(15).Value);
        Assert.Equal(1L, typed.WeightOf(20).Value);
    }

    [Fact]
    public void Cte_ReferencedTwiceSharesStream()
    {
        // CTE referenced twice (self-join shape). The CTE body
        // should compile once and both references share the cached
        // typed stream — same operator graph, no duplicate state.
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "WITH x AS (SELECT k, v FROM t WHERE v > 0) " +
            "SELECT a.v, b.v FROM x AS a JOIN x AS b ON a.k = b.k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(2, 20);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(10, 10).Value);
        Assert.Equal(1L, typed.WeightOf(20, 20).Value);
        Assert.Equal(0L, typed.WeightOf(10, 20).Value);
    }

    [Fact]
    public void Cte_DifferentialAgainstStructural()
    {
        const string sql =
            "WITH x AS (SELECT a FROM t WHERE a > 0) " +
            "SELECT a FROM x UNION ALL SELECT a FROM x";
        var ddl = new[] { "CREATE TABLE t (a INT NOT NULL)" };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (object?[], long)[]
        {
            ([5], 1L), ([10], 1L), ([5], 1L), ([10], -1L),
        };
        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            for (var k = 0; k <= 12; k++)
            {
                Assert.Equal(
                    structural.WeightOf(k).Value,
                    typed.WeightOf(k).Value);
            }
        }
    }

    [Fact]
    public void SetOp_Except_MatchesStructural()
    {
        // EXCEPT decomposes to Distinct + Difference + Intersect in
        // the resolver. Differential against structural so we don't
        // assert against a hand-derived expectation that might
        // mismatch the operator's actual semantics.
        const string sql = "SELECT a FROM t EXCEPT SELECT a FROM u";
        var ddl = new[]
        {
            "CREATE TABLE t (a INT NOT NULL)",
            "CREATE TABLE u (a INT NOT NULL)",
        };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (string Table, object?[] Values, long Weight)[]
        {
            ("t", [1], 1L), ("t", [2], 1L), ("t", [3], 1L),
            ("u", [2], 1L), ("u", [4], 1L),
            ("t", [3], -1L), ("u", [2], -1L), ("t", [5], 1L),
        };

        foreach (var (table, vs, w) in deltas)
        {
            typed!.Table(table).Push(new[] { (vs, w) });
            structural.Table(table).Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            for (var k = 0; k <= 6; k++)
            {
                Assert.Equal(
                    structural.WeightOf(k).Value,
                    typed.WeightOf(k).Value);
            }
        }
    }

    [Fact]
    public void Distinct_DifferentialAgainstStructural()
    {
        const string sql = "SELECT a FROM t UNION SELECT a FROM u";
        var ddl = new[]
        {
            "CREATE TABLE t (a INT NOT NULL)",
            "CREATE TABLE u (a INT NOT NULL)",
        };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (string Table, object?[] Values, long Weight)[]
        {
            ("t", [1], 1L), ("t", [1], 1L), ("u", [2], 1L),
            ("t", [1], -1L), ("u", [3], 1L), ("t", [1], -1L),
            ("u", [2], -1L),
        };

        foreach (var (table, vs, w) in deltas)
        {
            typed!.Table(table).Push(new[] { (vs, w) });
            structural.Table(table).Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            for (var k = 0; k <= 4; k++)
            {
                Assert.Equal(
                    structural.WeightOf(k).Value,
                    typed.WeightOf(k).Value);
            }
        }
    }

    [Fact]
    public void UnionAll_DifferentialAgainstStructural()
    {
        const string sql =
            "SELECT a FROM t UNION ALL SELECT a FROM u UNION ALL SELECT a FROM v";
        var ddl = new[]
        {
            "CREATE TABLE t (a INT NOT NULL)",
            "CREATE TABLE u (a INT NOT NULL)",
            "CREATE TABLE v (a INT NOT NULL)",
        };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (string Table, object?[] Values, long Weight)[]
        {
            ("t", [1], 1L), ("t", [2], 1L), ("u", [2], 1L),
            ("v", [2], 1L), ("u", [3], 1L), ("t", [1], -1L),
            ("v", [2], -1L),
        };

        foreach (var (table, vs, w) in deltas)
        {
            typed!.Table(table).Push(new[] { (vs, w) });
            structural.Table(table).Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            for (var k = 0; k <= 4; k++)
            {
                Assert.Equal(
                    structural.WeightOf(k).Value,
                    typed.WeightOf(k).Value);
            }
        }
    }


    [Fact]
    public void TryCompile_AcceptsNullableSchema()
    {
        // Phase N3: scan-level nullable gate lifted. Scan + bare
        // projection of nullable columns round-trips through the
        // typed pipeline.
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT)"],   // nullable
            "SELECT * FROM t");

        var ok = TypedPlanCompiler.TryCompile(plan, out var typed);
        Assert.True(ok);
        Assert.NotNull(typed);
        typed!.Table("t").Insert(new object?[] { 7 });
        typed.Table("t").Insert(new object?[] { null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(7).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { null }).Value);
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
    public void Project_FunctionCalls()
    {
        // Phase 1.9: function calls land on the typed path.
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL)"],
            "SELECT ABS(a) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(-7);
        typed.Step();
        Assert.Equal(1L, typed.WeightOf(7).Value);
    }

    [Fact]
    public void Project_Nullif_NowSupported()
    {
        // Phase N3: NULLIF flows end-to-end through the typed
        // pipeline. The output column is Nullable<int>.
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT NULLIF(a, b) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(5, 5);    // NULLIF → NULL
        typed.Table("t").Insert(3, 7);     // NULLIF → 3
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(new object?[] { null }).Value);
        Assert.Equal(1L, typed.WeightOf(3).Value);
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
    public void Project_BuiltinUpper_NullablePropagatesNull()
    {
        // Per-function NULL propagation: UPPER(nullable_str) is now
        // accepted on the typed path and propagates NULL.
        var plan = CompilePlan(
            ["CREATE TABLE t (s VARCHAR)"],
            "SELECT UPPER(s) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert("hello");
        typed.Table("t").Insert(new object?[] { null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(new object?[] { "HELLO" }).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { null }).Value);
    }

    [Fact]
    public void Project_BuiltinAbs_NullablePropagatesNull()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (v INT)"],
            "SELECT ABS(v) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(-5);
        typed.Table("t").Insert(new object?[] { null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(5).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { null }).Value);
    }

    [Fact]
    public void Project_BuiltinPower_NullablePropagatesNull()
    {
        // POWER(nullable, nullable): NULL on either arg → NULL result.
        var plan = CompilePlan(
            ["CREATE TABLE t (b INT, e INT)"],
            "SELECT POWER(b, e) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(2, 3);
        typed.Table("t").Insert(new object?[] { null, 3 });
        typed.Table("t").Insert(new object?[] { 2, null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(8.0).Value);
        Assert.Equal(2L, typed.WeightOf(new object?[] { null }).Value);
    }

    [Fact]
    public void Project_BuiltinConcat_SkipsNullsPgSemantics()
    {
        // CONCAT skips NULL args (matches structural's PG semantics).
        // Result is never NULL even for all-NULL input.
        var plan = CompilePlan(
            ["CREATE TABLE t (a VARCHAR, b VARCHAR)"],
            "SELECT CONCAT(a, '|', b) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert("x", "y");
        typed.Table("t").Insert(new object?[] { null, "y" });
        typed.Table("t").Insert(new object?[] { "x", null });
        typed.Table("t").Insert(new object?[] { null, null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf("x|y").Value);
        Assert.Equal(1L, typed.WeightOf("|y").Value);
        Assert.Equal(1L, typed.WeightOf("x|").Value);
        Assert.Equal(1L, typed.WeightOf("|").Value);
    }

    [Fact]
    public void Project_BuiltinGreatest_SkipsNullsPgSemantics()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (a INT, b INT)"],
            "SELECT GREATEST(a, b) FROM t");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(3, 7);                       // 7
        typed.Table("t").Insert(new object?[] { null, 5 });   // 5 (null skipped)
        typed.Table("t").Insert(new object?[] { 9, null });   // 9
        typed.Table("t").Insert(new object?[] { null, null }); // NULL
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(7).Value);
        Assert.Equal(1L, typed.WeightOf(5).Value);
        Assert.Equal(1L, typed.WeightOf(9).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { null }).Value);
    }

    [Fact]
    public void Filter_NullablePredicate_NullDropsRow()
    {
        // WHERE on a nullable column: "v > 5" evaluates to TRUE,
        // FALSE, or NULL per 3VL. SQL WHERE keeps only TRUE rows;
        // NULL and FALSE both drop. Inside the typed pipeline this
        // is Nullable<bool>.GetValueOrDefault().
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, v INT)"],
            "SELECT id FROM t WHERE v > 5");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);                  // TRUE  → keep
        typed.Table("t").Insert(2, 3);                    // FALSE → drop
        typed.Table("t").Insert(new object?[] { 3, null }); // NULL → drop
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1).Value);
        Assert.Equal(0L, typed.WeightOf(2).Value);
        Assert.Equal(0L, typed.WeightOf(3).Value);
    }

    [Fact]
    public void Filter_NullablePredicate_IsNullKeepsNullRows()
    {
        // IS NULL on a nullable column returns a non-nullable bool
        // (the value is definite TRUE/FALSE — never NULL), so the
        // predicate's resolver type is plain bool and the
        // bool-typed fast path handles it. Just pin the behavior.
        var plan = CompilePlan(
            ["CREATE TABLE t (id INT NOT NULL, v INT)"],
            "SELECT id FROM t WHERE v IS NULL");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(new object?[] { 2, null });
        typed.Table("t").Insert(new object?[] { 3, null });
        typed.Step();

        Assert.Equal(0L, typed.WeightOf(1).Value);
        Assert.Equal(1L, typed.WeightOf(2).Value);
        Assert.Equal(1L, typed.WeightOf(3).Value);
    }

    [Fact]
    public void Filter_NullablePredicate_DifferentialAgainstStructural()
    {
        const string sql = "SELECT id, v FROM t WHERE v > 5 AND v < 50";
        var ddl = new[] { "CREATE TABLE t (id INT NOT NULL, v INT)" };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (object?[], long)[]
        {
            (new object?[] { 1, 10 }, 1L),    // TRUE — kept
            (new object?[] { 2, 3 }, 1L),     // FALSE — dropped
            (new object?[] { 3, null }, 1L),  // NULL — dropped
            (new object?[] { 4, 100 }, 1L),   // FALSE (v < 50 fails) — dropped
            (new object?[] { 5, 20 }, 1L),    // TRUE — kept
            (new object?[] { 1, 10 }, -1L),   // retract
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            object?[][] candidates =
            [
                [1, 10], [2, 3], [3, null], [4, 100], [5, 20],
            ];
            foreach (var cand in candidates)
            {
                Assert.Equal(
                    structural.WeightOf(cand).Value,
                    typed.WeightOf(cand).Value);
            }
        }
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
    public void Aggregate_Min_NonNullArg()
    {
        // Phase N5: MIN/MAX wired into the typed dispatch. Non-null
        // arg variant tracks Active set of positive-weight values
        // and returns the min in O(log n) on the distinct count.
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, MIN(v), MAX(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 3);
        typed.Table("t").Insert(1, 7);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 3, 10).Value);
    }

    [Fact]
    public void Aggregate_Min_RetractsShifts()
    {
        // Removing the current min shifts MIN to the next-smallest
        // positive-weight value.
        var plan = CompilePlan(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, MIN(v) FROM t GROUP BY k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 5);
        typed.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 15);
        typed.Step();
        Assert.Equal(1L, typed.WeightOf(1, 5).Value);

        typed.Table("t").Delete(1, 5);
        typed.Step();
        Assert.Equal(-1L, typed.WeightOf(1, 5).Value);
        Assert.Equal(1L, typed.WeightOf(1, 10).Value);
    }

    [Fact]
    public void Aggregate_Min_NullableArg_SkipsNullsAndAllNullEmitsNull()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, MIN(v), MAX(v) FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        // Mixed group: NULLs skipped.
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(new object?[] { 1, null });
        typed.Table("t").Insert(1, 3);
        // All-NULL group: MIN/MAX both NULL.
        typed.Table("t").Insert(new object?[] { 2, null });
        typed.Table("t").Insert(new object?[] { 2, null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 3, 10).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { 2, null, null }).Value);
    }

    [Fact]
    public void Aggregate_MinMaxDifferentialAgainstStructural()
    {
        const string sql = "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY g";
        var ddl = new[] { "CREATE TABLE t (g INT NOT NULL, v INT)" };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (object?[], long)[]
        {
            (new object?[] { 1, 5 }, 1L),
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 1, null }, 1L),
            (new object?[] { 1, 5 }, -1L),
            (new object?[] { 2, null }, 1L),
            (new object?[] { 2, 3 }, 1L),
            (new object?[] { 2, null }, -1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            object?[][] candidates =
            [
                [1, 5, 5], [1, 5, 10], [1, 10, 10], [1, null, null],
                [2, 3, 3], [2, null, null],
            ];
            foreach (var cand in candidates)
            {
                Assert.Equal(
                    structural.WeightOf(cand).Value,
                    typed.WeightOf(cand).Value);
            }
        }
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
    public void Aggregate_NullableGroupKey_NullIsOneBucket()
    {
        // Nullable group-key column flows through typed: TKey
        // carries a Nullable<int> field, and SQL GROUP BY semantics
        // (one bucket for NULL, distinct from any non-null value)
        // come from the emitted struct's IEquatable / GetHashCode.
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT, v INT NOT NULL)"],
            "SELECT g, SUM(v) FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, 20);
        typed.Table("t").Insert(new object?[] { null, 5 });
        typed.Table("t").Insert(new object?[] { null, 7 });
        typed.Table("t").Insert(2, 100);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 30L).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { null, 12L }).Value);
        Assert.Equal(1L, typed.WeightOf(2, 100L).Value);
    }

    [Fact]
    public void Aggregate_NullableGroupKey_DifferentialAgainstStructural()
    {
        const string sql = "SELECT g, COUNT(*) AS cs, SUM(v) AS sv FROM t GROUP BY g";
        var ddl = new[] { "CREATE TABLE t (g INT, v INT NOT NULL)" };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (object?[], long)[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { null, 5 }, 1L),
            (new object?[] { 1, 20 }, 1L),
            (new object?[] { null, 7 }, 1L),
            (new object?[] { 2, 100 }, 1L),
            (new object?[] { null, 5 }, -1L),   // retract one null-group row
            (new object?[] { 1, 10 }, -1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            object?[][] candidates =
            [
                [1, 1L, 10L], [1, 1L, 20L], [1, 2L, 30L],
                [null, 1L, 5L], [null, 1L, 7L], [null, 2L, 12L],
                [2, 1L, 100L],
            ];
            foreach (var cand in candidates)
            {
                Assert.Equal(
                    structural.WeightOf(cand).Value,
                    typed.WeightOf(cand).Value);
            }
        }
    }

    [Fact]
    public void Aggregate_CountNullable_SkipsNulls()
    {
        // Phase N4: COUNT(nullable col) routes through typed and
        // dispatches to the nullable-arg variant. COUNT(*) counts
        // every row; COUNT(v) counts only rows where v is not NULL.
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, COUNT(*) AS cs, COUNT(v) AS cv FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, null);
        typed.Table("t").Insert(1, 20);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(1, 3L, 2L).Value);
    }

    [Fact]
    public void Aggregate_SumNullable_MixedGroup()
    {
        // Phase N4: SUM(nullable col) skips NULL contributions and
        // returns Nullable<long>. With at least one non-null row
        // present the result is a non-null long.
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, SUM(v) FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 10);
        typed.Table("t").Insert(1, null);
        typed.Table("t").Insert(1, 20);
        typed.Step();

        // sum = 10 + 20 = 30 (NULL row skipped). Result column is
        // nullable per resolver — assert by the wire shape.
        Assert.Equal(1L, typed.WeightOf(1, 30L).Value);
    }

    [Fact]
    public void Aggregate_SumNullable_AllNullGroup_EmitsNull()
    {
        // Phase N4: a non-empty group whose every row has a NULL arg
        // emits SUM = NULL. The linear gate emits the row (group is
        // present); the nullable SUM aggregator returns null because
        // DistinctNonNullRows = 0.
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, SUM(v) FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(new object?[] { 1, null });
        typed.Table("t").Insert(new object?[] { 1, null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(new object?[] { 1, null }).Value);
    }

    [Fact]
    public void Aggregate_AvgNullable_AllNullGroup_EmitsNull()
    {
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT NOT NULL, v DOUBLE PRECISION)"],
            "SELECT g, AVG(v) FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(new object?[] { 1, null });
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(new object?[] { 1, null }).Value);
    }

    [Fact]
    public void Aggregate_SumNullable_TransitionsToAllNull_OnRetraction()
    {
        // Retracting the only non-null row in a group leaves the
        // group non-empty (still has NULL rows) so a row is emitted,
        // but SUM transitions to NULL.
        var plan = CompilePlan(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, SUM(v) FROM t GROUP BY g");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("t").Insert(1, 5);
        typed.Table("t").Insert(new object?[] { 1, null });
        typed.Step();
        Assert.Equal(1L, typed.WeightOf(1, 5L).Value);

        typed.Table("t").Delete(1, 5);
        typed.Step();
        // The (1, 5) row is retracted; a new (1, NULL) row replaces it.
        Assert.Equal(-1L, typed.WeightOf(1, 5L).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { 1, null }).Value);
    }

    [Fact]
    public void Aggregate_NullableDifferentialAgainstStructural()
    {
        const string sql = "SELECT g, COUNT(*) AS cs, COUNT(v) AS cv, SUM(v) AS sv FROM t GROUP BY g";
        var ddl = new[] { "CREATE TABLE t (g INT NOT NULL, v INT)" };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var deltas = new (object?[], long)[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 1, null }, 1L),
            (new object?[] { 1, 20 }, 1L),
            (new object?[] { 2, null }, 1L),
            (new object?[] { 1, 10 }, -1L),
            (new object?[] { 2, null }, -1L),
            (new object?[] { 2, 7 }, 1L),
        };

        foreach (var (vs, w) in deltas)
        {
            typed!.Table("t").Push(new[] { (vs, w) });
            structural.Table("t").Push(new[] { (vs, w) });
            typed.Step();
            structural.Step();

            // Compare a sweep that covers the values actually produced
            // by the inputs (including NULL SUM results).
            object?[][] candidates =
            [
                [1, 0L, 0L, null], [1, 1L, 0L, null], [1, 1L, 1L, 10L], [1, 1L, 1L, 20L],
                [1, 2L, 1L, 10L], [1, 2L, 1L, 20L], [1, 2L, 2L, 30L], [1, 3L, 2L, 30L],
                [2, 0L, 0L, null], [2, 1L, 0L, null], [2, 1L, 1L, 7L],
            ];
            foreach (var cand in candidates)
            {
                Assert.Equal(
                    structural.WeightOf(cand).Value,
                    typed.WeightOf(cand).Value);
            }
        }
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
    public void Join_LeftOuter_NullPadsUnmatchedRight()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT v, w FROM l LEFT JOIN r ON l.k = r.k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("l").Insert(1, 10);
        typed.Table("l").Insert(2, 20);          // no match in r → NULL-padded
        typed.Table("r").Insert(1, 100);
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(10, 100).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { 20, null }).Value);
    }

    [Fact]
    public void Join_RightOuter_NullPadsUnmatchedLeft()
    {
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT v, w FROM l RIGHT JOIN r ON l.k = r.k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("l").Insert(1, 10);
        typed.Table("r").Insert(1, 100);
        typed.Table("r").Insert(2, 200);         // no match in l → NULL-padded
        typed.Step();

        Assert.Equal(1L, typed.WeightOf(10, 100).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { null, 200 }).Value);
    }

    [Fact]
    public void Join_LeftOuter_RetractMatch_ShiftsToNullPadded()
    {
        // Insert a matching right row, then retract it — the matched
        // row retracts and a NULL-padded row appears in its place.
        var plan = CompilePlan(
            [
                "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT v, w FROM l LEFT JOIN r ON l.k = r.k");

        Assert.True(TypedPlanCompiler.TryCompile(plan, out var typed));
        typed!.Table("l").Insert(1, 10);
        typed.Table("r").Insert(1, 100);
        typed.Step();
        Assert.Equal(1L, typed.WeightOf(10, 100).Value);
        Assert.Equal(0L, typed.WeightOf(new object?[] { 10, null }).Value);

        typed.Table("r").Delete(1, 100);
        typed.Step();
        Assert.Equal(-1L, typed.WeightOf(10, 100).Value);
        Assert.Equal(1L, typed.WeightOf(new object?[] { 10, null }).Value);
    }

    [Fact]
    public void Join_LeftOuter_DifferentialAgainstStructural()
    {
        const string sql = "SELECT v, w FROM l LEFT JOIN r ON l.k = r.k";
        var ddl = new[]
        {
            "CREATE TABLE l (k INT NOT NULL, v INT NOT NULL)",
            "CREATE TABLE r (k INT NOT NULL, w INT NOT NULL)",
        };
        var planTyped = CompilePlan(ddl, sql);
        var planStructural = CompilePlan(ddl, sql);

        Assert.True(TypedPlanCompiler.TryCompile(planTyped, out var typed));
        var structural = PlanToCircuit.Compile(planStructural);

        var lDeltas = new (object?[], long)[]
        {
            (new object?[] { 1, 10 }, 1L),
            (new object?[] { 2, 20 }, 1L),
            (new object?[] { 1, 11 }, 1L),
            (new object?[] { 2, 20 }, -1L),
        };
        var rDeltas = new (object?[], long)[]
        {
            (new object?[] { 1, 100 }, 1L),
            (new object?[] { 3, 300 }, 1L),
            (new object?[] { 1, 100 }, -1L),
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

            object?[][] candidates =
            [
                [10, 100], [11, 100], [20, 100],
                [10, null], [11, null], [20, null],
            ];
            foreach (var cand in candidates)
            {
                Assert.Equal(
                    structural.WeightOf(cand).Value,
                    typed.WeightOf(cand).Value);
            }
        }
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
