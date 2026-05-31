// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class CompilerTests
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

    // ---- Filter ----

    [Fact]
    public void Filter_KeepsMatchingRows()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, v INT NOT NULL)"],
            "SELECT id, v FROM t WHERE v > 10");

        q.Table("t").Insert(1, 5);
        q.Table("t").Insert(2, 20);
        q.Table("t").Insert(3, 15);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2, 20));
        Assert.Equal(1, WeightOf(q.Current, 3, 15));
    }

    [Fact]
    public void Filter_EmitsRetractionsOnDelete()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t WHERE id > 0");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();
        Assert.Equal(2, q.Current.Count);

        q.Table("t").Delete(1);
        q.Step();
        // The DELTA output is what's in Current: one negative-weight row.
        Assert.Equal(-1, WeightOf(q.Current, 1));
    }

    [Fact]
    public void Filter_NullPredicate_CoercesToFalse()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, v INT)"],
            "SELECT id FROM t WHERE v > 0");

        q.Table("t").Insert(1, 5);
        q.Table("t").Insert(2, null);
        q.Table("t").Insert(3, -1);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
    }

    // ---- Projection ----

    [Fact]
    public void Project_ComputesExpression()
    {
        var q = Compile(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a + b AS sum FROM t");

        q.Table("t").Insert(1, 2);
        q.Table("t").Insert(3, 4);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(1, WeightOf(q.Current, 7));
    }

    [Fact]
    public void Project_Coalesce()
    {
        var q = Compile(
            ["CREATE TABLE t (a INT, b INT NOT NULL)"],
            "SELECT COALESCE(a, b) AS x FROM t");

        q.Table("t").Insert(null, 99);
        q.Table("t").Insert(1, 99);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 99));
        Assert.Equal(1, WeightOf(q.Current, 1));
    }

    // ---- Inner join ----

    [Fact]
    public void InnerJoin_CombinesMatchingRows()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("a").Insert(2, 200);
        q.Table("b").Insert(1, 10);
        q.Table("b").Insert(3, 30);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 100, 10));
    }

    [Fact]
    public void InnerJoin_NullKeysAreDropped()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT, v INT NOT NULL)",
                "CREATE TABLE b (k INT, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");

        // Null-keyed row on either side must not produce a join tuple.
        q.Table("a").Insert(null, 100);
        q.Table("b").Insert(null, 10);
        q.Step();
        Assert.Equal(0, q.Current.Count);
    }

    [Fact]
    public void InnerJoin_IsIncremental()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k");

        // Initial state.
        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 100, 10));

        // Second tick adds one row on each side; delta should contain the three
        // cross-terms: Δa ⋈ b_prev, a_prev ⋈ Δb, Δa ⋈ Δb (Feldera §4.3).
        q.Table("a").Insert(1, 200);
        q.Table("b").Insert(1, 20);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 200, 10));
        Assert.Equal(1, WeightOf(q.Current, 100, 20));
        Assert.Equal(1, WeightOf(q.Current, 200, 20));
    }

    // ---- Cross / non-equi inner join (keyless, unit-key nested loop) ----

    [Fact]
    public void CrossJoin_ProducesCartesianProduct()
    {
        var q = Compile(
            [
                "CREATE TABLE a (v INT NOT NULL)",
                "CREATE TABLE b (w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a CROSS JOIN b");

        q.Table("a").Insert(1);
        q.Table("a").Insert(2);
        q.Table("b").Insert(10);
        q.Table("b").Insert(20);
        q.Step();

        Assert.Equal(4, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 10));
        Assert.Equal(1, WeightOf(q.Current, 1, 20));
        Assert.Equal(1, WeightOf(q.Current, 2, 10));
        Assert.Equal(1, WeightOf(q.Current, 2, 20));
    }

    [Fact]
    public void NonEquiJoin_FiltersCrossProduct()
    {
        var q = Compile(
            [
                "CREATE TABLE a (x INT NOT NULL)",
                "CREATE TABLE b (y INT NOT NULL)",
            ],
            "SELECT a.x, b.y FROM a JOIN b ON a.x > b.y");

        q.Table("a").Insert(1);
        q.Table("a").Insert(5);
        q.Table("b").Insert(2);
        q.Table("b").Insert(4);
        q.Step();

        // Only pairs with a.x > b.y survive: (5,2) and (5,4).
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 5, 2));
        Assert.Equal(1, WeightOf(q.Current, 5, 4));
    }

    [Fact]
    public void CrossJoin_IsIncremental()
    {
        var q = Compile(
            [
                "CREATE TABLE a (v INT NOT NULL)",
                "CREATE TABLE b (w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a CROSS JOIN b");

        q.Table("a").Insert(1);
        q.Table("b").Insert(10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 10));

        // Add one row on each side: delta has the three cross-terms.
        q.Table("a").Insert(2);
        q.Table("b").Insert(20);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 20));
        Assert.Equal(1, WeightOf(q.Current, 2, 10));
        Assert.Equal(1, WeightOf(q.Current, 2, 20));

        // Retract a left row: every pair it contributed retracts.
        q.Table("a").Delete(1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 10));
        Assert.Equal(-1, WeightOf(q.Current, 1, 20));
    }

    [Fact]
    public void NonEquiJoin_TypedPathCompiles()
    {
        // The keyless inner join must compile on the typed fast path (the
        // residual predicate a.x > b.y is typed-compilable).
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE a (x INT NOT NULL)"));
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE b (y INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT a.x, b.y FROM a JOIN b ON a.x > b.y"))).Query;

        Assert.True(TypedPlanCompiler.TryCompile(plan, out _));
    }

    [Fact]
    public void CrossJoin_SpineAndFlatAgree()
    {
        const string ddlA = "CREATE TABLE a (v INT NOT NULL)";
        const string ddlB = "CREATE TABLE b (w INT NOT NULL)";
        const string sql = "SELECT a.v, b.w FROM a CROSS JOIN b";

        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(ddlA));
        resolver.Resolve(Parser.ParseStatement(ddlB));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;

        var flat = PlanToCircuit.Compile(plan);
        var spine = PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine });

        foreach (var q in new[] { flat, spine })
        {
            q.Table("a").Insert(1);
            q.Table("a").Insert(2);
            q.Table("b").Insert(10);
            q.Step();
            Assert.Equal(2, q.Current.Count);
            Assert.Equal(1, WeightOf(q.Current, 1, 10));
            Assert.Equal(1, WeightOf(q.Current, 2, 10));
        }
    }

    [Fact]
    public void CommaJoin_WithWhereEquiPredicate_ActsAsInnerJoin()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a, b WHERE a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("a").Insert(2, 200);
        q.Table("b").Insert(1, 10);
        q.Table("b").Insert(3, 30);
        q.Step();

        // Implicit cross product filtered by a.k = b.k → only (1,…) matches.
        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 100, 10));
    }

    [Fact]
    public void CommaJoin_ThreeTables_CartesianProduct()
    {
        var q = Compile(
            [
                "CREATE TABLE a (v INT NOT NULL)",
                "CREATE TABLE b (w INT NOT NULL)",
                "CREATE TABLE c (z INT NOT NULL)",
            ],
            "SELECT a.v, b.w, c.z FROM a, b, c");

        q.Table("a").Insert(1);
        q.Table("a").Insert(2);
        q.Table("b").Insert(10);
        q.Table("c").Insert(100);
        q.Step();

        // 2 × 1 × 1 = 2 rows.
        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 100));
        Assert.Equal(1, WeightOf(q.Current, 2, 10, 100));
    }

    // ---- Group-by aggregate ----

    [Fact]
    public void GroupBy_Sum_ReturnsPerGroupTotals()
    {
        var q = Compile(
            ["CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)"],
            "SELECT dept, SUM(salary) AS total FROM e GROUP BY dept");

        q.Table("e").Insert("eng", 100);
        q.Table("e").Insert("eng", 200);
        q.Table("e").Insert("sales", 150);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, "eng", 300L));
        Assert.Equal(1, WeightOf(q.Current, "sales", 150L));
    }

    [Fact]
    public void GroupBy_EmitsRetractionOnChange()
    {
        var q = Compile(
            ["CREATE TABLE e (dept VARCHAR NOT NULL, salary INT NOT NULL)"],
            "SELECT dept, SUM(salary) AS total FROM e GROUP BY dept");

        q.Table("e").Insert("eng", 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, "eng", 100L));

        // Add another eng row — previous total must retract, new total must emit.
        q.Table("e").Insert("eng", 50);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, "eng", 100L));
        Assert.Equal(1, WeightOf(q.Current, "eng", 150L));
    }

    [Fact]
    public void GroupBy_CountAndCountStar_DifferOnNulls()
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, COUNT(*) AS cs, COUNT(v) AS cv FROM t GROUP BY g");

        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, null);
        q.Table("t").Insert(1, 20);
        q.Step();

        // group 1: 3 rows total (COUNT(*)), 2 with non-NULL v (COUNT(v)).
        Assert.Equal(1, WeightOf(q.Current, 1, 3L, 2L));
    }

    [Fact]
    public void GroupBy_NoKeys_SingleRowOut()
    {
        var q = Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT SUM(v) AS s FROM t");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 6L));
    }

    [Fact]
    public void GroupBy_MinMax_HandleNullsAndRetractions()
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, MIN(v) AS mn, MAX(v) AS mx FROM t GROUP BY g");

        q.Table("t").Insert(1, 5);
        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(1, null);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 5, 10));

        // Remove the min — max stays, min shifts.
        q.Table("t").Delete(1, 5);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 5, 10));
        Assert.Equal(1, WeightOf(q.Current, 1, 10, 10));
    }

    // ---- LEFT OUTER JOIN ----

    [Fact]
    public void LeftJoin_MatchedAndUnmatchedRowsAppear()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("a").Insert(2, 200);
        q.Table("b").Insert(1, 10);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 100, 10));         // matched
        Assert.Equal(1, WeightOf(q.Current, 200, null));        // unmatched → NULL-padded
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void LeftJoin_GainedMatch_RetractsNullPaddedRow()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 100, null));

        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 100, null));
        Assert.Equal(1, WeightOf(q.Current, 100, 10));
    }

    [Fact]
    public void LeftJoin_LostMatch_RetractsJoined_EmitsNullPadded()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 100, 10));

        q.Table("b").Delete(1, 10);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 100, 10));
        Assert.Equal(1, WeightOf(q.Current, 100, null));
    }

    [Fact]
    public void LeftJoin_NullKeyLeftRow_ProducesNullPaddedOutput()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

        q.Table("a").Insert(null, 100);
        q.Table("b").Insert(1, 10);
        q.Step();

        // NULL-keyed left row can never match anything → one NULL-padded row.
        Assert.Equal(1, WeightOf(q.Current, 100, null));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void LeftJoin_KeyWithTwoMatches_LosesOne_StillMatched()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(1, 10);
        q.Table("b").Insert(1, 20);
        q.Step();
        Assert.Equal(2, q.Current.Count);

        q.Table("b").Delete(1, 10);
        q.Step();
        // Only one joined row retracts; key still has a match, so no NULL-padded row.
        Assert.Equal(-1, WeightOf(q.Current, 100, 10));
        Assert.Equal(0, WeightOf(q.Current, 100, null));
    }

    [Fact]
    public void LeftJoin_RightNullKey_DoesNotMatchAnything()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(null, 999); // NULL key can never equi-match
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 100, null)); // unmatched → NULL-padded
        Assert.Equal(1, q.Current.Count);
    }

    // ---- RIGHT OUTER JOIN ----

    [Fact]
    public void RightJoin_MatchedAndUnmatchedRowsAppear()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a RIGHT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(1, 10);   // matched
        q.Table("b").Insert(2, 20);   // right-only (no matching a.k=2)
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 100, 10));         // matched
        Assert.Equal(1, WeightOf(q.Current, null, 20));         // unmatched right
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void RightJoin_LostMatch_RetractsJoined_EmitsNullPaddedLeft()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a RIGHT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 100, 10));

        q.Table("a").Delete(1, 100);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 100, 10));
        Assert.Equal(1, WeightOf(q.Current, null, 10));
    }

    [Fact]
    public void RightJoin_NullKeyRightRow_ProducesNullPaddedOutput()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a RIGHT JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(null, 999); // NULL key can never match
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, null, 999));
        Assert.Equal(1, q.Current.Count);
    }

    // ---- FULL OUTER JOIN ----

    [Fact]
    public void FullJoin_MatchedLeftOnlyRightOnly_AllAppear()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.k, a.v, b.k, b.w FROM a FULL JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100); // matched
        q.Table("a").Insert(2, 200); // left-only
        q.Table("b").Insert(1, 10);  // matched
        q.Table("b").Insert(3, 30);  // right-only
        q.Step();

        Assert.Equal(3, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 100, 1, 10));       // matched
        Assert.Equal(1, WeightOf(q.Current, 2, 200, null, null));  // left-only → pad right
        Assert.Equal(1, WeightOf(q.Current, null, null, 3, 30));   // right-only → pad left
    }

    [Fact]
    public void FullJoin_GainedMatch_RetractsPad_EmitsJoined()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.k, a.v, b.k, b.w FROM a FULL JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100, null, null));

        // Right row arrives on the same key: pad-right retracts, joined emits.
        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 100, null, null));
        Assert.Equal(1, WeightOf(q.Current, 1, 100, 1, 10));
    }

    [Fact]
    public void FullJoin_LostRight_RetractsJoined_EmitsNullPaddedRight()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.k, a.v, b.k, b.w FROM a FULL JOIN b ON a.k = b.k");

        q.Table("a").Insert(1, 100);
        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 100, 1, 10));

        // Remove the only right row: joined retracts, left row becomes pad-right.
        q.Table("b").Delete(1, 10);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1, 100, 1, 10));
        Assert.Equal(1, WeightOf(q.Current, 1, 100, null, null));
    }

    [Fact]
    public void FullJoin_NullKeysBypassToBothBranches()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT, v INT NOT NULL)",
                "CREATE TABLE b (k INT, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a FULL JOIN b ON a.k = b.k");

        // NULL keys never match: each emits its own NULL-padded row.
        q.Table("a").Insert(null, 100);
        q.Table("b").Insert(null, 10);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 100, null)); // left-only (a.v, NULL b.w)
        Assert.Equal(1, WeightOf(q.Current, null, 10));  // right-only (NULL a.v, b.w)
    }

    [Fact]
    public void FullJoin_Using_CoalescesMergedKey()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT k, v, w FROM a FULL JOIN b USING (k)");

        q.Table("a").Insert(1, 100); // matched
        q.Table("a").Insert(2, 200); // left-only
        q.Table("b").Insert(1, 10);  // matched
        q.Table("b").Insert(3, 30);  // right-only
        q.Step();

        // Merged k is COALESCE(a.k, b.k): present for every row including the
        // right-only one (k=3) where a.k is NULL.
        Assert.Equal(3, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1, 100, 10));
        Assert.Equal(1, WeightOf(q.Current, 2, 200, null));
        Assert.Equal(1, WeightOf(q.Current, 3, null, 30));
    }

    [Fact]
    public void FullJoin_TypedPathCompiles()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)"));
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT a.v, b.w FROM a FULL JOIN b ON a.k = b.k"))).Query;

        Assert.True(TypedPlanCompiler.TryCompile(plan, out _));
    }

    [Fact]
    public void FullJoin_SpineAndFlatAgree()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)"));
        resolver.Resolve(Parser.ParseStatement("CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)"));
        var plan = ((SelectPlan)resolver.Resolve(
            Parser.ParseStatement("SELECT a.k, a.v, b.k, b.w FROM a FULL JOIN b ON a.k = b.k"))).Query;

        var flat = PlanToCircuit.Compile(plan);
        var spine = PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine });

        foreach (var q in new[] { flat, spine })
        {
            q.Table("a").Insert(1, 100);
            q.Table("a").Insert(2, 200);
            q.Table("b").Insert(1, 10);
            q.Table("b").Insert(3, 30);
            q.Step();
            Assert.Equal(3, q.Current.Count);
            Assert.Equal(1, WeightOf(q.Current, 1, 100, 1, 10));
            Assert.Equal(1, WeightOf(q.Current, 2, 200, null, null));
            Assert.Equal(1, WeightOf(q.Current, null, null, 3, 30));
        }
    }

    // ---- Combined (join + group-by) ----

    [Fact]
    public void JoinedGroupBy_MatchesOracle()
    {
        var q = Compile(
            [
                "CREATE TABLE orders (cust INT NOT NULL, amount INT NOT NULL)",
                "CREATE TABLE customers (id INT NOT NULL, region VARCHAR NOT NULL)",
            ],
            "SELECT c.region, SUM(o.amount) AS total " +
            "FROM orders o JOIN customers c ON o.cust = c.id " +
            "GROUP BY c.region");

        q.Table("customers").Insert(1, "us");
        q.Table("customers").Insert(2, "us");
        q.Table("customers").Insert(3, "eu");
        q.Table("orders").Insert(1, 100);
        q.Table("orders").Insert(2, 50);
        q.Table("orders").Insert(3, 200);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, "us", 150L));
        Assert.Equal(1, WeightOf(q.Current, "eu", 200L));
    }
}
