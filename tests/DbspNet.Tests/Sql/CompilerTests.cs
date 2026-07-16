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

    // ---- Computed equi-keys (hoisted to synthetic key columns) ----

    /// <summary>
    /// The differential that matters for the computed-equi-key lowering: a
    /// hoisted key join must agree with the keyless cross-product-plus-filter
    /// spelling it replaces. The batch oracle cannot catch a lowering bug here
    /// (it evaluates the same lowered plan), so compare the two spellings.
    /// </summary>
    private static void AssertComputedKeyMatchesCrossFilter(
        string[] ddl, string onPredicate, Action<Func<string, TableInput>> load)
    {
        var keyed = Compile(ddl, $"SELECT a.x, b.y FROM a JOIN b ON {onPredicate}");
        var cross = Compile(ddl, $"SELECT a.x, b.y FROM a CROSS JOIN b WHERE {onPredicate}");
        load(keyed.Table);
        load(cross.Table);
        keyed.Step();
        cross.Step();
        Assert.True(
            keyed.Current.Equals(cross.Current),
            $"ON {onPredicate}\n  keyed={keyed.Current}\n  cross={cross.Current}");
    }

    [Fact]
    public void ComputedEquiKey_CastBothSides_MatchesCrossFilter() =>
        AssertComputedKeyMatchesCrossFilter(
            ["CREATE TABLE a (x INT NOT NULL)", "CREATE TABLE b (y BIGINT NOT NULL)"],
            "CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)",
            tbl =>
            {
                tbl("a").Insert(1);
                tbl("a").Insert(2);
                tbl("a").Insert(2);   // duplicate: bag multiplicity must survive
                tbl("b").Insert(2L);
                tbl("b").Insert(3L);
            });

    [Fact]
    public void ComputedEquiKey_UpperOnOneSide_MatchesCrossFilter() =>
        AssertComputedKeyMatchesCrossFilter(
            ["CREATE TABLE a (x VARCHAR NOT NULL)", "CREATE TABLE b (y VARCHAR NOT NULL)"],
            "UPPER(a.x) = b.y",
            tbl =>
            {
                tbl("a").Insert("ab");
                tbl("a").Insert("AB");
                tbl("b").Insert("AB");
                tbl("b").Insert("zz");
            });

    [Fact]
    public void ComputedEquiKey_NullOperand_MatchesCrossFilter() =>
        // NULL handling is where the two spellings could diverge: as an equi-key
        // a NULL key is filtered; as a residual, `NULL = NULL` is NULL, not TRUE.
        // Both drop the row — this pins that they agree.
        AssertComputedKeyMatchesCrossFilter(
            ["CREATE TABLE a (x INT)", "CREATE TABLE b (y BIGINT)"],
            "CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)",
            tbl =>
            {
                tbl("a").Insert(new object?[] { null });
                tbl("a").Insert(1);
                tbl("b").Insert(new object?[] { null });
                tbl("b").Insert(1L);
            });

    [Fact]
    public void ComputedEquiKey_WithRightSideResidual_MatchesCrossFilter() =>
        // Two conjuncts: one hoisted to a key, one left as a residual. The
        // residual MUST reference a RIGHT column — hoisting a left key shifts
        // every right index right by leftSynth.Count, and only a right-side
        // reference exercises that remap. A left-only residual would pass even
        // with the remap deleted.
        AssertComputedKeyMatchesCrossFilter(
            ["CREATE TABLE a (x INT NOT NULL)", "CREATE TABLE b (y BIGINT NOT NULL)"],
            "CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR) AND b.y > 1",
            tbl =>
            {
                tbl("a").Insert(1);
                tbl("a").Insert(2);
                tbl("b").Insert(1L);
                tbl("b").Insert(2L);
            });

    [Fact]
    public void ComputedEquiKey_WithCrossSideResidual_MatchesCrossFilter() =>
        AssertComputedKeyMatchesCrossFilter(
            ["CREATE TABLE a (x INT NOT NULL)", "CREATE TABLE b (y BIGINT NOT NULL)"],
            "CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR) AND CAST(a.x AS BIGINT) >= b.y",
            tbl =>
            {
                tbl("a").Insert(1);
                tbl("a").Insert(2);
                tbl("b").Insert(1L);
                tbl("b").Insert(2L);
            });

    [Fact]
    public void ComputedEquiKey_Retraction_MatchesCrossFilter() =>
        AssertComputedKeyMatchesCrossFilter(
            ["CREATE TABLE a (x INT NOT NULL)", "CREATE TABLE b (y BIGINT NOT NULL)"],
            "CAST(a.x AS VARCHAR) = CAST(b.y AS VARCHAR)",
            tbl =>
            {
                tbl("a").Insert(1);
                tbl("a").Insert(2);
                tbl("b").Insert(1L);
                tbl("b").Insert(2L);
                tbl("a").Delete(1);
                tbl("b").Delete(2L);
            });

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
    public void GroupBy_ArithmeticExpression()
    {
        // GROUP BY a + b — an expression key, not a bare column.
        var q = Compile(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, v INT NOT NULL)"],
            "SELECT a + b AS k, SUM(v) AS total FROM t GROUP BY a + b");

        q.Table("t").Insert(1, 2, 10);   // k = 3
        q.Table("t").Insert(3, 0, 20);   // k = 3
        q.Table("t").Insert(2, 2, 5);    // k = 4
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 3, 30L));
        Assert.Equal(1, WeightOf(q.Current, 4, 5L));
    }

    [Fact]
    public void GroupBy_ScalarFunctionExpression()
    {
        // GROUP BY LENGTH(name) — group rows by the length of a string.
        var q = Compile(
            ["CREATE TABLE t (name VARCHAR NOT NULL)"],
            "SELECT LENGTH(name) AS len, COUNT(*) AS c FROM t GROUP BY LENGTH(name)");

        q.Table("t").Insert("ab");     // len 2
        q.Table("t").Insert("cd");     // len 2
        q.Table("t").Insert("efg");    // len 3
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2, 2L));
        Assert.Equal(1, WeightOf(q.Current, 3, 1L));
    }

    [Fact]
    public void GroupBy_ExpressionKey_ReferencedDifferentlyInSelect()
    {
        // The group key (a + b) can be combined further in the SELECT list;
        // sub-trees that equal the key read from the group-key column.
        var q = Compile(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT (a + b) * 10 AS scaled, COUNT(*) AS c FROM t GROUP BY a + b");

        q.Table("t").Insert(1, 2);   // a+b = 3
        q.Table("t").Insert(2, 1);   // a+b = 3
        q.Table("t").Insert(4, 0);   // a+b = 4
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 30, 2L));   // (3)*10, 2 rows
        Assert.Equal(1, WeightOf(q.Current, 40, 1L));   // (4)*10, 1 row
    }

    [Fact]
    public void GroupBy_ExpressionKey_HavingReferencesKey()
    {
        var q = Compile(
            ["CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)"],
            "SELECT a + b AS k, COUNT(*) AS c FROM t GROUP BY a + b HAVING a + b > 3");

        q.Table("t").Insert(1, 2);   // k = 3 — filtered out by HAVING
        q.Table("t").Insert(2, 2);   // k = 4
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 4, 1L));
    }

    // ---- Nested ROW columns (flattened) ----

    [Fact]
    public void RowColumn_EndToEnd_ReadsNestedLeavesAsScalars()
    {
        // A flattened ROW table is indistinguishable from a flat table at
        // runtime: push rows positionally over the leaf columns, read nested
        // leaves back. Mirrors ivm-bench crm_customer_mgmt's leaf extraction.
        var q = Compile(
            [
                "CREATE TABLE cm (" +
                "  id BIGINT NOT NULL," +
                "  Customer ROW(" +
                "    Name ROW(First VARCHAR NULL, Last VARCHAR NULL) NULL," +
                "    Contact ROW(Phone ROW(Ctry BIGINT NULL, Local VARCHAR NULL) NULL) NULL," +
                "    _Tier BIGINT NULL" +
                "  ) NULL" +
                ")",
            ],
            "SELECT cm.id, cm.Customer.Name.Last AS ln, " +
            "cm.Customer.Contact.Phone.Ctry AS cc, " +
            "CAST(cm.Customer._Tier AS INTEGER) AS tier " +
            "FROM cm");

        // Leaves in declaration order: id, first, last, ctry, local, tier.
        q.Table("cm").Insert(1L, "Ada", "Lovelace", 44L, "1234", 3L);
        q.Table("cm").Insert(2L, "Alan", "Turing", 1L, "5678", 1L);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1L, "Lovelace", 44L, 3));
        Assert.Equal(1, WeightOf(q.Current, 2L, "Turing", 1L, 1));
    }

    [Fact]
    public void RowColumn_NullStruct_YieldsNullLeaves()
    {
        // A wholly-NULL nested struct presents each leaf as NULL — the
        // leaf-access-only semantics flattening relies on.
        var q = Compile(
            [
                "CREATE TABLE cm (" +
                "  id BIGINT NOT NULL," +
                "  Customer ROW(Name ROW(Last VARCHAR NULL) NULL) NULL" +
                ")",
            ],
            "SELECT cm.id, cm.Customer.Name.Last AS ln FROM cm");

        q.Table("cm").Insert(new object?[] { 1L, null });   // id=1, last=NULL
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1L, null));
    }

    // ---- STDDEV / VARIANCE ----

    private static double AggDouble(CompiledQuery q, params object?[] keyThenValueless)
    {
        // Find the single output row matching the leading key columns and return
        // its last (aggregate) column as a double.
        foreach (var (values, weight) in q.Current)
        {
            if (weight <= 0)
            {
                continue;
            }

            var match = true;
            for (var i = 0; i < keyThenValueless.Length; i++)
            {
                if (!Equals(values[i], keyThenValueless[i]))
                {
                    match = false;
                    break;
                }
            }

            if (match)
            {
                return Convert.ToDouble(values[^1], System.Globalization.CultureInfo.InvariantCulture);
            }
        }

        throw new Xunit.Sdk.XunitException("no matching output row");
    }

    [Fact]
    public void Stddev_PopAndSamp_KnownDataset()
    {
        // {2,4,4,4,5,5,7,9}: mean 5, population variance 4 (stddev 2),
        // sample variance 32/7 ≈ 4.5714 (stddev ≈ 2.1381).
        foreach (var (fn, expected) in new[]
        {
            ("STDDEV_POP(v)", 2.0),
            ("VAR_POP(v)", 4.0),
            ("STDDEV_SAMP(v)", Math.Sqrt(32.0 / 7.0)),
            ("VAR_SAMP(v)", 32.0 / 7.0),
            ("STDDEV(v)", Math.Sqrt(32.0 / 7.0)),   // bare = sample
            ("VARIANCE(v)", 32.0 / 7.0),
        })
        {
            var q = Compile(
                ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
                $"SELECT g, {fn} AS s FROM t GROUP BY g");
            foreach (var x in new[] { 2, 4, 4, 4, 5, 5, 7, 9 })
            {
                q.Table("t").Insert(1, x);
            }

            q.Step();
            Assert.Equal(expected, AggDouble(q, 1), 10);
        }
    }

    [Fact]
    public void Stddev_RevisesOnRetraction()
    {
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, VAR_POP(v) AS s FROM t GROUP BY g");
        q.Table("t").Insert(1, 2);
        q.Table("t").Insert(1, 4);
        q.Table("t").Insert(1, 100);
        q.Step();
        // {2,4,100}: mean 106/3, var_pop = ((2-m)²+(4-m)²+(100-m)²)/3.
        var m = 106.0 / 3.0;
        var expected3 = (Math.Pow(2 - m, 2) + Math.Pow(4 - m, 2) + Math.Pow(100 - m, 2)) / 3.0;
        Assert.Equal(expected3, AggDouble(q, 1), 8);

        q.Table("t").Delete(1, 100);   // back to {2,4}: mean 3, var_pop 1
        q.Step();
        Assert.Equal(1.0, AggDouble(q, 1), 10);
    }

    [Fact]
    public void Stddev_NullCountBoundaries()
    {
        // Empty group emits nothing; a single row → VAR_POP 0 but VAR_SAMP NULL.
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT)"],
            "SELECT g, VAR_POP(v) AS p, VAR_SAMP(v) AS s FROM t GROUP BY g");

        q.Table("t").Insert(1, 7);                       // single row
        q.Table("t").Insert(new object?[] { 2, null });  // all-NULL group (but present)
        q.Step();

        // Group 1: one non-null value → pop 0, samp NULL (n<2).
        Assert.Equal(1, WeightOf(q.Current, 1, 0.0, null));
        // Group 2: no non-null values → both NULL.
        Assert.Equal(1, WeightOf(q.Current, 2, null, null));
    }

    [Fact]
    public void Stddev_ZeroVarianceClampsNonNegative()
    {
        // All-equal values: true variance 0. The moment form can round to a tiny
        // negative before the clamp; result must be exactly 0, never NaN.
        var q = Compile(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, STDDEV_SAMP(v) AS s FROM t GROUP BY g");
        q.Table("t").Insert(1, 1000000);
        q.Table("t").Insert(1, 1000000);
        q.Table("t").Insert(1, 1000000);
        q.Step();
        Assert.Equal(0.0, AggDouble(q, 1), 12);
    }

    [Fact]
    public void GroupBy_AggregateInKey_Rejected()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (v INT NOT NULL)"],
            "SELECT SUM(v) FROM t GROUP BY SUM(v)"));
        Assert.Contains("aggregate", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    // ---- Outer join with a residual (non-equi ON conjunct) ----

    [Fact]
    public void LeftJoin_Residual_KeyMatchesButResidualFails_NullPads()
    {
        // The crux: a.k = b.k holds, but the residual a.v > b.w fails, so the
        // row is UNMATCHED and must NULL-pad — not vanish (post-join filter) and
        // not join (ignored residual).
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k AND a.v > b.w");

        q.Table("a").Insert(1, 5);
        q.Table("b").Insert(1, 10);   // key matches, but 5 > 10 is false
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 5, null));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void LeftJoin_Residual_TwoLeftRowsSameKeyDisagree()
    {
        // Two left rows share a key; the residual sorts them: one matches, one
        // pads. This is exactly what a per-key match test cannot express.
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k AND a.v > b.w");

        q.Table("a").Insert(1, 20);   // 20 > 10 → joins
        q.Table("a").Insert(1, 5);    // 5 > 10 fails → NULL-pads
        q.Table("b").Insert(1, 10);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 20, 10));
        Assert.Equal(1, WeightOf(q.Current, 5, null));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void LeftJoin_Residual_ResidualFlipsAcrossTicks()
    {
        // Incremental pad↔join transition driven by the residual, not the key.
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k AND a.v > b.w");

        q.Table("a").Insert(1, 5);
        q.Table("b").Insert(1, 10);   // 5 > 10 false → padded
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 5, null));

        q.Table("b").Delete(1, 10);
        q.Table("b").Insert(1, 3);    // now 5 > 3 → the pad retracts, the join appears
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 5, null));
        Assert.Equal(1, WeightOf(q.Current, 5, 3));
    }

    [Fact]
    public void LeftJoin_Residual_NullBearingLeftRowThatMatches_NoSpuriousPad()
    {
        // The NULL-safety hazard. The left row has a NULL in a non-key column
        // AND it matches. The anti-join keys on the whole row; if it dropped
        // NULL-bearing rows (as CompileSemiJoin does for probe keys), this row
        // would survive the subtraction and emit a NULL-pad ALONGSIDE its join
        // output. It must produce the join row only.
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.k, a.v, b.w FROM a LEFT JOIN b ON a.k = b.k AND b.w > 0");

        q.Table("a").Insert(new object?[] { 1, null });
        q.Table("b").Insert(1, 10);   // key matches, 10 > 0 → joins
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, null, 10));
        Assert.Equal(1, q.Current.Count);   // NOT 2 — no spurious pad
    }

    [Fact]
    public void LeftJoin_ComputedKeyAndCrossSideResidual_TpcdiScdShape()
    {
        // The exact ivm-bench securities/financials shape: a CAST-both-sides
        // equi-key (4a) AND a cross-side BETWEEN residual (4b), on a LEFT JOIN.
        // sec.pts must fall within [comp.lo, comp.hi] for the matching cik.
        var q = Compile(
            [
                "CREATE TABLE sec (cik INT NOT NULL, pts INT NOT NULL)",
                "CREATE TABLE comp (cid BIGINT NOT NULL, lo INT NOT NULL, hi INT NOT NULL)",
            ],
            "SELECT s.cik, s.pts, c.lo FROM sec s LEFT JOIN comp c " +
            "ON CAST(s.cik AS VARCHAR) = CAST(c.cid AS VARCHAR) " +
            "AND s.pts BETWEEN c.lo AND c.hi");

        q.Table("comp").Insert(1L, 100, 200);
        q.Table("sec").Insert(1, 150);   // cik 1 = cid 1, 150 in [100,200] → joins
        q.Table("sec").Insert(1, 50);    // cik matches but 50 not in [100,200] → pads
        q.Table("sec").Insert(2, 150);   // no matching cid → pads
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1, 150, 100));
        Assert.Equal(1, WeightOf(q.Current, 1, 50, null));
        Assert.Equal(1, WeightOf(q.Current, 2, 150, null));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void FullJoin_Residual_BothSidesPadIndependently()
    {
        var q = Compile(
            [
                "CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE b (k INT NOT NULL, w INT NOT NULL)",
            ],
            "SELECT a.v, b.w FROM a FULL JOIN b ON a.k = b.k AND a.v > b.w");

        q.Table("a").Insert(1, 5);    // key matches b(1,10) but 5 > 10 false
        q.Table("b").Insert(1, 10);
        q.Step();
        // Neither side matches under the residual → each pads.
        Assert.Equal(1, WeightOf(q.Current, 5, null));
        Assert.Equal(1, WeightOf(q.Current, null, 10));
        Assert.Equal(2, q.Current.Count);
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
