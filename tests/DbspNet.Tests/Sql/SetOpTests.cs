using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class SetOpTests
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
        z.WeightOf(new StructuralRow(row)).Value;

    // ---- Parser / precedence ----

    [Fact]
    public void Parser_UnionAllKindDistinguishedFromUnion()
    {
        var all = (SetOpQuery)Parser.ParseStatement("SELECT a FROM t UNION ALL SELECT b FROM u");
        var dist = (SetOpQuery)Parser.ParseStatement("SELECT a FROM t UNION SELECT b FROM u");
        Assert.Equal(SetOpKind.UnionAll, all.Kind);
        Assert.Equal(SetOpKind.Union, dist.Kind);
    }

    [Fact]
    public void Parser_IntersectAndExcept()
    {
        var i = (SetOpQuery)Parser.ParseStatement("SELECT a FROM t INTERSECT SELECT b FROM u");
        var e = (SetOpQuery)Parser.ParseStatement("SELECT a FROM t EXCEPT SELECT b FROM u");
        Assert.Equal(SetOpKind.Intersect, i.Kind);
        Assert.Equal(SetOpKind.Except, e.Kind);
    }

    [Fact]
    public void Parser_IntersectBindsTighterThanUnion()
    {
        // `a UNION b INTERSECT c` = `a UNION (b INTERSECT c)`.
        var q = (SetOpQuery)Parser.ParseStatement(
            "SELECT a FROM t UNION SELECT b FROM u INTERSECT SELECT c FROM v");
        Assert.Equal(SetOpKind.Union, q.Kind);
        Assert.Equal(2, q.Branches.Count);
        Assert.IsType<SelectStatement>(q.Branches[0]);
        var inner = Assert.IsType<SetOpQuery>(q.Branches[1]);
        Assert.Equal(SetOpKind.Intersect, inner.Kind);
    }

    [Fact]
    public void Parser_SameKindChainsFlatten()
    {
        var q = (SetOpQuery)Parser.ParseStatement(
            "SELECT a FROM t INTERSECT SELECT a FROM u INTERSECT SELECT a FROM v");
        Assert.Equal(SetOpKind.Intersect, q.Kind);
        Assert.Equal(3, q.Branches.Count);
    }

    [Fact]
    public void Parser_IntersectAllIsRejected()
    {
        Assert.Throws<ParseException>(() => Parser.ParseStatement(
            "SELECT a FROM t INTERSECT ALL SELECT b FROM u"));
    }

    [Fact]
    public void Parser_ExceptAllIsRejected()
    {
        Assert.Throws<ParseException>(() => Parser.ParseStatement(
            "SELECT a FROM t EXCEPT ALL SELECT b FROM u"));
    }

    // ---- UNION (DISTINCT) ----

    [Fact]
    public void Union_DedupsAcrossBranches()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t UNION SELECT v FROM u");

        q.Table("t").Insert(1);
        q.Table("u").Insert(1);  // duplicate across branches
        q.Table("u").Insert(2);
        q.Step();

        // UNION (DISTINCT): each row once.
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Union_DedupsWithinOneBranch()
    {
        // Intra-branch duplicates (k=1,v=x appearing twice) also collapse.
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t UNION SELECT v FROM u");

        q.Table("t").Insert(5);
        q.Table("t").Insert(5);  // same row inserted twice → weight 2 in bag, 1 in set
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Union_Incremental_RetractOneOfTwoContributors_KeepsRow()
    {
        // Row (1) is contributed by both t and u. Removing the t copy
        // shouldn't emit a retraction — set-view still has it from u.
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t UNION SELECT v FROM u");

        q.Table("t").Insert(1);
        q.Table("u").Insert(1);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 1));

        q.Table("t").Delete(1);
        q.Step();
        // Set still contains 1 via u — no delta.
        Assert.Equal(0, WeightOf(q.Current, 1));
    }

    [Fact]
    public void Union_Incremental_RetractBothContributors_RetractsRow()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t UNION SELECT v FROM u");

        q.Table("t").Insert(1);
        q.Table("u").Insert(1);
        q.Step();

        q.Table("t").Delete(1);
        q.Table("u").Delete(1);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 1));
    }

    // ---- INTERSECT ----

    [Fact]
    public void Intersect_KeepsOnlyRowsInBoth()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t INTERSECT SELECT v FROM u");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Table("u").Insert(2);
        q.Table("u").Insert(3);
        q.Table("u").Insert(4);
        q.Step();

        // Intersection: {2, 3}, each once.
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Intersect_IsDistinct_IgnoresDuplicateCopies()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t INTERSECT SELECT v FROM u");

        q.Table("t").Insert(5);
        q.Table("t").Insert(5);  // dup
        q.Table("u").Insert(5);
        q.Table("u").Insert(5);
        q.Table("u").Insert(5);  // triple
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Intersect_TreatsNullAsEqualToNull()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT)",
                "CREATE TABLE u (v INT)",
            ],
            "SELECT v FROM t INTERSECT SELECT v FROM u");

        q.Table("t").Insert((object?)null);
        q.Table("u").Insert((object?)null);
        q.Step();

        // SQL: `NULL INTERSECT NULL` → one NULL row (unlike equi-join,
        // which would drop NULL keys).
        Assert.Equal(1, WeightOf(q.Current, (object?)null));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void Intersect_Incremental_NewMatchAcrossSidesEmits()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t INTERSECT SELECT v FROM u");

        q.Table("t").Insert(5);
        q.Step();
        Assert.Empty(q.Current);   // no match on u side yet

        q.Table("u").Insert(5);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 5));   // now matched
    }

    // ---- EXCEPT ----

    [Fact]
    public void Except_ReturnsRowsInLeftNotInRight()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t EXCEPT SELECT v FROM u");

        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Table("t").Insert(3);
        q.Table("u").Insert(2);   // in both, excluded
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Except_IsDistinct()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t EXCEPT SELECT v FROM u");

        q.Table("t").Insert(1);
        q.Table("t").Insert(1);  // duplicates in left collapse
        q.Table("t").Insert(2);
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(2, q.Current.Count);
    }

    [Fact]
    public void Except_Incremental_AddingMatchToRight_Retracts()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t EXCEPT SELECT v FROM u");

        q.Table("t").Insert(5);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 5));

        // Adding 5 to u means it's now also in right — excepted.
        q.Table("u").Insert(5);
        q.Step();
        Assert.Equal(-1, WeightOf(q.Current, 5));
    }

    [Fact]
    public void Except_Incremental_RemovingFromRight_ReemitsLeftRow()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT NOT NULL)",
                "CREATE TABLE u (v INT NOT NULL)",
            ],
            "SELECT v FROM t EXCEPT SELECT v FROM u");

        q.Table("t").Insert(5);
        q.Table("u").Insert(5);
        q.Step();
        Assert.Empty(q.Current);

        q.Table("u").Delete(5);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 5));
    }

    [Fact]
    public void Except_TreatsNullAsEqualToNull()
    {
        var q = Compile(
            [
                "CREATE TABLE t (v INT)",
                "CREATE TABLE u (v INT)",
            ],
            "SELECT v FROM t EXCEPT SELECT v FROM u");

        q.Table("t").Insert((object?)null);
        q.Table("t").Insert(1);
        q.Table("u").Insert((object?)null);   // excepts the NULL from t
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, q.Current.Count);
    }

    // ---- Chains and compositions ----

    [Fact]
    public void IntersectChain_KeepsOnlyRowsInAllBranches()
    {
        var q = Compile(
            [
                "CREATE TABLE a (v INT NOT NULL)",
                "CREATE TABLE b (v INT NOT NULL)",
                "CREATE TABLE c (v INT NOT NULL)",
            ],
            "SELECT v FROM a INTERSECT SELECT v FROM b INTERSECT SELECT v FROM c");

        q.Table("a").Insert(1);
        q.Table("a").Insert(2);
        q.Table("b").Insert(2);
        q.Table("b").Insert(3);
        q.Table("c").Insert(2);
        q.Table("c").Insert(4);
        q.Step();

        // Only 2 appears in all three.
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, q.Current.Count);
    }

    [Fact]
    public void UnionIntersectComposition_RespectsPrecedence()
    {
        // `a UNION b INTERSECT c` = `a UNION (b ∩ c)`
        var q = Compile(
            [
                "CREATE TABLE a (v INT NOT NULL)",
                "CREATE TABLE b (v INT NOT NULL)",
                "CREATE TABLE c (v INT NOT NULL)",
            ],
            "SELECT v FROM a UNION SELECT v FROM b INTERSECT SELECT v FROM c");

        q.Table("a").Insert(1);
        q.Table("a").Insert(99);  // only in a
        q.Table("b").Insert(2);
        q.Table("b").Insert(3);
        q.Table("c").Insert(2);
        q.Table("c").Insert(4);
        q.Step();

        // b ∩ c = {2}. a ∪ {2} = {1, 99, 2}.
        Assert.Equal(1, WeightOf(q.Current, 1));
        Assert.Equal(1, WeightOf(q.Current, 99));
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(3, q.Current.Count);
    }

    [Fact]
    public void SetOps_WorkOnMultiColumnSchemas()
    {
        var q = Compile(
            [
                "CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)",
                "CREATE TABLE u (k INT NOT NULL, v INT NOT NULL)",
            ],
            "SELECT k, v FROM t INTERSECT SELECT k, v FROM u");

        q.Table("t").Insert(1, 10);
        q.Table("t").Insert(2, 20);
        q.Table("u").Insert(1, 10);   // match
        q.Table("u").Insert(2, 99);   // same k but different v — NOT a match
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, 1, 10));
        Assert.Equal(1, q.Current.Count);
    }
}
