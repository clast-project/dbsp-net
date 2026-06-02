// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

public class RecursiveCteTests
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

    // ---- Parser ----

    [Fact]
    public void Parser_WithRecursive_SetsFlag()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH RECURSIVE r AS (SELECT 1 AS x FROM t UNION ALL SELECT x FROM r) SELECT x FROM r");
        Assert.Single(stmt.Ctes);
        Assert.True(stmt.Ctes[0].IsRecursive);
    }

    [Fact]
    public void Parser_WithoutRecursive_FlagIsFalse()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH c AS (SELECT a FROM t) SELECT a FROM c");
        Assert.False(stmt.Ctes[0].IsRecursive);
    }

    [Fact]
    public void Parser_RecursiveFlagPropagatesToAllCtesInClause()
    {
        var stmt = (SelectStatement)Parser.ParseStatement(
            "WITH RECURSIVE a AS (SELECT 1 AS x FROM t UNION ALL SELECT x FROM a), b AS (SELECT x FROM a) SELECT x FROM b");
        Assert.Equal(2, stmt.Ctes.Count);
        Assert.True(stmt.Ctes[0].IsRecursive);
        Assert.True(stmt.Ctes[1].IsRecursive);
    }

    // ---- Resolver errors ----

    [Fact]
    public void Resolver_RecursiveCteMustBeUnionAll()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "WITH RECURSIVE r AS (SELECT a FROM t) SELECT a FROM r")));
    }

    [Fact]
    public void Resolver_RecursiveCteWithoutSelfReference_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        // RECURSIVE declared but body never references itself.
        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "WITH RECURSIVE r AS (SELECT a FROM t UNION ALL SELECT a FROM t) SELECT a FROM r")));
    }

    [Fact]
    public void Resolver_RecursiveCteAllBranchesSelfReferencing_Throws()
    {
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (a INT NOT NULL)"));

        // No base case: every branch references the CTE.
        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "WITH RECURSIVE r AS (SELECT a FROM r UNION ALL SELECT a FROM r) SELECT a FROM r")));
    }

    // ---- Transitive closure ----

    [Fact]
    public void TransitiveClosure_FindsAllReachablePairs()
    {
        // Graph: 1 -> 2 -> 3 -> 4, plus 2 -> 5.
        // Reachable pairs: (1,2),(2,3),(3,4),(2,5) base + transitive closure.
        var q = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        q.Table("edges").Insert(1, 2);
        q.Table("edges").Insert(2, 3);
        q.Table("edges").Insert(3, 4);
        q.Table("edges").Insert(2, 5);
        q.Step();

        // Direct edges.
        Assert.Equal(1, WeightOf(q.Current, 1, 2));
        Assert.Equal(1, WeightOf(q.Current, 2, 3));
        Assert.Equal(1, WeightOf(q.Current, 3, 4));
        Assert.Equal(1, WeightOf(q.Current, 2, 5));
        // Transitive reachability.
        Assert.Equal(1, WeightOf(q.Current, 1, 3));
        Assert.Equal(1, WeightOf(q.Current, 1, 4));
        Assert.Equal(1, WeightOf(q.Current, 1, 5));
        Assert.Equal(1, WeightOf(q.Current, 2, 4));
        Assert.Equal(8, q.Current.Count);
    }

    [Fact]
    public void TransitiveClosure_IsIncrementalAcrossTicks()
    {
        var q = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        // Tick 1: chain 1 -> 2 -> 3.
        q.Table("edges").Insert(1, 2);
        q.Table("edges").Insert(2, 3);
        q.Step();
        // Expected reach: (1,2),(2,3),(1,3) — three pairs.
        Assert.Equal(3, q.Current.Count);

        // Tick 2: add edge 3 -> 4. Delta contains NEW pairs only.
        q.Table("edges").Insert(3, 4);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 3, 4));  // direct edge
        Assert.Equal(1, WeightOf(q.Current, 2, 4));  // 2 via 3
        Assert.Equal(1, WeightOf(q.Current, 1, 4));  // 1 via 2 via 3
        // The three previously-emitted pairs should NOT re-emit.
        Assert.Equal(0, WeightOf(q.Current, 1, 2));
        Assert.Equal(0, WeightOf(q.Current, 2, 3));
        Assert.Equal(0, WeightOf(q.Current, 1, 3));
    }

    [Fact]
    public void TransitiveClosure_RetractionRemovesAffectedPairs()
    {
        var q = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        q.Table("edges").Insert(1, 2);
        q.Table("edges").Insert(2, 3);
        q.Step();
        // Reach: (1,2),(2,3),(1,3).

        // Remove the bridging edge.
        q.Table("edges").Delete(2, 3);
        q.Step();
        // Pairs that should be retracted: (2,3) and (1,3).
        Assert.Equal(-1, WeightOf(q.Current, 2, 3));
        Assert.Equal(-1, WeightOf(q.Current, 1, 3));
        Assert.Equal(0, WeightOf(q.Current, 1, 2));  // still reachable
    }

    // ---- Cycles in the graph ----

    [Fact]
    public void CyclicGraph_Terminates_UnderSetSemantics()
    {
        // 1 -> 2 -> 1 (cycle). Set semantics ensures we don't diverge.
        var q = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        q.Table("edges").Insert(1, 2);
        q.Table("edges").Insert(2, 1);
        q.Step();

        // Reachable pairs: (1,2),(2,1),(1,1),(2,2).
        Assert.Equal(1, WeightOf(q.Current, 1, 2));
        Assert.Equal(1, WeightOf(q.Current, 2, 1));
        Assert.Equal(1, WeightOf(q.Current, 1, 1));
        Assert.Equal(1, WeightOf(q.Current, 2, 2));
        Assert.Equal(4, q.Current.Count);
    }

    // ---- Filtering ----

    [Fact]
    public void RecursiveCte_CanBeFilteredByOuterQuery()
    {
        var q = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT dst FROM reach WHERE src = 1");

        q.Table("edges").Insert(1, 2);
        q.Table("edges").Insert(2, 3);
        q.Table("edges").Insert(3, 4);
        q.Table("edges").Insert(5, 6);   // disconnected
        q.Step();

        // Reachable from 1: {2, 3, 4}.
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
        Assert.Equal(1, WeightOf(q.Current, 4));
        Assert.Equal(3, q.Current.Count);
    }

    // ---- Termination guard ----
    //
    // Under set semantics, transitive closure over a finite edge set always
    // terminates. The termination guard would kick in only under pathological
    // queries that produce fresh rows forever — those require types like
    // unbounded integers and a query that e.g. always increments, which v1
    // doesn't easily let us construct. So we just verify the guard is wired
    // up (reaching the iterations cap throws).
    //
    // The guard lives in the nested fixpoint driver (FixpointOperator's
    // maxIterations). Since all practical SQL recursive CTEs over finite inputs
    // DO terminate under set semantics, a natural trigger is hard to construct;
    // we skip a dedicated trigger test.

    // ---- Non-recursive CTE still works ----

    [Fact]
    public void RecursiveFlagOnNonSelfReferencingCte_Rejected()
    {
        // Consistency check: a CTE declared RECURSIVE must self-reference.
        var cat = new Catalog();
        var r = new Resolver(cat);
        r.Resolve(Parser.ParseStatement("CREATE TABLE t (x INT NOT NULL)"));

        Assert.Throws<ResolveException>(() => r.Resolve(Parser.ParseStatement(
            "WITH RECURSIVE r AS (SELECT x FROM t UNION ALL SELECT x FROM t) SELECT x FROM r")));
    }

    // ---- Semi-naïve incremental path ----

    [Fact]
    public void TransitiveClosure_ManySequentialInsertTicks_MatchesBatch()
    {
        // Stress the semi-naïve incremental extension path: build a
        // 10-node path graph one edge at a time across 9 separate ticks,
        // and confirm the final accumulated output matches what a single
        // batch tick would produce.
        var incremental = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        var accumulated = ZSet<StructuralRow, Z64>.Empty;
        for (var i = 1; i < 10; i++)
        {
            incremental.Table("edges").Insert(i, i + 1);
            incremental.Step();
            accumulated = accumulated + incremental.Current;
        }

        // Oracle: same graph, single tick.
        var batch = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");
        for (var i = 1; i < 10; i++)
        {
            batch.Table("edges").Insert(i, i + 1);
        }

        batch.Step();

        Assert.Equal(batch.Current, accumulated);
    }

    [Fact]
    public void TransitiveClosure_InsertThenRetractThenInsert_StaysCorrect()
    {
        // Exercise the retraction fallback: after semi-naïve extension,
        // retract an edge (forcing full recompute), then extend again
        // (semi-naïve again from a correctly-rebuilt R).
        var q = Compile(
            ["CREATE TABLE edges (src INT NOT NULL, dst INT NOT NULL)"],
            "WITH RECURSIVE reach AS ( " +
            "    SELECT src, dst FROM edges " +
            "    UNION ALL " +
            "    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) " +
            "SELECT src, dst FROM reach");

        // Tick 1: build 1→2→3 incrementally (second edge via semi-naïve).
        q.Table("edges").Insert(1, 2);
        q.Step();
        q.Table("edges").Insert(2, 3);
        q.Step();

        // Tick 3: retract the bridge → R drops to {(1,2)} via full recompute.
        q.Table("edges").Delete(2, 3);
        q.Step();
        Assert.Equal(-1, q.Current.WeightOf(new StructuralRow(2, 3)).Value);
        Assert.Equal(-1, q.Current.WeightOf(new StructuralRow(1, 3)).Value);

        // Tick 4: add 2→3 back, plus 3→4. Semi-naïve from R={(1,2)}:
        // should re-derive (2,3),(1,3) and also derive the new rows through 3→4.
        q.Table("edges").Insert(2, 3);
        q.Table("edges").Insert(3, 4);
        q.Step();
        Assert.Equal(1, q.Current.WeightOf(new StructuralRow(2, 3)).Value);
        Assert.Equal(1, q.Current.WeightOf(new StructuralRow(1, 3)).Value);
        Assert.Equal(1, q.Current.WeightOf(new StructuralRow(3, 4)).Value);
        Assert.Equal(1, q.Current.WeightOf(new StructuralRow(2, 4)).Value);
        Assert.Equal(1, q.Current.WeightOf(new StructuralRow(1, 4)).Value);
    }

    // ---- Non-recursive CTE still works ----

    [Fact]
    public void NonRecursiveCte_StillWorksWithoutRecursiveKeyword()
    {
        var q = Compile(
            ["CREATE TABLE t (x INT NOT NULL)"],
            "WITH c AS (SELECT x FROM t WHERE x > 0) SELECT x FROM c");

        q.Table("t").Insert(-1);
        q.Table("t").Insert(5);
        q.Step();
        Assert.Equal(1, WeightOf(q.Current, 5));
        Assert.Equal(1, q.Current.Count);
    }
}
