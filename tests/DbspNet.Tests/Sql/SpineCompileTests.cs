// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase-2 smoke tests for the spine trace family wired into the SQL
/// compiler via <see cref="CompileOptions"/>. Each scenario drives a stateful
/// operator (DISTINCT, GROUP BY, INNER / LEFT JOIN) in spine mode, asserts the
/// incremental deltas are correct, confirms a spine operator actually engaged,
/// and cross-checks that the spine output matches the flat baseline tick for
/// tick. The exhaustive equivalence sweep lives in the PBT suite (Phase 3);
/// this is the wiring sanity check.
/// </summary>
public class SpineCompileTests
{
    private static (CompiledQuery Flat, CompiledQuery Spine) CompilePair(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        var flat = PlanToCircuit.Compile(plan);
        var spine = PlanToCircuit.Compile(plan, null, new CompileOptions { TraceFamily = TraceFamily.Spine });
        return (flat, spine);
    }

    private static void AssertSpineEngaged(CompiledQuery q) =>
        Assert.Contains(
            q.Circuit.Operators,
            op => op.GetType().Name.StartsWith("Spine", StringComparison.Ordinal));

    private static void AssertNoSpineOps(CompiledQuery q) =>
        Assert.DoesNotContain(
            q.Circuit.Operators,
            op => op.GetType().Name.StartsWith("Spine", StringComparison.Ordinal));

    /// <summary>
    /// Assert that at least one spine operator on <paramref name="q"/>'s
    /// circuit is closed over an emitted typed row, not over
    /// <see cref="StructuralRow"/>. This is the proof that the typed-row
    /// pipeline took the spine path (rather than silently falling back to
    /// the structural compile). The emitted struct types live in the
    /// <c>DbspNet.TypedRows</c> dynamic assembly and have names of the
    /// form <c>TypedRow_{guid}</c>; the absence of <c>StructuralRow</c>
    /// in the closure is the test.
    /// </summary>
    private static void AssertSpineClosedOverTypedRow(CompiledQuery q)
    {
        var spineOps = q.Circuit.Operators
            .Where(op => op.GetType().Name.StartsWith("Spine", StringComparison.Ordinal))
            .ToList();
        Assert.NotEmpty(spineOps);

        var anyTyped = spineOps.Any(op =>
            op.GetType().GetGenericArguments()
                .Any(t => t != typeof(StructuralRow) && t.Name.StartsWith("TypedRow_", StringComparison.Ordinal)));
        Assert.True(anyTyped,
            "expected at least one spine operator closed over an emitted typed row; " +
            "found only: " + string.Join(", ", spineOps.Select(op => op.GetType().ToString())));
    }

    /// <summary>Assert the two queries' current output deltas are identical.</summary>
    private static void AssertSameDelta(CompiledQuery flat, CompiledQuery spine)
    {
        Assert.Equal(flat.Current.Count, spine.Current.Count);
        foreach (var (row, w) in flat.Current)
        {
            Assert.Equal(w, spine.Current.WeightOf(row));
        }
    }

    [Fact]
    public void Distinct_ViaUnion_RetractsWhenLastSourceRemoved()
    {
        var (flat, spine) = CompilePair(
            ["CREATE TABLE a (x INT NOT NULL)", "CREATE TABLE b (x INT NOT NULL)"],
            "SELECT x FROM a UNION SELECT x FROM b");

        AssertSpineEngaged(spine);
        AssertNoSpineOps(flat);

        // Tick 1: distinct of {1, 2, 2} = {1, 2}.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("a").Insert(1);
            q.Table("a").Insert(2);
            q.Table("b").Insert(2);
            q.Step();
        }

        Assert.Equal(1, spine.WeightOf(1).Value);
        Assert.Equal(1, spine.WeightOf(2).Value);
        AssertSameDelta(flat, spine);

        // Tick 2: remove value 2 from both sources — it crosses back to zero,
        // so distinct emits a single retraction; value 1 is untouched.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("a").Delete(2);
            q.Table("b").Delete(2);
            q.Step();
        }

        Assert.Equal(1, spine.Current.Count);
        Assert.Equal(-1, spine.WeightOf(2).Value);
        AssertSameDelta(flat, spine);
    }

    [Fact]
    public void GroupBySum_RetractsAndReemitsChangedGroup()
    {
        var (flat, spine) = CompilePair(
            ["CREATE TABLE t (g INT NOT NULL, v INT NOT NULL)"],
            "SELECT g, SUM(v) AS total FROM t GROUP BY g");

        AssertSpineEngaged(spine);

        // Tick 1: group 1 sums to 30, group 2 to 5.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("t").Insert(1, 10);
            q.Table("t").Insert(1, 20);
            q.Table("t").Insert(2, 5);
            q.Step();
        }

        Assert.Equal(1, spine.WeightOf(1, 30L).Value);
        Assert.Equal(1, spine.WeightOf(2, 5L).Value);
        AssertSameDelta(flat, spine);

        // Tick 2: add 5 to group 1 — old total retracts, new total emits;
        // group 2 untouched.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("t").Insert(1, 5);
            q.Step();
        }

        Assert.Equal(2, spine.Current.Count);
        Assert.Equal(-1, spine.WeightOf(1, 30L).Value);
        Assert.Equal(1, spine.WeightOf(1, 35L).Value);
        AssertSameDelta(flat, spine);
    }

    [Fact]
    public void InnerJoin_EmitsOnLateArrival()
    {
        var (flat, spine) = CompilePair(
            ["CREATE TABLE a (id INT NOT NULL)", "CREATE TABLE b (id INT NOT NULL, tag VARCHAR NOT NULL)"],
            "SELECT a.id, b.tag FROM a INNER JOIN b ON a.id = b.id");

        AssertSpineEngaged(spine);

        // Tick 1: left row with no right match yet — nothing emitted.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("a").Insert(1);
            q.Step();
        }

        Assert.True(spine.Current.IsEmpty);
        AssertSameDelta(flat, spine);

        // Tick 2: the matching right row lands — the historical left row joins.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("b").Insert(1, "x");
            q.Step();
        }

        Assert.Equal(1, spine.Current.Count);
        Assert.Equal(1, spine.WeightOf(1, "x").Value);
        AssertSameDelta(flat, spine);
    }

    [Fact]
    public void LeftJoin_RetractsNullPadWhenMatchArrives()
    {
        var (flat, spine) = CompilePair(
            ["CREATE TABLE a (id INT NOT NULL)", "CREATE TABLE b (id INT NOT NULL, tag VARCHAR NOT NULL)"],
            "SELECT a.id, b.tag FROM a LEFT JOIN b ON a.id = b.id");

        AssertSpineEngaged(spine);

        // Tick 1: unmatched left row emits NULL-padded.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("a").Insert(1);
            q.Step();
        }

        Assert.Equal(1, spine.WeightOf(1, null).Value);
        AssertSameDelta(flat, spine);

        // Tick 2: match arrives — NULL-pad retracts, real join row emits.
        foreach (var q in new[] { flat, spine })
        {
            q.Table("b").Insert(1, "x");
            q.Step();
        }

        Assert.Equal(2, spine.Current.Count);
        Assert.Equal(-1, spine.WeightOf(1, null).Value);
        Assert.Equal(1, spine.WeightOf(1, "x").Value);
        AssertSameDelta(flat, spine);
    }

    // ---- Typed-row + spine integration ----
    //
    // The next block asserts the spine path is actually taking the
    // typed-row pipeline for typed-eligible queries (rather than
    // falling back to structural). The assertion looks at the spine
    // operators' closed generic args.

    [Fact]
    public void SpineDistinct_ClosesOverTypedRow()
    {
        // UNION (set semantics) lifts to a Distinct over UnionAll —
        // the simplest way to land a DistinctPlan on the typed-row
        // pipeline without relying on `SELECT DISTINCT` (the
        // resolver-level SELECT-DISTINCT form is a P1 deferral).
        var (_, spine) = CompilePair(
            ["CREATE TABLE a (x INT NOT NULL)", "CREATE TABLE b (x INT NOT NULL)"],
            "SELECT x FROM a UNION SELECT x FROM b");
        AssertSpineEngaged(spine);
        AssertSpineClosedOverTypedRow(spine);
    }

    [Fact]
    public void SpineAggregate_ClosesOverTypedRow()
    {
        var (_, spine) = CompilePair(
            ["CREATE TABLE t (k INT NOT NULL, v INT NOT NULL)"],
            "SELECT k, SUM(v) FROM t GROUP BY k");
        AssertSpineEngaged(spine);
        AssertSpineClosedOverTypedRow(spine);
    }

    [Fact]
    public void SpineInnerJoin_ClosesOverTypedRow()
    {
        var (_, spine) = CompilePair(
            ["CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
             "CREATE TABLE b (k INT NOT NULL, y INT NOT NULL)"],
            "SELECT a.x, b.y FROM a INNER JOIN b ON a.k = b.k");
        AssertSpineEngaged(spine);
        AssertSpineClosedOverTypedRow(spine);
    }

    [Fact]
    public void SpineLeftJoin_ClosesOverTypedRow()
    {
        var (_, spine) = CompilePair(
            ["CREATE TABLE a (k INT NOT NULL, x INT NOT NULL)",
             "CREATE TABLE b (k INT NOT NULL, y INT NOT NULL)"],
            "SELECT a.x, b.y FROM a LEFT JOIN b ON a.k = b.k");
        AssertSpineEngaged(spine);
        AssertSpineClosedOverTypedRow(spine);
    }
}
